// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

pub mod data_manager;
pub mod file_store_uploader;
pub mod metadata_manager;
pub mod metrics;
pub mod service;

use crate::{
    data_manager::DataManager, metadata_manager::MetadataManager, service::GrpcManagerService,
};
use anyhow::Result;
use aptos_indexer_grpc_server_framework::RunnableConfig;
use aptos_indexer_grpc_utils::config::IndexerGrpcFileStoreConfig;
use aptos_protos::indexer::v1::grpc_manager_server::GrpcManagerServer;
use file_store_uploader::FileStoreUploader;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tonic::{codec::CompressionEncoding, transport::Server};

const HTTP2_PING_INTERVAL_DURATION: Duration = Duration::from_secs(60);
const HTTP2_PING_TIMEOUT_DURATION: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ServiceConfig {
    listen_address: SocketAddr,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerGrpcManagerConfig {
    chain_id: u64,
    service_config: ServiceConfig,
    file_store_config: IndexerGrpcFileStoreConfig,
    self_advertised_address: String,
    grpc_manager_addresses: Vec<String>,
    fullnode_addresses: Vec<String>,
}

#[async_trait::async_trait]
impl RunnableConfig for IndexerGrpcManagerConfig {
    async fn run(&self) -> Result<()> {
        GrpcManager::new(self).await.start(&self.service_config);

        Ok(())
    }

    fn get_server_name(&self) -> String {
        "grpc_manager".to_string()
    }
}

struct GrpcManager {
    chain_id: u64,
    filestore_uploader: Mutex<FileStoreUploader>,
    metadata_manager: Arc<MetadataManager>,
    data_manager: Arc<DataManager>,
}

impl GrpcManager {
    pub(crate) async fn new(config: &IndexerGrpcManagerConfig) -> Self {
        let chain_id = config.chain_id;
        let filestore_uploader = Mutex::new(
            FileStoreUploader::new(chain_id, config.file_store_config.clone())
                .await
                .expect(&format!(
                    "Failed to create filestore uploader, config: {:?}.",
                    config.file_store_config
                )),
        );
        let metadata_manager = Arc::new(MetadataManager::new(
            config.self_advertised_address.clone(),
            config.grpc_manager_addresses.clone(),
            config.fullnode_addresses.clone(),
        ));
        let data_manager = Arc::new(
            DataManager::new(
                chain_id,
                config.file_store_config.clone(),
                filestore_uploader.lock().await.version(),
                metadata_manager.clone(),
            )
            .await,
        );
        Self {
            chain_id,
            filestore_uploader,
            metadata_manager,
            data_manager,
        }
    }

    pub(crate) fn start(&self, service_config: &ServiceConfig) {
        let service = GrpcManagerServer::new(GrpcManagerService::new(
            self.chain_id,
            self.metadata_manager.clone(),
            self.data_manager.clone(),
        ))
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd);
        let server = Server::builder()
            .http2_keepalive_interval(Some(HTTP2_PING_INTERVAL_DURATION))
            .http2_keepalive_timeout(Some(HTTP2_PING_TIMEOUT_DURATION))
            .add_service(service);

        tokio_scoped::scope(|s| {
            s.spawn(async move {
                self.metadata_manager.start().await.unwrap();
            });
            s.spawn(async move { self.data_manager.start().await });
            s.spawn(async move {
                self.filestore_uploader
                    .lock()
                    .await
                    .start(self.data_manager.clone())
                    .await
                    .unwrap();
            });
            s.spawn(async move {
                server.serve(service_config.listen_address).await.unwrap();
            });
        });
    }
}
