// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::metadata_manager::MetadataManager;
use anyhow::{bail, Result};
use aptos_indexer_grpc_utils::{
    config::IndexerGrpcFileStoreConfig, file_store_operator_v2::FileStoreOperatorV2,
};
use aptos_protos::{
    internal::fullnode::v1::{
        transactions_from_node_response::Response, GetTransactionsFromNodeRequest,
    },
    transaction::v1::Transaction,
};
use futures::StreamExt;
use prost::Message;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc::channel, RwLock};
use tracing::{error, trace, warn};

const MAX_CACHE_SIZE: usize = 10 * (1 << 30);
const TARGET_CACHE_SIZE: usize = 8 * (1 << 30);

struct Cache {
    start_version: u64,
    file_store_version: AtomicU64,
    transactions: VecDeque<Transaction>,
    cache_size: usize,
}

impl Cache {
    fn new(file_store_version: u64) -> Self {
        Self {
            start_version: file_store_version,
            file_store_version: AtomicU64::new(file_store_version),
            transactions: VecDeque::new(),
            cache_size: 0,
        }
    }

    fn maybe_evict(&mut self) -> bool {
        if self.cache_size <= MAX_CACHE_SIZE {
            return true;
        }

        while self.start_version < self.file_store_version.load(Ordering::SeqCst)
            && self.cache_size > TARGET_CACHE_SIZE
        {
            let transaction = self.transactions.pop_front().unwrap();
            self.cache_size -= transaction.encoded_len();
            self.start_version += 1;
        }

        self.cache_size <= MAX_CACHE_SIZE
    }

    fn put_transactions(&mut self, transactions: Vec<Transaction>) {
        self.cache_size += transactions
            .iter()
            .map(|transaction| transaction.encoded_len())
            .sum::<usize>();
        self.transactions.extend(transactions);
    }

    fn get_transactions(
        &self,
        start_version: u64,
        max_size_bytes: usize,
        update_file_store_version: bool,
    ) -> Vec<Transaction> {
        if !update_file_store_version {
            trace!(
            "Requesting version {start_version} from cache, update_file_store_version = {update_file_store_version}.",
        );
            trace!(
                "Current data range in cache: [{}, {}).",
                self.start_version,
                self.start_version + self.transactions.len() as u64
            );
        }
        if start_version < self.start_version {
            return vec![];
        }

        let mut transactions = vec![];
        let mut size_bytes = 0;
        for transaction in self
            .transactions
            .iter()
            .skip((start_version - self.start_version) as usize)
        {
            size_bytes += transaction.encoded_len();
            transactions.push(transaction.clone());
            if size_bytes > max_size_bytes {
                // Note: We choose to not pop the last transaction here, so the size could be
                // slightly larger than the `max_size_bytes`. This is fine.
                break;
            }
        }
        if update_file_store_version {
            self.file_store_version
                .fetch_add(transactions.len() as u64, Ordering::SeqCst);
        } else {
            trace!(
                "Returned {} transactions from Cache, total {size_bytes} bytes.",
                transactions.len()
            );
        }
        transactions
    }
}

pub(crate) struct DataManager {
    cache: RwLock<Cache>,
    file_store_operator: FileStoreOperatorV2,
    metadata_manager: Arc<MetadataManager>,
}

impl DataManager {
    pub(crate) async fn new(
        chain_id: u64,
        file_store_config: IndexerGrpcFileStoreConfig,
        file_store_version: u64,
        metadata_manager: Arc<MetadataManager>,
    ) -> Self {
        let file_store = file_store_config.create_filestore().await;
        let file_store_operator = FileStoreOperatorV2::new(chain_id, file_store, 10000);
        Self {
            cache: RwLock::new(Cache::new(file_store_version)),
            file_store_operator,
            metadata_manager,
        }
    }

    pub(crate) async fn start(&self) {
        'out: loop {
            let mut fullnode_client = self.metadata_manager.get_fullnode_for_request();
            let cache = self.cache.read().await;
            let request = GetTransactionsFromNodeRequest {
                starting_version: Some(cache.start_version + cache.transactions.len() as u64),
                transactions_count: Some(100000),
            };
            drop(cache);

            let response = fullnode_client.get_transactions_from_node(request).await;
            if response.is_err() {
                warn!(
                    "Error when getting transactions from fullnode: {}",
                    response.err().unwrap()
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            let mut response = response.unwrap().into_inner();
            while let Some(response_item) = response.next().await {
                loop {
                    if self.cache.write().await.maybe_evict() {
                        break;
                    }
                    let cache = self.cache.read().await;
                    warn!("Filestore is lagging behind, cache is full [{}, {}), known_latest_version ({}).", cache.start_version, cache.start_version + cache.transactions.len() as u64, self.metadata_manager.get_known_latest_version());
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                match response_item {
                    Ok(r) => {
                        if let Some(response) = r.response {
                            match response {
                                Response::Data(data) => {
                                    self.cache.write().await.put_transactions(data.transactions);
                                },
                                Response::Status(_) => continue,
                            }
                        } else {
                            warn!("Error when getting transactions from fullnode: no data.");
                            continue 'out;
                        }
                    },
                    Err(e) => {
                        warn!("Error when getting transactions from fullnode: {}", e);
                        continue 'out;
                    },
                }
            }
        }
    }

    pub(crate) fn lagging(&self, cache_next_version: u64) -> bool {
        // TODO(grao): Need a better way, we can use the information in the metadata_manager.
        cache_next_version + 20000 < self.metadata_manager.get_known_latest_version()
    }

    pub(crate) async fn get_transactions(
        &self,
        start_version: u64,
        max_size: usize,
    ) -> Result<Vec<Transaction>> {
        let cache = self.cache.read().await;
        let cache_start_version = cache.start_version;
        let cache_next_version = cache_start_version + cache.transactions.len() as u64;
        drop(cache);

        if start_version >= cache_start_version {
            if start_version >= cache_next_version {
                // If lagging, try to fetch the data from FN.
                if self.lagging(cache_next_version) {
                    trace!("GrpcManager is lagging, getting data from FN, requested_version: {start_version}, cache_next_version: {cache_next_version}.");
                    let request = GetTransactionsFromNodeRequest {
                        starting_version: Some(cache_next_version),
                        transactions_count: Some(5000),
                    };

                    let mut fullnode_client = self.metadata_manager.get_fullnode_for_request();
                    let response = fullnode_client.get_transactions_from_node(request).await?;
                    let mut response = response.into_inner();
                    while let Some(Ok(response_item)) = response.next().await {
                        if let Some(response) = response_item.response {
                            match response {
                                Response::Data(data) => {
                                    return Ok(data.transactions);
                                },
                                Response::Status(_) => continue,
                            }
                        }
                    }
                }

                // Let client side to retry.
                return Ok(vec![]);
            }
            // NOTE: We are not holding the read lock for cache here. Therefore it's possible that
            // the start_version becomes older than the cache.start_version. In that case the
            // following function will return empty return, and let the client to retry.
            return Ok(self
                .get_transactions_from_cache(
                    start_version,
                    max_size,
                    /*update_file_store_version=*/ false,
                )
                .await);
        }

        let (tx, mut rx) = channel(1);
        self.file_store_operator
            .get_transaction_batch(
                start_version,
                /*retries=*/ 3,
                /*max_files=*/ Some(1),
                tx,
            )
            .await;

        if let Some(mut transactions) = rx.recv().await {
            trace!(
                "Transactions returned from filestore: [{start_version}, {}).",
                transactions.last().unwrap().version
            );
            let first_version = transactions.first().unwrap().version;
            Ok(transactions.split_off((first_version - start_version) as usize))
        } else {
            let error_msg = "Failed to fetch transactions from filestore, either filestore is not available, or data is corrupted.";
            // TODO(grao): Consider downgrade this to warn! if this happens too frequently when
            // filestore is unavailable.
            error!(error_msg);
            bail!(error_msg);
        }
    }

    pub(crate) async fn get_transactions_from_cache(
        &self,
        start_version: u64,
        max_size: usize,
        update_file_store_version: bool,
    ) -> Vec<Transaction> {
        self.cache
            .read()
            .await
            .get_transactions(start_version, max_size, update_file_store_version)
    }
}
