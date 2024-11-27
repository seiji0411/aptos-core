// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! This module defines representation of AptosDB indexer data structures at physical level via schemas
//! that implement [`aptos_schemadb::schema::Schema`].
//!
//! All schemas are `pub(crate)` so not shown in rustdoc, refer to the source code to see details.

pub mod event_by_key;
pub mod event_by_version;
pub mod indexer_metadata;
pub mod state_keys;
pub mod table_info;
pub mod ordered_transaction_by_account;
pub mod orderless_transaction_by_account;
pub mod transaction_summaries_by_account;
use aptos_schemadb::ColumnFamilyName;

pub const DEFAULT_COLUMN_FAMILY_NAME: ColumnFamilyName = "default";
pub const INDEXER_METADATA_CF_NAME: ColumnFamilyName = "indexer_metadata";
pub const INTERNAL_INDEXER_METADATA_CF_NAME: ColumnFamilyName = "internal_indexer_metadata";
pub const TABLE_INFO_CF_NAME: ColumnFamilyName = "table_info";
pub const EVENT_BY_KEY_CF_NAME: ColumnFamilyName = "event_by_key";
pub const EVENT_BY_VERSION_CF_NAME: ColumnFamilyName = "event_by_version";
pub const ORDERED_TRANSACTION_BY_ACCOUNT_CF_NAME: ColumnFamilyName = "transaction_by_account";
pub const ORDERLESS_TRANSACTION_BY_ACCOUNT_CF_NAME: ColumnFamilyName = "orderless_transaction_by_account";
pub const TRANSACTION_SUMMARIES_BY_ACCOUNT_CF_NAME: ColumnFamilyName = "transaction_summaries_by_account";
pub const STATE_KEYS_CF_NAME: ColumnFamilyName = "state_keys";

pub fn column_families() -> Vec<ColumnFamilyName> {
    vec![
        /* empty cf */ DEFAULT_COLUMN_FAMILY_NAME,
        INDEXER_METADATA_CF_NAME,
        TABLE_INFO_CF_NAME,
    ]
}

pub fn internal_indexer_column_families() -> Vec<ColumnFamilyName> {
    vec![
        /* empty cf */ DEFAULT_COLUMN_FAMILY_NAME,
        INTERNAL_INDEXER_METADATA_CF_NAME,
        EVENT_BY_KEY_CF_NAME,
        EVENT_BY_VERSION_CF_NAME,
        ORDERED_TRANSACTION_BY_ACCOUNT_CF_NAME,
        ORDERLESS_TRANSACTION_BY_ACCOUNT_CF_NAME,
        TRANSACTION_SUMMARIES_BY_ACCOUNT_CF_NAME,
        STATE_KEYS_CF_NAME,
    ]
}
