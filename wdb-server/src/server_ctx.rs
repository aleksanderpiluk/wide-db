use std::sync::Arc;

use wdb_storage_engine::StorageEngine;

#[derive(Clone)]
pub struct ServerCtx {
    pub storage_engine: Arc<StorageEngine>,
}