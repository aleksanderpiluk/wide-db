use std::sync::Arc;

use wdb_storage_engine::{PersistanceLayer, StorageEngine};

#[derive(Clone)]
pub struct ServerCtx<P: PersistanceLayer> {
    pub storage_engine: Arc<StorageEngine<P>>,
}