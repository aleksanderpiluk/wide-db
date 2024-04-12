use std::sync::Arc;

use crate::storage_engine::StorageEngine;

pub struct CommandContext {
    pub storage_engine: Arc<dyn StorageEngine>,
}