use std::sync::{Arc, Mutex};

use wdb_storage_engine::StorageEngine;

use crate::server_ctx::ServerCtx;

pub struct Server {
    storage_engine: Arc<StorageEngine>,
    ctx: ServerCtx,
}

impl Server {
    pub fn init(storage_engine: Arc<StorageEngine>) -> Server {
        let ctx = ServerCtx {
            storage_engine: storage_engine.clone(),
        };

        Server {
            storage_engine, 
            ctx,
        }
    }

    pub fn get_ctx(&self) -> &ServerCtx {
        &self.ctx
    }
}