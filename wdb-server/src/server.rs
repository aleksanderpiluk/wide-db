use std::sync::{Arc, Mutex};

use wdb_storage_engine::{PersistanceLayer, StorageEngine};

use crate::server_ctx::ServerCtx;

pub struct Server<P: PersistanceLayer> {
    storage_engine: Arc<StorageEngine<P>>,
    ctx: ServerCtx<P>,
}

impl<P: PersistanceLayer> Server<P> {
    pub fn init(storage_engine: Arc<StorageEngine<P>>) -> Server<P> {
        let ctx = ServerCtx {
            storage_engine: storage_engine.clone(),
        };

        Server {
            storage_engine, 
            ctx,
        }
    }

    pub fn get_ctx(&self) -> &ServerCtx<P> {
        &self.ctx
    }
}