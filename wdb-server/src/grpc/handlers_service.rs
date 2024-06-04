use tonic::{Request, Response, Status};
use wdb_grpc::wdb_grpc::{wide_db_server::WideDb, *};
use wdb_storage_engine::PersistanceLayer;

use crate::server_ctx::ServerCtx;

use super::handlers;

pub struct HandlersService<P: PersistanceLayer> {
    server_ctx: ServerCtx<P>,
}

impl<P: PersistanceLayer> HandlersService<P> {
    pub fn new(ctx: ServerCtx<P>) -> HandlersService<P> {
        HandlersService { server_ctx: ctx }
    }
}

#[tonic::async_trait]
impl<P: PersistanceLayer> WideDb for HandlersService<P> {
    async fn create_table(&self, request: Request<CreateTableRequest>) -> Result<Response<Table>, Status> {
        handlers::create_table(&self.server_ctx, request).await
    }

    async fn list_tables(&self, request: Request<()>) -> Result<Response<ListTablesResponse>, Status> {
        todo!()
    } 

    async fn read_row(&self, request: Request<ReadRowRequest>) -> Result<Response<ReadRowResponse>, Status> {
        handlers::read_row(&self.server_ctx, request).await
    }

    async fn mutate_row(&self, request: Request<MutateRowRequest>) -> Result<Response<()>, Status> {
        handlers::row_mutate(&self.server_ctx, request).await
    }
}