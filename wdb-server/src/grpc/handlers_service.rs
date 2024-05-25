use tonic::{Request, Response, Status};
use wdb_grpc::wdb_grpc::{wide_db_server::WideDb, *};

use crate::server_ctx::ServerCtx;

use super::handlers;

pub struct HandlersService {
    server_ctx: ServerCtx,
}

impl HandlersService {
    pub fn new(ctx: ServerCtx) -> HandlersService {
        HandlersService { server_ctx: ctx }
    }
}

#[tonic::async_trait]
impl WideDb for HandlersService {
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