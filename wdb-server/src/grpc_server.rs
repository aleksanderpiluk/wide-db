use std::{net::{IpAddr, Ipv4Addr, SocketAddr}, sync::Arc};

use tonic::{transport::Server, Request, Response, Status};
use wdb_core::{Module, StorageEngine};
use wdb_grpc::wdb_grpc::{wide_db_server::{WideDb, WideDbServer}, GetRequest, GetResponse, CreateTableRequest, ListTablesResponse, FILE_DESCRIPTOR_SET};

#[derive(Debug)]
pub struct WideDBImpl {
    storage_engine: Arc<dyn StorageEngine>
}

#[tonic::async_trait]
impl WideDb for WideDBImpl {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        todo!();
    }

    async fn create_table(&self, request: Request<CreateTableRequest>) -> Result<Response<()>, Status> {
        let reqData = request.into_inner();
        let tableName = reqData.name;
        self.storage_engine.create_table(&tableName);

        Ok(Response::new(()))
    }

    async fn list_tables(&self, request: Request<()>) -> Result<Response<ListTablesResponse>, Status> {
        let names = self.storage_engine.list_tables();

        Ok(Response::new(ListTablesResponse { tables: names }))
    }
}

pub struct GrpcServer {}

impl Module for GrpcServer {
    fn init(&self, storage_engine: Arc<dyn StorageEngine>) {
        println!("GrpcServer is starting...");
        const DEFAULT_PORT: u16 = 50051;

        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build()
            .unwrap();

            Server::builder()
                .add_service(WideDbServer::new(WideDBImpl{ storage_engine: storage_engine }))
                .add_service(service)
                .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), DEFAULT_PORT))
                .await.unwrap();
        });
    }

    fn destoy(&self) {
        
    }
}