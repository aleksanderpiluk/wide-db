use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use tonic::transport::Server;
use wdb_grpc::wdb_grpc::{wide_db_server::WideDbServer, FILE_DESCRIPTOR_SET};
use wdb_storage_engine::PersistanceLayer;

use crate::{grpc::HandlersService, server_ctx::ServerCtx};

pub struct GrpcApi {
    
}

impl GrpcApi {
    pub fn init<P: PersistanceLayer>(server_ctx: ServerCtx<P>) -> GrpcApi {
        const DEFAULT_PORT: u16 = 50051;
        const IP_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
        const SOCKET_ADDR: SocketAddr = SocketAddr::new(IP_ADDR, DEFAULT_PORT);

        let grpc_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build()
            .unwrap();

        let handlers_service = HandlersService::new(server_ctx);

        tokio::spawn(async {
            Server::builder()
                .add_service(WideDbServer::new(handlers_service))
                .add_service(grpc_service)
                .serve(SOCKET_ADDR).await.unwrap();
        });

        GrpcApi {  }
    }
}