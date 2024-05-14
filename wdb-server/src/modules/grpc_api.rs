use std::{net::{IpAddr, Ipv4Addr, SocketAddr}, sync::Arc};

use tokio::task::JoinError;
use tonic::{transport::Server, Request, Response, Status};
use wdb_core::{command::{command_error::CommandError, command_executor::CommandExecutor, commands::{create_table::CommandCreateTable, get_row::CommandGetRow}}, module::Module};
use wdb_grpc::wdb_grpc::{wide_db_server::{WideDb, WideDbServer}, CreateTableRequest, DeleteRequest, GetRowRequest, ListTablesResponse, PutRowRequest, FILE_DESCRIPTOR_SET};

#[derive(Debug)]
pub struct WideDBImpl {
    cmd_exec: Arc<CommandExecutor>,
}

#[tonic::async_trait]
impl WideDb for WideDBImpl {

    async fn create_table(&self, request: Request<CreateTableRequest>) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let result = self.cmd_exec.exec_command(CommandCreateTable {
            name: request.name,
            families: request.families,
        }).await;

        let result = WideDBImpl::handle_cmd_exec_err(result)?;

        // TODO: It's not always invalid argument error...
        Ok(Response::new(()))
    }

    async fn list_tables(&self, request: Request<()>) -> Result<Response<ListTablesResponse>, Status> {
        todo!()
        // let names = self.storage_engine.list_tables();

        // Ok(Response::new(ListTablesResponse { tables: names }))
    }

    async fn put_row(&self, request: Request<PutRowRequest>) -> Result<Response<()>, Status> {
        panic!();
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<()>, Status> {
        panic!();
    }

    async fn get_row(&self, request: Request<GetRowRequest>) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        
        let result = self.cmd_exec.exec_command(CommandGetRow {
            table: request.table,
            row: request.row,
        }).await;

        let result = WideDBImpl::handle_cmd_exec_err(result)?;
        Ok(Response::new(()))
    }
}

impl WideDBImpl {
    fn handle_cmd_exec_err<T>(result: Result<Result<T, CommandError>, JoinError>) -> Result<T, Status> {
        match result {
            Ok(r) => {
                match r {
                    Ok(val) => Ok(val),
                    Err(err) => {
                        match err {
                            CommandError::InputValidadationError(str) => Err(Status::invalid_argument(str)),
                            CommandError::ExecutionError(str) => Err(Status::failed_precondition(str)),
                        }
                    }
                }
            },
            Err(err) => Err(Status::internal(err.to_string())),
        }
    }
}

pub struct GrpcApi {}

impl Module for GrpcApi {
    fn init(&self, cmd_exec: Arc<CommandExecutor>) {
        println!("GrpcServer is starting...");
        const DEFAULT_PORT: u16 = 50051;

        tokio::spawn(async {
            let service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build()
            .unwrap();

            Server::builder()
                .add_service(WideDbServer::new(WideDBImpl{ cmd_exec }))
                .add_service(service)
                .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), DEFAULT_PORT))
                .await.unwrap();
        });
    }

    fn destoy(&self) {
        
    }
}