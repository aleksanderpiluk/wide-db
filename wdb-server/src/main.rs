mod server;
mod server_ctx;
mod grpc;

use log::info;
use tokio::signal;
use wdb_storage_engine::{FSPersistance, StorageEngine};

use self::server::Server;
use grpc::GrpcApi;

#[tokio::main]
async fn main() {
    env_logger::init();

    info!("WideDB server is starting...");

    info!("Initializing storage engine...");
    let storage_engine = StorageEngine::empty(FSPersistance::new(), true);
    info!("Storage engine initialization success!");

    info!("Initializing app server...");
    let server = Server::init(storage_engine);
    info!("App server initialization success!");

    info!("Starting grpc server...");
    GrpcApi::init(server.get_ctx().clone());

    match signal::ctrl_c().await {
        Ok(()) => {},
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
        }
    };
}
