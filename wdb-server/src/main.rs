mod modules;
mod server;

use std::sync::Arc;
use tokio::signal;

use wdb_storage_engine::storage_engine::DefaultStorageEngine;

use self::server::Server;
use self::modules::GrpcApi;

#[tokio::main]
async fn main() {
    println!("WideDB server is starting...");

    println!("Initializing storage engine...");
    let storage_engine = Arc::new(DefaultStorageEngine::init());
    println!("Storage engine initialization success!");

    println!("Initializing app server...");
    let mut server = Server::init(storage_engine);
    println!("App server initialization success!");

    println!("Starting app modules initialization...");

    server.add_module(GrpcApi {});

    match signal::ctrl_c().await {
        Ok(()) => {},
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
        }
    };
}
