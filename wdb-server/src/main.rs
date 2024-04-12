mod grpc_server;

use std::sync::Arc;

use wdb_engine_default::DefaultStorageEngine;
use wdb_core::app_controller::AppController;

use crate::grpc_server::GrpcServer;

fn main() {
    println!("WideDB server is starting...");

    println!("Initializing storage engine...");
    let storage_engine = Arc::new(DefaultStorageEngine::init());
    println!("Storage engine initialization success!");
    
    println!("Initializing app controller...");
    let mut controller = AppController::init(storage_engine);
    println!("App controller initialization success!");

    println!("Starting app modules initialization...");

    controller.add_module(Box::new(GrpcServer{}));
}
