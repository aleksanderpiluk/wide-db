mod grpc_server;

use std::sync::Arc;

use wdb_engine_default::DefaultStorageEngine;
use wdb_fs_default::DefaultFSController;
use wdb_core::app_controller::AppController;

use crate::grpc_server::GrpcServer;

fn main() {
    println!("WideDB server is starting...");

    let fs = Box::new(DefaultFSController::init());
    let storageEngine = Arc::new(DefaultStorageEngine::init());
    
    let mut controller = AppController::init(fs, storageEngine);
    controller.add_module(Box::new(GrpcServer{}));
}
