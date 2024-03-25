use std::sync::Arc;

use crate::{Module, StorageEngine, FS};

pub struct AppController {
    storage_engine: Arc<dyn StorageEngine>,
    fs: Box<dyn FS>,
    modules: Vec<Box<dyn Module>>
} 

impl AppController {
    pub fn init(fs: Box<dyn FS>, storageEngine: Arc<dyn StorageEngine>) -> AppController {
        AppController { 
            storage_engine: storageEngine, 
            fs: fs, 
            modules: vec![] 
        }
    }

    pub fn add_module(&mut self, module: Box<dyn Module>) {
        module.init(self.storage_engine.clone());
        self.modules.push(module);
    }
}