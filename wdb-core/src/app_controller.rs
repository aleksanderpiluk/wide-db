use std::sync::Arc;

use crate::{command::command_executor::CommandExecutor, module::Module, storage_engine::StorageEngine};

pub struct AppController {
    cmd_exec: Arc<CommandExecutor>,
    storage_engine: Arc<dyn StorageEngine>,
    modules: Vec<Box<dyn Module>>
} 

impl AppController {
    pub fn init(storage_engine: Arc<dyn StorageEngine>) -> AppController {
        let cmd_exec = Arc::new(CommandExecutor {
            storage_engine: storage_engine.clone(),
        });

        AppController { 
            cmd_exec,
            storage_engine, 
            modules: vec![] 
        }
    }

    pub fn add_module(&mut self, module: Box<dyn Module>) {
        module.init(self.cmd_exec.clone());
        self.modules.push(module);
    }
}