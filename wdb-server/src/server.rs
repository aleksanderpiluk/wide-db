use std::sync::{Arc, Mutex};

use wdb_core::{command::command_executor::CommandExecutor, module::Module, storage_engine::StorageEngine};

pub struct Server {
    cmd_exec: Arc<CommandExecutor>,
    storage_engine: Arc<dyn StorageEngine>,
    modules: Vec<Arc<dyn Module>>
}

impl Server {
    pub fn init(storage_engine: Arc<dyn StorageEngine>) -> Server {
        let cmd_exec = Arc::new(CommandExecutor {
            storage_engine: storage_engine.clone(),
        });

        Server {
            cmd_exec,
            storage_engine, 
            modules: vec![] 
        }
    }

    pub fn add_module<T: Module + Send + Sync + 'static>(&mut self, module: T) {
        let module = Arc::new(module);
        {
            let module = module.clone();
            let cmd_exec = self.cmd_exec.clone();
            tokio::spawn(async move {
                module.init(cmd_exec);
            });
        }
        self.modules.push(module);
    }
}