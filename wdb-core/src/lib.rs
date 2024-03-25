use std::{fmt::Debug, sync::Arc};

pub mod app_controller;

pub trait StorageEngine: Sync + Send + Debug {
    fn create_table(&self, name: &String);
    fn list_tables(&self) -> Vec<String>;
}

pub trait FS {
    
}

pub trait Module {
    fn init(&self, storage_engine: Arc<dyn StorageEngine>);
    fn destoy(&self);
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn it_works() {
//         let result = add(2, 2);
//         assert_eq!(result, 4);
//     }
// }
