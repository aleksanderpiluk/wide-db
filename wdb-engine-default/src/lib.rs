mod storage_paths;

use std::{collections::HashMap, fs::{create_dir, read_dir}, sync::{Arc, RwLock}};

use storage_paths::StoragePaths;
use wdb_core::StorageEngine;

#[derive(Debug)]
struct TableMetadata {
    name: String,
    id: u32,
}

#[derive(Debug)]
pub struct DefaultStorageEngine {
    tables: Arc<RwLock<HashMap<String, TableMetadata>>>,
}

impl DefaultStorageEngine {
    pub fn init() -> DefaultStorageEngine {
        let p = StoragePaths::base();
        if !p.is_dir() {
            if p.exists() {
                panic!("Fatal error: {} exists but it's not a directory.", p.display());
            }

            create_dir(p).unwrap();
            DefaultStorageEngine::create_base_storage_structure();
        }

        DefaultStorageEngine::eventual_structure_recovery();

        DefaultStorageEngine::read_tables_metadata();

        let tables = Arc::new(RwLock::new(HashMap::<String, TableMetadata>::new()));

        DefaultStorageEngine { tables: tables }
    }

    fn create_base_storage_structure() {
        create_dir(StoragePaths::table_metadata()).unwrap();
        create_dir(StoragePaths::table_data()).unwrap();
    }

    fn eventual_structure_recovery() {
        let p = StoragePaths::table_metadata();
        if !p.exists() {
            create_dir(p).unwrap();
        }

        let p = StoragePaths::table_data();
        if !p.exists() {
            create_dir(p).unwrap();
        }
    }

    fn read_tables_metadata() {
        // for entry in read_dir(StoragePaths::table_metadata())? {
            
        // }
    }
}

impl StorageEngine for DefaultStorageEngine {
    fn create_table(&self, name: &String) {
        let name = name.clone();
        self.tables.write().unwrap().entry(name.clone()).or_insert(TableMetadata {
            id: 0,
            name: name,
        });
    }

    fn list_tables(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        let iter = tables.iter();
        let mut result: Vec<String> = vec![];
        
        for item in iter {
            let (name, _) = item;
            result.push(name.clone());
        }

        return result;
    }
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
