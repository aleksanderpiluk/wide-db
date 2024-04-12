mod utils;
mod table;
mod fs_controller;

use std::{collections::HashMap, fs::{create_dir, read_dir}, hash::Hash, sync::{Arc, RwLock}};

use fs_controller::{structs::TableMetadata, FSController};
use utils::table_id::TableIdGen;
use wdb_core::storage_engine::StorageEngine;

#[derive(Debug)]
pub struct DefaultStorageEngine {
    // tables: Arc<RwLock<HashMap<String, TableMetadata>>>,
    tables: RwLock<Vec<Arc<TableMetadata>>>,
    id_mapping: HashMap<String, Arc<TableMetadata>>,
    name_mapping: HashMap<String, Arc<TableMetadata>>,
}

impl DefaultStorageEngine {
    pub fn init() -> DefaultStorageEngine {
        if !FSController::structure_exists() {
            FSController::create_initial_structure();

            DefaultStorageEngine {
                tables: RwLock::new(vec![]),
                id_mapping: HashMap::new(),
                name_mapping: HashMap::new(),
            }
        } else {
            let root_file = FSController::read_root_file();
            println!("{:?}", root_file);
            let metadata = FSController::read_tables_metadata(&root_file.tables).unwrap();   

            let mut tables: Vec<Arc<TableMetadata>> = vec![];
            let mut id_mapping = HashMap::<String, Arc<TableMetadata>>::with_capacity(metadata.len());
            let mut name_mapping = HashMap::<String, Arc<TableMetadata>>::with_capacity(metadata.len());

            for item in metadata {
                let arc = Arc::new(item);
                id_mapping.insert(arc.id.clone(), arc.clone());
                name_mapping.insert(arc.name.clone(), arc.clone());
                tables.push(arc)
            }

            DefaultStorageEngine {
                tables: RwLock::new(tables),
                id_mapping: id_mapping,
                name_mapping: name_mapping,
            }
        }
    }

}

impl StorageEngine for DefaultStorageEngine {
    fn create_table(&self, name: &String, families: &Vec<String>) -> Result<(), &'static str>{
        let mut tables = self.tables.write().unwrap();
        let exists = self.table_with_name_exists(name);
        if exists {
            return Err("Table with this name already exists.");
        }


        let name = name.clone();
        let id = TableIdGen::gen_id();

        let metadata = TableMetadata::new(id, name, families);
        FSController::create_table_initial_files(&metadata);

        tables.push(Arc::new(metadata));
        FSController::update_root_file_from_metadata(tables.clone());
        
        Ok(())
    }

    fn add_table_family(&self, table_name: &String, family_name: &String) -> Result<(), &'static str> {
        todo!()
        // let mut tables = self.tables.write().unwrap();
        
        // let table = self.name_mapping.get(table_name.clone().as_str()).ok_or("Table with this name not exists.")?;
        
        // if table.families.get(family_name).is_some() {
        //     return Err("Family with this name already exists.");
        // }

        // table.families.insert(family_name.clone());
        // FSController::update_root_file_from_metadata(tables.clone());
        // Ok(())
    }

    fn list_tables(&self) -> Vec<String> {
        todo!()
        // let tables = self.tables.read().unwrap();
        // let iter = tables.iter();
        // let mut result: Vec<String> = vec![];
        
        // for item in iter {
        //     let (name, _) = item;
        //     result.push(name.clone());
        // }

        // return result;
    }

    fn table_with_name_exists(&self, name: &String) -> bool {
        self.name_mapping.contains_key(name)
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
