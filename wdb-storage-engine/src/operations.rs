use std::sync::Arc;

use wdb_core::storage_engine::StorageEngine;

use crate::{fs_controller::{structs::TableMetadata, FSController}, utils::table_id::TableIdGen};

use super::{row_mutation::RowMutation, DefaultStorageEngine};

impl StorageEngine for DefaultStorageEngine {
    fn create_table(&self, name: &String, families: &Vec<String>) -> Result<(), &'static str>{
        todo!()
        // let mut tables = self.tables.write().unwrap();
        // let exists = self.table_with_name_exists(name);
        // if exists {
        //     return Err("Table with this name already exists.");
        // }


        // let name = name.clone();
        // let id = TableIdGen::gen_id();

        // let metadata = TableMetadata::new(id, name, families);
        // FSController::create_table_initial_files(&metadata);

        // tables.push(Arc::new(metadata));
        // FSController::update_root_file_from_metadata(tables.clone());
        
        // Ok(())
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
        todo!();
        // self.name_mapping.contains_key(name)
    }

    fn get_row(&self, table_name: &String, row: &String) -> Result<(), &'static str> {
        todo!()
    }
}
