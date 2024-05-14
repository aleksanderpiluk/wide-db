mod storage_paths;
pub mod structs;

use std::{fs, sync::Arc};

use self::{storage_paths::StoragePaths, structs::{DbRootFile, TableMetadata}};

pub struct FSController {

}

impl FSController {
    pub fn structure_exists() -> bool {
        if StoragePaths::base().exists() && StoragePaths::root_file().exists() {
            true
        } else {
            false
        }
    }

    pub fn read_root_file() -> DbRootFile {
        let p = StoragePaths::root_file();

        let b = fs::read(p).expect("Failed to read root file.");
        let data: DbRootFile = bincode::deserialize(&b).expect("Failed to deserialize root file.");

        data
    }

    pub fn read_tables_metadata(ids: &Vec<String>) -> Result<Vec<TableMetadata>, &'static str> {
        let mut results = vec![];

        for id in ids {
            let m = FSController::read_table_metadata(id)?;
            results.push(m);
        }

        Ok(results)
    }

    pub fn create_initial_structure() {
        let p = StoragePaths::base();
        if p.is_dir() {
            let is_empty = fs::read_dir(p.clone()).unwrap().next().is_none();
            if !is_empty {
                panic!("{} is not empty. Remove all files to create database files structure.", p.display());    
            }
        } else if p.exists() {
            panic!("{} exists. Remove it in order to create database files structure.", p.display());
        } else {
            fs::create_dir(p).expect("Failed to create base directory.");
        }
               
        let p = StoragePaths::root_file();
        let data = DbRootFile::empty();
        let serialized = bincode::serialize(&data).expect("Failed to serialize root file data.");
        fs::write(p, serialized).expect("Failed to write root file.");
    }

    pub fn update_root_file_from_metadata(metadatas: Vec<Arc<TableMetadata>>) {
        let p = StoragePaths::root_file();

        let mut ids: Vec<String> = Vec::<String>::with_capacity(metadatas.len());
        for item in metadatas {
            ids.push(item.id.clone());
        }
        
        let data = bincode::serialize(&DbRootFile::from(ids)).expect("Failed to serialize root file data");
        fs::write(p, data).expect("Failed to write root file.");
    }
 
    pub fn read_table_metadata(id: &String) -> Result<TableMetadata, &'static str> {
        let p = StoragePaths::table_metadata(id);

        let b = fs::read(p).expect(format!("Failed to read metadata of table ID: {}", id).as_str());
        let data: Result<TableMetadata, _> = bincode::deserialize(&b);
        match data {
            Ok(data) => Ok(data),
            Err(_) => Err(Box::leak(format!("Failed to read table metadata ID: {}", &id).into_boxed_str())),
        }
    }

    pub fn create_table_initial_files(metadata: &TableMetadata) {
        let p = StoragePaths::table_metadata(&metadata.id);
        let data = bincode::serialize(metadata).expect("Failed to serialize table metadata.");
        fs::create_dir_all(p.clone().parent().unwrap()).expect("Failed to create table directories");
        fs::write(p, data).expect("Failed to write table metadata to disk.");
    }
}