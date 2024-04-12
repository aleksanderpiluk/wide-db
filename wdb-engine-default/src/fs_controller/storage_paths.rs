use std::path::{Path, PathBuf};

pub struct StoragePaths { }

impl StoragePaths {
    pub fn base() -> PathBuf {
        Path::new("/usr/local/wdb/").to_path_buf()
    }

    pub fn root_file() -> PathBuf {
        StoragePaths::base().join(".db")        
    }

    pub fn table_metadata(id: &String) -> PathBuf {
        StoragePaths::base().join(id).join(".meta")
    }

    pub fn table_data() -> PathBuf {
        StoragePaths::base().join("table_data/")
    }
}