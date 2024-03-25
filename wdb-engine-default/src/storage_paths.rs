use std::path::{Path, PathBuf};

pub struct StoragePaths { }

impl StoragePaths {
    pub fn base() -> PathBuf {
        Path::new("/usr/local/wdb/").to_path_buf()
    }

    pub fn table_metadata() -> PathBuf {
        StoragePaths::base().join("table_metadata/")
    }

    pub fn table_data() -> PathBuf {
        StoragePaths::base().join("table_data/")
    }
}