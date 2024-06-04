use std::path::{Path, PathBuf};

use bytes::Bytes;

pub struct StoragePaths { }

impl StoragePaths {
    pub fn base() -> PathBuf {
        Path::new("/usr/local/wdb/").to_path_buf()
    }

    pub fn table_dir(table_name: &Bytes) -> PathBuf {
        let table_name = std::str::from_utf8(&table_name.clone()).unwrap().to_string();
        StoragePaths::base().join(table_name + ".table/")
    }

    pub fn get_family_dir(table_name: &Bytes, family_name: &Bytes) -> PathBuf {
        let family_name = std::str::from_utf8(&family_name.clone()).unwrap().to_string();
        StoragePaths::table_dir(table_name).join(family_name + ".family/")
    }
}