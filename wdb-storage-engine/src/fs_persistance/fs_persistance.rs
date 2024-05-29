use std::{fs, io::{Cursor, Write}};
use bytes::Bytes;
use log::debug;

use crate::PersistanceLayer;

use super::storage_paths::StoragePaths;

#[derive(Debug, Clone)]
pub struct FSPersistance {
    cursor: Cursor<Vec<u8>>
}  

impl FSPersistance {
    pub fn new() -> FSPersistance {
        FSPersistance { 
            cursor: Cursor::new(vec![])
        }
    }
}

impl PersistanceLayer for FSPersistance {
    fn get_sstable_write(&mut self, name: bytes::Bytes) -> &mut impl Write {
        &mut self.cursor
    }

    fn get_segment_write(&self, table: &Bytes, family: Bytes, segment: &Bytes) -> impl Write {
        let segment = std::str::from_utf8(&segment.clone()).unwrap().to_string();
        let path = StoragePaths::get_family_dir(table, &family).join(segment);
        debug!("Path {:?}", path);
        let parent = path.parent().unwrap();
        fs::create_dir_all(parent).unwrap();
        let res = fs::File::create(path);
        match res {
            Err(err) => panic!("{:?}", err),
            Ok(file) => file,
        }
    }
}