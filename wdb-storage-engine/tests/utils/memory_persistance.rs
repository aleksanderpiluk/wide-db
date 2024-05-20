use std::{collections::HashMap, io::{Cursor, Write}};

use bytes::Bytes;
use wdb_storage_engine::PersistanceLayer;

pub struct MemoryPersistance {
    sstable_files: HashMap<Bytes, Cursor<Vec<u8>>>
}

impl MemoryPersistance {
    pub fn new() -> MemoryPersistance {
        MemoryPersistance {
            sstable_files: HashMap::new()
        }
    }
}

impl PersistanceLayer for MemoryPersistance {
    fn get_sstable_write(&mut self, name: Bytes) -> &mut impl Write {
        let c = self.sstable_files.entry(name.clone()).or_insert_with(|| Cursor::new(Vec::new()));
        c
    }
}