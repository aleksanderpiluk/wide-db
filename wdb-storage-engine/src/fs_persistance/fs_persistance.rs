use std::{cmp::max, collections::HashMap, fs::{self, read_dir}, io::{Cursor, Read, Seek, Write}};
use bytes::Bytes;
use log::debug;

use crate::{utils::sstable::SSTable, PersistanceLayer};

use super::storage_paths::StoragePaths;

#[derive(Debug, Clone)]
pub struct FSPersistance {}  

impl FSPersistance {
    pub fn new() -> FSPersistance {
        FSPersistance { }
    }
}

impl PersistanceLayer for FSPersistance {
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

    fn get_segment_read(&self, table: &Bytes, family: &Bytes, segment: &Bytes) -> impl Read + Seek {
        let segment = std::str::from_utf8(&segment.clone()).unwrap().to_string();
        let path = StoragePaths::get_family_dir(table, &family).join(segment);

        let res = fs::File::open(path);
        match res {
            Err(err) => panic!("{:?}", err),
            Ok(file) => file,
        }
    }

    fn get_tables_list(&self) -> Vec<(Bytes, u64, Vec<(Bytes, Vec<SSTable>)>)> {
        let paths = read_dir(StoragePaths::base()).unwrap();

        let mut results = vec![];

        for path in paths {
            let path = path.unwrap().path();
            let name = path.file_name().unwrap().to_str().unwrap();
            if name.ends_with(".table") {
                let table_name = Bytes::from(name.strip_suffix(".table").unwrap().to_string());
                let mut families = vec![];
                let mut max_mvcc: u64 = 0;

                let paths = read_dir(path).unwrap();
                for path in paths {
                    let path = path.unwrap().path();
                    let name = path.file_name().unwrap().to_str().unwrap();

                    if name.ends_with(".family") {
                        let family_name = Bytes::from(name.strip_suffix(".family").unwrap().to_string());
                        let mut segments = vec![];
                        
                        let paths = read_dir(path).unwrap();
                        for path in paths {
                            let path = path.unwrap().path();
                            let name = Bytes::from(path.file_name().unwrap().to_str().unwrap().to_string());
                            
                            let r = fs::File::open(path).unwrap();
                            let segment = SSTable::read(&table_name, &family_name, &name, r);
                            max_mvcc = max(max_mvcc, segment.get_max_mvcc_id());
                            segments.push(segment);
                        }

                        families.push((family_name, segments));
                    }
                }

                results.push((table_name, max_mvcc, families));
            }
        }

        results
    }
}