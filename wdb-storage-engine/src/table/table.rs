use std::{ops::{Bound, Range, RangeBounds}, sync::{Mutex, RwLock}};

use bytes::Bytes;
use dashmap::{iter::Iter, mapref::one::RefMut, DashMap};
use itertools::kmerge;

use crate::{cell::{Cell, CellType}, delete_tracker::DeleteTracker, key_value::KeyValue, kv_scanner::KVScanner, memtable::Memtable, row_lock::RowLockContext, utils::hashed_bytes::HashedBytes};

use super::{table_family::TableFamily};

#[derive(Debug)]
pub struct Table {
    id: u64,
    name: Bytes,
    families: DashMap<u64, TableFamily>,
    memtable: Memtable,
    row_locks: DashMap<u64, RowLockContext>,
    families_lock: Mutex<()>,
}

impl Table {
    pub fn new(id: u64, name: Bytes) -> Table {
        Table {
            id,
            name,
            families: DashMap::new(),
            memtable: Memtable::new(),
            row_locks: DashMap::new(),
            families_lock: Mutex::new(()),
        }
    }

    pub fn get_family(&self, name: &Bytes) -> Option<RefMut<u64, TableFamily>> {
        let name = HashedBytes::from_bytes(name.clone());

        self.families.get_mut(name.hash_as_ref())
    }

    pub fn get_families_iter(&self) -> Iter<u64, TableFamily> {
        self.families.iter()
    }

    pub fn create_family(&mut self, name: Bytes) -> Result<(), &'static str> {
        let name = HashedBytes::from_bytes(name);
        let id = *name.hash_as_ref();
        
        let _lock = self.families_lock.lock().unwrap();

        if self.families.contains_key(&id) {
            return Err("Family with this name already exists.")
        }

        let family = TableFamily::new(id, name.bytes_as_ref().clone());
        self.families.insert(id, family);

        Ok(())
    }
 
    pub fn get_row_lock(&self, row: &HashedBytes) -> dashmap::mapref::one::RefMut<u64, RowLockContext, std::hash::RandomState> {
        let hash = *row.hash_as_ref();
        
        let lock = self.row_locks.entry(hash).or_insert_with(|| RowLockContext {
            row: row.clone(),
            lock: RwLock::new(true),
        });
        lock
    }

    pub fn insert_kv(&self, cell: KeyValue) {
        self.memtable.insert(cell);
    }

}

impl KVScanner for Table {
    fn scan(&self, start: Option<KeyValue>, end: Option<KeyValue>) -> impl Iterator<Item = KeyValue> {
        let iter = self.memtable.scan(start.clone(), end.clone());
        
        let merge_iter = kmerge(vec![iter]);

        let mut delete_tracker = DeleteTracker::new();

        let mut current_row: Vec<u8> = vec![];
        merge_iter.map(move |cell| {
            let row = cell.get_row();
            if row != current_row {
                delete_tracker.reset();
                current_row = row.to_vec();
            }

            delete_tracker.add(&cell);

            match cell.get_cell_type() {
                CellType::Put => {
                    if delete_tracker.is_deleted(&cell) {
                        return None;
                    }
                    return Some(cell);
                },
                _ => {},
            }
            return None;
        }).flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_test() {
        let mut table: Table = Table::new(0, Bytes::from("test_name"));
        
        table.create_family(Bytes::from("")).unwrap();
        table.get_family(&Bytes::from(""));

        assert!(table.get_families_iter().count() == 1);
    }
}