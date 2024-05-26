use std::sync::{Arc, Mutex};

use bytes::Bytes;
use dashmap::{mapref::one::RefMut, DashMap};

use crate::{ flush_agent::FlushAgent, key_value::KeyValue, kv_scanner::KVScanner, row_filter::RowFilter, row_mutation::RowMutationExecutor, row_result::RowResult, table::Table, utils::{hashed_bytes::HashedBytes, Timestamp}, RowMutation, RowMutationOp, TableFamily};

pub struct StorageEngine {
    tables: DashMap<u64, Table>,
    tables_lock: Mutex<()>,    
}

impl StorageEngine {
    pub fn empty() -> Arc<StorageEngine> {
        let engine = Arc::new(StorageEngine {
            tables: DashMap::new(),
            tables_lock: Mutex::new(()),
        });

        FlushAgent::new(engine.clone());

        engine
    }    

    pub fn create_table(&self, name: Bytes) -> Result<(), &'static str> {
        let name = HashedBytes::from_bytes(name);

        let _lock = self.tables_lock.lock().unwrap();

        let id = *name.hash_as_ref();
        if self.tables.contains_key(&id) {
            return Err("Table with this name already exists.");
        }

        let table = Table::new(id, name.bytes_as_ref().clone());

        self.tables.insert(id, table);

        Ok(())
    }

    pub fn get_tables_iter(&self) -> dashmap::iter::Iter<u64, Table, std::hash::RandomState, DashMap<u64, Table>> {
        self.tables.iter()
    }

    pub fn get_table(&self, name: Bytes) -> Option<RefMut<u64, Table>> {
        let name = HashedBytes::from_bytes(name);
        let id = *name.hash_as_ref();
        
        self.tables.get_mut(&id)
    }

    pub fn execute_row_mutation(&self, mutation: RowMutation) {
        let table = self.get_table(mutation.table.clone()).unwrap();
        let row = HashedBytes::from_bytes(mutation.row.clone());
        
        RowMutationExecutor::unsafe_execute_row_mutation(table, row, mutation.ops);
    }

    pub fn read_row(&self, table: Bytes, row: Bytes, filter: Option<&dyn RowFilter>) -> RowResult {
        let table: RefMut<u64, Table, std::hash::RandomState> = self.get_table(table.clone()).unwrap();

        let row = HashedBytes::from_bytes(row.clone());

        let start = KeyValue::new_first_on_row(row.bytes_as_ref());
        let end = KeyValue::new_last_on_row(row.bytes_as_ref());

        let iter = table.scan(Some(start), Some(end));
        
        return RowResult { 
            row: row.bytes_as_ref().clone(), 
            cells: iter.collect(), 
        }
    }

    pub fn scan(&self, table: Bytes, start: Option<KeyValue>, end: Option<KeyValue>, filter: Option<&dyn RowFilter>) -> Vec<KeyValue> {
        let table: RefMut<u64, Table, std::hash::RandomState> = self.get_table(table.clone()).unwrap();

        let iter = table.scan(start, end);
        iter.collect::<Vec<KeyValue>>()
    }
}