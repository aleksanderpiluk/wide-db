use std::sync::{Arc, Mutex};

use bytes::Bytes;
use dashmap::{mapref::one::RefMut, DashMap};

use crate::{ flush_agent::FlushAgent, key_value::KeyValue, row_filter::RowFilter, row_mutation::RowMutationExecutor, row_result::RowResult, table::Table, utils::{hashed_bytes::HashedBytes, Timestamp}, PersistanceLayer, RowMutation, RowMutationOp, TableFamily};

pub struct StorageEngine<P: PersistanceLayer> {
    tables: DashMap<u64, Table>,
    tables_lock: Mutex<()>,
    persistance_layer: P,
}

impl<P: PersistanceLayer> StorageEngine<P> {
    pub fn empty(persistance_layer: P, flush_agent: bool) -> Arc<StorageEngine<P>> {
        let tables_data = persistance_layer.get_tables_list();
        let tables = DashMap::new();
        for table_data in tables_data {
            let name = HashedBytes::from_bytes(table_data.0);
            let id = *name.hash_as_ref();
            tables.insert(
                id, 
                Table::new_from_families_vec(
                    id, 
                    name.bytes_as_ref().clone(), 
                    table_data.1,
                    table_data.2
                )
            );
        }

        let engine = Arc::new(StorageEngine {
            tables,
            tables_lock: Mutex::new(()),
            persistance_layer,
        });

        if flush_agent {
            FlushAgent::new(engine.clone());
        }

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

        let iter = table.scan(self.get_persitance_layer(), Some(start), Some(end));
        
        return RowResult { 
            row: row.bytes_as_ref().clone(), 
            cells: iter.collect(), 
        }
    }

    pub fn scan(&self, table: Bytes, start: Option<KeyValue>, end: Option<KeyValue>, filter: Option<&dyn RowFilter>) -> Vec<KeyValue> {
        let table: RefMut<u64, Table, std::hash::RandomState> = self.get_table(table.clone()).unwrap();

        let iter = table.scan(self.get_persitance_layer(), start, end);
        iter.collect::<Vec<KeyValue>>()
    }

    pub fn get_persitance_layer(&self) -> &P {
        &self.persistance_layer
    }
}