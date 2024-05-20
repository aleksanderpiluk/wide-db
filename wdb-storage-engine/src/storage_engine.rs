use std::{ops::{Bound, Range, RangeBounds}, sync::Mutex, time::{SystemTime, UNIX_EPOCH}};

use bytes::Bytes;
use dashmap::{mapref::one::RefMut, DashMap};

use crate::{cell::CellType, key_value::KeyValue, kv_scanner::KVScanner, row_filter::RowFilter, row_result::RowResult, table::Table, utils::hashed_bytes::HashedBytes, RowMutation, RowMutationOp};

pub struct StorageEngine {
    tables: DashMap<u64, Table>,
    tables_lock: Mutex<()>,    
}

impl StorageEngine {
    pub fn empty() -> StorageEngine {
        StorageEngine {
            tables: DashMap::new(),
            tables_lock: Mutex::new(()),
        }
    }    

    pub fn create_table(&mut self, name: Bytes) -> Result<(), &'static str> {
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

    pub fn get_table(&mut self, name: Bytes) -> Option<RefMut<u64, Table>> {
        let name = HashedBytes::from_bytes(name);
        let id = *name.hash_as_ref();
        
        self.tables.get_mut(&id)
    }

    pub fn execute_row_mutation(&mut self, mutation: RowMutation) {
        let table = self.get_table(mutation.table.clone()).unwrap();

        let row = HashedBytes::from_bytes(mutation.row.clone());

        for op in mutation.ops {
            match op {
                RowMutationOp::Put { family, column, timestamp, value } => {
                    let family_name = family.clone();
                    let family = table.get_family(&family).unwrap(); //TODO: Only to check existance.

                    let row_lock = table.get_row_lock(&row);
                    let w = row_lock.write_lock();

                    let timestamp = match timestamp {
                        Some(ts) => ts,
                        None => {
                            SystemTime::now().duration_since(UNIX_EPOCH).expect("Time is broken...").as_millis() as u64
                        }
                    };

                    table.insert_kv(KeyValue::new(row.bytes_as_ref(), &family_name, &column, &timestamp, &CellType::Put, &value));

                    drop(w);
                },
                RowMutationOp::DeleteCell { family, column, timestamp} => {
                    let family_name = family.clone();
                    let family = table.get_family(&family).unwrap();

                    let row_lock = table.get_row_lock(&row);
                    let w = row_lock.write_lock();

                    let timestamp = match timestamp {
                        Some(ts) => ts,
                        None => {
                            SystemTime::now().duration_since(UNIX_EPOCH).expect("Time is broken...").as_millis() as u64
                        }
                    };

                    table.insert_kv(KeyValue::new(row.bytes_as_ref(), &family_name, &column, &timestamp, &CellType::Delete, &Bytes::from("")));

                    drop(w);
                },
                RowMutationOp::DeleteColumn { family, column, timestamp } => {
                    let family_name = family.clone();
                    let family = table.get_family(&family).unwrap();

                    let row_lock = table.get_row_lock(&row);
                    let w = row_lock.write_lock();

                    let timestamp = match timestamp {
                        Some(ts) => ts,
                        None => {
                            SystemTime::now().duration_since(UNIX_EPOCH).expect("Time is broken...").as_millis() as u64
                        }
                    };

                    table.insert_kv(KeyValue::new(row.bytes_as_ref(), &family_name, &column, &timestamp, &CellType::DeleteColumn, &Bytes::from("")));

                    drop(w);
                },
                RowMutationOp::DeleteFamily { family, timestamp } => {
                    let family_name = family.clone();
                    let family = table.get_family(&family).unwrap();

                    let row_lock = table.get_row_lock(&row);
                    let w = row_lock.write_lock();

                    let timestamp = match timestamp {
                        Some(ts) => ts,
                        None => {
                            SystemTime::now().duration_since(UNIX_EPOCH).expect("Time is broken...").as_millis() as u64
                        }
                    };

                    table.insert_kv(KeyValue::new(row.bytes_as_ref(), &family_name, &Bytes::from_static(b""), &timestamp, &CellType::DeleteFamily, &Bytes::from_static(b"")));

                    drop(w);
                },
            //     RowMutationOp::DeleteRow => {
            //         let row_lock = table.get_row_lock(&row);

            //         let w = row_lock.write_lock();

            //         let families = table.get_families_iter();

            //         let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).expect("Millenium bug strikes back?").as_millis() as u64;
            //         for family in families {
            //             family.insert_cell(Cell {
            //                 cell_type: CellType::DeleteFamily,
            //                 row: row.bytes_as_ref().clone(),
            //                 column_name: Bytes::from(""),
            //                 timestamp,
            //                 data: Bytes::from(""),
            //             });
            //         }

            //         drop(w);
            //     }
            }
        }
    }

    pub fn read_row(&mut self, table: Bytes, row: Bytes, filter: Option<&dyn RowFilter>) -> RowResult {
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

    pub fn scan(&mut self, table: Bytes, start: Option<KeyValue>, end: Option<KeyValue>, filter: Option<&dyn RowFilter>) -> Vec<KeyValue> {
        let table: RefMut<u64, Table, std::hash::RandomState> = self.get_table(table.clone()).unwrap();

        let iter = table.scan(start, end);
        iter.collect::<Vec<KeyValue>>()
    }
}