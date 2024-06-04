use std::{cmp, collections::{HashMap, HashSet}};

use bytes::Bytes;

use crate::{cell::{Cell, CellType}, key_value::KeyValue, utils::Timestamp};

pub struct DeleteTracker {
    families_deleted: HashMap<Vec<u8>, Timestamp>,
    columns_deleted: HashMap<Vec<u8>, Timestamp>,
    cells_deleted: HashSet<Vec<u8>>,
}

impl DeleteTracker {
    pub fn new() -> DeleteTracker {
        DeleteTracker {
            cells_deleted: HashSet::new(),
            columns_deleted: HashMap::new(),
            families_deleted: HashMap::new(),
        }
    }

    pub fn add(&mut self, cell: &KeyValue) {
        match cell.get_cell_type() {
            CellType::Delete => {
                self.cells_deleted.insert(cell.get_key_without_cell_type().to_vec());
            },
            CellType::DeleteColumn => {
                self.columns_deleted.entry(cell.get_key_row_cf_col().to_vec())
                    .and_modify(|ts| { *ts = cmp::max(*ts, cell.get_timestamp()) })
                    .or_insert(cell.get_timestamp());
            },
            CellType::DeleteFamily => {
                self.families_deleted.entry(cell.get_cf().to_vec())
                    .and_modify(|ts| { *ts = cmp::max(*ts, cell.get_timestamp() )})
                    .or_insert(cell.get_timestamp());
            }
            _ => return,
        }
    }

    pub fn is_deleted(&self, cell: &KeyValue) -> bool {
        if let Some(ts) = self.families_deleted.get(cell.get_cf()) {
            if *ts >= cell.get_timestamp() {
                return true;
            }
        }

        if let Some(ts) = self.columns_deleted.get(cell.get_key_row_cf_col()) {
            if *ts >= cell.get_timestamp() {
                return true;
            }
        }

        if self.cells_deleted.contains(cell.get_key_without_cell_type()) {
            return true;
        }

        false
    }

    pub fn reset(&mut self) {
        self.columns_deleted.clear();
        self.families_deleted.clear();
        self.cells_deleted.clear();
    }
}