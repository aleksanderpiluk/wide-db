use std::{cmp::Ordering, ops::Bound};

use bytes::Bytes;
use crossbeam_skiplist::SkipSet;

use super::cell::Cell;

type Segment = SkipSet<Cell>;

#[derive(Debug)]
pub struct Memtable {
    active: Option<Box<Segment>>,
    snapshot: Option<Box<Segment>>
}

impl Memtable {
    pub fn new() -> Memtable {
        Memtable { active: Some(Box::new(Segment::new())), snapshot: Some(Box::new(Segment::new())) }
    }

    pub fn insert(&self, cell: Cell) {
        self.active.as_ref().unwrap().insert(cell);
    } 

    // TODO: THIS IS NOT THREAD-SAFE OPERATION
    pub fn snapshot(&mut self) {
        self.snapshot = self.active.take();
        self.active = Some(Box::new(Segment::new()));
    }

    pub fn read_row(&self, row: &Bytes) -> Option<()> {
        let row_cell = Cell {
            row: row.clone(),
            cell_type: None,
            column_name: None,
            data: None,
            timestamp: None,
        };

        let segment = self.active.unwrap();
        
        let iter = match segment.lower_bound(Bound::Excluded(&row_cell)) {
            None => return  None,
            Some(iter) => iter,
        };
        
        let cell: Cell;
        while ((cell = iter.next()) && (cell.cmp(&row_cell) == Ordering::Greater)) {
            
        }

        return None
    }
}