use std::cmp::Ordering;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, PartialOrd, Ord, Clone)]
pub enum Type {
    Put = 1,
    Delete = 2,
}

pub struct Key {
    pub row: Bytes,
    pub column: Option<Bytes>,
    pub timestamp: Option<Bytes>,
    pub key_type: Option<Type>,
}

type KeyValue = (Key, Bytes);

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Cell {
    pub cell_type: Cell,
    pub row: Bytes,
    pub column_name: Option<Bytes>,
    pub timestamp: Option<u64>,
    pub data: Option<Bytes>,
}

impl Cell {
    fn Put() -> Cell {
        Cell {
            cell_type: Some(CellType::Put),
            timestamp: Some(timestamp),
        }
    }
}

impl Ord for Cell {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.row.cmp(&other.row) {
            Ordering::Equal => {},
            ord => return ord,
        };

        match self.column_name.cmp(&other.column_name) {
            Ordering::Equal => {},
            ord => return ord,
        }

        match other.timestamp.cmp(&self.timestamp) {
            Ordering::Equal => {}
            ord => return ord,
        }

        match self.cell_type.cmp(&other.cell_type)  {
            Ordering::Equal => {},
            ord => return ord,
        }

        Ordering::Equal
    }
}

impl PartialOrd for Cell {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

trait CellGet {
    fn get(&self) -> Cell;
}

trait CellPut {
    fn put(&self, cell: Cell) -> Result<(), &'static str>;
}

#[cfg(test)]
mod tests {
    use super::*;

    let cell = Cell {

    }
}