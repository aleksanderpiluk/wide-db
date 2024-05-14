use std::u8;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::cell::{Cell, CellType};

/*
Keyvalue buffer structure:
- key_length: u16
- value_length: u64
- key
- value

Key structure:
- row_length: u16
- row
- cf_length: u16
- cf
- col
- timestamp: u64
- key_type: u8
 */
#[derive(Debug, PartialEq, Eq)]
pub struct KeyValue {
    buffer: Bytes
}

impl KeyValue {
    pub fn new_from_row(row: &Bytes) -> KeyValue {
        KeyValue::new_from_row_and_value(row, &Bytes::from(""))
    }

    pub fn new_from_row_and_value(row: &Bytes, value: &Bytes) -> KeyValue {
        KeyValue::new(row, &Bytes::from(""), &Bytes::from(""), &u64::MAX, &CellType::Maximum, value)
    }

    pub fn new_from_row_and_timestamp(row: &Bytes, timestamp: &u64) -> KeyValue {
        let b_empty = Bytes::from("");
        KeyValue::new(row, &b_empty, &b_empty, timestamp, &CellType::Maximum, &b_empty)
    }

    pub fn new(row: &Bytes, cf: &Bytes, col: &Bytes, timestamp: &u64, key_type: &CellType, value: &Bytes) -> KeyValue {
        let row_len = row.len();
        let cf_len = cf.len();
        let key_len = 2 + row_len + 2 + cf_len + col.len() + 8 + 1;
        let value_len = value.len();
        let capacity = key_len + value_len;

        let mut buffer = BytesMut::with_capacity(capacity);
        
        buffer.put_u16(key_len as u16);
        buffer.put_u64(value_len as u64);

        buffer.put_u16(row_len as u16);
        buffer.put(row.clone());
        buffer.put_u16(cf_len as u16);
        buffer.put(cf.clone());
        buffer.put(col.clone());
        buffer.put_u64(*timestamp);
        buffer.put_u8(*key_type as u8);

        buffer.put(value.clone());
        
        KeyValue { buffer: buffer.freeze() }
    }
}

impl Cell for KeyValue {
    fn get_key_len(&self) -> u16 {
        let mut buf = &self.buffer[0..];
        buf.get_u16()
    }

    fn get_value_len(&self) -> u64 {
        let mut buf = &self.buffer[2..];
        buf.get_u64()
    }

    fn get_row_len(&self) -> u16 {
        let mut buf = &self.buffer[10..];
        buf.get_u16()
    }

    fn get_row(&self) -> &[u8] {
        let len = self.get_row_len() as usize;
        &self.buffer[12..(12+len)]
    }

    fn get_cf_len(&self) -> u16 {
        let len = self.get_row_len() as usize;
        let mut buf = &self.buffer[12+len..];
        buf.get_u16()
    }

    fn get_cf(&self) -> &[u8] {
        let pos = 12 + 2 + self.get_row_len() as usize;
        let len = self.get_cf_len() as usize;
        &self.buffer[pos..(pos+len)]
    }

    fn get_col_len(&self) -> u16 {
        let prev_len = 2 + 2 + self.get_row_len() as usize + self.get_cf_len() as usize;
        let len = self.get_key_len() as usize - 1 - 8 - prev_len;
        len as u16
    }
    
    fn get_col(&self) -> &[u8] {
        let pos = 10 + 2 + 2 + self.get_row_len() as usize + self.get_cf_len() as usize;
        let len = self.get_col_len() as usize;
        &self.buffer[pos..(pos+len)]
    }

    fn get_timestamp(&self) -> u64 {
        let pos = 10 + self.get_key_len() as usize - 8 - 1;
        let mut buf = &self.buffer[pos..];
        buf.get_u64()
    }

    fn get_cell_type(&self) -> CellType {
        let pos = 10 + self.get_key_len() as usize - 1;
        let mut buf = &self.buffer[pos..];
        CellType::try_from(buf.get_u8()).unwrap()
    }
}

impl Ord for KeyValue {    
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.get_row().cmp(other.get_row()) {
            std::cmp::Ordering::Equal => {},
            ord => return ord,
        }

        if self.get_cf_len() + self.get_col_len() == 0 && self.get_cell_type() == CellType::Minimum {
            return std::cmp::Ordering::Greater;
        }

        if other.get_cf_len() + other.get_col_len() == 0 && other.get_cell_type() == CellType::Minimum {
            return std::cmp::Ordering::Less;
        }

        if self.get_cf_len() != other.get_cf_len() {
            return self.get_cf().cmp(other.get_cf());
        }

        match self.get_col().cmp(other.get_col()) {
            std::cmp::Ordering::Equal => {},
            ord => return ord,
        }

        match self.get_timestamp().cmp(&other.get_timestamp()) {
            std::cmp::Ordering::Equal => {},
            ord => return ord,
        }

        return (other.get_cell_type() as u8).cmp(&(self.get_cell_type() as u8))
    }
}

impl PartialOrd for KeyValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_key_value() {
        let row = Bytes::from("some_row");
        let cf = Bytes::from("cf");
        let col = Bytes::from("123");
        let timestamp = 333;
        let value = Bytes::from("some unique data...");
        

        let kv = KeyValue::new(&row, &cf, &col, &timestamp, &CellType::Maximum, &value);
        

        let row_len = row.len();
        let cf_len = cf.len();
        let col_len = col.len();
        let key_len = 2 + row_len + 2 + cf_len + col_len + 8 + 1;
        let val_len = value.len() as u64;

        assert_eq!(kv.get_key_len(), key_len as u16);
        assert_eq!(kv.get_value_len(), val_len as u64);
        assert_eq!(kv.get_row_len(), row_len as u16);
        assert_eq!(*kv.get_row(), row[..]);
        assert_eq!(kv.get_cf_len(), cf_len as u16);
        assert_eq!(*kv.get_cf(), cf[..]);
        assert_eq!(kv.get_col_len(), col_len as u16);
        assert_eq!(*kv.get_col(), col[..]);
        assert_eq!(kv.get_timestamp(), timestamp);
        assert_eq!(kv.get_cell_type(), CellType::Maximum);
    }
}