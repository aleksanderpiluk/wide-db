use std::fmt::Debug;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{cell::{Cell, CellType}, utils::Timestamp};

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
#[derive(PartialEq, Eq, Clone)]
pub struct KeyValue {
    mvcc_id: u64,
    buffer: Bytes
}

impl KeyValue {
    pub fn new_from_row(row: &Bytes) -> KeyValue {
        KeyValue::new_from_row_and_value(row, &Bytes::from(""))
    }

    pub fn new_first_on_row(row: &Bytes) -> KeyValue {
        let b_empty = Bytes::from("");
        KeyValue::new(row, &b_empty, &b_empty, Timestamp::MAX, &CellType::Maximum, &b_empty)
    }

    pub fn new_last_on_row(row: &Bytes) -> KeyValue {
        let b_empty = Bytes::from("");
        KeyValue::new(row, &b_empty, &b_empty, Timestamp::MIN, &CellType::Minimum, &b_empty)
    }

    pub fn new_from_row_and_value(row: &Bytes, value: &Bytes) -> KeyValue {
        KeyValue::new(row, &Bytes::from(""), &Bytes::from(""), Timestamp::MAX, &CellType::Maximum, value)
    }

    pub fn new_from_row_and_timestamp(row: &Bytes, timestamp: Timestamp) -> KeyValue {
        let b_empty = Bytes::from("");
        KeyValue::new(row, &b_empty, &b_empty, timestamp, &CellType::Maximum, &b_empty)
    }

    pub fn new_from_row_and_type(row: &Bytes, cell_type: &CellType) -> KeyValue {
        let b_empty = Bytes::from("");
        KeyValue::new(row, &b_empty, &b_empty, Timestamp::MIN, cell_type, &b_empty)
    }

    pub fn new(row: &Bytes, cf: &Bytes, col: &Bytes, timestamp: Timestamp, key_type: &CellType, value: &Bytes) -> KeyValue {
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
        buffer.put_u64(timestamp.into());
        buffer.put_u8(*key_type as u8);

        buffer.put(value.clone());
        
        KeyValue { buffer: buffer.freeze(), mvcc_id: 0 }
    }

    pub fn as_bytes(&self) -> Bytes {
        self.buffer.clone()
    }

    pub fn set_mvcc_id(&mut self, mvcc_id: u64) {
        self.mvcc_id = mvcc_id;
    }

    pub fn get_mvcc_id(&self) -> u64 {
        self.mvcc_id
    }
}

impl Cell for KeyValue {
    fn get_size(&self) -> u64 {
        let size: u64 = 2 + 8 + self.get_key_len() as u64 + self.get_value_len();
        size
    }

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

    fn get_timestamp(&self) -> Timestamp {
        let pos = 10 + self.get_key_len() as usize - 8 - 1;
        let mut buf = &self.buffer[pos..];
        Timestamp::from(buf.get_u64())
    }

    fn get_cell_type(&self) -> CellType {
        let pos = 10 + self.get_key_len() as usize - 1;
        let mut buf = &self.buffer[pos..];
        CellType::try_from(buf.get_u8()).unwrap()
    }

    fn get_key(&self) -> &[u8] {
        let len = self.get_key_len() as usize;
        &self.buffer[10..(10+len)]
    }

    fn get_key_without_cell_type(&self) -> &[u8] {
        let len = self.get_key_len() as usize - 1;
        &self.buffer[10..(10+len)]
    }

    fn get_key_row_cf_col(&self) -> &[u8] {
        let len = self.get_key_len() as usize - 9;
        &self.buffer[10..(10+len)]
    }
    
    fn get_value(&self) -> &[u8] {
        let pos = 10 + self.get_key_len() as usize;
        let len = self.get_value_len() as usize;
        &self.buffer[pos..(pos+len)]
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

        // if self.get_cf_len() != other.get_cf_len() {
            // return self.get_cf().cmp(other.get_cf());
        // }
        match self.get_cf().cmp(other.get_cf()) {
            std::cmp::Ordering::Equal => {},
            ord => return ord,
        }

        match self.get_col().cmp(other.get_col()) {
            std::cmp::Ordering::Equal => {},
            ord => return ord,
        }

        match other.get_timestamp().cmp(&self.get_timestamp()) {
            std::cmp::Ordering::Equal => {},
            ord => return ord,
        }

        match (other.get_cell_type() as u8).cmp(&(self.get_cell_type() as u8)) {
            std::cmp::Ordering::Equal => {},
            ord => return ord,
        }

        other.mvcc_id.cmp(&self.mvcc_id)
    }
}

impl PartialOrd for KeyValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Debug for KeyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyValue")
            .field("row", &String::from_utf8(self.get_row().to_vec()).unwrap())
            .field("cf", &String::from_utf8(self.get_cf().to_vec()).unwrap())
            .field("col", &String::from_utf8(self.get_col().to_vec()).unwrap())
            .field("ts", &self.get_timestamp())
            .field("type", &self.get_cell_type())
            .field("value", &String::from_utf8(self.get_value().to_vec()).unwrap())
            .finish()
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
        let timestamp = Timestamp::new(333);
        let value = Bytes::from("some unique data...");
        

        let kv = KeyValue::new(&row, &cf, &col, timestamp, &CellType::Maximum, &value);        

        let row_len = row.len();
        let cf_len = cf.len();
        let col_len = col.len();
        let key_len = 2 + row_len + 2 + cf_len + col_len + 8 + 1;
        let val_len = value.len() as u64;

        let mut key = BytesMut::with_capacity(key_len);
        key.put_u16(row_len as u16);
        key.put(&row[..]);
        key.put_u16(cf_len as u16);
        key.put(&cf[..]);
        key.put(&col[..]);
        key.put_u64(timestamp.into());
        key.put_u8(CellType::Maximum as u8);

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
        assert_eq!(*kv.get_key(), key[..]);
        assert_eq!(*kv.get_value(), value[..]);
    }

    #[test]
    fn order_test() {
        let b_empty = Bytes::from_static(b"");

        let delete_family = KeyValue::new(&b_empty, &b_empty, &b_empty, Timestamp::MIN, &CellType::DeleteFamily, &b_empty);
        let delete_column = KeyValue::new(&b_empty, &b_empty, &b_empty, Timestamp::MIN, &CellType::DeleteColumn, &b_empty);
        let delete = KeyValue::new(&b_empty, &b_empty, &b_empty, Timestamp::MIN, &CellType::Delete, &b_empty);
        let put = KeyValue::new(&b_empty, &b_empty, &b_empty, Timestamp::MIN, &CellType::Put, &b_empty);
        let first = KeyValue::new_first_on_row(&b_empty);
        let last = KeyValue::new_last_on_row(&b_empty);

        let mut v = vec![last.clone(), put.clone(), delete.clone(), delete_column.clone(), delete_family.clone(), first.clone()];
        v.sort();
        // println!("{:?}", v);

        assert!(v == [first, delete_family, delete_column, delete, put, last]);
    }
}