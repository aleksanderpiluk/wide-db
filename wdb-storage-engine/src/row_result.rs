use bytes::Bytes;

use crate::key_value::KeyValue;

#[derive(Debug, Clone)]
pub struct RowResult {
    pub row: Bytes,
    pub cells: Vec<KeyValue>,
}