use std::iter::Iterator;

use crate::key_value::KeyValue;

pub trait KVScanner {
    fn scan(&self, start: Option<KeyValue>, end: Option<KeyValue>, read_point: Option<u64>) -> impl Iterator<Item = KeyValue>;
}

pub type ScanIterator<'a> = dyn Iterator<Item = KeyValue> + 'a;