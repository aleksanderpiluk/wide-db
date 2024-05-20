use std::iter::Iterator;

use crate::key_value::KeyValue;

pub trait KVScanner {
    fn scan(&self, start: Option<KeyValue>, end: Option<KeyValue>) -> impl Iterator<Item = KeyValue>;
}