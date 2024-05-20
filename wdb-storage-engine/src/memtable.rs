use bytes::Bytes;
use crossbeam_skiplist::{set::{Entry, Range}, SkipSet};
use std::{cmp, ops::{Bound, RangeBounds, RangeFrom}};

use crate::{key_value::KeyValue, kv_scanner::KVScanner};

type Segment = SkipSet<KeyValue>;

#[derive(Debug)]
pub struct Memtable {
    active: Option<Box<Segment>>,
    snapshot: Option<Box<Segment>>
}

impl Memtable {
    pub fn new() -> Memtable {
        Memtable { active: Some(Box::new(Segment::new())), snapshot: Some(Box::new(Segment::new())) }
    }

    pub fn insert(&self, cell: KeyValue) {
        self.active.as_ref().unwrap().insert(cell);
    } 

    // TODO: THIS IS NOT THREAD-SAFE OPERATION
    pub fn snapshot(&mut self) {
        self.snapshot = self.active.take();
        self.active = Some(Box::new(Segment::new()));
    }
}

impl KVScanner for Memtable {
    fn scan(&self, start: Option<KeyValue>, end: Option<KeyValue>) -> impl Iterator<Item = KeyValue> {
        let segment = self.active.as_ref().unwrap();

        match (start, end) {
            (Some(start), None) => {
                let v: Vec<KeyValue> = segment.range(start..).map(|x| { x.value().clone() }).collect();
                v.into_iter()
            },
            (None, None) => {
                let v: Vec<KeyValue> = segment.range(..).map(|x| { x.value().clone() }).collect();
                v.into_iter()
            }
            (None, Some(end)) => {
                let v: Vec<KeyValue> = segment.range(..=end).map(|x| { x.value().clone() }).collect();
                v.into_iter()
            }
            (Some(start), Some(end)) => {
                let v: Vec<KeyValue> = segment.range(start..=end).map(|x| { x.value().clone() }).collect();
                v.into_iter()
            }
        }
    }
}