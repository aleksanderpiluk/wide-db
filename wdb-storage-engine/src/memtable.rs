use crossbeam_skiplist::{set::Entry, SkipSet};

use crate::{key_value::KeyValue, kv_scanner::KVScanner, Cell};

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

    pub fn scan<'a>(&self, start: Option<KeyValue>, end: Option<KeyValue>, read_point: Option<u64>) -> Box<dyn Iterator<Item = KeyValue> + '_> {
        let segment = self.active.as_ref().unwrap();
        
        fn run_scan<'a, T: Iterator<Item = Entry<'a, KeyValue>>>(iter: T, read_point: Option<u64>) -> MemtableIterator<'a, T> {
            MemtableIterator::new(iter, read_point)
        }

        match (start, end) {
            (Some(start), None) => {
                Box::new(run_scan(segment.range(start..).into_iter(), read_point))
            },
            (None, None) => {
                Box::new(run_scan(segment.range(..).into_iter(), read_point))
            }
            (None, Some(end)) => {
                Box::new(run_scan(segment.range(..=end).into_iter(), read_point))
            }
            (Some(start), Some(end)) => {
                Box::new(run_scan(segment.range(start..=end).into_iter(), read_point))
            }
        }
    }
}

struct MemtableIterator<'a, T: Iterator<Item = Entry<'a, KeyValue>>> {
    iter: T,
    last_key: Option<Vec<u8>>,
    read_point: Option<u64>
}

impl<'a, T: Iterator<Item = Entry<'a, KeyValue>>> MemtableIterator<'a, T> {
    fn new(iter: T, read_point: Option<u64>) -> MemtableIterator<'a, T> {
        MemtableIterator {
            iter,
            last_key: None,
            read_point,
        }
    }
}

impl<'a, T: Iterator<Item = Entry<'a, KeyValue>>> Iterator for MemtableIterator<'a, T> {
    type Item = KeyValue;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(kv) = self.iter.next() {
            let kv = kv.value();
            // Check if MVCC enabled
            if let Some(point) = self.read_point {
                if kv.get_mvcc_id() > point {
                    continue;
                }
            }

            let key_vec = kv.get_key().to_vec();
            if let Some(last_key) = &self.last_key {
                if last_key.eq(&key_vec) {
                    continue;
                }
            }

            self.last_key = Some(key_vec);
            return Some(kv.clone())
        }

        None
    }
}

// impl KVScanner for Memtable {
    
// }