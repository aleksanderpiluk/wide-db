use std::{ops::RangeBounds, sync::{atomic::{AtomicU64, AtomicUsize, Ordering}, Arc}, vec::IntoIter};

use arc_swap::ArcSwap;
use crossbeam_skiplist::{set::{Entry, Range}, SkipSet};

use crate::{key_value::KeyValue, kv_scanner::KVScanner, Cell};

type Segment = SkipSet<KeyValue>;

#[derive(Debug)]
pub struct Memtable {
    active: ArcSwap<Segment>,
    activeSize: AtomicU64,
    snapshot: ArcSwap<Segment>
}

impl Memtable {
    pub fn new() -> Memtable {
        Memtable { 
            active: ArcSwap::from(Arc::new(Segment::new())),
            activeSize: AtomicU64::new(0),
            snapshot: ArcSwap::default(),
        }
    }

    pub fn insert(&self, cell: KeyValue) {
        let size = cell.get_size();        
        self.active.load().insert(cell);
        self.activeSize.fetch_add(size, Ordering::Relaxed);
    } 

    // TODO: This operation may override old snapshot. Add correct handling.
    pub fn snapshot(&self) {
        let to_flush = self.active.load_full();
        let new_active = Arc::new(Segment::new());
        self.snapshot.store(to_flush);
        self.active.store(new_active);
    }

    pub fn get_active_size(&self) -> u64 {
        self.activeSize.load(Ordering::Relaxed)
    }

    pub fn scan<'a>(&self, start: Option<KeyValue>, end: Option<KeyValue>, read_point: Option<u64>) -> impl Iterator<Item = KeyValue> + '_ {
        let segment = self.active.load_full();

        match (start, end) {
            (Some(start), None) => {
                MemtableIterator::new(segment, start.., read_point)
            },
            (None, None) => {
                MemtableIterator::new(segment, .., read_point)
            }
            (None, Some(end)) => {
                MemtableIterator::new(segment, ..=end, read_point)
            }
            (Some(start), Some(end)) => {
                MemtableIterator::new(segment, start..=end, read_point)
            }
        }
    }
}

struct MemtableIterator {
    iter: IntoIter<KeyValue>,
    last_key: Option<Vec<u8>>,
    read_point: Option<u64>
}

impl MemtableIterator {
    fn new<R: RangeBounds<KeyValue>>(segment: Arc<Segment>, range: R, read_point: Option<u64>) -> MemtableIterator {
        let iter = segment.range(range).map(|entry| { entry.value().clone() }).collect::<Vec<KeyValue>>();
        MemtableIterator {
            iter: iter.into_iter(),
            last_key: None,
            read_point,
        }
    }
}

impl<'a> Iterator for MemtableIterator {
    type Item = KeyValue;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(kv) = self.iter.next() {
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