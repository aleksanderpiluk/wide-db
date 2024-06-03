use std::sync::Arc;

use arc_swap::{access::Access, ArcSwap};
use bytes::Bytes;
use itertools::{kmerge, Itertools};

use crate::{key_value::KeyValue, kv_scanner::ScanIterator, memtable::Memtable, utils::sstable::{SSTable, SSTableReader, SSTableWriter}, PersistanceLayer};

pub struct TableFamily {
    id: u64,
    name: Bytes,
    sstables: ArcSwap<Vec<SSTable>>,
    memtable: Memtable,
}

impl TableFamily {
    pub fn new(id: u64, name: Bytes) -> TableFamily {
        TableFamily { id, name, sstables: ArcSwap::default(), memtable: Memtable::new(), }
    }

    pub fn new_from_segments_vec(id: u64, name: Bytes, segments: Vec<SSTable>) -> TableFamily {
        TableFamily { id, name, sstables: ArcSwap::new(Arc::new(segments)), memtable: Memtable::new(), }   
    }

    pub fn get_name(&self) -> Bytes {
        self.name.clone()
    }

    pub fn insert_kv(&self, cell: KeyValue) {
        self.memtable.insert(cell);
    }

    pub fn get_memtable_size(&self) -> u64 {
        self.memtable.get_active_size()
    }

    pub fn flush_memtable<P: PersistanceLayer>(&self, table_name: &Bytes, persistance: &P) {
        let segment = self.memtable.snapshot();

        let segment_name = segment.get_id();
        let mut write = persistance.get_segment_write(table_name, self.get_name().clone(), segment_name);
        let mut sstable_writer = SSTableWriter::new(&mut write);

        segment.iter().for_each(|kv| {
            sstable_writer.write_kv(kv.value())
        });

        let index = sstable_writer.end();

        let mut sstables: Vec<SSTable> = self.sstables.load_full().iter().map(|ss_table| {
            ss_table.clone()
        }).collect_vec();
        sstables.push(SSTable::new(table_name, &self.get_name(), segment_name, index));
        self.sstables.swap(Arc::new(sstables));
    }

    pub fn scan<P: PersistanceLayer>(&self, persistance: &P , start: Option<KeyValue>, end: Option<KeyValue>, read_point: Option<u64>) -> impl Iterator<Item = KeyValue> + '_ {
        let mut iters = vec![];
        iters.push(self.memtable.scan(start.clone(), end.clone(), read_point).into_iter());

        let sstables = self.sstables.load();
        for sstable in sstables.iter() {
            let blocks = sstable.get_blocks(start.clone(), end.clone());
            
            let mut reader = SSTableReader::new(persistance.get_segment_read(sstable.get_table(), sstable.get_family(), sstable.get_segment()));
            iters.push(reader.read_blocks(blocks).into_iter());
        }

        kmerge(iters).map(|kv| {kv.clone()})
    }
    
}