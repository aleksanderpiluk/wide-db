use bytes::Bytes;

use crate::{key_value::KeyValue, memtable::Memtable, utils::sstable::SSTableWriter, PersistanceLayer};

#[derive(Debug)]
pub struct TableFamily {
    id: u64,
    name: Bytes,
    sstables: Vec<()>,
    memtable: Memtable,
}

impl TableFamily {
    pub fn new(id: u64, name: Bytes) -> TableFamily {
        TableFamily { id, name, sstables: Vec::new(), memtable: Memtable::new(), }
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

        let mut write = persistance.get_segment_write(table_name, self.get_name().clone(), segment.get_id());
        let mut sstable_writer = SSTableWriter::new(&mut write);

        segment.iter().for_each(|kv| {
            sstable_writer.write_kv(kv.value())
        });

        sstable_writer.end();
    }

    pub fn scan(&self, start: Option<KeyValue>, end: Option<KeyValue>, read_point: Option<u64>) -> impl Iterator<Item = KeyValue> + '_ {
        let iter = self.memtable.scan(start.clone(), end.clone(), read_point);

        iter
    }
    
}