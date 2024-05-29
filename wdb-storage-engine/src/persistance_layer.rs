use std::io::Write;

use bytes::Bytes;

pub trait PersistanceLayer: Send + Sync + 'static {
    fn get_sstable_write(&mut self, name: Bytes) -> &mut impl Write;
    fn get_segment_write(&self, table: &Bytes, family: Bytes, segment: &Bytes) -> impl Write;
}