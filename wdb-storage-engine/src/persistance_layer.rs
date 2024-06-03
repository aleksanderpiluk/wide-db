use std::io::{Read, Seek, Write};

use bytes::Bytes;

use crate::utils::sstable::SSTable;

pub trait PersistanceLayer: Send + Sync + 'static {
    fn get_segment_write(&self, table: &Bytes, family: Bytes, segment: &Bytes) -> impl Write;
    fn get_segment_read(&self, table: &Bytes, family: &Bytes, segment: &Bytes) -> impl Read + Seek;
    fn get_tables_list(&self) -> Vec<(Bytes, Vec<(Bytes, Vec<SSTable>)>)>;
}