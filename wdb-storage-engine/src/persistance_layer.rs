use std::io::Write;

use bytes::Bytes;

pub trait PersistanceLayer {
    fn get_sstable_write(&mut self, name: Bytes) -> &mut impl Write;
}