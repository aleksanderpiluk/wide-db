use std::{io::{Read, Seek, SeekFrom}, iter::Skip};

use bytes::{Buf, Bytes};
use crossbeam_skiplist::SkipMap;
use itertools::Itertools;

use crate::key_value::KeyValue;

use super::data_block::DataBlock;

pub struct SSTableReader<R: Read + Seek> {
    r: R,
    index_pos: u64,
    index_len: u64,
}


impl<R> SSTableReader<R>
where
    R: Read + Seek
    {

    pub fn new(mut r: R) -> SSTableReader<R> {
        r.seek(SeekFrom::End(-3 * 8)).unwrap();
        let mut buf = [0u8; 3 * 8];
        r.read_exact(&mut buf).unwrap();
        let mut buf = Bytes::from(buf.to_vec());
        if buf.get_u64() != 0xDB1234AB {
            panic!("Invalid magic number. Not an SSTable file.");
        }
        let index_pos = buf.get_u64();
        let index_len = buf.get_u64();

        SSTableReader { r, index_pos, index_len }
    }

    pub fn read_index(&mut self) -> SkipMap<KeyValue, DataBlock>{
        let result: SkipMap<KeyValue, DataBlock> = SkipMap::new();

        self.r.seek(SeekFrom::Start(self.index_pos)).unwrap();
        let mut buf = vec![0u8; self.index_len as usize];
        self.r.read_exact(&mut buf).unwrap();
        
        let mut buf = Bytes::from(buf);

        while buf.has_remaining() {
            let offset = buf.get_u64() as usize;
            let data_size = buf.get_u64() as usize;

            let key_len = buf.get_u16();
            
            let key = buf.get(..key_len as usize).unwrap().to_vec();
            buf.advance(key_len as usize);
            let key = Bytes::from(key);

            result.insert(KeyValue::new_from_key(key_len, key.clone()), DataBlock { offset, data_size, key_len, key });
        }

        result
    }

    pub fn read_blocks(&mut self, blocks: Vec<DataBlock>) -> Vec<KeyValue> {
        blocks.iter().map(|block| {
            self.r.seek(SeekFrom::Start(block.get_offset() as u64));
            let mut buf = vec![0u8; block.get_data_size()];
            self.r.read_exact(&mut buf).unwrap();

            let mut buf = Bytes::from(buf);
            let mut results = vec![];
            while buf.has_remaining() {
                let key_len = buf.get_u16();
                let val_len = buf.get_u64();
                let key = buf.get(..key_len as usize).unwrap().to_vec();
                buf.advance(key_len as usize);
                let val = buf.get(..val_len as usize).unwrap().to_vec();
                buf.advance(val_len as usize);
                let kv = KeyValue::new_from_kv_bytes(key_len, key, val_len, val);
                results.push(kv);
            }

            results
        }).flatten().collect_vec()
    }

} 