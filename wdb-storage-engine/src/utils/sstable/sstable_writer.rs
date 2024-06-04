use std::{cell::RefCell, cmp::max, io::{Read, Write}, rc::Rc};

use bytes::{BufMut, Bytes, BytesMut};
use crossbeam_skiplist::SkipMap;

use crate::{cell::Cell, key_value::KeyValue};

use super::data_block::DataBlock;

pub struct SSTableWriter<'a, W:Write> {
    writer: &'a mut W,
    offset: usize,
    max_mvcc: u64,
    data_blocks: Vec<Rc<RefCell<DataBlock>>>,
    curr_data_block: Option<Rc<RefCell<DataBlock>>>,
}

impl<W: Write> SSTableWriter<'_, W> {
    pub fn new(w: &mut W) -> SSTableWriter<W> {
        SSTableWriter {
            writer: w,
            offset: 0,
            max_mvcc: 0,
            data_blocks: vec![],
            curr_data_block: None,
        }
    }

    pub fn write_kv(&mut self, kv: &KeyValue) {
        let key_len = kv.get_key_len();
        let mut key: Vec<u8> = Vec::with_capacity(key_len as usize);
        key.extend_from_slice(kv.get_key());
        self.create_data_block_if_necessary(key_len, &Bytes::from(key));

        self.max_mvcc = max(self.max_mvcc, kv.get_mvcc_id());

        let len = self.writer.write(&kv.as_bytes()).unwrap();
        self.offset += len;
        self.curr_data_block.as_ref().unwrap().borrow_mut().data_size += len;
    }

    pub fn end(&mut self) -> SkipMap<KeyValue, DataBlock> {
        let index: SkipMap<KeyValue, DataBlock> = SkipMap::new();

        let mut buf = BytesMut::new();
        for block in self.data_blocks.iter() {
            let block = block.borrow_mut();
            buf.put_u64(block.offset as u64);
            buf.put_u64(block.data_size as u64);
            buf.put_u16(block.key_len);
            buf.put(&block.key[..]);
            index.insert(KeyValue::new_from_key(block.key_len, block.key.clone()), block.clone());
        }
        
        let index_pos = self.offset;
        let len = self.writer.write(&buf.freeze()).unwrap();

        let mut buf = BytesMut::new();
        buf.put_u64(0xDB1234AB); // magic number for validation check
        buf.put_u64(index_pos as u64);
        buf.put_u64(len as u64);
        buf.put_u64(self.max_mvcc);

        self.writer.write(&buf.freeze()).unwrap();
        self.writer.flush().unwrap();

        index
    }

    pub fn get_max_mvcc_id(&self) -> u64 {
        self.max_mvcc
    }

    fn create_data_block_if_necessary(&mut self, key_len: u16, key: &Bytes) {
        if let Some(db) = &self.curr_data_block {
            if db.borrow_mut().data_size < (2 << 16) {
                return;
            }
        }

        let db = Rc::new(RefCell::new(DataBlock {
            offset: self.offset,
            data_size: 0,
            key_len,
            key: key.clone(),
        }));
        self.curr_data_block = Some(db.clone());
        self.data_blocks.push(db);
    }
}