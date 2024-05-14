use std::io::{Read, Seek, SeekFrom};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct SSTableFooter {
    pub index_pos: u64,
    pub index_size: u64,
}

#[derive(Serialize, Deserialize)]
pub struct SSTableIndex {
    pub row: String,
    pub column_name: String,
    pub timestamp: u64,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone)]
pub struct SSTableRow {
    pub row: String,
    pub column_name: String,
    pub timestamp: u64,
    pub data: Bytes,
}

pub struct SSTable<'a, R: Read + Seek> {
    reader: &'a mut R,
    footer: SSTableFooter,
    indexes: Vec::<SSTableIndex>,
    curr: u64,
}

impl<R: Read + Seek> SSTable<'_, R> {
    pub fn new(r: &mut R) -> SSTable<R> {
        let footer = SSTable::read_footer(r);
        
        r.seek(SeekFrom::Start(footer.index_pos)).unwrap();
        
        let mut buf = vec![0u8; footer.index_size as usize];
        r.read_exact(&mut buf).unwrap();

        let indexes: Vec<SSTableIndex> = bincode::deserialize(&buf).expect("Failed to deserialize indexes");

        SSTable { reader: r, footer, indexes, curr: 0 }
    }

    // pub fn read_row(&mut self, index: &SSTableIndex) -> SSTableRow {
    //     let mut buf = BytesMut::with_capacity(index.length as usize);
    //     self.reader.seek(SeekFrom::Start(index.offset)).unwrap();
    //     self.reader.read_exact(&mut buf).expect("Failed to read file data");

    //     SSTableRow { 
    //         row: index.row.clone(),
    //         column_name: index.column_name.clone(),
    //         timestamp: index.timestamp, 
    //         data: buf.freeze()
    //     }
    // }

    fn read_footer(reader: &mut R) -> SSTableFooter {
        reader.seek(SeekFrom::End(-16)).unwrap();
        let mut buf = [0u8; 16];
        reader.read_exact(&mut buf).unwrap();
        
        let footer: SSTableFooter = bincode::deserialize(&buf).expect("Failed to deserialize footer");
        footer
    }
}

impl<R: Read + Seek> Iterator for SSTable<'_, R> {
    type Item = SSTableRow;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.curr as usize;
        if current >= self.indexes.len() {
            return None;
        }

        self.curr += 1;

        let index = self.indexes.get(current).unwrap();
        let mut buf = vec![0u8; index.length as usize];
        self.reader.seek(SeekFrom::Start(index.offset)).unwrap();
        self.reader.read_exact(&mut buf).expect("Failed to read file data");

        
        Some(SSTableRow { 
            row: index.row.clone(),
            column_name: index.column_name.clone(),
            timestamp: index.timestamp, 
            data: Bytes::from(buf)
        })
    }
}