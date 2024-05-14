use std::io::Write;

use super::sstable::{SSTableFooter, SSTableIndex, SSTableRow};

pub struct SSTableWriter<'a, W:Write> {
    indexes: Vec<SSTableIndex>,
    writer: &'a mut W,
    offset: u64,
}

impl<W: Write> SSTableWriter<'_, W> {
    pub fn new(w: &mut W) -> SSTableWriter<W> {
        SSTableWriter {
            indexes: vec![],
            offset: 0,
            writer: w,
        }
    }

    pub fn write_row(&mut self, row: SSTableRow) {
        let data = row.data;
        
        let len = self.writer.write(&data).expect("Failed to write data to SSTable.") as u64; 

        self.indexes.push(SSTableIndex {
            column_name: row.column_name,
            row: row.row,
            timestamp: row.timestamp,
            length: len,
            offset: self.offset,
        });

        self.offset += len;
    }

    pub fn end(&mut self) {
        let data = bincode::serialize(&self.indexes).expect("Failed to serialize SSTable indexes.");
        let index_pos: u64 = self.offset;
        let len = self.writer.write(&data).expect("Failed to write indexes data.") as u64;

        let footer = SSTableFooter {
            index_pos: self.offset,
            index_size: len,
        };
        let data = bincode::serialize(&footer).expect("Failed to serialize SSTable footer.");
        self.writer.write(&data).expect("Failed to write footer data.");
        self.writer.flush();
    }
}