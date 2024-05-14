mod sstable;
mod sstable_compactor;
mod sstable_writer;

pub use sstable_writer::SSTableWriter;
pub use sstable_compactor::SSTableCompactor;

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Seek};

    use bytes::Bytes;

    use super::{sstable::{SSTable, SSTableRow}, SSTableWriter};

    #[test]
    fn sstable_writer_writes_and_reader_reads() {
        let test_rows = [
            SSTableRow {
                row: String::from("testRow"),
                column_name: String::from("fancyName"),
                timestamp: 1234,
                data: Bytes::from("Some interesting data!"),
            },
            SSTableRow {
                row: String::from("testRow"),
                column_name: String::from("other_fancy_name"),
                timestamp: 1234,
                data: Bytes::from("More interesting data!"),
            },
            SSTableRow {
                row: String::from("row_name"),
                column_name: String::from("fancyName"),
                timestamp: 555,
                data: Bytes::from("Even more data"),
            }
        ];

        let mut c = Cursor::new(Vec::new());
        let mut writer = SSTableWriter::new(&mut c);
        writer.write_row(test_rows[0].clone());
        writer.write_row(test_rows[1].clone());
        writer.write_row(test_rows[2].clone());
        writer.end();
        
        println!("{:?}", c);
        let mut sstable = SSTable::new(&mut c);
        println!("{:?}", sstable.next().unwrap());
        println!("{:?}", sstable.next().unwrap());
        println!("{:?}", sstable.next().unwrap());
    }
}