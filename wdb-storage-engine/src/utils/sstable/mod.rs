mod sstable;
mod sstable_writer;
mod sstable_reader;
mod data_block;

pub use sstable_writer::SSTableWriter;
pub use sstable::SSTable;
pub use sstable_reader::SSTableReader;

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Seek};

    use bytes::{BufMut, Bytes, BytesMut};

    use crate::{cell::Cell, key_value::KeyValue};

    use super::{sstable::{SSTable, SSTableRow}, SSTableWriter};

    #[test]
    fn sstable_writer_writes_and_reader_reads() {
        let mut c = Cursor::new(Vec::new());
        let mut writer = SSTableWriter::new(&mut c);

        let r1: Vec<u8> = vec![0xF0, 0xF1, 0xF2];
        let r2: Vec<u8> = vec![0xE0, 0xE1, 0xE2];
        let r3: Vec<u8> = vec![0xD0, 0xD1, 0xD2];

        let v1: Vec<u8> = vec![0x11, 0x22, 0x33];
        let v2: Vec<u8> = vec![0x44, 0x55, 0x66];
        let v3: Vec<u8> = vec![0x77, 0x88, 0x99];

        let kv1 = KeyValue::new_from_row_and_value(&Bytes::from(r1), &Bytes::from(v1));
        let kv2 = KeyValue::new_from_row_and_value(&Bytes::from(r2), &Bytes::from(v2));
        let kv3 = KeyValue::new_from_row_and_value(&Bytes::from(r3), &Bytes::from(v3));

        writer.write_kv(
            &kv1
        );
        writer.write_kv(
            &kv2
        );
        writer.write_kv(
            &kv3
        );
        writer.end();
        
        let inner = c.into_inner();

        let mut buf = BytesMut::new();
        buf.put(kv1.as_bytes());
        buf.put(kv2.as_bytes());
        buf.put(kv3.as_bytes());

        let data_size = kv1.as_bytes().len() as u64 +
            kv2.as_bytes().len() as u64 +
            kv3.as_bytes().len() as u64;

        buf.put_u64(0);
        buf.put_u64(data_size);
        buf.put_u16(kv1.get_key_len());
        buf.put(kv1.get_key());

        buf.put_u64(0xDB1234AB);
        buf.put_u64(data_size);
        buf.put_u64(8 + 8 + 2 + kv1.get_key_len() as u64);

        println!("{:?}", inner);
        assert_eq!(inner, Vec::<u8>::try_from(buf.freeze()).unwrap());
    }
}