use bytes::Bytes;

use crate::cell::CellType;

/// KV buffer layout
/// timestamp: u64  [0 .. 8]
/// key_type: u8    [8 .. 9]
/// row_end: u16    [9 .. 11]
/// cf_end: u16     [11 .. 13]
/// col_end: u16    [13 .. 15]
/// val_end: u64    [15 .. 23]
/// row             [23 .. row_end]
/// cf              [row_end .. cf_end]
/// col             [cf_end .. col_end]
/// val             [col_end .. val_end]
#[derive(PartialEq, Eq, Clone)]
pub struct KV {
    buf: Bytes,
    mvcc: u64,
}

impl KV {
    fn timestamp(&self) -> u64 {
        u64::from_be_bytes(
            self.buf[KVLayout::TIMESTAMP.start .. KVLayout::TIMESTAMP.end]
            .try_into().expect("timestamp must be 8 bytes")
        )
    }

    fn key_type(&self) -> u8 {
        u8::from_be_bytes(
            self.buf[KVLayout::KEY_TYPE.start .. KVLayout::KEY_TYPE.end]
            .try_into().expect("key_type must be 1 byte")
        )
    }

    fn row_end(&self) -> u16 {
        u16::from_be_bytes(
            self.buf[KVLayout::ROW_END.start .. KVLayout::ROW_END.end]
            .try_into().expect("row_end must be 2 bytes")
        )
    }

    fn cf_end(&self) -> u16 {
        u16::from_be_bytes(
            self.buf[KVLayout::CF_END.start .. KVLayout::CF_END.end]
            .try_into().expect("cf_end must be 2 bytes")
        )
    }

    fn col_end(&self) -> u16 {
        u16::from_be_bytes(
            self.buf[KVLayout::COL_END.start .. KVLayout::COL_END.end]
            .try_into().expect("col_end must be 2 bytes")
        )
    }

    fn val_end(&self) -> u64 {
        u64::from_be_bytes(
            self.buf[KVLayout::VAL_END.start .. KVLayout::VAL_END.end]
            .try_into().expect("val_end must be 8 bytes")
        )
    }

    fn row(&self) -> &[u8] {        
        let end = self.row_end() as usize;
        &self.buf[KVLayout::VAL_END.end .. end]
    }

    fn cf(&self) -> &[u8] {
        let start = self.row_end() as usize;
        let end = self.cf_end() as usize;

        &self.buf[start .. end]
    }

    fn col(&self) -> &[u8] {
        let start = self.cf_end() as usize;
        let end = self.col_end() as usize;

        &self.buf[start .. end]
    }

    fn val(&self) -> &[u8] {
        let start = self.col_end() as usize;
        let end = self.val_end() as usize;

        &self.buf[start .. end]
    }
}

impl Ord for KV {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.row().cmp(other.row()) {
            std::cmp::Ordering::Equal => {},
            ord => return ord,
        }

        if self.col_end() - self.row_end() == 0 && self.key_type() == CellType::Minimum as u8 {
            return std::cmp::Ordering::Greater;
        }

        if other.col_end() - other.row_end() == 0 && other.key_type() == CellType::Minimum as u8 {
            return std::cmp::Ordering::Less;
        }

        match self.cf().cmp(other.cf()) {
            std::cmp::Ordering::Equal => {},
            ord => return ord,
        }

        match self.col().cmp(other.col()) {
            std::cmp::Ordering::Equal => {},
            ord => return ord,
        }

        match other.timestamp().cmp(&self.timestamp()) {
            std::cmp::Ordering::Equal => {},
            ord => return ord,
        }

        match other.key_type().cmp(&self.key_type()) {
            std::cmp::Ordering::Equal => {},
            ord => return ord,
        }

        other.mvcc.cmp(&self.mvcc)
    }
}

impl PartialOrd for KV {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct KVLayout {start: usize, end: usize, len: usize}

impl KVLayout {
    const TIMESTAMP: Self = Self{start: 0, end: 8, len: 8};
    const KEY_TYPE: Self = Self{start: 8, end: 9, len: 1};
    const ROW_END: Self = Self{start: 9, end: 11, len: 2};
    const CF_END: Self = Self{start: 11, end: 13, len: 2};
    const COL_END: Self = Self{start: 13, end: 15, len: 2};
    const VAL_END: Self = Self{start: 15, end: 23, len: 8};
}