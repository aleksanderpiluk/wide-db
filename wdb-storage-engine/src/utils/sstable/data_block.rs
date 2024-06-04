use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct DataBlock {
    pub offset: usize,
    pub data_size: usize,
    pub key_len: u16,
    pub key: Bytes,
}

impl DataBlock {
    pub fn get_offset(&self) -> usize {
        self.offset
    }

    pub fn get_data_size(&self) -> usize {
        self.data_size
    }
}