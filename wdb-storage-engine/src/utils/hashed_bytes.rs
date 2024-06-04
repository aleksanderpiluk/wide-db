use std::hash::{DefaultHasher, Hash, Hasher};

use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct HashedBytes {
    bytes: Bytes,
    hash: u64,
}

impl HashedBytes {
    pub fn from_bytes(bytes: Bytes) -> HashedBytes {
        let mut hasher = DefaultHasher::new();
        bytes.hash(&mut hasher);
        let hash = hasher.finish();

        HashedBytes {
            bytes,
            hash,
        }
    }

    pub fn bytes_as_ref(&self) -> &Bytes {
        return &self.bytes;
    }

    pub fn hash_as_ref(&self) -> &u64 {
        return &self.hash;
    }
}