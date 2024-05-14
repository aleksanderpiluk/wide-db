use std::sync::{RwLock, RwLockWriteGuard};

use crate::utils::hashed_bytes::HashedBytes;

#[derive(Debug)]
pub struct RowLockContext {
    pub row: HashedBytes,
    pub lock: RwLock<bool>,
}

impl RowLockContext {
    pub fn write_lock(&self) -> RwLockWriteGuard<'_, bool> {
        self.lock.write().unwrap()
    }
}