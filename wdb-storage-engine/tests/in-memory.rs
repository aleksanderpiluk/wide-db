use std::collections::HashMap;

use bytes::Bytes;
use wdb_storage_engine::StorageEngine;

#[test]
fn in_memory_test() {
    let tables = HashMap::<Bytes, ()>::new();
    
    let mut storage_engine = StorageEngine::empty();
    storage_engine.create_table(table_name.clone()).unwrap();

    let mut num_ops = 20;
}