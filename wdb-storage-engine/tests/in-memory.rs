mod utils;

use std::collections::HashMap;

use bytes::Bytes;
use utils::MemoryPersistance;
use wdb_storage_engine::{PersistanceLayer, StorageEngine};

#[test]
fn in_memory_test() {
    let mut persistance = MemoryPersistance::new();

    let tables = HashMap::<Bytes, ()>::new();
    
    let mut storage_engine = StorageEngine::empty();
    storage_engine.create_table(table_name.clone()).unwrap();

    let mut num_ops = 20;
}