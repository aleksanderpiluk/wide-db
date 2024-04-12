use std::{collections::HashMap, sync::RwLock};

use crate::utils::memtable::Memtable;

struct TableFamily {
    name: String,
    active_memtable: Memtable,
    flush_memtable: Option<Memtable>,
}

struct Table {
    id: u32,
    name: String,
    families: RwLock<HashMap<String, TableFamily>>,
}

