use std::collections::HashSet;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct DbRootFile {
    pub tables: Vec<String>,
}

impl DbRootFile {
    pub fn empty() -> DbRootFile {
        DbRootFile { 
            tables: vec![], 
        }
    }

    pub fn from(tables: Vec<String>) -> DbRootFile {
        DbRootFile { 
            tables,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TableMetadata {
    pub id: String,
    pub name: String,
    pub families: HashSet<String>,
}

impl TableMetadata {
    pub fn new(id: String, name: String, families: &Vec<String>) -> TableMetadata {
        let mut families_set = HashSet::new();

        for family in families {
            families_set.insert(family.clone());
        }

        TableMetadata { 
            id, 
            name, 
            families: families_set,
        }
    }
}