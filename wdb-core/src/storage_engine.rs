use std::fmt::Debug;
use bytes::Bytes;

pub trait StorageEngine: Sync + Send + Debug {
    fn create_table(&self, name: &String, families: &Vec<String>) -> Result<(), &'static str>;
    fn add_table_family(&self, table_name: &String, family_name: &String) -> Result<(), &'static str>;
    fn list_tables(&self) -> Vec<String>;
    fn table_with_name_exists(&self, name: &String) -> bool;
    fn get_row(&self, table_name: &String, row: &String) -> Result<(), &'static str>;
}

pub struct DeleteParams {
    pub table: Bytes,
    pub row: Bytes,
    pub family: Option<Bytes>,
    pub column: Option<Bytes>,
    pub timestamp: Option<u64>
}