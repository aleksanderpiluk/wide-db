use std::fmt::Debug;

pub trait StorageEngine: Sync + Send + Debug {
    fn create_table(&self, name: &String, families: &Vec<String>) -> Result<(), &'static str>;
    fn add_table_family(&self, table_name: &String, family_name: &String) -> Result<(), &'static str>;
    fn list_tables(&self) -> Vec<String>;
    fn table_with_name_exists(&self, name: &String) -> bool;
}