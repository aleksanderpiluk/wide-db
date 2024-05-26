use std::sync::Arc;

use log::{debug, info};
use tokio::time::{sleep, Duration};

use crate::{storage_engine, StorageEngine};

pub struct FlushAgent {

}

impl FlushAgent {
    pub fn new(storage_engine: Arc<StorageEngine>) {
        tokio::spawn(async move {
            loop {  
                debug!("Scanning start...");
                for table in storage_engine.get_tables_iter() {
                    let table_name = std::str::from_utf8(&table.get_name()).unwrap().to_string();
                    let memtable_size = table.get_memtable_size();
                    debug!("Checking table {}. Memtable size: {} bytes.", table_name, memtable_size);
                    if memtable_size >= 8 * 1024 * 1024 {
                        info!("Flushing memtable of table {}. Memtable size: {} bytes.", table_name, memtable_size);
                        table.flush_memtable();
                    }
                }
                debug!("Scanning end.");
                sleep(Duration::from_secs(5)).await;
            }
        });
    }
}