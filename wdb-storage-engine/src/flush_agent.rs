use std::sync::Arc;

use log::{debug, info};
use tokio::time::{sleep, Duration};

use crate::{PersistanceLayer, StorageEngine};

pub struct FlushAgent {

}

impl FlushAgent {
    pub fn new<T: PersistanceLayer + Send + Sync + 'static>(storage_engine: Arc<StorageEngine<T>>) {
        tokio::spawn(async move {
            loop {  
                debug!("Scanning start...");
                for table in storage_engine.get_tables_iter() {
                    let table_name = std::str::from_utf8(&table.get_name()).unwrap().to_string();
                    for family in table.get_families_iter() {
                        let family_name = std::str::from_utf8(&family.get_name()).unwrap().to_string();
                        let memtable_size = family.get_memtable_size();
                        debug!("Checking table {} family {}. Memtable size: {} bytes.", table_name, family_name, memtable_size);
                        if memtable_size >= 80 /*8 * 1024 * 1024*/ {
                            info!("Flushing memtable of table {} family {}. Memtable size: {} bytes.", table_name, family_name, memtable_size);
                            family.flush_memtable(&table.get_name(), storage_engine.get_persitance_layer());
                        }
                    }
                }
                debug!("Scanning end.");
                sleep(Duration::from_secs(5)).await;
            }
        });
    }
}