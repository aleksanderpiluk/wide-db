mod storage_engine;
mod row_lock;
mod table;
mod row_mutation;
mod utils;

mod fs_controller;
mod flush_agent;
mod row_scanner;


pub use row_mutation::RowMutation;
pub use row_mutation::RowMutationOp;

pub use storage_engine::StorageEngine;

pub use table::Table;
pub use table::TableFamily;

// #[derive(Debug)]
// pub struct DefaultStorageEngine {
//     tables: Vec<Table>,
//     tables_name_map: DashMap<Bytes, usize>,
//     tables_lock: Mutex<()>,
//     // id_mapping: HashMap<String, Arc<TableMetadata>>,
//     // name_mapping: HashMap<String, Arc<TableMetadata>>,
// }

// impl DefaultStorageEngine {
//     pub fn empty() -> DefaultStorageEngine {
//         DefaultStorageEngine {
//             tables: Vec::new(),
//             tables_name_map: DashMap::new(),
//             tables_lock: Mutex::new(()),
//         }
//     }

//     pub fn init() -> DefaultStorageEngine {
//         todo!("fn to reimplement")

    
//         // if !FSController::structure_exists() {
//         //     FSController::create_initial_structure();

//         //     DefaultStorageEngine {
//         //         tables: RwLock::new(vec![]),
//         //         id_mapping: HashMap::new(),
//         //         name_mapping: HashMap::new(),
//         //     }
//         // } else {
//         //     let root_file = FSController::read_root_file();
//         //     println!("{:?}", root_file);
//         //     let metadata = FSController::read_tables_metadata(&root_file.tables).unwrap();   

//         //     let mut tables: Vec<Arc<TableMetadata>> = vec![];
//         //     let mut id_mapping = HashMap::<String, Arc<TableMetadata>>::with_capacity(metadata.len());
//         //     let mut name_mapping = HashMap::<String, Arc<TableMetadata>>::with_capacity(metadata.len());

//         //     for item in metadata {
//         //         let arc = Arc::new(item);
//         //         id_mapping.insert(arc.id.clone(), arc.clone());
//         //         name_mapping.insert(arc.name.clone(), arc.clone());
//         //         tables.push(arc)
//         //     }

//         //     DefaultStorageEngine {
//         //         tables: RwLock::new(tables),
//         //         id_mapping: id_mapping,
//         //         name_mapping: name_mapping,
//         //     }
//         // }
//     }

//     pub fn create_table(&mut self, name: Bytes) -> Result<(), &'static str> {
//         let _lock = self.tables_lock.lock().unwrap();

//         match self.get_table_id_for_name(&name) {
//             Some(_) => return Err("Table with this name already exists."),
//             None => {}
//         }

//         let id = self.tables.len();
//         let table = Table::new(id, name.clone());

//         self.tables.push(table);
//         self.tables_name_map.insert(name, id);

//         Ok(())
//     }

//     pub fn get_table_id_for_name(&self, name: &Bytes) -> Option<usize> {
//         match self.tables_name_map.get(name) {
//             Some(s) => Some(*s),
//             None => None
//         }
//     }

//     pub fn get_table(&mut self, id: usize) -> &mut Table {
//         self.tables.get_mut(id).expect("Table not found")
//     }

// }
