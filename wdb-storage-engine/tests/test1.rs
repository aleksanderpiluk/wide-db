use bytes::Bytes;
use rand::seq::SliceRandom;
use uuid::Uuid;
use wdb_storage_engine::*;

const names: &[&str] = &["John", "James", "William", "Bill", "Paul", "Daniel", "Steven", "Donald", "Albert", "Mary", "Maria", "Susan", "Liz", "Sarah", "Monica", "Ann", "Rose", "Judy", "Emma"];
static surnames: &[&str] = &["Doe", "Simpson", "Tucker", "Palmer", "Gates", "Ballmer", "Armstrong", "Cohen", "Miles", "Rodgers", "Reese", "Potter", "Logan", "Norton", "Stepherd", "Meyers", "Berg", "Tanner"];

#[test]
fn test_row_mutations() {
    let mut num_ops = 10_000;
    let table_name = Bytes::from("users");
    
    let mut rng = rand::thread_rng();

    let mut storage_engine = StorageEngine::empty();
    storage_engine.create_table(table_name.clone()).unwrap();
    
    let mut table = storage_engine.get_table(table_name.clone()).unwrap();
    table.create_family(Bytes::from("")).unwrap();
    drop(table);
    
    while num_ops > 0 {
        let row = Uuid::now_v7().to_string();

        let name = *names.choose(&mut rng).unwrap();
        let surname = *surnames.choose(&mut rng).unwrap();

        println!("Inserting: {} {} -> #{}", name, surname, row);

        storage_engine.execute_row_mutation(RowMutation { 
            table: table_name.clone(), 
            row: Bytes::from(row), 
            ops: Vec::from([
                RowMutationOp::SetCell { family: Bytes::from(""), column: Bytes::from("name"), timestamp: None, value: Bytes::from(name) },
                RowMutationOp::SetCell { family: Bytes::from(""), column: Bytes::from("surname"), timestamp: None, value: Bytes::from(surname) },
            ])
        });

        num_ops -= 1;
    }
    // let row = Bytes::from("abcd");
}