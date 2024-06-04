mod utils;

use bytes::Bytes;
use wdb_storage_engine::{RowMutation, RowMutationOp, StorageEngine, Timestamp};

use crate::utils::MemoryPersistance;

#[test]
fn in_memory_test() {
    let table_name = Bytes::from("users");
    let timestamp = Some(Timestamp::from(1500000000));

    let storage_engine = StorageEngine::empty(MemoryPersistance::new(), false);

    storage_engine.create_table(table_name.clone()).unwrap();
    let mut table = storage_engine.get_table(table_name.clone()).unwrap();
    table.create_family(Bytes::from("")).unwrap();
    table.create_family(Bytes::from("account")).unwrap();
    table.create_family(Bytes::from("address")).unwrap();
    drop(table);
    
    storage_engine.execute_row_mutation(RowMutation {
        table: table_name.clone(),
        row: Bytes::from("user1"),
        ops: vec![
            RowMutationOp::Put { family: Bytes::from(""), column: Bytes::from("name"), timestamp: timestamp, value: Bytes::from("John") },
            RowMutationOp::Put { family: Bytes::from(""), column: Bytes::from("surname"), timestamp: timestamp, value: Bytes::from("Doe") },
            RowMutationOp::Put { family: Bytes::from(""), column: Bytes::from("email"), timestamp: timestamp, value: Bytes::from("john@example.com") },
            RowMutationOp::Put { family: Bytes::from("account"), column: Bytes::from("saldo"), timestamp: timestamp, value: Bytes::from("1234") },
            RowMutationOp::Put { family: Bytes::from("account"), column: Bytes::from("tokens"), timestamp: timestamp, value: Bytes::from("5") },
            RowMutationOp::Put { family: Bytes::from("address"), column: Bytes::from("country"), timestamp: timestamp, value: Bytes::from("USA") },
            RowMutationOp::Put { family: Bytes::from("address"), column: Bytes::from("city"), timestamp: timestamp, value: Bytes::from("New York") },
            RowMutationOp::Put { family: Bytes::from("address"), column: Bytes::from("street"), timestamp: timestamp, value: Bytes::from("Wall Street") },
        ]
    });

    storage_engine.execute_row_mutation(RowMutation {
        table: table_name.clone(),
        row: Bytes::from("user2"),
        ops: vec![
            RowMutationOp::Put { family: Bytes::from(""), column: Bytes::from("name"), timestamp: timestamp, value: Bytes::from("Jan") },
            RowMutationOp::Put { family: Bytes::from(""), column: Bytes::from("surname"), timestamp: timestamp, value: Bytes::from("Kowalski") },
            RowMutationOp::Put { family: Bytes::from(""), column: Bytes::from("email"), timestamp: timestamp, value: Bytes::from("jan@example.com") },
            RowMutationOp::Put { family: Bytes::from("account"), column: Bytes::from("saldo"), timestamp: timestamp, value: Bytes::from("250") },
            RowMutationOp::Put { family: Bytes::from("account"), column: Bytes::from("tokens"), timestamp: timestamp, value: Bytes::from("10") },
            RowMutationOp::Put { family: Bytes::from("address"), column: Bytes::from("country"), timestamp: timestamp, value: Bytes::from("Poland") },
            RowMutationOp::Put { family: Bytes::from("address"), column: Bytes::from("city"), timestamp: timestamp, value: Bytes::from("Warsaw") },
            RowMutationOp::Put { family: Bytes::from("address"), column: Bytes::from("street"), timestamp: timestamp, value: Bytes::from("Marszalkowska") },
        ]
    });

    storage_engine.execute_row_mutation(RowMutation {
        table: table_name.clone(),
        row: Bytes::from("user3"),
        ops: vec![
            RowMutationOp::Put { family: Bytes::from(""), column: Bytes::from("name"), timestamp: timestamp, value: Bytes::from("Jane") },
            RowMutationOp::Put { family: Bytes::from(""), column: Bytes::from("surname"), timestamp: timestamp, value: Bytes::from("Smith") },
            RowMutationOp::Put { family: Bytes::from(""), column: Bytes::from("email"), timestamp: timestamp, value: Bytes::from("jane@example.com") },
            RowMutationOp::Put { family: Bytes::from("account"), column: Bytes::from("saldo"), timestamp: timestamp, value: Bytes::from("999") },
            RowMutationOp::Put { family: Bytes::from("account"), column: Bytes::from("tokens"), timestamp: timestamp, value: Bytes::from("20") },
            RowMutationOp::Put { family: Bytes::from("address"), column: Bytes::from("country"), timestamp: timestamp, value: Bytes::from("UK") },
            RowMutationOp::Put { family: Bytes::from("address"), column: Bytes::from("city"), timestamp: timestamp, value: Bytes::from("London") },
            RowMutationOp::Put { family: Bytes::from("address"), column: Bytes::from("street"), timestamp: timestamp, value: Bytes::from("Abbey Road") },
        ]
    });
    
    let row_result = storage_engine.read_row(table_name.clone(), Bytes::from("user2"), None);
    println!("{:#?}", row_result);


    storage_engine.execute_row_mutation(RowMutation {
        table: table_name.clone(),
        row: Bytes::from("user1"),
        ops: vec![
            RowMutationOp::DeleteCell { family: Bytes::from(""), column: Bytes::from("email"), timestamp: timestamp},
        ]
    });

    let row_result = storage_engine.read_row(table_name.clone(), Bytes::from("user1"), None);
    println!("{:#?}", row_result);

    storage_engine.execute_row_mutation(RowMutation {
        table: table_name.clone(),
        row: Bytes::from("user3"),
        ops: vec![
            RowMutationOp::Put { family: Bytes::from("account"), column: Bytes::from("tokens"), timestamp: Some(Timestamp::from(1500000000 + 10)), value: Bytes::from("22") },
            RowMutationOp::Put { family: Bytes::from("account"), column: Bytes::from("tokens"), timestamp: Some(Timestamp::from(1500000000 + 20)), value: Bytes::from("18") },
            RowMutationOp::Put { family: Bytes::from("account"), column: Bytes::from("tokens"), timestamp: Some(Timestamp::from(1500000000 + 30)), value: Bytes::from("10") },
        ]
    });

    let row_result = storage_engine.read_row(table_name.clone(), Bytes::from("user3"), None);
    println!("{:?}", row_result);

    storage_engine.execute_row_mutation(RowMutation {
        table: table_name.clone(),
        row: Bytes::from("user3"),
        ops: vec![
            RowMutationOp::DeleteColumn { family: Bytes::from("account"), column: Bytes::from("tokens"), timestamp: Some(Timestamp::from(1500000000 + 30)) }
        ]
    });

    let row_result = storage_engine.read_row(table_name.clone(), Bytes::from("user3"), None);
    println!("{:?}", row_result);

    storage_engine.execute_row_mutation(RowMutation {
        table: table_name.clone(),
        row: Bytes::from("user2"),
        ops: vec![
            RowMutationOp::DeleteFamily { family: Bytes::from("address"), timestamp  }
        ]
    });

    let row_result = storage_engine.read_row(table_name.clone(), Bytes::from("user2"), None);
    println!("{:?}", row_result);

    println!("-- FULL SCAN --");
    let result = storage_engine.scan(table_name.clone(), None, None, None);
    println!("{:?}", result);
}