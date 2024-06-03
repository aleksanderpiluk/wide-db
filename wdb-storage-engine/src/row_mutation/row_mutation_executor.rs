use bytes::Bytes;
use dashmap::mapref::one::{Ref, RefMut};
use log::debug;

use crate::{cell::CellType, key_value::KeyValue, utils::hashed_bytes::HashedBytes, PersistanceLayer, RowMutationOp, Table, TableFamily, Timestamp};

pub struct RowMutationExecutor {}

impl RowMutationExecutor {
    pub fn unsafe_execute_row_mutation(table: RefMut<u64, Table>, row: HashedBytes, ops: Vec<RowMutationOp>) {
        // Stage I - mutation preprocessing
        debug!("RowMutationExecutor - Stage I begin");
        let mut parsed: Vec<RowMutationOpParsed> = Vec::new();

        for op in ops {
            let parsed_op = match &op {
                RowMutationOp::DeleteCell { family, column, timestamp } => {
                    let family = table.get_family(&family).unwrap();
                    RowMutationOpParsed(family, op)
                },
                RowMutationOp::DeleteColumn { family, column, timestamp } => {
                    let family = table.get_family(&family).unwrap();
                    RowMutationOpParsed(family, op)
                },
                RowMutationOp::DeleteFamily { family, timestamp } => {
                    let family = table.get_family(&family).unwrap();
                    RowMutationOpParsed(family, op)
                },
                RowMutationOp::Put { family, column, timestamp, value } => {
                    let family = table.get_family(&family).unwrap();
                    RowMutationOpParsed(family, op)
                }
            };
            parsed.push(parsed_op);
        }
        debug!("RowMutationExecutor - Stage I end");

        
        // Stage II - ensure row write lock and get MVCC write number
        debug!("RowMutationExecutor - Stage II begin");
        let row_lock = table.get_row_lock(&row);
        let w = row_lock.write_lock();
        let write_entry = table.mvcc_new_write();
        let mvcc_id = write_entry.get_write_num();
        debug!("Got MVCC write number {}", mvcc_id);
        debug!("RowMutationExecutor - Stage II end");


        // Stage III - parsed operations execution
        debug!("RowMutationExecutor - Stage III begin");
        for op in parsed {
            let family = op.0;
            let op = op.1;

            match op {
                RowMutationOp::Put { column, timestamp, value, ..} => {
                    RowMutationExecutor::unsafe_put(&table, &family, row.clone(), column, timestamp, value, mvcc_id);
                },
                RowMutationOp::DeleteCell { column, timestamp, .. } => {
                    RowMutationExecutor::unsafe_delete_cell(&table, &family, row.clone(), column, timestamp, mvcc_id);
                },
                RowMutationOp::DeleteColumn { column, timestamp, .. } => {
                    RowMutationExecutor::unsafe_delete_column(&table, &family, row.clone(), column, timestamp, mvcc_id);
                },
                RowMutationOp::DeleteFamily { timestamp, .. } => {
                    RowMutationExecutor::unsafe_delete_family(&table, &family, row.clone(), timestamp, mvcc_id);
                },
            }
        }
        table.mvcc_complete(write_entry);
        debug!("RowMutationExecutor - Stage III end");
    }

    fn unsafe_put(table: &Table, family: &TableFamily, row: HashedBytes, column: Bytes, ts: Option<Timestamp>, value: Bytes, mvcc_id: u64) {
        let ts = Timestamp::ensure_timestamp(ts);
        let mut cell = KeyValue::new(row.bytes_as_ref(), &family.get_name(), &column, ts, &CellType::Put, &value);
        cell.set_mvcc_id(mvcc_id);
        family.insert_kv(cell);
    }

    fn unsafe_delete_cell(table: &Table, family: &TableFamily, row: HashedBytes, column: Bytes, ts: Option<Timestamp>, mvcc_id: u64) {
        let ts = Timestamp::ensure_timestamp(ts);
        let mut cell = KeyValue::new(row.bytes_as_ref(), &family.get_name(), &column, ts, &CellType::Delete, &Bytes::from(""));
        cell.set_mvcc_id(mvcc_id);
        family.insert_kv(cell);
    }

    fn unsafe_delete_column(table: &Table, family: &TableFamily, row: HashedBytes, column: Bytes, ts: Option<Timestamp>, mvcc_id: u64) {
        let ts = Timestamp::ensure_timestamp(ts);
        let mut cell = KeyValue::new(row.bytes_as_ref(), &family.get_name(), &column, ts, &CellType::DeleteColumn, &Bytes::from(""));
        cell.set_mvcc_id(mvcc_id);
        family.insert_kv(cell);
    } 

    fn unsafe_delete_family(table: &Table, family: &TableFamily, row: HashedBytes, ts: Option<Timestamp>, mvcc_id: u64) {
        let ts = Timestamp::ensure_timestamp(ts);   
        let mut cell = KeyValue::new(row.bytes_as_ref(), &family.get_name(), &Bytes::from_static(b""), ts, &CellType::DeleteFamily, &Bytes::from_static(b""));
        cell.set_mvcc_id(mvcc_id);
        family.insert_kv(cell);
    }
}

struct RowMutationOpParsed<'a>(Ref<'a, u64, TableFamily, std::hash::RandomState>, RowMutationOp);