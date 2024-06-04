use bytes::Bytes;

use crate::RowMutationOp;

pub struct RowMutation {
    pub table: Bytes,
    pub row: Bytes,
    pub ops: Vec<RowMutationOp>
}