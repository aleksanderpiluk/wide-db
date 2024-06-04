use bytes::Bytes;

use crate::utils::Timestamp;

#[derive(Debug, Clone)]
pub enum RowMutationOp {
    Put {
        family: Bytes,
        column: Bytes,
        timestamp: Option<Timestamp>,
        value: Bytes,
    },

    DeleteCell {
        family: Bytes,
        column: Bytes,
        timestamp: Option<Timestamp>,
    },

    // DeleteRow,
    DeleteColumn {
        family: Bytes,
        column: Bytes,
        timestamp: Option<Timestamp>,
    },
    DeleteFamily {
        family: Bytes,
        timestamp: Option<Timestamp>,
    }
}