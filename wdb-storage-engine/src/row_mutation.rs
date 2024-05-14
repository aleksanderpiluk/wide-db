use bytes::Bytes;

pub struct RowMutation {
    pub table: Bytes,
    pub row: Bytes,
    pub ops: Vec<RowMutationOp>
}

pub enum RowMutationOp {
    Put {
        family: Bytes,
        column: Bytes,
        timestamp: Option<u64>,
        value: Bytes,
    },

    DeleteCell {
        family: Bytes,
        column: Bytes,
        timestamp: Option<u64>,
    },

    // DeleteRow,
    // DeleteColumn {
    //     family: Bytes,
    //     column: Bytes,
    //     timestamp: Option<u64>,
    // },
    // DeleteFamily {
    //     family: Bytes,
    // }
}