use bytes::Bytes;
use tonic::{Request, Response, Status};
use wdb_grpc::wdb_grpc::MutateRowRequest;
use wdb_storage_engine::{RowMutation, RowMutationOp, Timestamp};

use crate::server_ctx::ServerCtx;

pub async fn row_mutate(ctx: &ServerCtx, request: Request<MutateRowRequest>) -> Result<Response<()>, Status> {
    let request = request.into_inner();

    let get_timestamp = |val: i64| -> Result<Option<Timestamp>, Status> {
        if val == -1 {
            Ok(None)
        } else if val >= 0 {
            Ok(Some(Timestamp::new(val as u64)))
        } else {
            Err(Status::invalid_argument("Invalid timestamp value. Allowed values >= -1."))
        }
    };
    
    let ops = request.mutations.iter().map(|mutation| {
        mutation.mutation.clone()
    }).flatten().map(|mutation| -> Result<RowMutationOp, Status> {
        match mutation {
            wdb_grpc::wdb_grpc::mutation::Mutation::PutCell(put_cell) => {
                Ok(RowMutationOp::Put { 
                    family: Bytes::from(put_cell.family_name), 
                    column: Bytes::from(put_cell.column_name), 
                    timestamp: get_timestamp(put_cell.timestamp)?, 
                    value: Bytes::from(put_cell.value), 
                })
            },
            wdb_grpc::wdb_grpc::mutation::Mutation::DeleteCell(delete_cell) => {
                Ok(RowMutationOp::DeleteCell { 
                    family: Bytes::from(delete_cell.family_name), 
                    column: Bytes::from(delete_cell.column_name), 
                    timestamp: get_timestamp(delete_cell.timestamp)?,
                })
            },
            wdb_grpc::wdb_grpc::mutation::Mutation::DeleteColumn(delete_col) => {
                Ok(RowMutationOp::DeleteColumn { 
                    family: Bytes::from(delete_col.family_name), 
                    column: Bytes::from(delete_col.column_name), 
                    timestamp: get_timestamp(delete_col.timestamp)?,
                })
            },
            wdb_grpc::wdb_grpc::mutation::Mutation::DeleteFamily(delete_family) => {
                Ok(RowMutationOp::DeleteFamily {
                    family: Bytes::from(delete_family.family_name), 
                    timestamp: get_timestamp(delete_family.timestamp)?,
                })
            }
        }
    }).collect::<Result<Vec<RowMutationOp>, Status>>()?;

    ctx.storage_engine.execute_row_mutation(RowMutation {
        table: Bytes::from(request.table_name), 
        row: Bytes::from(request.row), 
        ops: ops, 
    });

    Ok(Response::new(()))
}