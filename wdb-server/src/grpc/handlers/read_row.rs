use bytes::Bytes;
use tonic::{Request, Response, Status};
use wdb_grpc::wdb_grpc::{Cell, ReadRowRequest, ReadRowResponse};
use wdb_storage_engine::Cell as CellTrait;

use crate::server_ctx::ServerCtx;

pub async fn read_row(ctx: &ServerCtx, request: Request<ReadRowRequest>) -> Result<Response<ReadRowResponse>, Status> {
    let request = request.into_inner();

    let result = ctx.storage_engine.read_row(
        Bytes::from(request.table_name), 
        Bytes::from(request.row_key),
        None
    );
    
    Ok(Response::new(ReadRowResponse { 
        cells: result.cells.iter().map(|cell| {
            let ts: u64 = cell.get_timestamp().into();

            Cell {
                row_key: std::str::from_utf8(&cell.get_row()).unwrap().to_string(),
                family: std::str::from_utf8(&cell.get_cf()).unwrap().to_string(),
                column: std::str::from_utf8(&cell.get_col()).unwrap().to_string(),
                timestamp: ts as i64,
                value: cell.get_value().to_vec(),
            }
        }).collect()
    }))
}