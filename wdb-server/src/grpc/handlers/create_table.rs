use std::collections::HashSet;

use bytes::Bytes;
use tonic::{Request, Response, Status};
use wdb_grpc::wdb_grpc::{CreateTableRequest, Table};

use crate::server_ctx::ServerCtx;

pub async fn create_table(ctx: &ServerCtx, request: Request<CreateTableRequest>) -> Result<Response<Table>, Status> {
    let request = request.into_inner();
    
    if request.table_name.len() <= 0 || request.table_name.len() > 64 {
        return Err(Status::invalid_argument(
            "Invalid table name. Table name cannot be an empty string or bigger than 64 bytes."
        ));
    }
    let table_name = Bytes::from(request.table_name);
    
    let mut families_set = HashSet::<String>::new();
    for name in request.families {
        if name.len() > 64 {
            return Err(Status::invalid_argument(
                "Invalid family name. Table name cannot be an empty string or bigger than 64 bytes."
            ));
        }
        families_set.insert(name);
    }
    
    ctx.storage_engine.create_table(table_name.clone()).unwrap();
    let mut table = ctx.storage_engine.get_table(table_name).unwrap();

    for family_name in families_set {
        table.create_family(Bytes::from(family_name)).unwrap();
    }

    Ok(Response::new(Table { 
        name: std::str::from_utf8(&table.get_name()).unwrap().to_string(),
        column_families: table.get_families_iter().map(|family| {
            std::str::from_utf8(&family.get_name()).unwrap().to_string()
        }).collect()
    }))
}