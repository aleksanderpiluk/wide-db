use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
    .file_descriptor_set_path(out_dir.join("widedb_descriptor.bin"))
    .compile(&[
        "protos/types.proto",
        "protos/create-table.proto", 
        "protos/list-tables.proto",
        "protos/mutate-row.proto",
        "protos/read-row.proto",
        "protos/widedb.proto"
    ], &["protos/"])
    .unwrap();

    // tonic_build::compile_protos("protos/types.proto")?;
    // tonic_build::compile_protos("protos/create-table.proto")?;
    // tonic_build::compile_protos("protos/list-tables.proto")?;
    // tonic_build::compile_protos("protos/mutate-row.proto")?;
    // tonic_build::compile_protos("protos/read-row.proto")?;
    // tonic_build::compile_protos("protos/widedb.proto")?;

    Ok(())
}