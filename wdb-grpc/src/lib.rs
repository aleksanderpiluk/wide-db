pub mod wdb_grpc {
    tonic::include_proto!("widedb");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("widedb_descriptor");
}