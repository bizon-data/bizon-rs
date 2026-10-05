fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    let proto_root = concat!(env!("CARGO_MANIFEST_DIR"), "/../../proto");
    let wkt = protoc_bin_vendored::include_path()?;
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .bytes(["."])
        .compile_protos(
            &[format!("{proto_root}/google/cloud/bigquery/storage/v1/storage.proto")],
            &[proto_root.to_string(), wkt.display().to_string()],
        )?;
    println!("cargo:rerun-if-changed={proto_root}");
    Ok(())
}
