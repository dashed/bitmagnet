//! Compiles the workspace's `.proto` files into Rust with tonic + prost.
//!
//! The protos live at the workspace root (`bitmagnet-rs/proto/`), so they are
//! reached relative to this crate's manifest directory. `search.proto` imports
//! `bitmagnet/common.proto`, hence the `proto/` directory is the include root.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    // crates/bitmagnet-proto -> bitmagnet-rs -> proto
    let proto_root = manifest_dir.join("..").join("..").join("proto");
    let common = proto_root.join("bitmagnet").join("common.proto");
    let search = proto_root.join("bitmagnet").join("search.proto");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[common, search], std::slice::from_ref(&proto_root))?;

    // Rebuild when any proto changes.
    println!("cargo:rerun-if-changed={}", proto_root.display());
    Ok(())
}
