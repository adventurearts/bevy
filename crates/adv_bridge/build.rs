//! Compiles `proto/adventure.proto` into Rust code in `OUT_DIR`.
//!
//! `protoc` is not required: the descriptor set is produced in pure Rust by
//! `protox` and handed to `tonic-prost-build`.

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let proto = "proto/adventure.proto";
    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed=build.rs");

    let file_descriptors = protox::compile([proto], ["proto"])?;
    tonic_prost_build::configure().compile_fds(file_descriptors)?;
    Ok(())
}
