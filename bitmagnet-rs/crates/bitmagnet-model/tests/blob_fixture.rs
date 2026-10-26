//! Cross-language verification of the torrent-file blob format.
//!
//! The `.blob` files in `tests/fixtures/` were produced by the REAL Go
//! serializer (`internal/blobmigration.SerializeFiles`) via a throwaway
//! generator that called it with the inputs reconstructed below. These tests
//! assert that:
//!
//! 1. the Rust deserializer reads each Go-produced blob into the exact expected
//!    files (Go → Rust wire compatibility — the path Phase 3 backfill relies on
//!    when reading existing DB blobs), and
//! 2. the inner MessagePack that the Rust *serializer* emits is byte-for-byte
//!    identical to Go's (Rust → Go wire compatibility), since both encode the
//!    struct as a map keyed `i`/`p`/`e`/`s` with minimal-width integers.
//!
//! Together these prove the Rust and Go encoders/decoders agree on the wire
//! format. (The outer ZSTD bytes are NOT compared: libzstd and klauspost
//! produce different — but mutually decodable — frames at the same level.)

use bitmagnet_model::{deserialize_files, serialize_files, BlobFile};

fn f(index: u32, path: &str, extension: &str, size: u64) -> BlobFile {
    BlobFile {
        index,
        path: path.to_owned(),
        extension: extension.to_owned(),
        size,
    }
}

/// Inputs MUST match those in the Go fixture generator.
fn basic() -> Vec<BlobFile> {
    vec![
        f(0, "Season 1/Episode 1.mkv", "mkv", 1_500_000_000),
        f(1, "Season 1/Episode 2.mkv", "mkv", 1_600_000_123),
        f(2, "Season 1/subs/ep1.srt", "srt", 40_000),
    ]
}

fn edge() -> Vec<BlobFile> {
    vec![
        f(0, "RÉADME", "", 0),
        f(1_234_567, "音楽/曲.flac", "flac", 9_999_999_999),
    ]
}

fn single() -> Vec<BlobFile> {
    vec![f(0, "ubuntu-24.04.iso", "iso", 6_203_484_160)]
}

fn empty() -> Vec<BlobFile> {
    Vec::new()
}

const BASIC_BLOB: &[u8] = include_bytes!("fixtures/basic.blob");
const EDGE_BLOB: &[u8] = include_bytes!("fixtures/edge.blob");
const SINGLE_BLOB: &[u8] = include_bytes!("fixtures/single.blob");
const EMPTY_BLOB: &[u8] = include_bytes!("fixtures/empty.blob");

#[test]
fn rust_deserializes_real_go_blobs() {
    assert_eq!(deserialize_files(BASIC_BLOB).unwrap(), basic());
    assert_eq!(deserialize_files(EDGE_BLOB).unwrap(), edge());
    assert_eq!(deserialize_files(SINGLE_BLOB).unwrap(), single());
    assert_eq!(deserialize_files(EMPTY_BLOB).unwrap(), empty());
}

#[test]
fn rust_serializer_inner_msgpack_matches_go() {
    for (name, files, go_blob) in [
        ("basic", basic(), BASIC_BLOB),
        ("edge", edge(), EDGE_BLOB),
        ("single", single(), SINGLE_BLOB),
        ("empty", empty(), EMPTY_BLOB),
    ] {
        let rust_blob = serialize_files(&files).unwrap();
        let go_msgpack = zstd::decode_all(go_blob).unwrap();
        let rust_msgpack = zstd::decode_all(rust_blob.as_slice()).unwrap();
        assert_eq!(
            rust_msgpack, go_msgpack,
            "inner MessagePack diverged from Go for the {name:?} fixture"
        );
    }
}

#[test]
fn rust_round_trip() {
    for files in [basic(), edge(), single(), empty()] {
        let blob = serialize_files(&files).unwrap();
        assert_eq!(deserialize_files(&blob).unwrap(), files);
    }
}
