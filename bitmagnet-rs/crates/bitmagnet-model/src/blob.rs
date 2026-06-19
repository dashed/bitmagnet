//! The compressed torrent-file blob format, mirroring Go's
//! `internal/blobmigration/serializer.go`.
//!
//! Wire format (verified byte-for-byte against the real Go serializer — see
//! `tests/blob_fixture.rs`):
//!
//! ```text
//! zstd( msgpack_array[ map{ "i": uint, "p": str, "e": str, "s": uint }, ... ] )
//! ```
//!
//! * MessagePack via `vmihailenco/msgpack/v5`, which encodes Go structs as
//!   **maps keyed by the msgpack tag** (`i`/`p`/`e`/`s`). [`BlobFile`]
//!   therefore uses `#[serde(rename = ...)]` and is (de)serialised through
//!   rmp-serde's *named* (map) representation — NOT the default positional
//!   array, which the Go decoder would reject.
//! * ZSTD via `klauspost/compress` at `SpeedDefault` (≈ level 3). The level is
//!   irrelevant when decompressing (any standard frame is accepted); we mirror
//!   it when compressing.

use serde::{Deserialize, Serialize};

/// ZSTD compression level mirroring klauspost's `SpeedDefault`. Decompression
/// ignores the level; this only affects bytes produced by [`serialize_files`].
const ZSTD_LEVEL: i32 = 3;

/// One file inside a torrent, as stored in the compressed `files_data` blob.
///
/// The field rename targets are the compact MessagePack keys used by the Go
/// `compactFile` struct: `i`, `p`, `e`, `s`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobFile {
    /// Zero-based file index within the torrent (Go `compactFile.i`).
    #[serde(rename = "i")]
    pub index: u32,
    /// File path relative to the torrent root (Go `compactFile.p`).
    #[serde(rename = "p")]
    pub path: String,
    /// Lowercased file extension without the leading dot, or empty when none.
    /// An empty string corresponds to a SQL `NULL` extension on the Go side
    /// (Go `compactFile.e`).
    #[serde(rename = "e")]
    pub extension: String,
    /// File size in bytes (Go `compactFile.s`).
    #[serde(rename = "s")]
    pub size: u64,
}

/// Serialises files to the compressed blob format (MessagePack → ZSTD),
/// wire-compatible with Go's `blobmigration.SerializeFiles`.
pub fn serialize_files(files: &[BlobFile]) -> Result<Vec<u8>, BlobError> {
    // `to_vec_named` emits each struct as a MessagePack *map* keyed by the
    // renamed fields, matching vmihailenco/msgpack's struct encoding. The
    // default `to_vec` would emit positional arrays, which Go would not decode.
    let raw = rmp_serde::to_vec_named(files)?;
    Ok(zstd::encode_all(raw.as_slice(), ZSTD_LEVEL)?)
}

/// Deserialises a compressed blob (ZSTD → MessagePack) produced by Go's
/// `blobmigration.SerializeFiles`.
pub fn deserialize_files(data: &[u8]) -> Result<Vec<BlobFile>, BlobError> {
    let raw = zstd::decode_all(data)?;
    Ok(rmp_serde::from_slice(&raw)?)
}

/// Errors (de)serialising the file blob.
#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    /// ZSTD compression or decompression failed (wraps the underlying I/O
    /// error reported by the `zstd` crate).
    #[error("zstd error: {0}")]
    Zstd(#[from] std::io::Error),
    /// MessagePack encoding failed.
    #[error("msgpack encode error: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    /// MessagePack decoding failed (e.g. a corrupt or unexpected blob).
    #[error("msgpack decode error: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
}

impl From<BlobError> for bitmagnet_common::Error {
    fn from(err: BlobError) -> Self {
        bitmagnet_common::Error::Other(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<BlobFile> {
        vec![
            BlobFile {
                index: 0,
                path: "a/b.mkv".to_owned(),
                extension: "mkv".to_owned(),
                size: 1_500_000_000,
            },
            BlobFile {
                index: 1,
                path: "noext".to_owned(),
                extension: String::new(),
                size: 0,
            },
        ]
    }

    #[test]
    fn round_trip() {
        let files = sample();
        let blob = serialize_files(&files).unwrap();
        assert_eq!(deserialize_files(&blob).unwrap(), files);
    }

    #[test]
    fn empty_round_trip() {
        let blob = serialize_files(&[]).unwrap();
        assert!(deserialize_files(&blob).unwrap().is_empty());
    }

    #[test]
    fn compressed_output_is_a_zstd_frame() {
        let blob = serialize_files(&sample()).unwrap();
        // Standard ZSTD magic number 0x28B52FFD (little-endian on the wire).
        assert_eq!(&blob[..4], &[0x28, 0xb5, 0x2f, 0xfd]);
    }

    #[test]
    fn deserialize_rejects_garbage() {
        assert!(deserialize_files(b"not a zstd frame").is_err());
    }

    #[test]
    fn blob_error_converts_to_common_error() {
        let err = deserialize_files(b"bad").unwrap_err();
        let common: bitmagnet_common::Error = err.into();
        assert!(matches!(common, bitmagnet_common::Error::Other(_)));
    }
}
