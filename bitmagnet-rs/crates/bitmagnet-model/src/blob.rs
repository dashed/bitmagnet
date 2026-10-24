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

use std::cell::Cell;
use std::fmt;
use std::io::Read;

use serde::de::{DeserializeSeed, Error as _, IgnoredAny, SeqAccess, Visitor};
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

impl BlobFile {
    /// Returns the variable-length string bytes owned by this decoded file.
    ///
    /// The fixed `BlobFile`/`Vec` allocation is independently bounded by the
    /// file-count limits. This value captures the path/extension allocation
    /// that can otherwise grow without a schema-level maximum.
    #[must_use]
    pub fn owned_string_bytes(&self) -> usize {
        self.path.len().saturating_add(self.extension.len())
    }
}

/// One bounded file-blob decode and its allocation-relevant byte counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedFiles {
    /// Decoded file rows in their original blob order.
    pub files: Vec<BlobFile>,
    /// MessagePack bytes produced by ZSTD before deserialisation.
    pub decompressed_bytes: usize,
    /// Path and extension string bytes owned by [`Self::files`].
    pub owned_string_bytes: usize,
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

/// Deserialises a file blob while bounding decompressed bytes and decoded rows.
///
/// The decoder grows its output buffer in capped fixed-size steps and probes
/// one byte beyond `max_decompressed_bytes`, so a highly compressible frame
/// cannot allocate an unbounded intermediate buffer. A custom MessagePack
/// sequence visitor rejects an oversized declared row count before allocating
/// the decoded vector; callers should still use their authoritative pre-decode
/// count probe.
pub fn deserialize_files_bounded(
    data: &[u8],
    max_decompressed_bytes: usize,
    max_files: usize,
) -> Result<DecodedFiles, BlobError> {
    let mut decoder = zstd::stream::read::Decoder::new(data)?;
    decoder.window_log_max(window_log_for_limit(max_decompressed_bytes))?;
    let raw = read_decompressed_bounded(&mut decoder, max_decompressed_bytes)?;

    let limit_hit = Cell::new(None);
    let mut deserializer = rmp_serde::Deserializer::from_read_ref(&raw);
    let files = match (BoundedFilesSeed {
        max_files,
        limit_hit: &limit_hit,
    })
    .deserialize(&mut deserializer)
    {
        Ok(files) => files,
        Err(_) if limit_hit.get().is_some() => {
            return Err(BlobError::FileCountLimitExceeded {
                count: limit_hit.get().unwrap_or(max_files.saturating_add(1)),
                limit: max_files,
            });
        }
        Err(error) => return Err(BlobError::Decode(error)),
    };
    let owned_string_bytes = files.iter().fold(0_usize, |total, file| {
        total.saturating_add(file.owned_string_bytes())
    });

    Ok(DecodedFiles {
        files,
        decompressed_bytes: raw.len(),
        owned_string_bytes,
    })
}

fn read_decompressed_bounded<R: Read>(reader: &mut R, limit: usize) -> Result<Vec<u8>, BlobError> {
    const CHUNK_BYTES: usize = 64 * 1024;

    let mut raw = Vec::new();
    let mut buffer = [0_u8; CHUNK_BYTES];
    while raw.len() < limit {
        let remaining = limit - raw.len();
        let read_len = remaining.min(buffer.len());
        let read = reader.read(&mut buffer[..read_len])?;
        if read == 0 {
            return Ok(raw);
        }

        let needed = raw.len().saturating_add(read);
        if raw.capacity() < needed {
            let doubled = if raw.capacity() == 0 {
                CHUNK_BYTES
            } else {
                raw.capacity().saturating_mul(2)
            };
            let target = doubled.max(needed).min(limit);
            raw.try_reserve_exact(target.saturating_sub(raw.len()))?;
        }
        raw.extend_from_slice(&buffer[..read]);
    }

    let mut probe = [0_u8; 1];
    if reader.read(&mut probe)? == 0 {
        Ok(raw)
    } else {
        Err(BlobError::DecompressedLimitExceeded { limit })
    }
}

fn window_log_for_limit(limit: usize) -> u32 {
    let limit = limit.max(1);
    let ceil_log = usize::BITS - (limit - 1).leading_zeros();
    // Keep compatibility with ordinary Go-produced frames at very small test
    // or remaining-chunk limits while still rejecting hostile huge windows.
    ceil_log.clamp(23, 31)
}

struct BoundedFilesSeed<'a> {
    max_files: usize,
    limit_hit: &'a Cell<Option<usize>>,
}

impl<'de> DeserializeSeed<'de> for BoundedFilesSeed<'_> {
    type Value = Vec<BlobFile>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedFilesVisitor {
            max_files: self.max_files,
            limit_hit: self.limit_hit,
        })
    }
}

struct BoundedFilesVisitor<'a> {
    max_files: usize,
    limit_hit: &'a Cell<Option<usize>>,
}

impl<'de> Visitor<'de> for BoundedFilesVisitor<'_> {
    type Value = Vec<BlobFile>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "at most {} torrent files", self.max_files)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let size_hint = sequence.size_hint().unwrap_or(0);
        if size_hint > self.max_files {
            self.limit_hit.set(Some(size_hint));
            return Err(A::Error::custom("torrent file count exceeds decode limit"));
        }
        let mut files = Vec::new();
        loop {
            if files.len() >= self.max_files {
                if sequence.next_element::<IgnoredAny>()?.is_some() {
                    let count = files.len().saturating_add(1);
                    self.limit_hit.set(Some(count));
                    return Err(A::Error::custom("torrent file count exceeds decode limit"));
                }
                break;
            }
            if files.len() == files.capacity() {
                let remaining = self.max_files - files.len();
                let growth = files.capacity().max(16).min(remaining);
                files.try_reserve_exact(growth).map_err(A::Error::custom)?;
            }
            match sequence.next_element::<BlobFile>()? {
                Some(file) => files.push(file),
                None => break,
            }
        }
        Ok(files)
    }
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
    /// A bounded decode buffer could not reserve its next capped allocation.
    #[error("bounded decode allocation failed: {0}")]
    Allocation(#[from] std::collections::TryReserveError),
    /// ZSTD output exceeded the caller's hard decompression ceiling.
    #[error("decompressed file blob exceeds {limit} bytes")]
    DecompressedLimitExceeded {
        /// Maximum accepted decompressed MessagePack bytes.
        limit: usize,
    },
    /// The decoded blob contained more rows than the caller permits.
    #[error("decoded file blob contains {count} files, exceeding limit {limit}")]
    FileCountLimitExceeded {
        /// Decoded row count observed in the blob.
        count: usize,
        /// Maximum accepted decoded row count.
        limit: usize,
    },
    /// Decoded path/extension strings exceeded the caller's owned-byte ceiling.
    #[error("decoded file blob owns {bytes} string bytes, exceeding limit {limit}")]
    OwnedStringLimitExceeded {
        /// Path and extension bytes observed after decoding.
        bytes: usize,
        /// Maximum accepted owned string bytes.
        limit: usize,
    },
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
    fn bounded_decode_reports_bytes_and_owned_strings() {
        let files = sample();
        let blob = serialize_files(&files).unwrap();
        let decoded = deserialize_files_bounded(&blob, 1_024, files.len()).unwrap();

        assert_eq!(decoded.files, files);
        assert!(decoded.decompressed_bytes > decoded.owned_string_bytes);
        assert_eq!(
            decoded.owned_string_bytes,
            files
                .iter()
                .map(BlobFile::owned_string_bytes)
                .sum::<usize>()
        );
    }

    #[test]
    fn bounded_decode_rejects_compression_expansion() {
        let files = vec![BlobFile {
            index: 0,
            path: "x".repeat(8_192),
            extension: "mkv".to_owned(),
            size: 1,
        }];
        let blob = serialize_files(&files).unwrap();

        assert!(matches!(
            deserialize_files_bounded(&blob, 128, 1),
            Err(BlobError::DecompressedLimitExceeded { limit: 128 })
        ));
    }

    #[test]
    fn bounded_decode_accepts_exact_raw_boundary_and_rejects_plus_one() {
        let files = sample();
        let raw = rmp_serde::to_vec_named(&files).unwrap();
        let blob = zstd::encode_all(raw.as_slice(), ZSTD_LEVEL).unwrap();

        let decoded = deserialize_files_bounded(&blob, raw.len(), files.len()).unwrap();
        assert_eq!(decoded.files, files);
        assert_eq!(decoded.decompressed_bytes, raw.len());
        assert!(matches!(
            deserialize_files_bounded(&blob, raw.len() - 1, files.len()),
            Err(BlobError::DecompressedLimitExceeded { .. })
        ));
    }

    #[test]
    fn bounded_decode_rejects_file_count_mismatch() {
        let files = sample();
        let blob = serialize_files(&files).unwrap();

        assert!(matches!(
            deserialize_files_bounded(&blob, 1_024, 1),
            Err(BlobError::FileCountLimitExceeded { count: 2, limit: 1 })
        ));
    }

    #[test]
    fn bounded_decode_rejects_hostile_declared_array_before_preallocation() {
        let declared_array32_max = [0xdd, 0xff, 0xff, 0xff, 0xff];
        let blob = zstd::encode_all(declared_array32_max.as_slice(), ZSTD_LEVEL).unwrap();

        assert!(matches!(
            deserialize_files_bounded(&blob, 64, 10),
            Err(BlobError::FileCountLimitExceeded { limit: 10, .. })
        ));
    }

    #[test]
    fn bounded_decode_rejects_truncated_hostile_string_header() {
        let truncated = [
            0x91, // one file
            0x84, // four map fields
            0xa1, b'i', 0x00, // index = 0
            0xa1, b'p', 0xdb, 0xff, 0xff, 0xff, 0xff, // impossible str32 path
        ];
        let blob = zstd::encode_all(truncated.as_slice(), ZSTD_LEVEL).unwrap();

        assert!(matches!(
            deserialize_files_bounded(&blob, 64, 1),
            Err(BlobError::Decode(_))
        ));
    }

    #[test]
    fn blob_error_converts_to_common_error() {
        let err = deserialize_files(b"bad").unwrap_err();
        let common: bitmagnet_common::Error = err.into();
        assert!(matches!(common, bitmagnet_common::Error::Other(_)));
    }
}
