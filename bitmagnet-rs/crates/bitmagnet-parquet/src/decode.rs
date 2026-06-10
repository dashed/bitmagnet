//! Blob → file-row decode, the reused `bench/blob_export` path.
//!
//! One torrent's `files_data` blob (zstd(msgpack[{i,p,e,s}]), see
//! `bitmagnet-model/src/blob.rs`) becomes a sequence of [`FileRow`]s, one per
//! file. The single rule that matters here is **G1**: the row's `extension` is
//! derived from the file PATH via [`file_extension_from_path`], **never** the
//! blob's stored `e` field (empty for crawl-path torrents). This matches the
//! live PostgreSQL generated column byte-for-byte and is the uniform basis for
//! `WHERE extension = 'mkv'`.

use bitmagnet_model::{deserialize_files, file_extension_from_path, BlobError, BlobFile};

/// One file inside a torrent, flattened for the columnar fact table.
///
/// `extension == None` is a SQL `NULL` extension (a file with no path-derived
/// extension), kept distinct from the empty string so the Parquet column's
/// null/zone-map statistics stay correct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRow {
    /// 40-char lowercase hex info hash of the owning torrent.
    pub info_hash_hex: String,
    /// Zero-based file index within the torrent.
    pub file_index: u32,
    /// File path relative to the torrent root.
    pub path: String,
    /// G1 path-derived extension (lowercased, no dot), or `None`.
    pub extension: Option<String>,
    /// File size in bytes.
    pub size: u64,
}

/// Running counters for an export, surfaced by V3 (the first production base
/// export is the "0 decode errors across all torrents" validation).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DecodeStats {
    /// Torrents whose blob was present and decoded (incl. empty file lists).
    pub torrents_ok: u64,
    /// Torrents whose blob failed to decode (zstd/msgpack error). **V3 target = 0.**
    pub decode_errors: u64,
    /// Total file rows emitted.
    pub file_rows: u64,
}

impl DecodeStats {
    /// Fold one torrent's outcome in.
    fn record_ok(&mut self, rows: usize) {
        self.torrents_ok += 1;
        self.file_rows += rows as u64;
    }

    fn record_error(&mut self) {
        self.decode_errors += 1;
    }
}

/// Decode one torrent's blob into [`FileRow`]s, applying G1.
///
/// `files` is the already-decompressed file list (callers that hold a raw blob
/// use [`decode_blob`]). Returns the rows; the caller updates [`DecodeStats`].
pub fn rows_from_files(info_hash_hex: &str, files: &[BlobFile]) -> Vec<FileRow> {
    files
        .iter()
        .map(|f| FileRow {
            info_hash_hex: info_hash_hex.to_owned(),
            file_index: f.index,
            path: f.path.clone(),
            // G1: ALWAYS path-derive; ignore the blob `e`.
            extension: file_extension_from_path(&f.path),
            size: f.size,
        })
        .collect()
}

/// Decode a raw compressed blob into [`FileRow`]s (the offline/from-hex path).
pub fn decode_blob(info_hash_hex: &str, blob: &[u8]) -> Result<Vec<FileRow>, BlobError> {
    let files = deserialize_files(blob)?;
    Ok(rows_from_files(info_hash_hex, &files))
}

/// Decode one torrent into a sink callback, folding the outcome into `stats`.
///
/// `decode` yields either the file list or the [`BlobError`]; a `None` blob (no
/// files stored) decodes to an empty list and still counts as `torrents_ok`.
pub fn decode_into<F>(
    info_hash_hex: &str,
    decode: Result<Vec<BlobFile>, BlobError>,
    stats: &mut DecodeStats,
    mut sink: F,
) where
    F: FnMut(FileRow),
{
    match decode {
        Ok(files) => {
            let rows = rows_from_files(info_hash_hex, &files);
            stats.record_ok(rows.len());
            for r in rows {
                sink(r);
            }
        }
        Err(_) => stats.record_error(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitmagnet_model::serialize_files;

    fn files() -> Vec<BlobFile> {
        vec![
            BlobFile {
                index: 0,
                // G1: ext derived from PATH, NOT the (intentionally wrong) `e`.
                path: "Movie/video.MKV".to_owned(),
                extension: "wrong".to_owned(),
                size: 2_000_000_000,
            },
            BlobFile {
                index: 1,
                path: "Movie/readme".to_owned(), // no extension
                extension: String::new(),
                size: 10,
            },
        ]
    }

    #[test]
    fn g1_extension_is_path_derived_lowercased() {
        let rows = rows_from_files("aa", &files());
        assert_eq!(rows[0].extension.as_deref(), Some("mkv"));
        assert_eq!(rows[1].extension, None);
        assert_eq!(rows[0].size, 2_000_000_000);
    }

    #[test]
    fn decode_blob_round_trips() {
        let blob = serialize_files(&files()).unwrap();
        let rows = decode_blob("bb", &blob).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].info_hash_hex, "bb");
    }

    #[test]
    fn decode_into_counts_ok_and_errors() {
        let mut stats = DecodeStats::default();
        let mut n = 0;
        decode_into("aa", Ok(files()), &mut stats, |_| n += 1);
        decode_into("bb", Ok(Vec::new()), &mut stats, |_| n += 1); // empty list still OK
        decode_into(
            "cc",
            Err(deserialize_files(b"garbage").unwrap_err()),
            &mut stats,
            |_| n += 1,
        );
        assert_eq!(stats.torrents_ok, 2);
        assert_eq!(stats.decode_errors, 1);
        assert_eq!(stats.file_rows, 2);
        assert_eq!(n, 2);
    }
}
