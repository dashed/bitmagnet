//! [`Torrent`] and [`TorrentFileSummary`], mirroring Go's `model.Torrent`
//! (`torrents.gen.go`) and `model.TorrentFileSummary`
//! (`torrent_file_summary.go`).

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::blob::{deserialize_files, BlobError, BlobFile};
use crate::enums::{file_extension_from_path, FileType, FilesStatus};
use crate::info_hash::InfoHash;

/// A torrent row from the `torrents` table.
///
/// This ports the scalar columns plus the Phase 1 blob columns (`files_data`,
/// `file_extensions`). The GORM association graph (contents, sources, files,
/// pieces, tags, hint) and the DB-managed `created_at` / `updated_at`
/// timestamps are intentionally omitted — the Rust services read those via
/// dedicated queries when needed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Torrent {
    /// 20-byte info hash (primary key).
    pub info_hash: InfoHash,
    /// Torrent display name.
    pub name: String,
    /// Total size in bytes.
    pub size: u64,
    /// Whether this is a private torrent.
    pub private: bool,
    /// Whether/how the file list is known.
    pub files_status: FilesStatus,
    /// Single-file torrent extension, if applicable.
    #[serde(default)]
    pub extension: Option<String>,
    /// Number of files, when known.
    #[serde(default)]
    pub files_count: Option<u32>,
    /// Compressed `files_data` blob (see [`deserialize_files`]); `None` when no
    /// file list is stored. Go tags this `json:"-"`; we keep it for in-process
    /// use, but skip it during (de)serialisation to match the Go API shape and
    /// avoid emitting the binary blob.
    #[serde(skip)]
    pub files_data: Option<Vec<u8>>,
    /// Distinct lowercased file extensions (the `file_extensions` JSON column).
    #[serde(default)]
    pub file_extensions: Vec<String>,
}

impl Torrent {
    /// Decompresses and decodes [`Self::files_data`], returning an empty vec
    /// when no blob is present.
    pub fn files(&self) -> Result<Vec<BlobFile>, BlobError> {
        match &self.files_data {
            Some(blob) => deserialize_files(blob),
            None => Ok(Vec::new()),
        }
    }
}

/// Aggregate statistics over a torrent's files, mirroring
/// `model.TorrentFileSummary` and the Go `BuildFileSummary` builder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentFileSummary {
    /// Info hash of the torrent these files belong to.
    pub info_hash: InfoHash,
    /// Number of files.
    pub file_count: u32,
    /// Sum of all file sizes in bytes.
    pub total_size: i64,
    /// Size of the largest single file in bytes.
    pub largest_file_size: i64,
    /// Sorted, de-duplicated list of file extensions.
    pub extensions: Vec<String>,
    /// Whether any file is a video.
    pub has_video: bool,
    /// Whether any file is a subtitle.
    pub has_subtitle: bool,
    /// Whether any file is audio.
    pub has_audio: bool,
}

impl TorrentFileSummary {
    /// Builds the summary from a torrent's files, mirroring Go's
    /// `blobmigration.BuildFileSummary` (extensions are derived from the file
    /// *path*, de-duplicated and sorted; the media flags come from those
    /// extensions' file types).
    pub fn from_files(info_hash: InfoHash, files: &[BlobFile]) -> Self {
        // BTreeSet yields the unique, sorted extensions Go produces via
        // ExtractUniqueExtensions + sort.Strings.
        let extensions: BTreeSet<String> = files
            .iter()
            .filter_map(|f| file_extension_from_path(&f.path))
            .collect();

        let mut total_size: i64 = 0;
        let mut largest_file_size: i64 = 0;
        for f in files {
            let size = f.size as i64;
            total_size += size;
            if size > largest_file_size {
                largest_file_size = size;
            }
        }

        let mut summary = Self {
            info_hash,
            file_count: files.len() as u32,
            total_size,
            largest_file_size,
            extensions: extensions.into_iter().collect(),
            has_video: false,
            has_subtitle: false,
            has_audio: false,
        };

        for ext in &summary.extensions {
            match FileType::from_extension(ext) {
                Some(FileType::Video) => summary.has_video = true,
                Some(FileType::Audio) => summary.has_audio = true,
                Some(FileType::Subtitles) => summary.has_subtitle = true,
                _ => {}
            }
        }

        summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ih() -> InfoHash {
        "0123456789abcdef0123456789abcdef01234567".parse().unwrap()
    }

    #[test]
    fn torrent_without_blob_has_no_files() {
        let t = Torrent {
            info_hash: ih(),
            name: "x".to_owned(),
            size: 0,
            private: false,
            files_status: FilesStatus::NoInfo,
            extension: None,
            files_count: None,
            files_data: None,
            file_extensions: vec![],
        };
        assert!(t.files().unwrap().is_empty());
    }

    #[test]
    fn summary_matches_go_build_file_summary() {
        let files = vec![
            BlobFile {
                index: 0,
                path: "S1/ep1.mkv".to_owned(),
                extension: "mkv".to_owned(),
                size: 100,
            },
            BlobFile {
                index: 1,
                path: "S1/ep2.mkv".to_owned(),
                extension: "mkv".to_owned(),
                size: 300,
            },
            BlobFile {
                index: 2,
                path: "S1/subs/ep1.srt".to_owned(),
                extension: "srt".to_owned(),
                size: 5,
            },
            BlobFile {
                index: 3,
                path: "readme".to_owned(),
                extension: String::new(),
                size: 1,
            },
        ];
        let s = TorrentFileSummary::from_files(ih(), &files);
        assert_eq!(s.file_count, 4);
        assert_eq!(s.total_size, 406);
        assert_eq!(s.largest_file_size, 300);
        // Unique + sorted; "readme" contributes no extension.
        assert_eq!(s.extensions, vec!["mkv".to_owned(), "srt".to_owned()]);
        assert!(s.has_video);
        assert!(s.has_subtitle);
        assert!(!s.has_audio);
    }
}
