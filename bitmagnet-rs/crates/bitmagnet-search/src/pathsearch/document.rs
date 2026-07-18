//! Path-bag document construction from torrent blob rows.

use anyhow::Context;
use bitmagnet_db::TorrentWithBlob;

/// One L3 pathsearch document: one torrent path-bag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathDocument {
    pub info_hash: Vec<u8>,
    /// Torrent display name, always indexed alongside `paths` so a term that
    /// lives only in the name (not any file path) is still recall-visible on the
    /// relevance route (F1). Independent of the single-file empty-`paths`
    /// surrogate below.
    pub name: String,
    pub paths: Vec<String>,
    pub size: u64,
    pub files_count: u64,
    pub seeders: u64,
    pub published_at: i64,
}

impl PathDocument {
    /// Build a pathsearch document from a torrent+blob row.
    ///
    /// Returns `Ok(None)` for torrents that expose no path text. For single-file
    /// torrents without a file blob, the torrent name is the path-equivalent.
    ///
    /// # Errors
    /// Returns blob decode errors from [`TorrentWithBlob::files`].
    pub fn from_torrent(row: &TorrentWithBlob) -> anyhow::Result<Option<Self>> {
        let files = row
            .files()
            .with_context(|| format!("decoding files_data for {}", row.info_hash))?;
        let mut paths: Vec<String> = files
            .into_iter()
            .filter_map(|f| {
                let path = f.path.trim();
                (!path.is_empty()).then(|| path.to_owned())
            })
            .collect();

        if paths.is_empty() && row.files_status.eq_ignore_ascii_case("single") {
            let name = row.name.trim();
            if !name.is_empty() {
                paths.push(name.to_owned());
            }
        }

        if paths.is_empty() {
            return Ok(None);
        }

        let files_count = row
            .files_count
            .and_then(|n| u64::try_from(n).ok())
            .unwrap_or(paths.len() as u64);

        Ok(Some(Self {
            info_hash: row.info_hash.as_slice().to_vec(),
            name: row.name.trim().to_owned(),
            paths,
            size: u64::try_from(row.size).unwrap_or(0),
            files_count,
            seeders: 0,
            published_at: row.published_at,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::PathDocument;
    use bitmagnet_db::TorrentWithBlob;
    use bitmagnet_model::{serialize_files, BlobFile};

    fn row(files_status: &str, files_data: Option<Vec<u8>>) -> TorrentWithBlob {
        TorrentWithBlob {
            info_hash: "0123456789abcdef0123456789abcdef01234567".parse().unwrap(),
            name: "Release.Name.mkv".to_owned(),
            size: 123,
            files_status: files_status.to_owned(),
            files_count: None,
            published_at: 1_600_000_000,
            files_data,
        }
    }

    #[test]
    fn uses_blob_paths_when_present() {
        let blob = serialize_files(&[BlobFile {
            index: 0,
            path: "Season 01/Episode.mkv".to_owned(),
            extension: "mkv".to_owned(),
            size: 123,
        }])
        .unwrap();
        let doc = PathDocument::from_torrent(&row("multi", Some(blob)))
            .unwrap()
            .unwrap();
        assert_eq!(doc.paths, vec!["Season 01/Episode.mkv"]);
        assert_eq!(doc.files_count, 1);
        // The display name is always captured, independent of the file paths, so a
        // multi-file torrent whose term lives only in the name stays recall-visible.
        assert_eq!(doc.name, "Release.Name.mkv");
    }

    #[test]
    fn single_file_without_blob_uses_torrent_name() {
        let doc = PathDocument::from_torrent(&row("single", None))
            .unwrap()
            .unwrap();
        assert_eq!(doc.paths, vec!["Release.Name.mkv"]);
    }

    #[test]
    fn no_path_text_skips_document() {
        assert!(PathDocument::from_torrent(&row("multi", None))
            .unwrap()
            .is_none());
    }
}
