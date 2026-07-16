//! Pure exact-refine predicates and pagination for L3 path-search candidates.
//!
//! This module ports `internal/search/pathsearch/refine.go`. L3 ngram path-bag
//! recall is a superset, so callers must verify each candidate's real file
//! paths here before applying the page window. The composer that orchestrates
//! that pipeline is intentionally outside this module.

use std::collections::HashSet;

use bitmagnet_model::{
    deserialize_files_bounded, file_extension_from_path, BlobError, BlobFile, DecodedFiles,
    FilesStatus, Torrent,
};

use crate::filters::Filters;

/// Exact-refine filter derived from typed search input.
///
/// This ports Go's `refinePredicate` from
/// `internal/search/pathsearch/refine.go`. L3 candidates carry neither real
/// path text nor file extension/size, so every candidate must be checked with
/// this predicate to remove torrent-level false positives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefinePredicate {
    /// Lower-cased real path substring to verify.
    substr: String,
    /// Allowed lower-cased extensions; empty accepts any extension.
    extensions: HashSet<String>,
    /// Minimum file size in bytes; zero is unbounded.
    min_size: u64,
    /// Maximum file size in bytes; zero is unbounded.
    max_size: u64,
}

impl Filters {
    /// Builds the exact-refine predicate.
    ///
    /// This ports Go's `Filters.predicate` in
    /// `internal/search/pathsearch/refine.go`.
    pub fn predicate(&self) -> RefinePredicate {
        RefinePredicate {
            substr: self.query.trim().to_lowercase(),
            extensions: self
                .extensions
                .iter()
                .map(|extension| extension.to_lowercase())
                .collect(),
            min_size: self.min_size,
            max_size: self.max_size,
        }
    }
}

impl RefinePredicate {
    /// Returns the normalized real-path substring this predicate verifies.
    ///
    /// The C3b composer uses this to require path text before selecting the L3
    /// route, matching Go's `pred.substr == ""` eligibility check.
    pub fn substr(&self) -> &str {
        &self.substr
    }

    /// Reports whether the normalized real-path substring is empty.
    pub fn is_empty_substr(&self) -> bool {
        self.substr.is_empty()
    }

    /// Reports whether an extension predicate is active.
    fn has_extension_filter(&self) -> bool {
        !self.extensions.is_empty()
    }
}

/// Returns a file's lower-cased extension.
///
/// This ports Go's `fileExtension` in
/// `internal/search/pathsearch/refine.go`. Crawl-path blobs can have an empty
/// stored extension, so deriving it from the real path is required to mirror
/// the PostgreSQL generated-column semantics.
pub(crate) fn file_extension(file: &BlobFile) -> String {
    if file.extension.is_empty() {
        file_extension_from_path(&file.path).unwrap_or_default()
    } else {
        file.extension.to_lowercase()
    }
}

/// Reports whether one file satisfies the exact-refine predicate.
///
/// This ports Go's `matchFile` in `internal/search/pathsearch/refine.go`.
pub(crate) fn match_file(file: &BlobFile, predicate: &RefinePredicate) -> bool {
    if !predicate.substr.is_empty() && !file.path.to_lowercase().contains(&predicate.substr) {
        return false;
    }

    if predicate.has_extension_filter() && !predicate.extensions.contains(&file_extension(file)) {
        return false;
    }

    if predicate.min_size > 0 && file.size < predicate.min_size {
        return false;
    }

    if predicate.max_size > 0 && file.size > predicate.max_size {
        return false;
    }

    true
}

/// Returns matching files in input order.
///
/// This ports Go's `matchedFiles` in
/// `internal/search/pathsearch/refine.go`.
fn matched_files(files: &[BlobFile], predicate: &RefinePredicate) -> Vec<BlobFile> {
    files
        .iter()
        .filter(|file| match_file(file, predicate))
        .cloned()
        .collect()
}

/// Reports whether a torrent keeps at least one exact matching file.
///
/// This ports Go's `torrentMatches` in
/// `internal/search/pathsearch/refine.go`. An L3 candidate with no matching
/// file is a recall false positive and must be dropped.
pub fn torrent_matches(files: &[BlobFile], predicate: &RefinePredicate) -> bool {
    files.iter().any(|file| match_file(file, predicate))
}

/// Resolves the file list used to exact-refine a candidate torrent.
///
/// This ports Go's `filesForRefine` in
/// `internal/search/pathsearch/refine.go`. Blob decode errors and empty blobs
/// fall through without panicking. A multi-file torrent with no obtainable
/// file list returns `None`; callers must fail loud or fall back rather than
/// silently dropping a possible match (Go CAVEAT B).
///
/// A single-file torrent falls back to its name and total size. Its extension
/// is deliberately empty so [`file_extension`] derives it from the name via
/// [`file_extension_from_path`]. This is the Go CAVEAT C soundness invariant:
/// it matches the PostgreSQL generated column and Tantivy document builder.
#[cfg(test)]
pub(crate) fn files_for_refine(torrent: &Torrent) -> Option<Vec<BlobFile>> {
    if let Ok(files) = torrent.files() {
        if !files.is_empty() {
            return Some(files);
        }
    }

    if torrent.files_status == FilesStatus::Single {
        return Some(vec![BlobFile {
            index: 0,
            path: torrent.name.clone(),
            extension: String::new(),
            size: torrent.size,
        }]);
    }

    None
}

/// One bounded file-list decode used by the composer.
pub(crate) struct BoundedRefineFiles {
    /// Decoded files in blob order.
    pub files: Vec<BlobFile>,
    /// MessagePack bytes materialised while decoding the blob.
    pub decompressed_bytes: usize,
    /// Path and extension bytes owned by the decoded files.
    pub owned_string_bytes: usize,
}

/// Resolves exact-refine files without permitting unbounded decompression.
///
/// Multi-file blob errors are returned so the composer can fail loud or serve
/// an explicitly capped prefix. Single-file torrents retain the Go-compatible
/// name surrogate even if an irrelevant blob is corrupt or oversized.
pub(crate) fn files_for_refine_bounded(
    torrent: &Torrent,
    max_decompressed_bytes: usize,
    max_owned_string_bytes: usize,
    max_files: usize,
) -> Result<Option<BoundedRefineFiles>, BlobError> {
    if let Some(blob) = &torrent.files_data {
        match deserialize_files_bounded(blob, max_decompressed_bytes, max_files) {
            Ok(DecodedFiles {
                files,
                decompressed_bytes,
                owned_string_bytes,
            }) if !files.is_empty() => {
                if owned_string_bytes > max_owned_string_bytes {
                    return Err(BlobError::OwnedStringLimitExceeded {
                        bytes: owned_string_bytes,
                        limit: max_owned_string_bytes,
                    });
                }
                return Ok(Some(BoundedRefineFiles {
                    files,
                    decompressed_bytes,
                    owned_string_bytes,
                }));
            }
            Ok(_) => {}
            Err(error) if torrent.files_status != FilesStatus::Single => return Err(error),
            Err(_) => {}
        }
    }

    if torrent.files_status == FilesStatus::Single {
        if max_files == 0 {
            return Err(BlobError::FileCountLimitExceeded { count: 1, limit: 0 });
        }
        if torrent.name.len() > max_owned_string_bytes {
            return Err(BlobError::OwnedStringLimitExceeded {
                bytes: torrent.name.len(),
                limit: max_owned_string_bytes,
            });
        }
        let file = BlobFile {
            index: 0,
            path: torrent.name.clone(),
            extension: String::new(),
            size: torrent.size,
        };
        let owned_string_bytes = file.owned_string_bytes();
        return Ok(Some(BoundedRefineFiles {
            files: vec![file],
            decompressed_bytes: 0,
            owned_string_bytes,
        }));
    }

    Ok(None)
}

/// Exact-refines one candidate torrent and reports whether refinement was possible.
///
/// This ports Go's `torrentRefine` in
/// `internal/search/pathsearch/refine.go`. The second tuple element is `false`
/// when no trustworthy file list is obtainable, propagating the CAVEAT B
/// fail-loud signal to the future composer.
#[cfg(test)]
pub(crate) fn torrent_refine(torrent: &Torrent, predicate: &RefinePredicate) -> (bool, bool) {
    let Some(files) = files_for_refine(torrent) else {
        return (false, false);
    };

    (torrent_matches(&files, predicate), true)
}

/// Applies offset and limit to an already-refined, already-ordered row set.
///
/// This ports Go's generic `paginate[T]` in
/// `internal/search/pathsearch/refine.go`. It is the only place the L3 route
/// applies the user's page window. A zero limit returns all remaining rows;
/// the future composer reports the candidate-derived total as an estimate.
pub fn paginate<T>(mut rows: Vec<T>, offset: u64, limit: u64) -> Vec<T> {
    let Ok(offset) = usize::try_from(offset) else {
        return Vec::new();
    };

    if offset >= rows.len() {
        return Vec::new();
    }

    let mut page = rows.split_off(offset);
    if limit > 0 {
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        if limit < page.len() {
            page.truncate(limit);
        }
    }

    page
}

/// Returns de-duplicated exact-matched paths in first-seen order.
///
/// This ports the Go `distinctMatchedPaths` collapse core from
/// `internal/search/pathsearch/refine.go`.
pub fn distinct_matched_paths(files: &[BlobFile], predicate: &RefinePredicate) -> Vec<String> {
    let matched = matched_files(files, predicate);
    let mut seen = HashSet::new();
    let mut paths = Vec::new();

    for file in &matched {
        if seen.insert(file.path.as_str()) {
            paths.push(file.path.clone());
        }
    }

    paths
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitmagnet_model::{serialize_files, InfoHash};

    fn file(path: &str, extension: &str, size: u64) -> BlobFile {
        BlobFile {
            index: 0,
            path: path.to_owned(),
            extension: extension.to_owned(),
            size,
        }
    }

    fn torrent(
        files_status: FilesStatus,
        name: &str,
        size: u64,
        files_data: Option<Vec<u8>>,
    ) -> Torrent {
        Torrent {
            info_hash: "0123456789abcdef0123456789abcdef01234567"
                .parse::<InfoHash>()
                .unwrap(),
            name: name.to_owned(),
            size,
            private: false,
            files_status,
            extension: None,
            files_count: None,
            files_data,
            file_extensions: Vec::new(),
        }
    }

    fn predicate(
        query: &str,
        extensions: &[&str],
        min_size: u64,
        max_size: u64,
    ) -> RefinePredicate {
        Filters {
            query: query.to_owned(),
            extensions: extensions
                .iter()
                .map(|extension| (*extension).to_owned())
                .collect(),
            min_size,
            max_size,
        }
        .predicate()
    }

    #[test]
    fn filters_predicate_normalizes_query_and_extensions() {
        let predicate = predicate("  InCePtIoN  ", &["MKV", "mkv", "MP4"], 10, 20);

        assert_eq!(predicate.substr(), "inception");
        assert!(!predicate.is_empty_substr());
        assert_eq!(
            predicate.extensions,
            HashSet::from(["mkv".to_owned(), "mp4".to_owned()])
        );
        assert_eq!(predicate.min_size, 10);
        assert_eq!(predicate.max_size, 20);
    }

    #[test]
    fn match_file_checks_case_insensitive_substring() {
        let predicate = predicate("inception", &[], 0, 0);

        assert!(!match_file(
            &file("movies/Interstellar.2014.mkv", "mkv", 0),
            &predicate
        ));
        assert!(match_file(
            &file("Movies/INCEPTION.2010.MKV", "MKV", 0),
            &predicate
        ));
    }

    #[test]
    fn match_file_checks_extension_filter() {
        let predicate = predicate("show", &["MKV", "mp4"], 0, 0);

        assert!(!match_file(&file("show.s01e01.avi", "avi", 0), &predicate));
        assert!(match_file(&file("show.s01e01.mp4", "mp4", 0), &predicate));
        assert!(match_file(&file("show.s01e02.mkv", "", 0), &predicate));
    }

    #[test]
    fn match_file_checks_inclusive_size_bounds() {
        let predicate = predicate("f", &[], 1_000, 5_000);

        for (size, expected) in [(999, false), (1_000, true), (5_000, true), (5_001, false)] {
            assert_eq!(
                match_file(&file("file.bin", "bin", size), &predicate),
                expected
            );
        }
    }

    #[test]
    fn empty_predicate_matches_every_file() {
        let predicate = Filters::default().predicate();

        assert!(predicate.is_empty_substr());
        assert!(match_file(&file("any/path", "", 0), &predicate));
    }

    #[test]
    fn file_extension_prefers_blob_value_and_falls_back_to_path() {
        assert_eq!(file_extension(&file("movie.avi", "MKV", 0)), "mkv");
        assert_eq!(file_extension(&file("Movie.2024.MKV", "", 0)), "mkv");
        assert_eq!(file_extension(&file("README", "", 0)), "");
    }

    #[test]
    fn matched_files_preserves_matching_input_order() {
        let files = vec![
            file("show/ep01.mkv", "mkv", 1),
            file("show/ep02.mkv", "mkv", 2),
            file("show/poster.jpg", "jpg", 3),
            file("show/special.avi", "avi", 4),
        ];
        let predicate = predicate("ep", &["mkv"], 0, 0);

        assert_eq!(matched_files(&files, &predicate), files[..2]);
    }

    #[test]
    fn torrent_matches_and_distinct_paths_deduplicate_in_first_seen_order() {
        let files = vec![
            file("a/Movie.mkv", "mkv", 1),
            file("b/movie.mkv", "mkv", 2),
            file("a/Movie.mkv", "mkv", 1),
            file("c/unrelated.txt", "txt", 3),
        ];
        let predicate = predicate("movie", &[], 0, 0);

        assert!(torrent_matches(&files, &predicate));
        assert_eq!(
            distinct_matched_paths(&files, &predicate),
            vec!["a/Movie.mkv".to_owned(), "b/movie.mkv".to_owned()]
        );
    }

    #[test]
    fn files_for_refine_decodes_non_empty_blob() {
        let expected = vec![file("Inception.2010.mkv", "mkv", 7)];
        let files_data = serialize_files(&expected).unwrap();
        let torrent = torrent(FilesStatus::Multi, "ignored", 99, Some(files_data));

        assert_eq!(files_for_refine(&torrent), Some(expected));
    }

    #[test]
    fn files_for_refine_builds_single_file_name_surrogate() {
        let torrent = torrent(FilesStatus::Single, "Inception.2010.1080p.mkv", 1_500, None);

        assert_eq!(
            files_for_refine(&torrent),
            Some(vec![file("Inception.2010.1080p.mkv", "", 1_500)])
        );
        assert_eq!(
            torrent_refine(&torrent, &predicate("inception", &["mkv"], 1_000, 0)),
            (true, true)
        );
        assert_eq!(
            torrent_refine(&torrent, &predicate("inception", &["avi"], 0, 0)),
            (false, true)
        );
    }

    #[test]
    fn files_for_refine_falls_through_decode_failure_and_empty_blob() {
        let corrupt_single = torrent(
            FilesStatus::Single,
            "fallback.mkv",
            42,
            Some(b"not-zstd".to_vec()),
        );
        assert_eq!(
            files_for_refine(&corrupt_single),
            Some(vec![file("fallback.mkv", "", 42)])
        );

        let empty_blob = serialize_files(&[]).unwrap();
        let empty_multi = torrent(FilesStatus::Multi, "multi", 42, Some(empty_blob));
        assert_eq!(files_for_refine(&empty_multi), None);
    }

    #[test]
    fn bounded_refine_enforces_owned_bytes_for_blob_and_single_surrogate() {
        let expected = vec![file("Inception.2010.mkv", "mkv", 7)];
        let owned_bytes = expected[0].owned_string_bytes();
        let files_data = serialize_files(&expected).unwrap();
        let multi = torrent(FilesStatus::Multi, "ignored", 99, Some(files_data));

        let decoded = files_for_refine_bounded(&multi, 4_096, owned_bytes, 1)
            .unwrap()
            .unwrap();
        assert_eq!(decoded.files, expected);
        assert!(matches!(
            files_for_refine_bounded(&multi, 4_096, owned_bytes - 1, 1),
            Err(BlobError::OwnedStringLimitExceeded { .. })
        ));

        let single_name = "single-name-is-bounded.mkv";
        let single = torrent(FilesStatus::Single, single_name, 42, None);
        assert!(matches!(
            files_for_refine_bounded(&single, 0, single_name.len() - 1, 1),
            Err(BlobError::OwnedStringLimitExceeded { .. })
        ));
    }

    #[test]
    fn multi_file_without_blob_is_fail_loud() {
        let torrent = torrent(FilesStatus::Multi, "multi", 42, None);

        assert_eq!(files_for_refine(&torrent), None);
        assert_eq!(
            torrent_refine(&torrent, &predicate("anything", &[], 0, 0)),
            (false, false)
        );
    }

    #[test]
    fn torrent_refine_drops_clean_non_match() {
        let files = vec![
            file("sample/readme.txt", "txt", 10),
            file("sample/Interstellar.mkv", "mkv", 100),
        ];
        let files_data = serialize_files(&files).unwrap();
        let torrent = torrent(FilesStatus::Multi, "sample", 110, Some(files_data));

        assert_eq!(
            torrent_refine(&torrent, &predicate("inception", &["mkv"], 0, 0)),
            (false, true)
        );
    }

    #[test]
    fn paginate_matches_go_page_window_semantics() {
        assert!(paginate(vec!["A", "B"], 5, 10).is_empty());
        assert_eq!(paginate(vec!["A", "B", "C", "D"], 1, 2), vec!["B", "C"]);
        assert_eq!(paginate(vec!["A", "B", "C"], 1, 0), vec!["B", "C"]);
        assert_eq!(paginate(vec!["A", "B", "C"], 1, 10), vec!["B", "C"]);
    }
}
