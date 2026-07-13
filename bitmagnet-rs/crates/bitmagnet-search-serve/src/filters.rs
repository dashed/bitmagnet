//! Shared exact-refine inputs and resolver-facing result rows.

/// Typed exact-refine inputs extracted from a resolver's search request.
///
/// The exact predicate derived from these inputs is implemented in
/// [`crate::refine`], mirroring Go's `pathsearch.Filters.predicate` in
/// `internal/search/pathsearch/refine.go`.
#[derive(Debug, Clone, Default)]
pub struct Filters {
    /// Raw path substring or free text; required for the L3 route.
    pub query: String,
    /// Allowed extensions after file-type expansion; empty accepts any extension.
    pub extensions: Vec<String>,
    /// Minimum file size in bytes; zero is unbounded.
    pub min_size: u64,
    /// Maximum file size in bytes; zero is unbounded.
    pub max_size: u64,
}

/// One collapsed distinct path and the candidate torrents containing that path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathGroup {
    /// Exact matching path shared by the group.
    pub path: String,
    /// Torrents containing a matching file at this path.
    pub info_hashes: Vec<bitmagnet_model::InfoHash>,
}

/// A resolver-requested file-row ordering from Go's `pathsearch.FileRowSort`.
#[derive(Debug, Clone)]
pub struct FileRowSort {
    /// File or hydrated torrent-content field to order by.
    pub field: String,
    /// Whether the selected field is ordered descending.
    pub descending: bool,
}

/// One exact-refined matching file row.
#[derive(Debug, Clone)]
pub struct FileRow {
    /// Torrent info hash containing the file.
    pub info_hash: bitmagnet_model::InfoHash,
    /// Zero-based index of the file within the torrent.
    pub index: u32,
    /// File path relative to the torrent root.
    pub path: String,
    /// Lowercased file extension without a leading dot.
    pub extension: String,
    /// File size in bytes.
    pub size: u64,
    /// Hydrated torrent-content row associated with this file.
    pub torrent_content: crate::pg::TorrentContentResultItem,
}

/// File-search-shaped result produced by the future pathsearch composer.
#[derive(Debug, Clone, Default)]
pub struct FileRowsResult {
    /// Exact-refined matching file rows for the requested page.
    pub rows: Vec<FileRow>,
    /// Candidate-derived count or upper-bound estimate.
    pub total_count: u64,
    /// Whether [`Self::total_count`] is an estimate rather than an exact count.
    pub total_count_is_estimate: bool,
    /// Whether another page may exist after this result.
    pub has_next_page: bool,
}
