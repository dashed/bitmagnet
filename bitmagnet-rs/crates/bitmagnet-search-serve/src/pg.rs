//! STUB. These types are Lane S's (`bitmagnet-search-query`) S1 contract; re-point to the real crate when S1 lands (feature `lane-s-stub` default-on until then).
//!
//! C1 intentionally does not depend on `bitmagnet-search-query` while Lane S is
//! in flight. The composer treats the richer query criteria, order, paging, and
//! nine-facet details as opaque.

use std::collections::{BTreeMap, HashMap};

/// Opaque facet aggregations returned by Lane S's PostgreSQL query layer.
#[derive(Debug, Clone, Default)]
pub struct Aggregations(pub BTreeMap<String, Vec<AggregationItem>>);

impl Aggregations {
    /// Returns an aggregation set containing no facet buckets.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }
}

/// One value bucket in a PostgreSQL facet aggregation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregationItem {
    /// Stable machine-readable facet value.
    pub value: String,
    /// Optional resolver-facing display label.
    pub label: Option<String>,
    /// Number of matching rows in this bucket.
    pub count: u64,
}

/// One hydrated torrent-content result row from Lane S.
#[derive(Debug, Clone)]
pub struct TorrentContentResultItem {
    /// Info hash used by the composer to restrict and preserve candidate order.
    pub info_hash: bitmagnet_model::InfoHash,
    /// Hydrated torrent, including the `files_data` blob when requested.
    pub torrent: bitmagnet_model::Torrent,
    /// Decoded files retained only for an exact-refined match.
    ///
    /// The temporary adapter initializes this empty. The composer decodes the
    /// blob once, clears `torrent.files_data`, and moves the decoded files here
    /// after the match and retained-budget checks pass. The real Lane-S adapter
    /// must preserve this seam, and Lane G's GraphQL mapper consumes it for the
    /// public `Torrent.files` field.
    pub refine_files: Vec<bitmagnet_model::BlobFile>,
    // Passthrough content fields Lane S hydrates are opaque to the composer.
}

/// Hydrated PostgreSQL torrent-content search result.
#[derive(Debug, Clone, Default)]
pub struct TorrentContentResult {
    /// Search result rows in backend order.
    pub items: Vec<TorrentContentResultItem>,
    /// Exact or estimated number of matching rows.
    pub total_count: u64,
    /// Whether [`Self::total_count`] is an estimate.
    pub total_count_is_estimate: bool,
    /// Whether another page may exist after this result.
    pub has_next_page: bool,
    /// Facet buckets computed for the search.
    pub aggregations: Aggregations,
}

/// Opaque Lane S option set that the composer may restrict to candidate hashes.
#[derive(Debug, Clone, Default)]
pub struct SearchOptions {
    /// Info-hash restriction appended by the composer as a candidate `IN` list.
    pub info_hash_in: Vec<bitmagnet_model::InfoHash>,
    /// Whether the torrent `files_data` blob hydrator is selected.
    pub hydrate_files: bool,
    /// Whether facet aggregations are computed.
    pub with_facets: bool,
    // Real query criteria, ordering, and paging are opaque Lane S fields.
}

impl SearchOptions {
    /// Returns a cloned option set restricted to the supplied candidate hashes.
    #[must_use]
    pub fn with_info_hash_in(&self, ids: &[bitmagnet_model::InfoHash]) -> Self {
        let mut options = self.clone();
        options.info_hash_in = ids.to_vec();
        options
    }
}

/// PostgreSQL option sets built by the GraphQL layer for composer refine paths.
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Blob hydrator plus facets for the common single-chunk fast path.
    pub combined: SearchOptions,
    /// Blob hydrator without facets for each multi-chunk refine query.
    pub refine: Option<SearchOptions>,
    /// Facets without blob hydration for decode-free refined-set re-aggregation.
    pub agg: SearchOptions,
}

impl QueryOptions {
    /// Returns per-chunk decode options, falling back to combined when omitted.
    #[must_use]
    pub fn refine_options(&self) -> &SearchOptions {
        self.refine.as_ref().unwrap_or(&self.combined)
    }
}

/// PostgreSQL dependency required by the L3/L1 composer.
///
/// This is the deliberately narrow real-Lane-S adapter seam. Its implementation
/// owns conversion between Lane S's eventual full `SearchOptions`/`SearchResult`
/// types and this crate; the composer never introspects criteria, ordering, or
/// facet definitions. `file_counts` remains a Lane-C operation and must mirror
/// Go's blob-free two-step lookup: `torrent_file_summary.file_count` by its
/// `info_hash` primary key, then `torrents.files_count` for missing summaries.
/// The result adapter initializes `TorrentContentResultItem::refine_files`
/// empty; after composition it maps that field to Lane G's `Torrent.files`.
#[async_trait::async_trait]
pub trait PgSearchBackend: Send + Sync {
    /// Hydrates and filters candidate rows using the supplied Lane S options.
    /// The adapter must preserve all non-page filters and must not add a page
    /// window; the composer paginates only after exact refine.
    async fn torrent_content(&self, options: SearchOptions) -> crate::Result<TorrentContentResult>;

    /// Reads authoritative per-torrent file counts without decoding file blobs.
    ///
    /// This is gate7-4's cheap pre-decode probe against
    /// `torrent_file_summary`, with `torrents.files_count` as the blob-free
    /// fallback; an ID missing from both sources is absent from the map.
    async fn file_counts(
        &self,
        ids: &[bitmagnet_model::InfoHash],
    ) -> crate::Result<HashMap<bitmagnet_model::InfoHash, i64>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info_hash(byte: u8) -> bitmagnet_model::InfoHash {
        bitmagnet_model::InfoHash::new([byte; bitmagnet_model::INFO_HASH_LEN])
    }

    #[test]
    fn stub_defaults_and_candidate_restriction_are_stable() {
        let result = TorrentContentResult::default();
        assert!(result.items.is_empty());
        assert!(result.aggregations.0.is_empty());
        assert_eq!(result.total_count, 0);
        assert!(!result.total_count_is_estimate);
        assert!(!result.has_next_page);

        let hash = info_hash(7);
        let restricted = SearchOptions::default().with_info_hash_in(&[hash]);
        assert_eq!(restricted.info_hash_in, vec![hash]);

        let options = QueryOptions::default();
        assert!(std::ptr::eq(options.refine_options(), &options.combined));
    }
}
