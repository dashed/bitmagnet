//! Isolated Lane-S/Lane-C integration seam for GraphQL read resolvers.
//!
//! The Phase-2 lane branches are intentionally kept disjoint. This module is
//! therefore a small, structurally equivalent copy of the published
//! `SearchOptions`/`SearchResult`/`SearchServe` surface. The integration branch
//! replaces only this seam with adapters to `bitmagnet-search-query` and
//! `bitmagnet-search-serve`; routing and GraphQL transformation code does not
//! change.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bitmagnet_model::{BlobFile, Content, ContentType, InfoHash, Torrent, TorrentContent};
use bitmagnet_proto::v1::SortBy;

/// Errors crossing the search integration seam.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A runtime dependency has not been composed yet.
    #[error("{0} is not configured")]
    NotConfigured(&'static str),
    /// A backend rejected a request.
    #[error("{0}")]
    Backend(String),
}

/// Result alias for search runtime operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Feature switches evaluated by the resolver layer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchFeatures {
    /// Enable the L2 file-search read surface.
    pub file_search_enabled: bool,
    /// Enable L2 file facet aggregation.
    pub file_search_facets_enabled: bool,
    /// Prefer the L3 prefix-index Suggest RPC before typeahead fallbacks.
    pub file_search_typeahead_rpc_enabled: bool,
}

/// Explicit Lane-S builder flags.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchBuildConfig {
    /// Use the JSONB file-extension predicate.
    pub file_extensions_jsonb: bool,
    /// Apply FIND-2's lone-relevance to seeders rewrite.
    pub popularity_sort_default: bool,
}

/// Search order direction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OrderDirection {
    /// Ascending order.
    Ascending,
    /// Descending order.
    #[default]
    Descending,
}

/// Full torrent-content order field set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentContentOrderField {
    /// Full-text rank.
    Relevance,
    /// Publication timestamp.
    PublishedAt,
    /// Update timestamp.
    UpdatedAt,
    /// Torrent size.
    Size,
    /// File count.
    FilesCount,
    /// Seeder count.
    Seeders,
    /// Leecher count.
    Leechers,
    /// Torrent name.
    Name,
    /// Canonical info hash.
    InfoHash,
}

/// One resolved order clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TorrentContentOrder {
    /// Ordered field.
    pub field: TorrentContentOrderField,
    /// Ordered direction.
    pub direction: OrderDirection,
}

/// Facet filter logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FacetLogic {
    /// All values must match.
    And,
    /// Any value may match.
    Or,
}

/// Torrent-content facet key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentContentFacet {
    /// Content type.
    ContentType,
    /// Torrent source.
    TorrentSource,
    /// Torrent tag.
    TorrentTag,
    /// Torrent file type.
    FileType,
    /// Language.
    Language,
    /// Content genre.
    ContentGenre,
    /// Release year.
    ReleaseYear,
    /// Video resolution.
    VideoResolution,
    /// Video source.
    VideoSource,
}

impl TorrentContentFacet {
    /// Stable key shared with Lane S aggregation maps.
    pub const fn key(self) -> &'static str {
        match self {
            Self::ContentType => "content_type",
            Self::TorrentSource => "torrent_source",
            Self::TorrentTag => "torrent_tag",
            Self::FileType => "file_type",
            Self::Language => "language",
            Self::ContentGenre => "content_genre",
            Self::ReleaseYear => "release_year",
            Self::VideoResolution => "video_resolution",
            Self::VideoSource => "video_source",
        }
    }
}

/// One facet request, including aggregation and selected-value filtering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetRequest {
    /// Requested facet.
    pub facet: TorrentContentFacet,
    /// Whether to compute buckets.
    pub aggregate: bool,
    /// Optional non-default filter logic.
    pub logic: Option<FacetLogic>,
    /// Selected values; `"null"` selects the null bucket.
    pub filter: BTreeSet<String>,
}

/// GraphQL-produced criteria outside facet filters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Criteria {
    /// Conjunction of criteria.
    And(Vec<Criteria>),
    /// Inclusive torrent-size range.
    SizeRange {
        /// Inclusive minimum size.
        min: Option<i64>,
        /// Inclusive maximum size.
        max: Option<i64>,
    },
    /// Published-at timeframe expression.
    PublishedAt(String),
    /// Canonical info-hash restriction.
    TorrentContentInfoHashIn(Vec<InfoHash>),
}

/// A resolved Lane-S search request.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchOptions {
    /// Free-text app query.
    pub query: Option<String>,
    /// Structured non-facet filter.
    pub filter: Option<Criteria>,
    /// Ordered clauses.
    pub order: Vec<TorrentContentOrder>,
    /// Facet aggregation/filter requests.
    pub facets: Vec<FacetRequest>,
    /// Row limit; `None` means no SQL limit.
    pub limit: Option<u32>,
    /// Row offset.
    pub offset: u32,
    /// Whether to compute total count.
    pub total_count: bool,
    /// Whether to over-fetch for a next-page signal.
    pub has_next_page: bool,
    /// Budget for count/facet aggregation.
    pub aggregation_budget: f64,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            query: None,
            filter: None,
            order: Vec::new(),
            facets: Vec::new(),
            limit: Some(10),
            offset: 0,
            total_count: false,
            has_next_page: false,
            aggregation_budget: 5_000.0,
        }
    }
}

/// Eager hydration requested from Lane S.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HydrateOptions {
    /// Hydrate the nested torrent.
    pub torrent: bool,
    /// Hydrate content metadata.
    pub content: bool,
    /// Select the compressed file blob for exact refine.
    pub files_data: bool,
}

/// Search request plus explicit builder and hydration options.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchRequest {
    /// Lane-S query options.
    pub options: SearchOptions,
    /// Lane-S builder flags.
    pub build: SearchBuildConfig,
    /// Eager hydration selection.
    pub hydrate: HydrateOptions,
}

/// The three Lane-C composer query shapes.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryOptions {
    /// Single-chunk hydrate plus facets.
    pub combined: SearchRequest,
    /// Multi-chunk hydrate without aggregation.
    pub refine: Option<SearchRequest>,
    /// Decode-free aggregation over the refined set.
    pub agg: SearchRequest,
    /// Retain decoded file rows only when the GraphQL projection selects them.
    pub retain_refine_files: bool,
}

/// Typed exact-refine inputs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filters {
    /// Raw path text.
    pub query: String,
    /// Allowed extensions.
    pub extensions: Vec<String>,
    /// Inclusive minimum file size; zero is unset.
    pub min_size: u64,
    /// Inclusive maximum file size; zero is unset.
    pub max_size: u64,
}

/// One facet bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregationItem {
    /// Display label.
    pub label: String,
    /// Count for this value.
    pub count: u64,
    /// Whether the count is estimated.
    pub is_estimate: bool,
}

/// One facet aggregation group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregationGroup {
    /// Human-readable group label.
    pub label: String,
    /// Facet logic.
    pub logic: FacetLogic,
    /// Stable value to aggregation item map.
    pub items: BTreeMap<String, AggregationItem>,
}

/// All returned facet groups, keyed by [`TorrentContentFacet::key`].
pub type Aggregations = BTreeMap<String, AggregationGroup>;

/// Season to episode mapping from the search result.
pub type Episodes = BTreeMap<i32, Vec<i32>>;

/// One hydrated torrent-source association.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentSourceInfo {
    /// Source key.
    pub key: String,
    /// Source display name.
    pub name: String,
    /// Optional import id.
    pub import_id: Option<String>,
    /// Source-specific seeders.
    pub seeders: Option<u32>,
    /// Source-specific leechers.
    pub leechers: Option<u32>,
    /// Number of observations.
    pub seen_count: u32,
    /// First observation epoch seconds.
    pub first_seen_at: i64,
    /// Last observation epoch seconds.
    pub last_seen_at: i64,
}

/// One expanded Lane-S result item.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResultItem {
    /// Canonical info hash.
    pub info_hash: InfoHash,
    /// Torrent name.
    pub name: String,
    /// Torrent size.
    pub size: u64,
    /// Classified content type.
    pub content_type: Option<ContentType>,
    /// Publication epoch seconds.
    pub published_at: i64,
    /// Maximum seeders.
    pub seeders: Option<u32>,
    /// Maximum leechers.
    pub leechers: Option<u32>,
    /// Number of files.
    pub files_count: Option<u32>,
    /// Video resolution database value.
    pub video_resolution: Option<String>,
    /// Video 3D database value.
    pub video_3d: Option<String>,
    /// Video codec database value.
    pub video_codec: Option<String>,
    /// Release group.
    pub release_group: Option<String>,
    /// Season/episode map.
    pub episodes: Episodes,
    /// Content release year.
    pub release_year: Option<i32>,
    /// IMDb external identifier hydrated from content metadata.
    pub imdb_id: Option<String>,
    /// TMDB external identifier hydrated from content metadata.
    pub tmdb_id: Option<String>,
    /// v1 SHA-1 identity.
    pub info_hash_v1: Option<[u8; 20]>,
    /// v2 SHA-256 identity.
    pub info_hash_v2: Option<[u8; 32]>,
    /// Scalar torrent-content row.
    pub torrent_content: TorrentContent,
    /// Supplemental video modifier database value.
    pub torrent_content_video_modifier: Option<String>,
    /// Torrent-content creation epoch seconds.
    pub torrent_content_created_at: i64,
    /// Torrent-content update epoch seconds.
    pub torrent_content_updated_at: i64,
    /// Hydrated torrent row.
    pub torrent: Torrent,
    /// Decoded files retained only by exact refine.
    pub refine_files: Vec<BlobFile>,
    /// Torrent creation epoch seconds.
    pub torrent_created_at: i64,
    /// Torrent update epoch seconds.
    pub torrent_updated_at: i64,
    /// BEP meta version.
    pub torrent_meta_version: Option<u16>,
    /// Hydrated source associations.
    pub torrent_sources: Vec<TorrentSourceInfo>,
    /// Hydrated tag names.
    pub torrent_tags: Vec<String>,
    /// Hydrated content metadata.
    pub content: Option<Content>,
    /// Go-compatible derived display title.
    pub title: String,
    /// DHT observation count.
    pub dht_seen_count: i32,
    /// First DHT observation epoch seconds.
    pub dht_first_seen_at: Option<i64>,
    /// Last DHT observation epoch seconds.
    pub dht_last_seen_at: Option<i64>,
    /// PostgreSQL text-search rank, or zero for browse queries.
    pub query_string_rank: f64,
}

/// Hydrated search result.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchResult {
    /// Total matching rows.
    pub total_count: u64,
    /// Whether total count is estimated.
    pub total_count_is_estimate: bool,
    /// Whether another page exists.
    pub has_next_page: bool,
    /// Ordered hydrated items.
    pub items: Vec<SearchResultItem>,
    /// Facet aggregations.
    pub aggregations: Aggregations,
}

/// One collapsed path group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathGroup {
    /// Exact path.
    pub path: String,
    /// Torrents containing the path.
    pub info_hashes: Vec<InfoHash>,
}

/// One file-row order clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRowSort {
    /// File or torrent field.
    pub field: String,
    /// Descending order.
    pub descending: bool,
}

/// One exact-refined file row.
#[derive(Debug, Clone, PartialEq)]
pub struct FileRow {
    /// Parent torrent hash.
    pub info_hash: InfoHash,
    /// File index.
    pub index: u32,
    /// File path.
    pub path: String,
    /// Lowercase extension.
    pub extension: String,
    /// File size.
    pub size: u64,
    /// Eagerly hydrated torrent content.
    pub torrent_content: SearchResultItem,
}

/// File-search result from either L3 refine or L2.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FileRowsResult {
    /// Ordered file rows.
    pub rows: Vec<FileRow>,
    /// Exact or candidate-derived total count.
    pub total_count: u64,
    /// Whether the count is estimated.
    pub total_count_is_estimate: bool,
    /// Whether another page exists.
    pub has_next_page: bool,
}

/// Validated file-search request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileSearchRequest {
    /// Normalized text query.
    pub query: String,
    /// LIKE-escaped query for L2.
    pub query_like_pattern: String,
    /// Normalized extensions.
    pub extensions: Vec<String>,
    /// Minimum file size.
    pub min_size: u64,
    /// Maximum file size.
    pub max_size: u64,
    /// Optional single-torrent scope.
    pub info_hash: Option<InfoHash>,
    /// Requested ordering.
    pub sort: Vec<FileRowSort>,
    /// Clamped page size.
    pub limit: u32,
    /// Row offset.
    pub offset: u32,
    /// Skip the exact L2 count.
    pub skip_total_count: bool,
}

/// Validated file facet request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileFacetsRequest {
    /// Normalized query.
    pub query: String,
    /// LIKE-escaped query.
    pub query_like_pattern: String,
    /// Normalized extensions.
    pub extensions: Vec<String>,
    /// Minimum size.
    pub min_size: u64,
    /// Maximum size.
    pub max_size: u64,
    /// Known facet fields.
    pub fields: Vec<String>,
}

/// One L2 file facet bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileFacetBucket {
    /// Bucket value.
    pub value: String,
    /// Exact file count.
    pub count: u64,
    /// Summed file size.
    pub total_size: u64,
}

/// One L2 file facet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileFacet {
    /// Facet field.
    pub field: String,
    /// Buckets.
    pub buckets: Vec<FileFacetBucket>,
}

/// L2 file facet result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileFacetsResult {
    /// Facets in backend order.
    pub facets: Vec<FileFacet>,
}

/// Validated L2 path-typeahead request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilePathTypeaheadRequest {
    /// Normalized prefix.
    pub prefix: String,
    /// LIKE-escaped prefix.
    pub prefix_like_pattern: String,
    /// Clamped limit.
    pub limit: u32,
}

/// Resolver-callable runtime contract.
#[async_trait::async_trait]
pub trait SearchRuntime: Send + Sync {
    /// Execute the authoritative plain PostgreSQL search path.
    async fn pg_torrent_content(&self, _request: SearchRequest) -> Result<SearchResult> {
        Err(Error::NotConfigured("PostgreSQL torrent-content search"))
    }

    /// Attempt Lane-C L3/L1 composition.
    async fn torrent_content(
        &self,
        _filters: Filters,
        _options: QueryOptions,
        _limit: u32,
        _offset: u32,
        _sorts: Vec<SortBy>,
    ) -> Result<(SearchResult, bool)> {
        Ok((SearchResult::default(), false))
    }

    /// Attempt collapsed-path composition.
    async fn collapse_paths(
        &self,
        _filters: Filters,
        _options: QueryOptions,
        _limit: u32,
        _offset: u32,
        _sorts: Vec<SortBy>,
    ) -> Result<(Vec<PathGroup>, bool)> {
        Ok((Vec::new(), false))
    }

    /// Attempt routed exact-refined file rows.
    async fn search_file_rows(
        &self,
        _filters: Filters,
        _options: QueryOptions,
        _limit: u32,
        _offset: u32,
        _sort_by: Vec<FileRowSort>,
    ) -> Result<(FileRowsResult, bool)> {
        Ok((FileRowsResult::default(), false))
    }

    /// Attempt candidate-derived path typeahead.
    async fn path_typeahead(
        &self,
        _prefix: String,
        _options: QueryOptions,
        _limit: u32,
    ) -> Result<(Vec<String>, bool)> {
        Ok((Vec::new(), false))
    }

    /// Attempt the L3 prefix-index Suggest RPC.
    async fn suggest(&self, _prefix: String, _limit: u32) -> Result<(Vec<String>, bool)> {
        Ok((Vec::new(), false))
    }

    /// Execute the L2 file-search fallback.
    async fn file_search(&self, _request: FileSearchRequest) -> Result<FileRowsResult> {
        Err(Error::NotConfigured("L2 file search"))
    }

    /// Execute the L2 file-facet fallback.
    async fn file_search_facets(&self, _request: FileFacetsRequest) -> Result<FileFacetsResult> {
        Err(Error::NotConfigured("L2 file facets"))
    }

    /// Execute the L2 path-typeahead fallback.
    async fn file_path_typeahead(&self, _request: FilePathTypeaheadRequest) -> Result<Vec<String>> {
        Err(Error::NotConfigured("L2 path typeahead"))
    }

    /// Broad-gram eligibility gate.
    fn eligible(&self, _query: &str) -> bool {
        false
    }

    /// Cached L3 health gate.
    fn healthy(&self) -> bool {
        false
    }

    /// Path-typeahead route switch.
    fn typeahead_enabled(&self) -> bool {
        false
    }

    /// File-search text route switch.
    fn file_search_route_text_enabled(&self) -> bool {
        false
    }

    /// Collapse-path route switch.
    fn collapse_enabled(&self) -> bool {
        false
    }

    /// Resolver-owned feature switches.
    fn features(&self) -> SearchFeatures {
        SearchFeatures::default()
    }

    /// Lane-S builder switches.
    fn search_build_config(&self) -> SearchBuildConfig {
        SearchBuildConfig::default()
    }
}

#[derive(Debug, Default)]
struct DisabledSearchRuntime;

#[async_trait::async_trait]
impl SearchRuntime for DisabledSearchRuntime {}

/// Cloneable async-graphql data wrapper around the runtime trait object.
#[derive(Clone)]
pub struct SearchRuntimeData(Arc<dyn SearchRuntime>);

impl SearchRuntimeData {
    /// Wrap a composed search runtime.
    #[must_use]
    pub fn new(runtime: Arc<dyn SearchRuntime>) -> Self {
        Self(runtime)
    }

    /// Safe dark default used by schemas without the final composition root.
    #[must_use]
    pub fn disabled() -> Self {
        Self(Arc::new(DisabledSearchRuntime))
    }

    /// Borrow the resolver-callable runtime.
    #[must_use]
    pub fn runtime(&self) -> &(dyn SearchRuntime + 'static) {
        self.0.as_ref()
    }

    /// Clone the shared runtime handle for async resolver use.
    #[must_use]
    pub fn shared(&self) -> Arc<dyn SearchRuntime> {
        Arc::clone(&self.0)
    }
}
