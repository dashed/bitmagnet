//! GraphQL-surface search inputs for the Phase-2 Lane S builder.
//!
//! These types mirror Go `query.SearchParams` / `query.ResolvedOptions` and the
//! facet and order wiring in `internal/gql/gqlmodel/torrent_content.go`. Lane G
//! resolves page arithmetic before constructing [`SearchOptions`]; Lane S
//! S2-S5 lower this contract to SQL and result aggregation.

use crate::aggregations::FacetLogic;
use crate::criteria::Criteria;
use crate::order::TorrentContentOrder;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// A resolved GraphQL torrent-content search request.
///
/// Mirrors Go `query.SearchParams` (`internal/database/query/params.go`),
/// `query.ResolvedOptions` (`resolve.go`), and the facet/order wiring in
/// `internal/gql/gqlmodel/torrent_content.go`. Lane S S2-S5 use these fields to
/// build the FTS/filter SQL, ordered membership query, count query, and facet
/// aggregation queries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchOptions {
    /// Free-text app-query (Go `SearchParams.QueryString`). `None` or `""`
    /// means no FTS predicate and no relevance order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Predicate tree lowered to SQL by Lane S S2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Criteria>,
    /// Ordered clauses applied in order by Lane S S4. Empty selects Go
    /// `TorrentContentDefaultOption`'s single-column
    /// `torrent_contents.published_at DESC` browse order.
    pub order: Vec<TorrentContentOrder>,
    /// Facet aggregations and selected-value filters lowered by Lane S S3.
    pub facets: Vec<FacetRequest>,
    /// Resolved row limit. `None` means no SQL `LIMIT`; `Some(0)` is an
    /// explicit zero limit, matching Go `query.ResolvedOptions.Limit`.
    pub limit: Option<u32>,
    /// Resolved row offset. Lane G performs Go `searchPageOffset`'s
    /// page-to-offset arithmetic before constructing this value.
    pub offset: u32,
    /// Whether Lane S S5 runs the budgeted count and fills total-count fields.
    pub total_count: bool,
    /// Whether Lane S S4 over-fetches `limit + 1` and reports a next page.
    pub has_next_page: bool,
    /// Budget for Go-style `BudgetedCount` queries; Go's default is `5000.0`.
    pub aggregation_budget: f64,
}

impl SearchOptions {
    /// Construct Go `query.DefaultOption()`'s GraphQL search defaults: no
    /// query/filter/order/facets, `LIMIT 10`, offset zero, count and next-page
    /// disabled, and an aggregation budget of `5000.0`.
    pub fn new() -> Self {
        Self {
            query: None,
            filter: None,
            order: Vec::new(),
            facets: Vec::new(),
            limit: Some(10),
            offset: 0,
            total_count: false,
            has_next_page: false,
            aggregation_budget: 5000.0,
        }
    }

    /// Set Go `SearchParams.QueryString`; Lane S S2 emits FTS SQL only for a
    /// non-empty value.
    pub fn with_query(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }

    /// Set the predicate tree that Lane S S2 lowers to `WHERE` SQL.
    pub fn with_filter(mut self, filter: Criteria) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Replace the ordered clauses that Lane S S4 lowers to `ORDER BY` SQL.
    pub fn with_order(mut self, order: impl IntoIterator<Item = TorrentContentOrder>) -> Self {
        self.order = order.into_iter().collect();
        self
    }

    /// Replace the Go `q.WithFacet` requests that Lane S S3 aggregates.
    pub fn with_facets(mut self, facets: impl IntoIterator<Item = FacetRequest>) -> Self {
        self.facets = facets.into_iter().collect();
        self
    }

    /// Set the resolved SQL limit (`None` means no limit; `Some(0)` is
    /// explicit zero), matching Go `query.ResolvedOptions.Limit`.
    pub fn with_limit(mut self, limit: impl Into<Option<u32>>) -> Self {
        self.limit = limit.into();
        self
    }

    /// Set the already-resolved SQL offset from Go `searchPageOffset`.
    pub const fn with_offset(mut self, offset: u32) -> Self {
        self.offset = offset;
        self
    }

    /// Set Go `WithTotalCount`; Lane S S5 emits the budgeted count SQL.
    pub const fn with_total_count(mut self, enabled: bool) -> Self {
        self.total_count = enabled;
        self
    }

    /// Set Go `WithHasNextPage`; Lane S S4 applies the `limit + 1` SQL window.
    pub const fn with_has_next_page(mut self, enabled: bool) -> Self {
        self.has_next_page = enabled;
        self
    }

    /// Set Go `WithAggregationBudget`, used by Lane S S3/S5 budgeted counts.
    pub const fn with_aggregation_budget(mut self, budget: f64) -> Self {
        self.aggregation_budget = budget;
        self
    }
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// One requested facet aggregation.
///
/// Mirrors gqlmodel `torrentContentFacetsOption` in
/// `internal/gql/gqlmodel/torrent_content.go` plus `q.NewFacetConfig` in
/// `internal/database/query/facets.go`. Lane S S3 turns it into selected-value
/// filter predicates and count-per-value aggregation SQL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FacetRequest {
    /// The facet key and builder selected for Lane S S3.
    pub facet: TorrentContentFacet,
    /// Go `FacetIsAggregated`: compute count-per-value SQL for this facet.
    pub aggregate: bool,
    /// Override the facet's default Go `FacetLogic`; `None` keeps that default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logic: Option<FacetLogic>,
    /// Selected Go `FacetFilter` values. The literal `"null"` selects the
    /// facet's `IS NULL` SQL bucket.
    pub filter: BTreeSet<String>,
}

/// Torrent-content facet keys used by the GraphQL input and aggregation map.
///
/// Values mirror each `Key()` in `internal/database/search/facet_*.go` and the
/// map lookups in `internal/gql/gqlmodel/facet.go`. The current resolver wires
/// nine aggregation outputs: content type, torrent source, torrent tag, file
/// type, language, content genre, release year, video resolution, and video
/// source. Video 3D, codec, and modifier have Go facet builders and remain part
/// of the full contract for Lane S S3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentContentFacet {
    /// `content_type`, backed by `facet_torrent_content_type.go`.
    ContentType,
    /// `torrent_source`, backed by `facet_torrent_source.go`.
    TorrentSource,
    /// `torrent_tag`, backed by `facet_torrent_tag.go`.
    TorrentTag,
    /// `file_type`, backed by `facet_torrent_file_type.go`.
    FileType,
    /// `language`, backed by `facet_torrent_content_language.go`.
    Language,
    /// `content_genre`, backed by `facet_torrent_content_genre.go`.
    ContentGenre,
    /// `release_year`, backed by `facet_release_year.go`.
    ReleaseYear,
    /// `video_resolution`, backed by
    /// `facet_torrent_content_video_resolution.go`.
    VideoResolution,
    /// `video_source`, backed by `facet_torrent_content_video_source.go`.
    VideoSource,
    /// `video_3d`, backed by `facet_torrent_content_video_3d.go`.
    #[serde(rename = "video_3d")]
    Video3D,
    /// `video_codec`, backed by `facet_torrent_content_video_codec.go`.
    VideoCodec,
    /// `video_modifier`, backed by
    /// `facet_torrent_content_video_modifier.go`.
    VideoModifier,
}

impl TorrentContentFacet {
    /// Return the GraphQL facet key and Go aggregation-map key used by Lane S
    /// S3's count-per-value SQL.
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
            Self::Video3D => "video_3d",
            Self::VideoCodec => "video_codec",
            Self::VideoModifier => "video_modifier",
        }
    }
}

/// Explicit server-side flags that alter the Phase-2 SQL builder.
///
/// Mirrors Go `search.FeatureFlagsValue()`. Lane G passes this value explicitly
/// so Lane S remains pure and the Torznab [`crate::build_query`] path never
/// receives GraphQL-only FIND-2 behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct SearchBuildConfig {
    /// Effective Go `GateFileExtensionsJSONB` (including
    /// `DropCompatibleReads`): Lane S S2 uses
    /// `torrents.file_extensions @> ...::jsonb` rather than the legacy
    /// `EXISTS (torrent_files ...)` branch.
    pub file_extensions_jsonb: bool,
    /// Go FIND-2 `PopularitySortDefault` (default off): Lane S S4 rewrites a
    /// lone relevance order plus query string to `seeders DESC`. This is
    /// GraphQL-only and never applies to Torznab.
    pub popularity_sort_default: bool,
}
