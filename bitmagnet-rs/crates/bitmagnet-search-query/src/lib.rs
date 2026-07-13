//! Phase-1 `search-query`: the subset of the Go PG search query builder that
//! Torznab exercises, ported against sqlx (roadmap 05 §Phase 1; reused by
//! Phase 2). Semantics parity with `internal/database/search` is gated by the
//! Phase-0 differential harness — every predicate added here needs a fixture
//! pair.
//!
//! Phase 2 expands the public contract to the full GraphQL torrent-content
//! search surface through [`SearchOptions`], the complete [`Criteria`] and
//! ordering enums, facet aggregation types, and [`SearchResult`].
//! [`build_search_query`] constructs the lean GraphQL membership statement;
//! [`search`] executes membership, hydration, optional count, pagination, and
//! facet aggregation. The frozen Torznab entry point remains [`build_query`].
//! The binding SQL contract is documented in `CONTRACT.md` §Phase-2.
//!
//! Lane contract (phase1-tasks.md): this crate owns query construction ONLY —
//! no HTTP, no XML, no Torznab category logic (that is bitmagnet-torznab).
//!
//! # v1 contract (Q1)
//!
//! Lane T (`bitmagnet-torznab`) parses an HTTP Torznab request, applies all
//! `t=`/`cat=` category logic, and reduces it to a [`TorznabSearchParams`]: a
//! free-text [`query`](TorznabSearchParams::query), a
//! [`filter`](TorznabSearchParams::filter) predicate tree built from the
//! [`Criteria`] constructors, an [`order`](TorznabSearchParams::order), and a
//! `limit`/`offset` window. It then calls [`build_query`] to get a
//! [`SearchQuery`] and runs [`SearchQuery::fetch`] (rows for XML) or
//! [`SearchQuery::fetch_info_hashes`] (the parity key list).
//!
//! ## Predicate / order / limit subset Torznab exercises
//!
//! Traced from `internal/torznab/adapter/{adapter,search_options}.go` down into
//! `internal/database/{search,query}` (full detail + SQL in `CONTRACT.md`):
//!
//! | Torznab input | [`Criteria`] leaf / field | Go source |
//! |---|---|---|
//! | `t=movie/tvsearch/music/book` | [`Criteria::ContentTypeIn`] | `TorrentContentTypeCriteria` |
//! | `cat=` resolution buckets (SD/HD/UHD) | [`Criteria::VideoResolutionIn`] | `VideoResolutionCriteria` |
//! | `cat=` 3D bucket | [`Criteria::Video3DIn`] | `Video3DCriteria` |
//! | `season`/`ep` | [`Criteria::Episodes`] | `TorrentContentEpisodesCriteria` |
//! | `imdbid=` | [`Criteria::AlternativeIdentifier`] | `ContentAlternativeIdentifierCriteria` |
//! | `tmdbid=` | [`Criteria::CanonicalIdentifier`] | `ContentCanonicalIdentifierCriteria` |
//! | profile tags | [`Criteria::TorrentTag`] | `TorrentTagCriteria` |
//! | `q=` | [`TorznabSearchParams::query`] | `query.SearchString` -> `tsv @@ ::tsquery` |
//! | order | [`TorrentContentOrder`] | `TorrentContentOrderBy.Clauses` |
//! | `limit`/`offset` | fields | `query.Limit` / `query.Offset` |
//!
//! `cat=`/`t=` combine as an OR-of-ANDs disjunction (Lane T composes it with
//! [`Criteria::or`]/[`Criteria::and`]); Torznab disables total-count and
//! has-next-page, so the page is a plain `LIMIT`/`OFFSET`.
//!
//! ## Not in v1 (Torznab never reaches these)
//!
//! Facets/aggregations, the `queue_jobs`/`torrent_files`/`content` search
//! entry points, orderings other than relevance & published_at, `WithTotalCount`
//! (Torznab forces it off), and the GraphQL-only FIND-2 popularity-sort rewrite
//! (`gqlmodel/torrent_content.go`) — Torznab does NOT pass through gqlmodel, so
//! FIND-2 does not apply on this path (see `CONTRACT.md` §Ordering).

mod aggregations;
mod criteria;
mod facets;
mod options;
mod order;
mod params;
mod query;
mod result;
mod search;

pub use aggregations::{AggregationGroup, AggregationItem, Aggregations, FacetLogic};
pub use criteria::{
    ContentCollectionRef, ContentRef, Criteria, Episodes, TorrentContentAttribute, Video3D,
    VideoCodec, VideoModifier, VideoResolution, VideoSource,
};
pub use facets::fetch_aggregations;
pub use options::{FacetRequest, SearchBuildConfig, SearchOptions, TorrentContentFacet};
pub use order::{OrderDirection, TorrentContentOrder, TorrentContentOrderField};
pub use params::TorznabSearchParams;
pub use query::{build_query, Bind, HydrateOptions, Result, SearchQuery, SearchQueryError};
pub use result::{SearchResult, SearchResultItem, TorrentSourceInfo};
pub use search::{build_search_query, search};

// Re-exported so Lane T and tests can name these without a direct
// `bitmagnet-model` dependency.
pub use bitmagnet_model::{
    Content, ContentType, FileType, FilesStatus, InfoHash, Torrent, TorrentContent,
};
