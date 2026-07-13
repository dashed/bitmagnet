//! Search-serving tier for the bitmagnet Rust rewrite (Phase 2).
//!
//! This crate is the search-serving tier that GraphQL resolvers call: the L1
//! blob-refine composer, the L3 gRPC client, and the engine-level Tantivy-serve
//! router decorator. C1 freezes their shared contract; C2 implements the L3 RPC
//! client and its fail-closed background health poller; C3 implements the
//! bounded exact-refine composer backed by Lane S's real PostgreSQL query API.

#![warn(missing_docs)]

pub mod api;
pub mod candidates;
pub mod client;
pub mod composer;
pub mod config;
pub mod filters;
pub mod health;
pub mod metrics;
pub mod pg;
pub mod refine;

pub use api::{Disabled, SearchServe};
pub use candidates::{CandidateSource, HealthGate};
pub use client::{Client, ClientConfig};
pub use composer::Composer;
pub use config::{
    ComposerConfig, DisabledServeRouter, ServeConfig, ServeMode, DEFAULT_MAX_CANDIDATES,
    DEFAULT_MAX_CHUNK_TORRENTS, DEFAULT_MAX_DECODE_CANDIDATES, DEFAULT_MAX_REFINE_FILES,
    DEFAULT_MIN_QUERY_LENGTH, DEFAULT_OVERSAMPLE_FACTOR, DEFAULT_REFINE_FILE_BUDGET,
    DEFAULT_RETAINED_FILE_BUDGET, DEFAULT_ROUTE_TIMEOUT,
};
pub use filters::{FileRow, FileRowSort, FileRowsResult, Filters, PathGroup};
pub use health::{
    gate, poll_once, poll_once_with_metrics, spawn_health_poller, spawn_health_poller_with_metrics,
    HealthConfig, HealthState,
};
pub use metrics::{PathsearchMetrics, RouteResult, ServeMetrics, ServeOutcome};
pub use pg::{
    AggregationGroup, AggregationItem, Aggregations, Criteria, HydrateOptions, PgSearch,
    PgSearchBackend, QueryOptions, SearchBuildConfig, SearchOptions, SearchRequest, SearchResult,
    SearchResultItem,
};
pub use refine::{
    distinct_matched_paths, files_for_refine, paginate, torrent_matches, torrent_refine,
    RefinePredicate,
};

/// Errors produced by the search-serving contract and its future implementations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The L3 candidate source failed.
    #[error("l3 candidate source error: {0}")]
    Candidate(String),
    /// The PostgreSQL search backend failed.
    #[error("pg search backend error: {0}")]
    Pg(String),
    /// A torrent file blob could not be decoded.
    #[error("blob decode error: {0}")]
    Blob(#[from] bitmagnet_model::BlobError),
    /// A failure that does not yet have a dedicated variant.
    #[error("{0}")]
    Other(String),
}

/// Convenient result alias for search-serving operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for bitmagnet_common::Error {
    fn from(error: Error) -> Self {
        bitmagnet_common::Error::Other(error.to_string())
    }
}
