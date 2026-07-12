//! Search-serving tier for the bitmagnet Rust rewrite (Phase 2).
//!
//! This crate is the search-serving tier that GraphQL resolvers call: the L1
//! blob-refine composer, the L3 gRPC client, and the engine-level Tantivy-serve
//! router decorator. C1 freezes their shared contract; later tasks implement the
//! composer pipeline, RPC client, and serving logic.

#![warn(missing_docs)]

#[cfg(feature = "lane-s-stub")]
pub mod api;
pub mod candidates;
pub mod config;
#[cfg(feature = "lane-s-stub")]
pub mod filters;
pub mod health;
#[cfg(feature = "lane-s-stub")]
pub mod pg;

#[cfg(not(feature = "lane-s-stub"))]
compile_error!(
    "Lane S (bitmagnet-search-query S1) integration not yet wired; build with default feature lane-s-stub"
);

#[cfg(feature = "lane-s-stub")]
pub use api::{Disabled, SearchServe};
pub use candidates::{CandidateSource, HealthGate};
pub use config::{
    ComposerConfig, DisabledServeRouter, ServeConfig, ServeMode, DEFAULT_MAX_CANDIDATES,
    DEFAULT_MAX_CHUNK_TORRENTS, DEFAULT_MAX_DECODE_CANDIDATES, DEFAULT_MAX_REFINE_FILES,
    DEFAULT_MIN_QUERY_LENGTH, DEFAULT_OVERSAMPLE_FACTOR, DEFAULT_REFINE_FILE_BUDGET,
    DEFAULT_RETAINED_FILE_BUDGET, DEFAULT_ROUTE_TIMEOUT,
};
#[cfg(feature = "lane-s-stub")]
pub use filters::{FileRow, FileRowSort, FileRowsResult, Filters, PathGroup};
pub use health::{gate, HealthState};
#[cfg(feature = "lane-s-stub")]
pub use pg::{
    AggregationItem, Aggregations, PgSearchBackend, QueryOptions, SearchOptions,
    TorrentContentResult, TorrentContentResultItem,
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
