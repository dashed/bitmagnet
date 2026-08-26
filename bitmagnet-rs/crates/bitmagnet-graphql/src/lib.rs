//! GraphQL read API for the bitmagnet Rust rewrite (Phase-2).
//!
//! Lane G: async-graphql code-first schema reproducing the Go gqlgen SDL
//! (0-diff gate via [`normalize`]) plus the read resolvers.

mod health;
pub mod normalize;
mod pg;
pub mod schema;

pub use health::RuntimeConfig;
pub use pg::{admit_pg, PgAdmissionError};
pub use schema::file_search_client::{
    DisabledFileSearchBackend, FileSearchClientConfig, L2FileHit, L2FileRowsResult,
    L2FileSearchBackend, TonicFileSearchClient, MAX_L2_FILE_WINDOW,
};
pub use schema::lane_c::LaneCSearchRuntime;
pub use schema::lane_s::{LaneSSearchBackend, SqlxLaneSSearchBackend};
pub use schema::runtime::{hydrate_l2_file_rows, PgL2SearchRuntime};
pub use schema::{
    build_runtime_schema, build_runtime_search_schema, build_schema, build_search_schema, schema,
    Schema,
};
pub use schema::{
    PgTorrentFilesRuntime, TorrentFilesBlob, TorrentFilesError, TorrentFilesLimits,
    TorrentFilesRuntime, TorrentFilesRuntimeData, TORRENT_FILES_SQL,
};
pub use schema::{
    PgTorrentSourcesRuntime, TorrentSourceRecord, TorrentSourcesError, TorrentSourcesRuntime,
    TorrentSourcesRuntimeData, MAX_TORRENT_SOURCES,
};
pub use schema::{SearchRuntime, SearchRuntimeData};
