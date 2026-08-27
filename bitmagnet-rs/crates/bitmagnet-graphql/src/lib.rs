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
    admit_torrent_delete_writer_authority, PgTorrentDeleteMutationsRuntime,
    TorrentDeleteMutationsError, TorrentDeleteMutationsRuntime, TorrentDeleteMutationsRuntimeData,
    TorrentDeleteRequest, TorrentDeleteWriterAdmission, MAX_TORRENT_DELETE_INFO_HASHES,
};
pub use schema::{
    build_runtime_schema, build_runtime_search_schema, build_runtime_search_schema_with_mutations,
    build_runtime_search_schema_with_tag_mutations, build_schema, build_search_schema, schema,
    Mutation, Query, Schema,
};
pub use schema::{
    DeleteTagsRequest, PgTorrentTagMutationsRuntime, PutTagsRequest, SetTagsRequest,
    TorrentTagMutationsError, TorrentTagMutationsRuntime, TorrentTagMutationsRuntimeData,
    MAX_TAG_MUTATION_INFO_HASHES, MAX_TAG_MUTATION_ROWS, MAX_TAG_MUTATION_TAG_NAMES,
};
pub use schema::{
    EnqueueReprocessTorrentsBatchRequest, PgQueueMutationsRuntime, PurgeJobsRequest,
    QueueMutationsError, QueueMutationsRuntime, QueueMutationsRuntimeData,
};
pub use schema::{
    MetricsBucket, MetricsError, MetricsRuntime, MetricsRuntimeData, PgMetricsRuntime,
    QueueMetricsRecord, QueueMetricsRequest, TorrentMetricsRecord, TorrentMetricsRequest,
};
pub use schema::{
    PgQueueJobsRuntime, QueueJobRecord, QueueJobsAggRecord, QueueJobsError, QueueJobsFacetRequest,
    QueueJobsOrder, QueueJobsOrderField, QueueJobsRecord, QueueJobsRequest, QueueJobsRuntime,
    QueueJobsRuntimeData, MAX_QUEUE_JOBS_FILTER_VALUES, MAX_QUEUE_JOBS_LIMIT,
    MAX_QUEUE_JOBS_OFFSET, MAX_QUEUE_NAME_CHARS,
};
pub use schema::{
    PgTorrentFilesRuntime, TorrentFilesBlob, TorrentFilesError, TorrentFilesLimits,
    TorrentFilesRuntime, TorrentFilesRuntimeData, TORRENT_FILES_SQL,
};
pub use schema::{
    PgTorrentSourcesRuntime, TorrentSourceRecord, TorrentSourcesError, TorrentSourcesRuntime,
    TorrentSourcesRuntimeData, MAX_TORRENT_SOURCES,
};
pub use schema::{
    PgTorrentTagsRuntime, SuggestTagsRequest, SuggestedTagRecord, TorrentTagsError,
    TorrentTagsRuntime, TorrentTagsRuntimeData,
};
pub use schema::{SearchRuntime, SearchRuntimeData};
