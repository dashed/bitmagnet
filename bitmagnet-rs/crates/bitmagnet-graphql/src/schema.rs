pub(crate) mod enums;
pub mod file_search_client;
mod inputs;
pub mod lane_c;
pub mod lane_s;
pub mod metrics;
pub(crate) mod objects;
pub mod queue_jobs;
pub mod queue_mutations;
mod roots;
pub mod runtime;
pub(crate) mod scalars;
pub mod search;
mod search_resolvers;
pub mod torrent_delete_mutations;
pub mod torrent_files;
pub mod torrent_sources;
pub mod torrent_tag_mutations;
pub mod torrent_tags;

use async_graphql::EmptySubscription;

use crate::health::{HealthRuntime, RuntimeConfig};

pub use metrics::{
    MetricsBucket, MetricsError, MetricsRuntime, MetricsRuntimeData, PgMetricsRuntime,
    QueueMetricsRecord, QueueMetricsRequest, TorrentMetricsRecord, TorrentMetricsRequest,
};
pub use queue_jobs::{
    PgQueueJobsRuntime, QueueJobRecord, QueueJobsAggRecord, QueueJobsError, QueueJobsFacetRequest,
    QueueJobsOrder, QueueJobsOrderField, QueueJobsRecord, QueueJobsRequest, QueueJobsRuntime,
    QueueJobsRuntimeData, MAX_QUEUE_JOBS_FILTER_VALUES, MAX_QUEUE_JOBS_LIMIT,
    MAX_QUEUE_JOBS_OFFSET, MAX_QUEUE_NAME_CHARS,
};
pub use queue_mutations::{
    EnqueueReprocessTorrentsBatchRequest, PgQueueMutationsRuntime, PurgeJobsRequest,
    QueueMutationsError, QueueMutationsRuntime, QueueMutationsRuntimeData,
};
pub use roots::{Mutation, Query};
pub use search::{SearchRuntime, SearchRuntimeData};
pub use torrent_delete_mutations::{
    admit_torrent_delete_writer_authority, PgTorrentDeleteMutationsRuntime,
    TorrentDeleteMutationsError, TorrentDeleteMutationsRuntime, TorrentDeleteMutationsRuntimeData,
    TorrentDeleteRequest, TorrentDeleteWriterAdmission, MAX_TORRENT_DELETE_INFO_HASHES,
};
pub use torrent_files::{
    PgTorrentFilesRuntime, TorrentFilesBlob, TorrentFilesError, TorrentFilesLimits,
    TorrentFilesRuntime, TorrentFilesRuntimeData, TORRENT_FILES_SQL,
};
pub use torrent_sources::{
    PgTorrentSourcesRuntime, TorrentSourceRecord, TorrentSourcesError, TorrentSourcesRuntime,
    TorrentSourcesRuntimeData, MAX_TORRENT_SOURCES,
};
pub use torrent_tag_mutations::{
    DeleteTagsRequest, PgTorrentTagMutationsRuntime, PutTagsRequest, SetTagsRequest,
    TorrentTagMutationsError, TorrentTagMutationsRuntime, TorrentTagMutationsRuntimeData,
    MAX_TAG_MUTATION_INFO_HASHES, MAX_TAG_MUTATION_ROWS, MAX_TAG_MUTATION_TAG_NAMES,
};
pub use torrent_tags::{
    PgTorrentTagsRuntime, SuggestTagsRequest, SuggestedTagRecord, TorrentTagsError,
    TorrentTagsRuntime, TorrentTagsRuntimeData,
};

/// Runtime version data available to GraphQL resolvers.
pub struct Version(pub String);

/// The complete bitmagnet GraphQL schema.
pub type Schema = async_graphql::Schema<Query, Mutation, EmptySubscription>;

/// Build the code-first GraphQL schema.
#[must_use]
pub fn schema() -> Schema {
    async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(SearchRuntimeData::disabled())
        .data(MetricsRuntimeData::disabled())
        .data(QueueJobsRuntimeData::disabled())
        .data(QueueMutationsRuntimeData::disabled())
        .data(TorrentFilesRuntimeData::disabled())
        .data(TorrentDeleteMutationsRuntimeData::disabled())
        .data(TorrentSourcesRuntimeData::disabled())
        .data(TorrentTagMutationsRuntimeData::disabled())
        .data(TorrentTagsRuntimeData::disabled())
        .finish()
}

/// Build the GraphQL schema with runtime version context attached.
#[must_use]
pub fn build_schema(version: String) -> Schema {
    async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(Version(version))
        .data(SearchRuntimeData::disabled())
        .data(MetricsRuntimeData::disabled())
        .data(QueueJobsRuntimeData::disabled())
        .data(QueueMutationsRuntimeData::disabled())
        .data(TorrentFilesRuntimeData::disabled())
        .data(TorrentDeleteMutationsRuntimeData::disabled())
        .data(TorrentSourcesRuntimeData::disabled())
        .data(TorrentTagMutationsRuntimeData::disabled())
        .data(TorrentTagsRuntimeData::disabled())
        .finish()
}

/// Build the GraphQL schema with a search runtime attached.
#[must_use]
pub fn build_search_schema(version: String, search: std::sync::Arc<dyn SearchRuntime>) -> Schema {
    async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(Version(version))
        .data(SearchRuntimeData::new(search))
        .data(MetricsRuntimeData::disabled())
        .data(QueueJobsRuntimeData::disabled())
        .data(QueueMutationsRuntimeData::disabled())
        .data(TorrentFilesRuntimeData::disabled())
        .data(TorrentDeleteMutationsRuntimeData::disabled())
        .data(TorrentSourcesRuntimeData::disabled())
        .data(TorrentTagMutationsRuntimeData::disabled())
        .data(TorrentTagsRuntimeData::disabled())
        .finish()
}

/// Build the runtime GraphQL schema with database health and optional peer
/// federation attached.
#[must_use]
pub fn build_runtime_schema(
    version: String,
    pool: bitmagnet_db::PgPool,
    config: RuntimeConfig,
) -> Schema {
    async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(Version(version))
        .data(HealthRuntime::new(pool.clone(), config))
        .data(SearchRuntimeData::disabled())
        .data(MetricsRuntimeData::pg(pool.clone()))
        .data(QueueJobsRuntimeData::pg(pool.clone()))
        .data(QueueMutationsRuntimeData::disabled())
        .data(TorrentFilesRuntimeData::pg(pool.clone()))
        .data(TorrentDeleteMutationsRuntimeData::disabled())
        .data(TorrentSourcesRuntimeData::pg(pool.clone()))
        .data(TorrentTagMutationsRuntimeData::disabled())
        .data(TorrentTagsRuntimeData::pg(pool))
        .finish()
}

/// Build the runtime GraphQL schema with database health, optional peer
/// federation, and the fully composed search runtime attached.
#[must_use]
pub fn build_runtime_search_schema(
    version: String,
    pool: bitmagnet_db::PgPool,
    config: RuntimeConfig,
    search: std::sync::Arc<dyn SearchRuntime>,
) -> Schema {
    build_runtime_search_schema_with_tag_mutations(
        version,
        pool,
        config,
        search,
        TorrentTagMutationsRuntimeData::disabled(),
    )
}

/// Build the complete runtime schema with an explicitly selected tag-mutation runtime.
///
/// Callers must construct the enabled runtime from a separate writer pool. All
/// other builders attach the fail-loud disabled implementation.
#[must_use]
pub fn build_runtime_search_schema_with_tag_mutations(
    version: String,
    pool: bitmagnet_db::PgPool,
    config: RuntimeConfig,
    search: std::sync::Arc<dyn SearchRuntime>,
    tag_mutations: TorrentTagMutationsRuntimeData,
) -> Schema {
    build_runtime_search_schema_with_mutations(
        version,
        pool,
        config,
        search,
        tag_mutations,
        QueueMutationsRuntimeData::disabled(),
        TorrentDeleteMutationsRuntimeData::disabled(),
    )
}

/// Build the complete runtime schema with explicitly selected mutation runtimes.
///
/// Each enabled family must be constructed from its own separately authorized
/// writer pool. All simpler builders attach fail-loud disabled implementations.
#[must_use]
pub fn build_runtime_search_schema_with_mutations(
    version: String,
    pool: bitmagnet_db::PgPool,
    config: RuntimeConfig,
    search: std::sync::Arc<dyn SearchRuntime>,
    tag_mutations: TorrentTagMutationsRuntimeData,
    queue_mutations: QueueMutationsRuntimeData,
    torrent_delete_mutations: TorrentDeleteMutationsRuntimeData,
) -> Schema {
    async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(Version(version))
        .data(HealthRuntime::new(pool.clone(), config))
        .data(SearchRuntimeData::new(search))
        .data(MetricsRuntimeData::pg(pool.clone()))
        .data(QueueJobsRuntimeData::pg(pool.clone()))
        .data(queue_mutations)
        .data(TorrentFilesRuntimeData::pg(pool.clone()))
        .data(torrent_delete_mutations)
        .data(TorrentSourcesRuntimeData::pg(pool.clone()))
        .data(tag_mutations)
        .data(TorrentTagsRuntimeData::pg(pool))
        .finish()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use async_graphql::value;
    use async_trait::async_trait;

    use super::{build_runtime_schema, build_runtime_search_schema, build_schema};
    use crate::health::RuntimeConfig;
    use crate::schema::search::{self, SearchRequest, SearchResult, SearchRuntime};

    struct FakeSearchRuntime {
        called: Arc<AtomicBool>,
    }

    #[async_trait]
    impl SearchRuntime for FakeSearchRuntime {
        async fn pg_torrent_content(
            &self,
            _request: SearchRequest,
        ) -> search::Result<SearchResult> {
            self.called.store(true, Ordering::Relaxed);
            Ok(SearchResult {
                total_count: 17,
                ..SearchResult::default()
            })
        }
    }

    #[tokio::test]
    async fn build_schema_injects_version_into_query_resolver() {
        let response = build_schema("t1.2.3".into()).execute("{ version }").await;

        assert!(
            response.errors.is_empty(),
            "version query returned errors: {:?}",
            response.errors
        );
        assert_eq!(response.data, value!({ "version": "t1.2.3" }));
    }

    #[tokio::test]
    async fn unconfigured_delete_mutation_is_declared_but_fails_loudly() {
        let response = build_schema("test".into())
            .execute(
                r#"mutation {
                    torrent {
                        delete(infoHashes: ["0123456789abcdef0123456789abcdef01234567"])
                    }
                }"#,
            )
            .await;

        assert_eq!(response.errors.len(), 1);
        assert!(response.errors[0]
            .message
            .contains("torrent delete mutations are disabled"));
    }

    #[tokio::test]
    async fn runtime_schema_reports_the_started_http_worker_without_peers() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/bitmagnet")
            .expect("lazy postgres pool");
        let response = build_runtime_schema("test".into(), pool, RuntimeConfig::default())
            .execute("{ workers { listAll { workers { key started } } } }")
            .await;

        assert!(
            response.errors.is_empty(),
            "workers query returned errors: {:?}",
            response.errors
        );
        assert_eq!(
            response.data,
            value!({
                "workers": {
                    "listAll": {
                        "workers": [{ "key": "http_server", "started": true }]
                    }
                }
            })
        );
    }

    #[tokio::test]
    async fn runtime_search_schema_keeps_health_and_injects_search_runtime() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/bitmagnet")
            .expect("lazy postgres pool");
        let called = Arc::new(AtomicBool::new(false));
        let search: Arc<dyn SearchRuntime> = Arc::new(FakeSearchRuntime {
            called: Arc::clone(&called),
        });
        let response =
            build_runtime_search_schema("test".into(), pool, RuntimeConfig::default(), search)
                .execute(
                    "{ workers { listAll { workers { key started } } } \
             torrentContent { search(input: {}) { totalCount } } }",
                )
                .await;

        assert!(
            response.errors.is_empty(),
            "combined runtime query returned errors: {:?}",
            response.errors
        );
        assert!(called.load(Ordering::Relaxed));
        assert_eq!(
            response.data,
            value!({
                "workers": {
                    "listAll": {
                        "workers": [{ "key": "http_server", "started": true }]
                    }
                },
                "torrentContent": { "search": { "totalCount": 17 } }
            })
        );
    }
}
