use async_graphql::{Context, Object};

use crate::health::{HealthRuntime, HealthSnapshot};

use super::enums::HealthStatus;
use super::inputs::{
    FileSearchFacetsInput, FileSearchInput, PathTypeaheadInput,
    QueueEnqueueReprocessTorrentsBatchInput, QueueJobsQueryInput, QueueMetricsQueryInput,
    QueuePurgeJobsInput, SuggestTagsQueryInput, TorrentContentCollapsePathsInput,
    TorrentContentSearchQueryInput, TorrentFilesQueryInput, TorrentMetricsQueryInput,
    TorrentReprocessInput,
};
use super::objects::{
    FileSearchFacetsResult, FileSearchResult, HealthCheck, PathTypeaheadResult,
    QueueJobsAggregations, QueueJobsQueryResult, QueueMetricsQueryResult,
    TorrentContentAggregations, TorrentContentCollapsePathsResult, TorrentContentSearchResult,
    TorrentFilesQueryResult, TorrentListSourcesResult, TorrentMetricsQueryResult,
    TorrentSuggestTagsResult, WorkersListAllQueryResult,
};
use super::scalars::{Hash20, Void};
use super::Version;

pub struct Query;

#[Object]
impl Query {
    async fn health(&self, ctx: &Context<'_>) -> HealthQuery {
        let snapshot = match ctx.data_opt::<HealthRuntime>() {
            Some(runtime) => runtime.health().await,
            None => HealthSnapshot {
                status: HealthStatus::Unknown,
                checks: Vec::new(),
            },
        };

        HealthQuery {
            status: snapshot.status,
            checks: snapshot.into_graphql_checks(),
        }
    }

    async fn queue(&self) -> QueueQuery {
        QueueQuery
    }

    async fn torrent(&self) -> TorrentQuery {
        TorrentQuery
    }

    async fn torrent_content(&self) -> TorrentContentQuery {
        TorrentContentQuery
    }

    async fn version(&self, ctx: &Context<'_>) -> async_graphql::Result<String> {
        Ok(ctx.data::<Version>()?.0.clone())
    }

    async fn workers(&self, ctx: &Context<'_>) -> WorkersQuery {
        let workers = match ctx.data_opt::<HealthRuntime>() {
            Some(runtime) => runtime.workers().await,
            None => Vec::new(),
        };

        WorkersQuery { workers }
    }
}

pub struct Mutation;

#[Object]
impl Mutation {
    async fn queue(&self) -> QueueMutation {
        QueueMutation
    }

    async fn torrent(&self) -> TorrentMutation {
        TorrentMutation
    }
}

pub(crate) struct HealthQuery {
    status: HealthStatus,
    checks: Vec<HealthCheck>,
}

#[Object]
impl HealthQuery {
    async fn checks(&self) -> Vec<HealthCheck> {
        self.checks.clone()
    }

    async fn status(&self) -> HealthStatus {
        self.status
    }
}

pub(crate) struct QueueQuery;

#[Object]
impl QueueQuery {
    async fn jobs(&self, input: QueueJobsQueryInput) -> QueueJobsQueryResult {
        let _ = &input;
        QueueJobsQueryResult {
            aggregations: QueueJobsAggregations {
                queue: None,
                status: None,
            },
            has_next_page: None,
            items: Vec::new(),
            total_count: 0,
        }
    }

    async fn metrics(&self, input: QueueMetricsQueryInput) -> QueueMetricsQueryResult {
        let _ = &input;
        QueueMetricsQueryResult {
            buckets: Vec::new(),
        }
    }
}

pub(crate) struct QueueMutation;

#[Object]
impl QueueMutation {
    async fn enqueue_reprocess_torrents_batch(
        &self,
        input: Option<QueueEnqueueReprocessTorrentsBatchInput>,
    ) -> Option<Void> {
        let _ = &input;
        None
    }

    async fn purge_jobs(&self, input: QueuePurgeJobsInput) -> Option<Void> {
        let _ = &input;
        None
    }
}

pub(crate) struct TorrentQuery;

#[Object]
impl TorrentQuery {
    async fn files(&self, input: TorrentFilesQueryInput) -> TorrentFilesQueryResult {
        let _ = &input;
        TorrentFilesQueryResult {
            has_next_page: None,
            items: Vec::new(),
            total_count: 0,
        }
    }

    async fn list_sources(&self) -> TorrentListSourcesResult {
        TorrentListSourcesResult {
            sources: Vec::new(),
        }
    }

    async fn metrics(&self, input: TorrentMetricsQueryInput) -> TorrentMetricsQueryResult {
        let _ = &input;
        TorrentMetricsQueryResult {
            buckets: Vec::new(),
        }
    }

    async fn suggest_tags(&self, input: Option<SuggestTagsQueryInput>) -> TorrentSuggestTagsResult {
        let _ = &input;
        TorrentSuggestTagsResult {
            suggestions: Vec::new(),
        }
    }
}

pub(crate) struct TorrentMutation;

#[Object]
impl TorrentMutation {
    async fn delete(&self, info_hashes: Vec<Hash20>) -> Option<Void> {
        let _ = &info_hashes;
        None
    }

    async fn delete_tags(
        &self,
        info_hashes: Option<Vec<Hash20>>,
        tag_names: Option<Vec<String>>,
    ) -> Option<Void> {
        let _ = (&info_hashes, &tag_names);
        None
    }

    async fn put_tags(&self, info_hashes: Vec<Hash20>, tag_names: Vec<String>) -> Option<Void> {
        let _ = (&info_hashes, &tag_names);
        None
    }

    async fn reprocess(&self, input: TorrentReprocessInput) -> Option<Void> {
        let _ = &input;
        None
    }

    async fn set_tags(&self, info_hashes: Vec<Hash20>, tag_names: Vec<String>) -> Option<Void> {
        let _ = (&info_hashes, &tag_names);
        None
    }
}

pub(crate) struct TorrentContentQuery;

#[Object]
impl TorrentContentQuery {
    async fn collapse_paths(
        &self,
        input: TorrentContentCollapsePathsInput,
    ) -> TorrentContentCollapsePathsResult {
        let _ = &input;
        TorrentContentCollapsePathsResult { groups: Vec::new() }
    }

    async fn file_search(&self, input: FileSearchInput) -> FileSearchResult {
        let _ = &input;
        FileSearchResult {
            has_next_page: false,
            items: Vec::new(),
            total_count: 0,
            total_count_is_estimate: false,
        }
    }

    async fn file_search_facets(&self, input: FileSearchFacetsInput) -> FileSearchFacetsResult {
        let _ = &input;
        FileSearchFacetsResult { facets: Vec::new() }
    }

    async fn path_typeahead(&self, input: PathTypeaheadInput) -> PathTypeaheadResult {
        let _ = &input;
        PathTypeaheadResult {
            suggestions: Vec::new(),
        }
    }

    async fn search(&self, input: TorrentContentSearchQueryInput) -> TorrentContentSearchResult {
        let _ = &input;
        TorrentContentSearchResult {
            aggregations: TorrentContentAggregations {
                content_type: None,
                genre: None,
                language: None,
                release_year: None,
                torrent_file_type: None,
                torrent_source: None,
                torrent_tag: None,
                video_resolution: None,
                video_source: None,
            },
            has_next_page: None,
            items: Vec::new(),
            total_count: 0,
            total_count_is_estimate: false,
        }
    }
}

pub(crate) struct WorkersQuery {
    workers: Vec<super::objects::Worker>,
}

#[Object]
impl WorkersQuery {
    async fn list_all(&self) -> WorkersListAllQueryResult {
        WorkersListAllQueryResult {
            workers: self.workers.clone(),
        }
    }
}
