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
    QueueJobsQueryResult, QueueMetricsQueryResult, TorrentContentCollapsePathsResult,
    TorrentContentSearchResult, TorrentFilesQueryResult, TorrentListSourcesResult,
    TorrentMetricsQueryResult, TorrentSuggestTagsResult, WorkersListAllQueryResult,
};
use super::scalars::{Hash20, Void};
use super::Version;

fn unserved<T>(surface: &str) -> async_graphql::Result<T> {
    Err(async_graphql::Error::new(format!(
        "{surface} is declared for SDL parity but is not served by the Phase-2 read API"
    )))
}

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
    async fn jobs(
        &self,
        input: QueueJobsQueryInput,
    ) -> async_graphql::Result<QueueJobsQueryResult> {
        let _ = &input;
        unserved("queue.jobs")
    }

    async fn metrics(
        &self,
        input: QueueMetricsQueryInput,
    ) -> async_graphql::Result<QueueMetricsQueryResult> {
        let _ = &input;
        unserved("queue.metrics")
    }
}

pub(crate) struct QueueMutation;

#[Object]
impl QueueMutation {
    async fn enqueue_reprocess_torrents_batch(
        &self,
        input: Option<QueueEnqueueReprocessTorrentsBatchInput>,
    ) -> async_graphql::Result<Option<Void>> {
        let _ = &input;
        unserved("queue mutation")
    }

    async fn purge_jobs(&self, input: QueuePurgeJobsInput) -> async_graphql::Result<Option<Void>> {
        let _ = &input;
        unserved("queue mutation")
    }
}

pub(crate) struct TorrentQuery;

#[Object]
impl TorrentQuery {
    async fn files(
        &self,
        ctx: &Context<'_>,
        input: TorrentFilesQueryInput,
    ) -> async_graphql::Result<TorrentFilesQueryResult> {
        let runtime = ctx.data::<super::torrent_files::TorrentFilesRuntimeData>()?;
        super::torrent_files::resolve(runtime, input).await
    }

    async fn list_sources(&self) -> async_graphql::Result<TorrentListSourcesResult> {
        unserved("torrent.listSources")
    }

    async fn metrics(
        &self,
        input: TorrentMetricsQueryInput,
    ) -> async_graphql::Result<TorrentMetricsQueryResult> {
        let _ = &input;
        unserved("torrent.metrics")
    }

    async fn suggest_tags(
        &self,
        input: Option<SuggestTagsQueryInput>,
    ) -> async_graphql::Result<TorrentSuggestTagsResult> {
        let _ = &input;
        unserved("torrent.suggestTags")
    }
}

pub(crate) struct TorrentMutation;

#[Object]
impl TorrentMutation {
    async fn delete(&self, info_hashes: Vec<Hash20>) -> async_graphql::Result<Option<Void>> {
        let _ = &info_hashes;
        unserved("torrent mutation")
    }

    async fn delete_tags(
        &self,
        info_hashes: Option<Vec<Hash20>>,
        tag_names: Option<Vec<String>>,
    ) -> async_graphql::Result<Option<Void>> {
        let _ = (&info_hashes, &tag_names);
        unserved("torrent mutation")
    }

    async fn put_tags(
        &self,
        info_hashes: Vec<Hash20>,
        tag_names: Vec<String>,
    ) -> async_graphql::Result<Option<Void>> {
        let _ = (&info_hashes, &tag_names);
        unserved("torrent mutation")
    }

    async fn reprocess(&self, input: TorrentReprocessInput) -> async_graphql::Result<Option<Void>> {
        let _ = &input;
        unserved("torrent mutation")
    }

    async fn set_tags(
        &self,
        info_hashes: Vec<Hash20>,
        tag_names: Vec<String>,
    ) -> async_graphql::Result<Option<Void>> {
        let _ = (&info_hashes, &tag_names);
        unserved("torrent mutation")
    }
}

pub(crate) struct TorrentContentQuery;

#[Object]
impl TorrentContentQuery {
    async fn collapse_paths(
        &self,
        ctx: &Context<'_>,
        input: TorrentContentCollapsePathsInput,
    ) -> async_graphql::Result<TorrentContentCollapsePathsResult> {
        super::search_resolvers::collapse_paths(ctx, input).await
    }

    async fn file_search(
        &self,
        ctx: &Context<'_>,
        input: FileSearchInput,
    ) -> async_graphql::Result<FileSearchResult> {
        super::search_resolvers::file_search(ctx, input).await
    }

    async fn file_search_facets(
        &self,
        ctx: &Context<'_>,
        input: FileSearchFacetsInput,
    ) -> async_graphql::Result<FileSearchFacetsResult> {
        super::search_resolvers::file_search_facets(ctx, input).await
    }

    async fn path_typeahead(
        &self,
        ctx: &Context<'_>,
        input: PathTypeaheadInput,
    ) -> async_graphql::Result<PathTypeaheadResult> {
        super::search_resolvers::path_typeahead(ctx, input).await
    }

    async fn search(
        &self,
        ctx: &Context<'_>,
        input: TorrentContentSearchQueryInput,
    ) -> async_graphql::Result<TorrentContentSearchResult> {
        super::search_resolvers::search(ctx, input).await
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
