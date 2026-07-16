use async_graphql::{InputObject, MaybeUndefined};

use super::enums::{
    ContentType, FacetLogic, FileFacetField, FileType, Language, MetricsBucketDuration,
    QueueJobStatus, QueueJobsOrderByField, TorrentContentOrderByField, TorrentFilesOrderByField,
    VideoResolution, VideoSource,
};
use super::scalars::{DateTime, Hash20, Year};

#[derive(InputObject)]
pub(crate) struct ContentTypeFacetInput {
    pub(crate) aggregate: MaybeUndefined<bool>,
    pub(crate) filter: Option<Vec<Option<ContentType>>>,
}

#[derive(InputObject)]
pub(crate) struct FileSearchFacetsInput {
    pub(crate) extensions: Option<Vec<String>>,
    pub(crate) facets: Option<Vec<FileFacetField>>,
    pub(crate) max_size: MaybeUndefined<i32>,
    pub(crate) min_size: MaybeUndefined<i32>,
    pub(crate) query: MaybeUndefined<String>,
}

#[derive(InputObject)]
pub(crate) struct FileSearchInput {
    pub(crate) extensions: Option<Vec<String>>,
    #[graphql(name = "infoHash")]
    pub(crate) info_hash: MaybeUndefined<Hash20>,
    pub(crate) limit: MaybeUndefined<i32>,
    pub(crate) max_size: MaybeUndefined<i32>,
    pub(crate) min_size: MaybeUndefined<i32>,
    pub(crate) offset: MaybeUndefined<i32>,
    pub(crate) query: MaybeUndefined<String>,
    pub(crate) sort: Option<Vec<FileSearchSortInput>>,
    pub(crate) total_count: MaybeUndefined<bool>,
}

#[derive(InputObject)]
pub(crate) struct FileSearchSortInput {
    pub(crate) descending: MaybeUndefined<bool>,
    pub(crate) field: String,
}

#[derive(InputObject)]
pub(crate) struct GenreFacetInput {
    pub(crate) aggregate: MaybeUndefined<bool>,
    pub(crate) filter: Option<Vec<String>>,
    pub(crate) logic: MaybeUndefined<FacetLogic>,
}

#[derive(InputObject)]
pub(crate) struct LanguageFacetInput {
    pub(crate) aggregate: MaybeUndefined<bool>,
    pub(crate) filter: Option<Vec<Language>>,
}

#[derive(InputObject)]
pub(crate) struct PathTypeaheadInput {
    pub(crate) limit: MaybeUndefined<i32>,
    pub(crate) prefix: String,
}

#[derive(InputObject)]
pub(crate) struct QueueEnqueueReprocessTorrentsBatchInput {
    pub(crate) apis_disabled: MaybeUndefined<bool>,
    pub(crate) batch_size: MaybeUndefined<i32>,
    pub(crate) chunk_size: MaybeUndefined<i32>,
    pub(crate) classifier_rematch: MaybeUndefined<bool>,
    pub(crate) classifier_workflow: MaybeUndefined<String>,
    pub(crate) content_types: Option<Vec<Option<ContentType>>>,
    pub(crate) local_search_disabled: MaybeUndefined<bool>,
    pub(crate) orphans: MaybeUndefined<bool>,
    pub(crate) purge: MaybeUndefined<bool>,
}

#[derive(InputObject)]
pub(crate) struct QueueJobQueueFacetInput {
    pub(crate) aggregate: MaybeUndefined<bool>,
    pub(crate) filter: Option<Vec<String>>,
}

#[derive(InputObject)]
pub(crate) struct QueueJobStatusFacetInput {
    pub(crate) aggregate: MaybeUndefined<bool>,
    pub(crate) filter: Option<Vec<QueueJobStatus>>,
}

#[derive(InputObject)]
pub(crate) struct QueueJobsFacetsInput {
    pub(crate) queue: MaybeUndefined<QueueJobQueueFacetInput>,
    pub(crate) status: MaybeUndefined<QueueJobStatusFacetInput>,
}

#[derive(InputObject)]
pub(crate) struct QueueJobsOrderByInput {
    pub(crate) descending: MaybeUndefined<bool>,
    pub(crate) field: QueueJobsOrderByField,
}

#[derive(InputObject)]
pub(crate) struct QueueJobsQueryInput {
    pub(crate) facets: MaybeUndefined<QueueJobsFacetsInput>,
    pub(crate) has_next_page: MaybeUndefined<bool>,
    pub(crate) limit: MaybeUndefined<i32>,
    pub(crate) offset: MaybeUndefined<i32>,
    pub(crate) order_by: Option<Vec<QueueJobsOrderByInput>>,
    pub(crate) page: MaybeUndefined<i32>,
    pub(crate) queues: Option<Vec<String>>,
    pub(crate) statuses: Option<Vec<QueueJobStatus>>,
    pub(crate) total_count: MaybeUndefined<bool>,
}

#[derive(InputObject)]
pub(crate) struct QueueMetricsQueryInput {
    pub(crate) bucket_duration: MetricsBucketDuration,
    pub(crate) end_time: MaybeUndefined<DateTime>,
    pub(crate) queues: Option<Vec<String>>,
    pub(crate) start_time: MaybeUndefined<DateTime>,
    pub(crate) statuses: Option<Vec<QueueJobStatus>>,
}

#[derive(InputObject)]
pub(crate) struct QueuePurgeJobsInput {
    pub(crate) queues: Option<Vec<String>>,
    pub(crate) statuses: Option<Vec<QueueJobStatus>>,
}

#[derive(InputObject)]
pub(crate) struct ReleaseYearFacetInput {
    pub(crate) aggregate: MaybeUndefined<bool>,
    pub(crate) filter: Option<Vec<Option<Year>>>,
}

#[derive(InputObject)]
pub(crate) struct SizeRangeInput {
    pub(crate) max: MaybeUndefined<i32>,
    pub(crate) min: MaybeUndefined<i32>,
}

#[derive(InputObject)]
pub(crate) struct SuggestTagsQueryInput {
    pub(crate) exclusions: Option<Vec<String>>,
    pub(crate) prefix: MaybeUndefined<String>,
}

#[derive(InputObject)]
pub(crate) struct TorrentContentCollapsePathsInput {
    pub(crate) limit: MaybeUndefined<i32>,
    pub(crate) offset: MaybeUndefined<i32>,
    pub(crate) query_string: String,
}

#[derive(InputObject)]
pub(crate) struct TorrentContentFacetsInput {
    pub(crate) content_type: MaybeUndefined<ContentTypeFacetInput>,
    pub(crate) genre: MaybeUndefined<GenreFacetInput>,
    pub(crate) language: MaybeUndefined<LanguageFacetInput>,
    pub(crate) published_at: MaybeUndefined<String>,
    pub(crate) release_year: MaybeUndefined<ReleaseYearFacetInput>,
    pub(crate) size_range: MaybeUndefined<SizeRangeInput>,
    pub(crate) torrent_file_type: MaybeUndefined<TorrentFileTypeFacetInput>,
    pub(crate) torrent_source: MaybeUndefined<TorrentSourceFacetInput>,
    pub(crate) torrent_tag: MaybeUndefined<TorrentTagFacetInput>,
    pub(crate) video_resolution: MaybeUndefined<VideoResolutionFacetInput>,
    pub(crate) video_source: MaybeUndefined<VideoSourceFacetInput>,
}

#[derive(InputObject)]
pub(crate) struct TorrentContentOrderByInput {
    pub(crate) descending: MaybeUndefined<bool>,
    pub(crate) field: TorrentContentOrderByField,
}

#[derive(InputObject)]
pub(crate) struct TorrentContentSearchQueryInput {
    pub(crate) aggregation_budget: MaybeUndefined<f64>,
    pub(crate) cached: MaybeUndefined<bool>,
    pub(crate) facets: MaybeUndefined<TorrentContentFacetsInput>,
    pub(crate) has_next_page: MaybeUndefined<bool>,
    pub(crate) info_hashes: Option<Vec<Hash20>>,
    pub(crate) limit: MaybeUndefined<i32>,
    pub(crate) offset: MaybeUndefined<i32>,
    pub(crate) order_by: Option<Vec<TorrentContentOrderByInput>>,
    pub(crate) page: MaybeUndefined<i32>,
    pub(crate) query_string: MaybeUndefined<String>,
    pub(crate) total_count: MaybeUndefined<bool>,
}

#[derive(InputObject)]
pub(crate) struct TorrentFileTypeFacetInput {
    pub(crate) aggregate: MaybeUndefined<bool>,
    pub(crate) filter: Option<Vec<FileType>>,
    pub(crate) logic: MaybeUndefined<FacetLogic>,
}

#[derive(InputObject)]
pub(crate) struct TorrentFilesOrderByInput {
    pub(crate) descending: MaybeUndefined<bool>,
    pub(crate) field: TorrentFilesOrderByField,
}

#[derive(InputObject)]
pub(crate) struct TorrentFilesQueryInput {
    pub(crate) cached: MaybeUndefined<bool>,
    pub(crate) has_next_page: MaybeUndefined<bool>,
    pub(crate) info_hashes: Option<Vec<Hash20>>,
    pub(crate) limit: MaybeUndefined<i32>,
    pub(crate) offset: MaybeUndefined<i32>,
    pub(crate) order_by: Option<Vec<TorrentFilesOrderByInput>>,
    pub(crate) page: MaybeUndefined<i32>,
    pub(crate) total_count: MaybeUndefined<bool>,
}

#[derive(InputObject)]
pub(crate) struct TorrentMetricsQueryInput {
    pub(crate) bucket_duration: MetricsBucketDuration,
    pub(crate) end_time: MaybeUndefined<DateTime>,
    pub(crate) sources: Option<Vec<String>>,
    pub(crate) start_time: MaybeUndefined<DateTime>,
}

#[derive(InputObject)]
pub(crate) struct TorrentReprocessInput {
    pub(crate) apis_disabled: MaybeUndefined<bool>,
    pub(crate) classifier_rematch: MaybeUndefined<bool>,
    pub(crate) classifier_workflow: MaybeUndefined<String>,
    pub(crate) info_hashes: Vec<Hash20>,
    pub(crate) local_search_disabled: MaybeUndefined<bool>,
}

#[derive(InputObject)]
pub(crate) struct TorrentSourceFacetInput {
    pub(crate) aggregate: MaybeUndefined<bool>,
    pub(crate) filter: Option<Vec<String>>,
    pub(crate) logic: MaybeUndefined<FacetLogic>,
}

#[derive(InputObject)]
pub(crate) struct TorrentTagFacetInput {
    pub(crate) aggregate: MaybeUndefined<bool>,
    pub(crate) filter: Option<Vec<String>>,
    pub(crate) logic: MaybeUndefined<FacetLogic>,
}

#[derive(InputObject)]
pub(crate) struct VideoResolutionFacetInput {
    pub(crate) aggregate: MaybeUndefined<bool>,
    pub(crate) filter: Option<Vec<Option<VideoResolution>>>,
}

#[derive(InputObject)]
pub(crate) struct VideoSourceFacetInput {
    pub(crate) aggregate: MaybeUndefined<bool>,
    pub(crate) filter: Option<Vec<Option<VideoSource>>>,
}
