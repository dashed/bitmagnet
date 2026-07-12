use async_graphql::{SimpleObject, ID};

use super::enums::{
    ContentType, FileFacetField, FileType, FilesStatus, HealthStatus, Language, QueueJobStatus,
    Video3D, VideoCodec, VideoModifier, VideoResolution, VideoSource,
};
use super::scalars::{Date, DateTime, Duration, Hash20, Hash32, Year};

#[derive(SimpleObject)]
pub(crate) struct Content {
    pub(crate) adult: Option<bool>,
    pub(crate) attributes: Vec<ContentAttribute>,
    pub(crate) collections: Vec<ContentCollection>,
    pub(crate) created_at: DateTime,
    pub(crate) external_links: Vec<ExternalLink>,
    pub(crate) id: String,
    pub(crate) metadata_source: MetadataSource,
    pub(crate) original_language: Option<LanguageInfo>,
    pub(crate) original_title: Option<String>,
    pub(crate) overview: Option<String>,
    pub(crate) popularity: Option<f64>,
    pub(crate) release_date: Option<Date>,
    pub(crate) release_year: Option<Year>,
    pub(crate) runtime: Option<i32>,
    pub(crate) source: String,
    pub(crate) title: String,
    #[graphql(name = "type")]
    pub(crate) content_type: ContentType,
    pub(crate) updated_at: DateTime,
    pub(crate) vote_average: Option<f64>,
    pub(crate) vote_count: Option<i32>,
}

#[derive(SimpleObject)]
pub(crate) struct ContentAttribute {
    pub(crate) created_at: DateTime,
    pub(crate) key: String,
    pub(crate) metadata_source: MetadataSource,
    pub(crate) source: String,
    pub(crate) updated_at: DateTime,
    pub(crate) value: String,
}

#[derive(SimpleObject)]
pub(crate) struct ContentCollection {
    pub(crate) created_at: DateTime,
    pub(crate) id: String,
    pub(crate) metadata_source: MetadataSource,
    pub(crate) name: String,
    pub(crate) source: String,
    pub(crate) r#type: String,
    pub(crate) updated_at: DateTime,
}

#[derive(SimpleObject)]
pub(crate) struct ContentTypeAgg {
    pub(crate) count: i32,
    pub(crate) is_estimate: bool,
    pub(crate) label: String,
    pub(crate) value: Option<ContentType>,
}

#[derive(SimpleObject)]
pub(crate) struct Episodes {
    pub(crate) label: String,
    pub(crate) seasons: Vec<Season>,
}

#[derive(SimpleObject)]
pub(crate) struct ExternalLink {
    pub(crate) metadata_source: MetadataSource,
    pub(crate) url: String,
}

#[derive(SimpleObject)]
pub(crate) struct FileFacetAgg {
    pub(crate) buckets: Vec<FileFacetBucketAgg>,
    pub(crate) field: FileFacetField,
}

#[derive(SimpleObject)]
pub(crate) struct FileFacetBucketAgg {
    pub(crate) count: i32,
    pub(crate) is_estimate: bool,
    pub(crate) total_size: i32,
    pub(crate) value: String,
}

#[derive(SimpleObject)]
pub(crate) struct FileSearchFacetsResult {
    pub(crate) facets: Vec<FileFacetAgg>,
}

#[derive(SimpleObject)]
pub(crate) struct FileSearchItem {
    pub(crate) extension: String,
    pub(crate) index: i32,
    #[graphql(name = "infoHash")]
    pub(crate) info_hash: Hash20,
    pub(crate) path: String,
    pub(crate) size: i32,
    pub(crate) torrent_content: TorrentContent,
}

#[derive(SimpleObject)]
pub(crate) struct FileSearchResult {
    pub(crate) has_next_page: bool,
    pub(crate) items: Vec<FileSearchItem>,
    pub(crate) total_count: i32,
    pub(crate) total_count_is_estimate: bool,
}

#[derive(SimpleObject)]
pub(crate) struct GenreAgg {
    pub(crate) count: i32,
    pub(crate) is_estimate: bool,
    pub(crate) label: String,
    pub(crate) value: String,
}

#[derive(SimpleObject)]
pub(crate) struct HealthCheck {
    pub(crate) error: Option<String>,
    pub(crate) key: String,
    pub(crate) status: HealthStatus,
    pub(crate) timestamp: DateTime,
}

#[derive(SimpleObject)]
pub(crate) struct LanguageAgg {
    pub(crate) count: i32,
    pub(crate) is_estimate: bool,
    pub(crate) label: String,
    pub(crate) value: Language,
}

#[derive(SimpleObject)]
pub(crate) struct LanguageInfo {
    pub(crate) id: String,
    pub(crate) name: String,
}

#[derive(SimpleObject)]
pub(crate) struct MetadataSource {
    pub(crate) key: String,
    pub(crate) name: String,
}

#[derive(SimpleObject)]
pub(crate) struct PathTypeaheadResult {
    pub(crate) suggestions: Vec<String>,
}

#[derive(SimpleObject)]
pub(crate) struct QueueJob {
    pub(crate) created_at: DateTime,
    pub(crate) error: Option<String>,
    pub(crate) id: ID,
    pub(crate) max_retries: i32,
    pub(crate) payload: String,
    pub(crate) priority: i32,
    pub(crate) queue: String,
    pub(crate) ran_at: Option<DateTime>,
    pub(crate) retries: i32,
    pub(crate) run_after: DateTime,
    pub(crate) status: QueueJobStatus,
}

#[derive(SimpleObject)]
pub(crate) struct QueueJobQueueAgg {
    pub(crate) count: i32,
    pub(crate) label: String,
    pub(crate) value: String,
}

#[derive(SimpleObject)]
pub(crate) struct QueueJobStatusAgg {
    pub(crate) count: i32,
    pub(crate) label: String,
    pub(crate) value: QueueJobStatus,
}

#[derive(SimpleObject)]
pub(crate) struct QueueJobsAggregations {
    pub(crate) queue: Option<Vec<QueueJobQueueAgg>>,
    pub(crate) status: Option<Vec<QueueJobStatusAgg>>,
}

#[derive(SimpleObject)]
pub(crate) struct QueueJobsQueryResult {
    pub(crate) aggregations: QueueJobsAggregations,
    pub(crate) has_next_page: Option<bool>,
    pub(crate) items: Vec<QueueJob>,
    pub(crate) total_count: i32,
}

#[derive(SimpleObject)]
pub(crate) struct QueueMetricsBucket {
    pub(crate) count: i32,
    pub(crate) created_at_bucket: DateTime,
    pub(crate) latency: Option<Duration>,
    pub(crate) queue: String,
    pub(crate) ran_at_bucket: Option<DateTime>,
    pub(crate) status: QueueJobStatus,
}

#[derive(SimpleObject)]
pub(crate) struct QueueMetricsQueryResult {
    pub(crate) buckets: Vec<QueueMetricsBucket>,
}

#[derive(SimpleObject)]
pub(crate) struct ReleaseYearAgg {
    pub(crate) count: i32,
    pub(crate) is_estimate: bool,
    pub(crate) label: String,
    pub(crate) value: Option<Year>,
}

#[derive(SimpleObject)]
pub(crate) struct Season {
    pub(crate) episodes: Option<Vec<i32>>,
    pub(crate) season: i32,
}

#[derive(SimpleObject)]
pub(crate) struct SuggestedTag {
    pub(crate) count: i32,
    pub(crate) name: String,
}

#[derive(SimpleObject)]
pub(crate) struct Torrent {
    pub(crate) created_at: DateTime,
    pub(crate) extension: Option<String>,
    pub(crate) file_extensions: Vec<String>,
    pub(crate) file_type: Option<FileType>,
    pub(crate) file_types: Option<Vec<FileType>>,
    pub(crate) files: Option<Vec<TorrentFile>>,
    pub(crate) files_count: Option<i32>,
    pub(crate) files_status: FilesStatus,
    pub(crate) has_files_info: bool,
    #[graphql(name = "infoHash")]
    pub(crate) info_hash: Hash20,
    #[graphql(name = "infoHashV2")]
    pub(crate) info_hash_v2: Option<Hash32>,
    pub(crate) leechers: Option<i32>,
    #[graphql(name = "magnetUri")]
    pub(crate) magnet_uri: String,
    #[graphql(name = "metaVersion")]
    pub(crate) meta_version: Option<i32>,
    pub(crate) name: String,
    pub(crate) seeders: Option<i32>,
    pub(crate) single_file: Option<bool>,
    pub(crate) size: i32,
    pub(crate) sources: Vec<TorrentSourceInfo>,
    pub(crate) tag_names: Vec<String>,
    pub(crate) updated_at: DateTime,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentContent {
    pub(crate) content: Option<Content>,
    #[graphql(name = "contentId")]
    pub(crate) content_id: Option<String>,
    #[graphql(name = "contentSource")]
    pub(crate) content_source: Option<String>,
    #[graphql(name = "contentType")]
    pub(crate) content_type: Option<ContentType>,
    pub(crate) created_at: DateTime,
    #[graphql(name = "dhtFirstSeenAt")]
    pub(crate) dht_first_seen_at: Option<DateTime>,
    #[graphql(name = "dhtLastSeenAt")]
    pub(crate) dht_last_seen_at: Option<DateTime>,
    #[graphql(name = "dhtSeenCount")]
    pub(crate) dht_seen_count: i32,
    pub(crate) episodes: Option<Episodes>,
    #[graphql(name = "id")]
    pub(crate) id: ID,
    #[graphql(name = "infoHash")]
    pub(crate) info_hash: Hash20,
    pub(crate) languages: Option<Vec<LanguageInfo>>,
    pub(crate) leechers: Option<i32>,
    pub(crate) published_at: DateTime,
    pub(crate) release_group: Option<String>,
    pub(crate) seeders: Option<i32>,
    pub(crate) title: String,
    pub(crate) torrent: Torrent,
    pub(crate) updated_at: DateTime,
    #[graphql(name = "video3d")]
    pub(crate) video_3d: Option<Video3D>,
    pub(crate) video_codec: Option<VideoCodec>,
    pub(crate) video_modifier: Option<VideoModifier>,
    pub(crate) video_resolution: Option<VideoResolution>,
    pub(crate) video_source: Option<VideoSource>,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentContentAggregations {
    pub(crate) content_type: Option<Vec<ContentTypeAgg>>,
    pub(crate) genre: Option<Vec<GenreAgg>>,
    pub(crate) language: Option<Vec<LanguageAgg>>,
    pub(crate) release_year: Option<Vec<ReleaseYearAgg>>,
    pub(crate) torrent_file_type: Option<Vec<TorrentFileTypeAgg>>,
    pub(crate) torrent_source: Option<Vec<TorrentSourceAgg>>,
    pub(crate) torrent_tag: Option<Vec<TorrentTagAgg>>,
    pub(crate) video_resolution: Option<Vec<VideoResolutionAgg>>,
    pub(crate) video_source: Option<Vec<VideoSourceAgg>>,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentContentCollapsePathsResult {
    pub(crate) groups: Vec<TorrentContentPathGroup>,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentContentPathGroup {
    pub(crate) info_hashes: Vec<Hash20>,
    pub(crate) path: String,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentContentSearchResult {
    pub(crate) aggregations: TorrentContentAggregations,
    pub(crate) has_next_page: Option<bool>,
    pub(crate) items: Vec<TorrentContent>,
    pub(crate) total_count: i32,
    pub(crate) total_count_is_estimate: bool,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentFile {
    pub(crate) created_at: DateTime,
    pub(crate) extension: Option<String>,
    pub(crate) file_type: Option<FileType>,
    pub(crate) index: i32,
    #[graphql(name = "infoHash")]
    pub(crate) info_hash: Hash20,
    pub(crate) path: String,
    pub(crate) size: i32,
    pub(crate) updated_at: DateTime,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentFileTypeAgg {
    pub(crate) count: i32,
    pub(crate) is_estimate: bool,
    pub(crate) label: String,
    pub(crate) value: FileType,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentFilesQueryResult {
    pub(crate) has_next_page: Option<bool>,
    pub(crate) items: Vec<TorrentFile>,
    pub(crate) total_count: i32,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentListSourcesResult {
    pub(crate) sources: Vec<TorrentSource>,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentMetricsBucket {
    pub(crate) bucket: DateTime,
    pub(crate) count: i32,
    pub(crate) source: String,
    pub(crate) updated: bool,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentMetricsQueryResult {
    pub(crate) buckets: Vec<TorrentMetricsBucket>,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentSource {
    pub(crate) key: String,
    pub(crate) name: String,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentSourceAgg {
    pub(crate) count: i32,
    pub(crate) is_estimate: bool,
    pub(crate) label: String,
    pub(crate) value: String,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentSourceInfo {
    pub(crate) first_seen_at: DateTime,
    pub(crate) import_id: Option<String>,
    pub(crate) key: String,
    pub(crate) last_seen_at: DateTime,
    pub(crate) leechers: Option<i32>,
    pub(crate) name: String,
    pub(crate) seeders: Option<i32>,
    pub(crate) seen_count: i32,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentSuggestTagsResult {
    pub(crate) suggestions: Vec<SuggestedTag>,
}

#[derive(SimpleObject)]
pub(crate) struct TorrentTagAgg {
    pub(crate) count: i32,
    pub(crate) is_estimate: bool,
    pub(crate) label: String,
    pub(crate) value: String,
}

#[derive(SimpleObject)]
pub(crate) struct VideoResolutionAgg {
    pub(crate) count: i32,
    pub(crate) is_estimate: bool,
    pub(crate) label: String,
    pub(crate) value: Option<VideoResolution>,
}

#[derive(SimpleObject)]
pub(crate) struct VideoSourceAgg {
    pub(crate) count: i32,
    pub(crate) is_estimate: bool,
    pub(crate) label: String,
    pub(crate) value: Option<VideoSource>,
}

#[derive(SimpleObject)]
pub(crate) struct Worker {
    pub(crate) key: String,
    pub(crate) started: bool,
}

#[derive(SimpleObject)]
pub(crate) struct WorkersListAllQueryResult {
    pub(crate) workers: Vec<Worker>,
}
