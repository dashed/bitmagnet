//! Conversion and execution adapters for the real Lane-S search-query crate.

use async_trait::async_trait;
use bitmagnet_search_query as lane_s;

use super::search as local;

/// Errors returned by a real Lane-S PostgreSQL search backend.
#[derive(Debug, thiserror::Error)]
pub enum LaneSBackendError {
    /// Lane S rejected or failed the PostgreSQL search.
    #[error("Lane-S search failed: {0}")]
    Search(#[from] lane_s::SearchQueryError),
    /// A fake or alternative backend failed.
    #[error("Lane-S backend failed: {0}")]
    Backend(String),
}

/// Result alias for Lane-S adapter operations.
pub type Result<T> = std::result::Result<T, LaneSBackendError>;

/// Search dependency consumed by the GraphQL runtime adapter.
///
/// The trait carries the real Lane-S types so tests can assert that L2
/// hydration removes free text, pagination, counts and facets before it reaches
/// PostgreSQL.
#[async_trait]
pub trait LaneSSearchBackend: Send + Sync {
    /// Execute one complete Lane-S search.
    async fn search(
        &self,
        options: lane_s::SearchOptions,
        config: lane_s::SearchBuildConfig,
        hydrate: lane_s::HydrateOptions,
    ) -> Result<lane_s::SearchResult>;
}

/// SQLx-backed production implementation of [`LaneSSearchBackend`].
#[derive(Clone)]
pub struct SqlxLaneSSearchBackend {
    pool: bitmagnet_db::PgPool,
}

impl SqlxLaneSSearchBackend {
    /// Wrap a PostgreSQL pool.
    #[must_use]
    pub fn new(pool: bitmagnet_db::PgPool) -> Self {
        Self { pool }
    }

    /// Borrow the underlying pool.
    #[must_use]
    pub const fn pool(&self) -> &bitmagnet_db::PgPool {
        &self.pool
    }
}

#[async_trait]
impl LaneSSearchBackend for SqlxLaneSSearchBackend {
    async fn search(
        &self,
        options: lane_s::SearchOptions,
        config: lane_s::SearchBuildConfig,
        hydrate: lane_s::HydrateOptions,
    ) -> Result<lane_s::SearchResult> {
        Ok(lane_s::search(&self.pool, &options, &config, hydrate).await?)
    }
}

/// Convert the GraphQL seam's search options into the frozen Lane-S type.
#[must_use]
pub fn to_lane_s_search_options(options: local::SearchOptions) -> lane_s::SearchOptions {
    lane_s::SearchOptions {
        query: options.query,
        filter: options.filter.map(to_lane_s_criteria),
        order: options.order.into_iter().map(to_lane_s_order).collect(),
        facets: options.facets.into_iter().map(to_lane_s_facet).collect(),
        limit: options.limit,
        offset: options.offset,
        total_count: options.total_count,
        has_next_page: options.has_next_page,
        aggregation_budget: options.aggregation_budget,
    }
}

/// Convert GraphQL-owned builder switches into Lane-S switches.
#[must_use]
pub const fn to_lane_s_build_config(config: local::SearchBuildConfig) -> lane_s::SearchBuildConfig {
    lane_s::SearchBuildConfig {
        file_extensions_jsonb: config.file_extensions_jsonb,
        popularity_sort_default: config.popularity_sort_default,
    }
}

/// Convert GraphQL hydration requirements into Lane-S hydration requirements.
///
/// Lane S always hydrates the scalar torrent and content rows. Its only
/// optional hydration bit is the compressed file blob.
#[must_use]
pub const fn to_lane_s_hydrate_options(options: local::HydrateOptions) -> lane_s::HydrateOptions {
    lane_s::HydrateOptions {
        files_data: options.files_data,
        max_files_data_bytes: None,
    }
}

/// Convert a complete Lane-S result into the GraphQL seam result.
#[must_use]
pub fn from_lane_s_search_result(result: lane_s::SearchResult) -> local::SearchResult {
    local::SearchResult {
        total_count: result.total_count,
        total_count_is_estimate: result.total_count_is_estimate,
        has_next_page: result.has_next_page,
        items: result
            .items
            .into_iter()
            .map(from_lane_s_search_result_item)
            .collect(),
        aggregations: from_lane_s_aggregations(result.aggregations),
    }
}

/// Convert one expanded Lane-S row without dropping composer-owned
/// `refine_files`.
#[must_use]
pub fn from_lane_s_search_result_item(item: lane_s::SearchResultItem) -> local::SearchResultItem {
    local::SearchResultItem {
        info_hash: item.info_hash,
        name: item.name,
        size: item.size,
        content_type: item.content_type,
        published_at: item.published_at,
        seeders: item.seeders,
        leechers: item.leechers,
        files_count: item.files_count,
        video_resolution: item.video_resolution.map(|value| value.as_str().to_owned()),
        video_3d: item.video_3d.map(|value| value.as_str().to_owned()),
        video_codec: item.video_codec,
        release_group: item.release_group,
        episodes: item.episodes.0,
        release_year: item.release_year,
        imdb_id: item.imdb_id,
        tmdb_id: item.tmdb_id,
        info_hash_v1: item.info_hash_v1,
        info_hash_v2: item.info_hash_v2,
        torrent_content: item.torrent_content,
        torrent_content_video_modifier: item.torrent_content_video_modifier,
        torrent_content_created_at: item.torrent_content_created_at,
        torrent_content_updated_at: item.torrent_content_updated_at,
        torrent: item.torrent,
        refine_files: item.refine_files,
        torrent_created_at: item.torrent_created_at,
        torrent_updated_at: item.torrent_updated_at,
        torrent_meta_version: item.torrent_meta_version,
        torrent_sources: item
            .torrent_sources
            .into_iter()
            .map(|source| local::TorrentSourceInfo {
                key: source.key,
                name: source.name,
                import_id: source.import_id,
                seeders: source.seeders,
                leechers: source.leechers,
                seen_count: source.seen_count,
                first_seen_at: source.first_seen_at,
                last_seen_at: source.last_seen_at,
            })
            .collect(),
        torrent_tags: item.torrent_tags,
        content: item.content,
        title: item.title,
        dht_seen_count: item.dht_seen_count,
        dht_first_seen_at: item.dht_first_seen_at,
        dht_last_seen_at: item.dht_last_seen_at,
        query_string_rank: item.query_string_rank,
    }
}

/// Convert real Lane-S facet maps into the GraphQL seam representation.
#[must_use]
pub fn from_lane_s_aggregations(aggregations: lane_s::Aggregations) -> local::Aggregations {
    aggregations
        .into_iter()
        .map(|(key, group)| {
            (
                key,
                local::AggregationGroup {
                    label: group.label,
                    logic: match group.logic {
                        lane_s::FacetLogic::And => local::FacetLogic::And,
                        lane_s::FacetLogic::Or => local::FacetLogic::Or,
                    },
                    items: group
                        .items
                        .into_iter()
                        .map(|(value, item)| {
                            (
                                value,
                                local::AggregationItem {
                                    label: item.label,
                                    count: item.count,
                                    is_estimate: item.is_estimate,
                                },
                            )
                        })
                        .collect(),
                },
            )
        })
        .collect()
}

fn to_lane_s_criteria(criteria: local::Criteria) -> lane_s::Criteria {
    match criteria {
        local::Criteria::And(criteria) => {
            lane_s::Criteria::And(criteria.into_iter().map(to_lane_s_criteria).collect())
        }
        local::Criteria::SizeRange { min, max } => lane_s::Criteria::SizeRange { min, max },
        local::Criteria::PublishedAt(value) => lane_s::Criteria::PublishedAt(value),
        local::Criteria::TorrentContentInfoHashIn(values) => {
            lane_s::Criteria::TorrentContentInfoHashIn(values)
        }
    }
}

const fn to_lane_s_order(order: local::TorrentContentOrder) -> lane_s::TorrentContentOrder {
    lane_s::TorrentContentOrder {
        field: match order.field {
            local::TorrentContentOrderField::Relevance => {
                lane_s::TorrentContentOrderField::Relevance
            }
            local::TorrentContentOrderField::PublishedAt => {
                lane_s::TorrentContentOrderField::PublishedAt
            }
            local::TorrentContentOrderField::UpdatedAt => {
                lane_s::TorrentContentOrderField::UpdatedAt
            }
            local::TorrentContentOrderField::Size => lane_s::TorrentContentOrderField::Size,
            local::TorrentContentOrderField::FilesCount => {
                lane_s::TorrentContentOrderField::FilesCount
            }
            local::TorrentContentOrderField::Seeders => lane_s::TorrentContentOrderField::Seeders,
            local::TorrentContentOrderField::Leechers => lane_s::TorrentContentOrderField::Leechers,
            local::TorrentContentOrderField::Name => lane_s::TorrentContentOrderField::Name,
            local::TorrentContentOrderField::InfoHash => lane_s::TorrentContentOrderField::InfoHash,
        },
        direction: match order.direction {
            local::OrderDirection::Ascending => lane_s::OrderDirection::Ascending,
            local::OrderDirection::Descending => lane_s::OrderDirection::Descending,
        },
    }
}

fn to_lane_s_facet(facet: local::FacetRequest) -> lane_s::FacetRequest {
    lane_s::FacetRequest {
        facet: match facet.facet {
            local::TorrentContentFacet::ContentType => lane_s::TorrentContentFacet::ContentType,
            local::TorrentContentFacet::TorrentSource => lane_s::TorrentContentFacet::TorrentSource,
            local::TorrentContentFacet::TorrentTag => lane_s::TorrentContentFacet::TorrentTag,
            local::TorrentContentFacet::FileType => lane_s::TorrentContentFacet::FileType,
            local::TorrentContentFacet::Language => lane_s::TorrentContentFacet::Language,
            local::TorrentContentFacet::ContentGenre => lane_s::TorrentContentFacet::ContentGenre,
            local::TorrentContentFacet::ReleaseYear => lane_s::TorrentContentFacet::ReleaseYear,
            local::TorrentContentFacet::VideoResolution => {
                lane_s::TorrentContentFacet::VideoResolution
            }
            local::TorrentContentFacet::VideoSource => lane_s::TorrentContentFacet::VideoSource,
        },
        aggregate: facet.aggregate,
        logic: facet.logic.map(|logic| match logic {
            local::FacetLogic::And => lane_s::FacetLogic::And,
            local::FacetLogic::Or => lane_s::FacetLogic::Or,
        }),
        filter: facet.filter,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use bitmagnet_model::{BlobFile, InfoHash};

    use super::*;

    fn info_hash(digit: char) -> InfoHash {
        digit.to_string().repeat(40).parse().expect("valid hash")
    }

    #[test]
    fn converts_complete_local_request_to_real_lane_s_types() {
        let hash = info_hash('a');
        let options = local::SearchOptions {
            query: Some("ubuntu".to_owned()),
            filter: Some(local::Criteria::And(vec![
                local::Criteria::SizeRange {
                    min: Some(100),
                    max: Some(200),
                },
                local::Criteria::PublishedAt("P1Y".to_owned()),
                local::Criteria::TorrentContentInfoHashIn(vec![hash]),
            ])),
            order: vec![local::TorrentContentOrder {
                field: local::TorrentContentOrderField::Seeders,
                direction: local::OrderDirection::Descending,
            }],
            facets: vec![local::FacetRequest {
                facet: local::TorrentContentFacet::FileType,
                aggregate: true,
                logic: Some(local::FacetLogic::Or),
                filter: BTreeSet::from(["video".to_owned(), "audio".to_owned()]),
            }],
            limit: Some(17),
            offset: 34,
            total_count: true,
            has_next_page: true,
            aggregation_budget: 1_234.5,
        };

        let converted = to_lane_s_search_options(options);
        assert_eq!(converted.query.as_deref(), Some("ubuntu"));
        assert_eq!(
            converted.filter,
            Some(lane_s::Criteria::And(vec![
                lane_s::Criteria::SizeRange {
                    min: Some(100),
                    max: Some(200),
                },
                lane_s::Criteria::PublishedAt("P1Y".to_owned()),
                lane_s::Criteria::TorrentContentInfoHashIn(vec![hash]),
            ]))
        );
        assert_eq!(
            converted.order,
            [lane_s::TorrentContentOrder {
                field: lane_s::TorrentContentOrderField::Seeders,
                direction: lane_s::OrderDirection::Descending,
            }]
        );
        assert_eq!(converted.facets.len(), 1);
        assert_eq!(
            converted.facets[0].facet,
            lane_s::TorrentContentFacet::FileType
        );
        assert!(converted.facets[0].aggregate);
        assert_eq!(converted.facets[0].logic, Some(lane_s::FacetLogic::Or));
        assert_eq!(
            converted.facets[0].filter,
            BTreeSet::from(["audio".to_owned(), "video".to_owned()])
        );
        assert_eq!(converted.limit, Some(17));
        assert_eq!(converted.offset, 34);
        assert!(converted.total_count);
        assert!(converted.has_next_page);
        assert_eq!(converted.aggregation_budget, 1_234.5);

        assert_eq!(
            to_lane_s_build_config(local::SearchBuildConfig {
                file_extensions_jsonb: true,
                popularity_sort_default: false,
            }),
            lane_s::SearchBuildConfig {
                file_extensions_jsonb: true,
                popularity_sort_default: false,
            }
        );
        assert_eq!(
            to_lane_s_hydrate_options(local::HydrateOptions {
                torrent: true,
                content: true,
                files_data: true,
            }),
            lane_s::HydrateOptions {
                files_data: true,
                max_files_data_bytes: None,
            }
        );
    }

    #[test]
    fn converts_real_result_without_losing_refine_files_or_facets() {
        let hash = info_hash('b');
        let refine_file = BlobFile {
            index: 4,
            path: "show/episode.mkv".to_owned(),
            extension: "mkv".to_owned(),
            size: 900,
        };
        let mut item = lane_s::SearchResultItem::for_test(hash, "release", 1_000);
        item.refine_files = vec![refine_file.clone()];
        item.video_resolution = Some(lane_s::VideoResolution::V2160p);
        item.video_3d = Some(lane_s::Video3D::V3DSBS);
        item.episodes = lane_s::Episodes::new().add_episode(2, 3);
        item.release_group = Some("GROUP".to_owned());
        item.torrent_sources = vec![lane_s::TorrentSourceInfo {
            key: "dht".to_owned(),
            name: "DHT".to_owned(),
            import_id: Some("id".to_owned()),
            seeders: Some(5),
            leechers: Some(2),
            published_at: Some(99),
            seen_count: 7,
            first_seen_at: 10,
            last_seen_at: 20,
        }];

        let aggregations = BTreeMap::from([(
            "file_type".to_owned(),
            lane_s::AggregationGroup {
                label: "File type".to_owned(),
                logic: lane_s::FacetLogic::And,
                items: BTreeMap::from([(
                    "video".to_owned(),
                    lane_s::AggregationItem {
                        label: "Video".to_owned(),
                        count: 8,
                        is_estimate: true,
                    },
                )]),
            },
        )]);
        let converted = from_lane_s_search_result(lane_s::SearchResult {
            total_count: 11,
            total_count_is_estimate: true,
            has_next_page: true,
            items: vec![item],
            aggregations,
        });

        assert_eq!(converted.total_count, 11);
        assert!(converted.total_count_is_estimate);
        assert!(converted.has_next_page);
        assert_eq!(converted.items.len(), 1);
        let converted_item = &converted.items[0];
        assert_eq!(converted_item.info_hash, hash);
        assert_eq!(converted_item.refine_files, [refine_file]);
        assert_eq!(converted_item.video_resolution.as_deref(), Some("V2160p"));
        assert_eq!(converted_item.video_3d.as_deref(), Some("V3DSBS"));
        assert_eq!(converted_item.episodes.get(&2), Some(&vec![3]));
        assert_eq!(converted_item.release_group.as_deref(), Some("GROUP"));
        assert_eq!(converted_item.torrent_sources.len(), 1);
        assert_eq!(converted_item.torrent_sources[0].key, "dht");
        assert_eq!(converted_item.torrent_sources[0].seen_count, 7);

        let facet = converted
            .aggregations
            .get("file_type")
            .expect("file type aggregation");
        assert_eq!(facet.label, "File type");
        assert_eq!(facet.logic, local::FacetLogic::And);
        assert_eq!(facet.items["video"].label, "Video");
        assert_eq!(facet.items["video"].count, 8);
        assert!(facet.items["video"].is_estimate);
    }
}
