//! Real PostgreSQL plus L2 implementation of the GraphQL search-runtime seam.
//!
//! Lane-C construction remains intentionally outside this module. The final
//! composition root may use this runtime directly for PG/L2-only operation or
//! reuse its public hydration helper inside a fuller decorator.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use bitmagnet_model::InfoHash;
use bitmagnet_search_query as lane_s_query;

use super::file_search_client::{FileSearchClientError, L2FileRowsResult, L2FileSearchBackend};
use super::lane_s::{
    from_lane_s_search_result, from_lane_s_search_result_item, to_lane_s_build_config,
    to_lane_s_hydrate_options, to_lane_s_search_options, LaneSBackendError, LaneSSearchBackend,
};
use super::search::{
    self, FileFacetsRequest, FileFacetsResult, FilePathTypeaheadRequest, FileRow, FileRowsResult,
    FileSearchRequest, SearchBuildConfig, SearchFeatures, SearchRequest, SearchResult,
    SearchRuntime,
};

/// Errors while adapting PG/L2 results to the GraphQL runtime.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeAdapterError {
    /// Lane-S PostgreSQL search failed.
    #[error(transparent)]
    LaneS(#[from] LaneSBackendError),
    /// The L2 client failed.
    #[error(transparent)]
    L2(#[from] FileSearchClientError),
    /// L2 returned a hit whose torrent-content row Lane S did not hydrate.
    #[error("file search torrent content missing for info_hash {0}")]
    MissingHydration(InfoHash),
}

/// PG/L2 runtime components and feature settings.
pub struct PgL2SearchRuntime {
    lane_s: Arc<dyn LaneSSearchBackend>,
    l2: Arc<dyn L2FileSearchBackend>,
    features: SearchFeatures,
    build_config: SearchBuildConfig,
}

impl PgL2SearchRuntime {
    /// Compose already-boxed production or test backends.
    #[must_use]
    pub fn new(
        lane_s: Arc<dyn LaneSSearchBackend>,
        l2: Arc<dyn L2FileSearchBackend>,
        features: SearchFeatures,
        build_config: SearchBuildConfig,
    ) -> Self {
        Self {
            lane_s,
            l2,
            features,
            build_config,
        }
    }

    /// Box concrete backends and compose the runtime.
    #[must_use]
    pub fn from_backends<S, L>(
        lane_s: S,
        l2: L,
        features: SearchFeatures,
        build_config: SearchBuildConfig,
    ) -> Self
    where
        S: LaneSSearchBackend + 'static,
        L: L2FileSearchBackend + 'static,
    {
        Self::new(Arc::new(lane_s), Arc::new(l2), features, build_config)
    }

    /// Borrow the Lane-S backend for a larger composition root.
    #[must_use]
    pub fn lane_s(&self) -> &(dyn LaneSSearchBackend + 'static) {
        self.lane_s.as_ref()
    }

    /// Borrow the L2 backend for a larger composition root.
    #[must_use]
    pub fn l2(&self) -> &(dyn L2FileSearchBackend + 'static) {
        self.l2.as_ref()
    }
}

#[async_trait]
impl SearchRuntime for PgL2SearchRuntime {
    async fn pg_torrent_content(&self, request: SearchRequest) -> search::Result<SearchResult> {
        let result = self
            .lane_s
            .search(
                to_lane_s_search_options(request.options),
                to_lane_s_build_config(request.build),
                to_lane_s_hydrate_options(request.hydrate),
            )
            .await
            .map_err(runtime_error)?;
        Ok(from_lane_s_search_result(result))
    }

    async fn file_search(&self, request: FileSearchRequest) -> search::Result<FileRowsResult> {
        let rows = self
            .l2
            .search_files(&request)
            .await
            .map_err(runtime_error)?;
        hydrate_l2_file_rows(self.lane_s.as_ref(), rows, self.build_config)
            .await
            .map_err(runtime_error)
    }

    async fn file_search_facets(
        &self,
        request: FileFacetsRequest,
    ) -> search::Result<FileFacetsResult> {
        self.l2.facets(&request).await.map_err(runtime_error)
    }

    async fn file_path_typeahead(
        &self,
        request: FilePathTypeaheadRequest,
    ) -> search::Result<Vec<String>> {
        self.l2
            .path_typeahead(&request)
            .await
            .map_err(runtime_error)
    }

    fn features(&self) -> SearchFeatures {
        self.features
    }

    fn search_build_config(&self) -> SearchBuildConfig {
        self.build_config
    }
}

/// Hydrate one L2 page in exactly one deduplicated Lane-S query, then restore
/// the original file-hit order and duplicates.
pub async fn hydrate_l2_file_rows(
    lane_s: &dyn LaneSSearchBackend,
    result: L2FileRowsResult,
    build_config: SearchBuildConfig,
) -> std::result::Result<FileRowsResult, RuntimeAdapterError> {
    if result.hits.is_empty() {
        return Ok(FileRowsResult {
            rows: Vec::new(),
            total_count: result.total_count,
            total_count_is_estimate: result.total_count_is_estimate,
            has_next_page: result.has_next_page,
        });
    }

    let mut seen = HashSet::with_capacity(result.hits.len());
    let info_hashes = result
        .hits
        .iter()
        .filter_map(|hit| seen.insert(hit.info_hash).then_some(hit.info_hash))
        .collect::<Vec<_>>();

    // This request is an identity hydration pass, not another search: no free
    // text, default LIMIT, offset, ordering, counts, next-page probe or facets.
    let hydration = lane_s
        .search(
            lane_s_query::SearchOptions {
                query: None,
                filter: Some(lane_s_query::Criteria::TorrentContentInfoHashIn(
                    info_hashes,
                )),
                order: Vec::new(),
                facets: Vec::new(),
                limit: None,
                offset: 0,
                total_count: false,
                has_next_page: false,
                aggregation_budget: 5_000.0,
            },
            to_lane_s_build_config(build_config),
            lane_s_query::HydrateOptions {
                files_data: false,
                max_files_data_bytes: None,
            },
        )
        .await?;

    // Match Go's map assignment semantics when a torrent has several
    // classification rows: the last hydrated row for a hash wins.
    let mut by_info_hash = HashMap::with_capacity(hydration.items.len());
    for item in hydration.items {
        by_info_hash.insert(item.info_hash, from_lane_s_search_result_item(item));
    }

    let rows = result
        .hits
        .into_iter()
        .map(|hit| {
            let torrent_content = by_info_hash
                .get(&hit.info_hash)
                .cloned()
                .ok_or(RuntimeAdapterError::MissingHydration(hit.info_hash))?;
            Ok(FileRow {
                info_hash: hit.info_hash,
                index: hit.index,
                path: hit.path,
                extension: hit.extension,
                size: hit.size,
                torrent_content,
            })
        })
        .collect::<std::result::Result<Vec<_>, RuntimeAdapterError>>()?;

    Ok(FileRowsResult {
        rows,
        total_count: result.total_count,
        total_count_is_estimate: result.total_count_is_estimate,
        has_next_page: result.has_next_page,
    })
}

fn runtime_error(error: impl std::fmt::Display) -> search::Error {
    search::Error::Backend(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use bitmagnet_model::BlobFile;

    use super::*;
    use crate::schema::file_search_client::L2FileHit;

    struct LaneSCall {
        options: lane_s_query::SearchOptions,
        config: lane_s_query::SearchBuildConfig,
        hydrate: lane_s_query::HydrateOptions,
    }

    struct FakeLaneSBackend {
        calls: Mutex<Vec<LaneSCall>>,
        response: Mutex<Option<lane_s_query::SearchResult>>,
    }

    impl FakeLaneSBackend {
        fn new(response: lane_s_query::SearchResult) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                response: Mutex::new(Some(response)),
            }
        }
    }

    #[async_trait]
    impl LaneSSearchBackend for FakeLaneSBackend {
        async fn search(
            &self,
            options: lane_s_query::SearchOptions,
            config: lane_s_query::SearchBuildConfig,
            hydrate: lane_s_query::HydrateOptions,
        ) -> super::super::lane_s::Result<lane_s_query::SearchResult> {
            self.calls.lock().expect("calls lock").push(LaneSCall {
                options,
                config,
                hydrate,
            });
            self.response
                .lock()
                .expect("response lock")
                .take()
                .ok_or_else(|| LaneSBackendError::Backend("no fake response".to_owned()))
        }
    }

    fn info_hash(digit: char) -> InfoHash {
        digit.to_string().repeat(40).parse().expect("valid hash")
    }

    fn search_result(items: Vec<lane_s_query::SearchResultItem>) -> lane_s_query::SearchResult {
        lane_s_query::SearchResult {
            total_count: 0,
            total_count_is_estimate: false,
            has_next_page: false,
            items,
            aggregations: BTreeMap::new(),
        }
    }

    fn hit(info_hash: InfoHash, index: u32, path: &str) -> L2FileHit {
        L2FileHit {
            info_hash,
            index,
            path: path.to_owned(),
            extension: "mkv".to_owned(),
            size: u64::from(index) * 100,
        }
    }

    #[tokio::test]
    async fn hydration_deduplicates_pg_query_and_restores_hit_order_and_duplicates() {
        let first_hash = info_hash('1');
        let second_hash = info_hash('2');

        let mut old_second =
            lane_s_query::SearchResultItem::for_test(second_hash, "old second", 200);
        old_second.release_group = Some("OLD".to_owned());
        let mut first = lane_s_query::SearchResultItem::for_test(first_hash, "first", 100);
        first.refine_files = vec![BlobFile {
            index: 9,
            path: "full/first.mkv".to_owned(),
            extension: "mkv".to_owned(),
            size: 999,
        }];
        let mut new_second =
            lane_s_query::SearchResultItem::for_test(second_hash, "new second", 201);
        new_second.release_group = Some("NEW".to_owned());

        let backend = FakeLaneSBackend::new(search_result(vec![old_second, first, new_second]));
        let result = hydrate_l2_file_rows(
            &backend,
            L2FileRowsResult {
                hits: vec![
                    hit(second_hash, 2, "second-a.mkv"),
                    hit(first_hash, 1, "first.mkv"),
                    hit(second_hash, 3, "second-b.mkv"),
                ],
                total_count: 77,
                total_count_is_estimate: true,
                has_next_page: true,
            },
            SearchBuildConfig {
                file_extensions_jsonb: true,
                popularity_sort_default: true,
            },
        )
        .await
        .expect("hydrate rows");

        let calls = backend.calls.lock().expect("calls lock");
        assert_eq!(calls.len(), 1);
        let call = &calls[0];
        assert_eq!(call.options.query, None);
        assert_eq!(
            call.options.filter,
            Some(lane_s_query::Criteria::TorrentContentInfoHashIn(vec![
                second_hash,
                first_hash,
            ]))
        );
        assert!(call.options.order.is_empty());
        assert!(call.options.facets.is_empty());
        assert_eq!(call.options.limit, None);
        assert_eq!(call.options.offset, 0);
        assert!(!call.options.total_count);
        assert!(!call.options.has_next_page);
        assert_eq!(call.options.aggregation_budget, 5_000.0);
        assert_eq!(
            call.config,
            lane_s_query::SearchBuildConfig {
                file_extensions_jsonb: true,
                popularity_sort_default: true,
            }
        );
        assert_eq!(
            call.hydrate,
            lane_s_query::HydrateOptions {
                files_data: false,
                max_files_data_bytes: None,
            }
        );
        drop(calls);

        assert_eq!(result.total_count, 77);
        assert!(result.total_count_is_estimate);
        assert!(result.has_next_page);
        assert_eq!(result.rows.len(), 3);
        assert_eq!(
            result
                .rows
                .iter()
                .map(|row| row.info_hash)
                .collect::<Vec<_>>(),
            [second_hash, first_hash, second_hash]
        );
        assert_eq!(result.rows[0].path, "second-a.mkv");
        assert_eq!(result.rows[1].path, "first.mkv");
        assert_eq!(result.rows[2].path, "second-b.mkv");
        assert_eq!(result.rows[0].torrent_content.name, "new second");
        assert_eq!(result.rows[2].torrent_content.name, "new second");
        assert_eq!(
            result.rows[0].torrent_content.release_group.as_deref(),
            Some("NEW")
        );
        assert_eq!(result.rows[1].torrent_content.refine_files.len(), 1);
        assert_eq!(
            result.rows[1].torrent_content.refine_files[0].path,
            "full/first.mkv"
        );
        assert_eq!(
            result.rows[0].torrent_content,
            result.rows[2].torrent_content
        );
    }

    #[tokio::test]
    async fn empty_page_skips_pg_and_missing_hydration_fails_closed() {
        let hash = info_hash('a');
        let empty_backend = FakeLaneSBackend::new(search_result(Vec::new()));
        let empty = hydrate_l2_file_rows(
            &empty_backend,
            L2FileRowsResult {
                hits: Vec::new(),
                total_count: 4,
                total_count_is_estimate: false,
                has_next_page: false,
            },
            SearchBuildConfig::default(),
        )
        .await
        .expect("empty page");
        assert!(empty.rows.is_empty());
        assert_eq!(empty.total_count, 4);
        assert!(empty_backend.calls.lock().expect("calls lock").is_empty());

        let missing_backend = FakeLaneSBackend::new(search_result(Vec::new()));
        let error = hydrate_l2_file_rows(
            &missing_backend,
            L2FileRowsResult {
                hits: vec![hit(hash, 1, "missing.mkv")],
                ..L2FileRowsResult::default()
            },
            SearchBuildConfig::default(),
        )
        .await
        .expect_err("missing identity hydration must fail");
        assert!(matches!(
            error,
            RuntimeAdapterError::MissingHydration(value) if value == hash
        ));
    }
}
