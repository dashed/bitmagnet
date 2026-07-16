//! Adapter from the GraphQL-owned search seam to the real Lane-C composer.

use std::sync::Arc;

use async_trait::async_trait;
use bitmagnet_search_serve as lane_c;

use super::lane_s::{
    from_lane_s_search_result, from_lane_s_search_result_item, to_lane_s_hydrate_options,
    to_lane_s_search_options,
};
use super::search::{
    self, FileFacetsRequest, FileFacetsResult, FilePathTypeaheadRequest, FileRow, FileRowsResult,
    FileSearchRequest, Filters, PathGroup, QueryOptions, SearchRequest, SearchResult,
    SearchRuntime,
};

/// GraphQL runtime decorator that delegates L3/L1 routes to Lane C and keeps
/// PostgreSQL/L2 operations on its underlying runtime.
pub struct LaneCSearchRuntime {
    base: Arc<dyn SearchRuntime>,
    lane_c: Arc<dyn lane_c::SearchServe>,
}

impl LaneCSearchRuntime {
    /// Compose an existing PostgreSQL/L2 runtime with a Lane-C serving facade.
    #[must_use]
    pub fn new(base: Arc<dyn SearchRuntime>, lane_c: Arc<dyn lane_c::SearchServe>) -> Self {
        Self { base, lane_c }
    }

    /// Box concrete runtime implementations and compose them.
    #[must_use]
    pub fn from_runtimes<B, C>(base: B, lane_c: C) -> Self
    where
        B: SearchRuntime + 'static,
        C: lane_c::SearchServe + 'static,
    {
        Self::new(Arc::new(base), Arc::new(lane_c))
    }
}

#[async_trait]
impl SearchRuntime for LaneCSearchRuntime {
    async fn pg_torrent_content(&self, request: SearchRequest) -> search::Result<SearchResult> {
        self.base.pg_torrent_content(request).await
    }

    async fn torrent_content(
        &self,
        filters: Filters,
        options: QueryOptions,
        limit: u32,
        offset: u32,
        sorts: Vec<bitmagnet_proto::v1::SortBy>,
    ) -> search::Result<(SearchResult, bool)> {
        let (result, served) = self
            .lane_c
            .torrent_content(
                to_lane_c_filters(filters),
                to_lane_c_query_options(options),
                limit,
                offset,
                sorts,
            )
            .await
            .map_err(lane_c_error)?;
        Ok((from_lane_s_search_result(result), served))
    }

    async fn collapse_paths(
        &self,
        filters: Filters,
        options: QueryOptions,
        limit: u32,
        offset: u32,
        sorts: Vec<bitmagnet_proto::v1::SortBy>,
    ) -> search::Result<(Vec<PathGroup>, bool)> {
        let (groups, served) = self
            .lane_c
            .collapse_paths(
                to_lane_c_filters(filters),
                to_lane_c_query_options(options),
                limit,
                offset,
                sorts,
            )
            .await
            .map_err(lane_c_error)?;
        Ok((
            groups
                .into_iter()
                .map(|group| PathGroup {
                    path: group.path,
                    info_hashes: group.info_hashes,
                })
                .collect(),
            served,
        ))
    }

    async fn search_file_rows(
        &self,
        filters: Filters,
        options: QueryOptions,
        limit: u32,
        offset: u32,
        sort_by: Vec<search::FileRowSort>,
    ) -> search::Result<(FileRowsResult, bool)> {
        let (result, served) = self
            .lane_c
            .search_file_rows(
                to_lane_c_filters(filters),
                to_lane_c_query_options(options),
                limit,
                offset,
                sort_by
                    .into_iter()
                    .map(|sort| lane_c::FileRowSort {
                        field: sort.field,
                        descending: sort.descending,
                    })
                    .collect(),
            )
            .await
            .map_err(lane_c_error)?;
        Ok((from_lane_c_file_rows(result), served))
    }

    async fn path_typeahead(
        &self,
        prefix: String,
        options: QueryOptions,
        limit: u32,
    ) -> search::Result<(Vec<String>, bool)> {
        self.lane_c
            .path_typeahead(prefix, to_lane_c_query_options(options), limit)
            .await
            .map_err(lane_c_error)
    }

    async fn suggest(&self, prefix: String, limit: u32) -> search::Result<(Vec<String>, bool)> {
        self.lane_c
            .suggest(prefix, limit)
            .await
            .map_err(lane_c_error)
    }

    async fn file_search(&self, request: FileSearchRequest) -> search::Result<FileRowsResult> {
        self.base.file_search(request).await
    }

    async fn file_search_facets(
        &self,
        request: FileFacetsRequest,
    ) -> search::Result<FileFacetsResult> {
        self.base.file_search_facets(request).await
    }

    async fn file_path_typeahead(
        &self,
        request: FilePathTypeaheadRequest,
    ) -> search::Result<Vec<String>> {
        self.base.file_path_typeahead(request).await
    }

    fn eligible(&self, query: &str) -> bool {
        self.lane_c.eligible(query)
    }

    fn healthy(&self) -> bool {
        self.lane_c.healthy()
    }

    fn typeahead_enabled(&self) -> bool {
        self.lane_c.typeahead_enabled()
    }

    fn file_search_route_text_enabled(&self) -> bool {
        self.lane_c.file_search_route_text_enabled()
    }

    fn collapse_enabled(&self) -> bool {
        self.lane_c.collapse_enabled()
    }

    fn features(&self) -> search::SearchFeatures {
        self.base.features()
    }

    fn search_build_config(&self) -> search::SearchBuildConfig {
        self.base.search_build_config()
    }
}

fn to_lane_c_filters(filters: Filters) -> lane_c::Filters {
    lane_c::Filters {
        query: filters.query,
        extensions: filters.extensions,
        min_size: filters.min_size,
        max_size: filters.max_size,
    }
}

fn to_lane_c_query_options(options: QueryOptions) -> lane_c::QueryOptions {
    lane_c::QueryOptions {
        combined: to_lane_c_search_request(options.combined),
        refine: options.refine.map(to_lane_c_search_request),
        agg: to_lane_c_search_request(options.agg),
        retain_refine_files: options.retain_refine_files,
    }
}

fn to_lane_c_search_request(request: SearchRequest) -> lane_c::SearchRequest {
    // Lane C's concrete PgSearch owns one stable build configuration. The
    // resolver-supplied build flags are therefore consumed by the composition
    // root, not copied into every request envelope.
    lane_c::SearchRequest::new(
        to_lane_s_search_options(request.options),
        to_lane_s_hydrate_options(request.hydrate),
    )
}

fn from_lane_c_file_rows(result: lane_c::FileRowsResult) -> FileRowsResult {
    FileRowsResult {
        rows: result
            .rows
            .into_iter()
            .map(|row| FileRow {
                info_hash: row.info_hash,
                index: row.index,
                path: row.path,
                extension: row.extension,
                size: row.size,
                torrent_content: from_lane_s_search_result_item(row.torrent_content),
            })
            .collect(),
        total_count: result.total_count,
        total_count_is_estimate: result.total_count_is_estimate,
        has_next_page: result.has_next_page,
    }
}

fn lane_c_error(error: lane_c::Error) -> search::Error {
    search::Error::Backend(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use bitmagnet_model::{BlobFile, InfoHash};

    use super::*;

    #[derive(Default)]
    struct BaseRuntime;

    #[async_trait]
    impl SearchRuntime for BaseRuntime {
        fn features(&self) -> search::SearchFeatures {
            search::SearchFeatures {
                file_search_enabled: true,
                file_search_facets_enabled: true,
                file_search_typeahead_rpc_enabled: false,
            }
        }

        fn search_build_config(&self) -> search::SearchBuildConfig {
            search::SearchBuildConfig {
                file_extensions_jsonb: true,
                popularity_sort_default: true,
            }
        }
    }

    struct RecordingLaneC {
        calls: Mutex<Vec<&'static str>>,
        fail_torrent: bool,
        info_hash: InfoHash,
    }

    #[async_trait]
    impl lane_c::SearchServe for RecordingLaneC {
        async fn torrent_content(
            &self,
            filters: lane_c::Filters,
            options: lane_c::QueryOptions,
            limit: u32,
            offset: u32,
            _sorts: Vec<bitmagnet_proto::v1::SortBy>,
        ) -> lane_c::Result<(lane_c::SearchResult, bool)> {
            self.calls.lock().expect("calls lock").push("torrent");
            if self.fail_torrent {
                return Err(lane_c::Error::Other("boom".to_owned()));
            }
            assert_eq!(filters.query, "needle");
            assert_eq!(options.combined.options.query.as_deref(), Some("needle"));
            assert_eq!(limit, 11);
            assert_eq!(offset, 3);
            let mut item = lane_c::SearchResultItem::for_test(self.info_hash, "torrent", 100);
            item.refine_files.push(BlobFile {
                index: 7,
                path: "dir/torrent.mkv".to_owned(),
                extension: "mkv".to_owned(),
                size: 99,
            });
            Ok((
                lane_c::SearchResult {
                    total_count: 1,
                    total_count_is_estimate: false,
                    has_next_page: false,
                    items: vec![item],
                    aggregations: lane_c::Aggregations::new(),
                },
                true,
            ))
        }

        async fn collapse_paths(
            &self,
            filters: lane_c::Filters,
            _options: lane_c::QueryOptions,
            _limit: u32,
            _offset: u32,
            _sorts: Vec<bitmagnet_proto::v1::SortBy>,
        ) -> lane_c::Result<(Vec<lane_c::PathGroup>, bool)> {
            self.calls.lock().expect("calls lock").push("collapse");
            assert_eq!(filters.extensions, ["mkv"]);
            Ok((
                vec![lane_c::PathGroup {
                    path: "dir/torrent.mkv".to_owned(),
                    info_hashes: vec![self.info_hash],
                }],
                true,
            ))
        }

        async fn search_file_rows(
            &self,
            _filters: lane_c::Filters,
            _options: lane_c::QueryOptions,
            _limit: u32,
            _offset: u32,
            sort_by: Vec<lane_c::FileRowSort>,
        ) -> lane_c::Result<(lane_c::FileRowsResult, bool)> {
            self.calls.lock().expect("calls lock").push("file_rows");
            assert_eq!(sort_by[0].field, "size");
            assert!(sort_by[0].descending);
            let mut item = lane_c::SearchResultItem::for_test(self.info_hash, "row", 100);
            item.refine_files.push(BlobFile {
                index: 8,
                path: "preserved.mkv".to_owned(),
                extension: "mkv".to_owned(),
                size: 88,
            });
            Ok((
                lane_c::FileRowsResult {
                    rows: vec![lane_c::FileRow {
                        info_hash: self.info_hash,
                        index: 2,
                        path: "dir/row.mkv".to_owned(),
                        extension: "mkv".to_owned(),
                        size: 77,
                        torrent_content: item,
                    }],
                    total_count: 9,
                    total_count_is_estimate: true,
                    has_next_page: true,
                },
                true,
            ))
        }

        async fn path_typeahead(
            &self,
            prefix: String,
            _options: lane_c::QueryOptions,
            limit: u32,
        ) -> lane_c::Result<(Vec<String>, bool)> {
            self.calls.lock().expect("calls lock").push("typeahead");
            assert_eq!(prefix, "dir/");
            assert_eq!(limit, 5);
            Ok((vec!["dir/torrent.mkv".to_owned()], true))
        }

        async fn suggest(&self, prefix: String, limit: u32) -> lane_c::Result<(Vec<String>, bool)> {
            self.calls.lock().expect("calls lock").push("suggest");
            assert_eq!(prefix, "tor");
            assert_eq!(limit, 4);
            Ok((vec!["torrent".to_owned()], true))
        }

        fn eligible(&self, query: &str) -> bool {
            query.len() >= 3
        }

        fn healthy(&self) -> bool {
            true
        }

        fn typeahead_enabled(&self) -> bool {
            true
        }

        fn file_search_route_text_enabled(&self) -> bool {
            true
        }

        fn collapse_enabled(&self) -> bool {
            true
        }
    }

    fn info_hash() -> InfoHash {
        "1111111111111111111111111111111111111111"
            .parse()
            .expect("valid hash")
    }

    fn local_query_options() -> QueryOptions {
        let request = SearchRequest {
            options: search::SearchOptions {
                query: Some("needle".to_owned()),
                ..search::SearchOptions::default()
            },
            build: search::SearchBuildConfig::default(),
            hydrate: search::HydrateOptions {
                torrent: true,
                content: true,
                files_data: true,
            },
        };
        QueryOptions {
            combined: request.clone(),
            refine: Some(request.clone()),
            agg: request,
            retain_refine_files: false,
        }
    }

    #[test]
    fn converts_all_composer_request_shapes_to_real_lane_s_types() {
        let search_request = SearchRequest {
            options: search::SearchOptions {
                query: Some("needle".to_owned()),
                limit: Some(37),
                offset: 9,
                total_count: true,
                ..search::SearchOptions::default()
            },
            build: search::SearchBuildConfig {
                file_extensions_jsonb: true,
                popularity_sort_default: true,
            },
            hydrate: search::HydrateOptions {
                torrent: true,
                content: true,
                files_data: true,
            },
        };
        let converted = to_lane_c_query_options(QueryOptions {
            combined: search_request.clone(),
            refine: Some(search_request.clone()),
            agg: search_request,
            retain_refine_files: true,
        });

        assert_eq!(converted.combined.options.query.as_deref(), Some("needle"));
        assert_eq!(converted.combined.options.limit, Some(37));
        assert_eq!(converted.combined.options.offset, 9);
        assert!(converted.combined.options.total_count);
        assert!(converted.combined.hydrate.files_data);
        assert!(converted.refine.expect("refine request").hydrate.files_data);
        assert!(converted.agg.hydrate.files_data);
        assert!(converted.retain_refine_files);
    }

    #[test]
    fn disabled_lane_c_keeps_base_features_and_builder_flags() {
        let runtime = LaneCSearchRuntime::from_runtimes(BaseRuntime, lane_c::Disabled);

        assert!(runtime.features().file_search_enabled);
        assert!(runtime.features().file_search_facets_enabled);
        assert!(runtime.search_build_config().file_extensions_jsonb);
        assert!(runtime.search_build_config().popularity_sort_default);
        assert!(!runtime.eligible("needle"));
        assert!(!runtime.healthy());
        assert!(!runtime.typeahead_enabled());
        assert!(!runtime.file_search_route_text_enabled());
        assert!(!runtime.collapse_enabled());
    }

    #[tokio::test]
    async fn delegates_all_lane_c_routes_gates_and_canonical_results() {
        let lane_c = Arc::new(RecordingLaneC {
            calls: Mutex::new(Vec::new()),
            fail_torrent: false,
            info_hash: info_hash(),
        });
        let runtime = LaneCSearchRuntime::new(Arc::new(BaseRuntime), lane_c.clone());
        let filters = Filters {
            query: "needle".to_owned(),
            extensions: vec!["mkv".to_owned()],
            min_size: 10,
            max_size: 100,
        };

        let (result, served) = runtime
            .torrent_content(filters.clone(), local_query_options(), 11, 3, Vec::new())
            .await
            .expect("torrent route");
        assert!(served);
        assert_eq!(result.total_count, 1);
        assert_eq!(result.items[0].refine_files[0].path, "dir/torrent.mkv");

        let (groups, served) = runtime
            .collapse_paths(filters.clone(), local_query_options(), 10, 0, Vec::new())
            .await
            .expect("collapse route");
        assert!(served);
        assert_eq!(groups[0].info_hashes, [info_hash()]);

        let (rows, served) = runtime
            .search_file_rows(
                filters,
                local_query_options(),
                10,
                0,
                vec![search::FileRowSort {
                    field: "size".to_owned(),
                    descending: true,
                }],
            )
            .await
            .expect("file-row route");
        assert!(served);
        assert!(rows.total_count_is_estimate);
        assert!(rows.has_next_page);
        assert_eq!(
            rows.rows[0].torrent_content.refine_files[0].path,
            "preserved.mkv"
        );

        let (typeahead, served) = runtime
            .path_typeahead("dir/".to_owned(), local_query_options(), 5)
            .await
            .expect("path typeahead route");
        assert!(served);
        assert_eq!(typeahead, ["dir/torrent.mkv"]);

        let (suggestions, served) = runtime
            .suggest("tor".to_owned(), 4)
            .await
            .expect("suggest route");
        assert!(served);
        assert_eq!(suggestions, ["torrent"]);

        assert!(runtime.eligible("needle"));
        assert!(runtime.healthy());
        assert!(runtime.typeahead_enabled());
        assert!(runtime.file_search_route_text_enabled());
        assert!(runtime.collapse_enabled());
        assert_eq!(
            *lane_c.calls.lock().expect("calls lock"),
            ["torrent", "collapse", "file_rows", "typeahead", "suggest"]
        );
    }

    #[tokio::test]
    async fn maps_lane_c_errors_to_graphql_backend_errors() {
        let runtime = LaneCSearchRuntime::new(
            Arc::new(BaseRuntime),
            Arc::new(RecordingLaneC {
                calls: Mutex::new(Vec::new()),
                fail_torrent: true,
                info_hash: info_hash(),
            }),
        );
        let error = runtime
            .torrent_content(
                Filters {
                    query: "needle".to_owned(),
                    ..Filters::default()
                },
                local_query_options(),
                11,
                3,
                Vec::new(),
            )
            .await
            .expect_err("lane-c failure must cross the runtime seam");

        assert!(matches!(error, search::Error::Backend(message) if message == "boom"));
    }
}
