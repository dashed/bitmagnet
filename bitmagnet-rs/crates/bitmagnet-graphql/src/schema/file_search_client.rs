//! Bounded tonic client for the L2 `FileSearchService`.

use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use bitmagnet_model::InfoHash;
use bitmagnet_proto::v1::{
    CountFilesRequest, FacetsRequest, FacetsResponse, FileFilters, FilePagination, FileSortBy,
    SearchFilesRequest, SearchFilesResponse,
};
use bitmagnet_proto::FileSearchServiceClient;
use tonic::transport::{Channel, Endpoint};
use tonic::Status;

use super::search::{
    FileFacet, FileFacetBucket, FileFacetsRequest, FileFacetsResult, FilePathTypeaheadRequest,
    FileSearchRequest,
};

/// Maximum `offset + limit` emulated through one cursorless L2 request.
pub const MAX_L2_FILE_WINDOW: u32 = 500;
const DEFAULT_FILE_LIMIT: u32 = 20;

/// Configuration for the production tonic file-search client.
#[derive(Debug, Clone)]
pub struct FileSearchClientConfig {
    /// Tonic endpoint, such as `http://bitmagnet-filesearch.bitmagnet.svc:50052`.
    pub endpoint: String,
    /// Hard deadline for one logical operation. `SearchFiles` and its optional
    /// count share it; zero leaves the deadline to the caller context.
    pub timeout: Duration,
    /// Maximum cursorless `offset + limit` window. Zero selects the safe
    /// default of [`MAX_L2_FILE_WINDOW`]; larger values are rejected.
    pub max_rows: u32,
}

impl FileSearchClientConfig {
    /// Build a client configuration.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, timeout: Duration) -> Self {
        Self {
            endpoint: endpoint.into(),
            timeout,
            max_rows: 0,
        }
    }

    /// Override the cursorless result window.
    #[must_use]
    pub const fn with_max_rows(mut self, max_rows: u32) -> Self {
        self.max_rows = max_rows;
        self
    }
}

/// L2 client and protocol errors.
#[derive(Debug, thiserror::Error)]
pub enum FileSearchClientError {
    /// The L2 backend construction switch is off.
    #[error("file search is disabled")]
    Disabled,
    /// The endpoint was empty.
    #[error("filesearch: empty endpoint")]
    EmptyEndpoint,
    /// Tonic rejected the endpoint URI.
    #[error("filesearch: invalid endpoint {endpoint:?}: {message}")]
    InvalidEndpoint {
        /// Rejected endpoint.
        endpoint: String,
        /// Parser error.
        message: String,
    },
    /// The configured cursorless window exceeded the absolute safety ceiling.
    #[error(
        "filesearch: max_rows exceeds the absolute safety ceiling: max_rows={max_rows} maximum={maximum}"
    )]
    MaxRowsExceeded {
        /// Rejected configured value.
        max_rows: u32,
        /// Absolute client-side safety ceiling.
        maximum: u32,
    },
    /// The current proto cannot represent an info-hash restriction.
    #[error("file search info_hash filter is not supported by the sidecar")]
    InfoHashUnsupported,
    /// Cursorless offset emulation would exceed the bounded window.
    #[error(
        "file search offset exceeds the sidecar result window: offset={offset} limit={limit} max_rows={max_rows}"
    )]
    OffsetUnsupported {
        /// Requested offset.
        offset: u32,
        /// Requested limit after defaulting.
        limit: u32,
        /// Hard client-side window.
        max_rows: u32,
    },
    /// One sidecar row contained a malformed info hash.
    #[error("filesearch: parse info_hash {value:?}: {message}")]
    InvalidInfoHash {
        /// Malformed value.
        value: String,
        /// Parser error.
        message: String,
    },
    /// A unary RPC exceeded its hard timeout.
    #[error("filesearch: {operation} timed out after {timeout:?}")]
    Timeout {
        /// RPC operation.
        operation: &'static str,
        /// Configured timeout.
        timeout: Duration,
    },
    /// The sidecar returned a gRPC status.
    #[error("filesearch: {operation} RPC failed: {source}")]
    Rpc {
        /// RPC operation.
        operation: &'static str,
        /// gRPC status.
        #[source]
        source: Status,
    },
    /// L2 has no path-typeahead RPC; Go returns the same explicit failure.
    #[error("path typeahead is not supported by the sidecar")]
    PathTypeaheadUnsupported,
}

/// Result alias for L2 client operations.
pub type Result<T> = std::result::Result<T, FileSearchClientError>;

/// One unhydrated file hit returned by L2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2FileHit {
    /// Parent torrent hash.
    pub info_hash: InfoHash,
    /// Zero-based file index.
    pub index: u32,
    /// File path.
    pub path: String,
    /// Lowercase extension or an empty string.
    pub extension: String,
    /// File size in bytes.
    pub size: u64,
}

/// L2 file page before Lane-S torrent-content hydration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct L2FileRowsResult {
    /// Ordered hits, including repeated torrent hashes.
    pub hits: Vec<L2FileHit>,
    /// Exact or estimated total count.
    pub total_count: u64,
    /// Whether the count RPC reported an estimate.
    pub total_count_is_estimate: bool,
    /// Whether the sidecar reports another keyset page.
    pub has_next_page: bool,
}

/// Fakeable subset of the generated tonic client.
#[async_trait]
pub trait FileSearchRpc: Send + Sync {
    /// Call `SearchFiles`.
    async fn search_files(
        &self,
        request: SearchFilesRequest,
    ) -> std::result::Result<SearchFilesResponse, Status>;

    /// Call `CountFiles`.
    async fn count_files(
        &self,
        request: CountFilesRequest,
    ) -> std::result::Result<bitmagnet_proto::v1::CountFilesResponse, Status>;

    /// Call `Facets`.
    async fn facets(&self, request: FacetsRequest) -> std::result::Result<FacetsResponse, Status>;
}

/// Generated tonic RPC implementation.
#[derive(Clone)]
pub struct TonicFileSearchRpc {
    client: FileSearchServiceClient<Channel>,
}

impl TonicFileSearchRpc {
    /// Wrap an already-created tonic channel.
    #[must_use]
    pub fn new(channel: Channel) -> Self {
        Self {
            client: FileSearchServiceClient::new(channel),
        }
    }
}

#[async_trait]
impl FileSearchRpc for TonicFileSearchRpc {
    async fn search_files(
        &self,
        request: SearchFilesRequest,
    ) -> std::result::Result<SearchFilesResponse, Status> {
        let mut client = self.client.clone();
        Ok(client.search_files(request).await?.into_inner())
    }

    async fn count_files(
        &self,
        request: CountFilesRequest,
    ) -> std::result::Result<bitmagnet_proto::v1::CountFilesResponse, Status> {
        let mut client = self.client.clone();
        Ok(client.count_files(request).await?.into_inner())
    }

    async fn facets(&self, request: FacetsRequest) -> std::result::Result<FacetsResponse, Status> {
        let mut client = self.client.clone();
        Ok(client.facets(request).await?.into_inner())
    }
}

/// Transport-neutral L2 operations consumed by the GraphQL runtime.
#[async_trait]
pub trait L2FileSearchBackend: Send + Sync {
    /// Search uncollapsed file rows.
    async fn search_files(&self, request: &FileSearchRequest) -> Result<L2FileRowsResult>;

    /// Aggregate file facets.
    async fn facets(&self, request: &FileFacetsRequest) -> Result<FileFacetsResult>;

    /// Attempt path typeahead. The current L2 proto deliberately returns
    /// [`FileSearchClientError::PathTypeaheadUnsupported`].
    async fn path_typeahead(&self, request: &FilePathTypeaheadRequest) -> Result<Vec<String>>;
}

/// Fail-closed L2 backend used when `SEARCH_FILE_SEARCH_ENABLED=false`.
#[derive(Debug, Clone, Copy, Default)]
pub struct DisabledFileSearchBackend;

#[async_trait]
impl L2FileSearchBackend for DisabledFileSearchBackend {
    async fn search_files(&self, _request: &FileSearchRequest) -> Result<L2FileRowsResult> {
        Err(FileSearchClientError::Disabled)
    }

    async fn facets(&self, _request: &FileFacetsRequest) -> Result<FileFacetsResult> {
        Err(FileSearchClientError::Disabled)
    }

    async fn path_typeahead(&self, _request: &FilePathTypeaheadRequest) -> Result<Vec<String>> {
        Err(FileSearchClientError::Disabled)
    }
}

/// Bounded L2 client over a fakeable RPC implementation.
pub struct TonicFileSearchClient<R = TonicFileSearchRpc> {
    rpc: R,
    timeout: Duration,
    max_rows: u32,
}

impl TonicFileSearchClient<TonicFileSearchRpc> {
    /// Create a lazy production channel. A missing URI scheme is normalized to
    /// `http://`, matching the in-cluster plaintext service.
    pub fn connect(config: FileSearchClientConfig) -> Result<Self> {
        let max_rows = normalize_max_rows(config.max_rows)?;
        let endpoint = normalize_endpoint(&config.endpoint)?;
        let channel = Endpoint::from_shared(endpoint.clone())
            .map_err(|error| FileSearchClientError::InvalidEndpoint {
                endpoint,
                message: error.to_string(),
            })?
            .connect_lazy();
        Self::with_rpc_and_max_rows(TonicFileSearchRpc::new(channel), config.timeout, max_rows)
    }
}

impl<R> TonicFileSearchClient<R> {
    /// Wrap an RPC implementation, primarily for deterministic tests.
    pub fn with_rpc(rpc: R, timeout: Duration) -> Result<Self> {
        Self::with_rpc_and_max_rows(rpc, timeout, 0)
    }

    /// Wrap an RPC implementation with an explicit cursorless window.
    pub fn with_rpc_and_max_rows(rpc: R, timeout: Duration, max_rows: u32) -> Result<Self> {
        Ok(Self {
            rpc,
            timeout,
            max_rows: normalize_max_rows(max_rows)?,
        })
    }

    /// Return the normalized cursorless result window.
    #[must_use]
    pub const fn max_rows(&self) -> u32 {
        self.max_rows
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        if self.timeout.is_zero() {
            None
        } else {
            let now = tokio::time::Instant::now();
            Some(now.checked_add(self.timeout).unwrap_or(now))
        }
    }

    async fn timed_at<T, F>(
        &self,
        operation: &'static str,
        deadline: Option<tokio::time::Instant>,
        future: F,
    ) -> Result<T>
    where
        F: Future<Output = std::result::Result<T, Status>>,
    {
        let result = if let Some(deadline) = deadline {
            tokio::time::timeout_at(deadline, future)
                .await
                .map_err(|_| FileSearchClientError::Timeout {
                    operation,
                    timeout: self.timeout,
                })?
        } else {
            future.await
        };
        result.map_err(|source| FileSearchClientError::Rpc { operation, source })
    }
}

#[async_trait]
impl<R> L2FileSearchBackend for TonicFileSearchClient<R>
where
    R: FileSearchRpc,
{
    async fn search_files(&self, request: &FileSearchRequest) -> Result<L2FileRowsResult> {
        if request.info_hash.is_some() {
            return Err(FileSearchClientError::InfoHashUnsupported);
        }

        let limit = if request.limit == 0 {
            DEFAULT_FILE_LIMIT
        } else {
            request.limit
        };
        let request_limit = request
            .offset
            .checked_add(limit)
            .filter(|window| *window <= self.max_rows && request.offset <= self.max_rows);
        let Some(request_limit) = request_limit else {
            return Err(FileSearchClientError::OffsetUnsupported {
                offset: request.offset,
                limit,
                max_rows: self.max_rows,
            });
        };

        let filters = file_filters(
            &request.query,
            &request.extensions,
            request.min_size,
            request.max_size,
        );
        // Match Go's one FileSearch context: SearchFiles and its optional
        // CountFiles call share one absolute deadline rather than each getting
        // the full timeout independently.
        let deadline = self.deadline();
        let response = self
            .timed_at(
                "SearchFiles",
                deadline,
                self.rpc.search_files(SearchFilesRequest {
                    filters: Some(filters.clone()),
                    pagination: Some(FilePagination {
                        limit: request_limit,
                        cursor: String::new(),
                    }),
                    sort: request
                        .sort
                        .iter()
                        .filter_map(|sort| {
                            let field = sort.field.trim();
                            (!field.is_empty()).then(|| FileSortBy {
                                field: field.to_owned(),
                                descending: sort.descending,
                            })
                        })
                        .collect(),
                    collapse_to_torrent: false,
                    preview_limit: 0,
                }),
            )
            .await?;

        let (total_count, total_count_is_estimate) = if request.skip_total_count {
            (0, false)
        } else {
            let count = self
                .timed_at(
                    "CountFiles",
                    deadline,
                    self.rpc.count_files(CountFilesRequest {
                        filters: Some(filters),
                        collapse_to_torrent: false,
                    }),
                )
                .await?;
            // This route uses CountFiles at file grain, which is the incumbent
            // Go contract's exact count path.
            (count.count, false)
        };

        let hits = response
            .files
            .into_iter()
            .map(|hit| {
                let value = hit.info_hash;
                let info_hash =
                    value
                        .parse()
                        .map_err(|error: bitmagnet_model::InfoHashError| {
                            FileSearchClientError::InvalidInfoHash {
                                value,
                                message: error.to_string(),
                            }
                        })?;
                Ok(L2FileHit {
                    info_hash,
                    index: hit.file_index,
                    path: hit.path,
                    extension: hit.extension,
                    size: hit.size,
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .skip(request.offset as usize)
            .take(limit as usize)
            .collect();

        Ok(L2FileRowsResult {
            hits,
            total_count,
            total_count_is_estimate,
            has_next_page: response.has_next,
        })
    }

    async fn facets(&self, request: &FileFacetsRequest) -> Result<FileFacetsResult> {
        let response = self
            .timed_at(
                "Facets",
                self.deadline(),
                self.rpc.facets(FacetsRequest {
                    filters: Some(file_filters(
                        &request.query,
                        &request.extensions,
                        request.min_size,
                        request.max_size,
                    )),
                    facet_fields: request.fields.clone(),
                }),
            )
            .await?;

        Ok(FileFacetsResult {
            facets: response
                .facets
                .into_iter()
                .map(|facet| FileFacet {
                    field: facet.field,
                    buckets: facet
                        .buckets
                        .into_iter()
                        .map(|bucket| FileFacetBucket {
                            value: bucket.value,
                            count: bucket.count,
                            total_size: bucket.total_size,
                        })
                        .collect(),
                })
                .collect(),
        })
    }

    async fn path_typeahead(&self, _request: &FilePathTypeaheadRequest) -> Result<Vec<String>> {
        Err(FileSearchClientError::PathTypeaheadUnsupported)
    }
}

fn normalize_endpoint(endpoint: &str) -> Result<String> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err(FileSearchClientError::EmptyEndpoint);
    }
    if endpoint.contains("://") {
        Ok(endpoint.to_owned())
    } else {
        Ok(format!("http://{endpoint}"))
    }
}

fn normalize_max_rows(max_rows: u32) -> Result<u32> {
    match max_rows {
        0 => Ok(MAX_L2_FILE_WINDOW),
        1..=MAX_L2_FILE_WINDOW => Ok(max_rows),
        _ => Err(FileSearchClientError::MaxRowsExceeded {
            max_rows,
            maximum: MAX_L2_FILE_WINDOW,
        }),
    }
}

fn file_filters(query: &str, extensions: &[String], min_size: u64, max_size: u64) -> FileFilters {
    FileFilters {
        extensions: extensions.to_vec(),
        content_types: Vec::new(),
        size_min: (min_size > 0).then_some(min_size),
        size_max: (max_size > 0).then_some(max_size),
        path_query: (!query.is_empty()).then(|| query.to_owned()),
        include_padding: false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use bitmagnet_proto::v1::{
        CountFilesResponse, FileFacet as ProtoFileFacet, FileFacetBucket as ProtoFileFacetBucket,
        FileHit,
    };

    use super::*;
    use crate::schema::search::{FileRowSort, FileSearchRequest};

    #[derive(Default)]
    struct FakeRpc {
        search_request: Mutex<Option<SearchFilesRequest>>,
        count_request: Mutex<Option<CountFilesRequest>>,
        facets_request: Mutex<Option<FacetsRequest>>,
        count_calls: AtomicUsize,
        search_response: SearchFilesResponse,
        count_response: CountFilesResponse,
        facets_response: FacetsResponse,
        hang_search: bool,
        search_delay: Duration,
        count_delay: Duration,
    }

    #[async_trait]
    impl FileSearchRpc for Arc<FakeRpc> {
        async fn search_files(
            &self,
            request: SearchFilesRequest,
        ) -> std::result::Result<SearchFilesResponse, Status> {
            if self.hang_search {
                return std::future::pending().await;
            }
            tokio::time::sleep(self.search_delay).await;
            *self.search_request.lock().expect("search request lock") = Some(request);
            Ok(self.search_response.clone())
        }

        async fn count_files(
            &self,
            request: CountFilesRequest,
        ) -> std::result::Result<CountFilesResponse, Status> {
            self.count_calls.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(self.count_delay).await;
            *self.count_request.lock().expect("count request lock") = Some(request);
            Ok(self.count_response)
        }

        async fn facets(
            &self,
            request: FacetsRequest,
        ) -> std::result::Result<FacetsResponse, Status> {
            *self.facets_request.lock().expect("facets request lock") = Some(request);
            Ok(self.facets_response.clone())
        }
    }

    fn info_hash(digit: char) -> InfoHash {
        digit.to_string().repeat(40).parse().expect("valid hash")
    }

    #[tokio::test]
    async fn max_rows_defaults_customizes_and_rejects_above_ceiling() {
        let default = TonicFileSearchClient::connect(FileSearchClientConfig::new(
            "filesearch.test:50052",
            Duration::from_secs(1),
        ))
        .expect("default config");
        assert_eq!(default.max_rows(), MAX_L2_FILE_WINDOW);

        let custom = TonicFileSearchClient::connect(
            FileSearchClientConfig::new("http://filesearch.test:50052", Duration::from_secs(1))
                .with_max_rows(123),
        )
        .expect("custom config");
        assert_eq!(custom.max_rows(), 123);

        let error = TonicFileSearchClient::connect(
            FileSearchClientConfig::new("filesearch.test:50052", Duration::from_secs(1))
                .with_max_rows(MAX_L2_FILE_WINDOW + 1),
        )
        .err()
        .expect("unsafe max_rows must fail");
        assert!(matches!(
            error,
            FileSearchClientError::MaxRowsExceeded {
                max_rows: 501,
                maximum: 500
            }
        ));
    }

    #[test]
    fn zero_timeout_leaves_deadline_to_caller() {
        TonicFileSearchClient::with_rpc(Arc::new(FakeRpc::default()), Duration::ZERO)
            .expect("zero timeout leaves the deadline to the caller");
    }

    #[tokio::test]
    async fn search_sends_raw_structured_request_and_maps_offset_count() {
        let first = info_hash('1');
        let second = info_hash('2');
        let third = info_hash('3');
        let rpc = Arc::new(FakeRpc {
            search_response: SearchFilesResponse {
                files: vec![
                    FileHit {
                        info_hash: first.to_string(),
                        file_index: 1,
                        path: "skip.mkv".to_owned(),
                        extension: "mkv".to_owned(),
                        size: 100,
                    },
                    FileHit {
                        info_hash: second.to_string(),
                        file_index: 2,
                        path: "keep.mp4".to_owned(),
                        extension: "mp4".to_owned(),
                        size: 200,
                    },
                    FileHit {
                        info_hash: third.to_string(),
                        file_index: 3,
                        path: "also.mkv".to_owned(),
                        extension: "mkv".to_owned(),
                        size: 300,
                    },
                ],
                has_next: true,
                ..SearchFilesResponse::default()
            },
            count_response: CountFilesResponse {
                count: 42,
                estimated: true,
            },
            ..FakeRpc::default()
        });
        let client =
            TonicFileSearchClient::with_rpc(rpc.clone(), Duration::from_secs(1)).expect("client");

        let result = client
            .search_files(&FileSearchRequest {
                query: "50%_raw".to_owned(),
                query_like_pattern: r"50\%\_raw".to_owned(),
                extensions: vec!["mkv".to_owned(), "mp4".to_owned()],
                min_size: 10,
                max_size: 20,
                sort: vec![
                    FileRowSort {
                        field: " size ".to_owned(),
                        descending: true,
                    },
                    FileRowSort {
                        field: "  ".to_owned(),
                        descending: false,
                    },
                ],
                limit: 2,
                offset: 1,
                ..FileSearchRequest::default()
            })
            .await
            .expect("search");

        let search = rpc
            .search_request
            .lock()
            .expect("search request lock")
            .clone()
            .expect("captured search request");
        let filters = search.filters.expect("filters");
        assert_eq!(filters.path_query.as_deref(), Some("50%_raw"));
        assert_eq!(filters.extensions, ["mkv", "mp4"]);
        assert_eq!(filters.size_min, Some(10));
        assert_eq!(filters.size_max, Some(20));
        assert!(!filters.include_padding);
        assert_eq!(search.pagination.expect("pagination").limit, 3);
        assert!(!search.collapse_to_torrent);
        assert_eq!(search.preview_limit, 0);
        assert_eq!(search.sort.len(), 1);
        assert_eq!(search.sort[0].field, "size");
        assert!(search.sort[0].descending);

        let count = rpc
            .count_request
            .lock()
            .expect("count request lock")
            .clone()
            .expect("captured count request");
        assert_eq!(
            count.filters.expect("count filters").path_query.as_deref(),
            Some("50%_raw")
        );
        assert!(!count.collapse_to_torrent);

        assert_eq!(result.total_count, 42);
        assert!(!result.total_count_is_estimate);
        assert!(result.has_next_page);
        assert_eq!(result.hits.len(), 2);
        assert_eq!(result.hits[0].info_hash, second);
        assert_eq!(result.hits[0].index, 2);
        assert_eq!(result.hits[0].path, "keep.mp4");
        assert_eq!(result.hits[1].info_hash, third);
    }

    #[tokio::test]
    async fn skip_count_bounds_and_unsupported_shapes_fail_closed() {
        let hit_hash = info_hash('a');
        let rpc = Arc::new(FakeRpc {
            search_response: SearchFilesResponse {
                files: vec![FileHit {
                    info_hash: hit_hash.to_string(),
                    file_index: 7,
                    path: "fast.mkv".to_owned(),
                    extension: "mkv".to_owned(),
                    size: 700,
                }],
                ..SearchFilesResponse::default()
            },
            ..FakeRpc::default()
        });
        let client =
            TonicFileSearchClient::with_rpc_and_max_rows(rpc.clone(), Duration::from_secs(1), 3)
                .expect("client");

        let result = client
            .search_files(&FileSearchRequest {
                limit: 1,
                skip_total_count: true,
                ..FileSearchRequest::default()
            })
            .await
            .expect("search without count");
        assert_eq!(result.hits[0].info_hash, hit_hash);
        assert_eq!(rpc.count_calls.load(Ordering::Relaxed), 0);

        let offset_error = client
            .search_files(&FileSearchRequest {
                limit: 2,
                offset: 2,
                ..FileSearchRequest::default()
            })
            .await
            .expect_err("window above configured max must fail");
        assert!(matches!(
            offset_error,
            FileSearchClientError::OffsetUnsupported {
                offset: 2,
                limit: 2,
                max_rows: 3
            }
        ));

        let hash_error = client
            .search_files(&FileSearchRequest {
                info_hash: Some(hit_hash),
                limit: 1,
                ..FileSearchRequest::default()
            })
            .await
            .expect_err("info hash is not in the proto");
        assert!(matches!(
            hash_error,
            FileSearchClientError::InfoHashUnsupported
        ));
    }

    #[tokio::test]
    async fn malformed_hash_timeout_and_typeahead_are_explicit_errors() {
        let invalid_rpc = Arc::new(FakeRpc {
            search_response: SearchFilesResponse {
                files: vec![FileHit {
                    info_hash: "not-a-hash".to_owned(),
                    ..FileHit::default()
                }],
                ..SearchFilesResponse::default()
            },
            ..FakeRpc::default()
        });
        let client =
            TonicFileSearchClient::with_rpc(invalid_rpc, Duration::from_secs(1)).expect("client");
        let error = client
            .search_files(&FileSearchRequest {
                limit: 1,
                skip_total_count: true,
                ..FileSearchRequest::default()
            })
            .await
            .expect_err("malformed hash must fail");
        assert!(matches!(
            error,
            FileSearchClientError::InvalidInfoHash { .. }
        ));

        let timeout_client = TonicFileSearchClient::with_rpc(
            Arc::new(FakeRpc {
                hang_search: true,
                ..FakeRpc::default()
            }),
            Duration::from_millis(1),
        )
        .expect("timeout client");
        let error = timeout_client
            .search_files(&FileSearchRequest {
                limit: 1,
                ..FileSearchRequest::default()
            })
            .await
            .expect_err("hung RPC must time out");
        assert!(matches!(
            error,
            FileSearchClientError::Timeout {
                operation: "SearchFiles",
                ..
            }
        ));

        let error = client
            .path_typeahead(&FilePathTypeaheadRequest {
                prefix: "ab".to_owned(),
                prefix_like_pattern: "ab%".to_owned(),
                limit: 5,
            })
            .await
            .expect_err("proto has no typeahead RPC");
        assert!(matches!(
            error,
            FileSearchClientError::PathTypeaheadUnsupported
        ));
    }

    #[tokio::test]
    async fn search_and_count_share_one_go_compatible_deadline() {
        let client = TonicFileSearchClient::with_rpc(
            Arc::new(FakeRpc {
                search_delay: Duration::from_millis(35),
                count_delay: Duration::from_millis(35),
                ..FakeRpc::default()
            }),
            Duration::from_millis(50),
        )
        .expect("client");
        let error = client
            .search_files(&FileSearchRequest {
                limit: 1,
                ..FileSearchRequest::default()
            })
            .await
            .expect_err("the second RPC must share the first RPC's deadline");

        assert!(matches!(
            error,
            FileSearchClientError::Timeout {
                operation: "CountFiles",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn facets_preserve_backend_order_and_structured_filters() {
        let rpc = Arc::new(FakeRpc {
            facets_response: FacetsResponse {
                facets: vec![ProtoFileFacet {
                    field: "extension".to_owned(),
                    buckets: vec![
                        ProtoFileFacetBucket {
                            value: "mkv".to_owned(),
                            count: 7,
                            total_size: 700,
                        },
                        ProtoFileFacetBucket {
                            value: "mp4".to_owned(),
                            count: 3,
                            total_size: 300,
                        },
                    ],
                }],
            },
            ..FakeRpc::default()
        });
        let client =
            TonicFileSearchClient::with_rpc(rpc.clone(), Duration::from_secs(1)).expect("client");
        let result = client
            .facets(&FileFacetsRequest {
                query: "50%_raw".to_owned(),
                query_like_pattern: r"50\%\_raw".to_owned(),
                extensions: vec!["mkv".to_owned(), "mp4".to_owned()],
                min_size: 10,
                max_size: 20,
                fields: vec!["extension".to_owned()],
            })
            .await
            .expect("facets");

        let request = rpc
            .facets_request
            .lock()
            .expect("facets request lock")
            .clone()
            .expect("captured facets request");
        assert_eq!(request.facet_fields, ["extension"]);
        let filters = request.filters.expect("filters");
        assert_eq!(filters.path_query.as_deref(), Some("50%_raw"));
        assert_eq!(filters.extensions, ["mkv", "mp4"]);
        assert_eq!(filters.size_min, Some(10));
        assert_eq!(filters.size_max, Some(20));

        assert_eq!(result.facets.len(), 1);
        assert_eq!(result.facets[0].field, "extension");
        assert_eq!(result.facets[0].buckets[0].value, "mkv");
        assert_eq!(result.facets[0].buckets[0].count, 7);
        assert_eq!(result.facets[0].buckets[1].value, "mp4");
    }

    #[tokio::test]
    async fn disabled_backend_rejects_every_l2_surface() {
        let backend = DisabledFileSearchBackend;

        assert!(matches!(
            backend.search_files(&FileSearchRequest::default()).await,
            Err(FileSearchClientError::Disabled)
        ));
        assert!(matches!(
            backend.facets(&FileFacetsRequest::default()).await,
            Err(FileSearchClientError::Disabled)
        ));
        assert!(matches!(
            backend
                .path_typeahead(&FilePathTypeaheadRequest {
                    prefix: "ab".to_owned(),
                    prefix_like_pattern: "ab%".to_owned(),
                    limit: 5,
                })
                .await,
            Err(FileSearchClientError::Disabled)
        ));
    }
}
