//! PostgreSQL adapter between the bounded composer and Lane S search queries.
//!
//! Candidate search never replaces resolver predicates. It intersects the
//! caller's structured criteria with the L3 candidate hashes, clears the free
//! text already evaluated by L3, and removes the caller's page window so exact
//! refine remains the only pagination boundary.

use std::collections::HashMap;

pub use bitmagnet_search_query::{
    AggregationGroup, AggregationItem, Aggregations, Criteria, HydrateOptions, SearchBuildConfig,
    SearchOptions, SearchResult, SearchResultItem,
};
use sqlx::{PgPool, Row};

const SUMMARY_COUNTS_SQL: &str = "SELECT info_hash, file_count::bigint AS file_count\
\nFROM torrent_file_summary\
\nWHERE info_hash = ANY($1::bytea[])";
const TORRENT_REFINE_METADATA_SQL: &str = "SELECT info_hash, files_count::bigint AS file_count,\
\n       octet_length(files_data)::bigint AS compressed_bytes\
\nFROM torrents\
\nWHERE info_hash = ANY($1::bytea[])";

/// Pre-hydration bounds for one exact-refine candidate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RefineMetadata {
    /// Authoritative summary count, falling back to `torrents.files_count`.
    pub file_count: Option<i64>,
    /// Compressed PostgreSQL `files_data` bytes, without materialising the blob.
    pub compressed_bytes: Option<u64>,
}

/// One executable Lane-S PostgreSQL request.
///
/// [`SearchOptions`] remains the authoritative query contract. The envelope
/// carries only the heavyweight hydration control that Lane S deliberately
/// keeps separate from that contract. The concrete backend owns the stable
/// server-side build configuration.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchRequest {
    /// Structured search, order, facet, count, and page options.
    pub options: SearchOptions,
    /// Optional heavyweight hydration controls.
    pub hydrate: HydrateOptions,
}

impl SearchRequest {
    /// Construct a request from real Lane-S options.
    #[must_use]
    pub const fn new(options: SearchOptions, hydrate: HydrateOptions) -> Self {
        Self { options, hydrate }
    }

    /// Restrict this request to L3 candidates without weakening user filters.
    ///
    /// The structured filter, order, facets, and aggregation budget survive
    /// unchanged. Free text and the inherited page/count controls do not: L3
    /// already evaluated the text, and the composer applies the only
    /// authoritative page after exact file-path refinement. The build
    /// configuration lives on [`PgSearch`], outside this transformation.
    #[must_use]
    pub fn for_candidates(
        &self,
        ids: &[bitmagnet_model::InfoHash],
        hydrate: HydrateOptions,
    ) -> Self {
        let mut request = self.clone();
        let candidates = Criteria::torrent_content_info_hash_in(ids.iter().copied());
        request.options.filter = Some(match request.options.filter.take() {
            Some(user_filter) => Criteria::and([user_filter, candidates]),
            None => candidates,
        });
        request.options.query = None;
        request.options.limit = None;
        request.options.offset = 0;
        request.options.total_count = false;
        request.options.has_next_page = false;
        request.hydrate = hydrate;
        request
    }
}

/// PostgreSQL option sets built by the GraphQL layer for composer refine paths.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryOptions {
    /// Compatibility request used when a dedicated refine request is omitted.
    ///
    /// Current Go gate7-8 does not use candidate-set combined facets when
    /// `refine` is present, even for one chunk; it re-aggregates exact IDs.
    pub combined: SearchRequest,
    /// Optional blob-hydration request used for every exact-refine chunk.
    pub refine: Option<SearchRequest>,
    /// Facet request for decode-free refined-set re-aggregation.
    pub agg: SearchRequest,
    /// Retain decoded file rows on matching torrent results.
    ///
    /// Exact path matching still decodes each candidate when this is false,
    /// but drops the file vector before retaining the torrent result. GraphQL
    /// sets this only when the selected projection includes `torrent.files`.
    pub retain_refine_files: bool,
}

impl QueryOptions {
    /// Return per-chunk options, falling back to combined when omitted.
    #[must_use]
    pub fn refine_request(&self) -> &SearchRequest {
        self.refine.as_ref().unwrap_or(&self.combined)
    }
}

/// Search dependency used by the bounded composer.
#[async_trait::async_trait]
pub trait PgSearchBackend: Send + Sync {
    /// Execute one fully shaped Lane-S PostgreSQL request.
    async fn torrent_content(&self, request: SearchRequest) -> crate::Result<SearchResult>;

    /// Read authoritative counts and compressed sizes without selecting blobs.
    async fn refine_metadata(
        &self,
        ids: &[bitmagnet_model::InfoHash],
    ) -> crate::Result<HashMap<bitmagnet_model::InfoHash, RefineMetadata>>;
}

/// Concrete sqlx implementation of the composer PostgreSQL dependency.
#[derive(Debug, Clone)]
pub struct PgSearch {
    pool: PgPool,
    build_config: SearchBuildConfig,
}

impl PgSearch {
    /// Construct the adapter around the GraphQL service's shared pool.
    #[must_use]
    pub const fn new(pool: PgPool, build_config: SearchBuildConfig) -> Self {
        Self { pool, build_config }
    }

    /// Access the shared pool used by this adapter.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Return the stable server-side builder flags used by all searches.
    #[must_use]
    pub const fn build_config(&self) -> SearchBuildConfig {
        self.build_config
    }

    /// Execute real Lane-S search directly for the plain PostgreSQL fallback.
    pub async fn search(
        &self,
        options: &SearchOptions,
        hydrate: HydrateOptions,
    ) -> crate::Result<SearchResult> {
        bitmagnet_search_query::search(&self.pool, options, &self.build_config, hydrate)
            .await
            .map_err(|error| crate::Error::Pg(error.to_string()))
    }
}

#[async_trait::async_trait]
impl PgSearchBackend for PgSearch {
    async fn torrent_content(&self, request: SearchRequest) -> crate::Result<SearchResult> {
        self.search(&request.options, request.hydrate).await
    }

    async fn refine_metadata(
        &self,
        ids: &[bitmagnet_model::InfoHash],
    ) -> crate::Result<HashMap<bitmagnet_model::InfoHash, RefineMetadata>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let requested = bytea_values(ids);
        let summary_rows = sqlx::query(SUMMARY_COUNTS_SQL)
            .bind(&requested)
            .fetch_all(&self.pool)
            .await
            .map_err(|error| crate::Error::Pg(error.to_string()))?;
        let mut metadata = HashMap::with_capacity(ids.len());
        for row in summary_rows {
            let raw: Vec<u8> = row
                .try_get("info_hash")
                .map_err(|error| crate::Error::Pg(error.to_string()))?;
            let info_hash = bitmagnet_model::InfoHash::from_slice(&raw)
                .map_err(|error| crate::Error::Pg(error.to_string()))?;
            let count: i64 = row
                .try_get("file_count")
                .map_err(|error| crate::Error::Pg(error.to_string()))?;
            metadata.insert(
                info_hash,
                RefineMetadata {
                    file_count: Some(count),
                    compressed_bytes: None,
                },
            );
        }

        let torrent_rows = sqlx::query(TORRENT_REFINE_METADATA_SQL)
            .bind(&requested)
            .fetch_all(&self.pool)
            .await
            .map_err(|error| crate::Error::Pg(error.to_string()))?;
        for row in torrent_rows {
            let raw: Vec<u8> = row
                .try_get("info_hash")
                .map_err(|error| crate::Error::Pg(error.to_string()))?;
            let info_hash = bitmagnet_model::InfoHash::from_slice(&raw)
                .map_err(|error| crate::Error::Pg(error.to_string()))?;
            let count: Option<i64> = row
                .try_get("file_count")
                .map_err(|error| crate::Error::Pg(error.to_string()))?;
            let compressed_bytes: Option<i64> = row
                .try_get("compressed_bytes")
                .map_err(|error| crate::Error::Pg(error.to_string()))?;
            let compressed_bytes = compressed_bytes
                .map(|bytes| {
                    u64::try_from(bytes).map_err(|_| {
                        crate::Error::Pg(format!(
                            "negative compressed files_data length for {info_hash}"
                        ))
                    })
                })
                .transpose()?;
            let entry = metadata.entry(info_hash).or_default();
            if entry.file_count.is_none() {
                entry.file_count = count;
            }
            entry.compressed_bytes = compressed_bytes;
        }
        Ok(metadata)
    }
}

pub(crate) fn empty_result() -> SearchResult {
    SearchResult {
        total_count: 0,
        total_count_is_estimate: false,
        has_next_page: false,
        items: Vec::new(),
        aggregations: Aggregations::new(),
    }
}

fn bytea_values(ids: &[bitmagnet_model::InfoHash]) -> Vec<Vec<u8>> {
    ids.iter().map(|id| id.as_slice().to_vec()).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use bitmagnet_search_query::{
        FacetRequest, OrderDirection, TorrentContentFacet, TorrentContentOrder,
        TorrentContentOrderField,
    };
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    fn info_hash(byte: u8) -> bitmagnet_model::InfoHash {
        bitmagnet_model::InfoHash::new([byte; bitmagnet_model::INFO_HASH_LEN])
    }

    #[test]
    fn candidate_scope_intersects_and_preserves_every_non_page_control() {
        let user_filter = Criteria::torrent_source_in(["dht".to_owned()]);
        let order = TorrentContentOrder {
            field: TorrentContentOrderField::Seeders,
            direction: OrderDirection::Descending,
        };
        let facet = FacetRequest {
            facet: TorrentContentFacet::ContentType,
            aggregate: true,
            logic: None,
            filter: BTreeSet::from(["movie".to_owned()]),
        };
        let options = SearchOptions::new()
            .with_query("inception")
            .with_filter(user_filter.clone())
            .with_order([order])
            .with_facets([facet.clone()])
            .with_limit(25)
            .with_offset(50)
            .with_total_count(true)
            .with_has_next_page(true)
            .with_aggregation_budget(123.5);
        let original = SearchRequest::new(
            options,
            HydrateOptions {
                files_data: false,
                max_files_data_bytes: None,
            },
        );

        let shaped = original.for_candidates(
            &[info_hash(1), info_hash(2)],
            HydrateOptions {
                files_data: true,
                max_files_data_bytes: None,
            },
        );

        assert_eq!(
            shaped.options.filter,
            Some(Criteria::and([
                user_filter,
                Criteria::torrent_content_info_hash_in([info_hash(1), info_hash(2)]),
            ]))
        );
        assert_eq!(shaped.options.query, None);
        assert_eq!(shaped.options.order, vec![order]);
        assert_eq!(shaped.options.facets, vec![facet]);
        assert_eq!(shaped.options.limit, None);
        assert_eq!(shaped.options.offset, 0);
        assert!(!shaped.options.total_count);
        assert!(!shaped.options.has_next_page);
        assert_eq!(shaped.options.aggregation_budget, 123.5);
        assert!(shaped.hydrate.files_data);

        assert_eq!(original.options.query.as_deref(), Some("inception"));
        assert_eq!(original.options.limit, Some(25));
    }

    #[test]
    fn candidate_scope_without_user_filter_still_restricts_membership() {
        let request =
            SearchRequest::default().for_candidates(&[info_hash(7)], HydrateOptions::default());
        assert_eq!(
            request.options.filter,
            Some(Criteria::torrent_content_info_hash_in([info_hash(7)]))
        );
    }

    #[test]
    fn query_options_falls_back_to_combined_request() {
        let options = QueryOptions::default();
        assert!(std::ptr::eq(options.refine_request(), &options.combined));
    }

    #[test]
    fn refine_metadata_sql_is_two_step_index_keyed_and_blob_safe() {
        assert!(SUMMARY_COUNTS_SQL.contains("FROM torrent_file_summary"));
        assert!(SUMMARY_COUNTS_SQL.contains("info_hash = ANY($1::bytea[])"));
        assert!(TORRENT_REFINE_METADATA_SQL.contains("FROM torrents"));
        assert!(TORRENT_REFINE_METADATA_SQL.contains("octet_length(files_data)"));
        for sql in [SUMMARY_COUNTS_SQL, TORRENT_REFINE_METADATA_SQL] {
            assert!(!sql.contains("files_data AS"));
            assert!(!sql.contains("torrent_files"));
            assert!(!sql.contains("JOIN"));
        }
    }

    #[tokio::test]
    async fn empty_count_probe_never_connects() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .expect("lazy test DSN");
        let backend = PgSearch::new(pool, SearchBuildConfig::default());
        assert_eq!(backend.refine_metadata(&[]).await.unwrap(), HashMap::new());
    }

    #[tokio::test]
    async fn pg_search_keeps_build_config_outside_candidate_shaping() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .expect("lazy test DSN");
        let build_config = SearchBuildConfig {
            file_extensions_jsonb: true,
            popularity_sort_default: true,
        };
        let backend = PgSearch::new(pool, build_config);
        let _shaped = SearchRequest::default().for_candidates(
            &[info_hash(9)],
            HydrateOptions {
                files_data: true,
                max_files_data_bytes: None,
            },
        );

        assert_eq!(backend.build_config(), build_config);
    }

    #[test]
    fn bytea_values_preserve_raw_info_hash_bytes() {
        assert_eq!(
            bytea_values(&[info_hash(1), info_hash(2)]),
            vec![vec![1; 20], vec![2; 20]]
        );
    }
}
