//! Bounded, read-only implementation of the `torrent.files` GraphQL field.
//!
//! The Go authority is `internal/gql/gqlmodel/torrent_files.go`. This adapter
//! deliberately rejects absent or empty `infoHashes`: accepting either would
//! turn the Go-compatible in-memory browse into an unbounded torrent scan.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;

use async_graphql::{Error, MaybeUndefined, Result};
use async_trait::async_trait;
use bitmagnet_db::PgPool;
use bitmagnet_model::{deserialize_files_bounded, file_extension_from_path, InfoHash};
use sqlx::Row;
use thiserror::Error;

use super::enums::{FileType, TorrentFilesOrderByField};
use super::inputs::TorrentFilesQueryInput;
use super::objects::{TorrentFile, TorrentFilesQueryResult};
use super::scalars::{DateTime, Hash20};

const DEFAULT_LIMIT: u32 = 10;
const ZERO_DATETIME: &str = "0001-01-01T00:00:00Z";

/// Fail-closed resource limits for one file-browser request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TorrentFilesLimits {
    /// Maximum distinct hashes in one request.
    pub max_info_hashes: usize,
    /// Maximum rows admitted from any one blob.
    pub max_files_per_blob: usize,
    /// Maximum rows admitted across the request.
    pub max_files_per_request: usize,
    /// Maximum compressed bytes admitted from any one blob.
    pub max_compressed_bytes_per_blob: usize,
    /// Maximum compressed bytes admitted across the request.
    pub max_compressed_bytes_per_request: usize,
    /// Maximum decompressed bytes admitted from any one blob.
    pub max_decompressed_bytes_per_blob: usize,
    /// Maximum decompressed bytes admitted across the request.
    pub max_decompressed_bytes_per_request: usize,
    /// Maximum decoded path and extension bytes retained across the request.
    pub max_owned_string_bytes_per_request: usize,
}

impl Default for TorrentFilesLimits {
    fn default() -> Self {
        const MIB: usize = 1024 * 1024;
        Self {
            max_info_hashes: 32,
            max_files_per_blob: 300_000,
            max_files_per_request: 300_000,
            max_compressed_bytes_per_blob: 64 * MIB,
            max_compressed_bytes_per_request: 64 * MIB,
            max_decompressed_bytes_per_blob: 64 * MIB,
            max_decompressed_bytes_per_request: 64 * MIB,
            max_owned_string_bytes_per_request: 64 * MIB,
        }
    }
}

/// One bounded blob selected by a torrent-files runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentFilesBlob {
    /// Torrent owning the blob.
    pub info_hash: InfoHash,
    /// Compressed blob; `None` represents an existing torrent without one.
    pub files_data: Option<Vec<u8>>,
    /// Authoritative denormalized row count.
    pub file_count: usize,
}

/// Typed failures from the bounded read adapter.
#[derive(Debug, Error)]
pub enum TorrentFilesError {
    /// No database runtime was attached to this schema.
    #[error("torrent.files is unavailable without a PostgreSQL runtime")]
    Disabled,
    /// The read-only SQL query failed.
    #[error("torrent.files PostgreSQL read failed: {0}")]
    Database(#[from] sqlx::Error),
    /// PostgreSQL returned a malformed info hash.
    #[error("torrent.files row has an invalid info hash: {0}")]
    InvalidInfoHash(#[from] bitmagnet_model::InfoHashError),
    /// A migrated blob lacks its required summary metadata.
    #[error("torrent.files metadata is incomplete for {info_hash}: {field} is NULL")]
    MissingMetadata {
        /// Torrent whose summary is incomplete.
        info_hash: InfoHash,
        /// Missing summary field.
        field: &'static str,
    },
    /// A configured resource bound was exceeded.
    #[error("torrent.files {resource} is {actual}, above the limit {limit}")]
    LimitExceeded {
        /// Resource being bounded.
        resource: &'static str,
        /// Observed amount.
        actual: usize,
        /// Configured limit.
        limit: usize,
    },
    /// The compressed blob did not decode under the configured bounds.
    #[error("torrent.files blob decode failed for {info_hash}: {source}")]
    Decode {
        /// Torrent owning the invalid blob.
        info_hash: InfoHash,
        /// Bounded decoder failure.
        source: bitmagnet_model::BlobError,
    },
    /// The summary count and decoded row count disagree.
    #[error("torrent.files count mismatch for {info_hash}: summary={summary}, decoded={decoded}")]
    CountMismatch {
        /// Torrent owning the inconsistent blob.
        info_hash: InfoHash,
        /// Denormalized summary count.
        summary: usize,
        /// Actual decoded rows.
        decoded: usize,
    },
}

/// Runtime seam used by the GraphQL resolver.
#[async_trait]
pub trait TorrentFilesRuntime: Send + Sync {
    /// Loads only the explicitly requested torrent blobs.
    async fn load(
        &self,
        info_hashes: &[InfoHash],
        limits: TorrentFilesLimits,
    ) -> std::result::Result<Vec<TorrentFilesBlob>, TorrentFilesError>;
}

struct DisabledTorrentFilesRuntime;

#[async_trait]
impl TorrentFilesRuntime for DisabledTorrentFilesRuntime {
    async fn load(
        &self,
        _info_hashes: &[InfoHash],
        _limits: TorrentFilesLimits,
    ) -> std::result::Result<Vec<TorrentFilesBlob>, TorrentFilesError> {
        Err(TorrentFilesError::Disabled)
    }
}

/// PostgreSQL implementation of the torrent-files read seam.
pub struct PgTorrentFilesRuntime {
    pool: PgPool,
}

impl PgTorrentFilesRuntime {
    /// Constructs a lazy read adapter over a caller-owned pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TorrentFilesRuntime for PgTorrentFilesRuntime {
    async fn load(
        &self,
        info_hashes: &[InfoHash],
        limits: TorrentFilesLimits,
    ) -> std::result::Result<Vec<TorrentFilesBlob>, TorrentFilesError> {
        let hashes = info_hashes
            .iter()
            .map(|hash| hash.as_slice().to_vec())
            .collect::<Vec<_>>();
        let rows = sqlx::query(
            "WITH requested AS (\
             \n  SELECT t.info_hash, t.files_data IS NULL AS files_data_is_null,\
             \n         s.file_count::bigint AS file_count,\
             \n         s.compressed_bytes::bigint AS compressed_bytes\
             \n  FROM torrents AS t\
             \n  LEFT JOIN torrent_file_summary AS s USING (info_hash)\
             \n  WHERE t.info_hash = ANY($1::bytea[])\
             \n), bounded AS (\
             \n  SELECT requested.*,\
             \n         sum(coalesce(file_count, 0)) OVER ()::bigint AS request_file_count,\
             \n         sum(coalesce(compressed_bytes, 0)) OVER ()::bigint\
             \n           AS request_compressed_bytes\
             \n  FROM requested\
             \n)\
             \nSELECT bounded.*,\
             \n       CASE WHEN file_count <= $2\
             \n                  AND request_file_count <= $3\
             \n                  AND compressed_bytes <= $4\
             \n                  AND request_compressed_bytes <= $5\
             \n            THEN t.files_data END AS files_data\
             \nFROM bounded\
             \nJOIN torrents AS t USING (info_hash)\
             \nORDER BY info_hash",
        )
        .bind(hashes)
        .bind(limit_i64(limits.max_files_per_blob))
        .bind(limit_i64(limits.max_files_per_request))
        .bind(limit_i64(limits.max_compressed_bytes_per_blob))
        .bind(limit_i64(limits.max_compressed_bytes_per_request))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| row_to_blob(row, limits))
            .collect()
    }
}

fn row_to_blob(
    row: sqlx::postgres::PgRow,
    limits: TorrentFilesLimits,
) -> std::result::Result<TorrentFilesBlob, TorrentFilesError> {
    let info_hash = InfoHash::from_slice(row.try_get::<Vec<u8>, _>("info_hash")?.as_slice())?;
    let file_count = required_nonnegative(
        &row,
        "file_count",
        info_hash,
        "torrent_file_summary.file_count",
    )?;
    let compressed_bytes = required_nonnegative(
        &row,
        "compressed_bytes",
        info_hash,
        "torrent_file_summary.compressed_bytes",
    )?;
    let request_file_count = nonnegative(row.try_get::<i64, _>("request_file_count")?)?;
    let request_compressed_bytes = nonnegative(row.try_get::<i64, _>("request_compressed_bytes")?)?;

    enforce_limit("blob file count", file_count, limits.max_files_per_blob)?;
    enforce_limit(
        "request file count",
        request_file_count,
        limits.max_files_per_request,
    )?;
    enforce_limit(
        "blob compressed bytes",
        compressed_bytes,
        limits.max_compressed_bytes_per_blob,
    )?;
    enforce_limit(
        "request compressed bytes",
        request_compressed_bytes,
        limits.max_compressed_bytes_per_request,
    )?;

    let files_data_is_null = row.try_get::<bool, _>("files_data_is_null")?;
    let files_data = row.try_get::<Option<Vec<u8>>, _>("files_data")?;
    if !files_data_is_null && files_data.is_none() {
        return Err(TorrentFilesError::LimitExceeded {
            resource: "files_data materialization",
            actual: compressed_bytes,
            limit: limits.max_compressed_bytes_per_blob,
        });
    }
    if let Some(data) = &files_data {
        if data.len() != compressed_bytes {
            return Err(TorrentFilesError::LimitExceeded {
                resource: "files_data metadata mismatch",
                actual: data.len(),
                limit: compressed_bytes,
            });
        }
    }

    Ok(TorrentFilesBlob {
        info_hash,
        files_data,
        file_count,
    })
}

fn required_nonnegative(
    row: &sqlx::postgres::PgRow,
    column: &str,
    info_hash: InfoHash,
    field: &'static str,
) -> std::result::Result<usize, TorrentFilesError> {
    row.try_get::<Option<i64>, _>(column)?
        .ok_or(TorrentFilesError::MissingMetadata { info_hash, field })
        .and_then(nonnegative)
}

fn nonnegative(value: i64) -> std::result::Result<usize, TorrentFilesError> {
    usize::try_from(value).map_err(|_| TorrentFilesError::LimitExceeded {
        resource: "negative PostgreSQL metadata",
        actual: usize::MAX,
        limit: 0,
    })
}

fn limit_i64(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

fn enforce_limit(
    resource: &'static str,
    actual: usize,
    limit: usize,
) -> std::result::Result<(), TorrentFilesError> {
    if actual > limit {
        Err(TorrentFilesError::LimitExceeded {
            resource,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

/// GraphQL context wrapper for a torrent-files runtime and its bounds.
#[derive(Clone)]
pub struct TorrentFilesRuntimeData {
    runtime: Arc<dyn TorrentFilesRuntime>,
    limits: TorrentFilesLimits,
}

impl TorrentFilesRuntimeData {
    /// Wraps an enabled runtime with production bounds.
    #[must_use]
    pub fn new(runtime: Arc<dyn TorrentFilesRuntime>) -> Self {
        Self {
            runtime,
            limits: TorrentFilesLimits::default(),
        }
    }

    /// Constructs the fail-loud context used by non-runtime schema builders.
    #[must_use]
    pub fn disabled() -> Self {
        Self::new(Arc::new(DisabledTorrentFilesRuntime))
    }

    /// Constructs the production PostgreSQL runtime.
    #[must_use]
    pub fn pg(pool: PgPool) -> Self {
        Self::new(Arc::new(PgTorrentFilesRuntime::new(pool)))
    }

    #[cfg(test)]
    fn with_limits(runtime: Arc<dyn TorrentFilesRuntime>, limits: TorrentFilesLimits) -> Self {
        Self { runtime, limits }
    }
}

pub(super) async fn resolve(
    runtime: &TorrentFilesRuntimeData,
    input: TorrentFilesQueryInput,
) -> Result<TorrentFilesQueryResult> {
    let info_hashes = parse_hashes(input.info_hashes, runtime.limits.max_info_hashes)?;
    let limit = u64::from(optional_nonnegative(&input.limit, "limit")?.unwrap_or(DEFAULT_LIMIT));
    let page = optional_nonnegative(&input.page, "page")?.map(u64::from);
    let explicit_offset = u64::from(optional_nonnegative(&input.offset, "offset")?.unwrap_or(0));
    let offset = page
        .filter(|value| *value > 0)
        .unwrap_or(1)
        .checked_sub(1)
        .and_then(|page| page.checked_mul(limit))
        .and_then(|page_offset| page_offset.checked_add(explicit_offset))
        .ok_or_else(|| Error::new("page window exceeds supported range"))?;

    let blobs = runtime
        .runtime
        .load(&info_hashes, runtime.limits)
        .await
        .map_err(|error| Error::new(error.to_string()))?;
    let mut files = Vec::new();
    let mut decompressed_bytes = 0_usize;
    let mut owned_string_bytes = 0_usize;
    for blob in blobs {
        let Some(data) = blob.files_data else {
            if blob.file_count != 0 {
                return Err(Error::new(
                    TorrentFilesError::CountMismatch {
                        info_hash: blob.info_hash,
                        summary: blob.file_count,
                        decoded: 0,
                    }
                    .to_string(),
                ));
            }
            continue;
        };
        let decoded = deserialize_files_bounded(
            &data,
            runtime.limits.max_decompressed_bytes_per_blob,
            runtime.limits.max_files_per_blob,
        )
        .map_err(|source| {
            Error::new(
                TorrentFilesError::Decode {
                    info_hash: blob.info_hash,
                    source,
                }
                .to_string(),
            )
        })?;
        if decoded.files.len() != blob.file_count {
            return Err(Error::new(
                TorrentFilesError::CountMismatch {
                    info_hash: blob.info_hash,
                    summary: blob.file_count,
                    decoded: decoded.files.len(),
                }
                .to_string(),
            ));
        }
        decompressed_bytes = decompressed_bytes
            .checked_add(decoded.decompressed_bytes)
            .ok_or_else(|| Error::new("torrent.files decompressed-byte count overflow"))?;
        enforce_limit(
            "request decompressed bytes",
            decompressed_bytes,
            runtime.limits.max_decompressed_bytes_per_request,
        )
        .map_err(|error| Error::new(error.to_string()))?;
        owned_string_bytes = owned_string_bytes
            .checked_add(decoded.owned_string_bytes)
            .ok_or_else(|| Error::new("torrent.files retained-string count overflow"))?;
        enforce_limit(
            "request retained string bytes",
            owned_string_bytes,
            runtime.limits.max_owned_string_bytes_per_request,
        )
        .map_err(|error| Error::new(error.to_string()))?;

        files.extend(decoded.files.into_iter().map(|file| {
            let extension = file_extension_from_path(&file.path);
            TorrentFile {
                created_at: DateTime(ZERO_DATETIME.to_owned()),
                extension: extension.clone(),
                file_type: extension.as_deref().and_then(graphql_file_type),
                index: i64::from(file.index),
                info_hash: Hash20(blob.info_hash.to_string()),
                path: file.path,
                size: i64::try_from(file.size).unwrap_or(i64::MAX),
                updated_at: DateTime(ZERO_DATETIME.to_owned()),
            }
        }));
    }

    sort_files(&mut files, input.order_by.as_deref());
    let total = files.len();
    let start = usize::try_from(offset).unwrap_or(total).min(total);
    let end = start
        .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
        .min(total);
    let items = files.drain(start..end).collect();
    let total_count = input.total_count.value().copied().unwrap_or(false);
    let has_next_page = input.has_next_page.value().copied().unwrap_or(false) && end < total;

    Ok(TorrentFilesQueryResult {
        has_next_page: Some(has_next_page),
        items,
        total_count: if total_count {
            i64::try_from(total).unwrap_or(i64::MAX)
        } else {
            0
        },
    })
}

fn parse_hashes(raw: Option<Vec<Hash20>>, max: usize) -> Result<Vec<InfoHash>> {
    let raw = raw.ok_or_else(|| Error::new("torrent.files infoHashes must be provided"))?;
    if raw.is_empty() {
        return Err(Error::new("torrent.files infoHashes must not be empty"));
    }
    if raw.len() > max {
        return Err(Error::new(format!(
            "torrent.files infoHashes has {} entries, above the limit {max}",
            raw.len()
        )));
    }
    let parsed = raw
        .into_iter()
        .map(|hash| {
            hash.0
                .parse::<InfoHash>()
                .map_err(|error| Error::new(format!("invalid Hash20: {error}")))
        })
        .collect::<Result<BTreeSet<_>>>()?;
    Ok(parsed.into_iter().collect())
}

fn optional_nonnegative(value: &MaybeUndefined<i32>, field: &str) -> Result<Option<u32>> {
    value
        .value()
        .map(|value| {
            u32::try_from(*value).map_err(|_| Error::new(format!("{field} must be non-negative")))
        })
        .transpose()
}

fn sort_files(
    files: &mut [TorrentFile],
    order_by: Option<&[super::inputs::TorrentFilesOrderByInput]>,
) {
    files.sort_by(|left, right| {
        let Some(order_by) = order_by.filter(|order_by| !order_by.is_empty()) else {
            return left.path.cmp(&right.path);
        };
        for order in order_by {
            let ordering = match order.field {
                TorrentFilesOrderByField::Extension => left.extension.cmp(&right.extension),
                TorrentFilesOrderByField::Index => left.index.cmp(&right.index),
                TorrentFilesOrderByField::Path => left.path.cmp(&right.path),
                TorrentFilesOrderByField::Size => left.size.cmp(&right.size),
            };
            if ordering != Ordering::Equal {
                return if order.descending.value().copied().unwrap_or(false) {
                    ordering.reverse()
                } else {
                    ordering
                };
            }
        }
        Ordering::Equal
    });
}

fn graphql_file_type(extension: &str) -> Option<FileType> {
    bitmagnet_model::FileType::from_extension(extension).map(|file_type| match file_type {
        bitmagnet_model::FileType::Archive => FileType::Archive,
        bitmagnet_model::FileType::Audio => FileType::Audio,
        bitmagnet_model::FileType::Data => FileType::Data,
        bitmagnet_model::FileType::Document => FileType::Document,
        bitmagnet_model::FileType::Image => FileType::Image,
        bitmagnet_model::FileType::Software => FileType::Software,
        bitmagnet_model::FileType::Subtitles => FileType::Subtitles,
        bitmagnet_model::FileType::Video => FileType::Video,
    })
}

#[cfg(test)]
mod tests {
    use async_graphql::{value, EmptySubscription};
    use bitmagnet_model::{serialize_files, BlobFile};

    use super::*;
    use crate::schema::roots::{Mutation, Query};

    #[derive(Clone)]
    struct FakeRuntime {
        blobs: Vec<TorrentFilesBlob>,
    }

    #[async_trait]
    impl TorrentFilesRuntime for FakeRuntime {
        async fn load(
            &self,
            info_hashes: &[InfoHash],
            _limits: TorrentFilesLimits,
        ) -> std::result::Result<Vec<TorrentFilesBlob>, TorrentFilesError> {
            Ok(self
                .blobs
                .iter()
                .filter(|blob| info_hashes.contains(&blob.info_hash))
                .cloned()
                .collect())
        }
    }

    fn test_schema(blobs: Vec<TorrentFilesBlob>) -> crate::schema::Schema {
        let runtime: Arc<dyn TorrentFilesRuntime> = Arc::new(FakeRuntime { blobs });
        async_graphql::Schema::build(Query, Mutation, EmptySubscription)
            .data(TorrentFilesRuntimeData::with_limits(
                runtime,
                TorrentFilesLimits::default(),
            ))
            .finish()
    }

    fn blob(info_hash: &str, files: &[BlobFile]) -> TorrentFilesBlob {
        TorrentFilesBlob {
            info_hash: info_hash.parse().expect("valid test info hash"),
            files_data: Some(serialize_files(files).expect("serialize test blob")),
            file_count: files.len(),
        }
    }

    #[tokio::test]
    async fn serves_path_derived_files_with_go_paging_flags() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let response = test_schema(vec![blob(
            hash,
            &[
                BlobFile {
                    index: 2,
                    path: "z/no-extension".to_owned(),
                    extension: "bogus".to_owned(),
                    size: 8,
                },
                BlobFile {
                    index: 0,
                    path: "a/Movie.MKV".to_owned(),
                    extension: "bogus".to_owned(),
                    size: 30,
                },
                BlobFile {
                    index: 1,
                    path: "b/readme.txt".to_owned(),
                    extension: "bogus".to_owned(),
                    size: 20,
                },
            ],
        )])
        .execute(format!(
            r#"{{ torrent {{ files(input: {{
                infoHashes: ["{hash}"], limit: 1, page: 2, offset: 0,
                cached: true, totalCount: true, hasNextPage: true,
                orderBy: [{{ field: size, descending: true }}]
            }}) {{ totalCount hasNextPage items {{
                infoHash index path extension fileType size createdAt updatedAt
            }} }} }} }}"#
        ))
        .await;

        assert!(
            response.errors.is_empty(),
            "torrent.files errors: {:?}",
            response.errors
        );
        assert_eq!(
            response.data,
            value!({
                "torrent": { "files": {
                    "totalCount": 3,
                    "hasNextPage": true,
                    "items": [{
                        "infoHash": hash,
                        "index": 1,
                        "path": "b/readme.txt",
                        "extension": "txt",
                        "fileType": "document",
                        "size": 20,
                        "createdAt": ZERO_DATETIME,
                        "updatedAt": ZERO_DATETIME,
                    }]
                }}
            })
        );
    }

    #[tokio::test]
    async fn default_sort_and_unrequested_counts_match_go() {
        let hash = "1111111111111111111111111111111111111111";
        let response = test_schema(vec![blob(
            hash,
            &[
                BlobFile {
                    index: 1,
                    path: "z/file.bin".to_owned(),
                    extension: String::new(),
                    size: 2,
                },
                BlobFile {
                    index: 0,
                    path: "a/file.mkv".to_owned(),
                    extension: String::new(),
                    size: 1,
                },
            ],
        )])
        .execute(format!(
            r#"{{ torrent {{ files(input: {{ infoHashes: ["{hash}"] }}) {{
                totalCount hasNextPage items {{ path extension fileType }}
            }} }} }}"#
        ))
        .await;

        assert!(response.errors.is_empty(), "errors: {:?}", response.errors);
        assert_eq!(
            response.data,
            value!({
                "torrent": { "files": {
                    "totalCount": 0,
                    "hasNextPage": false,
                    "items": [
                        {"path": "a/file.mkv", "extension": "mkv", "fileType": "video"},
                        {"path": "z/file.bin", "extension": "bin", "fileType": "software"},
                    ]
                }}
            })
        );
    }

    #[tokio::test]
    async fn rejects_absent_empty_and_oversized_hash_lists() {
        let schema = test_schema(Vec::new());
        for (query, message) in [
            (
                "{ torrent { files(input: {}) { totalCount } } }".to_owned(),
                "infoHashes must be provided",
            ),
            (
                "{ torrent { files(input: { infoHashes: [] }) { totalCount } } }".to_owned(),
                "infoHashes must not be empty",
            ),
            (
                format!(
                    "{{ torrent {{ files(input: {{ infoHashes: [{}] }}) {{ totalCount }} }} }}",
                    std::iter::repeat_n(
                        "\"0123456789abcdef0123456789abcdef01234567\"",
                        TorrentFilesLimits::default().max_info_hashes + 1,
                    )
                    .collect::<Vec<_>>()
                    .join(",")
                ),
                "above the limit",
            ),
        ] {
            let response = schema.execute(query).await;
            assert_eq!(response.errors.len(), 1);
            assert!(
                response.errors[0].message.contains(message),
                "expected {message:?}, got {:?}",
                response.errors
            );
        }
    }

    #[tokio::test]
    async fn fails_closed_on_summary_count_mismatch() {
        let hash = "2222222222222222222222222222222222222222";
        let mut inconsistent = blob(
            hash,
            &[BlobFile {
                index: 0,
                path: "only.txt".to_owned(),
                extension: "txt".to_owned(),
                size: 1,
            }],
        );
        inconsistent.file_count = 2;
        let response = test_schema(vec![inconsistent])
            .execute(format!(
                "{{ torrent {{ files(input: {{ infoHashes: [\"{hash}\"] }}) {{ totalCount }} }} }}"
            ))
            .await;

        assert_eq!(response.errors.len(), 1);
        assert!(response.errors[0].message.contains("count mismatch"));
    }
}
