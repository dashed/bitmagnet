//! Atomic PostgreSQL persistence for one planned DHT torrent batch.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;

use async_trait::async_trait;
use bitmagnet_db::PgPool;
use bitmagnet_dht::Id20;
use bitmagnet_model::InfoHash;
use bitmagnet_queue::{prepare_pg_queue_job_values, PgQueueJobValues, QueuePgError};
use sqlx::{Postgres, QueryBuilder};

use crate::{
    DhtTorrentBatchWriteError, DhtTorrentBatchWriter, DhtTorrentFileSummaryWrite,
    DhtTorrentFileWrite, DhtTorrentPiecesWrite, DhtTorrentSourceLinkWrite,
    DhtTorrentTransactionPlan, DhtTorrentWrite,
};

/// Maximum rows in one torrents/files/summaries/sources statement.
pub const PG_DHT_TORRENT_WRITE_CHUNK_LIMIT: usize = 100;
/// Maximum rows in one pieces or queue-jobs statement.
pub const PG_DHT_TORRENT_SMALL_WRITE_CHUNK_LIMIT: usize = 10;

/// Ordered PostgreSQL stages in one torrent-plan transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgDhtTorrentBatchWriteStage {
    Torrents,
    Files,
    FileSummaries,
    Sources,
    Pieces,
    QueueJobs,
}

impl PgDhtTorrentBatchWriteStage {
    /// Stable table-family label used in error context.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Torrents => "torrents",
            Self::Files => "torrent_files",
            Self::FileSummaries => "torrent_file_summary",
            Self::Sources => "torrents_torrent_sources",
            Self::Pieces => "torrent_pieces",
            Self::QueueJobs => "queue_jobs",
        }
    }
}

impl fmt::Display for PgDhtTorrentBatchWriteStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Validation or PostgreSQL error underlying one torrent writer outcome.
///
/// Execute variants carry the stage and zero-based chunk/row context. A
/// rollback failure retains both errors because the database outcome is then
/// unknowable. Validation and `BEGIN` errors happen before any eligible effect;
/// commit errors are always outcome-unknown.
#[derive(Debug)]
pub enum PgDhtTorrentBatchWriterError {
    DuplicateTorrentInfoHash {
        info_hash: Id20,
        first_index: usize,
        duplicate_index: usize,
    },
    DuplicateSummaryInfoHash {
        info_hash: InfoHash,
        first_index: usize,
        duplicate_index: usize,
    },
    IntegerOutOfRange {
        stage: PgDhtTorrentBatchWriteStage,
        row_index: usize,
        field: &'static str,
        value: u64,
    },
    NegativeInteger {
        stage: PgDhtTorrentBatchWriteStage,
        row_index: usize,
        field: &'static str,
        value: i64,
    },
    TextContainsNul {
        stage: PgDhtTorrentBatchWriteStage,
        row_index: usize,
        field: &'static str,
        element_index: Option<usize>,
    },
    BlobExtensionPresenceMismatch {
        torrent_index: usize,
        info_hash: Id20,
        files_data_present: bool,
        file_extensions_present: bool,
    },
    InvalidExtension {
        stage: PgDhtTorrentBatchWriteStage,
        row_index: usize,
        field: &'static str,
        element_index: usize,
        value: String,
        reason: &'static str,
    },
    ExtensionsNotStrictlySorted {
        stage: PgDhtTorrentBatchWriteStage,
        row_index: usize,
        field: &'static str,
        previous_index: usize,
        element_index: usize,
        previous: String,
        value: String,
    },
    InvalidMetaVersion {
        torrent_index: usize,
        info_hash: Id20,
        meta_version: u16,
    },
    InvalidTorrentIdentity {
        torrent_index: usize,
        info_hash: Id20,
        meta_version: u16,
        reason: &'static str,
    },
    QueuePreparation {
        job_index: usize,
        source: QueuePgError,
    },
    Begin {
        source: Box<sqlx::Error>,
    },
    ExecuteRolledBack {
        stage: PgDhtTorrentBatchWriteStage,
        chunk_index: usize,
        row_offset: usize,
        row_count: usize,
        source: Box<sqlx::Error>,
    },
    RollbackFailed {
        stage: PgDhtTorrentBatchWriteStage,
        chunk_index: usize,
        row_offset: usize,
        row_count: usize,
        execute_error: Box<sqlx::Error>,
        rollback_error: Box<sqlx::Error>,
    },
    Commit {
        source: Box<sqlx::Error>,
    },
}

impl fmt::Display for PgDhtTorrentBatchWriterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateTorrentInfoHash {
                info_hash,
                first_index,
                duplicate_index,
            } => write!(
                formatter,
                "duplicate torrent info hash {info_hash} at indexes {first_index} and {duplicate_index}"
            ),
            Self::DuplicateSummaryInfoHash {
                info_hash,
                first_index,
                duplicate_index,
            } => write!(
                formatter,
                "duplicate torrent summary info hash {info_hash} at indexes {first_index} and {duplicate_index}"
            ),
            Self::IntegerOutOfRange {
                stage,
                row_index,
                field,
                value,
            } => write!(
                formatter,
                "{stage} row {row_index} {field} value {value} exceeds its PostgreSQL signed integer type"
            ),
            Self::NegativeInteger {
                stage,
                row_index,
                field,
                value,
            } => write!(
                formatter,
                "{stage} row {row_index} {field} value {value} is negative"
            ),
            Self::TextContainsNul {
                stage,
                row_index,
                field,
                element_index,
            } => {
                write!(formatter, "{stage} row {row_index} {field}")?;
                if let Some(element_index) = element_index {
                    write!(formatter, " element {element_index}")?;
                }
                formatter.write_str(" contains a PostgreSQL-incompatible NUL")
            }
            Self::BlobExtensionPresenceMismatch {
                torrent_index,
                info_hash,
                files_data_present,
                file_extensions_present,
            } => write!(
                formatter,
                "torrent {torrent_index} ({info_hash}) has mismatched files_data ({files_data_present}) and file_extensions ({file_extensions_present}) presence"
            ),
            Self::InvalidExtension {
                stage,
                row_index,
                field,
                element_index,
                value,
                reason,
            } => write!(
                formatter,
                "{stage} row {row_index} {field} element {element_index} ({value:?}) is invalid: {reason}"
            ),
            Self::ExtensionsNotStrictlySorted {
                stage,
                row_index,
                field,
                previous_index,
                element_index,
                previous,
                value,
            } => write!(
                formatter,
                "{stage} row {row_index} {field} is not strictly sorted and unique between elements {previous_index} ({previous:?}) and {element_index} ({value:?})"
            ),
            Self::InvalidMetaVersion {
                torrent_index,
                info_hash,
                meta_version,
            } => write!(
                formatter,
                "torrent {torrent_index} ({info_hash}) has unsupported meta version {meta_version}"
            ),
            Self::InvalidTorrentIdentity {
                torrent_index,
                info_hash,
                meta_version,
                reason,
            } => write!(
                formatter,
                "torrent {torrent_index} ({info_hash}) has incoherent meta-version-{meta_version} identity: {reason}"
            ),
            Self::QueuePreparation { job_index, source } => {
                write!(formatter, "prepare queue job {job_index}: {source}")
            }
            Self::Begin { source } => write!(formatter, "begin DHT torrent transaction: {source}"),
            Self::ExecuteRolledBack {
                stage,
                chunk_index,
                row_offset,
                row_count,
                source,
            } => write!(
                formatter,
                "execute {stage} chunk {chunk_index} at offset {row_offset} with {row_count} rows, then rolled back: {source}"
            ),
            Self::RollbackFailed {
                stage,
                chunk_index,
                row_offset,
                row_count,
                execute_error,
                rollback_error,
            } => write!(
                formatter,
                "execute {stage} chunk {chunk_index} at offset {row_offset} with {row_count} rows failed ({execute_error}); rollback also failed ({rollback_error})"
            ),
            Self::Commit { source } => {
                write!(formatter, "commit DHT torrent transaction: {source}")
            }
        }
    }
}

impl Error for PgDhtTorrentBatchWriterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::QueuePreparation { source, .. } => Some(source),
            Self::Begin { source }
            | Self::ExecuteRolledBack { source, .. }
            | Self::Commit { source } => Some(source.as_ref()),
            Self::RollbackFailed { rollback_error, .. } => Some(rollback_error.as_ref()),
            Self::DuplicateTorrentInfoHash { .. }
            | Self::DuplicateSummaryInfoHash { .. }
            | Self::IntegerOutOfRange { .. }
            | Self::NegativeInteger { .. }
            | Self::TextContainsNul { .. }
            | Self::BlobExtensionPresenceMismatch { .. }
            | Self::InvalidExtension { .. }
            | Self::ExtensionsNotStrictlySorted { .. }
            | Self::InvalidMetaVersion { .. }
            | Self::InvalidTorrentIdentity { .. } => None,
        }
    }
}

/// PostgreSQL-backed atomic writer for [`DhtTorrentTransactionPlan`].
///
/// The adapter clones an application-owned pool and never creates, closes, or
/// retries it. It prevalidates all six collections before acquiring a
/// connection, then always opens and commits one transaction, including for an
/// empty plan. Dynamic statements execute in the fixed public stage order and
/// are non-persistent because placeholder counts vary by chunk.
///
/// PostgreSQL's transaction-stable `CURRENT_TIMESTAMP` owns every row timestamp
/// and queue delay calculation. Go instead binds application-clock timestamps;
/// this is an intentional clock-authority delta. Hashes are bound directly as
/// raw `bytea`, rather than Go's hexadecimal/decode construction. A successful
/// call confirms the transaction decision, not affected-row counts. Offline
/// tests do not claim live constraints, triggers, codecs, rollback behavior, or
/// equality to Go clock instants. Dropped-future uncertainty remains owned by
/// the worker contract.
#[derive(Clone, Debug)]
#[must_use = "the adapter must be installed as a DHT torrent batch writer"]
pub struct PgDhtTorrentBatchWriter {
    pool: PgPool,
}

impl PgDhtTorrentBatchWriter {
    /// Wrap an already configured, application-owned pool.
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the shared application pool.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn write_batch_db(
        &self,
        plan: &DhtTorrentTransactionPlan,
    ) -> Result<(), DhtTorrentBatchWriteError> {
        let prepared = prepare_plan(plan).map_err(DhtTorrentBatchWriteError::rejected)?;
        let mut transaction = self.pool.begin().await.map_err(|source| {
            DhtTorrentBatchWriteError::rejected(PgDhtTorrentBatchWriterError::Begin {
                source: Box::new(source),
            })
        })?;

        macro_rules! execute_chunks {
            ($rows:expr, $limit:expr, $stage:expr, $builder:ident) => {
                for (chunk_index, chunk) in $rows.chunks($limit).enumerate() {
                    let row_offset = chunk_index * $limit;
                    let mut query = $builder(chunk);
                    if let Err(execute_error) = query
                        .build()
                        .persistent(false)
                        .execute(&mut *transaction)
                        .await
                    {
                        return match transaction.rollback().await {
                            Ok(()) => Err(DhtTorrentBatchWriteError::rejected(
                                PgDhtTorrentBatchWriterError::ExecuteRolledBack {
                                    stage: $stage,
                                    chunk_index,
                                    row_offset,
                                    row_count: chunk.len(),
                                    source: Box::new(execute_error),
                                },
                            )),
                            Err(rollback_error) => Err(DhtTorrentBatchWriteError::outcome_unknown(
                                PgDhtTorrentBatchWriterError::RollbackFailed {
                                    stage: $stage,
                                    chunk_index,
                                    row_offset,
                                    row_count: chunk.len(),
                                    execute_error: Box::new(execute_error),
                                    rollback_error: Box::new(rollback_error),
                                },
                            )),
                        };
                    }
                }
            };
        }

        execute_chunks!(
            prepared.torrents,
            PG_DHT_TORRENT_WRITE_CHUNK_LIMIT,
            PgDhtTorrentBatchWriteStage::Torrents,
            build_torrents_query
        );
        execute_chunks!(
            prepared.files,
            PG_DHT_TORRENT_WRITE_CHUNK_LIMIT,
            PgDhtTorrentBatchWriteStage::Files,
            build_files_query
        );
        execute_chunks!(
            prepared.file_summaries,
            PG_DHT_TORRENT_WRITE_CHUNK_LIMIT,
            PgDhtTorrentBatchWriteStage::FileSummaries,
            build_summaries_query
        );
        execute_chunks!(
            prepared.sources,
            PG_DHT_TORRENT_WRITE_CHUNK_LIMIT,
            PgDhtTorrentBatchWriteStage::Sources,
            build_sources_query
        );
        execute_chunks!(
            prepared.pieces,
            PG_DHT_TORRENT_SMALL_WRITE_CHUNK_LIMIT,
            PgDhtTorrentBatchWriteStage::Pieces,
            build_pieces_query
        );
        execute_chunks!(
            prepared.queue_jobs,
            PG_DHT_TORRENT_SMALL_WRITE_CHUNK_LIMIT,
            PgDhtTorrentBatchWriteStage::QueueJobs,
            build_queue_query
        );

        transaction.commit().await.map_err(|source| {
            DhtTorrentBatchWriteError::outcome_unknown(PgDhtTorrentBatchWriterError::Commit {
                source: Box::new(source),
            })
        })
    }
}

#[async_trait]
impl DhtTorrentBatchWriter for PgDhtTorrentBatchWriter {
    async fn write_batch(
        &self,
        plan: &DhtTorrentTransactionPlan,
    ) -> Result<(), DhtTorrentBatchWriteError> {
        self.write_batch_db(plan).await
    }
}

struct PreparedPlan<'a> {
    torrents: Vec<PreparedTorrent<'a>>,
    files: Vec<PreparedFile<'a>>,
    file_summaries: Vec<PreparedSummary<'a>>,
    sources: Vec<PreparedSource<'a>>,
    pieces: Vec<PreparedPieces<'a>>,
    queue_jobs: Vec<PreparedQueueJob<'a>>,
}

#[derive(Clone, Copy)]
struct PreparedTorrent<'a> {
    row: &'a DhtTorrentWrite,
    meta_version: i16,
    size: i64,
    files_count: Option<i32>,
    file_extensions: &'a [String],
}

#[derive(Clone, Copy)]
struct PreparedFile<'a> {
    row: &'a DhtTorrentFileWrite,
    index: i32,
    size: i64,
}

#[derive(Clone, Copy)]
struct PreparedSummary<'a> {
    row: &'a DhtTorrentFileSummaryWrite,
    file_count: i32,
    compressed_bytes: Option<i64>,
}

#[derive(Clone, Copy)]
struct PreparedSource<'a> {
    row: &'a DhtTorrentSourceLinkWrite,
}

#[derive(Clone, Copy)]
struct PreparedPieces<'a> {
    row: &'a DhtTorrentPiecesWrite,
}

#[derive(Clone, Copy)]
struct PreparedQueueJob<'a> {
    values: PgQueueJobValues<'a>,
}

fn prepare_plan(
    plan: &DhtTorrentTransactionPlan,
) -> Result<PreparedPlan<'_>, PgDhtTorrentBatchWriterError> {
    Ok(PreparedPlan {
        torrents: prepare_torrents(&plan.torrents)?,
        files: prepare_files(&plan.files)?,
        file_summaries: prepare_summaries(&plan.file_summaries)?,
        sources: prepare_sources(&plan.sources)?,
        pieces: prepare_pieces(&plan.pieces)?,
        queue_jobs: prepare_queue_jobs(&plan.queue_jobs)?,
    })
}

fn prepare_torrents(
    rows: &[DhtTorrentWrite],
) -> Result<Vec<PreparedTorrent<'_>>, PgDhtTorrentBatchWriterError> {
    let stage = PgDhtTorrentBatchWriteStage::Torrents;
    let mut first_indexes = HashMap::with_capacity(rows.len());
    let mut prepared = Vec::with_capacity(rows.len());
    for (row_index, row) in rows.iter().enumerate() {
        if let Some(first_index) = first_indexes.insert(row.info_hash, row_index) {
            return Err(PgDhtTorrentBatchWriterError::DuplicateTorrentInfoHash {
                info_hash: row.info_hash,
                first_index,
                duplicate_index: row_index,
            });
        }
        require_no_nul(stage, row_index, "name", None, &row.name)?;
        let meta_version = validate_identity(row_index, row)?;
        let size = checked_i64(stage, row_index, "size", row.size)?;
        let files_count = row
            .files_count
            .map(|value| checked_i32(stage, row_index, "files_count", value))
            .transpose()?;
        let files_data_present = row.files_data.is_some();
        let file_extensions_present = row.file_extensions.is_some();
        if files_data_present != file_extensions_present {
            return Err(
                PgDhtTorrentBatchWriterError::BlobExtensionPresenceMismatch {
                    torrent_index: row_index,
                    info_hash: row.info_hash,
                    files_data_present,
                    file_extensions_present,
                },
            );
        }
        let file_extensions = row.file_extensions.as_deref().unwrap_or(&[]);
        validate_extensions(stage, row_index, "file_extensions", file_extensions)?;
        prepared.push(PreparedTorrent {
            row,
            meta_version,
            size,
            files_count,
            file_extensions,
        });
    }
    Ok(prepared)
}

fn prepare_files(
    rows: &[DhtTorrentFileWrite],
) -> Result<Vec<PreparedFile<'_>>, PgDhtTorrentBatchWriterError> {
    let stage = PgDhtTorrentBatchWriteStage::Files;
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            require_no_nul(stage, row_index, "path", None, &row.path)?;
            Ok(PreparedFile {
                row,
                index: checked_i32(stage, row_index, "index", row.index)?,
                size: checked_i64(stage, row_index, "size", row.size)?,
            })
        })
        .collect()
}

fn prepare_summaries(
    rows: &[DhtTorrentFileSummaryWrite],
) -> Result<Vec<PreparedSummary<'_>>, PgDhtTorrentBatchWriterError> {
    let stage = PgDhtTorrentBatchWriteStage::FileSummaries;
    let mut first_indexes = HashMap::with_capacity(rows.len());
    let mut prepared = Vec::with_capacity(rows.len());
    for (row_index, row) in rows.iter().enumerate() {
        let summary = &row.summary;
        if let Some(first_index) = first_indexes.insert(summary.info_hash, row_index) {
            return Err(PgDhtTorrentBatchWriterError::DuplicateSummaryInfoHash {
                info_hash: summary.info_hash,
                first_index,
                duplicate_index: row_index,
            });
        }
        if summary.total_size < 0 {
            return Err(PgDhtTorrentBatchWriterError::NegativeInteger {
                stage,
                row_index,
                field: "total_size",
                value: summary.total_size,
            });
        }
        if summary.largest_file_size < 0 {
            return Err(PgDhtTorrentBatchWriterError::NegativeInteger {
                stage,
                row_index,
                field: "largest_file_size",
                value: summary.largest_file_size,
            });
        }
        validate_extensions(stage, row_index, "extensions", &summary.extensions)?;
        prepared.push(PreparedSummary {
            row,
            file_count: checked_i32(stage, row_index, "file_count", summary.file_count)?,
            compressed_bytes: row
                .compressed_bytes
                .map(|value| checked_i64(stage, row_index, "compressed_bytes", value))
                .transpose()?,
        });
    }
    Ok(prepared)
}

fn prepare_sources(
    rows: &[DhtTorrentSourceLinkWrite],
) -> Result<Vec<PreparedSource<'_>>, PgDhtTorrentBatchWriterError> {
    let stage = PgDhtTorrentBatchWriteStage::Sources;
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            require_no_nul(stage, row_index, "source", None, &row.source)?;
            Ok(PreparedSource { row })
        })
        .collect()
}

fn prepare_pieces(
    rows: &[DhtTorrentPiecesWrite],
) -> Result<Vec<PreparedPieces<'_>>, PgDhtTorrentBatchWriterError> {
    let stage = PgDhtTorrentBatchWriteStage::Pieces;
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            if row.piece_length < 0 {
                return Err(PgDhtTorrentBatchWriterError::NegativeInteger {
                    stage,
                    row_index,
                    field: "piece_length",
                    value: row.piece_length,
                });
            }
            Ok(PreparedPieces { row })
        })
        .collect()
}

fn prepare_queue_jobs(
    rows: &[bitmagnet_queue::QueueJob],
) -> Result<Vec<PreparedQueueJob<'_>>, PgDhtTorrentBatchWriterError> {
    rows.iter()
        .enumerate()
        .map(|(job_index, job)| {
            let values = prepare_pg_queue_job_values(job).map_err(|source| {
                PgDhtTorrentBatchWriterError::QueuePreparation { job_index, source }
            })?;
            Ok(PreparedQueueJob { values })
        })
        .collect()
}

fn validate_identity(
    torrent_index: usize,
    row: &DhtTorrentWrite,
) -> Result<i16, PgDhtTorrentBatchWriterError> {
    match row.meta_version {
        1 => {
            if row.info_hash_v1 != Some(row.info_hash) || row.info_hash_v2.is_some() {
                return Err(PgDhtTorrentBatchWriterError::InvalidTorrentIdentity {
                    torrent_index,
                    info_hash: row.info_hash,
                    meta_version: row.meta_version,
                    reason: "v1 requires matching info_hash_v1 and no info_hash_v2",
                });
            }
        }
        2 => {
            let Some(info_hash_v2) = row.info_hash_v2 else {
                return Err(PgDhtTorrentBatchWriterError::InvalidTorrentIdentity {
                    torrent_index,
                    info_hash: row.info_hash,
                    meta_version: row.meta_version,
                    reason: "v2 requires info_hash_v2",
                });
            };
            let matches_v1 = row.info_hash_v1 == Some(row.info_hash);
            let matches_truncated_v2 = row.info_hash.as_bytes().as_slice() == &info_hash_v2[..20];
            if !matches_v1 && !matches_truncated_v2 {
                return Err(PgDhtTorrentBatchWriterError::InvalidTorrentIdentity {
                    torrent_index,
                    info_hash: row.info_hash,
                    meta_version: row.meta_version,
                    reason: "primary is neither info_hash_v1 nor truncated info_hash_v2",
                });
            }
        }
        meta_version => {
            return Err(PgDhtTorrentBatchWriterError::InvalidMetaVersion {
                torrent_index,
                info_hash: row.info_hash,
                meta_version,
            });
        }
    }
    Ok(i16::try_from(row.meta_version).expect("validated meta version fits smallint"))
}

fn checked_i32(
    stage: PgDhtTorrentBatchWriteStage,
    row_index: usize,
    field: &'static str,
    value: u32,
) -> Result<i32, PgDhtTorrentBatchWriterError> {
    i32::try_from(value).map_err(|_| PgDhtTorrentBatchWriterError::IntegerOutOfRange {
        stage,
        row_index,
        field,
        value: u64::from(value),
    })
}

fn checked_i64(
    stage: PgDhtTorrentBatchWriteStage,
    row_index: usize,
    field: &'static str,
    value: u64,
) -> Result<i64, PgDhtTorrentBatchWriterError> {
    i64::try_from(value).map_err(|_| PgDhtTorrentBatchWriterError::IntegerOutOfRange {
        stage,
        row_index,
        field,
        value,
    })
}

fn require_no_nul(
    stage: PgDhtTorrentBatchWriteStage,
    row_index: usize,
    field: &'static str,
    element_index: Option<usize>,
    value: &str,
) -> Result<(), PgDhtTorrentBatchWriterError> {
    if value.contains('\0') {
        Err(PgDhtTorrentBatchWriterError::TextContainsNul {
            stage,
            row_index,
            field,
            element_index,
        })
    } else {
        Ok(())
    }
}

fn validate_extensions(
    stage: PgDhtTorrentBatchWriteStage,
    row_index: usize,
    field: &'static str,
    extensions: &[String],
) -> Result<(), PgDhtTorrentBatchWriterError> {
    for (element_index, extension) in extensions.iter().enumerate() {
        require_no_nul(stage, row_index, field, Some(element_index), extension)?;
        if extension.is_empty() {
            return Err(PgDhtTorrentBatchWriterError::InvalidExtension {
                stage,
                row_index,
                field,
                element_index,
                value: extension.clone(),
                reason: "extension must be nonempty",
            });
        }
        if !extension
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        {
            return Err(PgDhtTorrentBatchWriterError::InvalidExtension {
                stage,
                row_index,
                field,
                element_index,
                value: extension.clone(),
                reason: "extension must contain only lowercase ASCII letters and digits",
            });
        }
    }
    for (previous_index, pair) in extensions.windows(2).enumerate() {
        if pair[0] >= pair[1] {
            return Err(PgDhtTorrentBatchWriterError::ExtensionsNotStrictlySorted {
                stage,
                row_index,
                field,
                previous_index,
                element_index: previous_index + 1,
                previous: pair[0].clone(),
                value: pair[1].clone(),
            });
        }
    }
    Ok(())
}

fn build_torrents_query(rows: &[PreparedTorrent<'_>]) -> QueryBuilder<Postgres> {
    debug_assert!(!rows.is_empty());
    debug_assert!(rows.len() <= PG_DHT_TORRENT_WRITE_CHUNK_LIMIT);
    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO torrents (info_hash, info_hash_v1, info_hash_v2, meta_version, name, size, private, files_status, files_count, files_data, file_extensions, created_at, updated_at) ",
    );
    query.push_values(rows, |mut values, row| {
        values
            .push_bind(row.row.info_hash.as_bytes().as_slice())
            .push_bind(
                row.row
                    .info_hash_v1
                    .as_ref()
                    .map(|hash| hash.as_bytes().as_slice()),
            )
            .push_bind(row.row.info_hash_v2.as_ref().map(<[u8; 32]>::as_slice))
            .push_bind(row.meta_version)
            .push_bind(&row.row.name)
            .push_bind(row.size)
            .push_bind(row.row.private)
            .push_bind(row.row.files_status.as_str())
            .push_unseparated("::\"FilesStatus\"")
            .push_bind(row.files_count)
            .push_bind(row.row.files_data.as_deref())
            .push_bind(sqlx::types::Json(row.file_extensions));
        values.push("CURRENT_TIMESTAMP").push("CURRENT_TIMESTAMP");
    });
    query.push(
        " ON CONFLICT (info_hash) DO UPDATE SET name = EXCLUDED.name, files_status = EXCLUDED.files_status, files_count = EXCLUDED.files_count, updated_at = EXCLUDED.updated_at, files_data = EXCLUDED.files_data, file_extensions = EXCLUDED.file_extensions",
    );
    query
}

fn build_files_query(rows: &[PreparedFile<'_>]) -> QueryBuilder<Postgres> {
    debug_assert!(!rows.is_empty());
    debug_assert!(rows.len() <= PG_DHT_TORRENT_WRITE_CHUNK_LIMIT);
    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO torrent_files (info_hash, index, path, size, created_at, updated_at) ",
    );
    query.push_values(rows, |mut values, row| {
        values
            .push_bind(row.row.info_hash.as_bytes().as_slice())
            .push_bind(row.index)
            .push_bind(&row.row.path)
            .push_bind(row.size)
            .push("CURRENT_TIMESTAMP")
            .push("CURRENT_TIMESTAMP");
    });
    query.push(" ON CONFLICT DO NOTHING");
    query
}

fn build_summaries_query(rows: &[PreparedSummary<'_>]) -> QueryBuilder<Postgres> {
    debug_assert!(!rows.is_empty());
    debug_assert!(rows.len() <= PG_DHT_TORRENT_WRITE_CHUNK_LIMIT);
    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO torrent_file_summary (info_hash, file_count, total_size, largest_file_size, extensions, has_video, has_subtitle, has_audio, compressed_bytes, created_at, updated_at) ",
    );
    query.push_values(rows, |mut values, row| {
        let summary = &row.row.summary;
        values
            .push_bind(summary.info_hash.as_slice())
            .push_bind(row.file_count)
            .push_bind(summary.total_size)
            .push_bind(summary.largest_file_size)
            .push_bind(sqlx::types::Json(summary.extensions.as_slice()))
            .push_bind(summary.has_video)
            .push_bind(summary.has_subtitle)
            .push_bind(summary.has_audio)
            .push_bind(row.compressed_bytes)
            .push("CURRENT_TIMESTAMP")
            .push("CURRENT_TIMESTAMP");
    });
    query.push(
        " ON CONFLICT (info_hash) DO UPDATE SET file_count = EXCLUDED.file_count, total_size = EXCLUDED.total_size, largest_file_size = EXCLUDED.largest_file_size, extensions = EXCLUDED.extensions, has_video = EXCLUDED.has_video, has_subtitle = EXCLUDED.has_subtitle, has_audio = EXCLUDED.has_audio, compressed_bytes = EXCLUDED.compressed_bytes, updated_at = EXCLUDED.updated_at",
    );
    query
}

fn build_sources_query(rows: &[PreparedSource<'_>]) -> QueryBuilder<Postgres> {
    debug_assert!(!rows.is_empty());
    debug_assert!(rows.len() <= PG_DHT_TORRENT_WRITE_CHUNK_LIMIT);
    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO torrents_torrent_sources (source, info_hash, import_id, seeders, leechers, published_at, seen_count, created_at, updated_at) ",
    );
    query.push_values(rows, |mut values, row| {
        values
            .push_bind(&row.row.source)
            .push_bind(row.row.info_hash.as_bytes().as_slice())
            .push("NULL")
            .push("NULL")
            .push("NULL")
            .push("NULL")
            .push("1")
            .push("CURRENT_TIMESTAMP")
            .push("CURRENT_TIMESTAMP");
    });
    query.push(" ON CONFLICT DO NOTHING");
    query
}

fn build_pieces_query(rows: &[PreparedPieces<'_>]) -> QueryBuilder<Postgres> {
    debug_assert!(!rows.is_empty());
    debug_assert!(rows.len() <= PG_DHT_TORRENT_SMALL_WRITE_CHUNK_LIMIT);
    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO torrent_pieces (info_hash, piece_length, pieces, created_at) ",
    );
    query.push_values(rows, |mut values, row| {
        values
            .push_bind(row.row.info_hash.as_bytes().as_slice())
            .push_bind(row.row.piece_length)
            .push_bind(row.row.pieces.as_slice())
            .push("CURRENT_TIMESTAMP");
    });
    query.push(" ON CONFLICT DO NOTHING");
    query
}

fn build_queue_query(rows: &[PreparedQueueJob<'_>]) -> QueryBuilder<Postgres> {
    debug_assert!(!rows.is_empty());
    debug_assert!(rows.len() <= PG_DHT_TORRENT_SMALL_WRITE_CHUNK_LIMIT);
    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO queue_jobs (fingerprint, queue, status, payload, retries, max_retries, run_after, ran_at, error, deadline, archival_duration, created_at, priority) ",
    );
    query.push_values(rows, |mut values, row| {
        values
            .push_bind(&row.values.job.fingerprint)
            .push_bind(&row.values.job.queue)
            .push("'pending'::queue_job_status")
            .push_bind(&row.values.job.payload)
            .push_unseparated("::jsonb")
            .push("0")
            .push_bind(row.values.max_retries)
            .push("CURRENT_TIMESTAMP + ")
            .push_bind_unseparated(row.values.delay)
            .push("NULL")
            .push("NULL")
            .push("NULL")
            .push_bind(row.values.archival_duration)
            .push("CURRENT_TIMESTAMP")
            .push_bind(row.values.job.priority);
    });
    query
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::time::Duration;

    use bitmagnet_model::{FilesStatus, TorrentFileSummary};
    use bitmagnet_queue::{
        fingerprint, process_torrent_job, ProcessTorrentParams, ProtocolId, QueueJob,
        QueueJobOptions, QueueJobStatus, DEFAULT_ARCHIVAL_DURATION,
    };
    use sqlx::postgres::{types::PgInterval, PgPoolOptions};
    use sqlx::{Arguments, Execute, Type, TypeInfo};

    use super::*;

    fn id(value: usize) -> Id20 {
        let mut bytes = [0_u8; 20];
        bytes[12..].copy_from_slice(&(value as u64).to_be_bytes());
        Id20::from_slice(&bytes).unwrap()
    }

    fn model_id(value: usize) -> InfoHash {
        InfoHash::new(*id(value).as_bytes())
    }

    fn torrent(value: usize) -> DhtTorrentWrite {
        DhtTorrentWrite {
            info_hash: id(value),
            info_hash_v1: Some(id(value)),
            info_hash_v2: None,
            meta_version: 1,
            name: format!("torrent-{value}"),
            size: u64::try_from(value + 1).unwrap(),
            private: value.is_multiple_of(2),
            files_status: FilesStatus::Multi,
            files_count: Some(1),
            files_data: Some(vec![u8::try_from(value % 255).unwrap()]),
            file_extensions: Some(vec!["mkv".to_owned()]),
        }
    }

    fn file(value: usize) -> DhtTorrentFileWrite {
        DhtTorrentFileWrite {
            info_hash: id(value),
            index: u32::try_from(value).unwrap(),
            path: format!("dir/file-{value}.mkv"),
            size: u64::try_from(value + 10).unwrap(),
        }
    }

    fn summary(value: usize) -> DhtTorrentFileSummaryWrite {
        DhtTorrentFileSummaryWrite {
            summary: TorrentFileSummary {
                info_hash: model_id(value),
                file_count: 1,
                total_size: i64::try_from(value + 10).unwrap(),
                largest_file_size: i64::try_from(value + 10).unwrap(),
                extensions: vec!["mkv".to_owned()],
                has_video: true,
                has_subtitle: false,
                has_audio: false,
            },
            compressed_bytes: Some(u64::try_from(value + 1).unwrap()),
        }
    }

    fn source(value: usize) -> DhtTorrentSourceLinkWrite {
        DhtTorrentSourceLinkWrite {
            source: "dht".to_owned(),
            info_hash: id(value),
        }
    }

    fn pieces(value: usize) -> DhtTorrentPiecesWrite {
        DhtTorrentPiecesWrite {
            info_hash: id(value),
            piece_length: 16_384,
            pieces: vec![u8::try_from(value % 255).unwrap(); 20],
        }
    }

    fn queue_job(value: usize) -> bitmagnet_queue::QueueJob {
        process_torrent_job(
            &ProcessTorrentParams {
                info_hashes: vec![ProtocolId::from_bytes(*id(value).as_bytes())],
                ..ProcessTorrentParams::default()
            },
            QueueJobOptions::default()
                .with_delay(Duration::from_secs(60))
                .with_priority(10),
        )
        .unwrap()
    }

    fn valid_plan() -> DhtTorrentTransactionPlan {
        DhtTorrentTransactionPlan {
            torrents: vec![torrent(1)],
            files: vec![file(1)],
            file_summaries: vec![summary(1)],
            sources: vec![source(1)],
            pieces: vec![pieces(1)],
            queue_jobs: vec![queue_job(1)],
        }
    }

    fn inspect_query(mut builder: QueryBuilder<Postgres>) -> (String, usize, bool) {
        let sql = builder.sql().as_str().to_owned();
        let mut query = builder.build().persistent(false);
        let persistent = Execute::persistent(&query);
        let arguments = query
            .take_arguments()
            .unwrap()
            .expect("QueryBuilder supplies arguments");
        (sql, arguments.len(), persistent)
    }

    fn typed_source(error: &DhtTorrentBatchWriteError) -> &PgDhtTorrentBatchWriterError {
        let source = match error {
            DhtTorrentBatchWriteError::Rejected { source }
            | DhtTorrentBatchWriteError::OutcomeUnknown { source } => source,
        };
        source
            .downcast_ref::<PgDhtTorrentBatchWriterError>()
            .expect("adapter preserves its typed source")
    }

    fn assert_send_sync<T: Send + Sync>() {}

    fn assert_send_future<F: Future + Send>(future: F) {
        drop(future);
    }

    #[test]
    fn public_surface_defaults_stage_order_and_types_are_exact() {
        assert_eq!(PG_DHT_TORRENT_WRITE_CHUNK_LIMIT, 100);
        assert_eq!(PG_DHT_TORRENT_SMALL_WRITE_CHUNK_LIMIT, 10);
        assert_eq!(
            [
                PgDhtTorrentBatchWriteStage::Torrents,
                PgDhtTorrentBatchWriteStage::Files,
                PgDhtTorrentBatchWriteStage::FileSummaries,
                PgDhtTorrentBatchWriteStage::Sources,
                PgDhtTorrentBatchWriteStage::Pieces,
                PgDhtTorrentBatchWriteStage::QueueJobs,
            ]
            .map(PgDhtTorrentBatchWriteStage::as_str),
            [
                "torrents",
                "torrent_files",
                "torrent_file_summary",
                "torrents_torrent_sources",
                "torrent_pieces",
                "queue_jobs",
            ]
        );
        assert_send_sync::<PgDhtTorrentBatchWriter>();
        assert_send_sync::<PgDhtTorrentBatchWriterError>();
        assert_eq!(<&str as Type<Postgres>>::type_info().name(), "TEXT");
        assert_eq!(<&[u8] as Type<Postgres>>::type_info().name(), "BYTEA");
        assert_eq!(<i16 as Type<Postgres>>::type_info().name(), "INT2");
        assert_eq!(<i32 as Type<Postgres>>::type_info().name(), "INT4");
        assert_eq!(<i64 as Type<Postgres>>::type_info().name(), "INT8");
        assert_eq!(<bool as Type<Postgres>>::type_info().name(), "BOOL");
        assert_eq!(
            <sqlx::types::Json<&[String]> as Type<Postgres>>::type_info().name(),
            "JSONB"
        );
        assert_eq!(
            <PgInterval as Type<Postgres>>::type_info().name(),
            "INTERVAL"
        );
    }

    #[test]
    fn all_six_queries_have_exact_sql_bind_counts_conflicts_and_clock_shape() {
        let plan = valid_plan();
        let prepared = prepare_plan(&plan).unwrap();
        let queries = [
            inspect_query(build_torrents_query(&prepared.torrents)),
            inspect_query(build_files_query(&prepared.files)),
            inspect_query(build_summaries_query(&prepared.file_summaries)),
            inspect_query(build_sources_query(&prepared.sources)),
            inspect_query(build_pieces_query(&prepared.pieces)),
            inspect_query(build_queue_query(&prepared.queue_jobs)),
        ];
        assert_eq!(
            queries.iter().map(|query| query.1).collect::<Vec<_>>(),
            [11, 4, 9, 2, 3, 7]
        );
        assert!(queries.iter().all(|query| !query.2));

        assert_eq!(
            queries[0].0,
            "INSERT INTO torrents (info_hash, info_hash_v1, info_hash_v2, meta_version, name, size, private, files_status, files_count, files_data, file_extensions, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::\"FilesStatus\", $9, $10, $11, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (info_hash) DO UPDATE SET name = EXCLUDED.name, files_status = EXCLUDED.files_status, files_count = EXCLUDED.files_count, updated_at = EXCLUDED.updated_at, files_data = EXCLUDED.files_data, file_extensions = EXCLUDED.file_extensions"
        );
        assert_eq!(
            queries[1].0,
            "INSERT INTO torrent_files (info_hash, index, path, size, created_at, updated_at) VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING"
        );
        assert_eq!(
            queries[2].0,
            "INSERT INTO torrent_file_summary (info_hash, file_count, total_size, largest_file_size, extensions, has_video, has_subtitle, has_audio, compressed_bytes, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (info_hash) DO UPDATE SET file_count = EXCLUDED.file_count, total_size = EXCLUDED.total_size, largest_file_size = EXCLUDED.largest_file_size, extensions = EXCLUDED.extensions, has_video = EXCLUDED.has_video, has_subtitle = EXCLUDED.has_subtitle, has_audio = EXCLUDED.has_audio, compressed_bytes = EXCLUDED.compressed_bytes, updated_at = EXCLUDED.updated_at"
        );
        assert_eq!(
            queries[3].0,
            "INSERT INTO torrents_torrent_sources (source, info_hash, import_id, seeders, leechers, published_at, seen_count, created_at, updated_at) VALUES ($1, $2, NULL, NULL, NULL, NULL, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING"
        );
        assert_eq!(
            queries[4].0,
            "INSERT INTO torrent_pieces (info_hash, piece_length, pieces, created_at) VALUES ($1, $2, $3, CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING"
        );
        assert_eq!(
            queries[5].0,
            "INSERT INTO queue_jobs (fingerprint, queue, status, payload, retries, max_retries, run_after, ran_at, error, deadline, archival_duration, created_at, priority) VALUES ($1, $2, 'pending'::queue_job_status, $3::jsonb, 0, $4, CURRENT_TIMESTAMP + $5, NULL, NULL, NULL, $6, CURRENT_TIMESTAMP, $7)"
        );

        let torrent_updates = queries[0].0.split_once("DO UPDATE SET").unwrap().1;
        for preserved in [
            "size =",
            "private =",
            "info_hash_v1 =",
            "info_hash_v2 =",
            "meta_version =",
            "created_at =",
        ] {
            assert!(!torrent_updates.contains(preserved));
        }
        let summary_updates = queries[2].0.split_once("DO UPDATE SET").unwrap().1;
        assert_eq!(summary_updates.matches(" = EXCLUDED.").count(), 9);
        assert!(!summary_updates.contains("created_at ="));
        assert!(!queries[5].0.contains("ON CONFLICT"));
        assert!(!queries.iter().any(|query| query.0.contains("RETURNING")));
    }

    #[test]
    fn nullable_blob_and_extension_normalization_are_exact() {
        let mut none = torrent(1);
        none.files_data = None;
        none.file_extensions = None;
        let mut empty = torrent(2);
        empty.files_data = Some(Vec::new());
        empty.file_extensions = Some(Vec::new());
        let rows = [none, empty];
        let prepared = prepare_torrents(&rows).unwrap();

        assert!(prepared[0].row.files_data.is_none());
        assert!(prepared[1].row.files_data.as_ref().unwrap().is_empty());
        assert_eq!(prepared[0].file_extensions, &[] as &[String]);
        assert_eq!(prepared[1].file_extensions, &[] as &[String]);
        let (sql, arguments, persistent) = inspect_query(build_torrents_query(&prepared));
        assert_eq!(arguments, 22);
        assert!(!persistent);
        assert!(sql.contains("$11, CURRENT_TIMESTAMP"));
        assert!(sql.contains("$22, CURRENT_TIMESTAMP"));
        assert!(!sql.contains("::jsonb"));
    }

    #[test]
    fn blob_and_extension_presence_must_match_in_both_directions() {
        let mut blob_only = torrent(1);
        blob_only.file_extensions = None;
        assert!(matches!(
            prepare_torrents(&[blob_only]),
            Err(PgDhtTorrentBatchWriterError::BlobExtensionPresenceMismatch {
                torrent_index: 0,
                info_hash,
                files_data_present: true,
                file_extensions_present: false,
            }) if info_hash == id(1)
        ));

        let mut extensions_only = torrent(2);
        extensions_only.files_data = None;
        extensions_only.file_extensions = Some(Vec::new());
        assert!(matches!(
            prepare_torrents(&[extensions_only]),
            Err(PgDhtTorrentBatchWriterError::BlobExtensionPresenceMismatch {
                torrent_index: 0,
                info_hash,
                files_data_present: false,
                file_extensions_present: true,
            }) if info_hash == id(2)
        ));
    }

    #[test]
    fn extension_arrays_are_nonempty_lowercase_ascii_sorted_and_unique() {
        for (extensions, expected_value, expected_reason) in [
            (vec!["".to_owned()], "", "extension must be nonempty"),
            (
                vec!["MKV".to_owned()],
                "MKV",
                "extension must contain only lowercase ASCII letters and digits",
            ),
            (
                vec!["mk-v".to_owned()],
                "mk-v",
                "extension must contain only lowercase ASCII letters and digits",
            ),
        ] {
            let mut row = torrent(1);
            row.file_extensions = Some(extensions);
            assert!(matches!(
                prepare_torrents(&[row]),
                Err(PgDhtTorrentBatchWriterError::InvalidExtension {
                    stage: PgDhtTorrentBatchWriteStage::Torrents,
                    row_index: 0,
                    field: "file_extensions",
                    element_index: 0,
                    value,
                    reason,
                }) if value == expected_value && reason == expected_reason
            ));
        }

        let mut unsorted = summary(1);
        unsorted.summary.extensions = vec!["srt".to_owned(), "mkv".to_owned()];
        assert!(matches!(
            prepare_summaries(&[unsorted]),
            Err(PgDhtTorrentBatchWriterError::ExtensionsNotStrictlySorted {
                stage: PgDhtTorrentBatchWriteStage::FileSummaries,
                row_index: 0,
                field: "extensions",
                previous_index: 0,
                element_index: 1,
                previous,
                value,
            }) if previous == "srt" && value == "mkv"
        ));

        let mut duplicate = torrent(2);
        duplicate.file_extensions = Some(vec!["mkv".to_owned(), "mkv".to_owned()]);
        assert!(matches!(
            prepare_torrents(&[duplicate]),
            Err(PgDhtTorrentBatchWriterError::ExtensionsNotStrictlySorted {
                stage: PgDhtTorrentBatchWriteStage::Torrents,
                row_index: 0,
                field: "file_extensions",
                previous_index: 0,
                element_index: 1,
                previous,
                value,
            }) if previous == "mkv" && value == "mkv"
        ));

        let mut valid_empty = torrent(3);
        valid_empty.files_data = Some(Vec::new());
        valid_empty.file_extensions = Some(Vec::new());
        let mut valid_sorted = torrent(4);
        valid_sorted.file_extensions =
            Some(vec!["1080p".to_owned(), "mkv".to_owned(), "srt".to_owned()]);
        let torrents = [valid_empty, valid_sorted];
        let prepared = prepare_torrents(&torrents).unwrap();
        assert!(prepared[0].file_extensions.is_empty());
        assert_eq!(prepared[1].file_extensions, ["1080p", "mkv", "srt"]);

        let mut empty_summary = summary(5);
        empty_summary.summary.extensions = Vec::new();
        let mut sorted_summary = summary(6);
        sorted_summary.summary.extensions = vec!["mp3".to_owned(), "srt".to_owned()];
        assert_eq!(
            prepare_summaries(&[empty_summary, sorted_summary])
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn prepared_values_preserve_input_order_and_every_checked_bind_projection() {
        let mut first = valid_plan();
        first.torrents.push(torrent(2));
        first.files.push(file(2));
        first.file_summaries.push(summary(2));
        first.sources.push(source(2));
        first.pieces.push(pieces(2));
        first.queue_jobs.push(queue_job(2));
        let prepared = prepare_plan(&first).unwrap();

        assert_eq!(
            prepared
                .torrents
                .iter()
                .map(|row| row.row.info_hash)
                .collect::<Vec<_>>(),
            [id(1), id(2)]
        );
        assert_eq!(prepared.torrents[0].meta_version, 1);
        assert_eq!(prepared.torrents[0].size, 2);
        assert_eq!(prepared.torrents[0].files_count, Some(1));
        assert_eq!(prepared.torrents[0].file_extensions, ["mkv"]);
        assert_eq!(prepared.files[0].row.info_hash, id(1));
        assert_eq!(prepared.files[0].index, 1);
        assert_eq!(prepared.files[0].size, 11);
        assert_eq!(
            prepared.file_summaries[0].row.summary.info_hash,
            model_id(1)
        );
        assert_eq!(prepared.file_summaries[0].file_count, 1);
        assert_eq!(prepared.file_summaries[0].compressed_bytes, Some(2));
        assert_eq!(prepared.sources[0].row.source, "dht");
        assert_eq!(prepared.sources[1].row.info_hash, id(2));
        assert_eq!(prepared.pieces[0].row.piece_length, 16_384);
        assert_eq!(prepared.pieces[1].row.info_hash, id(2));
        assert_eq!(prepared.queue_jobs[0].values.max_retries, 2);
        assert_eq!(prepared.queue_jobs[0].values.delay.microseconds, 60_000_000);
        assert_eq!(
            prepared.queue_jobs[0].values.archival_duration.microseconds,
            604_800_000_000
        );
        assert_eq!(prepared.queue_jobs[0].values.job.priority, 10);
        assert_eq!(prepared.queue_jobs[1].values.job, &first.queue_jobs[1]);
    }

    #[test]
    fn every_stage_chunks_at_the_frozen_limit_and_resets_placeholders() {
        let plan = DhtTorrentTransactionPlan {
            torrents: (0..101).map(torrent).collect(),
            files: (0..101).map(file).collect(),
            file_summaries: (0..101).map(summary).collect(),
            sources: (0..101).map(source).collect(),
            pieces: (0..11).map(pieces).collect(),
            queue_jobs: (0..11).map(queue_job).collect(),
        };
        let prepared = prepare_plan(&plan).unwrap();

        macro_rules! assert_chunks {
            ($rows:expr, $limit:expr, $binds:expr, $builder:ident) => {{
                assert_eq!(
                    $rows.chunks($limit).map(<[_]>::len).collect::<Vec<_>>(),
                    [$limit, 1]
                );
                for (chunk, expected_arguments) in
                    $rows.chunks($limit).zip([$limit * $binds, $binds])
                {
                    let (sql, arguments, persistent) = inspect_query($builder(chunk));
                    assert_eq!(arguments, expected_arguments);
                    assert!(sql.contains(&format!("${expected_arguments}")));
                    assert!(!sql.contains(&format!("${}", expected_arguments + 1)));
                    assert!(!persistent);
                }
            }};
        }
        assert_chunks!(
            prepared.torrents,
            PG_DHT_TORRENT_WRITE_CHUNK_LIMIT,
            11,
            build_torrents_query
        );
        assert_chunks!(
            prepared.files,
            PG_DHT_TORRENT_WRITE_CHUNK_LIMIT,
            4,
            build_files_query
        );
        assert_chunks!(
            prepared.file_summaries,
            PG_DHT_TORRENT_WRITE_CHUNK_LIMIT,
            9,
            build_summaries_query
        );
        assert_chunks!(
            prepared.sources,
            PG_DHT_TORRENT_WRITE_CHUNK_LIMIT,
            2,
            build_sources_query
        );
        assert_chunks!(
            prepared.pieces,
            PG_DHT_TORRENT_SMALL_WRITE_CHUNK_LIMIT,
            3,
            build_pieces_query
        );
        assert_chunks!(
            prepared.queue_jobs,
            PG_DHT_TORRENT_SMALL_WRITE_CHUNK_LIMIT,
            7,
            build_queue_query
        );
    }

    #[test]
    fn v1_pure_v2_and_both_hybrid_primary_identities_are_accepted() {
        let v1 = torrent(1);

        let mut pure_v2 = torrent(2);
        let mut pure_v2_hash = [0x22_u8; 32];
        pure_v2_hash[..20].copy_from_slice(id(2).as_bytes());
        pure_v2.info_hash_v1 = None;
        pure_v2.info_hash_v2 = Some(pure_v2_hash);
        pure_v2.meta_version = 2;

        let mut hybrid_v1_primary = torrent(3);
        hybrid_v1_primary.info_hash_v2 = Some([0x33; 32]);
        hybrid_v1_primary.meta_version = 2;

        let mut hybrid_v2_primary = torrent(4);
        let v1_identity = id(44);
        let mut hybrid_v2_hash = [0x44_u8; 32];
        hybrid_v2_hash[..20].copy_from_slice(id(4).as_bytes());
        hybrid_v2_primary.info_hash_v1 = Some(v1_identity);
        hybrid_v2_primary.info_hash_v2 = Some(hybrid_v2_hash);
        hybrid_v2_primary.meta_version = 2;

        let rows = [v1, pure_v2, hybrid_v1_primary, hybrid_v2_primary];
        let prepared = prepare_torrents(&rows).unwrap();
        assert_eq!(
            prepared
                .iter()
                .map(|row| row.meta_version)
                .collect::<Vec<_>>(),
            [1, 2, 2, 2]
        );
    }

    #[test]
    fn identity_version_duplicate_and_numeric_validation_are_exact() {
        let duplicate = [torrent(1), torrent(2), torrent(1)];
        assert!(matches!(
            prepare_torrents(&duplicate),
            Err(PgDhtTorrentBatchWriterError::DuplicateTorrentInfoHash {
                info_hash,
                first_index: 0,
                duplicate_index: 2,
            }) if info_hash == id(1)
        ));

        let duplicate_summary = [summary(1), summary(2), summary(1)];
        assert!(matches!(
            prepare_summaries(&duplicate_summary),
            Err(PgDhtTorrentBatchWriterError::DuplicateSummaryInfoHash {
                info_hash,
                first_index: 0,
                duplicate_index: 2,
            }) if info_hash == model_id(1)
        ));

        let mut invalid_version = torrent(3);
        invalid_version.meta_version = 0;
        assert!(matches!(
            prepare_torrents(&[invalid_version]),
            Err(PgDhtTorrentBatchWriterError::InvalidMetaVersion {
                torrent_index: 0,
                meta_version: 0,
                ..
            })
        ));

        let mut invalid_v1 = torrent(4);
        invalid_v1.info_hash_v1 = None;
        assert!(matches!(
            prepare_torrents(&[invalid_v1]),
            Err(PgDhtTorrentBatchWriterError::InvalidTorrentIdentity {
                torrent_index: 0,
                meta_version: 1,
                ..
            })
        ));

        let mut invalid_v2 = torrent(5);
        invalid_v2.meta_version = 2;
        invalid_v2.info_hash_v1 = None;
        invalid_v2.info_hash_v2 = None;
        assert!(matches!(
            prepare_torrents(&[invalid_v2]),
            Err(PgDhtTorrentBatchWriterError::InvalidTorrentIdentity {
                torrent_index: 0,
                meta_version: 2,
                ..
            })
        ));

        let mut mismatch = torrent(6);
        mismatch.meta_version = 2;
        mismatch.info_hash_v1 = Some(id(66));
        mismatch.info_hash_v2 = Some([0x66; 32]);
        assert!(matches!(
            prepare_torrents(&[mismatch]),
            Err(PgDhtTorrentBatchWriterError::InvalidTorrentIdentity {
                torrent_index: 0,
                meta_version: 2,
                ..
            })
        ));

        let too_large_i32 = i32::MAX as u32 + 1;
        let too_large_i64 = i64::MAX as u64 + 1;
        let mut cases = Vec::new();
        let mut torrent_size = valid_plan();
        torrent_size.torrents[0].size = too_large_i64;
        cases.push((torrent_size, PgDhtTorrentBatchWriteStage::Torrents, "size"));
        let mut torrent_count = valid_plan();
        torrent_count.torrents[0].files_count = Some(too_large_i32);
        cases.push((
            torrent_count,
            PgDhtTorrentBatchWriteStage::Torrents,
            "files_count",
        ));
        let mut file_index = valid_plan();
        file_index.files[0].index = too_large_i32;
        cases.push((file_index, PgDhtTorrentBatchWriteStage::Files, "index"));
        let mut file_size = valid_plan();
        file_size.files[0].size = too_large_i64;
        cases.push((file_size, PgDhtTorrentBatchWriteStage::Files, "size"));
        let mut summary_count = valid_plan();
        summary_count.file_summaries[0].summary.file_count = too_large_i32;
        cases.push((
            summary_count,
            PgDhtTorrentBatchWriteStage::FileSummaries,
            "file_count",
        ));
        let mut compressed = valid_plan();
        compressed.file_summaries[0].compressed_bytes = Some(too_large_i64);
        cases.push((
            compressed,
            PgDhtTorrentBatchWriteStage::FileSummaries,
            "compressed_bytes",
        ));
        for (plan, expected_stage, expected_field) in cases {
            assert!(matches!(
                prepare_plan(&plan),
                Err(PgDhtTorrentBatchWriterError::IntegerOutOfRange {
                    stage,
                    row_index: 0,
                    field,
                    ..
                }) if stage == expected_stage && field == expected_field
            ));
        }

        let mut maximum = valid_plan();
        maximum.torrents[0].size = i64::MAX as u64;
        maximum.torrents[0].files_count = Some(i32::MAX as u32);
        maximum.files[0].index = i32::MAX as u32;
        maximum.files[0].size = i64::MAX as u64;
        maximum.file_summaries[0].summary.file_count = i32::MAX as u32;
        maximum.file_summaries[0].compressed_bytes = Some(i64::MAX as u64);
        let prepared = prepare_plan(&maximum).unwrap();
        assert_eq!(prepared.torrents[0].size, i64::MAX);
        assert_eq!(prepared.torrents[0].files_count, Some(i32::MAX));
        assert_eq!(prepared.files[0].index, i32::MAX);
        assert_eq!(prepared.files[0].size, i64::MAX);
        assert_eq!(prepared.file_summaries[0].file_count, i32::MAX);
        assert_eq!(prepared.file_summaries[0].compressed_bytes, Some(i64::MAX));
    }

    #[test]
    fn negative_text_and_queue_validation_cover_every_owned_value_family() {
        let mut negative_total = valid_plan();
        negative_total.file_summaries[0].summary.total_size = -1;
        let mut negative_largest = valid_plan();
        negative_largest.file_summaries[0].summary.largest_file_size = -2;
        let mut negative_pieces = valid_plan();
        negative_pieces.pieces[0].piece_length = -3;
        for (plan, stage, field, value) in [
            (
                negative_total,
                PgDhtTorrentBatchWriteStage::FileSummaries,
                "total_size",
                -1,
            ),
            (
                negative_largest,
                PgDhtTorrentBatchWriteStage::FileSummaries,
                "largest_file_size",
                -2,
            ),
            (
                negative_pieces,
                PgDhtTorrentBatchWriteStage::Pieces,
                "piece_length",
                -3,
            ),
        ] {
            assert!(matches!(
                prepare_plan(&plan),
                Err(PgDhtTorrentBatchWriterError::NegativeInteger {
                    stage: actual_stage,
                    row_index: 0,
                    field: actual_field,
                    value: actual_value,
                }) if actual_stage == stage && actual_field == field && actual_value == value
            ));
        }

        let mut nul_cases = Vec::new();
        let mut name = valid_plan();
        name.torrents[0].name = "bad\0name".to_owned();
        nul_cases.push((name, PgDhtTorrentBatchWriteStage::Torrents, "name", None));
        let mut file_extension = valid_plan();
        file_extension.torrents[0].file_extensions = Some(vec!["mk\0v".to_owned()]);
        nul_cases.push((
            file_extension,
            PgDhtTorrentBatchWriteStage::Torrents,
            "file_extensions",
            Some(0),
        ));
        let mut path = valid_plan();
        path.files[0].path = "bad\0path".to_owned();
        nul_cases.push((path, PgDhtTorrentBatchWriteStage::Files, "path", None));
        let mut summary_extension = valid_plan();
        summary_extension.file_summaries[0].summary.extensions = vec!["m\0kv".to_owned()];
        nul_cases.push((
            summary_extension,
            PgDhtTorrentBatchWriteStage::FileSummaries,
            "extensions",
            Some(0),
        ));
        let mut source_nul = valid_plan();
        source_nul.sources[0].source = "d\0ht".to_owned();
        nul_cases.push((
            source_nul,
            PgDhtTorrentBatchWriteStage::Sources,
            "source",
            None,
        ));
        for (plan, stage, field, element_index) in nul_cases {
            assert!(matches!(
                prepare_plan(&plan),
                Err(PgDhtTorrentBatchWriterError::TextContainsNul {
                    stage: actual_stage,
                    row_index: 0,
                    field: actual_field,
                    element_index: actual_element,
                }) if actual_stage == stage && actual_field == field && actual_element == element_index
            ));
        }

        let mut invalid_queue = valid_plan();
        invalid_queue.queue_jobs[0].status = QueueJobStatus::Retry;
        assert!(matches!(
            prepare_plan(&invalid_queue),
            Err(PgDhtTorrentBatchWriterError::QueuePreparation {
                job_index: 0,
                source: QueuePgError::InvalidProducerJob("status must be pending"),
            })
        ));

        let mut decoded_nul = valid_plan();
        decoded_nul.queue_jobs[0].payload = r#"{"nested":"\u0000"}"#.to_owned();
        decoded_nul.queue_jobs[0].fingerprint = fingerprint(
            &decoded_nul.queue_jobs[0].queue,
            &decoded_nul.queue_jobs[0].payload,
        );
        assert!(matches!(
            prepare_plan(&decoded_nul),
            Err(PgDhtTorrentBatchWriterError::QueuePreparation {
                job_index: 0,
                source: QueuePgError::InvalidProducerJob(
                    "payload JSON contains a decoded NUL string"
                ),
            })
        ));

        let mut precise = valid_plan();
        precise.queue_jobs[0].delay = Duration::from_nanos(1);
        assert!(matches!(
            prepare_plan(&precise),
            Err(PgDhtTorrentBatchWriterError::QueuePreparation {
                job_index: 0,
                source: QueuePgError::InvalidProducerDurationPrecision {
                    field: "delay",
                    submicro_nanoseconds: 1,
                },
            })
        ));
    }

    #[test]
    fn validation_deliberately_leaves_provenance_and_database_constraints_authoritative() {
        let custom_queue = "custom_queue".to_owned();
        let payload = r#"{"arbitrary":true}"#.to_owned();
        let custom_job = QueueJob {
            fingerprint: fingerprint(&custom_queue, &payload),
            queue: custom_queue,
            status: QueueJobStatus::Pending,
            payload,
            max_retries: 7,
            priority: -42,
            archival_duration: DEFAULT_ARCHIVAL_DURATION,
            delay: Duration::from_secs(3),
        };
        let duplicate_file = DhtTorrentFileWrite {
            info_hash: id(999),
            index: 0,
            path: "foreign/file.bin".to_owned(),
            size: 1,
        };
        let plan = DhtTorrentTransactionPlan {
            torrents: Vec::new(),
            files: vec![duplicate_file.clone(), duplicate_file],
            file_summaries: Vec::new(),
            sources: vec![source(998), source(998)],
            pieces: vec![pieces(997), pieces(997)],
            queue_jobs: vec![custom_job.clone(), custom_job],
        };
        let prepared = prepare_plan(&plan).unwrap();
        assert_eq!(prepared.torrents.len(), 0);
        assert_eq!(prepared.files.len(), 2);
        assert_eq!(prepared.sources.len(), 2);
        assert_eq!(prepared.pieces.len(), 2);
        assert_eq!(prepared.queue_jobs.len(), 2);
        assert_eq!(prepared.queue_jobs[0].values.max_retries, 7);
        assert_eq!(prepared.queue_jobs[0].values.job.priority, -42);
        assert_eq!(prepared.queue_jobs[0].values.delay.microseconds, 3_000_000);
    }

    #[test]
    fn typed_stage_errors_preserve_context_sources_and_outcome_classification() {
        let rolled_back = PgDhtTorrentBatchWriterError::ExecuteRolledBack {
            stage: PgDhtTorrentBatchWriteStage::FileSummaries,
            chunk_index: 2,
            row_offset: 200,
            row_count: 17,
            source: Box::new(sqlx::Error::PoolClosed),
        };
        assert!(rolled_back
            .to_string()
            .contains("torrent_file_summary chunk 2 at offset 200 with 17"));
        assert!(matches!(
            rolled_back.source(),
            Some(source) if source.to_string() == "attempted to acquire a connection on a closed pool"
        ));
        let rejected = DhtTorrentBatchWriteError::rejected(rolled_back);
        assert!(matches!(
            rejected,
            DhtTorrentBatchWriteError::Rejected { .. }
        ));

        let rollback_failed = PgDhtTorrentBatchWriterError::RollbackFailed {
            stage: PgDhtTorrentBatchWriteStage::QueueJobs,
            chunk_index: 1,
            row_offset: 10,
            row_count: 10,
            execute_error: Box::new(sqlx::Error::PoolTimedOut),
            rollback_error: Box::new(sqlx::Error::PoolClosed),
        };
        assert!(matches!(
            &rollback_failed,
            PgDhtTorrentBatchWriterError::RollbackFailed {
                execute_error,
                rollback_error,
                ..
            } if matches!(execute_error.as_ref(), sqlx::Error::PoolTimedOut)
                && matches!(rollback_error.as_ref(), sqlx::Error::PoolClosed)
        ));
        assert!(matches!(
            rollback_failed.source(),
            Some(source) if source.to_string() == "attempted to acquire a connection on a closed pool"
        ));
        let unknown = DhtTorrentBatchWriteError::outcome_unknown(rollback_failed);
        assert!(matches!(
            unknown,
            DhtTorrentBatchWriteError::OutcomeUnknown { .. }
        ));

        let commit = PgDhtTorrentBatchWriterError::Commit {
            source: Box::new(sqlx::Error::PoolClosed),
        };
        assert!(matches!(
            DhtTorrentBatchWriteError::outcome_unknown(commit),
            DhtTorrentBatchWriteError::OutcomeUnknown { .. }
        ));
    }

    #[tokio::test]
    async fn validation_precedes_begin_and_empty_and_valid_plans_always_begin() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres@127.0.0.1:1/unused")
            .unwrap();
        let adapter = PgDhtTorrentBatchWriter::new(pool);
        let clone = adapter.clone();
        assert!(!adapter.pool().is_closed());
        assert!(!clone.pool().is_closed());
        let object: Arc<dyn DhtTorrentBatchWriter> = Arc::new(clone);
        let plan = valid_plan();
        assert_send_future(object.write_batch(&plan));

        adapter.pool().close().await;
        for plan in [DhtTorrentTransactionPlan::default(), valid_plan()] {
            let error = DhtTorrentBatchWriter::write_batch(&adapter, &plan)
                .await
                .unwrap_err();
            assert!(matches!(&error, DhtTorrentBatchWriteError::Rejected { .. }));
            assert!(matches!(
                typed_source(&error),
                PgDhtTorrentBatchWriterError::Begin { source }
                    if matches!(source.as_ref(), sqlx::Error::PoolClosed)
            ));
        }

        let mut invalid = valid_plan();
        invalid.torrents[0].size = i64::MAX as u64 + 1;
        let error = DhtTorrentBatchWriter::write_batch(&adapter, &invalid)
            .await
            .unwrap_err();
        assert!(matches!(
            typed_source(&error),
            PgDhtTorrentBatchWriterError::IntegerOutOfRange {
                stage: PgDhtTorrentBatchWriteStage::Torrents,
                row_index: 0,
                field: "size",
                ..
            }
        ));

        let mut invalid_last_stage = valid_plan();
        invalid_last_stage.queue_jobs[0].status = QueueJobStatus::Failed;
        let error = DhtTorrentBatchWriter::write_batch(&adapter, &invalid_last_stage)
            .await
            .unwrap_err();
        assert!(matches!(
            typed_source(&error),
            PgDhtTorrentBatchWriterError::QueuePreparation {
                job_index: 0,
                source: QueuePgError::InvalidProducerJob("status must be pending"),
            }
        ));
    }
}
