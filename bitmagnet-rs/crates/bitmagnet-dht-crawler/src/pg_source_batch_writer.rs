//! PostgreSQL adapter for atomic DHT source-batch persistence.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;

use async_trait::async_trait;
use bitmagnet_db::PgPool;
use bitmagnet_dht::Id20;
use sqlx::{Postgres, QueryBuilder};

use crate::{DhtSourceBatchWriteError, DhtSourceBatchWriter, DhtSourceWrite};

/// Maximum number of source rows emitted by one dynamic SQL statement.
pub const PG_DHT_SOURCE_WRITE_CHUNK_LIMIT: usize = 100;

const DHT_SOURCE: &str = "dht";
const UPSERT_PREFIX: &str = "\
INSERT INTO torrents_torrent_sources \
(source, info_hash, seeders, leechers, published_at, seen_count, created_at, updated_at) \
SELECT incoming.source, incoming.info_hash, incoming.seeders, incoming.leechers, \
NULL, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP \
FROM (";
const UPSERT_SUFFIX: &str = "\
) AS incoming(source, info_hash, seeders, leechers) \
WHERE EXISTS (SELECT 1 FROM torrents WHERE torrents.info_hash = incoming.info_hash) \
ON CONFLICT (info_hash, source) DO UPDATE SET \
seeders = EXCLUDED.seeders, \
leechers = EXCLUDED.leechers, \
published_at = EXCLUDED.published_at, \
updated_at = EXCLUDED.updated_at, \
seen_count = torrents_torrent_sources.seen_count + 1";

/// PostgreSQL operation or validation error underlying one typed writer result.
///
/// The variant is the failed stage. Chunk context uses a zero-based chunk index
/// and source offset. [`RollbackFailed`](Self::RollbackFailed) retains both the
/// original execute error and the rollback error because either can be needed
/// to diagnose an outcome whose final database state is unknowable.
#[derive(Debug)]
pub enum PgDhtSourceBatchWriterError {
    /// An info hash occurred more than once in the supposedly unique batch.
    DuplicateInfoHash {
        /// Duplicated hash.
        info_hash: Id20,
        /// Zero-based index of the first occurrence.
        first_index: usize,
        /// Zero-based index of the later occurrence.
        duplicate_index: usize,
    },
    /// A cardinality does not fit PostgreSQL's signed `integer` type.
    CountOutOfRange {
        /// Zero-based source index.
        source_index: usize,
        /// `seeders` or `leechers`.
        field: &'static str,
        /// Rejected unsigned value.
        value: u32,
    },
    /// Opening the transaction failed, so no write could start.
    Begin {
        /// SQLx error from `BEGIN`.
        source: Box<sqlx::Error>,
    },
    /// A chunk failed and the explicit rollback completed successfully.
    ExecuteRolledBack {
        /// Zero-based chunk index.
        chunk_index: usize,
        /// Zero-based input offset of the chunk.
        source_offset: usize,
        /// Number of sources in the chunk.
        source_count: usize,
        /// SQLx error from executing the chunk.
        source: Box<sqlx::Error>,
    },
    /// A chunk failed and the subsequent explicit rollback also failed.
    RollbackFailed {
        /// Zero-based chunk index.
        chunk_index: usize,
        /// Zero-based input offset of the chunk.
        source_offset: usize,
        /// Number of sources in the chunk.
        source_count: usize,
        /// Original SQLx chunk-execution error.
        execute_error: Box<sqlx::Error>,
        /// SQLx error from the rollback attempt.
        rollback_error: Box<sqlx::Error>,
    },
    /// `COMMIT` returned an error after all chunks executed.
    Commit {
        /// SQLx error from `COMMIT`.
        source: Box<sqlx::Error>,
    },
}

impl fmt::Display for PgDhtSourceBatchWriterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateInfoHash {
                info_hash,
                first_index,
                duplicate_index,
            } => write!(
                formatter,
                "duplicate DHT source info hash {info_hash} at indexes {first_index} and {duplicate_index}"
            ),
            Self::CountOutOfRange {
                source_index,
                field,
                value,
            } => write!(
                formatter,
                "DHT source {field} value {value} at index {source_index} exceeds PostgreSQL integer"
            ),
            Self::Begin { source } => {
                write!(formatter, "begin DHT source batch transaction: {source}")
            }
            Self::ExecuteRolledBack {
                chunk_index,
                source_offset,
                source_count,
                source,
            } => write!(
                formatter,
                "execute DHT source chunk {chunk_index} at offset {source_offset} with {source_count} sources, then rolled back: {source}"
            ),
            Self::RollbackFailed {
                chunk_index,
                source_offset,
                source_count,
                execute_error,
                rollback_error,
            } => write!(
                formatter,
                "execute DHT source chunk {chunk_index} at offset {source_offset} with {source_count} sources failed ({execute_error}); rollback also failed ({rollback_error})"
            ),
            Self::Commit { source } => {
                write!(formatter, "commit DHT source batch transaction: {source}")
            }
        }
    }
}

impl Error for PgDhtSourceBatchWriterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::DuplicateInfoHash { .. } | Self::CountOutOfRange { .. } => None,
            Self::Begin { source }
            | Self::ExecuteRolledBack { source, .. }
            | Self::Commit { source } => Some(source.as_ref()),
            Self::RollbackFailed { rollback_error, .. } => Some(rollback_error.as_ref()),
        }
    }
}

/// PostgreSQL-backed [`DhtSourceBatchWriter`].
///
/// The adapter owns only a cheap clone of an application-configured [`PgPool`].
/// It does not create, close, or retry the pool or its transactions. Every
/// nonempty call is prevalidated before `BEGIN`, then written in ordered chunks
/// inside one transaction. PostgreSQL's transaction-stable
/// `CURRENT_TIMESTAMP` supplies both timestamps in every chunk. Go instead
/// samples the application clock once with `time.Now()` and binds that instant
/// to every row. Using the database transaction clock is an intentional Rust
/// delta in clock authority and capture instant. Parent-missing hashes are
/// intentional logical no-ops, and affected-row counts are ignored.
/// Go binds each hash as 40 lowercase hexadecimal characters and calls
/// PostgreSQL `decode(..., 'hex')`; Rust binds the already validated 20 raw
/// bytes directly as `bytea`. This is an intentional construction delta with
/// the same intended stored bytes.
///
/// Dynamic statements use SQLx's runtime query builder and are deliberately
/// non-persistent because their placeholder count varies by chunk. Tests are
/// offline and do not claim live schema, constraint, query-plan, rollback, or
/// cancellation evidence. They prove the timestamp SQL shape, not equality to
/// Go's live application-clock instant, and do not prove live PostgreSQL codec
/// equality between Go's decode path and Rust's direct bytes. Dropping an
/// in-flight future retains the worker contract's unknown-outcome semantics; it
/// does not prove PostgreSQL stopped.
#[derive(Clone, Debug)]
#[must_use = "the adapter must be installed as a DHT source batch writer"]
pub struct PgDhtSourceBatchWriter {
    pool: PgPool,
}

impl PgDhtSourceBatchWriter {
    /// Wrap an already configured, application-owned pool.
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the shared pool used by this adapter.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn write_batch_db(
        &self,
        sources: &[DhtSourceWrite],
    ) -> Result<(), DhtSourceBatchWriteError> {
        if sources.is_empty() {
            return Ok(());
        }

        let prepared = prepare_sources(sources).map_err(DhtSourceBatchWriteError::rejected)?;
        let mut transaction = self.pool.begin().await.map_err(|source| {
            DhtSourceBatchWriteError::rejected(PgDhtSourceBatchWriterError::Begin {
                source: Box::new(source),
            })
        })?;

        for (chunk_index, chunk) in prepared.chunks(PG_DHT_SOURCE_WRITE_CHUNK_LIMIT).enumerate() {
            let source_offset = chunk_index * PG_DHT_SOURCE_WRITE_CHUNK_LIMIT;
            let mut query = build_upsert_query(chunk);
            let result = query
                .build()
                .persistent(false)
                .execute(&mut *transaction)
                .await;
            if let Err(execute_error) = result {
                return match transaction.rollback().await {
                    Ok(()) => Err(DhtSourceBatchWriteError::rejected(
                        PgDhtSourceBatchWriterError::ExecuteRolledBack {
                            chunk_index,
                            source_offset,
                            source_count: chunk.len(),
                            source: Box::new(execute_error),
                        },
                    )),
                    Err(rollback_error) => Err(DhtSourceBatchWriteError::outcome_unknown(
                        PgDhtSourceBatchWriterError::RollbackFailed {
                            chunk_index,
                            source_offset,
                            source_count: chunk.len(),
                            execute_error: Box::new(execute_error),
                            rollback_error: Box::new(rollback_error),
                        },
                    )),
                };
            }
        }

        transaction.commit().await.map_err(|source| {
            DhtSourceBatchWriteError::outcome_unknown(PgDhtSourceBatchWriterError::Commit {
                source: Box::new(source),
            })
        })
    }
}

#[async_trait]
impl DhtSourceBatchWriter for PgDhtSourceBatchWriter {
    async fn write_batch(
        &self,
        sources: &[DhtSourceWrite],
    ) -> Result<(), DhtSourceBatchWriteError> {
        self.write_batch_db(sources).await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PreparedSource {
    info_hash: Id20,
    seeders: i32,
    leechers: i32,
}

fn prepare_sources(
    sources: &[DhtSourceWrite],
) -> Result<Vec<PreparedSource>, PgDhtSourceBatchWriterError> {
    let mut first_indexes = HashMap::with_capacity(sources.len());
    let mut prepared = Vec::with_capacity(sources.len());
    for (source_index, source) in sources.iter().enumerate() {
        if let Some(first_index) = first_indexes.insert(source.info_hash, source_index) {
            return Err(PgDhtSourceBatchWriterError::DuplicateInfoHash {
                info_hash: source.info_hash,
                first_index,
                duplicate_index: source_index,
            });
        }
        let seeders = i32::try_from(source.seeders).map_err(|_| {
            PgDhtSourceBatchWriterError::CountOutOfRange {
                source_index,
                field: "seeders",
                value: source.seeders,
            }
        })?;
        let leechers = i32::try_from(source.leechers).map_err(|_| {
            PgDhtSourceBatchWriterError::CountOutOfRange {
                source_index,
                field: "leechers",
                value: source.leechers,
            }
        })?;
        prepared.push(PreparedSource {
            info_hash: source.info_hash,
            seeders,
            leechers,
        });
    }
    Ok(prepared)
}

fn build_upsert_query(sources: &[PreparedSource]) -> QueryBuilder<Postgres> {
    debug_assert!(!sources.is_empty());
    debug_assert!(sources.len() <= PG_DHT_SOURCE_WRITE_CHUNK_LIMIT);
    let mut query = QueryBuilder::<Postgres>::new(UPSERT_PREFIX);
    query.push_values(sources, |mut row, source| {
        row.push_bind(DHT_SOURCE)
            .push_bind(source.info_hash.as_bytes().as_slice())
            .push_bind(source.seeders)
            .push_bind(source.leechers);
    });
    query.push(UPSERT_SUFFIX);
    query
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Arguments, Execute, Type, TypeInfo};

    use super::*;

    fn id(value: usize) -> Id20 {
        let mut bytes = [0_u8; 20];
        bytes[12..].copy_from_slice(&(value as u64).to_be_bytes());
        Id20::from_slice(&bytes).unwrap()
    }

    fn source(value: usize) -> DhtSourceWrite {
        DhtSourceWrite {
            info_hash: id(value),
            seeders: u32::try_from(value).unwrap(),
            leechers: u32::try_from(value * 2).unwrap(),
        }
    }

    fn assert_send_sync<T: Send + Sync>() {}

    fn assert_send_future<F: Future + Send>(future: F) {
        drop(future);
    }

    fn typed_source(error: &DhtSourceBatchWriteError) -> &PgDhtSourceBatchWriterError {
        let source = match error {
            DhtSourceBatchWriteError::Rejected { source }
            | DhtSourceBatchWriteError::OutcomeUnknown { source } => source,
        };
        source
            .downcast_ref::<PgDhtSourceBatchWriterError>()
            .expect("adapter preserves its typed source error")
    }

    fn inspect_query(sources: &[PreparedSource]) -> (String, usize, bool) {
        let mut builder = build_upsert_query(sources);
        let sql = builder.sql().as_str().to_owned();
        let mut query = builder.build().persistent(false);
        let persistent = Execute::persistent(&query);
        let arguments = query
            .take_arguments()
            .unwrap()
            .expect("QueryBuilder supplies arguments");
        (sql, arguments.len(), persistent)
    }

    #[test]
    fn prepared_order_sql_shape_argument_count_and_dynamic_statement_policy_are_exact() {
        let prepared = prepare_sources(&[source(1), source(2)]).unwrap();
        assert_eq!(
            prepared,
            [
                PreparedSource {
                    info_hash: id(1),
                    seeders: 1,
                    leechers: 2,
                },
                PreparedSource {
                    info_hash: id(2),
                    seeders: 2,
                    leechers: 4,
                },
            ]
        );
        let (sql, arguments, persistent) = inspect_query(&prepared);
        assert_eq!(arguments, 8);
        assert!(!persistent);
        assert_eq!(<&str as Type<Postgres>>::type_info().name(), "TEXT");
        assert_eq!(<&[u8] as Type<Postgres>>::type_info().name(), "BYTEA");
        assert_eq!(<i32 as Type<Postgres>>::type_info().name(), "INT4");
        assert_eq!(
            sql,
            "INSERT INTO torrents_torrent_sources (source, info_hash, seeders, leechers, published_at, seen_count, created_at, updated_at) SELECT incoming.source, incoming.info_hash, incoming.seeders, incoming.leechers, NULL, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM (VALUES ($1, $2, $3, $4), ($5, $6, $7, $8)) AS incoming(source, info_hash, seeders, leechers) WHERE EXISTS (SELECT 1 FROM torrents WHERE torrents.info_hash = incoming.info_hash) ON CONFLICT (info_hash, source) DO UPDATE SET seeders = EXCLUDED.seeders, leechers = EXCLUDED.leechers, published_at = EXCLUDED.published_at, updated_at = EXCLUDED.updated_at, seen_count = torrents_torrent_sources.seen_count + 1"
        );
        assert_eq!(DHT_SOURCE, "dht");
        assert!(!sql.contains("RETURNING"));
        let updates = sql.split_once("DO UPDATE SET").unwrap().1;
        assert!(!updates.contains("created_at ="));
        assert!(!updates.contains("import_id"));
    }

    #[test]
    fn chunk_boundaries_never_exceed_one_hundred_and_reset_bind_numbers() {
        assert_eq!(PG_DHT_SOURCE_WRITE_CHUNK_LIMIT, 100);
        let prepared = prepare_sources(&(0..201).map(source).collect::<Vec<_>>()).unwrap();
        assert_eq!(
            prepared
                .chunks(PG_DHT_SOURCE_WRITE_CHUNK_LIMIT)
                .map(<[PreparedSource]>::len)
                .collect::<Vec<_>>(),
            [100, 100, 1]
        );
        for (chunk, expected_arguments) in prepared
            .chunks(PG_DHT_SOURCE_WRITE_CHUNK_LIMIT)
            .zip([400, 400, 4])
        {
            let (sql, arguments, persistent) = inspect_query(chunk);
            assert_eq!(arguments, expected_arguments);
            assert!(sql.contains(&format!("${expected_arguments}")));
            assert!(!sql.contains(&format!("${}", expected_arguments + 1)));
            assert!(!persistent);
        }
        assert_eq!(prepared[99].info_hash, id(99));
        assert_eq!(prepared[100].info_hash, id(100));
        assert_eq!(prepared[200].info_hash, id(200));
    }

    #[test]
    fn duplicate_and_both_postgres_integer_bounds_are_rejected_with_context() {
        let duplicate = prepare_sources(&[source(7), source(8), source(7)]).unwrap_err();
        assert!(matches!(
            duplicate,
            PgDhtSourceBatchWriterError::DuplicateInfoHash {
                info_hash,
                first_index: 0,
                duplicate_index: 2,
            } if info_hash == id(7)
        ));

        let too_large = i32::MAX as u32 + 1;
        for (field, input) in [
            (
                "seeders",
                DhtSourceWrite {
                    seeders: too_large,
                    ..source(1)
                },
            ),
            (
                "leechers",
                DhtSourceWrite {
                    leechers: too_large,
                    ..source(2)
                },
            ),
        ] {
            assert!(matches!(
                prepare_sources(&[source(0), input]),
                Err(PgDhtSourceBatchWriterError::CountOutOfRange {
                    source_index: 1,
                    field: actual_field,
                    value,
                }) if actual_field == field && value == too_large
            ));
        }
        let maximum = DhtSourceWrite {
            seeders: i32::MAX as u32,
            leechers: i32::MAX as u32,
            ..source(3)
        };
        assert_eq!(
            prepare_sources(&[maximum]).unwrap()[0],
            PreparedSource {
                info_hash: id(3),
                seeders: i32::MAX,
                leechers: i32::MAX,
            }
        );
    }

    #[tokio::test]
    async fn empty_short_circuits_and_validation_precedes_begin_on_closed_pool() {
        assert_send_sync::<PgDhtSourceBatchWriter>();
        assert_send_sync::<PgDhtSourceBatchWriterError>();
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres@127.0.0.1:1/unused")
            .unwrap();
        let adapter = PgDhtSourceBatchWriter::new(pool);
        let clone = adapter.clone();
        assert!(!adapter.pool().is_closed());
        assert!(!clone.pool().is_closed());
        let object: Arc<dyn DhtSourceBatchWriter> = Arc::new(clone);
        let one = [source(1)];
        assert_send_future(object.write_batch(&one));

        adapter.pool().close().await;
        assert!(adapter.pool().is_closed());
        DhtSourceBatchWriter::write_batch(&adapter, &[])
            .await
            .unwrap();

        let duplicate =
            DhtSourceBatchWriter::write_batch(&adapter, &[source(4), source(5), source(4)])
                .await
                .unwrap_err();
        assert!(matches!(
            &duplicate,
            DhtSourceBatchWriteError::Rejected { .. }
        ));
        assert!(matches!(
            typed_source(&duplicate),
            PgDhtSourceBatchWriterError::DuplicateInfoHash { .. }
        ));

        for invalid in [
            DhtSourceWrite {
                seeders: i32::MAX as u32 + 1,
                ..source(6)
            },
            DhtSourceWrite {
                leechers: i32::MAX as u32 + 1,
                ..source(7)
            },
        ] {
            let count = DhtSourceBatchWriter::write_batch(&adapter, &[invalid])
                .await
                .unwrap_err();
            assert!(matches!(&count, DhtSourceBatchWriteError::Rejected { .. }));
            assert!(matches!(
                typed_source(&count),
                PgDhtSourceBatchWriterError::CountOutOfRange { .. }
            ));
        }

        let begin = object.write_batch(&one).await.unwrap_err();
        assert!(matches!(&begin, DhtSourceBatchWriteError::Rejected { .. }));
        assert!(matches!(
            typed_source(&begin),
            PgDhtSourceBatchWriterError::Begin { source }
                if matches!(source.as_ref(), sqlx::Error::PoolClosed)
        ));
    }

    #[test]
    fn stage_errors_preserve_execute_and_rollback_context_and_sources() {
        let rolled_back = PgDhtSourceBatchWriterError::ExecuteRolledBack {
            chunk_index: 2,
            source_offset: 200,
            source_count: 17,
            source: Box::new(sqlx::Error::PoolClosed),
        };
        assert!(rolled_back
            .to_string()
            .contains("chunk 2 at offset 200 with 17"));
        assert!(
            matches!(rolled_back.source(), Some(source) if source.to_string() == "attempted to acquire a connection on a closed pool")
        );

        let rollback_failed = PgDhtSourceBatchWriterError::RollbackFailed {
            chunk_index: 1,
            source_offset: 100,
            source_count: 100,
            execute_error: Box::new(sqlx::Error::PoolTimedOut),
            rollback_error: Box::new(sqlx::Error::PoolClosed),
        };
        assert!(matches!(
            &rollback_failed,
            PgDhtSourceBatchWriterError::RollbackFailed {
                execute_error,
                rollback_error,
                ..
            } if matches!(execute_error.as_ref(), sqlx::Error::PoolTimedOut)
                && matches!(rollback_error.as_ref(), sqlx::Error::PoolClosed)
        ));
        assert!(
            matches!(rollback_failed.source(), Some(source) if source.to_string() == "attempted to acquire a connection on a closed pool")
        );

        let commit = PgDhtSourceBatchWriterError::Commit {
            source: Box::new(sqlx::Error::PoolClosed),
        };
        let classified = DhtSourceBatchWriteError::outcome_unknown(commit);
        assert!(matches!(
            classified,
            DhtSourceBatchWriteError::OutcomeUnknown { .. }
        ));
    }
}
