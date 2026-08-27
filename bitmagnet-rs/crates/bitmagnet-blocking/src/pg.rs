use async_trait::async_trait;
use bitmagnet_bloom::{
    DecrementStartSource, StableBloomCodecError, StableBloomFilter, StableBloomGeometry,
};
use bitmagnet_model::InfoHash;
use sqlx::postgres::types::Oid;
use sqlx::{PgPool, Row};
use thiserror::Error;

use super::{AtomicBlockingStore, BlockingClock, BoxError, CommittedFilter};

// SQLx 0.9 has no PostgreSQL large-object descriptor wrapper. Runtime queries
// therefore use lo_get/lo_create/lo_put inside the same transaction. This is a
// data-equivalence claim for fresh objects and strictly decoded exact-length
// objects, not descriptor-call parity. The adapter deliberately adds neither
// row/advisory locking nor a truncation behavior absent from Go; an oversized
// existing object is rejected by the bounded strict decode before any write.

const BLOCKED_TORRENTS_KEY: &str = "blocked_torrents";
const CELL_COUNT: usize = 100_000_000;
const BITS_PER_CELL: u8 = 2;
const HASH_FUNCTIONS: usize = 5;
const DECREMENT_CELLS: usize = 49;
const ENCODED_BYTES: usize = 25_000_091;
const BOUNDED_READ_BYTES: usize = ENCODED_BYTES + 1;

const BEGIN_TRANSACTION_SQL: &str = "BEGIN READ WRITE";
const DELETE_TORRENTS_SQL: &str = "DELETE FROM public.torrents WHERE info_hash = ANY($1::bytea[])";
const SELECT_FILTER_OID_SQL: &str = "SELECT oid FROM public.bloom_filters WHERE key = $1::text";
const READ_LARGE_OBJECT_SQL: &str = "SELECT pg_catalog.lo_get($1::oid, 0::bigint, $2::integer)";
const CREATE_LARGE_OBJECT_SQL: &str = "SELECT pg_catalog.lo_create(0::oid)";
const WRITE_LARGE_OBJECT_SQL: &str = "SELECT pg_catalog.lo_put($1::oid, 0::bigint, $2::bytea)";
const INSERT_FILTER_SQL: &str = "INSERT INTO public.bloom_filters \
    (key, oid, created_at, updated_at) \
    VALUES ($1::text, $2::oid, $3::timestamptz, $3::timestamptz)";
const UPDATE_NULL_FILTER_SQL: &str = "UPDATE public.bloom_filters \
    SET oid = $1::oid, updated_at = $2::timestamptz \
    WHERE key = $3::text";

#[derive(Debug, Error)]
enum PostgresBlockingStoreError {
    #[error("failed to begin blocking transaction")]
    Begin(#[source] sqlx::Error),
    #[error("failed to delete blocked torrents")]
    DeleteTorrents(#[source] sqlx::Error),
    #[error("failed to get bloom-filter object ID")]
    SelectObjectId(#[source] sqlx::Error),
    #[error("failed to read current bloom-filter large object")]
    ReadLargeObject(#[source] sqlx::Error),
    #[error("current bloom-filter large object is invalid")]
    DecodeFilter(#[source] StableBloomCodecError),
    #[error("failed to create bloom-filter large object")]
    CreateLargeObject(#[source] sqlx::Error),
    #[error("failed to encode bloom filter")]
    EncodeFilter(#[source] StableBloomCodecError),
    #[error("failed to write bloom-filter large object")]
    WriteLargeObject(#[source] sqlx::Error),
    #[error("failed to save new bloom-filter record")]
    InsertMetadata(#[source] sqlx::Error),
    #[error("failed to repair bloom-filter record with a NULL object ID")]
    UpdateNullMetadata(#[source] sqlx::Error),
    #[error("failed to commit blocking transaction")]
    Commit(#[source] sqlx::Error),
    #[error("bloom-filter encoded length does not fit a PostgreSQL integer")]
    ReadLengthOverflow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetadataState {
    Missing,
    NullObjectId,
    ExistingObjectId(Oid),
}

pub(super) struct PostgresAtomicBlockingStore {
    pool: PgPool,
    geometry: StableBloomGeometry,
    clock: std::sync::Arc<dyn BlockingClock>,
}

impl PostgresAtomicBlockingStore {
    pub(super) fn new(pool: PgPool, clock: std::sync::Arc<dyn BlockingClock>) -> Self {
        Self {
            pool,
            geometry: production_geometry(),
            clock,
        }
    }
}

#[async_trait]
impl AtomicBlockingStore for PostgresAtomicBlockingStore {
    async fn commit(
        &mut self,
        blocked: &[InfoHash],
        decrement_starts: &mut (dyn DecrementStartSource + Send),
    ) -> Result<CommittedFilter, BoxError> {
        let mut transaction = self
            .pool
            .begin_with(BEGIN_TRANSACTION_SQL)
            .await
            .map_err(PostgresBlockingStoreError::Begin)?;

        if !blocked.is_empty() {
            let raw_hashes = blocked
                .iter()
                .map(|hash| hash.as_slice().to_vec())
                .collect::<Vec<_>>();
            sqlx::query(DELETE_TORRENTS_SQL)
                .bind(raw_hashes)
                .execute(&mut *transaction)
                .await
                .map_err(PostgresBlockingStoreError::DeleteTorrents)?;
        }

        let oid_row = sqlx::query(SELECT_FILTER_OID_SQL)
            .bind(BLOCKED_TORRENTS_KEY)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(PostgresBlockingStoreError::SelectObjectId)?;
        let oid_row = oid_row
            .map(|row| row.try_get::<Option<Oid>, _>("oid"))
            .transpose()
            .map_err(PostgresBlockingStoreError::SelectObjectId)?;
        let metadata_state = classify_metadata_state(oid_row);

        let mut filter = match metadata_state {
            MetadataState::ExistingObjectId(oid) => {
                let bounded_read = i32::try_from(BOUNDED_READ_BYTES)
                    .map_err(|_| PostgresBlockingStoreError::ReadLengthOverflow)?;
                let bytes = sqlx::query_scalar::<_, Vec<u8>>(READ_LARGE_OBJECT_SQL)
                    .bind(oid)
                    .bind(bounded_read)
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(PostgresBlockingStoreError::ReadLargeObject)?;
                decode_filter(&bytes, self.geometry)?
            }
            MetadataState::Missing | MetadataState::NullObjectId => {
                StableBloomFilter::new(self.geometry)
            }
        };

        let oid = match metadata_state {
            MetadataState::ExistingObjectId(oid) => oid,
            MetadataState::Missing | MetadataState::NullObjectId => {
                sqlx::query_scalar::<_, Oid>(CREATE_LARGE_OBJECT_SQL)
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(PostgresBlockingStoreError::CreateLargeObject)?
            }
        };

        for hash in blocked {
            filter.add(hash.as_slice(), decrement_starts);
        }

        let encoded = encode_filter(&filter)?;
        sqlx::query(WRITE_LARGE_OBJECT_SQL)
            .bind(oid)
            .bind(encoded)
            .execute(&mut *transaction)
            .await
            .map_err(PostgresBlockingStoreError::WriteLargeObject)?;

        // Go samples once after the BF write and before metadata work and the
        // transaction commit. Instant drives manager policy; UTC is persisted.
        let clock_sample = self.clock.now();
        match metadata_state {
            MetadataState::Missing => {
                sqlx::query(INSERT_FILTER_SQL)
                    .bind(BLOCKED_TORRENTS_KEY)
                    .bind(oid)
                    .bind(clock_sample.wall)
                    .execute(&mut *transaction)
                    .await
                    .map_err(PostgresBlockingStoreError::InsertMetadata)?;
            }
            MetadataState::NullObjectId => {
                sqlx::query(UPDATE_NULL_FILTER_SQL)
                    .bind(oid)
                    .bind(clock_sample.wall)
                    .bind(BLOCKED_TORRENTS_KEY)
                    .execute(&mut *transaction)
                    .await
                    .map_err(PostgresBlockingStoreError::UpdateNullMetadata)?;
            }
            MetadataState::ExistingObjectId(_) => {}
        }

        transaction
            .commit()
            .await
            .map_err(PostgresBlockingStoreError::Commit)?;
        Ok(CommittedFilter {
            filter,
            flushed_at: clock_sample.monotonic,
        })
    }
}

fn production_geometry() -> StableBloomGeometry {
    let geometry =
        StableBloomGeometry::new(CELL_COUNT, BITS_PER_CELL, HASH_FUNCTIONS, DECREMENT_CELLS)
            .expect("production blocking geometry is statically valid");
    debug_assert_eq!(geometry.encoded_bytes(), ENCODED_BYTES);
    geometry
}

pub(super) fn validate_go_blocked_torrents_filter(
    bytes: &[u8],
) -> Result<(), StableBloomCodecError> {
    StableBloomFilter::from_bytes(bytes, production_geometry()).map(|_| ())
}

fn classify_metadata_state(oid_row: Option<Option<Oid>>) -> MetadataState {
    match oid_row {
        None => MetadataState::Missing,
        Some(None) => MetadataState::NullObjectId,
        Some(Some(oid)) => MetadataState::ExistingObjectId(oid),
    }
}

fn decode_filter(
    bytes: &[u8],
    geometry: StableBloomGeometry,
) -> Result<StableBloomFilter, PostgresBlockingStoreError> {
    StableBloomFilter::from_bytes(bytes, geometry).map_err(PostgresBlockingStoreError::DecodeFilter)
}

fn encode_filter(filter: &StableBloomFilter) -> Result<Vec<u8>, PostgresBlockingStoreError> {
    let mut encoded = Vec::with_capacity(filter.geometry().encoded_bytes());
    filter
        .write_to(&mut encoded)
        .map_err(PostgresBlockingStoreError::EncodeFilter)?;
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::num::NonZeroUsize;

    use bitmagnet_bloom::StableBloomGeometry;
    use sqlx::postgres::PgPoolOptions;

    use super::*;
    use crate::{BlockingManager, DEFAULT_MAX_BUFFER_SIZE, DEFAULT_MAX_FLUSH_WAIT};

    #[test]
    fn production_geometry_and_bounded_read_are_exact() {
        let geometry = production_geometry();
        assert_eq!(geometry.cell_count(), CELL_COUNT);
        assert_eq!(geometry.bits_per_cell(), BITS_PER_CELL);
        assert_eq!(geometry.hash_functions(), HASH_FUNCTIONS);
        assert_eq!(geometry.decrement_cells(), DECREMENT_CELLS);
        assert_eq!(geometry.encoded_bytes(), ENCODED_BYTES);
        assert_eq!(BOUNDED_READ_BYTES, 25_000_092);
        assert_eq!(i32::try_from(BOUNDED_READ_BYTES).unwrap(), 25_000_092);
    }

    #[test]
    fn runtime_queries_pin_tables_functions_and_parameter_casts() {
        assert_eq!(BEGIN_TRANSACTION_SQL, "BEGIN READ WRITE");
        assert_eq!(
            DELETE_TORRENTS_SQL,
            "DELETE FROM public.torrents WHERE info_hash = ANY($1::bytea[])"
        );
        assert_eq!(
            SELECT_FILTER_OID_SQL,
            "SELECT oid FROM public.bloom_filters WHERE key = $1::text"
        );
        assert_eq!(
            READ_LARGE_OBJECT_SQL,
            "SELECT pg_catalog.lo_get($1::oid, 0::bigint, $2::integer)"
        );
        assert_eq!(
            CREATE_LARGE_OBJECT_SQL,
            "SELECT pg_catalog.lo_create(0::oid)"
        );
        assert_eq!(
            WRITE_LARGE_OBJECT_SQL,
            "SELECT pg_catalog.lo_put($1::oid, 0::bigint, $2::bytea)"
        );
        assert!(INSERT_FILTER_SQL.contains("$3::timestamptz, $3::timestamptz"));
        assert!(UPDATE_NULL_FILTER_SQL.contains("SET oid = $1::oid"));
        assert!(UPDATE_NULL_FILTER_SQL.contains("WHERE key = $3::text"));
    }

    #[test]
    fn metadata_rows_distinguish_missing_null_and_existing_object_ids() {
        assert_eq!(classify_metadata_state(None), MetadataState::Missing);
        assert_eq!(
            classify_metadata_state(Some(None)),
            MetadataState::NullObjectId
        );
        assert_eq!(
            classify_metadata_state(Some(Some(Oid(41)))),
            MetadataState::ExistingObjectId(Oid(41))
        );
        assert_eq!(
            classify_metadata_state(Some(Some(Oid(0)))),
            MetadataState::ExistingObjectId(Oid(0)),
            "OID zero is non-NULL and must take the existing-object read branch"
        );
    }

    #[test]
    fn strict_codec_helper_rejects_truncated_and_trailing_objects() {
        let geometry = StableBloomGeometry::new(128, 2, 3, 4).unwrap();
        let filter = StableBloomFilter::new(geometry);
        let encoded = encode_filter(&filter).unwrap();
        assert!(decode_filter(&encoded, geometry).is_ok());
        assert!(matches!(
            decode_filter(&encoded[..encoded.len() - 1], geometry),
            Err(PostgresBlockingStoreError::DecodeFilter(_))
        ));
        let mut trailing = encoded;
        trailing.push(0);
        assert!(matches!(
            decode_filter(&trailing, geometry),
            Err(PostgresBlockingStoreError::DecodeFilter(_))
        ));
    }

    #[test]
    fn error_surface_retains_stage_and_source() {
        let error = PostgresBlockingStoreError::ReadLengthOverflow;
        assert_eq!(
            error.to_string(),
            "bloom-filter encoded length does not fit a PostgreSQL integer"
        );
        assert!(error.source().is_none());
    }

    #[tokio::test]
    async fn public_constructor_is_lazy_and_owns_default_uninitialized_state() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgresql://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let manager = BlockingManager::new(pool);
        assert_eq!(manager.config.max_buffer_size, DEFAULT_MAX_BUFFER_SIZE);
        assert_eq!(manager.config.max_flush_wait, DEFAULT_MAX_FLUSH_WAIT);

        let mut inner = manager.inner.lock().await;
        assert!(inner.buffer.is_empty());
        assert!(inner.filter.is_none());
        assert!(inner.last_flushed_at.is_none());
        let start = inner
            .decrement_starts
            .next_start(NonZeroUsize::new(CELL_COUNT).unwrap());
        assert!(start < CELL_COUNT);
    }

    #[test]
    fn source_order_pins_delete_load_mutate_write_timestamp_metadata_commit() {
        let source = include_str!("pg.rs");
        let implementation = source
            .split_once("impl AtomicBlockingStore for PostgresAtomicBlockingStore")
            .unwrap()
            .1
            .split_once("fn production_geometry")
            .unwrap()
            .0;
        let positions = [
            "BEGIN_TRANSACTION_SQL",
            "DELETE_TORRENTS_SQL",
            "SELECT_FILTER_OID_SQL",
            "READ_LARGE_OBJECT_SQL",
            "for hash in blocked",
            "WRITE_LARGE_OBJECT_SQL",
            "let clock_sample = self.clock.now()",
            "sqlx::query(INSERT_FILTER_SQL)",
            ".commit()",
        ]
        .map(|needle| implementation.find(needle).unwrap());
        assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(implementation.contains("MetadataState::ExistingObjectId(_) => {}"));
        assert_eq!(implementation.matches("self.clock.now()").count(), 1);

        let constructor_source = include_str!("lib.rs");
        assert!(
            constructor_source.contains("PostgresAtomicBlockingStore::new(pool, clock.clone())")
        );
        assert!(constructor_source
            .contains("decrement_starts: Box::new(RandomDecrementStartSource::new())"));
    }

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn manager_and_store_error_are_send_sync() {
        assert_send_sync::<BlockingManager>();
        assert_send_sync::<PostgresBlockingStoreError>();
    }
}
