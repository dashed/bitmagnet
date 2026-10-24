//! PostgreSQL adapter for existing full-v2 torrent identity lookup.

use async_trait::async_trait;
use bitmagnet_db::{DbError, PgPool, Result as DbResult};
use bitmagnet_dht::Id20;

use crate::{DhtExistingV2Row, DhtTorrentV2Lookup, PersistTorrentCollaboratorError};

/// Existing primary/full-v2 rows for one worker-owned lookup chunk.
///
/// Result order is deliberately unspecified. The query has no `DISTINCT`,
/// grouping, ordering, or limit because `torrents.info_hash_v2` is non-unique;
/// every stored row must reach the worker's deterministic duplicate
/// canonicalization boundary.
const LOOKUP_SQL: &str = "SELECT torrents.info_hash, torrents.info_hash_v2 FROM torrents WHERE torrents.info_hash_v2 = ANY($1::bytea[])";

#[derive(Debug, sqlx::FromRow)]
struct RawExistingV2Row {
    info_hash: Vec<u8>,
    info_hash_v2: Vec<u8>,
}

/// PostgreSQL-backed [`DhtTorrentV2Lookup`].
///
/// The adapter owns a cheap clone of an application-owned [`PgPool`]. It does
/// not create or close the pool, start tasks, open a transaction, chunk input,
/// retry, canonicalize duplicate rows, or impose a statement timeout. The
/// worker supplies sorted, unique, nonempty chunks no longer than its configured
/// lookup limit; an empty direct call still short-circuits without pool access.
///
/// Full-v2 hashes are bound directly as 32 raw bytes in one PostgreSQL
/// `bytea[]`, rather than as hexadecimal text. Returned primary and full-v2
/// values must decode to exactly 20 and 32 bytes respectively. One malformed
/// row fails the whole call with [`DbError::Decode`], allowing the worker to
/// apply its chunk-level fail-open policy.
///
/// Dropping an in-flight lookup future stops awaiting SQLx. It does not claim
/// that PostgreSQL synchronously cancelled server-side work. Offline tests
/// freeze SQL, bindings, decoding, and error identity; they do not claim live
/// PostgreSQL array-codec, schema/index-plan, or cancellation evidence.
#[derive(Clone, Debug)]
#[must_use = "the adapter must be installed as a DHT torrent v2 lookup collaborator"]
pub struct PgDhtTorrentV2Lookup {
    pool: PgPool,
}

impl PgDhtTorrentV2Lookup {
    /// Wrap an already configured, application-owned pool.
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the shared pool used by this adapter.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn lookup_db(&self, info_hashes_v2: &[[u8; 32]]) -> DbResult<Vec<DhtExistingV2Row>> {
        if info_hashes_v2.is_empty() {
            return Ok(Vec::new());
        }

        let bindings = lookup_bindings(info_hashes_v2);
        let rows = sqlx::query_as::<_, RawExistingV2Row>(LOOKUP_SQL)
            .bind(bindings)
            .fetch_all(&self.pool)
            .await?;
        decode_rows(rows)
    }
}

#[async_trait]
impl DhtTorrentV2Lookup for PgDhtTorrentV2Lookup {
    async fn lookup_existing_v2(
        &self,
        info_hashes_v2: &[[u8; 32]],
    ) -> Result<Vec<DhtExistingV2Row>, PersistTorrentCollaboratorError> {
        self.lookup_db(info_hashes_v2)
            .await
            .map_err(|error| Box::new(error) as PersistTorrentCollaboratorError)
    }
}

fn lookup_bindings(info_hashes_v2: &[[u8; 32]]) -> Vec<Vec<u8>> {
    info_hashes_v2
        .iter()
        .map(|info_hash_v2| info_hash_v2.to_vec())
        .collect()
}

fn decode_rows(rows: Vec<RawExistingV2Row>) -> DbResult<Vec<DhtExistingV2Row>> {
    rows.into_iter().map(decode_row).collect()
}

fn decode_row(row: RawExistingV2Row) -> DbResult<DhtExistingV2Row> {
    let primary_info_hash = Id20::from_slice(&row.info_hash).map_err(|error| {
        DbError::Decode(format!(
            "DHT torrent v2 lookup info_hash ({} bytes): {error}",
            row.info_hash.len()
        ))
    })?;
    let info_hash_v2 = <[u8; 32]>::try_from(row.info_hash_v2.as_slice()).map_err(|_| {
        DbError::Decode(format!(
            "DHT torrent v2 lookup info_hash_v2 must be exactly 32 bytes, got {}",
            row.info_hash_v2.len()
        ))
    })?;
    Ok(DhtExistingV2Row {
        info_hash_v2,
        primary_info_hash,
    })
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;

    use sqlx::postgres::PgPoolOptions;

    use super::*;

    fn id(byte: u8) -> Id20 {
        Id20::from_slice(&[byte; 20]).unwrap()
    }

    fn raw(primary: &[u8], full_v2: &[u8]) -> RawExistingV2Row {
        RawExistingV2Row {
            info_hash: primary.to_vec(),
            info_hash_v2: full_v2.to_vec(),
        }
    }

    fn assert_send_sync<T: Send + Sync>() {}

    fn assert_send_future<F>(_: F)
    where
        F: Future + Send,
    {
    }

    #[test]
    fn sql_shape_is_exact_membership_without_row_loss_or_order_claims() {
        assert_eq!(
            LOOKUP_SQL,
            "SELECT torrents.info_hash, torrents.info_hash_v2 FROM torrents WHERE torrents.info_hash_v2 = ANY($1::bytea[])"
        );
        for forbidden in ["DISTINCT", "GROUP BY", "ORDER BY", "LIMIT"] {
            assert!(!LOOKUP_SQL.contains(forbidden));
        }
    }

    #[test]
    fn bindings_preserve_direct_bytes_order_and_occurrences() {
        let first = [0x11; 32];
        let second = [0x22; 32];
        assert_eq!(
            lookup_bindings(&[first, second, first]),
            [first.to_vec(), second.to_vec(), first.to_vec()]
        );
    }

    #[test]
    fn valid_row_decodes_exact_primary_and_full_v2_hashes() {
        assert_eq!(
            decode_row(raw(&[0x33; 20], &[0x44; 32])).unwrap(),
            DhtExistingV2Row {
                info_hash_v2: [0x44; 32],
                primary_info_hash: id(0x33),
            }
        );
    }

    #[test]
    fn primary_and_v2_widths_are_strict_with_column_context() {
        for width in [19, 21] {
            let error = decode_row(raw(&vec![0x11; width], &[0x22; 32])).unwrap_err();
            assert!(matches!(error, DbError::Decode(_)));
            assert!(error.to_string().contains("info_hash"));
            assert!(error.to_string().contains(&format!("{width} bytes")));
        }
        for width in [31, 33] {
            let error = decode_row(raw(&[0x11; 20], &vec![0x22; width])).unwrap_err();
            assert!(matches!(error, DbError::Decode(_)));
            assert!(error.to_string().contains("info_hash_v2"));
            assert!(error.to_string().contains(&width.to_string()));
        }
    }

    #[test]
    fn duplicate_v2_rows_and_database_order_are_preserved_for_worker_canonicalization() {
        let full = [0xaa; 32];
        assert_eq!(
            decode_rows(vec![
                raw(&[0x30; 20], &full),
                raw(&[0x10; 20], &full),
                raw(&[0x20; 20], &[0xbb; 32]),
            ])
            .unwrap(),
            [
                DhtExistingV2Row {
                    info_hash_v2: full,
                    primary_info_hash: id(0x30),
                },
                DhtExistingV2Row {
                    info_hash_v2: full,
                    primary_info_hash: id(0x10),
                },
                DhtExistingV2Row {
                    info_hash_v2: [0xbb; 32],
                    primary_info_hash: id(0x20),
                },
            ]
        );
    }

    #[tokio::test]
    async fn empty_lookup_short_circuits_without_pool_access() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres@127.0.0.1:1/unused")
            .unwrap();
        pool.close().await;
        let adapter = PgDhtTorrentV2Lookup::new(pool);
        assert!(DhtTorrentV2Lookup::lookup_existing_v2(&adapter, &[])
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn clone_object_api_and_closed_pool_error_preserve_database_type() {
        assert_send_sync::<PgDhtTorrentV2Lookup>();
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres@127.0.0.1:1/unused")
            .unwrap();
        let adapter = PgDhtTorrentV2Lookup::new(pool);
        let object: Arc<dyn DhtTorrentV2Lookup> = Arc::new(adapter.clone());
        assert_send_future(object.lookup_existing_v2(&[[0x55; 32]]));

        adapter.pool().close().await;
        assert!(adapter.pool().is_closed());
        let error = object.lookup_existing_v2(&[[0x55; 32]]).await.unwrap_err();
        let database = error
            .downcast::<DbError>()
            .expect("adapter errors preserve the database error type");
        assert!(matches!(*database, DbError::Sqlx(sqlx::Error::PoolClosed)));
    }
}
