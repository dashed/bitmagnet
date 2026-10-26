//! PostgreSQL adapter for the info-hash-triage database projection.

use async_trait::async_trait;
use bitmagnet_db::{DbError, PgPool, Result as DbResult};
use bitmagnet_dht::Id20;
use bitmagnet_model::FilesStatus;

use crate::{DhtTorrentTriageLookup, DhtTorrentTriageRow, TriageCollaboratorError};

const DHT_SOURCE: &str = "dht";

/// The six-column Go triage projection, normalized for SQLx decoding.
///
/// The DHT source predicate deliberately remains in the `LEFT JOIN`: moving it
/// into `WHERE` would discard torrents that have no DHT source row. PostgreSQL
/// enum and integer columns are projected into stable runtime types, and the
/// source timestamp becomes the signed Unix-microsecond representation expected
/// by [`DhtTorrentTriageRow`]. Result order remains unspecified, as in Go.
const LOOKUP_SQL: &str = "\
SELECT torrents.info_hash, \
torrents.files_status::text AS files_status, \
torrents.files_count::bigint AS files_count, \
torrents_torrent_sources.seeders::bigint AS dht_seeders, \
torrents_torrent_sources.leechers::bigint AS dht_leechers, \
CAST(EXTRACT(EPOCH FROM torrents_torrent_sources.updated_at) * 1000000 AS bigint) AS dht_updated_at_unix_micros \
FROM torrents \
LEFT JOIN torrents_torrent_sources \
ON torrents.info_hash = torrents_torrent_sources.info_hash \
AND torrents_torrent_sources.source = $1 \
WHERE torrents.info_hash = ANY($2::bytea[])";

#[derive(Debug, sqlx::FromRow)]
struct RawTriageRow {
    info_hash: Vec<u8>,
    files_status: String,
    files_count: Option<i64>,
    dht_seeders: Option<i64>,
    dht_leechers: Option<i64>,
    dht_updated_at_unix_micros: Option<i64>,
}

/// PostgreSQL-backed [`DhtTorrentTriageLookup`].
///
/// The adapter owns a cheap clone of an application-owned [`PgPool`]. It does
/// not create or close the pool, start tasks, retry queries, open an explicit
/// transaction, or impose a statement timeout. Dropping an in-flight lookup
/// stops awaiting SQLx; it does not claim that PostgreSQL has synchronously
/// stopped server-side work.
///
/// The static query binds the occurrence-preserving input as one `bytea[]`.
/// This has the same membership behavior as Go's dynamic `IN` list, but exact
/// SQL text and bind cardinality are deliberately not claimed as Go parity.
///
/// Database decoding is deliberately fail-closed where the Go scanners are
/// permissive: hashes must be exactly 20 bytes, and counts must be nonnegative.
/// A missing joined timestamp remains `None`, which the triage policy treats as
/// scrape-eligible; the Go oracle does not cover that nullable timestamp path.
/// The adapter's tests freeze SQL construction, bindings, and row decoding
/// offline. They do not claim live PostgreSQL array-codec, schema, index/plan,
/// or query-cancellation evidence.
#[derive(Clone, Debug)]
#[must_use = "the adapter must be installed as a triage lookup collaborator"]
pub struct PgDhtTorrentTriageLookup {
    pool: PgPool,
}

impl PgDhtTorrentTriageLookup {
    /// Wrap an already configured, application-owned pool.
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the shared pool used by this adapter.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn lookup_db(&self, info_hashes: &[Id20]) -> DbResult<Vec<DhtTorrentTriageRow>> {
        if info_hashes.is_empty() {
            return Ok(Vec::new());
        }

        let bindings = lookup_bindings(info_hashes);
        let rows = sqlx::query_as::<_, RawTriageRow>(LOOKUP_SQL)
            .bind(DHT_SOURCE)
            .bind(bindings)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(decode_row).collect()
    }
}

#[async_trait]
impl DhtTorrentTriageLookup for PgDhtTorrentTriageLookup {
    async fn lookup(
        &self,
        info_hashes: &[Id20],
    ) -> Result<Vec<DhtTorrentTriageRow>, TriageCollaboratorError> {
        self.lookup_db(info_hashes)
            .await
            .map_err(|error| Box::new(error) as TriageCollaboratorError)
    }
}

fn lookup_bindings(info_hashes: &[Id20]) -> Vec<Vec<u8>> {
    info_hashes
        .iter()
        .map(|info_hash| info_hash.as_bytes().to_vec())
        .collect()
}

fn decode_row(row: RawTriageRow) -> DbResult<DhtTorrentTriageRow> {
    let info_hash = Id20::from_slice(&row.info_hash).map_err(|error| {
        DbError::Decode(format!(
            "DHT triage info_hash ({} bytes): {error}",
            row.info_hash.len()
        ))
    })?;
    let files_status = row.files_status.parse::<FilesStatus>().map_err(|error| {
        DbError::Decode(format!(
            "DHT triage files_status {:?}: {error}",
            row.files_status
        ))
    })?;

    Ok(DhtTorrentTriageRow {
        info_hash,
        files_status,
        files_count: decode_count("files_count", row.files_count)?,
        dht_seeders: decode_count("dht_seeders", row.dht_seeders)?,
        dht_leechers: decode_count("dht_leechers", row.dht_leechers)?,
        dht_updated_at_unix_micros: row.dht_updated_at_unix_micros,
    })
}

fn decode_count(column: &'static str, value: Option<i64>) -> DbResult<Option<u64>> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| {
                DbError::Decode(format!(
                    "DHT triage {column} must be nonnegative, got {value}"
                ))
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;

    use bitmagnet_db::DbError;
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    fn id(byte: u8) -> Id20 {
        Id20::from_slice(&[byte; 20]).unwrap()
    }

    fn raw(files_status: &str) -> RawTriageRow {
        RawTriageRow {
            info_hash: id(7).as_bytes().to_vec(),
            files_status: files_status.to_owned(),
            files_count: Some(10),
            dht_seeders: Some(20),
            dht_leechers: Some(30),
            dht_updated_at_unix_micros: Some(1_700_000_000_000_123),
        }
    }

    fn assert_send_sync<T: Send + Sync>() {}

    fn assert_send_future<F>(_: F)
    where
        F: Future + Send,
    {
    }

    #[test]
    fn sql_shape_projection_left_join_source_and_array_membership_are_exact() {
        assert_eq!(
            LOOKUP_SQL,
            "SELECT torrents.info_hash, torrents.files_status::text AS files_status, torrents.files_count::bigint AS files_count, torrents_torrent_sources.seeders::bigint AS dht_seeders, torrents_torrent_sources.leechers::bigint AS dht_leechers, CAST(EXTRACT(EPOCH FROM torrents_torrent_sources.updated_at) * 1000000 AS bigint) AS dht_updated_at_unix_micros FROM torrents LEFT JOIN torrents_torrent_sources ON torrents.info_hash = torrents_torrent_sources.info_hash AND torrents_torrent_sources.source = $1 WHERE torrents.info_hash = ANY($2::bytea[])"
        );
        assert_eq!(DHT_SOURCE, "dht");
        assert!(!LOOKUP_SQL.contains("ORDER BY"));
    }

    #[test]
    fn bindings_preserve_every_input_occurrence_in_order() {
        let first = id(1);
        let second = id(2);
        assert_eq!(
            lookup_bindings(&[first, second, first]),
            [
                first.as_bytes().to_vec(),
                second.as_bytes().to_vec(),
                first.as_bytes().to_vec(),
            ]
        );
    }

    #[test]
    fn all_statuses_counts_and_signed_microseconds_decode_exactly() {
        for (text, expected) in [
            ("no_info", FilesStatus::NoInfo),
            ("single", FilesStatus::Single),
            ("multi", FilesStatus::Multi),
            ("over_threshold", FilesStatus::OverThreshold),
        ] {
            let mut value = raw(text);
            value.dht_updated_at_unix_micros = Some(-7);
            assert_eq!(
                decode_row(value).unwrap(),
                DhtTorrentTriageRow {
                    info_hash: id(7),
                    files_status: expected,
                    files_count: Some(10),
                    dht_seeders: Some(20),
                    dht_leechers: Some(30),
                    dht_updated_at_unix_micros: Some(-7),
                }
            );
        }
    }

    #[test]
    fn missing_left_join_values_remain_none() {
        let mut value = raw("single");
        value.files_count = None;
        value.dht_seeders = None;
        value.dht_leechers = None;
        value.dht_updated_at_unix_micros = None;
        assert_eq!(
            decode_row(value).unwrap(),
            DhtTorrentTriageRow {
                info_hash: id(7),
                files_status: FilesStatus::Single,
                files_count: None,
                dht_seeders: None,
                dht_leechers: None,
                dht_updated_at_unix_micros: None,
            }
        );
    }

    #[test]
    fn malformed_hash_invalid_status_and_negative_counts_are_decode_errors() {
        let mut malformed_hash = raw("single");
        malformed_hash.info_hash.pop();
        assert!(matches!(
            decode_row(malformed_hash),
            Err(DbError::Decode(_))
        ));

        assert!(matches!(
            decode_row(raw("unknown")),
            Err(DbError::Decode(_))
        ));

        for column in ["files_count", "dht_seeders", "dht_leechers"] {
            let mut value = raw("single");
            match column {
                "files_count" => value.files_count = Some(-1),
                "dht_seeders" => value.dht_seeders = Some(-2),
                "dht_leechers" => value.dht_leechers = Some(-3),
                _ => unreachable!(),
            }
            let error = decode_row(value).unwrap_err();
            assert!(matches!(error, DbError::Decode(_)));
            assert!(error.to_string().contains(column));
        }
    }

    #[tokio::test]
    async fn empty_lookup_short_circuits_without_pool_access() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres@127.0.0.1:1/unused")
            .unwrap();
        pool.close().await;
        let adapter = PgDhtTorrentTriageLookup::new(pool);
        assert!(DhtTorrentTriageLookup::lookup(&adapter, &[])
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn clone_object_api_and_closed_pool_error_are_exact() {
        assert_send_sync::<PgDhtTorrentTriageLookup>();
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres@127.0.0.1:1/unused")
            .unwrap();
        let adapter = PgDhtTorrentTriageLookup::new(pool);
        let clone = adapter.clone();
        let object: Arc<dyn DhtTorrentTriageLookup> = Arc::new(clone);
        assert_send_future(object.lookup(&[id(9)]));

        adapter.pool().close().await;
        assert!(adapter.pool().is_closed());
        let error = object.lookup(&[id(9)]).await.unwrap_err();
        let database = error
            .downcast::<DbError>()
            .expect("adapter errors preserve the database error type");
        assert!(matches!(*database, DbError::Sqlx(sqlx::Error::PoolClosed)));
    }
}
