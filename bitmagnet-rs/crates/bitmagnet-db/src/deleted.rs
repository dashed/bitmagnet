//! The L2 deletion-audit reader.
//!
//! Hard-deleted torrents vanish from `torrents`, so the delta carve cannot see
//! them — the audit table `deleted_torrents` (a tiny `AFTER DELETE` trigger
//! target on `torrents`; source-owned DDL ships in
//! `migrations/00024_l1_l2_l3_follow_contract.sql`, while homelab keeps an
//! idempotent adoption playbook for already-deployed clusters) records each
//! deletion:
//!
//! ```sql
//! CREATE TABLE deleted_torrents (
//!     info_hash  bytea PRIMARY KEY,
//!     deleted_at timestamptz NOT NULL DEFAULT now()
//! );
//! ```
//!
//! The delta job reads the same half-open `(since, until]` window it carves
//! changes with (both bounds commit-visibility-lagged) and tombstones the
//! result — the read-time anti-join makes a pure-tombstone torrent vanish.
//! Re-added-then-redeleted torrents upsert `deleted_at`, so a hash can appear
//! in a later window again; tombstoning an already-absent hash is harmless.

use bitmagnet_model::InfoHash;
use sqlx::{PgPool, Row};

use crate::error::{DbError, Result};

/// SQL for [`read_deleted_torrents`]. Same window contract as the change carve
/// (`stream_changed_torrents`): half-open `(since, until]` on epoch-second
/// bounds. Deletions are rare, so a single bounded read (no keyset) suffices;
/// `LIMIT $3` is a runaway guard, not pagination.
const READ_DELETED_SQL: &str = "\
SELECT info_hash \
FROM deleted_torrents \
WHERE deleted_at > to_timestamp($1) \
AND deleted_at <= to_timestamp($2) \
ORDER BY deleted_at ASC \
LIMIT $3";

/// SQL for [`prune_deleted_torrents`]. A merge-base cut is inclusive: every
/// deletion at or before it has been folded into the new base.
const PRUNE_DELETED_SQL: &str = "\
DELETE FROM deleted_torrents \
WHERE deleted_at <= to_timestamp($1)";

/// Reads the info hashes of torrents hard-deleted in the half-open window
/// `(since_epoch, until_epoch]` from the `deleted_torrents` audit table.
/// Returns at most `limit` hashes (deletes are rare; a full window normally
/// fits one read — a truncated read just means the NEXT delta run picks the
/// rest up, since the watermark only advances to `until_epoch`).
pub async fn read_deleted_torrents(
    pool: &PgPool,
    since_epoch: i64,
    until_epoch: i64,
    limit: i64,
) -> Result<Vec<InfoHash>> {
    let rows = sqlx::query(READ_DELETED_SQL)
        .bind(since_epoch)
        .bind(until_epoch)
        .bind(limit)
        .fetch_all(pool)
        .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let raw: Vec<u8> = row.try_get("info_hash")?;
        let info_hash =
            InfoHash::from_slice(&raw).map_err(|e| DbError::Decode(format!("info_hash: {e}")))?;
        out.push(info_hash);
    }
    Ok(out)
}

/// Delete audit tombstones at or before `cutoff_epoch`, returning the number of
/// rows removed. Callers must derive the cutoff from a successfully published
/// merge-base cut (normally with an additional safety margin).
pub async fn prune_deleted_torrents(pool: &PgPool, cutoff_epoch: i64) -> Result<u64> {
    let result = sqlx::query(PRUNE_DELETED_SQL)
        .bind(cutoff_epoch)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deleted_sql_shape() {
        // The window contract must mirror the change carve: half-open
        // (since, until] so a lagged watermark never double-reads or skips.
        assert!(READ_DELETED_SQL.contains("FROM deleted_torrents"));
        assert!(READ_DELETED_SQL.contains("deleted_at > to_timestamp($1)"));
        assert!(READ_DELETED_SQL.contains("deleted_at <= to_timestamp($2)"));
        assert!(READ_DELETED_SQL.contains("LIMIT $3"));
    }

    #[test]
    fn prune_deleted_sql_shape() {
        assert!(PRUNE_DELETED_SQL.contains("DELETE FROM deleted_torrents"));
        assert!(PRUNE_DELETED_SQL.contains("deleted_at <= to_timestamp($1)"));
    }

    /// End-to-end prune semantics against a live PostgreSQL. A single-connection
    /// pool plus a TEMP table shadows the real audit table, so this test cannot
    /// prune production tombstones. Ignored by default:
    ///
    /// ```sh
    /// BITMAGNET_POSTGRES_DSN=postgres://postgres@localhost/bitmagnet \
    ///   cargo test -p bitmagnet-db deleted::tests::prune_removes_only_rows_at_or_before_cutoff -- --ignored
    /// ```
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL (set BITMAGNET_POSTGRES_DSN)"]
    async fn prune_removes_only_rows_at_or_before_cutoff() {
        let mut config = crate::DbConfig::from_env().expect("postgres config from env");
        config.max_connections = 1;
        let pool = crate::connect(&config).await.expect("connect to postgres");
        sqlx::query(
            "CREATE TEMP TABLE deleted_torrents (\
             info_hash bytea PRIMARY KEY, \
             deleted_at timestamptz NOT NULL)",
        )
        .execute(&pool)
        .await
        .expect("create temporary deleted_torrents table");

        let cutoff = 1_700_000_000_i64;
        for (byte, deleted_at) in [(1_u8, cutoff - 20), (2, cutoff - 10), (3, cutoff + 10)] {
            sqlx::query(
                "INSERT INTO deleted_torrents (info_hash, deleted_at) \
                 VALUES ($1, to_timestamp($2))",
            )
            .bind(vec![byte; 20])
            .bind(deleted_at)
            .execute(&pool)
            .await
            .expect("seed deleted torrent");
        }

        let rows_deleted = prune_deleted_torrents(&pool, cutoff)
            .await
            .expect("prune deleted torrents");
        assert_eq!(rows_deleted, 2);

        let newer = read_deleted_torrents(&pool, cutoff, cutoff + 20, 10)
            .await
            .expect("read retained tombstones");
        assert_eq!(newer, vec![InfoHash::from_slice(&[3; 20]).unwrap()]);
    }
}
