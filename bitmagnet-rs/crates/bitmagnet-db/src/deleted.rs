//! The L2 deletion-audit reader.
//!
//! Hard-deleted torrents vanish from `torrents`, so the delta carve cannot see
//! them — the audit table `deleted_torrents` (a tiny `AFTER DELETE` trigger
//! target on `torrents`; DDL ships with the homelab playbook
//! `bitmagnet_deleted_audit.yml` and is documented in
//! `docs/dev/l2-verify-and-shadow-runbook.md`) records each deletion:
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
}
