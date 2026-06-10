//! Keyset-paginated reads of torrents for the **L3 path-FTS** (per-torrent
//! path-bag) index — both the one-shot backfill and the steady-state PG-tail
//! follow loop.
//!
//! Unlike [`crate::stream::stream_torrents_for_index`] (which drives FROM
//! `torrent_contents`, one row per *search document*), the path index is
//! **per-torrent** (one path-bag doc per torrent), so these drive FROM
//! `torrents` and pull the torrent's file blob plus a single `seeders` ranking
//! signal (the max over the torrent's content classifications).
//!
//! Two cursors, one row shape:
//! * **Backfill** ([`stream_torrents_for_pathsearch`]) — keyset on the
//!   `info_hash` primary key; a stable full scan that needs no ordering by a
//!   mutable column.
//! * **Follow** ([`stream_torrents_for_pathsearch_since`]) — keyset on the
//!   composite `(updated_at_micros, info_hash)` **watermark**, so a re-crawl
//!   (which bumps `torrents.updated_at`) is swept up again. `updated_at` is
//!   exposed as `bigint` microseconds (`EXTRACT(EPOCH …) * 1e6`) so the cursor
//!   compares without a `chrono`/`time` SQLx feature, and microsecond precision
//!   matches Postgres `timestamptz`.

use bitmagnet_model::{deserialize_files, BlobError, BlobFile, InfoHash};
use sqlx::{PgPool, Row};

use crate::error::{DbError, Result};

/// One torrent's path-bag source row: identity + ranking signals + the
/// compressed `files_data` blob (decoded lazily via [`Self::files`]).
#[derive(Debug, Clone)]
pub struct TorrentForPathIndex {
    /// 20-byte info hash — the path-bag doc's delete/upsert key and hit identity.
    pub info_hash: InfoHash,
    /// Torrent display name (the single-file path-bag fallback when the blob is
    /// empty — see the sidecar's `build_path_document`).
    pub name: String,
    /// Total size in bytes (`torrents.size`).
    pub size: i64,
    /// Files-status enum value as text (e.g. `"single"`, `"multi"`).
    pub files_status: String,
    /// Number of files, when known.
    pub files_count: Option<i64>,
    /// Max seeders across the torrent's content classifications (0 when none /
    /// unclassified). The index sort + typeahead rank key.
    pub seeders: i64,
    /// `updated_at` as Unix **microseconds** — the follow-loop watermark
    /// component. `0` for the backfill query (it does not select it).
    pub updated_at_micros: i64,
    /// Compressed file list (`NULL` when no blob is stored).
    pub files_data: Option<Vec<u8>>,
}

impl TorrentForPathIndex {
    /// Decompresses and decodes [`Self::files_data`], returning an empty vec when
    /// no blob is present.
    pub fn files(&self) -> std::result::Result<Vec<BlobFile>, BlobError> {
        match &self.files_data {
            Some(blob) => deserialize_files(blob),
            None => Ok(Vec::new()),
        }
    }
}

// The seeders signal shared by both queries: the max over the torrent's content
// rows. A correlated scalar subquery (no GROUP BY) so it rides the
// `torrent_contents` PK's `info_hash` prefix. Inlined into the two static SQL
// literals below (sqlx 0.9 requires a `&'static str`, so these cannot be
// format!-built — they carry no dynamic data anyway).

/// Backfill SQL: keyset on the `info_hash` PK (`$1` exclusive lower bound, NULL
/// for the first page; `$2` the page size). Stable full scan. `updated_at` is
/// also projected (as micros) so the backfill bin can seed the follow-loop
/// watermark to the max it saw — without it the loop would re-sweep the whole
/// corpus on first start. The keyset/ordering stay on the stable `info_hash` PK.
const BACKFILL_SQL: &str = "\
SELECT t.info_hash AS info_hash, t.name AS name, t.size AS size, \
t.files_status::text AS files_status, t.files_count AS files_count, \
COALESCE((SELECT MAX(tc.seeders) FROM torrent_contents tc WHERE tc.info_hash = t.info_hash), 0)::bigint AS seeders, \
CAST(EXTRACT(EPOCH FROM t.updated_at) * 1000000 AS bigint) AS updated_at_micros, \
t.files_data AS files_data \
FROM torrents t \
WHERE ($1::bytea IS NULL OR t.info_hash > $1) \
ORDER BY t.info_hash ASC \
LIMIT $2";

/// Follow SQL: keyset on the `(updated_at, info_hash)` watermark. `$1` = last
/// seen `updated_at` in Unix micros (converted to `timestamptz` server-side so
/// the comparison and `ORDER BY` use the RAW column — index-friendly; an
/// expression like `EXTRACT(EPOCH ...)` here would defeat any
/// `torrents (updated_at, info_hash)` btree and force a full sort per poll),
/// `$2` = last seen info_hash (the tiebreak for rows sharing a timestamp),
/// `$3` = page size. `updated_at` is still SURFACED as `bigint` micros for a
/// `chrono`-free cursor compare (an output expression doesn't affect index use).
///
/// The `now() - interval '30 seconds'` LAG closes the commit-visibility race:
/// `updated_at` is set at statement time inside the writer's transaction, so a
/// transaction that COMMITS after the loop has already read past its
/// `updated_at` would otherwise be skipped forever. By never reading rows newer
/// than now()-30s, the watermark can't advance past a row that is still
/// invisible (any writer transaction shorter than 30s commits while its rows
/// are still ahead of the watermark). Re-processing on overlap is safe —
/// supersession (delete_term + re-add) is idempotent.
///
/// Round-trip precision: PG timestamps are integral microseconds; micros ≈
/// 1.7e15 ≪ 2^53, so the f64 division in `to_timestamp($1/1e6)` is exact to
/// <0.5 µs and to_timestamp's round-to-µs recovers the original instant.
///
/// ⚠️ Deploy prerequisite: `torrents` needs a btree on `(updated_at, info_hash)`
/// (or at least `(updated_at)`) — see dv3-l3-build-notes.md.
const FOLLOW_SQL: &str = "\
SELECT t.info_hash AS info_hash, t.name AS name, t.size AS size, \
t.files_status::text AS files_status, t.files_count AS files_count, \
COALESCE((SELECT MAX(tc.seeders) FROM torrent_contents tc WHERE tc.info_hash = t.info_hash), 0)::bigint AS seeders, \
CAST(EXTRACT(EPOCH FROM t.updated_at) * 1000000 AS bigint) AS updated_at_micros, \
t.files_data AS files_data \
FROM torrents t \
WHERE (t.updated_at, t.info_hash) > (to_timestamp(($1::bigint)::double precision / 1000000.0), $2) \
AND t.updated_at <= now() - interval '30 seconds' \
ORDER BY t.updated_at ASC, t.info_hash ASC \
LIMIT $3";

/// Map a backfill/`info_hash`-cursor row (no `updated_at` column).
fn map_backfill_row(row: &sqlx::postgres::PgRow) -> Result<TorrentForPathIndex> {
    let raw: Vec<u8> = row.try_get("info_hash")?;
    let info_hash =
        InfoHash::from_slice(&raw).map_err(|e| DbError::Decode(format!("info_hash: {e}")))?;
    Ok(TorrentForPathIndex {
        info_hash,
        name: row.try_get("name")?,
        size: row.try_get("size")?,
        files_status: row.try_get("files_status")?,
        files_count: row.try_get("files_count")?,
        seeders: row.try_get("seeders")?,
        updated_at_micros: row.try_get("updated_at_micros")?,
        files_data: row.try_get("files_data")?,
    })
}

/// Reads up to `limit` torrents whose `info_hash` is greater than
/// `after_info_hash` (or from the start when `None`), ordered by `info_hash` —
/// the one-shot path-FTS backfill cursor. Pass the last returned info_hash back
/// as `after_info_hash` for the next page.
pub async fn stream_torrents_for_pathsearch(
    pool: &PgPool,
    after_info_hash: Option<&InfoHash>,
    limit: i64,
) -> Result<Vec<TorrentForPathIndex>> {
    let after: Option<Vec<u8>> = after_info_hash.map(|ih| ih.as_slice().to_vec());
    let rows = sqlx::query(BACKFILL_SQL)
        .bind(after)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    rows.iter().map(map_backfill_row).collect()
}

/// Reads up to `limit` torrents strictly after the `(after_updated_at_micros,
/// after_info_hash)` watermark, ordered by that same composite key — the
/// steady-state follow cursor. The first call uses `(0, &[])` (epoch) to sweep
/// everything; thereafter pass the last returned `(updated_at_micros, info_hash)`.
pub async fn stream_torrents_for_pathsearch_since(
    pool: &PgPool,
    after_updated_at_micros: i64,
    after_info_hash: &[u8],
    limit: i64,
) -> Result<Vec<TorrentForPathIndex>> {
    let rows = sqlx::query(FOLLOW_SQL)
        .bind(after_updated_at_micros)
        .bind(after_info_hash.to_vec())
        .bind(limit)
        .fetch_all(pool)
        .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let raw: Vec<u8> = row.try_get("info_hash")?;
        let info_hash =
            InfoHash::from_slice(&raw).map_err(|e| DbError::Decode(format!("info_hash: {e}")))?;
        out.push(TorrentForPathIndex {
            info_hash,
            name: row.try_get("name")?,
            size: row.try_get("size")?,
            files_status: row.try_get("files_status")?,
            files_count: row.try_get("files_count")?,
            seeders: row.try_get("seeders")?,
            updated_at_micros: row.try_get("updated_at_micros")?,
            files_data: row.try_get("files_data")?,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backfill_sql_shape() {
        let sql = BACKFILL_SQL;
        assert!(sql.contains("FROM torrents t"));
        assert!(sql.contains("t.info_hash > $1"));
        assert!(sql.contains("ORDER BY t.info_hash ASC"));
        assert!(sql.contains("LIMIT $2"));
        assert!(sql.contains("MAX(tc.seeders)"));
        // updated_at is PROJECTED (to seed the follow watermark) but the keyset
        // and ordering stay on the stable info_hash PK, not the mutable column.
        assert!(sql.contains("updated_at_micros"));
        assert!(!sql.contains("ORDER BY") || sql.contains("ORDER BY t.info_hash ASC"));
        assert!(!sql.contains("(updated_at"));
    }

    #[test]
    fn follow_sql_shape() {
        let sql = FOLLOW_SQL;
        assert!(sql.contains("FROM torrents t"));
        // Cursor compares the RAW (updated_at, info_hash) columns (index-usable;
        // an EXTRACT(...) expression in WHERE/ORDER BY would defeat the btree).
        assert!(sql.contains("(t.updated_at, t.info_hash) > (to_timestamp("));
        assert!(sql.contains("ORDER BY t.updated_at ASC, t.info_hash ASC"));
        // The 30s commit-visibility lag: never read rows newer than now()-30s,
        // so the watermark can't overtake a still-invisible transaction.
        assert!(sql.contains("now() - interval '30 seconds'"));
        assert!(sql.contains("LIMIT $3"));
        // micros are still SURFACED for the chrono-free cursor (output only —
        // does not affect index use).
        assert!(sql.contains("updated_at_micros"));
        assert!(sql.contains("EXTRACT(EPOCH FROM t.updated_at) * 1000000"));
        // ...but the OUTER WHERE clause must NOT wrap the cursor compare in
        // EXTRACT. (Split after FROM: the seeders correlated subquery in the
        // SELECT list has its own inner WHERE.)
        let after_from = sql.split("FROM torrents t").nth(1).unwrap();
        let where_only = after_from
            .split("WHERE")
            .nth(1)
            .unwrap()
            .split("ORDER BY")
            .next()
            .unwrap();
        assert!(!where_only.contains("EXTRACT"));
    }

    #[test]
    fn files_round_trips_through_dto() {
        use bitmagnet_model::serialize_files;
        let original = vec![BlobFile {
            index: 0,
            path: "a.mkv".to_owned(),
            extension: "mkv".to_owned(),
            size: 10,
        }];
        let blob = serialize_files(&original).unwrap();
        let row = TorrentForPathIndex {
            info_hash: "0123456789abcdef0123456789abcdef01234567".parse().unwrap(),
            name: "t".to_owned(),
            size: 10,
            files_status: "multi".to_owned(),
            files_count: Some(1),
            seeders: 7,
            updated_at_micros: 1_700_000_000_000_000,
            files_data: Some(blob),
        };
        assert_eq!(row.files().unwrap(), original);
    }
}
