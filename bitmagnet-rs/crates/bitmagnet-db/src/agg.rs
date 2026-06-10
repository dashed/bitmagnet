//! Batch aggregate readers for the L2 `verify` (Job A) parity checker.
//!
//! The expected side of the check is recomputed from the `files_data` blob by
//! `bitmagnet-parquet` (the SAME G1 decode the export uses); this module reads
//! the **actual** side — the live `torrent_files` table the blob replaces —
//! aggregated to the per-`(torrent, extension)` grain.

use bitmagnet_model::InfoHash;
use sqlx::{PgPool, Row};

use crate::error::{DbError, Result};

/// One `(info_hash, extension, max_size)` aggregate row from `torrent_files`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileExtAgg {
    pub info_hash: InfoHash,
    /// Valid (non-NULL) G1 path-derived extension — `torrent_files.extension`
    /// is the PG generated column.
    pub extension: String,
    /// `max(size)` over the torrent's files of this extension.
    pub max_size: i64,
}

/// SQL for [`batch_torrent_files_ext_agg`]. `extension IS NOT NULL` mirrors the
/// blob side skipping empty path-derived extensions (L2-P0 §7 null/empty
/// symmetry — both sides see valid extensions only, or every no-ext torrent
/// false-positives). The `::bytea[]` cast is belt-and-suspenders: sqlx already
/// sends the bound `Vec<Vec<u8>>` as bytea[] (OID 1001) in the Parse message.
const BATCH_TORRENT_FILES_AGG_SQL: &str = "\
SELECT info_hash, extension, max(size) AS max_size \
FROM torrent_files \
WHERE info_hash = ANY($1::bytea[]) AND extension IS NOT NULL \
GROUP BY info_hash, extension";

/// Reads the per-`(torrent, extension)` aggregate of `torrent_files` for a
/// batch of torrents. Order is unspecified; group by [`FileExtAgg::info_hash`]
/// on the caller side. An empty `keys` slice short-circuits to `Ok(vec![])`
/// (binding an empty array is pointless work).
pub async fn batch_torrent_files_ext_agg(
    pool: &PgPool,
    keys: &[InfoHash],
) -> Result<Vec<FileExtAgg>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let keys: Vec<Vec<u8>> = keys.iter().map(|ih| ih.as_slice().to_vec()).collect();

    let rows = sqlx::query(BATCH_TORRENT_FILES_AGG_SQL)
        .bind(keys)
        .fetch_all(pool)
        .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let raw: Vec<u8> = row.try_get("info_hash")?;
        let info_hash =
            InfoHash::from_slice(&raw).map_err(|e| DbError::Decode(format!("info_hash: {e}")))?;
        out.push(FileExtAgg {
            info_hash,
            // IS NOT NULL filter guarantees non-NULL; read as String directly
            // (sqlx keys decode off the per-row value, not column metadata).
            extension: row.try_get("extension")?,
            // max(size) over bigint is bigint — exact int8 OID match.
            max_size: row.try_get("max_size")?,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agg_sql_shape() {
        // Guard the contract the parity checker depends on: per-(torrent,ext)
        // grain, valid extensions only, batched by bytea[] key array.
        assert!(BATCH_TORRENT_FILES_AGG_SQL.contains("FROM torrent_files"));
        assert!(BATCH_TORRENT_FILES_AGG_SQL.contains("ANY($1::bytea[])"));
        assert!(BATCH_TORRENT_FILES_AGG_SQL.contains("extension IS NOT NULL"));
        assert!(BATCH_TORRENT_FILES_AGG_SQL.contains("GROUP BY info_hash, extension"));
        assert!(BATCH_TORRENT_FILES_AGG_SQL.contains("max(size)"));
    }
}
