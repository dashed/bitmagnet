//! Keyset-paginated read of torrents together with their compressed file
//! blob, for the Phase 3 search backfill.

use bitmagnet_model::{deserialize_files, BlobError, BlobFile, InfoHash};
use sqlx::{PgPool, Row};

use crate::error::{DbError, Result};

/// One torrent row plus its compressed `files_data` blob.
///
/// Scalar columns are kept in their raw PostgreSQL types (e.g. `size` as `i64`,
/// `files_status` as `String`); call [`Self::files`] to decode the blob into
/// [`BlobFile`]s.
#[derive(Debug, Clone)]
pub struct TorrentWithBlob {
    /// 20-byte info hash.
    pub info_hash: InfoHash,
    /// Torrent display name.
    pub name: String,
    /// Total size in bytes (PostgreSQL `bigint`).
    pub size: i64,
    /// Files-status enum value as text (e.g. `"single"`, `"multi"`).
    pub files_status: String,
    /// Number of files, when known.
    pub files_count: Option<i64>,
    /// Compressed file list (`NULL` when no blob is stored).
    pub files_data: Option<Vec<u8>>,
}

impl TorrentWithBlob {
    /// Decompresses and decodes [`Self::files_data`], returning an empty vec
    /// when no blob is present.
    pub fn files(&self) -> std::result::Result<Vec<BlobFile>, BlobError> {
        match &self.files_data {
            Some(blob) => deserialize_files(blob),
            None => Ok(Vec::new()),
        }
    }
}

/// SQL for [`stream_torrents_with_files`]. Keyset pagination on the `info_hash`
/// primary key: `$1` is the exclusive lower bound (`NULL` for the first page),
/// `$2` the page size. `files_status` is cast to `text` so it decodes into a
/// `String` regardless of whether the column is a PostgreSQL enum.
const STREAM_SQL: &str = "\
SELECT info_hash, name, size, files_status::text AS files_status, files_count, files_data \
FROM torrents \
WHERE ($1::bytea IS NULL OR info_hash > $1) \
ORDER BY info_hash ASC \
LIMIT $2";

/// Reads up to `limit` torrents whose `info_hash` is greater than
/// `after_info_hash` (or from the start when `None`), ordered by `info_hash`.
///
/// Pass the last returned hash back as `after_info_hash` to fetch the next
/// page. Uses the runtime [`sqlx::query`] API, so it compiles without a live
/// database.
pub async fn stream_torrents_with_files(
    pool: &PgPool,
    after_info_hash: Option<&InfoHash>,
    limit: i64,
) -> Result<Vec<TorrentWithBlob>> {
    let after: Option<Vec<u8>> = after_info_hash.map(|ih| ih.as_slice().to_vec());

    let rows = sqlx::query(STREAM_SQL)
        .bind(after)
        .bind(limit)
        .fetch_all(pool)
        .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let raw: Vec<u8> = row.try_get("info_hash")?;
        let info_hash =
            InfoHash::from_slice(&raw).map_err(|e| DbError::Decode(format!("info_hash: {e}")))?;
        out.push(TorrentWithBlob {
            info_hash,
            name: row.try_get("name")?,
            size: row.try_get("size")?,
            files_status: row.try_get("files_status")?,
            files_count: row.try_get("files_count")?,
            files_data: row.try_get("files_data")?,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitmagnet_model::serialize_files;

    #[test]
    fn files_decodes_blob_round_trip() {
        // Ties the db DTO to the model serializer without needing a database:
        // a blob produced by the model crate must read back through `files()`.
        let original = vec![
            BlobFile {
                index: 0,
                path: "a.mkv".to_owned(),
                extension: "mkv".to_owned(),
                size: 10,
            },
            BlobFile {
                index: 1,
                path: "b.srt".to_owned(),
                extension: "srt".to_owned(),
                size: 2,
            },
        ];
        let blob = serialize_files(&original).unwrap();
        let row = TorrentWithBlob {
            info_hash: "0123456789abcdef0123456789abcdef01234567".parse().unwrap(),
            name: "t".to_owned(),
            size: 12,
            files_status: "multi".to_owned(),
            files_count: Some(2),
            files_data: Some(blob),
        };
        assert_eq!(row.files().unwrap(), original);
    }

    #[test]
    fn files_is_empty_without_blob() {
        let row = TorrentWithBlob {
            info_hash: "0123456789abcdef0123456789abcdef01234567".parse().unwrap(),
            name: "t".to_owned(),
            size: 0,
            files_status: "no_info".to_owned(),
            files_count: None,
            files_data: None,
        };
        assert!(row.files().unwrap().is_empty());
    }

    #[test]
    fn stream_sql_shape() {
        // Guard the keyset-pagination contract the SQL must keep.
        assert!(STREAM_SQL.contains("FROM torrents"));
        assert!(STREAM_SQL.contains("info_hash > $1"));
        assert!(STREAM_SQL.contains("ORDER BY info_hash ASC"));
        assert!(STREAM_SQL.contains("LIMIT $2"));
        assert!(STREAM_SQL.contains("files_data"));
    }
}
