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
    /// Number of files, when known (`int4` in PG, cast to `bigint` in SQL —
    /// sqlx errors on an OID-mismatched `i64` read).
    pub files_count: Option<i64>,
    /// Torrent creation time as Unix epoch seconds. Used by pathsearch as a
    /// stable sort/debug field; exact result ordering can still be refined by
    /// the app/PG layer.
    pub published_at: i64,
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
SELECT info_hash, name, size, files_status::text AS files_status, \
files_count::bigint AS files_count, \
CAST(EXTRACT(EPOCH FROM created_at) AS bigint) AS published_at, \
files_data \
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
            published_at: row.try_get("published_at")?,
            files_data: row.try_get("files_data")?,
        });
    }
    Ok(out)
}

/// SQL for [`stream_changed_torrents`]. The L2 delta carve: torrents whose
/// `updated_at` advanced past the watermark `$1` (an epoch-seconds bound). Note
/// `persist.go` bumps `updated_at` on every `DoUpdates`, so this captures both
/// brand-new torrents and re-crawls. Keyset on `info_hash` (`$2`) within the
/// window so a big delta still pages. Deleted torrents are NOT visible here (the
/// row is gone) — the delta job carries deletions as a separate tombstone key
/// set (see `bitmagnet_parquet::delta`), and the read-time anti-join makes a
/// pure-tombstone (no fact rows) torrent vanish.
const STREAM_CHANGED_SQL: &str = "\
SELECT info_hash, name, size, files_status::text AS files_status, \
files_count::bigint AS files_count, \
CAST(EXTRACT(EPOCH FROM created_at) AS bigint) AS published_at, \
files_data \
FROM torrents \
WHERE updated_at > to_timestamp($1) \
AND updated_at <= to_timestamp($2) \
AND ($3::bytea IS NULL OR info_hash > $3) \
ORDER BY info_hash ASC \
LIMIT $4";

/// Reads up to `limit` torrents changed in the half-open window
/// `(since_epoch, until_epoch]` whose `info_hash` is greater than
/// `after_info_hash`, ordered by `info_hash` — the per-minute delta carve for
/// the L2 Parquet/agg refresh. Requires an index on `torrents.updated_at` for
/// an efficient carve (see the L2 spec §2a/§6).
///
/// `until_epoch` MUST be a commit-visibility-lagged "now" (now − ~30 s; see
/// `bitmagnet_parquet::export::CARVE_LAG_SECS`): `updated_at` is set at
/// statement time inside the writer's transaction, so an UNBOUNDED carve whose
/// watermark then advances to `now` would permanently skip any row whose
/// transaction committed between the query and `now`. Bounding the window at
/// now−lag (and persisting exactly that bound as the new watermark) guarantees
/// every row is read by exactly the run whose window contains its `updated_at`.
///
/// Returns the same [`TorrentWithBlob`] shape as
/// [`stream_torrents_with_files`]; decode each blob with [`TorrentWithBlob::files`].
pub async fn stream_changed_torrents(
    pool: &PgPool,
    since_epoch: i64,
    until_epoch: i64,
    after_info_hash: Option<&InfoHash>,
    limit: i64,
) -> Result<Vec<TorrentWithBlob>> {
    let after: Option<Vec<u8>> = after_info_hash.map(|ih| ih.as_slice().to_vec());

    let rows = sqlx::query(STREAM_CHANGED_SQL)
        .bind(since_epoch)
        .bind(until_epoch)
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
            published_at: row.try_get("published_at")?,
            files_data: row.try_get("files_data")?,
        });
    }
    Ok(out)
}

/// One `torrent_contents` row joined with its torrent and (when classified) its
/// content metadata — everything the search backfill needs to build a proto
/// `TorrentDocument`. One value per torrent_content row (NOT per torrent): a
/// torrent with several classifications yields several of these, each a distinct
/// search document keyed by [`Self::id`].
///
/// Scalar columns keep their raw PostgreSQL types; nullable columns are
/// `Option`. Integer columns are widened to `i64` in SQL so the backfill maps
/// them onto the proto's `u32`/`u64`/`i64` fields uniformly. `languages` and
/// `genres` arrive as Postgres `text[]` (the JSONB `languages` column is
/// flattened in SQL), so no JSON decoding is needed here.
#[derive(Debug, Clone)]
pub struct TorrentForIndex {
    /// `torrent_contents.id`: the generated composite PK
    /// `hex(info_hash):content_type:content_source:content_id`. Both the keyset
    /// cursor and the Tantivy upsert key (the sidecar's `doc_id`).
    pub id: String,
    /// 20-byte info hash.
    pub info_hash: InfoHash,
    /// Torrent display name (`torrents.name`).
    pub torrent_name: String,
    /// Files-status enum value as text (e.g. `"single"`, `"multi"`); cast to
    /// `text` in SQL so it decodes into a `String` regardless of the column's
    /// PostgreSQL enum type. Drives the single-file `file_extensions` fallback.
    pub files_status: String,
    /// Classification key; all `None` for an unclassified torrent_content.
    pub content_type: Option<String>,
    pub content_source: Option<String>,
    pub content_id: Option<String>,
    /// Classified content title / original title (`content.*`, `None` when
    /// unclassified or absent).
    pub content_title: Option<String>,
    pub original_title: Option<String>,
    /// Release year, from the `content` table (`torrent_contents` dropped its
    /// `release_year` in migration 00007); widened to `i64`.
    pub release_year: Option<i64>,
    /// Parsed video attributes.
    pub video_resolution: Option<String>,
    pub video_source: Option<String>,
    pub video_codec: Option<String>,
    pub video_3d: Option<String>,
    pub video_modifier: Option<String>,
    pub release_group: Option<String>,
    /// Swarm / ordering stats (widened to `i64`).
    pub seeders: Option<i64>,
    pub leechers: Option<i64>,
    /// Total torrent size in bytes (`torrents.size`).
    pub size: i64,
    pub files_count: Option<i64>,
    /// `epoch(coalesce(tc.published_at, t.created_at))` in seconds.
    pub published_at: i64,
    /// Detected content languages (flattened from the JSONB column).
    pub languages: Vec<String>,
    /// Genre collection names (`content_collections` of type `genre`).
    pub genres: Vec<String>,
    /// Compressed file list (`NULL` when no blob is stored); decode with
    /// [`Self::files`] to derive file paths / extensions.
    pub files_data: Option<Vec<u8>>,
}

impl TorrentForIndex {
    /// Decompresses and decodes [`Self::files_data`], returning an empty vec
    /// when no blob is present.
    pub fn files(&self) -> std::result::Result<Vec<BlobFile>, BlobError> {
        match &self.files_data {
            Some(blob) => deserialize_files(blob),
            None => Ok(Vec::new()),
        }
    }
}

/// SQL for [`stream_torrents_for_index`]. Drives FROM `torrent_contents` (one
/// row = one search document), mirroring bitmagnet's `tsv @@ tsquery` search so
/// the Tantivy index matches Postgres exactly — in particular it never indexes
/// unclassified torrents that have no `torrent_contents` row (and would make
/// Tantivy a superset of PG results). Keyset pagination is on the generated text
/// PK `tc.id`, which is also the Tantivy `doc_id`. Integers are cast to
/// `bigint`, the JSONB `languages` column is flattened to `text[]`, and genres
/// come from a correlated `content_collections` (type `genre`) subquery.
const STREAM_FOR_INDEX_SQL: &str = "\
SELECT \
tc.id AS id, \
tc.info_hash AS info_hash, \
t.name AS torrent_name, \
t.files_status::text AS files_status, \
tc.content_type AS content_type, \
tc.content_source AS content_source, \
tc.content_id AS content_id, \
c.title AS content_title, \
c.original_title AS original_title, \
c.release_year::bigint AS release_year, \
tc.video_resolution AS video_resolution, \
tc.video_source AS video_source, \
tc.video_codec AS video_codec, \
tc.video_3d AS video_3d, \
tc.video_modifier AS video_modifier, \
tc.release_group AS release_group, \
tc.seeders::bigint AS seeders, \
tc.leechers::bigint AS leechers, \
t.size AS size, \
tc.files_count::bigint AS files_count, \
CAST(EXTRACT(EPOCH FROM COALESCE(tc.published_at, t.created_at)) AS bigint) AS published_at, \
ARRAY(SELECT jsonb_array_elements_text(tc.languages)) AS languages, \
ARRAY( \
SELECT cc.name FROM content_collections_content ccc \
JOIN content_collections cc \
ON cc.type = ccc.content_collection_type \
AND cc.source = ccc.content_collection_source \
AND cc.id = ccc.content_collection_id \
WHERE ccc.content_type = tc.content_type \
AND ccc.content_source = tc.content_source \
AND ccc.content_id = tc.content_id \
AND cc.type = 'genre' \
ORDER BY cc.name \
) AS genres, \
t.files_data AS files_data \
FROM torrent_contents tc \
JOIN torrents t ON t.info_hash = tc.info_hash \
LEFT JOIN content c \
ON c.type = tc.content_type \
AND c.source = tc.content_source \
AND c.id = tc.content_id \
WHERE ($1::text IS NULL OR tc.id > $1) \
ORDER BY tc.id ASC \
LIMIT $2";

/// Reads up to `limit` `torrent_contents` rows whose `id` is greater than
/// `after_id` (or from the start when `None`), ordered by `id` — one row per
/// search document. Pass the last returned [`TorrentForIndex::id`] back as
/// `after_id` for the next page. Uses the runtime [`sqlx::query`] API, so it
/// compiles without a live database.
pub async fn stream_torrents_for_index(
    pool: &PgPool,
    after_id: Option<&str>,
    limit: i64,
) -> Result<Vec<TorrentForIndex>> {
    let after = after_id.map(str::to_owned);

    let rows = sqlx::query(STREAM_FOR_INDEX_SQL)
        .bind(after)
        .bind(limit)
        .fetch_all(pool)
        .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let raw: Vec<u8> = row.try_get("info_hash")?;
        let info_hash =
            InfoHash::from_slice(&raw).map_err(|e| DbError::Decode(format!("info_hash: {e}")))?;
        out.push(TorrentForIndex {
            id: row.try_get("id")?,
            info_hash,
            torrent_name: row.try_get("torrent_name")?,
            files_status: row.try_get("files_status")?,
            content_type: row.try_get("content_type")?,
            content_source: row.try_get("content_source")?,
            content_id: row.try_get("content_id")?,
            content_title: row.try_get("content_title")?,
            original_title: row.try_get("original_title")?,
            release_year: row.try_get("release_year")?,
            video_resolution: row.try_get("video_resolution")?,
            video_source: row.try_get("video_source")?,
            video_codec: row.try_get("video_codec")?,
            video_3d: row.try_get("video_3d")?,
            video_modifier: row.try_get("video_modifier")?,
            release_group: row.try_get("release_group")?,
            seeders: row.try_get("seeders")?,
            leechers: row.try_get("leechers")?,
            size: row.try_get("size")?,
            files_count: row.try_get("files_count")?,
            published_at: row.try_get("published_at")?,
            languages: row.try_get("languages")?,
            genres: row.try_get("genres")?,
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
            published_at: 1_600_000_000,
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
            published_at: 1_600_000_000,
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
        // files_count is int4 in PG; sqlx needs the exact int8 OID for an i64
        // read (caught live by the GATE A smoke run, 2026-06-10).
        assert!(STREAM_SQL.contains("files_count::bigint"));
    }

    #[test]
    fn for_index_files_round_trip() {
        let original = vec![BlobFile {
            index: 0,
            path: "a.mkv".to_owned(),
            extension: "mkv".to_owned(),
            size: 10,
        }];
        let blob = serialize_files(&original).unwrap();
        let row = TorrentForIndex {
            id: "0123456789abcdef0123456789abcdef01234567:movie:tmdb:1".to_owned(),
            info_hash: "0123456789abcdef0123456789abcdef01234567".parse().unwrap(),
            torrent_name: "t".to_owned(),
            files_status: "multi".to_owned(),
            content_type: Some("movie".to_owned()),
            content_source: Some("tmdb".to_owned()),
            content_id: Some("1".to_owned()),
            content_title: Some("Title".to_owned()),
            original_title: None,
            release_year: Some(2020),
            video_resolution: Some("1080p".to_owned()),
            video_source: None,
            video_codec: None,
            video_3d: None,
            video_modifier: None,
            release_group: None,
            seeders: Some(5),
            leechers: Some(1),
            size: 10,
            files_count: Some(1),
            published_at: 1_600_000_000,
            languages: vec!["en".to_owned()],
            genres: vec!["Action".to_owned()],
            files_data: Some(blob),
        };
        assert_eq!(row.files().unwrap(), original);
    }

    #[test]
    fn changed_sql_shape() {
        // The delta carve: a BOUNDED half-open updated_at window
        // (since, until] + keyset on info_hash. The upper bound is the
        // commit-visibility-lagged "now" (export::CARVE_LAG_SECS) that the
        // caller also persists as the new watermark — an unbounded carve with
        // watermark=now would permanently skip rows whose transaction commits
        // between the query and now.
        assert!(STREAM_CHANGED_SQL.contains("FROM torrents"));
        assert!(STREAM_CHANGED_SQL.contains("updated_at > to_timestamp($1)"));
        assert!(STREAM_CHANGED_SQL.contains("updated_at <= to_timestamp($2)"));
        assert!(STREAM_CHANGED_SQL.contains("info_hash > $3"));
        assert!(STREAM_CHANGED_SQL.contains("ORDER BY info_hash ASC"));
        assert!(STREAM_CHANGED_SQL.contains("LIMIT $4"));
        assert!(STREAM_CHANGED_SQL.contains("files_data"));
        assert!(STREAM_CHANGED_SQL.contains("files_count::bigint"));
    }

    #[test]
    fn for_index_sql_shape() {
        // Drives from torrent_contents (one row = one search doc), joins
        // torrents + content, keysets on the composite PK tc.id.
        assert!(STREAM_FOR_INDEX_SQL.contains("FROM torrent_contents tc"));
        assert!(STREAM_FOR_INDEX_SQL.contains("t.files_status::text AS files_status"));
        assert!(STREAM_FOR_INDEX_SQL.contains("JOIN torrents t ON t.info_hash = tc.info_hash"));
        assert!(STREAM_FOR_INDEX_SQL.contains("LEFT JOIN content c"));
        assert!(STREAM_FOR_INDEX_SQL.contains("tc.id > $1"));
        assert!(STREAM_FOR_INDEX_SQL.contains("ORDER BY tc.id ASC"));
        assert!(STREAM_FOR_INDEX_SQL.contains("LIMIT $2"));
        // Genres via content_collections (type 'genre'); JSONB languages flattened.
        assert!(STREAM_FOR_INDEX_SQL.contains("content_collections_content ccc"));
        assert!(STREAM_FOR_INDEX_SQL.contains("cc.type = 'genre'"));
        assert!(STREAM_FOR_INDEX_SQL.contains("jsonb_array_elements_text(tc.languages)"));
    }
}
