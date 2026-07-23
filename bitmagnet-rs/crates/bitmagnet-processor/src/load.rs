//! Read-only hydration for the processor shadow.
//!
//! This loader intentionally stays inside the frozen shadow-role boundary:
//! `torrents` supplies the classifier image, `torrent_hints` supplies the
//! explicit hint, and `torrent_contents` supplies both stale IDs and the
//! reusable effective hint used by Go's default mode.

use std::collections::BTreeMap;

use bitmagnet_classifier::{ClassifierInput, InputFile, InputHint};
use bitmagnet_model::{deserialize_files_bounded, BlobError};
use bitmagnet_queue::{ProcessTorrentParams, CLASSIFY_MODE_REMATCH};
use futures::TryStreamExt;
use sqlx::{PgConnection, PgPool, Row};

use super::LoadedTorrent;

const MAX_COMPRESSED_OR_DECOMPRESSED_BYTES: usize = 64 * 1024 * 1024;
const MAX_FILES_PER_TORRENT: usize = 300_000;
const MAX_JOB_COMPRESSED_BYTES: usize = 64 * 1024 * 1024;
const MAX_JOB_DECODED_BYTES: usize = 128 * 1024 * 1024;
const MAX_JOB_FILES: usize = 300_000;

/// Hydrate the requested torrents without locking or mutating live rows.
///
/// Missing hashes are intentionally omitted. The materializer turns those
/// omissions into its `failed_info_hashes` image, matching the Go republish
/// boundary.
pub async fn load_torrents(
    pool: &PgPool,
    params: &ProcessTorrentParams,
) -> Result<Vec<LoadedTorrent>, LoadError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    let loaded = load_torrents_in(&mut tx, params).await?;
    tx.commit().await?;
    Ok(loaded)
}

pub(crate) async fn load_torrents_in(
    connection: &mut PgConnection,
    params: &ProcessTorrentParams,
) -> Result<Vec<LoadedTorrent>, LoadError> {
    let requested = params
        .info_hashes
        .iter()
        .map(|id| id.as_bytes().to_vec())
        .collect::<Vec<_>>();
    if requested.is_empty() {
        return Ok(Vec::new());
    }

    let content_rows = sqlx::query(
        "SELECT encode(info_hash, 'hex') AS info_hash, id, \
                content_type::text AS content_type, content_source, content_id \
         FROM torrent_contents \
         WHERE info_hash = ANY($1::bytea[]) \
         ORDER BY info_hash, id",
    )
    .bind(&requested)
    .fetch_all(&mut *connection)
    .await?;
    let mut current = BTreeMap::<String, Vec<CurrentContent>>::new();
    for row in content_rows {
        current
            .entry(row.try_get("info_hash")?)
            .or_default()
            .push(CurrentContent {
                id: row.try_get("id")?,
                content_type: row.try_get("content_type")?,
                content_source: row.try_get("content_source")?,
                content_id: row.try_get("content_id")?,
            });
    }

    let hint_rows = sqlx::query(
        "SELECT encode(info_hash, 'hex') AS info_hash, content_type, \
                content_source, content_id \
         FROM torrent_hints \
         WHERE info_hash = ANY($1::bytea[]) \
         ORDER BY info_hash",
    )
    .bind(&requested)
    .fetch_all(&mut *connection)
    .await?;
    let mut explicit_hints = BTreeMap::<String, CurrentHint>::new();
    for row in hint_rows {
        explicit_hints.insert(
            row.try_get("info_hash")?,
            CurrentHint {
                content_type: row.try_get("content_type")?,
                content_source: row.try_get("content_source")?,
                content_id: row.try_get("content_id")?,
            },
        );
    }

    let mut rows = sqlx::query(
        "SELECT encode(info_hash, 'hex') AS info_hash, name, size, \
                files_status::text AS files_status, extension, files_count, \
                octet_length(files_data)::bigint AS files_data_bytes, \
                CASE WHEN octet_length(files_data) <= $2 THEN files_data END AS files_data \
         FROM torrents \
         WHERE info_hash = ANY($1::bytea[]) \
         ORDER BY info_hash",
    )
    .bind(&requested)
    .bind(i64::try_from(MAX_COMPRESSED_OR_DECOMPRESSED_BYTES).expect("64 MiB fits i64"))
    .fetch(&mut *connection);

    let mut loaded = Vec::new();
    let mut job_compressed_bytes = 0_usize;
    let mut job_decoded_bytes = 0_usize;
    let mut job_files = 0_usize;
    while let Some(row) = rows.try_next().await? {
        let info_hash: String = row.try_get("info_hash")?;
        let size = nonnegative_u64("size", row.try_get("size")?)?;
        let files_count = row
            .try_get::<Option<i32>, _>("files_count")?
            .map(|value| nonnegative_u32("files_count", value))
            .transpose()?;
        let compressed_bytes = row
            .try_get::<Option<i64>, _>("files_data_bytes")?
            .map(|bytes| nonnegative_usize("files_data_bytes", bytes))
            .transpose()?
            .unwrap_or_default();
        if compressed_bytes > MAX_COMPRESSED_OR_DECOMPRESSED_BYTES {
            return Err(LoadError::CompressedBlobTooLarge {
                bytes: compressed_bytes,
                limit: MAX_COMPRESSED_OR_DECOMPRESSED_BYTES,
            });
        }
        job_compressed_bytes = job_compressed_bytes.saturating_add(compressed_bytes);
        enforce_job_budget(
            "compressed bytes",
            job_compressed_bytes,
            MAX_JOB_COMPRESSED_BYTES,
        )?;
        let mut files = match row.try_get::<Option<Vec<u8>>, _>("files_data")? {
            Some(blob) => {
                if blob.len() > MAX_COMPRESSED_OR_DECOMPRESSED_BYTES {
                    return Err(LoadError::CompressedBlobTooLarge {
                        bytes: blob.len(),
                        limit: MAX_COMPRESSED_OR_DECOMPRESSED_BYTES,
                    });
                }
                let decoded = deserialize_files_bounded(
                    &blob,
                    MAX_COMPRESSED_OR_DECOMPRESSED_BYTES,
                    MAX_FILES_PER_TORRENT,
                )?;
                job_decoded_bytes = job_decoded_bytes
                    .saturating_add(decoded.decompressed_bytes)
                    .saturating_add(decoded.owned_string_bytes);
                enforce_job_budget(
                    "decoded and retained string bytes",
                    job_decoded_bytes,
                    MAX_JOB_DECODED_BYTES,
                )?;
                job_files = job_files.saturating_add(decoded.files.len());
                enforce_job_budget("files", job_files, MAX_JOB_FILES)?;
                decoded.files
            }
            None => Vec::new(),
        };
        // Go's Torrent.AfterFind orders the hydrated blob image by path before
        // classification; retain the original file index within that ordering.
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let files = files
            .into_iter()
            .map(|file| InputFile {
                index: file.index,
                path: file.path,
                extension: file.extension,
                size: file.size,
            })
            .collect();

        let existing = current.remove(&info_hash).unwrap_or_default();
        let explicit = explicit_hints.remove(&info_hash);
        let allow_content_reuse = params.classify_mode != CLASSIFY_MODE_REMATCH;
        let attach_hint_unsupported = explicit.is_some()
            || (allow_content_reuse && existing.iter().any(CurrentContent::is_source_backed));
        let hint = effective_hint(explicit, &existing, allow_content_reuse);
        loaded.push(LoadedTorrent {
            info_hash: info_hash.clone(),
            classifier_input: ClassifierInput {
                id: info_hash,
                name: row.try_get("name")?,
                size,
                files_status: row.try_get("files_status")?,
                extension: row.try_get("extension")?,
                files_count,
                files,
                hint,
            },
            existing_content_ids: existing.into_iter().map(|content| content.id).collect(),
            attach_hint_unsupported,
        });
    }
    Ok(loaded)
}

struct CurrentHint {
    content_type: String,
    content_source: Option<String>,
    content_id: Option<String>,
}

struct CurrentContent {
    id: String,
    content_type: Option<String>,
    content_source: Option<String>,
    content_id: Option<String>,
}

impl CurrentContent {
    fn is_source_backed(&self) -> bool {
        self.content_type.is_some() && self.content_source.is_some()
    }

    fn as_reusable_hint(&self) -> Option<InputHint> {
        Some(InputHint {
            content_type: self.content_type.clone()?,
            content_source: self.content_source.clone()?,
            content_id: self.content_id.clone().unwrap_or_default(),
        })
    }
}

fn effective_hint(
    explicit: Option<CurrentHint>,
    existing: &[CurrentContent],
    allow_content_reuse: bool,
) -> Option<InputHint> {
    let explicit = explicit.and_then(|hint| {
        (!hint.content_type.is_empty()).then_some(InputHint {
            content_type: hint.content_type,
            content_source: hint.content_source.unwrap_or_default(),
            content_id: hint.content_id.unwrap_or_default(),
        })
    });
    if explicit
        .as_ref()
        .is_some_and(|hint| !hint.content_source.is_empty())
    {
        return explicit;
    }
    if allow_content_reuse {
        existing
            .iter()
            .filter(|content| {
                explicit.as_ref().is_none_or(|hint| {
                    content.content_type.as_deref() == Some(hint.content_type.as_str())
                })
            })
            .find_map(CurrentContent::as_reusable_hint)
            .or(explicit)
    } else {
        explicit
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("compressed files_data blob is {bytes} bytes, above the {limit}-byte shadow limit")]
    CompressedBlobTooLarge { bytes: usize, limit: usize },
    #[error("shadow job {resource} budget exceeded: {actual} > {limit}")]
    JobBudgetExceeded {
        resource: &'static str,
        actual: usize,
        limit: usize,
    },
    #[error("negative {field} value in live row: {value}")]
    NegativeInteger { field: &'static str, value: i64 },
}

fn nonnegative_u64(field: &'static str, value: i64) -> Result<u64, LoadError> {
    u64::try_from(value).map_err(|_| LoadError::NegativeInteger { field, value })
}

fn nonnegative_u32(field: &'static str, value: i32) -> Result<u32, LoadError> {
    u32::try_from(value).map_err(|_| LoadError::NegativeInteger {
        field,
        value: i64::from(value),
    })
}

fn nonnegative_usize(field: &'static str, value: i64) -> Result<usize, LoadError> {
    usize::try_from(value).map_err(|_| LoadError::NegativeInteger { field, value })
}

fn enforce_job_budget(
    resource: &'static str,
    actual: usize,
    limit: usize,
) -> Result<(), LoadError> {
    if actual > limit {
        Err(LoadError::JobBudgetExceeded {
            resource,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{effective_hint, CurrentContent, CurrentHint};

    #[test]
    fn reusable_hint_requires_type_and_source() {
        let complete = CurrentContent {
            id: "tc".into(),
            content_type: Some("movie".into()),
            content_source: Some("tmdb".into()),
            content_id: Some("603".into()),
        };
        let hint = complete.as_reusable_hint().expect("complete hint");
        assert_eq!(hint.content_type, "movie");
        assert_eq!(hint.content_source, "tmdb");
        assert_eq!(hint.content_id, "603");

        let incomplete = CurrentContent {
            content_source: None,
            ..complete
        };
        assert!(incomplete.as_reusable_hint().is_none());

        let empty_but_non_null = CurrentContent {
            id: "tc-empty".into(),
            content_type: Some("movie".into()),
            content_source: Some(String::new()),
            content_id: None,
        };
        assert!(empty_but_non_null.is_source_backed());
        assert_eq!(
            empty_but_non_null
                .as_reusable_hint()
                .expect("non-null empty source is valid in Go")
                .content_source,
            ""
        );
    }

    #[test]
    fn effective_hint_matches_go_precedence() {
        let existing = vec![CurrentContent {
            id: "tc".into(),
            content_type: Some("movie".into()),
            content_source: Some("tmdb".into()),
            content_id: Some("603".into()),
        }];
        let explicit_source = CurrentHint {
            content_type: "movie".into(),
            content_source: Some("imdb".into()),
            content_id: Some("tt0133093".into()),
        };
        let hint = effective_hint(Some(explicit_source), &existing, true).unwrap();
        assert_eq!(hint.content_source, "imdb");

        let type_only = CurrentHint {
            content_type: "movie".into(),
            content_source: None,
            content_id: None,
        };
        let hint = effective_hint(Some(type_only), &existing, true).unwrap();
        assert_eq!(hint.content_source, "tmdb");
        assert_eq!(hint.content_id, "603");

        let mismatched_type = CurrentHint {
            content_type: "tv_show".into(),
            content_source: None,
            content_id: None,
        };
        let hint = effective_hint(Some(mismatched_type), &existing, true).unwrap();
        assert_eq!(hint.content_type, "tv_show");
        assert!(hint.content_source.is_empty());
    }

    #[test]
    fn rematch_preserves_explicit_hint_but_skips_content_reuse() {
        let existing = vec![CurrentContent {
            id: "tc".into(),
            content_type: Some("movie".into()),
            content_source: Some("tmdb".into()),
            content_id: Some("603".into()),
        }];
        let explicit = CurrentHint {
            content_type: "tv_show".into(),
            content_source: None,
            content_id: None,
        };

        let hint = effective_hint(Some(explicit), &existing, false).expect("explicit hint");
        assert_eq!(hint.content_type, "tv_show");
        assert!(hint.content_source.is_empty());
        assert!(effective_hint(None, &existing, false).is_none());
    }
}
