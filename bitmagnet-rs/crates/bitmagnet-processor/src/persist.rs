//! PostgreSQL persistence for the currently supported processor write image.
//!
//! Go persists attached `content` rows, stale-content deletes, classified
//! `torrent_contents`, tags, and whole-torrent deletes in that order. The
//! current writer image does not carry the base content TSV and associations,
//! and its stable comparison image deliberately excludes volatile source
//! snapshots and the FTS vector. This module therefore refuses attached-content
//! writes and requires those omitted volatile values explicitly before it opens
//! a transaction.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;

use futures::future::BoxFuture;
use serde_json::{Map, Value};
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::{infer_id, validate_info_hash, TorrentContentWrite, WriteSet};

const BATCH_SIZE: usize = 100;

/// Error type accepted from a blocking-manager implementation.
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// The pre-transaction delete blocklist boundary.
///
/// Go calls `blockingManager.Block(..., false)` before beginning the persistence
/// transaction. Implementations may buffer the hashes or flush their backing
/// bloom filter, but must return only after the block operation has succeeded.
pub trait BlockingManager: Send + Sync {
    fn block<'a>(&'a self, info_hashes: &'a [String]) -> BoxFuture<'a, Result<(), BoxError>>;
}

/// Source-derived values intentionally excluded from [`TorrentContentWrite`].
///
/// One entry, keyed by the generated `torrent_contents.id`, is required for
/// every row passed to [`persist_write_set`]. `published_at_micros` is measured
/// from the Unix epoch so PostgreSQL receives the timestamp without a lossy
/// floating-point conversion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TorrentContentPersistence {
    pub seeders: Option<u64>,
    pub leechers: Option<u64>,
    pub published_at_micros: i64,
    pub tsv: String,
}

/// Persist the supported portion of a materialized write-set in one transaction.
///
/// The transaction order mirrors `internal/processor/persist.go`: delete stale
/// `torrent_contents`, upsert the classified rows in batches of 100, insert tags
/// with `ON CONFLICT DO NOTHING`, then delete whole torrents. Attached content
/// is rejected until the writer projection carries the base content TSV and
/// the kernel owns its association rows.
pub async fn persist_write_set<B>(
    pool: &PgPool,
    write_set: &WriteSet,
    persistence: &BTreeMap<String, TorrentContentPersistence>,
    blocking_manager: &B,
) -> Result<(), PersistError>
where
    B: BlockingManager + ?Sized,
{
    let prepared = PreparedWriteSet::new(write_set, persistence)?;

    if !write_set.delete_info_hashes.is_empty() {
        blocking_manager
            .block(&write_set.delete_info_hashes)
            .await
            .map_err(PersistError::Blocking)?;
    }

    if prepared.is_empty() && write_set.delete_ids.is_empty() {
        return Ok(());
    }

    let mut tx = pool.begin().await?;

    if !write_set.delete_ids.is_empty() {
        sqlx::query("DELETE FROM torrent_contents WHERE id = ANY($1::text[])")
            .bind(&write_set.delete_ids)
            .execute(&mut *tx)
            .await?;
    }

    for chunk in prepared.torrent_contents.chunks(BATCH_SIZE) {
        upsert_torrent_contents(&mut tx, chunk).await?;
    }

    for chunk in prepared.tags.chunks(BATCH_SIZE) {
        insert_tags(&mut tx, chunk).await?;
    }

    if !prepared.delete_info_hashes.is_empty() {
        sqlx::query("DELETE FROM torrents WHERE info_hash = ANY($1::bytea[])")
            .bind(&prepared.delete_info_hashes)
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;
    Ok(())
}

/// Run every pure persistence-input check without blocking or touching the DB.
pub(crate) fn validate_persistence_input(
    write_set: &WriteSet,
    persistence: &BTreeMap<String, TorrentContentPersistence>,
) -> Result<(), PersistError> {
    PreparedWriteSet::new(write_set, persistence).map(drop)
}

#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("attached content persistence is not supported by the current writer image")]
    AttachedContentUnsupported,
    #[error("missing persistence metadata for torrent_content '{0}'")]
    MissingPersistenceMetadata(String),
    #[error("unexpected persistence metadata for torrent_content '{0}'")]
    UnexpectedPersistenceMetadata(String),
    #[error("torrent_content '{id}' has an invalid generated ID; expected '{expected}'")]
    InvalidGeneratedId { id: String, expected: String },
    #[error("invalid episodes image '{0}'")]
    InvalidEpisodes(String),
    #[error("invalid tag name '{0}'")]
    InvalidTagName(String),
    #[error("{field} value {value} is outside PostgreSQL {postgres_type}")]
    IntegerOutOfRange {
        field: &'static str,
        value: u64,
        postgres_type: &'static str,
    },
    #[error("blocking whole-torrent deletes failed")]
    Blocking(#[source] BoxError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    InvalidInfoHash(#[from] super::MaterializeError),
}

struct PreparedWriteSet {
    torrent_contents: Vec<PreparedTorrentContent>,
    tags: Vec<PreparedTag>,
    delete_info_hashes: Vec<Vec<u8>>,
}

impl PreparedWriteSet {
    fn new(
        write_set: &WriteSet,
        persistence: &BTreeMap<String, TorrentContentPersistence>,
    ) -> Result<Self, PersistError> {
        if !write_set.contents.is_empty() {
            return Err(PersistError::AttachedContentUnsupported);
        }

        let expected_ids = write_set
            .torrent_contents
            .iter()
            .map(|tc| tc.id.as_str())
            .collect::<BTreeSet<_>>();
        if let Some(unexpected) = persistence
            .keys()
            .find(|id| !expected_ids.contains(id.as_str()))
        {
            return Err(PersistError::UnexpectedPersistenceMetadata(
                unexpected.clone(),
            ));
        }

        let torrent_contents = write_set
            .torrent_contents
            .iter()
            .map(|tc| {
                let metadata = persistence
                    .get(&tc.id)
                    .ok_or_else(|| PersistError::MissingPersistenceMetadata(tc.id.clone()))?;
                PreparedTorrentContent::new(tc, metadata)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut tags = Vec::new();
        for (info_hash, names) in &write_set.add_tags {
            let info_hash = decode_info_hash(info_hash)?;
            for name in names {
                validate_tag_name(name)?;
                tags.push(PreparedTag {
                    info_hash: info_hash.clone(),
                    name: name.clone(),
                });
            }
        }

        let delete_info_hashes = write_set
            .delete_info_hashes
            .iter()
            .map(|info_hash| decode_info_hash(info_hash))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            torrent_contents,
            tags,
            delete_info_hashes,
        })
    }

    fn is_empty(&self) -> bool {
        self.torrent_contents.is_empty()
            && self.tags.is_empty()
            && self.delete_info_hashes.is_empty()
    }
}

struct PreparedTorrentContent {
    info_hash: Vec<u8>,
    content_type: Option<String>,
    content_source: Option<String>,
    content_id: Option<String>,
    languages: Value,
    episodes: Value,
    video_resolution: Option<String>,
    video_source: Option<String>,
    video_codec: Option<String>,
    video_3d: Option<String>,
    video_modifier: Option<String>,
    release_group: Option<String>,
    tsv: String,
    seeders: Option<i32>,
    leechers: Option<i32>,
    published_at_micros: i64,
    size: i64,
    files_count: Option<i32>,
}

impl PreparedTorrentContent {
    fn new(
        tc: &TorrentContentWrite,
        metadata: &TorrentContentPersistence,
    ) -> Result<Self, PersistError> {
        let expected = infer_id(
            &tc.info_hash,
            tc.content_type.as_deref(),
            tc.content_source.as_deref(),
            tc.content_id.as_deref(),
        );
        if tc.id != expected {
            return Err(PersistError::InvalidGeneratedId {
                id: tc.id.clone(),
                expected,
            });
        }

        Ok(Self {
            info_hash: decode_info_hash(&tc.info_hash)?,
            content_type: tc.content_type.clone(),
            content_source: tc.content_source.clone(),
            content_id: tc.content_id.clone(),
            languages: serde_json::to_value(&tc.languages)
                .expect("serializing Vec<String> cannot fail"),
            episodes: episodes_json(&tc.episodes)?,
            video_resolution: tc.video_resolution.clone(),
            video_source: tc.video_source.clone(),
            video_codec: tc.video_codec.clone(),
            video_3d: tc.video_3d.clone(),
            video_modifier: tc.video_modifier.clone(),
            release_group: tc.release_group.clone(),
            tsv: metadata.tsv.clone(),
            seeders: optional_i32("seeders", metadata.seeders)?,
            leechers: optional_i32("leechers", metadata.leechers)?,
            published_at_micros: metadata.published_at_micros,
            size: i64::try_from(tc.size).map_err(|_| PersistError::IntegerOutOfRange {
                field: "size",
                value: tc.size,
                postgres_type: "bigint",
            })?,
            files_count: optional_i32("files_count", tc.files_count)?,
        })
    }
}

struct PreparedTag {
    info_hash: Vec<u8>,
    name: String,
}

fn decode_info_hash(value: &str) -> Result<Vec<u8>, PersistError> {
    validate_info_hash(value)?;
    Ok(hex::decode(value).expect("validated hexadecimal info hash"))
}

fn optional_i32(field: &'static str, value: Option<u64>) -> Result<Option<i32>, PersistError> {
    value
        .map(|value| {
            i32::try_from(value).map_err(|_| PersistError::IntegerOutOfRange {
                field,
                value,
                postgres_type: "integer",
            })
        })
        .transpose()
}

fn validate_tag_name(name: &str) -> Result<(), PersistError> {
    let valid = !name.is_empty()
        && name.len() <= 30
        && name.split('-').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        });
    if valid {
        Ok(())
    } else {
        Err(PersistError::InvalidTagName(name.to_owned()))
    }
}

async fn upsert_torrent_contents(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    rows: &[PreparedTorrentContent],
) -> Result<(), sqlx::Error> {
    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO torrent_contents (\
         info_hash, content_type, content_source, content_id, languages, episodes, \
         video_resolution, video_source, video_codec, video_3d, video_modifier, \
         release_group, created_at, updated_at, tsv, seeders, leechers, \
         published_at, size, files_count) ",
    );
    query.push_values(rows, |mut row, tc| {
        row.push_bind(&tc.info_hash)
            .push_bind(&tc.content_type)
            .push_bind(&tc.content_source)
            .push_bind(&tc.content_id)
            .push_bind(sqlx::types::Json(&tc.languages))
            .push_bind(sqlx::types::Json(&tc.episodes))
            .push_bind(&tc.video_resolution)
            .push_bind(&tc.video_source)
            .push_bind(&tc.video_codec)
            .push_bind(&tc.video_3d)
            .push_bind(&tc.video_modifier)
            .push_bind(&tc.release_group)
            .push("NOW()")
            .push("NOW()")
            .push_bind(&tc.tsv)
            .push_unseparated("::tsvector")
            .push_bind(tc.seeders)
            .push_bind(tc.leechers)
            .push_bind(tc.published_at_micros)
            .push_unseparated(" * INTERVAL '1 microsecond' + TIMESTAMPTZ 'epoch'")
            .push_bind(tc.size)
            .push_bind(tc.files_count);
    });
    query.push(
        " ON CONFLICT (id) DO UPDATE SET \
         content_type = EXCLUDED.content_type, \
         content_source = EXCLUDED.content_source, \
         content_id = EXCLUDED.content_id, \
         languages = EXCLUDED.languages, \
         episodes = EXCLUDED.episodes, \
         video_resolution = EXCLUDED.video_resolution, \
         video_source = EXCLUDED.video_source, \
         video_codec = EXCLUDED.video_codec, \
         video_3d = EXCLUDED.video_3d, \
         video_modifier = EXCLUDED.video_modifier, \
         release_group = EXCLUDED.release_group, \
         updated_at = EXCLUDED.updated_at, \
         tsv = EXCLUDED.tsv, \
         seeders = EXCLUDED.seeders, \
         leechers = EXCLUDED.leechers, \
         published_at = EXCLUDED.published_at, \
         size = EXCLUDED.size, \
         files_count = EXCLUDED.files_count",
    );
    query.build().execute(&mut **tx).await?;
    Ok(())
}

async fn insert_tags(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tags: &[PreparedTag],
) -> Result<(), sqlx::Error> {
    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO torrent_tags (info_hash, name, created_at, updated_at) ",
    );
    query.push_values(tags, |mut row, tag| {
        row.push_bind(&tag.info_hash)
            .push_bind(&tag.name)
            .push("NOW()")
            .push("NOW()");
    });
    query.push(" ON CONFLICT DO NOTHING");
    query.build().execute(&mut **tx).await?;
    Ok(())
}

fn episodes_json(value: &str) -> Result<Value, PersistError> {
    let mut seasons = Map::new();
    if value.is_empty() {
        return Ok(Value::Object(seasons));
    }

    for season_part in value.split(", ") {
        let raw = season_part
            .strip_prefix('S')
            .ok_or_else(|| PersistError::InvalidEpisodes(value.to_owned()))?;

        if let Some((season, episodes)) = raw.split_once('E') {
            let season = parse_number(season, value)?;
            let mut episode_map = Map::new();
            for episode_part in episodes.split(',') {
                let raw_episode = episode_part.strip_prefix('E').unwrap_or(episode_part);
                for episode in parse_range(raw_episode, value)? {
                    episode_map.insert(episode.to_string(), Value::Object(Map::new()));
                }
            }
            seasons.insert(season.to_string(), Value::Object(episode_map));
        } else {
            for season in parse_range(raw, value)? {
                seasons.insert(season.to_string(), Value::Object(Map::new()));
            }
        }
    }

    Ok(Value::Object(seasons))
}

fn parse_range(raw: &str, image: &str) -> Result<std::ops::RangeInclusive<u16>, PersistError> {
    let (start, end) = match raw.split_once('-') {
        Some((start, end)) => (parse_number(start, image)?, parse_number(end, image)?),
        None => {
            let value = parse_number(raw, image)?;
            (value, value)
        }
    };
    if start > end {
        return Err(PersistError::InvalidEpisodes(image.to_owned()));
    }
    Ok(start..=end)
}

fn parse_number(raw: &str, image: &str) -> Result<u16, PersistError> {
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(PersistError::InvalidEpisodes(image.to_owned()));
    }
    raw.parse()
        .map_err(|_| PersistError::InvalidEpisodes(image.to_owned()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{episodes_json, validate_tag_name, PreparedWriteSet};
    use crate::WriteSet;

    #[test]
    fn delete_only_write_set_is_not_treated_as_empty() {
        let write_set = WriteSet {
            delete_info_hashes: vec!["1111111111111111111111111111111111111111".to_owned()],
            ..WriteSet::default()
        };

        let prepared = PreparedWriteSet::new(&write_set, &BTreeMap::new()).unwrap();

        assert!(!prepared.is_empty());
    }

    #[test]
    fn episodes_string_converts_to_go_json_shape() {
        assert_eq!(episodes_json("").unwrap(), json!({}));
        assert_eq!(episodes_json("S01").unwrap(), json!({"1": {}}));
        assert_eq!(
            episodes_json("S01-03").unwrap(),
            json!({"1": {}, "2": {}, "3": {}})
        );
        assert_eq!(
            episodes_json("S01E01-03,E05, S02E07").unwrap(),
            json!({
                "1": {"1": {}, "2": {}, "3": {}, "5": {}},
                "2": {"7": {}}
            })
        );
    }

    #[test]
    fn tag_validation_matches_go_model_hook() {
        for valid in ["trusted", "tracker-42", "a"] {
            validate_tag_name(valid).unwrap();
        }
        for invalid in [
            "",
            "INVALID",
            "-leading",
            "trailing-",
            "double--dash",
            "this-tag-name-is-over-thirty-bytes",
        ] {
            assert!(validate_tag_name(invalid).is_err(), "{invalid}");
        }
    }
}
