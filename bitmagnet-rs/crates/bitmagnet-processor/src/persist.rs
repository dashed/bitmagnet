//! PostgreSQL persistence for the currently supported processor write image.
//!
//! Go persists attached `content` rows, stale-content deletes, classified
//! `torrent_contents`, tags, and whole-torrent deletes in that order. The
//! complete attached-content persistence image is carried separately from the
//! stable comparison write-set, which deliberately excludes volatile source
//! snapshots, associations, and FTS vectors. Exact keyset validation happens
//! before blocking or opening a transaction.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;

use bitmagnet_fts::Tsvector;
use futures::future::BoxFuture;
use serde_json::{Map, Value};
use sqlx::{PgPool, Postgres, QueryBuilder};

use bitmagnet_model::Content;

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

/// Stable primary key for one attached `content` persistence image.
///
/// This key deliberately uses canonical strings instead of changing the
/// comparison-facing [`super::ContentWrite`] or requiring ordering traits on
/// the shared model enum.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ContentPersistenceKey {
    pub content_type: String,
    pub source: String,
    pub id: String,
}

/// Full attached content carried beside, but never serialized into, a
/// comparison [`WriteSet`].
///
/// Go rebuilds `Content.Tsv` for both new TMDB content and reused database
/// content. Only a row whose `CreatedAt` is zero is sent to GORM's content
/// upsert, while both cases provide the base TSV for `TorrentContent.UpdateTsv`.
#[derive(Clone, Debug, PartialEq)]
pub struct ContentPersistence {
    key: ContentPersistenceKey,
    base_tsv: Tsvector,
    upsert: Option<Content>,
}

impl ContentPersistence {
    #[must_use]
    pub fn from_content(content: &Content) -> Self {
        let should_upsert = content.created_at.is_none();
        let mut content = content.clone();
        content.update_tsv();
        let key = ContentPersistenceKey {
            content_type: content.content_type.as_str().to_owned(),
            source: content.source.clone(),
            id: content.id.clone(),
        };
        Self {
            key,
            base_tsv: content.tsv.clone(),
            upsert: should_upsert.then_some(content),
        }
    }

    #[must_use]
    pub fn key(&self) -> ContentPersistenceKey {
        self.key.clone()
    }

    #[must_use]
    pub fn base_tsv(&self) -> &Tsvector {
        &self.base_tsv
    }

    #[must_use]
    pub fn upsert(&self) -> Option<&Content> {
        self.upsert.as_ref()
    }
}

/// Persist the supported portion of a materialized write-set in one transaction.
///
/// The transaction order mirrors `internal/processor/persist.go`: upsert new
/// attached content and its additive associations in batches of 100, delete
/// stale `torrent_contents`, upsert the classified rows, insert tags with
/// `ON CONFLICT DO NOTHING`, then delete whole torrents.
pub async fn persist_write_set<B>(
    pool: &PgPool,
    write_set: &WriteSet,
    persistence: &BTreeMap<String, TorrentContentPersistence>,
    content_persistence: &BTreeMap<ContentPersistenceKey, ContentPersistence>,
    blocking_manager: &B,
) -> Result<(), PersistError>
where
    B: BlockingManager + ?Sized,
{
    let prepared = PreparedWriteSet::new(write_set, persistence, content_persistence)?;

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

    for chunk in prepared.contents.chunks(BATCH_SIZE) {
        upsert_content_batch(&mut tx, chunk).await?;
    }

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
    content_persistence: &BTreeMap<ContentPersistenceKey, ContentPersistence>,
) -> Result<(), PersistError> {
    PreparedWriteSet::new(write_set, persistence, content_persistence).map(drop)
}

#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error(
        "missing persistence metadata for attached content '{content_type}:{content_source}:{id}'"
    )]
    MissingContentPersistence {
        content_type: String,
        content_source: String,
        id: String,
    },
    #[error(
        "unexpected persistence metadata for attached content '{content_type}:{content_source}:{id}'"
    )]
    UnexpectedContentPersistence {
        content_type: String,
        content_source: String,
        id: String,
    },
    #[error("attached persistence map key '{map_key}' does not match image key '{image_key}'")]
    ContentPersistenceKeyMismatch { map_key: String, image_key: String },
    #[error("stable content write-set identities do not match attached persistence identities")]
    ContentIdentityMismatch,
    #[error("torrent_content has a partial attached content reference")]
    PartialContentReference,
    #[error("missing persistence metadata for torrent_content '{0}'")]
    MissingPersistenceMetadata(String),
    #[error("unexpected persistence metadata for torrent_content '{0}'")]
    UnexpectedPersistenceMetadata(String),
    #[error("torrent_content '{id}' has an invalid generated ID; expected '{expected}'")]
    InvalidGeneratedId { id: String, expected: String },
    #[error("invalid episodes image '{0}'")]
    InvalidEpisodes(String),
    #[error("invalid content release date '{0}'")]
    InvalidContentDate(String),
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

fn attached_content_key(
    row: &TorrentContentWrite,
) -> Result<Option<ContentPersistenceKey>, PersistError> {
    match (&row.content_type, &row.content_source, &row.content_id) {
        (_, None, None) => Ok(None),
        (Some(content_type), Some(source), Some(id)) => Ok(Some(ContentPersistenceKey {
            content_type: content_type.clone(),
            source: source.clone(),
            id: id.clone(),
        })),
        _ => Err(PersistError::PartialContentReference),
    }
}

struct PreparedWriteSet {
    contents: Vec<PreparedContent>,
    torrent_contents: Vec<PreparedTorrentContent>,
    tags: Vec<PreparedTag>,
    delete_info_hashes: Vec<Vec<u8>>,
}

#[derive(Clone)]
struct PreparedContent {
    content_type: String,
    source: String,
    id: String,
    title: String,
    release_date: Option<String>,
    release_year: Option<i32>,
    adult: Option<bool>,
    original_language: Option<String>,
    original_title: Option<String>,
    overview: Option<String>,
    runtime: Option<i32>,
    popularity: Option<f32>,
    vote_average: Option<f32>,
    vote_count: Option<i64>,
    tsv: String,
    attributes: Vec<PreparedContentAttribute>,
    collections: Vec<PreparedContentCollection>,
}

impl PreparedContent {
    fn new(content: &Content) -> Result<Self, PersistError> {
        let content_type = content.content_type.as_str().to_owned();
        let source = content.source.clone();
        let id = content.id.clone();
        let attributes = content
            .attributes
            .iter()
            .map(|attribute| PreparedContentAttribute {
                content_type: content_type.clone(),
                content_source: source.clone(),
                content_id: id.clone(),
                source: attribute.source.clone(),
                key: attribute.key.clone(),
                value: attribute.value.clone(),
            })
            .collect();
        let collections = content
            .collections
            .iter()
            .map(|collection| PreparedContentCollection {
                collection_type: collection.collection_type.clone(),
                source: collection.source.clone(),
                id: collection.id.clone(),
                name: collection.name.clone(),
            })
            .collect();

        Ok(Self {
            content_type,
            source,
            id,
            title: content.title.clone(),
            release_date: content.release_date.map(format_content_date).transpose()?,
            release_year: optional_i32("release_year", content.release_year.map(u64::from))?,
            adult: content.adult,
            original_language: content.original_language.clone(),
            original_title: content.original_title.clone(),
            overview: content.overview.clone(),
            runtime: optional_i32("runtime", content.runtime.map(u64::from))?,
            popularity: content.popularity,
            vote_average: content.vote_average,
            vote_count: content.vote_count.map(i64::from),
            tsv: content.tsv.to_string(),
            attributes,
            collections,
        })
    }
}

#[derive(Clone)]
struct PreparedContentAttribute {
    content_type: String,
    content_source: String,
    content_id: String,
    source: String,
    key: String,
    value: String,
}

#[derive(Clone)]
struct PreparedContentCollection {
    collection_type: String,
    source: String,
    id: String,
    name: String,
}

fn format_content_date(date: bitmagnet_model::Date) -> Result<String, PersistError> {
    if date.is_nil() {
        return Err(PersistError::InvalidContentDate("0000-00-00".to_owned()));
    }
    let days = match date.month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if date.year.is_multiple_of(400)
            || (date.year.is_multiple_of(4) && !date.year.is_multiple_of(100)) =>
        {
            29
        }
        2 => 28,
        _ => 0,
    };
    if date.day == 0 || date.day > days || date.year == 0 {
        return Err(PersistError::InvalidContentDate(format!(
            "{:04}-{:02}-{:02}",
            date.year, date.month, date.day
        )));
    }
    Ok(format!(
        "{:04}-{:02}-{:02}",
        date.year, date.month, date.day
    ))
}

impl PreparedWriteSet {
    fn new(
        write_set: &WriteSet,
        persistence: &BTreeMap<String, TorrentContentPersistence>,
        content_persistence: &BTreeMap<ContentPersistenceKey, ContentPersistence>,
    ) -> Result<Self, PersistError> {
        if let Some((map_key, image_key)) = content_persistence.iter().find_map(|(key, image)| {
            let image_key = image.key();
            (key != &image_key).then_some((key, image_key))
        }) {
            return Err(PersistError::ContentPersistenceKeyMismatch {
                map_key: format!("{}:{}:{}", map_key.content_type, map_key.source, map_key.id),
                image_key: format!(
                    "{}:{}:{}",
                    image_key.content_type, image_key.source, image_key.id
                ),
            });
        }
        let expected_content_keys = write_set
            .torrent_contents
            .iter()
            .map(attached_content_key)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<BTreeSet<_>>();
        if let Some(missing) = expected_content_keys
            .iter()
            .find(|key| !content_persistence.contains_key(*key))
        {
            return Err(PersistError::MissingContentPersistence {
                content_type: missing.content_type.clone(),
                content_source: missing.source.clone(),
                id: missing.id.clone(),
            });
        }
        if let Some(unexpected) = content_persistence
            .keys()
            .find(|key| !expected_content_keys.contains(*key))
        {
            return Err(PersistError::UnexpectedContentPersistence {
                content_type: unexpected.content_type.clone(),
                content_source: unexpected.source.clone(),
                id: unexpected.id.clone(),
            });
        }
        let stable_content_keys = write_set
            .contents
            .iter()
            .map(|content| ContentPersistenceKey {
                content_type: content.content_type.clone(),
                source: content.source.clone(),
                id: content.id.clone(),
            })
            .collect::<BTreeSet<_>>();
        if stable_content_keys.len() != write_set.contents.len()
            || stable_content_keys != expected_content_keys
        {
            return Err(PersistError::ContentIdentityMismatch);
        }

        let contents = content_persistence
            .values()
            .filter_map(ContentPersistence::upsert)
            .map(PreparedContent::new)
            .collect::<Result<Vec<_>, _>>()?;

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
            contents,
            torrent_contents,
            tags,
            delete_info_hashes,
        })
    }

    fn is_empty(&self) -> bool {
        self.contents.is_empty()
            && self.torrent_contents.is_empty()
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

async fn upsert_content_batch(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    contents: &[PreparedContent],
) -> Result<(), sqlx::Error> {
    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO content (\
         type, source, id, title, release_date, release_year, adult, original_language, \
         original_title, overview, runtime, popularity, vote_average, vote_count, \
         created_at, updated_at, tsv) ",
    );
    query.push_values(contents, |mut row, content| {
        row.push_bind(&content.content_type)
            .push_bind(&content.source)
            .push_bind(&content.id)
            .push_bind(&content.title)
            .push_bind(&content.release_date)
            .push_unseparated("::date")
            .push_bind(content.release_year)
            .push_bind(content.adult)
            .push_bind(&content.original_language)
            .push_bind(&content.original_title)
            .push_bind(&content.overview)
            .push_bind(content.runtime)
            .push_bind(content.popularity)
            .push_bind(content.vote_average)
            .push_bind(content.vote_count)
            .push("NOW()")
            .push("NOW()")
            .push_bind(&content.tsv)
            .push_unseparated("::tsvector");
    });
    query.push(
        " ON CONFLICT (type, source, id) DO UPDATE SET \
         title = EXCLUDED.title, \
         release_date = EXCLUDED.release_date, \
         release_year = EXCLUDED.release_year, \
         adult = EXCLUDED.adult, \
         original_language = EXCLUDED.original_language, \
         original_title = EXCLUDED.original_title, \
         overview = EXCLUDED.overview, \
         runtime = EXCLUDED.runtime, \
         popularity = EXCLUDED.popularity, \
         vote_average = EXCLUDED.vote_average, \
         vote_count = EXCLUDED.vote_count, \
         updated_at = EXCLUDED.updated_at, \
         tsv = EXCLUDED.tsv",
    );
    query.build().execute(&mut **tx).await?;

    let mut attributes = BTreeMap::new();
    let mut collections = BTreeMap::new();
    let mut links = BTreeSet::new();
    for content in contents {
        for attribute in &content.attributes {
            attributes
                .entry((
                    attribute.content_type.clone(),
                    attribute.content_source.clone(),
                    attribute.content_id.clone(),
                    attribute.source.clone(),
                    attribute.key.clone(),
                ))
                .or_insert_with(|| attribute.clone());
        }
        for collection in &content.collections {
            collections
                .entry((
                    collection.collection_type.clone(),
                    collection.source.clone(),
                    collection.id.clone(),
                ))
                .or_insert_with(|| collection.clone());
            links.insert((
                content.content_type.clone(),
                content.source.clone(),
                content.id.clone(),
                collection.collection_type.clone(),
                collection.source.clone(),
                collection.id.clone(),
            ));
        }
    }

    let attributes = attributes.into_values().collect::<Vec<_>>();
    for chunk in attributes.chunks(BATCH_SIZE) {
        insert_content_attributes(tx, chunk).await?;
    }
    let collections = collections.into_values().collect::<Vec<_>>();
    for chunk in collections.chunks(BATCH_SIZE) {
        insert_content_collections(tx, chunk).await?;
    }
    let links = links.into_iter().collect::<Vec<_>>();
    for chunk in links.chunks(BATCH_SIZE) {
        insert_content_collection_links(tx, chunk).await?;
    }
    Ok(())
}

async fn insert_content_attributes(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    attributes: &[PreparedContentAttribute],
) -> Result<(), sqlx::Error> {
    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO content_attributes (\
         content_type, content_source, content_id, source, key, value, created_at, updated_at) ",
    );
    query.push_values(attributes, |mut row, attribute| {
        row.push_bind(&attribute.content_type)
            .push_bind(&attribute.content_source)
            .push_bind(&attribute.content_id)
            .push_bind(&attribute.source)
            .push_bind(&attribute.key)
            .push_bind(&attribute.value)
            .push("NOW()")
            .push("NOW()");
    });
    // GORM updates only the parent foreign-key columns for this has-many
    // association. They are also conflict-key columns, so DO NOTHING is the
    // same observable operation and deliberately preserves an existing value.
    query.push(" ON CONFLICT DO NOTHING");
    query.build().execute(&mut **tx).await?;
    Ok(())
}

async fn insert_content_collections(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    collections: &[PreparedContentCollection],
) -> Result<(), sqlx::Error> {
    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO content_collections (type, source, id, name, created_at, updated_at) ",
    );
    query.push_values(collections, |mut row, collection| {
        row.push_bind(&collection.collection_type)
            .push_bind(&collection.source)
            .push_bind(&collection.id)
            .push_bind(&collection.name)
            .push("NOW()")
            .push("NOW()");
    });
    query.push(" ON CONFLICT DO NOTHING");
    query.build().execute(&mut **tx).await?;
    Ok(())
}

type PreparedContentCollectionLink = (String, String, String, String, String, String);

async fn insert_content_collection_links(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    links: &[PreparedContentCollectionLink],
) -> Result<(), sqlx::Error> {
    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO content_collections_content (\
         content_type, content_source, content_id, content_collection_type, \
         content_collection_source, content_collection_id) ",
    );
    query.push_values(links, |mut row, link| {
        row.push_bind(&link.0)
            .push_bind(&link.1)
            .push_bind(&link.2)
            .push_bind(&link.3)
            .push_bind(&link.4)
            .push_bind(&link.5);
    });
    query.push(" ON CONFLICT DO NOTHING");
    query.build().execute(&mut **tx).await?;
    Ok(())
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
    // Go/GORM's UpdateAll omits PublishedAt because the generated model field
    // carries a database-default tag. Keep the projected value insert-only.
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

    use bitmagnet_model::{Content, ContentAttribute, ContentCollection, ContentType};
    use serde_json::json;

    use super::{
        episodes_json, validate_tag_name, ContentPersistence, PersistError, PreparedWriteSet,
    };
    use crate::{ContentWrite, TorrentContentPersistence, TorrentContentWrite, WriteSet};

    const INFO_HASH: &str = "1111111111111111111111111111111111111111";

    fn content(created_at: Option<i64>) -> Content {
        Content {
            content_type: ContentType::Movie,
            source: "tmdb".to_owned(),
            id: "42".to_owned(),
            title: "Attached Title".to_owned(),
            release_date: None,
            release_year: Some(2026),
            adult: Some(false),
            original_language: Some("en".to_owned()),
            original_title: None,
            overview: None,
            runtime: Some(90),
            popularity: Some(1.5),
            vote_average: Some(7.0),
            vote_count: Some(10),
            created_at,
            updated_at: None,
            tsv: Default::default(),
            collections: vec![ContentCollection {
                collection_type: "genre".to_owned(),
                source: "tmdb".to_owned(),
                id: "7".to_owned(),
                name: "Mystery".to_owned(),
            }],
            attributes: vec![ContentAttribute {
                content_type: ContentType::Movie,
                content_source: "tmdb".to_owned(),
                content_id: "42".to_owned(),
                source: "imdb".to_owned(),
                key: "id".to_owned(),
                value: "tt0042".to_owned(),
            }],
        }
    }

    fn attached_inputs(
        created_at: Option<i64>,
    ) -> (
        WriteSet,
        BTreeMap<String, TorrentContentPersistence>,
        BTreeMap<super::ContentPersistenceKey, ContentPersistence>,
    ) {
        let row = TorrentContentWrite {
            id: format!("{INFO_HASH}:movie:tmdb:42"),
            info_hash: INFO_HASH.to_owned(),
            content_type: Some("movie".to_owned()),
            content_source: Some("tmdb".to_owned()),
            content_id: Some("42".to_owned()),
            languages: Vec::new(),
            episodes: String::new(),
            video_resolution: None,
            video_source: None,
            video_codec: None,
            video_3d: None,
            video_modifier: None,
            release_group: None,
            size: 1,
            files_count: None,
        };
        let torrent_persistence = BTreeMap::from([(
            row.id.clone(),
            TorrentContentPersistence {
                seeders: None,
                leechers: None,
                published_at_micros: 1,
                tsv: "'attached':1A".to_owned(),
            },
        )]);
        let content_persistence = ContentPersistence::from_content(&content(created_at));
        let content_key = content_persistence.key();
        let write_set = WriteSet {
            contents: vec![ContentWrite {
                content_type: "movie".to_owned(),
                source: "tmdb".to_owned(),
                id: "42".to_owned(),
                title: "Attached Title".to_owned(),
                release_year: Some(2026),
                identifiers: BTreeMap::new(),
            }],
            torrent_contents: vec![row],
            ..WriteSet::default()
        };
        (
            write_set,
            torrent_persistence,
            BTreeMap::from([(content_key, content_persistence)]),
        )
    }

    #[test]
    fn delete_only_write_set_is_not_treated_as_empty() {
        let write_set = WriteSet {
            delete_info_hashes: vec!["1111111111111111111111111111111111111111".to_owned()],
            ..WriteSet::default()
        };

        let prepared =
            PreparedWriteSet::new(&write_set, &BTreeMap::new(), &BTreeMap::new()).unwrap();

        assert!(!prepared.is_empty());
    }

    #[test]
    fn content_persistence_rebuilds_tsv_and_preserves_go_new_row_marker() {
        let mut content = content(None);

        let new_row = ContentPersistence::from_content(&content);
        assert!(new_row.upsert().is_some());
        assert_eq!(new_row.key().content_type, "movie");
        assert!(new_row.base_tsv().to_string().contains("'attached':1A"));
        assert!(new_row.base_tsv().to_string().contains("'2026':4B"));
        assert!(new_row.base_tsv().to_string().contains("'mystery':6"));
        assert!(new_row.base_tsv().to_string().contains("'tt0042':8"));

        content.created_at = Some(1);
        let reused_row = ContentPersistence::from_content(&content);
        assert!(reused_row.upsert().is_none());
        assert_eq!(reused_row.base_tsv(), new_row.base_tsv());
    }

    #[test]
    fn prepared_attached_content_requires_exact_identity_coverage() {
        let (write_set, torrent_persistence, content_persistence) = attached_inputs(None);
        let prepared =
            PreparedWriteSet::new(&write_set, &torrent_persistence, &content_persistence)
                .expect("prepare exact attached image");
        assert_eq!(prepared.contents.len(), 1);
        assert_eq!(prepared.contents[0].attributes.len(), 1);
        assert_eq!(prepared.contents[0].collections.len(), 1);

        assert!(matches!(
            PreparedWriteSet::new(&write_set, &torrent_persistence, &BTreeMap::new()),
            Err(PersistError::MissingContentPersistence { .. })
        ));
        assert!(matches!(
            PreparedWriteSet::new(&WriteSet::default(), &BTreeMap::new(), &content_persistence,),
            Err(PersistError::UnexpectedContentPersistence { .. })
        ));

        let (mut partial, torrent_persistence, _) = attached_inputs(None);
        partial.torrent_contents[0].content_type = None;
        assert!(matches!(
            PreparedWriteSet::new(&partial, &torrent_persistence, &BTreeMap::new()),
            Err(PersistError::PartialContentReference)
        ));
    }

    #[test]
    fn reused_attached_content_supplies_base_without_root_or_association_writes() {
        let (write_set, torrent_persistence, content_persistence) = attached_inputs(Some(1));
        let prepared =
            PreparedWriteSet::new(&write_set, &torrent_persistence, &content_persistence)
                .expect("prepare reused attached image");
        assert!(prepared.contents.is_empty());
        assert_eq!(prepared.torrent_contents.len(), 1);
    }

    #[test]
    fn attached_content_map_key_must_match_new_and_reused_images() {
        for created_at in [None, Some(1)] {
            let (write_set, torrent_persistence, content_persistence) = attached_inputs(created_at);
            let image = content_persistence.into_values().next().unwrap();
            let wrong_key = super::ContentPersistenceKey {
                content_type: "movie".to_owned(),
                source: "tmdb".to_owned(),
                id: "99".to_owned(),
            };
            assert!(matches!(
                PreparedWriteSet::new(
                    &write_set,
                    &torrent_persistence,
                    &BTreeMap::from([(wrong_key, image)]),
                ),
                Err(PersistError::ContentPersistenceKeyMismatch { .. })
            ));
        }
    }

    #[test]
    fn two_torrents_share_one_new_content_upsert() {
        let (mut write_set, mut torrent_persistence, content_persistence) = attached_inputs(None);
        let second_hash = "2222222222222222222222222222222222222222";
        let mut second = write_set.torrent_contents[0].clone();
        second.info_hash = second_hash.to_owned();
        second.id = format!("{second_hash}:movie:tmdb:42");
        let second_metadata = torrent_persistence
            .values()
            .next()
            .expect("first metadata")
            .clone();
        torrent_persistence.insert(second.id.clone(), second_metadata);
        write_set.torrent_contents.push(second);

        let prepared =
            PreparedWriteSet::new(&write_set, &torrent_persistence, &content_persistence)
                .expect("prepare two references to one attached content");
        assert_eq!(prepared.contents.len(), 1);
        assert_eq!(prepared.torrent_contents.len(), 2);
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
