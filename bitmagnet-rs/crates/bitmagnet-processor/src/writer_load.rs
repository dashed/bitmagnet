//! Bounded read boundary for the processor writer projection.
//!
//! This module joins the existing classifier hydration image to the raw
//! volatile rows that [`crate::project_unattached_persistence`] needs. The
//! public loader owns a read-only repeatable-read transaction; the ingest-shadow
//! runtime uses the in-transaction form so source validation, planning, and
//! comparison cannot observe a torn source/torrent image.

use std::collections::{BTreeMap, BTreeSet};

use bitmagnet_model::{Content, ContentAttribute, ContentCollection};
use bitmagnet_queue::{ProcessTorrentParams, ProtocolId};
use futures::TryStreamExt;
use sqlx::{PgConnection, PgPool, Row};

use crate::load::{load_torrents_in, LoadError};
use crate::{ContentPersistenceKey, LoadedTorrent, TorrentSnapshot, TorrentSourceSnapshot};

const MAX_REQUESTED_TORRENTS: usize = 100;
const MAX_SOURCES_PER_TORRENT: usize = 1_024;
const MAX_TOTAL_SOURCES: usize = MAX_REQUESTED_TORRENTS * MAX_SOURCES_PER_TORRENT;
const MAX_CONTENT_ATTRIBUTES_PER_CONTENT: usize = 1_024;
const MAX_CONTENT_COLLECTION_LINKS_PER_CONTENT: usize = 1_024;
const MAX_TOTAL_CONTENT_ATTRIBUTES: usize =
    MAX_REQUESTED_TORRENTS * MAX_CONTENT_ATTRIBUTES_PER_CONTENT;
const MAX_TOTAL_CONTENT_COLLECTION_LINKS: usize =
    MAX_REQUESTED_TORRENTS * MAX_CONTENT_COLLECTION_LINKS_PER_CONTENT;

const TORRENT_SNAPSHOTS_SQL: &str = "\
SELECT encode(info_hash, 'hex') AS info_hash, \
       (EXTRACT(EPOCH FROM created_at) * 1000000)::bigint AS created_at_micros \
FROM torrents \
WHERE info_hash = ANY($1::bytea[]) \
ORDER BY info_hash";

// Each requested key gets a bounded lateral source scan. The inner LIMIT emits
// one sentinel row past the per-torrent ceiling; the outer LIMIT does the same
// for total cardinality. Counting stays in the streaming Rust decoder so the
// database never has to scan every source row for a pathological torrent.
const SOURCE_SNAPSHOTS_SQL: &str = "\
SELECT encode(r.info_hash, 'hex') AS info_hash, \
       s.seeders, s.leechers, \
       (EXTRACT(EPOCH FROM s.published_at) * 1000000)::bigint AS published_at_micros, \
       (EXTRACT(EPOCH FROM s.created_at) * 1000000)::bigint AS created_at_micros \
FROM UNNEST($1::bytea[]) AS r(info_hash) \
CROSS JOIN LATERAL ( \
    SELECT source, seeders, leechers, published_at, created_at \
    FROM torrents_torrent_sources \
    WHERE info_hash = r.info_hash \
    LIMIT $2 \
) AS s \
ORDER BY r.info_hash, s.source \
LIMIT $3";

// The writer-only association scans use the same sentinel pattern as source
// snapshots: one row past each per-content and job-wide ceiling proves that an
// apparently hydrated content image was not silently truncated.
const CONTENT_ATTRIBUTES_SQL: &str = "\
SELECT r.content_type, r.content_source, r.content_id, \
       a.source AS attribute_source, a.key AS attribute_key, a.value \
FROM UNNEST($1::text[], $2::text[], $3::text[]) \
     AS r(content_type, content_source, content_id) \
CROSS JOIN LATERAL ( \
    SELECT source, key, value \
    FROM content_attributes \
    WHERE content_type = r.content_type \
      AND content_source = r.content_source \
      AND content_id = r.content_id \
    LIMIT $4 \
) AS a \
ORDER BY r.content_type, r.content_source, r.content_id, a.source, a.key \
LIMIT $5";

const CONTENT_COLLECTION_LINKS_SQL: &str = "\
SELECT r.content_type, r.content_source, r.content_id, \
       l.content_collection_type, l.content_collection_source, \
       l.content_collection_id \
FROM UNNEST($1::text[], $2::text[], $3::text[]) \
     AS r(content_type, content_source, content_id) \
CROSS JOIN LATERAL ( \
    SELECT content_collection_type, content_collection_source, \
           content_collection_id \
    FROM content_collections_content \
    WHERE content_type = r.content_type \
      AND content_source = r.content_source \
      AND content_id = r.content_id \
    LIMIT $4 \
) AS l \
ORDER BY r.content_type, r.content_source, r.content_id, \
         l.content_collection_type, l.content_collection_source, \
         l.content_collection_id \
LIMIT $5";

const CONTENT_COLLECTIONS_SQL: &str = "\
SELECT type AS collection_type, source, id, name \
FROM content_collections \
WHERE (type, source, id) \
      IN (SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[])) \
ORDER BY type, source, id";

type CollectionKey = (String, String, String);

/// One classifier input and the raw volatile snapshots used by its writer
/// projection.
///
/// Source rows retain their database order (`info_hash`, then source key),
/// nullable counts, and timestamps. No maxima, publication cutoff, or fallback
/// is applied in SQL; those semantics belong to the pure writer projection.
#[derive(Debug)]
pub struct WriterLoadedTorrent {
    pub loaded: LoadedTorrent,
    pub torrent_snapshot: TorrentSnapshot,
    pub source_snapshots: Vec<TorrentSourceSnapshot>,
    /// True only when the writer loader hydrated the selected reused content's
    /// scalar row, every attribute, every collection link, and every linked
    /// collection inside its repeatable-read transaction.
    pub reusable_content_fully_hydrated: bool,
}

#[derive(Debug)]
struct WriterPersistenceSnapshots {
    torrent_snapshot: TorrentSnapshot,
    source_snapshots: Vec<TorrentSourceSnapshot>,
}

/// Load the existing classifier image plus writer-only volatile snapshots.
///
/// Missing requested torrents retain [`crate::load_torrents`] semantics: they
/// are omitted. Request duplicates count toward the fail-closed input ceiling,
/// then are deduplicated and sorted before any query. This standalone entry
/// point owns its read-only repeatable-read transaction and does not persist.
pub async fn load_writer_torrents(
    pool: &PgPool,
    params: &ProcessTorrentParams,
) -> Result<Vec<WriterLoadedTorrent>, WriterLoadError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    let result = load_writer_torrents_in(&mut tx, params).await?;
    tx.commit().await?;
    Ok(result)
}

/// Load the classifier and writer snapshots from the caller's existing
/// transaction. The caller owns its isolation/read-only contract.
pub(crate) async fn load_writer_torrents_in(
    connection: &mut PgConnection,
    params: &ProcessTorrentParams,
) -> Result<Vec<WriterLoadedTorrent>, WriterLoadError> {
    validate_request_cardinality(params.info_hashes.len())?;

    let info_hashes = params
        .info_hashes
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut deduplicated_params = params.clone();
    deduplicated_params.info_hashes.clone_from(&info_hashes);

    let mut loaded = load_torrents_in(&mut *connection, &deduplicated_params).await?;
    let hydrated_reusable =
        hydrate_reusable_content_associations(&mut *connection, &mut loaded).await?;
    let snapshots = load_writer_snapshots_in(&mut *connection, &info_hashes).await?;
    associate_loaded_torrents(loaded, snapshots, &hydrated_reusable)
}

/// Complete only the source-backed row the default-mode classifier will reuse.
///
/// The ordinary shadow loader deliberately cannot read these association
/// tables. Keeping the expansion here makes it writer-only, and running it on
/// the caller's connection keeps scalar content, attributes, collection links,
/// and collections in the same repeatable-read image as the torrent snapshots.
async fn hydrate_reusable_content_associations(
    connection: &mut PgConnection,
    loaded: &mut [LoadedTorrent],
) -> Result<BTreeSet<String>, WriterLoadError> {
    let mut base_contents = BTreeMap::<ContentPersistenceKey, Content>::new();
    let mut selected = BTreeMap::<String, ContentPersistenceKey>::new();

    for torrent in loaded.iter() {
        let Some((key, content)) = selected_reusable_content(torrent)? else {
            continue;
        };
        if let Some(previous) = base_contents.insert(key.clone(), content.clone()) {
            if &previous != content {
                return Err(WriterLoadError::ConflictingReusableContent {
                    key: render_content_key(&key),
                });
            }
        }
        selected.insert(torrent.info_hash.clone(), key);
    }

    if base_contents.is_empty() {
        return Ok(BTreeSet::new());
    }

    let keys = base_contents.keys().cloned().collect::<Vec<_>>();
    let attributes = load_content_attributes(connection, &keys).await?;
    let links = load_content_collection_links(connection, &keys).await?;
    let collection_keys = links
        .values()
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let collections = load_content_collections(connection, &collection_keys).await?;

    for (key, content) in &mut base_contents {
        content.attributes = attributes.get(key).cloned().unwrap_or_default();
        content.collections = links
            .get(key)
            .into_iter()
            .flatten()
            .map(|collection_key| {
                collections.get(collection_key).cloned().ok_or_else(|| {
                    WriterLoadError::ReusableCollectionMissing {
                        content_key: render_content_key(key),
                        collection_key: render_collection_key(collection_key),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
    }

    for torrent in loaded.iter_mut() {
        let Some(key) = selected.get(&torrent.info_hash) else {
            continue;
        };
        let hydrated = base_contents
            .get(key)
            .expect("selected reusable content key was collected")
            .clone();
        let association = torrent
            .classifier_input
            .contents
            .iter_mut()
            .find(|association| association_matches_key(association, key))
            .ok_or_else(|| WriterLoadError::ReusableContentIncomplete {
                info_hash: torrent.info_hash.clone(),
                detail: "selected association disappeared during writer hydration",
            })?;
        association.content = Some(hydrated);
    }

    Ok(selected.into_keys().collect())
}

fn selected_reusable_content(
    torrent: &LoadedTorrent,
) -> Result<Option<(ContentPersistenceKey, &Content)>, WriterLoadError> {
    if !torrent.source_backed_content_present || torrent.attach_hint_unsupported {
        return Ok(None);
    }
    let hint = torrent
        .classifier_input
        .hint
        .as_ref()
        .filter(|hint| !hint.content_source.is_empty())
        .ok_or_else(|| WriterLoadError::ReusableContentIncomplete {
            info_hash: torrent.info_hash.clone(),
            detail: "source-backed reuse has no sourced effective hint",
        })?;
    let key = ContentPersistenceKey {
        content_type: hint.content_type.clone(),
        source: hint.content_source.clone(),
        id: hint.content_id.clone(),
    };
    let association = torrent
        .classifier_input
        .contents
        .iter()
        .find(|association| association_matches_key(association, &key))
        .ok_or_else(|| WriterLoadError::ReusableContentIncomplete {
            info_hash: torrent.info_hash.clone(),
            detail: "effective hint has no matching existing association",
        })?;
    let content =
        association
            .content
            .as_ref()
            .ok_or_else(|| WriterLoadError::ReusableContentIncomplete {
                info_hash: torrent.info_hash.clone(),
                detail: "selected association has no content row",
            })?;
    if content.created_at.is_none() {
        return Err(WriterLoadError::ReusableContentIncomplete {
            info_hash: torrent.info_hash.clone(),
            detail: "selected content row has no existing-row timestamp",
        });
    }
    let actual_key = ContentPersistenceKey {
        content_type: content.content_type.as_str().to_owned(),
        source: content.source.clone(),
        id: content.id.clone(),
    };
    if actual_key != key {
        return Err(WriterLoadError::ReusableContentKeyMismatch {
            info_hash: torrent.info_hash.clone(),
            expected: render_content_key(&key),
            actual: render_content_key(&actual_key),
        });
    }
    Ok(Some((key, content)))
}

fn association_matches_key(
    association: &bitmagnet_classifier::InputContent,
    key: &ContentPersistenceKey,
) -> bool {
    association.content_type == key.content_type
        && association.content_source == key.source
        && association.content_id == key.id
}

async fn load_content_attributes(
    connection: &mut PgConnection,
    keys: &[ContentPersistenceKey],
) -> Result<BTreeMap<ContentPersistenceKey, Vec<ContentAttribute>>, WriterLoadError> {
    let (types, sources, ids) = content_key_arrays(keys);
    let per_content_limit = i64::try_from(MAX_CONTENT_ATTRIBUTES_PER_CONTENT + 1)
        .expect("bounded attribute limit fits i64");
    let total_limit = i64::try_from(MAX_TOTAL_CONTENT_ATTRIBUTES + 1)
        .expect("bounded total attribute limit fits i64");
    let mut rows = sqlx::query(CONTENT_ATTRIBUTES_SQL)
        .bind(&types)
        .bind(&sources)
        .bind(&ids)
        .bind(per_content_limit)
        .bind(total_limit)
        .fetch(&mut *connection);
    let mut result = BTreeMap::<ContentPersistenceKey, Vec<ContentAttribute>>::new();
    let mut counts = BTreeMap::<ContentPersistenceKey, usize>::new();
    let mut total = 0_usize;
    while let Some(row) = rows.try_next().await? {
        let key = content_key_from_row(&row)?;
        let count = counts.entry(key.clone()).or_default();
        *count = count.saturating_add(1);
        total = total.saturating_add(1);
        validate_content_association_cardinality(
            &key,
            "attributes",
            *count,
            MAX_CONTENT_ATTRIBUTES_PER_CONTENT,
            total,
            MAX_TOTAL_CONTENT_ATTRIBUTES,
        )?;
        if !keys.contains(&key) {
            return Err(WriterLoadError::UnexpectedReusableContent {
                key: render_content_key(&key),
            });
        }
        let content_type = key
            .content_type
            .parse()
            .map_err(|_| WriterLoadError::UnknownContentType(key.content_type.clone()))?;
        result.entry(key).or_default().push(ContentAttribute {
            content_type,
            content_source: row.try_get("content_source")?,
            content_id: row.try_get("content_id")?,
            source: row.try_get("attribute_source")?,
            key: row.try_get("attribute_key")?,
            value: row.try_get("value")?,
        });
    }
    Ok(result)
}

async fn load_content_collection_links(
    connection: &mut PgConnection,
    keys: &[ContentPersistenceKey],
) -> Result<BTreeMap<ContentPersistenceKey, Vec<CollectionKey>>, WriterLoadError> {
    let (types, sources, ids) = content_key_arrays(keys);
    let per_content_limit = i64::try_from(MAX_CONTENT_COLLECTION_LINKS_PER_CONTENT + 1)
        .expect("bounded collection-link limit fits i64");
    let total_limit = i64::try_from(MAX_TOTAL_CONTENT_COLLECTION_LINKS + 1)
        .expect("bounded total collection-link limit fits i64");
    let mut rows = sqlx::query(CONTENT_COLLECTION_LINKS_SQL)
        .bind(&types)
        .bind(&sources)
        .bind(&ids)
        .bind(per_content_limit)
        .bind(total_limit)
        .fetch(&mut *connection);
    let mut result = BTreeMap::<ContentPersistenceKey, Vec<CollectionKey>>::new();
    let mut counts = BTreeMap::<ContentPersistenceKey, usize>::new();
    let mut total = 0_usize;
    while let Some(row) = rows.try_next().await? {
        let key = content_key_from_row(&row)?;
        if !keys.contains(&key) {
            return Err(WriterLoadError::UnexpectedReusableContent {
                key: render_content_key(&key),
            });
        }
        let count = counts.entry(key.clone()).or_default();
        *count = count.saturating_add(1);
        total = total.saturating_add(1);
        validate_content_association_cardinality(
            &key,
            "collection links",
            *count,
            MAX_CONTENT_COLLECTION_LINKS_PER_CONTENT,
            total,
            MAX_TOTAL_CONTENT_COLLECTION_LINKS,
        )?;
        result.entry(key).or_default().push((
            row.try_get("content_collection_type")?,
            row.try_get("content_collection_source")?,
            row.try_get("content_collection_id")?,
        ));
    }
    Ok(result)
}

async fn load_content_collections(
    connection: &mut PgConnection,
    keys: &[CollectionKey],
) -> Result<BTreeMap<CollectionKey, ContentCollection>, WriterLoadError> {
    if keys.is_empty() {
        return Ok(BTreeMap::new());
    }
    let types = keys.iter().map(|key| key.0.clone()).collect::<Vec<_>>();
    let sources = keys.iter().map(|key| key.1.clone()).collect::<Vec<_>>();
    let ids = keys.iter().map(|key| key.2.clone()).collect::<Vec<_>>();
    let rows = sqlx::query(CONTENT_COLLECTIONS_SQL)
        .bind(&types)
        .bind(&sources)
        .bind(&ids)
        .fetch_all(&mut *connection)
        .await?;
    let mut result = BTreeMap::new();
    for row in rows {
        let key: CollectionKey = (
            row.try_get("collection_type")?,
            row.try_get("source")?,
            row.try_get("id")?,
        );
        let collection = ContentCollection {
            collection_type: key.0.clone(),
            source: key.1.clone(),
            id: key.2.clone(),
            name: row.try_get("name")?,
        };
        if result.insert(key.clone(), collection).is_some() {
            return Err(WriterLoadError::DuplicateReusableCollection {
                key: render_collection_key(&key),
            });
        }
    }
    Ok(result)
}

fn content_key_arrays(keys: &[ContentPersistenceKey]) -> (Vec<String>, Vec<String>, Vec<String>) {
    (
        keys.iter().map(|key| key.content_type.clone()).collect(),
        keys.iter().map(|key| key.source.clone()).collect(),
        keys.iter().map(|key| key.id.clone()).collect(),
    )
}

fn content_key_from_row(row: &sqlx::postgres::PgRow) -> Result<ContentPersistenceKey, sqlx::Error> {
    Ok(ContentPersistenceKey {
        content_type: row.try_get("content_type")?,
        source: row.try_get("content_source")?,
        id: row.try_get("content_id")?,
    })
}

fn render_content_key(key: &ContentPersistenceKey) -> String {
    format!("{}:{}:{}", key.content_type, key.source, key.id)
}

fn render_collection_key(key: &CollectionKey) -> String {
    format!("{}:{}:{}", key.0, key.1, key.2)
}

fn validate_content_association_cardinality(
    key: &ContentPersistenceKey,
    association: &'static str,
    per_content: usize,
    per_content_limit: usize,
    total: usize,
    total_limit: usize,
) -> Result<(), WriterLoadError> {
    if per_content > per_content_limit {
        return Err(WriterLoadError::TooManyContentAssociations {
            key: render_content_key(key),
            association,
            actual: per_content,
            limit: per_content_limit,
        });
    }
    if total > total_limit {
        return Err(WriterLoadError::TooManyContentAssociationsTotal {
            association,
            actual: total,
            limit: total_limit,
        });
    }
    Ok(())
}

async fn load_writer_snapshots_in(
    connection: &mut PgConnection,
    info_hashes: &[ProtocolId],
) -> Result<BTreeMap<String, WriterPersistenceSnapshots>, WriterLoadError> {
    if info_hashes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let requested = info_hashes
        .iter()
        .map(|id| id.as_bytes().to_vec())
        .collect::<Vec<_>>();

    let torrent_rows = sqlx::query(TORRENT_SNAPSHOTS_SQL)
        .bind(&requested)
        .fetch_all(&mut *connection)
        .await?;
    let mut snapshots = BTreeMap::new();
    for row in torrent_rows {
        let info_hash = row.try_get::<String, _>("info_hash")?;
        let value = WriterPersistenceSnapshots {
            torrent_snapshot: TorrentSnapshot {
                created_at_micros: row.try_get("created_at_micros")?,
            },
            source_snapshots: Vec::new(),
        };
        if snapshots.insert(info_hash.clone(), value).is_some() {
            return Err(WriterLoadError::DuplicateTorrentSnapshot { info_hash });
        }
    }

    let per_torrent_limit =
        i64::try_from(MAX_SOURCES_PER_TORRENT + 1).expect("bounded source limit fits i64");
    let total_limit =
        i64::try_from(MAX_TOTAL_SOURCES + 1).expect("bounded total source limit fits i64");
    let mut source_rows = sqlx::query(SOURCE_SNAPSHOTS_SQL)
        .bind(&requested)
        .bind(per_torrent_limit)
        .bind(total_limit)
        .fetch(&mut *connection);
    let mut total_sources = 0_usize;
    let mut source_counts = BTreeMap::<String, usize>::new();
    while let Some(row) = source_rows.try_next().await? {
        let info_hash = row.try_get::<String, _>("info_hash")?;
        let Some(torrent) = snapshots.get_mut(&info_hash) else {
            return Err(WriterLoadError::SourceWithoutTorrent { info_hash });
        };
        let source_count = source_counts.entry(info_hash.clone()).or_default();
        *source_count = source_count.saturating_add(1);
        total_sources = total_sources.saturating_add(1);
        validate_source_cardinality(&info_hash, *source_count, total_sources)?;

        let source = TorrentSourceSnapshot {
            seeders: optional_nonnegative_u64(&info_hash, "seeders", row.try_get("seeders")?)?,
            leechers: optional_nonnegative_u64(&info_hash, "leechers", row.try_get("leechers")?)?,
            published_at_micros: row.try_get("published_at_micros")?,
            created_at_micros: row.try_get("created_at_micros")?,
        };
        torrent.source_snapshots.push(source);
    }

    Ok(snapshots)
}

fn associate_loaded_torrents(
    loaded: Vec<LoadedTorrent>,
    mut snapshots: BTreeMap<String, WriterPersistenceSnapshots>,
    hydrated_reusable: &BTreeSet<String>,
) -> Result<Vec<WriterLoadedTorrent>, WriterLoadError> {
    let mut result = Vec::with_capacity(loaded.len());
    let mut loaded_keys = BTreeSet::new();
    for loaded in loaded {
        if loaded.info_hash != loaded.classifier_input.id {
            return Err(WriterLoadError::LoadedInfoHashMismatch {
                loaded_info_hash: loaded.info_hash,
                classifier_info_hash: loaded.classifier_input.id,
            });
        }
        if !loaded_keys.insert(loaded.info_hash.clone()) {
            return Err(WriterLoadError::DuplicateLoadedTorrent {
                info_hash: loaded.info_hash,
            });
        }
        let Some(snapshot) = snapshots.remove(&loaded.info_hash) else {
            return Err(WriterLoadError::SnapshotMissingForLoaded {
                info_hash: loaded.info_hash,
            });
        };
        result.push(WriterLoadedTorrent {
            reusable_content_fully_hydrated: hydrated_reusable.contains(&loaded.info_hash),
            loaded,
            torrent_snapshot: snapshot.torrent_snapshot,
            source_snapshots: snapshot.source_snapshots,
        });
    }
    if let Some((info_hash, _)) = snapshots.into_iter().next() {
        return Err(WriterLoadError::SnapshotWithoutLoaded { info_hash });
    }
    Ok(result)
}

fn validate_request_cardinality(actual: usize) -> Result<(), WriterLoadError> {
    if actual > MAX_REQUESTED_TORRENTS {
        Err(WriterLoadError::RequestTooLarge {
            actual,
            limit: MAX_REQUESTED_TORRENTS,
        })
    } else {
        Ok(())
    }
}

fn validate_source_cardinality(
    info_hash: &str,
    per_torrent: usize,
    total: usize,
) -> Result<(), WriterLoadError> {
    if per_torrent > MAX_SOURCES_PER_TORRENT {
        return Err(WriterLoadError::TooManySourcesForTorrent {
            info_hash: info_hash.to_owned(),
            actual: per_torrent,
            limit: MAX_SOURCES_PER_TORRENT,
        });
    }
    if total > MAX_TOTAL_SOURCES {
        return Err(WriterLoadError::TooManySourcesTotal {
            actual: total,
            limit: MAX_TOTAL_SOURCES,
        });
    }
    Ok(())
}

fn optional_nonnegative_u64(
    info_hash: &str,
    field: &'static str,
    value: Option<i32>,
) -> Result<Option<u64>, WriterLoadError> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| WriterLoadError::NegativeSourceCount {
                info_hash: info_hash.to_owned(),
                field,
                value: i64::from(value),
            })
        })
        .transpose()
}

/// Fail-closed errors from the bounded writer loader.
#[derive(Debug, thiserror::Error)]
pub enum WriterLoadError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Load(#[from] LoadError),
    #[error("writer snapshot request has {actual} items, above the {limit}-item limit")]
    RequestTooLarge { actual: usize, limit: usize },
    #[error("torrent {info_hash} has {actual} source rows, above the {limit}-row limit")]
    TooManySourcesForTorrent {
        info_hash: String,
        actual: usize,
        limit: usize,
    },
    #[error("writer snapshot request has {actual} total source rows, above the {limit}-row limit")]
    TooManySourcesTotal { actual: usize, limit: usize },
    #[error("negative {field} value in writer source snapshot for {info_hash}: {value}")]
    NegativeSourceCount {
        info_hash: String,
        field: &'static str,
        value: i64,
    },
    #[error("source snapshot has no torrent snapshot for {info_hash}")]
    SourceWithoutTorrent { info_hash: String },
    #[error("duplicate torrent snapshot for {info_hash}")]
    DuplicateTorrentSnapshot { info_hash: String },
    #[error("duplicate loaded torrent for {info_hash}")]
    DuplicateLoadedTorrent { info_hash: String },
    #[error(
        "loaded torrent key {loaded_info_hash} does not match classifier key {classifier_info_hash}"
    )]
    LoadedInfoHashMismatch {
        loaded_info_hash: String,
        classifier_info_hash: String,
    },
    #[error("loaded torrent has no writer snapshot for {info_hash}")]
    SnapshotMissingForLoaded { info_hash: String },
    #[error("writer snapshot has no loaded torrent for {info_hash}")]
    SnapshotWithoutLoaded { info_hash: String },
    #[error("reusable content for {info_hash} is incomplete: {detail}")]
    ReusableContentIncomplete {
        info_hash: String,
        detail: &'static str,
    },
    #[error("reusable content key mismatch for {info_hash}: expected {expected}, got {actual}")]
    ReusableContentKeyMismatch {
        info_hash: String,
        expected: String,
        actual: String,
    },
    #[error("conflicting reusable content images for {key}")]
    ConflictingReusableContent { key: String },
    #[error("writer association query returned unexpected reusable content {key}")]
    UnexpectedReusableContent { key: String },
    #[error("unknown content type '{0}' in writer association image")]
    UnknownContentType(String),
    #[error("reusable content {key} has {actual} {association}, above the {limit}-row limit")]
    TooManyContentAssociations {
        key: String,
        association: &'static str,
        actual: usize,
        limit: usize,
    },
    #[error(
        "writer reusable-content image has {actual} total {association}, above the {limit}-row limit"
    )]
    TooManyContentAssociationsTotal {
        association: &'static str,
        actual: usize,
        limit: usize,
    },
    #[error("duplicate reusable collection {key}")]
    DuplicateReusableCollection { key: String },
    #[error("reusable content {content_key} references missing collection {collection_key}")]
    ReusableCollectionMissing {
        content_key: String,
        collection_key: String,
    },
}

#[cfg(test)]
mod tests {
    use super::{
        associate_loaded_torrents, optional_nonnegative_u64, selected_reusable_content,
        validate_content_association_cardinality, validate_request_cardinality,
        validate_source_cardinality, WriterLoadError, WriterPersistenceSnapshots,
        CONTENT_ATTRIBUTES_SQL, CONTENT_COLLECTION_LINKS_SQL, MAX_CONTENT_ATTRIBUTES_PER_CONTENT,
        MAX_REQUESTED_TORRENTS, MAX_SOURCES_PER_TORRENT, MAX_TOTAL_CONTENT_ATTRIBUTES,
        MAX_TOTAL_SOURCES, SOURCE_SNAPSHOTS_SQL,
    };
    use crate::{LoadedTorrent, TorrentSnapshot};
    use bitmagnet_classifier::{ClassifierInput, InputContent, InputHint};
    use bitmagnet_model::{Content, ContentType};
    use std::collections::{BTreeMap, BTreeSet};

    const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn loaded(info_hash: &str) -> LoadedTorrent {
        LoadedTorrent {
            info_hash: info_hash.to_owned(),
            classifier_input: ClassifierInput {
                id: info_hash.to_owned(),
                name: "fixture".to_owned(),
                size: 1,
                files_status: "single".to_owned(),
                extension: None,
                files_count: None,
                files: Vec::new(),
                hint: None,
                contents: Vec::new(),
            },
            existing_content_ids: Vec::new(),
            attach_hint_unsupported: false,
            source_backed_content_present: false,
        }
    }

    fn reusable_loaded() -> LoadedTorrent {
        let mut torrent = loaded(HASH_A);
        torrent.source_backed_content_present = true;
        torrent.classifier_input.hint = Some(InputHint {
            content_type: "movie".to_owned(),
            content_source: "tmdb".to_owned(),
            content_id: "42".to_owned(),
            ..InputHint::default()
        });
        torrent.classifier_input.contents = vec![InputContent {
            content_type: "movie".to_owned(),
            content_source: "tmdb".to_owned(),
            content_id: "42".to_owned(),
            content: Some(Content {
                content_type: ContentType::Movie,
                source: "tmdb".to_owned(),
                id: "42".to_owned(),
                title: "Reusable".to_owned(),
                release_date: None,
                release_year: Some(1999),
                adult: None,
                original_language: None,
                original_title: None,
                overview: None,
                runtime: None,
                popularity: None,
                vote_average: None,
                vote_count: None,
                created_at: Some(1),
                updated_at: Some(2),
                tsv: Default::default(),
                collections: Vec::new(),
                attributes: Vec::new(),
            }),
        }];
        torrent
    }

    fn snapshots(info_hash: &str) -> BTreeMap<String, WriterPersistenceSnapshots> {
        BTreeMap::from([(
            info_hash.to_owned(),
            WriterPersistenceSnapshots {
                torrent_snapshot: TorrentSnapshot {
                    created_at_micros: 1_700_000_000_123_456,
                },
                source_snapshots: Vec::new(),
            },
        )])
    }

    #[test]
    fn request_limit_counts_duplicates_before_deduplication() {
        validate_request_cardinality(MAX_REQUESTED_TORRENTS).expect("exact limit is accepted");
        let error = validate_request_cardinality(MAX_REQUESTED_TORRENTS + 1)
            .expect_err("one over the request limit is refused");
        assert!(matches!(
            error,
            WriterLoadError::RequestTooLarge {
                actual,
                limit: MAX_REQUESTED_TORRENTS
            } if actual == MAX_REQUESTED_TORRENTS + 1
        ));
    }

    #[test]
    fn source_cardinality_limits_are_inclusive_and_independent() {
        validate_source_cardinality(HASH_A, MAX_SOURCES_PER_TORRENT, MAX_TOTAL_SOURCES)
            .expect("exact limits are accepted");
        assert!(matches!(
            validate_source_cardinality(HASH_A, MAX_SOURCES_PER_TORRENT + 1, 1),
            Err(WriterLoadError::TooManySourcesForTorrent { actual, .. })
                if actual == MAX_SOURCES_PER_TORRENT + 1
        ));
        assert!(matches!(
            validate_source_cardinality(HASH_A, 1, MAX_TOTAL_SOURCES + 1),
            Err(WriterLoadError::TooManySourcesTotal { actual, .. })
                if actual == MAX_TOTAL_SOURCES + 1
        ));
    }

    #[test]
    fn nullable_counts_preserve_none_and_zero_but_refuse_negative_values() {
        assert_eq!(
            optional_nonnegative_u64(HASH_A, "seeders", None).unwrap(),
            None
        );
        assert_eq!(
            optional_nonnegative_u64(HASH_A, "seeders", Some(0)).unwrap(),
            Some(0)
        );
        assert_eq!(
            optional_nonnegative_u64(HASH_A, "leechers", Some(7)).unwrap(),
            Some(7)
        );
        assert!(matches!(
            optional_nonnegative_u64(HASH_A, "seeders", Some(-1)),
            Err(WriterLoadError::NegativeSourceCount {
                info_hash,
                field: "seeders",
                value: -1
            }) if info_hash == HASH_A
        ));
    }

    #[test]
    fn source_query_bounds_each_lateral_scan_and_the_total_stream() {
        assert!(SOURCE_SNAPSHOTS_SQL.contains("CROSS JOIN LATERAL"));
        assert!(SOURCE_SNAPSHOTS_SQL.contains("LIMIT $2"));
        assert!(SOURCE_SNAPSHOTS_SQL.contains("ORDER BY r.info_hash, s.source"));
        assert!(SOURCE_SNAPSHOTS_SQL.contains("LIMIT $3"));
        let lateral_body = SOURCE_SNAPSHOTS_SQL
            .split_once("CROSS JOIN LATERAL (")
            .expect("lateral source scan")
            .1
            .split_once(") AS s")
            .expect("bounded lateral source scan")
            .0;
        assert!(!lateral_body.contains("ORDER BY"));
        assert!(!SOURCE_SNAPSHOTS_SQL.contains("count("));
        assert!(!SOURCE_SNAPSHOTS_SQL.contains("max("));
    }

    #[test]
    fn reusable_content_requires_an_existing_exact_hydrated_row() {
        let torrent = reusable_loaded();
        let (key, content) = selected_reusable_content(&torrent)
            .expect("validate reusable content")
            .expect("selected reusable content");
        assert_eq!(key.content_type, "movie");
        assert_eq!(content.title, "Reusable");

        let mut missing = reusable_loaded();
        missing.classifier_input.contents[0].content = None;
        assert!(matches!(
            selected_reusable_content(&missing),
            Err(WriterLoadError::ReusableContentIncomplete { .. })
        ));

        let mut new_content = reusable_loaded();
        new_content.classifier_input.contents[0]
            .content
            .as_mut()
            .unwrap()
            .created_at = None;
        assert!(matches!(
            selected_reusable_content(&new_content),
            Err(WriterLoadError::ReusableContentIncomplete { .. })
        ));
    }

    #[test]
    fn reusable_association_queries_and_cardinality_are_fail_closed() {
        for sql in [CONTENT_ATTRIBUTES_SQL, CONTENT_COLLECTION_LINKS_SQL] {
            assert!(sql.contains("CROSS JOIN LATERAL"));
            assert!(sql.contains("LIMIT $4"));
            assert!(sql.contains("LIMIT $5"));
        }
        let key = crate::ContentPersistenceKey {
            content_type: "movie".to_owned(),
            source: "tmdb".to_owned(),
            id: "42".to_owned(),
        };
        validate_content_association_cardinality(
            &key,
            "attributes",
            MAX_CONTENT_ATTRIBUTES_PER_CONTENT,
            MAX_CONTENT_ATTRIBUTES_PER_CONTENT,
            MAX_TOTAL_CONTENT_ATTRIBUTES,
            MAX_TOTAL_CONTENT_ATTRIBUTES,
        )
        .expect("exact association limits are accepted");
        assert!(matches!(
            validate_content_association_cardinality(
                &key,
                "attributes",
                MAX_CONTENT_ATTRIBUTES_PER_CONTENT + 1,
                MAX_CONTENT_ATTRIBUTES_PER_CONTENT,
                1,
                MAX_TOTAL_CONTENT_ATTRIBUTES,
            ),
            Err(WriterLoadError::TooManyContentAssociations { actual, .. })
                if actual == MAX_CONTENT_ATTRIBUTES_PER_CONTENT + 1
        ));
    }

    #[test]
    fn association_requires_exact_loaded_and_snapshot_keysets() {
        let associated =
            associate_loaded_torrents(vec![loaded(HASH_A)], snapshots(HASH_A), &BTreeSet::new())
                .expect("matching keys associate");
        assert_eq!(associated.len(), 1);
        assert_eq!(
            associated[0].torrent_snapshot.created_at_micros,
            1_700_000_000_123_456
        );

        assert!(matches!(
            associate_loaded_torrents(
                vec![loaded(HASH_A)],
                snapshots(HASH_B),
                &BTreeSet::new()
            ),
            Err(WriterLoadError::SnapshotMissingForLoaded { info_hash }) if info_hash == HASH_A
        ));
        assert!(matches!(
            associate_loaded_torrents(Vec::new(), snapshots(HASH_B), &BTreeSet::new()),
            Err(WriterLoadError::SnapshotWithoutLoaded { info_hash }) if info_hash == HASH_B
        ));
    }

    #[test]
    fn association_refuses_duplicate_and_internal_loaded_keys() {
        assert!(matches!(
            associate_loaded_torrents(
                vec![loaded(HASH_A), loaded(HASH_A)],
                snapshots(HASH_A),
                &BTreeSet::new(),
            ),
            Err(WriterLoadError::DuplicateLoadedTorrent { info_hash }) if info_hash == HASH_A
        ));

        let mut mismatched = loaded(HASH_A);
        mismatched.classifier_input.id = HASH_B.to_owned();
        assert!(matches!(
            associate_loaded_torrents(vec![mismatched], snapshots(HASH_A), &BTreeSet::new()),
            Err(WriterLoadError::LoadedInfoHashMismatch {
                loaded_info_hash,
                classifier_info_hash
            }) if loaded_info_hash == HASH_A && classifier_info_hash == HASH_B
        ));
    }
}
