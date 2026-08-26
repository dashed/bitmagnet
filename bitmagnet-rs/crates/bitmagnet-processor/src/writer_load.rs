//! Disconnected read boundary for the processor writer projection.
//!
//! This module deliberately has no caller in the active shadow runtime. It
//! joins the existing classifier hydration image to the raw volatile rows that
//! [`crate::project_unattached_persistence`] needs, but does so under one
//! read-only repeatable-read transaction so a future writer cannot observe a
//! torn source/torrent image.

use std::collections::{BTreeMap, BTreeSet};

use bitmagnet_queue::{ProcessTorrentParams, ProtocolId};
use futures::TryStreamExt;
use sqlx::{PgConnection, PgPool, Row};

use crate::load::{load_torrents_in, LoadError};
use crate::{LoadedTorrent, TorrentSnapshot, TorrentSourceSnapshot};

const MAX_REQUESTED_TORRENTS: usize = 100;
const MAX_SOURCES_PER_TORRENT: usize = 1_024;
const MAX_TOTAL_SOURCES: usize = MAX_REQUESTED_TORRENTS * MAX_SOURCES_PER_TORRENT;

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
/// then are deduplicated and sorted before any query. This function is
/// intentionally disconnected from [`crate::ShadowRuntime`] and persistence.
pub async fn load_writer_torrents(
    pool: &PgPool,
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

    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    let loaded = load_torrents_in(&mut tx, &deduplicated_params).await?;
    let snapshots = load_writer_snapshots_in(&mut tx, &info_hashes).await?;
    let result = associate_loaded_torrents(loaded, snapshots)?;
    tx.commit().await?;
    Ok(result)
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

/// Fail-closed errors from the disconnected writer loader.
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
}

#[cfg(test)]
mod tests {
    use super::{
        associate_loaded_torrents, optional_nonnegative_u64, validate_request_cardinality,
        validate_source_cardinality, WriterLoadError, WriterPersistenceSnapshots,
        MAX_REQUESTED_TORRENTS, MAX_SOURCES_PER_TORRENT, MAX_TOTAL_SOURCES, SOURCE_SNAPSHOTS_SQL,
    };
    use crate::{LoadedTorrent, TorrentSnapshot};
    use bitmagnet_classifier::ClassifierInput;
    use std::collections::BTreeMap;

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
        }
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
    fn association_requires_exact_loaded_and_snapshot_keysets() {
        let associated = associate_loaded_torrents(vec![loaded(HASH_A)], snapshots(HASH_A))
            .expect("matching keys associate");
        assert_eq!(associated.len(), 1);
        assert_eq!(
            associated[0].torrent_snapshot.created_at_micros,
            1_700_000_000_123_456
        );

        assert!(matches!(
            associate_loaded_torrents(vec![loaded(HASH_A)], snapshots(HASH_B)),
            Err(WriterLoadError::SnapshotMissingForLoaded { info_hash }) if info_hash == HASH_A
        ));
        assert!(matches!(
            associate_loaded_torrents(Vec::new(), snapshots(HASH_B)),
            Err(WriterLoadError::SnapshotWithoutLoaded { info_hash }) if info_hash == HASH_B
        ));
    }

    #[test]
    fn association_refuses_duplicate_and_internal_loaded_keys() {
        assert!(matches!(
            associate_loaded_torrents(
                vec![loaded(HASH_A), loaded(HASH_A)],
                snapshots(HASH_A)
            ),
            Err(WriterLoadError::DuplicateLoadedTorrent { info_hash }) if info_hash == HASH_A
        ));

        let mut mismatched = loaded(HASH_A);
        mismatched.classifier_input.id = HASH_B.to_owned();
        assert!(matches!(
            associate_loaded_torrents(vec![mismatched], snapshots(HASH_A)),
            Err(WriterLoadError::LoadedInfoHashMismatch {
                loaded_info_hash,
                classifier_info_hash
            }) if loaded_info_hash == HASH_A && classifier_info_hash == HASH_B
        ));
    }
}
