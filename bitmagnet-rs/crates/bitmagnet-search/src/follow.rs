//! Incremental maintenance loop for the main Tantivy search index.
//!
//! The loop lives inside the serving process so it can share the server's
//! single Tantivy `IndexWriter`. A separate follower binary cannot safely run
//! beside the server because Tantivy takes one writer lock per index directory.
//! Freshness keys on `torrents.updated_at` (the 00024 contract), so content-table-only metadata refreshes that do not touch the torrent row are not re-indexed until the torrent is next touched, matching the L2/L3 followers.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Context;
use bitmagnet_db::{
    connect, read_deleted_torrents, stream_changed_torrent_keys,
    stream_torrents_for_index_info_hashes, ChangedTorrentKey, DbConfig, PgPool, TorrentForIndex,
};
use bitmagnet_model::{BlobFile, InfoHash};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::pathsearch::watermark::{current_epoch, read_watermark, write_watermark};
use crate::proto::TorrentDocument;
use crate::server::{SearchServer, TorrentDocumentReplacement};
use crate::transform::build_document;

pub const DEFAULT_FOLLOW_INTERVAL_SECS: u64 = 15;
pub const DEFAULT_CARVE_LAG_SECS: i64 = 30;
pub const DEFAULT_FOLLOW_MAX_WINDOW_SECS: i64 = 3600;
pub const DEFAULT_FOLLOW_BATCH_SIZE: i64 = 1000;
pub const DEFAULT_DELETED_LIMIT: i64 = 100_000;
pub const DEFAULT_WATERMARK_FILE_NAME: &str = "watermark";

/// Backoff cap for a failing follow loop: repeated error ticks settle at one
/// retry per 5 minutes rather than hammering PostgreSQL.
const FOLLOW_MAX_BACKOFF_SECS: u64 = 300;

/// Follow-loop runtime configuration, normally built from CLI flags/env.
#[derive(Debug, Clone)]
pub struct FollowConfig {
    /// Optional PostgreSQL DSN override. Empty means use `BITMAGNET_POSTGRES_*`.
    pub postgres_dsn: String,
    /// Steady-state poll interval.
    pub interval_secs: u64,
    /// Commit-visibility lag for the upper bound of each carve window.
    pub carve_lag_secs: i64,
    /// Maximum epoch-second width for a single carved follow window.
    pub max_window_secs: i64,
    /// Changed-torrent keyset page size and scoped `torrent_contents` page size.
    pub batch_size: i64,
    /// Runaway guard for a single deleted-torrents read.
    pub deleted_limit: i64,
    /// Atomic sidecar watermark file. The serving binary resolves an omitted
    /// flag/env value to `<index-path>/watermark`.
    pub watermark_file: PathBuf,
}

impl FollowConfig {
    #[must_use]
    fn normalized(mut self) -> Self {
        self.interval_secs = self.interval_secs.max(1);
        self.carve_lag_secs = self.carve_lag_secs.max(1);
        self.max_window_secs = self.max_window_secs.max(1);
        self.batch_size = self.batch_size.max(1);
        self.deleted_limit = self.deleted_limit.max(1);
        self
    }
}

impl Default for FollowConfig {
    fn default() -> Self {
        Self {
            postgres_dsn: String::new(),
            interval_secs: DEFAULT_FOLLOW_INTERVAL_SECS,
            carve_lag_secs: DEFAULT_CARVE_LAG_SECS,
            max_window_secs: DEFAULT_FOLLOW_MAX_WINDOW_SECS,
            batch_size: DEFAULT_FOLLOW_BATCH_SIZE,
            deleted_limit: DEFAULT_DELETED_LIMIT,
            watermark_file: PathBuf::from(DEFAULT_WATERMARK_FILE_NAME),
        }
    }
}

/// One lagged follow window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FollowWindow {
    pub since: i64,
    pub until: i64,
}

/// Per-tick counters emitted to tracing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowStats {
    pub since: i64,
    pub until: i64,
    /// Changed torrents that produced at least one current `torrent_contents`
    /// document.
    pub upserts: u64,
    /// Documents added across those rebuilt torrents.
    pub documents: u64,
    /// Live tombstones applied from `deleted_torrents`.
    pub deletes: u64,
    /// Changed torrents that currently have no main-search documents.
    pub skips: u64,
    pub blob_errors: u64,
    pub info_hash_decode_skips: u64,
    pub stale_tombstones_skipped: u64,
    /// Tantivy commits completed through the real follow apply path.
    pub commits_performed: u64,
}

impl FollowStats {
    fn new(since: i64, until: i64) -> Self {
        Self {
            since,
            until,
            upserts: 0,
            documents: 0,
            deletes: 0,
            skips: 0,
            blob_errors: 0,
            info_hash_decode_skips: 0,
            stale_tombstones_skipped: 0,
            commits_performed: 0,
        }
    }
}

/// Spawn the endless PostgreSQL-tail follow loop.
///
/// Startup configuration errors (bad env/DSN, missing watermark directory) are
/// returned to the caller. Once spawned, per-tick errors are logged with
/// exponential backoff and never kill the server process.
pub async fn spawn_follow_loop(
    config: FollowConfig,
    server: SearchServer,
) -> anyhow::Result<JoinHandle<()>> {
    let config = config.normalized();

    let mut db_config = DbConfig::from_env().context("reading postgres config from env")?;
    if !config.postgres_dsn.is_empty() {
        db_config.dsn = config.postgres_dsn.clone();
    }
    let pool = connect(&db_config)
        .await
        .context("connecting to postgres")?;

    let watermark = read_watermark(&config.watermark_file)
        .unwrap_or_else(|| current_epoch().saturating_sub(config.carve_lag_secs).max(0));
    write_watermark(&config.watermark_file, watermark).context("initializing search watermark")?;

    Ok(tokio::spawn(async move {
        run_follow_loop(pool, server, config, watermark).await;
    }))
}

async fn run_follow_loop(
    pool: PgPool,
    server: SearchServer,
    config: FollowConfig,
    mut watermark: i64,
) {
    let mut consecutive_failures: u32 = 0;
    info!(
        watermark,
        interval_secs = config.interval_secs,
        carve_lag_secs = config.carve_lag_secs,
        max_window_secs = config.max_window_secs,
        batch_size = config.batch_size,
        deleted_limit = config.deleted_limit,
        watermark_file = %config.watermark_file.display(),
        "search follow loop starting"
    );

    loop {
        let now_epoch = current_epoch();
        let Some(window) = carve_window_with_max(
            watermark,
            now_epoch,
            config.carve_lag_secs,
            config.max_window_secs,
        ) else {
            tokio::time::sleep(Duration::from_secs(config.interval_secs)).await;
            continue;
        };
        let backlog_remaining = window_has_backlog(window, now_epoch, config.carve_lag_secs);

        let started = Instant::now();
        match follow_tick(
            &pool,
            &server,
            window.since,
            window.until,
            config.batch_size,
            config.deleted_limit,
        )
        .await
        .and_then(|stats| {
            write_watermark(&config.watermark_file, window.until)
                .context("writing search follow watermark")?;
            Ok(stats)
        }) {
            Ok(stats) => {
                watermark = window.until;
                consecutive_failures = 0;
                info!(
                    since = stats.since,
                    until = stats.until,
                    upserts = stats.upserts,
                    documents = stats.documents,
                    deletes = stats.deletes,
                    skips = stats.skips,
                    blob_errors = stats.blob_errors,
                    info_hash_decode_skips = stats.info_hash_decode_skips,
                    stale_tombstones_skipped = stats.stale_tombstones_skipped,
                    commits_performed = stats.commits_performed,
                    backlog_remaining,
                    duration_ms = started.elapsed().as_millis() as u64,
                    "search follow tick complete"
                );
            }
            Err(error) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                let backoff = follow_backoff_secs(config.interval_secs, consecutive_failures);
                warn!(
                    error = format!("{error:#}"),
                    since = window.since,
                    until = window.until,
                    consecutive_failures,
                    backoff_secs = backoff,
                    duration_ms = started.elapsed().as_millis() as u64,
                    "search follow tick failed; backing off"
                );
            }
        }

        tokio::time::sleep(Duration::from_secs(follow_sleep_secs(
            config.interval_secs,
            consecutive_failures,
            backlog_remaining,
        )))
        .await;
    }
}

/// Build and commit one follow window.
///
/// Changed-key pages are committed incrementally to bound writer-side memory.
/// Tombstones remain a window-final phase: all changed keys are collected before
/// tombstones are filtered, and the caller only advances the watermark after this
/// whole function succeeds.
async fn follow_tick(
    pool: &PgPool,
    server: &SearchServer,
    since: i64,
    until: i64,
    batch_size: i64,
    deleted_limit: i64,
) -> anyhow::Result<FollowStats> {
    let mut stats = FollowStats::new(since, until);
    let mut upserted_in_window: HashSet<Vec<u8>> = HashSet::new();

    let mut cursor: Option<ChangedTorrentKey> = None;
    loop {
        let page = stream_changed_torrent_keys(pool, since, until, cursor.as_ref(), batch_size)
            .await
            .context("reading changed torrent keys")?;
        if page.is_empty() {
            break;
        }

        for key in &page {
            upserted_in_window.insert(key.info_hash.as_slice().to_vec());
        }

        let page_replacements = read_replacements(pool, &page, batch_size, &mut stats)
            .await
            .context("reading current torrent_contents for changed torrents")?;
        for replacement in &page_replacements {
            if replacement.documents.is_empty() {
                stats.skips += 1;
            } else {
                stats.upserts += 1;
                stats.documents += replacement.documents.len() as u64;
            }
        }
        commit_follow_batch(
            server,
            &page_replacements,
            &[],
            &mut stats,
            "committing search follow changed-key page",
        )
        .await?;
        cursor = page.last().cloned();
    }

    let deleted_read_limit = deleted_read_limit_with_sentinel(deleted_limit);
    let deleted = read_deleted_torrents(pool, since, until, deleted_read_limit)
        .await
        .context("reading deleted torrents")?;
    if deleted_read_truncated(deleted.len(), deleted_limit) {
        anyhow::bail!(
            "deleted_torrents window hit limit {}; refusing to advance watermark",
            deleted_limit
        );
    }

    let total_deleted = deleted.len();
    let live = live_tombstones(deleted, &upserted_in_window);
    stats.stale_tombstones_skipped = (total_deleted - live.len()) as u64;
    stats.deletes = live.len() as u64;
    let deleted_info_hashes: Vec<Vec<u8>> = live
        .into_iter()
        .map(|info_hash| info_hash.as_slice().to_vec())
        .collect();

    if !deleted_info_hashes.is_empty() {
        commit_follow_batch(
            server,
            &[],
            &deleted_info_hashes,
            &mut stats,
            "committing search follow tombstones",
        )
        .await?;
    }

    Ok(stats)
}

async fn commit_follow_batch(
    server: &SearchServer,
    replacements: &[TorrentDocumentReplacement],
    deleted_info_hashes: &[Vec<u8>],
    stats: &mut FollowStats,
    context: &'static str,
) -> anyhow::Result<()> {
    server
        .apply_follow_batch(replacements, deleted_info_hashes)
        .await
        .context(context)?;
    stats.commits_performed += 1;
    Ok(())
}

async fn read_replacements(
    pool: &PgPool,
    keys: &[ChangedTorrentKey],
    batch_size: i64,
    stats: &mut FollowStats,
) -> anyhow::Result<Vec<TorrentDocumentReplacement>> {
    let mut docs_by_hash: BTreeMap<Vec<u8>, Vec<TorrentDocument>> = BTreeMap::new();
    for key in keys {
        docs_by_hash
            .entry(key.info_hash.as_slice().to_vec())
            .or_default();
    }

    let info_hashes: Vec<InfoHash> = keys.iter().map(|key| key.info_hash).collect();
    let mut cursor: Option<String> = None;
    loop {
        let page = stream_torrents_for_index_info_hashes(
            pool,
            &info_hashes,
            cursor.as_deref(),
            batch_size,
        )
        .await
        .context("reading scoped torrent_contents page")?;
        stats.info_hash_decode_skips += page.skipped_info_hash_decodes;

        if page.is_empty() {
            if let Some(last_seen_id) = page.last_seen_id {
                cursor = Some(last_seen_id);
                continue;
            }
            break;
        }

        for row in &page {
            let files = decode_files(row, stats);
            let document = build_document(row, &files);
            docs_by_hash
                .entry(row.info_hash.as_slice().to_vec())
                .or_default()
                .push(document);
        }
        cursor = page.last_seen_id;
    }

    Ok(docs_by_hash
        .into_iter()
        .map(|(info_hash, documents)| TorrentDocumentReplacement {
            info_hash,
            documents,
        })
        .collect())
}

fn decode_files(row: &TorrentForIndex, stats: &mut FollowStats) -> Vec<BlobFile> {
    match row.files() {
        Ok(files) => files,
        Err(error) => {
            stats.blob_errors += 1;
            warn!(info_hash = %row.info_hash, %error, "skipping undecodable file blob");
            Vec::new()
        }
    }
}

/// Compute the next lagged carve window. Returns `None` until lagged now has
/// moved beyond the durable watermark.
#[must_use]
pub fn carve_window(watermark: i64, now_epoch: i64, carve_lag_secs: i64) -> Option<FollowWindow> {
    carve_window_with_max(
        watermark,
        now_epoch,
        carve_lag_secs,
        DEFAULT_FOLLOW_MAX_WINDOW_SECS,
    )
}

/// Compute the next lagged carve window, capped to a bounded width.
#[must_use]
pub fn carve_window_with_max(
    watermark: i64,
    now_epoch: i64,
    carve_lag_secs: i64,
    max_window_secs: i64,
) -> Option<FollowWindow> {
    let lagged_until = lagged_until(now_epoch, carve_lag_secs);
    let max_until = watermark.saturating_add(max_window_secs.max(1));
    let until = lagged_until.min(max_until);
    (until > watermark).then_some(FollowWindow {
        since: watermark,
        until,
    })
}

fn lagged_until(now_epoch: i64, carve_lag_secs: i64) -> i64 {
    let lag = carve_lag_secs.max(0);
    now_epoch.saturating_sub(lag).max(0)
}

fn window_has_backlog(window: FollowWindow, now_epoch: i64, carve_lag_secs: i64) -> bool {
    window.until < lagged_until(now_epoch, carve_lag_secs)
}

/// Seconds to sleep before the next follow tick. On success
/// (`consecutive_failures == 0`) this is the base interval; after the nth
/// consecutive failure it is `min(interval * 2^(n-1), 300)`.
#[must_use]
pub fn follow_backoff_secs(interval_secs: u64, consecutive_failures: u32) -> u64 {
    let interval_secs = interval_secs.max(1);
    if consecutive_failures == 0 {
        return interval_secs;
    }
    let factor = 1u64
        .checked_shl(consecutive_failures - 1)
        .unwrap_or(u64::MAX);
    interval_secs
        .saturating_mul(factor)
        .min(FOLLOW_MAX_BACKOFF_SECS)
}

/// Seconds to sleep after a tick. A successful capped window should immediately
/// carve the next backlog slice; failures still use exponential backoff.
#[must_use]
pub fn follow_sleep_secs(
    interval_secs: u64,
    consecutive_failures: u32,
    backlog_remaining: bool,
) -> u64 {
    if consecutive_failures == 0 && backlog_remaining {
        0
    } else {
        follow_backoff_secs(interval_secs, consecutive_failures)
    }
}

fn deleted_read_truncated(found: usize, deleted_limit: i64) -> bool {
    deleted_limit > 0 && found > deleted_limit as usize
}

fn deleted_read_limit_with_sentinel(deleted_limit: i64) -> i64 {
    deleted_limit.max(1).saturating_add(1)
}

/// Drop stale tombstones for torrents that currently exist and were rebuilt
/// earlier in the same window.
fn live_tombstones(deleted: Vec<InfoHash>, upserted: &HashSet<Vec<u8>>) -> Vec<InfoHash> {
    deleted
        .into_iter()
        .filter(|info_hash| !upserted.contains(info_hash.as_slice()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        carve_window, carve_window_with_max, commit_follow_batch, deleted_read_limit_with_sentinel,
        deleted_read_truncated, follow_backoff_secs, follow_sleep_secs, follow_tick,
        live_tombstones, window_has_backlog, FollowStats, FollowWindow, DEFAULT_CARVE_LAG_SECS,
    };
    use crate::proto::search_service_server::SearchService;
    use crate::proto::{ContentType, HealthCheckRequest, TorrentDocument};
    use crate::server::TorrentDocumentReplacement;
    use bitmagnet_db::{connect, DbConfig};
    use bitmagnet_model::InfoHash;
    use std::collections::HashSet;
    use tonic::Request;

    fn info_hash(byte: u8) -> InfoHash {
        InfoHash::from_slice(&[byte; 20]).unwrap()
    }

    fn doc(info_hash: Vec<u8>, name: &str, content_id: &str) -> TorrentDocument {
        TorrentDocument {
            info_hash,
            torrent_name: name.to_owned(),
            content_title: name.to_owned(),
            original_title: String::new(),
            release_year: 2022,
            video_resolution: "1080p".to_owned(),
            video_source: "BluRay".to_owned(),
            video_codec: "x264".to_owned(),
            genres: vec!["action".to_owned()],
            file_paths: vec![format!("{name}.mkv")],
            content_type: ContentType::Movie as i32,
            seeders: 10,
            leechers: 1,
            files_count: 1,
            size: 1_000_000,
            published_at: 1_600_000_000,
            languages: vec!["en".to_owned()],
            file_extensions: vec!["mkv".to_owned()],
            video_3d: String::new(),
            video_modifier: String::new(),
            release_group: "GRP".to_owned(),
            audio_languages: vec!["en".to_owned()],
            content_source: "tmdb".to_owned(),
            content_id: content_id.to_owned(),
        }
    }

    async fn count(server: &crate::SearchServer) -> u64 {
        server
            .health_check(Request::new(HealthCheckRequest {}))
            .await
            .expect("health_check ok")
            .into_inner()
            .doc_count
    }

    #[test]
    fn carve_window_uses_lagged_now_and_waits_when_not_ready() {
        assert_eq!(carve_window(100, 129, 30), None);
        assert_eq!(
            carve_window(100, 130, 30),
            None,
            "the window is half-open and only advances when until > watermark"
        );
        assert_eq!(
            carve_window(100, 131, 30),
            Some(FollowWindow {
                since: 100,
                until: 101
            })
        );
    }

    #[test]
    fn carve_window_saturates_lagged_now_at_zero() {
        assert_eq!(carve_window(0, 10, 30), None);
        assert_eq!(
            carve_window(0, 31, 30),
            Some(FollowWindow { since: 0, until: 1 })
        );
    }

    #[test]
    fn carve_window_caps_huge_since_gap() {
        assert_eq!(
            carve_window_with_max(100, 10_000, 30, 3_600),
            Some(FollowWindow {
                since: 100,
                until: 3_700
            })
        );
    }

    #[test]
    fn capped_windows_advance_consecutively_across_backlog() {
        let now = 10_000;
        let lag = 30;
        let max_window = 3_600;

        let first = carve_window_with_max(0, now, lag, max_window).expect("first window");
        assert_eq!(
            first,
            FollowWindow {
                since: 0,
                until: 3_600
            }
        );
        assert!(window_has_backlog(first, now, lag));

        let second =
            carve_window_with_max(first.until, now, lag, max_window).expect("second window");
        assert_eq!(
            second,
            FollowWindow {
                since: 3_600,
                until: 7_200
            }
        );
        assert!(window_has_backlog(second, now, lag));

        let third =
            carve_window_with_max(second.until, now, lag, max_window).expect("third window");
        assert_eq!(
            third,
            FollowWindow {
                since: 7_200,
                until: 9_970
            }
        );
        assert!(!window_has_backlog(third, now, lag));
    }

    #[test]
    fn backoff_is_base_interval_when_no_failures() {
        assert_eq!(follow_backoff_secs(15, 0), 15);
        assert_eq!(follow_backoff_secs(60, 0), 60);
    }

    #[test]
    fn backoff_doubles_each_consecutive_failure() {
        assert_eq!(follow_backoff_secs(15, 1), 15);
        assert_eq!(follow_backoff_secs(15, 2), 30);
        assert_eq!(follow_backoff_secs(15, 3), 60);
        assert_eq!(follow_backoff_secs(15, 4), 120);
        assert_eq!(follow_backoff_secs(15, 5), 240);
    }

    #[test]
    fn backoff_caps_at_300_seconds_and_saturates() {
        assert_eq!(follow_backoff_secs(15, 6), 300);
        assert_eq!(follow_backoff_secs(60, 4), 300);
        assert_eq!(follow_backoff_secs(15, 100), 300);
        assert_eq!(follow_backoff_secs(u64::MAX, u32::MAX), 300);
    }

    #[test]
    fn capped_window_success_skips_full_interval_sleep() {
        assert_eq!(follow_sleep_secs(15, 0, true), 0);
        assert_eq!(follow_sleep_secs(15, 0, false), 15);
        assert_eq!(
            follow_sleep_secs(15, 2, true),
            30,
            "failed ticks still use backoff even when backlog remains"
        );
    }

    #[test]
    fn deleted_read_truncation_uses_limit_plus_one_sentinel() {
        assert_eq!(deleted_read_limit_with_sentinel(100), 101);
        assert!(!deleted_read_truncated(0, 100));
        assert!(!deleted_read_truncated(99, 100));
        assert!(
            !deleted_read_truncated(100, 100),
            "an exactly-full page is complete because the query asks for limit + 1"
        );
        assert!(deleted_read_truncated(101, 100));
        assert!(!deleted_read_truncated(100, 0));
    }

    #[test]
    fn live_tombstones_drops_hashes_rebuilt_in_same_window() {
        let deleted = vec![info_hash(1), info_hash(2), info_hash(3)];
        let upserted: HashSet<Vec<u8>> = [info_hash(2).as_slice().to_vec()].into_iter().collect();
        let live = live_tombstones(deleted, &upserted);
        assert_eq!(
            live.into_iter()
                .map(|info_hash| info_hash.as_slice()[0])
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    #[tokio::test]
    async fn changed_key_pages_commit_incrementally_through_real_apply_path() {
        let server = crate::SearchServer::in_ram().expect("in-ram search server");
        let mut stats = FollowStats::new(10, 20);

        for byte in [1_u8, 2, 3] {
            let info_hash = vec![byte; 20];
            let page = vec![TorrentDocumentReplacement {
                info_hash: info_hash.clone(),
                documents: vec![doc(
                    info_hash,
                    &format!("Torrent {byte}"),
                    &byte.to_string(),
                )],
            }];
            commit_follow_batch(&server, &page, &[], &mut stats, "test changed-key page")
                .await
                .expect("page commit");
        }

        assert_eq!(stats.commits_performed, 3);
        assert!(
            stats.commits_performed > 1,
            "a three-page window must not collapse into one Tantivy commit"
        );
        assert_eq!(count(&server).await, 3);
    }

    /// A single main-search follow tick against live PostgreSQL. Ignored by
    /// default because CI has no database:
    ///
    /// ```sh
    /// BITMAGNET_POSTGRES_DSN=postgres://postgres@localhost/bitmagnet \
    ///   cargo test -p bitmagnet-search follow::tests::one_follow_tick_against_live_postgres -- --ignored
    /// ```
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL (set BITMAGNET_POSTGRES_DSN)"]
    async fn one_follow_tick_against_live_postgres() {
        let cfg = DbConfig::from_env().expect("postgres config from env");
        let pool = connect(&cfg).await.expect("connect to postgres");
        let server = crate::SearchServer::in_ram().expect("in-ram search server");

        let until = super::current_epoch().saturating_sub(DEFAULT_CARVE_LAG_SECS);
        let since = until.saturating_sub(1);
        let stats = follow_tick(&pool, &server, since, until, 100, 1000)
            .await
            .expect("one follow tick");
        assert_eq!(stats.since, since);
        assert_eq!(stats.until, until);
    }
}
