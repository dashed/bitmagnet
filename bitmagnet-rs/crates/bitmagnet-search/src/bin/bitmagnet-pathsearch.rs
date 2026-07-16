//! Entry point for the L3 pathsearch sidecar.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bitmagnet_common::metrics::maybe_spawn_metrics_server;
use bitmagnet_common::serve::{serve_router, shutdown_signal, Listen};
use bitmagnet_db::{
    connect, read_deleted_torrents, stream_changed_torrents, DbConfig, PgPool, TorrentWithBlob,
};
use bitmagnet_model::InfoHash;
use bitmagnet_proto::v1::path_search_service_server::PathSearchServiceServer;
use bitmagnet_search::pathsearch::document::PathDocument;
use bitmagnet_search::pathsearch::index::DEFAULT_WRITER_HEAP_BYTES;
use bitmagnet_search::pathsearch::server::PathSearchMutation;
use bitmagnet_search::pathsearch::watermark::{current_epoch, read_watermark, write_watermark};
use bitmagnet_search::pathsearch::{
    PathSearchServer, PrefixIndex, PrefixIndexConfig, PREFIX_INDEX_FILENAME,
};
use clap::Parser;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tracing::{info, warn};

/// `bitmagnet-pathsearch` — the L3 path-bag candidate sidecar.
#[derive(Debug, Parser)]
#[command(
    name = "bitmagnet-pathsearch",
    about = "Tantivy-backed L3 pathsearch candidate sidecar"
)]
struct Args {
    /// Listen address. HOST:PORT starts TCP; any other value, optionally
    /// prefixed `unix:`, is treated as a Unix-domain-socket path.
    #[arg(
        long,
        env = "BITMAGNET_PATHSEARCH_ADDR",
        default_value = "127.0.0.1:50053"
    )]
    addr: String,

    /// Directory holding the pathsearch Tantivy index.
    #[arg(
        long,
        env = "BITMAGNET_PATHSEARCH_INDEX",
        default_value = "/var/lib/bitmagnet/pathsearch"
    )]
    index_path: PathBuf,

    /// Total writer heap in MiB.
    #[arg(long, default_value_t = DEFAULT_WRITER_HEAP_BYTES / 1024 / 1024)]
    writer_heap_mb: usize,

    /// Tantivy writer threads. Keep 1 for ngram follow/backfill unless re-tested.
    #[arg(long, default_value_t = 1)]
    writer_threads: usize,

    /// Enable the PG-tail follow loop.
    #[arg(long, env = "BITMAGNET_PATHSEARCH_FOLLOW", default_value_t = false)]
    follow: bool,

    /// PostgreSQL DSN for follow mode. When empty, BITMAGNET_POSTGRES_* env vars
    /// are used.
    #[arg(long, default_value = "")]
    postgres_dsn: String,

    /// Follow poll interval in seconds.
    #[arg(long, env = "BITMAGNET_PATHSEARCH_FOLLOW_SECS", default_value_t = 15)]
    follow_secs: u64,

    /// Commit-visibility lag for the follow window upper bound.
    #[arg(
        long,
        env = "BITMAGNET_PATHSEARCH_CARVE_LAG_SECS",
        default_value_t = 30
    )]
    carve_lag_secs: i64,

    /// Changed-torrent page size for follow mode.
    #[arg(long, default_value_t = 1000)]
    follow_batch_size: i64,

    /// Deleted-torrent runaway guard for one follow window.
    #[arg(long, default_value_t = 100_000)]
    deleted_limit: i64,

    /// Watermark file for follow mode.
    #[arg(
        long,
        env = "BITMAGNET_PATHSEARCH_WATERMARK_FILE",
        default_value = "/var/lib/bitmagnet/pathsearch/watermark"
    )]
    watermark_file: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bitmagnet_common::init_tracing();
    let args = Args::parse();
    let heap = args.writer_heap_mb * 1024 * 1024;
    let suggest_path = args.index_path.join(PREFIX_INDEX_FILENAME);
    let prefix = PrefixIndex::open(&suggest_path, PrefixIndexConfig::default())
        .with_context(|| format!("opening prefix index at {}", suggest_path.display()))?
        .map(Arc::new);
    match &prefix {
        Some(index) => info!(
            entries = index.len(),
            path = %suggest_path.display(),
            "prefix index loaded"
        ),
        None => info!(
            path = %suggest_path.display(),
            "prefix index absent — Suggest disabled"
        ),
    }
    let server = PathSearchServer::open(&args.index_path, heap, args.writer_threads, prefix)
        .with_context(|| format!("opening pathsearch index at {}", args.index_path.display()))?;
    let _metrics_server = maybe_start_metrics().await?;

    // Prefix suggestions are intentionally rebuilt only by backfill. Typeahead
    // tolerates staleness; the backend can fall back to live path candidates.
    let _follow_task = if args.follow {
        Some(spawn_follow_loop(&args, server.clone()).await?)
    } else {
        None
    };

    let service = PathSearchServiceServer::new(server);
    info!(
        index_path = %args.index_path.display(),
        follow = args.follow,
        "bitmagnet-pathsearch starting"
    );

    let listen = Listen::parse(&args.addr);
    let termination_context = match &listen {
        Listen::Tcp(_) => "gRPC server (TCP) terminated with an error",
        Listen::Unix(_) => "gRPC server (unix) terminated with an error",
    };
    let router = Server::builder().add_service(service);
    serve_router(router, listen, shutdown_signal())
        .await
        .context(termination_context)?;

    info!("bitmagnet-pathsearch stopped");
    Ok(())
}

async fn maybe_start_metrics(
) -> anyhow::Result<Option<(tokio::task::JoinHandle<()>, std::net::SocketAddr)>> {
    maybe_spawn_metrics_server()
        .await
        .context("starting Prometheus metrics listener")
}

async fn spawn_follow_loop(
    args: &Args,
    server: PathSearchServer,
) -> anyhow::Result<JoinHandle<()>> {
    let mut cfg = DbConfig::from_env().context("reading postgres config from env")?;
    if !args.postgres_dsn.is_empty() {
        cfg.dsn = args.postgres_dsn.clone();
    }
    let pool = connect(&cfg).await.context("connecting to postgres")?;
    let watermark_file = args.watermark_file.clone();
    let mut watermark = read_watermark(&watermark_file)
        .unwrap_or_else(|| current_epoch().saturating_sub(args.carve_lag_secs));
    server.set_watermark_epoch(watermark);
    write_watermark(&watermark_file, watermark)?;

    let follow_secs = args.follow_secs.max(1);
    let carve_lag_secs = args.carve_lag_secs.max(1);
    let batch_size = args.follow_batch_size.max(1);
    let deleted_limit = args.deleted_limit.max(1);

    Ok(tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(follow_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let until = current_epoch().saturating_sub(carve_lag_secs);
            if until <= watermark {
                continue;
            }
            match follow_window(&pool, &server, watermark, until, batch_size, deleted_limit).await {
                Ok(stats) => {
                    // `follow_window` returns only after every changed page and
                    // the final live-tombstone batch commit and reload. Any
                    // batch failure takes the error arm and leaves both
                    // watermarks at the previous successful boundary.
                    watermark = stats.until;
                    server.set_watermark_epoch(watermark);
                    if let Err(error) = write_watermark(&watermark_file, watermark) {
                        warn!(%error, path = %watermark_file.display(), "failed writing pathsearch watermark");
                    }
                    info!(
                        since = stats.since,
                        until = stats.until,
                        upserts = stats.upserts,
                        deletes = stats.deletes,
                        skips = stats.skips,
                        blob_errors = stats.blob_errors,
                        stale_tombstones_skipped = stats.stale_tombstones_skipped,
                        "pathsearch follow tick complete"
                    );
                }
                Err(error) => {
                    warn!(%error, since = watermark, until, "pathsearch follow tick failed");
                }
            }
        }
    }))
}

#[derive(Debug)]
struct FollowStats {
    since: i64,
    until: i64,
    upserts: u64,
    deletes: u64,
    skips: u64,
    blob_errors: u64,
    /// Tombstones skipped because the torrent was (re-)upserted earlier in the
    /// same window — a stale `deleted_torrents` row from a delete-then-re-add.
    stale_tombstones_skipped: u64,
}

#[derive(Debug, Default)]
struct PreparedChangedBatch {
    upserted_info_hashes: Vec<Vec<u8>>,
    upserts: u64,
    skips: u64,
    blob_errors: u64,
}

async fn follow_window(
    pool: &PgPool,
    server: &PathSearchServer,
    since: i64,
    until: i64,
    batch_size: i64,
    deleted_limit: i64,
) -> anyhow::Result<FollowStats> {
    let mut stats = FollowStats {
        since,
        until,
        upserts: 0,
        deletes: 0,
        skips: 0,
        blob_errors: 0,
        stale_tombstones_skipped: 0,
    };

    // Torrents (re-)upserted in this window currently exist in `torrents`, so any
    // `deleted_torrents` row for them is a stale leftover from an earlier
    // delete-then-re-add in the same window. We apply deletes after upserts, so
    // without this guard the stale delete would evict the fresh doc.
    let mut upserted_in_window: HashSet<Vec<u8>> = HashSet::new();

    let mut cursor: Option<InfoHash> = None;
    loop {
        let page = stream_changed_torrents(pool, since, until, cursor.as_ref(), batch_size)
            .await
            .context("reading changed torrents")?;
        if page.is_empty() {
            break;
        }
        let applied = apply_changed_batch(server, &page).await?;
        cursor = page.last().map(|row| row.info_hash);
        upserted_in_window.extend(applied.upserted_info_hashes);
        stats.upserts = stats.upserts.saturating_add(applied.upserts);
        stats.skips = stats.skips.saturating_add(applied.skips);
        stats.blob_errors = stats.blob_errors.saturating_add(applied.blob_errors);
    }

    let deleted = read_deleted_torrents(pool, since, until, deleted_limit)
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
    let delete_count = u64::try_from(live.len()).unwrap_or(u64::MAX);
    server
        .apply_follow_batch(
            live.iter()
                .map(|info_hash| PathSearchMutation::Delete(info_hash.as_slice())),
        )
        .await
        .context("applying live path-bag tombstone batch")?;
    stats.deletes = delete_count;

    Ok(stats)
}

/// Drop tombstones for torrents that were upserted earlier in the same window.
///
/// A torrent present in `torrents` (hence upsertable) cannot be a live tombstone:
/// its existence supersedes a stale `deleted_torrents` row left by an earlier
/// delete-then-re-add inside this window. This can never mask a legitimate
/// deletion — a truly-deleted torrent is gone from `torrents`, so it never
/// appears in the changed stream and is never in `upserted`.
#[must_use]
fn live_tombstones(deleted: Vec<InfoHash>, upserted: &HashSet<Vec<u8>>) -> Vec<InfoHash> {
    deleted
        .into_iter()
        .filter(|ih| !upserted.contains(ih.as_slice()))
        .collect()
}

/// Prepare and apply one fetched page as a single commit-visible index batch.
///
/// All replacements are queued before tombstones, and the server commits and
/// reloads once for the whole page. A torrent without path text — or with an
/// undecodable blob — is tombstoned so a re-crawl cannot leave a stale
/// candidate behind.
///
/// # Errors
/// Returns Tantivy write/commit/reload failures from the server batch primitive.
async fn apply_changed_batch(
    server: &PathSearchServer,
    rows: &[TorrentWithBlob],
) -> anyhow::Result<PreparedChangedBatch> {
    let mut batch = PreparedChangedBatch::default();
    let mutations = rows
        .iter()
        .map(|row| match PathDocument::from_torrent(row) {
            Ok(Some(document)) => {
                batch.upserted_info_hashes.push(document.info_hash.clone());
                batch.upserts = batch.upserts.saturating_add(1);
                PathSearchMutation::Replace(document)
            }
            Ok(None) => {
                batch.skips = batch.skips.saturating_add(1);
                PathSearchMutation::Delete(row.info_hash.as_slice())
            }
            Err(error) => {
                batch.blob_errors = batch.blob_errors.saturating_add(1);
                warn!(info_hash = %row.info_hash, %error, "changed torrent has undecodable blob");
                PathSearchMutation::Delete(row.info_hash.as_slice())
            }
        });

    server
        .apply_follow_batch(mutations)
        .await
        .context("applying changed path-bag batch")?;
    Ok(batch)
}

/// Whether a deleted-torrent read was truncated by its runaway guard.
///
/// A truncated read means the window is incomplete, so the follow loop MUST NOT
/// advance the watermark — otherwise the un-read tombstones are lost forever
/// (the watermark would skip past them). Equality counts as truncation: a read
/// that returns exactly `deleted_limit` rows may have more behind the `LIMIT`.
#[must_use]
fn deleted_read_truncated(found: usize, deleted_limit: i64) -> bool {
    deleted_limit > 0 && found as i64 >= deleted_limit
}

#[cfg(test)]
mod tests {
    use super::{
        apply_changed_batch, current_epoch, deleted_read_truncated, live_tombstones,
        maybe_start_metrics,
    };
    use bitmagnet_db::TorrentWithBlob;
    use bitmagnet_model::{serialize_files, BlobFile, InfoHash};
    use bitmagnet_search::pathsearch::PathSearchServer;
    use bitmagnet_search::proto::path_search_service_server::PathSearchService;
    use bitmagnet_search::proto::{HealthCheckRequest, PathCandidatesRequest};
    use std::collections::HashSet;
    use std::ffi::OsString;
    use tonic::Request;

    struct MetricsAddrRestore(Option<OsString>);

    impl MetricsAddrRestore {
        fn clear() -> Self {
            let original = std::env::var_os("BITMAGNET_METRICS_ADDR");
            std::env::remove_var("BITMAGNET_METRICS_ADDR");
            Self(original)
        }
    }

    impl Drop for MetricsAddrRestore {
        fn drop(&mut self) {
            match self.0.take() {
                Some(value) => std::env::set_var("BITMAGNET_METRICS_ADDR", value),
                None => std::env::remove_var("BITMAGNET_METRICS_ADDR"),
            }
        }
    }

    fn changed_row(byte: u8, files_status: &str, files_data: Option<Vec<u8>>) -> TorrentWithBlob {
        TorrentWithBlob {
            info_hash: InfoHash::from_slice(&[byte; 20]).unwrap(),
            name: "Release.Name.mkv".to_owned(),
            size: 100,
            files_status: files_status.to_owned(),
            files_count: None,
            published_at: 1_600_000_000,
            files_data,
        }
    }

    fn blob(path: &str) -> Vec<u8> {
        serialize_files(&[BlobFile {
            index: 0,
            path: path.to_owned(),
            extension: "mkv".to_owned(),
            size: 100,
        }])
        .unwrap()
    }

    async fn doc_count(server: &PathSearchServer) -> u64 {
        server
            .health_check(Request::new(HealthCheckRequest {}))
            .await
            .unwrap()
            .into_inner()
            .doc_count
    }

    async fn candidate_total(server: &PathSearchServer, query: &str) -> u64 {
        server
            .path_candidates(Request::new(PathCandidatesRequest {
                query: query.to_owned(),
                limit: 50,
                oversample: 0,
                sort: Vec::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .candidate_total
    }

    #[tokio::test]
    async fn metrics_listener_is_disabled_by_default() {
        let _restore = MetricsAddrRestore::clear();
        let server = maybe_start_metrics()
            .await
            .expect("an unset metrics address is valid");

        assert!(server.is_none(), "no metrics listener should be created");
    }

    /// The changed-torrent path is the follow loop's core invariant: a torrent
    /// with paths is upserted (delete_term(info_hash)+add), a re-crawl that
    /// changes the fileset supersedes in place (doc_count stays 1; old paths
    /// gone, new present), and a torrent that loses all path text is tombstoned.
    #[tokio::test]
    async fn apply_changed_batch_upserts_supersedes_and_tombstones() {
        let server = PathSearchServer::in_ram().unwrap();

        let batch = apply_changed_batch(
            &server,
            &[changed_row(
                1,
                "multi",
                Some(blob("Season01/Old.Episode.mkv")),
            )],
        )
        .await
        .unwrap();
        assert_eq!(batch.upserts, 1);
        assert_eq!(batch.skips, 0);
        assert_eq!(batch.blob_errors, 0);
        assert_eq!(doc_count(&server).await, 1);
        assert_eq!(candidate_total(&server, "old.episode").await, 1);

        // Re-crawl with a new fileset: supersede via delete_term + add. The
        // torrent must not duplicate, and the old path must no longer match.
        let batch = apply_changed_batch(
            &server,
            &[changed_row(
                1,
                "multi",
                Some(blob("Season02/New.Feature.mkv")),
            )],
        )
        .await
        .unwrap();
        assert_eq!(batch.upserts, 1);
        assert_eq!(
            doc_count(&server).await,
            1,
            "supersession must not duplicate the torrent doc"
        );
        assert_eq!(
            candidate_total(&server, "old.episode").await,
            0,
            "old path must be gone after supersession"
        );
        assert_eq!(candidate_total(&server, "new.feature").await, 1);

        // The torrent loses its files (multi-file, no blob): tombstone it so no
        // stale candidate survives.
        let batch = apply_changed_batch(&server, &[changed_row(1, "multi", None)])
            .await
            .unwrap();
        assert_eq!(batch.upserts, 0);
        assert_eq!(batch.skips, 1);
        assert_eq!(batch.blob_errors, 0);
        assert_eq!(doc_count(&server).await, 0);
    }

    /// An undecodable blob tombstones the torrent rather than leaving it
    /// pointing at a corrupt path-bag.
    #[tokio::test]
    async fn apply_changed_batch_tombstones_on_corrupt_blob() {
        let server = PathSearchServer::in_ram().unwrap();
        apply_changed_batch(&server, &[changed_row(2, "multi", Some(blob("Keep.mkv")))])
            .await
            .unwrap();
        assert_eq!(doc_count(&server).await, 1);

        let batch = apply_changed_batch(
            &server,
            &[changed_row(2, "multi", Some(b"not a zstd frame".to_vec()))],
        )
        .await
        .unwrap();
        assert_eq!(batch.upserts, 0);
        assert_eq!(batch.skips, 0);
        assert_eq!(batch.blob_errors, 1);
        assert_eq!(
            doc_count(&server).await,
            0,
            "corrupt blob must tombstone, not keep a stale candidate"
        );
    }

    #[tokio::test]
    async fn apply_changed_batch_handles_a_mixed_fetched_page() {
        let server = PathSearchServer::in_ram().unwrap();
        apply_changed_batch(
            &server,
            &[
                changed_row(2, "multi", Some(blob("Stale.Two.mkv"))),
                changed_row(3, "multi", Some(blob("Stale.Three.mkv"))),
            ],
        )
        .await
        .unwrap();

        let batch = apply_changed_batch(
            &server,
            &[
                changed_row(1, "multi", Some(blob("Fresh.One.mkv"))),
                changed_row(2, "multi", None),
                changed_row(3, "multi", Some(b"not a zstd frame".to_vec())),
                changed_row(4, "multi", Some(blob("Fresh.Four.mkv"))),
            ],
        )
        .await
        .unwrap();

        assert_eq!(batch.upserts, 2);
        assert_eq!(batch.skips, 1);
        assert_eq!(batch.blob_errors, 1);
        assert_eq!(
            batch.upserted_info_hashes,
            vec![vec![1; 20], vec![4; 20]],
            "the window-level stale-tombstone filter receives only successful upserts"
        );
        assert_eq!(doc_count(&server).await, 2);
        assert_eq!(candidate_total(&server, "fresh.one").await, 1);
        assert_eq!(candidate_total(&server, "fresh.four").await, 1);
        assert_eq!(candidate_total(&server, "stale.two").await, 0);
        assert_eq!(candidate_total(&server, "stale.three").await, 0);
    }

    /// The HARD-FAIL guard: a deleted-torrent read that reaches its runaway
    /// limit is truncated, so the watermark must NOT advance. Equality counts as
    /// truncated; a zero/disabled limit never truncates.
    #[test]
    fn deleted_read_truncation_is_detected_at_the_limit() {
        assert!(!deleted_read_truncated(0, 100));
        assert!(!deleted_read_truncated(99, 100));
        assert!(
            deleted_read_truncated(100, 100),
            "an exactly-full page may hide more behind LIMIT"
        );
        assert!(deleted_read_truncated(101, 100));
        assert!(
            !deleted_read_truncated(5, 0),
            "a disabled guard never reports truncation"
        );
    }

    /// A delete-then-re-add inside one follow window must not evict the fresh
    /// doc: a tombstone whose torrent was upserted this window is dropped, while
    /// genuine tombstones (never upserted) survive.
    #[test]
    fn live_tombstones_drops_same_window_readds() {
        let readded = InfoHash::from_slice(&[1; 20]).unwrap();
        let genuinely_deleted = InfoHash::from_slice(&[2; 20]).unwrap();

        let mut upserted: HashSet<Vec<u8>> = HashSet::new();
        upserted.insert(readded.as_slice().to_vec());

        let live = live_tombstones(vec![readded, genuinely_deleted], &upserted);
        assert_eq!(
            live,
            vec![genuinely_deleted],
            "the re-added torrent's stale tombstone is dropped; the real deletion survives"
        );

        // With no upserts, every tombstone is live.
        let live = live_tombstones(vec![readded, genuinely_deleted], &HashSet::new());
        assert_eq!(live, vec![readded, genuinely_deleted]);
    }

    /// End-to-end against a live PostgreSQL: a recent follow window must complete
    /// without error and echo its bounds (so the loop would advance the
    /// watermark to `until`). Read-only against PG; writes only to a throwaway
    /// index. Requires the `deleted_torrents` audit table. Ignored by default:
    ///
    /// ```sh
    /// BITMAGNET_POSTGRES_DSN=postgres://postgres@localhost/bitmagnet \
    ///   cargo test -p bitmagnet-search --bin bitmagnet-pathsearch -- --ignored
    /// ```
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL with the deleted_torrents audit table (set BITMAGNET_POSTGRES_DSN)"]
    async fn follow_window_processes_a_recent_window() {
        use super::follow_window;
        use bitmagnet_db::{connect, DbConfig};

        let cfg = DbConfig::from_env().expect("postgres config from env");
        let pool = connect(&cfg).await.expect("connect to postgres");

        let dir = std::env::temp_dir().join(format!(
            "bitmagnet-pathsearch-follow-it-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let server = PathSearchServer::open(&dir, 256 * 1024 * 1024, 1, None).expect("open server");

        let until = current_epoch() - 30;
        let since = until - 24 * 3600;
        let stats = follow_window(&pool, &server, since, until, 1000, 100_000)
            .await
            .expect("follow window completes");
        assert_eq!(stats.since, since);
        assert_eq!(stats.until, until);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// End-to-end against a live PostgreSQL: the follow loop's watermark-FILE
    /// hand-off ACROSS TWO TICKS. `spawn_follow_loop` seeds a lagged watermark
    /// file on startup, then after every successful `follow_window` publishes
    /// `stats.until` back to the file so the NEXT tick carves `[that, new_until]`
    /// — the carve origin moves strictly forward and a tick never re-carves from
    /// the old origin. This drives the full read -> window -> write -> re-read ->
    /// next-window loop that `follow_window_processes_a_recent_window` omits (it
    /// runs one window and never touches the file), and guards the exact
    /// "ticks must advance the origin" regression class that bit L2 (l2-7→l2-8).
    /// Read-only against PG; writes only a throwaway index + watermark file.
    /// Requires the `deleted_torrents` audit table. Ignored by default:
    ///
    /// ```sh
    /// BITMAGNET_POSTGRES_DSN=postgres://postgres@localhost/bitmagnet \
    ///   cargo test -p bitmagnet-search --bin bitmagnet-pathsearch -- --ignored
    /// ```
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL with the deleted_torrents audit table (set BITMAGNET_POSTGRES_DSN)"]
    async fn follow_watermark_file_advances_monotonically_across_ticks() {
        use super::follow_window;
        use bitmagnet_db::{connect, DbConfig};
        use bitmagnet_search::pathsearch::watermark::{read_watermark, write_watermark};

        let cfg = DbConfig::from_env().expect("postgres config from env");
        let pool = connect(&cfg).await.expect("connect to postgres");

        let dir = std::env::temp_dir().join(format!(
            "bitmagnet-pathsearch-followwm-it-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create index dir");
        let server = PathSearchServer::open(&dir, 256 * 1024 * 1024, 1, None).expect("open server");
        let watermark_file = dir.join("watermark");

        // Two non-overlapping windows derived from ONE clock read, so the second
        // window's upper bound is deterministically greater than the first's.
        let now = current_epoch();
        let origin = now - 2 * 3600; // seeded lagged start (2h ago)
        let until1 = now - 3600; // first tick upper bound (1h ago)
        let until2 = now - 30; // second tick upper bound (carve-lag boundary)

        // Startup, mirroring spawn_follow_loop: an absent file reads as None, so
        // the loop seeds a lagged origin and publishes it.
        assert_eq!(read_watermark(&watermark_file), None);
        write_watermark(&watermark_file, origin).expect("seed origin watermark");
        assert_eq!(read_watermark(&watermark_file), Some(origin));

        // TICK 1: carve [persisted origin, until1] against live PG, then publish
        // the new upper bound exactly as the follow loop's success arm does.
        let since1 = read_watermark(&watermark_file).expect("origin present");
        assert_eq!(since1, origin, "first tick carves from the seeded origin");
        let stats1 = follow_window(&pool, &server, since1, until1, 1000, 100_000)
            .await
            .expect("first follow window completes");
        assert_eq!(stats1.since, origin);
        assert_eq!(stats1.until, until1);
        write_watermark(&watermark_file, stats1.until).expect("advance watermark after tick 1");
        assert_eq!(
            read_watermark(&watermark_file),
            Some(until1),
            "the persisted watermark advanced to the first window's upper bound"
        );

        // TICK 2: MUST resume from the ADVANCED origin (until1), never re-carve
        // from the old origin. This is the l2-7→l2-8 guard.
        let since2 = read_watermark(&watermark_file).expect("advanced origin present");
        assert_eq!(
            since2, until1,
            "second tick resumes from the advanced origin, not the original seed"
        );
        assert_ne!(
            since2, origin,
            "a tick must never re-carve from the old origin"
        );
        let stats2 = follow_window(&pool, &server, since2, until2, 1000, 100_000)
            .await
            .expect("second follow window completes");
        assert_eq!(
            stats2.since, until1,
            "the second window's lower bound is the first window's upper bound"
        );
        assert_eq!(stats2.until, until2);
        write_watermark(&watermark_file, stats2.until).expect("advance watermark after tick 2");
        assert_eq!(read_watermark(&watermark_file), Some(until2));

        // Strict forward progress of the persisted origin across both ticks.
        assert!(
            origin < until1 && until1 < until2,
            "the persisted watermark advances strictly forward, never backward or in place"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
