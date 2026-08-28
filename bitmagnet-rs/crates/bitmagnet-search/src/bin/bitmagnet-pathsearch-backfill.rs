//! Build or refresh the L3 pathsearch index from production `files_data` blobs.

use std::path::{Path, PathBuf};

use anyhow::Context;
use bitmagnet_db::{connect, stream_torrents_with_files, DbConfig};
use bitmagnet_model::InfoHash;
use bitmagnet_search::pathsearch::document::PathDocument;
use bitmagnet_search::pathsearch::index::{open_or_create, writer, DEFAULT_WRITER_HEAP_BYTES};
use bitmagnet_search::pathsearch::indexer::{delete, upsert};
use bitmagnet_search::pathsearch::schema::Fields;
use bitmagnet_search::pathsearch::watermark::{current_epoch, write_watermark};
use bitmagnet_search::pathsearch::{PrefixIndexBuilder, PrefixIndexConfig, PREFIX_INDEX_FILENAME};
use clap::Parser;
use tracing::{info, warn};

/// `bitmagnet-pathsearch-backfill` — build the L3 path-bag index from blobs.
#[derive(Debug, Parser)]
#[command(
    name = "bitmagnet-pathsearch-backfill",
    about = "Backfill the L3 pathsearch Tantivy index from PostgreSQL blobs"
)]
struct Args {
    /// Directory holding the pathsearch Tantivy index.
    #[arg(
        long,
        env = "BITMAGNET_PATHSEARCH_INDEX",
        default_value = "/var/lib/bitmagnet/pathsearch"
    )]
    index_path: PathBuf,

    /// PostgreSQL DSN. When empty, BITMAGNET_POSTGRES_* env vars are used.
    #[arg(long, default_value = "")]
    postgres_dsn: String,

    /// Number of torrent rows fetched per keyset page.
    #[arg(long, default_value_t = 1000)]
    batch_size: i64,

    /// Stop after indexing this many path-bag documents.
    #[arg(long)]
    limit: Option<u64>,

    /// Commit every N indexed/deleted documents.
    #[arg(long, default_value_t = 10_000)]
    commit_interval: u64,

    /// Resume after this info hash (40-char hex).
    #[arg(long)]
    after_info_hash: Option<InfoHash>,

    /// Total writer heap in MiB.
    #[arg(long, default_value_t = DEFAULT_WRITER_HEAP_BYTES / 1024 / 1024)]
    writer_heap_mb: usize,

    /// Tantivy writer threads. Keep 1 for ngram backfills unless re-tested.
    #[arg(long, default_value_t = 1)]
    writer_threads: usize,

    /// Follow-loop watermark file to seed on FULL completion so the serving pod
    /// resumes from this backfill's snapshot epoch (no freshness gap). MUST match
    /// the server's BITMAGNET_PATHSEARCH_WATERMARK_FILE.
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
    run(Args::parse()).await
}

async fn run(args: Args) -> anyhow::Result<()> {
    anyhow::ensure!(args.batch_size > 0, "--batch-size must be positive");
    anyhow::ensure!(
        args.commit_interval > 0,
        "--commit-interval must be positive"
    );

    // Capture the snapshot epoch BEFORE the keyset scan begins. On full
    // completion this seeds the follow watermark, so the serving pod re-covers
    // torrents crawled DURING the backfill instead of skipping them. Captured
    // early on purpose: re-processing a slightly wider window is idempotent
    // (info_hash supersession); skipping torrents is not.
    let snapshot_epoch = current_epoch();

    let mut cfg = DbConfig::from_env().context("reading postgres config from env")?;
    if !args.postgres_dsn.is_empty() {
        cfg.dsn = args.postgres_dsn.clone();
    }
    let pool = connect(&cfg).await.context("connecting to postgres")?;

    let index = open_or_create(&args.index_path)?;
    let fields = Fields::from_schema(&index.schema()).context("resolving pathsearch fields")?;
    let heap = args.writer_heap_mb * 1024 * 1024;
    let mut index_writer =
        writer(&index, heap, args.writer_threads).context("allocating pathsearch writer")?;
    let suggest_path = args.index_path.join(PREFIX_INDEX_FILENAME);
    let prefix_cfg = prefix_config_from_env();
    let mut prefix_builder = PrefixIndexBuilder::new(prefix_cfg);

    let mut cursor = args.after_info_hash;
    let mut indexed = 0_u64;
    let mut skipped = 0_u64;
    let mut blob_errors = 0_u64;
    let mut since_commit = 0_u64;
    // Whether the keyset scan reached the end (vs stopping early on --limit).
    // Only a full scan may seed the follow watermark.
    let mut completed_full_scan = false;

    info!(
        index_path = %args.index_path.display(),
        batch_size = args.batch_size,
        limit = ?args.limit,
        start_after = ?cursor,
        suggest_path = %suggest_path.display(),
        suggest_max_tracked = prefix_cfg.max_tracked,
        suggest_min_freq = prefix_cfg.min_freq,
        suggest_max_entries = prefix_cfg.max_entries,
        "pathsearch backfill starting"
    );

    'pages: loop {
        let page = stream_torrents_with_files(&pool, cursor.as_ref(), args.batch_size)
            .await
            .context("reading torrents page")?;
        if page.is_empty() {
            completed_full_scan = true;
            break;
        }

        for row in &page {
            cursor = Some(row.info_hash);
            match PathDocument::from_torrent(row) {
                Ok(Some(doc)) => {
                    upsert(&index_writer, &fields, &doc).context("indexing path-bag")?;
                    prefix_builder.add_paths(&doc.paths);
                    indexed += 1;
                    since_commit += 1;
                }
                Ok(None) => {
                    // Keep reruns/supersessions correct: no path text means no
                    // candidate document should remain for this torrent.
                    delete(&index_writer, &fields, row.info_hash.as_slice());
                    skipped += 1;
                    since_commit += 1;
                }
                Err(error) => {
                    // A corrupt blob must not leave a stale candidate document.
                    delete(&index_writer, &fields, row.info_hash.as_slice());
                    blob_errors += 1;
                    since_commit += 1;
                    warn!(info_hash = %row.info_hash, %error, "skipping undecodable path blob");
                }
            }

            if since_commit >= args.commit_interval {
                index_writer.commit().context("committing index")?;
                since_commit = 0;
                info!(indexed, skipped, blob_errors, last_info_hash = ?cursor, "committed");
            }

            if args.limit.is_some_and(|limit| indexed >= limit) {
                info!(limit = indexed, "path document limit reached");
                break 'pages;
            }
        }
    }

    index_writer.commit().context("final index commit")?;
    // Clean shutdown: drain in-flight auto-merges so the on-disk index (meta.json,
    // .managed.json, segment files) is fully consistent and merged before this
    // backfill process exits. `IndexWriter::drop` only `kill()`s its merge threads;
    // `wait_merging_threads` joins them, so the serving pod opens a tidy,
    // fully-committed index. (A bare `commit()` + drop is already crash-consistent
    // and durable across a separate-process reopen — proven by
    // tests/pathsearch_durability.rs — so this is defense-in-depth and a cleaner
    // hand-off, not the fix for an observed Rust-side data loss.)
    index_writer
        .wait_merging_threads()
        .context("draining pathsearch index merge threads")?;

    match prefix_builder.finalize(&suggest_path) {
        Ok(stats) => info!(
            suggest_entries = stats.entries,
            suggest_path = %stats.out_path.display(),
            "pathsearch prefix index built"
        ),
        Err(error) => warn!(
            %error,
            suggest_path = %suggest_path.display(),
            "pathsearch prefix index build failed; Suggest disabled"
        ),
    }

    let watermark_seeded =
        seed_followup_watermark(&args.watermark_file, snapshot_epoch, completed_full_scan)
            .context("seeding follow watermark")?;
    info!(
        indexed,
        skipped,
        blob_errors,
        last_info_hash = ?cursor,
        completed_full_scan,
        watermark_seeded,
        snapshot_epoch,
        "pathsearch backfill complete"
    );
    Ok(())
}

fn prefix_config_from_env() -> PrefixIndexConfig {
    let defaults = PrefixIndexConfig::default();
    PrefixIndexConfig {
        max_tracked: env_parse_or_default("PATHSEARCH_SUGGEST_MAX_TRACKED", defaults.max_tracked),
        min_freq: env_parse_or_default("PATHSEARCH_SUGGEST_MIN_FREQ", defaults.min_freq),
        max_entries: env_parse_or_default("PATHSEARCH_SUGGEST_MAX_ENTRIES", defaults.max_entries),
        ..defaults
    }
}

fn env_parse_or_default<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// On a FULL backfill (the keyset scan reached the end), seed the follow-loop
/// watermark to `snapshot_epoch` — captured before the scan began — so the
/// serving pod resumes from there and re-covers torrents crawled DURING the
/// backfill (idempotent via `info_hash` supersession) instead of starting at
/// ~now and skipping them.
///
/// A PARTIAL run (stopped early by `--limit`) writes NOTHING: its index is
/// incomplete, so claiming "all torrents up to `snapshot_epoch` are indexed"
/// would make the follow loop skip the un-scanned remainder. Returns whether the
/// watermark was written.
///
/// # Errors
/// Returns the watermark write error.
fn seed_followup_watermark(
    watermark_file: &Path,
    snapshot_epoch: i64,
    completed_full_scan: bool,
) -> anyhow::Result<bool> {
    if !completed_full_scan {
        return Ok(false);
    }
    write_watermark(watermark_file, snapshot_epoch).context("writing follow watermark")?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::{run, seed_followup_watermark, Args};
    use bitmagnet_search::pathsearch::index::{open_or_create, reader};
    use bitmagnet_search::pathsearch::watermark::read_watermark;

    /// A full backfill seeds the follow watermark to the snapshot epoch; a
    /// partial (`--limit`) run writes nothing, so the follow loop won't claim
    /// coverage the incomplete index doesn't have.
    #[test]
    fn seed_followup_watermark_writes_only_on_full_scan() {
        let path =
            std::env::temp_dir().join(format!("bitmagnet-pathsearch-seed-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);

        assert!(!seed_followup_watermark(&path, 1_700_000_000, false).unwrap());
        assert_eq!(
            read_watermark(&path),
            None,
            "a partial backfill must not seed a watermark"
        );

        assert!(seed_followup_watermark(&path, 1_700_000_000, true).unwrap());
        assert_eq!(read_watermark(&path), Some(1_700_000_000));

        let _ = std::fs::remove_file(&path);
    }

    /// End-to-end against a live PostgreSQL: a small capped path-bag backfill
    /// must open a fresh index, page through `torrents` + `files_data` blobs,
    /// and leave searchable per-torrent path-bag documents. Ignored by default
    /// (there is no DB in CI / `cargo test`); run it against a populated server
    /// with:
    ///
    /// ```sh
    /// BITMAGNET_POSTGRES_DSN=postgres://postgres@localhost/bitmagnet \
    ///   cargo test -p bitmagnet-search --bin bitmagnet-pathsearch-backfill -- --ignored
    /// ```
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL (set BITMAGNET_POSTGRES_DSN)"]
    async fn capped_backfill_indexes_path_bag_documents() {
        let dir = std::env::temp_dir().join(format!(
            "bitmagnet-pathsearch-backfill-it-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        run(Args {
            index_path: dir.clone(),
            postgres_dsn: String::new(), // BITMAGNET_POSTGRES_* from the environment
            batch_size: 100,
            limit: Some(500),
            commit_interval: 100,
            after_info_hash: None,
            writer_heap_mb: 256,
            writer_threads: 1,
            watermark_file: dir.join("watermark"),
        })
        .await
        .expect("pathsearch backfill run");

        // Re-open the committed index and confirm it holds path-bag documents.
        let index = open_or_create(&dir).expect("reopen index");
        let reader = reader(&index).expect("build reader");
        reader.reload().unwrap();
        assert!(
            reader.searcher().num_docs() > 0,
            "a capped backfill of a populated database indexes some path-bag documents"
        );
        // This is a PARTIAL run (--limit 500), so it must NOT seed the watermark.
        assert!(
            !dir.join("watermark").exists(),
            "a --limit (partial) backfill must not seed the follow watermark"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// End-to-end against a live PostgreSQL: a `--limit` backfill is BOUNDED by
    /// the limit and IDEMPOTENT — re-running it over the same keyset prefix
    /// upserts by `info_hash` rather than duplicating, so the doc count is stable
    /// and a partial run never seeds the follow watermark. Complements
    /// `capped_backfill_indexes_path_bag_documents` (which proves docs>0 +
    /// no-watermark) with the bound + idempotency invariants. Ignored by default:
    ///
    /// ```sh
    /// BITMAGNET_POSTGRES_DSN=postgres://postgres@localhost/bitmagnet \
    ///   cargo test -p bitmagnet-search --bin bitmagnet-pathsearch-backfill -- --ignored
    /// ```
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL (set BITMAGNET_POSTGRES_DSN)"]
    async fn capped_backfill_is_bounded_and_idempotent() {
        let dir = std::env::temp_dir().join(format!(
            "bitmagnet-pathsearch-backfill-idem-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let watermark = dir.join("watermark");

        let args = || Args {
            index_path: dir.clone(),
            postgres_dsn: String::new(), // BITMAGNET_POSTGRES_* from the environment
            batch_size: 100,
            limit: Some(300),
            commit_interval: 100,
            after_info_hash: None,
            writer_heap_mb: 256,
            writer_threads: 1,
            watermark_file: watermark.clone(),
        };

        run(args()).await.expect("first capped backfill run");
        let first = {
            let index = open_or_create(&dir).expect("reopen index");
            let reader = reader(&index).expect("build reader");
            reader.reload().unwrap();
            reader.searcher().num_docs()
        };
        assert!(
            first > 0,
            "a capped backfill of a populated database indexes some path-bag documents"
        );
        assert!(first <= 300, "--limit must bound the indexed doc count");
        assert!(
            !watermark.exists(),
            "a partial (--limit) backfill must not seed the follow watermark"
        );

        // Re-run the same capped backfill over the same dir. Documents are keyed
        // by info_hash and upserted (delete-then-add), so the count must not grow.
        run(args()).await.expect("second capped backfill run");
        let second = {
            let index = open_or_create(&dir).expect("reopen index");
            let reader = reader(&index).expect("build reader");
            reader.reload().unwrap();
            reader.searcher().num_docs()
        };
        assert_eq!(
            second, first,
            "re-running a capped backfill must be idempotent (upsert by info_hash, no duplicates)"
        );
        assert!(
            !watermark.exists(),
            "re-running a partial backfill still seeds no watermark"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
