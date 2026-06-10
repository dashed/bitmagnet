//! `backfill-files` — bulk-build the **L3 per-torrent path-bag** typeahead index
//! from PostgreSQL (PS-T4 §6, the initial build).
//!
//! Reads `torrents` (+ a max-seeders sub-select) in `info_hash`-keyset pages,
//! decodes each `files_data` blob, and upserts one path-bag document per torrent
//! into the Tantivy index the path-search sidecar serves.
//!
//! ## Load-bearing config (measured — EXP-D/PS-T4)
//! * **Single writer thread + ≥2 GiB arena** — the ngram multi-thread writer
//!   crashes; [`bitmagnet_search::pathsearch::index::path_writer`] is the only
//!   correct allocation (NOT tunable away).
//! * **No force-merge.** We commit periodically and stop — leaving the bounded
//!   `LogMergePolicy` segments for the steady-state writer to keep merging
//!   (PS-T4 §6 "prefer incremental merge"; saves the ~94 GB-class transient and a
//!   long single-threaded merge).
//! * **Idempotent + resumable.** Upserts (delete-by-info_hash then add), keyset by
//!   `info_hash`; `--after-info-hash` resumes from a logged hash.
//!
//! On completion it seeds the follow-loop watermark to the max `updated_at` seen
//! (with an empty info_hash tiebreak, so the loop reprocesses only the tiny set
//! at exactly that timestamp + everything newer — not the whole corpus).

use std::path::PathBuf;

use anyhow::Context;
use bitmagnet_db::{connect, stream_torrents_for_pathsearch, DbConfig};
use bitmagnet_model::InfoHash;
use bitmagnet_search::pathsearch::follow::{index_torrent_row, save_watermark, Watermark};
use bitmagnet_search::pathsearch::index::{open_or_create_path, path_writer};
use bitmagnet_search::pathsearch::schema::PathFields;
use clap::Parser;
use tracing::info;

/// `backfill-files` — build the path-bag typeahead index from PostgreSQL.
#[derive(Debug, Parser)]
#[command(
    name = "backfill-files",
    about = "Backfill the bitmagnet path-FTS (per-torrent path-bag) index from PostgreSQL"
)]
struct Args {
    /// Path-bag index directory (created if absent).
    #[arg(
        long,
        env = "BITMAGNET_PATHSEARCH_INDEX",
        default_value = "/var/lib/bitmagnet/search-files"
    )]
    index_path: PathBuf,

    /// PostgreSQL DSN. Empty → built from `BITMAGNET_POSTGRES_*` env vars.
    #[arg(long, default_value = "")]
    postgres_dsn: String,

    /// Rows fetched per keyset page.
    #[arg(long, default_value_t = 1000)]
    batch_size: i64,

    /// Stop after indexing this many torrents (default: index everything). Use a
    /// small value for the smoke gate (extrapolate size & docs/s).
    #[arg(long)]
    limit: Option<u64>,

    /// Commit (making new docs searchable) every N torrents.
    #[arg(long, default_value_t = 50_000)]
    commit_interval: u64,

    /// Resume: only index torrents whose `info_hash` is strictly greater than
    /// this hex hash (the value a prior run logged on commit).
    #[arg(long)]
    after_info_hash: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bitmagnet_common::init_tracing();
    run(Args::parse()).await
}

async fn run(args: Args) -> anyhow::Result<()> {
    anyhow::ensure!(args.batch_size > 0, "--batch-size must be positive");
    anyhow::ensure!(args.commit_interval > 0, "--commit-interval must be positive");

    let mut cfg = DbConfig::from_env().context("reading postgres config from env")?;
    if !args.postgres_dsn.is_empty() {
        cfg.dsn = args.postgres_dsn.clone();
    }
    let pool = connect(&cfg).await.context("connecting to postgres")?;

    info!(index_path = %args.index_path.display(), "opening path-bag index");
    let index = open_or_create_path(&args.index_path)?;
    let fields = PathFields::from_schema(&index.schema()).context("resolving path fields")?;
    // Single-thread, ≥2 GiB arena — the ngram crash-avoidance writer.
    let mut writer = path_writer(&index).context("allocating single-thread path writer")?;

    let mut cursor: Option<InfoHash> = match args.after_info_hash.as_deref() {
        Some(hex) => Some(hex.parse::<InfoHash>().context("parsing --after-info-hash")?),
        None => None,
    };

    info!(
        batch_size = args.batch_size,
        commit_interval = args.commit_interval,
        limit = ?args.limit,
        "path-bag backfill starting (no force-merge; incremental merge retained)"
    );

    let mut indexed: u64 = 0;
    let mut since_commit: u64 = 0;
    let mut max_micros: i64 = 0;

    'pages: loop {
        let page = stream_torrents_for_pathsearch(&pool, cursor.as_ref(), args.batch_size)
            .await
            .context("reading torrents page")?;
        if page.is_empty() {
            break;
        }

        for row in &page {
            index_torrent_row(&writer, &fields, row).context("indexing path-bag doc")?;
            max_micros = max_micros.max(row.updated_at_micros);
            cursor = Some(row.info_hash);
            indexed += 1;
            since_commit += 1;

            if since_commit >= args.commit_interval {
                writer.commit().context("committing index")?;
                since_commit = 0;
                info!(indexed, last_hash = %row.info_hash, "committed");
            }
            if args.limit.is_some_and(|l| indexed >= l) {
                info!(limit = indexed, "torrent limit reached");
                break 'pages;
            }
        }
    }

    writer.commit().context("final index commit")?;

    // Seed the follow watermark so the serving pod's loop starts incrementally.
    let wm = Watermark {
        updated_at_micros: max_micros,
        info_hash: Vec::new(),
    };
    save_watermark(&args.index_path, &wm).context("seeding follow watermark")?;

    info!(
        indexed,
        watermark_micros = max_micros,
        last_hash = cursor.as_ref().map(ToString::to_string),
        "path-bag backfill complete (watermark seeded)"
    );
    Ok(())
}
