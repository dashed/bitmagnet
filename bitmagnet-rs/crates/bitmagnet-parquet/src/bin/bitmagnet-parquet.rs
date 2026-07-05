//! `bitmagnet-parquet` — the L2 export/refresh CLI.
//!
//! Subcommands:
//! * `base` — full export → sorted fact + rollups → atomic base swap.
//!   This is the **V3** run: it prints the decode-error count
//!   (target 0 across all torrents) and exits non-zero if any
//!   blob failed to decode (`--fail-on-decode-error`).
//! * `delta` — minute carve (`updated_at > watermark` + deleted list) →
//!   delta generation (fact + tombstones) → swap + watermark.
//! * `seal` — carve one immutable segment, manifest-publish it, then
//!   monotonically advance the delta carve origin.
//! * `fold` — merge same-tier sealed segments into one higher-tier segment
//!   (requires the `duckdb-sort` feature).
//! * `merge-base` — fold base + sealed segments into a fresh base generation
//!   (requires the `duckdb-sort` feature).
//! * `compact` — full rebuild + empty-delta reset (`--fail-on-decode-error`
//!   mirrors `base`).
//! * `prune` — generation GC: keep `current` + newest-N per kind, delete
//!   the rest (`--keep-base`/`--keep-delta`/`--dry-run`).
//! * `from-hex` — OFFLINE smoke: `info_hash|count|hex` lines → a base
//!   generation with no database (CI / local verification).
//! * `verify` — STUB: agg-vs-torrent_files parity (Job A); see build notes.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use bitmagnet_parquet::export::{self, Sinks};
use bitmagnet_parquet::fact::SortMode;
use bitmagnet_parquet::generation::{Kind, Layout};
use bitmagnet_parquet::seal::{self, SealOutcome};
use clap::{Parser, Subcommand, ValueEnum};
use tracing::{error, info};

#[derive(Debug, Parser)]
#[command(
    name = "bitmagnet-parquet",
    about = "L2 Parquet export/refresh for bitmagnet file search"
)]
struct Cli {
    /// Generation root (the `{base,delta}/…` tree + watermark).
    #[arg(
        long,
        env = "BITMAGNET_PARQUET_ROOT",
        default_value = "/var/lib/bitmagnet/parquet"
    )]
    root: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum SortArg {
    /// Arrival order (info_hash keyset) — queries correct, no row-group pruning.
    None,
    /// Buffer + sort by (extension, size) in memory (bounded inputs ONLY).
    Memory,
    /// Spilling DuckDB post-pass sort (full-corpus safe; production image only
    /// — needs the `duckdb-sort` feature). Env knobs: BITMAGNET_SORT_MEMORY
    /// (8GB), BITMAGNET_SORT_THREADS (8).
    External,
}

impl From<SortArg> for SortMode {
    fn from(s: SortArg) -> Self {
        match s {
            SortArg::None => SortMode::None,
            SortArg::Memory => SortMode::InMemory,
            SortArg::External => SortMode::External,
        }
    }
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Full base export (V3 0-decode-errors validation).
    Base {
        #[arg(long, env = "BITMAGNET_POSTGRES_DSN")]
        dsn: String,
        /// Generation version token (default: current epoch seconds).
        #[arg(long)]
        version: Option<String>,
        #[arg(long, value_enum, default_value = "memory")]
        sort: SortArg,
        #[arg(long, default_value_t = 20_000)]
        page_size: i64,
        /// Exit non-zero if any blob failed to decode (V3 gate).
        #[arg(long, default_value_t = false)]
        fail_on_decode_error: bool,
    },
    /// Minute delta carve.
    Delta {
        #[arg(long, env = "BITMAGNET_POSTGRES_DSN")]
        dsn: String,
        #[arg(long)]
        version: Option<String>,
        /// New watermark (epoch seconds) to advance to after a successful swap;
        /// default = the carve start time.
        #[arg(long)]
        watermark: Option<i64>,
        /// Where the deleted-torrents list comes from: `audit` reads the
        /// `deleted_torrents` trigger table over the SAME carve window;
        /// `file` reads --deleted-file; `none` carries no deletions.
        #[arg(long, value_enum, default_value = "none")]
        deleted_source: DeletedSource,
        /// File of newline-separated deleted info_hash hex (with
        /// `--deleted-source file`).
        #[arg(long)]
        deleted_file: Option<PathBuf>,
        #[arg(long, default_value_t = 20_000)]
        page_size: i64,
    },
    /// Seal an immutable segment and advance the cumulative delta origin.
    Seal {
        #[arg(long, env = "BITMAGNET_POSTGRES_DSN")]
        dsn: String,
        /// Where the deleted-torrents list comes from (same semantics as delta).
        #[arg(long, value_enum, default_value = "none")]
        deleted_source: DeletedSource,
        /// File of newline-separated deleted info_hash hex (with
        /// `--deleted-source file`).
        #[arg(long)]
        deleted_file: Option<PathBuf>,
        #[arg(long, default_value_t = 20_000)]
        page_size: i64,
        /// Skip sealing when changed torrents are below this count and lag is
        /// below --max-lag-secs.
        #[arg(long, default_value_t = 50)]
        min_torrents: u64,
        /// Force a seal once the window lag reaches this many seconds, even if
        /// the changed-torrent count is below --min-torrents.
        #[arg(long, default_value_t = 86_400)]
        max_lag_secs: i64,
        /// Explicit carve end (epoch seconds) instead of now − CARVE_LAG.
        /// Must not exceed the lagged now (commit-visibility contract); used by
        /// the parity/shadow gates to seal a frozen tree deterministically.
        #[arg(long)]
        window_end: Option<i64>,
    },
    /// Fold same-tier sealed segments into one higher-tier segment.
    Fold {
        #[arg(long)]
        tier: u8,
    },
    /// Fold base + all sealed segments into a fresh base generation.
    MergeBase,
    /// In-process follow loop: run a `delta` tick, sleep, repeat — the L2
    /// freshness engine that replaces the per-minute delta CronJob (minute →
    /// seconds). Each tick is IDENTICAL to `delta` (a cumulative carve from the
    /// base watermark to a lagged now, same `--deleted-source`); ticks NEVER
    /// advance the base watermark (compaction owns it), so every tick is
    /// idempotent. A failed tick is logged and retried with exponential backoff
    /// — it never kills the loop; only a startup config error (bad DSN) exits
    /// non-zero.
    Follow {
        #[arg(long, env = "BITMAGNET_POSTGRES_DSN")]
        dsn: String,
        /// Deleted-torrents source per tick (same semantics as `delta`).
        #[arg(long, value_enum, default_value = "none")]
        deleted_source: DeletedSource,
        /// File of newline-separated deleted info_hash hex (with
        /// `--deleted-source file`); re-read every tick.
        #[arg(long)]
        deleted_file: Option<PathBuf>,
        #[arg(long, default_value_t = 20_000)]
        page_size: i64,
        /// Seconds to sleep between ticks (~45s freshness at 15s: tick + the
        /// 30s serving self-reload).
        #[arg(
            long,
            env = "BITMAGNET_PARQUET_FOLLOW_INTERVAL_SECS",
            default_value_t = 15
        )]
        interval_secs: u64,
        /// TESTING ONLY: stop after N ticks (0 = endless). Lets a test run one
        /// or two ticks and exit.
        #[arg(long, hide = true, default_value_t = 0)]
        max_ticks: u64,
    },
    /// Full rebuild + empty-delta reset.
    Compact {
        #[arg(long, env = "BITMAGNET_POSTGRES_DSN")]
        dsn: String,
        #[arg(long)]
        version: Option<String>,
        #[arg(long, value_enum, default_value = "memory")]
        sort: SortArg,
        #[arg(long, default_value_t = 20_000)]
        page_size: i64,
        /// Exit non-zero if any blob failed to decode (V3 gate; mirrors `base`).
        #[arg(long, default_value_t = false)]
        fail_on_decode_error: bool,
    },
    /// Generation GC: keep `current` + the newest-N versions per kind, delete
    /// the rest. Resolves `current` via the live symlink and REFUSES to delete
    /// it even outside keep-N; `--dry-run` lists the delete set without
    /// unlinking. Safe to run while the delta CronJob ticks (it only ever
    /// targets old, non-current dirs; re-checks `current` before each unlink).
    Prune {
        /// Keep the current base + the newest `keep_base − 1` previous bases.
        #[arg(long, default_value_t = 2)]
        keep_base: usize,
        /// Keep the current delta + the newest `keep_delta − 1` previous deltas.
        #[arg(long, default_value_t = 5)]
        keep_delta: usize,
        /// List what WOULD be deleted (+ reclaimed bytes) without deleting.
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Segment GC grace: unreferenced seg/v* dirs younger than this many
        /// seconds are retained.
        #[arg(long, default_value_t = 900)]
        gc_grace_secs: u64,
    },
    /// OFFLINE smoke: build a base generation from a `info_hash|count|hex` file.
    FromHex {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        version: Option<String>,
        #[arg(long, value_enum, default_value = "memory")]
        sort: SortArg,
        #[arg(long, default_value_t = false)]
        fail_on_decode_error: bool,
    },
    /// Job A — the DROP-gate parity checker: blob ⟺ torrent_files at the
    /// per-(torrent, extension) aggregate grain. Read-only; exits non-zero on
    /// ANY mismatch or blob decode error (structural divergence is zero, so a
    /// mismatch is a bug, never an accepted loss).
    Verify {
        #[arg(long, env = "BITMAGNET_POSTGRES_DSN")]
        dsn: String,
        /// `full` walks the whole corpus; `sample` stops at --sample-size.
        #[arg(long, value_enum, default_value = "sample")]
        mode: VerifyMode,
        #[arg(long, default_value_t = 100_000)]
        sample_size: u64,
        /// Resume/start cursor: exclusive info_hash hex lower bound.
        #[arg(long)]
        after: Option<String>,
        /// Torrents per page / per ANY(...) batch.
        #[arg(long, default_value_t = 1_000)]
        batch_size: i64,
        /// Print at most this many mismatch details (all are counted).
        #[arg(long, default_value_t = 20)]
        max_mismatch_print: u64,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum VerifyMode {
    Full,
    Sample,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DeletedSource {
    /// No deletions this run.
    None,
    /// Newline-separated info_hash hex from --deleted-file.
    File,
    /// The `deleted_torrents` audit table (AFTER DELETE trigger on torrents),
    /// read over the same (watermark, new_watermark] window as the carve.
    Audit,
}

/// Epoch seconds now (binary context: std::time is fine).
fn now_epoch() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[tokio::main]
async fn main() -> Result<()> {
    bitmagnet_common::init_tracing();
    let cli = Cli::parse();
    let layout = Layout::new(cli.root);

    match cli.cmd {
        Cmd::Base {
            dsn,
            version,
            sort,
            page_size,
            fail_on_decode_error,
        } => {
            let pool = bitmagnet_db::PgPool::connect(&dsn)
                .await
                .context("connecting to postgres")?;
            let version = version.unwrap_or_else(|| now_epoch().to_string());
            let stats = export::run_base(&pool, &layout, &version, sort.into(), page_size).await?;
            report("base", &stats);
            if fail_on_decode_error && !stats.is_clean() {
                anyhow::bail!(
                    "V3 FAILED: {} blob decode errors (target 0)",
                    stats.decode.decode_errors
                );
            }
        }
        Cmd::Delta {
            dsn,
            version,
            watermark,
            deleted_source,
            deleted_file,
            page_size,
        } => {
            let pool = bitmagnet_db::PgPool::connect(&dsn)
                .await
                .context("connecting to postgres")?;
            let stats = one_delta_tick(
                &pool,
                &layout,
                version,
                watermark,
                deleted_source,
                deleted_file.as_deref(),
                page_size,
            )
            .await?;
            report("delta", &stats);
        }
        Cmd::Seal {
            dsn,
            deleted_source,
            deleted_file,
            page_size,
            min_torrents,
            max_lag_secs,
            window_end,
        } => {
            let pool = bitmagnet_db::PgPool::connect(&dsn)
                .await
                .context("connecting to postgres")?;
            let window_end = resolve_seal_window_end(now_epoch(), window_end)?;
            let since = seal::reconcile_watermark_with_manifest(&layout)?;
            let deleted = read_deleted_for_window(
                &pool,
                deleted_source,
                deleted_file.as_deref(),
                since,
                window_end,
            )
            .await?;
            match seal::run_seal(
                &pool,
                &layout,
                since,
                window_end,
                &deleted,
                page_size,
                min_torrents,
                max_lag_secs,
            )
            .await?
            {
                SealOutcome::Skipped {
                    changed_torrents,
                    lag_secs,
                } => {
                    println!(
                        "seal: skipped changed_torrents={} lag_secs={} min_torrents={} max_lag_secs={}",
                        changed_torrents, lag_secs, min_torrents, max_lag_secs
                    );
                }
                SealOutcome::Sealed {
                    segment,
                    stats,
                    manifest_mver,
                    watermark_advanced,
                } => {
                    report("seal", &stats);
                    println!(
                        "seal: segment=v{} from={} to={} tier={} manifest_mver={} watermark_advanced={}",
                        segment.version,
                        segment.from,
                        segment.to,
                        segment.tier,
                        manifest_mver,
                        watermark_advanced
                    );
                }
            }
        }
        Cmd::Fold { tier } => {
            let outcome = seal::run_fold(&layout, tier, now_epoch() as u64)?;
            match outcome.output {
                Some(segment) => println!(
                    "fold: acted={} input_count={} output=v{} from={} to={} tier={} manifest_mver={}",
                    outcome.acted,
                    outcome.input_count,
                    segment.version,
                    segment.from,
                    segment.to,
                    segment.tier,
                    outcome.manifest_mver.unwrap_or(0)
                ),
                None => println!(
                    "fold: acted=false input_count={} manifest_mver={}",
                    outcome.input_count,
                    outcome.manifest_mver.unwrap_or(0)
                ),
            }
        }
        Cmd::MergeBase => {
            let outcome = seal::run_merge_base(&layout, now_epoch() as u64)?;
            match outcome.base {
                Some(base) => println!(
                    "merge-base: acted={} input_count={} base=v{} cut={} manifest_mver={}",
                    outcome.acted,
                    outcome.input_count,
                    base.version,
                    base.cut,
                    outcome.manifest_mver.unwrap_or(0)
                ),
                None => println!(
                    "merge-base: acted=false input_count={} manifest_mver=0",
                    outcome.input_count
                ),
            }
        }
        Cmd::Follow {
            dsn,
            deleted_source,
            deleted_file,
            page_size,
            interval_secs,
            max_ticks,
        } => {
            // Startup config errors (bad DSN) exit non-zero here; from this
            // point on a per-tick failure is logged + retried, never fatal.
            let pool = bitmagnet_db::PgPool::connect(&dsn)
                .await
                .context("connecting to postgres")?;
            run_follow(
                &pool,
                &layout,
                deleted_source,
                deleted_file.as_deref(),
                page_size,
                interval_secs,
                max_ticks,
            )
            .await?;
        }
        Cmd::Compact {
            dsn,
            version,
            sort,
            page_size,
            fail_on_decode_error,
        } => {
            let pool = bitmagnet_db::PgPool::connect(&dsn)
                .await
                .context("connecting to postgres")?;
            let version = version.unwrap_or_else(|| now_epoch().to_string());
            let stats = export::run_compaction(
                &pool,
                &layout,
                &version,
                now_epoch() - export::CARVE_LAG_SECS,
                sort.into(),
                page_size,
            )
            .await?;
            report("compact", &stats);
            if fail_on_decode_error && !stats.is_clean() {
                anyhow::bail!(
                    "V3 FAILED: {} blob decode errors (target 0)",
                    stats.decode.decode_errors
                );
            }
        }
        Cmd::Prune {
            keep_base,
            keep_delta,
            dry_run,
            gc_grace_secs,
        } => {
            let reports = layout.prune(
                keep_base,
                keep_delta,
                Duration::from_secs(gc_grace_secs),
                dry_run,
            )?;
            for r in &reports {
                let kind = match r.kind {
                    Kind::Base => "base",
                    Kind::Segment => "seg",
                    Kind::Delta => "delta",
                };
                println!(
                    "prune {kind}: kept={} deleted={} reclaimed_bytes={} dry_run={}",
                    r.kept,
                    r.deleted.len(),
                    r.reclaimed_bytes,
                    r.dry_run,
                );
                for d in &r.deleted {
                    println!(
                        "  {} {}",
                        if dry_run { "WOULD-DELETE" } else { "DELETED" },
                        d.display()
                    );
                }
            }
        }
        Cmd::FromHex {
            input,
            version,
            sort,
            fail_on_decode_error,
        } => {
            let version = version.unwrap_or_else(|| now_epoch().to_string());
            let stats = run_from_hex(&layout, &version, sort.into(), &input)?;
            report("from-hex", &stats);
            if fail_on_decode_error && !stats.is_clean() {
                anyhow::bail!(
                    "{} blob decode errors (target 0)",
                    stats.decode.decode_errors
                );
            }
        }
        Cmd::Verify {
            dsn,
            mode,
            sample_size,
            after,
            batch_size,
            max_mismatch_print,
        } => {
            let pool = bitmagnet_db::PgPool::connect(&dsn)
                .await
                .context("connecting to postgres")?;
            let after = after
                .map(|h| h.parse())
                .transpose()
                .map_err(|e| anyhow::anyhow!("--after must be 40-char info_hash hex: {e}"))?;
            let opts = bitmagnet_parquet::VerifyOpts {
                sample_size: match mode {
                    VerifyMode::Full => None,
                    VerifyMode::Sample => Some(sample_size),
                },
                after,
                batch_size,
                max_mismatch_print,
            };
            let stats = bitmagnet_parquet::verify::run_verify(&pool, &opts).await?;
            println!(
                "verify: torrents_checked={} exact={} mismatched={} decode_errors={} clean={}",
                stats.torrents_checked,
                stats.exact,
                stats.mismatched,
                stats.decode_errors,
                stats.is_clean(),
            );
            if !stats.is_clean() {
                anyhow::bail!(
                    "VERIFY FAILED: {} mismatches, {} decode errors (target 0 — \
                     structural divergence is zero, so any difference is a bug)",
                    stats.mismatched,
                    stats.decode_errors
                );
            }
        }
    }
    Ok(())
}

fn report(job: &str, s: &export::BuildStats) {
    println!(
        "{job}: torrents_ok={} decode_errors={} file_rows={} padding_rows={} agg_ext={} agg_torrent_ext={} tombstones={} clean={}",
        s.decode.torrents_ok,
        s.decode.decode_errors,
        s.fact_rows,
        s.decode.padding_rows,
        s.agg_ext_rows,
        s.agg_torrent_ext_rows,
        s.tombstones,
        s.is_clean(),
    );
}

/// One delta carve — the shared body of the `delta` subcommand and each
/// `follow` tick. Cumulative: carves `(base watermark, window_end]` (plus the
/// deleted-audit window) into a fresh delta generation that atomically replaces
/// the current one, and NEVER advances the base watermark (compaction owns it).
/// `follow` passes `version = None` / `watermark = None` so every tick gets a
/// fresh version dir and re-evaluates the lagged-now window end.
async fn one_delta_tick(
    pool: &bitmagnet_db::PgPool,
    layout: &Layout,
    version: Option<String>,
    watermark: Option<i64>,
    deleted_source: DeletedSource,
    deleted_file: Option<&std::path::Path>,
    page_size: i64,
) -> Result<export::BuildStats> {
    let version = version.unwrap_or_else(|| now_epoch().to_string());
    // Lagged now: the carve window END (export::CARVE_LAG_SECS closes the
    // commit-visibility race). The carve ORIGIN is the base watermark —
    // compaction-owned; ticks never advance it.
    let window_end = watermark.unwrap_or_else(|| now_epoch() - export::CARVE_LAG_SECS);
    let since = layout.read_watermark();
    let deleted =
        read_deleted_for_window(pool, deleted_source, deleted_file, since, window_end).await?;
    export::run_delta(pool, layout, &version, window_end, &deleted, page_size).await
}

/// Resolve the seal carve end: an explicit override may not exceed the lagged
/// now (rows committing later than the lag could otherwise fall between runs —
/// the CARVE_LAG commit-visibility contract) and must be positive.
fn resolve_seal_window_end(now_epoch: i64, override_end: Option<i64>) -> Result<i64> {
    let lagged_now = seal::default_seal_window_end(now_epoch);
    match override_end {
        None => Ok(lagged_now),
        Some(end) if end <= 0 => anyhow::bail!("--window-end must be a positive epoch, got {end}"),
        Some(end) if end > lagged_now => anyhow::bail!(
            "--window-end {end} exceeds the lagged now {lagged_now}; sealing an un-lagged window breaks the commit-visibility contract"
        ),
        Some(end) => Ok(end),
    }
}

async fn read_deleted_for_window(
    pool: &bitmagnet_db::PgPool,
    deleted_source: DeletedSource,
    deleted_file: Option<&std::path::Path>,
    since: i64,
    window_end: i64,
) -> Result<Vec<String>> {
    match deleted_source {
        DeletedSource::None => Ok(Vec::new()),
        DeletedSource::File => read_deleted(deleted_file),
        DeletedSource::Audit => {
            // Same half-open (since, window_end] window as the change carve,
            // so a deletion is tombstoned by exactly the run whose window
            // contains its deleted_at.
            Ok(
                bitmagnet_db::read_deleted_torrents(pool, since, window_end, 1_000_000)
                    .await
                    .context("reading deleted_torrents audit window")?
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            )
        }
    }
}

/// Backoff cap for a failing `follow` loop: a tick that keeps erroring settles
/// at one retry per 5 minutes rather than hammering PG.
const FOLLOW_MAX_BACKOFF_SECS: u64 = 300;

/// Seconds to sleep before the next `follow` tick. On success
/// (`consecutive_failures == 0`) this is the base `interval_secs`; after the
/// nth consecutive failure it is `min(interval * 2^(n-1), 300)` — exponential
/// backoff that resets to the base interval as soon as a tick succeeds.
/// Saturating throughout: no overflow/panic on extreme inputs.
fn follow_backoff_secs(interval_secs: u64, consecutive_failures: u32) -> u64 {
    if consecutive_failures == 0 {
        return interval_secs;
    }
    // 2^(n-1); an over-large shift means "way past the cap" → factor = u64::MAX.
    let factor = 1u64
        .checked_shl(consecutive_failures - 1)
        .unwrap_or(u64::MAX);
    interval_secs
        .saturating_mul(factor)
        .min(FOLLOW_MAX_BACKOFF_SECS)
}

/// The in-process `follow` loop: run a delta tick, log it, sleep, repeat. A
/// failed tick is logged at error level and retried with exponential backoff
/// ([`follow_backoff_secs`]); the loop never exits on a tick failure. Returns
/// only when `max_ticks` (> 0, testing) is reached.
async fn run_follow(
    pool: &bitmagnet_db::PgPool,
    layout: &Layout,
    deleted_source: DeletedSource,
    deleted_file: Option<&std::path::Path>,
    page_size: i64,
    interval_secs: u64,
    max_ticks: u64,
) -> Result<()> {
    use std::time::{Duration, Instant};

    let interval = interval_secs.max(1);
    let mut consecutive_failures: u32 = 0;
    let mut ticks: u64 = 0;
    info!(
        interval_secs = interval,
        max_ticks,
        carve_lag_secs = export::CARVE_LAG_SECS,
        "bitmagnet-parquet follow starting (in-process cumulative delta loop)"
    );
    loop {
        let started = Instant::now();
        match one_delta_tick(
            pool,
            layout,
            None,
            None,
            deleted_source,
            deleted_file,
            page_size,
        )
        .await
        {
            Ok(stats) => {
                consecutive_failures = 0;
                // Same numbers as the `delta` subcommand's report(), plus tick
                // duration, so `kubectl logs` shows freshness at a glance.
                info!(
                    torrents_ok = stats.decode.torrents_ok,
                    decode_errors = stats.decode.decode_errors,
                    file_rows = stats.fact_rows,
                    tombstones = stats.tombstones,
                    clean = stats.is_clean(),
                    duration_ms = started.elapsed().as_millis() as u64,
                    "delta follow tick complete"
                );
            }
            Err(err) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                let backoff = follow_backoff_secs(interval, consecutive_failures);
                error!(
                    error = format!("{err:#}"),
                    consecutive_failures,
                    backoff_secs = backoff,
                    duration_ms = started.elapsed().as_millis() as u64,
                    "delta follow tick FAILED; backing off (loop continues)"
                );
            }
        }
        ticks += 1;
        if max_ticks != 0 && ticks >= max_ticks {
            info!(ticks, "follow reached --max-ticks; exiting");
            break;
        }
        tokio::time::sleep(Duration::from_secs(follow_backoff_secs(
            interval,
            consecutive_failures,
        )))
        .await;
    }
    Ok(())
}

fn read_deleted(path: Option<&std::path::Path>) -> Result<Vec<String>> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .collect())
}

/// OFFLINE base build from `info_hash|files_count|blob_hex` lines (no DB).
fn run_from_hex(
    layout: &Layout,
    version: &str,
    sort: SortMode,
    input: &std::path::Path,
) -> Result<export::BuildStats> {
    use bitmagnet_model::deserialize_files;

    layout.ensure_dirs()?;
    let dir = layout.new_version_dir(Kind::Base, version)?;
    let mut sinks = Sinks::create(&dir, sort, false)?;

    let text =
        std::fs::read_to_string(input).with_context(|| format!("reading {}", input.display()))?;
    for line in text.lines() {
        let mut parts = line.splitn(3, '|');
        let ih_hex = parts.next().unwrap_or_default();
        let _count = parts.next().unwrap_or_default();
        let blob_hex = match parts.next() {
            Some(h) if !h.is_empty() => h,
            _ => continue,
        };
        let blob = hex::decode(blob_hex).context("hex-decoding blob")?;
        sinks.push_torrent(ih_hex, deserialize_files(&blob))?;
    }
    let stats = sinks.finish(&dir)?;
    layout.publish(Kind::Base, &dir)?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::follow_backoff_secs;
    use super::resolve_seal_window_end;

    #[test]
    fn seal_window_end_defaults_to_lagged_now_and_rejects_bad_overrides() {
        let now = 1_000_000;
        let lagged = now - bitmagnet_parquet::export::CARVE_LAG_SECS;
        assert_eq!(resolve_seal_window_end(now, None).unwrap(), lagged);
        assert_eq!(resolve_seal_window_end(now, Some(lagged)).unwrap(), lagged);
        assert_eq!(
            resolve_seal_window_end(now, Some(lagged - 500)).unwrap(),
            lagged - 500
        );
        assert!(resolve_seal_window_end(now, Some(lagged + 1)).is_err());
        assert!(resolve_seal_window_end(now, Some(0)).is_err());
        assert!(resolve_seal_window_end(now, Some(-5)).is_err());
    }

    #[test]
    fn backoff_is_base_interval_when_no_failures() {
        // The steady-state cadence: a clean tick sleeps exactly interval_secs.
        assert_eq!(follow_backoff_secs(15, 0), 15);
        assert_eq!(follow_backoff_secs(60, 0), 60);
    }

    #[test]
    fn backoff_doubles_each_consecutive_failure() {
        // n-th failure → interval * 2^(n-1): first retry keeps the base cadence,
        // then doubles.
        assert_eq!(follow_backoff_secs(15, 1), 15); // 15 * 2^0
        assert_eq!(follow_backoff_secs(15, 2), 30); // 15 * 2^1
        assert_eq!(follow_backoff_secs(15, 3), 60);
        assert_eq!(follow_backoff_secs(15, 4), 120);
        assert_eq!(follow_backoff_secs(15, 5), 240);
    }

    #[test]
    fn backoff_caps_at_300_seconds() {
        assert_eq!(follow_backoff_secs(15, 6), 300); // 480 → capped
        assert_eq!(follow_backoff_secs(60, 3), 240); // 240 < cap
        assert_eq!(follow_backoff_secs(60, 4), 300); // 480 → capped
    }

    #[test]
    fn backoff_saturates_without_panicking() {
        // A huge shift or product must clamp to the cap, never overflow/panic.
        assert_eq!(follow_backoff_secs(15, 100), 300);
        assert_eq!(follow_backoff_secs(1, u32::MAX), 300);
        assert_eq!(follow_backoff_secs(u64::MAX, 1), 300);
        assert_eq!(follow_backoff_secs(u64::MAX, u32::MAX), 300);
    }

    /// A single `follow` tick against a live PostgreSQL: identical carve to the
    /// `delta` subcommand (cumulative, watermark unchanged). Read-only against
    /// PG apart from a throwaway generation root under the temp dir. Ignored by
    /// default:
    ///
    /// ```sh
    /// BITMAGNET_POSTGRES_DSN=postgres://postgres@localhost/bitmagnet \
    ///   cargo test -p bitmagnet-parquet --bin bitmagnet-parquet -- --ignored
    /// ```
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL (set BITMAGNET_POSTGRES_DSN)"]
    async fn one_follow_tick_carves_a_delta() {
        use super::{one_delta_tick, DeletedSource};
        use bitmagnet_parquet::generation::Layout;

        let dsn = std::env::var("BITMAGNET_POSTGRES_DSN").expect("BITMAGNET_POSTGRES_DSN set");
        let pool = bitmagnet_db::PgPool::connect(&dsn)
            .await
            .expect("connect to postgres");

        let root = std::env::temp_dir().join(format!(
            "bitmagnet-parquet-follow-it-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let layout = Layout::new(root.clone());
        layout.ensure_dirs().expect("ensure generation dirs");

        // Exactly what a follow tick runs: version/watermark = None so the tick
        // mints a fresh version and carves to a lagged now from the base
        // watermark (0 on a fresh root → full cumulative window).
        let stats = one_delta_tick(
            &pool,
            &layout,
            None,
            None,
            DeletedSource::None,
            None,
            20_000,
        )
        .await
        .expect("delta tick completes");
        assert!(
            stats.is_clean(),
            "a clean-corpus tick must report zero decode errors: {stats:?}"
        );

        let _ = std::fs::remove_dir_all(&root);
    }
}
