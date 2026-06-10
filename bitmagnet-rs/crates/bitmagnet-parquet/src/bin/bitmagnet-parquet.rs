//! `bitmagnet-parquet` — the L2 export/refresh CLI.
//!
//! Subcommands:
//! * `base`       — full export → sorted fact + rollups → atomic base swap.
//!                  This is the **V3** run: it prints the decode-error count
//!                  (target 0 across all torrents) and exits non-zero if any
//!                  blob failed to decode (`--fail-on-decode-error`).
//! * `delta`      — minute carve (`updated_at > watermark` + deleted list) →
//!                  delta generation (fact + tombstones) → swap + watermark.
//! * `compact`    — full rebuild + empty-delta reset.
//! * `from-hex`   — OFFLINE smoke: `info_hash|count|hex` lines → a base
//!                  generation with no database (CI / local verification).
//! * `verify`     — STUB: agg-vs-torrent_files parity (Job A); see build notes.

use std::path::PathBuf;

use anyhow::{Context, Result};
use bitmagnet_parquet::export::{self, Sinks};
use bitmagnet_parquet::fact::SortMode;
use bitmagnet_parquet::generation::{Kind, Layout};
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "bitmagnet-parquet", about = "L2 Parquet export/refresh for bitmagnet file search")]
struct Cli {
    /// Generation root (the `{base,delta}/…` tree + watermark).
    #[arg(long, env = "BITMAGNET_PARQUET_ROOT", default_value = "/var/lib/bitmagnet/parquet")]
    root: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum SortArg {
    /// Arrival order (info_hash keyset); pair with a downstream DuckDB sort.
    None,
    /// Buffer + sort by (extension, size) in memory (bounded inputs).
    Memory,
}

impl From<SortArg> for SortMode {
    fn from(s: SortArg) -> Self {
        match s {
            SortArg::None => SortMode::None,
            SortArg::Memory => SortMode::InMemory,
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
        /// File of newline-separated deleted info_hash hex (the audit source).
        #[arg(long)]
        deleted_file: Option<PathBuf>,
        #[arg(long, default_value_t = 20_000)]
        page_size: i64,
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
    /// STUB — agg-vs-torrent_files parity (Job A). See dv2-l2-build-notes.md §V.
    Verify {
        #[arg(long, env = "BITMAGNET_POSTGRES_DSN")]
        dsn: String,
    },
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
            let stats =
                export::run_base(&pool, &layout, &version, sort.into(), page_size).await?;
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
            deleted_file,
            page_size,
        } => {
            let pool = bitmagnet_db::PgPool::connect(&dsn)
                .await
                .context("connecting to postgres")?;
            let version = version.unwrap_or_else(|| now_epoch().to_string());
            // Lagged now: the carve window end + persisted cursor (see
            // export::CARVE_LAG_SECS — closes the commit-visibility race).
            let new_wm = watermark.unwrap_or_else(|| now_epoch() - export::CARVE_LAG_SECS);
            let deleted = read_deleted(deleted_file.as_deref())?;
            let stats =
                export::run_delta(&pool, &layout, &version, new_wm, &deleted, page_size).await?;
            report("delta", &stats);
        }
        Cmd::Compact {
            dsn,
            version,
            sort,
            page_size,
        } => {
            let pool = bitmagnet_db::PgPool::connect(&dsn)
                .await
                .context("connecting to postgres")?;
            let version = version.unwrap_or_else(|| now_epoch().to_string());
            let stats =
                export::run_compaction(
                    &pool,
                    &layout,
                    &version,
                    now_epoch() - export::CARVE_LAG_SECS,
                    sort.into(),
                    page_size,
                )
                    .await?;
            report("compact", &stats);
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
        Cmd::Verify { dsn: _ } => {
            anyhow::bail!(
                "verify is a STUB — the agg-vs-torrent_files parity checker (Job A/B) \
                 is specified in docs/dev/dv2-l2-build-notes.md §V and L2-P0 spec §7, not yet implemented"
            );
        }
    }
    Ok(())
}

fn report(job: &str, s: &export::BuildStats) {
    println!(
        "{job}: torrents_ok={} decode_errors={} file_rows={} agg_ext={} agg_torrent_ext={} tombstones={} clean={}",
        s.decode.torrents_ok,
        s.decode.decode_errors,
        s.fact_rows,
        s.agg_ext_rows,
        s.agg_torrent_ext_rows,
        s.tombstones,
        s.is_clean(),
    );
}

fn read_deleted(path: Option<&std::path::Path>) -> Result<Vec<String>> {
    let Some(path) = path else { return Ok(Vec::new()) };
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
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

    let text = std::fs::read_to_string(input).with_context(|| format!("reading {}", input.display()))?;
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
