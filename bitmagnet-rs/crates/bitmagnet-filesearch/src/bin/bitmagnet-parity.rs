#[cfg(feature = "duckdb-engine")]
use std::path::PathBuf;
use std::process::ExitCode;

#[cfg(feature = "duckdb-engine")]
use anyhow::{bail, Context, Result};
#[cfg(feature = "duckdb-engine")]
use bitmagnet_filesearch::engine::duck::{DuckConfig, DuckEngine};
#[cfg(feature = "duckdb-engine")]
use bitmagnet_filesearch::generation::resolve;
#[cfg(feature = "duckdb-engine")]
use bitmagnet_filesearch::parity::{
    build_battery, default_deadline, default_extensions, run_battery_pair,
    validate_generation_artifacts, CaseReport, DEFAULT_REPS,
};
#[cfg(feature = "duckdb-engine")]
use bitmagnet_parquet::generation::Layout;
#[cfg(feature = "duckdb-engine")]
use clap::Parser;

#[cfg(feature = "duckdb-engine")]
#[derive(Debug, Parser)]
#[command(
    name = "bitmagnet-parity",
    about = "Run L2 filesearch parity and latency battery against two generation roots"
)]
struct Args {
    /// Baseline generation root.
    #[arg(long)]
    root_a: PathBuf,

    /// Candidate generation root.
    #[arg(long)]
    root_b: PathBuf,

    /// Repetitions per root per case; the first repetition is discarded as cold.
    #[arg(long, default_value_t = DEFAULT_REPS)]
    reps: usize,

    /// Optional JSON report output path.
    #[arg(long)]
    json: Option<PathBuf>,

    /// Comma-separated extension list for the default battery filters.
    #[arg(long)]
    extensions: Option<String>,
}

#[cfg(feature = "duckdb-engine")]
fn main() -> ExitCode {
    match try_main() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(e) => {
            eprintln!("{e:?}");
            ExitCode::from(2)
        }
    }
}

#[cfg(feature = "duckdb-engine")]
fn try_main() -> Result<bool> {
    bitmagnet_common::init_tracing();
    let args = Args::parse();
    if args.reps == 0 {
        bail!("--reps must be at least 1");
    }
    let extensions = args
        .extensions
        .as_deref()
        .map(parse_extensions)
        .transpose()?
        .unwrap_or_else(default_extensions);

    let gen_a = resolve(&Layout::new(&args.root_a))
        .with_context(|| format!("resolving root-a {}", args.root_a.display()))?;
    validate_generation_artifacts(&gen_a)
        .with_context(|| format!("validating root-a {}", args.root_a.display()))?;
    let gen_b = resolve(&Layout::new(&args.root_b))
        .with_context(|| format!("resolving root-b {}", args.root_b.display()))?;
    validate_generation_artifacts(&gen_b)
        .with_context(|| format!("validating root-b {}", args.root_b.display()))?;

    let engine = DuckEngine::open(DuckConfig::default()).context("opening DuckDB engine")?;
    let cases = build_battery(&extensions);
    let reports = run_battery_pair(
        &engine,
        &gen_a,
        &gen_b,
        &cases,
        args.reps,
        default_deadline(),
    )?;

    print_summary(&reports);
    if let Some(path) = &args.json {
        let json = serde_json::to_vec_pretty(&reports).context("serializing JSON report")?;
        std::fs::write(path, json).with_context(|| format!("writing {}", path.display()))?;
    }
    Ok(reports.iter().all(|report| report.equal))
}

#[cfg(feature = "duckdb-engine")]
fn parse_extensions(input: &str) -> Result<Vec<String>> {
    let extensions: Vec<String> = input
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_lowercase())
        .collect();
    if extensions.is_empty() {
        bail!("--extensions must contain at least one extension");
    }
    Ok(extensions)
}

#[cfg(feature = "duckdb-engine")]
fn print_summary(reports: &[CaseReport]) {
    println!(
        "{:<52} {:<5} {:>7} {:>7} {:>10} {:>10} {:>10} {:>10}",
        "case_id", "equal", "rows_a", "rows_b", "p50_a", "p50_b", "max_a", "max_b"
    );
    for report in reports {
        println!(
            "{:<52} {:<5} {:>7} {:>7} {:>10.2} {:>10.2} {:>10.2} {:>10.2}",
            report.case_id,
            report.equal,
            report.rows_a,
            report.rows_b,
            report.p50_ms_a,
            report.p50_ms_b,
            report.max_ms_a,
            report.max_ms_b
        );
    }
    let divergent: Vec<&CaseReport> = reports.iter().filter(|report| !report.equal).collect();
    println!(
        "summary: cases={} equal={} divergent={}",
        reports.len(),
        reports.len() - divergent.len(),
        divergent.len()
    );
    for report in divergent {
        println!(
            "divergence {}: {}",
            report.case_id,
            report.first_divergence.as_deref().unwrap_or("unknown")
        );
    }
}

#[cfg(not(feature = "duckdb-engine"))]
fn main() -> ExitCode {
    eprintln!(
        "bitmagnet-parity was built WITHOUT the `duckdb-engine` feature; rebuild with \
         `--features duckdb-engine` to run the parity/latency battery."
    );
    ExitCode::from(2)
}
