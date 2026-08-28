//! `v2-shadow` — run the dual-read suite and report.
//!
//! ```text
//! v2-shadow --pg <dsn> --sidecar http://127.0.0.1:50052 [--pairs pairs.json] \
//!           [--csv out.csv] [--repeat 1]
//! ```
//!
//! Exit code 0 ⇔ zero mismatches (the gate). Run against the SAME snapshot
//! (restore + generation exported from it) — see the crate docs for the live
//! drift / ILIKE caveats.

use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use bitmagnet_proto::v1::file_search_service_client::FileSearchServiceClient;
use bitmagnet_shadow::{compare, default_suite, grpcmap, pg, PairSpec};
use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "v2-shadow",
    about = "Dual-read shadow harness: torrent_files SQL vs FileSearchService gRPC (the L2 DROP gate)"
)]
struct Args {
    /// PostgreSQL DSN (read-only usage).
    #[arg(long, env = "BITMAGNET_POSTGRES_DSN")]
    pg: String,
    /// Sidecar gRPC endpoint.
    #[arg(
        long,
        env = "BITMAGNET_FILESEARCH_ENDPOINT",
        default_value = "http://127.0.0.1:50052"
    )]
    sidecar: String,
    /// JSON array of pair specs; omitted = the built-in suite.
    #[arg(long)]
    pairs: Option<PathBuf>,
    /// CSV output path (`-` or omitted = stdout only).
    #[arg(long)]
    csv: Option<PathBuf>,
    /// Repeat the whole suite N times (a cheap sustained-window mode).
    #[arg(long, default_value_t = 1)]
    repeat: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    bitmagnet_common::init_tracing();
    let args = Args::parse();

    let suite: Vec<PairSpec> = match &args.pairs {
        Some(p) => {
            let text =
                std::fs::read_to_string(p).with_context(|| format!("reading {}", p.display()))?;
            serde_json::from_str(&text).context("parsing --pairs JSON")?
        }
        None => default_suite(),
    };

    let pool = bitmagnet_db::PgPool::connect(&args.pg)
        .await
        .context("connecting to postgres")?;
    let mut client = FileSearchServiceClient::connect(args.sidecar.clone())
        .await
        .with_context(|| format!("connecting to sidecar {}", args.sidecar))?;

    let mut csv =
        String::from("run,shape,label,filter,pg_n,sidecar_n,equal,pg_ms,sidecar_ms,detail\n");
    let mut mismatches = 0u64;
    let mut total = 0u64;

    for run in 1..=args.repeat {
        for spec in &suite {
            total += 1;
            let label = spec.label.clone().unwrap_or_default();

            let t0 = Instant::now();
            let expected = pg::run(&pool, spec)
                .await
                .with_context(|| format!("pg side failed for pair '{label}'"))?;
            let pg_ms = t0.elapsed().as_millis();

            let t1 = Instant::now();
            let actual = grpcmap::run(&mut client, spec)
                .await
                .with_context(|| format!("sidecar side failed for pair '{label}'"))?;
            let sidecar_ms = t1.elapsed().as_millis();

            let detail = compare(&expected, &actual);
            let equal = detail.is_none();
            if !equal {
                mismatches += 1;
            }
            let detail_str = detail.unwrap_or_default().replace([',', '\n'], ";");
            tracing::info!(
                run,
                shape = ?spec.shape,
                label,
                equal,
                pg_n = expected.len(),
                sidecar_n = actual.len(),
                pg_ms,
                sidecar_ms,
                detail = %detail_str,
                "pair"
            );
            csv.push_str(&format!(
                "{run},{:?},{label},{},{},{},{equal},{pg_ms},{sidecar_ms},{detail_str}\n",
                spec.shape,
                spec.filter_summary().replace(',', ";"),
                expected.len(),
                actual.len(),
            ));
        }
    }

    if let Some(path) = &args.csv {
        std::fs::write(path, &csv).with_context(|| format!("writing {}", path.display()))?;
    }
    let mut out = std::io::stdout().lock();
    out.write_all(csv.as_bytes())?;
    writeln!(
        out,
        "v2-shadow: pairs={total} mismatches={mismatches} gate={}",
        if mismatches == 0 { "PASS" } else { "FAIL" }
    )?;

    if mismatches > 0 {
        anyhow::bail!("{mismatches} pair(s) mismatched — the DROP gate requires zero");
    }
    Ok(())
}
