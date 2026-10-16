//! Standalone, non-deployed persisting Rust `process_torrent` queue worker.
//!
//! It claims only admitted supported-subset jobs (explicit `default` workflow,
//! all three attach flags explicitly false) with Go's `FOR UPDATE SKIP LOCKED`
//! order, so it can run beside the Go worker on the same queue: every other
//! row, and every claimed row the Rust kernel cannot reproduce, is left to Go.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bitmagnet_blocking::BlockingManager as PersistentBlockingManager;
use bitmagnet_classifier::core_config_digest;
use bitmagnet_processor::{
    register_worker_metrics, Materializer, ProcessTorrentWorker, ProcessTorrentWorkerConfig,
};
use bitmagnet_queue::{QueueStore, PROCESS_TORRENT};
use clap::Parser;
use sqlx::postgres::PgPoolOptions;

/// Oldest Goose head the worker runs against: the partial index that serves the
/// admitted claim (`00035_queue_admitted_process_torrent_claim_index.sql`).
/// Without it every poll reads the whole organic `process_torrent` backlog. The
/// loader, writer kernel, and generic queue SQL need head 26.
const MIN_GOOSE_VERSION: i64 = 35;

#[derive(Debug, Parser)]
#[command(name = "bitmagnet-process-torrent-worker")]
struct Args {
    #[arg(long, env = "BITMAGNET_POSTGRES_DSN")]
    postgres_dsn: String,
    #[arg(long, env = "BITMAGNET_POSTGRES_MAX_CONNECTIONS", default_value_t = 3)]
    postgres_max_connections: u32,
    #[arg(
        long,
        env = "BITMAGNET_POSTGRES_ACQUIRE_TIMEOUT_SECONDS",
        default_value_t = 5
    )]
    postgres_acquire_timeout_seconds: u64,
    /// Digest logged by the live Go processor (`classifier runner initialized`).
    #[arg(
        long,
        env = "BITMAGNET_PROCESS_TORRENT_WORKER_EXPECTED_CLASSIFIER_CONFIG_DIGEST"
    )]
    expected_classifier_config_digest: Option<String>,
    #[arg(long, default_value_t = 30)]
    check_interval_seconds: u64,
    #[arg(long, default_value_t = 600)]
    job_timeout_seconds: u64,
    #[arg(long, default_value_t = 600)]
    abstain_cooloff_seconds: u64,
    /// How long an in-flight job may run after SIGTERM before it is abandoned
    /// (0 abandons it at once). Keep it, plus the blocking-manager flush,
    /// under the pod's `terminationGracePeriodSeconds`.
    #[arg(long, default_value_t = 20)]
    shutdown_grace_seconds: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    bitmagnet_common::init_tracing();
    let args = Args::parse();
    validate_args(&args)?;
    let expected = args
        .expected_classifier_config_digest
        .as_deref()
        .map(str::trim)
        .unwrap_or_default();
    let actual = core_config_digest().context("digesting embedded classifier configuration")?;
    anyhow::ensure!(
        expected == actual,
        "classifier configuration mismatch: expected live Go digest {expected}, Rust embedded digest {actual}"
    );

    let pool = PgPoolOptions::new()
        .max_connections(args.postgres_max_connections)
        .acquire_timeout(Duration::from_secs(args.postgres_acquire_timeout_seconds))
        .connect(&args.postgres_dsn)
        .await
        .context("connecting process_torrent worker to PostgreSQL")?;
    let head = bitmagnet_db::read_goose_applied_head(&pool)
        .await
        .context("reading Goose migration head")?
        .context("database has no applied Goose migration")?;
    anyhow::ensure!(
        head.version >= MIN_GOOSE_VERSION,
        "Goose migration head is {}; the process_torrent worker requires at least {MIN_GOOSE_VERSION}",
        head.version
    );

    let metrics = register_worker_metrics(bitmagnet_common::metrics::registry())
        .context("registering worker metrics")?;
    let _metrics_server = bitmagnet_common::metrics::maybe_spawn_metrics_server()
        .await
        .context("starting process_torrent worker metrics listener")?;

    let blocking_manager = PersistentBlockingManager::new(pool.clone());
    let config = ProcessTorrentWorkerConfig {
        check_interval: Duration::from_secs(args.check_interval_seconds),
        job_timeout: Duration::from_secs(args.job_timeout_seconds),
        abstain_cooloff: Duration::from_secs(args.abstain_cooloff_seconds),
        shutdown_grace: Duration::from_secs(args.shutdown_grace_seconds),
        ..ProcessTorrentWorkerConfig::default()
    };
    let worker = ProcessTorrentWorker::new(
        QueueStore::new(pool.clone()),
        Materializer::from_core().context("compiling embedded classifier")?,
        Arc::new(blocking_manager.clone()),
        config,
    )
    .with_metrics(metrics);

    tracing::info!(
        queue = PROCESS_TORRENT,
        goose_version = head.version,
        classifier_config_digest = %actual,
        postgres_max_connections = args.postgres_max_connections,
        check_interval_seconds = args.check_interval_seconds,
        job_timeout_seconds = args.job_timeout_seconds,
        shutdown_grace_seconds = args.shutdown_grace_seconds,
        "starting persisting Rust process_torrent worker"
    );
    worker
        .run_until(bitmagnet_common::serve::shutdown_signal())
        .await;
    // Go flushes the blocking buffer when the application stops; do the same
    // before the pool closes.
    blocking_manager
        .flush()
        .await
        .context("flushing blocked info hashes on shutdown")
}

fn validate_args(args: &Args) -> Result<()> {
    anyhow::ensure!(
        args.postgres_max_connections >= 2,
        "process_torrent worker requires at least two PostgreSQL connections: one held by the claimed queue row and one for loading and persisting"
    );
    anyhow::ensure!(
        args.expected_classifier_config_digest
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty()),
        "process_torrent worker requires --expected-classifier-config-digest (or BITMAGNET_PROCESS_TORRENT_WORKER_EXPECTED_CLASSIFIER_CONFIG_DIGEST)"
    );
    for (name, value) in [
        (
            "postgres-acquire-timeout-seconds",
            args.postgres_acquire_timeout_seconds,
        ),
        ("check-interval-seconds", args.check_interval_seconds),
        ("job-timeout-seconds", args.job_timeout_seconds),
    ] {
        anyhow::ensure!(value > 0, "{name} must be positive");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{validate_args, Args};

    fn args() -> Args {
        Args {
            postgres_dsn: "postgres://example.invalid/bitmagnet".to_owned(),
            postgres_max_connections: 3,
            postgres_acquire_timeout_seconds: 5,
            expected_classifier_config_digest: Some("sha256:test".to_owned()),
            check_interval_seconds: 30,
            job_timeout_seconds: 600,
            abstain_cooloff_seconds: 600,
            shutdown_grace_seconds: 20,
        }
    }

    #[test]
    fn runtime_contract_requires_two_connections_a_digest_and_positive_durations() {
        assert!(validate_args(&args()).is_ok());

        let mut invalid = args();
        invalid.postgres_max_connections = 1;
        assert!(validate_args(&invalid)
            .expect_err("one connection would deadlock the claimed queue row")
            .to_string()
            .contains("at least two"));

        for mutate in [
            |args: &mut Args| args.expected_classifier_config_digest = None,
            |args: &mut Args| args.expected_classifier_config_digest = Some("  ".to_owned()),
            |args: &mut Args| args.postgres_acquire_timeout_seconds = 0,
            |args: &mut Args| args.check_interval_seconds = 0,
            |args: &mut Args| args.job_timeout_seconds = 0,
        ] {
            let mut invalid = args();
            mutate(&mut invalid);
            assert!(validate_args(&invalid).is_err());
        }
    }
}
