//! Separate mirror-writer and non-persisting consumer processes for Lane P.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bitmagnet_processor::{MirrorMetrics, ShadowMetrics, ShadowRuntime};
use bitmagnet_queue::{Consumer, ConsumerConfig, MirrorConfig, QueueStore};
use clap::{Parser, Subcommand};
use sqlx::postgres::PgPoolOptions;

#[derive(Debug, Parser)]
#[command(name = "bitmagnet-ingest-shadow")]
struct Args {
    #[arg(long, env = "BITMAGNET_POSTGRES_DSN")]
    postgres_dsn: String,
    #[arg(long, env = "BITMAGNET_POSTGRES_MAX_CONNECTIONS", default_value_t = 4)]
    postgres_max_connections: u32,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Copy only the explicitly flags-off supported subset into the scratch queue.
    Mirror {
        /// Deterministic sample rate. Zero is the fail-closed staging default.
        #[arg(
            long,
            env = "BITMAGNET_INGEST_SHADOW_SAMPLE_BASIS_POINTS",
            default_value_t = 0
        )]
        sample_basis_points: u16,
        #[arg(long, default_value_t = 100)]
        page_size: u32,
        #[arg(long, default_value_t = 1_000)]
        active_depth_cap: u32,
        #[arg(long, default_value_t = 30)]
        delay_seconds: u64,
        #[arg(long, default_value_t = 3_600)]
        archival_seconds: u64,
        #[arg(long, default_value_t = 30)]
        idle_seconds: u64,
    },
    /// Consume the scratch queue, compare against settled live rows, and discard.
    Consume {
        /// Digest printed by `bitmagnet classifier digest` in the live Go
        /// processor environment.
        #[arg(
            long,
            env = "BITMAGNET_INGEST_SHADOW_EXPECTED_CLASSIFIER_CONFIG_DIGEST"
        )]
        expected_classifier_config_digest: Option<String>,
        #[arg(long, default_value_t = 30)]
        check_interval_seconds: u64,
        #[arg(long, default_value_t = 600)]
        job_timeout_seconds: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    bitmagnet_common::init_tracing();
    let args = Args::parse();
    validate_args(&args)?;
    let pool = PgPoolOptions::new()
        .max_connections(args.postgres_max_connections)
        .connect(&args.postgres_dsn)
        .await
        .context("connecting to PostgreSQL")?;
    let _metrics_server = bitmagnet_common::metrics::maybe_spawn_metrics_server()
        .await
        .context("starting metrics listener")?;

    match args.command {
        Command::Mirror {
            sample_basis_points,
            page_size,
            active_depth_cap,
            delay_seconds,
            archival_seconds,
            idle_seconds,
        } => {
            let config = MirrorConfig {
                sample_basis_points,
                page_size,
                active_depth_cap,
                delay: Duration::from_secs(delay_seconds),
                archival_duration: Duration::from_secs(archival_seconds),
                ..MirrorConfig::default()
            };
            let metrics = MirrorMetrics::register().context("registering mirror metrics")?;
            run_mirror(
                QueueStore::new(pool.clone()),
                config,
                Duration::from_secs(idle_seconds),
                &metrics,
            )
            .await?;
        }
        Command::Consume {
            expected_classifier_config_digest,
            check_interval_seconds,
            job_timeout_seconds,
        } => {
            let mut config = ConsumerConfig::new(bitmagnet_queue::PROCESS_TORRENT_SHADOW);
            config.check_interval = Duration::from_secs(check_interval_seconds);
            config.job_timeout = Duration::from_secs(job_timeout_seconds);
            let consumer = Consumer::new(QueueStore::new(pool.clone()), config);
            let runtime = Arc::new(ShadowRuntime::from_core(
                pool,
                expected_classifier_config_digest.as_deref(),
            )?);
            let metrics = ShadowMetrics::register().context("registering shadow metrics")?;
            consumer
                .run_until(
                    move |job| {
                        let runtime = Arc::clone(&runtime);
                        let metrics = metrics.clone();
                        async move {
                            let comparison = match runtime.process_job(&job).await {
                                Ok(comparison) => comparison,
                                Err(error) if error.unsupported_reason().is_some() => {
                                    let reason = error
                                        .unsupported_reason()
                                        .expect("guarded unsupported reason");
                                    metrics.observe_unsupported(reason);
                                    tracing::warn!(
                                        job_id = %job.id,
                                        reason,
                                        %error,
                                        "ingest shadow job is outside the supported subset"
                                    );
                                    return Ok(());
                                }
                                Err(error) => return Err(error),
                            };
                            metrics.observe(&comparison);
                            if comparison.is_match() {
                                tracing::info!(
                                    job_id = %job.id,
                                    torrents = comparison.torrents.len(),
                                    "ingest shadow matched"
                                );
                            } else {
                                for mismatch in comparison
                                    .torrents
                                    .iter()
                                    .filter(|item| {
                                        item.verdict
                                            == bitmagnet_processor::ComparisonVerdict::Mismatch
                                    })
                                    .take(10)
                                {
                                    tracing::warn!(
                                        job_id = %job.id,
                                        info_hash = %mismatch.info_hash,
                                        content_type = mismatch.content_type.as_deref().unwrap_or("unclassified"),
                                        drift = ?mismatch.drift_fields,
                                        "ingest shadow mismatch"
                                    );
                                }
                            }
                            Ok::<(), bitmagnet_processor::ShadowRuntimeError>(())
                        }
                    },
                    shutdown_signal(),
                )
                .await
                .context("shadow consumer stopped")?;
        }
    }
    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    anyhow::ensure!(
        args.postgres_max_connections > 0,
        "postgres-max-connections must be positive"
    );
    match &args.command {
        Command::Mirror {
            sample_basis_points,
            page_size,
            active_depth_cap,
            delay_seconds,
            archival_seconds,
            idle_seconds,
        } => {
            anyhow::ensure!(
                *sample_basis_points <= 10_000,
                "sample-basis-points cannot exceed 10000"
            );
            anyhow::ensure!(*page_size > 0, "page-size must be positive");
            anyhow::ensure!(*active_depth_cap > 0, "active-depth-cap must be positive");
            anyhow::ensure!(*delay_seconds > 0, "delay-seconds must be positive");
            anyhow::ensure!(*archival_seconds > 0, "archival-seconds must be positive");
            anyhow::ensure!(*idle_seconds > 0, "idle-seconds must be positive");
        }
        Command::Consume {
            expected_classifier_config_digest,
            check_interval_seconds,
            job_timeout_seconds,
        } => {
            anyhow::ensure!(
                args.postgres_max_connections >= 2,
                "consume requires at least two PostgreSQL connections: one held by the queue transaction and one for shadow reads"
            );
            anyhow::ensure!(
                expected_classifier_config_digest
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty()),
                "consume requires --expected-classifier-config-digest (or BITMAGNET_INGEST_SHADOW_EXPECTED_CLASSIFIER_CONFIG_DIGEST)"
            );
            anyhow::ensure!(
                *check_interval_seconds > 0,
                "check-interval-seconds must be positive"
            );
            anyhow::ensure!(
                *job_timeout_seconds > 0,
                "job-timeout-seconds must be positive"
            );
        }
    }
    Ok(())
}

async fn run_mirror(
    store: QueueStore,
    config: MirrorConfig,
    idle: Duration,
    metrics: &MirrorMetrics,
) -> Result<()> {
    loop {
        tokio::select! {
            result = store.mirror_processed_page(&config) => {
                let report = result.context("mirroring processed queue rows")?;
                metrics.observe(&report);
                tracing::info!(
                    scanned = report.scanned,
                    sampled = report.sampled,
                    inserted = report.inserted,
                    ineligible = ?report.ineligible,
                    active_depth = report.active_depth,
                    capped = report.capped,
                    "ingest shadow mirror page"
                );
                if report.scanned < config.page_size || report.capped {
                    tokio::time::sleep(idle).await;
                } else {
                    tokio::task::yield_now().await;
                }
            }
            () = shutdown_signal() => return Ok(()),
        }
    }
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "failed to install shutdown signal");
        std::future::pending::<()>().await;
    }
}
