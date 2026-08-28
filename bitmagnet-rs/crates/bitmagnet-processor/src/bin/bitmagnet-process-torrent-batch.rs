//! Standalone, non-deployed Rust `process_torrent_batch` queue worker.

use std::time::Duration;

use anyhow::{Context, Result};
use bitmagnet_queue::{Consumer, ConsumerConfig, QueueStore, PROCESS_TORRENT_BATCH};
use clap::Parser;
use sqlx::postgres::PgPoolOptions;

#[derive(Debug, Parser)]
#[command(name = "bitmagnet-process-torrent-batch")]
struct Args {
    #[arg(long, env = "BITMAGNET_POSTGRES_DSN")]
    postgres_dsn: String,
    #[arg(long, env = "BITMAGNET_POSTGRES_MAX_CONNECTIONS", default_value_t = 2)]
    postgres_max_connections: u32,
    #[arg(
        long,
        env = "BITMAGNET_POSTGRES_ACQUIRE_TIMEOUT_SECONDS",
        default_value_t = 5
    )]
    postgres_acquire_timeout_seconds: u64,
    #[arg(long, default_value_t = 30)]
    check_interval_seconds: u64,
    #[arg(long, default_value_t = 600)]
    job_timeout_seconds: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    bitmagnet_common::init_tracing();
    let args = Args::parse();
    validate_args(&args)?;

    let pool = PgPoolOptions::new()
        .max_connections(args.postgres_max_connections)
        .acquire_timeout(Duration::from_secs(args.postgres_acquire_timeout_seconds))
        .connect(&args.postgres_dsn)
        .await
        .context("connecting batch worker to PostgreSQL")?;
    let store = QueueStore::new(pool);
    let handler_store = store.clone();
    let metrics_store = store.clone();
    let _metrics_server =
        bitmagnet_common::metrics::maybe_spawn_metrics_server_with_async_gatherer(move || {
            let store = metrics_store.clone();
            async move { store.status_metric_families().await }
        })
        .await
        .context("starting batch worker metrics listener")?;
    let mut config = ConsumerConfig::new(PROCESS_TORRENT_BATCH);
    config.check_interval = Duration::from_secs(args.check_interval_seconds);
    config.job_timeout = Duration::from_secs(args.job_timeout_seconds);
    let consumer = Consumer::new(store, config);

    tracing::info!(
        queue = PROCESS_TORRENT_BATCH,
        postgres_max_connections = args.postgres_max_connections,
        postgres_acquire_timeout_seconds = args.postgres_acquire_timeout_seconds,
        check_interval_seconds = args.check_interval_seconds,
        job_timeout_seconds = args.job_timeout_seconds,
        "starting standalone Rust batch worker"
    );
    consumer
        .run_until(
            move |job| {
                let store = handler_store.clone();
                async move {
                    match store
                        .handle_process_torrent_batch_payload(&job.payload)
                        .await
                    {
                        Ok(report) => {
                            tracing::info!(
                                job_id = %job.id,
                                selected = report.selected,
                                child_jobs = report.child_jobs,
                                continuation_inserted = report.continuation_inserted,
                                max_info_hash = %report.max_info_hash.to_hex(),
                                done = report.done,
                                "Rust batch job planned and enqueued"
                            );
                            Ok(())
                        }
                        Err(error) => {
                            tracing::error!(job_id = %job.id, %error, "Rust batch job failed");
                            Err(error)
                        }
                    }
                }
            },
            bitmagnet_common::serve::shutdown_signal(),
        )
        .await
        .context("Rust batch consumer stopped")?;
    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    anyhow::ensure!(
        args.postgres_max_connections >= 2,
        "batch worker requires at least two PostgreSQL connections: one held by the parent queue transaction and one for selection and child insertion"
    );
    anyhow::ensure!(
        args.postgres_acquire_timeout_seconds > 0,
        "postgres-acquire-timeout-seconds must be positive"
    );
    anyhow::ensure!(
        args.check_interval_seconds > 0,
        "check-interval-seconds must be positive"
    );
    anyhow::ensure!(
        args.job_timeout_seconds > 0,
        "job-timeout-seconds must be positive"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{validate_args, Args};

    fn args() -> Args {
        Args {
            postgres_dsn: "postgres://example.invalid/bitmagnet".to_owned(),
            postgres_max_connections: 2,
            postgres_acquire_timeout_seconds: 5,
            check_interval_seconds: 30,
            job_timeout_seconds: 600,
        }
    }

    #[test]
    fn runtime_contract_requires_two_connections_and_positive_durations() {
        assert!(validate_args(&args()).is_ok());

        let mut invalid = args();
        invalid.postgres_max_connections = 1;
        assert!(validate_args(&invalid)
            .expect_err("one connection would deadlock the retained parent transaction")
            .to_string()
            .contains("at least two"));

        for mutate in [
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
