//! Entry point for the bitmagnet Tantivy search sidecar.
//!
//! Serves the Tantivy-backed `bitmagnet.v1` `SearchService` plus the standard
//! `grpc.health.v1.Health` service over gRPC for operators and orchestrators.

use std::future::Future;
use std::path::{Path, PathBuf};

use anyhow::Context;
use bitmagnet_common::metrics::{maybe_spawn_metrics_server, register_computed_gauge};
use bitmagnet_common::serve::{health, serve_router, shutdown_signal, Listen};
use bitmagnet_search::follow::{
    spawn_follow_loop, FollowConfig, DEFAULT_CARVE_LAG_SECS, DEFAULT_DELETED_LIMIT,
    DEFAULT_FOLLOW_BATCH_SIZE, DEFAULT_FOLLOW_INTERVAL_SECS, DEFAULT_FOLLOW_MAX_WINDOW_SECS,
};
use bitmagnet_search::pathsearch::watermark::current_epoch;
use bitmagnet_search::proto::search_service_server::SearchServiceServer;
use bitmagnet_search::SearchServer;
use clap::Parser;
use tonic::server::NamedService;
use tonic::transport::Server;
use tonic_health::server::HealthReporter;
use tonic_health::ServingStatus;
use tracing::info;

/// `bitmagnet-search` — the Tantivy-backed search sidecar.
#[derive(Debug, Parser)]
#[command(
    name = "bitmagnet-search",
    about = "Tantivy-backed search sidecar for bitmagnet"
)]
struct Args {
    /// Listen address. `HOST:PORT` (e.g. `127.0.0.1:50051`) starts a TCP
    /// listener; any other value, optionally prefixed `unix:`, is treated as a
    /// Unix-domain-socket path.
    #[arg(long, env = "BITMAGNET_SEARCH_ADDR", default_value = "127.0.0.1:50051")]
    addr: String,

    /// Directory holding the Tantivy index.
    #[arg(
        long,
        env = "BITMAGNET_SEARCH_INDEX",
        default_value = "/var/lib/bitmagnet/search"
    )]
    index_path: PathBuf,

    /// Enable the in-process PostgreSQL-tail follow loop.
    #[arg(long, env = "BITMAGNET_SEARCH_FOLLOW", default_value_t = false)]
    follow: bool,

    /// PostgreSQL DSN for follow mode. When empty, BITMAGNET_POSTGRES_* env vars
    /// are used.
    #[arg(long, env = "BITMAGNET_POSTGRES_DSN", default_value = "")]
    postgres_dsn: String,

    /// Follow poll interval in seconds.
    #[arg(
        long,
        env = "BITMAGNET_SEARCH_FOLLOW_INTERVAL_SECS",
        default_value_t = DEFAULT_FOLLOW_INTERVAL_SECS
    )]
    follow_interval_secs: u64,

    /// Commit-visibility lag for the follow window upper bound.
    #[arg(
        long,
        env = "BITMAGNET_SEARCH_CARVE_LAG_SECS",
        default_value_t = DEFAULT_CARVE_LAG_SECS
    )]
    carve_lag_secs: i64,

    /// Maximum follow window width in seconds.
    #[arg(
        long,
        env = "BITMAGNET_SEARCH_FOLLOW_MAX_WINDOW_SECS",
        default_value_t = DEFAULT_FOLLOW_MAX_WINDOW_SECS
    )]
    follow_max_window_secs: i64,

    /// Changed-torrent page size for follow mode.
    #[arg(
        long,
        env = "BITMAGNET_SEARCH_FOLLOW_BATCH_SIZE",
        default_value_t = DEFAULT_FOLLOW_BATCH_SIZE
    )]
    follow_batch_size: i64,

    /// Deleted-torrent runaway guard for one follow window.
    #[arg(
        long,
        env = "BITMAGNET_SEARCH_DELETED_LIMIT",
        default_value_t = DEFAULT_DELETED_LIMIT
    )]
    deleted_limit: i64,

    /// Watermark file for follow mode. Defaults to `<index-path>/watermark`.
    #[arg(long, env = "BITMAGNET_SEARCH_WATERMARK_FILE", value_name = "PATH")]
    watermark_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bitmagnet_common::init_tracing();
    let args = Args::parse();
    let watermark_file = resolved_watermark_file(&args.index_path, args.watermark_file.as_deref());

    let (health_reporter, health_service) = health();
    set_health_status(&health_reporter, ServingStatus::NotServing).await;

    let server = SearchServer::open(&args.index_path)
        .with_context(|| format!("opening search index at {}", args.index_path.display()))?;
    set_health_status(&health_reporter, ServingStatus::Serving).await;

    let metrics_server = maybe_start_metrics().await?;

    let follow_task = if args.follow {
        Some(
            spawn_follow_loop(
                FollowConfig {
                    postgres_dsn: args.postgres_dsn.clone(),
                    interval_secs: args.follow_interval_secs,
                    carve_lag_secs: args.carve_lag_secs,
                    max_window_secs: args.follow_max_window_secs,
                    batch_size: args.follow_batch_size,
                    deleted_limit: args.deleted_limit,
                    watermark_file: watermark_file.clone(),
                },
                server.clone(),
            )
            .await?,
        )
    } else {
        None
    };

    register_search_metrics(metrics_server.is_some(), &server);

    let service = SearchServiceServer::new(server);
    info!(
        index_path = %args.index_path.display(),
        follow = args.follow,
        "bitmagnet-search starting (Tantivy read/write path and gRPC health services live)"
    );

    let listen = Listen::parse(&args.addr);
    let termination_context = match &listen {
        Listen::Tcp(_) => "gRPC server (TCP) terminated with an error",
        Listen::Unix(_) => "gRPC server (unix) terminated with an error",
    };
    let router = Server::builder()
        .add_service(health_service)
        .add_service(service);
    let serve = async move {
        serve_router(router, listen, shutdown_signal())
            .await
            .context(termination_context)
    };
    supervise_server(serve, follow_task).await?;

    info!("bitmagnet-search stopped");
    Ok(())
}

async fn maybe_start_metrics(
) -> anyhow::Result<Option<(tokio::task::JoinHandle<()>, std::net::SocketAddr)>> {
    maybe_spawn_metrics_server()
        .await
        .context("starting Prometheus metrics listener")
}

fn watermark_age_seconds(now_epoch: i64, watermark_epoch: i64) -> f64 {
    now_epoch.saturating_sub(watermark_epoch).max(0) as f64
}

fn register_search_metrics(metrics_enabled: bool, server: &SearchServer) {
    if !metrics_enabled {
        return;
    }
    let server = server.clone();
    register_computed_gauge(
        "search_follow_watermark_age_seconds",
        "Seconds since the main-search follow watermark.",
        move || watermark_age_seconds(current_epoch(), server.watermark_epoch()),
    );
}

fn resolved_watermark_file(index_path: &Path, explicit_watermark_file: Option<&Path>) -> PathBuf {
    explicit_watermark_file.map_or_else(|| index_path.join("watermark"), Path::to_path_buf)
}

async fn supervise_server<F>(
    server: F,
    follow_task: Option<tokio::task::JoinHandle<()>>,
) -> anyhow::Result<()>
where
    F: Future<Output = anyhow::Result<()>>,
{
    let Some(mut follow_task) = follow_task else {
        return server.await;
    };
    tokio::pin!(server);

    tokio::select! {
        biased;
        server_result = &mut server => server_result,
        follow_result = &mut follow_task => {
            let cause = match follow_result {
                Ok(()) => "follow task returned unexpectedly".to_owned(),
                Err(error) => format!("follow task failed: {error}"),
            };
            tracing::error!(%cause, "search follow task terminated; stopping server");
            Err(anyhow::anyhow!(cause))
        }
    }
}

async fn set_health_status(reporter: &HealthReporter, status: ServingStatus) {
    reporter.set_service_status("", status).await;
    reporter
        .set_service_status(
            <SearchServiceServer<SearchServer> as NamedService>::NAME,
            status,
        )
        .await;
}

#[cfg(test)]
mod tests {
    use super::{
        maybe_start_metrics, register_search_metrics, resolved_watermark_file, supervise_server,
        watermark_age_seconds, Args,
    };
    use bitmagnet_common::metrics::gather_text;
    use bitmagnet_search::SearchServer;
    use clap::Parser;
    use std::ffi::OsString;
    use std::future;
    use std::path::{Path, PathBuf};

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

    #[test]
    fn computed_default_watermark_path_follows_index_path_and_explicit_flag_wins() {
        let index_path = Path::new("/tmp/custom-search-index");
        let explicit_args = Args::try_parse_from([
            "bitmagnet-search",
            "--index-path",
            "/tmp/custom-search-index",
            "--watermark-file",
            "/tmp/explicit-watermark",
        ])
        .expect("explicit watermark flag parses");

        assert_eq!(
            resolved_watermark_file(index_path, None),
            PathBuf::from("/tmp/custom-search-index/watermark")
        );
        assert_eq!(
            resolved_watermark_file(index_path, explicit_args.watermark_file.as_deref()),
            PathBuf::from("/tmp/explicit-watermark")
        );
    }

    #[tokio::test]
    async fn metrics_listener_is_disabled_by_default() {
        let _restore = MetricsAddrRestore::clear();
        let server = maybe_start_metrics()
            .await
            .expect("an unset metrics address is valid");

        assert!(server.is_none(), "no metrics listener should be created");
    }

    #[test]
    fn watermark_age_is_saturating_and_never_negative() {
        assert_eq!(watermark_age_seconds(120, 100), 20.0);
        assert_eq!(watermark_age_seconds(100, 120), 0.0);
        assert_eq!(watermark_age_seconds(i64::MAX, i64::MIN), i64::MAX as f64);
    }

    #[test]
    fn watermark_metric_is_gated_and_reads_the_shared_atomic() {
        const METRIC_NAME: &str = "search_follow_watermark_age_seconds";

        let server = SearchServer::in_ram().expect("in-memory search server");
        register_search_metrics(false, &server);
        assert!(!gather_text()
            .lines()
            .any(|line| line.starts_with(METRIC_NAME)));

        server.set_watermark_epoch(i64::MAX);
        register_search_metrics(true, &server);
        let first = metric_value(&gather_text(), METRIC_NAME);
        assert_eq!(first, 0.0, "a future watermark clamps the age to zero");

        server.set_watermark_epoch(0);
        let second = metric_value(&gather_text(), METRIC_NAME);
        assert!(second > 1_000_000_000.0, "the updated atomic is re-read");
    }

    fn metric_value(text: &str, name: &str) -> f64 {
        let prefix = format!("{name} ");
        text.lines()
            .find_map(|line| line.strip_prefix(prefix.as_str()))
            .expect("metric sample is present")
            .parse()
            .expect("metric value is numeric")
    }

    #[tokio::test]
    async fn panicking_follow_task_fails_the_combined_server_future() {
        let follow_task = tokio::spawn(async {
            panic!("first follow tick panicked");
        });
        let error = supervise_server(future::pending::<anyhow::Result<()>>(), Some(follow_task))
            .await
            .expect_err("a follow panic must stop the server");

        let message = format!("{error:#}");
        assert!(message.contains("follow task failed"));
        assert!(message.contains("first follow tick panicked"));
    }

    #[tokio::test]
    async fn clean_follow_return_is_also_fatal() {
        let follow_task = tokio::spawn(async {});
        let error = supervise_server(future::pending::<anyhow::Result<()>>(), Some(follow_task))
            .await
            .expect_err("an unexpected clean return must stop the server");

        assert!(format!("{error:#}").contains("follow task returned unexpectedly"));
    }
}
