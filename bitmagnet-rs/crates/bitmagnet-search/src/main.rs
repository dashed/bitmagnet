//! Entry point for the bitmagnet Tantivy search sidecar.
//!
//! Serves the Tantivy-backed `bitmagnet.v1` `SearchService` plus the standard
//! `grpc.health.v1.Health` service over gRPC for operators and orchestrators.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use bitmagnet_search::follow::{
    spawn_follow_loop, FollowConfig, DEFAULT_CARVE_LAG_SECS, DEFAULT_DELETED_LIMIT,
    DEFAULT_FOLLOW_BATCH_SIZE, DEFAULT_FOLLOW_INTERVAL_SECS, DEFAULT_FOLLOW_MAX_WINDOW_SECS,
    DEFAULT_WATERMARK_FILE,
};
use bitmagnet_search::proto::search_service_server::SearchServiceServer;
use bitmagnet_search::SearchServer;
use clap::Parser;
use tonic::server::NamedService;
use tonic::transport::Server;
use tonic_health::pb::health_server::HealthServer as GrpcHealthServer;
use tonic_health::server::{HealthReporter, HealthService};
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

    /// Watermark file for follow mode.
    #[arg(
        long,
        env = "BITMAGNET_SEARCH_WATERMARK_FILE",
        default_value = DEFAULT_WATERMARK_FILE
    )]
    watermark_file: PathBuf,
}

/// How the server should listen, derived from `--addr`.
#[derive(Debug)]
enum Listen {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

type StandardHealthService = GrpcHealthServer<HealthService>;

impl Listen {
    fn parse(addr: &str) -> Self {
        let candidate = addr.strip_prefix("unix:").unwrap_or(addr);
        candidate
            .parse::<SocketAddr>()
            .map_or_else(|_| Listen::Unix(PathBuf::from(candidate)), Listen::Tcp)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bitmagnet_common::init_tracing();
    let args = Args::parse();

    let health_reporter = HealthReporter::new();
    set_health_status(&health_reporter, ServingStatus::NotServing).await;

    let server = SearchServer::open(&args.index_path)
        .with_context(|| format!("opening search index at {}", args.index_path.display()))?;
    set_health_status(&health_reporter, ServingStatus::Serving).await;

    let _follow_task = if args.follow {
        Some(
            spawn_follow_loop(
                FollowConfig {
                    postgres_dsn: args.postgres_dsn.clone(),
                    interval_secs: args.follow_interval_secs,
                    carve_lag_secs: args.carve_lag_secs,
                    max_window_secs: args.follow_max_window_secs,
                    batch_size: args.follow_batch_size,
                    deleted_limit: args.deleted_limit,
                    watermark_file: args.watermark_file.clone(),
                },
                server.clone(),
            )
            .await?,
        )
    } else {
        None
    };

    let health_service =
        GrpcHealthServer::new(HealthService::from_health_reporter(health_reporter));
    let service = SearchServiceServer::new(server);
    info!(
        index_path = %args.index_path.display(),
        follow = args.follow,
        "bitmagnet-search starting (Tantivy read/write path and gRPC health services live)"
    );

    match Listen::parse(&args.addr) {
        Listen::Tcp(addr) => {
            info!(%addr, "serving gRPC over TCP");
            Server::builder()
                .add_service(health_service)
                .add_service(service)
                .serve_with_shutdown(addr, shutdown_signal())
                .await
                .context("gRPC server (TCP) terminated with an error")?;
        }
        Listen::Unix(path) => serve_unix(service, health_service, path).await?,
    }

    info!("bitmagnet-search stopped");
    Ok(())
}

#[cfg(unix)]
async fn serve_unix(
    service: SearchServiceServer<SearchServer>,
    health_service: StandardHealthService,
    path: PathBuf,
) -> anyhow::Result<()> {
    use tokio::net::UnixListener;
    use tokio_stream::wrappers::UnixListenerStream;

    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("removing stale socket at {}", path.display()))?;
    }
    let listener = UnixListener::bind(&path)
        .with_context(|| format!("binding unix socket at {}", path.display()))?;
    info!(path = %path.display(), "serving gRPC over unix socket");
    Server::builder()
        .add_service(health_service)
        .add_service(service)
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown_signal())
        .await
        .context("gRPC server (unix) terminated with an error")?;
    Ok(())
}

#[cfg(not(unix))]
#[allow(clippy::unused_async)]
async fn serve_unix(
    _service: SearchServiceServer<SearchServer>,
    _health_service: StandardHealthService,
    path: PathBuf,
) -> anyhow::Result<()> {
    anyhow::bail!(
        "unix-socket listening ({}) is only supported on unix platforms",
        path.display()
    )
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

/// Resolves when the process receives `SIGINT` (Ctrl-C) or, on unix, `SIGTERM`.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install ctrl-c handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => tracing::error!(%error, "failed to install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
    info!("shutdown signal received");
}
