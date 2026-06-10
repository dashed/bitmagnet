//! `pathsearch-server` — the L3 path-FTS typeahead gRPC sidecar.
//!
//! Serves `bitmagnet.v1.PathSearchService` over gRPC and, with `--follow`, runs
//! the in-pod PG-tail watermark loop so the path-bag index stays fresh with zero
//! Go-side change (PS-T4 §3.2 source (b), the keep-everything default). The
//! serving pod is the permanent sole writer: the RPCs and the follow loop share
//! one `Arc<Mutex<IndexWriter>>`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use bitmagnet_db::{connect, DbConfig};
use bitmagnet_search::pathsearch::follow::{run_follow_loop, FollowConfig};
use bitmagnet_search::proto::path_search_service_server::PathSearchServiceServer;
use bitmagnet_search::PathSearchServer;
use clap::Parser;
use tonic::transport::Server;
use tracing::{info, warn};

/// `pathsearch-server` — the Tantivy path-bag typeahead sidecar.
#[derive(Debug, Parser)]
#[command(
    name = "pathsearch-server",
    about = "Path-FTS (per-torrent path-bag) typeahead sidecar for bitmagnet"
)]
struct Args {
    /// Listen address. `HOST:PORT` starts TCP; any other value (optionally
    /// `unix:`-prefixed) is a Unix-domain-socket path.
    #[arg(
        long,
        env = "BITMAGNET_PATHSEARCH_ADDR",
        default_value = "0.0.0.0:50051"
    )]
    addr: String,

    /// Path-bag index directory.
    #[arg(
        long,
        env = "BITMAGNET_PATHSEARCH_INDEX",
        default_value = "/var/lib/bitmagnet/search-files"
    )]
    index_path: PathBuf,

    /// Run the PG-tail follow loop (steady-state freshness). When off, the pod
    /// only serves reads over a static index (e.g. right after a backfill).
    #[arg(long, env = "BITMAGNET_PATHSEARCH_FOLLOW", default_value_t = false)]
    follow: bool,

    /// PostgreSQL DSN for the follow loop. Empty → `BITMAGNET_POSTGRES_*` env.
    #[arg(long, default_value = "")]
    postgres_dsn: String,

    /// Follow-loop poll interval (seconds) when caught up.
    #[arg(long, env = "BITMAGNET_PATHSEARCH_POLL_SECS", default_value_t = 15)]
    poll_secs: u64,

    /// Follow-loop rows per page / commit cadence.
    #[arg(long, env = "BITMAGNET_PATHSEARCH_BATCH", default_value_t = 500)]
    batch_size: i64,
}

#[derive(Debug)]
enum Listen {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

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

    let server = PathSearchServer::open(&args.index_path)
        .with_context(|| format!("opening path index at {}", args.index_path.display()))?;

    // Spawn the follow loop (it shares the server's SOLE writer).
    if args.follow {
        let mut cfg = DbConfig::from_env().context("reading postgres config from env")?;
        if !args.postgres_dsn.is_empty() {
            cfg.dsn = args.postgres_dsn.clone();
        }
        let pool = connect(&cfg).await.context("connecting to postgres for follow loop")?;
        let follow_cfg = FollowConfig {
            index_path: args.index_path.clone(),
            batch_size: args.batch_size,
            poll_interval: Duration::from_secs(args.poll_secs),
        };
        let writer = server.writer_handle();
        let reader = server.reader();
        let fields = server.fields();
        info!(poll_secs = args.poll_secs, batch = args.batch_size, "spawning PG-tail follow loop");
        tokio::spawn(async move {
            if let Err(error) = run_follow_loop(writer, reader, fields, pool, follow_cfg).await {
                // The loop only returns on a fatal error; surface it loudly. The
                // pod stays up serving reads over the last-committed index.
                warn!(%error, "pathsearch follow loop exited");
            }
        });
    } else {
        info!("follow loop disabled (--follow not set): serving reads over a static index");
    }

    let service = PathSearchServiceServer::new(server);
    info!(index_path = %args.index_path.display(), "pathsearch-server starting");

    match Listen::parse(&args.addr) {
        Listen::Tcp(addr) => {
            info!(%addr, "serving gRPC over TCP");
            Server::builder()
                .add_service(service)
                .serve_with_shutdown(addr, shutdown_signal())
                .await
                .context("gRPC server (TCP) terminated with an error")?;
        }
        Listen::Unix(path) => serve_unix(service, path).await?,
    }

    info!("pathsearch-server stopped");
    Ok(())
}

#[cfg(unix)]
async fn serve_unix(
    service: PathSearchServiceServer<PathSearchServer>,
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
        .add_service(service)
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown_signal())
        .await
        .context("gRPC server (unix) terminated with an error")?;
    Ok(())
}

#[cfg(not(unix))]
#[allow(clippy::unused_async)]
async fn serve_unix(
    _service: PathSearchServiceServer<PathSearchServer>,
    path: PathBuf,
) -> anyhow::Result<()> {
    anyhow::bail!(
        "unix-socket listening ({}) is only supported on unix platforms",
        path.display()
    )
}

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
