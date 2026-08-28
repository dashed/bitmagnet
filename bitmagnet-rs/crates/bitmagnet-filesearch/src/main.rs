//! Entry point for the bitmagnet file-search sidecar.
//!
//! Serves the `bitmagnet.v1` `FileSearchService` over gRPC from the current
//! immutable Parquet generation. The real (DuckDB) engine is compiled only with
//! the `duckdb-engine` feature (the production image); without it the binary
//! still builds and starts but refuses to serve (so CI / the default build stay
//! fast and offline).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
#[cfg(any(feature = "duckdb-engine", test))]
use bitmagnet_common::metrics::maybe_spawn_metrics_server;
#[cfg(feature = "duckdb-engine")]
use bitmagnet_common::serve::{serve_router, shutdown_signal, Listen};
use bitmagnet_filesearch::generation::GenerationManager;
use bitmagnet_filesearch::service::ServiceConfig;
use bitmagnet_parquet::generation::Layout;
use clap::Parser;
#[cfg(feature = "duckdb-engine")]
use tracing::info;

#[derive(Debug, Parser)]
#[command(
    name = "bitmagnet-filesearch",
    about = "DuckDB-on-Parquet file-search sidecar"
)]
struct Args {
    /// gRPC listen address (`HOST:PORT`), or a `unix:` socket path.
    #[arg(
        long,
        env = "BITMAGNET_FILESEARCH_ADDR",
        default_value = "127.0.0.1:50052"
    )]
    addr: String,

    /// Generation root (the `{base,seg,delta}/…` tree written by bitmagnet-parquet).
    #[arg(
        long,
        env = "BITMAGNET_PARQUET_ROOT",
        default_value = "/var/lib/bitmagnet/parquet"
    )]
    root: PathBuf,

    /// Max concurrent in-flight engine queries (CB knee).
    #[arg(long, env = "BITMAGNET_FILESEARCH_CONCURRENCY", default_value_t = 6)]
    concurrency: usize,

    /// Per-query deadline in milliseconds.
    #[arg(
        long,
        env = "BITMAGNET_FILESEARCH_DEADLINE_MS",
        default_value_t = 10_000
    )]
    deadline_ms: u64,

    /// DuckDB per-query threads (production engine only).
    #[arg(long, env = "BITMAGNET_FILESEARCH_THREADS", default_value_t = 4)]
    threads: u32,

    /// DuckDB memory limit (production engine only).
    #[arg(long, env = "BITMAGNET_FILESEARCH_MEMORY", default_value = "4GB")]
    memory: String,

    /// Periodic generation re-resolve interval in seconds (0 = disabled).
    /// The refresh CronJob publishes new generations by atomic symlink swap;
    /// without a self-reload the sidecar would keep serving its open
    /// generation until a Reload RPC or restart. `reload()` short-circuits
    /// when the `current` pointers haven't moved, so the idle cost is two
    /// readlinks per tick.
    #[arg(long, env = "BITMAGNET_FILESEARCH_RELOAD_SECS", default_value_t = 30)]
    reload_secs: u64,
}

/// Spawn the periodic self-reload loop (freshness: delta swaps become visible
/// within `reload_secs` without any RPC coordination).
fn spawn_reload_loop(gens: Arc<GenerationManager>, reload_secs: u64) {
    if reload_secs == 0 {
        return;
    }
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(reload_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            match gens.reload(None) {
                Ok((gen, true)) => tracing::info!(
                    base = %gen.base_version,
                    manifest_version = gen.manifest_version,
                    segment_count = gen.segment_count,
                    delta = %gen.delta_version,
                    "generation reloaded"
                ),
                Ok((_, false)) => {}
                Err(e) => tracing::warn!(error = %e, "generation reload failed; keeping current"),
            }
        }
    });
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bitmagnet_common::init_tracing();
    let args = Args::parse();

    let layout = Layout::new(&args.root);
    let gens = Arc::new(
        GenerationManager::open(layout)
            .with_context(|| format!("opening generation at {}", args.root.display()))?,
    );
    let cfg = ServiceConfig {
        max_concurrency: args.concurrency,
        query_deadline: Duration::from_millis(args.deadline_ms),
    };
    spawn_reload_loop(gens.clone(), args.reload_secs);

    run(&args, gens, cfg).await
}

/// Production path: build the DuckDB engine + serve.
#[cfg(feature = "duckdb-engine")]
async fn run(args: &Args, gens: Arc<GenerationManager>, cfg: ServiceConfig) -> anyhow::Result<()> {
    use bitmagnet_filesearch::engine::duck::{DuckConfig, DuckEngine};
    use bitmagnet_filesearch::service::FileSearchServer;
    use bitmagnet_proto::v1::file_search_service_server::FileSearchServiceServer;

    let engine = Arc::new(DuckEngine::open(DuckConfig {
        threads: args.threads,
        memory_limit: args.memory.clone(),
        pool_size: args.concurrency.max(1) + 1,
        ..DuckConfig::default() // temp_dir: BITMAGNET_FILESEARCH_TMPDIR or $TMPDIR spill dir
    })?);
    let _metrics_server = maybe_start_metrics().await?;
    let service = FileSearchServiceServer::new(FileSearchServer::new(gens, engine, cfg));

    let listen = Listen::parse(&args.addr);
    info!(addr = %args.addr, "bitmagnet-filesearch serving (duckdb engine)");
    serve(service, listen).await
}

/// Default build: no engine compiled — refuse to serve with a clear message.
#[cfg(not(feature = "duckdb-engine"))]
async fn run(
    _args: &Args,
    _gens: Arc<GenerationManager>,
    _cfg: ServiceConfig,
) -> anyhow::Result<()> {
    anyhow::bail!(
        "bitmagnet-filesearch was built WITHOUT the `duckdb-engine` feature — the production \
         image must build with `--features duckdb-engine` (needs a C++ toolchain for libduckdb). \
         See docs/dev/dv2-l2-build-notes.md."
    )
}

#[cfg(feature = "duckdb-engine")]
async fn serve(
    service: bitmagnet_proto::v1::file_search_service_server::FileSearchServiceServer<
        bitmagnet_filesearch::service::FileSearchServer<
            bitmagnet_filesearch::engine::duck::DuckEngine,
        >,
    >,
    listen: Listen,
) -> anyhow::Result<()> {
    use tonic::transport::Server;

    let termination_context = match &listen {
        Listen::Tcp(_) => "gRPC server (TCP) terminated",
        Listen::Unix(_) => "gRPC server (unix) terminated",
    };
    let router = Server::builder().add_service(service);
    serve_router(router, listen, shutdown_signal())
        .await
        .context(termination_context)
}

#[cfg(any(feature = "duckdb-engine", test))]
async fn maybe_start_metrics(
) -> anyhow::Result<Option<(tokio::task::JoinHandle<()>, std::net::SocketAddr)>> {
    maybe_spawn_metrics_server()
        .await
        .context("starting Prometheus metrics listener")
}

#[cfg(test)]
mod tests {
    use super::maybe_start_metrics;
    use std::ffi::OsString;

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

    #[tokio::test]
    async fn metrics_listener_is_disabled_by_default() {
        let _restore = MetricsAddrRestore::clear();
        let server = maybe_start_metrics()
            .await
            .expect("an unset metrics address is valid");

        assert!(server.is_none(), "no metrics listener should be created");
    }
}
