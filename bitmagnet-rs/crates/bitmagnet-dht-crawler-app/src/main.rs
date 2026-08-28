use std::future::Future;
use std::io;
use std::pin::Pin;

use anyhow::{bail, Context as _};
use bitmagnet_dht_crawler::DhtCrawlerPipelineSupervisor;
use bitmagnet_dht_crawler_app::{
    register_writer_metrics, supervise_writer_process, DhtCrawlerWriterAppConfig,
    DhtCrawlerWriterAppProjection, DhtCrawlerWriterSignalReceiver,
};
use clap::Parser;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{error, info};

fn main() -> anyhow::Result<()> {
    bitmagnet_common::init_tracing();
    let config = DhtCrawlerWriterAppConfig::parse();
    let projection = config.projection()?;
    let runtime =
        BoundedWriterRuntime::build().context("could not build writer-process Tokio runtime")?;
    runtime.block_on(run_writer(projection))
}

struct BoundedWriterRuntime(Option<Runtime>);

impl BoundedWriterRuntime {
    fn build() -> io::Result<Self> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map(|runtime| Self(Some(runtime)))
    }

    fn runtime(&self) -> &Runtime {
        self.0
            .as_ref()
            .expect("writer runtime remains present until drop")
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime().block_on(future)
    }
}

impl Drop for BoundedWriterRuntime {
    fn drop(&mut self) {
        // The async coordinator has already used its configured hard deadline
        // to join or classify every owned task. A blocking OS resolver can
        // outlive that proof, so normal returns and unwinding must not inherit
        // Tokio's unbounded Runtime::drop wait.
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_timeout(std::time::Duration::ZERO);
        }
    }
}

async fn run_writer(projection: DhtCrawlerWriterAppProjection) -> anyhow::Result<()> {
    let signals = DhtCrawlerWriterSignalReceiver::install()
        .context("could not install writer-process signal handlers")?;
    let mut signal: WriterSignalFuture = Box::pin(signals.recv());
    let startup_deadline = Instant::now() + projection.startup_timeout.duration();

    // Bind every TCP listener before loading secrets, connecting PostgreSQL, or
    // binding UDP so a local address conflict leaves no partial writer graph.
    let listener = match await_startup_or_signal(
        tokio::time::timeout_at(
            startup_deadline,
            tokio::net::TcpListener::bind(projection.http_listen_addr),
        ),
        &mut signal,
    )
    .await
    {
        StartupWait::Completed(Ok(result)) => result.with_context(|| {
            format!(
                "could not bind writer HTTP listener {}",
                projection.http_listen_addr
            )
        })?,
        StartupWait::Completed(Err(_)) => {
            bail!("writer HTTP bind exceeded the configured startup timeout")
        }
        StartupWait::Signal(result) => {
            return finish_startup_signal(result, "before HTTP listener bind", true)
        }
    };
    let bound_http_addr = listener.local_addr()?;
    let metrics = match await_startup_or_signal(
        tokio::time::timeout_at(
            startup_deadline,
            bitmagnet_common::metrics::maybe_spawn_metrics_server(),
        ),
        &mut signal,
    )
    .await
    {
        StartupWait::Completed(Ok(result)) => {
            result.context("could not bind optional writer metrics listener")?
        }
        StartupWait::Completed(Err(_)) => {
            bail!("writer metrics bind exceeded the configured startup timeout")
        }
        StartupWait::Signal(result) => {
            return finish_startup_signal(result, "before metrics listener bind", true)
        }
    };

    let db_config = match bitmagnet_db::DbConfig::from_compatible_env() {
        Ok(config) => config,
        Err(error) => {
            stop_metrics_until(
                metrics,
                startup_cleanup_deadline(startup_deadline, projection.shutdown_timeout.duration()),
            )
            .await;
            return Err(error).context("could not load compatible PostgreSQL configuration");
        }
    };
    let pool = match await_startup_or_signal(
        tokio::time::timeout_at(startup_deadline, bitmagnet_db::connect(&db_config)),
        &mut signal,
    )
    .await
    {
        StartupWait::Completed(result) => match result {
            Ok(Ok(pool)) => pool,
            Ok(Err(error)) => {
                stop_metrics_until(
                    metrics,
                    startup_cleanup_deadline(
                        startup_deadline,
                        projection.shutdown_timeout.duration(),
                    ),
                )
                .await;
                return Err(error).context("could not connect writer PostgreSQL pool");
            }
            Err(_) => {
                stop_metrics_until(metrics, startup_deadline).await;
                bail!("writer PostgreSQL startup exceeded the configured startup timeout");
            }
        },
        StartupWait::Signal(result) => {
            let cleanup_complete = stop_metrics_until(
                metrics,
                startup_cleanup_deadline(startup_deadline, projection.shutdown_timeout.duration()),
            )
            .await;
            return finish_startup_signal(result, "during PostgreSQL connection", cleanup_complete);
        }
    };

    let graph_start = match await_startup_or_signal(
        tokio::time::timeout_at(
            startup_deadline,
            DhtCrawlerPipelineSupervisor::start(projection.crawler, &pool),
        ),
        &mut signal,
    )
    .await
    {
        StartupWait::Completed(result) => result,
        StartupWait::Signal(result) => {
            let cleanup_deadline =
                startup_cleanup_deadline(startup_deadline, projection.shutdown_timeout.duration());
            let _ = close_pool_until(pool, cleanup_deadline).await;
            let _ = stop_metrics_until(metrics, cleanup_deadline).await;
            return finish_startup_signal(result, "during writer graph construction", false);
        }
    };
    let (supervisor, handles) = match graph_start {
        Ok(Ok(started)) => started,
        Ok(Err(error)) => {
            let _ = close_pool_until(pool, startup_deadline).await;
            let _ = stop_metrics_until(metrics, startup_deadline).await;
            return Err(error.into());
        }
        Err(_) => {
            let _ = close_pool_until(pool, startup_deadline).await;
            let _ = stop_metrics_until(metrics, startup_deadline).await;
            bail!("writer graph startup exceeded the configured startup timeout");
        }
    };
    let dht_addr = supervisor.local_addr();
    let observability = supervisor.observability_handle();
    register_writer_metrics(observability.clone());
    let (metrics_handle, metrics_addr) = match metrics {
        Some((handle, address)) => (Some(handle), Some(address)),
        None => (None, None),
    };

    info!(
        http_addr = %bound_http_addr,
        %dht_addr,
        ?metrics_addr,
        "bitmagnet-dht-crawler started"
    );
    let exit = supervise_writer_process(
        listener,
        pool,
        supervisor,
        handles,
        observability,
        metrics_handle,
        signal,
        projection.shutdown_timeout,
    )
    .await;
    if !exit.is_success() {
        error!(?exit, "bitmagnet-dht-crawler stopped abnormally");
        bail!("writer process did not complete a clean signal-triggered shutdown");
    }
    info!(?exit, "bitmagnet-dht-crawler stopped cleanly");
    Ok(())
}

type WriterSignalFuture = Pin<
    Box<
        dyn Future<Output = io::Result<bitmagnet_dht_crawler_app::DhtCrawlerWriterProcessSignal>>
            + Send,
    >,
>;

enum StartupWait<T> {
    Completed(T),
    Signal(io::Result<bitmagnet_dht_crawler_app::DhtCrawlerWriterProcessSignal>),
}

async fn await_startup_or_signal<T>(
    future: impl Future<Output = T>,
    signal: &mut WriterSignalFuture,
) -> StartupWait<T> {
    tokio::select! {
        biased;
        result = signal.as_mut() => StartupWait::Signal(result),
        result = future => StartupWait::Completed(result),
    }
}

fn finish_startup_signal(
    result: io::Result<bitmagnet_dht_crawler_app::DhtCrawlerWriterProcessSignal>,
    phase: &str,
    cleanup_complete: bool,
) -> anyhow::Result<()> {
    match (result, cleanup_complete) {
        (Ok(signal), true) => {
            info!(?signal, %phase, "bitmagnet-dht-crawler stopped during startup");
            Ok(())
        }
        (Ok(signal), false) => {
            bail!("writer startup interrupted by {signal:?} {phase} without complete cleanup proof")
        }
        (Err(error), _) => Err(error).context("writer signal watcher failed during startup"),
    }
}

fn startup_cleanup_deadline(
    startup_deadline: Instant,
    shutdown_timeout: std::time::Duration,
) -> Instant {
    std::cmp::min(startup_deadline, Instant::now() + shutdown_timeout)
}

async fn close_pool_until(pool: bitmagnet_db::PgPool, deadline: Instant) -> bool {
    let close = pool.close();
    tokio::pin!(close);
    tokio::select! {
        biased;
        () = &mut close => true,
        () = tokio::time::sleep_until(deadline) => false,
    }
}

async fn stop_metrics_until(
    metrics: Option<(JoinHandle<()>, std::net::SocketAddr)>,
    deadline: Instant,
) -> bool {
    if let Some((handle, _)) = metrics {
        handle.abort();
        let mut handle = handle;
        tokio::select! {
            biased;
            _ = &mut handle => true,
            () = tokio::time::sleep_until(deadline) => false,
        }
    } else {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant as StdInstant};

    use clap::CommandFactory;

    use super::*;

    #[test]
    fn binary_help_contains_no_database_cli_or_secret_value_surface() {
        let command = DhtCrawlerWriterAppConfig::command();
        let arguments = command
            .get_arguments()
            .map(|argument| argument.get_id().as_str().to_owned())
            .collect::<BTreeSet<_>>();
        assert!(!arguments.iter().any(|argument| {
            argument.contains("postgres")
                || argument.contains("password")
                || argument.contains("dsn")
        }));
    }

    #[tokio::test]
    async fn pending_startup_effect_never_hides_an_installed_signal() {
        let mut signal: WriterSignalFuture = Box::pin(std::future::ready(Ok(
            bitmagnet_dht_crawler_app::DhtCrawlerWriterProcessSignal::Terminate,
        )));
        let outcome = await_startup_or_signal(std::future::pending::<()>(), &mut signal).await;
        assert!(matches!(
            outcome,
            StartupWait::Signal(Ok(
                bitmagnet_dht_crawler_app::DhtCrawlerWriterProcessSignal::Terminate
            ))
        ));
    }

    #[test]
    fn startup_signal_requires_complete_cleanup_for_success() {
        let signal = bitmagnet_dht_crawler_app::DhtCrawlerWriterProcessSignal::Interrupt;
        assert!(finish_startup_signal(Ok(signal), "before effects", true).is_ok());
        assert!(finish_startup_signal(Ok(signal), "during graph start", false).is_err());
        assert!(finish_startup_signal(
            Err(io::Error::other("watcher failed")),
            "before effects",
            true,
        )
        .is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn startup_metrics_cleanup_obeys_its_hard_deadline() {
        let release = Arc::new(AtomicBool::new(false));
        let release_task = release.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let metrics = tokio::task::spawn_blocking(move || {
            let _ = started_tx.send(());
            while !release_task.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
        });
        started_rx.await.unwrap();
        let deadline = Instant::now() + std::time::Duration::from_secs(3);
        let cleanup = tokio::spawn(stop_metrics_until(
            Some((metrics, "127.0.0.1:1".parse().unwrap())),
            deadline,
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(3)).await;

        assert!(!cleanup.await.unwrap());
        release.store(true, Ordering::Release);
    }

    #[test]
    fn runtime_drop_does_not_wait_for_retained_blocking_work() {
        let runtime = BoundedWriterRuntime::build().unwrap();
        let release = Arc::new(AtomicBool::new(false));
        let release_task = release.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (stopped_tx, stopped_rx) = std::sync::mpsc::channel();
        runtime.runtime().spawn_blocking(move || {
            started_tx.send(()).unwrap();
            while !release_task.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            stopped_tx.send(()).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(3)).unwrap();

        let started = StdInstant::now();
        drop(runtime);
        assert!(started.elapsed() < Duration::from_secs(1));

        release.store(true, Ordering::Release);
        stopped_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    }

    #[test]
    fn runtime_drop_is_bounded_while_block_on_unwinds() {
        let release = Arc::new(AtomicBool::new(false));
        let release_task = release.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (stopped_tx, stopped_rx) = std::sync::mpsc::channel();
        let started = StdInstant::now();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let runtime = BoundedWriterRuntime::build().unwrap();
            runtime.runtime().spawn_blocking(move || {
                started_tx.send(()).unwrap();
                while !release_task.load(Ordering::Acquire) {
                    std::thread::yield_now();
                }
                stopped_tx.send(()).unwrap();
            });
            started_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            runtime.block_on(async { panic!("runtime unwind canary") });
        }));

        assert!(outcome.is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
        release.store(true, Ordering::Release);
        stopped_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    }
}
