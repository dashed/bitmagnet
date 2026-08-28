//! Bounded whole-process ownership for the writer-capable DHT graph.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use bitmagnet_blocking::{BlockingFinalizeOutcome, BlockingFinalizer};
use bitmagnet_db::PgPool;
use bitmagnet_dht_crawler::{
    DhtCrawlerPipelineExit, DhtCrawlerPipelineHandles, DhtCrawlerPipelineObservabilityHandle,
    DhtCrawlerPipelineObservabilitySnapshot, DhtCrawlerPipelineSupervisor,
};
use hyper::server::conn::http1::Builder as HttpConnectionBuilder;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::time::Instant;
use tracing::trace;

use crate::{writer_http_router, DhtCrawlerWriterProcessTimeout};

/// OS signal which initiated a normal writer-process drain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DhtCrawlerWriterProcessSignal {
    Interrupt,
    Terminate,
}

/// First process-level event observed by the coordinator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DhtCrawlerWriterProcessTrigger {
    Signal(DhtCrawlerWriterProcessSignal),
    SignalWatcher,
    Supervisor,
    HttpServer,
    MetricsServer,
}

/// Why the coordinator explicitly forced an owned task or descendants to stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DhtCrawlerWriterForcedStopReason {
    CoordinatorCleanup,
    GracefulDeadline,
}

/// Bounded terminal evidence and forced-stop provenance for one owned task tree.
///
/// `result` is absent only when the hard process deadline elapsed before the
/// already-forced owner could be joined. Dropping the owner then preserves the
/// deadline rather than manufacturing cleanup proof.
#[derive(Debug)]
#[non_exhaustive]
pub struct DhtCrawlerWriterTaskExit<T> {
    pub forced_stop_requested: Option<DhtCrawlerWriterForcedStopReason>,
    pub result: Option<Result<T, JoinError>>,
}

/// Deliberate disposition of the retained external finalizer capability.
///
/// No process path retries automatically. A returned store error can represent
/// an acknowledged or ambiguous commit and the stable Bloom mutation is not
/// replay-idempotent. Cancellation additionally lacks producer-quiescence proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DhtCrawlerWriterFinalizerDisposition {
    Completed(BlockingFinalizeOutcome),
    AbandonedAmbiguous,
    AbandonedUnprovenQuiescence,
}

/// Terminal evidence for caller-owned PostgreSQL pool closure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DhtCrawlerWriterPoolCloseDisposition {
    Closed,
    TimedOut,
}

/// Complete joined evidence for one writer-process run.
#[derive(Debug)]
#[non_exhaustive]
pub struct DhtCrawlerWriterProcessExit {
    pub first_trigger: DhtCrawlerWriterProcessTrigger,
    pub shutdown_timeout: DhtCrawlerWriterProcessTimeout,
    pub graceful_shutdown_timed_out: bool,
    pub signal: DhtCrawlerWriterTaskExit<io::Result<DhtCrawlerWriterProcessSignal>>,
    pub supervisor: DhtCrawlerWriterTaskExit<DhtCrawlerPipelineExit>,
    pub http: DhtCrawlerWriterTaskExit<io::Result<()>>,
    pub metrics: Option<DhtCrawlerWriterTaskExit<()>>,
    pub finalizer: DhtCrawlerWriterFinalizerDisposition,
    pub pool_close: DhtCrawlerWriterPoolCloseDisposition,
    pub final_observability: DhtCrawlerPipelineObservabilitySnapshot,
}

impl DhtCrawlerWriterProcessExit {
    /// Whether a real signal produced a complete canonical drain and pool close.
    #[must_use]
    pub fn is_success(&self) -> bool {
        let DhtCrawlerWriterProcessTrigger::Signal(first_signal) = self.first_trigger else {
            return false;
        };
        !self.graceful_shutdown_timed_out
            && self.signal.forced_stop_requested.is_none()
            && matches!(
                self.signal.result.as_ref(),
                Some(Ok(Ok(observed_signal))) if *observed_signal == first_signal
            )
            && self.supervisor.forced_stop_requested.is_none()
            && matches!(
                self.supervisor.result.as_ref(),
                Some(Ok(exit)) if exit.is_clean_external_shutdown()
            )
            && self.http.forced_stop_requested.is_none()
            && matches!(self.http.result.as_ref(), Some(Ok(Ok(()))))
            && self.metrics.as_ref().is_none_or(|metrics| {
                metrics.forced_stop_requested
                    == Some(DhtCrawlerWriterForcedStopReason::CoordinatorCleanup)
                    && matches!(
                        metrics.result.as_ref(),
                        Some(Err(error)) if error.is_cancelled()
                    )
            })
            && matches!(
                self.finalizer,
                DhtCrawlerWriterFinalizerDisposition::Completed(_)
            )
            && self.pool_close == DhtCrawlerWriterPoolCloseDisposition::Closed
    }
}

/// Signal receivers prepared before any listener, pool, or UDP runtime exists.
pub struct DhtCrawlerWriterSignalReceiver {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl DhtCrawlerWriterSignalReceiver {
    pub fn install() -> io::Result<Self> {
        #[cfg(unix)]
        {
            Ok(Self {
                interrupt: tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::interrupt(),
                )?,
                terminate: tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::terminate(),
                )?,
            })
        }

        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    pub async fn recv(mut self) -> io::Result<DhtCrawlerWriterProcessSignal> {
        #[cfg(unix)]
        {
            tokio::select! {
                biased;
                signal = self.interrupt.recv() => signal
                    .map(|()| DhtCrawlerWriterProcessSignal::Interrupt)
                    .ok_or_else(signal_stream_closed),
                signal = self.terminate.recv() => signal
                    .map(|()| DhtCrawlerWriterProcessSignal::Terminate)
                    .ok_or_else(signal_stream_closed),
            }
        }

        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await?;
            Ok(DhtCrawlerWriterProcessSignal::Interrupt)
        }
    }
}

fn signal_stream_closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "process signal stream closed")
}

/// Own the writer graph, HTTP server, signal watcher, optional metrics task,
/// external finalizer decision, and PostgreSQL pool through one process lifetime.
///
/// HTTP remains available until the graph drains, allowing readiness to turn
/// 503 before listener shutdown. The pool remains open until graph and HTTP
/// owners are joined, the no-retry finalizer disposition is recorded, and the
/// retained capability is dropped. Metrics stop only after the pool-close gate.
#[allow(clippy::too_many_arguments)]
pub async fn supervise_writer_process<S>(
    listener: TcpListener,
    pool: PgPool,
    supervisor: DhtCrawlerPipelineSupervisor,
    handles: DhtCrawlerPipelineHandles,
    observability: DhtCrawlerPipelineObservabilityHandle,
    metrics: Option<JoinHandle<()>>,
    signal: S,
    shutdown_timeout: DhtCrawlerWriterProcessTimeout,
) -> DhtCrawlerWriterProcessExit
where
    S: Future<Output = io::Result<DhtCrawlerWriterProcessSignal>> + Send + 'static,
{
    let DhtCrawlerPipelineHandles {
        blocking_finalizer,
        downstream_stats: _,
        lifecycle: _,
    } = handles;
    let router = writer_http_router(observability.clone(), pool.clone());
    let (supervisor_stop, supervisor_stop_rx) = watch::channel(false);
    let (signal, shutdown_before_start) = gate_supervisor_activation_on_signal(signal).await;
    let supervisor_shutdown: Pin<Box<dyn Future<Output = ()> + Send>> = if shutdown_before_start {
        Box::pin(std::future::ready(()))
    } else {
        Box::pin(wait_for_stop(supervisor_stop_rx))
    };
    let supervisor_run = supervisor.run(supervisor_shutdown);
    let snapshot = move || observability.snapshot();
    coordinate_tasks(
        signal,
        supervisor_run,
        supervisor_stop,
        OwnedHttpServer::spawn(listener, router),
        metrics,
        pool,
        blocking_finalizer,
        snapshot,
        shutdown_timeout,
    )
    .await
}

type WriterSignalFuture =
    Pin<Box<dyn Future<Output = io::Result<DhtCrawlerWriterProcessSignal>> + Send + 'static>>;

async fn gate_supervisor_activation_on_signal<S>(signal: S) -> (WriterSignalFuture, bool)
where
    S: Future<Output = io::Result<DhtCrawlerWriterProcessSignal>> + Send + 'static,
{
    let mut signal = Box::pin(signal);
    let pending_signal = tokio::select! {
        biased;
        result = signal.as_mut() => Some(result),
        () = tokio::task::yield_now() => None,
    };
    match pending_signal {
        Some(result) => (Box::pin(std::future::ready(result)), true),
        None => (signal, false),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HttpShutdown {
    Running,
    Graceful,
    Force,
}

struct OwnedHttpServer {
    task: OwnedTask<io::Result<()>>,
    shutdown: watch::Sender<HttpShutdown>,
}

impl OwnedHttpServer {
    fn spawn(listener: TcpListener, router: Router) -> Self {
        let (shutdown, shutdown_rx) = watch::channel(HttpShutdown::Running);
        let task = OwnedTask::spawn(run_owned_http_server(listener, router, shutdown_rx));
        Self { task, shutdown }
    }

    fn graceful_shutdown(&self) {
        self.shutdown.send_if_modified(|state| {
            if *state == HttpShutdown::Running {
                *state = HttpShutdown::Graceful;
                true
            } else {
                false
            }
        });
    }

    fn force_shutdown(&self) {
        self.shutdown.send_replace(HttpShutdown::Force);
    }

    async fn join(&mut self) -> Result<io::Result<()>, JoinError> {
        self.task.join().await
    }

    #[cfg(test)]
    fn abort_for_test(&mut self) {
        self.task.abort_all();
    }
}

async fn run_owned_http_server(
    listener: TcpListener,
    router: Router,
    mut shutdown: watch::Receiver<HttpShutdown>,
) -> io::Result<()> {
    let (connection_stop, _) = watch::channel(false);
    let mut connections = JoinSet::new();
    let mut owner_error = None;
    let mut force_connections = 'accept: loop {
        tokio::select! {
            biased;
            requested = wait_for_http_shutdown(&mut shutdown) => {
                break 'accept requested == HttpShutdown::Force;
            }
            result = connections.join_next(), if !connections.is_empty() => {
                match result.expect("a nonempty HTTP connection set has a task") {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        trace!(%error, "writer HTTP connection ended with a protocol error");
                    }
                    Err(error) => {
                        owner_error = Some(http_connection_join_error(error));
                        break 'accept true;
                    }
                }
            }
            accepted = listener.accept() => match accepted {
                Ok((stream, remote_addr)) => {
                    let router = router.clone();
                    let connection_stop = connection_stop.subscribe();
                    connections.spawn(async move {
                        trace!(%remote_addr, "writer HTTP connection accepted");
                        serve_owned_http_connection(stream, router, connection_stop).await
                    });
                }
                Err(error) => {
                    trace!(%error, "writer HTTP accept failed; retrying");
                    tokio::select! {
                        biased;
                        requested = wait_for_http_shutdown(&mut shutdown) => {
                            break 'accept requested == HttpShutdown::Force;
                        }
                        () = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        }
    };
    drop(listener);

    if force_connections {
        connections.abort_all();
    } else {
        connection_stop.send_replace(true);
    }

    while !connections.is_empty() {
        tokio::select! {
            biased;
            requested = wait_for_http_force(&mut shutdown), if !force_connections => {
                debug_assert_eq!(requested, HttpShutdown::Force);
                force_connections = true;
                connections.abort_all();
            }
            result = connections.join_next() => {
                let result = result.expect("a nonempty HTTP connection set has a task");
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        trace!(%error, "writer HTTP connection ended with a protocol error");
                    }
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => {
                        owner_error.get_or_insert_with(|| http_connection_join_error(error));
                    }
                }
            }
        }
    }

    owner_error.map_or(Ok(()), Err)
}

async fn serve_owned_http_connection(
    stream: tokio::net::TcpStream,
    router: Router,
    graceful_stop: watch::Receiver<bool>,
) -> io::Result<()> {
    let builder = HttpConnectionBuilder::new();
    let connection =
        builder.serve_connection(TokioIo::new(stream), TowerToHyperService::new(router));
    tokio::pin!(connection);

    tokio::select! {
        biased;
        result = &mut connection => result.map_err(io::Error::other),
        () = wait_for_stop(graceful_stop) => {
            connection.as_mut().graceful_shutdown();
            connection.await.map_err(io::Error::other)
        }
    }
}

async fn wait_for_http_shutdown(receiver: &mut watch::Receiver<HttpShutdown>) -> HttpShutdown {
    loop {
        let state = *receiver.borrow_and_update();
        if state != HttpShutdown::Running {
            return state;
        }
        if receiver.changed().await.is_err() {
            return HttpShutdown::Force;
        }
    }
}

async fn wait_for_http_force(receiver: &mut watch::Receiver<HttpShutdown>) -> HttpShutdown {
    loop {
        if *receiver.borrow_and_update() == HttpShutdown::Force {
            return HttpShutdown::Force;
        }
        if receiver.changed().await.is_err() {
            return HttpShutdown::Force;
        }
    }
}

fn http_connection_join_error(error: JoinError) -> io::Error {
    io::Error::other(format!("writer HTTP connection task failed: {error}"))
}

#[allow(clippy::too_many_arguments)]
async fn coordinate_tasks<S, G, O>(
    signal: S,
    supervisor: G,
    supervisor_stop: watch::Sender<bool>,
    mut http_task: OwnedHttpServer,
    metrics: Option<JoinHandle<()>>,
    pool: PgPool,
    finalizer: Arc<dyn BlockingFinalizer>,
    observability: O,
    shutdown_timeout: DhtCrawlerWriterProcessTimeout,
) -> DhtCrawlerWriterProcessExit
where
    S: Future<Output = io::Result<DhtCrawlerWriterProcessSignal>> + Send + 'static,
    G: Future<Output = DhtCrawlerPipelineExit> + Send + 'static,
    O: Fn() -> DhtCrawlerPipelineObservabilitySnapshot,
{
    let mut signal_task = OwnedTask::spawn(signal);
    let mut supervisor_task = OwnedTask::spawn(supervisor);
    let mut metrics_task = metrics.map(OwnedMetricsTask::new);
    let metrics_enabled = metrics_task.is_some();

    let mut signal_result = None;
    let mut supervisor_result = None;
    let mut http_result = None;
    let mut metrics_result = None;
    let mut signal_forced_stop = None;
    let mut supervisor_forced_stop = None;
    let mut http_forced_stop = None;
    let mut metrics_forced_stop = None;

    let first_trigger = tokio::select! {
        biased;
        result = signal_task.join() => {
            let trigger = match &result {
                Ok(Ok(signal)) => DhtCrawlerWriterProcessTrigger::Signal(*signal),
                Ok(Err(_)) | Err(_) => DhtCrawlerWriterProcessTrigger::SignalWatcher,
            };
            signal_result = Some(result);
            trigger
        }
        result = supervisor_task.join() => {
            supervisor_result = Some(result);
            DhtCrawlerWriterProcessTrigger::Supervisor
        }
        result = http_task.join() => {
            http_result = Some(result);
            DhtCrawlerWriterProcessTrigger::HttpServer
        }
        result = wait_for_metrics(&mut metrics_task) => {
            metrics_result = Some(result);
            metrics_task.take();
            DhtCrawlerWriterProcessTrigger::MetricsServer
        }
    };

    let shutdown_started = Instant::now();
    let total = shutdown_timeout.duration();
    let reserve = Duration::from_secs((shutdown_timeout.seconds() / 4).min(5) as u64);
    let graceful_deadline = shutdown_started + total - reserve;
    let hard_deadline = shutdown_started + total;

    let _ = supervisor_stop.send(true);
    let mut http_graceful_stop_sent = false;
    if supervisor_result.is_some() {
        http_task.graceful_shutdown();
        http_graceful_stop_sent = true;
    }

    let graceful = tokio::time::sleep_until(graceful_deadline);
    tokio::pin!(graceful);
    let mut graceful_shutdown_timed_out = false;

    while supervisor_result.is_none() || http_result.is_none() {
        tokio::select! {
            biased;
            result = supervisor_task.join(), if supervisor_result.is_none() => {
                supervisor_result = Some(result);
                if !http_graceful_stop_sent {
                    http_task.graceful_shutdown();
                    http_graceful_stop_sent = true;
                }
            }
            result = http_task.join(), if http_result.is_none() => {
                http_result = Some(result);
            }
            result = wait_for_metrics(&mut metrics_task), if metrics_task.is_some() => {
                metrics_result = Some(result);
                metrics_task.take();
            }
            result = signal_task.join(), if signal_result.is_none() => {
                signal_result = Some(result);
            }
            () = &mut graceful => {
                graceful_shutdown_timed_out = true;
                if signal_result.is_none() {
                    signal_forced_stop = Some(DhtCrawlerWriterForcedStopReason::GracefulDeadline);
                    signal_task.abort_all();
                }
                if supervisor_result.is_none() {
                    supervisor_forced_stop = Some(DhtCrawlerWriterForcedStopReason::GracefulDeadline);
                    supervisor_task.abort_all();
                }
                if http_result.is_none() {
                    http_forced_stop = Some(DhtCrawlerWriterForcedStopReason::GracefulDeadline);
                    http_task.force_shutdown();
                }
                break;
            }
        }
    }

    if signal_result.is_none() {
        if signal_forced_stop.is_none() {
            signal_forced_stop = Some(DhtCrawlerWriterForcedStopReason::CoordinatorCleanup);
        }
        signal_task.abort_all();
        signal_result = join_task_until(&mut signal_task, hard_deadline).await;
    }
    if supervisor_result.is_none() {
        supervisor_result = join_task_until(&mut supervisor_task, hard_deadline).await;
    }
    if http_result.is_none() {
        http_result = join_http_until(&mut http_task, hard_deadline).await;
    }

    let finalizer_disposition = supervisor_result.as_ref().map_or(
        DhtCrawlerWriterFinalizerDisposition::AbandonedUnprovenQuiescence,
        classify_finalizer_disposition,
    );
    drop(finalizer);

    let pool_close = close_pool_until(pool, hard_deadline).await;

    if let Some(metrics) = metrics_task.as_mut() {
        if metrics_forced_stop.is_none() {
            metrics_forced_stop = Some(DhtCrawlerWriterForcedStopReason::CoordinatorCleanup);
        }
        metrics.abort();
        metrics_result = join_metrics_until(metrics, hard_deadline).await;
        metrics_task.take();
    }

    DhtCrawlerWriterProcessExit {
        first_trigger,
        shutdown_timeout,
        graceful_shutdown_timed_out,
        signal: DhtCrawlerWriterTaskExit {
            forced_stop_requested: signal_forced_stop,
            result: signal_result,
        },
        supervisor: DhtCrawlerWriterTaskExit {
            forced_stop_requested: supervisor_forced_stop,
            result: supervisor_result,
        },
        http: DhtCrawlerWriterTaskExit {
            forced_stop_requested: http_forced_stop,
            result: http_result,
        },
        metrics: metrics_enabled.then(|| DhtCrawlerWriterTaskExit {
            forced_stop_requested: metrics_forced_stop,
            result: metrics_result,
        }),
        finalizer: finalizer_disposition,
        pool_close,
        final_observability: observability(),
    }
}

async fn join_task_until<T>(
    task: &mut OwnedTask<T>,
    hard_deadline: Instant,
) -> Option<Result<T, JoinError>>
where
    T: Send + 'static,
{
    tokio::select! {
        biased;
        result = task.join() => Some(result),
        () = tokio::time::sleep_until(hard_deadline) => None,
    }
}

async fn join_http_until(
    http: &mut OwnedHttpServer,
    hard_deadline: Instant,
) -> Option<Result<io::Result<()>, JoinError>> {
    tokio::select! {
        biased;
        result = http.join() => Some(result),
        () = tokio::time::sleep_until(hard_deadline) => None,
    }
}

async fn join_metrics_until(
    metrics: &mut OwnedMetricsTask,
    hard_deadline: Instant,
) -> Option<Result<(), JoinError>> {
    tokio::select! {
        biased;
        result = metrics.join() => Some(result),
        () = tokio::time::sleep_until(hard_deadline) => None,
    }
}

fn classify_finalizer_disposition(
    supervisor: &Result<DhtCrawlerPipelineExit, JoinError>,
) -> DhtCrawlerWriterFinalizerDisposition {
    let Ok(exit) = supervisor else {
        return DhtCrawlerWriterFinalizerDisposition::AbandonedUnprovenQuiescence;
    };
    let blocking = match exit {
        DhtCrawlerPipelineExit::ShutdownBeforeStart { blocking, .. } => blocking,
        DhtCrawlerPipelineExit::Completed(exit) => &exit.blocking,
    };
    match blocking {
        Ok(Ok(outcome)) => DhtCrawlerWriterFinalizerDisposition::Completed(*outcome),
        Ok(Err(_)) | Err(_) => DhtCrawlerWriterFinalizerDisposition::AbandonedAmbiguous,
    }
}

async fn close_pool_until(
    pool: PgPool,
    hard_deadline: Instant,
) -> DhtCrawlerWriterPoolCloseDisposition {
    let close = pool.close();
    tokio::pin!(close);
    tokio::select! {
        biased;
        () = &mut close => DhtCrawlerWriterPoolCloseDisposition::Closed,
        () = tokio::time::sleep_until(hard_deadline) => {
            DhtCrawlerWriterPoolCloseDisposition::TimedOut
        }
    }
}

async fn wait_for_stop(mut receiver: watch::Receiver<bool>) {
    loop {
        if *receiver.borrow_and_update() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

async fn wait_for_metrics(metrics: &mut Option<OwnedMetricsTask>) -> Result<(), JoinError> {
    match metrics {
        Some(metrics) => metrics.join().await,
        None => std::future::pending().await,
    }
}

struct OwnedTask<T>
where
    T: Send + 'static,
{
    tasks: JoinSet<T>,
}

impl<T> OwnedTask<T>
where
    T: Send + 'static,
{
    fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        let mut tasks = JoinSet::new();
        tasks.spawn(future);
        Self { tasks }
    }

    async fn join(&mut self) -> Result<T, JoinError> {
        self.tasks
            .join_next()
            .await
            .expect("the one-task owner is awaited only while its task is pending")
    }

    fn abort_all(&mut self) {
        self.tasks.abort_all();
    }
}

struct OwnedMetricsTask {
    handle: Option<JoinHandle<()>>,
}

impl OwnedMetricsTask {
    fn new(handle: JoinHandle<()>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    async fn join(&mut self) -> Result<(), JoinError> {
        let result = self
            .handle
            .as_mut()
            .expect("the metrics handle is awaited only once")
            .await;
        self.handle.take();
        result
    }

    fn abort(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

impl Drop for OwnedMetricsTask {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    use async_trait::async_trait;
    use axum::routing::get;
    use bitmagnet_blocking::{BlockingError, BlockingFinalizer};
    use bitmagnet_dht::DhtRuntimeExit;
    use bitmagnet_dht_crawler::{
        DhtCrawlerPipelineDownstreamObservabilitySnapshot,
        DhtCrawlerPipelineMaintenanceObservabilitySnapshot, DhtCrawlerPipelineObservedLifecycle,
        DhtCrawlerPipelineRuntimeObservabilitySnapshot,
    };
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use tokio::io::AsyncWriteExt as _;
    use tokio::sync::oneshot;

    use super::*;

    fn timeout(seconds: u64) -> DhtCrawlerWriterProcessTimeout {
        DhtCrawlerWriterProcessTimeout::from_seconds(seconds).unwrap()
    }

    fn lazy_pool() -> PgPool {
        PgPoolOptions::new().connect_lazy_with(
            PgConnectOptions::new()
                .host("127.0.0.1")
                .port(1)
                .database("bitmagnet_process_test"),
        )
    }

    fn snapshot(
        lifecycle: DhtCrawlerPipelineObservedLifecycle,
    ) -> DhtCrawlerPipelineObservabilitySnapshot {
        DhtCrawlerPipelineObservabilitySnapshot {
            lifecycle,
            runtime: DhtCrawlerPipelineRuntimeObservabilitySnapshot::default(),
            maintenance: DhtCrawlerPipelineMaintenanceObservabilitySnapshot::default(),
            downstream: DhtCrawlerPipelineDownstreamObservabilitySnapshot::default(),
        }
    }

    fn clean_exit() -> DhtCrawlerPipelineExit {
        DhtCrawlerPipelineExit::ShutdownBeforeStart {
            runtime: Ok(DhtRuntimeExit::Shutdown),
            blocking: Ok(Ok(BlockingFinalizeOutcome::NothingPending)),
        }
    }

    fn ambiguous_exit() -> DhtCrawlerPipelineExit {
        DhtCrawlerPipelineExit::ShutdownBeforeStart {
            runtime: Ok(DhtRuntimeExit::Shutdown),
            blocking: Ok(Err(BlockingError::Store(Box::new(io::Error::other(
                "ambiguous commit",
            ))))),
        }
    }

    struct NeverRetryFinalizer(AtomicUsize);

    #[async_trait]
    impl BlockingFinalizer for NeverRetryFinalizer {
        async fn finalize(&self) -> Result<BlockingFinalizeOutcome, BlockingError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("the process boundary must never retry finalization")
        }
    }

    async fn listener_and_http() -> (OwnedHttpServer, std::net::SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = Router::new().route("/livez", get(|| async { "ok" }));
        (OwnedHttpServer::spawn(listener, router), address)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn immediately_ready_signal_prevents_supervisor_first_poll() {
        let supervisor_polled = Arc::new(AtomicBool::new(false));
        let supervisor_task = tokio::spawn({
            let supervisor_polled = supervisor_polled.clone();
            async move {
                supervisor_polled.store(true, Ordering::SeqCst);
                std::future::pending::<()>().await;
            }
        });

        let (signal, shutdown_before_start) = gate_supervisor_activation_on_signal(
            std::future::ready(Ok(DhtCrawlerWriterProcessSignal::Terminate)),
        )
        .await;

        assert!(shutdown_before_start);
        assert_eq!(
            signal.await.unwrap(),
            DhtCrawlerWriterProcessSignal::Terminate
        );
        assert!(!supervisor_polled.load(Ordering::SeqCst));
        supervisor_task.abort();
        assert!(supervisor_task.await.unwrap_err().is_cancelled());
        assert!(!supervisor_polled.load(Ordering::SeqCst));
    }

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn forced_http_shutdown_aborts_and_joins_a_blocked_handler() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (entered_tx, entered_rx) = oneshot::channel();
        let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let dropped_tx = Arc::new(Mutex::new(Some(dropped_tx)));
        let router = Router::new().route(
            "/blocked",
            get({
                let entered_tx = entered_tx.clone();
                let dropped_tx = dropped_tx.clone();
                move || {
                    let entered_tx = entered_tx.clone();
                    let dropped_tx = dropped_tx.clone();
                    async move {
                        let guard = DropSignal(dropped_tx.lock().unwrap().take());
                        if let Some(sender) = entered_tx.lock().unwrap().take() {
                            let _ = sender.send(());
                        }
                        std::future::pending::<()>().await;
                        drop(guard);
                        "unreachable"
                    }
                }
            }),
        );
        let mut http = OwnedHttpServer::spawn(listener, router);
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET /blocked HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        entered_rx.await.expect("the blocked handler started");

        http.graceful_shutdown();
        tokio::task::yield_now().await;
        http.force_shutdown();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), http.join()).await,
            Ok(Ok(Ok(())))
        ));
        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("the blocked handler was dropped")
            .expect("the drop signal was delivered");
        drop(client);
        drop(
            TcpListener::bind(address)
                .await
                .expect("HTTP socket is released"),
        );
    }

    #[tokio::test]
    async fn signal_drains_graph_closes_http_then_pool_without_retry() {
        let pool = lazy_pool();
        let retained_pool = pool.clone();
        let finalizer = Arc::new(NeverRetryFinalizer(AtomicUsize::new(0)));
        let (http, address) = listener_and_http().await;
        let (stop, stop_rx) = watch::channel(false);
        let supervisor = async move {
            wait_for_stop(stop_rx).await;
            clean_exit()
        };

        let exit = coordinate_tasks(
            std::future::ready(Ok(DhtCrawlerWriterProcessSignal::Terminate)),
            supervisor,
            stop,
            http,
            Some(tokio::spawn(std::future::pending())),
            pool,
            finalizer.clone(),
            || snapshot(DhtCrawlerPipelineObservedLifecycle::Stopped),
            timeout(10),
        )
        .await;

        assert!(exit.is_success(), "{exit:?}");
        assert_eq!(finalizer.0.load(Ordering::SeqCst), 0);
        assert!(retained_pool.is_closed());
        drop(
            TcpListener::bind(address)
                .await
                .expect("HTTP address released"),
        );
    }

    #[tokio::test]
    async fn ambiguous_finalizer_is_abandoned_once_and_process_fails() {
        let pool = lazy_pool();
        let retained_pool = pool.clone();
        let finalizer = Arc::new(NeverRetryFinalizer(AtomicUsize::new(0)));
        let (http, _) = listener_and_http().await;
        let (stop, stop_rx) = watch::channel(false);
        let supervisor = async move {
            wait_for_stop(stop_rx).await;
            ambiguous_exit()
        };

        let exit = coordinate_tasks(
            std::future::ready(Ok(DhtCrawlerWriterProcessSignal::Interrupt)),
            supervisor,
            stop,
            http,
            None,
            pool,
            finalizer.clone(),
            || snapshot(DhtCrawlerPipelineObservedLifecycle::Stopped),
            timeout(10),
        )
        .await;

        assert_eq!(
            exit.finalizer,
            DhtCrawlerWriterFinalizerDisposition::AbandonedAmbiguous
        );
        assert!(!exit.is_success());
        assert_eq!(finalizer.0.load(Ordering::SeqCst), 0);
        assert!(retained_pool.is_closed());
    }

    #[tokio::test(start_paused = true)]
    async fn shared_deadline_aborts_graph_and_abandons_unproven_finalizer() {
        let pool = lazy_pool();
        let retained_pool = pool.clone();
        let finalizer = Arc::new(NeverRetryFinalizer(AtomicUsize::new(0)));
        let (http, _) = listener_and_http().await;
        let (stop, _stop_rx) = watch::channel(false);

        let exit = coordinate_tasks(
            std::future::ready(Ok(DhtCrawlerWriterProcessSignal::Terminate)),
            std::future::pending(),
            stop,
            http,
            Some(tokio::spawn(std::future::pending())),
            pool,
            finalizer.clone(),
            || {
                snapshot(DhtCrawlerPipelineObservedLifecycle::Cancelled {
                    last: bitmagnet_dht_crawler::DhtCrawlerPipelineLifecycle::Stopping,
                })
            },
            timeout(10),
        )
        .await;

        assert!(exit.graceful_shutdown_timed_out);
        assert_eq!(
            exit.finalizer,
            DhtCrawlerWriterFinalizerDisposition::AbandonedUnprovenQuiescence
        );
        assert!(matches!(
            exit.supervisor.result.as_ref(),
            Some(Err(error)) if error.is_cancelled()
        ));
        assert_eq!(finalizer.0.load(Ordering::SeqCst), 0);
        assert!(retained_pool.is_closed());
        assert!(!exit.is_success());
    }

    #[tokio::test(start_paused = true)]
    async fn hard_deadline_does_not_wait_for_noncooperative_metrics_owner() {
        let pool = lazy_pool();
        let finalizer = Arc::new(NeverRetryFinalizer(AtomicUsize::new(0)));
        let (http, _) = listener_and_http().await;
        let (stop, stop_rx) = watch::channel(false);
        let supervisor = async move {
            wait_for_stop(stop_rx).await;
            clean_exit()
        };
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
        let started = Instant::now();

        let process = tokio::spawn(coordinate_tasks(
            std::future::ready(Ok(DhtCrawlerWriterProcessSignal::Terminate)),
            supervisor,
            stop,
            http,
            Some(metrics),
            pool,
            finalizer,
            || snapshot(DhtCrawlerPipelineObservedLifecycle::Stopped),
            timeout(10),
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        let exit = process.await.unwrap();
        release.store(true, Ordering::Release);

        assert_eq!(Instant::now() - started, Duration::from_secs(10));
        assert!(matches!(
            &exit.metrics,
            Some(DhtCrawlerWriterTaskExit {
                forced_stop_requested: Some(DhtCrawlerWriterForcedStopReason::CoordinatorCleanup),
                result: None,
            })
        ));
        assert!(!exit.is_success());
    }

    #[tokio::test]
    async fn unexpected_http_completion_drains_graph_but_is_not_success() {
        let pool = lazy_pool();
        let finalizer = Arc::new(NeverRetryFinalizer(AtomicUsize::new(0)));
        let (mut http, _) = listener_and_http().await;
        http.abort_for_test();
        let (stop, stop_rx) = watch::channel(false);
        let supervisor = async move {
            wait_for_stop(stop_rx).await;
            clean_exit()
        };

        let exit = coordinate_tasks(
            std::future::pending(),
            supervisor,
            stop,
            http,
            None,
            pool,
            finalizer,
            || snapshot(DhtCrawlerPipelineObservedLifecycle::Stopped),
            timeout(10),
        )
        .await;

        assert_eq!(
            exit.first_trigger,
            DhtCrawlerWriterProcessTrigger::HttpServer
        );
        assert!(!exit.is_success());
        assert!(matches!(
            exit.supervisor.result.as_ref(),
            Some(Ok(graph)) if graph.is_clean_external_shutdown()
        ));
    }

    #[tokio::test]
    async fn unexpected_supervisor_completion_closes_http_and_pool_but_is_not_success() {
        let pool = lazy_pool();
        let retained_pool = pool.clone();
        let finalizer = Arc::new(NeverRetryFinalizer(AtomicUsize::new(0)));
        let (http, address) = listener_and_http().await;
        let (stop, _stop_rx) = watch::channel(false);

        let exit = coordinate_tasks(
            std::future::pending(),
            std::future::ready(clean_exit()),
            stop,
            http,
            None,
            pool,
            finalizer,
            || snapshot(DhtCrawlerPipelineObservedLifecycle::Stopped),
            timeout(10),
        )
        .await;

        assert_eq!(
            exit.first_trigger,
            DhtCrawlerWriterProcessTrigger::Supervisor
        );
        assert_eq!(
            exit.signal.forced_stop_requested,
            Some(DhtCrawlerWriterForcedStopReason::CoordinatorCleanup)
        );
        assert!(matches!(
            exit.signal.result.as_ref(),
            Some(Err(error)) if error.is_cancelled()
        ));
        assert!(matches!(exit.http.result.as_ref(), Some(Ok(Ok(())))));
        assert!(retained_pool.is_closed());
        assert!(!exit.is_success());
        drop(
            TcpListener::bind(address)
                .await
                .expect("HTTP address released"),
        );
    }

    #[tokio::test]
    async fn unexpected_metrics_completion_drains_graph_but_is_not_success() {
        let pool = lazy_pool();
        let retained_pool = pool.clone();
        let finalizer = Arc::new(NeverRetryFinalizer(AtomicUsize::new(0)));
        let (http, _) = listener_and_http().await;
        let (stop, stop_rx) = watch::channel(false);
        let supervisor = async move {
            wait_for_stop(stop_rx).await;
            clean_exit()
        };

        let exit = coordinate_tasks(
            std::future::pending(),
            supervisor,
            stop,
            http,
            Some(tokio::spawn(async {})),
            pool,
            finalizer,
            || snapshot(DhtCrawlerPipelineObservedLifecycle::Stopped),
            timeout(10),
        )
        .await;

        assert_eq!(
            exit.first_trigger,
            DhtCrawlerWriterProcessTrigger::MetricsServer
        );
        assert!(matches!(
            &exit.metrics,
            Some(DhtCrawlerWriterTaskExit {
                forced_stop_requested: None,
                result: Some(Ok(())),
            })
        ));
        assert!(matches!(
            exit.supervisor.result.as_ref(),
            Some(Ok(graph)) if graph.is_clean_external_shutdown()
        ));
        assert!(retained_pool.is_closed());
        assert!(!exit.is_success());
    }

    #[tokio::test]
    async fn signal_watcher_failure_drains_graph_but_is_not_success() {
        let pool = lazy_pool();
        let finalizer = Arc::new(NeverRetryFinalizer(AtomicUsize::new(0)));
        let (http, _) = listener_and_http().await;
        let (stop, stop_rx) = watch::channel(false);
        let supervisor = async move {
            wait_for_stop(stop_rx).await;
            clean_exit()
        };

        let exit = coordinate_tasks(
            std::future::ready(Err(io::Error::other("signal watcher failed"))),
            supervisor,
            stop,
            http,
            None,
            pool,
            finalizer,
            || snapshot(DhtCrawlerPipelineObservedLifecycle::Stopped),
            timeout(10),
        )
        .await;

        assert_eq!(
            exit.first_trigger,
            DhtCrawlerWriterProcessTrigger::SignalWatcher
        );
        assert!(
            matches!(exit.signal.result.as_ref(), Some(Ok(Err(error))) if error.kind() == io::ErrorKind::Other)
        );
        assert!(!exit.is_success());
    }
}
