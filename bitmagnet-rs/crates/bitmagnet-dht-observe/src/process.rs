//! Bounded whole-process ownership for the observe-only DHT graph.

use std::fmt;
use std::future::Future;
use std::io;
use std::num::NonZeroU16;
use std::str::FromStr;
use std::time::Duration;

use axum::Router;
use bitmagnet_dht_crawler::{
    DhtCrawlerObserveOnlyExit, DhtCrawlerObserveOnlyObservabilityHandle,
    DhtCrawlerObserveOnlyObservabilitySnapshot, DhtCrawlerObserveOnlySupervisor,
};
use hyper::server::conn::http1::Builder as HttpConnectionBuilder;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tracing::trace;

use crate::observe_only_http_router;

/// Default whole-process drain budget.
pub const DHT_OBSERVE_DEFAULT_SHUTDOWN_TIMEOUT_SECONDS: u16 = 20;
/// Largest accepted whole-process drain budget.
pub const DHT_OBSERVE_MAX_SHUTDOWN_TIMEOUT_SECONDS: u16 = 300;

/// Validated positive, operationally bounded whole-process drain budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DhtObserveShutdownTimeout(NonZeroU16);

impl DhtObserveShutdownTimeout {
    /// The production default.
    pub const DEFAULT: Self = Self(
        NonZeroU16::new(DHT_OBSERVE_DEFAULT_SHUTDOWN_TIMEOUT_SECONDS)
            .expect("the default shutdown timeout is nonzero"),
    );

    /// Validate a timeout expressed in whole seconds.
    pub const fn from_seconds(seconds: u64) -> Result<Self, DhtObserveShutdownTimeoutError> {
        if seconds == 0 {
            return Err(DhtObserveShutdownTimeoutError::Zero);
        }
        if seconds > DHT_OBSERVE_MAX_SHUTDOWN_TIMEOUT_SECONDS as u64 {
            return Err(DhtObserveShutdownTimeoutError::TooLarge {
                seconds,
                maximum_seconds: DHT_OBSERVE_MAX_SHUTDOWN_TIMEOUT_SECONDS,
            });
        }
        Ok(Self(
            NonZeroU16::new(seconds as u16).expect("validated seconds are nonzero"),
        ))
    }

    /// Return the configured whole seconds.
    #[must_use]
    pub const fn seconds(self) -> u16 {
        self.0.get()
    }

    /// Return the timeout as a standard duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        Duration::from_secs(self.seconds() as u64)
    }
}

impl Default for DhtObserveShutdownTimeout {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl fmt::Display for DhtObserveShutdownTimeout {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.seconds().fmt(formatter)
    }
}

impl FromStr for DhtObserveShutdownTimeout {
    type Err = DhtObserveShutdownTimeoutError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let seconds = value
            .parse::<u64>()
            .map_err(|_| DhtObserveShutdownTimeoutError::NotUnsignedInteger)?;
        Self::from_seconds(seconds)
    }
}

/// Invalid whole-process drain budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DhtObserveShutdownTimeoutError {
    /// The budget was not an unsigned whole-second value.
    #[error("observe-only shutdown timeout must be an unsigned integer number of seconds")]
    NotUnsignedInteger,
    /// Zero cannot bound cleanup meaningfully.
    #[error("observe-only shutdown timeout must be positive")]
    Zero,
    /// The budget would defeat the operational shutdown ceiling.
    #[error("observe-only shutdown timeout {seconds}s exceeds maximum {maximum_seconds}s")]
    TooLarge { seconds: u64, maximum_seconds: u16 },
}

/// OS signal which initiated a normal process drain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DhtObserveProcessSignal {
    Interrupt,
    Terminate,
}

/// First process-level event observed by the coordinator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DhtObserveProcessTrigger {
    Signal(DhtObserveProcessSignal),
    SignalWatcher,
    Supervisor,
    HttpServer,
    MetricsServer,
}

/// Why the coordinator explicitly forced an owned task or its descendants to stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DhtObserveForcedStopReason {
    /// A peer completed and the remaining background owner was no longer needed.
    CoordinatorCleanup,
    /// The shared whole-process drain budget expired.
    ShutdownDeadline,
}

/// Exact terminal evidence and forced-stop provenance for one owned task tree.
#[derive(Debug)]
#[non_exhaustive]
pub struct DhtObserveTaskExit<T> {
    /// Explicit forced stop requested for this task or owned descendants.
    pub forced_stop_requested: Option<DhtObserveForcedStopReason>,
    /// Joined terminal result of the top-level owner.
    pub result: Result<T, JoinError>,
}

/// Complete joined evidence for one observe-only process run.
#[derive(Debug)]
#[non_exhaustive]
pub struct DhtObserveProcessExit {
    pub first_trigger: DhtObserveProcessTrigger,
    pub shutdown_timeout: DhtObserveShutdownTimeout,
    pub shutdown_timed_out: bool,
    pub signal: DhtObserveTaskExit<io::Result<DhtObserveProcessSignal>>,
    pub supervisor: DhtObserveTaskExit<DhtCrawlerObserveOnlyExit>,
    pub http: DhtObserveTaskExit<io::Result<()>>,
    pub metrics: Option<DhtObserveTaskExit<()>>,
    pub final_observability: DhtCrawlerObserveOnlyObservabilitySnapshot,
}

impl DhtObserveProcessExit {
    /// Whether a real signal led to the exact complete graceful-drain evidence.
    #[must_use]
    pub fn is_success(&self) -> bool {
        let DhtObserveProcessTrigger::Signal(first_signal) = self.first_trigger else {
            return false;
        };
        !self.shutdown_timed_out
            && self.signal.forced_stop_requested.is_none()
            && matches!(
                &self.signal.result,
                Ok(Ok(observed_signal)) if *observed_signal == first_signal
            )
            && self.supervisor.forced_stop_requested.is_none()
            && matches!(
                &self.supervisor.result,
                Ok(exit) if exit.is_clean_external_shutdown()
            )
            && self.http.forced_stop_requested.is_none()
            && matches!(&self.http.result, Ok(Ok(())))
            && self.metrics.as_ref().is_none_or(|metrics| {
                metrics.forced_stop_requested
                    == Some(DhtObserveForcedStopReason::CoordinatorCleanup)
                    && matches!(&metrics.result, Err(error) if error.is_cancelled())
            })
    }
}

/// Signal receivers prepared before either process listener is bound.
///
/// Unix registers both streams during [`Self::install`]. Other targets defer
/// `ctrl_c` registration until [`Self::recv`] is first polled.
pub struct DhtObserveSignalReceiver {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl DhtObserveSignalReceiver {
    /// Prepare process signal handling without binding TCP or UDP.
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

    /// Wait for the first installed interrupt or termination signal.
    pub async fn recv(mut self) -> io::Result<DhtObserveProcessSignal> {
        #[cfg(unix)]
        {
            tokio::select! {
                biased;
                signal = self.interrupt.recv() => signal
                    .map(|()| DhtObserveProcessSignal::Interrupt)
                    .ok_or_else(signal_stream_closed),
                signal = self.terminate.recv() => signal
                    .map(|()| DhtObserveProcessSignal::Terminate)
                    .ok_or_else(signal_stream_closed),
            }
        }

        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await?;
            Ok(DhtObserveProcessSignal::Interrupt)
        }
    }
}

fn signal_stream_closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "process signal stream closed")
}

/// Own the observe-only graph, Axum server, signal watcher, and optional
/// metrics task through one bounded process lifecycle.
///
/// HTTP remains available while the graph drains, so readiness can turn 503
/// before the listener stops. Every spawned task, including every accepted
/// HTTP connection, is joined on normal return. Deadline expiry aborts only
/// unfinished owners and then drains their exact terminal evidence.
pub async fn supervise_observe_only_process<S>(
    listener: TcpListener,
    supervisor: DhtCrawlerObserveOnlySupervisor,
    observability: DhtCrawlerObserveOnlyObservabilityHandle,
    metrics: Option<JoinHandle<()>>,
    signal: S,
    shutdown_timeout: DhtObserveShutdownTimeout,
) -> DhtObserveProcessExit
where
    S: Future<Output = io::Result<DhtObserveProcessSignal>> + Send + 'static,
{
    let (supervisor_stop, supervisor_stop_rx) = watch::channel(false);
    let supervisor_run = supervisor.run(wait_for_stop(supervisor_stop_rx));
    let router = observe_only_http_router(observability.clone());
    let http = OwnedHttpServer::spawn(listener, router);

    coordinate_tasks(
        signal,
        supervisor_run,
        supervisor_stop,
        http,
        metrics,
        observability,
        shutdown_timeout,
    )
    .await
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
                        trace!(%error, "observe-only HTTP connection ended with a protocol error");
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
                        trace!(%remote_addr, "observe-only HTTP connection accepted");
                        serve_owned_http_connection(stream, router, connection_stop).await
                    });
                }
                Err(error) => {
                    trace!(%error, "observe-only HTTP accept failed; retrying");
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
                        trace!(%error, "observe-only HTTP connection ended with a protocol error");
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
    io::Error::other(format!("observe-only HTTP connection task failed: {error}"))
}

#[allow(clippy::too_many_arguments)]
async fn coordinate_tasks<S, G>(
    signal: S,
    supervisor: G,
    supervisor_stop: watch::Sender<bool>,
    mut http_task: OwnedHttpServer,
    metrics: Option<JoinHandle<()>>,
    observability: DhtCrawlerObserveOnlyObservabilityHandle,
    shutdown_timeout: DhtObserveShutdownTimeout,
) -> DhtObserveProcessExit
where
    S: Future<Output = io::Result<DhtObserveProcessSignal>> + Send + 'static,
    G: Future<Output = DhtCrawlerObserveOnlyExit> + Send + 'static,
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
                Ok(Ok(signal)) => DhtObserveProcessTrigger::Signal(*signal),
                Ok(Err(_)) | Err(_) => DhtObserveProcessTrigger::SignalWatcher,
            };
            signal_result = Some(result);
            trigger
        }
        result = supervisor_task.join() => {
            supervisor_result = Some(result);
            DhtObserveProcessTrigger::Supervisor
        }
        result = http_task.join() => {
            http_result = Some(result);
            DhtObserveProcessTrigger::HttpServer
        }
        result = wait_for_metrics(&mut metrics_task) => {
            metrics_result = Some(result);
            metrics_task.take();
            DhtObserveProcessTrigger::MetricsServer
        }
    };

    let _ = supervisor_stop.send(true);
    let mut http_graceful_stop_sent = false;
    if supervisor_result.is_some() {
        http_task.graceful_shutdown();
        http_graceful_stop_sent = true;
    }

    let deadline = tokio::time::sleep(shutdown_timeout.duration());
    tokio::pin!(deadline);
    let mut shutdown_timed_out = false;

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
            () = &mut deadline => {
                shutdown_timed_out = true;
                if signal_result.is_none() {
                    signal_forced_stop = Some(DhtObserveForcedStopReason::ShutdownDeadline);
                    signal_task.abort_all();
                }
                if supervisor_result.is_none() {
                    supervisor_forced_stop = Some(DhtObserveForcedStopReason::ShutdownDeadline);
                    supervisor_task.abort_all();
                }
                if http_result.is_none() {
                    http_forced_stop = Some(DhtObserveForcedStopReason::ShutdownDeadline);
                    http_task.force_shutdown();
                }
                if let Some(metrics) = metrics_task.as_mut() {
                    metrics_forced_stop = Some(DhtObserveForcedStopReason::ShutdownDeadline);
                    metrics.abort();
                }
                break;
            }
        }
    }

    if signal_result.is_none() {
        if signal_forced_stop.is_none() {
            signal_forced_stop = Some(DhtObserveForcedStopReason::CoordinatorCleanup);
        }
        signal_task.abort_all();
        signal_result = Some(signal_task.join().await);
    }
    if supervisor_result.is_none() {
        supervisor_result = Some(supervisor_task.join().await);
    }
    if http_result.is_none() {
        http_result = Some(http_task.join().await);
    }
    if let Some(metrics) = metrics_task.as_mut() {
        if metrics_forced_stop.is_none() {
            metrics_forced_stop = Some(DhtObserveForcedStopReason::CoordinatorCleanup);
        }
        metrics.abort();
        metrics_result = Some(metrics.join().await);
        metrics_task.take();
    }

    DhtObserveProcessExit {
        first_trigger,
        shutdown_timeout,
        shutdown_timed_out,
        signal: DhtObserveTaskExit {
            forced_stop_requested: signal_forced_stop,
            result: signal_result.expect("the owned signal task has terminal evidence"),
        },
        supervisor: DhtObserveTaskExit {
            forced_stop_requested: supervisor_forced_stop,
            result: supervisor_result.expect("the owned supervisor task has terminal evidence"),
        },
        http: DhtObserveTaskExit {
            forced_stop_requested: http_forced_stop,
            result: http_result.expect("the owned HTTP task has terminal evidence"),
        },
        metrics: metrics_enabled.then(|| DhtObserveTaskExit {
            forced_stop_requested: metrics_forced_stop,
            result: metrics_result.expect("the owned metrics task has terminal evidence"),
        }),
        final_observability: observability.snapshot(),
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
    use std::future::pending;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::sync::{Arc, Mutex};

    use axum::routing::get;
    use bitmagnet_dht::{
        DhtBootstrapPingProducerConfig, DhtCrawlerMaintenanceConfig, DhtRuntimeConfig,
    };
    use bitmagnet_dht_crawler::{
        DhtCrawlerObserveOnlyConfig, DhtCrawlerObserveOnlySupervisor,
        DhtCrawlerPipelineObservedLifecycle,
    };
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{TcpListener, UdpSocket};
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::*;

    fn shutdown_timeout(seconds: u64) -> DhtObserveShutdownTimeout {
        DhtObserveShutdownTimeout::from_seconds(seconds).expect("test timeout is valid")
    }

    fn offline_config() -> DhtCrawlerObserveOnlyConfig {
        DhtCrawlerObserveOnlyConfig {
            runtime: DhtRuntimeConfig {
                bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
                ..DhtRuntimeConfig::default()
            },
            maintenance: DhtCrawlerMaintenanceConfig {
                bootstrap_ping: DhtBootstrapPingProducerConfig {
                    bootstrap_nodes: Vec::new(),
                    ..DhtBootstrapPingProducerConfig::default()
                },
                ..DhtCrawlerMaintenanceConfig::default()
            },
            ..DhtCrawlerObserveOnlyConfig::default()
        }
    }

    async fn started() -> (
        DhtCrawlerObserveOnlySupervisor,
        DhtCrawlerObserveOnlyObservabilityHandle,
        SocketAddrV4,
        TcpListener,
        std::net::SocketAddr,
    ) {
        let (supervisor, observability) = DhtCrawlerObserveOnlySupervisor::start(offline_config())
            .await
            .expect("offline observe-only graph starts");
        let udp_addr = supervisor.local_addr();
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ephemeral HTTP listener binds");
        let http_addr = listener.local_addr().unwrap();
        (supervisor, observability, udp_addr, listener, http_addr)
    }

    async fn get_status(address: std::net::SocketAddr, path: &str) -> u16 {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response.split_whitespace().nth(1).unwrap().parse().unwrap()
    }

    #[tokio::test]
    async fn signal_joins_ready_graph_http_and_metrics_then_releases_sockets() {
        let (supervisor, observability, udp_addr, listener, http_addr) = started().await;
        let mut lifecycle = observability.lifecycle();
        let signal = async move {
            while lifecycle.changed().await.is_some() {
                if lifecycle.is_ready() {
                    return Ok(DhtObserveProcessSignal::Terminate);
                }
            }
            Err(signal_stream_closed())
        };
        let metrics = tokio::spawn(std::future::pending());

        let exit = timeout(
            Duration::from_secs(5),
            supervise_observe_only_process(
                listener,
                supervisor,
                observability,
                Some(metrics),
                signal,
                shutdown_timeout(2),
            ),
        )
        .await
        .expect("clean process shutdown remains bounded");

        assert!(exit.is_success(), "{exit:?}");
        assert_eq!(
            exit.first_trigger,
            DhtObserveProcessTrigger::Signal(DhtObserveProcessSignal::Terminate)
        );
        assert_eq!(
            exit.final_observability.lifecycle,
            DhtCrawlerPipelineObservedLifecycle::Stopped
        );
        let metrics = exit.metrics.expect("metrics evidence is retained");
        assert_eq!(
            metrics.forced_stop_requested,
            Some(DhtObserveForcedStopReason::CoordinatorCleanup)
        );
        assert!(metrics.result.unwrap_err().is_cancelled());
        drop(
            UdpSocket::bind(udp_addr)
                .await
                .expect("UDP socket is released"),
        );
        drop(
            TcpListener::bind(http_addr)
                .await
                .expect("HTTP socket is released"),
        );
    }

    #[tokio::test]
    async fn unexpected_http_completion_drains_graph_but_is_not_success() {
        let (supervisor, observability, udp_addr, listener, _http_addr) = started().await;
        let (supervisor_stop, supervisor_stop_rx) = watch::channel(false);
        let supervisor_run = supervisor.run(wait_for_stop(supervisor_stop_rx));
        let mut http =
            OwnedHttpServer::spawn(listener, observe_only_http_router(observability.clone()));
        http.abort_for_test();

        let exit = timeout(
            Duration::from_secs(5),
            coordinate_tasks(
                std::future::pending(),
                supervisor_run,
                supervisor_stop,
                http,
                None,
                observability,
                shutdown_timeout(2),
            ),
        )
        .await
        .expect("unexpected HTTP completion still drains the graph");

        assert_eq!(exit.first_trigger, DhtObserveProcessTrigger::HttpServer);
        assert!(!exit.is_success());
        assert!(exit.http.forced_stop_requested.is_none());
        assert!(exit.http.result.unwrap_err().is_cancelled());
        assert!(matches!(&exit.supervisor.result, Ok(graph) if graph.is_clean_external_shutdown()));
        assert_eq!(
            exit.final_observability.lifecycle,
            DhtCrawlerPipelineObservedLifecycle::Stopped
        );
        drop(
            UdpSocket::bind(udp_addr)
                .await
                .expect("UDP socket is released"),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_aborts_and_joins_every_unfinished_owned_task() {
        let (supervisor, observability, _udp_addr, listener, _http_addr) = started().await;
        drop(supervisor);
        let (supervisor_stop, _supervisor_stop_rx) = watch::channel(false);
        let http =
            OwnedHttpServer::spawn(listener, observe_only_http_router(observability.clone()));
        let metrics = tokio::spawn(std::future::pending());

        let exit = coordinate_tasks(
            std::future::ready(Ok(DhtObserveProcessSignal::Interrupt)),
            std::future::pending(),
            supervisor_stop,
            http,
            Some(metrics),
            observability,
            shutdown_timeout(2),
        )
        .await;

        assert!(exit.shutdown_timed_out);
        assert!(!exit.is_success());
        assert!(exit.signal.forced_stop_requested.is_none());
        assert_eq!(
            exit.supervisor.forced_stop_requested,
            Some(DhtObserveForcedStopReason::ShutdownDeadline)
        );
        assert!(exit.supervisor.result.unwrap_err().is_cancelled());
        assert_eq!(
            exit.http.forced_stop_requested,
            Some(DhtObserveForcedStopReason::ShutdownDeadline)
        );
        assert!(matches!(exit.http.result, Ok(Ok(()))));
        let metrics = exit.metrics.expect("metrics evidence is retained");
        assert_eq!(
            metrics.forced_stop_requested,
            Some(DhtObserveForcedStopReason::ShutdownDeadline)
        );
        assert!(metrics.result.unwrap_err().is_cancelled());
        assert!(matches!(
            exit.final_observability.lifecycle,
            DhtCrawlerPipelineObservedLifecycle::Cancelled { .. }
        ));
    }

    #[tokio::test]
    async fn signal_watcher_error_drains_without_relabeling_the_failure() {
        let (supervisor, observability, udp_addr, listener, http_addr) = started().await;
        let (supervisor_stop, supervisor_stop_rx) = watch::channel(false);
        let supervisor_run = supervisor.run(wait_for_stop(supervisor_stop_rx));
        let http =
            OwnedHttpServer::spawn(listener, observe_only_http_router(observability.clone()));
        let metrics = tokio::spawn(pending());

        let exit = timeout(
            Duration::from_secs(5),
            coordinate_tasks(
                std::future::ready(Err(signal_stream_closed())),
                supervisor_run,
                supervisor_stop,
                http,
                Some(metrics),
                observability,
                shutdown_timeout(2),
            ),
        )
        .await
        .expect("signal watcher failure still drains all peers");

        assert_eq!(exit.first_trigger, DhtObserveProcessTrigger::SignalWatcher);
        assert!(exit.signal.forced_stop_requested.is_none());
        assert!(matches!(
            &exit.signal.result,
            Ok(Err(error)) if error.kind() == io::ErrorKind::BrokenPipe
        ));
        assert!(exit.supervisor.forced_stop_requested.is_none());
        assert!(matches!(
            &exit.supervisor.result,
            Ok(graph) if graph.is_clean_external_shutdown()
        ));
        assert!(exit.http.forced_stop_requested.is_none());
        assert!(matches!(exit.http.result, Ok(Ok(()))));
        assert_eq!(
            exit.metrics.as_ref().unwrap().forced_stop_requested,
            Some(DhtObserveForcedStopReason::CoordinatorCleanup)
        );
        assert!(!exit.is_success());
        drop(
            UdpSocket::bind(udp_addr)
                .await
                .expect("UDP socket is released"),
        );
        drop(
            TcpListener::bind(http_addr)
                .await
                .expect("HTTP socket is released"),
        );
    }

    #[tokio::test]
    async fn metrics_completion_is_unexpected_and_drains_every_peer() {
        let (supervisor, observability, udp_addr, listener, http_addr) = started().await;
        let (supervisor_stop, supervisor_stop_rx) = watch::channel(false);
        let supervisor_run = supervisor.run(wait_for_stop(supervisor_stop_rx));
        let http =
            OwnedHttpServer::spawn(listener, observe_only_http_router(observability.clone()));
        let metrics = tokio::spawn(async {});

        let exit = timeout(
            Duration::from_secs(5),
            coordinate_tasks(
                pending(),
                supervisor_run,
                supervisor_stop,
                http,
                Some(metrics),
                observability,
                shutdown_timeout(2),
            ),
        )
        .await
        .expect("metrics completion still drains all peers");

        assert_eq!(exit.first_trigger, DhtObserveProcessTrigger::MetricsServer);
        let metrics = exit.metrics.as_ref().expect("metrics evidence is retained");
        assert!(metrics.forced_stop_requested.is_none());
        assert!(matches!(metrics.result, Ok(())));
        assert_eq!(
            exit.signal.forced_stop_requested,
            Some(DhtObserveForcedStopReason::CoordinatorCleanup)
        );
        assert!(matches!(&exit.supervisor.result, Ok(graph) if graph.is_clean_external_shutdown()));
        assert!(matches!(exit.http.result, Ok(Ok(()))));
        assert!(!exit.is_success());
        drop(
            UdpSocket::bind(udp_addr)
                .await
                .expect("UDP socket is released"),
        );
        drop(
            TcpListener::bind(http_addr)
                .await
                .expect("HTTP socket is released"),
        );
    }

    #[tokio::test]
    async fn supervisor_completion_is_unexpected_and_stops_http() {
        let (supervisor, observability, udp_addr, listener, http_addr) = started().await;
        let (supervisor_stop, _supervisor_stop_rx) = watch::channel(false);
        let supervisor_run = supervisor.run(std::future::ready(()));
        let http =
            OwnedHttpServer::spawn(listener, observe_only_http_router(observability.clone()));

        let exit = timeout(
            Duration::from_secs(5),
            coordinate_tasks(
                pending(),
                supervisor_run,
                supervisor_stop,
                http,
                None,
                observability,
                shutdown_timeout(2),
            ),
        )
        .await
        .expect("supervisor completion still stops HTTP");

        assert_eq!(exit.first_trigger, DhtObserveProcessTrigger::Supervisor);
        assert!(exit.supervisor.forced_stop_requested.is_none());
        assert!(matches!(
            &exit.supervisor.result,
            Ok(graph) if graph.is_clean_external_shutdown()
        ));
        assert_eq!(
            exit.signal.forced_stop_requested,
            Some(DhtObserveForcedStopReason::CoordinatorCleanup)
        );
        assert!(exit.http.forced_stop_requested.is_none());
        assert!(matches!(exit.http.result, Ok(Ok(()))));
        assert!(!exit.is_success());
        drop(
            UdpSocket::bind(udp_addr)
                .await
                .expect("UDP socket is released"),
        );
        drop(
            TcpListener::bind(http_addr)
                .await
                .expect("HTTP socket is released"),
        );
    }

    #[tokio::test]
    async fn http_remains_live_while_the_coordinator_waits_for_graph_drain() {
        let (supervisor, observability, _udp_addr, listener, http_addr) = started().await;
        let (supervisor_stop, supervisor_stop_rx) = watch::channel(false);
        let (drain_entered_tx, drain_entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let supervisor_run = supervisor.run(async move {
            wait_for_stop(supervisor_stop_rx).await;
            let _ = drain_entered_tx.send(());
            let _ = release_rx.await;
        });
        let http =
            OwnedHttpServer::spawn(listener, observe_only_http_router(observability.clone()));
        let process = tokio::spawn(coordinate_tasks(
            std::future::ready(Ok(DhtObserveProcessSignal::Terminate)),
            supervisor_run,
            supervisor_stop,
            http,
            None,
            observability,
            shutdown_timeout(2),
        ));

        drain_entered_rx
            .await
            .expect("the graph drain reached its controlled gate");
        assert_eq!(get_status(http_addr, "/livez").await, 200);
        assert_eq!(get_status(http_addr, "/readyz").await, 503);
        release_tx.send(()).unwrap();

        let exit = timeout(Duration::from_secs(5), process)
            .await
            .expect("the released process drain remains bounded")
            .expect("the process coordinator task joins");
        assert!(exit.is_success(), "{exit:?}");
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
                        pending::<()>().await;
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
            timeout(Duration::from_secs(2), http.join()).await,
            Ok(Ok(Ok(())))
        ));
        timeout(Duration::from_secs(1), dropped_rx)
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

    #[test]
    fn shutdown_timeout_is_positive_and_bounded() {
        assert_eq!(DhtObserveShutdownTimeout::DEFAULT.seconds(), 20);
        assert_eq!(shutdown_timeout(300).duration(), Duration::from_secs(300));
        assert_eq!(
            DhtObserveShutdownTimeout::from_seconds(0),
            Err(DhtObserveShutdownTimeoutError::Zero)
        );
        assert_eq!(
            DhtObserveShutdownTimeout::from_seconds(301),
            Err(DhtObserveShutdownTimeoutError::TooLarge {
                seconds: 301,
                maximum_seconds: 300,
            })
        );
    }
}
