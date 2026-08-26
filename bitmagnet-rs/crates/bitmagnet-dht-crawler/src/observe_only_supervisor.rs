//! Closed composition for a PostgreSQL-nonmutating DHT network soak.
//!
//! This graph owns the production UDP runtime and crawler-maintenance workers,
//! but terminates their info-hash route in the concrete counter-only observer.
//! No PostgreSQL pool, blocking manager, peer-wire metainfo requester, or
//! persistence adapter can be supplied to this constructor. The graph still
//! binds UDP, resolves bootstrap names, contacts and responds to public DHT
//! nodes, and mutates its in-memory routing table.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::net::SocketAddrV4;
use std::num::NonZeroUsize;

use bitmagnet_dht::{
    dht_info_hash_triage_channel, DhtBootstrapPingProducerExit, DhtCrawlerMaintenanceConfig,
    DhtCrawlerMaintenanceConfigError, DhtCrawlerMaintenanceStatsHandle,
    DhtCrawlerMaintenanceSupervisor, DhtCrawlerMaintenanceSupervisorExit,
    DhtCrawlerMaintenanceWithConfigError, DhtDiscoveredNodeFindWorkerExit,
    DhtDiscoveredNodePingWorkerExit, DhtDiscoveredNodeSchedulerExit, DhtInboundStats,
    DhtOldestNodeFindProducerExit, DhtOldestNodePingProducerExit, DhtRuntime, DhtRuntimeConfig,
    DhtRuntimeConfigError, DhtRuntimeExit, DhtRuntimeHealthHandle, DhtRuntimeHealthStatus,
    DhtRuntimeStartError, DhtSampleInfoHashesProducerExit, DhtSampleInfoHashesWorkerExit,
    DHT_CHANNEL_MAX_CAPACITY,
};
use tokio::sync::{oneshot, watch};
use tokio::task::{JoinError, JoinSet};

use crate::{
    DhtCrawlerPipelineLifecycle, DhtCrawlerPipelineLifecycleHandle,
    DhtCrawlerPipelineMaintenanceObservabilitySnapshot, DhtCrawlerPipelineObservedLifecycle,
    DhtCrawlerPipelineRuntimeObservabilitySnapshot, DhtInfoHashObservationStats,
    DhtInfoHashObservationStatsHandle, DhtInfoHashObservationWorker,
    DhtInfoHashObservationWorkerExit,
};

/// Complete taskless policy for the first PostgreSQL-nonmutating DHT soak.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtCrawlerObserveOnlyConfig {
    pub runtime: DhtRuntimeConfig,
    pub maintenance: DhtCrawlerMaintenanceConfig,
    pub observation_capacity: NonZeroUsize,
}

impl Default for DhtCrawlerObserveOnlyConfig {
    fn default() -> Self {
        Self {
            runtime: DhtRuntimeConfig::default(),
            maintenance: DhtCrawlerMaintenanceConfig::default(),
            observation_capacity: NonZeroUsize::new(
                bitmagnet_dht::DHT_INFO_HASH_TRIAGE_DEFAULT_CAPACITY,
            )
            .expect("the production observation capacity is nonzero"),
        }
    }
}

impl DhtCrawlerObserveOnlyConfig {
    /// Validate every bounded route and maintenance timer before binding UDP.
    pub fn validate(&self) -> Result<(), DhtCrawlerObserveOnlyConfigError> {
        self.runtime
            .validate()
            .map_err(DhtCrawlerObserveOnlyConfigError::Runtime)?;
        self.maintenance
            .validate()
            .map_err(DhtCrawlerObserveOnlyConfigError::Maintenance)?;
        if self.observation_capacity.get() > DHT_CHANNEL_MAX_CAPACITY {
            return Err(
                DhtCrawlerObserveOnlyConfigError::ObservationCapacityOutOfRange {
                    capacity: self.observation_capacity,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                },
            );
        }
        Ok(())
    }
}

/// A closed observe-only policy could not be validated safely.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum DhtCrawlerObserveOnlyConfigError {
    #[error(transparent)]
    Runtime(DhtRuntimeConfigError),
    #[error(transparent)]
    Maintenance(DhtCrawlerMaintenanceConfigError),
    #[error("the observation capacity {capacity} exceeds Tokio's maximum of {maximum}")]
    ObservationCapacityOutOfRange {
        capacity: NonZeroUsize,
        maximum: usize,
    },
}

/// Failure to start the closed observe-only graph.
#[derive(Debug)]
pub enum DhtCrawlerObserveOnlyStartError {
    /// Side-effect-free configuration validation failed.
    Config(DhtCrawlerObserveOnlyConfigError),
    /// The owned UDP runtime could not start.
    Runtime(DhtRuntimeStartError),
    /// Maintenance construction failed after runtime start. The runtime was
    /// explicitly shut down and its exact cleanup result is retained.
    Maintenance {
        source: Box<DhtCrawlerMaintenanceWithConfigError>,
        runtime_cleanup: Result<DhtRuntimeExit, JoinError>,
    },
}

impl fmt::Display for DhtCrawlerObserveOnlyStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(source) => write!(formatter, "invalid observe-only configuration: {source}"),
            Self::Runtime(source) => write!(formatter, "could not start observe-only runtime: {source}"),
            Self::Maintenance {
                source,
                runtime_cleanup,
            } => write!(
                formatter,
                "could not start observe-only maintenance: {source}; runtime cleanup: {runtime_cleanup:?}"
            ),
        }
    }
}

impl Error for DhtCrawlerObserveOnlyStartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Config(source) => Some(source),
            Self::Runtime(source) => Some(source),
            Self::Maintenance { source, .. } => Some(source.as_ref()),
        }
    }
}

/// First terminal condition observed by the closed composition owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtCrawlerObserveOnlyTrigger {
    External,
    Runtime,
    Maintenance,
    Observation,
}

/// Exact joined terminal evidence for the entire observe-only graph.
#[derive(Debug)]
pub struct DhtCrawlerObserveOnlyExit {
    pub first_trigger: DhtCrawlerObserveOnlyTrigger,
    /// Maintenance's exact exit, task panic, or unexpected task cancellation.
    pub maintenance: Result<DhtCrawlerMaintenanceSupervisorExit, JoinError>,
    pub observation: DhtInfoHashObservationWorkerExit,
    pub runtime: Result<DhtRuntimeExit, JoinError>,
}

impl DhtCrawlerObserveOnlyExit {
    /// Whether an external request produced the complete canonical drain.
    ///
    /// The outer maintenance `Shutdown` classification is insufficient by
    /// itself because it deliberately retains any child result already ready
    /// at that boundary. Every nested result is therefore checked explicitly.
    #[must_use]
    pub fn is_clean_external_shutdown(&self) -> bool {
        self.first_trigger == DhtCrawlerObserveOnlyTrigger::External
            && maintenance_shutdown_is_clean(&self.maintenance)
            && self.observation == DhtInfoHashObservationWorkerExit::InputClosed
            && matches!(&self.runtime, Ok(DhtRuntimeExit::Shutdown))
    }
}

fn maintenance_shutdown_is_clean(
    result: &Result<DhtCrawlerMaintenanceSupervisorExit, JoinError>,
) -> bool {
    match result {
        Ok(DhtCrawlerMaintenanceSupervisorExit::ShutdownBeforeStart) => true,
        Ok(DhtCrawlerMaintenanceSupervisorExit::Shutdown { children }) => {
            matches!(
                children.scheduler,
                DhtDiscoveredNodeSchedulerExit::Shutdown { .. }
            ) && matches!(
                children.ping,
                DhtDiscoveredNodePingWorkerExit::Shutdown { .. }
            ) && matches!(
                children.find_node,
                DhtDiscoveredNodeFindWorkerExit::Shutdown { .. }
            ) && matches!(
                children.sample_infohashes_worker,
                DhtSampleInfoHashesWorkerExit::Shutdown { .. }
            ) && matches!(
                children.oldest_find,
                DhtOldestNodeFindProducerExit::Shutdown { .. }
            ) && matches!(
                children.oldest_ping,
                DhtOldestNodePingProducerExit::Shutdown { .. }
            ) && matches!(
                children.bootstrap_ping,
                DhtBootstrapPingProducerExit::Shutdown { .. }
            ) && matches!(
                children.sample_infohashes_producer,
                DhtSampleInfoHashesProducerExit::Shutdown { .. }
            ) && matches!(&children.target, Ok(()))
        }
        Ok(DhtCrawlerMaintenanceSupervisorExit::Failed { .. }) | Err(_) => false,
    }
}

/// Fixed-shape observations for the closed PostgreSQL-nonmutating graph.
///
/// Constituent atomics are read independently and are not a transactional
/// snapshot. This surface performs no database/schema preflight and supplies
/// no HTTP server, metrics exporter, whole-application readiness, or
/// deployment-readiness claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtCrawlerObserveOnlyObservabilitySnapshot {
    pub lifecycle: DhtCrawlerPipelineObservedLifecycle,
    pub runtime: DhtCrawlerPipelineRuntimeObservabilitySnapshot,
    pub maintenance: DhtCrawlerPipelineMaintenanceObservabilitySnapshot,
    pub observation: DhtInfoHashObservationStats,
}

impl DhtCrawlerObserveOnlyObservabilitySnapshot {
    /// Whether every worker started and at least one outbound query succeeded
    /// within the exact Go freshness window.
    ///
    /// Requiring a recorded success deliberately excludes the runtime-health
    /// policy's initial 30-second grace from an operator soak-admission claim.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.lifecycle == DhtCrawlerPipelineObservedLifecycle::Ready
            && self.runtime.health.status() == DhtRuntimeHealthStatus::Up
            && self.runtime.health.last_success_ago.is_some()
    }
}

/// Cloneable sender-free health and counter view of the closed graph.
#[derive(Clone)]
pub struct DhtCrawlerObserveOnlyObservabilityHandle {
    lifecycle: DhtCrawlerPipelineLifecycleHandle,
    runtime_health: DhtRuntimeHealthHandle,
    runtime_inbound: DhtInboundStats,
    maintenance: DhtCrawlerMaintenanceStatsHandle,
    observation: DhtInfoHashObservationStatsHandle,
}

impl DhtCrawlerObserveOnlyObservabilityHandle {
    /// Read every constituent without retaining a socket, route, or worker.
    #[must_use]
    pub fn snapshot(&self) -> DhtCrawlerObserveOnlyObservabilitySnapshot {
        DhtCrawlerObserveOnlyObservabilitySnapshot {
            lifecycle: self.lifecycle.observed(),
            runtime: DhtCrawlerPipelineRuntimeObservabilitySnapshot {
                health: self.runtime_health.snapshot(),
                inbound: self.runtime_inbound.snapshot(),
                discovery: self.maintenance.discovery.snapshot(),
            },
            maintenance: DhtCrawlerPipelineMaintenanceObservabilitySnapshot {
                scheduler: self.maintenance.scheduler.snapshot(),
                ping: self.maintenance.ping.snapshot(),
                find_node: self.maintenance.find_node.snapshot(),
                sample_infohashes_worker: self.maintenance.sample_infohashes_worker.snapshot(),
                oldest_find: self.maintenance.oldest_find.snapshot(),
                oldest_ping: self.maintenance.oldest_ping.snapshot(),
                bootstrap_ping: self.maintenance.bootstrap_ping.snapshot(),
                sample_infohashes_producer: self.maintenance.sample_infohashes_producer.snapshot(),
            },
            observation: self.observation.snapshot(),
        }
    }

    /// Evaluate only the lifecycle and proven outbound-success health gate.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.lifecycle.observed() == DhtCrawlerPipelineObservedLifecycle::Ready && {
            let health = self.runtime_health.snapshot();
            health.status() == DhtRuntimeHealthStatus::Up && health.last_success_ago.is_some()
        }
    }

    /// Clone the sender-free lifecycle projection for lightweight waiters.
    #[must_use]
    pub fn lifecycle(&self) -> DhtCrawlerPipelineLifecycleHandle {
        self.lifecycle.clone()
    }
}

/// Closed owner of the production DHT runtime, maintenance, and discard sink.
#[must_use = "the observe-only graph must be run or deliberately dropped"]
pub struct DhtCrawlerObserveOnlySupervisor {
    local_addr: SocketAddrV4,
    runtime: DhtRuntime,
    maintenance: DhtCrawlerMaintenanceSupervisor,
    root_triage_input: bitmagnet_dht::DhtInfoHashTriageInput,
    observation: DhtInfoHashObservationWorker,
    lifecycle: watch::Sender<DhtCrawlerPipelineLifecycle>,
    observability: DhtCrawlerObserveOnlyObservabilityHandle,
}

impl DhtCrawlerObserveOnlySupervisor {
    /// Validate, bind, and construct the complete closed graph.
    ///
    /// Construction starts only the UDP runtime task. Maintenance and the
    /// observer remain taskless until [`Self::run`]. If later maintenance
    /// construction fails, the started runtime is explicitly shut down before
    /// the error is returned.
    pub async fn start(
        config: DhtCrawlerObserveOnlyConfig,
    ) -> Result<(Self, DhtCrawlerObserveOnlyObservabilityHandle), DhtCrawlerObserveOnlyStartError>
    {
        config
            .validate()
            .map_err(DhtCrawlerObserveOnlyStartError::Config)?;
        let mut runtime = DhtRuntime::start(config.runtime)
            .await
            .map_err(DhtCrawlerObserveOnlyStartError::Runtime)?;
        let local_addr = runtime.local_addr();
        let runtime_health = runtime.health();
        let runtime_inbound = runtime.inbound_stats();
        let client = runtime.client();
        let table = runtime.table().clone();
        let discovery = runtime
            .take_discovered_nodes()
            .expect("a newly started DHT runtime owns its discovery receiver");
        let (root_triage_input, observation_receiver) =
            dht_info_hash_triage_channel(config.observation_capacity);
        let (observation, observation_stats) =
            DhtInfoHashObservationWorker::new(observation_receiver);
        let (maintenance, maintenance_stats) = match DhtCrawlerMaintenanceSupervisor::with_config(
            discovery,
            &client,
            &table,
            &root_triage_input,
            config.maintenance,
        ) {
            Ok(composition) => composition,
            Err(source) => {
                drop((client, table, root_triage_input, observation));
                let runtime_cleanup = runtime.shutdown().await;
                return Err(DhtCrawlerObserveOnlyStartError::Maintenance {
                    source: Box::new(source),
                    runtime_cleanup,
                });
            }
        };
        drop((client, table));

        let (lifecycle, lifecycle_handle) = DhtCrawlerPipelineLifecycleHandle::channel();
        let observability = DhtCrawlerObserveOnlyObservabilityHandle {
            lifecycle: lifecycle_handle,
            runtime_health,
            runtime_inbound,
            maintenance: maintenance_stats,
            observation: observation_stats,
        };
        Ok((
            Self {
                local_addr,
                runtime,
                maintenance,
                root_triage_input,
                observation,
                lifecycle,
                observability: observability.clone(),
            },
            observability,
        ))
    }

    /// Return the actual UDP address, including an OS-assigned test port.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddrV4 {
        self.local_addr
    }

    /// Clone the sender-free observation surface before consuming the owner.
    #[must_use]
    pub fn observability_handle(&self) -> DhtCrawlerObserveOnlyObservabilityHandle {
        self.observability.clone()
    }

    /// Run until a signal or component exit, then join in no-write order.
    ///
    /// Maintenance stops first while the UDP runtime remains available. The
    /// original triage input is then dropped so the observer drains to EOF,
    /// and only then is the runtime stopped and joined. Cancelling this future
    /// makes no clean-drain claim; owned component drops request abort instead.
    pub async fn run<F>(self, shutdown: F) -> DhtCrawlerObserveOnlyExit
    where
        F: Future<Output = ()>,
    {
        self.run_inner(shutdown, None).await
    }

    #[cfg(test)]
    async fn run_with_maintenance_panic<F>(
        self,
        shutdown: F,
        panic_signal: oneshot::Receiver<()>,
    ) -> DhtCrawlerObserveOnlyExit
    where
        F: Future<Output = ()>,
    {
        self.run_inner(shutdown, Some(panic_signal)).await
    }

    async fn run_inner<F>(
        self,
        shutdown: F,
        maintenance_panic: Option<oneshot::Receiver<()>>,
    ) -> DhtCrawlerObserveOnlyExit
    where
        F: Future<Output = ()>,
    {
        let Self {
            local_addr: _,
            runtime,
            maintenance,
            root_triage_input,
            observation,
            lifecycle,
            observability: _,
        } = self;
        let mut lifecycle = LifecyclePublisher::new(lifecycle);
        let (maintenance_stop, maintenance_stop_rx) = watch::channel(false);
        let (runtime_stop, runtime_stop_rx) = watch::channel(false);
        let (notifications, maintenance_run) =
            maintenance.run_with_notifications(wait_for_stop(maintenance_stop_rx));
        let maintenance_task = async move {
            if let Some(mut panic_signal) = maintenance_panic {
                tokio::select! {
                    result = maintenance_run => result,
                    _ = &mut panic_signal => panic!("injected observe-only maintenance panic"),
                }
            } else {
                maintenance_run.await
            }
        };
        let mut maintenance_tasks = JoinSet::new();
        maintenance_tasks.spawn(maintenance_task);
        let maintenance_run = async move {
            maintenance_tasks
                .join_next()
                .await
                .expect("the observe-only maintenance task remains owned until completion")
        };
        let (mut maintenance_started, mut maintenance_stopping) = notifications.into_parts();
        let (observation_started, mut observation_started_rx) = oneshot::channel();
        let observation_run = async move {
            let _ = observation_started.send(());
            observation.run(std::future::pending()).await
        };
        let runtime_run = runtime.run_until_shutdown(wait_for_stop(runtime_stop_rx));

        tokio::pin!(shutdown);
        tokio::pin!(maintenance_run);
        tokio::pin!(observation_run);
        tokio::pin!(runtime_run);

        let mut maintenance_result = None;
        let mut observation_result = None;
        let mut runtime_result = None;
        let mut maintenance_is_started = false;
        let mut observation_is_started = false;

        let first_trigger = loop {
            let trigger = tokio::select! {
                biased;
                () = &mut shutdown => Some(DhtCrawlerObserveOnlyTrigger::External),
                result = &mut runtime_run => {
                    runtime_result = Some(result);
                    Some(DhtCrawlerObserveOnlyTrigger::Runtime)
                }
                result = &mut maintenance_run => {
                    maintenance_result = Some(result);
                    Some(DhtCrawlerObserveOnlyTrigger::Maintenance)
                }
                result = &mut observation_run => {
                    observation_result = Some(result);
                    Some(DhtCrawlerObserveOnlyTrigger::Observation)
                }
                _ = maintenance_stopping.notified() => {
                    Some(DhtCrawlerObserveOnlyTrigger::Maintenance)
                }
                result = maintenance_started.notified(), if !maintenance_is_started => {
                    maintenance_is_started = result.is_ok();
                    if maintenance_is_started { None } else { Some(DhtCrawlerObserveOnlyTrigger::Maintenance) }
                }
                result = &mut observation_started_rx, if !observation_is_started => {
                    observation_is_started = result.is_ok();
                    if observation_is_started { None } else { Some(DhtCrawlerObserveOnlyTrigger::Observation) }
                }
            };
            if let Some(trigger) = trigger {
                break trigger;
            }
            if maintenance_is_started && observation_is_started {
                lifecycle.publish_ready();
                break tokio::select! {
                    biased;
                    () = &mut shutdown => DhtCrawlerObserveOnlyTrigger::External,
                    result = &mut runtime_run => {
                        runtime_result = Some(result);
                        DhtCrawlerObserveOnlyTrigger::Runtime
                    }
                    result = &mut maintenance_run => {
                        maintenance_result = Some(result);
                        DhtCrawlerObserveOnlyTrigger::Maintenance
                    }
                    result = &mut observation_run => {
                        observation_result = Some(result);
                        DhtCrawlerObserveOnlyTrigger::Observation
                    }
                    _ = maintenance_stopping.notified() => DhtCrawlerObserveOnlyTrigger::Maintenance,
                };
            }
        };

        lifecycle.publish_stopping();
        let _ = maintenance_stop.send(true);
        let maintenance = match maintenance_result {
            Some(result) => result,
            None => maintenance_run.await,
        };
        drop(root_triage_input);
        let observation = match observation_result {
            Some(result) => result,
            None => observation_run.await,
        };
        let _ = runtime_stop.send(true);
        let runtime = match runtime_result {
            Some(result) => result,
            None => runtime_run.await,
        };
        lifecycle.publish_stopped();

        DhtCrawlerObserveOnlyExit {
            first_trigger,
            maintenance,
            observation,
            runtime,
        }
    }
}

struct LifecyclePublisher {
    sender: watch::Sender<DhtCrawlerPipelineLifecycle>,
    ready: bool,
    stopped: bool,
}

impl LifecyclePublisher {
    fn new(sender: watch::Sender<DhtCrawlerPipelineLifecycle>) -> Self {
        Self {
            sender,
            ready: false,
            stopped: false,
        }
    }

    fn publish_ready(&mut self) {
        let _ = self.sender.send(DhtCrawlerPipelineLifecycle::Ready);
        self.ready = true;
    }

    fn publish_stopping(&mut self) {
        let _ = self.sender.send(DhtCrawlerPipelineLifecycle::Stopping);
        self.ready = false;
    }

    fn publish_stopped(&mut self) {
        let _ = self.sender.send(DhtCrawlerPipelineLifecycle::Stopped);
        self.ready = false;
        self.stopped = true;
    }
}

impl Drop for LifecyclePublisher {
    fn drop(&mut self) {
        if self.ready && !self.stopped {
            let _ = self.sender.send(DhtCrawlerPipelineLifecycle::Stopping);
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

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::time::Duration;

    use bitmagnet_dht::{
        DhtBootstrapPingProducerConfig, DhtCrawlerMaintenanceSupervisorExit, DhtRuntimeConfig,
        DhtRuntimeExit,
    };
    use tokio::net::UdpSocket;
    use tokio::time::timeout;

    use super::*;

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

    async fn wait_for_lifecycle(
        handle: &DhtCrawlerObserveOnlyObservabilityHandle,
        expected: DhtCrawlerPipelineObservedLifecycle,
    ) {
        timeout(Duration::from_secs(5), async {
            loop {
                if handle.snapshot().lifecycle == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("observe-only lifecycle reaches expected state");
    }

    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn public_owner_and_sender_free_handles_have_exact_task_traits() {
        assert_send::<DhtCrawlerObserveOnlySupervisor>();
        assert_send::<DhtCrawlerObserveOnlyExit>();
        assert_send_sync::<DhtCrawlerObserveOnlyObservabilityHandle>();
        assert_send_sync::<DhtCrawlerObserveOnlyObservabilitySnapshot>();
        let source = include_str!("observe_only_supervisor.rs");
        assert!(!source.contains(&["Pg", "Pool"].concat()));
        assert!(!source.contains(&["bitmagnet", "_db"].concat()));
        assert!(!source.contains(&["Blocking", "Manager"].concat()));
    }

    #[test]
    fn validation_is_side_effect_free_and_has_deterministic_precedence() {
        let maximum = DHT_CHANNEL_MAX_CAPACITY;
        let over_max = NonZeroUsize::new(maximum + 1).unwrap();
        let mut config = offline_config();
        config.runtime.discovery_capacity = over_max;
        config.observation_capacity = over_max;
        assert_eq!(
            config.validate(),
            Err(DhtCrawlerObserveOnlyConfigError::Runtime(
                DhtRuntimeConfigError::DiscoveryCapacityOutOfRange {
                    capacity: over_max,
                    maximum,
                }
            ))
        );

        config.runtime.discovery_capacity = NonZeroUsize::MIN;
        assert_eq!(
            config.validate(),
            Err(
                DhtCrawlerObserveOnlyConfigError::ObservationCapacityOutOfRange {
                    capacity: over_max,
                    maximum,
                }
            )
        );
    }

    #[tokio::test]
    async fn pre_ready_shutdown_starts_no_maintenance_and_releases_udp() {
        let (supervisor, observability) = DhtCrawlerObserveOnlySupervisor::start(offline_config())
            .await
            .unwrap();
        let addr = supervisor.local_addr();
        let mut exit = timeout(
            Duration::from_secs(5),
            supervisor.run(std::future::ready(())),
        )
        .await
        .expect("pre-ready cleanup remains bounded");

        assert!(exit.is_clean_external_shutdown());
        assert_eq!(exit.first_trigger, DhtCrawlerObserveOnlyTrigger::External);
        assert!(matches!(
            &exit.maintenance,
            Ok(DhtCrawlerMaintenanceSupervisorExit::ShutdownBeforeStart)
        ));
        assert_eq!(
            exit.observation,
            DhtInfoHashObservationWorkerExit::InputClosed
        );
        assert!(matches!(&exit.runtime, Ok(DhtRuntimeExit::Shutdown)));
        exit.first_trigger = DhtCrawlerObserveOnlyTrigger::Runtime;
        assert!(!exit.is_clean_external_shutdown());
        exit.first_trigger = DhtCrawlerObserveOnlyTrigger::External;
        exit.observation = DhtInfoHashObservationWorkerExit::Shutdown { queued_dropped: 0 };
        assert!(!exit.is_clean_external_shutdown());
        let snapshot = observability.snapshot();
        assert_eq!(
            snapshot.lifecycle,
            DhtCrawlerPipelineObservedLifecycle::Stopped
        );
        assert_eq!(
            snapshot.observation,
            DhtInfoHashObservationStats {
                input_closed: 1,
                ..DhtInfoHashObservationStats::default()
            }
        );
        assert!(!snapshot.is_ready());
        drop(observability);
        drop(UdpSocket::bind(addr).await.expect("shutdown releases UDP"));
    }

    #[tokio::test]
    async fn ready_graph_stays_fail_closed_without_a_success_then_drains_cleanly() {
        let (supervisor, observability) = DhtCrawlerObserveOnlySupervisor::start(offline_config())
            .await
            .unwrap();
        let (shutdown, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn(supervisor.run(async move {
            let _ = shutdown_rx.await;
        }));
        wait_for_lifecycle(&observability, DhtCrawlerPipelineObservedLifecycle::Ready).await;
        let ready = observability.snapshot();
        assert!(ready.runtime.health.active);
        assert!(ready.runtime.health.last_success_ago.is_none());
        assert!(!ready.is_ready(), "startup grace is not success evidence");

        shutdown.send(()).unwrap();
        let mut exit = timeout(Duration::from_secs(5), run)
            .await
            .expect("ready graph cleanup remains bounded")
            .unwrap();
        assert!(exit.is_clean_external_shutdown());
        assert_eq!(exit.first_trigger, DhtCrawlerObserveOnlyTrigger::External);
        assert!(matches!(
            &exit.maintenance,
            Ok(DhtCrawlerMaintenanceSupervisorExit::Shutdown { .. })
        ));
        assert_eq!(
            exit.observation,
            DhtInfoHashObservationWorkerExit::InputClosed
        );
        assert!(matches!(&exit.runtime, Ok(DhtRuntimeExit::Shutdown)));
        let Ok(DhtCrawlerMaintenanceSupervisorExit::Shutdown { children }) = &mut exit.maintenance
        else {
            unreachable!("ready external shutdown retains child evidence")
        };
        children.scheduler = DhtDiscoveredNodeSchedulerExit::InputClosed;
        assert!(
            !exit.is_clean_external_shutdown(),
            "an outer shutdown must not hide a non-shutdown child exit"
        );

        let stopped = observability.snapshot();
        assert_eq!(
            stopped.lifecycle,
            DhtCrawlerPipelineObservedLifecycle::Stopped
        );
        assert_eq!(
            stopped.observation.observed,
            stopped.maintenance.sample_infohashes_worker.triage_queued
        );
        assert!(!stopped.is_ready());
    }

    #[tokio::test]
    async fn cancellation_closes_at_stopping_without_claiming_stopped() {
        let (supervisor, observability) = DhtCrawlerObserveOnlySupervisor::start(offline_config())
            .await
            .unwrap();
        let addr = supervisor.local_addr();
        let run = tokio::spawn(supervisor.run(std::future::pending()));
        wait_for_lifecycle(&observability, DhtCrawlerPipelineObservedLifecycle::Ready).await;
        run.abort();
        assert!(run.await.unwrap_err().is_cancelled());
        assert_eq!(
            observability.snapshot().lifecycle,
            DhtCrawlerPipelineObservedLifecycle::Cancelled {
                last: DhtCrawlerPipelineLifecycle::Stopping,
            }
        );
        let cancelled = observability.snapshot();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            observability.snapshot(),
            cancelled,
            "the abort-on-drop maintenance owner must stop counter activity"
        );
        drop(observability);
        timeout(Duration::from_secs(5), UdpSocket::bind(addr))
            .await
            .expect("cancelled owner releases UDP promptly")
            .expect("cancelled owner releases UDP socket");
    }

    #[tokio::test]
    async fn maintenance_task_panic_is_retained_after_whole_graph_cleanup() {
        let (supervisor, observability) = DhtCrawlerObserveOnlySupervisor::start(offline_config())
            .await
            .unwrap();
        let addr = supervisor.local_addr();
        let (panic_signal, panic_rx) = oneshot::channel();
        let run =
            tokio::spawn(supervisor.run_with_maintenance_panic(std::future::pending(), panic_rx));
        wait_for_lifecycle(&observability, DhtCrawlerPipelineObservedLifecycle::Ready).await;
        panic_signal.send(()).unwrap();

        let exit = timeout(Duration::from_secs(5), run)
            .await
            .expect("panic cleanup remains bounded")
            .unwrap();
        assert!(!exit.is_clean_external_shutdown());
        assert_eq!(
            exit.first_trigger,
            DhtCrawlerObserveOnlyTrigger::Maintenance
        );
        assert!(exit
            .maintenance
            .expect_err("maintenance panic is retained")
            .is_panic());
        assert_eq!(
            exit.observation,
            DhtInfoHashObservationWorkerExit::InputClosed
        );
        assert!(matches!(exit.runtime, Ok(DhtRuntimeExit::Shutdown)));
        assert_eq!(
            observability.snapshot().lifecycle,
            DhtCrawlerPipelineObservedLifecycle::Stopped
        );
        drop(observability);
        drop(
            UdpSocket::bind(addr)
                .await
                .expect("panic cleanup releases UDP"),
        );
    }
}
