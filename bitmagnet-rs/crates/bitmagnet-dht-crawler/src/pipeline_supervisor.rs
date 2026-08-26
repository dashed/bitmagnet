//! Staged lifecycle ownership for one concrete DHT crawler pipeline.
//!
//! Graceful shutdown first stops the maintenance composition, then releases
//! the original info-hash-triage producer and lets the six downstream workers
//! drain through natural route EOF. The DHT runtime remains live during that
//! drain so already-admitted get-peers and scrape work can still issue queries.
//! Once every downstream worker has terminated, runtime shutdown and one
//! blocking-finalization attempt are driven concurrently.
//!
//! This is a Rust lifecycle hardening, not a whole-crawler lossless guarantee.
//! Maintenance shutdown may report discarded work, and a downstream
//! `InputClosed` exit means only that every accepted item reached the worker's
//! existing terminal disposition; collaborator rejection or unknown database
//! outcomes remain visible in statistics. Dropping the consuming `run` future
//! drops the root input and requests abort through task and worker `Drop`
//! implementations, but does not asynchronously join them; arbitrary task code
//! may continue until its next yield, and there is no terminal accounting or
//! quiescence proof. A blocking DNS lookup already dispatched by maintenance
//! may still finish internally after its owning child is aborted. The retained
//! finalizer must not be used after cancellation unless the caller independently
//! proves every blocking producer has quiesced.

use std::collections::HashMap;
use std::future::{poll_fn, Future};
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use bitmagnet_blocking::{BlockingError, BlockingFinalizeOutcome, BlockingFinalizer};
use bitmagnet_db::{
    assert_goose_applied_head, read_goose_applied_head, DbError, GooseHeadMismatch, PgPool,
};
use bitmagnet_dht::{
    DhtBootstrapPingProducerStats, DhtCrawlerMaintenanceConfig, DhtCrawlerMaintenanceStatsHandle,
    DhtCrawlerMaintenanceSupervisor, DhtCrawlerMaintenanceSupervisorExit,
    DhtCrawlerMaintenanceWithConfigError, DhtDiscoveredNodeFindStats, DhtDiscoveredNodePingStats,
    DhtDiscoveredNodeSchedulerStats, DhtDiscoveryReceiver, DhtDiscoveryStats,
    DhtDiscoveryStatsHandle, DhtInboundStats, DhtInboundStatsSnapshot, DhtInfoHashTriageInput,
    DhtOldestNodeFindProducerStats, DhtOldestNodePingProducerStats, DhtRuntime, DhtRuntimeClient,
    DhtRuntimeExit, DhtRuntimeHealthHandle, DhtRuntimeHealthSnapshot, DhtRuntimeHealthStatus,
    DhtRuntimeStartError, DhtSampleInfoHashesProducerStats, DhtSampleInfoHashesWorkerStats, KTable,
};
use tokio::sync::{mpsc, watch};
use tokio::task::{Id, JoinError, JoinSet};

use crate::{
    random_metainfo_peer_id, DhtCrawlerAppProjection, DhtCrawlerAppProjectionError,
    DhtCrawlerDownstreamComposition, DhtCrawlerDownstreamStatsHandle, DhtCrawlerDownstreamWorkers,
    DhtGetPeersWorkerExit, DhtGetPeersWorkerStats, DhtInfoHashTriageStats,
    DhtInfoHashTriageWorkerExit, DhtPersistSourceWorkerExit, DhtPersistSourceWorkerStats,
    DhtPersistTorrentWorkerExit, DhtPersistTorrentWorkerStats, DhtRequestMetaInfoWorkerExit,
    DhtRequestMetaInfoWorkerStats, DhtScrapeWorkerExit, DhtScrapeWorkerStats,
};

/// One downstream child with a stable identity in terminal evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtCrawlerPipelineDownstreamChild {
    Triage,
    GetPeers,
    RequestMetaInfo,
    PersistTorrent,
    Scrape,
    PersistSource,
}

/// The first terminal trigger observed by the supervisor.
///
/// This is deliberately not called a root cause: concurrently ready events and
/// failures observed during cleanup remain represented in the full exit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtCrawlerPipelineTrigger {
    ExternalShutdown,
    Runtime,
    Maintenance,
    Downstream(DhtCrawlerPipelineDownstreamChild),
}

/// Exact fixed-shape results from the six downstream tasks.
///
/// `JoinError` retains panic versus cancellation and the original Tokio task
/// identity. A graceful admission drain expects each successful value to be
/// `InputClosed`, but this structure never rewrites an abnormal exit.
#[derive(Debug)]
pub struct DhtCrawlerPipelineDownstreamExits {
    /// Info-hash triage task result.
    pub triage: Result<DhtInfoHashTriageWorkerExit, JoinError>,
    /// Recursive get-peers task result.
    pub get_peers: Result<DhtGetPeersWorkerExit, JoinError>,
    /// Peer-wire metainfo-request task result.
    pub request_meta_info: Result<DhtRequestMetaInfoWorkerExit, JoinError>,
    /// Atomic torrent-persistence task result.
    pub persist_torrent: Result<DhtPersistTorrentWorkerExit, JoinError>,
    /// BEP-33 scrape task result.
    pub scrape: Result<DhtScrapeWorkerExit, JoinError>,
    /// Source-persistence task result.
    pub persist_source: Result<DhtPersistSourceWorkerExit, JoinError>,
}

/// Exact result of the one blocking-finalization task.
///
/// The outer result is task panic/cancellation. The inner result is the exact
/// blocking-manager transaction outcome.
pub type DhtCrawlerPipelineBlockingResult =
    Result<Result<BlockingFinalizeOutcome, BlockingError>, JoinError>;

#[derive(Debug, thiserror::Error)]
enum DhtCrawlerGooseAdmissionError {
    #[error("could not read the applied Goose migration head: {0}")]
    Read(#[source] DbError),
    #[error(transparent)]
    Head(GooseHeadMismatch),
}

#[async_trait::async_trait]
trait DhtCrawlerGooseAdmission: Send + Sync {
    async fn admit(
        &self,
        pool: &PgPool,
        expected_version: i64,
    ) -> Result<(), DhtCrawlerGooseAdmissionError>;
}

struct PgDhtCrawlerGooseAdmission;

#[async_trait::async_trait]
impl DhtCrawlerGooseAdmission for PgDhtCrawlerGooseAdmission {
    async fn admit(
        &self,
        pool: &PgPool,
        expected_version: i64,
    ) -> Result<(), DhtCrawlerGooseAdmissionError> {
        let actual = read_goose_applied_head(pool)
            .await
            .map_err(DhtCrawlerGooseAdmissionError::Read)?;
        assert_goose_applied_head(actual, expected_version)
            .map_err(DhtCrawlerGooseAdmissionError::Head)?;
        Ok(())
    }
}

/// Fixed terminal evidence from a started pipeline.
#[derive(Debug)]
pub struct DhtCrawlerPipelineCompletedExit {
    /// First terminal event selected by the coordinator, without a causality
    /// claim.
    pub first_trigger: DhtCrawlerPipelineTrigger,
    /// Exact maintenance task result after cleanup joining.
    pub maintenance: Result<DhtCrawlerMaintenanceSupervisorExit, JoinError>,
    /// Exact fixed-shape downstream task results after cleanup joining.
    pub downstream: DhtCrawlerPipelineDownstreamExits,
    /// Exact owned DHT runtime result after the drain and shutdown request.
    pub runtime: Result<DhtRuntimeExit, JoinError>,
    /// Exact inner finalization result or outer finalizer-task failure.
    pub blocking: DhtCrawlerPipelineBlockingResult,
}

/// Terminal result of the staged pipeline supervisor.
#[derive(Debug)]
pub enum DhtCrawlerPipelineExit {
    /// External shutdown was ready at the preflight boundary. Maintenance and
    /// downstream tasks were never spawned or polled, so no child exits are
    /// fabricated. Their owned values and the root triage input were dropped;
    /// runtime shutdown and one finalization attempt were still completed.
    ShutdownBeforeStart {
        /// Exact owned DHT runtime result after the shutdown request.
        runtime: Result<DhtRuntimeExit, JoinError>,
        /// Exact inner finalization result or outer finalizer-task failure.
        blocking: DhtCrawlerPipelineBlockingResult,
    },
    /// The pipeline started and retained every task's exact terminal result.
    Completed(Box<DhtCrawlerPipelineCompletedExit>),
}

impl DhtCrawlerPipelineExit {
    /// Whether an external request produced a complete canonical drain.
    ///
    /// A successful outer shutdown classification is not sufficient: every
    /// nested maintenance and downstream child, the UDP runtime, and blocking
    /// finalization must also retain the expected terminal evidence. A shutdown
    /// observed before task start is clean only when runtime shutdown and the
    /// one finalization attempt both completed successfully.
    #[must_use]
    pub fn is_clean_external_shutdown(&self) -> bool {
        match self {
            Self::ShutdownBeforeStart { runtime, blocking } => {
                matches!(runtime, Ok(DhtRuntimeExit::Shutdown)) && matches!(blocking, Ok(Ok(_)))
            }
            Self::Completed(exit) => {
                exit.first_trigger == DhtCrawlerPipelineTrigger::ExternalShutdown
                    && crate::observe_only_supervisor::maintenance_shutdown_is_clean(
                        &exit.maintenance,
                    )
                    && matches!(
                        &exit.downstream.triage,
                        Ok(DhtInfoHashTriageWorkerExit::InputClosed)
                    )
                    && matches!(
                        &exit.downstream.get_peers,
                        Ok(DhtGetPeersWorkerExit::InputClosed)
                    )
                    && matches!(
                        &exit.downstream.request_meta_info,
                        Ok(DhtRequestMetaInfoWorkerExit::InputClosed)
                    )
                    && matches!(
                        &exit.downstream.persist_torrent,
                        Ok(DhtPersistTorrentWorkerExit::InputClosed)
                    )
                    && matches!(
                        &exit.downstream.scrape,
                        Ok(DhtScrapeWorkerExit::InputClosed)
                    )
                    && matches!(
                        &exit.downstream.persist_source,
                        Ok(DhtPersistSourceWorkerExit::InputClosed)
                    )
                    && matches!(&exit.runtime, Ok(DhtRuntimeExit::Shutdown))
                    && matches!(&exit.blocking, Ok(Ok(_)))
            }
        }
    }
}

/// Observable one-shot lifecycle of a crawler pipeline run.
///
/// `Ready` is published only after all six downstream workers and all nine
/// nested maintenance children have entered their run futures while no
/// terminal event is observable at either supervisor boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtCrawlerPipelineLifecycle {
    /// The supervisor has been constructed but has not proved every child
    /// entered its run future.
    Starting,
    /// Every required child entered its run future and the supervisor had not
    /// observed shutdown or a component terminal result at that boundary.
    Ready,
    /// A terminal trigger began staged cleanup, or cancellation invalidated a
    /// previously ready run.
    Stopping,
    /// Staged cleanup, runtime shutdown, and blocking finalization completed.
    Stopped,
}

/// Lifecycle projected with channel closure made explicit.
///
/// A closed publisher is normal only after `Stopped`. Any other final value is
/// cancellation evidence; the last published lifecycle is retained without
/// pretending staged cleanup completed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtCrawlerPipelineObservedLifecycle {
    /// Publisher is live and has not established readiness.
    Starting,
    /// Publisher is live and every required child entered its run future.
    Ready,
    /// Publisher is live and staged cleanup has begun.
    Stopping,
    /// Staged cleanup completed; publisher closure is also normal here.
    Stopped,
    /// Publisher closed without completing staged cleanup.
    Cancelled {
        /// Final lifecycle value published before cancellation.
        last: DhtCrawlerPipelineLifecycle,
    },
}

/// Cloneable sender-free view of the pipeline lifecycle.
///
/// Channel closure without `Stopped` means the consuming run future was
/// cancelled. Cancellation after readiness first publishes `Stopping`, so a
/// retained snapshot cannot continue to claim readiness.
#[derive(Clone)]
pub struct DhtCrawlerPipelineLifecycleHandle {
    receiver: watch::Receiver<DhtCrawlerPipelineLifecycle>,
}

impl DhtCrawlerPipelineLifecycleHandle {
    pub(crate) fn channel() -> (
        watch::Sender<DhtCrawlerPipelineLifecycle>,
        DhtCrawlerPipelineLifecycleHandle,
    ) {
        let (sender, receiver) = watch::channel(DhtCrawlerPipelineLifecycle::Starting);
        (sender, DhtCrawlerPipelineLifecycleHandle { receiver })
    }

    /// Return the most recently published lifecycle state.
    #[must_use]
    pub fn snapshot(&self) -> DhtCrawlerPipelineLifecycle {
        *self.receiver.borrow()
    }

    /// Whether the live publisher currently reports `Ready`.
    ///
    /// A closed channel is never ready, even if a stale receiver last observed
    /// the ready state.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.observed() == DhtCrawlerPipelineObservedLifecycle::Ready
    }

    /// Project publisher closure without leaving a stale `Ready` value.
    #[must_use]
    pub fn observed(&self) -> DhtCrawlerPipelineObservedLifecycle {
        let mut last = self.snapshot();
        match self.receiver.has_changed() {
            Err(_) => {
                // Once closed, the publisher's final value is stable.
                last = self.snapshot();
                return if last == DhtCrawlerPipelineLifecycle::Stopped {
                    DhtCrawlerPipelineObservedLifecycle::Stopped
                } else {
                    DhtCrawlerPipelineObservedLifecycle::Cancelled { last }
                };
            }
            Ok(true) => {
                // A publication may have raced the first borrow. Re-read it so
                // readiness cannot survive an already-visible transition.
                last = self.snapshot();
            }
            Ok(false) => {}
        }
        match last {
            DhtCrawlerPipelineLifecycle::Starting => DhtCrawlerPipelineObservedLifecycle::Starting,
            DhtCrawlerPipelineLifecycle::Ready => DhtCrawlerPipelineObservedLifecycle::Ready,
            DhtCrawlerPipelineLifecycle::Stopping => DhtCrawlerPipelineObservedLifecycle::Stopping,
            DhtCrawlerPipelineLifecycle::Stopped => DhtCrawlerPipelineObservedLifecycle::Stopped,
        }
    }

    /// Wait for a lifecycle change and return its new value.
    ///
    /// `None` means the run future was cancelled or the terminal publisher was
    /// otherwise dropped. A normal run publishes `Stopped` before closure.
    pub async fn changed(&mut self) -> Option<DhtCrawlerPipelineLifecycle> {
        self.receiver.changed().await.ok()?;
        Some(*self.receiver.borrow_and_update())
    }
}

/// Fixed-shape runtime observations owned by the crawler pipeline.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DhtCrawlerPipelineRuntimeObservabilitySnapshot {
    pub health: DhtRuntimeHealthSnapshot,
    pub inbound: DhtInboundStatsSnapshot,
    pub discovery: DhtDiscoveryStats,
}

/// Fixed-shape maintenance observations excluding the canonical discovery
/// counters already present in the runtime snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DhtCrawlerPipelineMaintenanceObservabilitySnapshot {
    pub scheduler: DhtDiscoveredNodeSchedulerStats,
    pub ping: DhtDiscoveredNodePingStats,
    pub find_node: DhtDiscoveredNodeFindStats,
    pub sample_infohashes_worker: DhtSampleInfoHashesWorkerStats,
    pub oldest_find: DhtOldestNodeFindProducerStats,
    pub oldest_ping: DhtOldestNodePingProducerStats,
    pub bootstrap_ping: DhtBootstrapPingProducerStats,
    pub sample_infohashes_producer: DhtSampleInfoHashesProducerStats,
}

/// Fixed-shape observations for all six downstream workers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DhtCrawlerPipelineDownstreamObservabilitySnapshot {
    pub triage: DhtInfoHashTriageStats,
    pub get_peers: DhtGetPeersWorkerStats,
    pub request_meta_info: DhtRequestMetaInfoWorkerStats,
    pub persist_torrent: DhtPersistTorrentWorkerStats,
    pub scrape: DhtScrapeWorkerStats,
    pub persist_source: DhtPersistSourceWorkerStats,
}

/// One non-transactional, fixed-shape crawler observability snapshot.
///
/// Constituent atomics are read independently. The shape preserves each
/// worker's typed semantics; it does not flatten counters, infer queue depth,
/// or classify normal peer/query failures as application health failures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtCrawlerPipelineObservabilitySnapshot {
    pub lifecycle: DhtCrawlerPipelineObservedLifecycle,
    pub runtime: DhtCrawlerPipelineRuntimeObservabilitySnapshot,
    pub maintenance: DhtCrawlerPipelineMaintenanceObservabilitySnapshot,
    pub downstream: DhtCrawlerPipelineDownstreamObservabilitySnapshot,
}

impl DhtCrawlerPipelineObservabilitySnapshot {
    /// Whether the pipeline is live, fully started, and within the outbound DHT
    /// success-freshness policy.
    ///
    /// This is not application or deployment readiness: database/schema
    /// preflight, writer mode, and outer process ownership remain separate.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.lifecycle == DhtCrawlerPipelineObservedLifecycle::Ready
            && self.runtime.health.status() == DhtRuntimeHealthStatus::Up
    }
}

/// Cloneable sender-free lifecycle and counter observations for one pipeline.
///
/// This handle retains no runtime client, route, worker, task, table, pool, or
/// blocking-finalization capability. It aggregates existing Rust observation
/// seams; it is not a claim of Go Prometheus parity and does not fabricate
/// absent KTable, query-duration, concurrency, or response-drop metrics.
#[derive(Clone)]
pub struct DhtCrawlerPipelineObservabilityHandle {
    lifecycle: DhtCrawlerPipelineLifecycleHandle,
    runtime_health: DhtRuntimeHealthHandle,
    runtime_inbound: DhtInboundStats,
    discovery: DhtDiscoveryStatsHandle,
    maintenance: DhtCrawlerMaintenanceStatsHandle,
    downstream: DhtCrawlerDownstreamStatsHandle,
}

impl DhtCrawlerPipelineObservabilityHandle {
    /// Read every fixed constituent without spawning work.
    #[must_use]
    pub fn snapshot(&self) -> DhtCrawlerPipelineObservabilitySnapshot {
        DhtCrawlerPipelineObservabilitySnapshot {
            lifecycle: self.lifecycle.observed(),
            runtime: DhtCrawlerPipelineRuntimeObservabilitySnapshot {
                health: self.runtime_health.snapshot(),
                inbound: self.runtime_inbound.snapshot(),
                discovery: self.discovery.snapshot(),
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
            downstream: DhtCrawlerPipelineDownstreamObservabilitySnapshot {
                triage: self.downstream.triage.snapshot(),
                get_peers: self.downstream.get_peers.snapshot(),
                request_meta_info: self.downstream.request_meta_info.snapshot(),
                persist_torrent: self.downstream.persist_torrent.snapshot(),
                scrape: self.downstream.scrape.snapshot(),
                persist_source: self.downstream.persist_source.snapshot(),
            },
        }
    }

    /// Evaluate only fresh lifecycle and runtime-health observations, without
    /// loading unrelated counters.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.lifecycle.observed() == DhtCrawlerPipelineObservedLifecycle::Ready
            && self.runtime_health.snapshot().status() == DhtRuntimeHealthStatus::Up
    }
}

/// Cloneable sender-free capabilities returned at supervisor construction.
///
/// The external finalizer clone is a retry/recovery capability after a completed
/// run reports a finalization failure. It must not be called concurrently with
/// [`DhtCrawlerPipelineSupervisor::run`]. After cancellation, abort is not an
/// asynchronous join, so the clone must not be called unless the caller has
/// independently proved every blocking producer has quiesced; without that
/// proof it must be abandoned or handled by a higher-level recovery policy. The
/// application-owned PostgreSQL pool must remain open until that retry or
/// abandon decision is complete.
#[derive(Clone)]
pub struct DhtCrawlerPipelineHandles {
    /// Sender-free counters for all six downstream workers.
    pub downstream_stats: DhtCrawlerDownstreamStatsHandle,
    /// Recovery capability retained independently of the consuming run future;
    /// cancellation alone does not establish that it is safe to call.
    pub blocking_finalizer: Arc<dyn BlockingFinalizer>,
    /// Sender-free lifecycle used by health and readiness surfaces.
    pub lifecycle: DhtCrawlerPipelineLifecycleHandle,
}

/// Typed failure to construct one complete crawler graph.
///
/// Projection, database-admission, and peer-identity failures happen before the
/// UDP runtime binds. A returned failure after a successful bind retains the
/// exact address and awaited runtime cleanup result. Cancelling the start
/// future during cleanup instead drops and aborts the runtime and therefore
/// carries no joined-cleanup proof.
#[derive(Debug, thiserror::Error)]
pub enum DhtCrawlerPipelineStartError {
    #[error("invalid DHT crawler application projection: {0}")]
    Projection(#[source] DhtCrawlerAppProjectionError),
    #[error("could not read the applied Goose migration head: {0}")]
    GooseRead(#[source] Box<DbError>),
    #[error("database failed the exact Goose migration-head assertion: {0}")]
    GooseHead(#[source] GooseHeadMismatch),
    #[error("could not generate the process-local metainfo peer ID: {0}")]
    MetaInfoPeerId(getrandom::Error),
    #[error("could not start the DHT runtime: {0}")]
    Runtime(#[source] Box<DhtRuntimeStartError>),
    #[error(
        "the DHT runtime bound at {local_addr} but exposed no live discovery producer; runtime cleanup: {runtime_cleanup:?}"
    )]
    DiscoveryUnavailable {
        local_addr: SocketAddrV4,
        runtime_cleanup: Result<DhtRuntimeExit, JoinError>,
    },
    #[error(
        "could not construct crawler maintenance after binding {local_addr}: {source}; runtime cleanup: {runtime_cleanup:?}"
    )]
    Maintenance {
        local_addr: SocketAddrV4,
        #[source]
        source: Box<DhtCrawlerMaintenanceWithConfigError>,
        runtime_cleanup: Result<DhtRuntimeExit, JoinError>,
    },
}

impl DhtCrawlerPipelineStartError {
    /// Borrow the exact cleanup result when construction failed after the UDP
    /// runtime had successfully bound and spawned.
    #[must_use]
    pub const fn runtime_cleanup(&self) -> Option<&Result<DhtRuntimeExit, JoinError>> {
        match self {
            Self::DiscoveryUnavailable {
                runtime_cleanup, ..
            }
            | Self::Maintenance {
                runtime_cleanup, ..
            } => Some(runtime_cleanup),
            Self::Projection(_)
            | Self::GooseRead(_)
            | Self::GooseHead(_)
            | Self::MetaInfoPeerId(_)
            | Self::Runtime(_) => None,
        }
    }

    /// Return the exact address that was bound when post-bind construction
    /// failed and cleanup completed.
    #[must_use]
    pub const fn bound_addr(&self) -> Option<SocketAddrV4> {
        match self {
            Self::DiscoveryUnavailable { local_addr, .. }
            | Self::Maintenance { local_addr, .. } => Some(*local_addr),
            Self::Projection(_)
            | Self::GooseRead(_)
            | Self::GooseHead(_)
            | Self::MetaInfoPeerId(_)
            | Self::Runtime(_) => None,
        }
    }
}

/// Concrete owner of the runtime, maintenance composition, root triage input,
/// six downstream workers, and blocking-finalization capability.
///
/// This supervisor owns no restart policy, timeout, metric exporter,
/// deployment integration, or production-readiness claim.
#[must_use = "the owned pipeline must be run or deliberately dropped"]
pub struct DhtCrawlerPipelineSupervisor {
    runtime: DhtRuntime,
    maintenance: DhtCrawlerMaintenanceSupervisor,
    root_triage_input: DhtInfoHashTriageInput,
    downstream: DhtCrawlerDownstreamWorkers,
    finalizer: Arc<dyn BlockingFinalizer>,
    lifecycle: watch::Sender<DhtCrawlerPipelineLifecycle>,
    observability: DhtCrawlerPipelineObservabilityHandle,
}

impl DhtCrawlerPipelineSupervisor {
    /// Validate and construct one complete crawler graph around a caller-owned
    /// PostgreSQL pool.
    ///
    /// The mutable projection is revalidated first. The exact applied Goose
    /// head is then read and asserted without applying or rolling back a
    /// migration. Only after that read-only admission and process-local
    /// metainfo peer-ID generation succeed may the UDP runtime bind.
    ///
    /// If construction fails after the runtime binds, this method explicitly
    /// requests shutdown and awaits the socket-owning task. The returned error
    /// retains the exact bound address and cleanup result. Cancelling this
    /// future during that cleanup falls back to the runtime's aborting `Drop`
    /// behavior and does not return joined-cleanup evidence.
    pub async fn start(
        projection: DhtCrawlerAppProjection,
        pool: &PgPool,
    ) -> Result<(Self, DhtCrawlerPipelineHandles), DhtCrawlerPipelineStartError> {
        Self::start_with_admission_and_maintenance_factory(
            projection,
            pool,
            &PgDhtCrawlerGooseAdmission,
            DhtCrawlerMaintenanceSupervisor::with_config,
        )
        .await
    }

    async fn start_with_admission_and_maintenance_factory<A, M>(
        projection: DhtCrawlerAppProjection,
        pool: &PgPool,
        admission: &A,
        maintenance_factory: M,
    ) -> Result<(Self, DhtCrawlerPipelineHandles), DhtCrawlerPipelineStartError>
    where
        A: DhtCrawlerGooseAdmission,
        M: FnOnce(
            DhtDiscoveryReceiver,
            &DhtRuntimeClient,
            &KTable,
            &DhtInfoHashTriageInput,
            DhtCrawlerMaintenanceConfig,
        ) -> Result<
            (
                DhtCrawlerMaintenanceSupervisor,
                DhtCrawlerMaintenanceStatsHandle,
            ),
            DhtCrawlerMaintenanceWithConfigError,
        >,
    {
        projection
            .validate()
            .map_err(DhtCrawlerPipelineStartError::Projection)?;
        admission
            .admit(pool, projection.expected_goose_version)
            .await
            .map_err(|source| match source {
                DhtCrawlerGooseAdmissionError::Read(source) => {
                    DhtCrawlerPipelineStartError::GooseRead(Box::new(source))
                }
                DhtCrawlerGooseAdmissionError::Head(source) => {
                    DhtCrawlerPipelineStartError::GooseHead(source)
                }
            })?;

        let metainfo_peer_id =
            random_metainfo_peer_id().map_err(DhtCrawlerPipelineStartError::MetaInfoPeerId)?;
        let DhtCrawlerAppProjection {
            expected_goose_version: _,
            runtime,
            maintenance,
            downstream,
        } = projection;
        let mut runtime = DhtRuntime::start(runtime)
            .await
            .map_err(|source| DhtCrawlerPipelineStartError::Runtime(Box::new(source)))?;
        let local_addr = runtime.local_addr();
        let discovery_receiver = runtime
            .take_discovered_nodes()
            .expect("a newly started private runtime exposes its discovery receiver once");
        let Some(discovery) = discovery_receiver.try_sender() else {
            drop(discovery_receiver);
            let runtime_cleanup = runtime.shutdown().await;
            return Err(DhtCrawlerPipelineStartError::DiscoveryUnavailable {
                local_addr,
                runtime_cleanup,
            });
        };

        let client = runtime.client();
        let table = runtime.table().clone();
        let (root_triage_input, downstream) = DhtCrawlerDownstreamComposition::with_config(
            discovery,
            &client,
            &table,
            pool,
            metainfo_peer_id,
            downstream,
        )
        .expect("the downstream projection was revalidated before runtime bind");
        let (maintenance, _) = match maintenance_factory(
            discovery_receiver,
            &client,
            &table,
            &root_triage_input,
            maintenance,
        ) {
            Ok(composition) => composition,
            Err(source) => {
                drop((client, table, root_triage_input, downstream));
                let runtime_cleanup = runtime.shutdown().await;
                return Err(DhtCrawlerPipelineStartError::Maintenance {
                    local_addr,
                    source: Box::new(source),
                    runtime_cleanup,
                });
            }
        };
        drop((client, table));

        Ok(Self::new(
            runtime,
            maintenance,
            root_triage_input,
            downstream,
        ))
    }

    /// Assemble staged lifecycle ownership around already-constructed parts.
    ///
    /// `root_triage_input` must be the original producer paired with the
    /// receiver consumed by `downstream`; maintenance already owns its own
    /// clone. The constructor cannot detect unrelated clones. Retaining another
    /// clone outside this supervisor can prevent the graceful EOF cascade.
    /// Likewise, `runtime`, `maintenance`, and `downstream` must come from one
    /// composition; this constructor cannot verify that pairing. The aggregate
    /// observability surface treats the runtime's discovery handle as canonical
    /// and intentionally omits maintenance's duplicate view.
    #[must_use = "the supervisor and its recovery/statistics handles must be retained"]
    pub fn new(
        runtime: DhtRuntime,
        maintenance: DhtCrawlerMaintenanceSupervisor,
        root_triage_input: DhtInfoHashTriageInput,
        downstream: DhtCrawlerDownstreamComposition,
    ) -> (Self, DhtCrawlerPipelineHandles) {
        let DhtCrawlerDownstreamComposition {
            workers,
            stats,
            blocking_manager,
        } = downstream;
        let finalizer: Arc<dyn BlockingFinalizer> = blocking_manager;
        let (lifecycle, lifecycle_handle) = DhtCrawlerPipelineLifecycleHandle::channel();
        let observability = DhtCrawlerPipelineObservabilityHandle {
            lifecycle: lifecycle_handle.clone(),
            runtime_health: runtime.health(),
            runtime_inbound: runtime.inbound_stats(),
            discovery: runtime.discovery_stats(),
            maintenance: maintenance.stats_handle(),
            downstream: stats.clone(),
        };
        let handles = DhtCrawlerPipelineHandles {
            downstream_stats: stats,
            blocking_finalizer: finalizer.clone(),
            lifecycle: lifecycle_handle,
        };
        (
            Self {
                runtime,
                maintenance,
                root_triage_input,
                downstream: workers,
                finalizer,
                lifecycle,
                observability,
            },
            handles,
        )
    }

    /// Return the actual bound IPv4 DHT address, including an OS-assigned port.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddrV4 {
        self.runtime.local_addr()
    }

    /// Clone the sender-free aggregate observability surface before consuming
    /// the supervisor with [`Self::run`].
    #[must_use]
    pub fn observability_handle(&self) -> DhtCrawlerPipelineObservabilityHandle {
        self.observability.clone()
    }

    /// Run until external shutdown or the first observed component terminal
    /// event, then perform the complete staged cleanup without early return.
    ///
    /// External shutdown has biased priority. A downstream-triggered cleanup
    /// cooperatively stops every remaining downstream worker and makes no
    /// no-loss claim. Maintenance/runtime/external triggers initially leave
    /// downstream shutdown unsignalled; an abnormal downstream result escalates
    /// the cooperative stop without replacing `first_trigger`.
    ///
    /// Cancelling this consuming future drops the root input and requests abort
    /// by dropping the owned `JoinSet`, worker futures, and pinned runtime
    /// future. Abort is not an asynchronous join: arbitrary task code may
    /// continue until its next yield, and maintenance blocking DNS has the
    /// additional qualification documented at module level. Cancellation
    /// returns no terminal accounting or quiescence proof. The caller-owned
    /// finalizer handle remains retained, but must not be called without an
    /// independent proof that every blocking producer has quiesced; otherwise
    /// the caller must abandon it or apply a higher-level recovery policy.
    pub async fn run<F>(self, shutdown: F) -> DhtCrawlerPipelineExit
    where
        F: Future<Output = ()>,
    {
        let (factories, lifecycle) = PipelineFactories::from_supervisor(self);
        run_with_factories_and_lifecycle(shutdown, factories, lifecycle).await
    }
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
type RuntimeFactory =
    Box<dyn FnOnce(watch::Receiver<bool>) -> BoxFuture<Result<DhtRuntimeExit, JoinError>> + Send>;
struct MaintenanceRun {
    future: BoxFuture<DhtCrawlerMaintenanceSupervisorExit>,
    started: BoxFuture<bool>,
    stopping: BoxFuture<bool>,
}

type MaintenanceFactory = Box<dyn FnOnce(watch::Receiver<bool>) -> MaintenanceRun + Send>;
type TriageFactory =
    Box<dyn FnOnce(watch::Receiver<bool>) -> BoxFuture<DhtInfoHashTriageWorkerExit> + Send>;
type GetPeersFactory =
    Box<dyn FnOnce(watch::Receiver<bool>) -> BoxFuture<DhtGetPeersWorkerExit> + Send>;
type RequestMetaInfoFactory =
    Box<dyn FnOnce(watch::Receiver<bool>) -> BoxFuture<DhtRequestMetaInfoWorkerExit> + Send>;
type PersistTorrentFactory =
    Box<dyn FnOnce(watch::Receiver<bool>) -> BoxFuture<DhtPersistTorrentWorkerExit> + Send>;
type ScrapeFactory =
    Box<dyn FnOnce(watch::Receiver<bool>) -> BoxFuture<DhtScrapeWorkerExit> + Send>;
type PersistSourceFactory =
    Box<dyn FnOnce(watch::Receiver<bool>) -> BoxFuture<DhtPersistSourceWorkerExit> + Send>;
type FinalizerFactory =
    Box<dyn FnOnce() -> BoxFuture<Result<BlockingFinalizeOutcome, BlockingError>> + Send>;

struct PipelineFactories {
    runtime: RuntimeFactory,
    maintenance: MaintenanceFactory,
    root_drop: Box<dyn FnOnce() + Send>,
    triage: TriageFactory,
    get_peers: GetPeersFactory,
    request_meta_info: RequestMetaInfoFactory,
    persist_torrent: PersistTorrentFactory,
    scrape: ScrapeFactory,
    persist_source: PersistSourceFactory,
    finalizer: FinalizerFactory,
}

impl PipelineFactories {
    fn from_supervisor(
        supervisor: DhtCrawlerPipelineSupervisor,
    ) -> (Self, watch::Sender<DhtCrawlerPipelineLifecycle>) {
        let DhtCrawlerPipelineSupervisor {
            runtime,
            maintenance,
            root_triage_input,
            downstream:
                DhtCrawlerDownstreamWorkers {
                    triage,
                    get_peers,
                    request_meta_info,
                    scrape,
                    persist_torrent,
                    persist_source,
                },
            finalizer,
            lifecycle,
            observability: _,
        } = supervisor;
        (
            Self {
                runtime: Box::new(move |stop| {
                    Box::pin(runtime.run_until_shutdown(wait_for_stop(stop)))
                }),
                maintenance: Box::new(move |stop| {
                    let (notifications, future) =
                        maintenance.run_with_notifications(wait_for_stop(stop));
                    let (mut started, mut stopping) = notifications.into_parts();
                    MaintenanceRun {
                        future: Box::pin(future),
                        started: Box::pin(async move { started.notified().await.is_ok() }),
                        stopping: Box::pin(async move { stopping.notified().await.is_ok() }),
                    }
                }),
                root_drop: Box::new(move || drop(root_triage_input)),
                triage: Box::new(move |stop| Box::pin(triage.run(wait_for_stop(stop)))),
                get_peers: Box::new(move |stop| Box::pin(get_peers.run(wait_for_stop(stop)))),
                request_meta_info: Box::new(move |stop| {
                    Box::pin(request_meta_info.run(wait_for_stop(stop)))
                }),
                persist_torrent: Box::new(move |stop| {
                    Box::pin(persist_torrent.run(wait_for_stop(stop)))
                }),
                scrape: Box::new(move |stop| Box::pin(scrape.run(wait_for_stop(stop)))),
                persist_source: Box::new(move |stop| {
                    Box::pin(persist_source.run(wait_for_stop(stop)))
                }),
                finalizer: Box::new(move || Box::pin(async move { finalizer.finalize().await })),
            },
            lifecycle,
        )
    }
}

struct LifecyclePublisher {
    sender: Option<watch::Sender<DhtCrawlerPipelineLifecycle>>,
}

impl LifecyclePublisher {
    fn new(sender: watch::Sender<DhtCrawlerPipelineLifecycle>) -> Self {
        Self {
            sender: Some(sender),
        }
    }

    fn publish(&self, state: DhtCrawlerPipelineLifecycle) {
        if let Some(sender) = &self.sender {
            sender.send_replace(state);
        }
    }

    fn finish(mut self) {
        self.publish(DhtCrawlerPipelineLifecycle::Stopped);
        self.sender.take();
    }
}

impl Drop for LifecyclePublisher {
    fn drop(&mut self) {
        let Some(sender) = &self.sender else {
            return;
        };
        if *sender.borrow() == DhtCrawlerPipelineLifecycle::Ready {
            sender.send_replace(DhtCrawlerPipelineLifecycle::Stopping);
        }
    }
}

async fn wait_for_stop(mut stop: watch::Receiver<bool>) {
    loop {
        if *stop.borrow_and_update() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TaskKind {
    Maintenance,
    Triage,
    GetPeers,
    RequestMetaInfo,
    PersistTorrent,
    Scrape,
    PersistSource,
}

impl TaskKind {
    fn downstream(self) -> Option<DhtCrawlerPipelineDownstreamChild> {
        match self {
            Self::Maintenance => None,
            Self::Triage => Some(DhtCrawlerPipelineDownstreamChild::Triage),
            Self::GetPeers => Some(DhtCrawlerPipelineDownstreamChild::GetPeers),
            Self::RequestMetaInfo => Some(DhtCrawlerPipelineDownstreamChild::RequestMetaInfo),
            Self::PersistTorrent => Some(DhtCrawlerPipelineDownstreamChild::PersistTorrent),
            Self::Scrape => Some(DhtCrawlerPipelineDownstreamChild::Scrape),
            Self::PersistSource => Some(DhtCrawlerPipelineDownstreamChild::PersistSource),
        }
    }
}

#[derive(Default)]
struct PipelineStartCollector {
    maintenance: bool,
    triage: bool,
    get_peers: bool,
    request_meta_info: bool,
    persist_torrent: bool,
    scrape: bool,
    persist_source: bool,
}

impl PipelineStartCollector {
    fn record(&mut self, kind: TaskKind) -> bool {
        let slot = match kind {
            TaskKind::Maintenance => &mut self.maintenance,
            TaskKind::Triage => &mut self.triage,
            TaskKind::GetPeers => &mut self.get_peers,
            TaskKind::RequestMetaInfo => &mut self.request_meta_info,
            TaskKind::PersistTorrent => &mut self.persist_torrent,
            TaskKind::Scrape => &mut self.scrape,
            TaskKind::PersistSource => &mut self.persist_source,
        };
        if *slot {
            return false;
        }
        *slot = true;
        self.complete()
    }

    fn complete(&self) -> bool {
        self.maintenance
            && self.triage
            && self.get_peers
            && self.request_meta_info
            && self.persist_torrent
            && self.scrape
            && self.persist_source
    }
}

enum TaskOutput {
    Maintenance(DhtCrawlerMaintenanceSupervisorExit),
    Triage(DhtInfoHashTriageWorkerExit),
    GetPeers(DhtGetPeersWorkerExit),
    RequestMetaInfo(DhtRequestMetaInfoWorkerExit),
    PersistTorrent(DhtPersistTorrentWorkerExit),
    Scrape(DhtScrapeWorkerExit),
    PersistSource(DhtPersistSourceWorkerExit),
}

#[derive(Default)]
struct TaskCollector {
    maintenance: Option<Result<DhtCrawlerMaintenanceSupervisorExit, JoinError>>,
    triage: Option<Result<DhtInfoHashTriageWorkerExit, JoinError>>,
    get_peers: Option<Result<DhtGetPeersWorkerExit, JoinError>>,
    request_meta_info: Option<Result<DhtRequestMetaInfoWorkerExit, JoinError>>,
    persist_torrent: Option<Result<DhtPersistTorrentWorkerExit, JoinError>>,
    scrape: Option<Result<DhtScrapeWorkerExit, JoinError>>,
    persist_source: Option<Result<DhtPersistSourceWorkerExit, JoinError>>,
}

impl TaskCollector {
    fn record(&mut self, kind: TaskKind, result: Result<TaskOutput, JoinError>) -> bool {
        let abnormal = match kind {
            TaskKind::Maintenance => {
                self.maintenance = Some(match result {
                    Ok(TaskOutput::Maintenance(exit)) => Ok(exit),
                    Ok(_) => panic!("maintenance task returned a mismatched output"),
                    Err(error) => Err(error),
                });
                false
            }
            TaskKind::Triage => {
                let result = match result {
                    Ok(TaskOutput::Triage(exit)) => Ok(exit),
                    Ok(_) => panic!("triage task returned a mismatched output"),
                    Err(error) => Err(error),
                };
                let abnormal = !matches!(&result, Ok(DhtInfoHashTriageWorkerExit::InputClosed));
                self.triage = Some(result);
                abnormal
            }
            TaskKind::GetPeers => {
                let result = match result {
                    Ok(TaskOutput::GetPeers(exit)) => Ok(exit),
                    Ok(_) => panic!("get-peers task returned a mismatched output"),
                    Err(error) => Err(error),
                };
                let abnormal = !matches!(&result, Ok(DhtGetPeersWorkerExit::InputClosed));
                self.get_peers = Some(result);
                abnormal
            }
            TaskKind::RequestMetaInfo => {
                let result = match result {
                    Ok(TaskOutput::RequestMetaInfo(exit)) => Ok(exit),
                    Ok(_) => panic!("request-metainfo task returned a mismatched output"),
                    Err(error) => Err(error),
                };
                let abnormal = !matches!(&result, Ok(DhtRequestMetaInfoWorkerExit::InputClosed));
                self.request_meta_info = Some(result);
                abnormal
            }
            TaskKind::PersistTorrent => {
                let result = match result {
                    Ok(TaskOutput::PersistTorrent(exit)) => Ok(exit),
                    Ok(_) => panic!("persist-torrent task returned a mismatched output"),
                    Err(error) => Err(error),
                };
                let abnormal = !matches!(&result, Ok(DhtPersistTorrentWorkerExit::InputClosed));
                self.persist_torrent = Some(result);
                abnormal
            }
            TaskKind::Scrape => {
                let result = match result {
                    Ok(TaskOutput::Scrape(exit)) => Ok(exit),
                    Ok(_) => panic!("scrape task returned a mismatched output"),
                    Err(error) => Err(error),
                };
                let abnormal = !matches!(&result, Ok(DhtScrapeWorkerExit::InputClosed));
                self.scrape = Some(result);
                abnormal
            }
            TaskKind::PersistSource => {
                let result = match result {
                    Ok(TaskOutput::PersistSource(exit)) => Ok(exit),
                    Ok(_) => panic!("persist-source task returned a mismatched output"),
                    Err(error) => Err(error),
                };
                let abnormal = !matches!(&result, Ok(DhtPersistSourceWorkerExit::InputClosed));
                self.persist_source = Some(result);
                abnormal
            }
        };
        abnormal
    }

    fn maintenance_done(&self) -> bool {
        self.maintenance.is_some()
    }

    fn downstream_done(&self) -> bool {
        self.triage.is_some()
            && self.get_peers.is_some()
            && self.request_meta_info.is_some()
            && self.persist_torrent.is_some()
            && self.scrape.is_some()
            && self.persist_source.is_some()
    }

    fn finish(
        self,
    ) -> (
        Result<DhtCrawlerMaintenanceSupervisorExit, JoinError>,
        DhtCrawlerPipelineDownstreamExits,
    ) {
        (
            self.maintenance.expect("maintenance result is complete"),
            DhtCrawlerPipelineDownstreamExits {
                triage: self.triage.expect("triage result is complete"),
                get_peers: self.get_peers.expect("get-peers result is complete"),
                request_meta_info: self
                    .request_meta_info
                    .expect("request-metainfo result is complete"),
                persist_torrent: self
                    .persist_torrent
                    .expect("persist-torrent result is complete"),
                scrape: self.scrape.expect("scrape result is complete"),
                persist_source: self
                    .persist_source
                    .expect("persist-source result is complete"),
            },
        )
    }
}

fn spawn_task<F>(
    tasks: &mut JoinSet<TaskOutput>,
    task_kinds: &mut HashMap<Id, TaskKind>,
    kind: TaskKind,
    future: F,
) where
    F: Future<Output = TaskOutput> + Send + 'static,
{
    let task = tasks.spawn(future);
    let previous = task_kinds.insert(task.id(), kind);
    assert!(
        previous.is_none(),
        "Tokio task IDs are unique within the set"
    );
}

async fn join_next_task(
    tasks: &mut JoinSet<TaskOutput>,
    task_kinds: &mut HashMap<Id, TaskKind>,
) -> (TaskKind, Result<TaskOutput, JoinError>) {
    let joined = tasks
        .join_next_with_id()
        .await
        .expect("maintenance or downstream task remains");
    match joined {
        Ok((id, output)) => {
            let kind = task_kinds
                .remove(&id)
                .expect("successful task has a registered identity");
            (kind, Ok(output))
        }
        Err(error) => {
            let kind = task_kinds
                .remove(&error.id())
                .expect("failed task has a registered identity");
            (kind, Err(error))
        }
    }
}

async fn finish_runtime_and_finalizer(
    mut runtime: BoxFuture<Result<DhtRuntimeExit, JoinError>>,
    runtime_result: Option<Result<DhtRuntimeExit, JoinError>>,
    finalizer: FinalizerFactory,
) -> (
    Result<DhtRuntimeExit, JoinError>,
    DhtCrawlerPipelineBlockingResult,
) {
    let mut runtime_result = runtime_result;
    let mut finalizer_tasks = JoinSet::new();
    finalizer_tasks.spawn(finalizer());
    let mut blocking = None;

    while runtime_result.is_none() || blocking.is_none() {
        tokio::select! {
            biased;
            result = runtime.as_mut(), if runtime_result.is_none() => {
                runtime_result = Some(result);
            }
            result = finalizer_tasks.join_next(), if blocking.is_none() => {
                blocking = Some(result.expect("one finalizer task remains"));
            }
        }
    }

    (
        runtime_result.expect("runtime result is complete"),
        blocking.expect("blocking result is complete"),
    )
}

#[cfg(test)]
async fn run_with_factories<F>(shutdown: F, factories: PipelineFactories) -> DhtCrawlerPipelineExit
where
    F: Future<Output = ()>,
{
    let (lifecycle, _lifecycle_receiver) = watch::channel(DhtCrawlerPipelineLifecycle::Starting);
    run_with_factories_and_lifecycle(shutdown, factories, lifecycle).await
}

async fn run_with_factories_and_lifecycle<F>(
    shutdown: F,
    factories: PipelineFactories,
    lifecycle: watch::Sender<DhtCrawlerPipelineLifecycle>,
) -> DhtCrawlerPipelineExit
where
    F: Future<Output = ()>,
{
    let lifecycle = LifecyclePublisher::new(lifecycle);
    tokio::pin!(shutdown);
    let pre_ready =
        poll_fn(|context| Poll::Ready(matches!(shutdown.as_mut().poll(context), Poll::Ready(()))))
            .await;

    let PipelineFactories {
        runtime,
        maintenance,
        root_drop,
        triage,
        get_peers,
        request_meta_info,
        persist_torrent,
        scrape,
        persist_source,
        finalizer,
    } = factories;

    if pre_ready {
        lifecycle.publish(DhtCrawlerPipelineLifecycle::Stopping);
        drop((
            maintenance,
            triage,
            get_peers,
            request_meta_info,
            persist_torrent,
            scrape,
            persist_source,
        ));
        root_drop();
        let (_runtime_stop, runtime_stop) = watch::channel(true);
        let runtime = runtime(runtime_stop);
        let (runtime, blocking) = finish_runtime_and_finalizer(runtime, None, finalizer).await;
        let exit = DhtCrawlerPipelineExit::ShutdownBeforeStart { runtime, blocking };
        lifecycle.finish();
        return exit;
    }

    let (maintenance_stop, maintenance_stop_rx) = watch::channel(false);
    let (downstream_stop, downstream_stop_rx) = watch::channel(false);
    let (runtime_stop, runtime_stop_rx) = watch::channel(false);
    let mut runtime = runtime(runtime_stop_rx);
    let mut root_drop = Some(root_drop);

    let MaintenanceRun {
        future: maintenance,
        started: mut maintenance_started,
        stopping: mut maintenance_stopping,
    } = maintenance(maintenance_stop_rx);
    let mut maintenance_start_open = true;
    let mut maintenance_stopping_open = true;
    let (downstream_started_tx, mut downstream_started_rx) = mpsc::unbounded_channel();
    let mut downstream_start_open = true;
    let mut tasks = JoinSet::new();
    let mut task_kinds = HashMap::new();
    spawn_task(
        &mut tasks,
        &mut task_kinds,
        TaskKind::Maintenance,
        async move { TaskOutput::Maintenance(maintenance.await) },
    );
    spawn_task(&mut tasks, &mut task_kinds, TaskKind::Triage, {
        let stop = downstream_stop_rx.clone();
        let started = downstream_started_tx.clone();
        async move {
            let future = triage(stop);
            let _ = started.send(TaskKind::Triage);
            TaskOutput::Triage(future.await)
        }
    });
    spawn_task(&mut tasks, &mut task_kinds, TaskKind::GetPeers, {
        let stop = downstream_stop_rx.clone();
        let started = downstream_started_tx.clone();
        async move {
            let future = get_peers(stop);
            let _ = started.send(TaskKind::GetPeers);
            TaskOutput::GetPeers(future.await)
        }
    });
    spawn_task(&mut tasks, &mut task_kinds, TaskKind::RequestMetaInfo, {
        let stop = downstream_stop_rx.clone();
        let started = downstream_started_tx.clone();
        async move {
            let future = request_meta_info(stop);
            let _ = started.send(TaskKind::RequestMetaInfo);
            TaskOutput::RequestMetaInfo(future.await)
        }
    });
    spawn_task(&mut tasks, &mut task_kinds, TaskKind::PersistTorrent, {
        let stop = downstream_stop_rx.clone();
        let started = downstream_started_tx.clone();
        async move {
            let future = persist_torrent(stop);
            let _ = started.send(TaskKind::PersistTorrent);
            TaskOutput::PersistTorrent(future.await)
        }
    });
    spawn_task(&mut tasks, &mut task_kinds, TaskKind::Scrape, {
        let stop = downstream_stop_rx.clone();
        let started = downstream_started_tx.clone();
        async move {
            let future = scrape(stop);
            let _ = started.send(TaskKind::Scrape);
            TaskOutput::Scrape(future.await)
        }
    });
    spawn_task(
        &mut tasks,
        &mut task_kinds,
        TaskKind::PersistSource,
        async move {
            let future = persist_source(downstream_stop_rx);
            let _ = downstream_started_tx.send(TaskKind::PersistSource);
            TaskOutput::PersistSource(future.await)
        },
    );

    let mut collector = TaskCollector::default();
    let mut starts = PipelineStartCollector::default();
    let mut runtime_result = None;
    enum FirstEvent {
        Trigger(DhtCrawlerPipelineTrigger),
        MaintenanceStarted(bool),
        MaintenanceStopping(bool),
        DownstreamStarted(Option<TaskKind>),
    }
    let first_trigger = loop {
        let event = tokio::select! {
            biased;
            () = &mut shutdown => {
                FirstEvent::Trigger(DhtCrawlerPipelineTrigger::ExternalShutdown)
            }
            result = runtime.as_mut() => {
                runtime_result = Some(result);
                FirstEvent::Trigger(DhtCrawlerPipelineTrigger::Runtime)
            }
            joined = join_next_task(&mut tasks, &mut task_kinds) => {
                let (kind, result) = joined;
                let child = kind.downstream();
                let _abnormal = collector.record(kind, result);
                FirstEvent::Trigger(match child {
                    Some(child) => DhtCrawlerPipelineTrigger::Downstream(child),
                    None => DhtCrawlerPipelineTrigger::Maintenance,
                })
            }
            stopping = maintenance_stopping.as_mut(), if maintenance_stopping_open => {
                FirstEvent::MaintenanceStopping(stopping)
            }
            started = maintenance_started.as_mut(),
                if maintenance_start_open && !starts.complete() =>
            {
                FirstEvent::MaintenanceStarted(started)
            }
            started = downstream_started_rx.recv(),
                if downstream_start_open && !starts.complete() =>
            {
                FirstEvent::DownstreamStarted(started)
            }
        };
        match event {
            FirstEvent::Trigger(trigger) => break trigger,
            FirstEvent::MaintenanceStopping(true) => {
                break DhtCrawlerPipelineTrigger::Maintenance;
            }
            FirstEvent::MaintenanceStopping(false) => maintenance_stopping_open = false,
            FirstEvent::MaintenanceStarted(true) => {
                maintenance_start_open = false;
                if starts.record(TaskKind::Maintenance) {
                    lifecycle.publish(DhtCrawlerPipelineLifecycle::Ready);
                }
            }
            FirstEvent::MaintenanceStarted(false) => maintenance_start_open = false,
            FirstEvent::DownstreamStarted(Some(kind)) => {
                if starts.record(kind) {
                    lifecycle.publish(DhtCrawlerPipelineLifecycle::Ready);
                }
            }
            FirstEvent::DownstreamStarted(None) => downstream_start_open = false,
        }
    };
    lifecycle.publish(DhtCrawlerPipelineLifecycle::Stopping);

    let _ = maintenance_stop.send(true);
    let mut downstream_stop_sent =
        matches!(first_trigger, DhtCrawlerPipelineTrigger::Downstream(_));
    if downstream_stop_sent {
        let _ = downstream_stop.send(true);
    }

    while !collector.maintenance_done() {
        tokio::select! {
            biased;
            result = runtime.as_mut(), if runtime_result.is_none() => {
                runtime_result = Some(result);
            }
            joined = join_next_task(&mut tasks, &mut task_kinds) => {
                let (kind, result) = joined;
                let is_downstream = kind.downstream().is_some();
                let abnormal = collector.record(kind, result);
                if is_downstream && (root_drop.is_some() || abnormal) && !downstream_stop_sent {
                    let _ = downstream_stop.send(true);
                    downstream_stop_sent = true;
                }
            }
        }
    }

    root_drop.take().expect("root input is dropped once")();

    while !collector.downstream_done() {
        tokio::select! {
            biased;
            result = runtime.as_mut(), if runtime_result.is_none() => {
                runtime_result = Some(result);
            }
            joined = join_next_task(&mut tasks, &mut task_kinds) => {
                let (kind, result) = joined;
                let abnormal = collector.record(kind, result);
                if abnormal && !downstream_stop_sent {
                    let _ = downstream_stop.send(true);
                    downstream_stop_sent = true;
                }
            }
        }
    }

    assert!(
        tasks.is_empty(),
        "all maintenance and downstream tasks joined"
    );
    assert!(task_kinds.is_empty(), "all task identities were consumed");
    let (maintenance, downstream) = collector.finish();

    let _ = runtime_stop.send(true);
    let (runtime, blocking) =
        finish_runtime_and_finalizer(runtime, runtime_result, finalizer).await;

    let exit = DhtCrawlerPipelineExit::Completed(Box::new(DhtCrawlerPipelineCompletedExit {
        first_trigger,
        maintenance,
        downstream,
        runtime,
        blocking,
    }));
    lifecycle.finish();
    exit
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fmt;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex, MutexGuard};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use bitmagnet_dht::{
        dht_discovery_channel, DhtCrawlerMaintenanceSupervisor, DhtDiscoveryOffer,
        DhtDiscoveryStats, DhtInfoHashTriageRequest, Id20, RoutingNode,
    };
    use clap::{CommandFactory, FromArgMatches};
    use sqlx::postgres::PgPoolOptions;
    use tokio::net::UdpSocket;
    use tokio::sync::{oneshot, Notify};
    use tokio::time::timeout;

    use super::*;
    use crate::{
        DhtCrawlerAppConfig, DhtCrawlerAppProjection, DhtCrawlerClassifierQueue,
        DhtGetPeersWorkerStats, DhtInfoHashTriageStats, DhtPersistSourceWorkerStats,
        DhtPersistTorrentWorkerStats, DhtRequestMetaInfoWorkerStats, DhtScrapeWorkerStats,
    };

    enum TestGooseAdmission {
        Allow,
        ReadFailure,
        MissingHead,
    }

    #[async_trait::async_trait]
    impl DhtCrawlerGooseAdmission for TestGooseAdmission {
        async fn admit(
            &self,
            _pool: &PgPool,
            expected_version: i64,
        ) -> Result<(), DhtCrawlerGooseAdmissionError> {
            match self {
                Self::Allow => Ok(()),
                Self::ReadFailure => Err(DhtCrawlerGooseAdmissionError::Read(DbError::Config(
                    "scripted Goose read failure".to_owned(),
                ))),
                Self::MissingHead => Err(DhtCrawlerGooseAdmissionError::Head(
                    GooseHeadMismatch::Missing {
                        required: expected_version,
                    },
                )),
            }
        }
    }

    #[derive(Default)]
    struct StartGate {
        count: AtomicUsize,
        notify: Notify,
    }

    impl StartGate {
        fn mark(&self, log: &EventLog, event: &'static str) {
            log.push(event);
            self.count.fetch_add(1, Ordering::SeqCst);
            self.notify.notify_waiters();
        }

        async fn wait_for(&self, target: usize) {
            loop {
                let notified = self.notify.notified();
                if self.count.load(Ordering::SeqCst) >= target {
                    return;
                }
                notified.await;
            }
        }
    }

    #[derive(Clone, Default)]
    struct EventLog(Arc<Mutex<Vec<&'static str>>>);

    impl EventLog {
        fn lock(&self) -> MutexGuard<'_, Vec<&'static str>> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }

        fn push(&self, event: &'static str) {
            self.lock().push(event);
        }

        fn snapshot(&self) -> Vec<&'static str> {
            self.lock().clone()
        }
    }

    #[derive(Clone, Copy)]
    enum MaintenanceMode {
        Stop,
        Panic,
    }

    #[derive(Clone, Copy)]
    enum TriageMode {
        Natural,
        Trigger,
    }

    #[derive(Clone, Copy)]
    enum PersistSourceMode {
        Natural,
        Panic,
    }

    enum FinalizerMode {
        Ok,
        Error,
        Panic,
    }

    #[derive(Debug)]
    struct FinalizerError;

    impl fmt::Display for FinalizerError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("sentinel finalizer error")
        }
    }

    impl Error for FinalizerError {}

    fn position(events: &[&str], event: &str) -> usize {
        events
            .iter()
            .position(|candidate| *candidate == event)
            .unwrap_or_else(|| panic!("missing event {event}: {events:?}"))
    }

    async fn wait_for_true(mut receiver: watch::Receiver<bool>) {
        loop {
            if *receiver.borrow_and_update() {
                return;
            }
            receiver.changed().await.unwrap();
        }
    }

    fn normal_factories(
        log: EventLog,
        gate: Arc<StartGate>,
        finalizer_calls: Arc<AtomicUsize>,
    ) -> PipelineFactories {
        scripted_factories(
            log,
            gate,
            MaintenanceMode::Stop,
            TriageMode::Natural,
            PersistSourceMode::Natural,
            FinalizerMode::Ok,
            finalizer_calls,
        )
    }

    struct PendingThenReady(bool);

    impl Future for PendingThenReady {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                context.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    fn completed(exit: DhtCrawlerPipelineExit) -> DhtCrawlerPipelineCompletedExit {
        match exit {
            DhtCrawlerPipelineExit::Completed(exit) => *exit,
            DhtCrawlerPipelineExit::ShutdownBeforeStart { .. } => {
                panic!("pipeline was expected to start")
            }
        }
    }

    async fn run_bounded<F>(shutdown: F, factories: PipelineFactories) -> DhtCrawlerPipelineExit
    where
        F: Future<Output = ()>,
    {
        timeout(
            Duration::from_secs(5),
            run_with_factories(shutdown, factories),
        )
        .await
        .expect("scripted pipeline lifecycle must remain bounded")
    }

    fn assert_natural_downstream(exits: &DhtCrawlerPipelineDownstreamExits) {
        assert!(matches!(
            exits.triage,
            Ok(DhtInfoHashTriageWorkerExit::InputClosed)
        ));
        assert!(matches!(
            exits.get_peers,
            Ok(DhtGetPeersWorkerExit::InputClosed)
        ));
        assert!(matches!(
            exits.request_meta_info,
            Ok(DhtRequestMetaInfoWorkerExit::InputClosed)
        ));
        assert!(matches!(
            exits.persist_torrent,
            Ok(DhtPersistTorrentWorkerExit::InputClosed)
        ));
        assert!(matches!(exits.scrape, Ok(DhtScrapeWorkerExit::InputClosed)));
        assert!(matches!(
            exits.persist_source,
            Ok(DhtPersistSourceWorkerExit::InputClosed)
        ));
    }

    fn lifecycle_channel() -> (
        watch::Sender<DhtCrawlerPipelineLifecycle>,
        DhtCrawlerPipelineLifecycleHandle,
    ) {
        let (sender, receiver) = watch::channel(DhtCrawlerPipelineLifecycle::Starting);
        (sender, DhtCrawlerPipelineLifecycleHandle { receiver })
    }

    async fn wait_for_lifecycle(
        lifecycle: &mut DhtCrawlerPipelineLifecycleHandle,
        expected: DhtCrawlerPipelineLifecycle,
    ) {
        timeout(Duration::from_secs(5), async {
            loop {
                if lifecycle.snapshot() == expected {
                    return;
                }
                lifecycle
                    .changed()
                    .await
                    .expect("lifecycle publisher remains live until the expected state");
            }
        })
        .await
        .unwrap_or_else(|_| panic!("lifecycle did not reach {expected:?}"));
    }

    fn scripted_maintenance_run<F, Fut>(run: F) -> MaintenanceRun
    where
        F: FnOnce(oneshot::Sender<()>, oneshot::Sender<()>) -> Fut,
        Fut: Future<Output = DhtCrawlerMaintenanceSupervisorExit> + Send + 'static,
    {
        let (started_tx, started_rx) = oneshot::channel();
        let (stopping_tx, stopping_rx) = oneshot::channel();
        MaintenanceRun {
            future: Box::pin(run(started_tx, stopping_tx)),
            started: Box::pin(async move { started_rx.await.is_ok() }),
            stopping: Box::pin(async move { stopping_rx.await.is_ok() }),
        }
    }

    // Scripted one-shot factories keep production ownership concrete while
    // making lifecycle ordering deterministic and network-free in unit tests.
    #[allow(clippy::too_many_lines)]
    fn scripted_factories(
        log: EventLog,
        gate: Arc<StartGate>,
        maintenance_mode: MaintenanceMode,
        triage_mode: TriageMode,
        persist_source_mode: PersistSourceMode,
        finalizer_mode: FinalizerMode,
        finalizer_calls: Arc<AtomicUsize>,
    ) -> PipelineFactories {
        let (root_closed, root_closed_rx) = watch::channel(false);

        let runtime_log = log.clone();
        let runtime_gate = gate.clone();
        let maintenance_log = log.clone();
        let maintenance_gate = gate.clone();
        let triage_log = log.clone();
        let triage_gate = gate.clone();
        let get_peers_log = log.clone();
        let get_peers_gate = gate.clone();
        let request_log = log.clone();
        let request_gate = gate.clone();
        let torrent_log = log.clone();
        let torrent_gate = gate.clone();
        let scrape_log = log.clone();
        let scrape_gate = gate.clone();
        let source_log = log.clone();
        let source_gate = gate;
        let finalizer_log = log.clone();
        let triage_root = root_closed_rx.clone();
        let get_peers_root = root_closed_rx.clone();
        let request_root = root_closed_rx.clone();
        let torrent_root = root_closed_rx.clone();
        let scrape_root = root_closed_rx.clone();
        let source_root = root_closed_rx;

        PipelineFactories {
            runtime: Box::new(move |stop| {
                Box::pin(async move {
                    runtime_gate.mark(&runtime_log, "runtime_started");
                    wait_for_stop(stop).await;
                    runtime_log.push("runtime_stopped");
                    Ok(DhtRuntimeExit::Shutdown)
                })
            }),
            maintenance: Box::new(move |stop| {
                scripted_maintenance_run(move |started, stopping| async move {
                    maintenance_gate.mark(&maintenance_log, "maintenance_started");
                    let _ = started.send(());
                    wait_for_stop(stop).await;
                    let _ = stopping.send(());
                    maintenance_log.push("maintenance_stopped");
                    match maintenance_mode {
                        MaintenanceMode::Stop => {
                            DhtCrawlerMaintenanceSupervisorExit::ShutdownBeforeStart
                        }
                        MaintenanceMode::Panic => panic!("sentinel maintenance panic"),
                    }
                })
            }),
            root_drop: Box::new(move || {
                log.push("root_dropped");
                let _ = root_closed.send(true);
            }),
            triage: Box::new(move |stop| {
                Box::pin(async move {
                    triage_gate.mark(&triage_log, "triage_started");
                    match triage_mode {
                        TriageMode::Trigger => {
                            triage_log.push("triage_triggered");
                            DhtInfoHashTriageWorkerExit::Shutdown {
                                queued_dropped: 0,
                                batch_dropped: 0,
                            }
                        }
                        TriageMode::Natural => {
                            tokio::select! {
                                biased;
                                () = wait_for_stop(stop) => {
                                    triage_log.push("triage_cooperative_stop");
                                    DhtInfoHashTriageWorkerExit::Shutdown {
                                        queued_dropped: 0,
                                        batch_dropped: 0,
                                    }
                                }
                                () = wait_for_true(triage_root) => {
                                    triage_log.push("triage_eof");
                                    DhtInfoHashTriageWorkerExit::InputClosed
                                }
                            }
                        }
                    }
                })
            }),
            get_peers: Box::new(move |stop| {
                Box::pin(async move {
                    get_peers_gate.mark(&get_peers_log, "get_peers_started");
                    tokio::select! {
                        biased;
                        () = wait_for_stop(stop) => {
                            get_peers_log.push("get_peers_cooperative_stop");
                            DhtGetPeersWorkerExit::Shutdown {
                                queued_dropped: 0,
                                tasks_cancelled: 0,
                                recursive_nodes_dropped: 0,
                                meta_info_requests_dropped: 0,
                            }
                        }
                        () = wait_for_true(get_peers_root) => {
                            get_peers_log.push("get_peers_eof");
                            DhtGetPeersWorkerExit::InputClosed
                        }
                    }
                })
            }),
            request_meta_info: Box::new(move |stop| {
                Box::pin(async move {
                    request_gate.mark(&request_log, "request_started");
                    tokio::select! {
                        biased;
                        () = wait_for_stop(stop) => {
                            request_log.push("request_cooperative_stop");
                            DhtRequestMetaInfoWorkerExit::Shutdown {
                                queued_dropped: 0,
                                tasks_cancelled: 0,
                                peer_occurrences_dropped: 0,
                                request_attempts_cancelled: 0,
                                block_calls_cancelled: 0,
                                persist_requests_dropped: 0,
                            }
                        }
                        () = wait_for_true(request_root) => {
                            request_log.push("request_eof");
                            DhtRequestMetaInfoWorkerExit::InputClosed
                        }
                    }
                })
            }),
            persist_torrent: Box::new(move |stop| {
                Box::pin(async move {
                    torrent_gate.mark(&torrent_log, "torrent_started");
                    tokio::select! {
                        biased;
                        () = wait_for_stop(stop) => {
                            torrent_log.push("torrent_cooperative_stop");
                            DhtPersistTorrentWorkerExit::Shutdown {
                                queued_dropped: 0,
                                preplan_dropped: 0,
                                write_abandoned: 0,
                                write_scrapes_suppressed: 0,
                                scrape_abandoned: 0,
                            }
                        }
                        () = wait_for_true(torrent_root) => {
                            torrent_log.push("torrent_eof");
                            DhtPersistTorrentWorkerExit::InputClosed
                        }
                    }
                })
            }),
            scrape: Box::new(move |stop| {
                Box::pin(async move {
                    scrape_gate.mark(&scrape_log, "scrape_started");
                    tokio::select! {
                        biased;
                        () = wait_for_stop(stop) => {
                            scrape_log.push("scrape_cooperative_stop");
                            DhtScrapeWorkerExit::Shutdown {
                                queued_dropped: 0,
                                tasks_cancelled: 0,
                                recursive_nodes_dropped: 0,
                                persist_source_requests_dropped: 0,
                            }
                        }
                        () = wait_for_true(scrape_root) => {
                            scrape_log.push("scrape_eof");
                            DhtScrapeWorkerExit::InputClosed
                        }
                    }
                })
            }),
            persist_source: Box::new(move |stop| {
                Box::pin(async move {
                    source_gate.mark(&source_log, "source_started");
                    match persist_source_mode {
                        PersistSourceMode::Panic => {
                            wait_for_true(source_root).await;
                            panic!("sentinel persist-source panic")
                        }
                        PersistSourceMode::Natural => {
                            tokio::select! {
                                biased;
                                () = wait_for_stop(stop) => {
                                    source_log.push("source_cooperative_stop");
                                    DhtPersistSourceWorkerExit::Shutdown {
                                        queued_dropped: 0,
                                        batch_dropped: 0,
                                        write_abandoned: 0,
                                    }
                                }
                                () = wait_for_true(source_root) => {
                                    source_log.push("source_eof");
                                    DhtPersistSourceWorkerExit::InputClosed
                                }
                            }
                        }
                    }
                })
            }),
            finalizer: Box::new(move || {
                Box::pin(async move {
                    finalizer_calls.fetch_add(1, Ordering::SeqCst);
                    finalizer_log.push("finalizer");
                    match finalizer_mode {
                        FinalizerMode::Ok => Ok(BlockingFinalizeOutcome::NothingPending),
                        FinalizerMode::Error => Err(BlockingError::Store(Box::new(FinalizerError))),
                        FinalizerMode::Panic => panic!("sentinel finalizer panic"),
                    }
                })
            }),
        }
    }

    #[tokio::test]
    async fn concurrent_start_and_external_staged_drain_are_exact() {
        let log = EventLog::default();
        let gate = Arc::new(StartGate::default());
        let finalizer_calls = Arc::new(AtomicUsize::new(0));
        let factories = normal_factories(log.clone(), gate.clone(), finalizer_calls.clone());

        let exit = run_bounded(gate.wait_for(8), factories).await;
        assert!(exit.is_clean_external_shutdown());
        let exit = completed(exit);

        assert_eq!(
            exit.first_trigger,
            DhtCrawlerPipelineTrigger::ExternalShutdown
        );
        assert!(matches!(
            exit.maintenance,
            Ok(DhtCrawlerMaintenanceSupervisorExit::ShutdownBeforeStart)
        ));
        assert_natural_downstream(&exit.downstream);
        assert!(matches!(exit.runtime, Ok(DhtRuntimeExit::Shutdown)));
        assert!(matches!(
            exit.blocking,
            Ok(Ok(BlockingFinalizeOutcome::NothingPending))
        ));
        assert_eq!(finalizer_calls.load(Ordering::SeqCst), 1);

        let events = log.snapshot();
        let maintenance_stopped = position(&events, "maintenance_stopped");
        let root_dropped = position(&events, "root_dropped");
        assert!(maintenance_stopped < root_dropped, "{events:?}");
        for eof in [
            "triage_eof",
            "get_peers_eof",
            "request_eof",
            "torrent_eof",
            "scrape_eof",
            "source_eof",
        ] {
            let eof = position(&events, eof);
            assert!(root_dropped < eof, "{events:?}");
            assert!(eof < position(&events, "runtime_stopped"), "{events:?}");
            assert!(eof < position(&events, "finalizer"), "{events:?}");
        }
    }

    #[tokio::test]
    async fn lifecycle_requires_every_start_then_tracks_stopping_and_stopped() {
        let log = EventLog::default();
        let gate = Arc::new(StartGate::default());
        let finalizer_calls = Arc::new(AtomicUsize::new(0));
        let mut factories = normal_factories(log.clone(), gate.clone(), finalizer_calls.clone());
        let maintenance_gate = gate.clone();
        let maintenance_log = log.clone();
        let (maintenance_stopping_tx, maintenance_stopping_rx) = oneshot::channel();
        let maintenance_release = Arc::new(Notify::new());
        let maintenance_release_task = maintenance_release.clone();
        factories.maintenance = Box::new(move |stop| {
            scripted_maintenance_run(move |started, stopping| async move {
                maintenance_gate.mark(&maintenance_log, "maintenance_started");
                let _ = started.send(());
                wait_for_stop(stop).await;
                let _ = stopping.send(());
                let _ = maintenance_stopping_tx.send(());
                maintenance_release_task.notified().await;
                maintenance_log.push("maintenance_stopped");
                DhtCrawlerMaintenanceSupervisorExit::ShutdownBeforeStart
            })
        });
        let (lifecycle_tx, mut lifecycle) = lifecycle_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn(run_with_factories_and_lifecycle(
            async move {
                let _ = shutdown_rx.await;
            },
            factories,
            lifecycle_tx,
        ));

        wait_for_lifecycle(&mut lifecycle, DhtCrawlerPipelineLifecycle::Ready).await;
        assert!(lifecycle.is_ready());
        assert_eq!(
            lifecycle.observed(),
            DhtCrawlerPipelineObservedLifecycle::Ready
        );
        shutdown_tx.send(()).unwrap();
        timeout(Duration::from_secs(5), maintenance_stopping_rx)
            .await
            .expect("maintenance must observe staged stop")
            .unwrap();
        assert_eq!(lifecycle.snapshot(), DhtCrawlerPipelineLifecycle::Stopping);
        assert!(!lifecycle.is_ready());
        assert_eq!(
            lifecycle.observed(),
            DhtCrawlerPipelineObservedLifecycle::Stopping
        );

        maintenance_release.notify_one();
        let exit = timeout(Duration::from_secs(5), run)
            .await
            .expect("pipeline must finish after maintenance release")
            .unwrap();
        assert!(matches!(exit, DhtCrawlerPipelineExit::Completed(_)));
        assert_eq!(lifecycle.snapshot(), DhtCrawlerPipelineLifecycle::Stopped);
        assert!(!lifecycle.is_ready());
        assert_eq!(
            lifecycle.observed(),
            DhtCrawlerPipelineObservedLifecycle::Stopped
        );
        assert_eq!(finalizer_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn nested_stopping_beats_a_queued_stale_start_during_hung_cleanup() {
        let gate = Arc::new(StartGate::default());
        let mut factories =
            normal_factories(EventLog::default(), gate, Arc::new(AtomicUsize::new(0)));
        let (nested_stopping_tx, nested_stopping_rx) = oneshot::channel();
        let cleanup_release = Arc::new(Notify::new());
        let cleanup_release_task = cleanup_release.clone();
        factories.maintenance = Box::new(move |_stop| {
            scripted_maintenance_run(move |started, stopping| async move {
                let _ = started.send(());
                let _ = stopping.send(());
                let _ = nested_stopping_tx.send(());
                cleanup_release_task.notified().await;
                DhtCrawlerMaintenanceSupervisorExit::ShutdownBeforeStart
            })
        });
        let (lifecycle_tx, mut lifecycle) = lifecycle_channel();
        let run = tokio::spawn(run_with_factories_and_lifecycle(
            std::future::pending(),
            factories,
            lifecycle_tx,
        ));

        timeout(Duration::from_secs(5), nested_stopping_rx)
            .await
            .expect("maintenance must queue start and stopping notifications")
            .unwrap();
        wait_for_lifecycle(&mut lifecycle, DhtCrawlerPipelineLifecycle::Stopping).await;
        assert!(!lifecycle.is_ready());
        tokio::task::yield_now().await;
        assert_eq!(lifecycle.snapshot(), DhtCrawlerPipelineLifecycle::Stopping);

        cleanup_release.notify_one();
        let exit = timeout(Duration::from_secs(5), run)
            .await
            .expect("released nested cleanup must finish the pipeline")
            .unwrap();
        let DhtCrawlerPipelineExit::Completed(exit) = exit else {
            panic!("nested maintenance terminal notification starts normal cleanup");
        };
        assert_eq!(exit.first_trigger, DhtCrawlerPipelineTrigger::Maintenance);
        assert_eq!(lifecycle.snapshot(), DhtCrawlerPipelineLifecycle::Stopped);
    }

    #[tokio::test]
    async fn cancellation_after_ready_closes_at_stopping_without_stopped() {
        let factories = normal_factories(
            EventLog::default(),
            Arc::new(StartGate::default()),
            Arc::new(AtomicUsize::new(0)),
        );
        let (lifecycle_tx, mut lifecycle) = lifecycle_channel();
        let run = tokio::spawn(run_with_factories_and_lifecycle(
            std::future::pending(),
            factories,
            lifecycle_tx,
        ));

        wait_for_lifecycle(&mut lifecycle, DhtCrawlerPipelineLifecycle::Ready).await;
        run.abort();
        assert!(run.await.unwrap_err().is_cancelled());
        while lifecycle.changed().await.is_some() {}
        assert_eq!(lifecycle.snapshot(), DhtCrawlerPipelineLifecycle::Stopping);
        assert!(!lifecycle.is_ready());
        assert_eq!(
            lifecycle.observed(),
            DhtCrawlerPipelineObservedLifecycle::Cancelled {
                last: DhtCrawlerPipelineLifecycle::Stopping,
            }
        );
    }

    #[test]
    fn closed_lifecycle_distinguishes_cancellation_from_normal_stop() {
        let (sender, lifecycle) = lifecycle_channel();
        drop(sender);
        assert_eq!(
            lifecycle.observed(),
            DhtCrawlerPipelineObservedLifecycle::Cancelled {
                last: DhtCrawlerPipelineLifecycle::Starting,
            }
        );
        assert!(!lifecycle.is_ready());

        let (sender, lifecycle) = lifecycle_channel();
        sender
            .send(DhtCrawlerPipelineLifecycle::Stopped)
            .expect("test receiver remains live");
        drop(sender);
        assert_eq!(
            lifecycle.observed(),
            DhtCrawlerPipelineObservedLifecycle::Stopped
        );
        assert!(!lifecycle.is_ready());
    }

    #[tokio::test]
    async fn pre_ready_shutdown_withholds_stopped_until_runtime_and_finalizer_finish() {
        let log = EventLog::default();
        let mut factories = normal_factories(
            log.clone(),
            Arc::new(StartGate::default()),
            Arc::new(AtomicUsize::new(0)),
        );
        let (runtime_started_tx, runtime_started_rx) = oneshot::channel();
        let runtime_release = Arc::new(Notify::new());
        let runtime_release_task = runtime_release.clone();
        factories.runtime = Box::new(move |_stop| {
            Box::pin(async move {
                let _ = runtime_started_tx.send(());
                runtime_release_task.notified().await;
                Ok(DhtRuntimeExit::Shutdown)
            })
        });
        let (finalizer_started_tx, finalizer_started_rx) = oneshot::channel();
        let finalizer_release = Arc::new(Notify::new());
        let finalizer_release_task = finalizer_release.clone();
        factories.finalizer = Box::new(move || {
            Box::pin(async move {
                let _ = finalizer_started_tx.send(());
                finalizer_release_task.notified().await;
                Ok(BlockingFinalizeOutcome::NothingPending)
            })
        });
        let (lifecycle_tx, lifecycle) = lifecycle_channel();
        let run = tokio::spawn(run_with_factories_and_lifecycle(
            std::future::ready(()),
            factories,
            lifecycle_tx,
        ));

        timeout(Duration::from_secs(5), async {
            runtime_started_rx.await.unwrap();
            finalizer_started_rx.await.unwrap();
        })
        .await
        .expect("pre-ready cleanup must poll runtime and finalizer");
        assert_eq!(lifecycle.snapshot(), DhtCrawlerPipelineLifecycle::Stopping);
        assert!(!lifecycle.is_ready());
        assert!(!log
            .snapshot()
            .iter()
            .any(|event| event.ends_with("_started") && *event != "runtime_started"));

        runtime_release.notify_one();
        tokio::task::yield_now().await;
        assert_eq!(lifecycle.snapshot(), DhtCrawlerPipelineLifecycle::Stopping);
        finalizer_release.notify_one();
        let exit = timeout(Duration::from_secs(5), run)
            .await
            .expect("both cleanup releases must finish pre-ready shutdown")
            .unwrap();
        assert!(matches!(
            exit,
            DhtCrawlerPipelineExit::ShutdownBeforeStart { .. }
        ));
        assert_eq!(lifecycle.snapshot(), DhtCrawlerPipelineLifecycle::Stopped);
    }

    #[tokio::test]
    async fn withheld_nested_maintenance_start_keeps_lifecycle_starting() {
        let log = EventLog::default();
        let gate = Arc::new(StartGate::default());
        let mut factories =
            normal_factories(log.clone(), gate.clone(), Arc::new(AtomicUsize::new(0)));
        let maintenance_gate = gate.clone();
        factories.maintenance = Box::new(move |stop| {
            scripted_maintenance_run(move |_started, stopping| async move {
                maintenance_gate.mark(&EventLog::default(), "maintenance_started");
                wait_for_stop(stop).await;
                let _ = stopping.send(());
                DhtCrawlerMaintenanceSupervisorExit::ShutdownBeforeStart
            })
        });
        let (lifecycle_tx, lifecycle) = lifecycle_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn(run_with_factories_and_lifecycle(
            async move {
                let _ = shutdown_rx.await;
            },
            factories,
            lifecycle_tx,
        ));

        timeout(Duration::from_secs(5), gate.wait_for(8))
            .await
            .expect("runtime, maintenance, and six downstream futures must enter");
        tokio::task::yield_now().await;
        assert_eq!(lifecycle.snapshot(), DhtCrawlerPipelineLifecycle::Starting);
        assert!(!lifecycle.is_ready());

        shutdown_tx.send(()).unwrap();
        timeout(Duration::from_secs(5), run)
            .await
            .expect("withheld startup pipeline must still stop")
            .unwrap();
        assert_eq!(lifecycle.snapshot(), DhtCrawlerPipelineLifecycle::Stopped);
    }

    #[test]
    fn duplicate_pipeline_start_does_not_replace_a_missing_identity() {
        let mut starts = PipelineStartCollector::default();
        for kind in [
            TaskKind::Maintenance,
            TaskKind::Triage,
            TaskKind::GetPeers,
            TaskKind::RequestMetaInfo,
            TaskKind::PersistTorrent,
            TaskKind::Scrape,
        ] {
            assert!(!starts.record(kind));
        }
        assert!(!starts.record(TaskKind::Triage));
        assert!(!starts.complete());
        assert!(starts.record(TaskKind::PersistSource));
        assert!(starts.complete());
    }

    #[tokio::test]
    async fn external_equal_ready_is_biased() {
        let mut factories = normal_factories(
            EventLog::default(),
            Arc::new(StartGate::default()),
            Arc::new(AtomicUsize::new(0)),
        );
        factories.runtime = Box::new(|_stop| Box::pin(async { Ok(DhtRuntimeExit::Shutdown) }));

        let exit = completed(run_bounded(PendingThenReady(false), factories).await);

        assert_eq!(
            exit.first_trigger,
            DhtCrawlerPipelineTrigger::ExternalShutdown
        );
        assert!(matches!(exit.runtime, Ok(DhtRuntimeExit::Shutdown)));
    }

    #[tokio::test]
    async fn ready_runtime_is_preserved_as_first_trigger() {
        let mut factories = normal_factories(
            EventLog::default(),
            Arc::new(StartGate::default()),
            Arc::new(AtomicUsize::new(0)),
        );
        factories.runtime = Box::new(|_stop| Box::pin(async { Ok(DhtRuntimeExit::Shutdown) }));

        let exit = completed(run_bounded(std::future::pending(), factories).await);

        assert_eq!(exit.first_trigger, DhtCrawlerPipelineTrigger::Runtime);
        assert!(matches!(exit.runtime, Ok(DhtRuntimeExit::Shutdown)));
        assert_natural_downstream(&exit.downstream);
    }

    #[tokio::test]
    async fn ready_maintenance_is_preserved_as_first_trigger() {
        let mut factories = normal_factories(
            EventLog::default(),
            Arc::new(StartGate::default()),
            Arc::new(AtomicUsize::new(0)),
        );
        factories.maintenance = Box::new(|_stop| {
            scripted_maintenance_run(|_started, stopping| async move {
                let _ = stopping.send(());
                DhtCrawlerMaintenanceSupervisorExit::ShutdownBeforeStart
            })
        });

        let exit = completed(run_bounded(std::future::pending(), factories).await);

        assert_eq!(exit.first_trigger, DhtCrawlerPipelineTrigger::Maintenance);
        assert!(matches!(
            exit.maintenance,
            Ok(DhtCrawlerMaintenanceSupervisorExit::ShutdownBeforeStart)
        ));
        assert_natural_downstream(&exit.downstream);
    }

    #[tokio::test]
    async fn downstream_trigger_cooperatively_stops_siblings() {
        let log = EventLog::default();
        let gate = Arc::new(StartGate::default());
        let factories = scripted_factories(
            log.clone(),
            gate,
            MaintenanceMode::Stop,
            TriageMode::Trigger,
            PersistSourceMode::Natural,
            FinalizerMode::Ok,
            Arc::new(AtomicUsize::new(0)),
        );

        let exit = completed(run_bounded(std::future::pending(), factories).await);

        assert_eq!(
            exit.first_trigger,
            DhtCrawlerPipelineTrigger::Downstream(DhtCrawlerPipelineDownstreamChild::Triage)
        );
        assert!(matches!(
            exit.downstream.triage,
            Ok(DhtInfoHashTriageWorkerExit::Shutdown { .. })
        ));
        assert!(matches!(
            exit.downstream.get_peers,
            Ok(DhtGetPeersWorkerExit::Shutdown { .. })
        ));
        assert!(matches!(
            exit.downstream.request_meta_info,
            Ok(DhtRequestMetaInfoWorkerExit::Shutdown { .. })
        ));
        assert!(matches!(
            exit.downstream.persist_torrent,
            Ok(DhtPersistTorrentWorkerExit::Shutdown { .. })
        ));
        assert!(matches!(
            exit.downstream.scrape,
            Ok(DhtScrapeWorkerExit::Shutdown { .. })
        ));
        assert!(matches!(
            exit.downstream.persist_source,
            Ok(DhtPersistSourceWorkerExit::Shutdown { .. })
        ));
        let events = log.snapshot();
        assert!(position(&events, "maintenance_stopped") < position(&events, "root_dropped"));
        for stopped in [
            "get_peers_cooperative_stop",
            "request_cooperative_stop",
            "torrent_cooperative_stop",
            "scrape_cooperative_stop",
            "source_cooperative_stop",
        ] {
            assert!(events.contains(&stopped), "{events:?}");
        }
    }

    #[tokio::test]
    async fn first_external_trigger_survives_cleanup_panics() {
        let log = EventLog::default();
        let gate = Arc::new(StartGate::default());
        let factories = scripted_factories(
            log,
            gate.clone(),
            MaintenanceMode::Panic,
            TriageMode::Natural,
            PersistSourceMode::Panic,
            FinalizerMode::Ok,
            Arc::new(AtomicUsize::new(0)),
        );

        let exit = run_bounded(gate.wait_for(8), factories).await;
        assert!(!exit.is_clean_external_shutdown());
        let exit = completed(exit);

        assert_eq!(
            exit.first_trigger,
            DhtCrawlerPipelineTrigger::ExternalShutdown
        );
        assert!(exit.maintenance.is_err_and(|error| error.is_panic()));
        assert!(exit
            .downstream
            .persist_source
            .is_err_and(|error| error.is_panic()));
        assert!(matches!(exit.runtime, Ok(DhtRuntimeExit::Shutdown)));
        assert!(matches!(
            exit.blocking,
            Ok(Ok(BlockingFinalizeOutcome::NothingPending))
        ));
    }

    #[tokio::test]
    async fn finalizer_runs_once_and_preserves_inner_error_and_outer_panic() {
        let error_calls = Arc::new(AtomicUsize::new(0));
        let error_exit = run_bounded(
            std::future::ready(()),
            scripted_factories(
                EventLog::default(),
                Arc::new(StartGate::default()),
                MaintenanceMode::Stop,
                TriageMode::Natural,
                PersistSourceMode::Natural,
                FinalizerMode::Error,
                error_calls.clone(),
            ),
        )
        .await;
        let DhtCrawlerPipelineExit::ShutdownBeforeStart { runtime, blocking } = error_exit else {
            panic!("pre-ready shutdown must not fabricate child exits")
        };
        assert!(matches!(runtime, Ok(DhtRuntimeExit::Shutdown)));
        let inner = blocking.expect("finalizer task did not panic");
        let error = inner.expect_err("sentinel store error is preserved");
        assert!(matches!(
            error,
            BlockingError::Store(ref source) if source.downcast_ref::<FinalizerError>().is_some()
        ));
        assert_eq!(error_calls.load(Ordering::SeqCst), 1);

        let panic_calls = Arc::new(AtomicUsize::new(0));
        let panic_exit = run_bounded(
            std::future::ready(()),
            scripted_factories(
                EventLog::default(),
                Arc::new(StartGate::default()),
                MaintenanceMode::Stop,
                TriageMode::Natural,
                PersistSourceMode::Natural,
                FinalizerMode::Panic,
                panic_calls.clone(),
            ),
        )
        .await;
        let DhtCrawlerPipelineExit::ShutdownBeforeStart { runtime, blocking } = panic_exit else {
            panic!("pre-ready shutdown must not fabricate child exits")
        };
        assert!(matches!(runtime, Ok(DhtRuntimeExit::Shutdown)));
        assert!(blocking.is_err_and(|error| error.is_panic()));
        assert_eq!(panic_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pre_ready_shutdown_starts_no_maintenance_or_downstream_child() {
        let log = EventLog::default();
        let finalizer_calls = Arc::new(AtomicUsize::new(0));
        let exit = run_bounded(
            std::future::ready(()),
            normal_factories(
                log.clone(),
                Arc::new(StartGate::default()),
                finalizer_calls.clone(),
            ),
        )
        .await;

        assert!(exit.is_clean_external_shutdown());
        assert!(matches!(
            exit,
            DhtCrawlerPipelineExit::ShutdownBeforeStart {
                runtime: Ok(DhtRuntimeExit::Shutdown),
                blocking: Ok(Ok(BlockingFinalizeOutcome::NothingPending)),
            }
        ));
        assert_eq!(finalizer_calls.load(Ordering::SeqCst), 1);
        let events = log.snapshot();
        assert!(events.contains(&"root_dropped"), "{events:?}");
        assert!(events.contains(&"runtime_started"), "{events:?}");
        assert!(events.contains(&"runtime_stopped"), "{events:?}");
        assert!(events.contains(&"finalizer"), "{events:?}");
        assert!(!events
            .iter()
            .any(|event| event.ends_with("_started") && *event != "runtime_started"));
    }

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    struct DropMark {
        gate: Arc<StartGate>,
        log: EventLog,
        event: &'static str,
    }

    struct DropCount(Arc<AtomicUsize>);

    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Drop for DropMark {
        fn drop(&mut self) {
            self.gate.mark(&self.log, self.event);
        }
    }

    #[tokio::test]
    async fn cancelling_started_run_drops_scripted_outer_futures_and_root() {
        let starts = Arc::new(StartGate::default());
        let drops = Arc::new(StartGate::default());
        let log = EventLog::default();
        let root_drops = Arc::new(AtomicUsize::new(0));
        let finalizer_calls = Arc::new(AtomicUsize::new(0));

        macro_rules! pending_factory {
            ($output:ty, $started:literal, $dropped:literal) => {{
                let starts = starts.clone();
                let drops = drops.clone();
                let log = log.clone();
                Box::new(move |_stop| -> BoxFuture<$output> {
                    Box::pin(async move {
                        starts.mark(&log, $started);
                        let _guard = DropMark {
                            gate: drops,
                            log,
                            event: $dropped,
                        };
                        std::future::pending().await
                    })
                })
            }};
        }

        let root_guard = DropCount(root_drops.clone());
        let finalizer_call_count = finalizer_calls.clone();
        let factories = PipelineFactories {
            runtime: pending_factory!(Result<DhtRuntimeExit, JoinError>, "runtime_started", "runtime_dropped"),
            maintenance: {
                let starts = starts.clone();
                let drops = drops.clone();
                let log = log.clone();
                Box::new(move |_stop| {
                    scripted_maintenance_run(move |started, _stopping| async move {
                        starts.mark(&log, "maintenance_started");
                        let _ = started.send(());
                        let _guard = DropMark {
                            gate: drops,
                            log,
                            event: "maintenance_dropped",
                        };
                        std::future::pending().await
                    })
                })
            },
            root_drop: Box::new(move || drop(root_guard)),
            triage: pending_factory!(
                DhtInfoHashTriageWorkerExit,
                "triage_started",
                "triage_dropped"
            ),
            get_peers: pending_factory!(
                DhtGetPeersWorkerExit,
                "get_peers_started",
                "get_peers_dropped"
            ),
            request_meta_info: pending_factory!(
                DhtRequestMetaInfoWorkerExit,
                "request_started",
                "request_dropped"
            ),
            persist_torrent: pending_factory!(
                DhtPersistTorrentWorkerExit,
                "torrent_started",
                "torrent_dropped"
            ),
            scrape: pending_factory!(DhtScrapeWorkerExit, "scrape_started", "scrape_dropped"),
            persist_source: pending_factory!(
                DhtPersistSourceWorkerExit,
                "source_started",
                "source_dropped"
            ),
            finalizer: Box::new(move || {
                finalizer_call_count.fetch_add(1, Ordering::SeqCst);
                Box::pin(std::future::pending())
            }),
        };

        let starts_for_shutdown = starts.clone();
        let task = tokio::spawn(run_with_factories(
            async move {
                starts_for_shutdown.wait_for(8).await;
                std::future::pending::<()>().await;
            },
            factories,
        ));
        timeout(Duration::from_secs(5), starts.wait_for(8))
            .await
            .expect("runtime and every child must start");
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        // These guards prove destruction of the eight scripted outer futures.
        // They do not claim nested production tasks are asynchronously joined
        // or that an immediate finalizer retry is safe.
        timeout(Duration::from_secs(5), drops.wait_for(8))
            .await
            .expect("dropping run must destroy every scripted outer future");
        assert_eq!(root_drops.load(Ordering::SeqCst), 1);
        assert_eq!(finalizer_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cancelling_run_drops_pending_outer_runtime_and_finalizer_futures() {
        let child_calls = Arc::new(AtomicUsize::new(0));
        let root_drops = Arc::new(AtomicUsize::new(0));
        let (runtime_started, runtime_started_rx) = oneshot::channel();
        let (runtime_dropped, runtime_dropped_rx) = oneshot::channel();
        let (finalizer_started, finalizer_started_rx) = oneshot::channel();
        let (finalizer_dropped, finalizer_dropped_rx) = oneshot::channel();

        macro_rules! unused_factory {
            ($exit:ty) => {{
                let calls = child_calls.clone();
                Box::new(move |_stop| -> BoxFuture<$exit> {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Box::pin(std::future::pending())
                })
            }};
        }

        let root_drop_count = root_drops.clone();
        let factories = PipelineFactories {
            runtime: Box::new(move |_stop| {
                Box::pin(async move {
                    let _guard = DropSignal(Some(runtime_dropped));
                    runtime_started.send(()).unwrap();
                    std::future::pending().await
                })
            }),
            maintenance: {
                let calls = child_calls.clone();
                Box::new(move |_stop| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    scripted_maintenance_run(|_started, _stopping| std::future::pending())
                })
            },
            root_drop: Box::new(move || {
                root_drop_count.fetch_add(1, Ordering::SeqCst);
            }),
            triage: unused_factory!(DhtInfoHashTriageWorkerExit),
            get_peers: unused_factory!(DhtGetPeersWorkerExit),
            request_meta_info: unused_factory!(DhtRequestMetaInfoWorkerExit),
            persist_torrent: unused_factory!(DhtPersistTorrentWorkerExit),
            scrape: unused_factory!(DhtScrapeWorkerExit),
            persist_source: unused_factory!(DhtPersistSourceWorkerExit),
            finalizer: Box::new(move || {
                Box::pin(async move {
                    let _guard = DropSignal(Some(finalizer_dropped));
                    finalizer_started.send(()).unwrap();
                    std::future::pending().await
                })
            }),
        };

        let task = tokio::spawn(run_with_factories(std::future::ready(()), factories));
        timeout(Duration::from_secs(5), runtime_started_rx)
            .await
            .expect("runtime factory must be polled")
            .unwrap();
        timeout(Duration::from_secs(5), finalizer_started_rx)
            .await
            .expect("finalizer task must be polled")
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        // The drop signals belong to the directly-owned scripted futures only;
        // they are not a nested-worker quiescence or safe-retry claim.
        timeout(Duration::from_secs(5), runtime_dropped_rx)
            .await
            .expect("runtime future must be dropped")
            .unwrap();
        timeout(Duration::from_secs(5), finalizer_dropped_rx)
            .await
            .expect("finalizer future must be dropped")
            .unwrap();
        assert_eq!(root_drops.load(Ordering::SeqCst), 1);
        assert_eq!(child_calls.load(Ordering::SeqCst), 0);
    }

    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    fn projected_app_config(scaling_factor: usize) -> DhtCrawlerAppProjection {
        let command = DhtCrawlerAppConfig::command()
            .mut_args(|argument| argument.env(Option::<&'static str>::None));
        let matches = command
            .try_get_matches_from([
                "bitmagnet-dht-crawler".to_owned(),
                "--expected-goose-version".to_owned(),
                "29".to_owned(),
                "--classifier-queue".to_owned(),
                "shadow".to_owned(),
                "--dht-crawler-scaling-factor".to_owned(),
                scaling_factor.to_string(),
            ])
            .expect("parse projected app config without ambient environment");
        DhtCrawlerAppConfig::from_arg_matches(&matches)
            .expect("build projected app config")
            .projection()
            .expect("project complete bounded graph")
    }

    fn scaled_id(value: usize) -> Id20 {
        let mut bytes = [0_u8; 20];
        bytes[12..].copy_from_slice(&u64::try_from(value).unwrap().to_be_bytes());
        Id20::from_slice(&bytes).unwrap()
    }

    fn scaled_node(value: usize) -> RoutingNode {
        RoutingNode {
            id: scaled_id(value),
            addr: SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                10_000 + u16::try_from(value).unwrap(),
            )),
        }
    }

    fn scaled_triage_request(value: usize) -> DhtInfoHashTriageRequest {
        DhtInfoHashTriageRequest {
            info_hash: scaled_id(value),
            source_node_addr: scaled_node(value).addr,
        }
    }

    #[test]
    fn public_supervisor_and_recovery_handles_have_owned_task_traits() {
        assert_send::<DhtCrawlerPipelineSupervisor>();
        assert_send::<DhtCrawlerPipelineStartError>();
        assert_send_sync::<DhtCrawlerPipelineHandles>();
        assert_send_sync::<DhtCrawlerPipelineObservabilityHandle>();
        assert_send_sync::<DhtCrawlerPipelineObservabilitySnapshot>();
        assert_send::<DhtCrawlerPipelineExit>();
    }

    #[tokio::test]
    async fn writer_start_revalidates_every_projection_before_admission_or_udp_bind() {
        let occupied = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let occupied_addr = match occupied.local_addr().unwrap() {
            SocketAddr::V4(addr) => addr,
            SocketAddr::V6(_) => unreachable!("the test binds IPv4"),
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();

        let mut invalid_goose = projected_app_config(2);
        invalid_goose.runtime.bind_addr = occupied_addr;
        invalid_goose.expected_goose_version = 0;
        let error =
            match DhtCrawlerPipelineSupervisor::start_with_admission_and_maintenance_factory(
                invalid_goose,
                &pool,
                &TestGooseAdmission::ReadFailure,
                DhtCrawlerMaintenanceSupervisor::with_config,
            )
            .await
            {
                Ok(_) => panic!("invalid Goose projection must fail"),
                Err(error) => error,
            };
        assert!(matches!(
            error,
            DhtCrawlerPipelineStartError::Projection(
                DhtCrawlerAppProjectionError::ExpectedGooseVersion { version: 0 }
            )
        ));

        let mut invalid_runtime = projected_app_config(2);
        invalid_runtime.runtime.bind_addr = occupied_addr;
        invalid_runtime.runtime.discovery_capacity =
            NonZeroUsize::new(bitmagnet_dht::DHT_CHANNEL_MAX_CAPACITY + 1).unwrap();
        let error =
            match DhtCrawlerPipelineSupervisor::start_with_admission_and_maintenance_factory(
                invalid_runtime,
                &pool,
                &TestGooseAdmission::ReadFailure,
                DhtCrawlerMaintenanceSupervisor::with_config,
            )
            .await
            {
                Ok(_) => panic!("invalid runtime projection must fail"),
                Err(error) => error,
            };
        assert!(matches!(
            error,
            DhtCrawlerPipelineStartError::Projection(DhtCrawlerAppProjectionError::Runtime(_))
        ));

        let mut invalid_maintenance = projected_app_config(2);
        invalid_maintenance.runtime.bind_addr = occupied_addr;
        invalid_maintenance
            .maintenance
            .bootstrap_ping
            .reseed_interval = Duration::ZERO;
        let error =
            match DhtCrawlerPipelineSupervisor::start_with_admission_and_maintenance_factory(
                invalid_maintenance,
                &pool,
                &TestGooseAdmission::ReadFailure,
                DhtCrawlerMaintenanceSupervisor::with_config,
            )
            .await
            {
                Ok(_) => panic!("invalid maintenance projection must fail"),
                Err(error) => error,
            };
        assert!(matches!(
            error,
            DhtCrawlerPipelineStartError::Projection(DhtCrawlerAppProjectionError::Maintenance(_))
        ));

        let mut invalid_downstream = projected_app_config(2);
        invalid_downstream.runtime.bind_addr = occupied_addr;
        invalid_downstream.downstream.root_triage_capacity =
            NonZeroUsize::new(bitmagnet_dht::DHT_CHANNEL_MAX_CAPACITY + 1).unwrap();
        let error =
            match DhtCrawlerPipelineSupervisor::start_with_admission_and_maintenance_factory(
                invalid_downstream,
                &pool,
                &TestGooseAdmission::ReadFailure,
                DhtCrawlerMaintenanceSupervisor::with_config,
            )
            .await
            {
                Ok(_) => panic!("invalid downstream projection must fail"),
                Err(error) => error,
            };
        assert!(matches!(
            error,
            DhtCrawlerPipelineStartError::Projection(DhtCrawlerAppProjectionError::Downstream(_))
        ));

        assert_eq!(pool.size(), 0);
        assert_eq!(
            occupied.local_addr().unwrap(),
            SocketAddr::V4(occupied_addr)
        );
        pool.close().await;
    }

    #[tokio::test]
    async fn writer_start_admission_failures_precede_peer_entropy_and_udp_bind() {
        let occupied = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let occupied_addr = match occupied.local_addr().unwrap() {
            SocketAddr::V4(addr) => addr,
            SocketAddr::V6(_) => unreachable!("the test binds IPv4"),
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();

        for (admission, expected_read_failure) in [
            (TestGooseAdmission::ReadFailure, true),
            (TestGooseAdmission::MissingHead, false),
        ] {
            let mut projection = projected_app_config(2);
            projection.runtime.bind_addr = occupied_addr;
            let error =
                match DhtCrawlerPipelineSupervisor::start_with_admission_and_maintenance_factory(
                    projection,
                    &pool,
                    &admission,
                    DhtCrawlerMaintenanceSupervisor::with_config,
                )
                .await
                {
                    Ok(_) => panic!("scripted admission must fail"),
                    Err(error) => error,
                };
            if expected_read_failure {
                assert!(matches!(&error, DhtCrawlerPipelineStartError::GooseRead(_)));
            } else {
                assert!(matches!(
                    &error,
                    DhtCrawlerPipelineStartError::GooseHead(GooseHeadMismatch::Missing {
                        required: 29
                    })
                ));
            }
            assert_eq!(error.bound_addr(), None);
            assert!(error.runtime_cleanup().is_none());
        }

        assert_eq!(pool.size(), 0);
        assert_eq!(
            occupied.local_addr().unwrap(),
            SocketAddr::V4(occupied_addr)
        );
        pool.close().await;
    }

    #[tokio::test]
    async fn writer_start_builds_the_projected_graph_without_worker_or_pool_activity() {
        let mut projection = projected_app_config(2);
        projection.runtime.bind_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();

        let (supervisor, handles) =
            DhtCrawlerPipelineSupervisor::start_with_admission_and_maintenance_factory(
                projection,
                &pool,
                &TestGooseAdmission::Allow,
                DhtCrawlerMaintenanceSupervisor::with_config,
            )
            .await
            .expect("the admitted offline graph starts");
        let runtime_addr = supervisor.local_addr();
        assert_eq!(
            supervisor
                .downstream
                .persist_torrent
                .config_for_test()
                .classifier_queue,
            DhtCrawlerClassifierQueue::Shadow
        );
        assert_eq!(pool.size(), 0, "the injected admission and graph are lazy");
        assert_eq!(
            handles.downstream_stats.triage.snapshot(),
            DhtInfoHashTriageStats::default()
        );

        let exit = timeout(
            Duration::from_secs(5),
            supervisor.run(std::future::ready(())),
        )
        .await
        .expect("pre-ready shutdown is bounded");
        assert!(matches!(
            exit,
            DhtCrawlerPipelineExit::ShutdownBeforeStart {
                runtime: Ok(DhtRuntimeExit::Shutdown),
                blocking: Ok(Ok(BlockingFinalizeOutcome::NothingPending)),
            }
        ));
        assert_eq!(pool.size(), 0);
        assert!(UdpSocket::bind(runtime_addr).await.is_ok());
        pool.close().await;
    }

    #[tokio::test]
    async fn writer_start_post_bind_failure_joins_runtime_and_releases_exact_udp_addr() {
        let mut projection = projected_app_config(2);
        projection.runtime.bind_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();

        let error =
            match DhtCrawlerPipelineSupervisor::start_with_admission_and_maintenance_factory(
                projection,
                &pool,
                &TestGooseAdmission::Allow,
                |real_discovery, client, table, triage, config| {
                    drop(real_discovery);
                    let (closed_sender, closed_receiver) =
                        dht_discovery_channel(NonZeroUsize::new(1).unwrap());
                    drop(closed_sender);
                    DhtCrawlerMaintenanceSupervisor::with_config(
                        closed_receiver,
                        client,
                        table,
                        triage,
                        config,
                    )
                },
            )
            .await
            {
                Ok(_) => panic!("scripted maintenance construction must fail"),
                Err(error) => error,
            };
        let DhtCrawlerPipelineStartError::Maintenance {
            local_addr,
            source,
            runtime_cleanup,
        } = error
        else {
            panic!("expected typed post-bind maintenance failure");
        };
        assert!(source.as_start_error().is_some());
        assert!(matches!(runtime_cleanup, Ok(DhtRuntimeExit::Shutdown)));
        assert_eq!(pool.size(), 0, "no downstream worker was started or polled");
        let rebound = UdpSocket::bind(local_addr)
            .await
            .expect("returned cleanup proof must release the exact UDP address");
        drop(rebound);
        pool.close().await;
    }

    #[tokio::test]
    async fn projected_scaling_graphs_are_bounded_offline_and_release_runtime() {
        for scaling_factor in [2, 10] {
            let DhtCrawlerAppProjection {
                expected_goose_version: _,
                mut runtime,
                maintenance,
                downstream,
            } = projected_app_config(scaling_factor);
            runtime.bind_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);

            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
                .unwrap();
            assert_eq!(pool.size(), 0);
            assert!(!pool.is_closed());

            let discovery_capacity = runtime.discovery_capacity.get();
            let mut runtime = DhtRuntime::start(runtime).await.unwrap();
            let runtime_addr = runtime.local_addr();
            let client = runtime.client();
            let table = runtime.table().clone();
            let mut discovery_receiver = runtime
                .take_discovered_nodes()
                .expect("the runtime exposes its discovery receiver once");
            let discovery = discovery_receiver
                .try_sender()
                .expect("the live runtime retains its discovery producer");
            let discovery_stats = discovery.stats_handle();
            for value in 0..discovery_capacity {
                assert_eq!(
                    discovery.offer(scaled_node(value)),
                    DhtDiscoveryOffer::Queued
                );
            }
            assert_eq!(
                discovery.offer(scaled_node(discovery_capacity)),
                DhtDiscoveryOffer::FullDropped
            );
            let expected_discovery_stats = DhtDiscoveryStats {
                offered: u64::try_from(discovery_capacity + 1).unwrap(),
                queued: u64::try_from(discovery_capacity).unwrap(),
                full_dropped: 1,
                receiver_closed_dropped: 0,
            };
            assert_eq!(discovery_stats.snapshot(), expected_discovery_stats);
            for value in 0..discovery_capacity {
                assert_eq!(discovery_receiver.recv().await, Some(scaled_node(value)));
            }

            let root_capacity = downstream.root_triage_capacity.get();
            let (triage_input, downstream) = DhtCrawlerDownstreamComposition::with_config(
                discovery,
                &client,
                &table,
                &pool,
                Id20::from_slice(b"-BM0001-composition0").unwrap(),
                downstream,
            )
            .unwrap();
            for value in 0..root_capacity {
                triage_input
                    .send(scaled_triage_request(value))
                    .await
                    .unwrap();
            }
            let mut blocked = Box::pin(triage_input.send(scaled_triage_request(root_capacity)));
            std::future::poll_fn(|context| match blocked.as_mut().poll(context) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("configured full root route must apply backpressure"),
            })
            .await;
            drop(blocked);

            let (maintenance, maintenance_stats) = DhtCrawlerMaintenanceSupervisor::with_config(
                discovery_receiver,
                &client,
                &table,
                &triage_input,
                maintenance,
            )
            .unwrap();
            let (supervisor, handles) =
                DhtCrawlerPipelineSupervisor::new(runtime, maintenance, triage_input, downstream);
            let observability = supervisor.observability_handle();
            drop((client, table));

            let initial = observability.snapshot();
            assert_eq!(
                initial.lifecycle,
                DhtCrawlerPipelineObservedLifecycle::Starting
            );
            assert_eq!(initial.runtime.health.status(), DhtRuntimeHealthStatus::Up);
            assert_eq!(initial.runtime.inbound, DhtInboundStatsSnapshot::default());
            assert_eq!(initial.runtime.discovery, expected_discovery_stats);
            assert_eq!(
                initial.maintenance,
                DhtCrawlerPipelineMaintenanceObservabilitySnapshot {
                    scheduler: DhtDiscoveredNodeSchedulerStats::default(),
                    ping: DhtDiscoveredNodePingStats::default(),
                    find_node: DhtDiscoveredNodeFindStats::default(),
                    sample_infohashes_worker: DhtSampleInfoHashesWorkerStats::default(),
                    oldest_find: DhtOldestNodeFindProducerStats::default(),
                    oldest_ping: DhtOldestNodePingProducerStats::default(),
                    bootstrap_ping: DhtBootstrapPingProducerStats::default(),
                    sample_infohashes_producer: DhtSampleInfoHashesProducerStats::default(),
                }
            );
            assert_eq!(
                initial.downstream,
                DhtCrawlerPipelineDownstreamObservabilitySnapshot {
                    triage: DhtInfoHashTriageStats::default(),
                    get_peers: DhtGetPeersWorkerStats::default(),
                    request_meta_info: DhtRequestMetaInfoWorkerStats::default(),
                    persist_torrent: DhtPersistTorrentWorkerStats::default(),
                    scrape: DhtScrapeWorkerStats::default(),
                    persist_source: DhtPersistSourceWorkerStats::default(),
                }
            );
            assert!(!initial.is_ready());
            assert!(!observability.is_ready());
            assert!(DhtCrawlerPipelineObservabilitySnapshot {
                lifecycle: DhtCrawlerPipelineObservedLifecycle::Ready,
                ..initial.clone()
            }
            .is_ready());
            assert!(!DhtCrawlerPipelineObservabilitySnapshot {
                lifecycle: DhtCrawlerPipelineObservedLifecycle::Ready,
                runtime: DhtCrawlerPipelineRuntimeObservabilitySnapshot {
                    health: DhtRuntimeHealthSnapshot::default(),
                    ..initial.runtime.clone()
                },
                ..initial.clone()
            }
            .is_ready());

            assert_eq!(pool.size(), 0, "construction must not acquire PostgreSQL");
            let exit = timeout(
                Duration::from_secs(5),
                supervisor.run(std::future::ready(())),
            )
            .await
            .expect("pre-ready shutdown must remain bounded");
            assert!(matches!(
                exit,
                DhtCrawlerPipelineExit::ShutdownBeforeStart {
                    runtime: Ok(DhtRuntimeExit::Shutdown),
                    blocking: Ok(Ok(BlockingFinalizeOutcome::NothingPending)),
                }
            ));
            let stopped = observability.snapshot();
            assert_eq!(
                stopped.lifecycle,
                DhtCrawlerPipelineObservedLifecycle::Stopped
            );
            assert_eq!(
                stopped.runtime.health.status(),
                DhtRuntimeHealthStatus::Inactive
            );
            assert_eq!(stopped.runtime.discovery, expected_discovery_stats);
            assert!(!stopped.is_ready());

            assert_eq!(
                handles.downstream_stats.triage.snapshot(),
                DhtInfoHashTriageStats::default()
            );
            assert_eq!(
                handles.downstream_stats.get_peers.snapshot(),
                DhtGetPeersWorkerStats::default()
            );
            assert_eq!(
                handles.downstream_stats.request_meta_info.snapshot(),
                DhtRequestMetaInfoWorkerStats::default()
            );
            assert_eq!(
                handles.downstream_stats.persist_torrent.snapshot(),
                DhtPersistTorrentWorkerStats::default()
            );
            assert_eq!(
                handles.downstream_stats.scrape.snapshot(),
                DhtScrapeWorkerStats::default()
            );
            assert_eq!(
                handles.downstream_stats.persist_source.snapshot(),
                DhtPersistSourceWorkerStats::default()
            );
            assert_eq!(
                maintenance_stats.discovery.snapshot(),
                expected_discovery_stats
            );
            assert_eq!(
                handles.blocking_finalizer.finalize().await.unwrap(),
                BlockingFinalizeOutcome::NothingPending
            );
            assert_eq!(pool.size(), 0, "empty finalization must remain offline");
            assert!(!pool.is_closed(), "the caller retains pool ownership");

            let rebound = UdpSocket::bind(runtime_addr)
                .await
                .expect("pipeline runtime shutdown must release its UDP socket");
            drop(rebound);
            pool.close().await;
        }
    }
}
