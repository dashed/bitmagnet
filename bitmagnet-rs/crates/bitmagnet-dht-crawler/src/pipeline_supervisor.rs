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
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use bitmagnet_blocking::{BlockingError, BlockingFinalizeOutcome, BlockingFinalizer};
use bitmagnet_dht::{
    DhtCrawlerMaintenanceSupervisor, DhtCrawlerMaintenanceSupervisorExit, DhtInfoHashTriageInput,
    DhtRuntime, DhtRuntimeExit,
};
use tokio::sync::watch;
use tokio::task::{Id, JoinError, JoinSet};

use crate::{
    DhtCrawlerDownstreamComposition, DhtCrawlerDownstreamStatsHandle, DhtCrawlerDownstreamWorkers,
    DhtGetPeersWorkerExit, DhtInfoHashTriageWorkerExit, DhtPersistSourceWorkerExit,
    DhtPersistTorrentWorkerExit, DhtRequestMetaInfoWorkerExit, DhtScrapeWorkerExit,
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
}

impl DhtCrawlerPipelineSupervisor {
    /// Assemble staged lifecycle ownership around already-constructed parts.
    ///
    /// `root_triage_input` must be the original producer paired with the
    /// receiver consumed by `downstream`; maintenance already owns its own
    /// clone. The constructor cannot detect unrelated clones. Retaining another
    /// clone outside this supervisor can prevent the graceful EOF cascade.
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
        let handles = DhtCrawlerPipelineHandles {
            downstream_stats: stats,
            blocking_finalizer: finalizer.clone(),
        };
        (
            Self {
                runtime,
                maintenance,
                root_triage_input,
                downstream: workers,
                finalizer,
            },
            handles,
        )
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
        run_with_factories(shutdown, PipelineFactories::from_supervisor(self)).await
    }
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
type RuntimeFactory =
    Box<dyn FnOnce(watch::Receiver<bool>) -> BoxFuture<Result<DhtRuntimeExit, JoinError>> + Send>;
type MaintenanceFactory =
    Box<dyn FnOnce(watch::Receiver<bool>) -> BoxFuture<DhtCrawlerMaintenanceSupervisorExit> + Send>;
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
    fn from_supervisor(supervisor: DhtCrawlerPipelineSupervisor) -> Self {
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
        } = supervisor;
        Self {
            runtime: Box::new(move |stop| {
                Box::pin(runtime.run_until_shutdown(wait_for_stop(stop)))
            }),
            maintenance: Box::new(move |stop| Box::pin(maintenance.run(wait_for_stop(stop)))),
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
            persist_source: Box::new(move |stop| Box::pin(persist_source.run(wait_for_stop(stop)))),
            finalizer: Box::new(move || Box::pin(async move { finalizer.finalize().await })),
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

async fn run_with_factories<F>(shutdown: F, factories: PipelineFactories) -> DhtCrawlerPipelineExit
where
    F: Future<Output = ()>,
{
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
        return DhtCrawlerPipelineExit::ShutdownBeforeStart { runtime, blocking };
    }

    let (maintenance_stop, maintenance_stop_rx) = watch::channel(false);
    let (downstream_stop, downstream_stop_rx) = watch::channel(false);
    let (runtime_stop, runtime_stop_rx) = watch::channel(false);
    let mut runtime = runtime(runtime_stop_rx);
    let mut root_drop = Some(root_drop);

    let mut tasks = JoinSet::new();
    let mut task_kinds = HashMap::new();
    spawn_task(
        &mut tasks,
        &mut task_kinds,
        TaskKind::Maintenance,
        async move { TaskOutput::Maintenance(maintenance(maintenance_stop_rx).await) },
    );
    spawn_task(&mut tasks, &mut task_kinds, TaskKind::Triage, {
        let stop = downstream_stop_rx.clone();
        async move { TaskOutput::Triage(triage(stop).await) }
    });
    spawn_task(&mut tasks, &mut task_kinds, TaskKind::GetPeers, {
        let stop = downstream_stop_rx.clone();
        async move { TaskOutput::GetPeers(get_peers(stop).await) }
    });
    spawn_task(&mut tasks, &mut task_kinds, TaskKind::RequestMetaInfo, {
        let stop = downstream_stop_rx.clone();
        async move { TaskOutput::RequestMetaInfo(request_meta_info(stop).await) }
    });
    spawn_task(&mut tasks, &mut task_kinds, TaskKind::PersistTorrent, {
        let stop = downstream_stop_rx.clone();
        async move { TaskOutput::PersistTorrent(persist_torrent(stop).await) }
    });
    spawn_task(&mut tasks, &mut task_kinds, TaskKind::Scrape, {
        let stop = downstream_stop_rx.clone();
        async move { TaskOutput::Scrape(scrape(stop).await) }
    });
    spawn_task(
        &mut tasks,
        &mut task_kinds,
        TaskKind::PersistSource,
        async move { TaskOutput::PersistSource(persist_source(downstream_stop_rx).await) },
    );

    let mut collector = TaskCollector::default();
    let mut runtime_result = None;
    let first_trigger = tokio::select! {
        biased;
        () = &mut shutdown => DhtCrawlerPipelineTrigger::ExternalShutdown,
        result = runtime.as_mut() => {
            runtime_result = Some(result);
            DhtCrawlerPipelineTrigger::Runtime
        }
        joined = join_next_task(&mut tasks, &mut task_kinds) => {
            let (kind, result) = joined;
            let child = kind.downstream();
            let _abnormal = collector.record(kind, result);
            match child {
                Some(child) => DhtCrawlerPipelineTrigger::Downstream(child),
                None => DhtCrawlerPipelineTrigger::Maintenance,
            }
        }
    };

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

    DhtCrawlerPipelineExit::Completed(Box::new(DhtCrawlerPipelineCompletedExit {
        first_trigger,
        maintenance,
        downstream,
        runtime,
        blocking,
    }))
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fmt;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex, MutexGuard};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use bitmagnet_dht::{
        dht_info_hash_triage_channel, DhtCrawlerMaintenanceSupervisor, DhtRuntimeConfig, Id20,
        DHT_INFO_HASH_TRIAGE_DEFAULT_CAPACITY,
    };
    use sqlx::postgres::PgPoolOptions;
    use tokio::net::UdpSocket;
    use tokio::sync::{oneshot, Notify};
    use tokio::time::timeout;

    use super::*;
    use crate::{
        DhtGetPeersWorkerStats, DhtInfoHashTriageStats, DhtPersistSourceWorkerStats,
        DhtPersistTorrentWorkerStats, DhtRequestMetaInfoWorkerStats, DhtScrapeWorkerStats,
    };

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
                Box::pin(async move {
                    maintenance_gate.mark(&maintenance_log, "maintenance_started");
                    wait_for_stop(stop).await;
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

        let exit = completed(run_bounded(gate.wait_for(8), factories).await);

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
            Box::pin(async { DhtCrawlerMaintenanceSupervisorExit::ShutdownBeforeStart })
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

        let exit = completed(run_bounded(gate.wait_for(8), factories).await);

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
            maintenance: pending_factory!(
                DhtCrawlerMaintenanceSupervisorExit,
                "maintenance_started",
                "maintenance_dropped"
            ),
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
            maintenance: unused_factory!(DhtCrawlerMaintenanceSupervisorExit),
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

    #[test]
    fn public_supervisor_and_recovery_handles_have_owned_task_traits() {
        assert_send::<DhtCrawlerPipelineSupervisor>();
        assert_send_sync::<DhtCrawlerPipelineHandles>();
        assert_send::<DhtCrawlerPipelineExit>();
    }

    #[tokio::test]
    async fn concrete_pre_ready_smoke_is_offline_and_releases_runtime() {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        assert_eq!(pool.size(), 0);
        assert!(!pool.is_closed());

        let mut runtime = DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            ..DhtRuntimeConfig::default()
        })
        .await
        .unwrap();
        let runtime_addr = runtime.local_addr();
        let client = runtime.client();
        let table = runtime.table().clone();
        let discovery_receiver = runtime
            .take_discovered_nodes()
            .expect("the runtime exposes its discovery receiver once");
        let discovery = discovery_receiver
            .try_sender()
            .expect("the live runtime retains its discovery producer");
        let (triage_input, triage_receiver) = dht_info_hash_triage_channel(
            NonZeroUsize::new(DHT_INFO_HASH_TRIAGE_DEFAULT_CAPACITY).unwrap(),
        );
        let (maintenance, maintenance_stats) = DhtCrawlerMaintenanceSupervisor::new(
            discovery_receiver,
            &client,
            &table,
            &triage_input,
        )
        .unwrap();
        let downstream = DhtCrawlerDownstreamComposition::new(
            triage_receiver,
            discovery,
            &client,
            &table,
            &pool,
            Id20::from_slice(b"-BM0001-composition0").unwrap(),
        );
        let (supervisor, handles) =
            DhtCrawlerPipelineSupervisor::new(runtime, maintenance, triage_input, downstream);
        drop((client, table));

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
            bitmagnet_dht::DhtDiscoveryStats::default()
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
