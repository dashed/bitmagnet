use std::any::Any;
use std::error::Error;
use std::fmt;
use std::future::{poll_fn, Future};
use std::panic::resume_unwind;
use std::pin::Pin;
use std::task::Poll;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinError, JoinSet};

use crate::{
    DhtBootstrapPingProducer, DhtBootstrapPingProducerExit, DhtBootstrapPingProducerStatsHandle,
    DhtCrawlerTargetError, DhtCrawlerTargetRotator, DhtDiscoveredNodeFindStatsHandle,
    DhtDiscoveredNodeFindWorker, DhtDiscoveredNodeFindWorkerExit, DhtDiscoveredNodePingInput,
    DhtDiscoveredNodePingStatsHandle, DhtDiscoveredNodePingWorker, DhtDiscoveredNodePingWorkerExit,
    DhtDiscoveredNodeScheduler, DhtDiscoveredNodeSchedulerExit,
    DhtDiscoveredNodeSchedulerStatsHandle, DhtDiscoveryReceiver, DhtDiscoveryStatsHandle,
    DhtInfoHashTriageInput, DhtOldestNodeFindProducer, DhtOldestNodeFindProducerExit,
    DhtOldestNodeFindProducerStatsHandle, DhtOldestNodePingProducer, DhtOldestNodePingProducerExit,
    DhtOldestNodePingProducerStatsHandle, DhtRuntimeClient, DhtSampleInfoHashesProducer,
    DhtSampleInfoHashesProducerExit, DhtSampleInfoHashesProducerStatsHandle,
    DhtSampleInfoHashesWorker, DhtSampleInfoHashesWorkerExit, DhtSampleInfoHashesWorkerStatsHandle,
    KTable,
};

/// Failure to construct the partial crawler maintenance composition.
///
/// Both variants retain the uniquely owned discovery receiver so callers do
/// not lose the ingress capability when construction fails.
pub enum DhtCrawlerMaintenanceStartError {
    /// Every strong sender was already gone, so recursive discovery could not
    /// recover a producer for this exact channel.
    DiscoveryClosed(DhtDiscoveryReceiver),
    /// Initial target entropy failed before any maintenance component started.
    TargetEntropy {
        discovery: DhtDiscoveryReceiver,
        source: DhtCrawlerTargetError,
    },
}

impl DhtCrawlerMaintenanceStartError {
    /// Recover the discovery receiver supplied to the failed constructor.
    #[must_use]
    pub fn into_discovery(self) -> DhtDiscoveryReceiver {
        match self {
            Self::DiscoveryClosed(discovery) | Self::TargetEntropy { discovery, .. } => discovery,
        }
    }
}

impl fmt::Debug for DhtCrawlerMaintenanceStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DiscoveryClosed(_) => formatter.write_str("DiscoveryClosed"),
            Self::TargetEntropy { source, .. } => formatter
                .debug_struct("TargetEntropy")
                .field("source", source)
                .finish_non_exhaustive(),
        }
    }
}

impl fmt::Display for DhtCrawlerMaintenanceStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DiscoveryClosed(_) => {
                formatter.write_str("the DHT discovery channel is already closed")
            }
            Self::TargetEntropy { source, .. } => {
                write!(formatter, "could not create the crawler target: {source}")
            }
        }
    }
}

impl Error for DhtCrawlerMaintenanceStartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::DiscoveryClosed(_) => None,
            Self::TargetEntropy { source, .. } => Some(source),
        }
    }
}

/// Cloneable sender-free bundle of every counter surface owned or shared by
/// the partial crawler maintenance composition.
#[derive(Clone)]
pub struct DhtCrawlerMaintenanceStatsHandle {
    /// Exact channel-global discovery counters shared with the runtime
    /// responder and every recursive-discovery sender clone.
    pub discovery: DhtDiscoveryStatsHandle,
    /// Scheduler-only counters. In particular, `routed_ping` excludes direct
    /// commits by the oldest-node and bootstrap producers, while
    /// `routed_find_node` excludes direct commits by the oldest-node find
    /// producer and `routed_sample_infohashes` excludes direct retained-handle
    /// commits by the periodic sample producer.
    pub scheduler: DhtDiscoveredNodeSchedulerStatsHandle,
    /// Ping-worker counters aggregated across scheduler, oldest-node, and
    /// bootstrap-producer nodes after any source commits to the shared route.
    pub ping: DhtDiscoveredNodePingStatsHandle,
    /// Find-worker counters aggregated across scheduler and oldest-producer
    /// nodes after either source commits to the shared route.
    pub find_node: DhtDiscoveredNodeFindStatsHandle,
    /// Sample-worker counters aggregated across scheduler-origin discovered
    /// snapshots and direct retained handles from the periodic producer.
    pub sample_infohashes_worker: DhtSampleInfoHashesWorkerStatsHandle,
    /// Counters local to the periodic oldest-node find producer.
    pub oldest_find: DhtOldestNodeFindProducerStatsHandle,
    /// Counters local to the periodic oldest-node ping producer.
    pub oldest_ping: DhtOldestNodePingProducerStatsHandle,
    /// Counters local to the periodic bootstrap-node ping producer.
    pub bootstrap_ping: DhtBootstrapPingProducerStatsHandle,
    /// Counters local to the periodic retained sample-candidate producer.
    pub sample_infohashes_producer: DhtSampleInfoHashesProducerStatsHandle,
}

/// Stable identity of one owned maintenance child.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtCrawlerMaintenanceChild {
    Scheduler,
    Ping,
    FindNode,
    SampleInfoHashesWorker,
    OldestFind,
    OldestPing,
    BootstrapPing,
    SampleInfoHashesProducer,
    Target,
}

/// One complete, fixed-shape terminal record for all nine children.
///
/// An outer shutdown remains the primary supervisor cause even if a child had
/// already completed before the shared shutdown signal. The concrete child
/// result is retained here rather than coerced into fabricated shutdown counts.
#[derive(Debug)]
pub struct DhtCrawlerMaintenanceChildExits {
    pub scheduler: DhtDiscoveredNodeSchedulerExit,
    pub ping: DhtDiscoveredNodePingWorkerExit,
    pub find_node: DhtDiscoveredNodeFindWorkerExit,
    pub sample_infohashes_worker: DhtSampleInfoHashesWorkerExit,
    pub oldest_find: DhtOldestNodeFindProducerExit,
    pub oldest_ping: DhtOldestNodePingProducerExit,
    pub bootstrap_ping: DhtBootstrapPingProducerExit,
    pub sample_infohashes_producer: DhtSampleInfoHashesProducerExit,
    pub target: Result<(), DhtCrawlerTargetError>,
}

/// Terminal result of the partial crawler maintenance composition.
#[derive(Debug)]
pub enum DhtCrawlerMaintenanceSupervisorExit {
    /// Shutdown was ready at the preflight boundary, so no child factory was
    /// invoked and no child task was spawned or polled.
    ShutdownBeforeStart,
    /// External shutdown won the biased outer selection. Every child was then
    /// fully joined and its concrete result is retained.
    Shutdown {
        children: DhtCrawlerMaintenanceChildExits,
    },
    /// The named child was the first terminal result observed before external
    /// shutdown. Siblings were signalled and fully joined.
    Failed {
        first: DhtCrawlerMaintenanceChild,
        children: DhtCrawlerMaintenanceChildExits,
    },
}

/// One sender-free, one-shot maintenance lifecycle notification.
pub struct DhtCrawlerMaintenanceNotification {
    receiver: oneshot::Receiver<()>,
}

impl DhtCrawlerMaintenanceNotification {
    /// Wait for the maintenance supervisor to publish this notification.
    ///
    /// A receive error means its run future ended or was cancelled without
    /// reaching the corresponding lifecycle boundary.
    pub async fn notified(&mut self) -> Result<(), oneshot::error::RecvError> {
        (&mut self.receiver).await
    }
}

/// Sender-free one-shot observations for one maintenance supervisor run.
pub struct DhtCrawlerMaintenanceRunNotifications {
    started: DhtCrawlerMaintenanceNotification,
    stopping: DhtCrawlerMaintenanceNotification,
}

impl DhtCrawlerMaintenanceRunNotifications {
    /// Split the uniquely owned start and stopping notifications.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        DhtCrawlerMaintenanceNotification,
        DhtCrawlerMaintenanceNotification,
    ) {
        (self.started, self.stopping)
    }
}

struct DhtCrawlerMaintenanceRunPublishers {
    started: Option<oneshot::Sender<()>>,
    stopping: Option<oneshot::Sender<()>>,
}

impl DhtCrawlerMaintenanceRunPublishers {
    fn channel() -> (Self, DhtCrawlerMaintenanceRunNotifications) {
        let (started, started_receiver) = oneshot::channel();
        let (stopping, stopping_receiver) = oneshot::channel();
        (
            Self {
                started: Some(started),
                stopping: Some(stopping),
            },
            DhtCrawlerMaintenanceRunNotifications {
                started: DhtCrawlerMaintenanceNotification {
                    receiver: started_receiver,
                },
                stopping: DhtCrawlerMaintenanceNotification {
                    receiver: stopping_receiver,
                },
            },
        )
    }

    fn publish_started(&mut self) {
        if let Some(started) = self.started.take() {
            let _ = started.send(());
        }
    }

    fn publish_stopping(&mut self) {
        if let Some(stopping) = self.stopping.take() {
            let _ = stopping.send(());
        }
    }
}

/// Owned partial DHT crawler maintenance composition.
///
/// This slice owns the discovered-node scheduler, ping, `find_node`, and
/// `sample_infohashes` workers, the oldest-node `find_node` and ping producers,
/// the bootstrap-node ping producer, the periodic sample-candidate producer,
/// and the target rotator. It does not own the info-hash triage receiver or
/// observe [`crate::DhtRuntime`]: its cloned weak client does not keep the UDP
/// runtime alive, and an outer owner must propagate runtime termination
/// through [`Self::run`]'s shutdown future.
///
/// The find and sample workers jointly keep scheduler ingress from reaching
/// EOF. The oldest-node producer keeps the shared find route open; the
/// oldest-node and bootstrap producers jointly keep the shared ping route
/// open; and the scheduler plus periodic sample producer jointly keep the
/// shared sample route open. Explicit shutdown or a child failure breaks all
/// four route-level cycles. Dropping this value or its run future aborts every
/// component-owned child task. A Tokio blocking DNS lookup already dispatched
/// by the bootstrap child may still finish internally after that child exits.
pub struct DhtCrawlerMaintenanceSupervisor {
    scheduler: DhtDiscoveredNodeScheduler,
    ping: DhtDiscoveredNodePingWorker,
    find_node: DhtDiscoveredNodeFindWorker,
    sample_infohashes_worker: DhtSampleInfoHashesWorker,
    oldest_find: DhtOldestNodeFindProducer,
    oldest_ping: DhtOldestNodePingProducer,
    bootstrap_ping: DhtBootstrapPingProducer,
    sample_infohashes_producer: DhtSampleInfoHashesProducer,
    target_rotator: DhtCrawlerTargetRotator,
}

impl DhtCrawlerMaintenanceSupervisor {
    /// Construct and wire the fixed partial maintenance composition.
    ///
    /// The client, table, and triage input are cloned; the discovery receiver
    /// is consumed on success and returned intact through either start-error
    /// variant. The unique triage receiver remains externally owned. Initial
    /// target entropy is obtained here, before [`Self::run`]'s shutdown
    /// preflight.
    pub fn new(
        discovery: DhtDiscoveryReceiver,
        client: &DhtRuntimeClient,
        table: &KTable,
        triage: &DhtInfoHashTriageInput,
    ) -> Result<(Self, DhtCrawlerMaintenanceStatsHandle), DhtCrawlerMaintenanceStartError> {
        Self::new_with_factories(
            discovery,
            client,
            table,
            triage,
            DhtCrawlerTargetRotator::new,
            DhtBootstrapPingProducer::new,
        )
    }

    #[cfg(test)]
    fn new_with_target_factory<T>(
        discovery: DhtDiscoveryReceiver,
        client: &DhtRuntimeClient,
        table: &KTable,
        triage: &DhtInfoHashTriageInput,
        target_factory: T,
    ) -> Result<(Self, DhtCrawlerMaintenanceStatsHandle), DhtCrawlerMaintenanceStartError>
    where
        T: FnOnce() -> Result<
            (crate::DhtCrawlerTarget, DhtCrawlerTargetRotator),
            DhtCrawlerTargetError,
        >,
    {
        Self::new_with_factories(
            discovery,
            client,
            table,
            triage,
            target_factory,
            DhtBootstrapPingProducer::new,
        )
    }

    fn new_with_factories<T, B>(
        discovery: DhtDiscoveryReceiver,
        client: &DhtRuntimeClient,
        table: &KTable,
        triage: &DhtInfoHashTriageInput,
        target_factory: T,
        bootstrap_factory: B,
    ) -> Result<(Self, DhtCrawlerMaintenanceStatsHandle), DhtCrawlerMaintenanceStartError>
    where
        T: FnOnce() -> Result<
            (crate::DhtCrawlerTarget, DhtCrawlerTargetRotator),
            DhtCrawlerTargetError,
        >,
        B: FnOnce(
            DhtDiscoveredNodePingInput,
        ) -> (
            DhtBootstrapPingProducer,
            DhtBootstrapPingProducerStatsHandle,
        ),
    {
        let recursive_discovery = match discovery.try_sender() {
            Some(sender) => sender,
            None => return Err(DhtCrawlerMaintenanceStartError::DiscoveryClosed(discovery)),
        };
        let discovery_stats = recursive_discovery.stats_handle();
        let (target, target_rotator) = match target_factory() {
            Ok(pair) => pair,
            Err(source) => {
                return Err(DhtCrawlerMaintenanceStartError::TargetEntropy { discovery, source });
            }
        };

        let (scheduler, routes, scheduler_stats) =
            DhtDiscoveredNodeScheduler::new(discovery, table.clone());
        let oldest_ping_input = scheduler.ping_input();
        let bootstrap_ping_input = scheduler.ping_input();
        let oldest_find_input = scheduler.find_node_input();
        let sample_infohashes_input = scheduler.sample_infohashes_input();
        let crate::DhtDiscoveredNodeRoutes {
            ping,
            find_node,
            sample_infohashes,
        } = routes;

        let (ping, ping_stats) =
            DhtDiscoveredNodePingWorker::new(ping, client.clone(), table.clone());
        let (find_node, find_node_stats) = DhtDiscoveredNodeFindWorker::new(
            find_node,
            client.clone(),
            table.clone(),
            recursive_discovery.clone(),
            target.clone(),
        );
        let (sample_infohashes_worker, sample_infohashes_worker_stats) =
            DhtSampleInfoHashesWorker::new(
                sample_infohashes,
                client.clone(),
                table.clone(),
                triage.clone(),
                recursive_discovery,
                target,
            );
        let (oldest_find, oldest_find_stats) =
            DhtOldestNodeFindProducer::new(table.clone(), oldest_find_input);
        let (oldest_ping, oldest_ping_stats) =
            DhtOldestNodePingProducer::new(table.clone(), oldest_ping_input);
        let (bootstrap_ping, bootstrap_ping_stats) = bootstrap_factory(bootstrap_ping_input);
        let (sample_infohashes_producer, sample_infohashes_producer_stats) =
            DhtSampleInfoHashesProducer::new(table.clone(), sample_infohashes_input);

        Ok((
            Self {
                scheduler,
                ping,
                find_node,
                sample_infohashes_worker,
                oldest_find,
                oldest_ping,
                bootstrap_ping,
                sample_infohashes_producer,
                target_rotator,
            },
            DhtCrawlerMaintenanceStatsHandle {
                discovery: discovery_stats,
                scheduler: scheduler_stats,
                ping: ping_stats,
                find_node: find_node_stats,
                sample_infohashes_worker: sample_infohashes_worker_stats,
                oldest_find: oldest_find_stats,
                oldest_ping: oldest_ping_stats,
                bootstrap_ping: bootstrap_ping_stats,
                sample_infohashes_producer: sample_infohashes_producer_stats,
            },
        ))
    }

    /// Run until explicit shutdown or the first child terminal result.
    ///
    /// A pre-ready shutdown returns before any child factory is invoked or any
    /// child task is spawned. Once started, all nine children receive one
    /// shared shutdown broadcast and are fully joined. A child panic is resumed
    /// with its exact payload after sibling cleanup; an unexpected task
    /// cancellation likewise cleans siblings and then panics.
    pub async fn run<F>(self, shutdown: F) -> DhtCrawlerMaintenanceSupervisorExit
    where
        F: Future<Output = ()>,
    {
        self.run_inner(shutdown, None).await
    }

    /// Build a run future with sender-free lifecycle notifications.
    ///
    /// `started` is published only after all nine uniquely identified child
    /// futures have been polled and neither shutdown nor a child terminal
    /// result is observable at that boundary. `stopping` is published
    /// immediately after the first shutdown or child terminal trigger is
    /// selected and before the shared stop signal or any child join cleanup.
    /// Cancellation closes any unpublished notification without fabricating it.
    pub fn run_with_notifications<F>(
        self,
        shutdown: F,
    ) -> (
        DhtCrawlerMaintenanceRunNotifications,
        impl Future<Output = DhtCrawlerMaintenanceSupervisorExit>,
    )
    where
        F: Future<Output = ()>,
    {
        let (publishers, notifications) = DhtCrawlerMaintenanceRunPublishers::channel();
        (notifications, self.run_inner(shutdown, Some(publishers)))
    }

    async fn run_inner<F>(
        self,
        shutdown: F,
        notifications: Option<DhtCrawlerMaintenanceRunPublishers>,
    ) -> DhtCrawlerMaintenanceSupervisorExit
    where
        F: Future<Output = ()>,
    {
        let Self {
            scheduler,
            ping,
            find_node,
            sample_infohashes_worker,
            oldest_find,
            oldest_ping,
            bootstrap_ping,
            sample_infohashes_producer,
            target_rotator,
        } = self;
        run_child_factories_with_notifications(
            shutdown,
            [
                Box::new(move |stop| {
                    Box::pin(async move {
                        ChildExit::Scheduler(scheduler.run(wait_for_shutdown(stop)).await)
                    })
                }),
                Box::new(move |stop| {
                    Box::pin(
                        async move { ChildExit::Ping(ping.run(wait_for_shutdown(stop)).await) },
                    )
                }),
                Box::new(move |stop| {
                    Box::pin(async move {
                        ChildExit::FindNode(find_node.run(wait_for_shutdown(stop)).await)
                    })
                }),
                Box::new(move |stop| {
                    Box::pin(async move {
                        ChildExit::SampleInfoHashesWorker(
                            sample_infohashes_worker.run(wait_for_shutdown(stop)).await,
                        )
                    })
                }),
                Box::new(move |stop| {
                    Box::pin(async move {
                        ChildExit::OldestFind(oldest_find.run(wait_for_shutdown(stop)).await)
                    })
                }),
                Box::new(move |stop| {
                    Box::pin(async move {
                        ChildExit::OldestPing(oldest_ping.run(wait_for_shutdown(stop)).await)
                    })
                }),
                Box::new(move |stop| {
                    Box::pin(async move {
                        ChildExit::BootstrapPing(bootstrap_ping.run(wait_for_shutdown(stop)).await)
                    })
                }),
                Box::new(move |stop| {
                    Box::pin(async move {
                        ChildExit::SampleInfoHashesProducer(
                            sample_infohashes_producer
                                .run(wait_for_shutdown(stop))
                                .await,
                        )
                    })
                }),
                Box::new(move |stop| {
                    Box::pin(async move {
                        ChildExit::Target(target_rotator.run(wait_for_shutdown(stop)).await)
                    })
                }),
            ],
            notifications,
        )
        .await
    }
}

type ChildFuture = Pin<Box<dyn Future<Output = ChildExit> + Send + 'static>>;
type ChildFactory = Box<dyn FnOnce(watch::Receiver<bool>) -> ChildFuture + Send + 'static>;

enum ChildExit {
    Scheduler(DhtDiscoveredNodeSchedulerExit),
    Ping(DhtDiscoveredNodePingWorkerExit),
    FindNode(DhtDiscoveredNodeFindWorkerExit),
    SampleInfoHashesWorker(DhtSampleInfoHashesWorkerExit),
    OldestFind(DhtOldestNodeFindProducerExit),
    OldestPing(DhtOldestNodePingProducerExit),
    BootstrapPing(DhtBootstrapPingProducerExit),
    SampleInfoHashesProducer(DhtSampleInfoHashesProducerExit),
    Target(Result<(), DhtCrawlerTargetError>),
}

#[cfg(test)]
async fn run_child_factories<F>(
    shutdown: F,
    factories: [ChildFactory; 9],
) -> DhtCrawlerMaintenanceSupervisorExit
where
    F: Future<Output = ()>,
{
    run_child_factories_with_notifications(shutdown, factories, None).await
}

const MAINTENANCE_CHILDREN: [DhtCrawlerMaintenanceChild; 9] = [
    DhtCrawlerMaintenanceChild::Scheduler,
    DhtCrawlerMaintenanceChild::Ping,
    DhtCrawlerMaintenanceChild::FindNode,
    DhtCrawlerMaintenanceChild::SampleInfoHashesWorker,
    DhtCrawlerMaintenanceChild::OldestFind,
    DhtCrawlerMaintenanceChild::OldestPing,
    DhtCrawlerMaintenanceChild::BootstrapPing,
    DhtCrawlerMaintenanceChild::SampleInfoHashesProducer,
    DhtCrawlerMaintenanceChild::Target,
];

#[derive(Default)]
struct ChildStartCollector {
    scheduler: bool,
    ping: bool,
    find_node: bool,
    sample_infohashes_worker: bool,
    oldest_find: bool,
    oldest_ping: bool,
    bootstrap_ping: bool,
    sample_infohashes_producer: bool,
    target: bool,
}

impl ChildStartCollector {
    fn record(&mut self, child: DhtCrawlerMaintenanceChild) -> bool {
        let slot = match child {
            DhtCrawlerMaintenanceChild::Scheduler => &mut self.scheduler,
            DhtCrawlerMaintenanceChild::Ping => &mut self.ping,
            DhtCrawlerMaintenanceChild::FindNode => &mut self.find_node,
            DhtCrawlerMaintenanceChild::SampleInfoHashesWorker => {
                &mut self.sample_infohashes_worker
            }
            DhtCrawlerMaintenanceChild::OldestFind => &mut self.oldest_find,
            DhtCrawlerMaintenanceChild::OldestPing => &mut self.oldest_ping,
            DhtCrawlerMaintenanceChild::BootstrapPing => &mut self.bootstrap_ping,
            DhtCrawlerMaintenanceChild::SampleInfoHashesProducer => {
                &mut self.sample_infohashes_producer
            }
            DhtCrawlerMaintenanceChild::Target => &mut self.target,
        };
        if *slot {
            return false;
        }
        *slot = true;
        self.complete()
    }

    fn complete(&self) -> bool {
        self.scheduler
            && self.ping
            && self.find_node
            && self.sample_infohashes_worker
            && self.oldest_find
            && self.oldest_ping
            && self.bootstrap_ping
            && self.sample_infohashes_producer
            && self.target
    }
}

async fn run_child_factories_with_notifications<F>(
    shutdown: F,
    factories: [ChildFactory; 9],
    mut notifications: Option<DhtCrawlerMaintenanceRunPublishers>,
) -> DhtCrawlerMaintenanceSupervisorExit
where
    F: Future<Output = ()>,
{
    tokio::pin!(shutdown);
    let pre_ready =
        poll_fn(|context| Poll::Ready(matches!(shutdown.as_mut().poll(context), Poll::Ready(()))))
            .await;
    if pre_ready {
        if let Some(notifications) = &mut notifications {
            notifications.publish_stopping();
        }
        drop(factories);
        return DhtCrawlerMaintenanceSupervisorExit::ShutdownBeforeStart;
    }

    let (stop_tx, stop_rx) = watch::channel(false);
    let (child_started_tx, mut child_started_rx) = mpsc::unbounded_channel();
    let mut tasks = JoinSet::new();
    for (child, factory) in MAINTENANCE_CHILDREN.into_iter().zip(factories) {
        let future = factory(stop_rx.clone());
        let child_started_tx = child_started_tx.clone();
        tasks.spawn(async move {
            let _ = child_started_tx.send(child);
            future.await
        });
    }
    drop(child_started_tx);
    drop(stop_rx);

    let mut collector = ChildCollector::default();
    let mut starts = ChildStartCollector::default();
    let mut first_child = None;
    enum First {
        Shutdown,
        Child(Result<ChildExit, JoinError>),
        Started(DhtCrawlerMaintenanceChild),
    }
    let first = loop {
        let first = tokio::select! {
            biased;
            () = &mut shutdown => First::Shutdown,
            child = tasks.join_next() => {
                First::Child(child.expect("nine maintenance children were spawned"))
            }
            child = child_started_rx.recv(), if !starts.complete() => {
                First::Started(child.expect("an unreported maintenance child remains"))
            }
        };
        match first {
            First::Started(child) => {
                if starts.record(child) {
                    if let Some(notifications) = &mut notifications {
                        notifications.publish_started();
                    }
                }
            }
            trigger => break trigger,
        }
    };
    if let Some(notifications) = &mut notifications {
        notifications.publish_stopping();
    }
    if let First::Child(child) = first {
        first_child = collector.record(child);
    }
    let _ = stop_tx.send(true);

    while let Some(child) = tasks.join_next().await {
        let _ = collector.record(child);
    }
    let children = collector.finish();
    match first_child {
        Some(first) => DhtCrawlerMaintenanceSupervisorExit::Failed { first, children },
        None => DhtCrawlerMaintenanceSupervisorExit::Shutdown { children },
    }
}

#[derive(Default)]
struct ChildCollector {
    first_panic: Option<Box<dyn Any + Send + 'static>>,
    unexpected_cancellation: bool,
    unexpected_join_error: Option<String>,
    duplicate_child: Option<DhtCrawlerMaintenanceChild>,
    scheduler: Option<DhtDiscoveredNodeSchedulerExit>,
    ping: Option<DhtDiscoveredNodePingWorkerExit>,
    find_node: Option<DhtDiscoveredNodeFindWorkerExit>,
    sample_infohashes_worker: Option<DhtSampleInfoHashesWorkerExit>,
    oldest_find: Option<DhtOldestNodeFindProducerExit>,
    oldest_ping: Option<DhtOldestNodePingProducerExit>,
    bootstrap_ping: Option<DhtBootstrapPingProducerExit>,
    sample_infohashes_producer: Option<DhtSampleInfoHashesProducerExit>,
    target: Option<Result<(), DhtCrawlerTargetError>>,
}

impl ChildCollector {
    fn record(
        &mut self,
        child: Result<ChildExit, JoinError>,
    ) -> Option<DhtCrawlerMaintenanceChild> {
        match child {
            Ok(exit) => Some(self.record_exit(exit)),
            Err(error) if error.is_panic() => {
                if self.first_panic.is_none() {
                    self.first_panic = Some(error.into_panic());
                }
                None
            }
            Err(error) if error.is_cancelled() => {
                self.unexpected_cancellation = true;
                None
            }
            Err(error) => {
                if self.unexpected_join_error.is_none() {
                    self.unexpected_join_error = Some(error.to_string());
                }
                None
            }
        }
    }

    fn record_exit(&mut self, exit: ChildExit) -> DhtCrawlerMaintenanceChild {
        match exit {
            ChildExit::Scheduler(exit) => {
                if self.scheduler.replace(exit).is_some() {
                    self.duplicate_child = Some(DhtCrawlerMaintenanceChild::Scheduler);
                }
                DhtCrawlerMaintenanceChild::Scheduler
            }
            ChildExit::Ping(exit) => {
                if self.ping.replace(exit).is_some() {
                    self.duplicate_child = Some(DhtCrawlerMaintenanceChild::Ping);
                }
                DhtCrawlerMaintenanceChild::Ping
            }
            ChildExit::FindNode(exit) => {
                if self.find_node.replace(exit).is_some() {
                    self.duplicate_child = Some(DhtCrawlerMaintenanceChild::FindNode);
                }
                DhtCrawlerMaintenanceChild::FindNode
            }
            ChildExit::SampleInfoHashesWorker(exit) => {
                if self.sample_infohashes_worker.replace(exit).is_some() {
                    self.duplicate_child = Some(DhtCrawlerMaintenanceChild::SampleInfoHashesWorker);
                }
                DhtCrawlerMaintenanceChild::SampleInfoHashesWorker
            }
            ChildExit::OldestFind(exit) => {
                if self.oldest_find.replace(exit).is_some() {
                    self.duplicate_child = Some(DhtCrawlerMaintenanceChild::OldestFind);
                }
                DhtCrawlerMaintenanceChild::OldestFind
            }
            ChildExit::OldestPing(exit) => {
                if self.oldest_ping.replace(exit).is_some() {
                    self.duplicate_child = Some(DhtCrawlerMaintenanceChild::OldestPing);
                }
                DhtCrawlerMaintenanceChild::OldestPing
            }
            ChildExit::BootstrapPing(exit) => {
                if self.bootstrap_ping.replace(exit).is_some() {
                    self.duplicate_child = Some(DhtCrawlerMaintenanceChild::BootstrapPing);
                }
                DhtCrawlerMaintenanceChild::BootstrapPing
            }
            ChildExit::SampleInfoHashesProducer(exit) => {
                if self.sample_infohashes_producer.replace(exit).is_some() {
                    self.duplicate_child =
                        Some(DhtCrawlerMaintenanceChild::SampleInfoHashesProducer);
                }
                DhtCrawlerMaintenanceChild::SampleInfoHashesProducer
            }
            ChildExit::Target(exit) => {
                if self.target.replace(exit).is_some() {
                    self.duplicate_child = Some(DhtCrawlerMaintenanceChild::Target);
                }
                DhtCrawlerMaintenanceChild::Target
            }
        }
    }

    fn finish(self) -> DhtCrawlerMaintenanceChildExits {
        if let Some(payload) = self.first_panic {
            resume_unwind(payload);
        }
        assert!(
            !self.unexpected_cancellation,
            "maintenance child was cancelled outside supervisor drop"
        );
        assert!(
            self.unexpected_join_error.is_none(),
            "unexpected maintenance child join error: {}",
            self.unexpected_join_error.as_deref().unwrap_or_default()
        );
        assert!(
            self.duplicate_child.is_none(),
            "duplicate maintenance child exit: {:?}",
            self.duplicate_child
        );
        DhtCrawlerMaintenanceChildExits {
            scheduler: self.scheduler.expect("scheduler child exit is present"),
            ping: self.ping.expect("ping child exit is present"),
            find_node: self.find_node.expect("find-node child exit is present"),
            sample_infohashes_worker: self
                .sample_infohashes_worker
                .expect("sample-infohashes worker child exit is present"),
            oldest_find: self.oldest_find.expect("oldest-find child exit is present"),
            oldest_ping: self.oldest_ping.expect("oldest-ping child exit is present"),
            bootstrap_ping: self
                .bootstrap_ping
                .expect("bootstrap-ping child exit is present"),
            sample_infohashes_producer: self
                .sample_infohashes_producer
                .expect("sample-infohashes producer child exit is present"),
            target: self.target.expect("target child exit is present"),
        }
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{pending, ready};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::num::NonZeroUsize;
    use std::panic::panic_any;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;
    use crate::{
        dht_discovery_channel, dht_info_hash_triage_channel, DhtBootstrapPingProducerStats,
        DhtDiscoveredNodeFindStats, DhtDiscoveredNodePingStats, DhtDiscoveredNodeSchedulerStats,
        DhtDiscoveryOffer, DhtDiscoveryStats, DhtInfoHashTriageReceiver,
        DhtOldestNodeFindProducerStats, DhtOldestNodePingProducerStats, DhtRuntime,
        DhtRuntimeConfig, DhtSampleInfoHashesProducerStats, DhtSampleInfoHashesWorkerStats, Id20,
        RoutingNode, RoutingPutResult,
    };

    fn child_factory<F, Fut>(factory: F) -> ChildFactory
    where
        F: FnOnce(watch::Receiver<bool>) -> Fut + Send + 'static,
        Fut: Future<Output = ChildExit> + Send + 'static,
    {
        Box::new(move |shutdown| Box::pin(factory(shutdown)))
    }

    fn cooperative_factories(stopped: Arc<AtomicUsize>) -> [ChildFactory; 9] {
        let scheduler_stopped = stopped.clone();
        let ping_stopped = stopped.clone();
        let find_stopped = stopped.clone();
        let sample_worker_stopped = stopped.clone();
        let oldest_find_stopped = stopped.clone();
        let oldest_ping_stopped = stopped.clone();
        let bootstrap_ping_stopped = stopped.clone();
        let sample_producer_stopped = stopped.clone();
        [
            child_factory(move |shutdown| async move {
                wait_for_shutdown(shutdown).await;
                scheduler_stopped.fetch_add(1, Ordering::SeqCst);
                ChildExit::Scheduler(DhtDiscoveredNodeSchedulerExit::Shutdown {
                    pending_dropped: 11,
                })
            }),
            child_factory(move |shutdown| async move {
                wait_for_shutdown(shutdown).await;
                ping_stopped.fetch_add(1, Ordering::SeqCst);
                ChildExit::Ping(DhtDiscoveredNodePingWorkerExit::Shutdown {
                    queued_dropped: 12,
                    queries_cancelled: 13,
                })
            }),
            child_factory(move |shutdown| async move {
                wait_for_shutdown(shutdown).await;
                find_stopped.fetch_add(1, Ordering::SeqCst);
                ChildExit::FindNode(DhtDiscoveredNodeFindWorkerExit::Shutdown {
                    queued_dropped: 14,
                    tasks_cancelled: 15,
                    recursive_nodes_dropped: 16,
                })
            }),
            child_factory(move |shutdown| async move {
                wait_for_shutdown(shutdown).await;
                sample_worker_stopped.fetch_add(1, Ordering::SeqCst);
                ChildExit::SampleInfoHashesWorker(DhtSampleInfoHashesWorkerExit::Shutdown {
                    queued_dropped: 17,
                    tasks_cancelled: 18,
                    triage_hashes_dropped: 19,
                    recursive_nodes_dropped: 20,
                })
            }),
            child_factory(move |shutdown| async move {
                wait_for_shutdown(shutdown).await;
                oldest_find_stopped.fetch_add(1, Ordering::SeqCst);
                ChildExit::OldestFind(DhtOldestNodeFindProducerExit::Shutdown {
                    selected_dropped: 21,
                })
            }),
            child_factory(move |shutdown| async move {
                wait_for_shutdown(shutdown).await;
                oldest_ping_stopped.fetch_add(1, Ordering::SeqCst);
                ChildExit::OldestPing(DhtOldestNodePingProducerExit::Shutdown {
                    selected_dropped: 22,
                })
            }),
            child_factory(move |shutdown| async move {
                wait_for_shutdown(shutdown).await;
                bootstrap_ping_stopped.fetch_add(1, Ordering::SeqCst);
                ChildExit::BootstrapPing(DhtBootstrapPingProducerExit::Shutdown {
                    selected_dropped: 23,
                })
            }),
            child_factory(move |shutdown| async move {
                wait_for_shutdown(shutdown).await;
                sample_producer_stopped.fetch_add(1, Ordering::SeqCst);
                ChildExit::SampleInfoHashesProducer(DhtSampleInfoHashesProducerExit::Shutdown {
                    selected_dropped: 24,
                })
            }),
            child_factory(move |shutdown| async move {
                wait_for_shutdown(shutdown).await;
                stopped.fetch_add(1, Ordering::SeqCst);
                ChildExit::Target(Ok(()))
            }),
        ]
    }

    fn new_with_bootstrap_nodes(
        discovery: DhtDiscoveryReceiver,
        client: &DhtRuntimeClient,
        table: &KTable,
        triage: &DhtInfoHashTriageInput,
        bootstrap_nodes: Vec<String>,
    ) -> Result<
        (
            DhtCrawlerMaintenanceSupervisor,
            DhtCrawlerMaintenanceStatsHandle,
        ),
        DhtCrawlerMaintenanceStartError,
    > {
        assert!(
            bootstrap_nodes
                .iter()
                .all(|endpoint| endpoint.parse::<SocketAddr>().is_ok()),
            "test bootstrap endpoints must be numeric socket addresses"
        );
        DhtCrawlerMaintenanceSupervisor::new_with_factories(
            discovery,
            client,
            table,
            triage,
            DhtCrawlerTargetRotator::new,
            move |input| DhtBootstrapPingProducer::with_bootstrap_nodes(input, bootstrap_nodes),
        )
    }

    fn triage_channel() -> (DhtInfoHashTriageInput, DhtInfoHashTriageReceiver) {
        dht_info_hash_triage_channel(NonZeroUsize::new(100).expect("nonzero"))
    }

    async fn test_runtime() -> DhtRuntime {
        DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            ..DhtRuntimeConfig::default()
        })
        .await
        .expect("loopback runtime starts")
    }

    fn node(value: u8) -> RoutingNode {
        let mut id = [0_u8; 20];
        id[19] = value;
        RoutingNode {
            id: Id20::from_slice(&id).unwrap(),
            addr: SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(127, 0, 0, value),
                10_000 + u16::from(value),
            )),
        }
    }

    async fn poll_once_pending<F>(mut future: Pin<&mut F>)
    where
        F: Future,
    {
        poll_fn(|context| match future.as_mut().poll(context) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(_) => panic!("future completed instead of remaining pending"),
        })
        .await;
    }

    async fn wait_for(mut predicate: impl FnMut() -> bool, description: &'static str) {
        tokio::time::timeout(Duration::from_secs(1), async move {
            loop {
                if predicate() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {description}"));
    }

    async fn assert_ping_eof_requires_both_producer_capabilities(drop_oldest_first: bool) {
        let (discovery_sender, discovery) =
            dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let (scheduler, mut routes, _stats) =
            DhtDiscoveredNodeScheduler::new(discovery, KTable::new(Id20::ZERO));
        let oldest_ping = scheduler.ping_input();
        let bootstrap_ping = scheduler.ping_input();
        drop(scheduler);
        drop(discovery_sender);

        let (first, last) = if drop_oldest_first {
            (oldest_ping, bootstrap_ping)
        } else {
            (bootstrap_ping, oldest_ping)
        };
        drop(first);
        let mut ping_eof = Box::pin(routes.ping.recv());
        poll_once_pending(ping_eof.as_mut()).await;
        drop(ping_eof);
        drop(last);
        assert_eq!(routes.ping.recv().await, None);
    }

    async fn assert_sample_eof_requires_scheduler_and_periodic_producer(
        drop_scheduler_first: bool,
    ) {
        let (_discovery_sender, discovery) =
            dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let (scheduler, mut routes, _stats) =
            DhtDiscoveredNodeScheduler::new(discovery, KTable::new(Id20::ZERO));
        let periodic_producer = scheduler.sample_infohashes_input();

        if drop_scheduler_first {
            drop(scheduler);
            let mut sample_eof = Box::pin(routes.sample_infohashes.recv());
            poll_once_pending(sample_eof.as_mut()).await;
            drop(sample_eof);
            drop(periodic_producer);
        } else {
            drop(periodic_producer);
            let mut sample_eof = Box::pin(routes.sample_infohashes.recv());
            poll_once_pending(sample_eof.as_mut()).await;
            drop(sample_eof);
            drop(scheduler);
        }
        assert_eq!(routes.sample_infohashes.recv().await, None);
    }

    async fn assert_discovery_eof_requires_find_and_sample_senders(drop_find_first: bool) {
        let (original, discovery) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let find_recursive = discovery.try_sender().expect("original sender is live");
        let sample_recursive = discovery.try_sender().expect("original sender is live");
        drop(original);
        let (scheduler, _routes, _stats) =
            DhtDiscoveredNodeScheduler::new(discovery, KTable::new(Id20::ZERO));
        let mut scheduler_run = Box::pin(scheduler.run(pending()));
        poll_once_pending(scheduler_run.as_mut()).await;

        if drop_find_first {
            drop(find_recursive);
            poll_once_pending(scheduler_run.as_mut()).await;
            drop(sample_recursive);
        } else {
            drop(sample_recursive);
            poll_once_pending(scheduler_run.as_mut()).await;
            drop(find_recursive);
        }
        assert_eq!(
            scheduler_run.await,
            DhtDiscoveredNodeSchedulerExit::InputClosed
        );
    }

    #[tokio::test]
    async fn constructor_wires_sample_children_and_preserves_external_triage_ownership() {
        let runtime = test_runtime().await;
        let client = runtime.client();
        let (_sender, discovery) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let (triage, mut triage_receiver) = triage_channel();
        let (supervisor, stats) =
            new_with_bootstrap_nodes(discovery, &client, runtime.table(), &triage, Vec::new())
                .unwrap();

        assert_eq!(stats.discovery.snapshot(), DhtDiscoveryStats::default());
        assert_eq!(
            stats.scheduler.snapshot(),
            DhtDiscoveredNodeSchedulerStats::default()
        );
        assert_eq!(stats.ping.snapshot(), DhtDiscoveredNodePingStats::default());
        assert_eq!(
            stats.find_node.snapshot(),
            DhtDiscoveredNodeFindStats::default()
        );
        assert_eq!(
            stats.sample_infohashes_worker.snapshot(),
            DhtSampleInfoHashesWorkerStats::default()
        );
        assert_eq!(
            stats.oldest_find.snapshot(),
            DhtOldestNodeFindProducerStats::default()
        );
        assert_eq!(
            stats.oldest_ping.snapshot(),
            DhtOldestNodePingProducerStats::default()
        );
        assert_eq!(
            stats.bootstrap_ping.snapshot(),
            DhtBootstrapPingProducerStats::default()
        );
        assert_eq!(
            stats.sample_infohashes_producer.snapshot(),
            DhtSampleInfoHashesProducerStats::default()
        );

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn(supervisor.run(async move {
            let _ = shutdown_rx.await;
        }));
        wait_for(
            || stats.sample_infohashes_producer.snapshot().table_queries == 1,
            "the sample producer to finish its immediate empty-table query",
        )
        .await;
        assert_eq!(
            stats.sample_infohashes_producer.snapshot(),
            DhtSampleInfoHashesProducerStats {
                table_queries: 1,
                ..DhtSampleInfoHashesProducerStats::default()
            }
        );
        assert_eq!(
            stats.sample_infohashes_worker.snapshot(),
            DhtSampleInfoHashesWorkerStats::default()
        );
        assert_eq!(
            stats.bootstrap_ping.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        assert!(matches!(
            triage_receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        shutdown_tx.send(()).unwrap();
        assert!(matches!(
            run.await.unwrap(),
            DhtCrawlerMaintenanceSupervisorExit::Shutdown { .. }
        ));
        drop(triage);
        assert_eq!(triage_receiver.recv().await, None);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn closed_external_triage_route_is_not_a_supervisor_terminal_condition() {
        let runtime = test_runtime().await;
        let client = runtime.client();
        let (_sender, discovery) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let (triage, mut triage_receiver) = triage_channel();
        triage_receiver.close();
        let (supervisor, stats) =
            new_with_bootstrap_nodes(discovery, &client, runtime.table(), &triage, Vec::new())
                .unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn(supervisor.run(async move {
            let _ = shutdown_rx.await;
        }));

        wait_for(
            || stats.sample_infohashes_producer.snapshot().table_queries == 1,
            "the sample producer to finish its immediate empty-table query",
        )
        .await;
        assert!(!run.is_finished());
        assert_eq!(
            stats.sample_infohashes_worker.snapshot(),
            DhtSampleInfoHashesWorkerStats::default()
        );

        shutdown_tx.send(()).unwrap();
        assert!(matches!(
            run.await.unwrap(),
            DhtCrawlerMaintenanceSupervisorExit::Shutdown { .. }
        ));
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn real_constructor_routes_oldest_ping_directly_to_the_ping_worker() {
        let runtime = DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            query_timeout: Duration::from_secs(60),
            ..DhtRuntimeConfig::default()
        })
        .await
        .expect("loopback runtime starts");
        let client = runtime.client();
        let table = KTable::new(Id20::ZERO);
        let oldest = node(99);
        assert_eq!(table.put_node(oldest), RoutingPutResult::Accepted);
        let (_sender, discovery) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let (triage, _triage_receiver) = triage_channel();
        let (supervisor, stats) =
            new_with_bootstrap_nodes(discovery, &client, &table, &triage, Vec::new()).unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let mut run = Box::pin(supervisor.run(async move {
            let _ = shutdown_rx.await;
        }));

        poll_once_pending(run.as_mut()).await;
        tokio::task::yield_now().await;
        assert_eq!(
            stats.bootstrap_ping.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        tokio::time::advance(Duration::from_millis(9_999)).await;
        assert_eq!(
            stats.oldest_ping.snapshot(),
            DhtOldestNodePingProducerStats::default()
        );
        assert_eq!(
            stats.bootstrap_ping.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                ..DhtBootstrapPingProducerStats::default()
            }
        );

        tokio::time::advance(Duration::from_millis(1)).await;
        wait_for(
            || stats.oldest_ping.snapshot().queued == 1 && stats.ping.snapshot().dequeued == 1,
            "the oldest-ping producer and ping worker to exchange one node",
        )
        .await;
        assert_eq!(
            stats.oldest_ping.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 1,
                queued: 1,
                ..DhtOldestNodePingProducerStats::default()
            }
        );
        assert_eq!(stats.ping.snapshot().dequeued, 1);
        assert_eq!(stats.scheduler.snapshot().routed_ping, 0);

        shutdown_tx.send(()).unwrap();
        let DhtCrawlerMaintenanceSupervisorExit::Shutdown { children } = run.await else {
            panic!("explicit shutdown remains the primary supervisor cause");
        };
        assert_eq!(
            children.oldest_ping,
            DhtOldestNodePingProducerExit::Shutdown {
                selected_dropped: 0,
            }
        );
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_sample_candidate_routes_directly_to_the_sample_worker() {
        let runtime = DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            query_timeout: Duration::from_secs(60),
            ..DhtRuntimeConfig::default()
        })
        .await
        .expect("loopback runtime starts");
        let client = runtime.client();
        let table = KTable::new(Id20::ZERO);
        assert_eq!(table.put_node(node(99)), RoutingPutResult::Accepted);
        let (_sender, discovery) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let (triage, _triage_receiver) = triage_channel();
        let (supervisor, stats) =
            new_with_bootstrap_nodes(discovery, &client, &table, &triage, Vec::new()).unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn(supervisor.run(async move {
            let _ = shutdown_rx.await;
        }));

        wait_for(
            || {
                stats.sample_infohashes_producer.snapshot().queued == 1
                    && stats.sample_infohashes_worker.snapshot().queries_started == 1
            },
            "the periodic sample producer and worker to exchange one retained handle",
        )
        .await;
        assert_eq!(
            stats.sample_infohashes_producer.snapshot(),
            DhtSampleInfoHashesProducerStats {
                table_queries: 1,
                selected: 1,
                queued: 1,
                ..DhtSampleInfoHashesProducerStats::default()
            }
        );
        assert_eq!(
            stats.sample_infohashes_worker.snapshot(),
            DhtSampleInfoHashesWorkerStats {
                dequeued: 1,
                queries_started: 1,
                ..DhtSampleInfoHashesWorkerStats::default()
            }
        );
        assert_eq!(stats.scheduler.snapshot().routed_sample_infohashes, 0);

        shutdown_tx.send(()).unwrap();
        let DhtCrawlerMaintenanceSupervisorExit::Shutdown { children } = run.await.unwrap() else {
            panic!("explicit shutdown remains the primary supervisor cause");
        };
        assert_eq!(
            children.sample_infohashes_producer,
            DhtSampleInfoHashesProducerExit::Shutdown {
                selected_dropped: 0,
            }
        );
        assert_eq!(
            children.sample_infohashes_worker,
            DhtSampleInfoHashesWorkerExit::Shutdown {
                queued_dropped: 0,
                tasks_cancelled: 1,
                triage_hashes_dropped: 0,
                recursive_nodes_dropped: 0,
            }
        );
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn numeric_bootstrap_ping_routes_directly_to_the_shared_ping_worker() {
        let runtime = DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            query_timeout: Duration::from_secs(60),
            ..DhtRuntimeConfig::default()
        })
        .await
        .expect("loopback runtime starts");
        let client = runtime.client();
        let table = KTable::new(Id20::ZERO);
        let (_sender, discovery) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let (triage, _triage_receiver) = triage_channel();
        let (supervisor, stats) = new_with_bootstrap_nodes(
            discovery,
            &client,
            &table,
            &triage,
            vec![String::from("127.0.0.1:19091")],
        )
        .unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn(supervisor.run(async move {
            let _ = shutdown_rx.await;
        }));

        wait_for(
            || stats.bootstrap_ping.snapshot().queued == 1 && stats.ping.snapshot().dequeued == 1,
            "the bootstrap-ping producer and ping worker to exchange one node",
        )
        .await;
        assert_eq!(
            stats.bootstrap_ping.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 1,
                resolution_attempts: 1,
                queued: 1,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        assert_eq!(
            stats.oldest_ping.snapshot(),
            DhtOldestNodePingProducerStats::default()
        );
        assert_eq!(stats.scheduler.snapshot().routed_ping, 0);

        shutdown_tx.send(()).unwrap();
        let DhtCrawlerMaintenanceSupervisorExit::Shutdown { children } = run.await.unwrap() else {
            panic!("explicit shutdown remains the primary supervisor cause");
        };
        assert_eq!(
            children.bootstrap_ping,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 0,
            }
        );
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn pre_ready_shutdown_invokes_no_factory_and_runs_no_child_work() {
        let invoked = Arc::new(AtomicUsize::new(0));
        let factories = std::array::from_fn(|_| {
            let invoked = invoked.clone();
            child_factory(move |_| {
                invoked.fetch_add(1, Ordering::SeqCst);
                async move {
                    pending::<()>().await;
                    unreachable!()
                }
            })
        });
        assert!(matches!(
            run_child_factories(ready(()), factories).await,
            DhtCrawlerMaintenanceSupervisorExit::ShutdownBeforeStart
        ));
        assert_eq!(invoked.load(Ordering::SeqCst), 0);

        let runtime = test_runtime().await;
        let client = runtime.client();
        let table = runtime.table().clone();
        assert_ne!(table.put_node(node(99)), RoutingPutResult::Rejected);
        let (sender, discovery) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let (triage, _triage_receiver) = triage_channel();
        assert_eq!(sender.offer(node(1)), DhtDiscoveryOffer::Queued);
        let (supervisor, stats) =
            DhtCrawlerMaintenanceSupervisor::new(discovery, &client, &table, &triage).unwrap();
        assert!(matches!(
            supervisor.run(ready(())).await,
            DhtCrawlerMaintenanceSupervisorExit::ShutdownBeforeStart
        ));
        assert_eq!(
            stats.scheduler.snapshot(),
            DhtDiscoveredNodeSchedulerStats::default()
        );
        assert_eq!(
            stats.oldest_find.snapshot(),
            DhtOldestNodeFindProducerStats::default()
        );
        assert_eq!(
            stats.oldest_ping.snapshot(),
            DhtOldestNodePingProducerStats::default()
        );
        assert_eq!(
            stats.bootstrap_ping.snapshot(),
            DhtBootstrapPingProducerStats::default()
        );
        assert_eq!(stats.ping.snapshot(), DhtDiscoveredNodePingStats::default());
        assert_eq!(
            stats.find_node.snapshot(),
            DhtDiscoveredNodeFindStats::default()
        );
        assert_eq!(
            stats.sample_infohashes_worker.snapshot(),
            DhtSampleInfoHashesWorkerStats::default()
        );
        assert_eq!(
            stats.sample_infohashes_producer.snapshot(),
            DhtSampleInfoHashesProducerStats::default()
        );
        assert_eq!(sender.offer(node(2)), DhtDiscoveryOffer::ReceiverClosed);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn typed_child_exit_is_first_cause_and_all_siblings_finish_shutdown() {
        let stopped = Arc::new(AtomicUsize::new(0));
        let mut factories = cooperative_factories(stopped.clone());
        factories[0] = child_factory(|_| async {
            ChildExit::Scheduler(DhtDiscoveredNodeSchedulerExit::InputClosed)
        });

        let DhtCrawlerMaintenanceSupervisorExit::Failed { first, children } =
            run_child_factories(pending(), factories).await
        else {
            panic!("typed child exit must be the primary failure");
        };
        assert_eq!(first, DhtCrawlerMaintenanceChild::Scheduler);
        assert_eq!(
            children.scheduler,
            DhtDiscoveredNodeSchedulerExit::InputClosed
        );
        assert_eq!(
            children.ping,
            DhtDiscoveredNodePingWorkerExit::Shutdown {
                queued_dropped: 12,
                queries_cancelled: 13,
            }
        );
        assert_eq!(
            children.find_node,
            DhtDiscoveredNodeFindWorkerExit::Shutdown {
                queued_dropped: 14,
                tasks_cancelled: 15,
                recursive_nodes_dropped: 16,
            }
        );
        assert_eq!(
            children.sample_infohashes_worker,
            DhtSampleInfoHashesWorkerExit::Shutdown {
                queued_dropped: 17,
                tasks_cancelled: 18,
                triage_hashes_dropped: 19,
                recursive_nodes_dropped: 20,
            }
        );
        assert_eq!(
            children.oldest_find,
            DhtOldestNodeFindProducerExit::Shutdown {
                selected_dropped: 21,
            }
        );
        assert_eq!(
            children.oldest_ping,
            DhtOldestNodePingProducerExit::Shutdown {
                selected_dropped: 22,
            }
        );
        assert_eq!(
            children.bootstrap_ping,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 23,
            }
        );
        assert_eq!(
            children.sample_infohashes_producer,
            DhtSampleInfoHashesProducerExit::Shutdown {
                selected_dropped: 24,
            }
        );
        assert!(children.target.is_ok());
        assert_eq!(stopped.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn sample_worker_typed_exit_is_first_cause_and_all_siblings_finish_shutdown() {
        let stopped = Arc::new(AtomicUsize::new(0));
        let mut factories = cooperative_factories(stopped.clone());
        factories[3] = child_factory(|_| async {
            ChildExit::SampleInfoHashesWorker(DhtSampleInfoHashesWorkerExit::InputClosed)
        });

        let DhtCrawlerMaintenanceSupervisorExit::Failed { first, children } =
            run_child_factories(pending(), factories).await
        else {
            panic!("sample worker input closure must be the primary failure");
        };
        assert_eq!(first, DhtCrawlerMaintenanceChild::SampleInfoHashesWorker);
        assert_eq!(
            children.sample_infohashes_worker,
            DhtSampleInfoHashesWorkerExit::InputClosed
        );
        assert_eq!(
            children.sample_infohashes_producer,
            DhtSampleInfoHashesProducerExit::Shutdown {
                selected_dropped: 24,
            }
        );
        assert_eq!(
            children.scheduler,
            DhtDiscoveredNodeSchedulerExit::Shutdown {
                pending_dropped: 11,
            }
        );
        assert!(children.target.is_ok());
        assert_eq!(stopped.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn sample_producer_typed_exit_is_first_cause_and_all_siblings_finish_shutdown() {
        let stopped = Arc::new(AtomicUsize::new(0));
        let mut factories = cooperative_factories(stopped.clone());
        factories[7] = child_factory(|_| async {
            ChildExit::SampleInfoHashesProducer(DhtSampleInfoHashesProducerExit::InputClosed {
                selected_dropped: 25,
            })
        });

        let DhtCrawlerMaintenanceSupervisorExit::Failed { first, children } =
            run_child_factories(pending(), factories).await
        else {
            panic!("sample producer input closure must be the primary failure");
        };
        assert_eq!(first, DhtCrawlerMaintenanceChild::SampleInfoHashesProducer);
        assert_eq!(
            children.sample_infohashes_producer,
            DhtSampleInfoHashesProducerExit::InputClosed {
                selected_dropped: 25,
            }
        );
        assert_eq!(
            children.sample_infohashes_worker,
            DhtSampleInfoHashesWorkerExit::Shutdown {
                queued_dropped: 17,
                tasks_cancelled: 18,
                triage_hashes_dropped: 19,
                recursive_nodes_dropped: 20,
            }
        );
        assert!(children.target.is_ok());
        assert_eq!(stopped.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn oldest_ping_typed_exit_is_first_cause_and_all_siblings_finish_shutdown() {
        let stopped = Arc::new(AtomicUsize::new(0));
        let mut factories = cooperative_factories(stopped.clone());
        factories[5] = child_factory(|_| async {
            ChildExit::OldestPing(DhtOldestNodePingProducerExit::InputClosed {
                selected_dropped: 19,
            })
        });

        let DhtCrawlerMaintenanceSupervisorExit::Failed { first, children } =
            run_child_factories(pending(), factories).await
        else {
            panic!("oldest-ping input closure must be the primary failure");
        };
        assert_eq!(first, DhtCrawlerMaintenanceChild::OldestPing);
        assert_eq!(
            children.oldest_ping,
            DhtOldestNodePingProducerExit::InputClosed {
                selected_dropped: 19,
            }
        );
        assert_eq!(
            children.scheduler,
            DhtDiscoveredNodeSchedulerExit::Shutdown {
                pending_dropped: 11,
            }
        );
        assert_eq!(
            children.oldest_find,
            DhtOldestNodeFindProducerExit::Shutdown {
                selected_dropped: 21,
            }
        );
        assert_eq!(
            children.bootstrap_ping,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 23,
            }
        );
        assert!(children.target.is_ok());
        assert_eq!(stopped.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn bootstrap_ping_typed_exit_is_first_cause_and_all_siblings_finish_shutdown() {
        let stopped = Arc::new(AtomicUsize::new(0));
        let mut factories = cooperative_factories(stopped.clone());
        factories[6] = child_factory(|_| async {
            ChildExit::BootstrapPing(DhtBootstrapPingProducerExit::InputClosed {
                selected_dropped: 20,
            })
        });

        let DhtCrawlerMaintenanceSupervisorExit::Failed { first, children } =
            run_child_factories(pending(), factories).await
        else {
            panic!("bootstrap-ping input closure must be the primary failure");
        };
        assert_eq!(first, DhtCrawlerMaintenanceChild::BootstrapPing);
        assert_eq!(
            children.bootstrap_ping,
            DhtBootstrapPingProducerExit::InputClosed {
                selected_dropped: 20,
            }
        );
        assert_eq!(
            children.scheduler,
            DhtDiscoveredNodeSchedulerExit::Shutdown {
                pending_dropped: 11,
            }
        );
        assert_eq!(
            children.oldest_ping,
            DhtOldestNodePingProducerExit::Shutdown {
                selected_dropped: 22,
            }
        );
        assert!(children.target.is_ok());
        assert_eq!(stopped.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn target_entropy_is_typed_and_all_siblings_finish_shutdown() {
        let stopped = Arc::new(AtomicUsize::new(0));
        let mut factories = cooperative_factories(stopped.clone());
        factories[8] = child_factory(|_| async {
            ChildExit::Target(Err(DhtCrawlerTargetError::Entropy(
                getrandom::Error::UNEXPECTED,
            )))
        });

        let DhtCrawlerMaintenanceSupervisorExit::Failed { first, children } =
            run_child_factories(pending(), factories).await
        else {
            panic!("target entropy must be the primary failure");
        };
        assert_eq!(first, DhtCrawlerMaintenanceChild::Target);
        assert!(matches!(
            children.target,
            Err(DhtCrawlerTargetError::Entropy(error))
                if error == getrandom::Error::UNEXPECTED
        ));
        assert_eq!(stopped.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn child_panic_joins_siblings_then_resumes_the_exact_payload() {
        let stopped = Arc::new(AtomicUsize::new(0));
        let mut factories = cooperative_factories(stopped.clone());
        factories[1] = child_factory(|_| async {
            panic_any(String::from("maintenance child panic"));
            #[allow(unreachable_code)]
            ChildExit::Target(Ok(()))
        });

        let error = tokio::spawn(run_child_factories(pending(), factories))
            .await
            .expect_err("the exact child panic is resumed");
        assert_eq!(
            error
                .into_panic()
                .downcast::<String>()
                .expect("panic payload remains a String")
                .as_str(),
            "maintenance child panic"
        );
        assert_eq!(stopped.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn external_shutdown_joins_nine_complete_structured_exits() {
        let stopped = Arc::new(AtomicUsize::new(0));
        let factories = cooperative_factories(stopped.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn(run_child_factories(
            async move {
                let _ = shutdown_rx.await;
            },
            factories,
        ));
        tokio::task::yield_now().await;
        shutdown_tx.send(()).unwrap();

        let DhtCrawlerMaintenanceSupervisorExit::Shutdown { children } = run.await.unwrap() else {
            panic!("external shutdown must remain the primary cause");
        };
        assert_eq!(
            children.scheduler,
            DhtDiscoveredNodeSchedulerExit::Shutdown {
                pending_dropped: 11,
            }
        );
        assert_eq!(
            children.ping,
            DhtDiscoveredNodePingWorkerExit::Shutdown {
                queued_dropped: 12,
                queries_cancelled: 13,
            }
        );
        assert_eq!(
            children.find_node,
            DhtDiscoveredNodeFindWorkerExit::Shutdown {
                queued_dropped: 14,
                tasks_cancelled: 15,
                recursive_nodes_dropped: 16,
            }
        );
        assert_eq!(
            children.sample_infohashes_worker,
            DhtSampleInfoHashesWorkerExit::Shutdown {
                queued_dropped: 17,
                tasks_cancelled: 18,
                triage_hashes_dropped: 19,
                recursive_nodes_dropped: 20,
            }
        );
        assert_eq!(
            children.oldest_find,
            DhtOldestNodeFindProducerExit::Shutdown {
                selected_dropped: 21,
            }
        );
        assert_eq!(
            children.oldest_ping,
            DhtOldestNodePingProducerExit::Shutdown {
                selected_dropped: 22,
            }
        );
        assert_eq!(
            children.bootstrap_ping,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 23,
            }
        );
        assert_eq!(
            children.sample_infohashes_producer,
            DhtSampleInfoHashesProducerExit::Shutdown {
                selected_dropped: 24,
            }
        );
        assert!(children.target.is_ok());
        assert_eq!(stopped.load(Ordering::SeqCst), 9);
    }

    struct ReadyAfter(Arc<AtomicBool>);

    impl Future for ReadyAfter {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut std::task::Context<'_>) -> Poll<()> {
            if self.0.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    #[tokio::test]
    async fn biased_external_shutdown_keeps_primary_cause_and_retains_precompleted_child_exit() {
        let stopped = Arc::new(AtomicUsize::new(0));
        let mut factories = cooperative_factories(stopped.clone());
        let child_completed = Arc::new(AtomicBool::new(false));
        let completed_by_child = child_completed.clone();
        factories[0] = child_factory(move |_| async move {
            completed_by_child.store(true, Ordering::SeqCst);
            ChildExit::Scheduler(DhtDiscoveredNodeSchedulerExit::InputClosed)
        });

        let (publishers, notifications) = DhtCrawlerMaintenanceRunPublishers::channel();
        let (mut started, mut stopping) = notifications.into_parts();
        let DhtCrawlerMaintenanceSupervisorExit::Shutdown { children } =
            run_child_factories_with_notifications(
                ReadyAfter(child_completed),
                factories,
                Some(publishers),
            )
            .await
        else {
            panic!("biased external shutdown must remain the primary cause");
        };
        assert!(
            started.notified().await.is_err(),
            "an equal-ready terminal observation must suppress startup acknowledgement"
        );
        stopping
            .notified()
            .await
            .expect("the selected terminal trigger is published before cleanup");
        assert_eq!(
            children.scheduler,
            DhtDiscoveredNodeSchedulerExit::InputClosed
        );
        assert_eq!(
            children.ping,
            DhtDiscoveredNodePingWorkerExit::Shutdown {
                queued_dropped: 12,
                queries_cancelled: 13,
            }
        );
        assert_eq!(
            children.find_node,
            DhtDiscoveredNodeFindWorkerExit::Shutdown {
                queued_dropped: 14,
                tasks_cancelled: 15,
                recursive_nodes_dropped: 16,
            }
        );
        assert_eq!(
            children.sample_infohashes_worker,
            DhtSampleInfoHashesWorkerExit::Shutdown {
                queued_dropped: 17,
                tasks_cancelled: 18,
                triage_hashes_dropped: 19,
                recursive_nodes_dropped: 20,
            }
        );
        assert_eq!(
            children.oldest_find,
            DhtOldestNodeFindProducerExit::Shutdown {
                selected_dropped: 21,
            }
        );
        assert_eq!(
            children.oldest_ping,
            DhtOldestNodePingProducerExit::Shutdown {
                selected_dropped: 22,
            }
        );
        assert_eq!(
            children.bootstrap_ping,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 23,
            }
        );
        assert_eq!(
            children.sample_infohashes_producer,
            DhtSampleInfoHashesProducerExit::Shutdown {
                selected_dropped: 24,
            }
        );
        assert!(children.target.is_ok());
        assert_eq!(stopped.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn all_nine_unique_child_starts_are_acknowledged_before_external_shutdown() {
        let stopped = Arc::new(AtomicUsize::new(0));
        let factories = cooperative_factories(stopped.clone());
        let (publishers, notifications) = DhtCrawlerMaintenanceRunPublishers::channel();
        let (mut started, mut stopping) = notifications.into_parts();

        let DhtCrawlerMaintenanceSupervisorExit::Shutdown { .. } =
            run_child_factories_with_notifications(
                async move {
                    started
                        .notified()
                        .await
                        .expect("all nine unique children must be acknowledged");
                },
                factories,
                Some(publishers),
            )
            .await
        else {
            panic!("startup acknowledgement should drive external shutdown");
        };
        stopping
            .notified()
            .await
            .expect("external shutdown publishes stopping before cleanup");
        assert_eq!(stopped.load(Ordering::SeqCst), 9);
    }

    #[test]
    fn duplicate_child_start_does_not_replace_a_missing_identity() {
        let mut starts = ChildStartCollector::default();
        for child in MAINTENANCE_CHILDREN.into_iter().take(8) {
            assert!(!starts.record(child));
        }
        assert!(!starts.record(DhtCrawlerMaintenanceChild::Scheduler));
        assert!(!starts.complete());
        assert!(starts.record(DhtCrawlerMaintenanceChild::Target));
        assert!(starts.complete());
    }

    struct DropFlag(Arc<AtomicUsize>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn dropping_run_aborts_every_owned_child_without_detaching() {
        let polled = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let factories = std::array::from_fn(|_| {
            let polled = polled.clone();
            let dropped = dropped.clone();
            child_factory(move |_| async move {
                let _drop_flag = DropFlag(dropped);
                polled.fetch_add(1, Ordering::SeqCst);
                pending::<()>().await;
                unreachable!()
            })
        });
        let mut run = Box::pin(run_child_factories(pending(), factories));
        poll_once_pending(run.as_mut()).await;
        wait_for(
            || polled.load(Ordering::SeqCst) == 9,
            "all child tasks to be polled",
        )
        .await;
        drop(run);
        wait_for(
            || dropped.load(Ordering::SeqCst) == 9,
            "all aborted child tasks to be dropped",
        )
        .await;
    }

    #[tokio::test]
    async fn recovered_discovery_and_direct_producer_capabilities_form_intentional_eof_cycles() {
        let (sender, discovery) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let find_recursive = discovery.try_sender().expect("original sender is live");
        let sample_recursive = discovery.try_sender().expect("original sender is live");
        drop(sender);
        let table = KTable::new(Id20::ZERO);
        let (scheduler, mut routes, _stats) = DhtDiscoveredNodeScheduler::new(discovery, table);
        let oldest_ping = scheduler.ping_input();
        let bootstrap_ping = scheduler.ping_input();
        let oldest_find = scheduler.find_node_input();
        let sample_producer = scheduler.sample_infohashes_input();
        let mut scheduler_run = Box::pin(scheduler.run(pending()));

        poll_once_pending(scheduler_run.as_mut()).await;
        drop(find_recursive);
        poll_once_pending(scheduler_run.as_mut()).await;
        drop(sample_recursive);
        assert_eq!(
            scheduler_run.await,
            DhtDiscoveredNodeSchedulerExit::InputClosed
        );
        let mut ping_eof = Box::pin(routes.ping.recv());
        poll_once_pending(ping_eof.as_mut()).await;
        drop(ping_eof);
        let mut find_eof = Box::pin(routes.find_node.recv());
        poll_once_pending(find_eof.as_mut()).await;
        drop(find_eof);
        drop(oldest_ping);
        let mut ping_eof = Box::pin(routes.ping.recv());
        poll_once_pending(ping_eof.as_mut()).await;
        drop(ping_eof);
        drop(bootstrap_ping);
        assert_eq!(routes.ping.recv().await, None);
        drop(oldest_find);
        assert_eq!(routes.find_node.recv().await, None);
        let mut sample_eof = Box::pin(routes.sample_infohashes.recv());
        poll_once_pending(sample_eof.as_mut()).await;
        drop(sample_eof);
        drop(sample_producer);
        assert_eq!(routes.sample_infohashes.recv().await, None);

        assert_ping_eof_requires_both_producer_capabilities(true).await;
        assert_ping_eof_requires_both_producer_capabilities(false).await;
        assert_sample_eof_requires_scheduler_and_periodic_producer(true).await;
        assert_sample_eof_requires_scheduler_and_periodic_producer(false).await;
        assert_discovery_eof_requires_find_and_sample_senders(true).await;
        assert_discovery_eof_requires_find_and_sample_senders(false).await;
    }

    #[tokio::test]
    async fn start_errors_return_the_discovery_receiver_and_preserve_entropy_source() {
        let runtime = test_runtime().await;
        let client = runtime.client();
        let (sender, discovery) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let (triage, mut triage_receiver) = triage_channel();
        drop(sender);
        let error = match DhtCrawlerMaintenanceSupervisor::new(
            discovery,
            &client,
            runtime.table(),
            &triage,
        ) {
            Ok(_) => panic!("closed discovery cannot recover a recursive sender"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            DhtCrawlerMaintenanceStartError::DiscoveryClosed(_)
        ));
        let mut discovery = error.into_discovery();
        assert_eq!(discovery.recv().await, None);
        assert!(matches!(
            triage_receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));

        let (sender, discovery) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let error = match DhtCrawlerMaintenanceSupervisor::new_with_target_factory(
            discovery,
            &client,
            runtime.table(),
            &triage,
            || -> Result<_, DhtCrawlerTargetError> {
                Err(DhtCrawlerTargetError::Entropy(getrandom::Error::UNEXPECTED))
            },
        ) {
            Ok(_) => panic!("target entropy failure must fail construction"),
            Err(error) => error,
        };
        assert!(matches!(
            error.source(),
            Some(source) if source.to_string().contains("failed to generate DHT crawler target")
        ));
        let mut discovery = error.into_discovery();
        assert_eq!(sender.offer(node(1)), DhtDiscoveryOffer::Queued);
        assert_eq!(discovery.recv().await, Some(node(1)));
        assert!(matches!(
            triage_receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        drop(triage);
        assert_eq!(triage_receiver.recv().await, None);
        runtime.shutdown().await.unwrap();
    }
}
