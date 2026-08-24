use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::resume_unwind;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::task::{JoinError, JoinSet};

use crate::{
    DhtCrawlerTarget, DhtDiscoveredNodeRouteReceiver, DhtDiscoveryOffer, DhtDiscoverySender,
    DhtRuntimeClient, FindNodeResult, Id20, KTable, KTableCommand, KTableNodeOption, RoutingNode,
};

const DEFAULT_MAX_INFLIGHT: NonZeroUsize = NonZeroUsize::new(100).unwrap();

/// Concurrency bound for discovered-node `find_node` queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtDiscoveredNodeFindWorkerConfig {
    pub max_inflight: NonZeroUsize,
}

impl Default for DhtDiscoveredNodeFindWorkerConfig {
    fn default() -> Self {
        Self {
            max_inflight: DEFAULT_MAX_INFLIGHT,
        }
    }
}

/// Terminal state of the owned discovered-node `find_node` worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtDiscoveredNodeFindWorkerExit {
    /// Every route producer is gone and every accepted task completed.
    InputClosed,
    /// Caller shutdown won before another completion or route receive.
    Shutdown {
        /// Nodes synchronously drained from the closed route queue.
        queued_dropped: usize,
        /// Accepted tasks whose abort was observed while joining.
        tasks_cancelled: usize,
        /// Response nodes abandoned by those cancelled tasks.
        recursive_nodes_dropped: usize,
    },
}

#[derive(Default)]
struct DhtDiscoveredNodeFindStatsInner {
    dequeued: AtomicU64,
    queries_started: AtomicU64,
    tasks_completed: AtomicU64,
    queries_succeeded: AtomicU64,
    queries_failed: AtomicU64,
    put_commands: AtomicU64,
    drop_commands: AtomicU64,
    recursive_nodes: AtomicU64,
    recursive_nodes_queued: AtomicU64,
    recursive_nodes_closed_dropped: AtomicU64,
    shutdown_queued_dropped: AtomicU64,
    shutdown_tasks_cancelled: AtomicU64,
    shutdown_recursive_nodes_dropped: AtomicU64,
}

/// Cloneable, sender-free view of discovered-node `find_node` counters.
#[derive(Clone, Default)]
pub struct DhtDiscoveredNodeFindStatsHandle {
    inner: Arc<DhtDiscoveredNodeFindStatsInner>,
}

/// One non-transactional snapshot of monotonic `find_node` worker counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtDiscoveredNodeFindStats {
    pub dequeued: u64,
    /// Query futures accepted into the owned in-flight set.
    pub queries_started: u64,
    /// Tasks that completed their table command and recursive fanout handling.
    pub tasks_completed: u64,
    pub queries_succeeded: u64,
    pub queries_failed: u64,
    /// Attempted table puts, including rejected/no-op commands.
    pub put_commands: u64,
    /// Attempted table drops, including absent-ID commands.
    pub drop_commands: u64,
    /// Ordered response nodes returned by successful queries.
    pub recursive_nodes: u64,
    /// Response nodes committed through reserved discovery capacity.
    pub recursive_nodes_queued: u64,
    /// Whole remaining response-node suffixes discarded after the first
    /// receiver-closed observation, including nodes for which no delivery was
    /// attempted. In contrast, [`crate::DhtDiscoveryStats`] records only actual
    /// delivery attempts and aggregates every sender sharing that channel.
    pub recursive_nodes_closed_dropped: u64,
    pub shutdown_queued_dropped: u64,
    pub shutdown_tasks_cancelled: u64,
    /// Response-node suffixes abandoned by graceful task cancellation.
    pub shutdown_recursive_nodes_dropped: u64,
}

impl DhtDiscoveredNodeFindStatsHandle {
    /// Read each monotonic counter independently with relaxed ordering.
    ///
    /// Cross-field invariants are guaranteed only after normal worker exit.
    #[must_use]
    pub fn snapshot(&self) -> DhtDiscoveredNodeFindStats {
        DhtDiscoveredNodeFindStats {
            dequeued: self.inner.dequeued.load(Ordering::Relaxed),
            queries_started: self.inner.queries_started.load(Ordering::Relaxed),
            tasks_completed: self.inner.tasks_completed.load(Ordering::Relaxed),
            queries_succeeded: self.inner.queries_succeeded.load(Ordering::Relaxed),
            queries_failed: self.inner.queries_failed.load(Ordering::Relaxed),
            put_commands: self.inner.put_commands.load(Ordering::Relaxed),
            drop_commands: self.inner.drop_commands.load(Ordering::Relaxed),
            recursive_nodes: self.inner.recursive_nodes.load(Ordering::Relaxed),
            recursive_nodes_queued: self.inner.recursive_nodes_queued.load(Ordering::Relaxed),
            recursive_nodes_closed_dropped: self
                .inner
                .recursive_nodes_closed_dropped
                .load(Ordering::Relaxed),
            shutdown_queued_dropped: self.inner.shutdown_queued_dropped.load(Ordering::Relaxed),
            shutdown_tasks_cancelled: self.inner.shutdown_tasks_cancelled.load(Ordering::Relaxed),
            shutdown_recursive_nodes_dropped: self
                .inner
                .shutdown_recursive_nodes_dropped
                .load(Ordering::Relaxed),
        }
    }
}

/// Owned, bounded consumer for the discovered-node `find_node` route.
///
/// Each accepted route item remains an immutable [`RoutingNode`] snapshot. The
/// shared crawler target is read exactly once immediately before constructing
/// the corresponding query future. A successful response synchronously marks
/// the advertised input node as responded before ordered recursive discovery
/// can await capacity; the response ID is deliberately ignored. Query errors
/// synchronously drop the advertised input ID.
///
/// At most `max_inflight` tasks are accepted at once. The route receiver is not
/// polled at capacity, so no extra item is held outside the route queue. All
/// query and fanout futures remain owned and are aborted if this worker or its
/// `run` future is dropped.
pub struct DhtDiscoveredNodeFindWorker {
    client: DhtRuntimeClient,
    target: DhtCrawlerTarget,
    core: DhtDiscoveredNodeFindWorkerCore,
}

impl DhtDiscoveredNodeFindWorker {
    /// Construct the production-compatible hundred-task worker.
    #[must_use]
    pub fn new(
        input: DhtDiscoveredNodeRouteReceiver,
        client: DhtRuntimeClient,
        table: KTable,
        discovery: DhtDiscoverySender,
        target: DhtCrawlerTarget,
    ) -> (Self, DhtDiscoveredNodeFindStatsHandle) {
        Self::with_config(
            input,
            client,
            table,
            discovery,
            target,
            DhtDiscoveredNodeFindWorkerConfig::default(),
        )
    }

    /// Construct a `find_node` worker with an explicit concurrency bound.
    #[must_use]
    pub fn with_config(
        input: DhtDiscoveredNodeRouteReceiver,
        client: DhtRuntimeClient,
        table: KTable,
        discovery: DhtDiscoverySender,
        target: DhtCrawlerTarget,
        config: DhtDiscoveredNodeFindWorkerConfig,
    ) -> (Self, DhtDiscoveredNodeFindStatsHandle) {
        let stats = DhtDiscoveredNodeFindStatsHandle::default();
        (
            Self {
                client,
                target,
                core: DhtDiscoveredNodeFindWorkerCore::new(
                    input,
                    table,
                    discovery,
                    config.max_inflight,
                    stats.clone(),
                ),
            },
            stats,
        )
    }

    /// Run until route EOF or caller shutdown.
    ///
    /// EOF stops intake and joins every accepted task. Shutdown is biased ahead
    /// of a ready child join and route receive, closes and drains queued input,
    /// aborts accepted tasks, and awaits every cancellation before returning.
    pub async fn run<F>(mut self, shutdown: F) -> DhtDiscoveredNodeFindWorkerExit
    where
        F: Future<Output = ()>,
    {
        let client = self.client.clone();
        let target = self.target.clone();
        self.core
            .run_with_query(
                shutdown,
                move || target.current(),
                move |remote, target| {
                    let client = client.clone();
                    async move { client.find_node(remote, target).await }
                },
            )
            .await
    }
}

struct DhtDiscoveredNodeFindWorkerCore {
    input: DhtDiscoveredNodeRouteReceiver,
    table: KTable,
    discovery: DhtDiscoverySender,
    max_inflight: NonZeroUsize,
    tasks: JoinSet<()>,
    stats: DhtDiscoveredNodeFindStatsHandle,
    abandoned_recursive_nodes: Arc<AtomicUsize>,
    shutdown_in_progress: Arc<AtomicBool>,
}

impl DhtDiscoveredNodeFindWorkerCore {
    fn new(
        input: DhtDiscoveredNodeRouteReceiver,
        table: KTable,
        discovery: DhtDiscoverySender,
        max_inflight: NonZeroUsize,
        stats: DhtDiscoveredNodeFindStatsHandle,
    ) -> Self {
        Self {
            input,
            table,
            discovery,
            max_inflight,
            tasks: JoinSet::new(),
            stats,
            abandoned_recursive_nodes: Arc::new(AtomicUsize::new(0)),
            shutdown_in_progress: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn run_with_query<F, T, Q, QF, E>(
        &mut self,
        shutdown: F,
        mut target_snapshot: T,
        mut query: Q,
    ) -> DhtDiscoveredNodeFindWorkerExit
    where
        F: Future<Output = ()>,
        T: FnMut() -> Id20,
        Q: FnMut(std::net::SocketAddr, Id20) -> QF,
        QF: Future<Output = Result<FindNodeResult, E>> + Send + 'static,
        E: Send + 'static,
    {
        tokio::pin!(shutdown);
        let mut input_closed = false;

        loop {
            if input_closed && self.tasks.is_empty() {
                return DhtDiscoveredNodeFindWorkerExit::InputClosed;
            }

            enum Event {
                Shutdown,
                Joined(Result<(), JoinError>),
                Input(Option<RoutingNode>),
            }

            let event = tokio::select! {
                biased;
                () = &mut shutdown => Event::Shutdown,
                joined = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    Event::Joined(joined.expect("guarded find-node task remains present"))
                }
                node = self.input.recv(),
                    if !input_closed && self.tasks.len() < self.max_inflight.get() =>
                {
                    Event::Input(node)
                }
            };

            match event {
                Event::Shutdown => return self.finish_shutdown().await,
                Event::Joined(Ok(())) => {}
                Event::Joined(Err(error)) => self.finish_abnormal_join(error).await,
                Event::Input(Some(node)) => {
                    increment_saturating(&self.stats.inner.dequeued);
                    let target = target_snapshot();
                    let query_future = query(node.addr, target);
                    let table = self.table.clone();
                    let discovery = self.discovery.clone();
                    let stats = self.stats.clone();
                    let abandoned_recursive_nodes = Arc::clone(&self.abandoned_recursive_nodes);
                    let shutdown_in_progress = Arc::clone(&self.shutdown_in_progress);
                    self.tasks.spawn(async move {
                        finish_find_query(
                            node,
                            query_future.await,
                            &table,
                            &discovery,
                            &stats,
                            abandoned_recursive_nodes,
                            shutdown_in_progress,
                        )
                        .await;
                    });
                    increment_saturating(&self.stats.inner.queries_started);
                }
                Event::Input(None) => input_closed = true,
            }
        }
    }

    async fn finish_shutdown(&mut self) -> DhtDiscoveredNodeFindWorkerExit {
        self.input.close();
        let mut queued_dropped = 0usize;
        while self.input.try_recv().is_ok() {
            queued_dropped = queued_dropped.saturating_add(1);
        }
        add_saturating(&self.stats.inner.shutdown_queued_dropped, queued_dropped);

        self.shutdown_in_progress.store(true, Ordering::SeqCst);
        self.tasks.abort_all();
        let mut tasks_cancelled = 0usize;
        let mut first_panic = None;
        while let Some(joined) = self.tasks.join_next().await {
            match joined {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {
                    tasks_cancelled = tasks_cancelled.saturating_add(1);
                }
                Err(error) if error.is_panic() => {
                    if first_panic.is_none() {
                        first_panic = Some(error.into_panic());
                    }
                }
                Err(error) => panic!("unexpected find-node task join error: {error}"),
            }
        }
        add_saturating(&self.stats.inner.shutdown_tasks_cancelled, tasks_cancelled);
        let recursive_nodes_dropped = self.abandoned_recursive_nodes.load(Ordering::Relaxed);
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
        DhtDiscoveredNodeFindWorkerExit::Shutdown {
            queued_dropped,
            tasks_cancelled,
            recursive_nodes_dropped,
        }
    }

    async fn finish_abnormal_join(&mut self, error: JoinError) -> ! {
        let panic_payload = error.is_panic().then(|| error.into_panic());
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
        if let Some(payload) = panic_payload {
            resume_unwind(payload);
        }
        panic!("find-node task was cancelled outside worker cleanup")
    }
}

impl Drop for DhtDiscoveredNodeFindWorkerCore {
    fn drop(&mut self) {
        self.input.close();
        self.tasks.abort_all();
    }
}

struct RecursiveFanoutGuard {
    remaining: usize,
    stats: DhtDiscoveredNodeFindStatsHandle,
    abandoned_recursive_nodes: Arc<AtomicUsize>,
    shutdown_in_progress: Arc<AtomicBool>,
}

impl RecursiveFanoutGuard {
    fn new(
        remaining: usize,
        stats: DhtDiscoveredNodeFindStatsHandle,
        abandoned_recursive_nodes: Arc<AtomicUsize>,
        shutdown_in_progress: Arc<AtomicBool>,
    ) -> Self {
        Self {
            remaining,
            stats,
            abandoned_recursive_nodes,
            shutdown_in_progress,
        }
    }

    fn queued_one(&mut self) {
        self.remaining = self.remaining.saturating_sub(1);
        increment_saturating(&self.stats.inner.recursive_nodes_queued);
    }

    fn receiver_closed(&mut self) {
        add_saturating(
            &self.stats.inner.recursive_nodes_closed_dropped,
            self.remaining,
        );
        self.remaining = 0;
    }
}

impl Drop for RecursiveFanoutGuard {
    fn drop(&mut self) {
        if self.remaining == 0 {
            return;
        }
        if !self.shutdown_in_progress.load(Ordering::SeqCst) {
            return;
        }
        add_saturating(
            &self.stats.inner.shutdown_recursive_nodes_dropped,
            self.remaining,
        );
        add_saturating_usize(&self.abandoned_recursive_nodes, self.remaining);
    }
}

async fn finish_find_query<E>(
    input: RoutingNode,
    result: Result<FindNodeResult, E>,
    table: &KTable,
    discovery: &DhtDiscoverySender,
    stats: &DhtDiscoveredNodeFindStatsHandle,
    abandoned_recursive_nodes: Arc<AtomicUsize>,
    shutdown_in_progress: Arc<AtomicBool>,
) {
    let nodes = match result {
        Ok(FindNodeResult { id: _, nodes }) => {
            increment_saturating(&stats.inner.queries_succeeded);
            table.batch_command(&[KTableCommand::PutNode {
                node: input,
                options: vec![KTableNodeOption::Responded],
            }]);
            increment_saturating(&stats.inner.put_commands);
            nodes
        }
        Err(_) => {
            increment_saturating(&stats.inner.queries_failed);
            table.batch_command(&[KTableCommand::DropNode { id: input.id }]);
            increment_saturating(&stats.inner.drop_commands);
            increment_saturating(&stats.inner.tasks_completed);
            return;
        }
    };

    add_saturating(&stats.inner.recursive_nodes, nodes.len());
    let mut fanout = RecursiveFanoutGuard::new(
        nodes.len(),
        stats.clone(),
        abandoned_recursive_nodes,
        shutdown_in_progress,
    );
    for node in nodes {
        let permit = match discovery.reserve().await {
            Ok(permit) => permit,
            Err(_) => {
                fanout.receiver_closed();
                break;
            }
        };
        match permit.deliver(node) {
            DhtDiscoveryOffer::Queued => fanout.queued_one(),
            DhtDiscoveryOffer::ReceiverClosed => {
                fanout.receiver_closed();
                break;
            }
            DhtDiscoveryOffer::FullDropped => {
                unreachable!("a reserved discovery permit cannot be capacity-dropped")
            }
        }
    }
    increment_saturating(&stats.inner.tasks_completed);
}

fn increment_saturating(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(1))
    });
}

fn add_saturating(counter: &AtomicU64, amount: usize) {
    let amount = u64::try_from(amount).unwrap_or(u64::MAX);
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(amount))
    });
}

fn add_saturating_usize(counter: &AtomicUsize, amount: usize) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(amount))
    });
}

#[cfg(test)]
#[path = "dht_discovered_node_find_worker_parity.rs"]
mod parity_tests;

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::{pending, poll_fn, ready};
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Mutex;
    use std::task::Poll;

    use tokio::sync::{oneshot, Semaphore};

    use super::*;
    use crate::{
        dht_discovery_channel, DhtDiscoveredNodeSchedulerConfig, DhtDiscoveryReceiver,
        DhtDiscoveryStatsHandle, RoutingPutResult,
    };

    fn id(value: u8) -> Id20 {
        let mut bytes = [0; 20];
        bytes[19] = value;
        Id20::from_slice(&bytes).unwrap()
    }

    fn node(value: u8, port: u16) -> RoutingNode {
        RoutingNode {
            id: id(value),
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        }
    }

    fn scoped_node(value: u8, port: u16, scope: u32) -> RoutingNode {
        RoutingNode {
            id: id(value),
            addr: SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, u16::from(value)),
                port,
                0,
                scope,
            )),
        }
    }

    #[allow(clippy::type_complexity)]
    fn core(
        route_capacity: usize,
        max_inflight: usize,
        discovery_capacity: usize,
    ) -> (
        tokio::sync::mpsc::Sender<RoutingNode>,
        DhtDiscoveredNodeFindWorkerCore,
        DhtDiscoveredNodeFindStatsHandle,
        KTable,
        DhtDiscoveryReceiver,
        DhtDiscoverySender,
        DhtDiscoveryStatsHandle,
    ) {
        let (sender, receiver) = DhtDiscoveredNodeRouteReceiver::test_channel(route_capacity);
        let (discovery, discovery_receiver) =
            dht_discovery_channel(NonZeroUsize::new(discovery_capacity).unwrap());
        let discovery_probe = discovery.clone();
        let discovery_stats = discovery.stats_handle();
        let table = KTable::new(id(250));
        let stats = DhtDiscoveredNodeFindStatsHandle::default();
        (
            sender,
            DhtDiscoveredNodeFindWorkerCore::new(
                receiver,
                table.clone(),
                discovery,
                NonZeroUsize::new(max_inflight).unwrap(),
                stats.clone(),
            ),
            stats,
            table,
            discovery_receiver,
            discovery_probe,
            discovery_stats,
        )
    }

    async fn wait_for(mut predicate: impl FnMut() -> bool, description: &'static str) {
        for _ in 0..1_000 {
            if predicate() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("timed out waiting for {description}");
    }

    fn assert_common_conservation(stats: DhtDiscoveredNodeFindStats) {
        assert_eq!(stats.dequeued, stats.queries_started);
        assert_eq!(stats.put_commands, stats.queries_succeeded);
        assert_eq!(stats.drop_commands, stats.queries_failed);
    }

    fn assert_input_closed_conservation(stats: DhtDiscoveredNodeFindStats) {
        assert_common_conservation(stats);
        assert_eq!(stats.queries_started, stats.tasks_completed);
        assert_eq!(
            stats.tasks_completed,
            stats.queries_succeeded.saturating_add(stats.queries_failed)
        );
        assert_eq!(
            stats.recursive_nodes,
            stats
                .recursive_nodes_queued
                .saturating_add(stats.recursive_nodes_closed_dropped)
        );
        assert_eq!(stats.shutdown_queued_dropped, 0);
        assert_eq!(stats.shutdown_tasks_cancelled, 0);
        assert_eq!(stats.shutdown_recursive_nodes_dropped, 0);
    }

    fn assert_shutdown_conservation(stats: DhtDiscoveredNodeFindStats) {
        assert_common_conservation(stats);
        assert_eq!(
            stats.queries_started,
            stats
                .tasks_completed
                .saturating_add(stats.shutdown_tasks_cancelled)
        );
        assert_eq!(
            stats.recursive_nodes,
            stats
                .recursive_nodes_queued
                .saturating_add(stats.recursive_nodes_closed_dropped)
                .saturating_add(stats.shutdown_recursive_nodes_dropped)
        );
    }

    struct DropFlag(Arc<AtomicUsize>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.fetch_add(1, AtomicOrdering::SeqCst);
        }
    }

    #[test]
    fn defaults_and_all_counters_saturate() {
        assert_eq!(
            DhtDiscoveredNodeFindWorkerConfig::default().max_inflight,
            NonZeroUsize::new(100).unwrap()
        );
        assert_eq!(
            DhtDiscoveredNodeSchedulerConfig::default().find_node_capacity,
            NonZeroUsize::new(100).unwrap()
        );

        let stats = DhtDiscoveredNodeFindStatsHandle::default();
        let counters = [
            &stats.inner.dequeued,
            &stats.inner.queries_started,
            &stats.inner.tasks_completed,
            &stats.inner.queries_succeeded,
            &stats.inner.queries_failed,
            &stats.inner.put_commands,
            &stats.inner.drop_commands,
            &stats.inner.recursive_nodes,
            &stats.inner.recursive_nodes_queued,
            &stats.inner.recursive_nodes_closed_dropped,
            &stats.inner.shutdown_queued_dropped,
            &stats.inner.shutdown_tasks_cancelled,
            &stats.inner.shutdown_recursive_nodes_dropped,
        ];
        for counter in counters {
            counter.store(u64::MAX - 1, Ordering::Relaxed);
            add_saturating(counter, usize::MAX);
            increment_saturating(counter);
            assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
        }
        assert_eq!(
            stats.snapshot(),
            DhtDiscoveredNodeFindStats {
                dequeued: u64::MAX,
                queries_started: u64::MAX,
                tasks_completed: u64::MAX,
                queries_succeeded: u64::MAX,
                queries_failed: u64::MAX,
                put_commands: u64::MAX,
                drop_commands: u64::MAX,
                recursive_nodes: u64::MAX,
                recursive_nodes_queued: u64::MAX,
                recursive_nodes_closed_dropped: u64::MAX,
                shutdown_queued_dropped: u64::MAX,
                shutdown_tasks_cancelled: u64::MAX,
                shutdown_recursive_nodes_dropped: u64::MAX,
            }
        );
        let local = AtomicUsize::new(usize::MAX - 1);
        add_saturating_usize(&local, usize::MAX);
        assert_eq!(local.load(Ordering::Relaxed), usize::MAX);
    }

    #[tokio::test]
    async fn success_ignores_response_id_and_preserves_order_duplicates_scope_and_input() {
        let (sender, mut core, stats, table, mut discovery_receiver, _probe, discovery_stats) =
            core(1, 1, 4);
        let advertised = RoutingNode {
            id: id(2),
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 2)), 6_882),
        };
        let first = node(31, 6_931);
        let second = node(32, 6_932);
        let scoped = scoped_node(33, 6_933, 7);
        let returned = vec![first, second, first, scoped];
        sender.send(advertised).await.unwrap();
        drop(sender);

        let calls = Arc::new(Mutex::new(Vec::new()));
        let calls_for_query = Arc::clone(&calls);
        let returned_by_query = returned.clone();
        let exit = core
            .run_with_query(
                pending(),
                || id(202),
                move |remote, target| {
                    calls_for_query.lock().unwrap().push((remote, target));
                    ready(Ok::<_, ()>(FindNodeResult {
                        id: id(222),
                        nodes: returned_by_query.clone(),
                    }))
                },
            )
            .await;
        assert_eq!(exit, DhtDiscoveredNodeFindWorkerExit::InputClosed);
        assert_eq!(*calls.lock().unwrap(), vec![(advertised.addr, id(202))]);

        let stored = table.node_handle(advertised.id).unwrap();
        assert_eq!(stored.addr(), advertised.addr);
        assert!(stored.last_responded_at().is_some());
        assert!(table.node_handle(id(222)).is_none());

        let mut discovered = Vec::new();
        while let Ok(node) = discovery_receiver.try_recv() {
            discovered.push(node);
        }
        assert_eq!(discovered, returned);
        assert_eq!(
            discovery_stats.snapshot(),
            crate::DhtDiscoveryStats {
                offered: 4,
                queued: 4,
                ..crate::DhtDiscoveryStats::default()
            }
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.queries_succeeded, 1);
        assert_eq!(snapshot.put_commands, 1);
        assert_eq!(snapshot.recursive_nodes, 4);
        assert_eq!(snapshot.recursive_nodes_queued, 4);
        assert_input_closed_conservation(snapshot);
    }

    #[tokio::test]
    async fn query_error_drops_the_advertised_id() {
        let (sender, mut core, stats, table, _receiver, _probe, _discovery_stats) = core(1, 1, 1);
        let advertised = node(3, 6_883);
        assert_ne!(table.put_node(advertised), RoutingPutResult::Rejected);
        sender.send(advertised).await.unwrap();
        drop(sender);

        let exit = core
            .run_with_query(
                pending(),
                || id(203),
                |_, _| ready(Err::<FindNodeResult, _>("oracle find_node failure")),
            )
            .await;
        assert_eq!(exit, DhtDiscoveredNodeFindWorkerExit::InputClosed);
        assert!(table.node_handle(advertised.id).is_none());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.queries_failed, 1);
        assert_eq!(snapshot.drop_commands, 1);
        assert_input_closed_conservation(snapshot);
    }

    #[tokio::test]
    async fn target_is_snapshotted_once_immediately_before_each_query_construction() {
        let (sender, mut core, stats, _table, _receiver, _probe, _discovery_stats) = core(2, 1, 1);
        sender.send(node(4, 6_884)).await.unwrap();
        sender.send(node(5, 6_885)).await.unwrap();
        drop(sender);

        let targets = Arc::new(Mutex::new(VecDeque::from([id(204), id(205)])));
        let events = Arc::new(Mutex::new(Vec::new()));
        let targets_for_reader = Arc::clone(&targets);
        let events_for_reader = Arc::clone(&events);
        let events_for_query = Arc::clone(&events);
        let exit = core
            .run_with_query(
                pending(),
                move || {
                    let target = targets_for_reader.lock().unwrap().pop_front().unwrap();
                    events_for_reader
                        .lock()
                        .unwrap()
                        .push(format!("target:{target}"));
                    target
                },
                move |remote, target| {
                    events_for_query
                        .lock()
                        .unwrap()
                        .push(format!("query:{}:{target}", remote.port()));
                    ready(Ok::<_, ()>(FindNodeResult {
                        id: id(240),
                        nodes: Vec::new(),
                    }))
                },
            )
            .await;
        assert_eq!(exit, DhtDiscoveredNodeFindWorkerExit::InputClosed);
        assert!(targets.lock().unwrap().is_empty());
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                format!("target:{}", id(204)),
                format!("query:6884:{}", id(204)),
                format!("target:{}", id(205)),
                format!("query:6885:{}", id(205)),
            ]
        );
        assert_input_closed_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn eof_waits_for_owned_query_tasks() {
        let (sender, mut core, stats, table, _receiver, _probe, _discovery_stats) = core(2, 2, 1);
        let permits = Arc::new(Semaphore::new(0));
        sender.send(node(6, 6_886)).await.unwrap();
        sender.send(node(7, 6_887)).await.unwrap();
        drop(sender);

        let query_permits = Arc::clone(&permits);
        let worker = tokio::spawn(async move {
            core.run_with_query(
                pending(),
                || id(206),
                move |_, _| {
                    let permits = Arc::clone(&query_permits);
                    async move {
                        let _permit = permits.acquire_owned().await.unwrap();
                        Ok::<_, ()>(FindNodeResult {
                            id: id(241),
                            nodes: Vec::new(),
                        })
                    }
                },
            )
            .await
        });

        wait_for(|| stats.snapshot().queries_started == 2, "two query tasks").await;
        assert!(!worker.is_finished());
        permits.add_permits(2);
        assert_eq!(
            worker.await.unwrap(),
            DhtDiscoveredNodeFindWorkerExit::InputClosed
        );
        assert!(table
            .node_handle(id(6))
            .unwrap()
            .last_responded_at()
            .is_some());
        assert!(table
            .node_handle(id(7))
            .unwrap()
            .last_responded_at()
            .is_some());
        assert_input_closed_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn eof_waits_for_successful_fanout_blocked_by_live_full_discovery() {
        let (sender, mut core, stats, table, mut receiver, probe, discovery_stats) = core(1, 1, 1);
        let sentinel = node(94, 6_994);
        assert_eq!(probe.offer(sentinel), DhtDiscoveryOffer::Queued);
        let advertised = node(22, 6_902);
        let returned = vec![
            node(31, 6_931),
            node(32, 6_932),
            node(31, 6_931),
            scoped_node(33, 6_933, 7),
        ];
        sender.send(advertised).await.unwrap();
        drop(sender);
        let returned_by_query = returned.clone();

        let worker = tokio::spawn(async move {
            core.run_with_query(
                pending(),
                || id(217),
                move |_, _| {
                    ready(Ok::<_, ()>(FindNodeResult {
                        id: id(249),
                        nodes: returned_by_query.clone(),
                    }))
                },
            )
            .await
        });

        wait_for(
            || {
                let snapshot = stats.snapshot();
                snapshot.put_commands == 1
                    && snapshot.recursive_nodes == 4
                    && snapshot.recursive_nodes_queued == 0
                    && snapshot.tasks_completed == 0
            },
            "successful EOF task blocked by full discovery",
        )
        .await;
        assert!(!worker.is_finished());
        assert!(table
            .node_handle(advertised.id)
            .unwrap()
            .last_responded_at()
            .is_some());
        assert_eq!(receiver.recv().await.unwrap(), sentinel);
        for expected in &returned {
            assert_eq!(receiver.recv().await.unwrap(), *expected);
        }
        assert_eq!(
            worker.await.unwrap(),
            DhtDiscoveredNodeFindWorkerExit::InputClosed
        );
        assert_eq!(
            discovery_stats.snapshot(),
            crate::DhtDiscoveryStats {
                offered: 5,
                queued: 5,
                ..crate::DhtDiscoveryStats::default()
            }
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.recursive_nodes, 4);
        assert_eq!(snapshot.recursive_nodes_queued, 4);
        assert_input_closed_conservation(snapshot);
    }

    #[tokio::test]
    async fn synchronous_put_precedes_a_blocked_first_recursive_reservation() {
        let (sender, mut core, stats, table, mut receiver, probe, _discovery_stats) = core(1, 1, 1);
        let sentinel = node(90, 6_990);
        assert_eq!(probe.offer(sentinel), DhtDiscoveryOffer::Queued);
        let advertised = node(8, 6_888);
        let returned = vec![
            node(31, 6_931),
            node(32, 6_932),
            node(31, 6_931),
            scoped_node(33, 6_933, 7),
        ];
        sender.send(advertised).await.unwrap();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();

        let worker = tokio::spawn(async move {
            core.run_with_query(
                async move {
                    let _ = shutdown_receiver.await;
                },
                || id(207),
                move |_, _| {
                    ready(Ok::<_, ()>(FindNodeResult {
                        id: id(242),
                        nodes: returned.clone(),
                    }))
                },
            )
            .await
        });

        wait_for(
            || {
                let snapshot = stats.snapshot();
                snapshot.put_commands == 1 && snapshot.recursive_nodes == 4
            },
            "put before blocked first recursive reservation",
        )
        .await;
        assert!(!worker.is_finished());
        assert_eq!(stats.snapshot().recursive_nodes_queued, 0);
        let stored = table.node_handle(advertised.id).unwrap();
        assert_eq!(stored.addr(), advertised.addr);
        assert!(stored.last_responded_at().is_some());

        let _ = shutdown_sender.send(());
        assert_eq!(
            worker.await.unwrap(),
            DhtDiscoveredNodeFindWorkerExit::Shutdown {
                queued_dropped: 0,
                tasks_cancelled: 1,
                recursive_nodes_dropped: 4,
            }
        );
        assert_eq!(receiver.try_recv().unwrap(), sentinel);
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.queries_succeeded, 1);
        assert_eq!(snapshot.put_commands, 1);
        assert_eq!(snapshot.shutdown_recursive_nodes_dropped, 4);
        assert_shutdown_conservation(snapshot);
    }

    #[tokio::test]
    async fn same_poll_query_success_commits_put_before_biased_shutdown_can_win() {
        let (sender, mut core, stats, table, mut receiver, probe, discovery_stats) = core(1, 1, 1);
        let sentinel = node(91, 6_991);
        assert_eq!(probe.offer(sentinel), DhtDiscoveryOffer::Queued);
        let advertised = node(17, 6_897);
        let returned = vec![
            node(31, 6_931),
            node(32, 6_932),
            node(31, 6_931),
            scoped_node(33, 6_933, 7),
        ];
        sender.send(advertised).await.unwrap();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let mut shutdown_sender = Some(shutdown_sender);

        let exit = core
            .run_with_query(
                async move {
                    let _ = shutdown_receiver.await;
                },
                || id(214),
                move |_, _| {
                    let mut trigger = Some(shutdown_sender.take().unwrap());
                    let returned = returned.clone();
                    poll_fn(move |_| {
                        let _ = trigger.take().unwrap().send(());
                        Poll::Ready(Ok::<_, ()>(FindNodeResult {
                            id: id(246),
                            nodes: returned.clone(),
                        }))
                    })
                },
            )
            .await;

        assert_eq!(
            exit,
            DhtDiscoveredNodeFindWorkerExit::Shutdown {
                queued_dropped: 0,
                tasks_cancelled: 1,
                recursive_nodes_dropped: 4,
            }
        );
        let stored = table.node_handle(advertised.id).unwrap();
        assert_eq!(stored.addr(), advertised.addr);
        assert!(stored.last_responded_at().is_some());
        assert!(table.node_handle(id(246)).is_none());
        assert_eq!(receiver.try_recv().unwrap(), sentinel);
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(
            discovery_stats.snapshot(),
            crate::DhtDiscoveryStats {
                offered: 1,
                queued: 1,
                ..crate::DhtDiscoveryStats::default()
            }
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.queries_succeeded, 1);
        assert_eq!(snapshot.put_commands, 1);
        assert_eq!(snapshot.tasks_completed, 0);
        assert_eq!(snapshot.recursive_nodes, 4);
        assert_eq!(snapshot.recursive_nodes_queued, 0);
        assert_eq!(snapshot.recursive_nodes_closed_dropped, 0);
        assert_eq!(snapshot.shutdown_recursive_nodes_dropped, 4);
        assert_shutdown_conservation(snapshot);
    }

    #[tokio::test]
    async fn shutdown_after_one_recursive_delivery_preserves_prefix_and_drops_suffix() {
        let (sender, mut core, stats, _table, mut receiver, _probe, _discovery_stats) =
            core(1, 1, 1);
        let returned = vec![
            node(31, 6_931),
            node(32, 6_932),
            node(31, 6_931),
            scoped_node(33, 6_933, 7),
        ];
        sender.send(node(9, 6_889)).await.unwrap();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();

        let worker = tokio::spawn(async move {
            core.run_with_query(
                async move {
                    let _ = shutdown_receiver.await;
                },
                || id(208),
                move |_, _| {
                    ready(Ok::<_, ()>(FindNodeResult {
                        id: id(243),
                        nodes: returned.clone(),
                    }))
                },
            )
            .await
        });

        wait_for(
            || {
                let snapshot = stats.snapshot();
                snapshot.recursive_nodes_queued == 1 && snapshot.tasks_completed == 0
            },
            "one recursive prefix and blocked second reservation",
        )
        .await;
        let _ = shutdown_sender.send(());
        assert_eq!(
            worker.await.unwrap(),
            DhtDiscoveredNodeFindWorkerExit::Shutdown {
                queued_dropped: 0,
                tasks_cancelled: 1,
                recursive_nodes_dropped: 3,
            }
        );
        assert_eq!(receiver.try_recv().unwrap(), node(31, 6_931));
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.recursive_nodes, 4);
        assert_eq!(snapshot.recursive_nodes_queued, 1);
        assert_eq!(snapshot.shutdown_recursive_nodes_dropped, 3);
        assert_shutdown_conservation(snapshot);
    }

    #[tokio::test]
    async fn receiver_close_after_one_delivery_accounts_for_the_exact_suffix() {
        let (sender, mut core, stats, _table, receiver, _probe, discovery_stats) = core(1, 1, 1);
        let returned = vec![
            node(31, 6_931),
            node(32, 6_932),
            node(31, 6_931),
            scoped_node(33, 6_933, 7),
        ];
        sender.send(node(10, 6_890)).await.unwrap();
        drop(sender);

        let worker = tokio::spawn(async move {
            core.run_with_query(
                pending(),
                || id(209),
                move |_, _| {
                    ready(Ok::<_, ()>(FindNodeResult {
                        id: id(244),
                        nodes: returned.clone(),
                    }))
                },
            )
            .await
        });
        wait_for(
            || stats.snapshot().recursive_nodes_queued == 1,
            "one recursive delivery before receiver close",
        )
        .await;
        drop(receiver);

        assert_eq!(
            worker.await.unwrap(),
            DhtDiscoveredNodeFindWorkerExit::InputClosed
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.recursive_nodes, 4);
        assert_eq!(snapshot.recursive_nodes_queued, 1);
        assert_eq!(snapshot.recursive_nodes_closed_dropped, 3);
        assert_input_closed_conservation(snapshot);
        assert_eq!(
            discovery_stats.snapshot(),
            crate::DhtDiscoveryStats {
                offered: 1,
                queued: 1,
                ..crate::DhtDiscoveryStats::default()
            }
        );
    }

    #[tokio::test]
    async fn hundred_active_tasks_leave_the_next_hundred_route_queued() {
        let (sender, mut core, stats, _table, _receiver, _probe, _discovery_stats) =
            core(100, 100, 1);
        for value in 1..=100 {
            sender
                .send(node(value, 7_000 + u16::from(value)))
                .await
                .unwrap();
        }
        let target_reads = Arc::new(AtomicUsize::new(0));
        let target_reads_for_worker = Arc::clone(&target_reads);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let worker = tokio::spawn(async move {
            core.run_with_query(
                async move {
                    let _ = shutdown_receiver.await;
                },
                move || {
                    target_reads_for_worker.fetch_add(1, AtomicOrdering::SeqCst);
                    id(210)
                },
                |_, _| pending::<Result<FindNodeResult, ()>>(),
            )
            .await
        });

        wait_for(
            || stats.snapshot().queries_started == 100,
            "one hundred active find-node tasks",
        )
        .await;
        for value in 101..=200 {
            sender
                .try_send(node(value, 7_000 + u16::from(value)))
                .unwrap();
        }
        assert!(matches!(
            sender.try_send(node(201, 7_201)),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_))
        ));
        let _ = shutdown_sender.send(());
        assert_eq!(
            worker.await.unwrap(),
            DhtDiscoveredNodeFindWorkerExit::Shutdown {
                queued_dropped: 100,
                tasks_cancelled: 100,
                recursive_nodes_dropped: 0,
            }
        );
        assert_eq!(target_reads.load(AtomicOrdering::SeqCst), 100);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.shutdown_queued_dropped, 100);
        assert_eq!(snapshot.shutdown_tasks_cancelled, 100);
        assert_eq!(
            200,
            snapshot
                .dequeued
                .saturating_add(snapshot.shutdown_queued_dropped)
        );
        assert_shutdown_conservation(snapshot);
    }

    #[tokio::test]
    async fn already_ready_shutdown_beats_target_read_query_and_intake() {
        let (sender, mut core, stats, _table, _receiver, _probe, _discovery_stats) = core(1, 1, 1);
        sender.send(node(11, 6_891)).await.unwrap();
        let target_reads = Arc::new(AtomicUsize::new(0));
        let query_calls = Arc::new(AtomicUsize::new(0));
        let target_reads_for_worker = Arc::clone(&target_reads);
        let query_calls_for_worker = Arc::clone(&query_calls);

        let exit = core
            .run_with_query(
                ready(()),
                move || {
                    target_reads_for_worker.fetch_add(1, AtomicOrdering::SeqCst);
                    id(211)
                },
                move |_, _| {
                    query_calls_for_worker.fetch_add(1, AtomicOrdering::SeqCst);
                    ready(Ok::<_, ()>(FindNodeResult {
                        id: id(245),
                        nodes: Vec::new(),
                    }))
                },
            )
            .await;
        assert_eq!(
            exit,
            DhtDiscoveredNodeFindWorkerExit::Shutdown {
                queued_dropped: 1,
                tasks_cancelled: 0,
                recursive_nodes_dropped: 0,
            }
        );
        assert_eq!(target_reads.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(query_calls.load(AtomicOrdering::SeqCst), 0);
        assert!(sender.send(node(12, 6_892)).await.is_err());
        assert_eq!(
            stats.snapshot(),
            DhtDiscoveredNodeFindStats {
                shutdown_queued_dropped: 1,
                ..DhtDiscoveredNodeFindStats::default()
            }
        );
    }

    #[tokio::test]
    async fn dropping_the_core_closes_intake_and_aborts_owned_tasks() {
        let (sender, mut core, stats, _table, _receiver, _probe, _discovery_stats) = core(1, 1, 1);
        sender.send(node(13, 6_893)).await.unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        let dropped_by_query = Arc::clone(&dropped);
        let worker = tokio::spawn(async move {
            core.run_with_query(
                pending(),
                || id(212),
                move |_, _| {
                    let guard = DropFlag(Arc::clone(&dropped_by_query));
                    async move {
                        let _guard = guard;
                        pending::<Result<FindNodeResult, ()>>().await
                    }
                },
            )
            .await
        });
        wait_for(
            || stats.snapshot().queries_started == 1,
            "one owned find-node task",
        )
        .await;
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        wait_for(
            || dropped.load(AtomicOrdering::SeqCst) == 1,
            "aborted query future drop",
        )
        .await;
        assert!(sender.send(node(14, 6_894)).await.is_err());
    }

    #[tokio::test]
    async fn dropping_core_during_blocked_fanout_does_not_report_graceful_shutdown() {
        let (sender, mut core, stats, table, _receiver, probe, _discovery_stats) = core(1, 1, 1);
        assert_eq!(probe.offer(node(92, 6_992)), DhtDiscoveryOffer::Queued);
        let advertised = node(18, 6_898);
        sender.send(advertised).await.unwrap();
        let returned = vec![
            node(31, 6_931),
            node(32, 6_932),
            node(31, 6_931),
            scoped_node(33, 6_933, 7),
        ];
        let worker = tokio::spawn(async move {
            core.run_with_query(
                pending(),
                || id(215),
                move |_, _| {
                    ready(Ok::<_, ()>(FindNodeResult {
                        id: id(247),
                        nodes: returned.clone(),
                    }))
                },
            )
            .await
        });

        wait_for(
            || {
                let snapshot = stats.snapshot();
                snapshot.recursive_nodes == 4
                    && snapshot.recursive_nodes_queued == 0
                    && snapshot.tasks_completed == 0
            },
            "fanout-blocked task before core drop",
        )
        .await;
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(sender.send(node(19, 6_899)).await.is_err());
        assert!(table
            .node_handle(advertised.id)
            .unwrap()
            .last_responded_at()
            .is_some());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.shutdown_queued_dropped, 0);
        assert_eq!(snapshot.shutdown_tasks_cancelled, 0);
        assert_eq!(snapshot.shutdown_recursive_nodes_dropped, 0);
    }

    #[tokio::test]
    async fn child_panic_aborts_and_drains_sibling_then_resumes_payload() {
        let (sender, mut core, stats, _table, _receiver, _probe, _discovery_stats) = core(2, 2, 1);
        sender.send(node(15, 6_895)).await.unwrap();
        sender.send(node(16, 6_896)).await.unwrap();
        drop(sender);
        let panic_gate = Arc::new(Semaphore::new(0));
        let sibling_started = Arc::new(AtomicUsize::new(0));
        let sibling_dropped = Arc::new(AtomicUsize::new(0));
        let panic_gate_for_query = Arc::clone(&panic_gate);
        let sibling_started_for_query = Arc::clone(&sibling_started);
        let sibling_dropped_for_query = Arc::clone(&sibling_dropped);

        let worker = tokio::spawn(async move {
            core.run_with_query(
                pending(),
                || id(213),
                move |remote, _| {
                    let panic_gate = Arc::clone(&panic_gate_for_query);
                    let sibling_started = Arc::clone(&sibling_started_for_query);
                    let sibling_dropped = Arc::clone(&sibling_dropped_for_query);
                    async move {
                        if remote.port() == 6_895 {
                            let _permit = panic_gate.acquire_owned().await.unwrap();
                            panic!("find-node child panic payload");
                        }
                        sibling_started.fetch_add(1, AtomicOrdering::SeqCst);
                        let _guard = DropFlag(sibling_dropped);
                        pending::<Result<FindNodeResult, ()>>().await
                    }
                },
            )
            .await
        });

        wait_for(
            || {
                stats.snapshot().queries_started == 2
                    && sibling_started.load(AtomicOrdering::SeqCst) == 1
            },
            "panic task and pending sibling",
        )
        .await;
        panic_gate.add_permits(1);
        let error = worker.await.unwrap_err();
        assert!(error.is_panic());
        let payload = error.into_panic();
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"find-node child panic payload")
        );
        assert_eq!(sibling_dropped.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn child_panic_aborting_blocked_fanout_does_not_report_graceful_shutdown() {
        let (sender, mut core, stats, table, _receiver, probe, discovery_stats) = core(2, 2, 1);
        let sentinel = node(93, 6_993);
        assert_eq!(probe.offer(sentinel), DhtDiscoveryOffer::Queued);
        let advertised = node(20, 6_900);
        sender.send(advertised).await.unwrap();
        sender.send(node(21, 6_901)).await.unwrap();
        drop(sender);
        let panic_gate = Arc::new(Semaphore::new(0));
        let panic_gate_for_query = Arc::clone(&panic_gate);
        let returned = vec![
            node(31, 6_931),
            node(32, 6_932),
            node(31, 6_931),
            scoped_node(33, 6_933, 7),
        ];

        let worker = tokio::spawn(async move {
            core.run_with_query(
                pending(),
                || id(216),
                move |remote, _| {
                    let panic_gate = Arc::clone(&panic_gate_for_query);
                    let returned = returned.clone();
                    async move {
                        if remote.port() == 6_901 {
                            let _permit = panic_gate.acquire_owned().await.unwrap();
                            panic!("find-node fanout sibling panic payload");
                        }
                        Ok::<_, ()>(FindNodeResult {
                            id: id(248),
                            nodes: returned,
                        })
                    }
                },
            )
            .await
        });

        wait_for(
            || {
                let snapshot = stats.snapshot();
                snapshot.queries_started == 2
                    && snapshot.recursive_nodes == 4
                    && snapshot.recursive_nodes_queued == 0
                    && snapshot.tasks_completed == 0
            },
            "fanout-blocked sibling before child panic",
        )
        .await;
        panic_gate.add_permits(1);
        let error = worker.await.unwrap_err();
        assert!(error.is_panic());
        let payload = error.into_panic();
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"find-node fanout sibling panic payload")
        );
        assert!(table
            .node_handle(advertised.id)
            .unwrap()
            .last_responded_at()
            .is_some());
        assert_eq!(
            discovery_stats.snapshot(),
            crate::DhtDiscoveryStats {
                offered: 1,
                queued: 1,
                ..crate::DhtDiscoveryStats::default()
            }
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.shutdown_queued_dropped, 0);
        assert_eq!(snapshot.shutdown_tasks_cancelled, 0);
        assert_eq!(snapshot.shutdown_recursive_nodes_dropped, 0);
    }
}
