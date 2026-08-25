use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::resume_unwind;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::task::{JoinError, JoinSet};

use crate::{
    DhtDiscoveredNodeRouteReceiver, DhtRuntimeClient, Id20, KTable, KTableCommand,
    KTableNodeOption, PingResult, RoutingNode,
};

const DEFAULT_MAX_INFLIGHT: NonZeroUsize = NonZeroUsize::new(10).unwrap();

/// Concurrency bound for discovered-node ping queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtDiscoveredNodePingWorkerConfig {
    pub max_inflight: NonZeroUsize,
}

impl Default for DhtDiscoveredNodePingWorkerConfig {
    fn default() -> Self {
        Self {
            max_inflight: DEFAULT_MAX_INFLIGHT,
        }
    }
}

/// Terminal state of the owned discovered-node ping worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtDiscoveredNodePingWorkerExit {
    /// Every route producer is gone and every accepted query completed.
    InputClosed,
    /// Caller shutdown won before another completion or route receive.
    Shutdown {
        /// Nodes synchronously drained from the closed route queue.
        queued_dropped: usize,
        /// Accepted query tasks whose abort was observed while joining.
        queries_cancelled: usize,
    },
}

#[derive(Default)]
struct DhtDiscoveredNodePingStatsInner {
    dequeued: AtomicU64,
    queries_started: AtomicU64,
    queries_succeeded: AtomicU64,
    queries_failed: AtomicU64,
    id_mismatches: AtomicU64,
    put_commands: AtomicU64,
    drop_commands: AtomicU64,
    shutdown_queued_dropped: AtomicU64,
    shutdown_queries_cancelled: AtomicU64,
}

/// Cloneable, sender-free view of discovered-node ping counters.
#[derive(Clone, Default)]
pub struct DhtDiscoveredNodePingStatsHandle {
    inner: Arc<DhtDiscoveredNodePingStatsInner>,
}

/// One non-transactional snapshot of monotonic ping-worker counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtDiscoveredNodePingStats {
    pub dequeued: u64,
    /// Query futures accepted into the owned in-flight set. This is not a
    /// count of rate-limit admissions or UDP datagrams sent.
    pub queries_started: u64,
    /// Successful client responses, including responses with a mismatching ID.
    pub queries_succeeded: u64,
    /// Client futures that returned an error before a table command.
    pub queries_failed: u64,
    pub id_mismatches: u64,
    /// Attempted Go-compatible table commands, including rejected/no-op puts.
    pub put_commands: u64,
    /// Attempted Go-compatible table commands, including absent zero-ID drops.
    pub drop_commands: u64,
    pub shutdown_queued_dropped: u64,
    /// Unresolved local futures observed cancelled during graceful shutdown;
    /// this does not prove that no datagram was already sent.
    pub shutdown_queries_cancelled: u64,
}

impl DhtDiscoveredNodePingStatsHandle {
    /// Read each monotonic counter independently with relaxed ordering.
    ///
    /// Cross-field invariants are guaranteed only after the worker terminates.
    #[must_use]
    pub fn snapshot(&self) -> DhtDiscoveredNodePingStats {
        DhtDiscoveredNodePingStats {
            dequeued: self.inner.dequeued.load(Ordering::Relaxed),
            queries_started: self.inner.queries_started.load(Ordering::Relaxed),
            queries_succeeded: self.inner.queries_succeeded.load(Ordering::Relaxed),
            queries_failed: self.inner.queries_failed.load(Ordering::Relaxed),
            id_mismatches: self.inner.id_mismatches.load(Ordering::Relaxed),
            put_commands: self.inner.put_commands.load(Ordering::Relaxed),
            drop_commands: self.inner.drop_commands.load(Ordering::Relaxed),
            shutdown_queued_dropped: self.inner.shutdown_queued_dropped.load(Ordering::Relaxed),
            shutdown_queries_cancelled: self
                .inner
                .shutdown_queries_cancelled
                .load(Ordering::Relaxed),
        }
    }
}

/// Owned, bounded consumer for the discovered-node ping route.
///
/// Route items are state-free [`RoutingNode`] values, matching Go's fresh
/// `nodeBase` inputs. Dropped/recent live-handle rechecks belong to the future
/// old-node maintenance producer and are deliberately not inferred by KTable
/// ID lookup here.
///
/// At most `max_inflight` queries are accepted at once. The route receiver is
/// not polled at capacity, so no extra item is held outside the route queue.
/// All query tasks remain owned and are aborted if this worker or its `run`
/// future is dropped.
pub struct DhtDiscoveredNodePingWorker {
    client: DhtRuntimeClient,
    core: DhtDiscoveredNodePingWorkerCore,
}

impl DhtDiscoveredNodePingWorker {
    /// Construct the production-compatible ten-query worker.
    #[must_use]
    pub fn new(
        input: DhtDiscoveredNodeRouteReceiver,
        client: DhtRuntimeClient,
        table: KTable,
    ) -> (Self, DhtDiscoveredNodePingStatsHandle) {
        Self::with_config(
            input,
            client,
            table,
            DhtDiscoveredNodePingWorkerConfig::default(),
        )
    }

    /// Construct a ping worker with an explicit concurrency bound.
    #[must_use]
    pub fn with_config(
        input: DhtDiscoveredNodeRouteReceiver,
        client: DhtRuntimeClient,
        table: KTable,
        config: DhtDiscoveredNodePingWorkerConfig,
    ) -> (Self, DhtDiscoveredNodePingStatsHandle) {
        let stats = DhtDiscoveredNodePingStatsHandle::default();
        (
            Self {
                client,
                core: DhtDiscoveredNodePingWorkerCore::new(
                    input,
                    table,
                    config.max_inflight,
                    stats.clone(),
                ),
            },
            stats,
        )
    }

    #[cfg(test)]
    pub(crate) const fn config_for_test(&self) -> DhtDiscoveredNodePingWorkerConfig {
        DhtDiscoveredNodePingWorkerConfig {
            max_inflight: self.core.max_inflight,
        }
    }

    /// Run until route EOF or caller shutdown.
    ///
    /// Shutdown is biased ahead of a ready child join and route receive. EOF
    /// stops intake and joins every accepted query. Explicit shutdown closes
    /// and drains queued input, aborts all accepted queries, and awaits every
    /// cancellation before returning exact counts.
    pub async fn run<F>(mut self, shutdown: F) -> DhtDiscoveredNodePingWorkerExit
    where
        F: Future<Output = ()>,
    {
        let client = self.client.clone();
        self.core
            .run_with_query(shutdown, move |remote| {
                let client = client.clone();
                async move { client.ping(remote).await }
            })
            .await
    }
}

struct DhtDiscoveredNodePingWorkerCore {
    input: DhtDiscoveredNodeRouteReceiver,
    table: KTable,
    max_inflight: NonZeroUsize,
    tasks: JoinSet<()>,
    stats: DhtDiscoveredNodePingStatsHandle,
}

impl DhtDiscoveredNodePingWorkerCore {
    fn new(
        input: DhtDiscoveredNodeRouteReceiver,
        table: KTable,
        max_inflight: NonZeroUsize,
        stats: DhtDiscoveredNodePingStatsHandle,
    ) -> Self {
        Self {
            input,
            table,
            max_inflight,
            tasks: JoinSet::new(),
            stats,
        }
    }

    async fn run_with_query<F, Q, QF, E>(
        &mut self,
        shutdown: F,
        mut query: Q,
    ) -> DhtDiscoveredNodePingWorkerExit
    where
        F: Future<Output = ()>,
        Q: FnMut(std::net::SocketAddr) -> QF,
        QF: Future<Output = Result<PingResult, E>> + Send + 'static,
        E: Send + 'static,
    {
        tokio::pin!(shutdown);
        let mut input_closed = false;

        loop {
            if input_closed && self.tasks.is_empty() {
                return DhtDiscoveredNodePingWorkerExit::InputClosed;
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
                    Event::Joined(joined.expect("guarded ping query remains present"))
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
                    let query_future = query(node.addr);
                    let table = self.table.clone();
                    let stats = self.stats.clone();
                    self.tasks.spawn(async move {
                        finish_ping_query(node, query_future.await, &table, &stats);
                    });
                    increment_saturating(&self.stats.inner.queries_started);
                }
                Event::Input(None) => input_closed = true,
            }
        }
    }

    async fn finish_shutdown(&mut self) -> DhtDiscoveredNodePingWorkerExit {
        self.input.close();
        let mut queued_dropped = 0usize;
        while self.input.try_recv().is_ok() {
            queued_dropped = queued_dropped.saturating_add(1);
        }
        add_saturating(&self.stats.inner.shutdown_queued_dropped, queued_dropped);

        self.tasks.abort_all();
        let mut queries_cancelled = 0usize;
        let mut first_panic = None;
        while let Some(joined) = self.tasks.join_next().await {
            match joined {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {
                    queries_cancelled = queries_cancelled.saturating_add(1);
                }
                Err(error) if error.is_panic() => {
                    if first_panic.is_none() {
                        first_panic = Some(error.into_panic());
                    }
                }
                Err(error) => panic!("unexpected ping query join error: {error}"),
            }
        }
        add_saturating(
            &self.stats.inner.shutdown_queries_cancelled,
            queries_cancelled,
        );
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
        DhtDiscoveredNodePingWorkerExit::Shutdown {
            queued_dropped,
            queries_cancelled,
        }
    }

    async fn finish_abnormal_join(&mut self, error: JoinError) -> ! {
        let panic_payload = error.is_panic().then(|| error.into_panic());
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
        if let Some(payload) = panic_payload {
            resume_unwind(payload);
        }
        panic!("ping query task was cancelled outside worker cleanup")
    }
}

impl Drop for DhtDiscoveredNodePingWorkerCore {
    fn drop(&mut self) {
        self.input.close();
        self.tasks.abort_all();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PingDecision {
    Put { id: Id20 },
    Drop { id: Id20, mismatch: bool },
}

fn ping_decision(input_id: Id20, response: Result<Id20, ()>) -> PingDecision {
    match response {
        Ok(response_id) if input_id != Id20::ZERO && input_id != response_id => {
            PingDecision::Drop {
                id: input_id,
                mismatch: true,
            }
        }
        Ok(response_id) => PingDecision::Put { id: response_id },
        Err(()) => PingDecision::Drop {
            id: Id20::ZERO,
            mismatch: false,
        },
    }
}

fn finish_ping_query<E>(
    input: RoutingNode,
    result: Result<PingResult, E>,
    table: &KTable,
    stats: &DhtDiscoveredNodePingStatsHandle,
) {
    let response = match result {
        Ok(response) => {
            increment_saturating(&stats.inner.queries_succeeded);
            Ok(response.id)
        }
        Err(_) => {
            increment_saturating(&stats.inner.queries_failed);
            Err(())
        }
    };

    match ping_decision(input.id, response) {
        PingDecision::Put { id } => {
            table.batch_command(&[KTableCommand::PutNode {
                node: RoutingNode {
                    id,
                    addr: input.addr,
                },
                options: vec![KTableNodeOption::Responded],
            }]);
            increment_saturating(&stats.inner.put_commands);
        }
        PingDecision::Drop { id, mismatch } => {
            if mismatch {
                increment_saturating(&stats.inner.id_mismatches);
            }
            table.batch_command(&[KTableCommand::DropNode { id }]);
            increment_saturating(&stats.inner.drop_commands);
        }
    }
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

#[cfg(test)]
#[path = "dht_discovered_node_ping_worker_parity.rs"]
mod parity_tests;

#[cfg(test)]
mod tests {
    use std::future::{pending, ready};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use tokio::sync::{oneshot, Semaphore};

    use super::*;

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

    fn core(
        capacity: usize,
        max_inflight: usize,
    ) -> (
        tokio::sync::mpsc::Sender<RoutingNode>,
        DhtDiscoveredNodePingWorkerCore,
        DhtDiscoveredNodePingStatsHandle,
        KTable,
    ) {
        let (sender, receiver) = DhtDiscoveredNodeRouteReceiver::test_channel(capacity);
        let table = KTable::new(id(250));
        let stats = DhtDiscoveredNodePingStatsHandle::default();
        (
            sender,
            DhtDiscoveredNodePingWorkerCore::new(
                receiver,
                table.clone(),
                NonZeroUsize::new(max_inflight).unwrap(),
                stats.clone(),
            ),
            stats,
            table,
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

    fn assert_input_closed_conservation(stats: DhtDiscoveredNodePingStats) {
        assert_eq!(stats.dequeued, stats.queries_started);
        assert_eq!(
            stats.queries_started,
            stats.queries_succeeded + stats.queries_failed
        );
        assert_eq!(
            stats.put_commands + stats.drop_commands,
            stats.queries_succeeded + stats.queries_failed
        );
        assert_eq!(
            stats.put_commands,
            stats.queries_succeeded - stats.id_mismatches
        );
        assert_eq!(
            stats.drop_commands,
            stats.queries_failed + stats.id_mismatches
        );
        assert_eq!(stats.shutdown_queued_dropped, 0);
        assert_eq!(stats.shutdown_queries_cancelled, 0);
    }

    struct DropFlag(Arc<AtomicUsize>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.fetch_add(1, AtomicOrdering::SeqCst);
        }
    }

    #[test]
    fn defaults_and_go_command_decisions_are_fixed() {
        assert_eq!(
            DhtDiscoveredNodePingWorkerConfig::default().max_inflight,
            NonZeroUsize::new(10).unwrap()
        );
        assert_eq!(
            ping_decision(Id20::ZERO, Ok(id(1))),
            PingDecision::Put { id: id(1) }
        );
        assert_eq!(
            ping_decision(id(2), Ok(id(2))),
            PingDecision::Put { id: id(2) }
        );
        assert_eq!(
            ping_decision(id(2), Ok(id(3))),
            PingDecision::Drop {
                id: id(2),
                mismatch: true,
            }
        );
        assert_eq!(
            ping_decision(id(2), Err(())),
            PingDecision::Drop {
                id: Id20::ZERO,
                mismatch: false,
            }
        );
    }

    #[tokio::test]
    async fn go_outcomes_mutate_the_table_and_conserve_terminal_stats() {
        let (sender, mut core, stats, table) = core(8, 4);
        let mismatch = node(5, 6_885);
        let failed = node(6, 6_886);
        assert_ne!(table.put_node(mismatch), crate::RoutingPutResult::Rejected);
        assert_ne!(table.put_node(failed), crate::RoutingPutResult::Rejected);

        for item in [node(0, 6_883), node(4, 6_884), mismatch, failed] {
            sender.send(item).await.unwrap();
        }
        drop(sender);

        let exit = core
            .run_with_query(pending(), |remote| async move {
                match remote.port() {
                    6_883 => Ok(PingResult { id: id(3) }),
                    6_884 => Ok(PingResult { id: id(4) }),
                    6_885 => Ok(PingResult { id: id(55) }),
                    6_886 => Err("oracle ping failure"),
                    port => panic!("unexpected ping port {port}"),
                }
            })
            .await;
        assert_eq!(exit, DhtDiscoveredNodePingWorkerExit::InputClosed);

        let learned = table.node_handle(id(3)).unwrap();
        assert_eq!(learned.addr(), node(0, 6_883).addr);
        assert!(learned.last_responded_at().is_some());
        let matched = table.node_handle(id(4)).unwrap();
        assert!(matched.last_responded_at().is_some());
        assert!(table.node_handle(id(5)).is_none());
        assert!(table.node_handle(id(55)).is_none());
        assert!(table.node_handle(id(6)).is_some());
        assert!(table.node_handle(Id20::ZERO).is_none());

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.queries_succeeded, 3);
        assert_eq!(snapshot.queries_failed, 1);
        assert_eq!(snapshot.id_mismatches, 1);
        assert_eq!(snapshot.put_commands, 2);
        assert_eq!(snapshot.drop_commands, 2);
        assert_input_closed_conservation(snapshot);
    }

    #[tokio::test]
    async fn eof_waits_for_owned_queries_before_returning() {
        let (sender, mut core, stats, table) = core(2, 2);
        let permits = Arc::new(Semaphore::new(0));
        sender.send(node(8, 6_888)).await.unwrap();
        sender.send(node(9, 6_889)).await.unwrap();
        drop(sender);

        let query_permits = Arc::clone(&permits);
        let worker = tokio::spawn(async move {
            core.run_with_query(pending(), move |remote| {
                let permits = Arc::clone(&query_permits);
                async move {
                    let _permit = permits.acquire_owned().await.unwrap();
                    Ok::<_, ()>(PingResult {
                        id: id(u8::try_from(remote.port() - 6_880).unwrap()),
                    })
                }
            })
            .await
        });

        wait_for(|| stats.snapshot().queries_started == 2, "two ping queries").await;
        assert!(!worker.is_finished());
        permits.add_permits(2);
        assert_eq!(
            worker.await.unwrap(),
            DhtDiscoveredNodePingWorkerExit::InputClosed
        );
        assert!(table.node_handle(id(8)).is_some());
        assert!(table.node_handle(id(9)).is_some());
        assert_input_closed_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn completion_triggered_shutdown_preserves_the_committed_put() {
        let (sender, mut core, stats, table) = core(1, 1);
        sender.send(node(10, 6_890)).await.unwrap();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let mut shutdown_sender = Some(shutdown_sender);

        let exit = core
            .run_with_query(
                async move {
                    let _ = shutdown_receiver.await;
                },
                move |_| {
                    let shutdown_sender = shutdown_sender.take().unwrap();
                    async move {
                        let _ = shutdown_sender.send(());
                        Ok::<_, ()>(PingResult { id: id(10) })
                    }
                },
            )
            .await;

        assert_eq!(
            exit,
            DhtDiscoveredNodePingWorkerExit::Shutdown {
                queued_dropped: 0,
                queries_cancelled: 0,
            }
        );
        assert!(table.node_handle(id(10)).is_some());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.queries_succeeded, 1);
        assert_eq!(snapshot.put_commands, 1);
        assert_eq!(snapshot.shutdown_queries_cancelled, 0);
    }

    #[tokio::test]
    async fn already_ready_shutdown_beats_intake_and_drains_the_queue() {
        let (sender, mut core, stats, _table) = core(1, 1);
        sender.send(node(11, 6_891)).await.unwrap();
        let query_calls = Arc::new(AtomicUsize::new(0));
        let query_calls_for_worker = Arc::clone(&query_calls);

        let exit = core
            .run_with_query(ready(()), move |_| {
                query_calls_for_worker.fetch_add(1, AtomicOrdering::SeqCst);
                ready(Ok::<_, ()>(PingResult { id: id(11) }))
            })
            .await;
        assert_eq!(
            exit,
            DhtDiscoveredNodePingWorkerExit::Shutdown {
                queued_dropped: 1,
                queries_cancelled: 0,
            }
        );
        assert_eq!(query_calls.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(
            stats.snapshot(),
            DhtDiscoveredNodePingStats {
                shutdown_queued_dropped: 1,
                ..DhtDiscoveredNodePingStats::default()
            }
        );
    }

    #[tokio::test]
    async fn ten_active_queries_leave_the_next_ten_in_the_route_queue() {
        let (sender, mut core, stats, _table) = core(10, 10);
        let dropped = Arc::new(AtomicUsize::new(0));
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        for value in 1..=10 {
            sender
                .send(node(value, 7_000 + u16::from(value)))
                .await
                .unwrap();
        }

        let dropped_by_queries = Arc::clone(&dropped);
        let worker = tokio::spawn(async move {
            core.run_with_query(
                async move {
                    let _ = shutdown_receiver.await;
                },
                move |_| {
                    let guard = DropFlag(Arc::clone(&dropped_by_queries));
                    async move {
                        let _guard = guard;
                        pending::<Result<PingResult, ()>>().await
                    }
                },
            )
            .await
        });

        wait_for(
            || stats.snapshot().queries_started == 10,
            "ten active ping queries",
        )
        .await;
        for value in 11..=20 {
            sender
                .try_send(node(value, 7_000 + u16::from(value)))
                .unwrap();
        }
        let _ = shutdown_sender.send(());
        assert_eq!(
            worker.await.unwrap(),
            DhtDiscoveredNodePingWorkerExit::Shutdown {
                queued_dropped: 10,
                queries_cancelled: 10,
            }
        );
        assert_eq!(dropped.load(AtomicOrdering::SeqCst), 10);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.dequeued, 10);
        assert_eq!(snapshot.queries_started, 10);
        assert_eq!(snapshot.queries_succeeded, 0);
        assert_eq!(snapshot.queries_failed, 0);
        assert_eq!(snapshot.shutdown_queued_dropped, 10);
        assert_eq!(snapshot.shutdown_queries_cancelled, 10);
        assert_eq!(
            snapshot.queries_started,
            snapshot.queries_succeeded
                + snapshot.queries_failed
                + snapshot.shutdown_queries_cancelled
        );
        assert_eq!(20, snapshot.dequeued + snapshot.shutdown_queued_dropped);
    }

    #[tokio::test]
    async fn one_completion_frees_exactly_one_query_slot() {
        let (sender, mut core, stats, _table) = core(2, 1);
        let first_permit = Arc::new(Semaphore::new(0));
        sender.send(node(40, 7_040)).await.unwrap();
        sender.send(node(41, 7_041)).await.unwrap();
        drop(sender);

        let query_permit = Arc::clone(&first_permit);
        let worker = tokio::spawn(async move {
            core.run_with_query(pending(), move |remote| {
                let permit = Arc::clone(&query_permit);
                async move {
                    if remote.port() == 7_040 {
                        let _permit = permit.acquire_owned().await.unwrap();
                    }
                    Ok::<_, ()>(PingResult {
                        id: id(u8::try_from(remote.port() - 7_000).unwrap()),
                    })
                }
            })
            .await
        });

        wait_for(
            || stats.snapshot().queries_started == 1,
            "first bounded ping query",
        )
        .await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(stats.snapshot().queries_started, 1);
        first_permit.add_permits(1);
        wait_for(
            || stats.snapshot().queries_started == 2,
            "second ping query after one completion",
        )
        .await;
        assert_eq!(
            worker.await.unwrap(),
            DhtDiscoveredNodePingWorkerExit::InputClosed
        );
        assert_input_closed_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn child_panic_aborts_and_drains_its_owned_sibling() {
        let (sender, mut core, stats, _table) = core(2, 2);
        let permits = Arc::new(Semaphore::new(0));
        let sibling_dropped = Arc::new(AtomicUsize::new(0));
        sender.send(node(21, 7_021)).await.unwrap();
        sender.send(node(22, 7_022)).await.unwrap();
        drop(sender);

        let query_permits = Arc::clone(&permits);
        let query_sibling_dropped = Arc::clone(&sibling_dropped);
        let worker = tokio::spawn(async move {
            core.run_with_query(pending(), move |remote| {
                let permits = Arc::clone(&query_permits);
                let guard =
                    (remote.port() == 7_022).then(|| DropFlag(Arc::clone(&query_sibling_dropped)));
                async move {
                    let _permit = permits.acquire_owned().await.unwrap();
                    if remote.port() == 7_021 {
                        panic!("ping child sentinel");
                    }
                    let _guard = guard;
                    pending::<Result<PingResult, ()>>().await
                }
            })
            .await
        });

        wait_for(
            || stats.snapshot().queries_started == 2,
            "two owned queries",
        )
        .await;
        permits.add_permits(2);
        let error = worker.await.unwrap_err();
        assert!(error.is_panic());
        assert_eq!(sibling_dropped.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dropping_the_core_closes_intake_and_aborts_tasks() {
        let (sender, mut core, _stats, _table) = core(1, 1);
        let dropped = Arc::new(AtomicUsize::new(0));
        let guard = DropFlag(Arc::clone(&dropped));
        core.tasks.spawn(async move {
            let _guard = guard;
            pending::<()>().await;
        });

        drop(core);
        sender.closed().await;
        wait_for(
            || dropped.load(AtomicOrdering::SeqCst) == 1,
            "aborted task future drop",
        )
        .await;
    }

    #[test]
    fn command_counters_include_table_no_ops_and_saturate() {
        let stats = DhtDiscoveredNodePingStatsHandle::default();
        let table = KTable::new(id(30));
        finish_ping_query(
            node(0, 7_031),
            Ok::<_, ()>(PingResult { id: id(30) }),
            &table,
            &stats,
        );
        finish_ping_query(node(32, 7_032), Err::<PingResult, _>(()), &table, &stats);
        assert_eq!(table.node_count(), 0);
        assert_eq!(stats.snapshot().put_commands, 1);
        assert_eq!(stats.snapshot().drop_commands, 1);

        stats.inner.put_commands.store(u64::MAX, Ordering::Relaxed);
        increment_saturating(&stats.inner.put_commands);
        add_saturating(&stats.inner.put_commands, usize::MAX);
        assert_eq!(stats.snapshot().put_commands, u64::MAX);
    }
}
