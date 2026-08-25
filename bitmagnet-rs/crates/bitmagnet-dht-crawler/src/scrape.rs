//! Owned execution of the crawler's DHT BEP-33 scrape stage.

use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::panic::resume_unwind;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bitmagnet_dht::{
    DhtDiscoveryOffer, DhtDiscoverySender, DhtInfoHashTriageRequest, DhtRuntimeClient,
    DhtScrapeReceiver, GetPeersScrapeResult, Id20, KTable, KTableCommand, KTableNodeOption,
    RoutingNode,
};
use tokio::task::{JoinError, JoinSet};

use crate::{DhtPersistSourceInput, DhtPersistSourceRequest};

const DEFAULT_MAX_INFLIGHT: NonZeroUsize = NonZeroUsize::new(200).unwrap();
const RECURSIVE_FANOUT_TIMEOUT: Duration = Duration::from_secs(1);

/// Concurrency bound for accepted BEP-33 scrape work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtScrapeWorkerConfig {
    pub max_inflight: NonZeroUsize,
}

impl Default for DhtScrapeWorkerConfig {
    fn default() -> Self {
        Self {
            max_inflight: DEFAULT_MAX_INFLIGHT,
        }
    }
}

/// Terminal state of the owned BEP-33 scrape worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtScrapeWorkerExit {
    /// Every shared-route producer is gone and every accepted task completed.
    InputClosed,
    /// Caller shutdown won before another completion or route receive.
    Shutdown {
        /// Requests drained from the closed input route.
        queued_dropped: usize,
        /// Accepted tasks whose abort was observed while joining.
        tasks_cancelled: usize,
        /// Ordered response-node suffixes abandoned by cancelled tasks.
        recursive_nodes_dropped: usize,
        /// Future scraped-source requests abandoned by cancelled tasks.
        persist_source_requests_dropped: usize,
    },
}

#[derive(Default)]
struct DhtScrapeWorkerStatsInner {
    dequeued: AtomicU64,
    queries_started: AtomicU64,
    tasks_completed: AtomicU64,
    queries_succeeded: AtomicU64,
    queries_failed: AtomicU64,
    put_node_commands: AtomicU64,
    drop_addr_commands: AtomicU64,
    peer_values_ignored: AtomicU64,
    recursive_nodes: AtomicU64,
    recursive_nodes_queued: AtomicU64,
    recursive_nodes_closed_dropped: AtomicU64,
    recursive_nodes_timed_out_dropped: AtomicU64,
    persist_source_queued: AtomicU64,
    persist_source_closed_dropped: AtomicU64,
    shutdown_queued_dropped: AtomicU64,
    shutdown_tasks_cancelled: AtomicU64,
    shutdown_recursive_nodes_dropped: AtomicU64,
    shutdown_persist_source_dropped: AtomicU64,
}

/// Cloneable, sender-free view of BEP-33 scrape worker counters.
#[derive(Clone, Default)]
pub struct DhtScrapeWorkerStatsHandle {
    inner: Arc<DhtScrapeWorkerStatsInner>,
}

/// One non-transactional snapshot of monotonic BEP-33 scrape counters.
///
/// After normal EOF, `dequeued = queries_started = tasks_completed`,
/// `queries_started = queries_succeeded + queries_failed`,
/// `put_node_commands = queries_succeeded`, and
/// `drop_addr_commands = queries_failed`. Recursive classification satisfies
/// `recursive_nodes = recursive_nodes_queued +
/// recursive_nodes_closed_dropped + recursive_nodes_timed_out_dropped`.
/// Every successful query produces one raw scraped-source request, so
/// `queries_succeeded = persist_source_queued +
/// persist_source_closed_dropped`. `peer_values_ignored` counts response
/// occurrences, including duplicates, that deliberately produce no table or
/// persistence projection.
///
/// On shutdown, `dequeued = tasks_completed + shutdown_tasks_cancelled`.
/// Adding `shutdown_recursive_nodes_dropped` to the recursive-node equation
/// accounts for fanout abandoned by graceful task cancellation. In addition,
/// `queries_succeeded = persist_source_queued +
/// persist_source_closed_dropped + shutdown_persist_source_dropped`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtScrapeWorkerStats {
    pub dequeued: u64,
    pub queries_started: u64,
    pub tasks_completed: u64,
    pub queries_succeeded: u64,
    pub queries_failed: u64,
    /// Attempted `PutNode` commands, including rejected or no-op commands.
    pub put_node_commands: u64,
    /// Attempted `DropAddr` commands, including absent-address commands.
    pub drop_addr_commands: u64,
    /// Ordered peer occurrences deliberately ignored from scrape responses.
    pub peer_values_ignored: u64,
    /// Ordered response nodes returned by successful queries.
    pub recursive_nodes: u64,
    pub recursive_nodes_queued: u64,
    /// Whole remaining response-node suffixes discarded after closure.
    pub recursive_nodes_closed_dropped: u64,
    /// Whole remaining response-node suffixes discarded at the shared deadline.
    pub recursive_nodes_timed_out_dropped: u64,
    pub persist_source_queued: u64,
    pub persist_source_closed_dropped: u64,
    pub shutdown_queued_dropped: u64,
    pub shutdown_tasks_cancelled: u64,
    pub shutdown_recursive_nodes_dropped: u64,
    pub shutdown_persist_source_dropped: u64,
}

impl DhtScrapeWorkerStatsHandle {
    /// Read each saturating counter independently with relaxed ordering.
    ///
    /// Cross-field conservation is guaranteed only after worker exit.
    #[must_use]
    pub fn snapshot(&self) -> DhtScrapeWorkerStats {
        let inner = &self.inner;
        DhtScrapeWorkerStats {
            dequeued: inner.dequeued.load(Ordering::Relaxed),
            queries_started: inner.queries_started.load(Ordering::Relaxed),
            tasks_completed: inner.tasks_completed.load(Ordering::Relaxed),
            queries_succeeded: inner.queries_succeeded.load(Ordering::Relaxed),
            queries_failed: inner.queries_failed.load(Ordering::Relaxed),
            put_node_commands: inner.put_node_commands.load(Ordering::Relaxed),
            drop_addr_commands: inner.drop_addr_commands.load(Ordering::Relaxed),
            peer_values_ignored: inner.peer_values_ignored.load(Ordering::Relaxed),
            recursive_nodes: inner.recursive_nodes.load(Ordering::Relaxed),
            recursive_nodes_queued: inner.recursive_nodes_queued.load(Ordering::Relaxed),
            recursive_nodes_closed_dropped: inner
                .recursive_nodes_closed_dropped
                .load(Ordering::Relaxed),
            recursive_nodes_timed_out_dropped: inner
                .recursive_nodes_timed_out_dropped
                .load(Ordering::Relaxed),
            persist_source_queued: inner.persist_source_queued.load(Ordering::Relaxed),
            persist_source_closed_dropped: inner
                .persist_source_closed_dropped
                .load(Ordering::Relaxed),
            shutdown_queued_dropped: inner.shutdown_queued_dropped.load(Ordering::Relaxed),
            shutdown_tasks_cancelled: inner.shutdown_tasks_cancelled.load(Ordering::Relaxed),
            shutdown_recursive_nodes_dropped: inner
                .shutdown_recursive_nodes_dropped
                .load(Ordering::Relaxed),
            shutdown_persist_source_dropped: inner
                .shutdown_persist_source_dropped
                .load(Ordering::Relaxed),
        }
    }
}

/// Owned, bounded consumer for the crawler's BEP-33 scrape route.
///
/// At most `max_inflight` requests are accepted at once. The input is not
/// polled at capacity, so no extra request is retained outside the shared
/// route. EOF joins every accepted task. Shutdown closes and drains input,
/// aborts accepted tasks, and awaits every cancellation before returning.
/// Discovery and scraped-source receiver closure is local to the affected
/// task and does not terminate this worker. Recursive fanout shares one
/// absolute one-second budget per successful response; expiry is biased over
/// an equal-ready capacity reservation and classifies the whole remaining
/// suffix. Successful work preserves the original request identity and raw
/// seeder/peer Bloom-filter direction while ignoring response peer values.
#[must_use = "the worker must be run to consume scrape work"]
pub struct DhtScrapeWorker {
    client: DhtRuntimeClient,
    core: DhtScrapeWorkerCore,
}

impl DhtScrapeWorker {
    /// Construct the production-compatible two-hundred-task worker.
    pub fn new(
        input: DhtScrapeReceiver,
        client: DhtRuntimeClient,
        table: KTable,
        persist_source: DhtPersistSourceInput,
        discovery: DhtDiscoverySender,
    ) -> (Self, DhtScrapeWorkerStatsHandle) {
        Self::with_config(
            input,
            client,
            table,
            persist_source,
            discovery,
            DhtScrapeWorkerConfig::default(),
        )
    }

    /// Construct a worker with an explicit concurrency bound.
    pub fn with_config(
        input: DhtScrapeReceiver,
        client: DhtRuntimeClient,
        table: KTable,
        persist_source: DhtPersistSourceInput,
        discovery: DhtDiscoverySender,
        config: DhtScrapeWorkerConfig,
    ) -> (Self, DhtScrapeWorkerStatsHandle) {
        let stats = DhtScrapeWorkerStatsHandle::default();
        (
            Self {
                client,
                core: DhtScrapeWorkerCore::new(
                    input,
                    table,
                    persist_source,
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
    /// Shutdown is biased ahead of ready task joins and receives. Cancelling an
    /// already-started runtime query stops awaiting it and removes its pending
    /// transaction registration, but cannot retract a UDP datagram that may
    /// already have been sent. Downstream receiver closure is counted by the
    /// child and does not stop later input work.
    pub async fn run<F>(mut self, shutdown: F) -> DhtScrapeWorkerExit
    where
        F: Future<Output = ()>,
    {
        let client = self.client.clone();
        self.core
            .run_with(
                shutdown,
                move |remote, info_hash| {
                    let client = client.clone();
                    async move { client.get_peers_scrape(remote, info_hash).await }
                },
                || tokio::time::Instant::now() + RECURSIVE_FANOUT_TIMEOUT,
                |_, _| std::future::ready(()),
                |_| std::future::ready(()),
                |_| {},
            )
            .await
    }
}

pub(super) struct DhtScrapeWorkerCore {
    input: DhtScrapeReceiver,
    table: KTable,
    persist_source: DhtPersistSourceInput,
    discovery: DhtDiscoverySender,
    max_inflight: NonZeroUsize,
    tasks: JoinSet<()>,
    stats: DhtScrapeWorkerStatsHandle,
    abandoned_recursive_nodes: Arc<AtomicUsize>,
    abandoned_persist_source_requests: Arc<AtomicUsize>,
    shutdown_in_progress: Arc<AtomicBool>,
}

impl DhtScrapeWorkerCore {
    pub(super) fn new(
        input: DhtScrapeReceiver,
        table: KTable,
        persist_source: DhtPersistSourceInput,
        discovery: DhtDiscoverySender,
        max_inflight: NonZeroUsize,
        stats: DhtScrapeWorkerStatsHandle,
    ) -> Self {
        Self {
            input,
            table,
            persist_source,
            discovery,
            max_inflight,
            tasks: JoinSet::new(),
            stats,
            abandoned_recursive_nodes: Arc::new(AtomicUsize::new(0)),
            abandoned_persist_source_requests: Arc::new(AtomicUsize::new(0)),
            shutdown_in_progress: Arc::new(AtomicBool::new(false)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_with<F, Q, QF, E, FD, BR, BRF, BP, BPF, OC>(
        &mut self,
        shutdown: F,
        query: Q,
        fanout_deadline: FD,
        before_recursive_reserve: BR,
        before_persist_source_send: BP,
        observe_command: OC,
    ) -> DhtScrapeWorkerExit
    where
        F: Future<Output = ()>,
        Q: Fn(SocketAddr, Id20) -> QF + Clone + Send + Sync + 'static,
        QF: Future<Output = Result<GetPeersScrapeResult, E>> + Send + 'static,
        E: Send + 'static,
        FD: Fn() -> tokio::time::Instant + Clone + Send + Sync + 'static,
        BR: Fn(usize, RoutingNode) -> BRF + Clone + Send + Sync + 'static,
        BRF: Future<Output = ()> + Send + 'static,
        BP: Fn(DhtPersistSourceRequest) -> BPF + Clone + Send + Sync + 'static,
        BPF: Future<Output = ()> + Send + 'static,
        OC: Fn(&KTableCommand) + Clone + Send + Sync + 'static,
    {
        tokio::pin!(shutdown);
        let mut input_closed = false;

        loop {
            if input_closed && self.tasks.is_empty() {
                return DhtScrapeWorkerExit::InputClosed;
            }

            enum Event {
                Shutdown,
                Joined(Result<(), JoinError>),
                Input(Option<DhtInfoHashTriageRequest>),
            }

            let event = tokio::select! {
                biased;
                () = &mut shutdown => Event::Shutdown,
                joined = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    Event::Joined(joined.expect("guarded scrape task remains present"))
                }
                request = self.input.recv(),
                    if !input_closed && self.tasks.len() < self.max_inflight.get() =>
                {
                    Event::Input(request)
                }
            };

            match event {
                Event::Shutdown => return self.finish_shutdown().await,
                Event::Joined(Ok(())) => {}
                Event::Joined(Err(error)) => self.finish_abnormal_join(error).await,
                Event::Input(Some(request)) => {
                    increment_saturating(&self.stats.inner.dequeued);
                    let table = self.table.clone();
                    let persist_source = self.persist_source.clone();
                    let discovery = self.discovery.clone();
                    let stats = self.stats.clone();
                    let query = query.clone();
                    let fanout_deadline = fanout_deadline.clone();
                    let before_recursive_reserve = before_recursive_reserve.clone();
                    let before_persist_source_send = before_persist_source_send.clone();
                    let observe_command = observe_command.clone();
                    let abandoned_recursive_nodes = Arc::clone(&self.abandoned_recursive_nodes);
                    let abandoned_persist_source_requests =
                        Arc::clone(&self.abandoned_persist_source_requests);
                    let shutdown_in_progress = Arc::clone(&self.shutdown_in_progress);
                    self.tasks.spawn(async move {
                        finish_scrape_work(
                            request,
                            &table,
                            &persist_source,
                            &discovery,
                            &stats,
                            abandoned_recursive_nodes,
                            abandoned_persist_source_requests,
                            shutdown_in_progress,
                            query,
                            fanout_deadline,
                            before_recursive_reserve,
                            before_persist_source_send,
                            observe_command,
                        )
                        .await;
                    });
                }
                Event::Input(None) => input_closed = true,
            }
        }
    }

    async fn finish_shutdown(&mut self) -> DhtScrapeWorkerExit {
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
                Err(error) => panic!("unexpected scrape task join error: {error}"),
            }
        }
        add_saturating(&self.stats.inner.shutdown_tasks_cancelled, tasks_cancelled);
        let recursive_nodes_dropped = self.abandoned_recursive_nodes.load(Ordering::Relaxed);
        let persist_source_requests_dropped = self
            .abandoned_persist_source_requests
            .load(Ordering::Relaxed);
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
        DhtScrapeWorkerExit::Shutdown {
            queued_dropped,
            tasks_cancelled,
            recursive_nodes_dropped,
            persist_source_requests_dropped,
        }
    }

    async fn finish_abnormal_join(&mut self, error: JoinError) -> ! {
        let panic_payload = error.is_panic().then(|| error.into_panic());
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
        if let Some(payload) = panic_payload {
            resume_unwind(payload);
        }
        panic!("scrape task was cancelled outside worker cleanup")
    }
}

impl Drop for DhtScrapeWorkerCore {
    fn drop(&mut self) {
        self.input.close();
        self.tasks.abort_all();
    }
}

struct RecursiveFanoutGuard {
    remaining: usize,
    stats: DhtScrapeWorkerStatsHandle,
    abandoned: Arc<AtomicUsize>,
    shutdown_in_progress: Arc<AtomicBool>,
}

impl RecursiveFanoutGuard {
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

    fn timed_out(&mut self) {
        add_saturating(
            &self.stats.inner.recursive_nodes_timed_out_dropped,
            self.remaining,
        );
        self.remaining = 0;
    }
}

impl Drop for RecursiveFanoutGuard {
    fn drop(&mut self) {
        if self.remaining == 0 || !self.shutdown_in_progress.load(Ordering::SeqCst) {
            return;
        }
        add_saturating(
            &self.stats.inner.shutdown_recursive_nodes_dropped,
            self.remaining,
        );
        add_saturating_usize(&self.abandoned, self.remaining);
    }
}

struct PersistSourceGuard {
    remaining: usize,
    stats: DhtScrapeWorkerStatsHandle,
    abandoned: Arc<AtomicUsize>,
    shutdown_in_progress: Arc<AtomicBool>,
}

impl PersistSourceGuard {
    fn queued(&mut self) {
        self.remaining = 0;
        increment_saturating(&self.stats.inner.persist_source_queued);
    }

    fn receiver_closed(&mut self) {
        self.remaining = 0;
        increment_saturating(&self.stats.inner.persist_source_closed_dropped);
    }
}

impl Drop for PersistSourceGuard {
    fn drop(&mut self) {
        if self.remaining == 0 || !self.shutdown_in_progress.load(Ordering::SeqCst) {
            return;
        }
        add_saturating(
            &self.stats.inner.shutdown_persist_source_dropped,
            self.remaining,
        );
        add_saturating_usize(&self.abandoned, self.remaining);
    }
}

#[allow(clippy::too_many_arguments)]
async fn finish_scrape_work<Q, QF, E, FD, BR, BRF, BP, BPF, OC>(
    request: DhtInfoHashTriageRequest,
    table: &KTable,
    persist_source: &DhtPersistSourceInput,
    discovery: &DhtDiscoverySender,
    stats: &DhtScrapeWorkerStatsHandle,
    abandoned_recursive_nodes: Arc<AtomicUsize>,
    abandoned_persist_source_requests: Arc<AtomicUsize>,
    shutdown_in_progress: Arc<AtomicBool>,
    query: Q,
    fanout_deadline: FD,
    before_recursive_reserve: BR,
    before_persist_source_send: BP,
    observe_command: OC,
) where
    Q: Fn(SocketAddr, Id20) -> QF,
    QF: Future<Output = Result<GetPeersScrapeResult, E>>,
    FD: Fn() -> tokio::time::Instant,
    BR: Fn(usize, RoutingNode) -> BRF,
    BRF: Future<Output = ()>,
    BP: Fn(DhtPersistSourceRequest) -> BPF,
    BPF: Future<Output = ()>,
    OC: Fn(&KTableCommand),
{
    increment_saturating(&stats.inner.queries_started);
    let GetPeersScrapeResult {
        id,
        values,
        nodes,
        peers_bloom,
        seeders_bloom,
    } = match query(request.source_node_addr, request.info_hash).await {
        Ok(result) => {
            increment_saturating(&stats.inner.queries_succeeded);
            result
        }
        Err(_) => {
            increment_saturating(&stats.inner.queries_failed);
            let command = KTableCommand::DropAddr {
                addr: request.source_node_addr,
            };
            table.batch_command(std::slice::from_ref(&command));
            increment_saturating(&stats.inner.drop_addr_commands);
            observe_command(&command);
            increment_saturating(&stats.inner.tasks_completed);
            return;
        }
    };

    add_saturating(&stats.inner.peer_values_ignored, values.len());

    let persist_request = DhtPersistSourceRequest {
        info_hash: request.info_hash,
        source_node_addr: request.source_node_addr,
        seeders_bloom,
        peers_bloom,
    };
    let mut persist_guard = PersistSourceGuard {
        remaining: 1,
        stats: stats.clone(),
        abandoned: abandoned_persist_source_requests,
        shutdown_in_progress: Arc::clone(&shutdown_in_progress),
    };

    let put_node = KTableCommand::PutNode {
        node: RoutingNode {
            id,
            addr: request.source_node_addr,
        },
        options: vec![KTableNodeOption::Responded],
    };
    table.batch_command(std::slice::from_ref(&put_node));
    increment_saturating(&stats.inner.put_node_commands);
    observe_command(&put_node);

    add_saturating(&stats.inner.recursive_nodes, nodes.len());
    let mut fanout = RecursiveFanoutGuard {
        remaining: nodes.len(),
        stats: stats.clone(),
        abandoned: abandoned_recursive_nodes,
        shutdown_in_progress,
    };

    if !nodes.is_empty() {
        let deadline = fanout_deadline();
        for (index, node) in nodes.into_iter().enumerate() {
            let reserve = async {
                before_recursive_reserve(index, node).await;
                discovery.reserve().await
            };
            let permit = tokio::select! {
                biased;
                () = tokio::time::sleep_until(deadline) => {
                    fanout.timed_out();
                    break;
                }
                permit = reserve => match permit {
                    Ok(permit) => permit,
                    Err(_) => {
                        fanout.receiver_closed();
                        break;
                    }
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
    }

    before_persist_source_send(persist_request.clone()).await;
    if persist_source.send(persist_request).await.is_ok() {
        persist_guard.queued();
    } else {
        persist_guard.receiver_closed();
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
mod tests {
    use std::future::{pending, ready};
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV6};
    use std::sync::atomic::{AtomicUsize as TestAtomicUsize, Ordering as TestOrdering};
    use std::sync::Mutex;

    use bitmagnet_dht::{
        dht_discovery_channel, dht_scrape_channel, DhtDiscoveryReceiver, DhtScrapeInput,
        ScrapeBloomFilter,
    };
    use tokio::sync::{oneshot, Semaphore};

    use super::*;
    use crate::{
        dht_persist_source_channel, DhtPersistSourceInput, DhtPersistSourceReceiver,
        DHT_PERSIST_SOURCE_ROUTE_CAPACITY,
    };

    fn id(value: u16) -> Id20 {
        let mut bytes = [0_u8; 20];
        bytes[18..].copy_from_slice(&value.to_be_bytes());
        Id20::from_slice(&bytes).unwrap()
    }

    fn ipv4(value: u16) -> SocketAddr {
        SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 0, (value >> 8) as u8, value as u8)),
            10_000 + value,
        )
    }

    fn ipv6(value: u16, scope: u32) -> SocketAddr {
        SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, value),
            20_000 + value,
            0,
            scope,
        ))
    }

    fn node(value: u16) -> RoutingNode {
        RoutingNode {
            id: id(value),
            addr: ipv4(value),
        }
    }

    fn request(value: u16) -> DhtInfoHashTriageRequest {
        DhtInfoHashTriageRequest {
            info_hash: id(value),
            source_node_addr: ipv4(value),
        }
    }

    fn source_request(value: u16) -> DhtPersistSourceRequest {
        DhtPersistSourceRequest {
            info_hash: id(value),
            source_node_addr: ipv4(value),
            seeders_bloom: ScrapeBloomFilter::from([value as u8; 256]),
            peers_bloom: ScrapeBloomFilter::from([(value >> 8) as u8; 256]),
        }
    }

    fn scrape_result(id_value: u16) -> GetPeersScrapeResult {
        GetPeersScrapeResult {
            id: id(id_value),
            values: vec![],
            nodes: vec![],
            peers_bloom: ScrapeBloomFilter::EMPTY,
            seeders_bloom: ScrapeBloomFilter::EMPTY,
        }
    }

    #[allow(clippy::type_complexity)]
    fn core(
        max_inflight: usize,
        discovery_capacity: usize,
    ) -> (
        DhtScrapeInput,
        DhtScrapeWorkerCore,
        DhtScrapeWorkerStatsHandle,
        KTable,
        DhtPersistSourceInput,
        DhtPersistSourceReceiver,
        DhtDiscoveryReceiver,
    ) {
        let (input, receiver) = dht_scrape_channel();
        let (persist_source, persist_source_receiver) = dht_persist_source_channel();
        let persist_source_probe = persist_source.clone();
        let (discovery, discovery_receiver) =
            dht_discovery_channel(NonZeroUsize::new(discovery_capacity).unwrap());
        let table = KTable::new(id(60_000));
        let stats = DhtScrapeWorkerStatsHandle::default();
        let core = DhtScrapeWorkerCore::new(
            receiver,
            table.clone(),
            persist_source,
            discovery,
            NonZeroUsize::new(max_inflight).unwrap(),
            stats.clone(),
        );
        (
            input,
            core,
            stats,
            table,
            persist_source_probe,
            persist_source_receiver,
            discovery_receiver,
        )
    }

    fn deadline() -> tokio::time::Instant {
        tokio::time::Instant::now() + RECURSIVE_FANOUT_TIMEOUT
    }

    async fn yield_until(mut predicate: impl FnMut() -> bool) {
        for _ in 0..1_000 {
            if predicate() {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert!(predicate(), "condition did not become true after yielding");
    }

    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn public_defaults_traits_and_saturating_snapshot_are_exact() {
        assert_eq!(DhtScrapeWorkerConfig::default().max_inflight.get(), 200);
        assert_send::<DhtScrapeWorker>();
        assert_send_sync::<DhtScrapeWorkerConfig>();
        assert_send_sync::<DhtScrapeWorkerExit>();
        assert_send_sync::<DhtScrapeWorkerStats>();
        assert_send_sync::<DhtScrapeWorkerStatsHandle>();

        let stats = DhtScrapeWorkerStatsHandle::default();
        stats.inner.dequeued.store(u64::MAX, Ordering::Relaxed);
        stats
            .inner
            .shutdown_recursive_nodes_dropped
            .store(u64::MAX - 1, Ordering::Relaxed);
        increment_saturating(&stats.inner.dequeued);
        add_saturating(&stats.inner.shutdown_recursive_nodes_dropped, usize::MAX);
        assert_eq!(stats.snapshot().dequeued, u64::MAX);
        assert_eq!(stats.snapshot().shutdown_recursive_nodes_dropped, u64::MAX);
    }

    #[tokio::test]
    async fn pre_ready_shutdown_drains_input_without_starting_queries() {
        let (input, mut core, stats, _, _, mut persist_receiver, mut discovery_receiver) =
            core(2, 2);
        input.send(request(1)).await.unwrap();
        input.send(request(2)).await.unwrap();
        let query_calls = Arc::new(TestAtomicUsize::new(0));
        let calls = Arc::clone(&query_calls);

        let exit = core
            .run_with(
                ready(()),
                move |_, _| {
                    calls.fetch_add(1, TestOrdering::Relaxed);
                    ready(Ok::<_, ()>(scrape_result(1)))
                },
                deadline,
                |_, _| ready(()),
                |_| ready(()),
                |_| {},
            )
            .await;

        assert_eq!(
            exit,
            DhtScrapeWorkerExit::Shutdown {
                queued_dropped: 2,
                tasks_cancelled: 0,
                recursive_nodes_dropped: 0,
                persist_source_requests_dropped: 0,
            }
        );
        assert_eq!(query_calls.load(TestOrdering::Relaxed), 0);
        assert_eq!(
            stats.snapshot(),
            DhtScrapeWorkerStats {
                shutdown_queued_dropped: 2,
                ..DhtScrapeWorkerStats::default()
            }
        );
        assert_eq!(
            persist_receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );
        assert_eq!(
            discovery_receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );
    }

    #[tokio::test]
    async fn error_drops_source_address_and_completes_without_downstream_work() {
        let (input, mut core, stats, table, _, mut persist_receiver, mut discovery_receiver) =
            core(1, 1);
        let source = ipv6(9, 7);
        let existing = RoutingNode {
            id: id(90),
            addr: source,
        };
        assert!(matches!(
            table.put_node(existing),
            bitmagnet_dht::RoutingPutResult::Accepted
        ));
        let _ = table.put_node(existing);
        let retained = table.node_handle(existing.id).unwrap();
        let req = DhtInfoHashTriageRequest {
            info_hash: id(9),
            source_node_addr: source,
        };
        input.send(req).await.unwrap();
        drop(input);
        let commands = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&commands);

        let exit = core
            .run_with(
                pending(),
                |_, _| ready(Err::<GetPeersScrapeResult, _>("query failed")),
                deadline,
                |_, _| ready(()),
                |_| ready(()),
                move |command| observed.lock().unwrap().push(command.clone()),
            )
            .await;

        assert_eq!(exit, DhtScrapeWorkerExit::InputClosed);
        assert!(retained.dropped());
        assert!(table.node_handle(existing.id).is_none());
        assert_eq!(
            *commands.lock().unwrap(),
            vec![KTableCommand::DropAddr { addr: source }]
        );
        assert_eq!(
            stats.snapshot(),
            DhtScrapeWorkerStats {
                dequeued: 1,
                queries_started: 1,
                tasks_completed: 1,
                queries_failed: 1,
                drop_addr_commands: 1,
                ..DhtScrapeWorkerStats::default()
            }
        );
        assert!(persist_receiver.try_recv().is_err());
        assert!(discovery_receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn success_preserves_raw_blooms_order_duplicates_and_ignores_peer_values() {
        let (input, mut core, stats, table, _, mut persist_receiver, mut discovery_receiver) =
            core(1, 4);
        let req = DhtInfoHashTriageRequest {
            info_hash: id(7),
            source_node_addr: ipv6(7, 12),
        };
        let response_id = id(70);
        let nodes = vec![node(1), node(2), node(1)];
        let values = vec![ipv4(40), ipv6(41, 13), ipv4(40)];
        let peers_pattern = std::array::from_fn(|index| index as u8);
        let seeders_pattern = std::array::from_fn(|index| 255_u8.wrapping_sub(index as u8));
        let expected_source = DhtPersistSourceRequest {
            info_hash: req.info_hash,
            source_node_addr: req.source_node_addr,
            seeders_bloom: ScrapeBloomFilter::from(seeders_pattern),
            peers_bloom: ScrapeBloomFilter::from(peers_pattern),
        };
        let result = GetPeersScrapeResult {
            id: response_id,
            values: values.clone(),
            nodes: nodes.clone(),
            peers_bloom: expected_source.peers_bloom,
            seeders_bloom: expected_source.seeders_bloom,
        };
        input.send(req).await.unwrap();
        drop(input);
        let events = Arc::new(Mutex::new(Vec::new()));
        let commands = Arc::new(Mutex::new(Vec::new()));
        let reserve_events = Arc::clone(&events);
        let persist_events = Arc::clone(&events);
        let command_events = Arc::clone(&events);
        let observed_commands = Arc::clone(&commands);

        assert_eq!(
            core.run_with(
                pending(),
                move |_, _| ready(Ok::<_, ()>(result.clone())),
                deadline,
                move |index, _| {
                    reserve_events.lock().unwrap().push(match index {
                        0 => "reserve:0",
                        1 => "reserve:1",
                        2 => "reserve:2",
                        _ => "reserve:other",
                    });
                    ready(())
                },
                move |_| {
                    persist_events.lock().unwrap().push("persist");
                    ready(())
                },
                move |command| {
                    assert!(matches!(command, KTableCommand::PutNode { .. }));
                    observed_commands.lock().unwrap().push(command.clone());
                    command_events.lock().unwrap().push("put_node");
                },
            )
            .await,
            DhtScrapeWorkerExit::InputClosed
        );

        for expected in &nodes {
            assert_eq!(discovery_receiver.recv().await, Some(*expected));
        }
        assert_eq!(persist_receiver.recv().await, Some(expected_source));
        assert_eq!(
            *events.lock().unwrap(),
            vec!["put_node", "reserve:0", "reserve:1", "reserve:2", "persist"]
        );
        assert_eq!(
            *commands.lock().unwrap(),
            vec![KTableCommand::PutNode {
                node: RoutingNode {
                    id: response_id,
                    addr: req.source_node_addr,
                },
                options: vec![KTableNodeOption::Responded],
            }]
        );
        let handle = table.node_handle(response_id).unwrap();
        assert_eq!(handle.addr(), req.source_node_addr);
        assert!(handle.last_responded_at().is_some());
        assert!(table.hash(req.info_hash).is_none());
        assert_eq!(
            stats.snapshot(),
            DhtScrapeWorkerStats {
                dequeued: 1,
                queries_started: 1,
                tasks_completed: 1,
                queries_succeeded: 1,
                put_node_commands: 1,
                peer_values_ignored: 3,
                recursive_nodes: 3,
                recursive_nodes_queued: 3,
                persist_source_queued: 1,
                ..DhtScrapeWorkerStats::default()
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn one_absolute_discovery_deadline_keeps_prefix_and_times_out_suffix() {
        let (input, mut core, stats, _, _, mut persist_receiver, mut discovery_receiver) =
            core(1, 1);
        let req = request(8);
        let nodes = vec![node(1), node(2), node(3)];
        input.send(req).await.unwrap();
        drop(input);
        let run = tokio::spawn(async move {
            core.run_with(
                pending(),
                move |_, _| {
                    let mut result = scrape_result(80);
                    result.nodes = nodes.clone();
                    ready(Ok::<_, ()>(result))
                },
                deadline,
                |_, _| ready(()),
                |_| ready(()),
                |_| {},
            )
            .await
        });
        yield_until(|| stats.snapshot().recursive_nodes_queued == 1).await;
        tokio::time::advance(RECURSIVE_FANOUT_TIMEOUT).await;
        assert_eq!(run.await.unwrap(), DhtScrapeWorkerExit::InputClosed);
        assert_eq!(discovery_receiver.recv().await, Some(node(1)));
        assert_eq!(stats.snapshot().recursive_nodes_timed_out_dropped, 2);
        assert_eq!(
            persist_receiver.recv().await,
            Some(DhtPersistSourceRequest {
                info_hash: req.info_hash,
                source_node_addr: req.source_node_addr,
                seeders_bloom: ScrapeBloomFilter::EMPTY,
                peers_bloom: ScrapeBloomFilter::EMPTY,
            })
        );
    }

    #[tokio::test]
    async fn capacity_bound_does_not_dequeue_an_extra_request_and_eof_joins_tasks() {
        let (input, mut core, stats, _, _, _, _) = core(2, 1);
        for value in 1..=3 {
            input.send(request(value)).await.unwrap();
        }
        drop(input);
        let permits = Arc::new(Semaphore::new(0));
        let query_calls = Arc::new(TestAtomicUsize::new(0));
        let run_permits = Arc::clone(&permits);
        let run_calls = Arc::clone(&query_calls);
        let run = tokio::spawn(async move {
            core.run_with(
                pending(),
                move |_, info_hash| {
                    let permits = Arc::clone(&run_permits);
                    let calls = Arc::clone(&run_calls);
                    async move {
                        calls.fetch_add(1, TestOrdering::Relaxed);
                        permits.acquire_owned().await.unwrap().forget();
                        Ok::<_, ()>(GetPeersScrapeResult {
                            id: info_hash,
                            ..scrape_result(99)
                        })
                    }
                },
                deadline,
                |_, _| ready(()),
                |_| ready(()),
                |_| {},
            )
            .await
        });

        yield_until(|| query_calls.load(TestOrdering::Relaxed) == 2).await;
        assert_eq!(stats.snapshot().dequeued, 2);
        assert!(!run.is_finished());
        permits.add_permits(1);
        yield_until(|| query_calls.load(TestOrdering::Relaxed) == 3).await;
        assert_eq!(stats.snapshot().dequeued, 3);
        permits.add_permits(2);
        assert_eq!(run.await.unwrap(), DhtScrapeWorkerExit::InputClosed);
        assert_eq!(stats.snapshot().tasks_completed, 3);
    }

    #[tokio::test]
    async fn shutdown_during_query_counts_only_started_owned_task() {
        let (input, mut core, stats, _, _, _, _) = core(1, 1);
        input.send(request(1)).await.unwrap();
        let (started_tx, started_rx) = oneshot::channel();
        let started_tx = Arc::new(Mutex::new(Some(started_tx)));
        let started = Arc::clone(&started_tx);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn(async move {
            core.run_with(
                async move {
                    let _ = shutdown_rx.await;
                },
                move |_, _| {
                    if let Some(tx) = started.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    pending::<Result<GetPeersScrapeResult, ()>>()
                },
                deadline,
                |_, _| ready(()),
                |_| ready(()),
                |_| {},
            )
            .await
        });
        started_rx.await.unwrap();
        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await.unwrap(),
            DhtScrapeWorkerExit::Shutdown {
                queued_dropped: 0,
                tasks_cancelled: 1,
                recursive_nodes_dropped: 0,
                persist_source_requests_dropped: 0,
            }
        );
        assert_eq!(
            stats.snapshot(),
            DhtScrapeWorkerStats {
                dequeued: 1,
                queries_started: 1,
                shutdown_tasks_cancelled: 1,
                ..DhtScrapeWorkerStats::default()
            }
        );
    }

    #[tokio::test]
    async fn shutdown_after_recursive_prefix_conserves_suffix_and_future_source() {
        let (input, mut core, stats, _, _, _, mut discovery_receiver) = core(1, 1);
        input.send(request(1)).await.unwrap();
        let (barrier_tx, barrier_rx) = oneshot::channel();
        let barrier_tx = Arc::new(Mutex::new(Some(barrier_tx)));
        let barrier = Arc::clone(&barrier_tx);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn(async move {
            core.run_with(
                async move {
                    let _ = shutdown_rx.await;
                },
                |_, _| {
                    let mut result = scrape_result(70);
                    result.nodes = vec![node(1), node(2), node(3)];
                    ready(Ok::<_, ()>(result))
                },
                deadline,
                move |index, _| {
                    if index == 1 {
                        if let Some(tx) = barrier.lock().unwrap().take() {
                            let _ = tx.send(());
                        }
                    }
                    ready(())
                },
                |_| ready(()),
                |_| {},
            )
            .await
        });

        barrier_rx.await.unwrap();
        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await.unwrap(),
            DhtScrapeWorkerExit::Shutdown {
                queued_dropped: 0,
                tasks_cancelled: 1,
                recursive_nodes_dropped: 2,
                persist_source_requests_dropped: 1,
            }
        );
        assert_eq!(discovery_receiver.recv().await, Some(node(1)));
        assert_eq!(
            stats.snapshot(),
            DhtScrapeWorkerStats {
                dequeued: 1,
                queries_started: 1,
                queries_succeeded: 1,
                put_node_commands: 1,
                recursive_nodes: 3,
                recursive_nodes_queued: 1,
                shutdown_tasks_cancelled: 1,
                shutdown_recursive_nodes_dropped: 2,
                shutdown_persist_source_dropped: 1,
                ..DhtScrapeWorkerStats::default()
            }
        );
    }

    #[tokio::test]
    async fn shutdown_while_persist_source_is_backpressured_counts_exact_request() {
        let (input, mut core, stats, _, persist_source, mut persist_receiver, _) = core(1, 1);
        for value in 0..DHT_PERSIST_SOURCE_ROUTE_CAPACITY {
            persist_source
                .send(source_request(u16::try_from(value).unwrap()))
                .await
                .unwrap();
        }
        input.send(request(90)).await.unwrap();
        let (barrier_tx, barrier_rx) = oneshot::channel();
        let barrier_tx = Arc::new(Mutex::new(Some(barrier_tx)));
        let barrier = Arc::clone(&barrier_tx);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn(async move {
            core.run_with(
                async move {
                    let _ = shutdown_rx.await;
                },
                |_, _| ready(Ok::<_, ()>(scrape_result(91))),
                deadline,
                |_, _| ready(()),
                move |_| {
                    if let Some(tx) = barrier.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    ready(())
                },
                |_| {},
            )
            .await
        });

        barrier_rx.await.unwrap();
        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await.unwrap(),
            DhtScrapeWorkerExit::Shutdown {
                queued_dropped: 0,
                tasks_cancelled: 1,
                recursive_nodes_dropped: 0,
                persist_source_requests_dropped: 1,
            }
        );
        assert_eq!(persist_receiver.recv().await, Some(source_request(0)));
        assert_eq!(
            stats.snapshot(),
            DhtScrapeWorkerStats {
                dequeued: 1,
                queries_started: 1,
                queries_succeeded: 1,
                put_node_commands: 1,
                shutdown_tasks_cancelled: 1,
                shutdown_persist_source_dropped: 1,
                ..DhtScrapeWorkerStats::default()
            }
        );
    }

    #[tokio::test]
    async fn closed_discovery_classifies_whole_suffix_then_still_persists() {
        let (input, mut core, stats, _, _, mut persist_receiver, discovery_receiver) = core(1, 1);
        drop(discovery_receiver);
        let req = request(120);
        input.send(req).await.unwrap();
        drop(input);

        assert_eq!(
            core.run_with(
                pending(),
                |_, _| {
                    let mut result = scrape_result(121);
                    result.nodes = vec![node(1), node(2), node(3)];
                    ready(Ok::<_, ()>(result))
                },
                deadline,
                |_, _| ready(()),
                |_| ready(()),
                |_| {},
            )
            .await,
            DhtScrapeWorkerExit::InputClosed
        );

        assert_eq!(
            persist_receiver.recv().await.unwrap().info_hash,
            req.info_hash
        );
        assert_eq!(
            stats.snapshot(),
            DhtScrapeWorkerStats {
                dequeued: 1,
                queries_started: 1,
                tasks_completed: 1,
                queries_succeeded: 1,
                put_node_commands: 1,
                recursive_nodes: 3,
                recursive_nodes_closed_dropped: 3,
                persist_source_queued: 1,
                ..DhtScrapeWorkerStats::default()
            }
        );
    }

    #[tokio::test]
    async fn closed_persist_source_classifies_requests_without_stopping_worker() {
        let (input, mut core, stats, _, _, persist_receiver, _) = core(1, 1);
        drop(persist_receiver);
        input.send(request(123)).await.unwrap();
        input.send(request(124)).await.unwrap();
        drop(input);

        assert_eq!(
            core.run_with(
                pending(),
                |_, info_hash| {
                    ready(Ok::<_, ()>(GetPeersScrapeResult {
                        id: info_hash,
                        ..scrape_result(125)
                    }))
                },
                deadline,
                |_, _| ready(()),
                |_| ready(()),
                |_| {},
            )
            .await,
            DhtScrapeWorkerExit::InputClosed
        );

        assert_eq!(
            stats.snapshot(),
            DhtScrapeWorkerStats {
                dequeued: 2,
                queries_started: 2,
                tasks_completed: 2,
                queries_succeeded: 2,
                put_node_commands: 2,
                persist_source_closed_dropped: 2,
                ..DhtScrapeWorkerStats::default()
            }
        );
    }

    #[tokio::test]
    async fn child_panic_aborts_and_joins_sibling_then_resumes_original_payload() {
        struct DropSignal(Arc<AtomicBool>);
        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let (input, mut core, stats, _, _, _, _) = core(2, 1);
        input.send(request(1)).await.unwrap();
        input.send(request(2)).await.unwrap();
        drop(input);
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let sibling_dropped = Arc::new(AtomicBool::new(false));
        let run_barrier = Arc::clone(&barrier);
        let run_sibling_dropped = Arc::clone(&sibling_dropped);

        let run = tokio::spawn(async move {
            core.run_with(
                pending(),
                move |_, info_hash| {
                    let barrier = Arc::clone(&run_barrier);
                    let sibling_dropped = Arc::clone(&run_sibling_dropped);
                    async move {
                        barrier.wait().await;
                        if info_hash == id(1) {
                            tokio::task::yield_now().await;
                            panic!("scrape-child-panic");
                        }
                        let _drop_signal = DropSignal(sibling_dropped);
                        pending::<Result<GetPeersScrapeResult, ()>>().await
                    }
                },
                deadline,
                |_, _| ready(()),
                |_| ready(()),
                |_| {},
            )
            .await
        });

        let error = run.await.unwrap_err();
        assert!(error.is_panic());
        let payload = error.into_panic();
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"scrape-child-panic"));
        assert!(sibling_dropped.load(Ordering::SeqCst));
        assert_eq!(
            stats.snapshot(),
            DhtScrapeWorkerStats {
                dequeued: 2,
                queries_started: 2,
                ..DhtScrapeWorkerStats::default()
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn equal_ready_deadline_is_biased_over_available_discovery_capacity() {
        let (input, mut core, stats, _, _, mut persist_receiver, mut discovery_receiver) =
            core(1, 2);
        input.send(request(130)).await.unwrap();
        drop(input);

        assert_eq!(
            core.run_with(
                pending(),
                |_, _| {
                    let mut result = scrape_result(131);
                    result.nodes = vec![node(1), node(2)];
                    ready(Ok::<_, ()>(result))
                },
                tokio::time::Instant::now,
                |_, _| ready(()),
                |_| ready(()),
                |_| {},
            )
            .await,
            DhtScrapeWorkerExit::InputClosed
        );

        assert_eq!(
            discovery_receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );
        assert!(persist_receiver.recv().await.is_some());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.recursive_nodes, 2);
        assert_eq!(snapshot.recursive_nodes_queued, 0);
        assert_eq!(snapshot.recursive_nodes_timed_out_dropped, 2);
        assert_eq!(snapshot.persist_source_queued, 1);
    }

    #[tokio::test]
    async fn shutdown_can_abort_an_accepted_never_polled_child_without_starting_query() {
        struct PendingThenReady(bool);
        impl Future for PendingThenReady {
            type Output = ();

            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                context: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                if self.0 {
                    std::task::Poll::Ready(())
                } else {
                    self.0 = true;
                    context.waker().wake_by_ref();
                    std::task::Poll::Pending
                }
            }
        }

        let (input, mut core, stats, _, _, _, _) = core(1, 1);
        input.send(request(132)).await.unwrap();
        let calls = Arc::new(TestAtomicUsize::new(0));
        let query_calls = Arc::clone(&calls);

        assert_eq!(
            core.run_with(
                PendingThenReady(false),
                move |_, _| {
                    query_calls.fetch_add(1, TestOrdering::Relaxed);
                    pending::<Result<GetPeersScrapeResult, ()>>()
                },
                deadline,
                |_, _| ready(()),
                |_| ready(()),
                |_| {},
            )
            .await,
            DhtScrapeWorkerExit::Shutdown {
                queued_dropped: 0,
                tasks_cancelled: 1,
                recursive_nodes_dropped: 0,
                persist_source_requests_dropped: 0,
            }
        );
        assert_eq!(calls.load(TestOrdering::Relaxed), 0);
        assert_eq!(
            stats.snapshot(),
            DhtScrapeWorkerStats {
                dequeued: 1,
                shutdown_tasks_cancelled: 1,
                ..DhtScrapeWorkerStats::default()
            }
        );
    }

    #[tokio::test]
    async fn dropping_active_run_aborts_child_and_closes_input_without_terminal_stats() {
        struct DropSignal(Arc<AtomicBool>);
        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let (input, mut core, stats, _, _, _, _) = core(1, 1);
        input.send(request(133)).await.unwrap();
        let (started_tx, started_rx) = oneshot::channel();
        let started_tx = Arc::new(Mutex::new(Some(started_tx)));
        let started = Arc::clone(&started_tx);
        let child_dropped = Arc::new(AtomicBool::new(false));
        let child_drop_signal = Arc::clone(&child_dropped);
        let run = tokio::spawn(async move {
            core.run_with(
                pending(),
                move |_, _| {
                    let started = Arc::clone(&started);
                    let child_drop_signal = Arc::clone(&child_drop_signal);
                    async move {
                        let _drop_signal = DropSignal(child_drop_signal);
                        if let Some(tx) = started.lock().unwrap().take() {
                            let _ = tx.send(());
                        }
                        pending::<Result<GetPeersScrapeResult, ()>>().await
                    }
                },
                deadline,
                |_, _| ready(()),
                |_| ready(()),
                |_| {},
            )
            .await
        });

        started_rx.await.unwrap();
        run.abort();
        assert!(run.await.unwrap_err().is_cancelled());
        assert!(child_dropped.load(Ordering::SeqCst));
        let later = request(134);
        assert_eq!(input.send(later).await.unwrap_err().into_request(), later);
        assert_eq!(stats.snapshot().queries_started, 1);
        assert_eq!(stats.snapshot().shutdown_tasks_cancelled, 0);
    }

    #[tokio::test]
    async fn dropping_core_closes_input_and_recovers_exact_request() {
        let (input, core, _, _, _, _, _) = core(1, 1);
        drop(core);
        let req = request(9);
        assert_eq!(input.send(req).await.unwrap_err().into_request(), req);
    }
}
