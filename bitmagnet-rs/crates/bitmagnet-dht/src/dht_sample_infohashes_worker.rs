use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::panic::resume_unwind;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::{JoinError, JoinSet};

use crate::dht_discovered_node_scheduler::DhtDiscoveredNodeSampleInfoHashesWork;
use crate::{
    DhtCrawlerTarget, DhtDiscoveredNodeSampleInfoHashesReceiver, DhtDiscoveryOffer,
    DhtDiscoverySender, DhtInfoHashDeduper, DhtInfoHashTriageInput, DhtInfoHashTriageRequest,
    DhtRuntimeClient, Id20, KTable, KTableCommand, KTableNodeOption, RoutingNode,
    SampleInfoHashesResult,
};

const DEFAULT_MAX_INFLIGHT: NonZeroUsize = NonZeroUsize::new(100).unwrap();
const ACTIVE_DISCOVERY_INTERVAL_SECONDS: i64 = 60;
const LONG_INTERVAL_THRESHOLD_SECONDS: i64 = 300;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const RECURSIVE_FANOUT_TIMEOUT: Duration = Duration::from_secs(1);

/// Concurrency bound for accepted `sample_infohashes` work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtSampleInfoHashesWorkerConfig {
    pub max_inflight: NonZeroUsize,
}

impl Default for DhtSampleInfoHashesWorkerConfig {
    fn default() -> Self {
        Self {
            max_inflight: DEFAULT_MAX_INFLIGHT,
        }
    }
}

/// Terminal state of the owned `sample_infohashes` worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtSampleInfoHashesWorkerExit {
    /// Every shared-route producer is gone and every accepted task completed.
    InputClosed,
    /// Caller shutdown won before another completion or route receive.
    Shutdown {
        /// Work items drained from the closed route before task cancellation.
        queued_dropped: usize,
        /// Accepted tasks whose abort was observed while joining.
        tasks_cancelled: usize,
        /// Novel-hash suffixes abandoned by those cancelled tasks.
        triage_hashes_dropped: usize,
        /// Recursive-node suffixes abandoned by those cancelled tasks.
        recursive_nodes_dropped: usize,
    },
}

#[derive(Default)]
struct DhtSampleInfoHashesWorkerStatsInner {
    dequeued: AtomicU64,
    candidate_skipped: AtomicU64,
    queries_started: AtomicU64,
    tasks_completed: AtomicU64,
    queries_succeeded: AtomicU64,
    queries_failed: AtomicU64,
    sample_hashes_returned: AtomicU64,
    sample_hashes_suppressed: AtomicU64,
    sample_hashes_novel: AtomicU64,
    triage_queued: AtomicU64,
    triage_closed_dropped: AtomicU64,
    put_commands: AtomicU64,
    drop_commands: AtomicU64,
    recursive_nodes: AtomicU64,
    recursive_nodes_queued: AtomicU64,
    recursive_nodes_closed_dropped: AtomicU64,
    recursive_nodes_timed_out_dropped: AtomicU64,
    shutdown_queued_dropped: AtomicU64,
    shutdown_tasks_cancelled: AtomicU64,
    shutdown_triage_hashes_dropped: AtomicU64,
    shutdown_recursive_nodes_dropped: AtomicU64,
}

/// Cloneable, sender-free view of `sample_infohashes` worker counters.
#[derive(Clone, Default)]
pub struct DhtSampleInfoHashesWorkerStatsHandle {
    inner: Arc<DhtSampleInfoHashesWorkerStatsInner>,
}

/// One non-transactional snapshot of monotonic worker counters.
///
/// After normal EOF, `dequeued = candidate_skipped + queries_started =
/// tasks_completed`, `queries_started = queries_succeeded + queries_failed`,
/// `sample_hashes_returned = sample_hashes_suppressed + sample_hashes_novel`,
/// and `sample_hashes_novel = triage_queued + triage_closed_dropped`.
/// Recursive nodes are classified as queued, receiver-closed, or timed out.
/// On shutdown, completed plus cancelled tasks account for every dequeued item,
/// while the two shutdown suffix counters extend the corresponding downstream
/// conservation equations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtSampleInfoHashesWorkerStats {
    pub dequeued: u64,
    pub candidate_skipped: u64,
    pub queries_started: u64,
    pub tasks_completed: u64,
    pub queries_succeeded: u64,
    pub queries_failed: u64,
    pub sample_hashes_returned: u64,
    pub sample_hashes_suppressed: u64,
    pub sample_hashes_novel: u64,
    pub triage_queued: u64,
    pub triage_closed_dropped: u64,
    /// Attempted atomic table puts, including rejected/no-op commands.
    pub put_commands: u64,
    /// Attempted atomic table drops, including absent-ID commands.
    pub drop_commands: u64,
    pub recursive_nodes: u64,
    pub recursive_nodes_queued: u64,
    pub recursive_nodes_closed_dropped: u64,
    pub recursive_nodes_timed_out_dropped: u64,
    pub shutdown_queued_dropped: u64,
    pub shutdown_tasks_cancelled: u64,
    pub shutdown_triage_hashes_dropped: u64,
    pub shutdown_recursive_nodes_dropped: u64,
}

impl DhtSampleInfoHashesWorkerStatsHandle {
    /// Read each saturating counter independently with relaxed ordering.
    ///
    /// Cross-field conservation is guaranteed only after normal worker exit.
    #[must_use]
    pub fn snapshot(&self) -> DhtSampleInfoHashesWorkerStats {
        let inner = &self.inner;
        DhtSampleInfoHashesWorkerStats {
            dequeued: inner.dequeued.load(Ordering::Relaxed),
            candidate_skipped: inner.candidate_skipped.load(Ordering::Relaxed),
            queries_started: inner.queries_started.load(Ordering::Relaxed),
            tasks_completed: inner.tasks_completed.load(Ordering::Relaxed),
            queries_succeeded: inner.queries_succeeded.load(Ordering::Relaxed),
            queries_failed: inner.queries_failed.load(Ordering::Relaxed),
            sample_hashes_returned: inner.sample_hashes_returned.load(Ordering::Relaxed),
            sample_hashes_suppressed: inner.sample_hashes_suppressed.load(Ordering::Relaxed),
            sample_hashes_novel: inner.sample_hashes_novel.load(Ordering::Relaxed),
            triage_queued: inner.triage_queued.load(Ordering::Relaxed),
            triage_closed_dropped: inner.triage_closed_dropped.load(Ordering::Relaxed),
            put_commands: inner.put_commands.load(Ordering::Relaxed),
            drop_commands: inner.drop_commands.load(Ordering::Relaxed),
            recursive_nodes: inner.recursive_nodes.load(Ordering::Relaxed),
            recursive_nodes_queued: inner.recursive_nodes_queued.load(Ordering::Relaxed),
            recursive_nodes_closed_dropped: inner
                .recursive_nodes_closed_dropped
                .load(Ordering::Relaxed),
            recursive_nodes_timed_out_dropped: inner
                .recursive_nodes_timed_out_dropped
                .load(Ordering::Relaxed),
            shutdown_queued_dropped: inner.shutdown_queued_dropped.load(Ordering::Relaxed),
            shutdown_tasks_cancelled: inner.shutdown_tasks_cancelled.load(Ordering::Relaxed),
            shutdown_triage_hashes_dropped: inner
                .shutdown_triage_hashes_dropped
                .load(Ordering::Relaxed),
            shutdown_recursive_nodes_dropped: inner
                .shutdown_recursive_nodes_dropped
                .load(Ordering::Relaxed),
        }
    }
}

/// Owned, bounded consumer for mixed discovered and retained sample work.
///
/// Discovered-node snapshots are always eligible. Generation-specific retained
/// handles are rechecked inside their accepted task and all later address reads
/// remain live. Every accepted task, sequential triage send, table update, and
/// recursive-discovery wait remains owned by this worker.
///
/// At most `max_inflight` tasks are accepted at once. The input is not polled at
/// capacity, so no extra work item is retained outside the shared route. EOF
/// joins every accepted task. Shutdown closes and drains input, aborts accepted
/// tasks, and awaits every cancellation before returning.
#[must_use = "the worker must be run to consume sample_infohashes work"]
pub struct DhtSampleInfoHashesWorker {
    client: DhtRuntimeClient,
    target: DhtCrawlerTarget,
    deduper: DhtInfoHashDeduper,
    core: DhtSampleInfoHashesWorkerCore,
}

impl DhtSampleInfoHashesWorker {
    /// Construct the fixed hundred-task worker with one fresh stable deduper.
    pub fn new(
        input: DhtDiscoveredNodeSampleInfoHashesReceiver,
        client: DhtRuntimeClient,
        table: KTable,
        triage: DhtInfoHashTriageInput,
        discovery: DhtDiscoverySender,
        target: DhtCrawlerTarget,
    ) -> (Self, DhtSampleInfoHashesWorkerStatsHandle) {
        Self::with_config(
            input,
            client,
            table,
            triage,
            discovery,
            target,
            DhtSampleInfoHashesWorkerConfig::default(),
        )
    }

    /// Construct a worker with an explicit concurrency bound.
    pub fn with_config(
        input: DhtDiscoveredNodeSampleInfoHashesReceiver,
        client: DhtRuntimeClient,
        table: KTable,
        triage: DhtInfoHashTriageInput,
        discovery: DhtDiscoverySender,
        target: DhtCrawlerTarget,
        config: DhtSampleInfoHashesWorkerConfig,
    ) -> (Self, DhtSampleInfoHashesWorkerStatsHandle) {
        let stats = DhtSampleInfoHashesWorkerStatsHandle::default();
        (
            Self {
                client,
                target,
                deduper: DhtInfoHashDeduper::new(),
                core: DhtSampleInfoHashesWorkerCore::new(
                    input,
                    table,
                    triage,
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
    /// Shutdown is biased ahead of a ready task join and receive. Cancelling an
    /// already-started runtime query stops awaiting it, but cannot retract a UDP
    /// datagram that may already have been sent.
    pub async fn run<F>(mut self, shutdown: F) -> DhtSampleInfoHashesWorkerExit
    where
        F: Future<Output = ()>,
    {
        let client = self.client.clone();
        let target = self.target.clone();
        let deduper = self.deduper.clone();
        self.core
            .run_with(
                shutdown,
                move || target.current(),
                move |remote, target| {
                    let client = client.clone();
                    async move { client.sample_infohashes(remote, target).await }
                },
                move |info_hash| deduper.test_and_add(info_hash),
                Instant::now,
                || tokio::time::Instant::now() + RECURSIVE_FANOUT_TIMEOUT,
                |_, _| {},
                |_, _| {},
            )
            .await
    }
}

struct DhtSampleInfoHashesWorkerCore {
    input: DhtDiscoveredNodeSampleInfoHashesReceiver,
    table: KTable,
    triage: DhtInfoHashTriageInput,
    discovery: DhtDiscoverySender,
    max_inflight: NonZeroUsize,
    tasks: JoinSet<()>,
    stats: DhtSampleInfoHashesWorkerStatsHandle,
    abandoned_triage_hashes: Arc<AtomicUsize>,
    abandoned_recursive_nodes: Arc<AtomicUsize>,
    shutdown_in_progress: Arc<AtomicBool>,
}

impl DhtSampleInfoHashesWorkerCore {
    fn new(
        input: DhtDiscoveredNodeSampleInfoHashesReceiver,
        table: KTable,
        triage: DhtInfoHashTriageInput,
        discovery: DhtDiscoverySender,
        max_inflight: NonZeroUsize,
        stats: DhtSampleInfoHashesWorkerStatsHandle,
    ) -> Self {
        Self {
            input,
            table,
            triage,
            discovery,
            max_inflight,
            tasks: JoinSet::new(),
            stats,
            abandoned_triage_hashes: Arc::new(AtomicUsize::new(0)),
            abandoned_recursive_nodes: Arc::new(AtomicUsize::new(0)),
            shutdown_in_progress: Arc::new(AtomicBool::new(false)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_with<F, T, Q, QF, E, D, N, FD, BT, BR>(
        &mut self,
        shutdown: F,
        target_snapshot: T,
        query: Q,
        dedup: D,
        now: N,
        fanout_deadline: FD,
        before_triage_send: BT,
        before_recursive_reserve: BR,
    ) -> DhtSampleInfoHashesWorkerExit
    where
        F: Future<Output = ()>,
        T: Fn() -> Id20 + Clone + Send + Sync + 'static,
        Q: Fn(SocketAddr, Id20) -> QF + Clone + Send + Sync + 'static,
        QF: Future<Output = Result<SampleInfoHashesResult, E>> + Send + 'static,
        E: Send + 'static,
        D: Fn(Id20) -> bool + Clone + Send + Sync + 'static,
        N: Fn() -> Instant + Clone + Send + Sync + 'static,
        FD: Fn() -> tokio::time::Instant + Clone + Send + Sync + 'static,
        BT: Fn(usize, &DhtInfoHashTriageRequest) + Clone + Send + Sync + 'static,
        BR: Fn(usize, &RoutingNode) + Clone + Send + Sync + 'static,
    {
        tokio::pin!(shutdown);
        let mut input_closed = false;

        loop {
            if input_closed && self.tasks.is_empty() {
                return DhtSampleInfoHashesWorkerExit::InputClosed;
            }

            enum Event {
                Shutdown,
                Joined(Result<(), JoinError>),
                Input(Option<DhtDiscoveredNodeSampleInfoHashesWork>),
            }

            let event = tokio::select! {
                biased;
                () = &mut shutdown => Event::Shutdown,
                joined = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    Event::Joined(joined.expect("guarded sample-infohashes task remains present"))
                }
                work = self.input.recv_work(),
                    if !input_closed && self.tasks.len() < self.max_inflight.get() =>
                {
                    Event::Input(work)
                }
            };

            match event {
                Event::Shutdown => return self.finish_shutdown().await,
                Event::Joined(Ok(())) => {}
                Event::Joined(Err(error)) => self.finish_abnormal_join(error).await,
                Event::Input(Some(work)) => {
                    increment_saturating(&self.stats.inner.dequeued);
                    let table = self.table.clone();
                    let triage = self.triage.clone();
                    let discovery = self.discovery.clone();
                    let stats = self.stats.clone();
                    let target_snapshot = target_snapshot.clone();
                    let query = query.clone();
                    let dedup = dedup.clone();
                    let now = now.clone();
                    let fanout_deadline = fanout_deadline.clone();
                    let before_triage_send = before_triage_send.clone();
                    let before_recursive_reserve = before_recursive_reserve.clone();
                    let abandoned_triage_hashes = Arc::clone(&self.abandoned_triage_hashes);
                    let abandoned_recursive_nodes = Arc::clone(&self.abandoned_recursive_nodes);
                    let shutdown_in_progress = Arc::clone(&self.shutdown_in_progress);
                    self.tasks.spawn(async move {
                        finish_sample_work(
                            work,
                            &table,
                            &triage,
                            &discovery,
                            &stats,
                            abandoned_triage_hashes,
                            abandoned_recursive_nodes,
                            shutdown_in_progress,
                            target_snapshot,
                            query,
                            dedup,
                            now,
                            fanout_deadline,
                            before_triage_send,
                            before_recursive_reserve,
                        )
                        .await;
                    });
                }
                Event::Input(None) => input_closed = true,
            }
        }
    }

    async fn finish_shutdown(&mut self) -> DhtSampleInfoHashesWorkerExit {
        self.input.close();
        let mut queued_dropped = 0usize;
        while self.input.recv_work().await.is_some() {
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
                Err(error) => panic!("unexpected sample-infohashes task join error: {error}"),
            }
        }
        add_saturating(&self.stats.inner.shutdown_tasks_cancelled, tasks_cancelled);
        let triage_hashes_dropped = self.abandoned_triage_hashes.load(Ordering::Relaxed);
        let recursive_nodes_dropped = self.abandoned_recursive_nodes.load(Ordering::Relaxed);
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
        DhtSampleInfoHashesWorkerExit::Shutdown {
            queued_dropped,
            tasks_cancelled,
            triage_hashes_dropped,
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
        panic!("sample-infohashes task was cancelled outside worker cleanup")
    }
}

impl Drop for DhtSampleInfoHashesWorkerCore {
    fn drop(&mut self) {
        self.input.close();
        self.tasks.abort_all();
    }
}

impl DhtDiscoveredNodeSampleInfoHashesWork {
    fn sample_candidate(&self) -> bool {
        match self {
            Self::Discovered(_) => true,
            Self::Retained(handle) => handle.is_sample_infohashes_candidate(),
        }
    }

    fn sample_id(&self) -> Id20 {
        match self {
            Self::Discovered(node) => node.id,
            Self::Retained(handle) => handle.id(),
        }
    }

    fn sample_addr(&self) -> SocketAddr {
        match self {
            Self::Discovered(node) => node.addr,
            Self::Retained(handle) => handle.addr(),
        }
    }
}

struct TriageGuard {
    remaining: usize,
    stats: DhtSampleInfoHashesWorkerStatsHandle,
    abandoned: Arc<AtomicUsize>,
    shutdown_in_progress: Arc<AtomicBool>,
}

impl TriageGuard {
    fn queued_one(&mut self) {
        self.remaining = self.remaining.saturating_sub(1);
        increment_saturating(&self.stats.inner.triage_queued);
    }

    fn receiver_closed(&mut self) {
        add_saturating(&self.stats.inner.triage_closed_dropped, self.remaining);
        self.remaining = 0;
    }
}

impl Drop for TriageGuard {
    fn drop(&mut self) {
        if self.remaining == 0 || !self.shutdown_in_progress.load(Ordering::SeqCst) {
            return;
        }
        add_saturating(
            &self.stats.inner.shutdown_triage_hashes_dropped,
            self.remaining,
        );
        add_saturating_usize(&self.abandoned, self.remaining);
    }
}

struct RecursiveFanoutGuard {
    remaining: usize,
    stats: DhtSampleInfoHashesWorkerStatsHandle,
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

#[allow(clippy::too_many_arguments)]
async fn finish_sample_work<T, Q, QF, E, D, N, FD, BT, BR>(
    work: DhtDiscoveredNodeSampleInfoHashesWork,
    table: &KTable,
    triage: &DhtInfoHashTriageInput,
    discovery: &DhtDiscoverySender,
    stats: &DhtSampleInfoHashesWorkerStatsHandle,
    abandoned_triage_hashes: Arc<AtomicUsize>,
    abandoned_recursive_nodes: Arc<AtomicUsize>,
    shutdown_in_progress: Arc<AtomicBool>,
    target_snapshot: T,
    query: Q,
    dedup: D,
    now: N,
    fanout_deadline: FD,
    before_triage_send: BT,
    before_recursive_reserve: BR,
) where
    T: Fn() -> Id20,
    Q: Fn(SocketAddr, Id20) -> QF,
    QF: Future<Output = Result<SampleInfoHashesResult, E>>,
    D: Fn(Id20) -> bool,
    N: Fn() -> Instant,
    FD: Fn() -> tokio::time::Instant,
    BT: Fn(usize, &DhtInfoHashTriageRequest),
    BR: Fn(usize, &RoutingNode),
{
    if !work.sample_candidate() {
        increment_saturating(&stats.inner.candidate_skipped);
        increment_saturating(&stats.inner.tasks_completed);
        return;
    }

    let remote = work.sample_addr();
    let target = target_snapshot();
    increment_saturating(&stats.inner.queries_started);
    let result = query(remote, target).await;
    let SampleInfoHashesResult {
        id: _,
        samples,
        nodes,
        num,
        interval,
    } = match result {
        Ok(result) => {
            increment_saturating(&stats.inner.queries_succeeded);
            result
        }
        Err(_) => {
            increment_saturating(&stats.inner.queries_failed);
            table.batch_command(&[KTableCommand::DropNode {
                id: work.sample_id(),
            }]);
            increment_saturating(&stats.inner.drop_commands);
            increment_saturating(&stats.inner.tasks_completed);
            return;
        }
    };

    let samples = samples.unwrap_or_default();
    add_saturating(&stats.inner.sample_hashes_returned, samples.len());
    let mut novel = Vec::new();
    for info_hash in samples {
        if dedup(info_hash) {
            increment_saturating(&stats.inner.sample_hashes_suppressed);
        } else {
            increment_saturating(&stats.inner.sample_hashes_novel);
            novel.push(DhtInfoHashTriageRequest {
                info_hash,
                source_node_addr: work.sample_addr(),
            });
        }
    }

    let novel_count = novel.len();
    let mut triage_guard = TriageGuard {
        remaining: novel_count,
        stats: stats.clone(),
        abandoned: abandoned_triage_hashes,
        shutdown_in_progress: Arc::clone(&shutdown_in_progress),
    };
    for (index, request) in novel.into_iter().enumerate() {
        before_triage_send(index, &request);
        if triage.send(request).await.is_err() {
            triage_guard.receiver_closed();
            break;
        }
        triage_guard.queued_one();
    }

    let id = work.sample_id();
    let addr = work.sample_addr();
    let discovered_num = i64::try_from(novel_count).unwrap_or(i64::MAX);
    let next_sample_at = next_sample_at(now(), interval, novel_count);
    table.batch_command(&[KTableCommand::PutNode {
        node: RoutingNode { id, addr },
        options: vec![
            KTableNodeOption::Responded,
            KTableNodeOption::Bep51Support(true),
            KTableNodeOption::SampleInfoHashesResponse {
                discovered_num,
                total_num: num,
                next_sample_at,
            },
        ],
    }]);
    increment_saturating(&stats.inner.put_commands);

    add_saturating(&stats.inner.recursive_nodes, nodes.len());
    if nodes.is_empty() {
        increment_saturating(&stats.inner.tasks_completed);
        return;
    }
    let mut fanout = RecursiveFanoutGuard {
        remaining: nodes.len(),
        stats: stats.clone(),
        abandoned: abandoned_recursive_nodes,
        shutdown_in_progress,
    };
    let deadline = fanout_deadline();
    for (index, node) in nodes.into_iter().enumerate() {
        let reserve = async {
            before_recursive_reserve(index, &node);
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
    increment_saturating(&stats.inner.tasks_completed);
}

fn effective_interval_duration_ns(raw_interval: i64, novel_count: usize) -> i64 {
    let effective = if novel_count > 0 && raw_interval > LONG_INTERVAL_THRESHOLD_SECONDS {
        ACTIVE_DISCOVERY_INTERVAL_SECONDS
    } else {
        raw_interval
    };
    effective.wrapping_mul(NANOS_PER_SECOND)
}

fn next_sample_at(now: Instant, raw_interval: i64, novel_count: usize) -> Instant {
    let duration_ns = effective_interval_duration_ns(raw_interval, novel_count);
    if duration_ns >= 0 {
        saturating_add_duration(now, Duration::from_nanos(duration_ns as u64))
    } else {
        saturating_sub_duration(now, Duration::from_nanos(duration_ns.unsigned_abs()))
    }
}

fn saturating_add_duration(value: Instant, duration: Duration) -> Instant {
    saturating_shift_duration(value, duration, true)
}

fn saturating_sub_duration(value: Instant, duration: Duration) -> Instant {
    saturating_shift_duration(value, duration, false)
}

fn saturating_shift_duration(value: Instant, duration: Duration, add: bool) -> Instant {
    let checked = |duration| {
        if add {
            value.checked_add(duration)
        } else {
            value.checked_sub(duration)
        }
    };
    if let Some(shifted) = checked(duration) {
        return shifted;
    }

    let mut low = 0_u64;
    let mut high = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
    while low < high {
        let middle = low + (high - low) / 2 + 1;
        if checked(Duration::from_nanos(middle)).is_some() {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    checked(Duration::from_nanos(low)).unwrap_or(value)
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
#[path = "dht_sample_infohashes_worker_parity.rs"]
mod parity_tests;

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::{pending, poll_fn, ready, Future};
    use std::net::{IpAddr, Ipv4Addr};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    use tokio::sync::{oneshot, Notify};

    use super::*;
    use crate::{
        dht_discovery_channel, dht_info_hash_triage_channel,
        DhtDiscoveredNodeSampleInfoHashesInput, DhtDiscoveryReceiver, DhtInfoHashTriageReceiver,
        KTableClock, KTableNodeHandle, RoutingPutResult,
    };

    struct PendingQueryDropProbe {
        drops: Arc<AtomicUsize>,
    }

    impl Future for PendingQueryDropProbe {
        type Output = Result<SampleInfoHashesResult, ()>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingQueryDropProbe {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct PanicClock;

    impl KTableClock for PanicClock {
        fn now(&self) -> Instant {
            panic!("worker test clock sentinel")
        }
    }

    fn id(value: u8) -> Id20 {
        let mut bytes = [0_u8; 20];
        bytes[19] = value;
        Id20::from_slice(&bytes).unwrap()
    }

    fn node(value: u8, port: u16) -> RoutingNode {
        RoutingNode {
            id: id(value),
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, value)), port),
        }
    }

    fn retained(table: &KTable, value: u8, port: u16) -> KTableNodeHandle {
        let node = node(value, port);
        assert_eq!(table.put_node(node), RoutingPutResult::Accepted);
        table.node_handle(node.id).unwrap()
    }

    fn response(
        samples: impl IntoIterator<Item = Id20>,
        nodes: impl IntoIterator<Item = RoutingNode>,
        num: i64,
        interval: i64,
    ) -> SampleInfoHashesResult {
        SampleInfoHashesResult {
            id: id(250),
            samples: Some(samples.into_iter().collect()),
            nodes: nodes.into_iter().collect(),
            num,
            interval,
        }
    }

    fn core(
        input: DhtDiscoveredNodeSampleInfoHashesReceiver,
        table: KTable,
        triage: DhtInfoHashTriageInput,
        discovery: DhtDiscoverySender,
        max_inflight: usize,
    ) -> (
        DhtSampleInfoHashesWorkerCore,
        DhtSampleInfoHashesWorkerStatsHandle,
    ) {
        let stats = DhtSampleInfoHashesWorkerStatsHandle::default();
        (
            DhtSampleInfoHashesWorkerCore::new(
                input,
                table,
                triage,
                discovery,
                NonZeroUsize::new(max_inflight).unwrap(),
                stats.clone(),
            ),
            stats,
        )
    }

    async fn poll_once_pending<F: Future>(mut future: Pin<&mut F>) {
        poll_fn(|context| {
            assert!(future.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
    }

    fn assert_normal_conservation(stats: DhtSampleInfoHashesWorkerStats) {
        assert_eq!(
            stats.dequeued,
            stats
                .candidate_skipped
                .saturating_add(stats.queries_started)
        );
        assert_eq!(stats.dequeued, stats.tasks_completed);
        assert_eq!(
            stats.queries_started,
            stats.queries_succeeded.saturating_add(stats.queries_failed)
        );
        assert_eq!(
            stats.sample_hashes_returned,
            stats
                .sample_hashes_suppressed
                .saturating_add(stats.sample_hashes_novel)
        );
        assert_eq!(
            stats.sample_hashes_novel,
            stats
                .triage_queued
                .saturating_add(stats.triage_closed_dropped)
        );
        assert_eq!(
            stats.recursive_nodes,
            stats
                .recursive_nodes_queued
                .saturating_add(stats.recursive_nodes_closed_dropped)
                .saturating_add(stats.recursive_nodes_timed_out_dropped)
        );
    }

    fn assert_shutdown_conservation(stats: DhtSampleInfoHashesWorkerStats) {
        assert_eq!(
            stats.dequeued,
            stats
                .tasks_completed
                .saturating_add(stats.shutdown_tasks_cancelled)
        );
        assert_eq!(
            stats.sample_hashes_novel,
            stats
                .triage_queued
                .saturating_add(stats.triage_closed_dropped)
                .saturating_add(stats.shutdown_triage_hashes_dropped)
        );
        assert_eq!(
            stats.recursive_nodes,
            stats
                .recursive_nodes_queued
                .saturating_add(stats.recursive_nodes_closed_dropped)
                .saturating_add(stats.recursive_nodes_timed_out_dropped)
                .saturating_add(stats.shutdown_recursive_nodes_dropped)
        );
    }

    #[test]
    fn public_types_constants_and_interval_vectors_are_fixed() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<DhtSampleInfoHashesWorker>();
        assert_send_sync::<DhtSampleInfoHashesWorkerConfig>();
        assert_send_sync::<DhtSampleInfoHashesWorkerExit>();
        assert_send_sync::<DhtSampleInfoHashesWorkerStats>();
        assert_send_sync::<DhtSampleInfoHashesWorkerStatsHandle>();
        assert_eq!(
            DhtSampleInfoHashesWorkerConfig::default()
                .max_inflight
                .get(),
            100
        );
        assert_eq!(RECURSIVE_FANOUT_TIMEOUT, Duration::from_secs(1));
        assert_eq!(
            [
                effective_interval_duration_ns(-7, 1),
                effective_interval_duration_ns(300, 1),
                effective_interval_duration_ns(301, 1),
                effective_interval_duration_ns(301, 0),
                effective_interval_duration_ns(i64::MAX, 1),
                effective_interval_duration_ns(i64::MAX, 0),
                effective_interval_duration_ns(i64::MIN, 1),
                effective_interval_duration_ns(i64::MIN, 0),
            ],
            [
                -7_000_000_000,
                300_000_000_000,
                60_000_000_000,
                301_000_000_000,
                60_000_000_000,
                -1_000_000_000,
                0,
                0,
            ]
        );

        let now = Instant::now();
        assert_eq!(next_sample_at(now, -7, 1), now - Duration::from_secs(7));
        assert_eq!(next_sample_at(now, 301, 1), now + Duration::from_secs(60));
        assert_eq!(
            next_sample_at(now, i64::MAX, 0),
            now - Duration::from_secs(1)
        );
        assert_eq!(next_sample_at(now, i64::MIN, 0), now);
    }

    #[test]
    fn every_counter_saturates() {
        let stats = DhtSampleInfoHashesWorkerStatsHandle::default();
        for counter in [
            &stats.inner.dequeued,
            &stats.inner.candidate_skipped,
            &stats.inner.queries_started,
            &stats.inner.tasks_completed,
            &stats.inner.queries_succeeded,
            &stats.inner.queries_failed,
            &stats.inner.sample_hashes_returned,
            &stats.inner.sample_hashes_suppressed,
            &stats.inner.sample_hashes_novel,
            &stats.inner.triage_queued,
            &stats.inner.triage_closed_dropped,
            &stats.inner.put_commands,
            &stats.inner.drop_commands,
            &stats.inner.recursive_nodes,
            &stats.inner.recursive_nodes_queued,
            &stats.inner.recursive_nodes_closed_dropped,
            &stats.inner.recursive_nodes_timed_out_dropped,
            &stats.inner.shutdown_queued_dropped,
            &stats.inner.shutdown_tasks_cancelled,
            &stats.inner.shutdown_triage_hashes_dropped,
            &stats.inner.shutdown_recursive_nodes_dropped,
        ] {
            counter.store(u64::MAX, Ordering::Relaxed);
            increment_saturating(counter);
            add_saturating(counter, usize::MAX);
            assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
        }
    }

    #[tokio::test]
    async fn ready_shutdown_drains_without_dequeue_or_query() {
        let table = KTable::new(Id20::ZERO);
        let first = retained(&table, 1, 1001);
        let second = retained(&table, 2, 1002);
        let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(2);
        input.send(first).await.unwrap();
        input.send(second).await.unwrap();
        drop(input);
        let (triage, _triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, _discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let (mut core, stats) = core(receiver, table, triage, discovery, 1);

        let exit = core
            .run_with(
                ready(()),
                || id(9),
                |_, _| ready(Err::<SampleInfoHashesResult, ()>(())),
                |_| false,
                Instant::now,
                || tokio::time::Instant::now() + Duration::from_secs(60),
                |_, _| {},
                |_, _| {},
            )
            .await;
        assert_eq!(
            exit,
            DhtSampleInfoHashesWorkerExit::Shutdown {
                queued_dropped: 2,
                tasks_cancelled: 0,
                triage_hashes_dropped: 0,
                recursive_nodes_dropped: 0,
            }
        );
        assert_eq!(
            stats.snapshot(),
            DhtSampleInfoHashesWorkerStats {
                shutdown_queued_dropped: 2,
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn retained_candidate_is_rechecked_after_queueing_and_skipped() {
        let table = KTable::new(Id20::ZERO);
        let handle = retained(&table, 3, 1003);
        let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        input.send(handle).await.unwrap();
        drop(input);
        table.put_node_with_options(node(3, 1003), &[KTableNodeOption::Bep51Support(false)]);
        let (triage, _triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, _discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let (mut core, stats) = core(receiver, table, triage, discovery, 1);

        let exit = core
            .run_with(
                pending(),
                || id(9),
                |_, _| ready(Err::<SampleInfoHashesResult, ()>(())),
                |_| false,
                Instant::now,
                || tokio::time::Instant::now() + Duration::from_secs(60),
                |_, _| {},
                |_, _| {},
            )
            .await;
        assert_eq!(exit, DhtSampleInfoHashesWorkerExit::InputClosed);
        assert_eq!(
            stats.snapshot(),
            DhtSampleInfoHashesWorkerStats {
                dequeued: 1,
                candidate_skipped: 1,
                tasks_completed: 1,
                ..Default::default()
            }
        );
        assert_normal_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn discovered_snapshot_is_always_eligible_and_error_drops_advertised_id() {
        let table = KTable::new(Id20::ZERO);
        let advertised = node(4, 1004);
        assert_eq!(table.put_node(advertised), RoutingPutResult::Accepted);
        let handle = table.node_handle(advertised.id).unwrap();
        let (triage, _triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, _discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let stats = DhtSampleInfoHashesWorkerStatsHandle::default();

        finish_sample_work(
            DhtDiscoveredNodeSampleInfoHashesWork::Discovered(advertised),
            &table,
            &triage,
            &discovery,
            &stats,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(false)),
            || id(44),
            move |remote, target| {
                assert_eq!(remote, advertised.addr);
                assert_eq!(target, id(44));
                ready(Err::<SampleInfoHashesResult, ()>(()))
            },
            |_| panic!("query error reached deduper"),
            Instant::now,
            || tokio::time::Instant::now() + Duration::from_secs(60),
            |_, _| {},
            |_, _| {},
        )
        .await;

        assert!(handle.dropped());
        assert_eq!(
            stats.snapshot(),
            DhtSampleInfoHashesWorkerStats {
                queries_started: 1,
                tasks_completed: 1,
                queries_failed: 1,
                drop_commands: 1,
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn success_preserves_full_dedupe_dynamic_addresses_and_atomic_put() {
        let table = KTable::new(Id20::ZERO);
        let handle = retained(&table, 5, 1005);
        let (triage, mut triage_receiver) =
            dht_info_hash_triage_channel(NonZeroUsize::new(4).unwrap());
        let (discovery, _discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let stats = DhtSampleInfoHashesWorkerStatsHandle::default();
        let target = id(90);
        let base = Instant::now();
        let dedup_results = Arc::new(Mutex::new(VecDeque::from([false, true, false])));
        let dedup_order = Arc::new(Mutex::new(Vec::new()));
        let deadline_calls = Arc::new(AtomicUsize::new(0));

        let target_table = table.clone();
        let dedup_table = table.clone();
        let dedup_results_clone = Arc::clone(&dedup_results);
        let dedup_order_clone = Arc::clone(&dedup_order);
        let put_table = table.clone();
        let deadline_calls_clone = Arc::clone(&deadline_calls);
        finish_sample_work(
            DhtDiscoveredNodeSampleInfoHashesWork::Retained(handle.clone()),
            &table,
            &triage,
            &discovery,
            &stats,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(false)),
            move || {
                target_table.put_node(node(5, 1006));
                target
            },
            move |remote, actual_target| {
                assert_eq!(remote, node(5, 1005).addr);
                assert_eq!(actual_target, target);
                ready(Ok::<_, ()>(response(
                    [id(10), id(11), id(12)],
                    [],
                    -17,
                    301,
                )))
            },
            move |info_hash| {
                dedup_order_clone.lock().unwrap().push(info_hash);
                let next_port = 1007 + dedup_order_clone.lock().unwrap().len() as u16;
                dedup_table.put_node(node(5, next_port));
                dedup_results_clone.lock().unwrap().pop_front().unwrap()
            },
            move || base,
            move || {
                deadline_calls_clone.fetch_add(1, Ordering::SeqCst);
                tokio::time::Instant::now() + Duration::from_secs(60)
            },
            move |index, _| {
                if index == 1 {
                    put_table.put_node(node(5, 1011));
                }
            },
            |_, _| {},
        )
        .await;

        assert_eq!(*dedup_order.lock().unwrap(), vec![id(10), id(11), id(12)]);
        assert_eq!(deadline_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            triage_receiver.recv().await,
            Some(DhtInfoHashTriageRequest {
                info_hash: id(10),
                source_node_addr: node(5, 1008).addr,
            })
        );
        assert_eq!(
            triage_receiver.recv().await,
            Some(DhtInfoHashTriageRequest {
                info_hash: id(12),
                source_node_addr: node(5, 1010).addr,
            })
        );
        assert_eq!(handle.addr(), node(5, 1011).addr);
        assert_eq!(handle.bep51_support(), crate::KTableBep51Support::Yes);
        assert_eq!(handle.sampled_num(), 2);
        assert_eq!(handle.last_discovered_num(), 2);
        assert_eq!(handle.total_num(), -17);
        assert_eq!(
            handle.next_sample_infohashes_at(),
            Some(base + Duration::from_secs(60))
        );
        assert_eq!(
            stats.snapshot(),
            DhtSampleInfoHashesWorkerStats {
                queries_started: 1,
                tasks_completed: 1,
                queries_succeeded: 1,
                sample_hashes_returned: 3,
                sample_hashes_suppressed: 1,
                sample_hashes_novel: 2,
                triage_queued: 2,
                put_commands: 1,
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn absent_samples_are_empty_and_apply_existing_zero_discovery_penalty() {
        let table = KTable::new(Id20::ZERO);
        let handle = retained(&table, 22, 1022);
        let (triage, mut triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, _discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let stats = DhtSampleInfoHashesWorkerStatsHandle::default();
        let base = Instant::now();
        let deadline_calls = Arc::new(AtomicUsize::new(0));
        let deadline_calls_clone = Arc::clone(&deadline_calls);

        finish_sample_work(
            DhtDiscoveredNodeSampleInfoHashesWork::Retained(handle.clone()),
            &table,
            &triage,
            &discovery,
            &stats,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(false)),
            || id(99),
            |_, _| {
                ready(Ok::<_, ()>(SampleInfoHashesResult {
                    id: id(200),
                    samples: None,
                    nodes: Vec::new(),
                    num: 7,
                    interval: 10,
                }))
            },
            |_| panic!("absent samples reached deduper"),
            move || base,
            move || {
                deadline_calls_clone.fetch_add(1, Ordering::SeqCst);
                tokio::time::Instant::now() + Duration::from_secs(60)
            },
            |_, _| {},
            |_, _| {},
        )
        .await;

        assert!(matches!(
            triage_receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(deadline_calls.load(Ordering::SeqCst), 0);
        assert_eq!(handle.sampled_num(), 0);
        assert_eq!(handle.last_discovered_num(), 0);
        assert_eq!(handle.total_num(), 7);
        assert_eq!(
            handle.next_sample_infohashes_at(),
            Some(base + Duration::from_secs(5 * 60 + 10))
        );
        assert_eq!(
            stats.snapshot(),
            DhtSampleInfoHashesWorkerStats {
                queries_started: 1,
                tasks_completed: 1,
                queries_succeeded: 1,
                put_commands: 1,
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn triage_closure_drops_whole_suffix_then_puts_and_fans_out() {
        let table = KTable::new(Id20::ZERO);
        let handle = retained(&table, 6, 1006);
        let (triage, mut triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        triage_receiver.close();
        let (discovery, mut discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let stats = DhtSampleInfoHashesWorkerStatsHandle::default();

        finish_sample_work(
            DhtDiscoveredNodeSampleInfoHashesWork::Retained(handle),
            &table,
            &triage,
            &discovery,
            &stats,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(false)),
            || id(9),
            |_, _| {
                ready(Ok::<_, ()>(response(
                    [id(20), id(21)],
                    [node(22, 2022)],
                    5,
                    10,
                )))
            },
            |_| false,
            Instant::now,
            || tokio::time::Instant::now() + Duration::from_secs(60),
            |_, _| {},
            |_, _| {},
        )
        .await;

        assert_eq!(discovery_receiver.recv().await, Some(node(22, 2022)));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.triage_closed_dropped, 2);
        assert_eq!(snapshot.put_commands, 1);
        assert_eq!(snapshot.recursive_nodes_queued, 1);
        assert_eq!(snapshot.tasks_completed, 1);
    }

    #[tokio::test]
    async fn recursive_receiver_closure_drops_whole_suffix() {
        let table = KTable::new(Id20::ZERO);
        let handle = retained(&table, 7, 1007);
        let (triage, _triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        drop(discovery_receiver);
        let stats = DhtSampleInfoHashesWorkerStatsHandle::default();

        finish_sample_work(
            DhtDiscoveredNodeSampleInfoHashesWork::Retained(handle),
            &table,
            &triage,
            &discovery,
            &stats,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(false)),
            || id(9),
            |_, _| {
                ready(Ok::<_, ()>(response(
                    [],
                    [node(30, 3030), node(31, 3031)],
                    0,
                    1,
                )))
            },
            |_| false,
            Instant::now,
            || tokio::time::Instant::now() + Duration::from_secs(60),
            |_, _| {},
            |_, _| {},
        )
        .await;

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.recursive_nodes, 2);
        assert_eq!(snapshot.recursive_nodes_closed_dropped, 2);
        assert_eq!(snapshot.tasks_completed, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn one_shared_recursive_deadline_preserves_prefix_and_times_out_suffix() {
        let table = KTable::new(Id20::ZERO);
        let handle = retained(&table, 8, 1008);
        let (triage, _triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, mut discovery_receiver) =
            dht_discovery_channel(NonZeroUsize::new(2).unwrap());
        let stats = DhtSampleInfoHashesWorkerStatsHandle::default();
        let now = Instant::now();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let deadline_calls = Arc::new(AtomicUsize::new(0));
        let deadline_calls_clone = Arc::clone(&deadline_calls);
        let future = finish_sample_work(
            DhtDiscoveredNodeSampleInfoHashesWork::Retained(handle),
            &table,
            &triage,
            &discovery,
            &stats,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(false)),
            || id(9),
            |_, _| {
                ready(Ok::<_, ()>(response(
                    [],
                    [
                        node(40, 4040),
                        node(41, 4041),
                        node(42, 4042),
                        node(43, 4043),
                    ],
                    0,
                    1,
                )))
            },
            |_| false,
            move || now,
            move || {
                deadline_calls_clone.fetch_add(1, Ordering::SeqCst);
                deadline
            },
            |_, _| {},
            |_, _| {},
        );
        tokio::pin!(future);
        poll_once_pending(future.as_mut()).await;
        tokio::time::advance(Duration::from_secs(1)).await;
        future.await;

        assert_eq!(discovery_receiver.try_recv(), Ok(node(40, 4040)));
        assert_eq!(discovery_receiver.try_recv(), Ok(node(41, 4041)));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.recursive_nodes, 4);
        assert_eq!(snapshot.recursive_nodes_queued, 2);
        assert_eq!(snapshot.recursive_nodes_timed_out_dropped, 2);
        assert_eq!(snapshot.tasks_completed, 1);
        assert_eq!(deadline_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shutdown_during_blocked_triage_counts_exact_suffix() {
        let table = KTable::new(Id20::ZERO);
        let handle = retained(&table, 9, 1009);
        let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        input.send(handle).await.unwrap();
        drop(input);
        let (triage, mut triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, _discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let (mut core, stats) = core(receiver, table, triage, discovery, 1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (blocked_tx, blocked_rx) = oneshot::channel();
        let blocked_tx = Arc::new(Mutex::new(Some(blocked_tx)));

        let run = tokio::spawn(async move {
            core.run_with(
                async {
                    let _ = shutdown_rx.await;
                },
                || id(99),
                |_, _| ready(Ok::<_, ()>(response([id(50), id(51)], [], 0, 1))),
                |_| false,
                Instant::now,
                || tokio::time::Instant::now() + Duration::from_secs(60),
                move |index, _| {
                    if index == 1 {
                        if let Some(sender) = blocked_tx.lock().unwrap().take() {
                            let _ = sender.send(());
                        }
                    }
                },
                |_, _| {},
            )
            .await
        });
        blocked_rx.await.unwrap();
        shutdown_tx.send(()).unwrap();
        let exit = run.await.unwrap();
        assert_eq!(
            exit,
            DhtSampleInfoHashesWorkerExit::Shutdown {
                queued_dropped: 0,
                tasks_cancelled: 1,
                triage_hashes_dropped: 1,
                recursive_nodes_dropped: 0,
            }
        );
        assert!(triage_receiver.recv().await.is_some());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.triage_queued, 1);
        assert_eq!(snapshot.shutdown_triage_hashes_dropped, 1);
        assert_eq!(snapshot.put_commands, 0);
        assert_eq!(snapshot.recursive_nodes, 0);
        assert_shutdown_conservation(snapshot);
    }

    #[tokio::test]
    async fn retained_old_generation_updates_current_generation_by_advertised_identity() {
        let table = KTable::new(Id20::ZERO);
        let old = retained(&table, 17, 1017);
        let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        input.send(old.clone()).await.unwrap();
        drop(input);
        assert!(table.drop_node(id(17)));
        let new = retained(&table, 17, 2017);
        assert_ne!(old, new);
        assert!(old.dropped());
        let (triage, _triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, _discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let (mut core, stats) = core(receiver, table, triage, discovery, 1);

        assert_eq!(
            core.run_with(
                pending(),
                || id(99),
                |remote, target| {
                    assert_eq!(remote, node(17, 1017).addr);
                    assert_eq!(target, id(99));
                    ready(Ok::<_, ()>(response([], [], 0, 1)))
                },
                |_| false,
                Instant::now,
                || tokio::time::Instant::now() + Duration::from_secs(60),
                |_, _| {},
                |_, _| {},
            )
            .await,
            DhtSampleInfoHashesWorkerExit::InputClosed
        );

        assert!(old.dropped());
        assert_eq!(old.addr(), node(17, 1017).addr);
        assert_eq!(old.last_responded_at(), None);
        assert_eq!(new.addr(), node(17, 1017).addr);
        assert!(new.last_responded_at().is_some());
        assert_normal_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn shutdown_during_blocked_recursive_fanout_counts_exact_suffix() {
        let table = KTable::new(Id20::ZERO);
        let handle = retained(&table, 10, 1010);
        let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        input.send(handle).await.unwrap();
        drop(input);
        let (triage, _triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, mut discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let (mut core, stats) = core(receiver, table, triage, discovery, 1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (blocked_tx, blocked_rx) = oneshot::channel();
        let blocked_tx = Arc::new(Mutex::new(Some(blocked_tx)));

        let run = tokio::spawn(async move {
            core.run_with(
                async {
                    let _ = shutdown_rx.await;
                },
                || id(99),
                |_, _| {
                    ready(Ok::<_, ()>(response(
                        [],
                        [node(60, 6060), node(61, 6061)],
                        0,
                        1,
                    )))
                },
                |_| false,
                Instant::now,
                || tokio::time::Instant::now() + Duration::from_secs(60),
                |_, _| {},
                move |index, _| {
                    if index == 1 {
                        if let Some(sender) = blocked_tx.lock().unwrap().take() {
                            let _ = sender.send(());
                        }
                    }
                },
            )
            .await
        });
        blocked_rx.await.unwrap();
        shutdown_tx.send(()).unwrap();
        let exit = run.await.unwrap();
        assert_eq!(
            exit,
            DhtSampleInfoHashesWorkerExit::Shutdown {
                queued_dropped: 0,
                tasks_cancelled: 1,
                triage_hashes_dropped: 0,
                recursive_nodes_dropped: 1,
            }
        );
        assert_eq!(discovery_receiver.recv().await, Some(node(60, 6060)));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.put_commands, 1);
        assert_eq!(snapshot.recursive_nodes_queued, 1);
        assert_eq!(snapshot.shutdown_recursive_nodes_dropped, 1);
        assert_shutdown_conservation(snapshot);
    }

    #[tokio::test]
    async fn max_inflight_does_not_overdequeue_and_shutdown_drains_queue() {
        let table = KTable::new(Id20::ZERO);
        let first = retained(&table, 11, 1011);
        let second = retained(&table, 12, 1012);
        let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(2);
        input.send(first).await.unwrap();
        input.send(second).await.unwrap();
        drop(input);
        let (triage, _triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, _discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let (mut core, stats) = core(receiver, table, triage, discovery, 1);
        let (started_tx, started_rx) = oneshot::channel();
        let started_tx = Arc::new(Mutex::new(Some(started_tx)));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let run = tokio::spawn(async move {
            core.run_with(
                async {
                    let _ = shutdown_rx.await;
                },
                || id(99),
                move |_, _| {
                    if let Some(sender) = started_tx.lock().unwrap().take() {
                        let _ = sender.send(());
                    }
                    pending::<Result<SampleInfoHashesResult, ()>>()
                },
                |_| false,
                Instant::now,
                || tokio::time::Instant::now() + Duration::from_secs(60),
                |_, _| {},
                |_, _| {},
            )
            .await
        });
        started_rx.await.unwrap();
        assert_eq!(stats.snapshot().dequeued, 1);
        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await.unwrap(),
            DhtSampleInfoHashesWorkerExit::Shutdown {
                queued_dropped: 1,
                tasks_cancelled: 1,
                triage_hashes_dropped: 0,
                recursive_nodes_dropped: 0,
            }
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.shutdown_queued_dropped, 1);
        assert_shutdown_conservation(snapshot);
    }

    #[tokio::test]
    async fn eof_waits_for_accepted_query_then_returns_input_closed() {
        let table = KTable::new(Id20::ZERO);
        let handle = retained(&table, 13, 1013);
        let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        input.send(handle).await.unwrap();
        drop(input);
        let (triage, _triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, _discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let (mut core, stats) = core(receiver, table, triage, discovery, 1);
        let (result_tx, result_rx) = oneshot::channel::<Result<SampleInfoHashesResult, ()>>();
        let result_rx = Arc::new(Mutex::new(Some(result_rx)));

        let run = core.run_with(
            pending(),
            || id(99),
            move |_, _| {
                let receiver = result_rx.lock().unwrap().take().unwrap();
                async move { receiver.await.unwrap() }
            },
            |_| false,
            Instant::now,
            || tokio::time::Instant::now() + Duration::from_secs(60),
            |_, _| {},
            |_, _| {},
        );
        tokio::pin!(run);
        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot().dequeued, 1);
        result_tx
            .send(Ok(response([], [], 0, 1)))
            .expect("accepted query remains waiting");
        assert_eq!(run.await, DhtSampleInfoHashesWorkerExit::InputClosed);
        assert_normal_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn dropping_core_closes_input_and_recovers_exact_retained_handle() {
        let table = KTable::new(Id20::ZERO);
        let handle = retained(&table, 14, 1014);
        let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        let (triage, _triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, _discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let (core, _stats) = core(receiver, table, triage, discovery, 1);
        drop(core);

        let recovered = input.send(handle.clone()).await.unwrap_err().into_node();
        assert_eq!(recovered, handle);
    }

    #[tokio::test]
    async fn dropping_an_active_run_aborts_query_without_terminal_accounting() {
        let table = KTable::new(Id20::ZERO);
        let handle = retained(&table, 18, 1018);
        let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        input.send(handle).await.unwrap();
        drop(input);
        let (triage, _triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, _discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let (mut core, stats) = core(receiver, table, triage, discovery, 1);
        let drops = Arc::new(AtomicUsize::new(0));
        let query_drops = Arc::clone(&drops);
        let (started_tx, started_rx) = oneshot::channel();
        let started_tx = Arc::new(Mutex::new(Some(started_tx)));

        let run = tokio::spawn(async move {
            core.run_with(
                pending(),
                || id(99),
                move |_, _| {
                    if let Some(sender) = started_tx.lock().unwrap().take() {
                        let _ = sender.send(());
                    }
                    PendingQueryDropProbe {
                        drops: Arc::clone(&query_drops),
                    }
                },
                |_| false,
                Instant::now,
                || tokio::time::Instant::now() + Duration::from_secs(60),
                |_, _| {},
                |_, _| {},
            )
            .await
        });
        started_rx.await.unwrap();
        run.abort();
        assert!(run.await.unwrap_err().is_cancelled());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            stats.snapshot(),
            DhtSampleInfoHashesWorkerStats {
                dequeued: 1,
                queries_started: 1,
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn shutdown_opaque_drain_does_not_project_poisoned_retained_handle() {
        let table = KTable::with_clock(Id20::ZERO, Arc::new(PanicClock));
        let handle = retained(&table, 19, 1019);
        let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        input.send(handle).await.unwrap();
        drop(input);
        assert!(catch_unwind(AssertUnwindSafe(|| {
            table.put_node_with_options(node(19, 1019), &[KTableNodeOption::Responded]);
        }))
        .is_err());
        let (triage, _triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, _discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let (mut core, stats) = core(receiver, table, triage, discovery, 1);

        let exit = core
            .run_with(
                ready(()),
                || id(99),
                |_, _| ready(Err::<SampleInfoHashesResult, ()>(())),
                |_| false,
                Instant::now,
                || tokio::time::Instant::now() + Duration::from_secs(60),
                |_, _| {},
                |_, _| {},
            )
            .await;
        assert_eq!(
            exit,
            DhtSampleInfoHashesWorkerExit::Shutdown {
                queued_dropped: 1,
                tasks_cancelled: 0,
                triage_hashes_dropped: 0,
                recursive_nodes_dropped: 0,
            }
        );
        assert_eq!(stats.snapshot().shutdown_queued_dropped, 1);
    }

    #[tokio::test]
    async fn shutdown_close_rejects_blocked_send_and_drains_committed_prefix() {
        let table = KTable::new(Id20::ZERO);
        let first = retained(&table, 20, 1020);
        let second = retained(&table, 21, 1021);
        let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        input.send(first).await.unwrap();
        let send = input.send(second.clone());
        tokio::pin!(send);
        poll_once_pending(send.as_mut()).await;
        let (triage, _triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, _discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let (mut core, stats) = core(receiver, table, triage, discovery, 1);

        let exit = core
            .run_with(
                ready(()),
                || id(99),
                |_, _| ready(Err::<SampleInfoHashesResult, ()>(())),
                |_| false,
                Instant::now,
                || tokio::time::Instant::now() + Duration::from_secs(60),
                |_, _| {},
                |_, _| {},
            )
            .await;
        assert_eq!(
            exit,
            DhtSampleInfoHashesWorkerExit::Shutdown {
                queued_dropped: 1,
                tasks_cancelled: 0,
                triage_hashes_dropped: 0,
                recursive_nodes_dropped: 0,
            }
        );
        assert_eq!(send.await.unwrap_err().into_node(), second);
        assert_eq!(stats.snapshot().shutdown_queued_dropped, 1);
    }

    #[tokio::test]
    async fn child_panic_aborts_sibling_and_resumes_payload() {
        let table = KTable::new(Id20::ZERO);
        let first = retained(&table, 15, 1015);
        let second = retained(&table, 16, 1016);
        let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(2);
        input.send(first).await.unwrap();
        input.send(second).await.unwrap();
        drop(input);
        let (triage, _triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
        let (discovery, _discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
        let (mut core, stats) = core(receiver, table, triage, discovery, 2);
        let panic_gate = Arc::new(Notify::new());
        let sibling_drops = Arc::new(AtomicUsize::new(0));
        let (sibling_started_tx, sibling_started_rx) = oneshot::channel();
        let sibling_started_tx = Arc::new(Mutex::new(Some(sibling_started_tx)));
        let query_panic_gate = Arc::clone(&panic_gate);
        let query_sibling_drops = Arc::clone(&sibling_drops);

        let outer = tokio::spawn(async move {
            core.run_with(
                pending(),
                || id(99),
                move |remote, _| {
                    let panic_gate = Arc::clone(&query_panic_gate);
                    let sibling_drops = Arc::clone(&query_sibling_drops);
                    let sibling_started_tx = Arc::clone(&sibling_started_tx);
                    async move {
                        if remote.port() == 1015 {
                            panic_gate.notified().await;
                            panic!("sample worker child sentinel");
                        }
                        if let Some(sender) = sibling_started_tx.lock().unwrap().take() {
                            let _ = sender.send(());
                        }
                        PendingQueryDropProbe {
                            drops: sibling_drops,
                        }
                        .await
                    }
                },
                |_| false,
                Instant::now,
                || tokio::time::Instant::now() + Duration::from_secs(60),
                |_, _| {},
                |_, _| {},
            )
            .await
        });
        sibling_started_rx.await.unwrap();
        assert_eq!(stats.snapshot().queries_started, 2);
        panic_gate.notify_waiters();
        let error = outer.await.unwrap_err();
        assert!(error.is_panic());
        let payload = error.into_panic();
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"sample worker child sentinel")
        );
        assert_eq!(sibling_drops.load(Ordering::SeqCst), 1);
    }

    #[allow(dead_code)]
    fn _receiver_types_are_nominal(
        _triage: DhtInfoHashTriageReceiver,
        _discovery: DhtDiscoveryReceiver,
    ) {
    }
}
