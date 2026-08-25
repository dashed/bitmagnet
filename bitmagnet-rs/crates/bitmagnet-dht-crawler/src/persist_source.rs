//! Owned batching and persistence policy for BEP-33 scrape source observations.

use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bitmagnet_dht::Id20;

use crate::persist_source_route::{DhtPersistSourceReceiver, DhtPersistSourceRequest};

/// Go production's maximum number of raw scrape observations in one batch.
pub const DHT_PERSIST_SOURCE_BATCH_LIMIT: usize = 1_000;
/// Rust's owned first-item-relative flush interval, equal to Go's configured
/// persist-source batching interval.
pub const DHT_PERSIST_SOURCE_BATCH_INTERVAL: Duration = Duration::from_secs(60);

/// Error type returned by an injected source-batch writer.
pub type PersistSourceCollaboratorError = Box<dyn Error + Send + Sync + 'static>;

/// Typed completion error for one atomic source-batch write.
///
/// Both variants mean the worker must continue without retrying the batch. A
/// rejection proves that none of the batch's eligible effects committed. An
/// unknown outcome preserves the no-proper-subset guarantee but means the
/// backend may have committed either none or all eligible effects, such as when
/// a remote `COMMIT` succeeds before its acknowledgement is lost. Inputs
/// skipped by a writer-owned predicate are logical no-ops, not rejections.
#[derive(Debug)]
pub enum DhtSourceBatchWriteError {
    /// No eligible effect from the batch committed.
    Rejected {
        /// Underlying collaborator error.
        source: PersistSourceCollaboratorError,
    },
    /// The collaborator cannot determine whether none or all eligible effects
    /// committed.
    OutcomeUnknown {
        /// Underlying collaborator error.
        source: PersistSourceCollaboratorError,
    },
}

impl DhtSourceBatchWriteError {
    /// Classify an error that proves no writer-eligible effect committed.
    pub fn rejected(source: impl Error + Send + Sync + 'static) -> Self {
        Self::Rejected {
            source: Box::new(source),
        }
    }

    /// Classify an error whose whole-batch acceptance outcome is unknowable.
    pub fn outcome_unknown(source: impl Error + Send + Sync + 'static) -> Self {
        Self::OutcomeUnknown {
            source: Box::new(source),
        }
    }
}

impl fmt::Display for DhtSourceBatchWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected { source } => write!(formatter, "DHT source batch rejected: {source}"),
            Self::OutcomeUnknown { source } => {
                write!(formatter, "DHT source batch outcome unknown: {source}")
            }
        }
    }
}

impl Error for DhtSourceBatchWriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        let source = match self {
            Self::Rejected { source } | Self::OutcomeUnknown { source } => source,
        };
        Some(source.as_ref())
    }
}

/// One DHT source observation projected for persistence.
///
/// The source-node address and raw bloom filters have completed their policy
/// purpose before this boundary. The concrete writer owns the invariant
/// `source = "dht"`, initial `seen_count = 1`, timestamps, parent-existence
/// predicate, and conflict update.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtSourceWrite {
    /// The observed torrent's 20-byte info hash.
    pub info_hash: Id20,
    /// Rounded BEP-33 seeder-filter cardinality.
    pub seeders: u32,
    /// Rounded BEP-33 peer-filter cardinality, persisted as leechers.
    pub leechers: u32,
}

/// Persistence boundary for one ordered, info-hash-unique DHT source batch.
///
/// A call is an atomic no-proper-subset unit over writer-eligible effects:
/// `Ok(())` confirms that the whole-batch decision or transaction completed,
/// while a typed error distinguishes definite rejection from an unknown
/// none-or-whole outcome. Writer-owned predicates may skip supplied records as
/// logical no-ops, so success never promises one affected row per input.
/// Concrete writers may chunk their SQL internally, but must prevent a chunk
/// failure from committing a proper subset of eligible effects (for example
/// with one transaction). This is an intentional Rust hardening delta: Go
/// writes nontransactional 100-row chunks, so a later chunk error can leave an
/// earlier prefix committed.
///
/// A remote `COMMIT` can succeed before its acknowledgement is lost, so an
/// error may mean either none or the whole batch committed. Likewise, dropping
/// the future stops awaiting the call but does not prove that a remote database
/// stopped or rolled back its work. Callers must not retry either ambiguous
/// outcome as if rejection were certain.
#[async_trait]
pub trait DhtSourceBatchWriter: Send + Sync {
    /// Persist one nonempty batch in slice order.
    async fn write_batch(&self, sources: &[DhtSourceWrite])
        -> Result<(), DhtSourceBatchWriteError>;
}

/// Owned batching policy for source persistence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtPersistSourceWorkerConfig {
    /// Maximum raw input occurrences collected before an immediate flush.
    pub batch_limit: NonZeroUsize,
    /// Maximum delay measured from the first item in each nonempty batch.
    pub batch_interval: Duration,
}

impl Default for DhtPersistSourceWorkerConfig {
    fn default() -> Self {
        Self {
            batch_limit: NonZeroUsize::new(DHT_PERSIST_SOURCE_BATCH_LIMIT).unwrap(),
            batch_interval: DHT_PERSIST_SOURCE_BATCH_INTERVAL,
        }
    }
}

#[derive(Default)]
struct DhtPersistSourceWorkerStatsInner {
    dequeued: AtomicU64,
    batches: AtomicU64,
    input_duplicates_dropped: AtomicU64,
    writer_calls: AtomicU64,
    writer_successes: AtomicU64,
    writer_rejections: AtomicU64,
    writer_outcomes_unknown: AtomicU64,
    writer_sources_submitted: AtomicU64,
    sources_persisted: AtomicU64,
    writer_rejected_sources: AtomicU64,
    writer_outcome_unknown_sources: AtomicU64,
    shutdown_queued_dropped: AtomicU64,
    shutdown_batch_dropped: AtomicU64,
    shutdown_write_abandoned: AtomicU64,
    shutdown_writer_calls_abandoned: AtomicU64,
}

/// Cloneable sender-free view of source-persistence counters.
#[derive(Clone, Default)]
pub struct DhtPersistSourceWorkerStatsHandle {
    inner: Arc<DhtPersistSourceWorkerStatsInner>,
}

/// One independently read snapshot of saturating worker counters.
///
/// At a terminal snapshot, every dequeued occurrence is classified by exactly
/// one of `input_duplicates_dropped`, `sources_persisted`,
/// `writer_rejected_sources`, `writer_outcome_unknown_sources`,
/// `shutdown_batch_dropped`, or `shutdown_write_abandoned`. Queued shutdown
/// drops were never dequeued and are tracked separately. Persisted counts are
/// records in confirmed-success writer calls, matching Go's logical metric;
/// they do not claim PostgreSQL rows affected after a parent-existence
/// predicate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtPersistSourceWorkerStats {
    /// Raw input occurrences removed from the route.
    pub dequeued: u64,
    /// Nonempty raw batches advanced to deduplication and a writer call.
    pub batches: u64,
    /// Later occurrences of a hash discarded within the same raw batch.
    pub input_duplicates_dropped: u64,
    /// Writer futures constructed for unique projected batches.
    pub writer_calls: u64,
    /// Writer calls that completed successfully.
    pub writer_successes: u64,
    /// Writer calls that reported rejection, proving no writer-eligible effect
    /// committed.
    pub writer_rejections: u64,
    /// Writer calls whose none-or-whole eligible-effect outcome is unknowable.
    pub writer_outcomes_unknown: u64,
    /// Unique projected records supplied across all writer calls.
    pub writer_sources_submitted: u64,
    /// Records in writer calls whose whole-batch success was confirmed.
    pub sources_persisted: u64,
    /// Unique submitted records in calls classified as rejected.
    ///
    /// These records leave the worker pipeline without retry. The rejection
    /// proves no writer-eligible effect committed, but the count can also
    /// include records that a writer predicate would have treated as logical
    /// no-ops.
    pub writer_rejected_sources: u64,
    /// Unique submitted records in calls whose eligible-effect outcome is
    /// unknowable.
    pub writer_outcome_unknown_sources: u64,
    /// Still-queued raw occurrences drained after shutdown won.
    pub shutdown_queued_dropped: u64,
    /// Dequeued raw occurrences dropped before deduplication and submission.
    pub shutdown_batch_dropped: u64,
    /// Unique submitted records whose writer future was cancelled by shutdown.
    pub shutdown_write_abandoned: u64,
    /// Writer calls cancelled by shutdown with an unknowable backend outcome.
    pub shutdown_writer_calls_abandoned: u64,
}

impl DhtPersistSourceWorkerStatsHandle {
    /// Read every counter independently with relaxed ordering.
    #[must_use]
    pub fn snapshot(&self) -> DhtPersistSourceWorkerStats {
        let inner = &self.inner;
        DhtPersistSourceWorkerStats {
            dequeued: inner.dequeued.load(Ordering::Relaxed),
            batches: inner.batches.load(Ordering::Relaxed),
            input_duplicates_dropped: inner.input_duplicates_dropped.load(Ordering::Relaxed),
            writer_calls: inner.writer_calls.load(Ordering::Relaxed),
            writer_successes: inner.writer_successes.load(Ordering::Relaxed),
            writer_rejections: inner.writer_rejections.load(Ordering::Relaxed),
            writer_outcomes_unknown: inner.writer_outcomes_unknown.load(Ordering::Relaxed),
            writer_sources_submitted: inner.writer_sources_submitted.load(Ordering::Relaxed),
            sources_persisted: inner.sources_persisted.load(Ordering::Relaxed),
            writer_rejected_sources: inner.writer_rejected_sources.load(Ordering::Relaxed),
            writer_outcome_unknown_sources: inner
                .writer_outcome_unknown_sources
                .load(Ordering::Relaxed),
            shutdown_queued_dropped: inner.shutdown_queued_dropped.load(Ordering::Relaxed),
            shutdown_batch_dropped: inner.shutdown_batch_dropped.load(Ordering::Relaxed),
            shutdown_write_abandoned: inner.shutdown_write_abandoned.load(Ordering::Relaxed),
            shutdown_writer_calls_abandoned: inner
                .shutdown_writer_calls_abandoned
                .load(Ordering::Relaxed),
        }
    }
}

/// Terminal state of the owned source-persistence worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtPersistSourceWorkerExit {
    /// Every input clone is gone and the final partial batch reached a writer
    /// outcome.
    ///
    /// This does not imply every write succeeded: rejected and unknown-outcome
    /// batches are counted separately and are not retried.
    InputClosed,
    /// Caller shutdown stopped intake, collection, or an in-flight writer call.
    Shutdown {
        /// Raw occurrences drained from the route without being dequeued.
        queued_dropped: usize,
        /// Dequeued raw occurrences dropped before writer submission.
        batch_dropped: usize,
        /// Unique submitted records left with an unknowable backend outcome.
        write_abandoned: usize,
    },
}

/// Owned sequential source-persistence worker.
///
/// Raw input batches use the first occurrence of each info hash and retain that
/// order. Definite rejection and unknown acceptance classify only their current
/// atomic-contract batch, are never retried, and the worker continues. EOF
/// flushes a final partial batch but does not convert either error class into
/// success. Shutdown is biased ahead of intake, the batch deadline, and writer
/// completion; it closes and drains the input, drops a buffered raw batch, and
/// cancels a pending writer. Cancelled writer records are reported as abandoned
/// because Rust cannot infer whether a remote database committed work before
/// cancellation was observed.
///
/// Unlike Go's generic batching channel, this worker starts no detached task,
/// has no separate output buffer, and measures the interval from each batch's
/// first item. This lowers total retention and makes EOF explicit.
#[must_use = "the worker must be run to consume source-persistence requests"]
pub struct DhtPersistSourceWorker {
    input: DhtPersistSourceReceiver,
    writer: Arc<dyn DhtSourceBatchWriter>,
    config: DhtPersistSourceWorkerConfig,
    stats: DhtPersistSourceWorkerStatsHandle,
}

impl DhtPersistSourceWorker {
    /// Construct a worker with production batching policy.
    pub fn new(
        input: DhtPersistSourceReceiver,
        writer: Arc<dyn DhtSourceBatchWriter>,
    ) -> (Self, DhtPersistSourceWorkerStatsHandle) {
        Self::with_config(input, writer, DhtPersistSourceWorkerConfig::default())
    }

    /// Construct a worker with explicit batching policy.
    pub fn with_config(
        input: DhtPersistSourceReceiver,
        writer: Arc<dyn DhtSourceBatchWriter>,
        config: DhtPersistSourceWorkerConfig,
    ) -> (Self, DhtPersistSourceWorkerStatsHandle) {
        let stats = DhtPersistSourceWorkerStatsHandle::default();
        (
            Self {
                input,
                writer,
                config,
                stats: stats.clone(),
            },
            stats,
        )
    }

    /// Run until input EOF or caller shutdown.
    pub async fn run<F>(mut self, shutdown: F) -> DhtPersistSourceWorkerExit
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);

        loop {
            let first = tokio::select! {
                biased;
                () = &mut shutdown => return self.finish_shutdown(0, 0),
                request = self.input.recv() => request,
            };
            let Some(first) = first else {
                return DhtPersistSourceWorkerExit::InputClosed;
            };
            increment_saturating(&self.stats.inner.dequeued);

            let mut batch = Vec::with_capacity(self.config.batch_limit.get().min(64));
            batch.push(first);
            let mut input_closed = false;
            if batch.len() < self.config.batch_limit.get() {
                let delay = tokio::time::sleep(self.config.batch_interval);
                tokio::pin!(delay);
                loop {
                    if batch.len() >= self.config.batch_limit.get() || input_closed {
                        break;
                    }
                    let next = tokio::select! {
                        biased;
                        () = &mut shutdown => return self.finish_shutdown(batch.len(), 0),
                        () = &mut delay => break,
                        request = self.input.recv() => request,
                    };
                    match next {
                        Some(request) => {
                            increment_saturating(&self.stats.inner.dequeued);
                            batch.push(request);
                        }
                        None => {
                            input_closed = true;
                            break;
                        }
                    }
                }
            }

            match self.process_batch(&batch, shutdown.as_mut()).await {
                BatchResult::Complete => {}
                BatchResult::Shutdown { write_abandoned } => {
                    return self.finish_shutdown(0, write_abandoned);
                }
            }

            if input_closed {
                return tokio::select! {
                    biased;
                    () = &mut shutdown => self.finish_shutdown(0, 0),
                    () = std::future::ready(()) => DhtPersistSourceWorkerExit::InputClosed,
                };
            }
        }
    }

    async fn process_batch<F>(
        &self,
        batch: &[DhtPersistSourceRequest],
        mut shutdown: std::pin::Pin<&mut F>,
    ) -> BatchResult
    where
        F: Future<Output = ()>,
    {
        increment_saturating(&self.stats.inner.batches);

        let mut seen = HashSet::with_capacity(batch.len());
        let mut writes = Vec::with_capacity(batch.len());
        for request in batch {
            if !seen.insert(request.info_hash) {
                increment_saturating(&self.stats.inner.input_duplicates_dropped);
                continue;
            }
            writes.push(DhtSourceWrite {
                info_hash: request.info_hash,
                seeders: request.seeders_bloom.approximated_size(),
                leechers: request.peers_bloom.approximated_size(),
            });
        }

        increment_saturating(&self.stats.inner.writer_calls);
        add_saturating(
            &self.stats.inner.writer_sources_submitted,
            count_u64(writes.len()),
        );
        let write = self.writer.write_batch(&writes);
        tokio::pin!(write);
        let result = tokio::select! {
            biased;
            () = shutdown.as_mut() => {
                increment_saturating(&self.stats.inner.shutdown_writer_calls_abandoned);
                return BatchResult::Shutdown {
                    write_abandoned: writes.len(),
                };
            }
            result = &mut write => result,
        };

        match result {
            Ok(()) => {
                increment_saturating(&self.stats.inner.writer_successes);
                add_saturating(&self.stats.inner.sources_persisted, count_u64(writes.len()));
            }
            Err(DhtSourceBatchWriteError::Rejected { source }) => {
                increment_saturating(&self.stats.inner.writer_rejections);
                add_saturating(
                    &self.stats.inner.writer_rejected_sources,
                    count_u64(writes.len()),
                );
                tracing::warn!(
                    %source,
                    "DHT source batch writer rejected batch; not retrying"
                );
            }
            Err(DhtSourceBatchWriteError::OutcomeUnknown { source }) => {
                increment_saturating(&self.stats.inner.writer_outcomes_unknown);
                add_saturating(
                    &self.stats.inner.writer_outcome_unknown_sources,
                    count_u64(writes.len()),
                );
                tracing::warn!(
                    %source,
                    "DHT source batch writer outcome unknown; not retrying"
                );
            }
        }
        BatchResult::Complete
    }

    fn finish_shutdown(
        &mut self,
        batch_dropped: usize,
        write_abandoned: usize,
    ) -> DhtPersistSourceWorkerExit {
        let queued_dropped = self.close_and_drain_input();
        add_saturating(
            &self.stats.inner.shutdown_queued_dropped,
            count_u64(queued_dropped),
        );
        add_saturating(
            &self.stats.inner.shutdown_batch_dropped,
            count_u64(batch_dropped),
        );
        add_saturating(
            &self.stats.inner.shutdown_write_abandoned,
            count_u64(write_abandoned),
        );
        DhtPersistSourceWorkerExit::Shutdown {
            queued_dropped,
            batch_dropped,
            write_abandoned,
        }
    }

    fn close_and_drain_input(&mut self) -> usize {
        self.input.close();
        let mut drained = 0_usize;
        while self.input.try_recv().is_ok() {
            drained = drained.saturating_add(1);
        }
        drained
    }
}

enum BatchResult {
    Complete,
    Shutdown { write_abandoned: usize },
}

fn increment_saturating(counter: &AtomicU64) {
    add_saturating(counter, 1);
}

fn add_saturating(counter: &AtomicU64, value: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(value);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn count_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::{pending, ready, Future};
    use std::io;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    use bitmagnet_dht::ScrapeBloomFilter;
    use tokio::sync::Notify;

    use crate::{dht_persist_source_channel, DhtPersistSourceInput};

    use super::*;

    enum Step {
        Ok,
        Rejected(&'static str),
        OutcomeUnknown(&'static str),
    }

    struct ScriptWriter {
        steps: Mutex<VecDeque<Step>>,
        calls: Mutex<Vec<Vec<DhtSourceWrite>>>,
    }

    impl ScriptWriter {
        fn new(steps: impl IntoIterator<Item = Step>) -> Arc<Self> {
            Arc::new(Self {
                steps: Mutex::new(steps.into_iter().collect()),
                calls: Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> Vec<Vec<DhtSourceWrite>> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DhtSourceBatchWriter for ScriptWriter {
        async fn write_batch(
            &self,
            sources: &[DhtSourceWrite],
        ) -> Result<(), DhtSourceBatchWriteError> {
            self.calls.lock().unwrap().push(sources.to_vec());
            match self
                .steps
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted writer step")
            {
                Step::Ok => Ok(()),
                Step::Rejected(message) => Err(DhtSourceBatchWriteError::rejected(
                    io::Error::other(message),
                )),
                Step::OutcomeUnknown(message) => Err(DhtSourceBatchWriteError::outcome_unknown(
                    io::Error::other(message),
                )),
            }
        }
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    struct BlockingWriter {
        entered: Notify,
        dropped: Arc<AtomicBool>,
        calls: Mutex<Vec<Vec<DhtSourceWrite>>>,
    }

    #[async_trait]
    impl DhtSourceBatchWriter for BlockingWriter {
        async fn write_batch(
            &self,
            sources: &[DhtSourceWrite],
        ) -> Result<(), DhtSourceBatchWriteError> {
            self.calls.lock().unwrap().push(sources.to_vec());
            let _drop_flag = DropFlag(Arc::clone(&self.dropped));
            self.entered.notify_one();
            pending().await
        }
    }

    struct SignalingWriter {
        signal: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl DhtSourceBatchWriter for SignalingWriter {
        async fn write_batch(
            &self,
            _sources: &[DhtSourceWrite],
        ) -> Result<(), DhtSourceBatchWriteError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if let Some(signal) = self.signal.lock().unwrap().take() {
                let _ = signal.send(());
            }
            Ok(())
        }
    }

    struct PendingThenReady {
        polls: Arc<AtomicUsize>,
    }

    impl Future for PendingThenReady {
        type Output = ();

        fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
            if self.polls.fetch_add(1, Ordering::Relaxed) == 0 {
                context.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }

    fn id(value: usize) -> Id20 {
        let mut bytes = [0_u8; 20];
        bytes[12..].copy_from_slice(&(value as u64).to_be_bytes());
        Id20::from_slice(&bytes).unwrap()
    }

    fn ipv4(value: u8) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(192, 0, 2, value),
            7_000 + u16::from(value),
        ))
    }

    fn scoped_ipv6(value: u16, scope_id: u32) -> SocketAddr {
        SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, value),
            8_000 + value,
            0,
            scope_id,
        ))
    }

    fn bloom(value: u8) -> ScrapeBloomFilter {
        ScrapeBloomFilter::from([value; 256])
    }

    fn request(value: usize) -> DhtPersistSourceRequest {
        DhtPersistSourceRequest {
            info_hash: id(value),
            source_node_addr: ipv4((value % 250 + 1) as u8),
            seeders_bloom: bloom((value as u8).wrapping_mul(3)),
            peers_bloom: bloom((value as u8).wrapping_mul(5)),
        }
    }

    fn config(batch_limit: usize) -> DhtPersistSourceWorkerConfig {
        DhtPersistSourceWorkerConfig {
            batch_limit: NonZeroUsize::new(batch_limit).unwrap(),
            batch_interval: Duration::from_secs(60 * 60),
        }
    }

    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}

    fn assert_future_send<F: Future + Send>(future: F) {
        drop(future);
    }

    fn assert_conserved(stats: DhtPersistSourceWorkerStats) {
        assert_eq!(
            stats.dequeued,
            stats.input_duplicates_dropped
                + stats.sources_persisted
                + stats.writer_rejected_sources
                + stats.writer_outcome_unknown_sources
                + stats.shutdown_batch_dropped
                + stats.shutdown_write_abandoned,
            "every dequeued source occurrence must have one terminal classification: {stats:?}"
        );
        assert_eq!(
            stats.writer_sources_submitted,
            stats.sources_persisted
                + stats.writer_rejected_sources
                + stats.writer_outcome_unknown_sources
                + stats.shutdown_write_abandoned,
            "every submitted source must have one observed writer outcome: {stats:?}"
        );
        assert_eq!(
            stats.writer_calls,
            stats.writer_successes
                + stats.writer_rejections
                + stats.writer_outcomes_unknown
                + stats.shutdown_writer_calls_abandoned,
            "every writer call must have one observed completion class: {stats:?}"
        );
        assert_eq!(stats.batches, stats.writer_calls);
    }

    async fn yield_until(mut predicate: impl FnMut() -> bool) {
        for _ in 0..100 {
            if predicate() {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            predicate(),
            "condition did not become ready after 100 yields"
        );
    }

    async fn send_all(
        input: &DhtPersistSourceInput,
        requests: impl IntoIterator<Item = DhtPersistSourceRequest>,
    ) {
        for request in requests {
            input.send(request).await.unwrap();
        }
    }

    #[test]
    fn defaults_public_traits_and_saturating_counters_are_exact() {
        let defaults = DhtPersistSourceWorkerConfig::default();
        assert_eq!(DHT_PERSIST_SOURCE_BATCH_LIMIT, 1_000);
        assert_eq!(DHT_PERSIST_SOURCE_BATCH_INTERVAL, Duration::from_secs(60));
        assert_eq!(defaults.batch_limit.get(), 1_000);
        assert_eq!(defaults.batch_interval, Duration::from_secs(60));
        assert_send_sync::<DhtSourceWrite>();
        assert_send_sync::<DhtSourceBatchWriteError>();
        assert_send_sync::<DhtPersistSourceWorkerStatsHandle>();
        assert_send_sync::<DhtPersistSourceWorkerStats>();
        assert_send::<DhtPersistSourceWorker>();

        let rejected = DhtSourceBatchWriteError::rejected(io::Error::other("rejected"));
        assert!(matches!(
            &rejected,
            DhtSourceBatchWriteError::Rejected { .. }
        ));
        assert_eq!(rejected.to_string(), "DHT source batch rejected: rejected");
        assert_eq!(rejected.source().unwrap().to_string(), "rejected");
        let unknown =
            DhtSourceBatchWriteError::outcome_unknown(io::Error::other("acknowledgement lost"));
        assert!(matches!(
            &unknown,
            DhtSourceBatchWriteError::OutcomeUnknown { .. }
        ));
        assert_eq!(
            unknown.to_string(),
            "DHT source batch outcome unknown: acknowledgement lost"
        );
        assert_eq!(
            unknown.source().unwrap().to_string(),
            "acknowledgement lost"
        );

        let counter = AtomicU64::new(u64::MAX - 1);
        add_saturating(&counter, 10);
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
        increment_saturating(&counter);
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);

        let (input, receiver) = dht_persist_source_channel();
        let writer = ScriptWriter::new([]);
        let (worker, stats) = DhtPersistSourceWorker::new(receiver, writer);
        assert_future_send(worker.run(pending()));
        assert_eq!(stats.snapshot(), DhtPersistSourceWorkerStats::default());
        drop(input);
    }

    #[tokio::test]
    async fn empty_and_directional_projection_discard_source_addresses_and_raw_filters() {
        let seeders = bloom(0x01);
        let peers = bloom(0x07);
        assert_ne!(seeders.approximated_size(), peers.approximated_size());
        let requests = [
            DhtPersistSourceRequest {
                info_hash: id(1),
                source_node_addr: ipv4(1),
                seeders_bloom: ScrapeBloomFilter::EMPTY,
                peers_bloom: ScrapeBloomFilter::EMPTY,
            },
            DhtPersistSourceRequest {
                info_hash: id(2),
                source_node_addr: scoped_ipv6(2, 42),
                seeders_bloom: seeders,
                peers_bloom: peers,
            },
        ];
        let writer = ScriptWriter::new([Step::Ok]);
        let (input, receiver) = dht_persist_source_channel();
        let (worker, stats) =
            DhtPersistSourceWorker::with_config(receiver, writer.clone(), config(2));
        send_all(&input, requests).await;
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtPersistSourceWorkerExit::InputClosed
        );
        assert_eq!(
            writer.calls(),
            vec![vec![
                DhtSourceWrite {
                    info_hash: id(1),
                    seeders: 0,
                    leechers: 0,
                },
                DhtSourceWrite {
                    info_hash: id(2),
                    seeders: seeders.approximated_size(),
                    leechers: peers.approximated_size(),
                },
            ]]
        );
        let snapshot = stats.snapshot();
        assert_eq!(
            snapshot,
            DhtPersistSourceWorkerStats {
                dequeued: 2,
                batches: 1,
                writer_calls: 1,
                writer_successes: 1,
                writer_sources_submitted: 2,
                sources_persisted: 2,
                ..DhtPersistSourceWorkerStats::default()
            }
        );
        assert_conserved(snapshot);
    }

    #[tokio::test]
    async fn first_occurrence_wins_within_a_batch() {
        let first = DhtPersistSourceRequest {
            info_hash: id(7),
            source_node_addr: ipv4(7),
            seeders_bloom: bloom(0x01),
            peers_bloom: bloom(0x02),
        };
        let duplicate = DhtPersistSourceRequest {
            info_hash: id(7),
            source_node_addr: scoped_ipv6(7, 77),
            seeders_bloom: bloom(0x7f),
            peers_bloom: bloom(0xff),
        };
        let writer = ScriptWriter::new([Step::Ok]);
        let (input, receiver) = dht_persist_source_channel();
        let (worker, stats) =
            DhtPersistSourceWorker::with_config(receiver, writer.clone(), config(3));
        send_all(&input, [first.clone(), duplicate, request(8)]).await;
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtPersistSourceWorkerExit::InputClosed
        );
        assert_eq!(
            writer.calls()[0][0],
            DhtSourceWrite {
                info_hash: first.info_hash,
                seeders: first.seeders_bloom.approximated_size(),
                leechers: first.peers_bloom.approximated_size(),
            }
        );
        assert_eq!(writer.calls()[0].len(), 2);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.dequeued, 3);
        assert_eq!(snapshot.input_duplicates_dropped, 1);
        assert_eq!(snapshot.sources_persisted, 2);
        assert_conserved(snapshot);
    }

    #[tokio::test]
    async fn deduplication_resets_across_batch_boundaries() {
        let writer = ScriptWriter::new([Step::Ok, Step::Ok]);
        let (input, receiver) = dht_persist_source_channel();
        let (worker, stats) =
            DhtPersistSourceWorker::with_config(receiver, writer.clone(), config(1));
        let first = request(9);
        let mut second = request(9);
        second.seeders_bloom = bloom(0x07);
        send_all(&input, [first.clone(), second.clone()]).await;
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtPersistSourceWorkerExit::InputClosed
        );
        assert_eq!(writer.calls().len(), 2);
        assert_eq!(writer.calls()[0][0].info_hash, first.info_hash);
        assert_eq!(writer.calls()[1][0].info_hash, second.info_hash);
        assert_ne!(writer.calls()[0][0].seeders, writer.calls()[1][0].seeders);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.input_duplicates_dropped, 0);
        assert_eq!(snapshot.sources_persisted, 2);
        assert_conserved(snapshot);
    }

    #[tokio::test]
    async fn raw_batch_limit_splits_one_thousand_and_carries_the_next_item() {
        let writer = ScriptWriter::new([Step::Ok, Step::Ok]);
        let (input, receiver) = dht_persist_source_channel();
        let (worker, stats) = DhtPersistSourceWorker::new(receiver, writer.clone());
        let task = tokio::spawn(worker.run(pending()));
        send_all(&input, (0..1_001).map(request)).await;
        drop(input);

        assert_eq!(task.await.unwrap(), DhtPersistSourceWorkerExit::InputClosed);
        let calls = writer.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].len(), 1_000);
        assert_eq!(
            calls[1],
            vec![DhtSourceWrite {
                info_hash: id(1_000),
                seeders: request(1_000).seeders_bloom.approximated_size(),
                leechers: request(1_000).peers_bloom.approximated_size(),
            }]
        );
        assert_eq!(calls[0][0].info_hash, id(0));
        assert_eq!(calls[0][999].info_hash, id(999));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.dequeued, 1_001);
        assert_eq!(snapshot.batches, 2);
        assert_eq!(snapshot.sources_persisted, 1_001);
        assert_conserved(snapshot);
    }

    #[tokio::test(start_paused = true)]
    async fn batch_deadline_is_first_item_relative_and_resets_after_each_flush() {
        let writer = ScriptWriter::new([Step::Ok, Step::Ok]);
        let (input, receiver) = dht_persist_source_channel();
        let mut timer_config = config(4);
        timer_config.batch_interval = Duration::from_secs(60);
        let (worker, stats) =
            DhtPersistSourceWorker::with_config(receiver, writer.clone(), timer_config);
        let task = tokio::spawn(worker.run(pending()));

        input.send(request(1)).await.unwrap();
        yield_until(|| stats.snapshot().dequeued == 1).await;
        tokio::time::advance(Duration::from_secs(59)).await;
        tokio::task::yield_now().await;
        assert_eq!(writer.calls().len(), 0);
        tokio::time::advance(Duration::from_secs(1)).await;
        yield_until(|| writer.calls().len() == 1).await;

        input.send(request(2)).await.unwrap();
        yield_until(|| stats.snapshot().dequeued == 2).await;
        tokio::time::advance(Duration::from_secs(59)).await;
        tokio::task::yield_now().await;
        assert_eq!(writer.calls().len(), 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        yield_until(|| writer.calls().len() == 2).await;
        drop(input);

        assert_eq!(task.await.unwrap(), DhtPersistSourceWorkerExit::InputClosed);
        assert_eq!(writer.calls()[0][0].info_hash, id(1));
        assert_eq!(writer.calls()[1][0].info_hash, id(2));
        assert_conserved(stats.snapshot());
    }

    #[tokio::test(start_paused = true)]
    async fn eof_is_immediate_when_empty_and_flushes_a_partial_batch() {
        let empty_writer = ScriptWriter::new([]);
        let (empty_input, empty_receiver) = dht_persist_source_channel();
        let (empty_worker, empty_stats) =
            DhtPersistSourceWorker::with_config(empty_receiver, empty_writer.clone(), config(3));
        drop(empty_input);
        assert_eq!(
            empty_worker.run(pending()).await,
            DhtPersistSourceWorkerExit::InputClosed
        );
        assert!(empty_writer.calls().is_empty());
        assert_eq!(
            empty_stats.snapshot(),
            DhtPersistSourceWorkerStats::default()
        );

        let writer = ScriptWriter::new([Step::Ok]);
        let (input, receiver) = dht_persist_source_channel();
        let (worker, stats) =
            DhtPersistSourceWorker::with_config(receiver, writer.clone(), config(3));
        send_all(&input, [request(3), request(4)]).await;
        drop(input);
        assert_eq!(
            worker.run(pending()).await,
            DhtPersistSourceWorkerExit::InputClosed
        );
        assert_eq!(writer.calls().len(), 1);
        assert_eq!(writer.calls()[0].len(), 2);
        assert_conserved(stats.snapshot());
    }

    #[tokio::test]
    async fn writer_rejection_classifies_only_that_atomic_batch_and_continues() {
        let writer = ScriptWriter::new([Step::Rejected("first"), Step::Ok]);
        let (input, receiver) = dht_persist_source_channel();
        let (worker, stats) =
            DhtPersistSourceWorker::with_config(receiver, writer.clone(), config(1));
        send_all(&input, [request(10), request(11)]).await;
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtPersistSourceWorkerExit::InputClosed
        );
        assert_eq!(writer.calls()[0][0].info_hash, id(10));
        assert_eq!(writer.calls()[1][0].info_hash, id(11));
        let snapshot = stats.snapshot();
        assert_eq!(
            snapshot,
            DhtPersistSourceWorkerStats {
                dequeued: 2,
                batches: 2,
                writer_calls: 2,
                writer_successes: 1,
                writer_rejections: 1,
                writer_sources_submitted: 2,
                sources_persisted: 1,
                writer_rejected_sources: 1,
                ..DhtPersistSourceWorkerStats::default()
            }
        );
        assert_conserved(snapshot);
    }

    #[tokio::test]
    async fn writer_unknown_outcome_classifies_only_that_atomic_batch_and_continues() {
        let writer = ScriptWriter::new([Step::OutcomeUnknown("commit acknowledgement"), Step::Ok]);
        let (input, receiver) = dht_persist_source_channel();
        let (worker, stats) =
            DhtPersistSourceWorker::with_config(receiver, writer.clone(), config(1));
        send_all(&input, [request(12), request(13)]).await;
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtPersistSourceWorkerExit::InputClosed
        );
        assert_eq!(writer.calls()[0][0].info_hash, id(12));
        assert_eq!(writer.calls()[1][0].info_hash, id(13));
        let snapshot = stats.snapshot();
        assert_eq!(
            snapshot,
            DhtPersistSourceWorkerStats {
                dequeued: 2,
                batches: 2,
                writer_calls: 2,
                writer_successes: 1,
                writer_outcomes_unknown: 1,
                writer_sources_submitted: 2,
                sources_persisted: 1,
                writer_outcome_unknown_sources: 1,
                ..DhtPersistSourceWorkerStats::default()
            }
        );
        assert_conserved(snapshot);
    }

    #[tokio::test]
    async fn ready_shutdown_before_intake_drains_queue_and_rejects_later_sends() {
        let writer = ScriptWriter::new([]);
        let (input, receiver) = dht_persist_source_channel();
        send_all(&input, [request(20), request(21)]).await;
        let (worker, stats) =
            DhtPersistSourceWorker::with_config(receiver, writer.clone(), config(3));

        assert_eq!(
            worker.run(ready(())).await,
            DhtPersistSourceWorkerExit::Shutdown {
                queued_dropped: 2,
                batch_dropped: 0,
                write_abandoned: 0,
            }
        );
        assert!(writer.calls().is_empty());
        assert_eq!(
            stats.snapshot(),
            DhtPersistSourceWorkerStats {
                shutdown_queued_dropped: 2,
                ..DhtPersistSourceWorkerStats::default()
            }
        );
        assert!(input.send(request(22)).await.is_err());
        assert_conserved(stats.snapshot());
    }

    #[tokio::test]
    async fn shutdown_is_polled_before_each_additional_prequeued_receive() {
        let writer = ScriptWriter::new([]);
        let (input, receiver) = dht_persist_source_channel();
        send_all(&input, [request(25), request(26), request(27)]).await;
        let (worker, stats) =
            DhtPersistSourceWorker::with_config(receiver, writer.clone(), config(3));
        let polls = Arc::new(AtomicUsize::new(0));

        assert_eq!(
            worker
                .run(PendingThenReady {
                    polls: Arc::clone(&polls),
                })
                .await,
            DhtPersistSourceWorkerExit::Shutdown {
                queued_dropped: 2,
                batch_dropped: 1,
                write_abandoned: 0,
            }
        );
        assert_eq!(polls.load(Ordering::Relaxed), 2);
        assert!(writer.calls().is_empty());
        assert_eq!(
            stats.snapshot(),
            DhtPersistSourceWorkerStats {
                dequeued: 1,
                shutdown_queued_dropped: 2,
                shutdown_batch_dropped: 1,
                ..DhtPersistSourceWorkerStats::default()
            }
        );
        assert!(input.send(request(28)).await.is_err());
        assert_conserved(stats.snapshot());
    }

    #[tokio::test(start_paused = true)]
    async fn zero_deadline_wins_before_each_additional_prequeued_receive() {
        let writer = ScriptWriter::new([Step::Ok, Step::Ok, Step::Ok]);
        let (input, receiver) = dht_persist_source_channel();
        send_all(&input, [request(35), request(36), request(37)]).await;
        drop(input);
        let (worker, stats) = DhtPersistSourceWorker::with_config(
            receiver,
            writer.clone(),
            DhtPersistSourceWorkerConfig {
                batch_limit: NonZeroUsize::new(1_000).unwrap(),
                batch_interval: Duration::ZERO,
            },
        );

        assert_eq!(
            worker.run(pending()).await,
            DhtPersistSourceWorkerExit::InputClosed
        );
        let calls = writer.calls();
        assert_eq!(calls.len(), 3);
        assert_eq!(
            calls.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![1, 1, 1]
        );
        assert_eq!(calls[0][0].info_hash, id(35));
        assert_eq!(calls[1][0].info_hash, id(36));
        assert_eq!(calls[2][0].info_hash, id(37));
        assert_eq!(
            stats.snapshot(),
            DhtPersistSourceWorkerStats {
                dequeued: 3,
                batches: 3,
                writer_calls: 3,
                writer_successes: 3,
                writer_sources_submitted: 3,
                sources_persisted: 3,
                ..DhtPersistSourceWorkerStats::default()
            }
        );
        assert_conserved(stats.snapshot());
    }

    #[tokio::test]
    async fn shutdown_while_collecting_drops_raw_batch_and_queued_suffix() {
        let writer = ScriptWriter::new([]);
        let (input, receiver) = dht_persist_source_channel();
        let (worker, stats) =
            DhtPersistSourceWorker::with_config(receiver, writer.clone(), config(5));
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(worker.run(async move {
            let _ = shutdown_receiver.await;
        }));
        input.send(request(30)).await.unwrap();
        yield_until(|| stats.snapshot().dequeued == 1).await;
        send_all(&input, [request(31), request(32), request(33)]).await;
        shutdown_sender.send(()).unwrap();

        assert_eq!(
            task.await.unwrap(),
            DhtPersistSourceWorkerExit::Shutdown {
                queued_dropped: 3,
                batch_dropped: 1,
                write_abandoned: 0,
            }
        );
        assert!(writer.calls().is_empty());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.shutdown_queued_dropped, 3);
        assert_eq!(snapshot.shutdown_batch_dropped, 1);
        assert_conserved(snapshot);
    }

    #[tokio::test]
    async fn shutdown_cancels_pending_writer_and_truthfully_abandons_unique_writes() {
        let dropped = Arc::new(AtomicBool::new(false));
        let writer = Arc::new(BlockingWriter {
            entered: Notify::new(),
            dropped: Arc::clone(&dropped),
            calls: Mutex::new(Vec::new()),
        });
        let (input, receiver) = dht_persist_source_channel();
        let (worker, stats) =
            DhtPersistSourceWorker::with_config(receiver, writer.clone(), config(2));
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(worker.run(async move {
            let _ = shutdown_receiver.await;
        }));
        let first = request(40);
        let mut duplicate = request(40);
        duplicate.seeders_bloom = bloom(0xff);
        send_all(&input, [first, duplicate]).await;
        tokio::time::timeout(Duration::from_secs(5), writer.entered.notified())
            .await
            .expect("writer did not start");
        input.send(request(41)).await.unwrap();
        shutdown_sender.send(()).unwrap();

        assert_eq!(
            task.await.unwrap(),
            DhtPersistSourceWorkerExit::Shutdown {
                queued_dropped: 1,
                batch_dropped: 0,
                write_abandoned: 1,
            }
        );
        assert!(dropped.load(Ordering::Relaxed));
        assert_eq!(writer.calls.lock().unwrap().len(), 1);
        let snapshot = stats.snapshot();
        assert_eq!(
            snapshot,
            DhtPersistSourceWorkerStats {
                dequeued: 2,
                batches: 1,
                input_duplicates_dropped: 1,
                writer_calls: 1,
                writer_sources_submitted: 1,
                shutdown_queued_dropped: 1,
                shutdown_write_abandoned: 1,
                shutdown_writer_calls_abandoned: 1,
                ..DhtPersistSourceWorkerStats::default()
            }
        );
        assert_conserved(snapshot);
    }

    #[tokio::test]
    async fn shutdown_signaled_by_successful_final_writer_wins_over_eof() {
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let writer = Arc::new(SignalingWriter {
            signal: Mutex::new(Some(shutdown_sender)),
            calls: AtomicUsize::new(0),
        });
        let (input, receiver) = dht_persist_source_channel();
        let (worker, stats) =
            DhtPersistSourceWorker::with_config(receiver, writer.clone(), config(1));
        input.send(request(50)).await.unwrap();
        drop(input);

        assert_eq!(
            worker
                .run(async move {
                    let _ = shutdown_receiver.await;
                })
                .await,
            DhtPersistSourceWorkerExit::Shutdown {
                queued_dropped: 0,
                batch_dropped: 0,
                write_abandoned: 0,
            }
        );
        assert_eq!(writer.calls.load(Ordering::Relaxed), 1);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.sources_persisted, 1);
        assert_eq!(snapshot.shutdown_write_abandoned, 0);
        assert_conserved(snapshot);
    }

    #[tokio::test]
    async fn dropping_unrun_worker_closes_receiver_without_classifying_input() {
        let writer = ScriptWriter::new([]);
        let (input, receiver) = dht_persist_source_channel();
        let (worker, stats) = DhtPersistSourceWorker::new(receiver, writer);
        drop(worker);
        assert!(input.send(request(60)).await.is_err());
        assert_eq!(stats.snapshot(), DhtPersistSourceWorkerStats::default());
    }
}
