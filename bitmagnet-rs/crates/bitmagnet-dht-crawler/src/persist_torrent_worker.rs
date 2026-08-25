//! Owned batching, lookup, persistence, and scrape-fanout lifecycle for DHT torrents.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bitmagnet_dht::{DhtScrapeInput, Id20};

use crate::{
    DhtPersistTorrentReceiver, DhtPersistTorrentRequest, DhtTorrentPersistPlan,
    DhtTorrentPlanConfig, DhtTorrentPlanDiagnostic, DhtTorrentPlanner, DhtTorrentTransactionPlan,
};

/// Maximum raw requests in one production torrent-persistence batch.
pub const DHT_PERSIST_TORRENT_BATCH_LIMIT: usize = 1_000;
/// First-item-relative production flush interval.
pub const DHT_PERSIST_TORRENT_BATCH_INTERVAL: Duration = Duration::from_secs(60);
/// Maximum sorted unique full-v2 keys in one lookup call.
pub const DHT_PERSIST_TORRENT_LOOKUP_CHUNK_LIMIT: usize = 1_000;

/// Boxed error returned by an injected torrent-persistence collaborator.
pub type PersistTorrentCollaboratorError = Box<dyn Error + Send + Sync + 'static>;

/// One full-v2 identity already represented by a stored primary hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtExistingV2Row {
    /// Full BEP-52 identity returned for a requested lookup key.
    pub info_hash_v2: [u8; 32],
    /// Existing stored primary identity associated with that full-v2 hash.
    pub primary_info_hash: Id20,
}

/// Already-resolved full-v2 lookup boundary.
///
/// Every call receives a nonempty, strictly sorted, duplicate-free slice no
/// longer than [`DhtPersistTorrentWorkerConfig::lookup_chunk_limit`]. Returned
/// rows may be in arbitrary order and may repeat a requested full-v2 key. The
/// worker canonicalizes legal duplicates to the lexicographically smallest
/// [`Id20`]. A row whose full-v2 key is not in the current call invalidates the
/// whole returned chunk.
///
/// Calls are sequential. On an error or a foreign row the worker retains
/// results from earlier chunks, discards the failing chunk, skips later chunks,
/// plans fail-open, and does not retry. Shutdown drops the pending lookup
/// future, discards the whole raw batch before planning, and likewise performs
/// no retry.
#[async_trait]
pub trait DhtTorrentV2Lookup: Send + Sync {
    async fn lookup_existing_v2(
        &self,
        info_hashes_v2: &[[u8; 32]],
    ) -> Result<Vec<DhtExistingV2Row>, PersistTorrentCollaboratorError>;
}

/// Typed outcome of one atomic torrent-transaction plan.
#[derive(Debug)]
pub enum DhtTorrentBatchWriteError {
    /// No writer-eligible effect committed.
    Rejected {
        source: PersistTorrentCollaboratorError,
    },
    /// The collaborator cannot determine whether none or all eligible effects committed.
    OutcomeUnknown {
        source: PersistTorrentCollaboratorError,
    },
}

impl DhtTorrentBatchWriteError {
    /// Classify an error that confirms no eligible effect committed.
    pub fn rejected(source: impl Error + Send + Sync + 'static) -> Self {
        Self::Rejected {
            source: Box::new(source),
        }
    }

    /// Classify an error for which none or the whole eligible set may have
    /// committed.
    pub fn outcome_unknown(source: impl Error + Send + Sync + 'static) -> Self {
        Self::OutcomeUnknown {
            source: Box::new(source),
        }
    }
}

impl fmt::Display for DhtTorrentBatchWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected { source } => write!(formatter, "DHT torrent batch rejected: {source}"),
            Self::OutcomeUnknown { source } => {
                write!(formatter, "DHT torrent batch outcome unknown: {source}")
            }
        }
    }
}

impl Error for DhtTorrentBatchWriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        let source = match self {
            Self::Rejected { source } | Self::OutcomeUnknown { source } => source,
        };
        Some(source.as_ref())
    }
}

/// Atomic persistence boundary for one completed nonempty raw batch's plan.
///
/// Implementations must apply the eligible effects in all six collections --
/// torrents, files, file summaries, sources, pieces, and queue jobs -- through
/// one transaction boundary. No proper subset of eligible effects may commit
/// because a later collection or chunk failed. In particular, queue jobs must
/// not be written through an independent pool or transaction.
///
/// The worker calls this method exactly once for every completed nonempty raw
/// batch, including when the planner produces an empty transaction plan, and
/// never retries. `Ok(())` confirms the whole-plan transaction decision and is
/// the only result that enables ordered scrape fanout; logical predicate skips
/// remain successful no-ops and are not affected-row claims. `Rejected` means
/// no eligible effect committed. `OutcomeUnknown` means none or the whole
/// eligible set may have committed. Dropping the future on shutdown is tracked
/// separately as an unconfirmed abandoned call and suppresses every scrape for
/// that plan.
#[async_trait]
pub trait DhtTorrentBatchWriter: Send + Sync {
    async fn write_batch(
        &self,
        plan: &DhtTorrentTransactionPlan,
    ) -> Result<(), DhtTorrentBatchWriteError>;
}

/// Owned batching, lookup, and planning policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtPersistTorrentWorkerConfig {
    /// Maximum FIFO raw requests collected into one batch.
    pub batch_limit: NonZeroUsize,
    /// Flush deadline measured from the first request in each batch.
    pub batch_interval: Duration,
    /// Maximum sorted unique full-v2 keys supplied to one lookup call.
    pub lookup_chunk_limit: NonZeroUsize,
    /// Immutable deterministic planner policy used for every batch.
    pub plan_config: DhtTorrentPlanConfig,
}

impl Default for DhtPersistTorrentWorkerConfig {
    fn default() -> Self {
        Self {
            batch_limit: NonZeroUsize::new(DHT_PERSIST_TORRENT_BATCH_LIMIT).unwrap(),
            batch_interval: DHT_PERSIST_TORRENT_BATCH_INTERVAL,
            lookup_chunk_limit: NonZeroUsize::new(DHT_PERSIST_TORRENT_LOOKUP_CHUNK_LIMIT).unwrap(),
            plan_config: DhtTorrentPlanConfig::default(),
        }
    }
}

#[derive(Default)]
struct StatsInner {
    dequeued: AtomicU64,
    raw_batches: AtomicU64,
    planner_inputs: AtomicU64,
    planner_v2_dropped: AtomicU64,
    planner_primary_dropped: AtomicU64,
    planner_projected: AtomicU64,
    planner_projection_failed: AtomicU64,
    planner_blob_diagnostics: AtomicU64,
    planner_queue_diagnostics: AtomicU64,
    lookup_keys_planned: AtomicU64,
    lookup_keys_submitted: AtomicU64,
    lookup_keys_skipped_after_error: AtomicU64,
    lookup_keys_skipped_shutdown: AtomicU64,
    lookup_calls: AtomicU64,
    lookup_successes: AtomicU64,
    lookup_failures: AtomicU64,
    shutdown_lookup_calls_abandoned: AtomicU64,
    lookup_duplicate_rows_collapsed: AtomicU64,
    writer_calls: AtomicU64,
    writer_successes: AtomicU64,
    writer_rejections: AtomicU64,
    writer_outcomes_unknown: AtomicU64,
    shutdown_writer_calls_abandoned: AtomicU64,
    projected_persisted: AtomicU64,
    writer_rejected_projected: AtomicU64,
    writer_outcome_unknown_projected: AtomicU64,
    shutdown_write_abandoned: AtomicU64,
    scrape_candidates: AtomicU64,
    scrape_sent: AtomicU64,
    scrape_send_failures: AtomicU64,
    scrape_suppressed_writer_rejected: AtomicU64,
    scrape_suppressed_writer_unknown: AtomicU64,
    shutdown_write_scrapes_suppressed: AtomicU64,
    shutdown_scrape_abandoned: AtomicU64,
    shutdown_queued_dropped: AtomicU64,
    shutdown_preplan_dropped: AtomicU64,
}

/// Cloneable sender-free worker statistics handle.
///
/// A snapshot loads each saturating counter independently with relaxed atomic
/// ordering. Mid-run snapshots may therefore span different worker moments;
/// conservation helpers are authoritative only on a terminal snapshot taken
/// after [`DhtPersistTorrentWorker::run`] returns.
#[derive(Clone, Default)]
pub struct DhtPersistTorrentWorkerStatsHandle {
    inner: Arc<StatsInner>,
}

/// Saturating counters and terminal conservation helpers.
///
/// Counts describe logical worker disposition, not database rows affected.
/// In particular, `projected_persisted` means the writer returned `Ok` for
/// those projected torrents; it does not assert that every conditional SQL row
/// changed. Each helper uses saturating addition to match the counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtPersistTorrentWorkerStats {
    pub dequeued: u64,
    pub raw_batches: u64,
    pub planner_inputs: u64,
    pub planner_v2_dropped: u64,
    pub planner_primary_dropped: u64,
    pub planner_projected: u64,
    pub planner_projection_failed: u64,
    /// Silent optional-blob availability fallbacks; intentionally not warned
    /// to match Go's behavior.
    pub planner_blob_diagnostics: u64,
    /// Queue-job construction failures, each also emitted as a structured warn.
    pub planner_queue_diagnostics: u64,
    pub lookup_keys_planned: u64,
    pub lookup_keys_submitted: u64,
    pub lookup_keys_skipped_after_error: u64,
    pub lookup_keys_skipped_shutdown: u64,
    pub lookup_calls: u64,
    pub lookup_successes: u64,
    pub lookup_failures: u64,
    pub shutdown_lookup_calls_abandoned: u64,
    pub lookup_duplicate_rows_collapsed: u64,
    pub writer_calls: u64,
    pub writer_successes: u64,
    pub writer_rejections: u64,
    pub writer_outcomes_unknown: u64,
    pub shutdown_writer_calls_abandoned: u64,
    /// Logical projected torrents whose writer returned `Ok`; not SQL rows
    /// affected.
    pub projected_persisted: u64,
    pub writer_rejected_projected: u64,
    pub writer_outcome_unknown_projected: u64,
    /// Logical projected torrents attached to a writer future dropped by
    /// shutdown; backend outcome remains unconfirmed.
    pub shutdown_write_abandoned: u64,
    pub scrape_candidates: u64,
    pub scrape_sent: u64,
    pub scrape_send_failures: u64,
    pub scrape_suppressed_writer_rejected: u64,
    pub scrape_suppressed_writer_unknown: u64,
    /// Scrapes suppressed because shutdown abandoned the pending writer call.
    pub shutdown_write_scrapes_suppressed: u64,
    /// Pending/current and suffix scrapes abandoned after a confirmed writer `Ok`.
    pub shutdown_scrape_abandoned: u64,
    pub shutdown_queued_dropped: u64,
    pub shutdown_preplan_dropped: u64,
}

impl DhtPersistTorrentWorkerStats {
    /// Check `dequeued = planner_inputs + shutdown_preplan_dropped` using
    /// saturating addition.
    #[must_use]
    pub fn dequeued_conserves(self) -> bool {
        saturating_sum_eq(
            self.dequeued,
            &[self.planner_inputs, self.shutdown_preplan_dropped],
        )
    }

    /// Check the planner input partition into v2 drops, primary drops,
    /// projections, and projection failures using saturating addition.
    #[must_use]
    pub fn planner_conserves(self) -> bool {
        saturating_sum_eq(
            self.planner_inputs,
            &[
                self.planner_v2_dropped,
                self.planner_primary_dropped,
                self.planner_projected,
                self.planner_projection_failed,
            ],
        )
    }

    /// Check the writer-call partition into success, rejection, unknown, and
    /// shutdown-abandoned outcomes using saturating addition.
    #[must_use]
    pub fn writer_calls_conserve(self) -> bool {
        saturating_sum_eq(
            self.writer_calls,
            &[
                self.writer_successes,
                self.writer_rejections,
                self.writer_outcomes_unknown,
                self.shutdown_writer_calls_abandoned,
            ],
        )
    }

    /// Check the logical projected-torrent write partition using saturating
    /// addition; this is not an affected-row equation.
    #[must_use]
    pub fn projected_writes_conserve(self) -> bool {
        saturating_sum_eq(
            self.planner_projected,
            &[
                self.projected_persisted,
                self.writer_rejected_projected,
                self.writer_outcome_unknown_projected,
                self.shutdown_write_abandoned,
            ],
        )
    }

    /// Check every scrape candidate's terminal sent, failed, suppressed, or
    /// abandoned disposition using saturating addition.
    #[must_use]
    pub fn scrapes_conserve(self) -> bool {
        saturating_sum_eq(
            self.scrape_candidates,
            &[
                self.scrape_sent,
                self.scrape_send_failures,
                self.scrape_suppressed_writer_rejected,
                self.scrape_suppressed_writer_unknown,
                self.shutdown_write_scrapes_suppressed,
                self.shutdown_scrape_abandoned,
            ],
        )
    }

    /// Check lookup calls across success, failure, and shutdown abandonment
    /// using saturating addition.
    #[must_use]
    pub fn lookup_calls_conserve(self) -> bool {
        saturating_sum_eq(
            self.lookup_calls,
            &[
                self.lookup_successes,
                self.lookup_failures,
                self.shutdown_lookup_calls_abandoned,
            ],
        )
    }

    /// Check planned lookup keys across submitted and unsubmitted terminal
    /// dispositions using saturating addition.
    #[must_use]
    pub fn lookup_keys_conserve(self) -> bool {
        saturating_sum_eq(
            self.lookup_keys_planned,
            &[
                self.lookup_keys_submitted,
                self.lookup_keys_skipped_after_error,
                self.lookup_keys_skipped_shutdown,
            ],
        )
    }
}

impl DhtPersistTorrentWorkerStatsHandle {
    /// Independently load every saturating counter.
    ///
    /// Use conservation helpers only after the worker has reached a terminal
    /// exit; relaxed loads intentionally do not make a live snapshot atomic.
    #[must_use]
    pub fn snapshot(&self) -> DhtPersistTorrentWorkerStats {
        macro_rules! load {
            ($field:ident) => {
                self.inner.$field.load(Ordering::Relaxed)
            };
        }
        DhtPersistTorrentWorkerStats {
            dequeued: load!(dequeued),
            raw_batches: load!(raw_batches),
            planner_inputs: load!(planner_inputs),
            planner_v2_dropped: load!(planner_v2_dropped),
            planner_primary_dropped: load!(planner_primary_dropped),
            planner_projected: load!(planner_projected),
            planner_projection_failed: load!(planner_projection_failed),
            planner_blob_diagnostics: load!(planner_blob_diagnostics),
            planner_queue_diagnostics: load!(planner_queue_diagnostics),
            lookup_keys_planned: load!(lookup_keys_planned),
            lookup_keys_submitted: load!(lookup_keys_submitted),
            lookup_keys_skipped_after_error: load!(lookup_keys_skipped_after_error),
            lookup_keys_skipped_shutdown: load!(lookup_keys_skipped_shutdown),
            lookup_calls: load!(lookup_calls),
            lookup_successes: load!(lookup_successes),
            lookup_failures: load!(lookup_failures),
            shutdown_lookup_calls_abandoned: load!(shutdown_lookup_calls_abandoned),
            lookup_duplicate_rows_collapsed: load!(lookup_duplicate_rows_collapsed),
            writer_calls: load!(writer_calls),
            writer_successes: load!(writer_successes),
            writer_rejections: load!(writer_rejections),
            writer_outcomes_unknown: load!(writer_outcomes_unknown),
            shutdown_writer_calls_abandoned: load!(shutdown_writer_calls_abandoned),
            projected_persisted: load!(projected_persisted),
            writer_rejected_projected: load!(writer_rejected_projected),
            writer_outcome_unknown_projected: load!(writer_outcome_unknown_projected),
            shutdown_write_abandoned: load!(shutdown_write_abandoned),
            scrape_candidates: load!(scrape_candidates),
            scrape_sent: load!(scrape_sent),
            scrape_send_failures: load!(scrape_send_failures),
            scrape_suppressed_writer_rejected: load!(scrape_suppressed_writer_rejected),
            scrape_suppressed_writer_unknown: load!(scrape_suppressed_writer_unknown),
            shutdown_write_scrapes_suppressed: load!(shutdown_write_scrapes_suppressed),
            shutdown_scrape_abandoned: load!(shutdown_scrape_abandoned),
            shutdown_queued_dropped: load!(shutdown_queued_dropped),
            shutdown_preplan_dropped: load!(shutdown_preplan_dropped),
        }
    }
}

/// Terminal worker state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtPersistTorrentWorkerExit {
    /// Every received request was processed through its terminal worker
    /// disposition and all input senders were dropped. This does not imply
    /// every writer call returned `Ok`.
    InputClosed,
    /// Caller shutdown won a biased lifecycle boundary.
    Shutdown {
        /// Requests drained from the closed input queue without dequeueing.
        queued_dropped: usize,
        /// Dequeued requests discarded before the planner ran.
        preplan_dropped: usize,
        /// Projected torrents whose pending writer future was dropped.
        write_abandoned: usize,
        /// Scrape candidates suppressed with that abandoned writer call.
        write_scrapes_suppressed: usize,
        /// Pending/current and suffix scrapes abandoned after writer `Ok`.
        scrape_abandoned: usize,
    },
}

/// Owned sequential torrent-persistence worker.
///
/// The worker owns the unique input receiver, forms first-item-relative FIFO
/// batches, resolves full-v2 identities sequentially, plans synchronously,
/// writes once, and fans out scrapes only after writer `Ok`. Shutdown is biased
/// over intake, deadline, lookup, write, and scrape capacity. No collaborator
/// is retried and no detached task survives `run`.
#[must_use = "the worker must be run to consume torrent-persistence requests"]
pub struct DhtPersistTorrentWorker {
    input: DhtPersistTorrentReceiver,
    scrape: DhtScrapeInput,
    lookup: Arc<dyn DhtTorrentV2Lookup>,
    writer: Arc<dyn DhtTorrentBatchWriter>,
    planner: DhtTorrentPlanner,
    config: DhtPersistTorrentWorkerConfig,
    stats: DhtPersistTorrentWorkerStatsHandle,
}

impl DhtPersistTorrentWorker {
    #[cfg(test)]
    pub(crate) const fn config_for_test(&self) -> DhtPersistTorrentWorkerConfig {
        self.config
    }

    /// Construct a worker with production batching, lookup, and planner
    /// defaults, returning a sender-free statistics handle alongside it.
    pub fn new(
        input: DhtPersistTorrentReceiver,
        scrape: DhtScrapeInput,
        lookup: Arc<dyn DhtTorrentV2Lookup>,
        writer: Arc<dyn DhtTorrentBatchWriter>,
    ) -> (Self, DhtPersistTorrentWorkerStatsHandle) {
        Self::with_config(
            input,
            scrape,
            lookup,
            writer,
            DhtPersistTorrentWorkerConfig::default(),
        )
    }

    /// Construct a worker with an explicit immutable policy.
    ///
    /// Construction starts no task and performs no collaborator call.
    pub fn with_config(
        input: DhtPersistTorrentReceiver,
        scrape: DhtScrapeInput,
        lookup: Arc<dyn DhtTorrentV2Lookup>,
        writer: Arc<dyn DhtTorrentBatchWriter>,
        config: DhtPersistTorrentWorkerConfig,
    ) -> (Self, DhtPersistTorrentWorkerStatsHandle) {
        let stats = DhtPersistTorrentWorkerStatsHandle::default();
        (
            Self {
                input,
                scrape,
                lookup,
                writer,
                planner: DhtTorrentPlanner::new(config.plan_config),
                config,
                stats: stats.clone(),
            },
            stats,
        )
    }

    /// Consume the owned input until EOF or caller shutdown.
    ///
    /// EOF flushes a final partial batch. Shutdown closes and drains queued
    /// input, drops any pending collaborator/send future at the biased boundary,
    /// and reports the exact owned work classifications in the terminal exit.
    pub async fn run<F>(mut self, shutdown: F) -> DhtPersistTorrentWorkerExit
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        loop {
            let first = tokio::select! {
                biased;
                () = &mut shutdown => return self.finish_shutdown(0, 0, 0, 0),
                request = self.input.recv() => request,
            };
            let Some(first) = first else {
                return DhtPersistTorrentWorkerExit::InputClosed;
            };
            increment(&self.stats.inner.dequeued);
            let mut batch = Vec::with_capacity(self.config.batch_limit.get().min(64));
            batch.push(first);
            let mut input_closed = false;
            if batch.len() < self.config.batch_limit.get() && !self.config.batch_interval.is_zero()
            {
                let deadline = tokio::time::sleep(self.config.batch_interval);
                tokio::pin!(deadline);
                loop {
                    if batch.len() >= self.config.batch_limit.get() || input_closed {
                        break;
                    }
                    let next = tokio::select! {
                        biased;
                        () = &mut shutdown => return self.finish_shutdown(batch.len(), 0, 0, 0),
                        () = &mut deadline => break,
                        request = self.input.recv() => request,
                    };
                    match next {
                        Some(request) => {
                            increment(&self.stats.inner.dequeued);
                            batch.push(request);
                        }
                        None => input_closed = true,
                    }
                }
            }

            match self.process_batch(&batch, shutdown.as_mut()).await {
                ProcessResult::Complete => {}
                ProcessResult::Shutdown {
                    preplan_dropped,
                    write_abandoned,
                    write_scrapes_suppressed,
                    scrape_abandoned,
                } => {
                    return self.finish_shutdown(
                        preplan_dropped,
                        write_abandoned,
                        write_scrapes_suppressed,
                        scrape_abandoned,
                    );
                }
            }
            if input_closed {
                return tokio::select! {
                    biased;
                    () = &mut shutdown => self.finish_shutdown(0, 0, 0, 0),
                    () = std::future::ready(()) => DhtPersistTorrentWorkerExit::InputClosed,
                };
            }
        }
    }

    async fn process_batch<F>(
        &self,
        batch: &[DhtPersistTorrentRequest],
        mut shutdown: std::pin::Pin<&mut F>,
    ) -> ProcessResult
    where
        F: Future<Output = ()>,
    {
        increment(&self.stats.inner.raw_batches);
        let keys = self.planner.v2_lookup_keys(batch);
        add(&self.stats.inner.lookup_keys_planned, count(keys.len()));
        let mut existing = BTreeMap::new();
        for (chunk_index, chunk) in keys
            .chunks(self.config.lookup_chunk_limit.get())
            .enumerate()
        {
            increment(&self.stats.inner.lookup_calls);
            add(&self.stats.inner.lookup_keys_submitted, count(chunk.len()));
            let lookup = self.lookup.lookup_existing_v2(chunk);
            tokio::pin!(lookup);
            let result = tokio::select! {
                biased;
                () = shutdown.as_mut() => {
                    increment(&self.stats.inner.shutdown_lookup_calls_abandoned);
                    let submitted_end = (chunk_index + 1)
                        .saturating_mul(self.config.lookup_chunk_limit.get())
                        .min(keys.len());
                    add(
                        &self.stats.inner.lookup_keys_skipped_shutdown,
                        count(keys.len().saturating_sub(submitted_end)),
                    );
                    return ProcessResult::Shutdown {
                        preplan_dropped: batch.len(),
                        write_abandoned: 0,
                        write_scrapes_suppressed: 0,
                        scrape_abandoned: 0,
                    };
                }
                result = &mut lookup => result,
            };
            let rows = match result {
                Ok(rows) => rows,
                Err(source) => {
                    increment(&self.stats.inner.lookup_failures);
                    let submitted_end = (chunk_index + 1)
                        .saturating_mul(self.config.lookup_chunk_limit.get())
                        .min(keys.len());
                    add(
                        &self.stats.inner.lookup_keys_skipped_after_error,
                        count(keys.len().saturating_sub(submitted_end)),
                    );
                    tracing::warn!(%source, "DHT torrent v2 lookup failed; planning fail-open");
                    break;
                }
            };
            if rows
                .iter()
                .any(|row| chunk.binary_search(&row.info_hash_v2).is_err())
            {
                increment(&self.stats.inner.lookup_failures);
                let submitted_end = (chunk_index + 1)
                    .saturating_mul(self.config.lookup_chunk_limit.get())
                    .min(keys.len());
                add(
                    &self.stats.inner.lookup_keys_skipped_after_error,
                    count(keys.len().saturating_sub(submitted_end)),
                );
                tracing::warn!("DHT torrent v2 lookup returned a foreign key; discarding chunk");
                break;
            }
            increment(&self.stats.inner.lookup_successes);
            let mut canonical = BTreeMap::new();
            for row in rows {
                match canonical.entry(row.info_hash_v2) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(row.primary_info_hash);
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        increment(&self.stats.inner.lookup_duplicate_rows_collapsed);
                        if row.primary_info_hash < *entry.get() {
                            entry.insert(row.primary_info_hash);
                        }
                    }
                }
            }
            existing.extend(canonical);
        }

        let plan = self.planner.plan(batch, &existing);
        self.record_plan(&plan);
        increment(&self.stats.inner.writer_calls);
        let write = self.writer.write_batch(&plan.transaction);
        tokio::pin!(write);
        let result = tokio::select! {
            biased;
            () = shutdown.as_mut() => {
                increment(&self.stats.inner.shutdown_writer_calls_abandoned);
                return ProcessResult::Shutdown {
                    preplan_dropped: 0,
                    write_abandoned: plan.transaction.torrents.len(),
                    write_scrapes_suppressed: plan.scrape_candidates.len(),
                    scrape_abandoned: 0,
                };
            }
            result = &mut write => result,
        };
        match result {
            Ok(()) => {
                increment(&self.stats.inner.writer_successes);
                add(
                    &self.stats.inner.projected_persisted,
                    count(plan.transaction.torrents.len()),
                );
            }
            Err(DhtTorrentBatchWriteError::Rejected { source }) => {
                increment(&self.stats.inner.writer_rejections);
                add(
                    &self.stats.inner.writer_rejected_projected,
                    count(plan.transaction.torrents.len()),
                );
                add(
                    &self.stats.inner.scrape_suppressed_writer_rejected,
                    count(plan.scrape_candidates.len()),
                );
                tracing::warn!(%source, "DHT torrent batch rejected; not retrying");
                return ProcessResult::Complete;
            }
            Err(DhtTorrentBatchWriteError::OutcomeUnknown { source }) => {
                increment(&self.stats.inner.writer_outcomes_unknown);
                add(
                    &self.stats.inner.writer_outcome_unknown_projected,
                    count(plan.transaction.torrents.len()),
                );
                add(
                    &self.stats.inner.scrape_suppressed_writer_unknown,
                    count(plan.scrape_candidates.len()),
                );
                tracing::warn!(%source, "DHT torrent batch outcome unknown; not retrying");
                return ProcessResult::Complete;
            }
        }

        for (index, candidate) in plan.scrape_candidates.iter().copied().enumerate() {
            let send = self.scrape.send(candidate);
            tokio::pin!(send);
            let result = tokio::select! {
                biased;
                () = shutdown.as_mut() => {
                    return ProcessResult::Shutdown {
                        preplan_dropped: 0,
                        write_abandoned: 0,
                        write_scrapes_suppressed: 0,
                        scrape_abandoned: plan.scrape_candidates.len().saturating_sub(index),
                    };
                }
                result = &mut send => result,
            };
            match result {
                Ok(()) => increment(&self.stats.inner.scrape_sent),
                Err(_) => increment(&self.stats.inner.scrape_send_failures),
            }
        }
        ProcessResult::Complete
    }

    fn record_plan(&self, plan: &DhtTorrentPersistPlan) {
        add(&self.stats.inner.planner_inputs, plan.counts.input);
        add(&self.stats.inner.planner_v2_dropped, plan.counts.v2_dropped);
        add(
            &self.stats.inner.planner_primary_dropped,
            plan.counts.primary_dropped,
        );
        add(&self.stats.inner.planner_projected, plan.counts.projected);
        add(
            &self.stats.inner.planner_projection_failed,
            plan.counts.projection_failed,
        );
        add(
            &self.stats.inner.scrape_candidates,
            count(plan.scrape_candidates.len()),
        );
        for failure in &plan.projection_failures {
            tracing::warn!(
                input_index = failure.input_index,
                info_hash = %failure.info_hash,
                error = %failure.error,
                "DHT torrent projection failed; scrape remains eligible"
            );
        }
        for diagnostic in &plan.diagnostics {
            match diagnostic {
                DhtTorrentPlanDiagnostic::BlobEncodingFailed { .. } => {
                    // Go treats optional blob failure as a silent availability
                    // fallback; retain only the observable counter here.
                    increment(&self.stats.inner.planner_blob_diagnostics);
                }
                DhtTorrentPlanDiagnostic::QueueConstructionFailed { group_index, error } => {
                    increment(&self.stats.inner.planner_queue_diagnostics);
                    tracing::warn!(
                        group_index,
                        error = %error,
                        "DHT torrent classifier queue-job construction failed"
                    );
                }
            }
        }
    }

    fn finish_shutdown(
        &mut self,
        preplan_dropped: usize,
        write_abandoned: usize,
        write_scrapes_suppressed: usize,
        scrape_abandoned: usize,
    ) -> DhtPersistTorrentWorkerExit {
        self.input.close();
        let mut queued_dropped = 0_usize;
        while self.input.try_recv().is_ok() {
            queued_dropped = queued_dropped.saturating_add(1);
        }
        add(
            &self.stats.inner.shutdown_queued_dropped,
            count(queued_dropped),
        );
        add(
            &self.stats.inner.shutdown_preplan_dropped,
            count(preplan_dropped),
        );
        add(
            &self.stats.inner.shutdown_write_abandoned,
            count(write_abandoned),
        );
        add(
            &self.stats.inner.shutdown_write_scrapes_suppressed,
            count(write_scrapes_suppressed),
        );
        add(
            &self.stats.inner.shutdown_scrape_abandoned,
            count(scrape_abandoned),
        );
        DhtPersistTorrentWorkerExit::Shutdown {
            queued_dropped,
            preplan_dropped,
            write_abandoned,
            write_scrapes_suppressed,
            scrape_abandoned,
        }
    }
}

enum ProcessResult {
    Complete,
    Shutdown {
        preplan_dropped: usize,
        write_abandoned: usize,
        write_scrapes_suppressed: usize,
        scrape_abandoned: usize,
    },
}

fn increment(counter: &AtomicU64) {
    add(counter, 1);
}

fn add(counter: &AtomicU64, value: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(value);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn saturating_sum_eq(total: u64, parts: &[u64]) -> bool {
    parts
        .iter()
        .fold(0_u64, |sum, value| sum.saturating_add(*value))
        == total
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, VecDeque};
    use std::future::Future;
    use std::io;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    use bitmagnet_dht::{
        dht_scrape_channel, DhtInfoHashTriageRequest, DhtScrapeReceiver, DHT_SCRAPE_ROUTE_CAPACITY,
    };
    use bitmagnet_metainfo::parse_info_bytes;
    use sha1::Sha1;
    use sha2::{Digest, Sha256};
    use tokio::sync::Notify;

    use crate::{dht_persist_torrent_channel, DhtPersistTorrentInput};

    use super::*;

    fn append_bytes(output: &mut Vec<u8>, value: &[u8]) {
        output.extend_from_slice(value.len().to_string().as_bytes());
        output.push(b':');
        output.extend_from_slice(value);
    }

    fn v1_request(marker: u16) -> DhtPersistTorrentRequest {
        let name = format!("worker-v1-{marker:04}.bin");
        let mut raw = b"d6:lengthi4096e4:name".to_vec();
        append_bytes(&mut raw, name.as_bytes());
        raw.extend_from_slice(b"12:piece lengthi16384e6:pieces20:");
        raw.extend_from_slice(&[marker as u8; 20]);
        raw.push(b'e');
        let hash: [u8; 20] = Sha1::digest(&raw).into();
        DhtPersistTorrentRequest {
            info_hash: Id20::from_slice(&hash).unwrap(),
            source_node_addr: SocketAddr::from((
                Ipv4Addr::new(192, 0, (marker >> 8) as u8, marker as u8),
                10_000 + marker,
            )),
            meta_info: Arc::new(parse_info_bytes(hash, &raw).unwrap()),
        }
    }

    fn v2_request(marker: u8) -> DhtPersistTorrentRequest {
        let name = format!("v2-{marker:06}");
        let mut raw = b"d9:file tree".to_vec();
        raw.push(b'd');
        append_bytes(&mut raw, name.as_bytes());
        raw.extend_from_slice(b"d0:d6:lengthi1e11:pieces root32:");
        raw.extend_from_slice(&[marker; 32]);
        raw.extend_from_slice(b"eee12:meta versioni2e4:name");
        append_bytes(&mut raw, name.as_bytes());
        raw.extend_from_slice(b"12:piece lengthi16384ee");
        let full: [u8; 32] = Sha256::digest(&raw).into();
        let primary: [u8; 20] = full[..20].try_into().unwrap();
        DhtPersistTorrentRequest {
            info_hash: Id20::from_slice(&primary).unwrap(),
            source_node_addr: SocketAddr::from((Ipv4Addr::new(198, 51, 100, marker), 20_000)),
            meta_info: Arc::new(parse_info_bytes(primary, &raw).unwrap()),
        }
    }

    fn scrape_request(marker: u16) -> DhtInfoHashTriageRequest {
        let request = v1_request(marker);
        DhtInfoHashTriageRequest {
            info_hash: request.info_hash,
            source_node_addr: request.source_node_addr,
        }
    }

    async fn prefill_scrape_route(
        scrape: &DhtScrapeInput,
        count: usize,
    ) -> Vec<DhtInfoHashTriageRequest> {
        let mut expected = Vec::with_capacity(count);
        for offset in 0..count {
            let request = scrape_request(1_000 + u16::try_from(offset).unwrap());
            scrape.send(request).await.unwrap();
            expected.push(request);
        }
        expected
    }

    fn config(batch_limit: usize, lookup_chunk_limit: usize) -> DhtPersistTorrentWorkerConfig {
        DhtPersistTorrentWorkerConfig {
            batch_limit: NonZeroUsize::new(batch_limit).unwrap(),
            batch_interval: Duration::from_secs(3_600),
            lookup_chunk_limit: NonZeroUsize::new(lookup_chunk_limit).unwrap(),
            plan_config: DhtTorrentPlanConfig::default(),
        }
    }

    enum LookupStep {
        Rows(Vec<DhtExistingV2Row>),
        Error(&'static str),
    }

    struct ScriptLookup {
        steps: Mutex<VecDeque<LookupStep>>,
        calls: Mutex<Vec<Vec<[u8; 32]>>>,
    }

    impl ScriptLookup {
        fn new(steps: impl IntoIterator<Item = LookupStep>) -> Arc<Self> {
            Arc::new(Self {
                steps: Mutex::new(steps.into_iter().collect()),
                calls: Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> Vec<Vec<[u8; 32]>> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DhtTorrentV2Lookup for ScriptLookup {
        async fn lookup_existing_v2(
            &self,
            info_hashes_v2: &[[u8; 32]],
        ) -> Result<Vec<DhtExistingV2Row>, PersistTorrentCollaboratorError> {
            self.calls.lock().unwrap().push(info_hashes_v2.to_vec());
            match self.steps.lock().unwrap().pop_front() {
                Some(LookupStep::Rows(rows)) => Ok(rows),
                Some(LookupStep::Error(message)) => Err(Box::new(io::Error::other(message))),
                None => Ok(Vec::new()),
            }
        }
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    struct BlockingLookup {
        entered: Notify,
        dropped: Arc<AtomicBool>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl DhtTorrentV2Lookup for BlockingLookup {
        async fn lookup_existing_v2(
            &self,
            _info_hashes_v2: &[[u8; 32]],
        ) -> Result<Vec<DhtExistingV2Row>, PersistTorrentCollaboratorError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let _drop_flag = DropFlag(Arc::clone(&self.dropped));
            self.entered.notify_one();
            std::future::pending().await
        }
    }

    struct SignalingLookup {
        signal: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        dropped: Arc<AtomicBool>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl DhtTorrentV2Lookup for SignalingLookup {
        async fn lookup_existing_v2(
            &self,
            _info_hashes_v2: &[[u8; 32]],
        ) -> Result<Vec<DhtExistingV2Row>, PersistTorrentCollaboratorError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let _drop_flag = DropFlag(Arc::clone(&self.dropped));
            let signal = self.signal.lock().unwrap().take();
            if let Some(signal) = signal {
                let _ = signal.send(());
            }
            tokio::task::yield_now().await;
            Ok(Vec::new())
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct WriteCall {
        torrents: Vec<Id20>,
        files: usize,
        summaries: usize,
        sources: usize,
        pieces: usize,
        queue_jobs: usize,
    }

    enum WriteStep {
        Ok,
        Rejected(&'static str),
        Unknown(&'static str),
    }

    struct ScriptWriter {
        steps: Mutex<VecDeque<WriteStep>>,
        calls: Mutex<Vec<WriteCall>>,
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

    impl ScriptWriter {
        fn new(steps: impl IntoIterator<Item = WriteStep>) -> Arc<Self> {
            Arc::new(Self {
                steps: Mutex::new(steps.into_iter().collect()),
                calls: Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> Vec<WriteCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DhtTorrentBatchWriter for ScriptWriter {
        async fn write_batch(
            &self,
            plan: &DhtTorrentTransactionPlan,
        ) -> Result<(), DhtTorrentBatchWriteError> {
            self.calls.lock().unwrap().push(WriteCall {
                torrents: plan
                    .torrents
                    .iter()
                    .map(|torrent| torrent.info_hash)
                    .collect(),
                files: plan.files.len(),
                summaries: plan.file_summaries.len(),
                sources: plan.sources.len(),
                pieces: plan.pieces.len(),
                queue_jobs: plan.queue_jobs.len(),
            });
            match self.steps.lock().unwrap().pop_front() {
                Some(WriteStep::Rejected(message)) => Err(DhtTorrentBatchWriteError::rejected(
                    io::Error::other(message),
                )),
                Some(WriteStep::Unknown(message)) => Err(
                    DhtTorrentBatchWriteError::outcome_unknown(io::Error::other(message)),
                ),
                Some(WriteStep::Ok) | None => Ok(()),
            }
        }
    }

    struct BlockingWriter {
        entered: Notify,
        dropped: Arc<AtomicBool>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl DhtTorrentBatchWriter for BlockingWriter {
        async fn write_batch(
            &self,
            _plan: &DhtTorrentTransactionPlan,
        ) -> Result<(), DhtTorrentBatchWriteError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let _drop_flag = DropFlag(Arc::clone(&self.dropped));
            self.entered.notify_one();
            std::future::pending().await
        }
    }

    struct SignalingWriter {
        signal: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        dropped: Arc<AtomicBool>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl DhtTorrentBatchWriter for SignalingWriter {
        async fn write_batch(
            &self,
            _plan: &DhtTorrentTransactionPlan,
        ) -> Result<(), DhtTorrentBatchWriteError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let _drop_flag = DropFlag(Arc::clone(&self.dropped));
            let signal = self.signal.lock().unwrap().take();
            if let Some(signal) = signal {
                let _ = signal.send(());
            }
            tokio::task::yield_now().await;
            Ok(())
        }
    }

    async fn enqueue(
        input: &DhtPersistTorrentInput,
        requests: impl IntoIterator<Item = DhtPersistTorrentRequest>,
    ) {
        for request in requests {
            input.send(request).await.unwrap();
        }
    }

    fn worker(
        input: DhtPersistTorrentReceiver,
        lookup: Arc<dyn DhtTorrentV2Lookup>,
        writer: Arc<dyn DhtTorrentBatchWriter>,
        config: DhtPersistTorrentWorkerConfig,
    ) -> (
        DhtPersistTorrentWorker,
        DhtPersistTorrentWorkerStatsHandle,
        DhtScrapeReceiver,
    ) {
        let (scrape, receiver) = dht_scrape_channel();
        let (worker, stats) =
            DhtPersistTorrentWorker::with_config(input, scrape, lookup, writer, config);
        (worker, stats, receiver)
    }

    fn assert_conservation(stats: DhtPersistTorrentWorkerStats) {
        assert!(stats.dequeued_conserves());
        assert!(stats.planner_conserves());
        assert!(stats.writer_calls_conserve());
        assert!(stats.projected_writes_conserve());
        assert!(stats.scrapes_conserve());
        assert!(stats.lookup_calls_conserve());
        assert!(stats.lookup_keys_conserve());
    }

    async fn yield_until(mut predicate: impl FnMut() -> bool) {
        for _ in 0..1_000 {
            if predicate() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition did not become true");
    }

    #[test]
    fn defaults_traits_errors_and_saturating_conservation_are_exact() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DhtExistingV2Row>();
        assert_send_sync::<DhtPersistTorrentWorkerConfig>();
        assert_send_sync::<DhtPersistTorrentWorkerStatsHandle>();
        assert_send_sync::<DhtTorrentBatchWriteError>();
        let config = DhtPersistTorrentWorkerConfig::default();
        assert_eq!(config.batch_limit.get(), 1_000);
        assert_eq!(config.batch_interval, Duration::from_secs(60));
        assert_eq!(config.lookup_chunk_limit.get(), 1_000);
        assert_eq!(config.plan_config, DhtTorrentPlanConfig::default());

        let rejected = DhtTorrentBatchWriteError::rejected(io::Error::other("no"));
        assert_eq!(rejected.to_string(), "DHT torrent batch rejected: no");
        assert!(rejected.source().is_some());
        let unknown = DhtTorrentBatchWriteError::outcome_unknown(io::Error::other("maybe"));
        assert_eq!(
            unknown.to_string(),
            "DHT torrent batch outcome unknown: maybe"
        );
        assert!(unknown.source().is_some());

        let saturated = DhtPersistTorrentWorkerStats {
            dequeued: u64::MAX,
            planner_inputs: u64::MAX,
            shutdown_preplan_dropped: 1,
            ..DhtPersistTorrentWorkerStats::default()
        };
        assert!(saturated.dequeued_conserves());
        let mismatched = DhtPersistTorrentWorkerStats {
            dequeued: u64::MAX - 1,
            ..saturated
        };
        assert!(!mismatched.dequeued_conserves());
    }

    #[tokio::test]
    async fn empty_eof_calls_no_collaborator() {
        let (input, receiver) = dht_persist_torrent_channel();
        drop(input);
        let lookup = ScriptLookup::new([]);
        let writer = ScriptWriter::new([]);
        let (worker, stats, _scrape) =
            worker(receiver, lookup.clone(), writer.clone(), config(10, 10));

        assert_eq!(
            worker.run(std::future::pending()).await,
            DhtPersistTorrentWorkerExit::InputClosed
        );
        assert!(lookup.calls().is_empty());
        assert!(writer.calls().is_empty());
        assert_eq!(stats.snapshot(), DhtPersistTorrentWorkerStats::default());
    }

    #[tokio::test]
    async fn no_v2_skips_lookup_and_eof_flushes_partial_batch() {
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [v1_request(1)]).await;
        drop(input);
        let lookup = ScriptLookup::new([]);
        let writer = ScriptWriter::new([]);
        let (worker, stats, mut scrape) =
            worker(receiver, lookup.clone(), writer.clone(), config(10, 10));
        assert_eq!(
            worker.run(std::future::pending()).await,
            DhtPersistTorrentWorkerExit::InputClosed
        );
        assert!(lookup.calls().is_empty());
        assert_eq!(writer.calls().len(), 1);
        assert_eq!(
            scrape.recv().await.unwrap().source_node_addr,
            v1_request(1).source_node_addr
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.dequeued, 1);
        assert_eq!(snapshot.raw_batches, 1);
        assert_eq!(snapshot.writer_calls, 1);
        assert_eq!(snapshot.scrape_sent, 1);
        assert_conservation(snapshot);
    }

    #[tokio::test]
    async fn lookup_keys_are_sorted_unique_and_chunked_at_n_and_n_plus_one() {
        let requests = [v2_request(3), v2_request(1), v2_request(2), v2_request(1)];
        let expected: Vec<_> = requests
            .iter()
            .filter_map(|request| request.meta_info.info_hash_v2())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, requests).await;
        drop(input);
        let lookup = ScriptLookup::new([]);
        let writer = ScriptWriter::new([]);
        let (worker, stats, _scrape) = worker(receiver, lookup.clone(), writer, config(10, 2));
        assert_eq!(
            worker.run(std::future::pending()).await,
            DhtPersistTorrentWorkerExit::InputClosed
        );
        assert_eq!(
            lookup.calls(),
            [expected[..2].to_vec(), expected[2..].to_vec()]
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.lookup_keys_planned, 3);
        assert_eq!(snapshot.lookup_keys_submitted, 3);
        assert_eq!(snapshot.lookup_calls, 2);
        assert_eq!(snapshot.lookup_successes, 2);
        assert_conservation(snapshot);
    }

    #[tokio::test]
    async fn partial_lookup_failure_retains_prefix_stops_suffix_and_plans_fail_open() {
        let requests = [v2_request(1), v2_request(2), v2_request(3)];
        let keys: Vec<_> = requests
            .iter()
            .map(|request| request.meta_info.info_hash_v2().unwrap())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let dropped_info_hash = requests
            .iter()
            .find(|request| request.meta_info.info_hash_v2() == Some(keys[0]))
            .unwrap()
            .info_hash;
        let lookup = ScriptLookup::new([
            LookupStep::Rows(vec![DhtExistingV2Row {
                info_hash_v2: keys[0],
                primary_info_hash: Id20::ZERO,
            }]),
            LookupStep::Error("second chunk failed"),
        ]);
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, requests).await;
        drop(input);
        let writer = ScriptWriter::new([]);
        let (worker, stats, _scrape) =
            worker(receiver, lookup.clone(), writer.clone(), config(10, 1));
        worker.run(std::future::pending()).await;
        assert_eq!(lookup.calls(), [vec![keys[0]], vec![keys[1]]]);
        let call = &writer.calls()[0];
        assert_eq!(call.torrents.len(), 2);
        assert!(!call.torrents.contains(&dropped_info_hash));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.lookup_successes, 1);
        assert_eq!(snapshot.lookup_failures, 1);
        assert_eq!(snapshot.lookup_keys_skipped_after_error, 1);
        assert_eq!(snapshot.planner_v2_dropped, 1);
        assert_conservation(snapshot);
    }

    #[tokio::test]
    async fn foreign_lookup_row_discards_whole_chunk_but_still_plans_fail_open() {
        let requests = [v2_request(1), v2_request(2)];
        let valid = requests[0].meta_info.info_hash_v2().unwrap();
        let lookup = ScriptLookup::new([LookupStep::Rows(vec![
            DhtExistingV2Row {
                info_hash_v2: valid,
                primary_info_hash: Id20::ZERO,
            },
            DhtExistingV2Row {
                info_hash_v2: [0xff; 32],
                primary_info_hash: Id20::ZERO,
            },
        ])]);
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, requests).await;
        drop(input);
        let writer = ScriptWriter::new([]);
        let (worker, stats, _scrape) = worker(receiver, lookup, writer.clone(), config(10, 10));
        worker.run(std::future::pending()).await;
        assert_eq!(writer.calls()[0].torrents.len(), 2);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.lookup_failures, 1);
        assert_eq!(snapshot.lookup_successes, 0);
        assert_eq!(snapshot.planner_v2_dropped, 0);
        assert_conservation(snapshot);
    }

    #[tokio::test]
    async fn duplicate_lookup_rows_canonicalize_to_lexicographically_smallest_primary() {
        let request = v2_request(1);
        let key = request.meta_info.info_hash_v2().unwrap();
        let smaller = Id20::ZERO;
        let larger = Id20::from_slice(&[0xff; 20]).unwrap();
        let lookup = ScriptLookup::new([LookupStep::Rows(vec![
            DhtExistingV2Row {
                info_hash_v2: key,
                primary_info_hash: request.info_hash,
            },
            DhtExistingV2Row {
                info_hash_v2: key,
                primary_info_hash: larger,
            },
            DhtExistingV2Row {
                info_hash_v2: key,
                primary_info_hash: smaller,
            },
        ])]);
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [request]).await;
        drop(input);
        let writer = ScriptWriter::new([]);
        let (worker, stats, _scrape) = worker(receiver, lookup, writer.clone(), config(10, 10));
        worker.run(std::future::pending()).await;
        assert!(writer.calls()[0].torrents.is_empty());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.lookup_duplicate_rows_collapsed, 2);
        assert_eq!(snapshot.planner_v2_dropped, 1);
        assert_conservation(snapshot);
    }

    #[tokio::test]
    async fn all_projection_failure_still_writes_empty_transaction_then_scrapes() {
        let mut invalid = v1_request(1);
        invalid.info_hash = Id20::ZERO;
        let expected = invalid.source_node_addr;
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [invalid]).await;
        drop(input);
        let lookup = ScriptLookup::new([]);
        let writer = ScriptWriter::new([]);
        let (worker, stats, mut scrape) = worker(receiver, lookup, writer.clone(), config(10, 10));
        worker.run(std::future::pending()).await;
        assert_eq!(
            writer.calls(),
            [WriteCall {
                torrents: Vec::new(),
                files: 0,
                summaries: 0,
                sources: 0,
                pieces: 0,
                queue_jobs: 0,
            }]
        );
        assert_eq!(scrape.recv().await.unwrap().source_node_addr, expected);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.planner_projection_failed, 1);
        assert_eq!(snapshot.planner_projected, 0);
        assert_eq!(snapshot.writer_calls, 1);
        assert_eq!(snapshot.writer_successes, 1);
        assert_eq!(snapshot.scrape_sent, 1);
        assert_conservation(snapshot);
    }

    #[tokio::test]
    async fn writer_outcomes_are_terminal_per_batch_and_later_batches_continue() {
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [v1_request(1), v1_request(2), v1_request(3)]).await;
        drop(input);
        let writer = ScriptWriter::new([
            WriteStep::Ok,
            WriteStep::Rejected("definite rejection"),
            WriteStep::Unknown("commit outcome unknown"),
        ]);
        let (worker, stats, mut scrape) = worker(
            receiver,
            ScriptLookup::new([]),
            writer.clone(),
            config(1, 10),
        );

        assert_eq!(
            worker.run(std::future::pending()).await,
            DhtPersistTorrentWorkerExit::InputClosed
        );
        assert_eq!(writer.calls().len(), 3);
        assert_eq!(
            scrape.recv().await.unwrap().source_node_addr,
            v1_request(1).source_node_addr
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.raw_batches, 3);
        assert_eq!(snapshot.writer_calls, 3);
        assert_eq!(snapshot.writer_successes, 1);
        assert_eq!(snapshot.writer_rejections, 1);
        assert_eq!(snapshot.writer_outcomes_unknown, 1);
        assert_eq!(snapshot.projected_persisted, 1);
        assert_eq!(snapshot.writer_rejected_projected, 1);
        assert_eq!(snapshot.writer_outcome_unknown_projected, 1);
        assert_eq!(snapshot.scrape_sent, 1);
        assert_eq!(snapshot.scrape_suppressed_writer_rejected, 1);
        assert_eq!(snapshot.scrape_suppressed_writer_unknown, 1);
        assert_conservation(snapshot);
    }

    #[tokio::test]
    async fn raw_batch_limit_splits_n_and_n_plus_one_in_fifo_order() {
        let requests = [v1_request(1), v1_request(2), v1_request(3)];
        let expected: Vec<_> = requests.iter().map(|request| request.info_hash).collect();
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, requests).await;
        drop(input);
        let writer = ScriptWriter::new([]);
        let (worker, stats, _scrape) = worker(
            receiver,
            ScriptLookup::new([]),
            writer.clone(),
            config(2, 10),
        );

        assert_eq!(
            worker.run(std::future::pending()).await,
            DhtPersistTorrentWorkerExit::InputClosed
        );
        let calls = writer.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].torrents, expected[..2]);
        assert_eq!(calls[1].torrents, expected[2..]);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.dequeued, 3);
        assert_eq!(snapshot.raw_batches, 2);
        assert_conservation(snapshot);
    }

    #[tokio::test]
    async fn zero_interval_wins_before_each_additional_prequeued_receive() {
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [v1_request(1), v1_request(2), v1_request(3)]).await;
        drop(input);
        let writer = ScriptWriter::new([]);
        let mut worker_config = config(10, 10);
        worker_config.batch_interval = Duration::ZERO;
        let (worker, stats, _scrape) = worker(
            receiver,
            ScriptLookup::new([]),
            writer.clone(),
            worker_config,
        );

        assert_eq!(
            worker.run(std::future::pending()).await,
            DhtPersistTorrentWorkerExit::InputClosed
        );
        assert_eq!(
            writer
                .calls()
                .iter()
                .map(|call| call.torrents.len())
                .collect::<Vec<_>>(),
            [1, 1, 1]
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.raw_batches, 3);
        assert_conservation(snapshot);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_is_first_item_relative_and_resets_after_each_flush() {
        let (input, receiver) = dht_persist_torrent_channel();
        let writer = ScriptWriter::new([]);
        let mut worker_config = config(4, 10);
        worker_config.batch_interval = Duration::from_secs(60);
        let (worker, stats, _scrape) = worker(
            receiver,
            ScriptLookup::new([]),
            writer.clone(),
            worker_config,
        );
        let task = tokio::spawn(worker.run(std::future::pending()));

        input.send(v1_request(1)).await.unwrap();
        yield_until(|| stats.snapshot().dequeued == 1).await;
        tokio::time::advance(Duration::from_secs(59)).await;
        tokio::task::yield_now().await;
        assert!(writer.calls().is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        yield_until(|| writer.calls().len() == 1).await;

        input.send(v1_request(2)).await.unwrap();
        yield_until(|| stats.snapshot().dequeued == 2).await;
        tokio::time::advance(Duration::from_secs(59)).await;
        tokio::task::yield_now().await;
        assert_eq!(writer.calls().len(), 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        yield_until(|| writer.calls().len() == 2).await;
        drop(input);

        assert_eq!(
            task.await.unwrap(),
            DhtPersistTorrentWorkerExit::InputClosed
        );
        let calls = writer.calls();
        assert_eq!(calls[0].torrents, [v1_request(1).info_hash]);
        assert_eq!(calls[1].torrents, [v1_request(2).info_hash]);
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn closed_scrape_route_counts_every_candidate_and_continues_later_batch() {
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [v1_request(1), v1_request(2)]).await;
        drop(input);
        let (scrape, scrape_receiver) = dht_scrape_channel();
        drop(scrape_receiver);
        let writer = ScriptWriter::new([]);
        let (worker, stats) = DhtPersistTorrentWorker::with_config(
            receiver,
            scrape,
            ScriptLookup::new([]),
            writer.clone(),
            config(1, 10),
        );

        assert_eq!(
            worker.run(std::future::pending()).await,
            DhtPersistTorrentWorkerExit::InputClosed
        );
        assert_eq!(writer.calls().len(), 2);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.writer_successes, 2);
        assert_eq!(snapshot.scrape_candidates, 2);
        assert_eq!(snapshot.scrape_send_failures, 2);
        assert_eq!(snapshot.scrape_sent, 0);
        assert_conservation(snapshot);
    }

    #[tokio::test]
    async fn ready_shutdown_before_intake_drains_queue_without_dequeueing() {
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [v1_request(1), v1_request(2)]).await;
        let lookup = ScriptLookup::new([]);
        let writer = ScriptWriter::new([]);
        let (worker, stats, _scrape) =
            worker(receiver, lookup.clone(), writer.clone(), config(3, 10));

        assert_eq!(
            worker.run(std::future::ready(())).await,
            DhtPersistTorrentWorkerExit::Shutdown {
                queued_dropped: 2,
                preplan_dropped: 0,
                write_abandoned: 0,
                write_scrapes_suppressed: 0,
                scrape_abandoned: 0,
            }
        );
        assert!(input.send(v1_request(3)).await.is_err());
        assert!(lookup.calls().is_empty());
        assert!(writer.calls().is_empty());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.shutdown_queued_dropped, 2);
        assert_eq!(snapshot.dequeued, 0);
        assert_conservation(snapshot);
    }

    #[tokio::test]
    async fn shutdown_is_polled_before_each_additional_prequeued_receive() {
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [v1_request(1), v1_request(2), v1_request(3)]).await;
        let writer = ScriptWriter::new([]);
        let (worker, stats, _scrape) = worker(
            receiver,
            ScriptLookup::new([]),
            writer.clone(),
            config(3, 10),
        );
        let polls = Arc::new(AtomicUsize::new(0));

        assert_eq!(
            worker
                .run(PendingThenReady {
                    polls: Arc::clone(&polls),
                })
                .await,
            DhtPersistTorrentWorkerExit::Shutdown {
                queued_dropped: 2,
                preplan_dropped: 1,
                write_abandoned: 0,
                write_scrapes_suppressed: 0,
                scrape_abandoned: 0,
            }
        );
        assert_eq!(polls.load(Ordering::Relaxed), 2);
        assert!(writer.calls().is_empty());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.dequeued, 1);
        assert_eq!(snapshot.shutdown_preplan_dropped, 1);
        assert_eq!(snapshot.shutdown_queued_dropped, 2);
        assert_conservation(snapshot);
    }

    #[tokio::test]
    async fn shutdown_cancels_pending_lookup_and_accounts_unsubmitted_suffix() {
        let dropped = Arc::new(AtomicBool::new(false));
        let lookup = Arc::new(BlockingLookup {
            entered: Notify::new(),
            dropped: Arc::clone(&dropped),
            calls: AtomicUsize::new(0),
        });
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [v2_request(1), v2_request(2)]).await;
        drop(input);
        let writer = ScriptWriter::new([]);
        let (worker, stats, _scrape) =
            worker(receiver, lookup.clone(), writer.clone(), config(2, 1));
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(worker.run(async move {
            let _ = shutdown_receiver.await;
        }));

        tokio::time::timeout(Duration::from_secs(5), lookup.entered.notified())
            .await
            .expect("lookup did not start");
        shutdown_sender.send(()).unwrap();
        assert_eq!(
            task.await.unwrap(),
            DhtPersistTorrentWorkerExit::Shutdown {
                queued_dropped: 0,
                preplan_dropped: 2,
                write_abandoned: 0,
                write_scrapes_suppressed: 0,
                scrape_abandoned: 0,
            }
        );
        assert!(dropped.load(Ordering::Relaxed));
        assert_eq!(lookup.calls.load(Ordering::Relaxed), 1);
        assert!(writer.calls().is_empty());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.lookup_keys_planned, 2);
        assert_eq!(snapshot.lookup_keys_submitted, 1);
        assert_eq!(snapshot.lookup_keys_skipped_shutdown, 1);
        assert_eq!(snapshot.lookup_calls, 1);
        assert_eq!(snapshot.shutdown_lookup_calls_abandoned, 1);
        assert_eq!(snapshot.shutdown_preplan_dropped, 2);
        assert_conservation(snapshot);
    }

    #[tokio::test]
    async fn equal_ready_shutdown_is_biased_over_lookup_completion() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let lookup = Arc::new(SignalingLookup {
            signal: Mutex::new(Some(shutdown_sender)),
            dropped: Arc::clone(&dropped),
            calls: AtomicUsize::new(0),
        });
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [v2_request(1)]).await;
        drop(input);
        let writer = ScriptWriter::new([]);
        let (worker, stats, _scrape) =
            worker(receiver, lookup.clone(), writer.clone(), config(1, 1));

        assert_eq!(
            worker
                .run(async move {
                    let _ = shutdown_receiver.await;
                })
                .await,
            DhtPersistTorrentWorkerExit::Shutdown {
                queued_dropped: 0,
                preplan_dropped: 1,
                write_abandoned: 0,
                write_scrapes_suppressed: 0,
                scrape_abandoned: 0,
            }
        );
        assert!(dropped.load(Ordering::Relaxed));
        assert_eq!(lookup.calls.load(Ordering::Relaxed), 1);
        assert!(writer.calls().is_empty());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.lookup_keys_planned, 1);
        assert_eq!(snapshot.lookup_keys_submitted, 1);
        assert_eq!(snapshot.shutdown_lookup_calls_abandoned, 1);
        assert_conservation(snapshot);
    }

    #[tokio::test]
    async fn shutdown_cancels_pending_writer_and_abandons_projection_and_scrape() {
        let dropped = Arc::new(AtomicBool::new(false));
        let writer = Arc::new(BlockingWriter {
            entered: Notify::new(),
            dropped: Arc::clone(&dropped),
            calls: AtomicUsize::new(0),
        });
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [v1_request(1)]).await;
        drop(input);
        let (worker, stats, _scrape) = worker(
            receiver,
            ScriptLookup::new([]),
            writer.clone(),
            config(1, 10),
        );
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(worker.run(async move {
            let _ = shutdown_receiver.await;
        }));

        tokio::time::timeout(Duration::from_secs(5), writer.entered.notified())
            .await
            .expect("writer did not start");
        shutdown_sender.send(()).unwrap();
        assert_eq!(
            task.await.unwrap(),
            DhtPersistTorrentWorkerExit::Shutdown {
                queued_dropped: 0,
                preplan_dropped: 0,
                write_abandoned: 1,
                write_scrapes_suppressed: 1,
                scrape_abandoned: 0,
            }
        );
        assert!(dropped.load(Ordering::Relaxed));
        assert_eq!(writer.calls.load(Ordering::Relaxed), 1);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.writer_calls, 1);
        assert_eq!(snapshot.shutdown_writer_calls_abandoned, 1);
        assert_eq!(snapshot.shutdown_write_abandoned, 1);
        assert_eq!(snapshot.shutdown_write_scrapes_suppressed, 1);
        assert_eq!(snapshot.shutdown_scrape_abandoned, 0);
        assert_conservation(snapshot);
    }

    #[tokio::test]
    async fn equal_ready_shutdown_is_biased_over_writer_completion() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let writer = Arc::new(SignalingWriter {
            signal: Mutex::new(Some(shutdown_sender)),
            dropped: Arc::clone(&dropped),
            calls: AtomicUsize::new(0),
        });
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [v1_request(1)]).await;
        drop(input);
        let (worker, stats, _scrape) = worker(
            receiver,
            ScriptLookup::new([]),
            writer.clone(),
            config(1, 10),
        );

        assert_eq!(
            worker
                .run(async move {
                    let _ = shutdown_receiver.await;
                })
                .await,
            DhtPersistTorrentWorkerExit::Shutdown {
                queued_dropped: 0,
                preplan_dropped: 0,
                write_abandoned: 1,
                write_scrapes_suppressed: 1,
                scrape_abandoned: 0,
            }
        );
        assert!(dropped.load(Ordering::Relaxed));
        assert_eq!(writer.calls.load(Ordering::Relaxed), 1);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.writer_calls, 1);
        assert_eq!(snapshot.shutdown_writer_calls_abandoned, 1);
        assert_eq!(snapshot.writer_successes, 0);
        assert_eq!(snapshot.shutdown_write_abandoned, 1);
        assert_eq!(snapshot.shutdown_write_scrapes_suppressed, 1);
        assert_eq!(snapshot.shutdown_scrape_abandoned, 0);
        assert_conservation(snapshot);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn post_commit_scrape_backpressure_shutdown_preserves_writer_success() {
        let (scrape, mut scrape_receiver) = dht_scrape_channel();
        let prefilled = prefill_scrape_route(&scrape, DHT_SCRAPE_ROUTE_CAPACITY).await;
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [v1_request(1), v1_request(2)]).await;
        drop(input);
        let writer = ScriptWriter::new([]);
        let (worker, stats) = DhtPersistTorrentWorker::with_config(
            receiver,
            scrape,
            ScriptLookup::new([]),
            writer.clone(),
            config(2, 10),
        );
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(worker.run(async move {
            let _ = shutdown_receiver.await;
        }));

        yield_until(|| stats.snapshot().writer_successes == 1).await;
        shutdown_sender.send(()).unwrap();
        assert_eq!(
            task.await.unwrap(),
            DhtPersistTorrentWorkerExit::Shutdown {
                queued_dropped: 0,
                preplan_dropped: 0,
                write_abandoned: 0,
                write_scrapes_suppressed: 0,
                scrape_abandoned: 2,
            }
        );
        assert_eq!(writer.calls().len(), 1);
        let mut retained = Vec::new();
        while let Ok(request) = scrape_receiver.try_recv() {
            retained.push(request);
        }
        assert_eq!(retained, prefilled);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.writer_successes, 1);
        assert_eq!(snapshot.projected_persisted, 2);
        assert_eq!(snapshot.scrape_candidates, 2);
        assert_eq!(snapshot.scrape_sent, 0);
        assert_eq!(snapshot.shutdown_scrape_abandoned, 2);
        assert_conservation(snapshot);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn equal_ready_shutdown_beats_freed_scrape_capacity_without_committing_candidate() {
        let (scrape, mut scrape_receiver) = dht_scrape_channel();
        let prefilled = prefill_scrape_route(&scrape, DHT_SCRAPE_ROUTE_CAPACITY).await;
        let candidate = scrape_request(1);
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [v1_request(1)]).await;
        drop(input);
        let writer = ScriptWriter::new([]);
        let (worker, stats) = DhtPersistTorrentWorker::with_config(
            receiver,
            scrape,
            ScriptLookup::new([]),
            writer,
            config(1, 10),
        );
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(worker.run(async move {
            let _ = shutdown_receiver.await;
        }));

        yield_until(|| stats.snapshot().writer_successes == 1).await;
        shutdown_sender.send(()).unwrap();
        assert_eq!(scrape_receiver.try_recv().unwrap(), prefilled[0]);
        assert_eq!(
            task.await.unwrap(),
            DhtPersistTorrentWorkerExit::Shutdown {
                queued_dropped: 0,
                preplan_dropped: 0,
                write_abandoned: 0,
                write_scrapes_suppressed: 0,
                scrape_abandoned: 1,
            }
        );
        let mut retained = Vec::new();
        while let Ok(request) = scrape_receiver.try_recv() {
            retained.push(request);
        }
        assert_eq!(retained, prefilled[1..]);
        assert!(!retained.contains(&candidate));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.writer_successes, 1);
        assert_eq!(snapshot.projected_persisted, 1);
        assert_eq!(snapshot.scrape_sent, 0);
        assert_eq!(snapshot.shutdown_scrape_abandoned, 1);
        assert_conservation(snapshot);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn post_success_scrape_prefix_is_retained_before_shutdown_abandons_suffix() {
        let (scrape, mut scrape_receiver) = dht_scrape_channel();
        let prefilled = prefill_scrape_route(&scrape, DHT_SCRAPE_ROUTE_CAPACITY - 1).await;
        let first_candidate = scrape_request(1);
        let second_candidate = scrape_request(2);
        let (input, receiver) = dht_persist_torrent_channel();
        enqueue(&input, [v1_request(1), v1_request(2)]).await;
        drop(input);
        let (worker, stats) = DhtPersistTorrentWorker::with_config(
            receiver,
            scrape,
            ScriptLookup::new([]),
            ScriptWriter::new([]),
            config(2, 10),
        );
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(worker.run(async move {
            let _ = shutdown_receiver.await;
        }));

        yield_until(|| stats.snapshot().scrape_sent == 1).await;
        shutdown_sender.send(()).unwrap();
        assert_eq!(
            task.await.unwrap(),
            DhtPersistTorrentWorkerExit::Shutdown {
                queued_dropped: 0,
                preplan_dropped: 0,
                write_abandoned: 0,
                write_scrapes_suppressed: 0,
                scrape_abandoned: 1,
            }
        );
        let mut retained = Vec::new();
        while let Ok(request) = scrape_receiver.try_recv() {
            retained.push(request);
        }
        let mut expected = prefilled;
        expected.push(first_candidate);
        assert_eq!(retained, expected);
        assert!(!retained.contains(&second_candidate));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.writer_successes, 1);
        assert_eq!(snapshot.projected_persisted, 2);
        assert_eq!(snapshot.scrape_sent, 1);
        assert_eq!(snapshot.shutdown_scrape_abandoned, 1);
        assert_conservation(snapshot);
    }
}
