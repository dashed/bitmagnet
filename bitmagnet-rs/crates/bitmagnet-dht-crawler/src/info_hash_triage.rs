use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bitmagnet_dht::{
    DhtGetPeersInput, DhtInfoHashTriageReceiver, DhtInfoHashTriageRequest, DhtScrapeInput, Id20,
};
use bitmagnet_model::FilesStatus;
use tokio::sync::mpsc::error::TryRecvError;

/// Go production's maximum number of requests in one triage batch.
pub const DHT_INFO_HASH_TRIAGE_BATCH_LIMIT: usize = 1_000;
/// Rust's owned first-item-relative flush interval, equal in length to Go's
/// production batching ticker.
pub const DHT_INFO_HASH_TRIAGE_BATCH_INTERVAL: Duration = Duration::from_secs(20);
/// Go production's default maximum retained file count.
pub const DHT_INFO_HASH_TRIAGE_SAVE_FILES_THRESHOLD: u64 = 100;
/// Go production's default age at which DHT swarm counts need rescraping.
pub const DHT_INFO_HASH_TRIAGE_RESCRAPE_THRESHOLD: Duration =
    Duration::from_secs(30 * 24 * 60 * 60);

/// Error type returned by injected blocking-filter and database collaborators.
pub type TriageCollaboratorError = Box<dyn Error + Send + Sync + 'static>;

/// Blocking-policy projection needed by info-hash triage.
#[async_trait]
pub trait DhtInfoHashBlockFilter: Send + Sync {
    /// Return the input hashes that remain eligible, preserving any order or
    /// duplicate behavior supplied by the concrete policy implementation.
    /// Every returned hash must occur in `info_hashes`; any foreign hash is a
    /// contract violation that fails the whole batch closed.
    async fn filter(&self, info_hashes: &[Id20]) -> Result<Vec<Id20>, TriageCollaboratorError>;
}

/// One database row used by the triage decision matrix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtTorrentTriageRow {
    pub info_hash: Id20,
    pub files_status: FilesStatus,
    pub files_count: Option<u64>,
    pub dht_seeders: Option<u64>,
    pub dht_leechers: Option<u64>,
    pub dht_updated_at_unix_micros: Option<i64>,
}

/// Database projection needed by info-hash triage.
#[async_trait]
pub trait DhtTorrentTriageLookup: Send + Sync {
    /// Look up torrent and DHT-source fields for the supplied hashes.
    /// Duplicate result hashes are permitted; the final row wins.
    async fn lookup(
        &self,
        info_hashes: &[Id20],
    ) -> Result<Vec<DhtTorrentTriageRow>, TriageCollaboratorError>;
}

/// Wall-clock seam for strict staleness decisions.
pub trait DhtInfoHashTriageClock: Send + Sync {
    fn now_unix_micros(&self) -> i64;
}

/// Production wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemDhtInfoHashTriageClock;

impl DhtInfoHashTriageClock for SystemDhtInfoHashTriageClock {
    fn now_unix_micros(&self) -> i64 {
        system_time_unix_micros(SystemTime::now())
    }
}

/// Owned batching and routing policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtInfoHashTriageConfig {
    pub batch_limit: NonZeroUsize,
    pub batch_interval: Duration,
    pub save_files_threshold: u64,
    pub rescrape_threshold: Duration,
}

impl Default for DhtInfoHashTriageConfig {
    fn default() -> Self {
        Self {
            batch_limit: NonZeroUsize::new(DHT_INFO_HASH_TRIAGE_BATCH_LIMIT).unwrap(),
            batch_interval: DHT_INFO_HASH_TRIAGE_BATCH_INTERVAL,
            save_files_threshold: DHT_INFO_HASH_TRIAGE_SAVE_FILES_THRESHOLD,
            rescrape_threshold: DHT_INFO_HASH_TRIAGE_RESCRAPE_THRESHOLD,
        }
    }
}

#[derive(Default)]
struct DhtInfoHashTriageStatsInner {
    dequeued: AtomicU64,
    batches: AtomicU64,
    input_duplicates_dropped: AtomicU64,
    filter_calls: AtomicU64,
    filter_failures: AtomicU64,
    filter_hashes_returned: AtomicU64,
    filter_suppressed: AtomicU64,
    filter_failure_dropped: AtomicU64,
    filter_contract_dropped: AtomicU64,
    lookup_calls: AtomicU64,
    lookup_failures: AtomicU64,
    lookup_failure_dropped: AtomicU64,
    unknown_filtered_hashes_dropped: AtomicU64,
    get_peers_queued: AtomicU64,
    scrape_queued: AtomicU64,
    discarded: AtomicU64,
    route_closures: AtomicU64,
    shutdown_queued_dropped: AtomicU64,
    shutdown_batch_dropped: AtomicU64,
    route_closed_queued_dropped: AtomicU64,
    route_closed_batch_dropped: AtomicU64,
}

/// Cloneable sender-free counter view.
#[derive(Clone, Default)]
pub struct DhtInfoHashTriageStatsHandle {
    inner: Arc<DhtInfoHashTriageStatsInner>,
}

/// One independently read snapshot of saturating worker counters.
///
/// At a terminal snapshot, `dequeued` is conserved by the sum of input
/// duplicates, filter suppression/failure/contract drops, lookup-failure drops,
/// committed routes, policy discards, and shutdown/route-closure batch drops.
/// Queued drops were never dequeued and are tracked separately.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtInfoHashTriageStats {
    pub dequeued: u64,
    pub batches: u64,
    pub input_duplicates_dropped: u64,
    pub filter_calls: u64,
    pub filter_failures: u64,
    pub filter_hashes_returned: u64,
    pub filter_suppressed: u64,
    pub filter_failure_dropped: u64,
    pub filter_contract_dropped: u64,
    pub lookup_calls: u64,
    pub lookup_failures: u64,
    pub lookup_failure_dropped: u64,
    pub unknown_filtered_hashes_dropped: u64,
    pub get_peers_queued: u64,
    pub scrape_queued: u64,
    pub discarded: u64,
    pub route_closures: u64,
    pub shutdown_queued_dropped: u64,
    pub shutdown_batch_dropped: u64,
    pub route_closed_queued_dropped: u64,
    pub route_closed_batch_dropped: u64,
}

impl DhtInfoHashTriageStatsHandle {
    #[must_use]
    pub fn snapshot(&self) -> DhtInfoHashTriageStats {
        let inner = &self.inner;
        DhtInfoHashTriageStats {
            dequeued: inner.dequeued.load(Ordering::Relaxed),
            batches: inner.batches.load(Ordering::Relaxed),
            input_duplicates_dropped: inner.input_duplicates_dropped.load(Ordering::Relaxed),
            filter_calls: inner.filter_calls.load(Ordering::Relaxed),
            filter_failures: inner.filter_failures.load(Ordering::Relaxed),
            filter_hashes_returned: inner.filter_hashes_returned.load(Ordering::Relaxed),
            filter_suppressed: inner.filter_suppressed.load(Ordering::Relaxed),
            filter_failure_dropped: inner.filter_failure_dropped.load(Ordering::Relaxed),
            filter_contract_dropped: inner.filter_contract_dropped.load(Ordering::Relaxed),
            lookup_calls: inner.lookup_calls.load(Ordering::Relaxed),
            lookup_failures: inner.lookup_failures.load(Ordering::Relaxed),
            lookup_failure_dropped: inner.lookup_failure_dropped.load(Ordering::Relaxed),
            unknown_filtered_hashes_dropped: inner
                .unknown_filtered_hashes_dropped
                .load(Ordering::Relaxed),
            get_peers_queued: inner.get_peers_queued.load(Ordering::Relaxed),
            scrape_queued: inner.scrape_queued.load(Ordering::Relaxed),
            discarded: inner.discarded.load(Ordering::Relaxed),
            route_closures: inner.route_closures.load(Ordering::Relaxed),
            shutdown_queued_dropped: inner.shutdown_queued_dropped.load(Ordering::Relaxed),
            shutdown_batch_dropped: inner.shutdown_batch_dropped.load(Ordering::Relaxed),
            route_closed_queued_dropped: inner.route_closed_queued_dropped.load(Ordering::Relaxed),
            route_closed_batch_dropped: inner.route_closed_batch_dropped.load(Ordering::Relaxed),
        }
    }
}

/// Terminal state of the owned triage worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtInfoHashTriageWorkerExit {
    /// Every input clone is gone and the final partial batch was processed.
    InputClosed,
    /// Caller shutdown stopped intake or a collaborator/downstream wait.
    Shutdown {
        queued_dropped: usize,
        batch_dropped: usize,
    },
    /// A required get-peers action could not be queued.
    GetPeersClosed {
        request: DhtInfoHashTriageRequest,
        queued_dropped: usize,
        batch_dropped: usize,
    },
    /// A required scrape action could not be queued.
    ScrapeClosed {
        request: DhtInfoHashTriageRequest,
        queued_dropped: usize,
        batch_dropped: usize,
    },
}

/// Owned sequential info-hash triage worker.
///
/// Input duplicates use the first request. Filter and lookup failures drop only
/// the current batch and continue. Lookup duplicates use the final row. Rust
/// routes the first occurrence of each filtered hash deterministically; Go's
/// map iteration order is deliberately not reproduced.
///
/// Intentional Go deltas are explicit EOF and typed downstream-closure exits,
/// first-item-relative batching within this task, no detached batcher or
/// one-batch output buffer, lower total retention, and deterministic route
/// order. All dequeued requests receive one terminal accounting outcome.
#[must_use = "the worker must be run to consume info-hash triage requests"]
pub struct DhtInfoHashTriageWorker {
    input: DhtInfoHashTriageReceiver,
    get_peers: DhtGetPeersInput,
    scrape: DhtScrapeInput,
    filter: Arc<dyn DhtInfoHashBlockFilter>,
    lookup: Arc<dyn DhtTorrentTriageLookup>,
    clock: Arc<dyn DhtInfoHashTriageClock>,
    config: DhtInfoHashTriageConfig,
    stats: DhtInfoHashTriageStatsHandle,
}

impl DhtInfoHashTriageWorker {
    #[cfg(test)]
    pub(crate) const fn config_for_test(&self) -> DhtInfoHashTriageConfig {
        self.config
    }

    /// Construct the production-policy worker with an injected filter and
    /// lookup and the system wall clock.
    pub fn new(
        input: DhtInfoHashTriageReceiver,
        get_peers: DhtGetPeersInput,
        scrape: DhtScrapeInput,
        filter: Arc<dyn DhtInfoHashBlockFilter>,
        lookup: Arc<dyn DhtTorrentTriageLookup>,
    ) -> (Self, DhtInfoHashTriageStatsHandle) {
        Self::with_config(
            input,
            get_peers,
            scrape,
            filter,
            lookup,
            Arc::new(SystemDhtInfoHashTriageClock),
            DhtInfoHashTriageConfig::default(),
        )
    }

    /// Construct with explicit policy and clock seams.
    #[allow(clippy::too_many_arguments)]
    pub fn with_config(
        input: DhtInfoHashTriageReceiver,
        get_peers: DhtGetPeersInput,
        scrape: DhtScrapeInput,
        filter: Arc<dyn DhtInfoHashBlockFilter>,
        lookup: Arc<dyn DhtTorrentTriageLookup>,
        clock: Arc<dyn DhtInfoHashTriageClock>,
        config: DhtInfoHashTriageConfig,
    ) -> (Self, DhtInfoHashTriageStatsHandle) {
        let stats = DhtInfoHashTriageStatsHandle::default();
        (
            Self {
                input,
                get_peers,
                scrape,
                filter,
                lookup,
                clock,
                config,
                stats: stats.clone(),
            },
            stats,
        )
    }

    /// Run until input EOF, caller shutdown, or a required output route closes.
    ///
    /// Rust owns its first-item-relative batch delay and starts no detached
    /// task. Shutdown is biased ahead of intake, collaborator completion, and
    /// output send commitment.
    pub async fn run<F>(self, shutdown: F) -> DhtInfoHashTriageWorkerExit
    where
        F: Future<Output = ()>,
    {
        self.run_with(shutdown, |_, _, _| {}).await
    }

    async fn run_with<F, O>(mut self, shutdown: F, before_send: O) -> DhtInfoHashTriageWorkerExit
    where
        F: Future<Output = ()>,
        O: Fn(usize, Route, &DhtInfoHashTriageRequest) + Send + Sync,
    {
        tokio::pin!(shutdown);

        loop {
            let first = tokio::select! {
                biased;
                () = &mut shutdown => return self.finish_shutdown(0),
                request = self.input.recv() => request,
            };
            let Some(first) = first else {
                return DhtInfoHashTriageWorkerExit::InputClosed;
            };
            increment_saturating(&self.stats.inner.dequeued);

            let mut batch = Vec::with_capacity(self.config.batch_limit.get().min(64));
            batch.push(first);
            let mut input_closed = false;
            if batch.len() < self.config.batch_limit.get() {
                let delay = tokio::time::sleep(self.config.batch_interval);
                tokio::pin!(delay);
                loop {
                    while batch.len() < self.config.batch_limit.get() {
                        match self.input.try_recv() {
                            Ok(request) => {
                                increment_saturating(&self.stats.inner.dequeued);
                                batch.push(request);
                            }
                            Err(TryRecvError::Empty) => break,
                            Err(TryRecvError::Disconnected) => {
                                input_closed = true;
                                break;
                            }
                        }
                    }
                    if batch.len() >= self.config.batch_limit.get() || input_closed {
                        break;
                    }
                    let next = tokio::select! {
                        biased;
                        () = &mut shutdown => return self.finish_shutdown(batch.len()),
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

            match self
                .process_batch(&batch, shutdown.as_mut(), &before_send)
                .await
            {
                BatchResult::Complete => {}
                BatchResult::Shutdown { batch_dropped } => {
                    return self.finish_shutdown(batch_dropped);
                }
                BatchResult::GetPeersClosed {
                    request,
                    batch_dropped,
                } => {
                    let queued_dropped = self.close_and_drain_input();
                    increment_saturating(&self.stats.inner.route_closures);
                    add_saturating(
                        &self.stats.inner.route_closed_queued_dropped,
                        queued_dropped as u64,
                    );
                    add_saturating(
                        &self.stats.inner.route_closed_batch_dropped,
                        batch_dropped as u64,
                    );
                    return DhtInfoHashTriageWorkerExit::GetPeersClosed {
                        request,
                        queued_dropped,
                        batch_dropped,
                    };
                }
                BatchResult::ScrapeClosed {
                    request,
                    batch_dropped,
                } => {
                    let queued_dropped = self.close_and_drain_input();
                    increment_saturating(&self.stats.inner.route_closures);
                    add_saturating(
                        &self.stats.inner.route_closed_queued_dropped,
                        queued_dropped as u64,
                    );
                    add_saturating(
                        &self.stats.inner.route_closed_batch_dropped,
                        batch_dropped as u64,
                    );
                    return DhtInfoHashTriageWorkerExit::ScrapeClosed {
                        request,
                        queued_dropped,
                        batch_dropped,
                    };
                }
            }
            if input_closed {
                return tokio::select! {
                    biased;
                    () = &mut shutdown => self.finish_shutdown(0),
                    () = std::future::ready(()) => DhtInfoHashTriageWorkerExit::InputClosed,
                };
            }
        }
    }

    async fn process_batch<F, O>(
        &self,
        batch: &[DhtInfoHashTriageRequest],
        mut shutdown: std::pin::Pin<&mut F>,
        before_send: &O,
    ) -> BatchResult
    where
        F: Future<Output = ()>,
        O: Fn(usize, Route, &DhtInfoHashTriageRequest) + Send + Sync,
    {
        increment_saturating(&self.stats.inner.batches);
        let mut requests = HashMap::with_capacity(batch.len());
        let mut unique_hashes = Vec::with_capacity(batch.len());
        for request in batch {
            if requests.contains_key(&request.info_hash) {
                increment_saturating(&self.stats.inner.input_duplicates_dropped);
                continue;
            }
            unique_hashes.push(request.info_hash);
            requests.insert(request.info_hash, *request);
        }

        increment_saturating(&self.stats.inner.filter_calls);
        let filter = self.filter.filter(&unique_hashes);
        tokio::pin!(filter);
        let filtered = tokio::select! {
            biased;
            () = shutdown.as_mut() => return BatchResult::Shutdown {
                batch_dropped: unique_hashes.len(),
            },
            result = &mut filter => match result {
                Ok(filtered) => filtered,
                Err(error) => {
                    increment_saturating(&self.stats.inner.filter_failures);
                    add_saturating(&self.stats.inner.filter_failure_dropped, unique_hashes.len() as u64);
                    tracing::warn!(%error, "DHT info-hash blocking filter failed; dropping batch");
                    return BatchResult::Complete;
                }
            },
        };
        add_saturating(
            &self.stats.inner.filter_hashes_returned,
            filtered.len() as u64,
        );
        if filtered.is_empty() {
            add_saturating(
                &self.stats.inner.filter_suppressed,
                unique_hashes.len() as u64,
            );
            return BatchResult::Complete;
        }

        let foreign_hashes = filtered
            .iter()
            .filter(|info_hash| !requests.contains_key(info_hash))
            .count();
        if foreign_hashes > 0 {
            add_saturating(
                &self.stats.inner.unknown_filtered_hashes_dropped,
                foreign_hashes as u64,
            );
            add_saturating(
                &self.stats.inner.filter_contract_dropped,
                unique_hashes.len() as u64,
            );
            tracing::warn!(
                foreign_hashes,
                "DHT info-hash filter returned hashes outside the input batch; dropping batch"
            );
            return BatchResult::Complete;
        }

        let mut filtered_seen = HashSet::with_capacity(filtered.len());
        let mut routing_hashes = Vec::with_capacity(filtered.len());
        for info_hash in &filtered {
            if filtered_seen.insert(*info_hash) {
                routing_hashes.push(*info_hash);
            }
        }
        add_saturating(
            &self.stats.inner.filter_suppressed,
            unique_hashes.len().saturating_sub(routing_hashes.len()) as u64,
        );

        increment_saturating(&self.stats.inner.lookup_calls);
        let rows = {
            let lookup = self.lookup.lookup(&filtered);
            tokio::pin!(lookup);
            tokio::select! {
                biased;
                () = shutdown.as_mut() => return BatchResult::Shutdown {
                    batch_dropped: routing_hashes.len(),
                },
                result = &mut lookup => match result {
                    Ok(rows) => rows,
                    Err(error) => {
                        increment_saturating(&self.stats.inner.lookup_failures);
                        add_saturating(&self.stats.inner.lookup_failure_dropped, routing_hashes.len() as u64);
                        tracing::warn!(%error, "DHT torrent triage lookup failed; dropping batch");
                        return BatchResult::Complete;
                    }
                },
            }
        };
        let mut found = HashMap::with_capacity(rows.len());
        for row in rows {
            found.insert(row.info_hash, row);
        }

        let routing_count = routing_hashes.len();
        let rescrape_threshold_micros = duration_micros_i128(self.config.rescrape_threshold);
        for (index, info_hash) in routing_hashes.into_iter().enumerate() {
            let request = requests[&info_hash];
            let route = match found.get(&info_hash) {
                None => Some(Route::GetPeers),
                Some(row) => {
                    let get_peers = row.files_status == FilesStatus::NoInfo
                        || (row.files_status != FilesStatus::Single && row.files_count.is_none())
                        || (row.files_status == FilesStatus::OverThreshold
                            && row
                                .files_count
                                .is_some_and(|count| count <= self.config.save_files_threshold));
                    if get_peers {
                        Some(Route::GetPeers)
                    } else if row.dht_seeders.is_none() || row.dht_leechers.is_none() {
                        Some(Route::Scrape)
                    } else {
                        let cutoff =
                            i128::from(self.clock.now_unix_micros()) - rescrape_threshold_micros;
                        row.dht_updated_at_unix_micros
                            .is_none_or(|updated_at| i128::from(updated_at) < cutoff)
                            .then_some(Route::Scrape)
                    }
                }
            };
            let Some(route) = route else {
                increment_saturating(&self.stats.inner.discarded);
                continue;
            };
            let batch_dropped = routing_count - index;
            before_send(index + 1, route, &request);
            match route {
                Route::GetPeers => {
                    let send = self.get_peers.send(request);
                    tokio::pin!(send);
                    let result = tokio::select! {
                        biased;
                        () = shutdown.as_mut() => return BatchResult::Shutdown { batch_dropped },
                        result = &mut send => result,
                    };
                    if let Err(error) = result {
                        return BatchResult::GetPeersClosed {
                            request: error.into_request(),
                            batch_dropped,
                        };
                    }
                    increment_saturating(&self.stats.inner.get_peers_queued);
                }
                Route::Scrape => {
                    let send = self.scrape.send(request);
                    tokio::pin!(send);
                    let result = tokio::select! {
                        biased;
                        () = shutdown.as_mut() => return BatchResult::Shutdown { batch_dropped },
                        result = &mut send => result,
                    };
                    if let Err(error) = result {
                        return BatchResult::ScrapeClosed {
                            request: error.into_request(),
                            batch_dropped,
                        };
                    }
                    increment_saturating(&self.stats.inner.scrape_queued);
                }
            }
        }
        BatchResult::Complete
    }

    fn finish_shutdown(&mut self, batch_dropped: usize) -> DhtInfoHashTriageWorkerExit {
        let queued_dropped = self.close_and_drain_input();
        add_saturating(
            &self.stats.inner.shutdown_queued_dropped,
            queued_dropped as u64,
        );
        add_saturating(
            &self.stats.inner.shutdown_batch_dropped,
            batch_dropped as u64,
        );
        DhtInfoHashTriageWorkerExit::Shutdown {
            queued_dropped,
            batch_dropped,
        }
    }

    fn close_and_drain_input(&mut self) -> usize {
        self.input.close();
        let mut drained = 0;
        while self.input.try_recv().is_ok() {
            drained += 1;
        }
        drained
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Route {
    GetPeers,
    Scrape,
}

enum BatchResult {
    Complete,
    Shutdown {
        batch_dropped: usize,
    },
    GetPeersClosed {
        request: DhtInfoHashTriageRequest,
        batch_dropped: usize,
    },
    ScrapeClosed {
        request: DhtInfoHashTriageRequest,
        batch_dropped: usize,
    },
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

fn duration_micros_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

fn duration_micros_i128(duration: Duration) -> i128 {
    i128::try_from(duration.as_micros()).expect("Duration microseconds fit i128")
}

fn system_time_unix_micros(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_micros_i64(duration),
        Err(error) => duration_micros_i64(error.duration()).saturating_neg(),
    }
}

#[cfg(test)]
#[path = "info_hash_triage_parity.rs"]
mod parity_tests;

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::pending;
    use std::io;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::sync::atomic::{AtomicI64, AtomicUsize};
    use std::sync::Mutex;

    use bitmagnet_dht::{
        dht_get_peers_channel, dht_info_hash_triage_channel, dht_scrape_channel,
        DHT_GET_PEERS_ROUTE_CAPACITY, DHT_SCRAPE_ROUTE_CAPACITY,
    };
    use tokio::sync::Notify;

    use super::*;

    enum Step<T> {
        Ok(T),
        Err(&'static str),
    }

    struct ScriptFilter {
        steps: Mutex<VecDeque<Step<Vec<Id20>>>>,
        calls: Mutex<Vec<Vec<Id20>>>,
    }

    impl ScriptFilter {
        fn new(steps: impl IntoIterator<Item = Step<Vec<Id20>>>) -> Arc<Self> {
            Arc::new(Self {
                steps: Mutex::new(steps.into_iter().collect()),
                calls: Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> Vec<Vec<Id20>> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DhtInfoHashBlockFilter for ScriptFilter {
        async fn filter(&self, info_hashes: &[Id20]) -> Result<Vec<Id20>, TriageCollaboratorError> {
            self.calls.lock().unwrap().push(info_hashes.to_vec());
            let step = self
                .steps
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted filter step");
            match step {
                Step::Ok(value) => Ok(value),
                Step::Err(message) => Err(Box::new(io::Error::other(message)) as _),
            }
        }
    }

    struct ScriptLookup {
        steps: Mutex<VecDeque<Step<Vec<DhtTorrentTriageRow>>>>,
        calls: Mutex<Vec<Vec<Id20>>>,
    }

    impl ScriptLookup {
        fn new(steps: impl IntoIterator<Item = Step<Vec<DhtTorrentTriageRow>>>) -> Arc<Self> {
            Arc::new(Self {
                steps: Mutex::new(steps.into_iter().collect()),
                calls: Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> Vec<Vec<Id20>> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DhtTorrentTriageLookup for ScriptLookup {
        async fn lookup(
            &self,
            info_hashes: &[Id20],
        ) -> Result<Vec<DhtTorrentTriageRow>, TriageCollaboratorError> {
            self.calls.lock().unwrap().push(info_hashes.to_vec());
            let step = self
                .steps
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted lookup step");
            match step {
                Step::Ok(value) => Ok(value),
                Step::Err(message) => Err(Box::new(io::Error::other(message)) as _),
            }
        }
    }

    struct FixedClock {
        now: AtomicI64,
        calls: AtomicUsize,
    }

    struct SignalingClock {
        now: i64,
        signal: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    struct BlockingFilter {
        entered: Notify,
    }

    #[async_trait]
    impl DhtInfoHashBlockFilter for BlockingFilter {
        async fn filter(
            &self,
            _info_hashes: &[Id20],
        ) -> Result<Vec<Id20>, TriageCollaboratorError> {
            self.entered.notify_one();
            pending().await
        }
    }

    struct BlockingLookup {
        entered: Notify,
    }

    #[async_trait]
    impl DhtTorrentTriageLookup for BlockingLookup {
        async fn lookup(
            &self,
            _info_hashes: &[Id20],
        ) -> Result<Vec<DhtTorrentTriageRow>, TriageCollaboratorError> {
            self.entered.notify_one();
            pending().await
        }
    }

    impl FixedClock {
        fn new(now: i64) -> Arc<Self> {
            Arc::new(Self {
                now: AtomicI64::new(now),
                calls: AtomicUsize::new(0),
            })
        }
    }

    impl DhtInfoHashTriageClock for FixedClock {
        fn now_unix_micros(&self) -> i64 {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.now.load(Ordering::Relaxed)
        }
    }

    impl DhtInfoHashTriageClock for SignalingClock {
        fn now_unix_micros(&self) -> i64 {
            if let Some(signal) = self.signal.lock().unwrap().take() {
                let _ = signal.send(());
            }
            self.now
        }
    }

    fn id(value: u8) -> Id20 {
        let mut bytes = [0_u8; 20];
        bytes[19] = value;
        Id20::from_slice(&bytes).unwrap()
    }

    fn request(hash: u8, node: u8) -> DhtInfoHashTriageRequest {
        DhtInfoHashTriageRequest {
            info_hash: id(hash),
            source_node_addr: SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(192, 0, 2, node),
                7_000 + u16::from(node),
            )),
        }
    }

    fn row(
        hash: u8,
        files_status: FilesStatus,
        files_count: Option<u64>,
        dht_seeders: Option<u64>,
        dht_leechers: Option<u64>,
        updated_at_unix_micros: i64,
    ) -> DhtTorrentTriageRow {
        DhtTorrentTriageRow {
            info_hash: id(hash),
            files_status,
            files_count,
            dht_seeders,
            dht_leechers,
            dht_updated_at_unix_micros: Some(updated_at_unix_micros),
        }
    }

    fn config(batch_limit: usize) -> DhtInfoHashTriageConfig {
        DhtInfoHashTriageConfig {
            batch_limit: NonZeroUsize::new(batch_limit).unwrap(),
            batch_interval: Duration::from_secs(60 * 60),
            save_files_threshold: 100,
            rescrape_threshold: Duration::from_micros(100),
        }
    }

    fn drain_get_peers(
        receiver: &mut bitmagnet_dht::DhtGetPeersReceiver,
    ) -> Vec<DhtInfoHashTriageRequest> {
        let mut output = Vec::new();
        while let Ok(request) = receiver.try_recv() {
            output.push(request);
        }
        output
    }

    fn drain_scrape(
        receiver: &mut bitmagnet_dht::DhtScrapeReceiver,
    ) -> Vec<DhtInfoHashTriageRequest> {
        let mut output = Vec::new();
        while let Ok(request) = receiver.try_recv() {
            output.push(request);
        }
        output
    }

    fn assert_dequeued_is_conserved(stats: DhtInfoHashTriageStats) {
        assert_eq!(
            stats.dequeued,
            stats.input_duplicates_dropped
                + stats.filter_suppressed
                + stats.filter_failure_dropped
                + stats.filter_contract_dropped
                + stats.lookup_failure_dropped
                + stats.get_peers_queued
                + stats.scrape_queued
                + stats.discarded
                + stats.shutdown_batch_dropped
                + stats.route_closed_batch_dropped,
            "every dequeued input occurrence must have exactly one terminal classification: {stats:?}"
        );
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

    #[test]
    fn defaults_constants_and_system_clock_are_sound() {
        let defaults = DhtInfoHashTriageConfig::default();
        assert_eq!(defaults.batch_limit.get(), 1_000);
        assert_eq!(defaults.batch_interval, Duration::from_secs(20));
        assert_eq!(defaults.save_files_threshold, 100);
        assert_eq!(defaults.rescrape_threshold, Duration::from_secs(2_592_000));
        assert!(SystemDhtInfoHashTriageClock.now_unix_micros() > 0);
        assert_eq!(system_time_unix_micros(UNIX_EPOCH), 0);
        assert_eq!(
            system_time_unix_micros(UNIX_EPOCH - Duration::from_micros(7)),
            -7
        );
    }

    #[tokio::test]
    async fn batch_limit_flushes_one_thousand_and_carries_the_next_item() {
        let filter = ScriptFilter::new([Step::Ok(Vec::new()), Step::Ok(Vec::new())]);
        let lookup = ScriptLookup::new([]);
        let clock = FixedClock::new(1_000);
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1_001).unwrap());
        let (get_peers, _get_peers_receiver) = dht_get_peers_channel();
        let (scrape, _scrape_receiver) = dht_scrape_channel();
        let (worker, stats) = DhtInfoHashTriageWorker::with_config(
            receiver,
            get_peers,
            scrape,
            filter.clone(),
            lookup,
            clock,
            DhtInfoHashTriageConfig::default(),
        );
        for value in 0..1_001_usize {
            input
                .send(request((value % 250) as u8, (value % 200 + 1) as u8))
                .await
                .unwrap();
        }
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtInfoHashTriageWorkerExit::InputClosed
        );
        let calls = filter.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].len(), 250);
        assert_eq!(calls[1], vec![id(0)]);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.dequeued, 1_001);
        assert_eq!(snapshot.batches, 2);
        assert_eq!(snapshot.input_duplicates_dropped, 750);
        assert_eq!(snapshot.filter_suppressed, 251);
        assert_dequeued_is_conserved(snapshot);
    }

    #[tokio::test(start_paused = true)]
    async fn batch_deadline_is_first_item_relative_and_restarts_for_the_next_batch() {
        let filter = ScriptFilter::new([Step::Ok(Vec::new()), Step::Ok(Vec::new())]);
        let lookup = ScriptLookup::new([]);
        let clock = FixedClock::new(1_000);
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(4).unwrap());
        let (get_peers, _get_peers_receiver) = dht_get_peers_channel();
        let (scrape, _scrape_receiver) = dht_scrape_channel();
        let mut timer_config = config(4);
        timer_config.batch_interval = Duration::from_secs(20);
        let (worker, stats) = DhtInfoHashTriageWorker::with_config(
            receiver,
            get_peers,
            scrape,
            filter,
            lookup,
            clock,
            timer_config,
        );
        let task = tokio::spawn(worker.run(pending()));

        input.send(request(1, 1)).await.unwrap();
        yield_until(|| stats.snapshot().dequeued == 1).await;
        tokio::time::advance(Duration::from_secs(19)).await;
        tokio::task::yield_now().await;
        assert_eq!(stats.snapshot().batches, 0);
        tokio::time::advance(Duration::from_secs(1)).await;
        yield_until(|| stats.snapshot().batches == 1).await;

        input.send(request(2, 2)).await.unwrap();
        yield_until(|| stats.snapshot().dequeued == 2).await;
        tokio::time::advance(Duration::from_secs(19)).await;
        tokio::task::yield_now().await;
        assert_eq!(stats.snapshot().batches, 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        yield_until(|| stats.snapshot().batches == 2).await;
        drop(input);

        assert_eq!(
            task.await.unwrap(),
            DhtInfoHashTriageWorkerExit::InputClosed
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.filter_suppressed, 2);
        assert_dequeued_is_conserved(snapshot);
    }

    #[tokio::test(start_paused = true)]
    async fn eof_is_immediate_for_empty_input_and_flushes_a_partial_batch() {
        let (empty_input, empty_receiver) =
            dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        let (get_peers, _get_peers_receiver) = dht_get_peers_channel();
        let (scrape, _scrape_receiver) = dht_scrape_channel();
        let (empty_worker, empty_stats) = DhtInfoHashTriageWorker::with_config(
            empty_receiver,
            get_peers,
            scrape,
            ScriptFilter::new([]),
            ScriptLookup::new([]),
            FixedClock::new(1_000),
            config(3),
        );
        drop(empty_input);
        assert_eq!(
            empty_worker.run(pending()).await,
            DhtInfoHashTriageWorkerExit::InputClosed
        );
        assert_eq!(empty_stats.snapshot(), DhtInfoHashTriageStats::default());

        let filter = ScriptFilter::new([Step::Ok(Vec::new())]);
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(2).unwrap());
        let (get_peers, _get_peers_receiver) = dht_get_peers_channel();
        let (scrape, _scrape_receiver) = dht_scrape_channel();
        let (worker, stats) = DhtInfoHashTriageWorker::with_config(
            receiver,
            get_peers,
            scrape,
            filter.clone(),
            ScriptLookup::new([]),
            FixedClock::new(1_000),
            config(3),
        );
        input.send(request(3, 3)).await.unwrap();
        input.send(request(4, 4)).await.unwrap();
        drop(input);
        assert_eq!(
            worker.run(pending()).await,
            DhtInfoHashTriageWorkerExit::InputClosed
        );
        assert_eq!(filter.calls(), vec![vec![id(3), id(4)]]);
        assert_eq!(stats.snapshot().filter_suppressed, 2);
        assert_dequeued_is_conserved(stats.snapshot());
    }

    #[tokio::test]
    async fn shutdown_while_collecting_accounts_local_batch_and_queued_suffix() {
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(4).unwrap());
        let (get_peers, _get_peers_receiver) = dht_get_peers_channel();
        let (scrape, _scrape_receiver) = dht_scrape_channel();
        let (worker, stats) = DhtInfoHashTriageWorker::with_config(
            receiver,
            get_peers,
            scrape,
            ScriptFilter::new([]),
            ScriptLookup::new([]),
            FixedClock::new(1_000),
            config(5),
        );
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(worker.run(async move {
            let _ = shutdown_receiver.await;
        }));
        input.send(request(5, 5)).await.unwrap();
        yield_until(|| stats.snapshot().dequeued == 1).await;
        input.send(request(6, 6)).await.unwrap();
        input.send(request(7, 7)).await.unwrap();
        input.send(request(8, 8)).await.unwrap();
        shutdown_sender.send(()).unwrap();

        assert_eq!(
            task.await.unwrap(),
            DhtInfoHashTriageWorkerExit::Shutdown {
                queued_dropped: 3,
                batch_dropped: 1,
            }
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.shutdown_queued_dropped, 3);
        assert_eq!(snapshot.shutdown_batch_dropped, 1);
        assert_dequeued_is_conserved(snapshot);
        assert!(input.send(request(9, 9)).await.is_err());
    }

    #[tokio::test]
    async fn first_duplicate_filter_lookup_and_lazy_decision_matrix_are_exact() {
        let requests = [
            request(1, 1),
            request(1, 99),
            request(2, 2),
            request(3, 3),
            request(4, 4),
            request(5, 5),
            request(6, 6),
        ];
        let filtered = vec![id(1), id(2), id(3), id(4), id(5), id(6)];
        let filter = ScriptFilter::new([Step::Ok(filtered.clone())]);
        let lookup = ScriptLookup::new([Step::Ok(vec![
            row(2, FilesStatus::NoInfo, None, None, None, 1_000),
            row(3, FilesStatus::Single, None, None, None, 1_000),
            row(4, FilesStatus::Multi, Some(1), Some(4), Some(5), 0),
            row(5, FilesStatus::Single, None, Some(6), Some(7), 1_000_000),
            row(
                6,
                FilesStatus::OverThreshold,
                Some(100),
                Some(8),
                Some(9),
                1_000_000,
            ),
        ])]);
        let clock = FixedClock::new(1_000_000);
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(16).unwrap());
        let (get_peers, mut get_peers_receiver) = dht_get_peers_channel();
        let (scrape, mut scrape_receiver) = dht_scrape_channel();
        let (worker, stats) = DhtInfoHashTriageWorker::with_config(
            receiver,
            get_peers,
            scrape,
            filter.clone(),
            lookup.clone(),
            clock.clone(),
            config(requests.len()),
        );
        for request in requests {
            input.send(request).await.unwrap();
        }
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtInfoHashTriageWorkerExit::InputClosed
        );
        assert_eq!(
            filter.calls(),
            vec![vec![id(1), id(2), id(3), id(4), id(5), id(6)]]
        );
        assert_eq!(lookup.calls(), vec![filtered]);
        assert_eq!(
            drain_get_peers(&mut get_peers_receiver),
            vec![request(1, 1), request(2, 2), request(6, 6)]
        );
        assert_eq!(
            drain_scrape(&mut scrape_receiver),
            vec![request(3, 3), request(4, 4)]
        );
        assert_eq!(clock.calls.load(Ordering::Relaxed), 2);
        let snapshot = stats.snapshot();
        assert_eq!(
            snapshot,
            DhtInfoHashTriageStats {
                dequeued: 7,
                batches: 1,
                input_duplicates_dropped: 1,
                filter_calls: 1,
                filter_hashes_returned: 6,
                lookup_calls: 1,
                get_peers_queued: 3,
                scrape_queued: 2,
                discarded: 1,
                ..DhtInfoHashTriageStats::default()
            }
        );
        assert_dequeued_is_conserved(snapshot);
    }

    #[tokio::test]
    async fn collaborator_errors_drop_only_the_batch_and_continue_to_eof() {
        let filter = ScriptFilter::new([
            Step::Err("filter"),
            Step::Ok(vec![id(11)]),
            Step::Ok(Vec::new()),
        ]);
        let lookup = ScriptLookup::new([Step::Err("lookup")]);
        let clock = FixedClock::new(1_000);
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(3).unwrap());
        let (get_peers, mut get_peers_receiver) = dht_get_peers_channel();
        let (scrape, mut scrape_receiver) = dht_scrape_channel();
        let (worker, stats) = DhtInfoHashTriageWorker::with_config(
            receiver,
            get_peers,
            scrape,
            filter.clone(),
            lookup.clone(),
            clock,
            config(1),
        );
        for value in 10..=12 {
            input.send(request(value, value)).await.unwrap();
        }
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtInfoHashTriageWorkerExit::InputClosed
        );
        assert_eq!(
            filter.calls(),
            vec![vec![id(10)], vec![id(11)], vec![id(12)]]
        );
        assert_eq!(lookup.calls(), vec![vec![id(11)]]);
        assert!(drain_get_peers(&mut get_peers_receiver).is_empty());
        assert!(drain_scrape(&mut scrape_receiver).is_empty());
        let snapshot = stats.snapshot();
        assert_eq!(
            snapshot,
            DhtInfoHashTriageStats {
                dequeued: 3,
                batches: 3,
                filter_calls: 3,
                filter_failures: 1,
                filter_hashes_returned: 1,
                filter_suppressed: 1,
                filter_failure_dropped: 1,
                lookup_calls: 1,
                lookup_failures: 1,
                lookup_failure_dropped: 1,
                ..DhtInfoHashTriageStats::default()
            }
        );
        assert_dequeued_is_conserved(snapshot);
    }

    #[tokio::test]
    async fn duplicate_filter_and_lookup_rows_route_once_last_row_wins_and_stale_is_strict() {
        let filter = ScriptFilter::new([Step::Ok(vec![id(30), id(30), id(31)])]);
        let lookup = ScriptLookup::new([Step::Ok(vec![
            row(30, FilesStatus::NoInfo, None, None, None, 0),
            row(30, FilesStatus::Single, None, Some(1), Some(2), 900),
            row(31, FilesStatus::Single, None, Some(3), Some(4), 899),
        ])]);
        let clock = FixedClock::new(1_000);
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(2).unwrap());
        let (get_peers, mut get_peers_receiver) = dht_get_peers_channel();
        let (scrape, mut scrape_receiver) = dht_scrape_channel();
        let (worker, stats) = DhtInfoHashTriageWorker::with_config(
            receiver,
            get_peers,
            scrape,
            filter,
            lookup.clone(),
            clock.clone(),
            config(2),
        );
        input.send(request(30, 30)).await.unwrap();
        input.send(request(31, 31)).await.unwrap();
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtInfoHashTriageWorkerExit::InputClosed
        );
        assert_eq!(lookup.calls(), vec![vec![id(30), id(30), id(31)]]);
        assert!(drain_get_peers(&mut get_peers_receiver).is_empty());
        assert_eq!(drain_scrape(&mut scrape_receiver), vec![request(31, 31)]);
        assert_eq!(clock.calls.load(Ordering::Relaxed), 2);
        assert_eq!(stats.snapshot().discarded, 1);
        assert_eq!(stats.snapshot().filter_hashes_returned, 3);
        assert_dequeued_is_conserved(stats.snapshot());
    }

    #[tokio::test]
    async fn foreign_filter_output_fails_batch_closed_before_lookup_or_route() {
        let filter = ScriptFilter::new([Step::Ok(vec![id(40), id(41)])]);
        let lookup = ScriptLookup::new([]);
        let clock = FixedClock::new(1_000);
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        let (get_peers, mut get_peers_receiver) = dht_get_peers_channel();
        let (scrape, mut scrape_receiver) = dht_scrape_channel();
        let (worker, stats) = DhtInfoHashTriageWorker::with_config(
            receiver,
            get_peers,
            scrape,
            filter,
            lookup.clone(),
            clock,
            config(1),
        );
        input.send(request(40, 40)).await.unwrap();
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtInfoHashTriageWorkerExit::InputClosed
        );
        assert!(lookup.calls().is_empty());
        assert!(drain_get_peers(&mut get_peers_receiver).is_empty());
        assert!(drain_scrape(&mut scrape_receiver).is_empty());
        assert_eq!(stats.snapshot().unknown_filtered_hashes_dropped, 1);
        assert_eq!(stats.snapshot().filter_contract_dropped, 1);
        assert_eq!(stats.snapshot().lookup_calls, 0);
        assert_dequeued_is_conserved(stats.snapshot());
    }

    #[tokio::test]
    async fn shutdown_cancels_a_blocked_filter_dependency_and_accounts_batch() {
        let filter = Arc::new(BlockingFilter {
            entered: Notify::new(),
        });
        let lookup = ScriptLookup::new([]);
        let clock = FixedClock::new(1_000);
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(3).unwrap());
        let (get_peers, _get_peers_receiver) = dht_get_peers_channel();
        let (scrape, _scrape_receiver) = dht_scrape_channel();
        let (worker, stats) = DhtInfoHashTriageWorker::with_config(
            receiver,
            get_peers,
            scrape,
            filter.clone(),
            lookup.clone(),
            clock,
            config(3),
        );
        input.send(request(50, 50)).await.unwrap();
        input.send(request(50, 150)).await.unwrap();
        input.send(request(51, 51)).await.unwrap();
        drop(input);
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(worker.run(async move {
            let _ = shutdown_receiver.await;
        }));
        filter.entered.notified().await;
        shutdown_sender.send(()).unwrap();

        assert_eq!(
            task.await.unwrap(),
            DhtInfoHashTriageWorkerExit::Shutdown {
                queued_dropped: 0,
                batch_dropped: 2,
            }
        );
        assert!(lookup.calls().is_empty());
        assert_eq!(stats.snapshot().filter_calls, 1);
        assert_eq!(stats.snapshot().input_duplicates_dropped, 1);
        assert_eq!(stats.snapshot().shutdown_batch_dropped, 2);
        assert_dequeued_is_conserved(stats.snapshot());
    }

    #[tokio::test]
    async fn shutdown_cancels_a_blocked_lookup_dependency_and_accounts_batch() {
        let filter = ScriptFilter::new([Step::Ok(vec![id(51)])]);
        let lookup = Arc::new(BlockingLookup {
            entered: Notify::new(),
        });
        let clock = FixedClock::new(1_000);
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(3).unwrap());
        let (get_peers, _get_peers_receiver) = dht_get_peers_channel();
        let (scrape, _scrape_receiver) = dht_scrape_channel();
        let (worker, stats) = DhtInfoHashTriageWorker::with_config(
            receiver,
            get_peers,
            scrape,
            filter,
            lookup.clone(),
            clock,
            config(3),
        );
        input.send(request(51, 51)).await.unwrap();
        input.send(request(51, 151)).await.unwrap();
        input.send(request(52, 52)).await.unwrap();
        drop(input);
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(worker.run(async move {
            let _ = shutdown_receiver.await;
        }));
        lookup.entered.notified().await;
        shutdown_sender.send(()).unwrap();

        assert_eq!(
            task.await.unwrap(),
            DhtInfoHashTriageWorkerExit::Shutdown {
                queued_dropped: 0,
                batch_dropped: 1,
            }
        );
        assert_eq!(stats.snapshot().lookup_calls, 1);
        assert_eq!(stats.snapshot().input_duplicates_dropped, 1);
        assert_eq!(stats.snapshot().filter_suppressed, 1);
        assert_eq!(stats.snapshot().shutdown_batch_dropped, 1);
        assert_dequeued_is_conserved(stats.snapshot());
    }

    #[tokio::test]
    async fn closed_output_routes_return_exact_unsent_request_and_suffix() {
        for scrape_case in [false, true] {
            let target = request(20, 20);
            let filter = ScriptFilter::new([Step::Ok(vec![id(20), id(21), id(22)])]);
            let rows = if scrape_case {
                vec![
                    row(20, FilesStatus::Single, None, None, None, 1_000),
                    row(21, FilesStatus::Single, None, None, None, 1_000),
                    row(22, FilesStatus::Single, None, None, None, 1_000),
                ]
            } else {
                Vec::new()
            };
            let lookup = ScriptLookup::new([Step::Ok(rows)]);
            let clock = FixedClock::new(1_000);
            let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(3).unwrap());
            let (get_peers, get_peers_receiver) = dht_get_peers_channel();
            let (scrape, scrape_receiver) = dht_scrape_channel();
            if scrape_case {
                drop(scrape_receiver);
            } else {
                drop(get_peers_receiver);
            }
            let (worker, stats) = DhtInfoHashTriageWorker::with_config(
                receiver,
                get_peers,
                scrape,
                filter,
                lookup,
                clock,
                config(3),
            );
            input.send(target).await.unwrap();
            input.send(request(21, 21)).await.unwrap();
            input.send(request(22, 22)).await.unwrap();
            drop(input);

            let exit = worker.run(pending()).await;
            let (request, queued_dropped, batch_dropped) = match exit {
                DhtInfoHashTriageWorkerExit::GetPeersClosed {
                    request,
                    queued_dropped,
                    batch_dropped,
                }
                | DhtInfoHashTriageWorkerExit::ScrapeClosed {
                    request,
                    queued_dropped,
                    batch_dropped,
                } => (request, queued_dropped, batch_dropped),
                other => panic!("unexpected exit: {other:?}"),
            };
            assert_eq!(request, target);
            assert_eq!((queued_dropped, batch_dropped), (0, 3));
            assert_eq!(stats.snapshot().route_closures, 1);
            assert_eq!(stats.snapshot().route_closed_batch_dropped, 3);
            assert_dequeued_is_conserved(stats.snapshot());
        }
    }

    #[tokio::test]
    async fn shutdown_at_observed_blocked_get_peers_send_commits_nothing() {
        let (get_peers, mut get_peers_receiver) = dht_get_peers_channel();
        for value in 0..DHT_GET_PEERS_ROUTE_CAPACITY {
            get_peers
                .send(request(value as u8, (value % 200 + 1) as u8))
                .await
                .unwrap();
        }
        let target = request(25, 225);
        let filter = ScriptFilter::new([Step::Ok(vec![target.info_hash])]);
        let lookup = ScriptLookup::new([Step::Ok(Vec::new())]);
        let clock = FixedClock::new(1_000);
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        let (scrape, _scrape_receiver) = dht_scrape_channel();
        let (worker, stats) = DhtInfoHashTriageWorker::with_config(
            receiver,
            get_peers,
            scrape,
            filter,
            lookup,
            clock,
            config(1),
        );
        input.send(target).await.unwrap();
        drop(input);
        let observed = Arc::new(Notify::new());
        let hook_observed = observed.clone();
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(worker.run_with(
            async move {
                let _ = shutdown_receiver.await;
            },
            move |call, route, request| {
                assert_eq!(call, 1);
                assert_eq!(route, Route::GetPeers);
                assert_eq!(*request, target);
                hook_observed.notify_one();
            },
        ));
        observed.notified().await;
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "full get-peers route must block the send"
        );
        shutdown_sender.send(()).unwrap();

        assert_eq!(
            task.await.unwrap(),
            DhtInfoHashTriageWorkerExit::Shutdown {
                queued_dropped: 0,
                batch_dropped: 1,
            }
        );
        assert_eq!(drain_get_peers(&mut get_peers_receiver).len(), 100);
        assert_eq!(stats.snapshot().get_peers_queued, 0);
        assert_eq!(stats.snapshot().shutdown_batch_dropped, 1);
        assert_dequeued_is_conserved(stats.snapshot());
    }

    #[tokio::test]
    async fn committed_prefix_blocked_get_peers_and_untouched_suffix_are_exact() {
        let (get_peers, mut get_peers_receiver) = dht_get_peers_channel();
        for value in 0..(DHT_GET_PEERS_ROUTE_CAPACITY - 1) {
            get_peers
                .send(request(value as u8, (value % 200 + 1) as u8))
                .await
                .unwrap();
        }
        let blocked = request(126, 126);
        let filter = ScriptFilter::new([Step::Ok(vec![id(125), id(126), id(127)])]);
        let lookup = ScriptLookup::new([Step::Ok(vec![row(
            127,
            FilesStatus::Single,
            None,
            Some(1),
            Some(2),
            0,
        )])]);
        let clock = FixedClock::new(1_000);
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(3).unwrap());
        let (scrape, _scrape_receiver) = dht_scrape_channel();
        let (worker, stats) = DhtInfoHashTriageWorker::with_config(
            receiver,
            get_peers,
            scrape,
            filter,
            lookup,
            clock.clone(),
            config(3),
        );
        input.send(request(125, 125)).await.unwrap();
        input.send(blocked).await.unwrap();
        input.send(request(127, 127)).await.unwrap();
        drop(input);
        let observed = Arc::new(Notify::new());
        let hook_observed = observed.clone();
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(worker.run_with(
            async move {
                let _ = shutdown_receiver.await;
            },
            move |call, route, request| {
                if call == 2 {
                    assert_eq!(route, Route::GetPeers);
                    assert_eq!(*request, blocked);
                    hook_observed.notify_one();
                }
            },
        ));
        observed.notified().await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished(), "the second get-peers send must block");
        shutdown_sender.send(()).unwrap();

        assert_eq!(
            task.await.unwrap(),
            DhtInfoHashTriageWorkerExit::Shutdown {
                queued_dropped: 0,
                batch_dropped: 2,
            }
        );
        assert_eq!(drain_get_peers(&mut get_peers_receiver).len(), 100);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.get_peers_queued, 1);
        assert_eq!(snapshot.shutdown_batch_dropped, 2);
        assert_eq!(clock.calls.load(Ordering::Relaxed), 0);
        assert_dequeued_is_conserved(snapshot);
    }

    #[tokio::test]
    async fn shutdown_at_observed_blocked_scrape_send_commits_nothing() {
        let (scrape, mut scrape_receiver) = dht_scrape_channel();
        for value in 0..DHT_SCRAPE_ROUTE_CAPACITY {
            scrape
                .send(request(value as u8, (value % 200 + 1) as u8))
                .await
                .unwrap();
        }
        let target = request(225, 225);
        let filter = ScriptFilter::new([Step::Ok(vec![target.info_hash])]);
        let lookup = ScriptLookup::new([Step::Ok(vec![row(
            225,
            FilesStatus::Single,
            None,
            None,
            None,
            1_000,
        )])]);
        let clock = FixedClock::new(1_000);
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        let (get_peers, _get_peers_receiver) = dht_get_peers_channel();
        let (worker, stats) = DhtInfoHashTriageWorker::with_config(
            receiver,
            get_peers,
            scrape,
            filter,
            lookup,
            clock,
            config(1),
        );
        input.send(target).await.unwrap();
        drop(input);
        let observed = Arc::new(Notify::new());
        let hook_observed = observed.clone();
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(worker.run_with(
            async move {
                let _ = shutdown_receiver.await;
            },
            move |call, route, request| {
                assert_eq!(call, 1);
                assert_eq!(route, Route::Scrape);
                assert_eq!(*request, target);
                hook_observed.notify_one();
            },
        ));
        observed.notified().await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished(), "full scrape route must block the send");
        shutdown_sender.send(()).unwrap();

        assert_eq!(
            task.await.unwrap(),
            DhtInfoHashTriageWorkerExit::Shutdown {
                queued_dropped: 0,
                batch_dropped: 1,
            }
        );
        assert_eq!(drain_scrape(&mut scrape_receiver).len(), 100);
        assert_eq!(stats.snapshot().scrape_queued, 0);
        assert_eq!(stats.snapshot().shutdown_batch_dropped, 1);
        assert_dequeued_is_conserved(stats.snapshot());
    }

    #[tokio::test]
    async fn ready_shutdown_wins_over_final_eof_after_a_clock_driven_discard() {
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let clock = Arc::new(SignalingClock {
            now: 1_000,
            signal: Mutex::new(Some(shutdown_sender)),
        });
        let filter = ScriptFilter::new([Step::Ok(vec![id(240)])]);
        let lookup = ScriptLookup::new([Step::Ok(vec![row(
            240,
            FilesStatus::Single,
            None,
            Some(1),
            Some(2),
            900,
        )])]);
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        let (get_peers, _get_peers_receiver) = dht_get_peers_channel();
        let (scrape, _scrape_receiver) = dht_scrape_channel();
        let (worker, stats) = DhtInfoHashTriageWorker::with_config(
            receiver,
            get_peers,
            scrape,
            filter,
            lookup,
            clock,
            config(1),
        );
        input.send(request(240, 240)).await.unwrap();
        drop(input);

        assert_eq!(
            worker
                .run(async move {
                    let _ = shutdown_receiver.await;
                })
                .await,
            DhtInfoHashTriageWorkerExit::Shutdown {
                queued_dropped: 0,
                batch_dropped: 0,
            }
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.discarded, 1);
        assert_dequeued_is_conserved(snapshot);
    }
}
