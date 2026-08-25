//! Owned execution of the crawler's peer metainfo-request stage.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::panic::resume_unwind;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bitmagnet_dht::Id20;
use bitmagnet_metainfo::{check_default_banning, Info, ParsedInfo};
use tokio::task::{JoinError, JoinSet};

use crate::{
    DhtMetaInfoRequest, DhtPersistTorrentInput, DhtPersistTorrentRequest,
    DhtRequestMetaInfoReceiver,
};

const DEFAULT_MAX_INFLIGHT: NonZeroUsize = NonZeroUsize::new(400).unwrap();

/// Error returned by an injected metainfo-request collaborator.
pub type RequestMetaInfoCollaboratorError = Box<dyn Error + Send + Sync + 'static>;

/// Peer-wire metainfo requester used by the owned worker.
#[async_trait]
pub trait DhtMetaInfoRequester: Send + Sync {
    /// Request and verify the info dictionary supplied by one peer.
    async fn request(
        &self,
        info_hash: Id20,
        peer: SocketAddr,
    ) -> Result<ParsedInfo, RequestMetaInfoCollaboratorError>;
}

/// Side-effect-free policy check applied to verified metainfo.
pub trait DhtMetaInfoBanningChecker: Send + Sync {
    /// Return `Err` when the metainfo must be banned.
    fn check(&self, info: &Info) -> Result<(), RequestMetaInfoCollaboratorError>;
}

/// Blocking side effect applied after the banning policy rejects metainfo.
#[async_trait]
pub trait DhtInfoHashBlocker: Send + Sync {
    async fn block(
        &self,
        info_hashes: &[Id20],
        flush: bool,
    ) -> Result<(), RequestMetaInfoCollaboratorError>;
}

/// Adapter for Bitmagnet's default, side-effect-free metainfo policy.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultDhtMetaInfoBanningChecker;

impl DhtMetaInfoBanningChecker for DefaultDhtMetaInfoBanningChecker {
    fn check(&self, info: &Info) -> Result<(), RequestMetaInfoCollaboratorError> {
        check_default_banning(info)
            .map_err(|error| Box::new(error) as RequestMetaInfoCollaboratorError)
    }
}

/// Ordered peer-request failures for one task.
///
/// The worker deliberately drops this value after classifying the task, while
/// sibling parity tests can inspect the original error objects and Go's
/// newline-joined presentation without reducing them to strings.
#[derive(Debug)]
pub(super) struct DhtMetaInfoRequestFailures {
    errors: Vec<RequestMetaInfoCollaboratorError>,
}

impl DhtMetaInfoRequestFailures {
    fn new(errors: Vec<RequestMetaInfoCollaboratorError>) -> Self {
        Self { errors }
    }

    pub(super) fn errors(&self) -> &[RequestMetaInfoCollaboratorError] {
        &self.errors
    }
}

impl fmt::Display for DhtMetaInfoRequestFailures {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, error) in self.errors.iter().enumerate() {
            if index != 0 {
                formatter.write_str("\n")?;
            }
            fmt::Display::fmt(error, formatter)?;
        }
        Ok(())
    }
}

impl Error for DhtMetaInfoRequestFailures {}

/// Concurrency bound for accepted metainfo-request work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtRequestMetaInfoWorkerConfig {
    pub max_inflight: NonZeroUsize,
}

impl Default for DhtRequestMetaInfoWorkerConfig {
    fn default() -> Self {
        Self {
            max_inflight: DEFAULT_MAX_INFLIGHT,
        }
    }
}

/// Terminal state of the owned metainfo-request worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtRequestMetaInfoWorkerExit {
    /// Every input producer is gone and every accepted task completed.
    InputClosed,
    /// Caller shutdown won before another completion or route receive.
    Shutdown {
        queued_dropped: usize,
        tasks_cancelled: usize,
        peer_occurrences_dropped: usize,
        request_attempts_cancelled: usize,
        block_calls_cancelled: usize,
        persist_requests_dropped: usize,
    },
}

#[derive(Default)]
struct DhtRequestMetaInfoWorkerStatsInner {
    dequeued: AtomicU64,
    tasks_completed: AtomicU64,
    peer_occurrences: AtomicU64,
    request_attempts_started: AtomicU64,
    request_attempts_failed: AtomicU64,
    request_attempts_succeeded: AtomicU64,
    peer_occurrences_skipped: AtomicU64,
    empty_peers_dropped: AtomicU64,
    all_peers_failed: AtomicU64,
    allowed: AtomicU64,
    banned: AtomicU64,
    block_calls_started: AtomicU64,
    block_succeeded: AtomicU64,
    block_failed_ignored: AtomicU64,
    persist_queued: AtomicU64,
    persist_closed_dropped: AtomicU64,
    shutdown_queued_dropped: AtomicU64,
    shutdown_tasks_cancelled: AtomicU64,
    shutdown_peer_occurrences_dropped: AtomicU64,
    shutdown_request_attempts_cancelled: AtomicU64,
    shutdown_block_calls_cancelled: AtomicU64,
    shutdown_persist_requests_dropped: AtomicU64,
}

/// Cloneable, sender-free view of metainfo-request worker counters.
#[derive(Clone, Default)]
pub struct DhtRequestMetaInfoWorkerStatsHandle {
    inner: Arc<DhtRequestMetaInfoWorkerStatsInner>,
}

/// One independently read snapshot of saturating metainfo-request counters.
///
/// After normal EOF:
/// `dequeued = tasks_completed`,
/// `peer_occurrences = request_attempts_failed +
/// request_attempts_succeeded + peer_occurrences_skipped`,
/// `request_attempts_started = request_attempts_failed + request_attempts_succeeded`,
/// `request_attempts_succeeded = allowed + banned`,
/// `tasks_completed = empty_peers_dropped + all_peers_failed + allowed + banned`,
/// `banned = block_calls_started = block_succeeded + block_failed_ignored`, and
/// `allowed = persist_queued + persist_closed_dropped`.
///
/// After shutdown, `dequeued = tasks_completed + shutdown_tasks_cancelled`,
/// `tasks_completed` retains its normal classification equation,
/// `peer_occurrences = request_attempts_failed +
/// request_attempts_succeeded + peer_occurrences_skipped +
/// shutdown_peer_occurrences_dropped`, and `request_attempts_started =
/// request_attempts_failed + request_attempts_succeeded +
/// shutdown_request_attempts_cancelled`.
/// A successful request is conserved by `allowed + banned +
/// shutdown_block_calls_cancelled + shutdown_persist_requests_dropped`;
/// `block_calls_started = banned + shutdown_block_calls_cancelled`, and
/// completed blocks and persistence retain their normal equations. Queued
/// drops were never dequeued and remain separate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtRequestMetaInfoWorkerStats {
    pub dequeued: u64,
    pub tasks_completed: u64,
    /// Ordered input peer occurrences, including duplicates.
    pub peer_occurrences: u64,
    pub request_attempts_started: u64,
    pub request_attempts_failed: u64,
    pub request_attempts_succeeded: u64,
    /// Ordered suffix occurrences skipped after the first success.
    pub peer_occurrences_skipped: u64,
    pub empty_peers_dropped: u64,
    pub all_peers_failed: u64,
    pub allowed: u64,
    pub banned: u64,
    pub block_calls_started: u64,
    pub block_succeeded: u64,
    pub block_failed_ignored: u64,
    pub persist_queued: u64,
    pub persist_closed_dropped: u64,
    pub shutdown_queued_dropped: u64,
    pub shutdown_tasks_cancelled: u64,
    /// Current and not-started peer occurrences abandoned by accepted tasks.
    pub shutdown_peer_occurrences_dropped: u64,
    pub shutdown_request_attempts_cancelled: u64,
    pub shutdown_block_calls_cancelled: u64,
    pub shutdown_persist_requests_dropped: u64,
}

impl DhtRequestMetaInfoWorkerStatsHandle {
    /// Read each counter independently. Conservation is terminal-only.
    #[must_use]
    pub fn snapshot(&self) -> DhtRequestMetaInfoWorkerStats {
        let inner = &self.inner;
        DhtRequestMetaInfoWorkerStats {
            dequeued: inner.dequeued.load(Ordering::Relaxed),
            tasks_completed: inner.tasks_completed.load(Ordering::Relaxed),
            peer_occurrences: inner.peer_occurrences.load(Ordering::Relaxed),
            request_attempts_started: inner.request_attempts_started.load(Ordering::Relaxed),
            request_attempts_failed: inner.request_attempts_failed.load(Ordering::Relaxed),
            request_attempts_succeeded: inner.request_attempts_succeeded.load(Ordering::Relaxed),
            peer_occurrences_skipped: inner.peer_occurrences_skipped.load(Ordering::Relaxed),
            empty_peers_dropped: inner.empty_peers_dropped.load(Ordering::Relaxed),
            all_peers_failed: inner.all_peers_failed.load(Ordering::Relaxed),
            allowed: inner.allowed.load(Ordering::Relaxed),
            banned: inner.banned.load(Ordering::Relaxed),
            block_calls_started: inner.block_calls_started.load(Ordering::Relaxed),
            block_succeeded: inner.block_succeeded.load(Ordering::Relaxed),
            block_failed_ignored: inner.block_failed_ignored.load(Ordering::Relaxed),
            persist_queued: inner.persist_queued.load(Ordering::Relaxed),
            persist_closed_dropped: inner.persist_closed_dropped.load(Ordering::Relaxed),
            shutdown_queued_dropped: inner.shutdown_queued_dropped.load(Ordering::Relaxed),
            shutdown_tasks_cancelled: inner.shutdown_tasks_cancelled.load(Ordering::Relaxed),
            shutdown_peer_occurrences_dropped: inner
                .shutdown_peer_occurrences_dropped
                .load(Ordering::Relaxed),
            shutdown_request_attempts_cancelled: inner
                .shutdown_request_attempts_cancelled
                .load(Ordering::Relaxed),
            shutdown_block_calls_cancelled: inner
                .shutdown_block_calls_cancelled
                .load(Ordering::Relaxed),
            shutdown_persist_requests_dropped: inner
                .shutdown_persist_requests_dropped
                .load(Ordering::Relaxed),
        }
    }
}

/// Owned, bounded consumer for peer metainfo requests.
///
/// Peer occurrences are tried sequentially in their original order, including
/// duplicates. Errors fall through; the first success skips the remaining
/// suffix. Empty inputs and all-failed tasks are dropped locally. A banning
/// result blocks the exact requested v1 hash with `flush = false`; that side
/// effect's error is counted and ignored. Allowed metainfo is handed off with
/// the original request identity. Output closure affects only that task.
#[must_use = "the worker must be run to consume metainfo-request work"]
pub struct DhtRequestMetaInfoWorker {
    input: DhtRequestMetaInfoReceiver,
    persist_torrent: DhtPersistTorrentInput,
    requester: Arc<dyn DhtMetaInfoRequester>,
    checker: Arc<dyn DhtMetaInfoBanningChecker>,
    blocker: Arc<dyn DhtInfoHashBlocker>,
    config: DhtRequestMetaInfoWorkerConfig,
    tasks: JoinSet<()>,
    stats: DhtRequestMetaInfoWorkerStatsHandle,
    shutdown_in_progress: Arc<AtomicBool>,
    abandoned_peers: Arc<AtomicUsize>,
    abandoned_requests: Arc<AtomicUsize>,
    abandoned_blocks: Arc<AtomicUsize>,
    abandoned_persists: Arc<AtomicUsize>,
}

impl DhtRequestMetaInfoWorker {
    pub fn new(
        input: DhtRequestMetaInfoReceiver,
        persist_torrent: DhtPersistTorrentInput,
        requester: Arc<dyn DhtMetaInfoRequester>,
        checker: Arc<dyn DhtMetaInfoBanningChecker>,
        blocker: Arc<dyn DhtInfoHashBlocker>,
    ) -> (Self, DhtRequestMetaInfoWorkerStatsHandle) {
        Self::with_config(
            input,
            persist_torrent,
            requester,
            checker,
            blocker,
            DhtRequestMetaInfoWorkerConfig::default(),
        )
    }

    pub fn with_config(
        input: DhtRequestMetaInfoReceiver,
        persist_torrent: DhtPersistTorrentInput,
        requester: Arc<dyn DhtMetaInfoRequester>,
        checker: Arc<dyn DhtMetaInfoBanningChecker>,
        blocker: Arc<dyn DhtInfoHashBlocker>,
        config: DhtRequestMetaInfoWorkerConfig,
    ) -> (Self, DhtRequestMetaInfoWorkerStatsHandle) {
        let stats = DhtRequestMetaInfoWorkerStatsHandle::default();
        (
            Self {
                input,
                persist_torrent,
                requester,
                checker,
                blocker,
                config,
                tasks: JoinSet::new(),
                stats: stats.clone(),
                shutdown_in_progress: Arc::new(AtomicBool::new(false)),
                abandoned_peers: Arc::new(AtomicUsize::new(0)),
                abandoned_requests: Arc::new(AtomicUsize::new(0)),
                abandoned_blocks: Arc::new(AtomicUsize::new(0)),
                abandoned_persists: Arc::new(AtomicUsize::new(0)),
            },
            stats,
        )
    }

    /// Run until route EOF or caller shutdown.
    ///
    /// The input is not polled at capacity. EOF joins all accepted tasks.
    /// Shutdown closes and drains input, aborts and joins every task, then
    /// resumes the first child panic if one was observed.
    pub async fn run<F>(mut self, shutdown: F) -> DhtRequestMetaInfoWorkerExit
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let mut input_closed = false;

        loop {
            if input_closed && self.tasks.is_empty() {
                return DhtRequestMetaInfoWorkerExit::InputClosed;
            }

            enum Event {
                Shutdown,
                Joined(Result<(), JoinError>),
                Input(Option<DhtMetaInfoRequest>),
            }

            let event = tokio::select! {
                biased;
                () = &mut shutdown => Event::Shutdown,
                joined = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    Event::Joined(joined.expect("guarded metainfo-request task remains present"))
                }
                request = self.input.recv(),
                    if !input_closed && self.tasks.len() < self.config.max_inflight.get() =>
                {
                    Event::Input(request)
                }
            };

            match event {
                Event::Shutdown => return self.finish_shutdown().await,
                Event::Joined(Ok(())) => {}
                Event::Joined(Err(error)) => self.finish_abnormal_join(error).await,
                Event::Input(Some(request)) => self.spawn_request(request),
                Event::Input(None) => input_closed = true,
            }
        }
    }

    fn spawn_request(&mut self, request: DhtMetaInfoRequest) {
        increment_saturating(&self.stats.inner.dequeued);
        add_saturating(&self.stats.inner.peer_occurrences, request.peers.len());
        let guard = TaskGuard {
            remaining_peers: request.peers.len(),
            request_inflight: false,
            block_inflight: false,
            persist_inflight: false,
            completed: false,
            stats: self.stats.clone(),
            shutdown_in_progress: Arc::clone(&self.shutdown_in_progress),
            abandoned_peers: Arc::clone(&self.abandoned_peers),
            abandoned_requests: Arc::clone(&self.abandoned_requests),
            abandoned_blocks: Arc::clone(&self.abandoned_blocks),
            abandoned_persists: Arc::clone(&self.abandoned_persists),
        };
        let requester = Arc::clone(&self.requester);
        let checker = Arc::clone(&self.checker);
        let blocker = Arc::clone(&self.blocker);
        let persist_torrent = self.persist_torrent.clone();
        self.tasks.spawn(finish_request_meta_info_work(
            request,
            persist_torrent,
            requester,
            checker,
            blocker,
            guard,
        ));
    }

    async fn finish_shutdown(&mut self) -> DhtRequestMetaInfoWorkerExit {
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
                Err(error) => panic!("unexpected metainfo-request task join error: {error}"),
            }
        }
        add_saturating(&self.stats.inner.shutdown_tasks_cancelled, tasks_cancelled);
        let peer_occurrences_dropped = self.abandoned_peers.load(Ordering::Relaxed);
        let request_attempts_cancelled = self.abandoned_requests.load(Ordering::Relaxed);
        let block_calls_cancelled = self.abandoned_blocks.load(Ordering::Relaxed);
        let persist_requests_dropped = self.abandoned_persists.load(Ordering::Relaxed);
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
        DhtRequestMetaInfoWorkerExit::Shutdown {
            queued_dropped,
            tasks_cancelled,
            peer_occurrences_dropped,
            request_attempts_cancelled,
            block_calls_cancelled,
            persist_requests_dropped,
        }
    }

    async fn finish_abnormal_join(&mut self, error: JoinError) -> ! {
        let panic_payload = error.is_panic().then(|| error.into_panic());
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
        if let Some(payload) = panic_payload {
            resume_unwind(payload);
        }
        panic!("metainfo-request task was cancelled outside worker cleanup")
    }
}

impl Drop for DhtRequestMetaInfoWorker {
    fn drop(&mut self) {
        self.input.close();
        self.tasks.abort_all();
    }
}

struct TaskGuard {
    remaining_peers: usize,
    request_inflight: bool,
    block_inflight: bool,
    persist_inflight: bool,
    completed: bool,
    stats: DhtRequestMetaInfoWorkerStatsHandle,
    shutdown_in_progress: Arc<AtomicBool>,
    abandoned_peers: Arc<AtomicUsize>,
    abandoned_requests: Arc<AtomicUsize>,
    abandoned_blocks: Arc<AtomicUsize>,
    abandoned_persists: Arc<AtomicUsize>,
}

pub(super) trait RequestMetaInfoAttemptObserver: Send {
    fn request_started(&mut self);

    fn request_failed(&mut self);

    fn request_succeeded(&mut self);
}

impl RequestMetaInfoAttemptObserver for TaskGuard {
    fn request_started(&mut self) {
        self.request_inflight = true;
        increment_saturating(&self.stats.inner.request_attempts_started);
    }

    fn request_failed(&mut self) {
        self.request_inflight = false;
        self.remaining_peers = self.remaining_peers.saturating_sub(1);
        increment_saturating(&self.stats.inner.request_attempts_failed);
    }

    fn request_succeeded(&mut self) {
        self.request_inflight = false;
        self.remaining_peers = self.remaining_peers.saturating_sub(1);
        increment_saturating(&self.stats.inner.request_attempts_succeeded);
        add_saturating(
            &self.stats.inner.peer_occurrences_skipped,
            self.remaining_peers,
        );
        self.remaining_peers = 0;
    }
}

impl TaskGuard {
    fn block_started(&mut self) {
        self.block_inflight = true;
        increment_saturating(&self.stats.inner.block_calls_started);
    }

    fn block_finished(&mut self, succeeded: bool) {
        self.block_inflight = false;
        if succeeded {
            increment_saturating(&self.stats.inner.block_succeeded);
        } else {
            increment_saturating(&self.stats.inner.block_failed_ignored);
        }
    }

    fn persist_started(&mut self) {
        self.persist_inflight = true;
    }

    fn persist_finished(&mut self, queued: bool) {
        self.persist_inflight = false;
        if queued {
            increment_saturating(&self.stats.inner.persist_queued);
        } else {
            increment_saturating(&self.stats.inner.persist_closed_dropped);
        }
    }

    fn complete(&mut self) {
        self.completed = true;
        increment_saturating(&self.stats.inner.tasks_completed);
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if self.completed || !self.shutdown_in_progress.load(Ordering::SeqCst) {
            return;
        }
        add_saturating(
            &self.stats.inner.shutdown_peer_occurrences_dropped,
            self.remaining_peers,
        );
        add_saturating_usize(&self.abandoned_peers, self.remaining_peers);
        if self.request_inflight {
            increment_saturating(&self.stats.inner.shutdown_request_attempts_cancelled);
            add_saturating_usize(&self.abandoned_requests, 1);
        }
        if self.block_inflight {
            increment_saturating(&self.stats.inner.shutdown_block_calls_cancelled);
            add_saturating_usize(&self.abandoned_blocks, 1);
        }
        if self.persist_inflight {
            increment_saturating(&self.stats.inner.shutdown_persist_requests_dropped);
            add_saturating_usize(&self.abandoned_persists, 1);
        }
    }
}

async fn finish_request_meta_info_work(
    request: DhtMetaInfoRequest,
    persist_torrent: DhtPersistTorrentInput,
    requester: Arc<dyn DhtMetaInfoRequester>,
    checker: Arc<dyn DhtMetaInfoBanningChecker>,
    blocker: Arc<dyn DhtInfoHashBlocker>,
    mut guard: TaskGuard,
) {
    if request.peers.is_empty() {
        increment_saturating(&guard.stats.inner.empty_peers_dropped);
        guard.complete();
        return;
    }

    let meta_info = match request_first_meta_info(
        requester.as_ref(),
        request.info_hash,
        &request.peers,
        &mut guard,
    )
    .await
    {
        Ok(meta_info) => meta_info,
        Err(failures) => {
            debug_assert!(!failures.errors().is_empty());
            increment_saturating(&guard.stats.inner.all_peers_failed);
            guard.complete();
            return;
        }
    };

    if checker.check(meta_info.info()).is_err() {
        guard.block_started();
        let result = blocker
            .block(std::slice::from_ref(&request.info_hash), false)
            .await;
        guard.block_finished(result.is_ok());
        increment_saturating(&guard.stats.inner.banned);
        guard.complete();
        return;
    }

    let persist_request = DhtPersistTorrentRequest {
        info_hash: request.info_hash,
        source_node_addr: request.source_node_addr,
        meta_info: Arc::new(meta_info),
    };
    guard.persist_started();
    let queued = persist_torrent.send(persist_request).await.is_ok();
    guard.persist_finished(queued);
    increment_saturating(&guard.stats.inner.allowed);
    guard.complete();
}

pub(super) async fn request_first_meta_info(
    requester: &dyn DhtMetaInfoRequester,
    info_hash: Id20,
    peers: &[SocketAddr],
    observer: &mut dyn RequestMetaInfoAttemptObserver,
) -> Result<ParsedInfo, DhtMetaInfoRequestFailures> {
    let mut failures = Vec::with_capacity(peers.len());
    for peer in peers.iter().copied() {
        observer.request_started();
        match requester.request(info_hash, peer).await {
            Ok(meta_info) => {
                observer.request_succeeded();
                return Ok(meta_info);
            }
            Err(error) => {
                observer.request_failed();
                failures.push(error);
            }
        }
    }
    Err(DhtMetaInfoRequestFailures::new(failures))
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
    use std::net::{IpAddr, Ipv4Addr};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize as TestAtomicUsize, Ordering as TestOrdering};
    use std::sync::Mutex;

    use tokio::sync::{oneshot, Semaphore};

    use super::*;
    use crate::{dht_persist_torrent_channel, dht_request_meta_info_channel};

    const VALID_HASH: [u8; 20] = [
        0x34, 0x5b, 0x04, 0xb6, 0x0b, 0x9a, 0xfe, 0xb8, 0xd1, 0xe1, 0x20, 0x9c, 0x19, 0xb0, 0xf6,
        0x25, 0xb3, 0xe7, 0xa8, 0xf8,
    ];
    const BANNED_HASH: [u8; 20] = [
        0xcc, 0x32, 0x11, 0x46, 0x04, 0x89, 0x26, 0xf7, 0xc0, 0x14, 0x01, 0x0c, 0x44, 0xdc, 0xab,
        0xce, 0x39, 0x10, 0x19, 0xcd,
    ];

    type RequestFuture = Pin<
        Box<
            dyn Future<Output = Result<ParsedInfo, RequestMetaInfoCollaboratorError>>
                + Send
                + 'static,
        >,
    >;
    type BlockFuture = Pin<
        Box<dyn Future<Output = Result<(), RequestMetaInfoCollaboratorError>> + Send + 'static>,
    >;
    type CheckerFn = dyn Fn(&Info) -> Result<(), RequestMetaInfoCollaboratorError> + Send + Sync;

    struct TestRequester {
        request: Arc<dyn Fn(Id20, SocketAddr) -> RequestFuture + Send + Sync>,
    }

    #[async_trait]
    impl DhtMetaInfoRequester for TestRequester {
        async fn request(
            &self,
            info_hash: Id20,
            peer: SocketAddr,
        ) -> Result<ParsedInfo, RequestMetaInfoCollaboratorError> {
            (self.request)(info_hash, peer).await
        }
    }

    struct TestChecker {
        check: Arc<CheckerFn>,
    }

    impl DhtMetaInfoBanningChecker for TestChecker {
        fn check(&self, info: &Info) -> Result<(), RequestMetaInfoCollaboratorError> {
            (self.check)(info)
        }
    }

    struct TestBlocker {
        block: Arc<dyn Fn(Vec<Id20>, bool) -> BlockFuture + Send + Sync>,
    }

    #[async_trait]
    impl DhtInfoHashBlocker for TestBlocker {
        async fn block(
            &self,
            info_hashes: &[Id20],
            flush: bool,
        ) -> Result<(), RequestMetaInfoCollaboratorError> {
            (self.block)(info_hashes.to_vec(), flush).await
        }
    }

    #[derive(Debug)]
    struct TestError(&'static str);

    impl fmt::Display for TestError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    impl Error for TestError {}

    fn boxed_error(message: &'static str) -> RequestMetaInfoCollaboratorError {
        Box::new(TestError(message))
    }

    fn id(value: u16) -> Id20 {
        let mut bytes = [0_u8; 20];
        bytes[18..].copy_from_slice(&value.to_be_bytes());
        Id20::from_slice(&bytes).unwrap()
    }

    fn addr(value: u16) -> SocketAddr {
        SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 0, (value >> 8) as u8, value as u8)),
            10_000 + value,
        )
    }

    fn valid_info() -> ParsedInfo {
        let mut raw =
            b"d6:lengthi4096e4:name20:synthetic-single.bin12:piece lengthi32768e6:pieces20:"
                .to_vec();
        raw.extend_from_slice(&[0; 20]);
        raw.push(b'e');
        bitmagnet_metainfo::parse_info_bytes(VALID_HASH, &raw).unwrap()
    }

    fn banned_info() -> ParsedInfo {
        let mut raw = b"d6:lengthi1e4:name1:x12:piece lengthi32768e6:pieces20:".to_vec();
        raw.extend_from_slice(&[0; 20]);
        raw.push(b'e');
        bitmagnet_metainfo::parse_info_bytes(BANNED_HASH, &raw).unwrap()
    }

    fn request(value: u16, peers: Vec<SocketAddr>) -> DhtMetaInfoRequest {
        DhtMetaInfoRequest {
            info_hash: id(value),
            source_node_addr: addr(value),
            peers,
        }
    }

    fn requester<F>(request: F) -> Arc<dyn DhtMetaInfoRequester>
    where
        F: Fn(Id20, SocketAddr) -> RequestFuture + Send + Sync + 'static,
    {
        Arc::new(TestRequester {
            request: Arc::new(request),
        })
    }

    fn checker<F>(check: F) -> Arc<dyn DhtMetaInfoBanningChecker>
    where
        F: Fn(&Info) -> Result<(), RequestMetaInfoCollaboratorError> + Send + Sync + 'static,
    {
        Arc::new(TestChecker {
            check: Arc::new(check),
        })
    }

    fn blocker<F>(block: F) -> Arc<dyn DhtInfoHashBlocker>
    where
        F: Fn(Vec<Id20>, bool) -> BlockFuture + Send + Sync + 'static,
    {
        Arc::new(TestBlocker {
            block: Arc::new(block),
        })
    }

    fn allowing_checker() -> Arc<dyn DhtMetaInfoBanningChecker> {
        checker(|_| Ok(()))
    }

    fn successful_blocker() -> Arc<dyn DhtInfoHashBlocker> {
        blocker(|_, _| Box::pin(ready(Ok(()))))
    }

    fn worker(
        max_inflight: usize,
        requester: Arc<dyn DhtMetaInfoRequester>,
        checker: Arc<dyn DhtMetaInfoBanningChecker>,
        blocker: Arc<dyn DhtInfoHashBlocker>,
    ) -> (
        crate::DhtRequestMetaInfoInput,
        DhtRequestMetaInfoWorker,
        DhtRequestMetaInfoWorkerStatsHandle,
        crate::DhtPersistTorrentReceiver,
        DhtPersistTorrentInput,
    ) {
        let (input, receiver) = dht_request_meta_info_channel();
        let (persist, persist_receiver) = dht_persist_torrent_channel();
        let (worker, stats) = DhtRequestMetaInfoWorker::with_config(
            receiver,
            persist.clone(),
            requester,
            checker,
            blocker,
            DhtRequestMetaInfoWorkerConfig {
                max_inflight: NonZeroUsize::new(max_inflight).unwrap(),
            },
        );
        (input, worker, stats, persist_receiver, persist)
    }

    async fn yield_until(mut condition: impl FnMut() -> bool) {
        for _ in 0..1_000 {
            if condition() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition did not become true");
    }

    fn assert_normal_conservation(stats: DhtRequestMetaInfoWorkerStats) {
        assert_eq!(stats.dequeued, stats.tasks_completed);
        assert_eq!(
            stats.peer_occurrences,
            stats.request_attempts_failed
                + stats.request_attempts_succeeded
                + stats.peer_occurrences_skipped
        );
        assert_eq!(
            stats.request_attempts_started,
            stats.request_attempts_failed + stats.request_attempts_succeeded
        );
        assert_eq!(
            stats.request_attempts_succeeded,
            stats.allowed + stats.banned
        );
        assert_eq!(
            stats.tasks_completed,
            stats.empty_peers_dropped + stats.all_peers_failed + stats.allowed + stats.banned
        );
        assert_eq!(stats.block_calls_started, stats.banned);
        assert_eq!(
            stats.banned,
            stats.block_succeeded + stats.block_failed_ignored
        );
        assert_eq!(
            stats.allowed,
            stats.persist_queued + stats.persist_closed_dropped
        );
    }

    fn assert_shutdown_conservation(stats: DhtRequestMetaInfoWorkerStats) {
        assert_eq!(
            stats.dequeued,
            stats.tasks_completed + stats.shutdown_tasks_cancelled
        );
        assert_eq!(
            stats.peer_occurrences,
            stats.request_attempts_failed
                + stats.request_attempts_succeeded
                + stats.peer_occurrences_skipped
                + stats.shutdown_peer_occurrences_dropped
        );
        assert_eq!(
            stats.request_attempts_started,
            stats.request_attempts_failed
                + stats.request_attempts_succeeded
                + stats.shutdown_request_attempts_cancelled
        );
        assert_eq!(
            stats.tasks_completed,
            stats.empty_peers_dropped + stats.all_peers_failed + stats.allowed + stats.banned
        );
        assert_eq!(
            stats.block_calls_started,
            stats.block_succeeded
                + stats.block_failed_ignored
                + stats.shutdown_block_calls_cancelled
        );
        assert_eq!(
            stats.request_attempts_succeeded,
            stats.allowed
                + stats.banned
                + stats.shutdown_block_calls_cancelled
                + stats.shutdown_persist_requests_dropped
        );
        assert_eq!(
            stats.banned,
            stats.block_succeeded + stats.block_failed_ignored
        );
        assert_eq!(
            stats.allowed,
            stats.persist_queued + stats.persist_closed_dropped
        );
    }

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn defaults_traits_default_checker_and_saturating_counters_are_exact() {
        assert_eq!(
            DhtRequestMetaInfoWorkerConfig::default().max_inflight.get(),
            400
        );
        assert_send_sync::<DhtRequestMetaInfoWorkerStatsHandle>();
        assert_send_sync::<Arc<dyn DhtMetaInfoRequester>>();
        assert_send_sync::<Arc<dyn DhtMetaInfoBanningChecker>>();
        assert_send_sync::<Arc<dyn DhtInfoHashBlocker>>();

        let default = DefaultDhtMetaInfoBanningChecker;
        assert!(default.check(valid_info().info()).is_ok());
        assert_eq!(
            default.check(banned_info().info()).unwrap_err().to_string(),
            "name too short\nsize too small"
        );

        let stats = DhtRequestMetaInfoWorkerStatsHandle::default();
        stats.inner.dequeued.store(u64::MAX, Ordering::Relaxed);
        increment_saturating(&stats.inner.dequeued);
        add_saturating(&stats.inner.dequeued, usize::MAX);
        assert_eq!(stats.snapshot().dequeued, u64::MAX);
    }

    #[tokio::test]
    async fn ordered_request_helper_preserves_calls_display_and_error_identity() {
        #[derive(Default)]
        struct Observer {
            started: usize,
            failed: usize,
            succeeded: usize,
        }
        impl RequestMetaInfoAttemptObserver for Observer {
            fn request_started(&mut self) {
                self.started += 1;
            }

            fn request_failed(&mut self) {
                self.failed += 1;
            }

            fn request_succeeded(&mut self) {
                self.succeeded += 1;
            }
        }

        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&calls);
        let requester = requester(move |hash, peer| {
            observed.lock().unwrap().push((hash, peer));
            Box::pin(ready(Err(boxed_error(match peer {
                peer if peer == addr(1) => "first",
                peer if peer == addr(2) => "second",
                _ => "third",
            }))))
        });
        let peers = vec![addr(1), addr(2), addr(3)];
        let mut observer = Observer::default();
        let failures = request_first_meta_info(requester.as_ref(), id(7), &peers, &mut observer)
            .await
            .unwrap_err();

        assert_eq!(
            *calls.lock().unwrap(),
            peers
                .iter()
                .copied()
                .map(|peer| (id(7), peer))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            (observer.started, observer.failed, observer.succeeded),
            (3, 3, 0)
        );
        assert_eq!(failures.to_string(), "first\nsecond\nthird");
        assert_eq!(failures.errors().len(), 3);
        assert_eq!(
            failures.errors()[1].downcast_ref::<TestError>().unwrap().0,
            "second"
        );
    }

    #[tokio::test]
    async fn empty_and_all_failed_tasks_drop_locally_and_conserve() {
        let calls = Arc::new(TestAtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let (input, worker, stats, mut output, _) = worker(
            2,
            requester(move |_, _| {
                observed.fetch_add(1, TestOrdering::Relaxed);
                Box::pin(ready(Err(boxed_error("peer failed"))))
            }),
            allowing_checker(),
            successful_blocker(),
        );
        input.send(request(1, vec![])).await.unwrap();
        input
            .send(request(2, vec![addr(20), addr(21), addr(20)]))
            .await
            .unwrap();
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtRequestMetaInfoWorkerExit::InputClosed
        );
        assert_eq!(calls.load(TestOrdering::Relaxed), 3);
        assert!(output.try_recv().is_err());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.empty_peers_dropped, 1);
        assert_eq!(snapshot.all_peers_failed, 1);
        assert_eq!(snapshot.peer_occurrences, 3);
        assert_eq!(snapshot.request_attempts_failed, 3);
        assert_normal_conservation(snapshot);
    }

    #[tokio::test]
    async fn ordered_duplicates_fall_through_first_success_and_preserve_request_identity() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&calls);
        let attempts = Arc::new(TestAtomicUsize::new(0));
        let attempt = Arc::clone(&attempts);
        let (input, worker, stats, mut output, _) = worker(
            1,
            requester(move |hash, peer| {
                observed.lock().unwrap().push((peer, hash));
                let index = attempt.fetch_add(1, TestOrdering::Relaxed);
                Box::pin(async move {
                    if index < 2 {
                        Err(boxed_error("fall through"))
                    } else {
                        Ok(valid_info())
                    }
                })
            }),
            allowing_checker(),
            successful_blocker(),
        );
        let peers = vec![addr(10), addr(10), addr(11), addr(12)];
        let expected = request(7, peers.clone());
        input.send(expected.clone()).await.unwrap();
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtRequestMetaInfoWorkerExit::InputClosed
        );
        assert_eq!(
            *calls.lock().unwrap(),
            peers[..3]
                .iter()
                .copied()
                .map(|peer| (peer, expected.info_hash))
                .collect::<Vec<_>>()
        );
        let persisted = output.recv().await.unwrap();
        assert_eq!(persisted.info_hash, expected.info_hash);
        assert_eq!(persisted.source_node_addr, expected.source_node_addr);
        assert_eq!(persisted.meta_info.as_ref(), &valid_info());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.request_attempts_failed, 2);
        assert_eq!(snapshot.request_attempts_succeeded, 1);
        assert_eq!(snapshot.peer_occurrences_skipped, 1);
        assert_eq!(snapshot.allowed, 1);
        assert_eq!(snapshot.persist_queued, 1);
        assert_normal_conservation(snapshot);
    }

    #[tokio::test]
    async fn banned_tasks_block_exact_requested_hash_false_and_ignore_block_errors() {
        let blocks = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&blocks);
        let (input, worker, stats, mut output, _) = worker(
            2,
            requester(|_, _| Box::pin(ready(Ok(valid_info())))),
            checker(|_| Err(boxed_error("banned"))),
            blocker(move |hashes, flush| {
                observed.lock().unwrap().push((hashes.clone(), flush));
                Box::pin(ready(if hashes == vec![id(1)] {
                    Ok(())
                } else {
                    Err(boxed_error("block failed"))
                }))
            }),
        );
        input.send(request(1, vec![addr(1)])).await.unwrap();
        input.send(request(2, vec![addr(2)])).await.unwrap();
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtRequestMetaInfoWorkerExit::InputClosed
        );
        let mut actual = blocks.lock().unwrap().clone();
        actual.sort_by_key(|(hashes, _)| *hashes[0].as_bytes());
        assert_eq!(actual, vec![(vec![id(1)], false), (vec![id(2)], false)]);
        assert!(output.try_recv().is_err());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.banned, 2);
        assert_eq!(snapshot.block_succeeded, 1);
        assert_eq!(snapshot.block_failed_ignored, 1);
        assert_normal_conservation(snapshot);
    }

    #[tokio::test]
    async fn default_banning_checker_drives_the_worker_block_path() {
        let requested_hash = Id20::from_slice(&BANNED_HASH).unwrap();
        let blocks = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&blocks);
        let (input, worker, stats, mut output, _) = worker(
            1,
            requester(|_, _| Box::pin(ready(Ok(banned_info())))),
            Arc::new(DefaultDhtMetaInfoBanningChecker),
            blocker(move |hashes, flush| {
                observed.lock().unwrap().push((hashes, flush));
                Box::pin(ready(Ok(())))
            }),
        );
        input
            .send(DhtMetaInfoRequest {
                info_hash: requested_hash,
                source_node_addr: addr(9),
                peers: vec![addr(10)],
            })
            .await
            .unwrap();
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtRequestMetaInfoWorkerExit::InputClosed
        );
        assert_eq!(*blocks.lock().unwrap(), vec![(vec![requested_hash], false)]);
        assert!(output.try_recv().is_err());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.banned, 1);
        assert_eq!(snapshot.block_succeeded, 1);
        assert_normal_conservation(snapshot);
    }

    #[tokio::test]
    async fn closed_persist_route_is_local_and_later_tasks_continue() {
        let (input, worker, stats, output, _) = worker(
            1,
            requester(|_, _| Box::pin(ready(Ok(valid_info())))),
            allowing_checker(),
            successful_blocker(),
        );
        drop(output);
        input.send(request(1, vec![addr(1)])).await.unwrap();
        input.send(request(2, vec![addr(2)])).await.unwrap();
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtRequestMetaInfoWorkerExit::InputClosed
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.tasks_completed, 2);
        assert_eq!(snapshot.allowed, 2);
        assert_eq!(snapshot.persist_closed_dropped, 2);
        assert_normal_conservation(snapshot);
    }

    #[tokio::test]
    async fn capacity_does_not_dequeue_extra_and_eof_joins_every_task() {
        let permits = Arc::new(Semaphore::new(0));
        let calls = Arc::new(TestAtomicUsize::new(0));
        let run_permits = Arc::clone(&permits);
        let run_calls = Arc::clone(&calls);
        let (input, worker, stats, mut output, _) = worker(
            2,
            requester(move |_, _| {
                let permits = Arc::clone(&run_permits);
                let calls = Arc::clone(&run_calls);
                Box::pin(async move {
                    calls.fetch_add(1, TestOrdering::Relaxed);
                    permits.acquire_owned().await.unwrap().forget();
                    Ok(valid_info())
                })
            }),
            allowing_checker(),
            successful_blocker(),
        );
        for value in 1..=3 {
            input.send(request(value, vec![addr(value)])).await.unwrap();
        }
        drop(input);
        let run = tokio::spawn(worker.run(pending()));
        yield_until(|| calls.load(TestOrdering::Relaxed) == 2).await;
        assert_eq!(stats.snapshot().dequeued, 2);
        permits.add_permits(1);
        yield_until(|| calls.load(TestOrdering::Relaxed) == 3).await;
        assert_eq!(stats.snapshot().dequeued, 3);
        permits.add_permits(2);
        assert_eq!(
            run.await.unwrap(),
            DhtRequestMetaInfoWorkerExit::InputClosed
        );
        for _ in 0..3 {
            output.recv().await.unwrap();
        }
        assert_normal_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn pre_ready_shutdown_drains_queued_work_without_starting_tasks() {
        let calls = Arc::new(TestAtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let (input, worker, stats, _, _) = worker(
            1,
            requester(move |_, _| {
                observed.fetch_add(1, TestOrdering::Relaxed);
                Box::pin(ready(Ok(valid_info())))
            }),
            allowing_checker(),
            successful_blocker(),
        );
        input.send(request(1, vec![addr(1)])).await.unwrap();
        input.send(request(2, vec![addr(2)])).await.unwrap();

        assert_eq!(
            worker.run(ready(())).await,
            DhtRequestMetaInfoWorkerExit::Shutdown {
                queued_dropped: 2,
                tasks_cancelled: 0,
                peer_occurrences_dropped: 0,
                request_attempts_cancelled: 0,
                block_calls_cancelled: 0,
                persist_requests_dropped: 0,
            }
        );
        assert_eq!(calls.load(TestOrdering::Relaxed), 0);
        assert_eq!(
            stats.snapshot(),
            DhtRequestMetaInfoWorkerStats {
                shutdown_queued_dropped: 2,
                ..DhtRequestMetaInfoWorkerStats::default()
            }
        );
    }

    #[tokio::test]
    async fn shutdown_during_request_counts_current_and_untouched_peer_occurrences() {
        let (started_tx, started_rx) = oneshot::channel();
        let started = Arc::new(Mutex::new(Some(started_tx)));
        let observed = Arc::clone(&started);
        let (input, worker, stats, _, _) = worker(
            1,
            requester(move |_, _| {
                if let Some(sender) = observed.lock().unwrap().take() {
                    let _ = sender.send(());
                }
                Box::pin(pending())
            }),
            allowing_checker(),
            successful_blocker(),
        );
        input
            .send(request(1, vec![addr(1), addr(2)]))
            .await
            .unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn(worker.run(async move {
            let _ = shutdown_rx.await;
        }));
        started_rx.await.unwrap();
        shutdown_tx.send(()).unwrap();

        assert_eq!(
            run.await.unwrap(),
            DhtRequestMetaInfoWorkerExit::Shutdown {
                queued_dropped: 0,
                tasks_cancelled: 1,
                peer_occurrences_dropped: 2,
                request_attempts_cancelled: 1,
                block_calls_cancelled: 0,
                persist_requests_dropped: 0,
            }
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.shutdown_peer_occurrences_dropped, 2);
        assert_eq!(snapshot.shutdown_request_attempts_cancelled, 1);
        assert_shutdown_conservation(snapshot);
    }

    #[tokio::test]
    async fn shutdown_during_block_accounts_block_only_after_success_skips_suffix() {
        let (started_tx, started_rx) = oneshot::channel();
        let started = Arc::new(Mutex::new(Some(started_tx)));
        let observed = Arc::clone(&started);
        let (input, worker, stats, _, _) = worker(
            1,
            requester(|_, _| Box::pin(ready(Ok(valid_info())))),
            checker(|_| Err(boxed_error("banned"))),
            blocker(move |_, _| {
                if let Some(sender) = observed.lock().unwrap().take() {
                    let _ = sender.send(());
                }
                Box::pin(pending())
            }),
        );
        input
            .send(request(1, vec![addr(1), addr(2)]))
            .await
            .unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn(worker.run(async move {
            let _ = shutdown_rx.await;
        }));
        started_rx.await.unwrap();
        shutdown_tx.send(()).unwrap();

        assert_eq!(
            run.await.unwrap(),
            DhtRequestMetaInfoWorkerExit::Shutdown {
                queued_dropped: 0,
                tasks_cancelled: 1,
                peer_occurrences_dropped: 0,
                request_attempts_cancelled: 0,
                block_calls_cancelled: 1,
                persist_requests_dropped: 0,
            }
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.peer_occurrences_skipped, 1);
        assert_eq!(snapshot.shutdown_block_calls_cancelled, 1);
        assert_shutdown_conservation(snapshot);
    }

    #[tokio::test]
    async fn shutdown_during_backpressured_persist_accounts_exact_request() {
        let (input, worker, stats, mut output, persist) = worker(
            1,
            requester(|_, _| Box::pin(ready(Ok(valid_info())))),
            allowing_checker(),
            successful_blocker(),
        );
        for value in 0..crate::DHT_PERSIST_TORRENT_ROUTE_CAPACITY {
            persist
                .send(DhtPersistTorrentRequest {
                    info_hash: id(value as u16),
                    source_node_addr: addr(value as u16),
                    meta_info: Arc::new(valid_info()),
                })
                .await
                .unwrap();
        }
        input.send(request(1, vec![addr(1)])).await.unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn(worker.run(async move {
            let _ = shutdown_rx.await;
        }));
        yield_until(|| stats.snapshot().request_attempts_succeeded == 1).await;
        tokio::task::yield_now().await;
        shutdown_tx.send(()).unwrap();

        let exit = run.await.unwrap();
        assert_eq!(
            exit,
            DhtRequestMetaInfoWorkerExit::Shutdown {
                queued_dropped: 0,
                tasks_cancelled: 1,
                peer_occurrences_dropped: 0,
                request_attempts_cancelled: 0,
                block_calls_cancelled: 0,
                persist_requests_dropped: 1,
            }
        );
        for value in 0..crate::DHT_PERSIST_TORRENT_ROUTE_CAPACITY {
            let persisted = output.recv().await.unwrap();
            assert_eq!(persisted.info_hash, id(value as u16));
            assert_eq!(persisted.source_node_addr, addr(value as u16));
        }
        assert_eq!(
            output.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );
        assert_shutdown_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn shutdown_can_abort_accepted_never_polled_task_and_account_all_peers() {
        struct PendingThenReady(bool);
        impl Future for PendingThenReady {
            type Output = ();

            fn poll(
                mut self: Pin<&mut Self>,
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

        let calls = Arc::new(TestAtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let (input, worker, stats, _, _) = worker(
            1,
            requester(move |_, _| {
                observed.fetch_add(1, TestOrdering::Relaxed);
                Box::pin(pending())
            }),
            allowing_checker(),
            successful_blocker(),
        );
        input
            .send(request(1, vec![addr(1), addr(2), addr(3)]))
            .await
            .unwrap();

        assert_eq!(
            worker.run(PendingThenReady(false)).await,
            DhtRequestMetaInfoWorkerExit::Shutdown {
                queued_dropped: 0,
                tasks_cancelled: 1,
                peer_occurrences_dropped: 3,
                request_attempts_cancelled: 0,
                block_calls_cancelled: 0,
                persist_requests_dropped: 0,
            }
        );
        assert_eq!(calls.load(TestOrdering::Relaxed), 0);
        assert_shutdown_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn child_panic_aborts_and_joins_sibling_then_resumes_payload() {
        struct DropSignal(Arc<AtomicBool>);
        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let sibling_dropped = Arc::new(AtomicBool::new(false));
        let run_barrier = Arc::clone(&barrier);
        let run_dropped = Arc::clone(&sibling_dropped);
        let (input, worker, _, _, _) = worker(
            2,
            requester(move |hash, _| {
                let barrier = Arc::clone(&run_barrier);
                let dropped = Arc::clone(&run_dropped);
                Box::pin(async move {
                    barrier.wait().await;
                    if hash == id(1) {
                        tokio::task::yield_now().await;
                        panic!("metainfo-child-panic");
                    }
                    let _signal = DropSignal(dropped);
                    pending::<Result<ParsedInfo, RequestMetaInfoCollaboratorError>>().await
                })
            }),
            allowing_checker(),
            successful_blocker(),
        );
        input.send(request(1, vec![addr(1)])).await.unwrap();
        input.send(request(2, vec![addr(2)])).await.unwrap();
        drop(input);

        let error = tokio::spawn(worker.run(pending())).await.unwrap_err();
        assert!(error.is_panic());
        assert_eq!(
            error.into_panic().downcast_ref::<&str>(),
            Some(&"metainfo-child-panic")
        );
        assert!(sibling_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn dropping_active_worker_run_aborts_child_and_closes_input() {
        struct DropSignal(Arc<AtomicBool>);
        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let started = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let run_started = Arc::clone(&started);
        let run_dropped = Arc::clone(&dropped);
        let (input, worker, stats, _, _) = worker(
            1,
            requester(move |_, _| {
                run_started.store(true, Ordering::SeqCst);
                let signal = DropSignal(Arc::clone(&run_dropped));
                Box::pin(async move {
                    let _signal = signal;
                    pending::<Result<ParsedInfo, RequestMetaInfoCollaboratorError>>().await
                })
            }),
            allowing_checker(),
            successful_blocker(),
        );
        input.send(request(1, vec![addr(1)])).await.unwrap();
        let run = tokio::spawn(worker.run(pending()));
        yield_until(|| started.load(Ordering::SeqCst)).await;
        run.abort();
        assert!(run.await.unwrap_err().is_cancelled());
        yield_until(|| dropped.load(Ordering::SeqCst)).await;
        assert!(input.send(request(2, vec![addr(2)])).await.is_err());
        assert_eq!(
            stats.snapshot(),
            DhtRequestMetaInfoWorkerStats {
                dequeued: 1,
                peer_occurrences: 1,
                request_attempts_started: 1,
                ..DhtRequestMetaInfoWorkerStats::default()
            }
        );
    }
}
