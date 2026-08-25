use std::collections::{HashSet, VecDeque};
use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bitmagnet_bloom::{
    DecrementStartSource, StableBloomFilter, StableBloomGeometry, StableBloomGeometryError,
};
use bitmagnet_model::InfoHash;
use chrono::{DateTime, Utc};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use super::*;

const TEST_CELLS: usize = 10_000;
const TEST_BITS: u8 = 2;
const TEST_HASHES: usize = 5;
const TEST_DECREMENT: usize = 1;
const SAFE_START: usize = 9_999;

#[derive(Debug)]
struct ManualClock {
    now: StdMutex<ClockSample>,
    reads: AtomicUsize,
}

impl ManualClock {
    fn new(now: Instant) -> Self {
        Self {
            now: StdMutex::new(ClockSample {
                monotonic: now,
                wall: DateTime::<Utc>::UNIX_EPOCH,
            }),
            reads: AtomicUsize::new(0),
        }
    }

    fn advance(&self, duration: Duration) {
        let mut now = self.now.lock().unwrap();
        now.monotonic += duration;
    }

    fn read_count(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl BlockingClock for ManualClock {
    fn now(&self) -> ClockSample {
        self.reads.fetch_add(1, Ordering::SeqCst);
        *self.now.lock().unwrap()
    }
}

struct RecordingStarts {
    calls: Arc<AtomicUsize>,
}

impl DecrementStartSource for RecordingStarts {
    fn next_start(&mut self, cell_count: NonZeroUsize) -> usize {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(cell_count.get(), TEST_CELLS);
        SAFE_START
    }
}

#[derive(Clone)]
enum StoreOutcome {
    Success,
    FailBeforeAdd,
    FailAfterAdd,
    PauseThenSuccess {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    },
    PanicBeforeAdd,
}

struct FakeStoreState {
    persisted: Option<Vec<u8>>,
    calls: Vec<Vec<InfoHash>>,
    outcomes: VecDeque<StoreOutcome>,
}

#[derive(Clone)]
struct FakeStoreHandle {
    state: Arc<StdMutex<FakeStoreState>>,
}

impl FakeStoreHandle {
    fn calls(&self) -> Vec<Vec<InfoHash>> {
        self.state.lock().unwrap().calls.clone()
    }

    fn persisted(&self) -> Option<Vec<u8>> {
        self.state.lock().unwrap().persisted.clone()
    }

    fn push_outcome(&self, outcome: StoreOutcome) {
        self.state.lock().unwrap().outcomes.push_back(outcome);
    }
}

struct FakeStore {
    state: Arc<StdMutex<FakeStoreState>>,
    clock: Arc<ManualClock>,
}

#[async_trait]
impl AtomicBlockingStore for FakeStore {
    async fn commit(
        &mut self,
        blocked: &[InfoHash],
        decrement_starts: &mut (dyn DecrementStartSource + Send),
    ) -> Result<CommittedFilter, BoxError> {
        let (outcome, persisted) = {
            let mut state = self.state.lock().unwrap();
            state.calls.push(blocked.to_vec());
            (
                state.outcomes.pop_front().unwrap_or(StoreOutcome::Success),
                state.persisted.clone(),
            )
        };

        match outcome {
            StoreOutcome::FailBeforeAdd => return Err(store_error("before add")),
            StoreOutcome::PanicBeforeAdd => panic!("scripted atomic-store panic"),
            _ => {}
        }

        let geometry = test_geometry().unwrap();
        let mut filter = match persisted {
            Some(bytes) => StableBloomFilter::from_bytes(&bytes, geometry)?,
            None => StableBloomFilter::new(geometry),
        };
        for hash in blocked {
            filter.add(hash.as_slice(), decrement_starts);
        }

        if matches!(outcome, StoreOutcome::FailAfterAdd) {
            return Err(store_error("after add"));
        }

        let mut encoded = Vec::with_capacity(geometry.encoded_bytes());
        filter.write_to(&mut encoded)?;
        let flushed_at = self.clock.now().monotonic;

        if let StoreOutcome::PauseThenSuccess { entered, release } = outcome {
            entered.notify_one();
            release.notified().await;
        }

        self.state.lock().unwrap().persisted = Some(encoded);
        Ok(CommittedFilter { filter, flushed_at })
    }
}

fn store_error(message: &'static str) -> BoxError {
    Box::new(io::Error::other(message))
}

fn test_geometry() -> Result<StableBloomGeometry, StableBloomGeometryError> {
    StableBloomGeometry::new(TEST_CELLS, TEST_BITS, TEST_HASHES, TEST_DECREMENT)
}

fn test_hash(value: u32) -> InfoHash {
    let mut bytes = [0_u8; 20];
    bytes[..4].copy_from_slice(&value.to_be_bytes());
    InfoHash::new(bytes)
}

fn manager(
    now: Instant,
    config: BlockingConfig,
) -> (
    BlockingManager,
    FakeStoreHandle,
    Arc<ManualClock>,
    Arc<AtomicUsize>,
) {
    manager_with_persisted(now, config, &[])
}

fn manager_with_persisted(
    now: Instant,
    config: BlockingConfig,
    persisted_hashes: &[InfoHash],
) -> (
    BlockingManager,
    FakeStoreHandle,
    Arc<ManualClock>,
    Arc<AtomicUsize>,
) {
    let clock = Arc::new(ManualClock::new(now));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut filter = StableBloomFilter::new(test_geometry().unwrap());
    let mut seed_source = RecordingStarts {
        calls: Arc::new(AtomicUsize::new(0)),
    };
    for hash in persisted_hashes {
        filter.add(hash.as_slice(), &mut seed_source);
    }
    let persisted = (!persisted_hashes.is_empty()).then(|| {
        let mut bytes = Vec::new();
        filter.write_to(&mut bytes).unwrap();
        bytes
    });
    let state = Arc::new(StdMutex::new(FakeStoreState {
        persisted,
        calls: Vec::new(),
        outcomes: VecDeque::new(),
    }));
    let handle = FakeStoreHandle {
        state: state.clone(),
    };
    let manager = BlockingManager::new_for_test(
        Box::new(FakeStore {
            state,
            clock: clock.clone(),
        }),
        clock.clone(),
        Box::new(RecordingStarts {
            calls: calls.clone(),
        }),
        config,
    );
    (manager, handle, clock, calls)
}

fn config(max_buffer_size: usize, max_flush_wait: Duration) -> BlockingConfig {
    BlockingConfig {
        max_buffer_size: NonZeroUsize::new(max_buffer_size).unwrap(),
        max_flush_wait,
    }
}

fn assert_send_sync<T: Send + Sync>() {}

#[tokio::test]
async fn defaults_empty_public_flush_traits_and_drop_are_taskless() {
    assert_eq!(
        BlockingConfig::default(),
        config(1_000, Duration::from_secs(300))
    );
    assert_send_sync::<BlockingManager>();

    let (manager, store, _, starts) = manager(Instant::now(), BlockingConfig::default());
    manager.flush().await.unwrap();
    assert!(store.calls().is_empty());
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    drop(manager);
    assert!(store.calls().is_empty());
}

#[tokio::test]
async fn first_filter_and_first_block_checkpoint_but_later_empty_block_waits() {
    let now = Instant::now();
    let (filter_manager, filter_store, _, _) = manager(now, BlockingConfig::default());
    assert!(filter_manager.filter(&[]).await.unwrap().is_empty());
    assert_eq!(filter_store.calls(), [Vec::<InfoHash>::new()]);

    let (block_manager, block_store, _, _) = manager(now, BlockingConfig::default());
    block_manager.block(&[], false).await.unwrap();
    assert_eq!(block_store.calls(), [Vec::<InfoHash>::new()]);
    block_manager.block(&[], false).await.unwrap();
    assert_eq!(block_store.calls().len(), 1);
}

#[tokio::test]
async fn initialized_empty_public_flush_ignores_an_elapsed_refresh_deadline() {
    let (manager, store, clock, _) = manager(Instant::now(), BlockingConfig::default());
    manager.filter(&[]).await.unwrap();
    clock.advance(DEFAULT_MAX_FLUSH_WAIT);

    manager.flush().await.unwrap();
    assert_eq!(store.calls().len(), 1);
    manager.filter(&[]).await.unwrap();
    assert_eq!(store.calls().len(), 2);
}

#[tokio::test]
async fn explicit_size_and_time_boundaries_are_inclusive_and_lazy() {
    let wait = Duration::from_secs(300);
    let (manager, store, clock, _) = manager(Instant::now(), config(3, wait));
    manager.filter(&[]).await.unwrap();
    assert_eq!(
        clock.read_count(),
        1,
        "only the store samples first refresh"
    );

    manager
        .block(&[test_hash(1), test_hash(1), test_hash(2)], false)
        .await
        .unwrap();
    assert_eq!(store.calls().len(), 1);
    assert_eq!(clock.read_count(), 2);
    manager.block(&[test_hash(3)], false).await.unwrap();
    assert_eq!(store.calls().len(), 2);
    assert_eq!(
        clock.read_count(),
        3,
        "the inclusive size threshold short-circuits the policy clock"
    );
    assert_eq!(
        store.calls()[1]
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .len(),
        3
    );

    clock.advance(wait - Duration::from_nanos(1));
    manager.block(&[], false).await.unwrap();
    assert_eq!(store.calls().len(), 2);
    clock.advance(Duration::from_nanos(1));
    manager.block(&[], false).await.unwrap();
    assert_eq!(store.calls().len(), 3);
    assert!(store.calls()[2].is_empty());

    let reads = clock.read_count();
    manager.block(&[], true).await.unwrap();
    assert_eq!(clock.read_count(), reads + 1, "only the store samples time");
    assert_eq!(store.calls().len(), 4);
}

#[tokio::test]
async fn filter_checks_buffer_before_bloom_and_preserves_order_and_duplicates() {
    let a = test_hash(10);
    let b = test_hash(11);
    let c = test_hash(12);
    let (manager, store, _, _) =
        manager_with_persisted(Instant::now(), BlockingConfig::default(), &[c]);
    manager.filter(&[]).await.unwrap();
    manager.block(&[b], false).await.unwrap();

    assert_eq!(manager.filter(&[a, b, a, c]).await.unwrap(), [a, a]);
    assert_eq!(store.calls().len(), 1);
}

#[tokio::test]
async fn committed_membership_is_cached_and_persisted_hashes_can_be_rebuffered() {
    let a = test_hash(20);
    let (manager, store, _, _) = manager(Instant::now(), BlockingConfig::default());
    manager.block(&[a], true).await.unwrap();
    assert!(manager.filter(&[a]).await.unwrap().is_empty());

    manager.block(&[a], false).await.unwrap();
    assert!(manager.filter(&[a]).await.unwrap().is_empty());
    manager.flush().await.unwrap();
    assert_eq!(store.calls().len(), 2);
    assert_eq!(store.calls()[1], [a]);
}

#[tokio::test]
async fn failures_retain_buffer_cache_and_time_and_retry_the_complete_set() {
    let a = test_hash(30);
    let b = test_hash(31);
    let c = test_hash(32);
    let (manager, store, clock, starts) =
        manager_with_persisted(Instant::now(), BlockingConfig::default(), &[c]);
    manager.filter(&[]).await.unwrap();
    let old_time = manager.inner.lock().await.last_flushed_at;

    store.push_outcome(StoreOutcome::FailBeforeAdd);
    clock.advance(DEFAULT_MAX_FLUSH_WAIT);
    assert!(matches!(
        manager.filter(&[b]).await,
        Err(BlockingError::Store(_))
    ));
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    {
        let inner = manager.inner.lock().await;
        assert_eq!(inner.last_flushed_at, old_time);
        assert!(inner.filter.as_ref().unwrap().test(c.as_slice()));
    }

    store.push_outcome(StoreOutcome::FailAfterAdd);
    assert!(matches!(
        manager.block(&[a], true).await,
        Err(BlockingError::Store(_))
    ));
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    {
        let inner = manager.inner.lock().await;
        assert!(inner.buffer.contains(&a));
        assert_eq!(inner.last_flushed_at, old_time);
        assert!(inner.filter.as_ref().unwrap().test(c.as_slice()));
    }

    manager.flush().await.unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert!(manager.inner.lock().await.buffer.is_empty());
    let snapshots = store.calls();
    assert!(snapshots[snapshots.len() - 2].contains(&a));
    assert!(snapshots[snapshots.len() - 1].contains(&a));
}

#[tokio::test]
async fn failed_forced_checkpoint_suppresses_retained_buffer_without_early_retry() {
    let a = test_hash(35);
    let b = test_hash(36);
    let (manager, store, _, _) = manager(Instant::now(), BlockingConfig::default());
    manager.filter(&[]).await.unwrap();
    store.push_outcome(StoreOutcome::FailAfterAdd);
    assert!(manager.block(&[a], true).await.is_err());
    assert_eq!(store.calls().len(), 2);

    assert_eq!(manager.filter(&[a, b]).await.unwrap(), [b]);
    assert_eq!(
        store.calls().len(),
        2,
        "before the deadline, the retained buffer filters without retrying"
    );
}

#[tokio::test]
async fn first_failed_block_retains_new_hash_and_retry_does_not_lose_it() {
    let a = test_hash(40);
    let (manager, store, _, starts) = manager(Instant::now(), BlockingConfig::default());
    store.push_outcome(StoreOutcome::FailBeforeAdd);
    assert!(manager.block(&[a], false).await.is_err());
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    assert!(manager.inner.lock().await.buffer.contains(&a));

    manager.flush().await.unwrap();
    assert!(manager.inner.lock().await.buffer.is_empty());
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(store.calls(), [vec![a], vec![a]]);
}

#[tokio::test]
async fn one_mutex_serializes_an_awaited_checkpoint_across_clones() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let a = test_hash(50);
    let (manager, store, _, _) = manager(Instant::now(), BlockingConfig::default());
    store.push_outcome(StoreOutcome::PauseThenSuccess {
        entered: entered.clone(),
        release: release.clone(),
    });

    let first = spawn_filter(manager.clone());
    entered.notified().await;
    let mut second = tokio::spawn({
        let manager = manager.clone();
        async move { manager.block(&[a], false).await }
    });
    assert!(timeout(Duration::from_millis(20), &mut second)
        .await
        .is_err());

    release.notify_one();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert_eq!(store.calls().len(), 1);
    assert!(manager.inner.lock().await.buffer.contains(&a));
}

#[tokio::test]
async fn slow_commit_publishes_the_store_timestamp_sampled_before_commit() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (manager, store, clock, _) = manager(Instant::now(), BlockingConfig::default());
    store.push_outcome(StoreOutcome::PauseThenSuccess {
        entered: entered.clone(),
        release: release.clone(),
    });

    let refresh = spawn_filter(manager.clone());
    entered.notified().await;
    clock.advance(DEFAULT_MAX_FLUSH_WAIT);
    release.notify_one();
    refresh.await.unwrap().unwrap();

    manager.block(&[], false).await.unwrap();
    assert_eq!(
        store.calls().len(),
        2,
        "time spent committing shortens the next effective flush interval"
    );
}

#[tokio::test]
async fn cancelling_checkpoint_releases_mutex_without_publishing_partial_state() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let a = test_hash(60);
    let (manager, store, _, starts) = manager(Instant::now(), BlockingConfig::default());
    store.push_outcome(StoreOutcome::PauseThenSuccess {
        entered: entered.clone(),
        release,
    });

    let task = tokio::spawn({
        let manager = manager.clone();
        async move { manager.block(&[a], false).await }
    });
    entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(store.persisted().is_none());
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    {
        let inner = manager.inner.lock().await;
        assert!(inner.buffer.contains(&a));
        assert!(inner.filter.is_none());
        assert!(inner.last_flushed_at.is_none());
    }

    manager.flush().await.unwrap();
    assert!(store.persisted().is_some());
    assert!(manager.inner.lock().await.buffer.is_empty());
}

#[tokio::test]
async fn collaborator_panic_does_not_poison_manager_state() {
    let a = test_hash(70);
    let (manager, store, _, _) = manager(Instant::now(), BlockingConfig::default());
    store.push_outcome(StoreOutcome::PanicBeforeAdd);
    let task = tokio::spawn({
        let manager = manager.clone();
        async move { manager.block(&[a], false).await }
    });
    assert!(task.await.unwrap_err().is_panic());
    assert!(manager.inner.lock().await.buffer.contains(&a));

    manager.flush().await.unwrap();
    assert!(manager.inner.lock().await.buffer.is_empty());
}

#[tokio::test]
async fn zero_wait_is_a_coherent_always_due_policy() {
    let (manager, store, _, _) = manager(Instant::now(), config(10, Duration::ZERO));
    manager.filter(&[]).await.unwrap();
    manager.block(&[], false).await.unwrap();
    assert_eq!(store.calls().len(), 2);
}

fn spawn_filter(manager: BlockingManager) -> JoinHandle<Result<Vec<InfoHash>, BlockingError>> {
    tokio::spawn(async move { manager.filter(&[]).await })
}
