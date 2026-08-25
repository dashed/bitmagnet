//! Buffered info-hash blocking policy above an atomic persistence boundary.
//!
//! This crate owns the concurrency, publication, and PostgreSQL persistence
//! semantics of Go's blocking manager. Application lifecycle and consumer
//! wiring remain deliberately deferred; the atomic store stays an internal
//! implementation detail.
//!
//! Construction does not connect to PostgreSQL, create or migrate its schema,
//! or validate large-object permissions. The manager supplies only in-process
//! serialization: it adds no cross-process locking, automatic retry, shutdown
//! flush, consumer wiring, or application lifecycle registration. An ambiguous
//! commit outcome is reported as failure and never publishes new in-memory
//! state, but this is not a claim that the server could not have committed.

use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bitmagnet_bloom::{DecrementStartSource, RandomDecrementStartSource, StableBloomFilter};
use bitmagnet_model::InfoHash;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use thiserror::Error;
use tokio::sync::Mutex;

mod pg;

/// Go production's maximum buffered unique hash count.
const DEFAULT_MAX_BUFFER_SIZE: NonZeroUsize = NonZeroUsize::new(1_000).unwrap();
/// Go production's maximum delay between successful persistent flushes.
const DEFAULT_MAX_FLUSH_WAIT: Duration = Duration::from_secs(5 * 60);

type BoxError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BlockingConfig {
    max_buffer_size: NonZeroUsize,
    max_flush_wait: Duration,
}

impl Default for BlockingConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: DEFAULT_MAX_BUFFER_SIZE,
            max_flush_wait: DEFAULT_MAX_FLUSH_WAIT,
        }
    }
}

/// Failure returned by an atomic persistence operation.
#[derive(Debug, Error)]
pub enum BlockingError {
    #[error("atomic blocking-store commit failed")]
    Store(#[source] BoxError),
}

struct CommittedFilter {
    filter: StableBloomFilter,
    /// Sampled after the filter write and before metadata work and commit.
    flushed_at: Instant,
}

/// Atomic store boundary for delete, load/create, mutation, persistence, and
/// commit. `Ok` may be returned only after the store observes a successful
/// commit. Error, cancellation, or an ambiguous commit outcome never permits
/// the manager to publish new in-memory state.
#[async_trait]
trait AtomicBlockingStore: Send {
    async fn commit(
        &mut self,
        blocked: &[InfoHash],
        decrement_starts: &mut (dyn DecrementStartSource + Send),
    ) -> Result<CommittedFilter, BoxError>;
}

#[derive(Clone, Copy, Debug)]
struct ClockSample {
    monotonic: Instant,
    wall: DateTime<Utc>,
}

trait BlockingClock: Send + Sync {
    fn now(&self) -> ClockSample;
}

#[derive(Clone, Copy, Debug, Default)]
struct SystemBlockingClock;

impl BlockingClock for SystemBlockingClock {
    fn now(&self) -> ClockSample {
        ClockSample {
            monotonic: Instant::now(),
            wall: Utc::now(),
        }
    }
}

struct Inner {
    store: Box<dyn AtomicBlockingStore>,
    clock: Arc<dyn BlockingClock>,
    buffer: HashSet<InfoHash>,
    filter: Option<StableBloomFilter>,
    last_flushed_at: Option<Instant>,
    decrement_starts: Box<dyn DecrementStartSource + Send>,
}

/// Cloneable handle to one serialized blocking policy state machine.
#[derive(Clone)]
pub struct BlockingManager {
    inner: Arc<Mutex<Inner>>,
    config: BlockingConfig,
}

impl fmt::Debug for BlockingManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingManager")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl BlockingManager {
    /// Construct a PostgreSQL-backed blocking manager.
    ///
    /// Construction is lazy and does not acquire a database connection. The
    /// first policy operation that needs a checkpoint performs the transaction.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        let config = BlockingConfig::default();
        let clock: Arc<dyn BlockingClock> = Arc::new(SystemBlockingClock);
        Self {
            inner: Arc::new(Mutex::new(Inner {
                store: Box::new(pg::PostgresAtomicBlockingStore::new(pool, clock.clone())),
                clock,
                buffer: HashSet::with_capacity(config.max_buffer_size.get()),
                filter: None,
                last_flushed_at: None,
                decrement_starts: Box::new(RandomDecrementStartSource::new()),
            })),
            config,
        }
    }

    /// Return hashes that remain eligible, preserving input order and
    /// duplicates. Exact buffered membership takes precedence over the
    /// probabilistic persistent filter.
    pub async fn filter(&self, info_hashes: &[InfoHash]) -> Result<Vec<InfoHash>, BlockingError> {
        let mut inner = self.inner.lock().await;
        if inner.filter.is_none() || should_flush(&inner, self.config) {
            flush_locked(&mut inner).await?;
        }

        let filter = inner
            .filter
            .as_ref()
            .expect("successful initial checkpoint publishes a filter");
        Ok(info_hashes
            .iter()
            .copied()
            .filter(|info_hash| {
                !inner.buffer.contains(info_hash) && !filter.test(info_hash.as_slice())
            })
            .collect())
    }

    /// Buffer hashes and optionally force their atomic persistence.
    pub async fn block(&self, info_hashes: &[InfoHash], flush: bool) -> Result<(), BlockingError> {
        let mut inner = self.inner.lock().await;
        inner.buffer.extend(info_hashes.iter().copied());
        if flush || should_flush(&inner, self.config) {
            flush_locked(&mut inner).await?;
        }
        Ok(())
    }

    /// Persist a non-empty buffer. An empty public flush is deliberately a
    /// no-op and does not initialize or reload the filter.
    pub async fn flush(&self) -> Result<(), BlockingError> {
        let mut inner = self.inner.lock().await;
        if inner.buffer.is_empty() {
            return Ok(());
        }
        flush_locked(&mut inner).await
    }

    #[cfg(test)]
    fn new_for_test(
        store: Box<dyn AtomicBlockingStore>,
        clock: Arc<dyn BlockingClock>,
        decrement_starts: Box<dyn DecrementStartSource + Send>,
        config: BlockingConfig,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                store,
                clock,
                buffer: HashSet::with_capacity(config.max_buffer_size.get()),
                filter: None,
                last_flushed_at: None,
                decrement_starts,
            })),
            config,
        }
    }
}

fn should_flush(inner: &Inner, config: BlockingConfig) -> bool {
    if inner.buffer.len() >= config.max_buffer_size.get() {
        return true;
    }
    let Some(last_flushed_at) = inner.last_flushed_at else {
        return true;
    };
    inner
        .clock
        .now()
        .monotonic
        .saturating_duration_since(last_flushed_at)
        >= config.max_flush_wait
}

async fn flush_locked(inner: &mut Inner) -> Result<(), BlockingError> {
    // HashSet iteration order is intentionally unspecified, matching Go's map.
    let blocked = inner.buffer.iter().copied().collect::<Vec<_>>();
    let committed = inner
        .store
        .commit(&blocked, inner.decrement_starts.as_mut())
        .await
        .map_err(BlockingError::Store)?;
    inner.buffer.clear();
    inner.filter = Some(committed.filter);
    inner.last_flushed_at = Some(committed.flushed_at);
    Ok(())
}

#[cfg(test)]
mod tests;
