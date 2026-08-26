use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const UNRECORDED_NANOS: u64 = u64::MAX;

/// Go's exact grace period before a started DHT server must have completed a
/// successful outbound query.
pub const DHT_RUNTIME_HEALTH_INITIAL_GRACE: Duration = Duration::from_secs(30);

/// Go's exact maximum age for the most recent successful outbound query.
pub const DHT_RUNTIME_HEALTH_SUCCESS_FRESHNESS: Duration = Duration::from_secs(60);

/// Why the outbound DHT health policy is down.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum DhtRuntimeHealthFailure {
    /// No query has succeeded by the end of the initial grace period.
    #[error("no response within 30 seconds")]
    NoResponseWithinInitialGrace,
    /// The most recent successful query is older than one minute.
    #[error("no successful responses within last minute")]
    NoSuccessfulResponseWithinFreshness,
}

/// Exact Go-compatible classification of the outbound DHT health policy.
///
/// This is not application readiness. In particular, `Inactive` is
/// noncritical in Go's aggregate health endpoint, and `Up` does not prove that
/// the Rust crawler pipeline, database, or writers are ready.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtRuntimeHealthStatus {
    /// The DHT runtime is not active.
    Inactive,
    /// The active runtime is within its startup grace or has a fresh success.
    Up,
    /// The active runtime has violated the successful-query freshness policy.
    Down(DhtRuntimeHealthFailure),
}

/// One consistent-age projection of the outbound DHT health observations.
///
/// `last_response_ago` is retained for parity evidence but deliberately does
/// not affect [`Self::status`], matching Go. A started runtime always supplies
/// `running_for`; the optional shape also represents Go's zero `StartTime`
/// oracle vector without inventing a timestamp.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtRuntimeHealthSnapshot {
    /// Whether the runtime was active when this snapshot was taken.
    pub active: bool,
    /// Age of the successful runtime start, or `None` for Go's zero start.
    pub running_for: Option<Duration>,
    /// Age of the most recently completed query, successful or not.
    pub last_response_ago: Option<Duration>,
    /// Age of the most recently completed successful query.
    pub last_success_ago: Option<Duration>,
}

impl DhtRuntimeHealthSnapshot {
    /// Evaluate Go's exact active/start/success policy.
    #[must_use]
    pub fn status(self) -> DhtRuntimeHealthStatus {
        if !self.active {
            return DhtRuntimeHealthStatus::Inactive;
        }
        let Some(running_for) = self.running_for else {
            return DhtRuntimeHealthStatus::Up;
        };
        let Some(last_success_ago) = self.last_success_ago else {
            return if running_for < DHT_RUNTIME_HEALTH_INITIAL_GRACE {
                DhtRuntimeHealthStatus::Up
            } else {
                DhtRuntimeHealthStatus::Down(DhtRuntimeHealthFailure::NoResponseWithinInitialGrace)
            };
        };
        if last_success_ago > DHT_RUNTIME_HEALTH_SUCCESS_FRESHNESS {
            DhtRuntimeHealthStatus::Down(
                DhtRuntimeHealthFailure::NoSuccessfulResponseWithinFreshness,
            )
        } else {
            DhtRuntimeHealthStatus::Up
        }
    }
}

/// Cloneable sender-free observations for one DHT runtime's outbound queries.
///
/// Query completion is recorded only after outbound admission. Dropping an
/// in-flight Rust future does not fabricate a terminal response or error, so
/// cancellation parity with Go remains an explicit nonclaim. Its active flag
/// follows the bound Rust DHT runtime, not Go's looser crawler `OnStart` flag;
/// a later application health surface must combine this with pipeline
/// lifecycle rather than treating it as readiness.
#[derive(Clone)]
pub struct DhtRuntimeHealthHandle {
    inner: Arc<DhtRuntimeHealthInner>,
}

struct DhtRuntimeHealthInner {
    origin: tokio::time::Instant,
    active: AtomicBool,
    started_nanos: AtomicU64,
    last_response_nanos: AtomicU64,
    last_success_nanos: AtomicU64,
}

impl DhtRuntimeHealthHandle {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(DhtRuntimeHealthInner {
                origin: tokio::time::Instant::now(),
                active: AtomicBool::new(false),
                started_nanos: AtomicU64::new(UNRECORDED_NANOS),
                last_response_nanos: AtomicU64::new(UNRECORDED_NANOS),
                last_success_nanos: AtomicU64::new(UNRECORDED_NANOS),
            }),
        }
    }

    /// Read the active flag and all observation ages without retaining a task,
    /// socket, query registry, or route sender.
    ///
    /// Query timestamps use relaxed atomic loads and are not transactional
    /// across concurrently completing queries. An inactive snapshot suppresses
    /// timestamps so a completion racing shutdown cannot resurrect stopped
    /// health evidence.
    #[must_use]
    pub fn snapshot(&self) -> DhtRuntimeHealthSnapshot {
        if !self.inner.active.load(Ordering::Acquire) {
            return DhtRuntimeHealthSnapshot::default();
        }
        let now = tokio::time::Instant::now();
        let started_nanos = self.inner.started_nanos.load(Ordering::Relaxed);
        let last_response_nanos = self.inner.last_response_nanos.load(Ordering::Relaxed);
        let last_success_nanos = self.inner.last_success_nanos.load(Ordering::Relaxed);
        let active = self.inner.active.load(Ordering::Acquire);
        if !active {
            return DhtRuntimeHealthSnapshot::default();
        }
        let now_nanos = elapsed_nanos(self.inner.origin, now);
        DhtRuntimeHealthSnapshot {
            active,
            running_for: elapsed_age(now_nanos, started_nanos),
            last_response_ago: elapsed_age(now_nanos, last_response_nanos),
            last_success_ago: elapsed_age(now_nanos, last_success_nanos),
        }
    }

    pub(crate) fn mark_started(&self) {
        self.inner
            .last_response_nanos
            .store(UNRECORDED_NANOS, Ordering::Relaxed);
        self.inner
            .last_success_nanos
            .store(UNRECORDED_NANOS, Ordering::Relaxed);
        self.inner.started_nanos.store(
            elapsed_nanos(self.inner.origin, tokio::time::Instant::now()),
            Ordering::Relaxed,
        );
        self.inner.active.store(true, Ordering::Release);
    }

    pub(crate) fn mark_stopped(&self) {
        self.inner.active.store(false, Ordering::Release);
        self.inner
            .started_nanos
            .store(UNRECORDED_NANOS, Ordering::Relaxed);
        self.inner
            .last_response_nanos
            .store(UNRECORDED_NANOS, Ordering::Relaxed);
        self.inner
            .last_success_nanos
            .store(UNRECORDED_NANOS, Ordering::Relaxed);
    }

    pub(crate) fn record_query_completion(&self, success: bool) {
        self.record_query_completion_at(success, tokio::time::Instant::now());
    }

    fn record_query_completion_at(&self, success: bool, completed_at: tokio::time::Instant) {
        if !self.inner.active.load(Ordering::Acquire) {
            return;
        }
        let completed_nanos = elapsed_nanos(self.inner.origin, completed_at);
        if success {
            store_latest(&self.inner.last_success_nanos, completed_nanos);
        }
        store_latest(&self.inner.last_response_nanos, completed_nanos);
    }
}

fn elapsed_nanos(origin: tokio::time::Instant, instant: tokio::time::Instant) -> u64 {
    u64::try_from(instant.saturating_duration_since(origin).as_nanos())
        .unwrap_or(UNRECORDED_NANOS - 1)
        .min(UNRECORDED_NANOS - 1)
}

fn store_latest(counter: &AtomicU64, completed_nanos: u64) {
    let _previous = counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(if current == UNRECORDED_NANOS {
                completed_nanos
            } else {
                current.max(completed_nanos)
            })
        })
        .expect("the latest-timestamp update always supplies a replacement");
}

fn elapsed_age(now_nanos: u64, recorded_nanos: u64) -> Option<Duration> {
    if recorded_nanos == UNRECORDED_NANOS {
        return None;
    }
    Some(Duration::from_nanos(
        now_nanos.saturating_sub(recorded_nanos),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_thresholds_ignore_last_response() {
        let snapshot =
            |running_for, last_response_ago, last_success_ago| DhtRuntimeHealthSnapshot {
                active: true,
                running_for,
                last_response_ago,
                last_success_ago,
            };
        assert_eq!(
            snapshot(None, None, None).status(),
            DhtRuntimeHealthStatus::Up
        );
        assert_eq!(
            snapshot(
                Some(DHT_RUNTIME_HEALTH_INITIAL_GRACE - Duration::from_nanos(1)),
                None,
                None,
            )
            .status(),
            DhtRuntimeHealthStatus::Up
        );
        assert_eq!(
            snapshot(
                Some(DHT_RUNTIME_HEALTH_INITIAL_GRACE),
                Some(Duration::ZERO),
                None,
            )
            .status(),
            DhtRuntimeHealthStatus::Down(DhtRuntimeHealthFailure::NoResponseWithinInitialGrace)
        );
        assert_eq!(
            snapshot(
                Some(Duration::from_secs(120)),
                Some(Duration::ZERO),
                Some(DHT_RUNTIME_HEALTH_SUCCESS_FRESHNESS),
            )
            .status(),
            DhtRuntimeHealthStatus::Up
        );
        assert_eq!(
            snapshot(
                Some(Duration::from_secs(120)),
                Some(Duration::ZERO),
                Some(DHT_RUNTIME_HEALTH_SUCCESS_FRESHNESS + Duration::from_nanos(1)),
            )
            .status(),
            DhtRuntimeHealthStatus::Down(
                DhtRuntimeHealthFailure::NoSuccessfulResponseWithinFreshness
            )
        );
    }

    #[tokio::test(start_paused = true)]
    async fn clones_share_start_completion_and_stop_without_retaining_runtime_resources() {
        let health = DhtRuntimeHealthHandle::new();
        let retained = health.clone();
        assert_eq!(health.snapshot().status(), DhtRuntimeHealthStatus::Inactive);

        health.mark_started();
        tokio::time::advance(Duration::from_secs(7)).await;
        assert_eq!(
            retained.snapshot().running_for,
            Some(Duration::from_secs(7))
        );
        health.record_query_completion(false);
        assert_eq!(retained.snapshot().last_response_ago, Some(Duration::ZERO));
        assert_eq!(retained.snapshot().last_success_ago, None);

        tokio::time::advance(Duration::from_secs(2)).await;
        retained.record_query_completion(true);
        assert_eq!(health.snapshot().last_success_ago, Some(Duration::ZERO));
        health.mark_stopped();
        assert_eq!(retained.snapshot(), DhtRuntimeHealthSnapshot::default());

        retained.record_query_completion(true);
        assert_eq!(health.snapshot(), DhtRuntimeHealthSnapshot::default());
    }

    #[tokio::test(start_paused = true)]
    async fn out_of_order_writers_cannot_regress_completion_timestamps() {
        let health = DhtRuntimeHealthHandle::new();
        health.mark_started();
        let earlier = tokio::time::Instant::now() + Duration::from_secs(1);
        let later = tokio::time::Instant::now() + Duration::from_secs(2);

        health.record_query_completion_at(true, later);
        health.record_query_completion_at(true, earlier);
        tokio::time::advance(Duration::from_secs(3)).await;

        assert_eq!(
            health.snapshot().last_response_ago,
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            health.snapshot().last_success_ago,
            Some(Duration::from_secs(1))
        );
    }
}
