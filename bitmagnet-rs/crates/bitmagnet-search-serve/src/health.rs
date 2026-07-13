//! Lock-free cached health state and background L3 health polling.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bitmagnet_proto::v1::path_search_health::ServingStatus;

use crate::candidates::CandidateSource;
use crate::metrics::PathsearchMetrics;

/// Configuration for the background L3 health reporter ported from Go
/// `searchfx.registerPathsearchHealthReporter`.
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Cadence of the background `HealthCheck` poll (Go
    /// `PathsearchHealthInterval`, default 15 seconds).
    pub interval: Duration,
    /// When non-zero, marks L3 unhealthy when its follow watermark lags the
    /// current time by more than this duration (default zero disables it).
    pub max_watermark_lag: Duration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15),
            max_watermark_lag: Duration::ZERO,
        }
    }
}

/// Cached last-known L3 health observation ported from Go's `pathsearch.HealthState`.
///
/// The zero value is fail-closed: [`Self::healthy`] remains false until a
/// successful background poll explicitly publishes a healthy observation.
#[derive(Debug)]
pub struct HealthState {
    healthy: AtomicBool,
    doc_count: AtomicI64,
    watermark_epoch: AtomicI64,
    last_success_epoch: AtomicI64,
}

impl HealthState {
    /// Creates a fail-closed health state with all metrics set to zero.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            healthy: AtomicBool::new(false),
            doc_count: AtomicI64::new(0),
            watermark_epoch: AtomicI64::new(0),
            last_success_epoch: AtomicI64::new(0),
        }
    }

    /// Reports the cached last-known L3 trust decision without blocking.
    #[must_use]
    pub fn healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// Publishes a fresh health observation from the background poller.
    pub fn set_healthy(
        &self,
        healthy: bool,
        doc_count: i64,
        watermark_epoch: i64,
        last_success_epoch: i64,
    ) {
        self.healthy.store(healthy, Ordering::Relaxed);
        self.doc_count.store(doc_count, Ordering::Relaxed);
        self.watermark_epoch
            .store(watermark_epoch, Ordering::Relaxed);
        self.last_success_epoch
            .store(last_success_epoch, Ordering::Relaxed);
    }

    /// Returns the epoch seconds of the last successful health poll, or zero.
    #[must_use]
    pub fn last_success_epoch(&self) -> i64 {
        self.last_success_epoch.load(Ordering::Relaxed)
    }

    /// Returns the cached health flag, document count, watermark, and last success.
    #[must_use]
    pub fn snapshot(&self) -> (bool, i64, i64, i64) {
        (
            self.healthy.load(Ordering::Relaxed),
            self.doc_count.load(Ordering::Relaxed),
            self.watermark_epoch.load(Ordering::Relaxed),
            self.last_success_epoch.load(Ordering::Relaxed),
        )
    }
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

/// Builds the cheap, lock-free composer health gate for a shared state.
#[must_use]
pub fn gate(state: Arc<HealthState>) -> crate::candidates::HealthGate {
    Arc::new(move || state.healthy())
}

/// Performs one L3 `HealthCheck`, publishes its trust decision, and logs the
/// outcome, mirroring Go `searchfx.pollPathsearchHealth` without C6 metrics.
pub async fn poll_once<C: CandidateSource + ?Sized>(
    client: &C,
    state: &HealthState,
    config: &HealthConfig,
    now_epoch: i64,
) {
    poll_once_with_metrics(client, state, config, now_epoch, None).await;
}

/// Performs one L3 health poll and publishes the canonical C6 health metrics.
///
/// This is the production composition-root variant of [`poll_once`]. Tests and
/// callers that intentionally omit metrics keep using the original function.
pub async fn poll_once_with_metrics<C: CandidateSource + ?Sized>(
    client: &C,
    state: &HealthState,
    config: &HealthConfig,
    now_epoch: i64,
    metrics: Option<&PathsearchMetrics>,
) {
    let response = match client.health_check().await {
        Ok(response) => response,
        Err(error) => {
            // Preserve the last observation so gauges added in C6 can continue
            // to report "was N docs, last successful at T" across a blip.
            let (_, doc_count, watermark_epoch, last_success_epoch) = state.snapshot();
            state.set_healthy(false, doc_count, watermark_epoch, last_success_epoch);
            if let Some(metrics) = metrics {
                metrics.inc_health_check(false);
                metrics.set_health(false, doc_count, watermark_epoch, last_success_epoch);
            }
            tracing::warn!(
                error = %error,
                "pathsearch: L3 HealthCheck RPC failed; route failing closed"
            );
            return;
        }
    };

    let doc_count = response.doc_count as i64;
    let watermark_epoch = response.watermark_epoch;
    let serving = response.status == ServingStatus::Serving as i32;
    let mut healthy = serving && doc_count > 0;
    let mut stale_lag = None;

    if healthy && !config.max_watermark_lag.is_zero() && watermark_epoch > 0 {
        let lag_seconds = (now_epoch - watermark_epoch).max(0) as u64;
        let lag = Duration::from_secs(lag_seconds);
        if lag > config.max_watermark_lag {
            healthy = false;
            stale_lag = Some(lag);
        }
    }

    state.set_healthy(healthy, doc_count, watermark_epoch, now_epoch);
    if let Some(metrics) = metrics {
        metrics.inc_health_check(true);
        metrics.set_health(healthy, doc_count, watermark_epoch, now_epoch);
    }

    if healthy {
        tracing::debug!(doc_count, watermark_epoch, "pathsearch: L3 healthy");
    } else if let Some(lag) = stale_lag {
        tracing::warn!(
            lag_seconds = lag.as_secs(),
            threshold_seconds = config.max_watermark_lag.as_secs(),
            watermark_epoch,
            "pathsearch: L3 watermark lag exceeds threshold; route failing closed"
        );
    } else {
        tracing::warn!(
            status = response.status,
            doc_count,
            "pathsearch: L3 reachable but not trusted (not serving or empty); route failing closed"
        );
    }
}

/// Starts the background L3 health reporter corresponding to Go
/// `searchfx.registerPathsearchHealthReporter`.
///
/// The first poll runs immediately. The composition root owns the returned
/// handle and should retain it for lifecycle management and abort it on
/// shutdown. A zero interval defensively falls back to the 15-second default.
pub fn spawn_health_poller<C: CandidateSource + 'static>(
    client: Arc<C>,
    state: Arc<HealthState>,
    config: HealthConfig,
) -> tokio::task::JoinHandle<()> {
    spawn_health_poller_with_metrics(client, state, config, None)
}

/// Starts the background health poller with canonical C6 metrics attached.
///
/// The optional facade keeps disabled/test roots lightweight while allowing the
/// GraphQL composition root to pass one registered shared instance.
pub fn spawn_health_poller_with_metrics<C: CandidateSource + 'static>(
    client: Arc<C>,
    state: Arc<HealthState>,
    config: HealthConfig,
    metrics: Option<Arc<PathsearchMetrics>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let period = if config.interval.is_zero() {
            HealthConfig::default().interval
        } else {
            config.interval
        };
        let mut ticker = tokio::time::interval(period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;
            poll_once_with_metrics(
                client.as_ref(),
                state.as_ref(),
                &config,
                current_epoch(),
                metrics.as_deref(),
            )
            .await;
        }
    })
}

fn current_epoch() -> i64 {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    i64::try_from(seconds).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    use bitmagnet_proto::v1::{
        PathCandidatesRequest, PathCandidatesResponse, PathSearchHealth, SuggestRequest,
        SuggestResponse,
    };

    #[derive(Debug, Clone, Copy)]
    enum FakeHealth {
        Response(PathSearchHealth),
        Error,
    }

    #[derive(Debug)]
    struct FakeCandidateSource {
        health: FakeHealth,
    }

    impl FakeCandidateSource {
        fn responding(response: PathSearchHealth) -> Self {
            Self {
                health: FakeHealth::Response(response),
            }
        }

        fn erroring() -> Self {
            Self {
                health: FakeHealth::Error,
            }
        }
    }

    #[async_trait::async_trait]
    impl CandidateSource for FakeCandidateSource {
        async fn path_candidates(
            &self,
            _request: PathCandidatesRequest,
        ) -> crate::Result<PathCandidatesResponse> {
            Err(crate::Error::Candidate(
                "unexpected PathCandidates call".into(),
            ))
        }

        async fn suggest(&self, _request: SuggestRequest) -> crate::Result<SuggestResponse> {
            Err(crate::Error::Candidate("unexpected Suggest call".into()))
        }

        async fn health_check(&self) -> crate::Result<PathSearchHealth> {
            match self.health {
                FakeHealth::Response(response) => Ok(response),
                FakeHealth::Error => Err(crate::Error::Candidate("health check failed".into())),
            }
        }
    }

    fn health(status: ServingStatus, doc_count: u64, watermark_epoch: i64) -> PathSearchHealth {
        PathSearchHealth {
            status: status as i32,
            doc_count,
            watermark_epoch,
            ..PathSearchHealth::default()
        }
    }

    #[test]
    fn zero_value_is_fail_closed() {
        let state = HealthState::default();

        assert!(!state.healthy());
        assert_eq!(state.last_success_epoch(), 0);
        assert_eq!(state.snapshot(), (false, 0, 0, 0));
    }

    #[test]
    fn set_healthy_flips_and_snapshot_returns_stored_values() {
        let state = Arc::new(HealthState::new());
        let health_gate = gate(Arc::clone(&state));

        assert!(!health_gate());
        state.set_healthy(true, 42, 1_700_000_000, 1_700_000_010);

        assert!(state.healthy());
        assert!(health_gate());
        assert_eq!(state.last_success_epoch(), 1_700_000_010);
        assert_eq!(state.snapshot(), (true, 42, 1_700_000_000, 1_700_000_010));
    }

    #[tokio::test]
    async fn poll_once_applies_trust_error_preservation_and_freshness_rules() {
        let config = HealthConfig::default();

        let healthy_state = HealthState::new();
        let serving = FakeCandidateSource::responding(health(ServingStatus::Serving, 5, 900));
        poll_once(&serving, &healthy_state, &config, 1_000).await;
        assert!(healthy_state.healthy());
        assert_eq!(healthy_state.snapshot(), (true, 5, 900, 1_000));

        let not_serving_state = HealthState::new();
        let not_serving =
            FakeCandidateSource::responding(health(ServingStatus::NotServing, 5, 900));
        poll_once(&not_serving, &not_serving_state, &config, 1_000).await;
        assert!(!not_serving_state.healthy());
        assert_eq!(not_serving_state.snapshot(), (false, 5, 900, 1_000));

        let empty_state = HealthState::new();
        let empty = FakeCandidateSource::responding(health(ServingStatus::Serving, 0, 900));
        poll_once(&empty, &empty_state, &config, 1_000).await;
        assert!(!empty_state.healthy());

        poll_once(
            &FakeCandidateSource::erroring(),
            &healthy_state,
            &config,
            1_100,
        )
        .await;
        assert_eq!(healthy_state.snapshot(), (false, 5, 900, 1_000));

        let stale_state = HealthState::new();
        let stale_config = HealthConfig {
            max_watermark_lag: Duration::from_secs(60),
            ..HealthConfig::default()
        };
        poll_once(&serving, &stale_state, &stale_config, 1_000).await;
        assert_eq!(stale_state.snapshot(), (false, 5, 900, 1_000));
    }

    #[tokio::test]
    async fn metrics_follow_success_error_and_preserved_snapshot() {
        let metrics = PathsearchMetrics::new();
        let state = HealthState::new();
        let serving = FakeCandidateSource::responding(health(ServingStatus::Serving, 5, 900));

        poll_once_with_metrics(
            &serving,
            &state,
            &HealthConfig::default(),
            1_000,
            Some(&metrics),
        )
        .await;
        assert_eq!(metrics.health_check_count(true), 1);
        assert_eq!(metrics.health_check_count(false), 0);
        assert_eq!(metrics.health_snapshot(), (true, 5, 900, 1_000));

        poll_once_with_metrics(
            &FakeCandidateSource::erroring(),
            &state,
            &HealthConfig::default(),
            1_100,
            Some(&metrics),
        )
        .await;
        assert_eq!(metrics.health_check_count(false), 1);
        assert_eq!(metrics.health_snapshot(), (false, 5, 900, 1_000));
    }
}
