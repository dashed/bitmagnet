//! Lock-free cached health state shared by the C2 poller and composer hot path.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
