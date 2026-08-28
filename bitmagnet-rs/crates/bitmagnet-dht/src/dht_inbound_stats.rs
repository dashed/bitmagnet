use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Cloneable monotonic counters for inbound DHT admission and rejection work.
///
/// Clones share the same atomic counters. Each field is individually monotonic,
/// but a snapshot is assembled from independent relaxed loads and is therefore
/// not a transactional view across fields.
#[derive(Clone, Default)]
pub struct DhtInboundStats {
    inner: Arc<DhtInboundStatsInner>,
}

#[derive(Default)]
struct DhtInboundStatsInner {
    admitted: AtomicU64,
    denied_per_ip: AtomicU64,
    denied_global: AtomicU64,
    denied_handler_capacity: AtomicU64,
    rejection_queued: AtomicU64,
    rejection_queue_full_dropped: AtomicU64,
    rejection_sent: AtomicU64,
    rejection_encode_failed: AtomicU64,
    rejection_transport_failed: AtomicU64,
}

/// One non-transactional snapshot of the monotonic inbound DHT counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtInboundStatsSnapshot {
    pub admitted: u64,
    pub denied_per_ip: u64,
    pub denied_global: u64,
    pub denied_handler_capacity: u64,
    pub rejection_queued: u64,
    pub rejection_queue_full_dropped: u64,
    pub rejection_sent: u64,
    pub rejection_encode_failed: u64,
    pub rejection_transport_failed: u64,
}

impl DhtInboundStats {
    /// Construct an independently owned set of zeroed counters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read each monotonic counter independently with relaxed ordering.
    ///
    /// The returned fields never decrease, but concurrent increments can become
    /// visible between field loads, so relationships across fields are not an
    /// atomic point-in-time observation.
    #[must_use]
    pub fn snapshot(&self) -> DhtInboundStatsSnapshot {
        DhtInboundStatsSnapshot {
            admitted: self.inner.admitted.load(Ordering::Relaxed),
            denied_per_ip: self.inner.denied_per_ip.load(Ordering::Relaxed),
            denied_global: self.inner.denied_global.load(Ordering::Relaxed),
            denied_handler_capacity: self.inner.denied_handler_capacity.load(Ordering::Relaxed),
            rejection_queued: self.inner.rejection_queued.load(Ordering::Relaxed),
            rejection_queue_full_dropped: self
                .inner
                .rejection_queue_full_dropped
                .load(Ordering::Relaxed),
            rejection_sent: self.inner.rejection_sent.load(Ordering::Relaxed),
            rejection_encode_failed: self.inner.rejection_encode_failed.load(Ordering::Relaxed),
            rejection_transport_failed: self
                .inner
                .rejection_transport_failed
                .load(Ordering::Relaxed),
        }
    }
}

// These mutation methods are deliberately crate-private: the supervisor is the
// sole classification boundary.
impl DhtInboundStats {
    pub(crate) fn record_admitted(&self) {
        increment_saturating(&self.inner.admitted);
    }

    pub(crate) fn record_denied_per_ip(&self) {
        increment_saturating(&self.inner.denied_per_ip);
    }

    pub(crate) fn record_denied_global(&self) {
        increment_saturating(&self.inner.denied_global);
    }

    pub(crate) fn record_denied_handler_capacity(&self) {
        increment_saturating(&self.inner.denied_handler_capacity);
    }

    pub(crate) fn record_rejection_queued(&self) {
        increment_saturating(&self.inner.rejection_queued);
    }

    pub(crate) fn record_rejection_queue_full_dropped(&self) {
        increment_saturating(&self.inner.rejection_queue_full_dropped);
    }

    pub(crate) fn record_rejection_sent(&self) {
        increment_saturating(&self.inner.rejection_sent);
    }

    pub(crate) fn record_rejection_encode_failed(&self) {
        increment_saturating(&self.inner.rejection_encode_failed);
    }

    pub(crate) fn record_rejection_transport_failed(&self) {
        increment_saturating(&self.inner.rejection_transport_failed);
    }
}

fn increment_saturating(counter: &AtomicU64) {
    let _previous = counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(1))
        })
        .expect("a saturating counter update always supplies a replacement");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_share_all_counters_and_increments_saturate() {
        let stats = DhtInboundStats::new();
        let clone = stats.clone();

        stats.record_admitted();
        clone.record_denied_per_ip();
        stats.record_denied_global();
        clone.record_denied_handler_capacity();
        stats.record_rejection_queued();
        clone.record_rejection_queue_full_dropped();
        stats.record_rejection_sent();
        clone.record_rejection_encode_failed();
        stats.record_rejection_transport_failed();

        assert_eq!(
            clone.snapshot(),
            DhtInboundStatsSnapshot {
                admitted: 1,
                denied_per_ip: 1,
                denied_global: 1,
                denied_handler_capacity: 1,
                rejection_queued: 1,
                rejection_queue_full_dropped: 1,
                rejection_sent: 1,
                rejection_encode_failed: 1,
                rejection_transport_failed: 1,
            }
        );

        stats.inner.admitted.store(u64::MAX - 1, Ordering::Relaxed);
        clone.record_admitted();
        stats.record_admitted();
        assert_eq!(stats.snapshot().admitted, u64::MAX);
    }
}
