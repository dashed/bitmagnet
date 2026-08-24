use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::RoutingNode;

/// Construct a bounded, single-consumer node-discovery handoff.
///
/// Offering a node does not await or block on queue capacity and never starts a
/// task. When the fixed queue is full or its receiver is gone, the newest node
/// is dropped and classified by both the return value and shared counters.
#[must_use]
pub fn dht_discovery_channel(capacity: NonZeroUsize) -> (DhtDiscoverySender, DhtDiscoveryReceiver) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    let stats = DhtDiscoveryStatsHandle::default();
    (
        DhtDiscoverySender {
            sender,
            stats: stats.clone(),
        },
        DhtDiscoveryReceiver { receiver },
    )
}

/// Cloneable producer for bounded best-effort node discovery.
#[derive(Clone)]
pub struct DhtDiscoverySender {
    sender: mpsc::Sender<RoutingNode>,
    stats: DhtDiscoveryStatsHandle,
}

/// Unique consumer for nodes accepted by [`DhtDiscoverySender`].
pub struct DhtDiscoveryReceiver {
    receiver: mpsc::Receiver<RoutingNode>,
}

/// Exact result of one nonblocking discovery offer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtDiscoveryOffer {
    Queued,
    FullDropped,
    ReceiverClosed,
}

#[derive(Default)]
struct DhtDiscoveryStatsInner {
    offered: AtomicU64,
    queued: AtomicU64,
    full_dropped: AtomicU64,
    receiver_closed_dropped: AtomicU64,
}

/// Cloneable read-only handle to the discovery counters.
///
/// This handle owns no queue sender and therefore cannot delay receiver EOF.
#[derive(Clone, Default)]
pub struct DhtDiscoveryStatsHandle {
    inner: Arc<DhtDiscoveryStatsInner>,
}

/// One non-transactional snapshot of monotonic discovery counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtDiscoveryStats {
    pub offered: u64,
    pub queued: u64,
    pub full_dropped: u64,
    pub receiver_closed_dropped: u64,
}

impl DhtDiscoverySender {
    /// Offer one node without waiting for queue capacity or receiver work.
    pub fn offer(&self, node: RoutingNode) -> DhtDiscoveryOffer {
        increment_saturating(&self.stats.inner.offered);
        match self.sender.try_send(node) {
            Ok(()) => {
                increment_saturating(&self.stats.inner.queued);
                DhtDiscoveryOffer::Queued
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                increment_saturating(&self.stats.inner.full_dropped);
                DhtDiscoveryOffer::FullDropped
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                increment_saturating(&self.stats.inner.receiver_closed_dropped);
                DhtDiscoveryOffer::ReceiverClosed
            }
        }
    }

    /// Read each monotonic counter independently with relaxed ordering.
    ///
    /// Concurrent offers can become visible between field loads, so the
    /// snapshot is not an atomic point-in-time view across fields.
    #[must_use]
    pub fn stats(&self) -> DhtDiscoveryStats {
        self.stats.snapshot()
    }

    /// Clone a read-only counter handle that does not own a queue sender.
    #[must_use]
    pub fn stats_handle(&self) -> DhtDiscoveryStatsHandle {
        self.stats.clone()
    }
}

impl DhtDiscoveryStatsHandle {
    /// Read each monotonic counter independently with relaxed ordering.
    ///
    /// Concurrent offers can become visible between field loads, so the
    /// snapshot is not an atomic point-in-time view across fields.
    #[must_use]
    pub fn snapshot(&self) -> DhtDiscoveryStats {
        DhtDiscoveryStats {
            offered: self.inner.offered.load(Ordering::Relaxed),
            queued: self.inner.queued.load(Ordering::Relaxed),
            full_dropped: self.inner.full_dropped.load(Ordering::Relaxed),
            receiver_closed_dropped: self.inner.receiver_closed_dropped.load(Ordering::Relaxed),
        }
    }
}

impl DhtDiscoveryReceiver {
    /// Receive the next queued node, or `None` once every sender is gone and
    /// the queue has drained.
    pub async fn recv(&mut self) -> Option<RoutingNode> {
        self.receiver.recv().await
    }

    /// Receive a currently queued node without waiting.
    pub fn try_recv(&mut self) -> Result<RoutingNode, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    /// Stop accepting new nodes while retaining already queued nodes to drain.
    pub fn close(&mut self) {
        self.receiver.close();
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
    use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

    use super::*;
    use crate::Id20;

    fn node(value: u8) -> RoutingNode {
        let mut bytes = [0_u8; 20];
        bytes[19] = value;
        RoutingNode {
            id: Id20::from_slice(&bytes).unwrap(),
            addr: SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::LOCALHOST,
                u16::from(value),
                u32::from(value),
                u32::from(value),
            )),
        }
    }

    #[tokio::test]
    async fn fixed_capacity_drops_newest_and_reopens_after_drain() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));

        assert_eq!(sender.offer(node(1)), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(node(2)), DhtDiscoveryOffer::FullDropped);
        assert_eq!(receiver.recv().await, Some(node(1)));
        assert_eq!(sender.offer(node(3)), DhtDiscoveryOffer::Queued);
        assert_eq!(receiver.try_recv().unwrap(), node(3));
        assert_eq!(
            sender.stats(),
            DhtDiscoveryStats {
                offered: 3,
                queued: 2,
                full_dropped: 1,
                receiver_closed_dropped: 0,
            }
        );
    }

    #[tokio::test]
    async fn clones_share_order_stats_and_receiver_lifecycle() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(3).expect("nonzero"));
        let clone = sender.clone();
        let stats = sender.stats_handle();

        assert_eq!(sender.offer(node(1)), DhtDiscoveryOffer::Queued);
        assert_eq!(clone.offer(node(2)), DhtDiscoveryOffer::Queued);
        assert_eq!(receiver.recv().await, Some(node(1)));
        assert_eq!(receiver.recv().await, Some(node(2)));
        assert_eq!(sender.stats(), clone.stats());

        drop(sender);
        assert_eq!(clone.offer(node(3)), DhtDiscoveryOffer::Queued);
        drop(clone);
        assert_eq!(receiver.recv().await, Some(node(3)));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(stats.snapshot().queued, 3);
    }

    #[test]
    fn closed_receiver_drops_immediately_and_counters_saturate() {
        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        drop(receiver);

        sender
            .stats
            .inner
            .offered
            .store(u64::MAX - 1, Ordering::Relaxed);
        assert_eq!(sender.offer(node(1)), DhtDiscoveryOffer::ReceiverClosed);
        assert_eq!(sender.offer(node(2)), DhtDiscoveryOffer::ReceiverClosed);
        assert_eq!(
            sender.stats(),
            DhtDiscoveryStats {
                offered: u64::MAX,
                queued: 0,
                full_dropped: 0,
                receiver_closed_dropped: 2,
            }
        );
    }

    #[test]
    fn explicit_close_rejects_new_nodes_but_preserves_the_queue() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(2).expect("nonzero"));
        assert_eq!(sender.offer(node(1)), DhtDiscoveryOffer::Queued);
        receiver.close();
        assert_eq!(sender.offer(node(2)), DhtDiscoveryOffer::ReceiverClosed);
        assert_eq!(receiver.try_recv().unwrap(), node(1));
        assert_eq!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        );
    }
}
