use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use thiserror::Error;
use tokio::sync::mpsc;

use crate::RoutingNode;

/// Construct a bounded, single-consumer node-discovery handoff.
///
/// Offering a node does not await or block on capacity and never starts a task.
/// When the channel has no unreserved capacity or its receiver is gone, the
/// newest node is dropped and classified by both the return value and shared
/// counters. Producers that require backpressure can instead reserve one slot
/// and then synchronously deliver through the resulting permit. Both paths
/// share the same attempt and outcome counters.
#[must_use]
pub fn dht_discovery_channel(capacity: NonZeroUsize) -> (DhtDiscoverySender, DhtDiscoveryReceiver) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    let weak_sender = sender.downgrade();
    let stats = DhtDiscoveryStatsHandle::default();
    let receiver_state = Arc::new(Mutex::new(DhtDiscoveryReceiverState { alive: true }));
    (
        DhtDiscoverySender {
            sender,
            stats: stats.clone(),
            receiver_state: receiver_state.clone(),
        },
        DhtDiscoveryReceiver {
            receiver,
            state: receiver_state,
            weak_sender,
            stats,
        },
    )
}

/// Cloneable producer for bounded best-effort node discovery.
#[derive(Clone)]
pub struct DhtDiscoverySender {
    sender: mpsc::Sender<RoutingNode>,
    stats: DhtDiscoveryStatsHandle,
    receiver_state: Arc<Mutex<DhtDiscoveryReceiverState>>,
}

/// Unique consumer for nodes accepted by [`DhtDiscoverySender`].
pub struct DhtDiscoveryReceiver {
    receiver: mpsc::Receiver<RoutingNode>,
    state: Arc<Mutex<DhtDiscoveryReceiverState>>,
    weak_sender: mpsc::WeakSender<RoutingNode>,
    stats: DhtDiscoveryStatsHandle,
}

struct DhtDiscoveryReceiverState {
    alive: bool,
}

/// One reserved discovery-queue slot.
///
/// Delivering through a permit is synchronous and cannot fail for capacity. It
/// returns an exact queued-or-receiver-closed outcome if receiver destruction
/// races the reservation. Dropping an unused permit releases its slot without
/// changing discovery counters. The permit owns one sender clone, so it delays
/// receiver EOF until it is either delivered or dropped.
#[must_use = "a held discovery permit consumes queue capacity and delays receiver EOF"]
pub struct DhtDiscoveryPermit {
    permit: mpsc::OwnedPermit<RoutingNode>,
    stats: DhtDiscoveryStatsHandle,
    receiver_state: Arc<Mutex<DhtDiscoveryReceiverState>>,
}

/// Failure to reserve discovery-queue capacity.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum DhtDiscoveryReserveError {
    /// The unique discovery receiver was closed or dropped.
    #[error("DHT discovery receiver is closed")]
    ReceiverClosed,
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
///
/// `offered` counts every nonblocking offer plus every attempted permit
/// delivery. At quiescence it equals
/// `queued.saturating_add(full_dropped).saturating_add(receiver_closed_dropped)`.
/// Waiting, cancelled, failed-closed, and acquired-but-abandoned reservations
/// have not offered a node and do not affect counters. The counters are shared
/// channel-wide by every sender clone, including responder and recursive
/// discovery producers.
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

    /// Wait for one queue slot without yet offering a node.
    ///
    /// The wait applies backpressure and is cancellation-safe: dropping the
    /// returned future loses only its place among capacity waiters. Channel
    /// capacity bounds queued nodes plus acquired permits; it does not bound
    /// pending reservation futures. A held permit can therefore make
    /// [`Self::offer`] return [`DhtDiscoveryOffer::FullDropped`] while the
    /// receive queue itself is empty. Dropping a successfully acquired permit
    /// releases its slot. Neither case changes discovery counters. If the
    /// receiver closes before capacity is acquired, this returns
    /// [`DhtDiscoveryReserveError::ReceiverClosed`] and likewise leaves
    /// counters unchanged.
    ///
    /// A successfully returned permit consumes one queue slot and owns a
    /// sender clone until it is delivered or dropped. Holding it indefinitely
    /// therefore reduces available capacity and delays receiver EOF.
    ///
    /// Once acquired, [`DhtDiscoveryPermit::deliver`] commits synchronously,
    /// including if the receiver was explicitly closed after the reservation.
    pub async fn reserve(&self) -> Result<DhtDiscoveryPermit, DhtDiscoveryReserveError> {
        let permit = self
            .sender
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| DhtDiscoveryReserveError::ReceiverClosed)?;
        Ok(DhtDiscoveryPermit {
            permit,
            stats: self.stats.clone(),
            receiver_state: self.receiver_state.clone(),
        })
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

impl DhtDiscoveryPermit {
    /// Commit one node through the reserved slot.
    ///
    /// An attempted delivery contributes once to `offered` and is classified
    /// as either queued or receiver-closed. An explicitly closed receiver can
    /// still drain a delivery from a permit acquired before it closed. Receiver
    /// destruction is synchronized with this commit so a node is never counted
    /// as queued when the receiver was already gone.
    pub fn deliver(self, node: RoutingNode) -> DhtDiscoveryOffer {
        let Self {
            permit,
            stats,
            receiver_state,
        } = self;
        let receiver_state = receiver_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        increment_saturating(&stats.inner.offered);
        if receiver_state.alive {
            let _sender = permit.send(node);
            increment_saturating(&stats.inner.queued);
            DhtDiscoveryOffer::Queued
        } else {
            drop(permit);
            increment_saturating(&stats.inner.receiver_closed_dropped);
            DhtDiscoveryOffer::ReceiverClosed
        }
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
    /// Recover a producer for this exact channel while any producer remains.
    ///
    /// The receiver stores only a weak sender, so this capability seam does
    /// not keep the channel open or delay receiver EOF by itself.
    pub(crate) fn try_sender(&self) -> Option<DhtDiscoverySender> {
        self.weak_sender.upgrade().map(|sender| DhtDiscoverySender {
            sender,
            stats: self.stats.clone(),
            receiver_state: self.state.clone(),
        })
    }

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

impl Drop for DhtDiscoveryReceiver {
    fn drop(&mut self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .alive = false;
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
    use std::future::{poll_fn, Future};
    use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
    use std::pin::Pin;
    use std::task::Poll;
    use std::time::Duration;

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

    async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
        poll_fn(|context| match future.as_mut().poll(context) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(_) => panic!("future completed instead of registering as pending"),
        })
        .await;
    }

    async fn within<F: Future>(future: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(1), future)
            .await
            .expect("operation timed out")
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
    async fn reservation_backpressures_then_counts_only_the_committed_delivery() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        assert_eq!(sender.offer(node(1)), DhtDiscoveryOffer::Queued);

        let waiting_sender = sender.clone();
        let mut waiting = Box::pin(waiting_sender.reserve());
        assert_pending(waiting.as_mut()).await;
        assert_eq!(
            sender.stats(),
            DhtDiscoveryStats {
                offered: 1,
                queued: 1,
                full_dropped: 0,
                receiver_closed_dropped: 0,
            }
        );

        assert_eq!(receiver.recv().await, Some(node(1)));
        let permit = waiting.await.unwrap();
        assert_eq!(sender.stats().offered, 1);
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));

        assert_eq!(permit.deliver(node(2)), DhtDiscoveryOffer::Queued);
        assert_eq!(receiver.recv().await, Some(node(2)));
        assert_eq!(
            sender.stats(),
            DhtDiscoveryStats {
                offered: 2,
                queued: 2,
                full_dropped: 0,
                receiver_closed_dropped: 0,
            }
        );
    }

    #[tokio::test]
    async fn cancelling_a_pending_reservation_leaks_no_capacity_or_stats() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        assert_eq!(sender.offer(node(1)), DhtDiscoveryOffer::Queued);

        let waiting_sender = sender.clone();
        let mut waiting = Box::pin(waiting_sender.reserve());
        assert_pending(waiting.as_mut()).await;
        drop(waiting);

        assert_eq!(receiver.recv().await, Some(node(1)));
        let permit = within(sender.reserve()).await.unwrap();
        assert_eq!(permit.deliver(node(2)), DhtDiscoveryOffer::Queued);
        assert_eq!(receiver.recv().await, Some(node(2)));
        assert_eq!(
            sender.stats(),
            DhtDiscoveryStats {
                offered: 2,
                queued: 2,
                full_dropped: 0,
                receiver_closed_dropped: 0,
            }
        );
    }

    #[tokio::test]
    async fn dropping_an_acquired_permit_reopens_capacity_for_legacy_offer() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let permit = sender.reserve().await.unwrap();

        assert_eq!(sender.offer(node(1)), DhtDiscoveryOffer::FullDropped);
        drop(permit);
        assert_eq!(sender.offer(node(2)), DhtDiscoveryOffer::Queued);
        assert_eq!(receiver.recv().await, Some(node(2)));
        assert_eq!(
            sender.stats(),
            DhtDiscoveryStats {
                offered: 2,
                queued: 1,
                full_dropped: 1,
                receiver_closed_dropped: 0,
            }
        );
    }

    #[tokio::test]
    async fn reservation_waiters_keep_fifo_order() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        assert_eq!(sender.offer(node(1)), DhtDiscoveryOffer::Queued);

        let first_sender = sender.clone();
        let mut first = Box::pin(first_sender.reserve());
        assert_pending(first.as_mut()).await;
        let second_sender = sender.clone();
        let mut second = Box::pin(second_sender.reserve());
        assert_pending(second.as_mut()).await;

        assert_eq!(receiver.recv().await, Some(node(1)));
        let first_permit = first.await.unwrap();
        assert_pending(second.as_mut()).await;
        assert_eq!(first_permit.deliver(node(2)), DhtDiscoveryOffer::Queued);
        assert_eq!(receiver.recv().await, Some(node(2)));
        let second_permit = second.await.unwrap();
        assert_eq!(second_permit.deliver(node(3)), DhtDiscoveryOffer::Queued);
        assert_eq!(receiver.recv().await, Some(node(3)));
        assert_eq!(
            sender.stats(),
            DhtDiscoveryStats {
                offered: 3,
                queued: 3,
                full_dropped: 0,
                receiver_closed_dropped: 0,
            }
        );
    }

    #[tokio::test]
    async fn closed_before_reservation_is_typed_and_uncounted() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        receiver.close();

        assert!(matches!(
            sender.reserve().await,
            Err(DhtDiscoveryReserveError::ReceiverClosed)
        ));
        assert_eq!(sender.stats(), DhtDiscoveryStats::default());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn closing_receiver_resolves_an_already_pending_reservation_uncounted() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        assert_eq!(sender.offer(node(1)), DhtDiscoveryOffer::Queued);

        let waiting_sender = sender.clone();
        let mut waiting = Box::pin(waiting_sender.reserve());
        assert_pending(waiting.as_mut()).await;
        receiver.close();

        assert!(matches!(
            within(waiting).await,
            Err(DhtDiscoveryReserveError::ReceiverClosed)
        ));
        assert_eq!(
            sender.stats(),
            DhtDiscoveryStats {
                offered: 1,
                queued: 1,
                full_dropped: 0,
                receiver_closed_dropped: 0,
            }
        );
        assert_eq!(receiver.recv().await, Some(node(1)));
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn acquired_permit_delivers_after_close_and_delays_eof() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let stats = sender.stats_handle();
        let permit = sender.reserve().await.unwrap();
        receiver.close();
        drop(sender);

        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        assert_eq!(permit.deliver(node(1)), DhtDiscoveryOffer::Queued);
        assert_eq!(receiver.recv().await, Some(node(1)));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(
            stats.snapshot(),
            DhtDiscoveryStats {
                offered: 1,
                queued: 1,
                full_dropped: 0,
                receiver_closed_dropped: 0,
            }
        );
    }

    #[tokio::test]
    async fn receiver_drop_after_reservation_is_classified_before_commit() {
        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let permit = sender.reserve().await.unwrap();
        drop(receiver);

        assert_eq!(permit.deliver(node(1)), DhtDiscoveryOffer::ReceiverClosed);
        assert_eq!(
            sender.stats(),
            DhtDiscoveryStats {
                offered: 1,
                queued: 0,
                full_dropped: 0,
                receiver_closed_dropped: 1,
            }
        );
    }

    #[tokio::test]
    async fn dropping_an_unused_final_sender_permit_allows_receiver_eof() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let stats = sender.stats_handle();
        let permit = sender.reserve().await.unwrap();
        drop(sender);

        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        drop(permit);
        assert_eq!(within(receiver.recv()).await, None);
        assert_eq!(stats.snapshot(), DhtDiscoveryStats::default());
    }

    #[tokio::test]
    async fn sequential_reservations_preserve_duplicate_bearing_fanout_order() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let expected = [node(1), node(2), node(1), node(3)];
        let mut received = Vec::with_capacity(expected.len());

        let first = sender.reserve().await.unwrap();
        assert_eq!(first.deliver(expected[0]), DhtDiscoveryOffer::Queued);
        for next in expected.iter().copied().skip(1) {
            let waiting_sender = sender.clone();
            let mut waiting = Box::pin(waiting_sender.reserve());
            assert_pending(waiting.as_mut()).await;
            received.push(within(receiver.recv()).await.unwrap());
            let permit = within(waiting).await.unwrap();
            assert_eq!(permit.deliver(next), DhtDiscoveryOffer::Queued);
        }
        received.push(within(receiver.recv()).await.unwrap());

        assert_eq!(received, expected);
        assert_eq!(
            sender.stats(),
            DhtDiscoveryStats {
                offered: 4,
                queued: 4,
                full_dropped: 0,
                receiver_closed_dropped: 0,
            }
        );
    }

    #[tokio::test]
    async fn mixed_offer_and_permit_outcomes_obey_saturating_conservation() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        assert_eq!(sender.offer(node(1)), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(node(2)), DhtDiscoveryOffer::FullDropped);
        assert_eq!(receiver.recv().await, Some(node(1)));

        let permit = sender.reserve().await.unwrap();
        assert_eq!(permit.deliver(node(3)), DhtDiscoveryOffer::Queued);
        assert_eq!(receiver.recv().await, Some(node(3)));
        drop(receiver);
        assert_eq!(sender.offer(node(4)), DhtDiscoveryOffer::ReceiverClosed);
        assert!(matches!(
            sender.reserve().await,
            Err(DhtDiscoveryReserveError::ReceiverClosed)
        ));

        let stats = sender.stats();
        assert_eq!(
            stats,
            DhtDiscoveryStats {
                offered: 4,
                queued: 2,
                full_dropped: 1,
                receiver_closed_dropped: 1,
            }
        );
        assert_eq!(
            stats.offered,
            stats
                .queued
                .saturating_add(stats.full_dropped)
                .saturating_add(stats.receiver_closed_dropped)
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

    #[tokio::test]
    async fn receiver_weak_sender_does_not_delay_eof() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));

        drop(sender);

        assert_eq!(within(receiver.recv()).await, None);
    }

    #[test]
    fn try_sender_recovers_the_exact_channel_stats_and_receiver_state() {
        let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let recovered = receiver
            .try_sender()
            .expect("the original sender keeps the channel open");

        assert_eq!(recovered.offer(node(1)), DhtDiscoveryOffer::Queued);
        assert_eq!(receiver.try_recv().unwrap(), node(1));
        receiver.close();
        assert_eq!(recovered.offer(node(2)), DhtDiscoveryOffer::ReceiverClosed);
        assert_eq!(
            sender.stats(),
            DhtDiscoveryStats {
                offered: 2,
                queued: 1,
                full_dropped: 0,
                receiver_closed_dropped: 1,
            }
        );
        assert_eq!(recovered.stats(), sender.stats());
    }

    #[test]
    fn try_sender_fails_after_the_last_strong_sender_drops() {
        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));

        drop(sender);

        assert!(receiver.try_sender().is_none());
    }

    #[tokio::test]
    async fn offer_and_permit_outcomes_saturate_every_counter() {
        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let permit = sender.reserve().await.unwrap();
        sender
            .stats
            .inner
            .offered
            .store(u64::MAX - 3, Ordering::Relaxed);
        sender
            .stats
            .inner
            .queued
            .store(u64::MAX - 1, Ordering::Relaxed);
        sender
            .stats
            .inner
            .full_dropped
            .store(u64::MAX - 1, Ordering::Relaxed);
        sender
            .stats
            .inner
            .receiver_closed_dropped
            .store(u64::MAX - 1, Ordering::Relaxed);

        assert_eq!(permit.deliver(node(1)), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(node(2)), DhtDiscoveryOffer::FullDropped);
        drop(receiver);
        assert_eq!(sender.offer(node(3)), DhtDiscoveryOffer::ReceiverClosed);
        assert_eq!(
            sender.stats(),
            DhtDiscoveryStats {
                offered: u64::MAX,
                queued: u64::MAX,
                full_dropped: u64::MAX,
                receiver_closed_dropped: u64::MAX,
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
