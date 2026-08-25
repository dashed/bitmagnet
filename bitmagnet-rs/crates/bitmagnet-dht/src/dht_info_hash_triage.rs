//! Bounded typed handoff from BEP-51 sample workers to future info-hash triage.
//!
//! The default capacity matches only the Go crawler's default triage input
//! capacity. Accepting an explicit capacity is a Rust composition and test
//! seam, not a claim that arbitrary capacities mirror Go production. This
//! route does not implement Go's 1,000-item batching, 20-second flush, buffered
//! batch output, spawned batcher, or total-retention behavior.
//!
//! A producer-side closure waiter is deliberately deferred until a consumer
//! needs that lifecycle boundary; send failures and receiver EOF fully define
//! this taskless route today.

use std::net::SocketAddr;
use std::num::NonZeroUsize;

use thiserror::Error;
use tokio::sync::mpsc;

use crate::{assert_dht_channel_capacity, Id20};

/// Default capacity of the bounded info-hash triage input queue.
///
/// This matches the Go crawler's default input capacity, not its batching
/// layer's total retention. The capacity bounds queued requests. Each pending
/// send owns one additional request outside the queue until it commits, is
/// cancelled, or observes receiver closure.
pub const DHT_INFO_HASH_TRIAGE_DEFAULT_CAPACITY: usize = 100;

/// One newly observed info hash and the DHT node that supplied it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtInfoHashTriageRequest {
    pub info_hash: Id20,
    pub source_node_addr: SocketAddr,
}

/// Construct one bounded, single-consumer info-hash triage route.
///
/// The explicit capacity applies to one shared FIFO across every input clone.
/// Construction starts no task. Dropping a pending send commits nothing;
/// receiver closure returns the exact unsent request.
///
/// # Panics
///
/// Panics before constructing the route if `capacity` exceeds
/// [`crate::DHT_CHANNEL_MAX_CAPACITY`].
#[must_use]
pub fn dht_info_hash_triage_channel(
    capacity: NonZeroUsize,
) -> (DhtInfoHashTriageInput, DhtInfoHashTriageReceiver) {
    assert_dht_channel_capacity(capacity);
    let (sender, receiver) = mpsc::channel(capacity.get());
    (
        DhtInfoHashTriageInput { sender },
        DhtInfoHashTriageReceiver { receiver },
    )
}

/// Cloneable producer capability for the bounded info-hash triage route.
///
/// Cloning creates neither a queue nor a task. Every clone shares capacity and
/// keeps receiver EOF pending until the clone is dropped or the receiver is
/// closed.
#[derive(Clone)]
pub struct DhtInfoHashTriageInput {
    sender: mpsc::Sender<DhtInfoHashTriageRequest>,
}

/// An info-hash triage request that could not be queued because the unique
/// receiver closed or was dropped.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("the DHT info-hash triage receiver is closed")]
pub struct DhtInfoHashTriageInputClosed {
    request: DhtInfoHashTriageRequest,
}

impl DhtInfoHashTriageInputClosed {
    /// Recover the exact request that was not queued.
    #[must_use]
    pub fn into_request(self) -> DhtInfoHashTriageRequest {
        self.request
    }
}

impl DhtInfoHashTriageInput {
    /// Wait for shared route capacity and queue one request.
    ///
    /// Cancelling this future before completion commits nothing and loses the
    /// sender's place among capacity waiters. Success means the request entered
    /// the queue and cannot be revoked. Receiver closure recovers the exact
    /// unsent request.
    pub async fn send(
        &self,
        request: DhtInfoHashTriageRequest,
    ) -> Result<(), DhtInfoHashTriageInputClosed> {
        self.sender
            .send(request)
            .await
            .map_err(|error| DhtInfoHashTriageInputClosed { request: error.0 })
    }
}

/// Unique consumer for the bounded info-hash triage route.
pub struct DhtInfoHashTriageReceiver {
    receiver: mpsc::Receiver<DhtInfoHashTriageRequest>,
}

impl DhtInfoHashTriageReceiver {
    /// Receive the next queued request.
    ///
    /// Returns `None` only after every input clone is gone, or after explicit
    /// closure, and all requests queued before that boundary have drained.
    pub async fn recv(&mut self) -> Option<DhtInfoHashTriageRequest> {
        self.receiver.recv().await
    }

    /// Receive one currently queued request without waiting.
    pub fn try_recv(&mut self) -> Result<DhtInfoHashTriageRequest, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    /// Reject later sends while retaining already queued requests to drain.
    pub fn close(&mut self) {
        self.receiver.close();
    }
}

#[cfg(test)]
mod tests {
    use std::future::{poll_fn, Future};
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
    use std::pin::Pin;
    use std::task::Poll;

    use super::*;

    fn request(value: u8) -> DhtInfoHashTriageRequest {
        let mut bytes = [0_u8; 20];
        bytes[19] = value;
        DhtInfoHashTriageRequest {
            info_hash: Id20::from_slice(&bytes).unwrap(),
            source_node_addr: SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                10_000 + u16::from(value),
            )),
        }
    }

    fn ipv6_request(value: u8) -> DhtInfoHashTriageRequest {
        let mut bytes = [0_u8; 20];
        bytes[0] = value;
        DhtInfoHashTriageRequest {
            info_hash: Id20::from_slice(&bytes).unwrap(),
            source_node_addr: SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::LOCALHOST,
                20_000 + u16::from(value),
                u32::from(value),
                u32::from(value) + 1,
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

    fn assert_send_sync<T: Send + Sync>() {}

    fn assert_send<T: Send>() {}

    fn assert_future_send<F: Future + Send>(future: F) {
        drop(future);
    }

    #[test]
    fn default_capacity_and_public_types_are_exact_and_taskless() {
        assert_eq!(DHT_INFO_HASH_TRIAGE_DEFAULT_CAPACITY, 100);
        assert_eq!(
            NonZeroUsize::new(DHT_INFO_HASH_TRIAGE_DEFAULT_CAPACITY)
                .unwrap()
                .get(),
            100
        );
        assert_send_sync::<DhtInfoHashTriageRequest>();
        assert_send_sync::<DhtInfoHashTriageInput>();
        assert_send_sync::<DhtInfoHashTriageInputClosed>();
        assert_send::<DhtInfoHashTriageReceiver>();

        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        drop(input);
        drop(receiver);
    }

    #[test]
    #[should_panic(expected = "exceeds Tokio's maximum")]
    fn over_max_capacity_panics_before_route_construction() {
        let over_max = NonZeroUsize::new(crate::DHT_CHANNEL_MAX_CAPACITY + 1).unwrap();
        let _ = dht_info_hash_triage_channel(over_max);
    }

    #[tokio::test]
    async fn request_preserves_exact_hash_and_ipv4_or_ipv6_source_address() {
        let (input, mut receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(2).unwrap());
        let ipv4 = request(1);
        let ipv6 = ipv6_request(2);

        input.send(ipv4).await.unwrap();
        input.send(ipv6).await.unwrap();

        assert_eq!(receiver.recv().await, Some(ipv4));
        assert_eq!(receiver.recv().await, Some(ipv6));
    }

    #[tokio::test]
    async fn explicit_capacity_backpressures_and_fifo_resumes_after_drain() {
        let (input, mut receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(2).unwrap());
        input.send(request(1)).await.unwrap();
        input.send(request(2)).await.unwrap();

        let mut third = Box::pin(input.send(request(3)));
        assert_pending(third.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(request(1)));
        assert_eq!(third.await, Ok(()));
        assert_eq!(receiver.recv().await, Some(request(2)));
        assert_eq!(receiver.recv().await, Some(request(3)));
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn default_capacity_holds_exact_hundred_prefix_and_cancelled_101st_commits_nothing() {
        let (input, mut receiver) = dht_info_hash_triage_channel(
            NonZeroUsize::new(DHT_INFO_HASH_TRIAGE_DEFAULT_CAPACITY).unwrap(),
        );
        for value in 0_u8..100 {
            input.send(request(value)).await.unwrap();
        }

        let mut cancelled = Box::pin(input.send(request(100)));
        assert_pending(cancelled.as_mut()).await;
        drop(cancelled);

        for value in 0_u8..100 {
            assert_eq!(receiver.recv().await, Some(request(value)));
        }
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn cancelling_blocked_send_commits_nothing_and_releases_waiter() {
        let (input, mut receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        input.send(request(1)).await.unwrap();

        let mut cancelled = Box::pin(input.send(request(2)));
        assert_pending(cancelled.as_mut()).await;
        drop(cancelled);

        assert_eq!(receiver.recv().await, Some(request(1)));
        input.send(request(3)).await.unwrap();
        assert_eq!(receiver.recv().await, Some(request(3)));
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn cancelling_middle_waiter_preserves_remaining_waiter_order() {
        let (input, mut receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        input.send(request(0)).await.unwrap();

        let mut first_waiter = Box::pin(input.send(request(1)));
        assert_pending(first_waiter.as_mut()).await;
        let mut middle_waiter = Box::pin(input.send(request(2)));
        assert_pending(middle_waiter.as_mut()).await;
        let mut last_waiter = Box::pin(input.send(request(3)));
        assert_pending(last_waiter.as_mut()).await;
        drop(middle_waiter);

        assert_eq!(receiver.recv().await, Some(request(0)));
        assert_eq!(first_waiter.await, Ok(()));
        assert_eq!(receiver.recv().await, Some(request(1)));
        assert_eq!(last_waiter.await, Ok(()));
        assert_eq!(receiver.recv().await, Some(request(3)));
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn close_with_live_clone_recovers_blocked_and_later_sends_then_drains_prefix() {
        let (input, mut receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(2).unwrap());
        let clone = input.clone();
        let queued_first = ipv6_request(1);
        let queued_second = ipv6_request(2);
        input.send(queued_first).await.unwrap();
        input.send(queued_second).await.unwrap();

        let blocked = ipv6_request(3);
        let mut send = Box::pin(input.send(blocked));
        assert_pending(send.as_mut()).await;
        receiver.close();

        assert_eq!(send.await.unwrap_err().into_request(), blocked);
        let later = ipv6_request(4);
        assert_eq!(clone.send(later).await.unwrap_err().into_request(), later);
        assert_eq!(receiver.recv().await, Some(queued_first));
        assert_eq!(receiver.recv().await, Some(queued_second));
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn receiver_drop_wakes_blocked_send_and_recovers_exact_request() {
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        input.send(request(1)).await.unwrap();
        let blocked = ipv6_request(2);

        let mut send = Box::pin(input.send(blocked));
        assert_pending(send.as_mut()).await;
        drop(receiver);

        assert_eq!(send.await.unwrap_err().into_request(), blocked);
    }

    #[tokio::test]
    async fn send_after_receiver_drop_recovers_exact_request() {
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        let unsent = ipv6_request(9);
        drop(receiver);

        assert_eq!(input.send(unsent).await.unwrap_err().into_request(), unsent);
    }

    #[tokio::test]
    async fn successful_send_is_irrevocable_and_explicit_close_drains_it() {
        let (input, mut receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        let committed = request(1);
        input.send(committed).await.unwrap();

        receiver.close();
        assert_eq!(receiver.recv().await, Some(committed));
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn every_input_clone_extends_lazy_eof() {
        let (input, mut receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        let last_input = input.clone();
        drop(input);

        let mut eof = Box::pin(receiver.recv());
        assert_pending(eof.as_mut()).await;
        drop(last_input);
        assert_eq!(eof.await, None);
    }

    #[tokio::test]
    async fn cancelling_pending_receive_does_not_consume_the_next_request() {
        let (input, mut receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());

        let mut cancelled = Box::pin(receiver.recv());
        assert_pending(cancelled.as_mut()).await;
        drop(cancelled);

        let next = request(4);
        input.send(next).await.unwrap();
        assert_eq!(receiver.recv().await, Some(next));
    }

    #[tokio::test]
    async fn try_recv_distinguishes_empty_item_and_disconnected() {
        let (input, mut receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));

        let queued = request(5);
        input.send(queued).await.unwrap();
        assert_eq!(receiver.try_recv(), Ok(queued));

        drop(input);
        assert_eq!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        );
    }

    #[tokio::test]
    async fn send_and_receive_futures_are_send() {
        let (input, mut receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        assert_future_send(input.send(request(1)));
        assert_future_send(receiver.recv());
    }
}
