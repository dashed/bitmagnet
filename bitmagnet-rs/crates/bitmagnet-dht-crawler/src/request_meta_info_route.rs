//! Taskless bounded handoff from successful DHT peer lookup to future
//! metainfo-request work.

use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::num::NonZeroUsize;

use bitmagnet_dht::{Id20, DHT_CHANNEL_MAX_CAPACITY};
use tokio::sync::mpsc;

/// Default capacity of the production request-metainfo route.
pub const DHT_REQUEST_META_INFO_ROUTE_CAPACITY: usize = 100;

/// One info hash, its supplying DHT node, and the ordered peers to try.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtMetaInfoRequest {
    pub info_hash: Id20,
    pub source_node_addr: SocketAddr,
    pub peers: Vec<SocketAddr>,
}

/// Construct the production-default capacity-100 request-metainfo route.
///
/// Construction starts no task. The returned input and receiver own one
/// bounded FIFO shared by every input clone.
#[must_use]
pub fn dht_request_meta_info_channel() -> (DhtRequestMetaInfoInput, DhtRequestMetaInfoReceiver) {
    dht_request_meta_info_channel_with_capacity(
        NonZeroUsize::new(DHT_REQUEST_META_INFO_ROUTE_CAPACITY).unwrap(),
    )
}

/// Construct a request-metainfo route with an explicit positive capacity.
///
/// Construction starts no task. Every returned input clone shares the exact
/// supplied FIFO capacity. The production default is exactly 100.
///
/// # Panics
///
/// Panics before constructing the route if `capacity` exceeds
/// [`DHT_CHANNEL_MAX_CAPACITY`].
#[must_use]
pub fn dht_request_meta_info_channel_with_capacity(
    capacity: NonZeroUsize,
) -> (DhtRequestMetaInfoInput, DhtRequestMetaInfoReceiver) {
    assert!(
        capacity.get() <= DHT_CHANNEL_MAX_CAPACITY,
        "DHT channel capacity {} exceeds Tokio's maximum of {}",
        capacity,
        DHT_CHANNEL_MAX_CAPACITY,
    );
    route(capacity)
}

fn route(capacity: NonZeroUsize) -> (DhtRequestMetaInfoInput, DhtRequestMetaInfoReceiver) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (
        DhtRequestMetaInfoInput { sender },
        DhtRequestMetaInfoReceiver { receiver },
    )
}

/// Cloneable producer capability for request-metainfo work.
///
/// A pending send owns its request outside the queue until it commits, is
/// cancelled, or observes receiver closure. The final input clone keeps
/// receiver EOF pending.
#[derive(Clone)]
pub struct DhtRequestMetaInfoInput {
    sender: mpsc::Sender<DhtMetaInfoRequest>,
}

/// A request rejected because the unique receiver closed or was dropped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtRequestMetaInfoInputClosed {
    request: DhtMetaInfoRequest,
}

impl fmt::Display for DhtRequestMetaInfoInputClosed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the DHT request-metainfo receiver is closed")
    }
}

impl Error for DhtRequestMetaInfoInputClosed {}

impl DhtRequestMetaInfoInputClosed {
    /// Recover the exact request that was not queued.
    #[must_use]
    pub fn into_request(self) -> DhtMetaInfoRequest {
        self.request
    }
}

impl DhtRequestMetaInfoInput {
    /// Wait for shared route capacity and queue one request.
    ///
    /// Cancelling before completion commits nothing. Success is irrevocable;
    /// receiver closure returns the exact unsent request.
    pub async fn send(
        &self,
        request: DhtMetaInfoRequest,
    ) -> Result<(), DhtRequestMetaInfoInputClosed> {
        self.sender
            .send(request)
            .await
            .map_err(|error| DhtRequestMetaInfoInputClosed { request: error.0 })
    }
}

/// Unique consumer for request-metainfo work.
pub struct DhtRequestMetaInfoReceiver {
    receiver: mpsc::Receiver<DhtMetaInfoRequest>,
}

impl DhtRequestMetaInfoReceiver {
    /// Receive the next request in FIFO order.
    ///
    /// Returns `None` only after every input clone is gone, or after explicit
    /// closure, and every request queued before that boundary has drained.
    pub async fn recv(&mut self) -> Option<DhtMetaInfoRequest> {
        self.receiver.recv().await
    }

    /// Receive one currently queued request without waiting.
    pub fn try_recv(&mut self) -> Result<DhtMetaInfoRequest, mpsc::error::TryRecvError> {
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

    fn id(value: u8) -> Id20 {
        let mut bytes = [0_u8; 20];
        bytes[19] = value;
        Id20::from_slice(&bytes).unwrap()
    }

    fn ipv4(value: u8) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(192, 0, 2, value),
            10_000 + u16::from(value),
        ))
    }

    fn ipv6(value: u8) -> SocketAddr {
        SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, u16::from(value)),
            20_000 + u16::from(value),
            u32::from(value),
            u32::from(value) + 1,
        ))
    }

    fn request(value: u8) -> DhtMetaInfoRequest {
        DhtMetaInfoRequest {
            info_hash: id(value),
            source_node_addr: ipv4(value),
            peers: vec![ipv4(value), ipv6(value), ipv4(value)],
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
    fn production_capacity_public_traits_and_taskless_construction_are_exact() {
        assert_eq!(DHT_REQUEST_META_INFO_ROUTE_CAPACITY, 100);
        assert_send_sync::<DhtMetaInfoRequest>();
        assert_send_sync::<DhtRequestMetaInfoInput>();
        assert_send_sync::<DhtRequestMetaInfoInputClosed>();
        assert_send::<DhtRequestMetaInfoReceiver>();

        let (input, mut receiver) = dht_request_meta_info_channel();
        assert_future_send(input.send(request(1)));
        assert_future_send(receiver.recv());
        drop((input, receiver));
    }

    #[test]
    #[should_panic(expected = "exceeds Tokio's maximum")]
    fn over_max_capacity_panics_before_route_construction() {
        let over_max = NonZeroUsize::new(DHT_CHANNEL_MAX_CAPACITY + 1).unwrap();
        let _ = dht_request_meta_info_channel_with_capacity(over_max);
    }

    #[tokio::test]
    async fn explicit_capacity_is_fifo_and_backpressured() {
        let (input, mut receiver) =
            dht_request_meta_info_channel_with_capacity(NonZeroUsize::new(2).unwrap());
        input.send(request(1)).await.unwrap();
        input.send(request(2)).await.unwrap();

        let mut blocked = Box::pin(input.send(request(3)));
        assert_pending(blocked.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(request(1)));
        assert_eq!(blocked.await, Ok(()));
        assert_eq!(receiver.recv().await, Some(request(2)));
        assert_eq!(receiver.recv().await, Some(request(3)));
    }

    #[tokio::test]
    async fn payload_preserves_ipv4_scoped_ipv6_order_and_duplicates() {
        let (input, mut receiver) = route(NonZeroUsize::new(1).unwrap());
        let expected = DhtMetaInfoRequest {
            info_hash: id(7),
            source_node_addr: ipv6(8),
            peers: vec![ipv4(1), ipv6(2), ipv4(1), ipv6(2)],
        };
        input.send(expected.clone()).await.unwrap();
        assert_eq!(receiver.recv().await, Some(expected));
    }

    #[tokio::test]
    async fn production_capacity_is_fifo_and_backpressured() {
        let (input, mut receiver) = dht_request_meta_info_channel();
        for value in 0_u8..100 {
            input.send(request(value)).await.unwrap();
        }

        let mut blocked = Box::pin(input.send(request(100)));
        assert_pending(blocked.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(request(0)));
        assert_eq!(blocked.await, Ok(()));

        for value in 1_u8..=100 {
            assert_eq!(receiver.recv().await, Some(request(value)));
        }
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn cancelling_a_backpressured_send_commits_nothing() {
        let (input, mut receiver) = route(NonZeroUsize::new(1).unwrap());
        input.send(request(1)).await.unwrap();

        let mut cancelled = Box::pin(input.send(request(2)));
        assert_pending(cancelled.as_mut()).await;
        drop(cancelled);

        assert_eq!(receiver.recv().await, Some(request(1)));
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        input.send(request(3)).await.unwrap();
        assert_eq!(receiver.recv().await, Some(request(3)));
    }

    #[tokio::test]
    async fn final_input_clone_controls_drain_then_eof() {
        let (input, mut receiver) = route(NonZeroUsize::new(1).unwrap());
        let clone = input.clone();
        drop(input);

        let mut eof = Box::pin(receiver.recv());
        assert_pending(eof.as_mut()).await;
        drop(clone);
        assert_eq!(eof.await, None);
    }

    #[tokio::test]
    async fn close_recovers_blocked_and_later_requests_then_drains_prefix() {
        let (input, mut receiver) = route(NonZeroUsize::new(1).unwrap());
        let first = request(1);
        let blocked_request = request(2);
        let later = request(3);
        input.send(first.clone()).await.unwrap();

        let mut blocked = Box::pin(input.send(blocked_request.clone()));
        assert_pending(blocked.as_mut()).await;
        receiver.close();

        assert_eq!(blocked.await.unwrap_err().into_request(), blocked_request);
        assert_eq!(
            input.send(later.clone()).await.unwrap_err().into_request(),
            later
        );
        assert_eq!(receiver.recv().await, Some(first));
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn receiver_drop_recovers_the_exact_blocked_request() {
        let (input, receiver) = route(NonZeroUsize::new(1).unwrap());
        input.send(request(1)).await.unwrap();
        let blocked_request = request(2);
        let mut blocked = Box::pin(input.send(blocked_request.clone()));
        assert_pending(blocked.as_mut()).await;

        drop(receiver);
        assert_eq!(blocked.await.unwrap_err().into_request(), blocked_request);
    }
}
