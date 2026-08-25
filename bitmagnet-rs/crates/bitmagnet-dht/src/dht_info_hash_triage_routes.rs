//! Typed bounded outputs for the info-hash triage decision stage.
//!
//! Both production routes have capacity 100, matching the Go crawler's
//! independently buffered `getPeers` and `scrape` inputs. They share only the
//! request type: each route owns a distinct queue and capacity budget. The
//! routes start no tasks and do not implement either downstream worker.

use std::num::NonZeroUsize;

use thiserror::Error;
use tokio::sync::mpsc;

use crate::DhtInfoHashTriageRequest;

/// Fixed capacity of the production get-peers route.
pub const DHT_GET_PEERS_ROUTE_CAPACITY: usize = 100;

/// Fixed capacity of the production scrape route.
pub const DHT_SCRAPE_ROUTE_CAPACITY: usize = 100;

/// Construct the fixed-capacity production get-peers route.
///
/// Construction starts no task. The returned input and receiver own one
/// bounded FIFO dedicated to get-peers work.
#[must_use]
pub fn dht_get_peers_channel() -> (DhtGetPeersInput, DhtGetPeersReceiver) {
    let (input, receiver) = route(NonZeroUsize::new(DHT_GET_PEERS_ROUTE_CAPACITY).unwrap());
    (DhtGetPeersInput { input }, DhtGetPeersReceiver { receiver })
}

/// Construct the fixed-capacity production scrape route.
///
/// Construction starts no task. The returned input and receiver own one
/// bounded FIFO dedicated to scrape work.
#[must_use]
pub fn dht_scrape_channel() -> (DhtScrapeInput, DhtScrapeReceiver) {
    let (input, receiver) = route(NonZeroUsize::new(DHT_SCRAPE_ROUTE_CAPACITY).unwrap());
    (DhtScrapeInput { input }, DhtScrapeReceiver { receiver })
}

#[derive(Clone)]
struct RouteInput {
    sender: mpsc::Sender<DhtInfoHashTriageRequest>,
}

struct RouteReceiver {
    receiver: mpsc::Receiver<DhtInfoHashTriageRequest>,
}

fn route(capacity: NonZeroUsize) -> (RouteInput, RouteReceiver) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (RouteInput { sender }, RouteReceiver { receiver })
}

impl RouteInput {
    async fn send(
        &self,
        request: DhtInfoHashTriageRequest,
    ) -> Result<(), DhtInfoHashTriageRequest> {
        self.sender.send(request).await.map_err(|error| error.0)
    }
}

impl RouteReceiver {
    async fn recv(&mut self) -> Option<DhtInfoHashTriageRequest> {
        self.receiver.recv().await
    }

    fn try_recv(&mut self) -> Result<DhtInfoHashTriageRequest, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    fn close(&mut self) {
        self.receiver.close();
    }
}

/// Cloneable producer capability for the bounded get-peers route.
///
/// Every clone shares one capacity-100 FIFO. A pending send owns its request
/// outside the queue until it commits, is cancelled, or observes receiver
/// closure. Competing sends register in Tokio's FIFO waiter queue, with no
/// producer priority promised. The final live clone keeps receiver EOF
/// pending.
#[derive(Clone)]
pub struct DhtGetPeersInput {
    input: RouteInput,
}

/// A get-peers request rejected because the unique receiver closed or was
/// dropped.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("the DHT get-peers receiver is closed")]
pub struct DhtGetPeersInputClosed {
    request: DhtInfoHashTriageRequest,
}

impl DhtGetPeersInputClosed {
    /// Recover the exact request that was not queued.
    #[must_use]
    pub fn into_request(self) -> DhtInfoHashTriageRequest {
        self.request
    }
}

impl DhtGetPeersInput {
    /// Wait for shared route capacity and queue one request.
    ///
    /// Cancelling before completion commits nothing. Success is irrevocable;
    /// receiver closure instead returns the exact unsent request.
    pub async fn send(
        &self,
        request: DhtInfoHashTriageRequest,
    ) -> Result<(), DhtGetPeersInputClosed> {
        self.input
            .send(request)
            .await
            .map_err(|request| DhtGetPeersInputClosed { request })
    }
}

/// Unique consumer for the bounded get-peers route.
pub struct DhtGetPeersReceiver {
    receiver: RouteReceiver,
}

impl DhtGetPeersReceiver {
    /// Receive the next request in FIFO order.
    ///
    /// Returns `None` only after every input clone is gone, or after explicit
    /// closure, and every request queued before that boundary has drained.
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

/// Cloneable producer capability for the bounded scrape route.
///
/// Every clone shares one capacity-100 FIFO. A pending send owns its request
/// outside the queue until it commits, is cancelled, or observes receiver
/// closure. Competing sends register in Tokio's FIFO waiter queue, with no
/// producer priority promised. The final live clone keeps receiver EOF
/// pending.
#[derive(Clone)]
pub struct DhtScrapeInput {
    input: RouteInput,
}

/// A scrape request rejected because the unique receiver closed or was
/// dropped.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("the DHT scrape receiver is closed")]
pub struct DhtScrapeInputClosed {
    request: DhtInfoHashTriageRequest,
}

impl DhtScrapeInputClosed {
    /// Recover the exact request that was not queued.
    #[must_use]
    pub fn into_request(self) -> DhtInfoHashTriageRequest {
        self.request
    }
}

impl DhtScrapeInput {
    /// Wait for shared route capacity and queue one request.
    ///
    /// Cancelling before completion commits nothing. Success is irrevocable;
    /// receiver closure instead returns the exact unsent request.
    pub async fn send(
        &self,
        request: DhtInfoHashTriageRequest,
    ) -> Result<(), DhtScrapeInputClosed> {
        self.input
            .send(request)
            .await
            .map_err(|request| DhtScrapeInputClosed { request })
    }
}

/// Unique consumer for the bounded scrape route.
pub struct DhtScrapeReceiver {
    receiver: RouteReceiver,
}

impl DhtScrapeReceiver {
    /// Receive the next request in FIFO order.
    ///
    /// Returns `None` only after every input clone is gone, or after explicit
    /// closure, and every request queued before that boundary has drained.
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
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
    use std::pin::Pin;
    use std::task::Poll;

    use super::*;
    use crate::Id20;

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

    fn route_with_capacity(capacity: usize) -> (RouteInput, RouteReceiver) {
        route(NonZeroUsize::new(capacity).unwrap())
    }

    fn assert_send_sync<T: Send + Sync>() {}

    fn assert_send<T: Send>() {}

    fn assert_future_send<F: Future + Send>(future: F) {
        drop(future);
    }

    #[test]
    fn production_capacities_and_nominal_public_types_are_exact_and_taskless() {
        assert_eq!(DHT_GET_PEERS_ROUTE_CAPACITY, 100);
        assert_eq!(DHT_SCRAPE_ROUTE_CAPACITY, 100);
        assert_send_sync::<DhtGetPeersInput>();
        assert_send_sync::<DhtGetPeersInputClosed>();
        assert_send::<DhtGetPeersReceiver>();
        assert_send_sync::<DhtScrapeInput>();
        assert_send_sync::<DhtScrapeInputClosed>();
        assert_send::<DhtScrapeReceiver>();

        let (get_peers, get_peers_receiver) = dht_get_peers_channel();
        let (scrape, scrape_receiver) = dht_scrape_channel();
        drop((get_peers, get_peers_receiver, scrape, scrape_receiver));
    }

    #[tokio::test]
    async fn each_production_route_has_its_own_exact_hundred_item_capacity() {
        let (get_peers, mut get_peers_receiver) = dht_get_peers_channel();
        let (scrape, mut scrape_receiver) = dht_scrape_channel();
        for value in 0_u8..100 {
            get_peers.send(request(value)).await.unwrap();
            scrape.send(ipv6_request(value)).await.unwrap();
        }

        let mut blocked_get_peers = Box::pin(get_peers.send(request(100)));
        let mut blocked_scrape = Box::pin(scrape.send(ipv6_request(100)));
        assert_pending(blocked_get_peers.as_mut()).await;
        assert_pending(blocked_scrape.as_mut()).await;

        assert_eq!(get_peers_receiver.recv().await, Some(request(0)));
        assert_eq!(blocked_get_peers.await, Ok(()));
        assert_eq!(scrape_receiver.recv().await, Some(ipv6_request(0)));
        assert_eq!(blocked_scrape.await, Ok(()));

        for value in 1_u8..=100 {
            assert_eq!(get_peers_receiver.recv().await, Some(request(value)));
            assert_eq!(scrape_receiver.recv().await, Some(ipv6_request(value)));
        }
    }

    #[tokio::test]
    async fn shared_route_core_is_fifo_backpressured_and_send_cancellation_safe() {
        let (input, mut receiver) = route_with_capacity(2);
        input.send(request(1)).await.unwrap();
        input.send(request(2)).await.unwrap();

        let mut cancelled = Box::pin(input.send(request(3)));
        assert_pending(cancelled.as_mut()).await;
        drop(cancelled);

        assert_eq!(receiver.recv().await, Some(request(1)));
        input.send(request(4)).await.unwrap();
        assert_eq!(receiver.recv().await, Some(request(2)));
        assert_eq!(receiver.recv().await, Some(request(4)));
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn get_peers_close_recovers_exact_blocked_and_later_requests_then_drains_prefix() {
        let (input, receiver) = route_with_capacity(2);
        let get_peers = DhtGetPeersInput { input };
        let clone = get_peers.clone();
        let mut receiver = DhtGetPeersReceiver { receiver };
        let first = request(1);
        let second = ipv6_request(2);
        get_peers.send(first).await.unwrap();
        get_peers.send(second).await.unwrap();

        let blocked = ipv6_request(3);
        let mut send = Box::pin(get_peers.send(blocked));
        assert_pending(send.as_mut()).await;
        receiver.close();

        assert_eq!(send.await.unwrap_err().into_request(), blocked);
        let later = request(4);
        assert_eq!(clone.send(later).await.unwrap_err().into_request(), later);
        assert_eq!(receiver.recv().await, Some(first));
        assert_eq!(receiver.recv().await, Some(second));
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn scrape_receiver_drop_recovers_exact_blocked_and_later_requests() {
        let (input, receiver) = route_with_capacity(1);
        let scrape = DhtScrapeInput { input };
        let receiver = DhtScrapeReceiver { receiver };
        scrape.send(request(1)).await.unwrap();

        let blocked = ipv6_request(2);
        let mut send = Box::pin(scrape.send(blocked));
        assert_pending(send.as_mut()).await;
        drop(receiver);

        assert_eq!(send.await.unwrap_err().into_request(), blocked);
        let later = request(3);
        assert_eq!(scrape.send(later).await.unwrap_err().into_request(), later);
    }

    #[tokio::test]
    async fn final_get_peers_clone_controls_lazy_eof_after_fifo_drain() {
        let (input, receiver) = route_with_capacity(1);
        let get_peers = DhtGetPeersInput { input };
        let last = get_peers.clone();
        let mut receiver = DhtGetPeersReceiver { receiver };
        get_peers.send(request(7)).await.unwrap();
        drop(get_peers);

        assert_eq!(receiver.recv().await, Some(request(7)));
        let mut eof = Box::pin(receiver.recv());
        assert_pending(eof.as_mut()).await;
        drop(last);
        assert_eq!(eof.await, None);
    }

    #[tokio::test]
    async fn scrape_try_recv_distinguishes_empty_item_and_disconnected() {
        let (scrape, mut receiver) = dht_scrape_channel();
        assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));

        let queued = ipv6_request(9);
        scrape.send(queued).await.unwrap();
        assert_eq!(receiver.try_recv(), Ok(queued));
        drop(scrape);
        assert_eq!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        );
    }

    #[tokio::test]
    async fn all_public_send_and_receive_futures_are_send() {
        let (get_peers, mut get_peers_receiver) = dht_get_peers_channel();
        let (scrape, mut scrape_receiver) = dht_scrape_channel();
        assert_future_send(get_peers.send(request(1)));
        assert_future_send(get_peers_receiver.recv());
        assert_future_send(scrape.send(request(2)));
        assert_future_send(scrape_receiver.recv());
    }
}
