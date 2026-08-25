//! Taskless bounded handoff from successful BEP-33 scrape work to future
//! source persistence.

use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::num::NonZeroUsize;

use bitmagnet_dht::{Id20, ScrapeBloomFilter};
use tokio::sync::mpsc;

/// Fixed capacity of the production scraped-source handoff.
pub const DHT_PERSIST_SOURCE_ROUTE_CAPACITY: usize = 1_000;

/// One info hash, its supplying DHT node, and the exact BEP-33 filters.
///
/// `peers_bloom` is the filter later projected as the DHT leecher count. This
/// route deliberately retains both raw filters and performs no count
/// projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtPersistSourceRequest {
    pub info_hash: Id20,
    pub source_node_addr: SocketAddr,
    pub seeders_bloom: ScrapeBloomFilter,
    pub peers_bloom: ScrapeBloomFilter,
}

/// Construct the fixed-capacity scraped-source route.
///
/// Construction starts no task. The returned input and receiver own one
/// bounded FIFO shared by every input clone. This route neither batches nor
/// implements persistence.
#[must_use]
pub fn dht_persist_source_channel() -> (DhtPersistSourceInput, DhtPersistSourceReceiver) {
    route(NonZeroUsize::new(DHT_PERSIST_SOURCE_ROUTE_CAPACITY).unwrap())
}

fn route(capacity: NonZeroUsize) -> (DhtPersistSourceInput, DhtPersistSourceReceiver) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (
        DhtPersistSourceInput { sender },
        DhtPersistSourceReceiver { receiver },
    )
}

/// Cloneable producer capability for scraped-source persistence work.
///
/// A pending send owns its request outside the queue until it commits, is
/// cancelled, or observes receiver closure. The final input clone keeps
/// receiver EOF pending.
#[derive(Clone)]
pub struct DhtPersistSourceInput {
    sender: mpsc::Sender<DhtPersistSourceRequest>,
}

/// A request rejected because the unique receiver closed or was dropped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtPersistSourceInputClosed {
    request: DhtPersistSourceRequest,
}

impl fmt::Display for DhtPersistSourceInputClosed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the DHT scraped-source receiver is closed")
    }
}

impl Error for DhtPersistSourceInputClosed {}

impl DhtPersistSourceInputClosed {
    /// Recover the exact request that was not queued.
    #[must_use]
    pub fn into_request(self) -> DhtPersistSourceRequest {
        self.request
    }
}

impl DhtPersistSourceInput {
    /// Wait for shared route capacity and queue one request.
    ///
    /// Cancelling before completion commits nothing. Success is irrevocable;
    /// receiver closure returns the exact unsent request.
    pub async fn send(
        &self,
        request: DhtPersistSourceRequest,
    ) -> Result<(), DhtPersistSourceInputClosed> {
        self.sender
            .send(request)
            .await
            .map_err(|error| DhtPersistSourceInputClosed { request: error.0 })
    }
}

/// Unique consumer for scraped-source persistence work.
pub struct DhtPersistSourceReceiver {
    receiver: mpsc::Receiver<DhtPersistSourceRequest>,
}

impl DhtPersistSourceReceiver {
    /// Receive the next request in FIFO order.
    ///
    /// Returns `None` only after every input clone is gone, or after explicit
    /// closure, and every request queued before that boundary has drained.
    pub async fn recv(&mut self) -> Option<DhtPersistSourceRequest> {
        self.receiver.recv().await
    }

    /// Receive one currently queued request without waiting.
    pub fn try_recv(&mut self) -> Result<DhtPersistSourceRequest, mpsc::error::TryRecvError> {
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

    fn id(value: u16) -> Id20 {
        let mut bytes = [0_u8; 20];
        bytes[18..].copy_from_slice(&value.to_be_bytes());
        Id20::from_slice(&bytes).unwrap()
    }

    fn ipv4(value: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(192, 0, (value >> 8) as u8, value as u8),
            10_000 + value,
        ))
    }

    fn scoped_ipv6(value: u16, scope_id: u32) -> SocketAddr {
        SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, value),
            20_000 + value,
            u32::from(value),
            scope_id,
        ))
    }

    fn repeated_bloom(value: u8) -> ScrapeBloomFilter {
        ScrapeBloomFilter::from([value; 256])
    }

    fn request(value: u16) -> DhtPersistSourceRequest {
        DhtPersistSourceRequest {
            info_hash: id(value),
            source_node_addr: ipv4(value),
            seeders_bloom: repeated_bloom(value as u8),
            peers_bloom: repeated_bloom((value >> 8) as u8),
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
        assert_eq!(DHT_PERSIST_SOURCE_ROUTE_CAPACITY, 1_000);
        assert_send_sync::<DhtPersistSourceRequest>();
        assert_send_sync::<DhtPersistSourceInput>();
        assert_send_sync::<DhtPersistSourceInputClosed>();
        assert_send::<DhtPersistSourceReceiver>();

        let (input, mut receiver) = dht_persist_source_channel();
        assert_future_send(input.send(request(1)));
        assert_future_send(receiver.recv());
        drop((input, receiver));
    }

    #[tokio::test]
    async fn payload_preserves_raw_empty_and_patterned_blooms_and_scoped_source() {
        let (input, mut receiver) = route(NonZeroUsize::new(1).unwrap());
        let seeders_pattern =
            std::array::from_fn(|index| (index as u8).wrapping_mul(31).wrapping_add(7));
        let peers_pattern = std::array::from_fn(|index| 255_u8.wrapping_sub(index as u8));
        let expected = DhtPersistSourceRequest {
            info_hash: id(7),
            source_node_addr: scoped_ipv6(8, 42),
            seeders_bloom: ScrapeBloomFilter::from(seeders_pattern),
            peers_bloom: ScrapeBloomFilter::from(peers_pattern),
        };
        input.send(expected.clone()).await.unwrap();
        let actual = receiver.recv().await.unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.seeders_bloom.as_bytes(), &seeders_pattern);
        assert_eq!(actual.peers_bloom.as_bytes(), &peers_pattern);

        let empty = DhtPersistSourceRequest {
            info_hash: id(9),
            source_node_addr: scoped_ipv6(10, 43),
            seeders_bloom: ScrapeBloomFilter::EMPTY,
            peers_bloom: ScrapeBloomFilter::EMPTY,
        };
        input.send(empty.clone()).await.unwrap();
        assert_eq!(receiver.recv().await, Some(empty));
    }

    #[tokio::test]
    async fn production_capacity_is_fifo_and_backpressured() {
        let (input, mut receiver) = dht_persist_source_channel();
        for value in 0_u16..1_000 {
            input.send(request(value)).await.unwrap();
        }

        let mut blocked = Box::pin(input.send(request(1_000)));
        assert_pending(blocked.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(request(0)));
        assert_eq!(blocked.await, Ok(()));

        for value in 1_u16..=1_000 {
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
