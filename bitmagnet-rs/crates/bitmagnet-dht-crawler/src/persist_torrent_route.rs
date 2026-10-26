//! Taskless bounded handoff from successful metainfo requests to future
//! torrent persistence.

use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bitmagnet_dht::Id20;
use bitmagnet_metainfo::ParsedInfo;
use tokio::sync::mpsc;

/// Fixed capacity of the production torrent-persistence route.
pub const DHT_PERSIST_TORRENT_ROUTE_CAPACITY: usize = 1_000;

/// One requested info hash, its supplying DHT node, and verified metainfo.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtPersistTorrentRequest {
    pub info_hash: Id20,
    pub source_node_addr: SocketAddr,
    pub meta_info: Arc<ParsedInfo>,
}

/// Construct the fixed-capacity torrent-persistence route.
///
/// Construction starts no task. The returned input and receiver own one
/// bounded FIFO shared by every input clone. This route neither batches nor
/// implements persistence.
#[must_use]
pub fn dht_persist_torrent_channel() -> (DhtPersistTorrentInput, DhtPersistTorrentReceiver) {
    route(NonZeroUsize::new(DHT_PERSIST_TORRENT_ROUTE_CAPACITY).unwrap())
}

fn route(capacity: NonZeroUsize) -> (DhtPersistTorrentInput, DhtPersistTorrentReceiver) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (
        DhtPersistTorrentInput { sender },
        DhtPersistTorrentReceiver { receiver },
    )
}

/// Cloneable producer capability for torrent-persistence work.
///
/// A pending send owns its request outside the queue until it commits, is
/// cancelled, or observes receiver closure. The final input clone keeps
/// receiver EOF pending.
#[derive(Clone)]
pub struct DhtPersistTorrentInput {
    sender: mpsc::Sender<DhtPersistTorrentRequest>,
}

/// A request rejected because the unique receiver closed or was dropped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtPersistTorrentInputClosed {
    request: DhtPersistTorrentRequest,
}

impl fmt::Display for DhtPersistTorrentInputClosed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the DHT torrent-persistence receiver is closed")
    }
}

impl Error for DhtPersistTorrentInputClosed {}

impl DhtPersistTorrentInputClosed {
    /// Recover the exact request that was not queued.
    #[must_use]
    pub fn into_request(self) -> DhtPersistTorrentRequest {
        self.request
    }
}

impl DhtPersistTorrentInput {
    /// Wait for shared route capacity and queue one request.
    ///
    /// Cancelling before completion commits nothing. Success is irrevocable;
    /// receiver closure returns the exact unsent request.
    pub async fn send(
        &self,
        request: DhtPersistTorrentRequest,
    ) -> Result<(), DhtPersistTorrentInputClosed> {
        self.sender
            .send(request)
            .await
            .map_err(|error| DhtPersistTorrentInputClosed { request: error.0 })
    }
}

/// Unique consumer for torrent-persistence work.
pub struct DhtPersistTorrentReceiver {
    receiver: mpsc::Receiver<DhtPersistTorrentRequest>,
}

impl DhtPersistTorrentReceiver {
    /// Receive the next request in FIFO order.
    ///
    /// Returns `None` only after every input clone is gone, or after explicit
    /// closure, and every request queued before that boundary has drained.
    pub async fn recv(&mut self) -> Option<DhtPersistTorrentRequest> {
        self.receiver.recv().await
    }

    /// Receive one currently queued request without waiting.
    pub fn try_recv(&mut self) -> Result<DhtPersistTorrentRequest, mpsc::error::TryRecvError> {
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

    use bitmagnet_metainfo::parse_info_bytes;

    use super::*;

    const SYNTHETIC_V1_HASH: [u8; 20] = [
        0x34, 0x5b, 0x04, 0xb6, 0x0b, 0x9a, 0xfe, 0xb8, 0xd1, 0xe1, 0x20, 0x9c, 0x19, 0xb0, 0xf6,
        0x25, 0xb3, 0xe7, 0xa8, 0xf8,
    ];

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

    fn parsed_synthetic_v1() -> Arc<ParsedInfo> {
        let mut raw =
            b"d6:lengthi4096e4:name20:synthetic-single.bin12:piece lengthi32768e6:pieces20:"
                .to_vec();
        raw.extend_from_slice(&[0; 20]);
        raw.push(b'e');
        Arc::new(parse_info_bytes(SYNTHETIC_V1_HASH, &raw).unwrap())
    }

    fn request(value: u16) -> DhtPersistTorrentRequest {
        DhtPersistTorrentRequest {
            info_hash: Id20::from_slice(&SYNTHETIC_V1_HASH).unwrap(),
            source_node_addr: ipv4(value),
            meta_info: parsed_synthetic_v1(),
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
        assert_eq!(DHT_PERSIST_TORRENT_ROUTE_CAPACITY, 1_000);
        assert_send_sync::<DhtPersistTorrentRequest>();
        assert_send_sync::<DhtPersistTorrentInput>();
        assert_send_sync::<DhtPersistTorrentInputClosed>();
        assert_send::<DhtPersistTorrentReceiver>();

        let (input, mut receiver) = dht_persist_torrent_channel();
        assert_future_send(input.send(request(1)));
        assert_future_send(receiver.recv());
        drop((input, receiver));
    }

    #[tokio::test]
    async fn payload_preserves_hash_scoped_source_and_parsed_info_allocation() {
        let (input, mut receiver) = route(NonZeroUsize::new(1).unwrap());
        let expected = DhtPersistTorrentRequest {
            info_hash: Id20::from_slice(&SYNTHETIC_V1_HASH).unwrap(),
            source_node_addr: scoped_ipv6(8, 42),
            meta_info: parsed_synthetic_v1(),
        };
        input.send(expected.clone()).await.unwrap();
        let actual = receiver.recv().await.unwrap();

        assert_eq!(actual, expected);
        assert!(Arc::ptr_eq(&actual.meta_info, &expected.meta_info));
        assert_eq!(actual.meta_info.info_hash_v1(), Some(SYNTHETIC_V1_HASH));
        assert_eq!(actual.meta_info.info_hash_v2(), None);
    }

    #[tokio::test]
    async fn production_capacity_is_fifo_and_backpressured() {
        let (input, mut receiver) = dht_persist_torrent_channel();
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
        input.send(request(1)).await.unwrap();
        drop(input);

        assert_eq!(receiver.recv().await, Some(request(1)));
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

        let recovered = blocked.await.unwrap_err().into_request();
        assert_eq!(recovered, blocked_request);
        assert!(Arc::ptr_eq(
            &recovered.meta_info,
            &blocked_request.meta_info
        ));
        assert_eq!(
            input.send(later.clone()).await.unwrap_err().into_request(),
            later
        );
        assert_eq!(receiver.recv().await, Some(first));
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn receiver_drop_recovers_exact_blocked_and_later_requests() {
        let (input, receiver) = route(NonZeroUsize::new(1).unwrap());
        input.send(request(1)).await.unwrap();
        let blocked_request = request(2);
        let later = request(3);
        let mut blocked = Box::pin(input.send(blocked_request.clone()));
        assert_pending(blocked.as_mut()).await;

        drop(receiver);
        let recovered = blocked.await.unwrap_err().into_request();
        assert_eq!(recovered, blocked_request);
        assert!(Arc::ptr_eq(
            &recovered.meta_info,
            &blocked_request.meta_info
        ));
        assert_eq!(
            input.send(later.clone()).await.unwrap_err().into_request(),
            later
        );
    }
}
