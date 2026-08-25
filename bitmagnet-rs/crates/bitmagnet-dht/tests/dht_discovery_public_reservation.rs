use std::future::{poll_fn, Future};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::task::Poll;

use bitmagnet_dht::{
    dht_discovery_channel, DhtDiscoveryOffer, DhtDiscoveryPermit, DhtDiscoveryReserveError,
    DhtDiscoveryStats, Id20, RoutingNode,
};

fn node(value: u8) -> RoutingNode {
    let mut id = [0_u8; 20];
    id[19] = value;
    RoutingNode {
        id: Id20::from_slice(&id).unwrap(),
        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10_000 + u16::from(value)),
    }
}

#[tokio::test]
async fn public_reservation_drop_delivery_stats_and_closed_error_are_exact() {
    let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).unwrap());

    let unused: DhtDiscoveryPermit = sender.reserve().await.unwrap();
    assert_eq!(sender.offer(node(1)), DhtDiscoveryOffer::FullDropped);
    drop(unused);

    assert_eq!(sender.offer(node(1)), DhtDiscoveryOffer::Queued);
    assert_eq!(receiver.recv().await, Some(node(1)));

    let permit: DhtDiscoveryPermit = sender.reserve().await.unwrap();
    assert_eq!(permit.deliver(node(2)), DhtDiscoveryOffer::Queued);
    assert_eq!(receiver.recv().await, Some(node(2)));
    assert_eq!(
        sender.stats(),
        DhtDiscoveryStats {
            offered: 3,
            queued: 2,
            full_dropped: 1,
            receiver_closed_dropped: 0,
        }
    );

    receiver.close();
    assert!(matches!(
        sender.reserve().await,
        Err(DhtDiscoveryReserveError::ReceiverClosed)
    ));
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

#[test]
fn public_receiver_sender_recovery_preserves_channel_stats_and_lifecycle() {
    let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).unwrap());
    let recovered = receiver
        .try_sender()
        .expect("the original sender keeps this channel live");

    assert_eq!(recovered.offer(node(3)), DhtDiscoveryOffer::Queued);
    assert_eq!(receiver.try_recv().unwrap(), node(3));
    assert_eq!(recovered.stats(), sender.stats());

    receiver.close();
    assert!(receiver.try_sender().is_some());
    assert_eq!(recovered.offer(node(4)), DhtDiscoveryOffer::ReceiverClosed);
    assert_eq!(
        sender.stats(),
        DhtDiscoveryStats {
            offered: 2,
            queued: 1,
            full_dropped: 0,
            receiver_closed_dropped: 1,
        }
    );

    drop((sender, recovered));
    assert!(receiver.try_sender().is_none());
}

#[tokio::test]
async fn public_recovered_sender_alone_delays_receiver_eof_until_drop() {
    let (sender, mut receiver) = dht_discovery_channel(NonZeroUsize::new(1).unwrap());
    let recovered = receiver
        .try_sender()
        .expect("the original sender keeps this channel live");
    drop(sender);

    let mut receive = Box::pin(receiver.recv());
    poll_fn(|context| match receive.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("the recovered sender must retain receiver EOF"),
    })
    .await;

    drop(recovered);
    assert_eq!(receive.await, None);
}
