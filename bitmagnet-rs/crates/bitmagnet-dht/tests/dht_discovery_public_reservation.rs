use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;

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
