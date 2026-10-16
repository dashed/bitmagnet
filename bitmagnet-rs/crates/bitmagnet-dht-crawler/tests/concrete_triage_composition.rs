use std::future::ready;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bitmagnet_blocking::BlockingManager;
use bitmagnet_dht::{
    dht_get_peers_channel, dht_info_hash_triage_channel, dht_scrape_channel,
    DhtInfoHashTriageRequest, Id20, DHT_INFO_HASH_TRIAGE_DEFAULT_CAPACITY,
};
use bitmagnet_dht_crawler::{
    DhtInfoHashBlockFilter, DhtInfoHashTriageStats, DhtInfoHashTriageWorker,
    DhtInfoHashTriageWorkerExit, DhtTorrentTriageLookup, PgDhtTorrentTriageLookup,
};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::timeout;

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

#[tokio::test]
async fn concrete_triage_composition_honors_pre_ready_shutdown_without_polling_collaborators() {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .unwrap();
    assert_eq!(pool.size(), 0);

    let manager = Arc::new(BlockingManager::new(pool.clone()));
    let filter: Arc<dyn DhtInfoHashBlockFilter> = manager.clone();
    let lookup = Arc::new(PgDhtTorrentTriageLookup::new(pool.clone()));
    let lookup_collaborator: Arc<dyn DhtTorrentTriageLookup> = lookup.clone();

    let (triage, triage_receiver) = dht_info_hash_triage_channel(
        NonZeroUsize::new(DHT_INFO_HASH_TRIAGE_DEFAULT_CAPACITY).unwrap(),
    );
    let (get_peers, mut get_peers_receiver) = dht_get_peers_channel();
    let (scrape, mut scrape_receiver) = dht_scrape_channel();
    for value in 1..=3 {
        triage.send(request(value)).await.unwrap();
    }

    let (worker, stats) = DhtInfoHashTriageWorker::new(
        triage_receiver,
        get_peers,
        scrape,
        filter,
        lookup_collaborator,
    );
    let exit = timeout(Duration::from_secs(1), worker.run(ready(())))
        .await
        .expect("pre-ready shutdown must not wait for collaborators or route consumers");

    assert_eq!(
        exit,
        DhtInfoHashTriageWorkerExit::Shutdown {
            queued_dropped: 3,
            batch_dropped: 0,
        }
    );
    assert_eq!(
        stats.snapshot(),
        DhtInfoHashTriageStats {
            shutdown_queued_dropped: 3,
            ..DhtInfoHashTriageStats::default()
        }
    );
    assert!(matches!(
        get_peers_receiver.try_recv(),
        Err(TryRecvError::Disconnected)
    ));
    assert!(matches!(
        scrape_receiver.try_recv(),
        Err(TryRecvError::Disconnected)
    ));
    let rejected = request(4);
    assert_eq!(
        triage.send(rejected).await.unwrap_err().into_request(),
        rejected
    );
    assert_eq!(pool.size(), 0);

    assert!(!pool.is_closed());
    manager.flush().await.unwrap();
    assert!(!pool.is_closed());
    assert_eq!(pool.size(), 0);
    assert_eq!(Arc::strong_count(&manager), 1);
    assert_eq!(Arc::strong_count(&lookup), 1);

    drop((get_peers_receiver, scrape_receiver, lookup, manager));
    pool.close().await;
    assert!(pool.is_closed());
}
