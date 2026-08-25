use std::future::ready;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use bitmagnet_dht::{Id20, ScrapeBloomFilter};
use bitmagnet_dht_crawler::{
    dht_persist_source_channel, DhtPersistSourceRequest, DhtPersistSourceWorker,
    DhtPersistSourceWorkerExit, DhtPersistSourceWorkerStats, DhtSourceBatchWriter,
    PgDhtSourceBatchWriter,
};
use sqlx::postgres::PgPoolOptions;
use tokio::time::timeout;

fn request(value: u8) -> DhtPersistSourceRequest {
    let mut bytes = [0_u8; 20];
    bytes[19] = value;
    DhtPersistSourceRequest {
        info_hash: Id20::from_slice(&bytes).unwrap(),
        source_node_addr: SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::LOCALHOST,
            10_000 + u16::from(value),
        )),
        seeders_bloom: ScrapeBloomFilter::EMPTY,
        peers_bloom: ScrapeBloomFilter::EMPTY,
    }
}

#[tokio::test]
async fn concrete_source_persistence_composition_stays_offline_on_pre_ready_shutdown() {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .unwrap();
    assert_eq!(pool.size(), 0);

    let writer = Arc::new(PgDhtSourceBatchWriter::new(pool.clone()));
    let writer_collaborator: Arc<dyn DhtSourceBatchWriter> = writer.clone();
    let (input, receiver) = dht_persist_source_channel();
    let queued = request(1);
    input.send(queued.clone()).await.unwrap();

    let (worker, stats) = DhtPersistSourceWorker::new(receiver, writer_collaborator);
    let exit = timeout(Duration::from_secs(1), worker.run(ready(())))
        .await
        .expect("pre-ready shutdown must not acquire a database connection");

    assert_eq!(
        exit,
        DhtPersistSourceWorkerExit::Shutdown {
            queued_dropped: 1,
            batch_dropped: 0,
            write_abandoned: 0,
        }
    );
    assert_eq!(
        stats.snapshot(),
        DhtPersistSourceWorkerStats {
            shutdown_queued_dropped: 1,
            ..DhtPersistSourceWorkerStats::default()
        }
    );

    let rejected = request(2);
    assert_eq!(
        input
            .send(rejected.clone())
            .await
            .unwrap_err()
            .into_request(),
        rejected
    );
    assert_eq!(pool.size(), 0);
    assert!(!pool.is_closed());
    assert_eq!(Arc::strong_count(&writer), 1);

    drop((input, writer));
    pool.close().await;
    assert!(pool.is_closed());
}
