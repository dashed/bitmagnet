//! Taskless concrete composition of the six database- and policy-dependent
//! DHT crawler workers.
//!
//! The DHT runtime and maintenance composition remain externally owned. This
//! module only constructs bounded routes, lazy collaborators, workers, and
//! sender-free statistics handles; construction starts no task and performs no
//! database, DNS, or peer-wire operation.

use std::sync::Arc;

use bitmagnet_blocking::BlockingManager;
use bitmagnet_db::PgPool;
use bitmagnet_dht::{
    dht_get_peers_channel, dht_scrape_channel, DhtDiscoverySender, DhtInfoHashTriageReceiver,
    DhtRuntimeClient, Id20, KTable,
};

use crate::{
    dht_persist_source_channel, dht_persist_torrent_channel, dht_request_meta_info_channel,
    DefaultDhtMetaInfoBanningChecker, DhtGetPeersWorker, DhtGetPeersWorkerStatsHandle,
    DhtInfoHashBlockFilter, DhtInfoHashBlocker, DhtInfoHashTriageStatsHandle,
    DhtInfoHashTriageWorker, DhtMetaInfoBanningChecker, DhtMetaInfoRequester,
    DhtPeerWireMetaInfoRequester, DhtPersistSourceWorker, DhtPersistSourceWorkerStatsHandle,
    DhtPersistTorrentWorker, DhtPersistTorrentWorkerStatsHandle, DhtRequestMetaInfoWorker,
    DhtRequestMetaInfoWorkerStatsHandle, DhtScrapeWorker, DhtScrapeWorkerStatsHandle,
    DhtSourceBatchWriter, DhtTorrentBatchWriter, DhtTorrentTriageLookup, DhtTorrentV2Lookup,
    PgDhtSourceBatchWriter, PgDhtTorrentBatchWriter, PgDhtTorrentTriageLookup,
    PgDhtTorrentV2Lookup,
};

/// The six concrete downstream workers, with every internal route capability
/// moved into exactly the worker that consumes or produces through it.
///
/// This bundle deliberately provides no `run` method. The application owns
/// task creation, shutdown signalling, join ordering, and failure policy.
#[must_use = "the workers must be run or deliberately dropped by an application owner"]
pub struct DhtCrawlerDownstreamWorkers {
    pub triage: DhtInfoHashTriageWorker,
    pub get_peers: DhtGetPeersWorker,
    pub request_meta_info: DhtRequestMetaInfoWorker,
    pub scrape: DhtScrapeWorker,
    pub persist_torrent: DhtPersistTorrentWorker,
    pub persist_source: DhtPersistSourceWorker,
}

/// Cloneable, sender-free statistics surface for the downstream composition.
///
/// Retaining this value cannot keep any route open. Each constituent snapshot
/// retains its worker's existing independently-read counter semantics.
#[derive(Clone)]
pub struct DhtCrawlerDownstreamStatsHandle {
    pub triage: DhtInfoHashTriageStatsHandle,
    pub get_peers: DhtGetPeersWorkerStatsHandle,
    pub request_meta_info: DhtRequestMetaInfoWorkerStatsHandle,
    pub scrape: DhtScrapeWorkerStatsHandle,
    pub persist_torrent: DhtPersistTorrentWorkerStatsHandle,
    pub persist_source: DhtPersistSourceWorkerStatsHandle,
}

/// Taskless concrete downstream construction result.
///
/// The returned blocking manager is the application finalization capability
/// shared with triage and request-metainfo. Finalization is not automatic and
/// should occur only after those workers can no longer enqueue blocked hashes.
/// No route input is retained outside [`DhtCrawlerDownstreamWorkers`].
#[must_use = "the workers and blocking finalization capability require application ownership"]
pub struct DhtCrawlerDownstreamComposition {
    pub workers: DhtCrawlerDownstreamWorkers,
    pub stats: DhtCrawlerDownstreamStatsHandle,
    pub blocking_manager: Arc<BlockingManager>,
}

impl DhtCrawlerDownstreamComposition {
    /// Construct all six downstream workers with their production defaults.
    ///
    /// `triage_receiver` is the unique consumer half of the application-owned
    /// triage route. `discovery` is consumed and cloned exactly once for the
    /// get-peers and scrape workers. The runtime client and table are cloned
    /// only into those workers. The application-owned pool is cloned into the
    /// shared blocking manager and four concrete PostgreSQL adapters.
    ///
    /// `metainfo_peer_id` remains stable for the concrete requester's lifetime
    /// and is intentionally distinct from the external DHT runtime node ID.
    #[allow(clippy::too_many_lines)]
    pub fn new(
        triage_receiver: DhtInfoHashTriageReceiver,
        discovery: DhtDiscoverySender,
        client: &DhtRuntimeClient,
        table: &KTable,
        pool: &PgPool,
        metainfo_peer_id: Id20,
    ) -> Self {
        let (get_peers_input, get_peers_receiver) = dht_get_peers_channel();
        let (scrape_input, scrape_receiver) = dht_scrape_channel();
        let persist_torrent_scrape_input = scrape_input.clone();
        let (request_meta_info_input, request_meta_info_receiver) = dht_request_meta_info_channel();
        let (persist_torrent_input, persist_torrent_receiver) = dht_persist_torrent_channel();
        let (persist_source_input, persist_source_receiver) = dht_persist_source_channel();

        let scrape_discovery = discovery.clone();

        let blocking_manager = Arc::new(BlockingManager::new(pool.clone()));
        let block_filter: Arc<dyn DhtInfoHashBlockFilter> = blocking_manager.clone();
        let blocker: Arc<dyn DhtInfoHashBlocker> = blocking_manager.clone();

        let triage_lookup: Arc<dyn DhtTorrentTriageLookup> =
            Arc::new(PgDhtTorrentTriageLookup::new(pool.clone()));
        let source_writer: Arc<dyn DhtSourceBatchWriter> =
            Arc::new(PgDhtSourceBatchWriter::new(pool.clone()));
        let torrent_lookup: Arc<dyn DhtTorrentV2Lookup> =
            Arc::new(PgDhtTorrentV2Lookup::new(pool.clone()));
        let torrent_writer: Arc<dyn DhtTorrentBatchWriter> =
            Arc::new(PgDhtTorrentBatchWriter::new(pool.clone()));
        let requester: Arc<dyn DhtMetaInfoRequester> =
            Arc::new(DhtPeerWireMetaInfoRequester::new(metainfo_peer_id));
        let checker: Arc<dyn DhtMetaInfoBanningChecker> =
            Arc::new(DefaultDhtMetaInfoBanningChecker);

        let (triage, triage_stats) = DhtInfoHashTriageWorker::new(
            triage_receiver,
            get_peers_input,
            scrape_input,
            block_filter,
            triage_lookup,
        );
        let (get_peers, get_peers_stats) = DhtGetPeersWorker::new(
            get_peers_receiver,
            client.clone(),
            table.clone(),
            request_meta_info_input,
            discovery,
        );
        let (request_meta_info, request_meta_info_stats) = DhtRequestMetaInfoWorker::new(
            request_meta_info_receiver,
            persist_torrent_input,
            requester,
            checker,
            blocker,
        );
        let (persist_torrent, persist_torrent_stats) = DhtPersistTorrentWorker::new(
            persist_torrent_receiver,
            persist_torrent_scrape_input,
            torrent_lookup,
            torrent_writer,
        );
        let (scrape, scrape_stats) = DhtScrapeWorker::new(
            scrape_receiver,
            client.clone(),
            table.clone(),
            persist_source_input,
            scrape_discovery,
        );
        let (persist_source, persist_source_stats) =
            DhtPersistSourceWorker::new(persist_source_receiver, source_writer);

        Self {
            workers: DhtCrawlerDownstreamWorkers {
                triage,
                get_peers,
                request_meta_info,
                scrape,
                persist_torrent,
                persist_source,
            },
            stats: DhtCrawlerDownstreamStatsHandle {
                triage: triage_stats,
                get_peers: get_peers_stats,
                request_meta_info: request_meta_info_stats,
                scrape: scrape_stats,
                persist_torrent: persist_torrent_stats,
                persist_source: persist_source_stats,
            },
            blocking_manager,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use bitmagnet_blocking::{BlockingFinalizeOutcome, BlockingFinalizer};
    use bitmagnet_dht::{
        dht_info_hash_triage_channel, DhtRuntime, DhtRuntimeConfig, DhtRuntimeExit,
        DHT_INFO_HASH_TRIAGE_DEFAULT_CAPACITY,
    };
    use sqlx::postgres::PgPoolOptions;
    use tokio::net::UdpSocket;
    use tokio::time::timeout;

    use super::*;
    use crate::{
        DhtGetPeersWorkerExit, DhtGetPeersWorkerStats, DhtInfoHashTriageStats,
        DhtInfoHashTriageWorkerExit, DhtPersistSourceWorkerExit, DhtPersistSourceWorkerStats,
        DhtPersistTorrentWorkerExit, DhtPersistTorrentWorkerStats, DhtRequestMetaInfoWorkerExit,
        DhtRequestMetaInfoWorkerStats, DhtScrapeWorkerExit, DhtScrapeWorkerStats,
    };

    fn peer_id() -> Id20 {
        Id20::from_slice(b"-BM0001-composition0").unwrap()
    }

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn public_bundles_and_stats_handles_are_send_sync() {
        assert_send_sync::<DhtCrawlerDownstreamWorkers>();
        assert_send_sync::<DhtCrawlerDownstreamStatsHandle>();
        assert_send_sync::<DhtCrawlerDownstreamComposition>();
    }

    #[tokio::test]
    async fn construction_is_offline_and_empty_root_eof_cascades_through_all_six_workers() {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        assert_eq!(pool.size(), 0);
        assert!(!pool.is_closed());

        let mut runtime = DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            ..DhtRuntimeConfig::default()
        })
        .await
        .unwrap();
        let runtime_addr = runtime.local_addr();
        let client = runtime.client();
        let table = runtime.table().clone();
        let mut discovery_receiver = runtime
            .take_discovered_nodes()
            .expect("the runtime exposes its discovery receiver once");
        let discovery = discovery_receiver
            .try_sender()
            .expect("the live runtime retains the original discovery sender");
        let discovery_stats = discovery.stats_handle();

        let (triage_input, triage_receiver) = dht_info_hash_triage_channel(
            NonZeroUsize::new(DHT_INFO_HASH_TRIAGE_DEFAULT_CAPACITY).unwrap(),
        );
        let composition = DhtCrawlerDownstreamComposition::new(
            triage_receiver,
            discovery,
            &client,
            &table,
            &pool,
            peer_id(),
        );

        assert_eq!(pool.size(), 0, "construction must not acquire PostgreSQL");
        assert_eq!(Arc::strong_count(&composition.blocking_manager), 3);
        assert_eq!(
            discovery_stats.snapshot(),
            bitmagnet_dht::DhtDiscoveryStats::default()
        );

        let DhtCrawlerDownstreamComposition {
            workers:
                DhtCrawlerDownstreamWorkers {
                    triage,
                    get_peers,
                    request_meta_info,
                    scrape,
                    persist_torrent,
                    persist_source,
                },
            stats,
            blocking_manager,
        } = composition;

        // This is the only producer outside the workers. Its destruction must
        // propagate EOF through get-peers, request-metainfo, persist-torrent,
        // scrape, and persist-source without a hidden composition-owned clone.
        drop(triage_input);

        let exits = timeout(Duration::from_secs(5), async {
            tokio::join!(
                triage.run(pending()),
                get_peers.run(pending()),
                request_meta_info.run(pending()),
                scrape.run(pending()),
                persist_torrent.run(pending()),
                persist_source.run(pending()),
            )
        })
        .await
        .expect("empty route EOF must reach all six workers");

        assert_eq!(exits.0, DhtInfoHashTriageWorkerExit::InputClosed);
        assert_eq!(exits.1, DhtGetPeersWorkerExit::InputClosed);
        assert_eq!(exits.2, DhtRequestMetaInfoWorkerExit::InputClosed);
        assert_eq!(exits.3, DhtScrapeWorkerExit::InputClosed);
        assert_eq!(exits.4, DhtPersistTorrentWorkerExit::InputClosed);
        assert_eq!(exits.5, DhtPersistSourceWorkerExit::InputClosed);

        assert_eq!(stats.triage.snapshot(), DhtInfoHashTriageStats::default());
        assert_eq!(
            stats.get_peers.snapshot(),
            DhtGetPeersWorkerStats::default()
        );
        assert_eq!(
            stats.request_meta_info.snapshot(),
            DhtRequestMetaInfoWorkerStats::default()
        );
        assert_eq!(stats.scrape.snapshot(), DhtScrapeWorkerStats::default());
        assert_eq!(
            stats.persist_torrent.snapshot(),
            DhtPersistTorrentWorkerStats::default()
        );
        assert_eq!(
            stats.persist_source.snapshot(),
            DhtPersistSourceWorkerStats::default()
        );
        assert_eq!(Arc::strong_count(&blocking_manager), 1);
        assert_eq!(
            BlockingFinalizer::finalize(blocking_manager.as_ref())
                .await
                .unwrap(),
            BlockingFinalizeOutcome::NothingPending
        );
        assert_eq!(pool.size(), 0, "empty finalization must remain offline");
        assert!(!pool.is_closed());

        drop((stats, blocking_manager, client, table));
        assert!(matches!(
            runtime.shutdown().await.unwrap(),
            DhtRuntimeExit::Shutdown
        ));
        assert_eq!(
            timeout(Duration::from_secs(1), discovery_receiver.recv())
                .await
                .expect("runtime shutdown must close discovery ingress"),
            None
        );
        drop(discovery_receiver);
        let rebound = UdpSocket::bind(runtime_addr)
            .await
            .expect("runtime shutdown must release its UDP socket");
        drop(rebound);

        pool.close().await;
        assert!(pool.is_closed());
    }
}
