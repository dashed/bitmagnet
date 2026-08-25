//! Taskless concrete composition of the six database- and policy-dependent
//! DHT crawler workers.
//!
//! The DHT runtime and maintenance composition remain externally owned. This
//! module only constructs bounded routes, lazy collaborators, workers, and
//! sender-free statistics handles; construction starts no task and performs no
//! database, DNS, or peer-wire operation.

use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bitmagnet_blocking::BlockingManager;
use bitmagnet_db::PgPool;
use bitmagnet_dht::{
    dht_get_peers_channel_with_capacity, dht_info_hash_triage_channel,
    dht_scrape_channel_with_capacity, DhtDiscoverySender, DhtGetPeersInput, DhtGetPeersReceiver,
    DhtInfoHashTriageInput, DhtInfoHashTriageReceiver, DhtRuntimeClient, DhtScrapeInput,
    DhtScrapeReceiver, Id20, KTable, DHT_CHANNEL_MAX_CAPACITY, DHT_GET_PEERS_ROUTE_CAPACITY,
    DHT_INFO_HASH_TRIAGE_DEFAULT_CAPACITY, DHT_SCRAPE_ROUTE_CAPACITY,
};

use crate::{
    dht_persist_source_channel, dht_persist_torrent_channel,
    dht_request_meta_info_channel_with_capacity, DefaultDhtMetaInfoBanningChecker,
    DhtGetPeersWorker, DhtGetPeersWorkerConfig, DhtGetPeersWorkerStatsHandle,
    DhtInfoHashBlockFilter, DhtInfoHashBlocker, DhtInfoHashTriageConfig,
    DhtInfoHashTriageStatsHandle, DhtInfoHashTriageWorker, DhtMetaInfoBanningChecker,
    DhtMetaInfoRequester, DhtPeerWireMetaInfoRequester, DhtPeerWireMetaInfoRequesterConfig,
    DhtPersistSourceInput, DhtPersistSourceReceiver, DhtPersistSourceWorker,
    DhtPersistSourceWorkerStatsHandle, DhtPersistTorrentInput, DhtPersistTorrentReceiver,
    DhtPersistTorrentWorker, DhtPersistTorrentWorkerConfig, DhtPersistTorrentWorkerStatsHandle,
    DhtRequestMetaInfoInput, DhtRequestMetaInfoReceiver, DhtRequestMetaInfoWorker,
    DhtRequestMetaInfoWorkerConfig, DhtRequestMetaInfoWorkerStatsHandle, DhtScrapeWorker,
    DhtScrapeWorkerConfig, DhtScrapeWorkerStatsHandle, DhtSourceBatchWriter, DhtTorrentBatchWriter,
    DhtTorrentTriageLookup, DhtTorrentV2Lookup, PgDhtSourceBatchWriter, PgDhtTorrentBatchWriter,
    PgDhtTorrentTriageLookup, PgDhtTorrentV2Lookup, SystemDhtInfoHashTriageClock,
    DHT_REQUEST_META_INFO_ROUTE_CAPACITY,
};

/// One downstream route budget and its matching owned worker concurrency.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtCrawlerDownstreamLaneConfig {
    pub route_capacity: NonZeroUsize,
    pub worker_max_inflight: NonZeroUsize,
}

/// Policy knobs consumed by the concrete downstream crawler composition.
///
/// Each field retains the complete worker policy so application projection can
/// override the Go-compatible values without changing batching, connection,
/// or lookup defaults owned by the individual workers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtCrawlerDownstreamConfig {
    pub root_triage_capacity: NonZeroUsize,
    pub get_peers_lane: DhtCrawlerDownstreamLaneConfig,
    pub scrape_lane: DhtCrawlerDownstreamLaneConfig,
    pub request_meta_info_lane: DhtCrawlerDownstreamLaneConfig,
    pub triage: DhtInfoHashTriageConfig,
    pub persist_torrent: DhtPersistTorrentWorkerConfig,
    pub metainfo_requester: DhtPeerWireMetaInfoRequesterConfig,
}

impl Default for DhtCrawlerDownstreamConfig {
    fn default() -> Self {
        Self {
            root_triage_capacity: NonZeroUsize::new(DHT_INFO_HASH_TRIAGE_DEFAULT_CAPACITY).unwrap(),
            get_peers_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: NonZeroUsize::new(DHT_GET_PEERS_ROUTE_CAPACITY).unwrap(),
                worker_max_inflight: DhtGetPeersWorkerConfig::default().max_inflight,
            },
            scrape_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: NonZeroUsize::new(DHT_SCRAPE_ROUTE_CAPACITY).unwrap(),
                worker_max_inflight: DhtScrapeWorkerConfig::default().max_inflight,
            },
            request_meta_info_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: NonZeroUsize::new(DHT_REQUEST_META_INFO_ROUTE_CAPACITY).unwrap(),
                worker_max_inflight: DhtRequestMetaInfoWorkerConfig::default().max_inflight,
            },
            triage: DhtInfoHashTriageConfig::default(),
            persist_torrent: DhtPersistTorrentWorkerConfig::default(),
            metainfo_requester: DhtPeerWireMetaInfoRequesterConfig::default(),
        }
    }
}

impl DhtCrawlerDownstreamConfig {
    /// Validate every Tokio-backed route bound before constructing any queue.
    pub fn validate(&self) -> Result<(), DhtCrawlerDownstreamConfigError> {
        for (capacity, error) in [
            (
                self.root_triage_capacity,
                DhtCrawlerDownstreamConfigError::RootTriageCapacityOutOfRange {
                    capacity: self.root_triage_capacity,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                },
            ),
            (
                self.get_peers_lane.route_capacity,
                DhtCrawlerDownstreamConfigError::GetPeersRouteCapacityOutOfRange {
                    capacity: self.get_peers_lane.route_capacity,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                },
            ),
            (
                self.scrape_lane.route_capacity,
                DhtCrawlerDownstreamConfigError::ScrapeRouteCapacityOutOfRange {
                    capacity: self.scrape_lane.route_capacity,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                },
            ),
            (
                self.request_meta_info_lane.route_capacity,
                DhtCrawlerDownstreamConfigError::RequestMetaInfoRouteCapacityOutOfRange {
                    capacity: self.request_meta_info_lane.route_capacity,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                },
            ),
        ] {
            if capacity.get() > DHT_CHANNEL_MAX_CAPACITY {
                return Err(error);
            }
        }
        Ok(())
    }
}

/// Invalid taskless downstream route policy.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum DhtCrawlerDownstreamConfigError {
    #[error("the root triage capacity {capacity} exceeds Tokio's maximum of {maximum}")]
    RootTriageCapacityOutOfRange {
        capacity: NonZeroUsize,
        maximum: usize,
    },
    #[error("the get-peers route capacity {capacity} exceeds Tokio's maximum of {maximum}")]
    GetPeersRouteCapacityOutOfRange {
        capacity: NonZeroUsize,
        maximum: usize,
    },
    #[error("the scrape route capacity {capacity} exceeds Tokio's maximum of {maximum}")]
    ScrapeRouteCapacityOutOfRange {
        capacity: NonZeroUsize,
        maximum: usize,
    },
    #[error("the request-metainfo route capacity {capacity} exceeds Tokio's maximum of {maximum}")]
    RequestMetaInfoRouteCapacityOutOfRange {
        capacity: NonZeroUsize,
        maximum: usize,
    },
}

/// Recoverable configured-construction failure before any downstream work exists.
pub struct DhtCrawlerDownstreamWithConfigError {
    discovery: DhtDiscoverySender,
    config: Box<DhtCrawlerDownstreamConfig>,
    source: DhtCrawlerDownstreamConfigError,
}

impl DhtCrawlerDownstreamWithConfigError {
    #[must_use]
    pub const fn config_error(&self) -> &DhtCrawlerDownstreamConfigError {
        &self.source
    }

    #[must_use]
    pub fn into_parts(self) -> (DhtDiscoverySender, DhtCrawlerDownstreamConfig) {
        (self.discovery, *self.config)
    }
}

impl fmt::Debug for DhtCrawlerDownstreamWithConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DhtCrawlerDownstreamWithConfigError")
            .field("config", &*self.config)
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for DhtCrawlerDownstreamWithConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid DHT crawler downstream configuration: {}",
            self.source
        )
    }
}

impl Error for DhtCrawlerDownstreamWithConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

struct DhtCrawlerDownstreamRoutes {
    get_peers_input: DhtGetPeersInput,
    get_peers_receiver: DhtGetPeersReceiver,
    scrape_input: DhtScrapeInput,
    scrape_receiver: DhtScrapeReceiver,
    request_meta_info_input: DhtRequestMetaInfoInput,
    request_meta_info_receiver: DhtRequestMetaInfoReceiver,
    persist_torrent_input: DhtPersistTorrentInput,
    persist_torrent_receiver: DhtPersistTorrentReceiver,
    persist_source_input: DhtPersistSourceInput,
    persist_source_receiver: DhtPersistSourceReceiver,
}

impl DhtCrawlerDownstreamRoutes {
    fn new(config: DhtCrawlerDownstreamConfig) -> Self {
        let (get_peers_input, get_peers_receiver) =
            dht_get_peers_channel_with_capacity(config.get_peers_lane.route_capacity);
        let (scrape_input, scrape_receiver) =
            dht_scrape_channel_with_capacity(config.scrape_lane.route_capacity);
        let (request_meta_info_input, request_meta_info_receiver) =
            dht_request_meta_info_channel_with_capacity(
                config.request_meta_info_lane.route_capacity,
            );
        let (persist_torrent_input, persist_torrent_receiver) = dht_persist_torrent_channel();
        let (persist_source_input, persist_source_receiver) = dht_persist_source_channel();
        Self {
            get_peers_input,
            get_peers_receiver,
            scrape_input,
            scrape_receiver,
            request_meta_info_input,
            request_meta_info_receiver,
            persist_torrent_input,
            persist_torrent_receiver,
            persist_source_input,
            persist_source_receiver,
        }
    }
}

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
        Self::compose(
            triage_receiver,
            discovery,
            client,
            table,
            pool,
            metainfo_peer_id,
            DhtCrawlerDownstreamConfig::default(),
        )
    }

    /// Construct the root route and all six downstream workers atomically with
    /// explicit downstream policy.
    ///
    /// Validation precedes every queue, capability clone, PostgreSQL adapter,
    /// and peer-wire requester construction. Success returns the unique input
    /// for the configured root route with the composition that owns its
    /// receiver. Failure returns the discovery sender and exact config intact.
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    pub fn with_config(
        discovery: DhtDiscoverySender,
        client: &DhtRuntimeClient,
        table: &KTable,
        pool: &PgPool,
        metainfo_peer_id: Id20,
        config: DhtCrawlerDownstreamConfig,
    ) -> Result<(DhtInfoHashTriageInput, Self), DhtCrawlerDownstreamWithConfigError> {
        if let Err(source) = config.validate() {
            return Err(DhtCrawlerDownstreamWithConfigError {
                discovery,
                config: Box::new(config),
                source,
            });
        }
        let (triage_input, triage_receiver) =
            dht_info_hash_triage_channel(config.root_triage_capacity);
        let composition = Self::compose(
            triage_receiver,
            discovery,
            client,
            table,
            pool,
            metainfo_peer_id,
            config,
        );
        Ok((triage_input, composition))
    }

    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    fn compose(
        triage_receiver: DhtInfoHashTriageReceiver,
        discovery: DhtDiscoverySender,
        client: &DhtRuntimeClient,
        table: &KTable,
        pool: &PgPool,
        metainfo_peer_id: Id20,
        config: DhtCrawlerDownstreamConfig,
    ) -> Self {
        let DhtCrawlerDownstreamRoutes {
            get_peers_input,
            get_peers_receiver,
            scrape_input,
            scrape_receiver,
            request_meta_info_input,
            request_meta_info_receiver,
            persist_torrent_input,
            persist_torrent_receiver,
            persist_source_input,
            persist_source_receiver,
        } = DhtCrawlerDownstreamRoutes::new(config);
        let persist_torrent_scrape_input = scrape_input.clone();

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
        let requester: Arc<dyn DhtMetaInfoRequester> = Arc::new(
            DhtPeerWireMetaInfoRequester::with_config(metainfo_peer_id, config.metainfo_requester),
        );
        let checker: Arc<dyn DhtMetaInfoBanningChecker> =
            Arc::new(DefaultDhtMetaInfoBanningChecker);

        let (triage, triage_stats) = DhtInfoHashTriageWorker::with_config(
            triage_receiver,
            get_peers_input,
            scrape_input,
            block_filter,
            triage_lookup,
            Arc::new(SystemDhtInfoHashTriageClock),
            config.triage,
        );
        let (get_peers, get_peers_stats) = DhtGetPeersWorker::with_config(
            get_peers_receiver,
            client.clone(),
            table.clone(),
            request_meta_info_input,
            discovery,
            DhtGetPeersWorkerConfig {
                max_inflight: config.get_peers_lane.worker_max_inflight,
            },
        );
        let (request_meta_info, request_meta_info_stats) = DhtRequestMetaInfoWorker::with_config(
            request_meta_info_receiver,
            persist_torrent_input,
            requester,
            checker,
            blocker,
            DhtRequestMetaInfoWorkerConfig {
                max_inflight: config.request_meta_info_lane.worker_max_inflight,
            },
        );
        let (persist_torrent, persist_torrent_stats) = DhtPersistTorrentWorker::with_config(
            persist_torrent_receiver,
            persist_torrent_scrape_input,
            torrent_lookup,
            torrent_writer,
            config.persist_torrent,
        );
        let (scrape, scrape_stats) = DhtScrapeWorker::with_config(
            scrape_receiver,
            client.clone(),
            table.clone(),
            persist_source_input,
            scrape_discovery,
            DhtScrapeWorkerConfig {
                max_inflight: config.scrape_lane.worker_max_inflight,
            },
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
    use std::future::{pending, poll_fn, Future};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::num::NonZeroUsize;
    use std::pin::Pin;
    use std::task::Poll;
    use std::time::Duration;

    use bitmagnet_blocking::{BlockingFinalizeOutcome, BlockingFinalizer};
    use bitmagnet_dht::{
        dht_discovery_channel, dht_info_hash_triage_channel, DhtDiscoveryOffer,
        DhtInfoHashTriageRequest, DhtRuntime, DhtRuntimeConfig, DhtRuntimeExit, RoutingNode,
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
        DHT_PERSIST_SOURCE_ROUTE_CAPACITY, DHT_PERSIST_TORRENT_ROUTE_CAPACITY,
    };

    fn peer_id() -> Id20 {
        Id20::from_slice(b"-BM0001-composition0").unwrap()
    }

    fn id(value: u8) -> Id20 {
        let mut bytes = [0_u8; 20];
        bytes[19] = value;
        Id20::from_slice(&bytes).unwrap()
    }

    fn triage_request(value: u8) -> DhtInfoHashTriageRequest {
        DhtInfoHashTriageRequest {
            info_hash: id(value),
            source_node_addr: SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                10_000 + u16::from(value),
            )),
        }
    }

    fn meta_info_request(value: u8) -> crate::DhtMetaInfoRequest {
        crate::DhtMetaInfoRequest {
            info_hash: id(value),
            source_node_addr: triage_request(value).source_node_addr,
            peers: vec![triage_request(value).source_node_addr],
        }
    }

    fn node(value: u8) -> RoutingNode {
        RoutingNode {
            id: id(value),
            addr: triage_request(value).source_node_addr,
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

    #[test]
    fn public_bundles_and_stats_handles_are_send_sync() {
        assert_send_sync::<DhtCrawlerDownstreamConfig>();
        assert_send_sync::<DhtCrawlerDownstreamWorkers>();
        assert_send_sync::<DhtCrawlerDownstreamStatsHandle>();
        assert_send_sync::<DhtCrawlerDownstreamComposition>();
    }

    #[test]
    fn defaults_and_route_validation_are_exact() {
        let defaults = DhtCrawlerDownstreamConfig::default();
        assert_eq!(
            defaults.root_triage_capacity,
            NonZeroUsize::new(100).unwrap()
        );
        assert_eq!(
            defaults.get_peers_lane,
            DhtCrawlerDownstreamLaneConfig {
                route_capacity: NonZeroUsize::new(100).unwrap(),
                worker_max_inflight: NonZeroUsize::new(200).unwrap(),
            }
        );
        assert_eq!(
            defaults.scrape_lane,
            DhtCrawlerDownstreamLaneConfig {
                route_capacity: NonZeroUsize::new(100).unwrap(),
                worker_max_inflight: NonZeroUsize::new(200).unwrap(),
            }
        );
        assert_eq!(
            defaults.request_meta_info_lane,
            DhtCrawlerDownstreamLaneConfig {
                route_capacity: NonZeroUsize::new(100).unwrap(),
                worker_max_inflight: NonZeroUsize::new(400).unwrap(),
            }
        );
        assert_eq!(DHT_PERSIST_TORRENT_ROUTE_CAPACITY, 1_000);
        assert_eq!(DHT_PERSIST_SOURCE_ROUTE_CAPACITY, 1_000);

        let maximum = NonZeroUsize::new(DHT_CHANNEL_MAX_CAPACITY).unwrap();
        assert_eq!(
            DhtCrawlerDownstreamConfig {
                root_triage_capacity: maximum,
                get_peers_lane: DhtCrawlerDownstreamLaneConfig {
                    route_capacity: maximum,
                    ..defaults.get_peers_lane
                },
                scrape_lane: DhtCrawlerDownstreamLaneConfig {
                    route_capacity: maximum,
                    ..defaults.scrape_lane
                },
                request_meta_info_lane: DhtCrawlerDownstreamLaneConfig {
                    route_capacity: maximum,
                    ..defaults.request_meta_info_lane
                },
                ..defaults
            }
            .validate(),
            Ok(())
        );

        let over_max = NonZeroUsize::new(DHT_CHANNEL_MAX_CAPACITY + 1).unwrap();
        let cases = [
            (
                DhtCrawlerDownstreamConfig {
                    root_triage_capacity: over_max,
                    ..defaults
                },
                DhtCrawlerDownstreamConfigError::RootTriageCapacityOutOfRange {
                    capacity: over_max,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                },
            ),
            (
                DhtCrawlerDownstreamConfig {
                    get_peers_lane: DhtCrawlerDownstreamLaneConfig {
                        route_capacity: over_max,
                        ..defaults.get_peers_lane
                    },
                    ..defaults
                },
                DhtCrawlerDownstreamConfigError::GetPeersRouteCapacityOutOfRange {
                    capacity: over_max,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                },
            ),
            (
                DhtCrawlerDownstreamConfig {
                    scrape_lane: DhtCrawlerDownstreamLaneConfig {
                        route_capacity: over_max,
                        ..defaults.scrape_lane
                    },
                    ..defaults
                },
                DhtCrawlerDownstreamConfigError::ScrapeRouteCapacityOutOfRange {
                    capacity: over_max,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                },
            ),
            (
                DhtCrawlerDownstreamConfig {
                    request_meta_info_lane: DhtCrawlerDownstreamLaneConfig {
                        route_capacity: over_max,
                        ..defaults.request_meta_info_lane
                    },
                    ..defaults
                },
                DhtCrawlerDownstreamConfigError::RequestMetaInfoRouteCapacityOutOfRange {
                    capacity: over_max,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                },
            ),
        ];
        for (config, expected) in cases {
            assert_eq!(config.validate(), Err(expected));
        }

        let all_invalid = DhtCrawlerDownstreamConfig {
            root_triage_capacity: over_max,
            get_peers_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: over_max,
                ..defaults.get_peers_lane
            },
            scrape_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: over_max,
                ..defaults.scrape_lane
            },
            request_meta_info_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: over_max,
                ..defaults.request_meta_info_lane
            },
            ..defaults
        };
        assert_eq!(
            all_invalid.validate(),
            Err(
                DhtCrawlerDownstreamConfigError::RootTriageCapacityOutOfRange {
                    capacity: over_max,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                }
            )
        );
    }

    #[tokio::test]
    async fn configured_root_and_owned_routes_enforce_distinct_fifo_capacities() {
        let config = DhtCrawlerDownstreamConfig {
            root_triage_capacity: NonZeroUsize::new(2).unwrap(),
            get_peers_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: NonZeroUsize::new(2).unwrap(),
                worker_max_inflight: NonZeroUsize::MIN,
            },
            scrape_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: NonZeroUsize::new(3).unwrap(),
                worker_max_inflight: NonZeroUsize::MIN,
            },
            request_meta_info_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: NonZeroUsize::new(4).unwrap(),
                worker_max_inflight: NonZeroUsize::MIN,
            },
            ..DhtCrawlerDownstreamConfig::default()
        };

        let (triage, mut triage_receiver) =
            dht_info_hash_triage_channel(config.root_triage_capacity);
        triage.send(triage_request(1)).await.unwrap();
        triage.send(triage_request(2)).await.unwrap();
        let mut blocked = Box::pin(triage.send(triage_request(3)));
        assert_pending(blocked.as_mut()).await;
        assert_eq!(triage_receiver.recv().await, Some(triage_request(1)));
        assert_eq!(blocked.await, Ok(()));
        assert_eq!(triage_receiver.recv().await, Some(triage_request(2)));
        assert_eq!(triage_receiver.recv().await, Some(triage_request(3)));

        let DhtCrawlerDownstreamRoutes {
            get_peers_input,
            mut get_peers_receiver,
            scrape_input,
            mut scrape_receiver,
            request_meta_info_input,
            mut request_meta_info_receiver,
            ..
        } = DhtCrawlerDownstreamRoutes::new(config);
        for value in 1..=2 {
            get_peers_input.send(triage_request(value)).await.unwrap();
        }
        let mut blocked = Box::pin(get_peers_input.send(triage_request(3)));
        assert_pending(blocked.as_mut()).await;
        assert_eq!(get_peers_receiver.recv().await, Some(triage_request(1)));
        assert_eq!(blocked.await, Ok(()));
        for value in 2..=3 {
            assert_eq!(get_peers_receiver.recv().await, Some(triage_request(value)));
        }

        for value in 1..=3 {
            scrape_input.send(triage_request(value)).await.unwrap();
        }
        let mut blocked = Box::pin(scrape_input.send(triage_request(4)));
        assert_pending(blocked.as_mut()).await;
        assert_eq!(scrape_receiver.recv().await, Some(triage_request(1)));
        assert_eq!(blocked.await, Ok(()));
        for value in 2..=4 {
            assert_eq!(scrape_receiver.recv().await, Some(triage_request(value)));
        }

        for value in 1..=4 {
            request_meta_info_input
                .send(meta_info_request(value))
                .await
                .unwrap();
        }
        let mut blocked = Box::pin(request_meta_info_input.send(meta_info_request(5)));
        assert_pending(blocked.as_mut()).await;
        assert_eq!(
            request_meta_info_receiver.recv().await,
            Some(meta_info_request(1))
        );
        assert_eq!(blocked.await, Ok(()));
        for value in 2..=5 {
            assert_eq!(
                request_meta_info_receiver.recv().await,
                Some(meta_info_request(value))
            );
        }
    }

    #[tokio::test]
    async fn with_config_installs_nondefault_policy_into_each_concrete_consumer() {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let mut runtime = DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            ..DhtRuntimeConfig::default()
        })
        .await
        .unwrap();
        let client = runtime.client();
        let table = runtime.table().clone();
        let discovery = runtime
            .take_discovered_nodes()
            .expect("the runtime exposes its discovery receiver once")
            .try_sender()
            .expect("the live runtime retains the original discovery sender");
        let config = DhtCrawlerDownstreamConfig {
            root_triage_capacity: NonZeroUsize::new(2).unwrap(),
            get_peers_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: NonZeroUsize::new(3).unwrap(),
                worker_max_inflight: NonZeroUsize::new(5).unwrap(),
            },
            scrape_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: NonZeroUsize::new(7).unwrap(),
                worker_max_inflight: NonZeroUsize::new(11).unwrap(),
            },
            request_meta_info_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: NonZeroUsize::new(13).unwrap(),
                worker_max_inflight: NonZeroUsize::new(17).unwrap(),
            },
            triage: DhtInfoHashTriageConfig {
                save_files_threshold: 500_000,
                rescrape_threshold: Duration::from_secs(48 * 60 * 60),
                ..DhtInfoHashTriageConfig::default()
            },
            persist_torrent: DhtPersistTorrentWorkerConfig {
                plan_config: crate::DhtTorrentPlanConfig {
                    save_files_threshold: 500_000,
                    save_pieces: true,
                },
                ..DhtPersistTorrentWorkerConfig::default()
            },
            metainfo_requester: DhtPeerWireMetaInfoRequesterConfig {
                request_timeout: Duration::from_millis(1_500),
                ..DhtPeerWireMetaInfoRequesterConfig::default()
            },
        };
        let (triage_input, composition) = DhtCrawlerDownstreamComposition::with_config(
            discovery,
            &client,
            &table,
            &pool,
            peer_id(),
            config,
        )
        .unwrap();

        triage_input.send(triage_request(1)).await.unwrap();
        triage_input.send(triage_request(2)).await.unwrap();
        let blocked_request = triage_request(3);
        let mut blocked = Box::pin(triage_input.send(blocked_request));
        assert_pending(blocked.as_mut()).await;

        assert_eq!(composition.workers.triage.config_for_test(), config.triage);
        assert_eq!(
            composition.workers.get_peers.config_for_test().max_inflight,
            config.get_peers_lane.worker_max_inflight
        );
        assert_eq!(
            composition.workers.scrape.config_for_test().max_inflight,
            config.scrape_lane.worker_max_inflight
        );
        assert_eq!(
            composition
                .workers
                .request_meta_info
                .config_for_test()
                .max_inflight,
            config.request_meta_info_lane.worker_max_inflight
        );
        assert_eq!(
            composition.workers.persist_torrent.config_for_test(),
            config.persist_torrent
        );
        assert_eq!(
            composition
                .workers
                .request_meta_info
                .peer_wire_config_for_test(),
            Some(config.metainfo_requester)
        );

        drop(composition);
        assert_eq!(blocked.await.unwrap_err().into_request(), blocked_request);
        drop((client, table));
        assert!(matches!(
            runtime.shutdown().await.unwrap(),
            DhtRuntimeExit::Shutdown
        ));
        pool.close().await;
    }

    #[tokio::test]
    async fn invalid_routes_recover_exact_policy_and_live_capabilities_before_offline_adapters() {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let runtime = DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            ..DhtRuntimeConfig::default()
        })
        .await
        .unwrap();
        let client = runtime.client();
        let defaults = DhtCrawlerDownstreamConfig::default();
        let over_max = NonZeroUsize::new(DHT_CHANNEL_MAX_CAPACITY + 1).unwrap();
        let cases = [
            (
                DhtCrawlerDownstreamConfig {
                    root_triage_capacity: over_max,
                    ..defaults
                },
                DhtCrawlerDownstreamConfigError::RootTriageCapacityOutOfRange {
                    capacity: over_max,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                },
            ),
            (
                DhtCrawlerDownstreamConfig {
                    get_peers_lane: DhtCrawlerDownstreamLaneConfig {
                        route_capacity: over_max,
                        ..defaults.get_peers_lane
                    },
                    ..defaults
                },
                DhtCrawlerDownstreamConfigError::GetPeersRouteCapacityOutOfRange {
                    capacity: over_max,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                },
            ),
            (
                DhtCrawlerDownstreamConfig {
                    scrape_lane: DhtCrawlerDownstreamLaneConfig {
                        route_capacity: over_max,
                        ..defaults.scrape_lane
                    },
                    ..defaults
                },
                DhtCrawlerDownstreamConfigError::ScrapeRouteCapacityOutOfRange {
                    capacity: over_max,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                },
            ),
            (
                DhtCrawlerDownstreamConfig {
                    request_meta_info_lane: DhtCrawlerDownstreamLaneConfig {
                        route_capacity: over_max,
                        ..defaults.request_meta_info_lane
                    },
                    ..defaults
                },
                DhtCrawlerDownstreamConfigError::RequestMetaInfoRouteCapacityOutOfRange {
                    capacity: over_max,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                },
            ),
        ];

        for (index, (config, expected)) in cases.into_iter().enumerate() {
            let (discovery, mut discovered) = dht_discovery_channel(NonZeroUsize::MIN);
            let error = match DhtCrawlerDownstreamComposition::with_config(
                discovery,
                &client,
                runtime.table(),
                &pool,
                peer_id(),
                config,
            ) {
                Ok(_) => panic!("invalid downstream route must fail before construction"),
                Err(error) => error,
            };
            assert_eq!(error.config_error(), &expected);
            let (discovery, recovered_config) = error.into_parts();
            assert_eq!(recovered_config, config);
            let value = u8::try_from(index + 1).unwrap();
            assert_eq!(discovery.offer(node(value)), DhtDiscoveryOffer::Queued);
            assert_eq!(discovered.recv().await, Some(node(value)));
            assert_eq!(pool.size(), 0, "validation must not acquire PostgreSQL");
        }

        drop((client, pool));
        runtime.shutdown().await.unwrap();
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
