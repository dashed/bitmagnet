//! Pure BitTorrent DHT wire contracts.

mod announce_token;
mod compact;
mod dht_bootstrap_ping_producer;
mod dht_client;
mod dht_concurrent_supervisor;
mod dht_crawler_maintenance_supervisor;
mod dht_crawler_target;
mod dht_discovered_node_find_worker;
mod dht_discovered_node_ping_worker;
mod dht_discovered_node_scheduler;
mod dht_discovery;
mod dht_dispatch;
mod dht_driver;
mod dht_inbound_stats;
mod dht_info_hash_deduper;
mod dht_info_hash_triage;
mod dht_info_hash_triage_routes;
mod dht_oldest_node_find_producer;
mod dht_oldest_node_ping_producer;
mod dht_responder;
mod dht_runtime;
mod dht_runtime_health;
mod dht_sample_infohashes_producer;
mod dht_sample_infohashes_worker;
mod dht_send;
mod dht_supervisor;
mod inbound;
mod krpc;
mod ktable;
mod ktable_core;
mod node_table;
mod ping_find_node;
mod ping_find_node_dispatch;
mod ping_find_node_driver;
mod ping_find_node_send;
mod ping_find_node_supervisor;
mod query_send;
mod rate_limit;
mod receive;
mod reply;
mod routing_tree;
mod scrape;
mod tokio_ipv4_udp;
mod transaction;

/// Largest capacity accepted by the public DHT Tokio channel constructors.
///
/// This is exactly Tokio's semaphore permit ceiling, which also bounds Tokio
/// bounded MPSC channels.
pub const DHT_CHANNEL_MAX_CAPACITY: usize = tokio::sync::Semaphore::MAX_PERMITS;

#[track_caller]
fn assert_dht_channel_capacity(capacity: std::num::NonZeroUsize) {
    assert!(
        capacity.get() <= DHT_CHANNEL_MAX_CAPACITY,
        "DHT channel capacity {} exceeds Tokio's maximum of {}",
        capacity,
        DHT_CHANNEL_MAX_CAPACITY,
    );
}

pub use compact::{CompactAddr, CompactCodecError, CompactNode, Id20};
pub use dht_bootstrap_ping_producer::{
    DhtBootstrapPingProducer, DhtBootstrapPingProducerConfig, DhtBootstrapPingProducerConfigError,
    DhtBootstrapPingProducerExit, DhtBootstrapPingProducerStartError,
    DhtBootstrapPingProducerStats, DhtBootstrapPingProducerStatsHandle,
};
pub use dht_client::{
    DhtClient, DhtClientError, FindNodeResult, GetPeersResult, GetPeersScrapeResult,
    PingFindNodeClient, PingFindNodeClientError, PingResult, SampleInfoHashesResult,
};
pub use dht_concurrent_supervisor::{
    DhtConcurrentSupervisor, DhtConcurrentSupervisorExit, DhtInboundAdmissionPolicy,
};
pub use dht_crawler_maintenance_supervisor::{
    DhtCrawlerMaintenanceChild, DhtCrawlerMaintenanceChildExits, DhtCrawlerMaintenanceConfig,
    DhtCrawlerMaintenanceConfigError, DhtCrawlerMaintenanceNotification,
    DhtCrawlerMaintenanceRunNotifications, DhtCrawlerMaintenanceStartError,
    DhtCrawlerMaintenanceStatsHandle, DhtCrawlerMaintenanceSupervisor,
    DhtCrawlerMaintenanceSupervisorExit, DhtCrawlerMaintenanceWithConfigError,
};
pub use dht_crawler_target::{DhtCrawlerTarget, DhtCrawlerTargetError, DhtCrawlerTargetRotator};
pub use dht_discovered_node_find_worker::{
    DhtDiscoveredNodeFindStats, DhtDiscoveredNodeFindStatsHandle, DhtDiscoveredNodeFindWorker,
    DhtDiscoveredNodeFindWorkerConfig, DhtDiscoveredNodeFindWorkerExit,
};
pub use dht_discovered_node_ping_worker::{
    DhtDiscoveredNodePingStats, DhtDiscoveredNodePingStatsHandle, DhtDiscoveredNodePingWorker,
    DhtDiscoveredNodePingWorkerConfig, DhtDiscoveredNodePingWorkerExit,
};
pub use dht_discovered_node_scheduler::{
    DhtDiscoveredNodeFindInput, DhtDiscoveredNodeFindInputClosed, DhtDiscoveredNodePingInput,
    DhtDiscoveredNodePingInputClosed, DhtDiscoveredNodeRouteReceiver, DhtDiscoveredNodeRoutes,
    DhtDiscoveredNodeSampleInfoHashesInput, DhtDiscoveredNodeSampleInfoHashesInputClosed,
    DhtDiscoveredNodeSampleInfoHashesReceiver, DhtDiscoveredNodeScheduler,
    DhtDiscoveredNodeSchedulerConfig, DhtDiscoveredNodeSchedulerConfigError,
    DhtDiscoveredNodeSchedulerExit, DhtDiscoveredNodeSchedulerStats,
    DhtDiscoveredNodeSchedulerStatsHandle,
};
pub use dht_discovery::{
    dht_discovery_channel, DhtDiscoveryOffer, DhtDiscoveryPermit, DhtDiscoveryReceiver,
    DhtDiscoveryReserveError, DhtDiscoverySender, DhtDiscoveryStats, DhtDiscoveryStatsHandle,
};
pub use dht_dispatch::{DhtDispatchOutcome, DhtDispatcher};
pub use dht_driver::{DhtDriver, DhtDriverError, DhtDriverOutcome};
pub use dht_inbound_stats::{DhtInboundStats, DhtInboundStatsSnapshot};
pub use dht_info_hash_deduper::DhtInfoHashDeduper;
pub use dht_info_hash_triage::{
    dht_info_hash_triage_channel, DhtInfoHashTriageInput, DhtInfoHashTriageInputClosed,
    DhtInfoHashTriageReceiver, DhtInfoHashTriageRequest, DHT_INFO_HASH_TRIAGE_DEFAULT_CAPACITY,
};
pub use dht_info_hash_triage_routes::{
    dht_get_peers_channel, dht_get_peers_channel_with_capacity, dht_scrape_channel,
    dht_scrape_channel_with_capacity, DhtGetPeersInput, DhtGetPeersInputClosed,
    DhtGetPeersReceiver, DhtScrapeInput, DhtScrapeInputClosed, DhtScrapeReceiver,
    DHT_GET_PEERS_ROUTE_CAPACITY, DHT_SCRAPE_ROUTE_CAPACITY,
};
pub use dht_oldest_node_find_producer::{
    DhtOldestNodeFindProducer, DhtOldestNodeFindProducerExit, DhtOldestNodeFindProducerStats,
    DhtOldestNodeFindProducerStatsHandle,
};
pub use dht_oldest_node_ping_producer::{
    DhtOldestNodePingProducer, DhtOldestNodePingProducerExit, DhtOldestNodePingProducerStats,
    DhtOldestNodePingProducerStatsHandle,
};
pub use dht_responder::{
    DhtResponder, DhtResponderError, DhtResponderLookup, DhtResponderSample, DhtResponderTable,
};
pub use dht_runtime::{
    DhtRuntime, DhtRuntimeClient, DhtRuntimeClientError, DhtRuntimeConfig, DhtRuntimeConfigError,
    DhtRuntimeControlledQueryError, DhtRuntimeDriverError, DhtRuntimeExit, DhtRuntimeStartError,
    DHT_DISCOVERY_QUEUE_CAPACITY,
};
pub use dht_runtime_health::{
    DhtRuntimeHealthFailure, DhtRuntimeHealthHandle, DhtRuntimeHealthSnapshot,
    DhtRuntimeHealthStatus, DHT_RUNTIME_HEALTH_INITIAL_GRACE, DHT_RUNTIME_HEALTH_SUCCESS_FRESHNESS,
};
pub use dht_sample_infohashes_producer::{
    DhtSampleInfoHashesProducer, DhtSampleInfoHashesProducerExit, DhtSampleInfoHashesProducerStats,
    DhtSampleInfoHashesProducerStatsHandle,
};
pub use dht_sample_infohashes_worker::{
    DhtSampleInfoHashesWorker, DhtSampleInfoHashesWorkerConfig, DhtSampleInfoHashesWorkerExit,
    DhtSampleInfoHashesWorkerStats, DhtSampleInfoHashesWorkerStatsHandle,
};
pub use dht_send::{send_dht_reply, DhtSendError};
pub use dht_supervisor::{DhtSupervisor, DhtSupervisorExit};
pub use inbound::{
    InboundError, InboundLimitKind, InboundShapeKind, InboundSyntaxKind,
    MAX_INBOUND_DATAGRAM_BYTES, MAX_INBOUND_NESTING_DEPTH, MAX_INBOUND_VALUES,
};
pub use krpc::{ByteString, KrpcError, KrpcMessage, MessageArgs, MessageReturn, WireError};
pub use ktable::{
    KTable, KTableBep51Support, KTableClock, KTableCommand, KTableLookup, KTableNodeHandle,
    KTableNodeOption, KTableSampleHashesAndNodes, SystemKTableClock,
};
pub use ktable_core::{
    KTableCore, KTableHash, KTableHashLookup, KTableHashPeer, KTableReverseInfo,
    HASH_TABLE_CAPACITY,
};
pub use node_table::{NodeTable, RoutingNode, NODE_TABLE_CAPACITY, NODE_TABLE_CLOSEST_LIMIT};
pub use ping_find_node::{PingFindNodeError, PingFindNodeResponder};
pub use ping_find_node_dispatch::{
    PingFindNodeDispatchOutcome, PingFindNodeDispatcher, PingFindNodeReply,
};
pub use ping_find_node_driver::{
    PingFindNodeDriver, PingFindNodeDriverError, PingFindNodeDriverOutcome,
};
pub use ping_find_node_send::{send_ping_find_node_reply, DatagramSender, PingFindNodeSendError};
pub use ping_find_node_supervisor::{PingFindNodeSupervisor, PingFindNodeSupervisorExit};
pub use query_send::{register_and_send_query, QuerySendError};
pub use rate_limit::{
    DhtInboundRateLimitDenial, DhtInboundRateLimiter, DhtOutboundRateLimiter,
    DhtOutboundRateLimiterConfigError, DhtRateLimitWaitError,
};
pub use receive::{
    DatagramReceiver, ReceiveDispatchError, ReceiveDispatchOutcome, ReceiveDispatcher,
    ReceivedDatagram,
};
pub use reply::DhtReply;
pub use routing_tree::{RoutingPutResult, RoutingTree, ROUTING_ID_BITS};
pub use scrape::{ScrapeBloomError, ScrapeBloomFilter, SCRAPE_BLOOM_BYTES};
pub use tokio_ipv4_udp::{
    TokioIpv4UdpError, TokioIpv4UdpReceiver, TokioIpv4UdpSender, TokioIpv4UdpTransport,
    TokioIpv4UdpWeakSendError, TokioIpv4UdpWeakSender,
};
pub use transaction::{
    CryptoTransactionIdIssuer, DeliveryOutcome, PendingTransaction, RegisterError,
    RegisterSendError, RegisteredQuery, TransactionId, TransactionIdError, TransactionIdIssuer,
    TransactionIdSourceError, TransactionRegistry, TransactionWaitOutcome,
};
