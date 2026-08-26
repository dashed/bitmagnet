//! Owned database- and policy-dependent DHT crawler behavior above the
//! protocol, runtime, scheduler, and maintenance primitives in `bitmagnet-dht`.
//!
//! This crate consumes typed DHT discovery products and makes database- and
//! policy-dependent routing decisions. The core worker still receives injected
//! collaborators, while this crate supplies concrete adapters for PostgreSQL
//! triage lookup and the persistent blocking manager. A staged pipeline
//! supervisor owns the constructed crawler lifecycle. The pure application
//! configuration contract is present; database-secret loading, executable,
//! readiness, and deployment wiring remain deferred.

mod app_config;
mod blocking_manager_filter;
mod downstream_composition;
mod get_peers;
mod info_hash_triage;
mod observe_info_hash;
mod observe_only_supervisor;
mod peer_wire_meta_info_requester;
mod persist_source;
mod persist_source_route;
mod persist_torrent;
mod persist_torrent_route;
mod persist_torrent_worker;
mod pg_source_batch_writer;
mod pg_torrent_batch_writer;
mod pg_torrent_triage_lookup;
mod pg_torrent_v2_lookup;
mod pipeline_supervisor;
mod rate_limited_meta_info_requester;
mod request_meta_info;
mod request_meta_info_route;
mod scrape;

#[cfg(test)]
mod composition_parity;

#[cfg(test)]
mod get_peers_parity;

#[cfg(test)]
mod peer_wire_meta_info_requester_parity;

#[cfg(test)]
mod request_meta_info_parity;

#[cfg(test)]
mod persist_source_parity;

#[cfg(test)]
mod persist_torrent_parity;

#[cfg(test)]
mod scrape_parity;

pub use app_config::{
    DhtCrawlerAppConfig, DhtCrawlerAppConfigError, DhtCrawlerAppConfigErrorKind,
    DhtCrawlerAppProjection, DhtCrawlerObserveOnlyAppConfig, DhtCrawlerObserveOnlyAppProjection,
    DEFAULT_BOOTSTRAP_NODES, DHT_CRAWLER_MAX_SCALING_FACTOR,
};
pub use downstream_composition::{
    DhtCrawlerDownstreamComposition, DhtCrawlerDownstreamConfig, DhtCrawlerDownstreamConfigError,
    DhtCrawlerDownstreamLaneConfig, DhtCrawlerDownstreamStatsHandle,
    DhtCrawlerDownstreamWithConfigError, DhtCrawlerDownstreamWorkers,
};
pub use get_peers::{
    DhtGetPeersWorker, DhtGetPeersWorkerConfig, DhtGetPeersWorkerExit, DhtGetPeersWorkerStats,
    DhtGetPeersWorkerStatsHandle,
};
pub use info_hash_triage::{
    DhtInfoHashBlockFilter, DhtInfoHashTriageClock, DhtInfoHashTriageConfig,
    DhtInfoHashTriageStats, DhtInfoHashTriageStatsHandle, DhtInfoHashTriageWorker,
    DhtInfoHashTriageWorkerExit, DhtTorrentTriageLookup, DhtTorrentTriageRow,
    SystemDhtInfoHashTriageClock, TriageCollaboratorError, DHT_INFO_HASH_TRIAGE_BATCH_INTERVAL,
    DHT_INFO_HASH_TRIAGE_BATCH_LIMIT, DHT_INFO_HASH_TRIAGE_RESCRAPE_THRESHOLD,
    DHT_INFO_HASH_TRIAGE_SAVE_FILES_THRESHOLD,
};
pub use observe_info_hash::{
    DhtInfoHashObservationStats, DhtInfoHashObservationStatsHandle, DhtInfoHashObservationWorker,
    DhtInfoHashObservationWorkerExit,
};
pub use observe_only_supervisor::{
    DhtCrawlerObserveOnlyConfig, DhtCrawlerObserveOnlyConfigError, DhtCrawlerObserveOnlyExit,
    DhtCrawlerObserveOnlyObservabilityHandle, DhtCrawlerObserveOnlyObservabilitySnapshot,
    DhtCrawlerObserveOnlyStartError, DhtCrawlerObserveOnlySupervisor, DhtCrawlerObserveOnlyTrigger,
};
pub use peer_wire_meta_info_requester::{
    random_metainfo_peer_id, DhtPeerWireMetaInfoRequester, DhtPeerWireMetaInfoRequesterConfig,
    DhtPeerWireMetaInfoRequesterError, DhtPeerWireMetaInfoRequesterStage,
    DHT_PEER_WIRE_CONNECT_TIMEOUT, DHT_PEER_WIRE_LOCAL_UT_METADATA_ID,
    DHT_PEER_WIRE_MAX_METADATA_SIZE, DHT_PEER_WIRE_METADATA_PIECE_SIZE,
    DHT_PEER_WIRE_REQUEST_TIMEOUT,
};
pub use persist_source::{
    DhtPersistSourceWorker, DhtPersistSourceWorkerConfig, DhtPersistSourceWorkerExit,
    DhtPersistSourceWorkerStats, DhtPersistSourceWorkerStatsHandle, DhtSourceBatchWriteError,
    DhtSourceBatchWriter, DhtSourceWrite, PersistSourceCollaboratorError,
    DHT_PERSIST_SOURCE_BATCH_INTERVAL, DHT_PERSIST_SOURCE_BATCH_LIMIT,
};
pub use persist_source_route::{
    dht_persist_source_channel, DhtPersistSourceInput, DhtPersistSourceInputClosed,
    DhtPersistSourceReceiver, DhtPersistSourceRequest, DHT_PERSIST_SOURCE_ROUTE_CAPACITY,
};
pub use persist_torrent::{
    plan_dht_torrent_batch, DhtResolvedExistingV2, DhtTorrentFileSummaryWrite, DhtTorrentFileWrite,
    DhtTorrentPersistPlan, DhtTorrentPiecesWrite, DhtTorrentPlanConfig, DhtTorrentPlanCounts,
    DhtTorrentPlanDiagnostic, DhtTorrentPlanner, DhtTorrentProjectionError,
    DhtTorrentProjectionFailure, DhtTorrentSourceLinkWrite, DhtTorrentTransactionPlan,
    DhtTorrentWrite, DHT_TORRENT_CLASSIFIER_BATCH_LIMIT, DHT_TORRENT_CLASSIFIER_DELAY,
    DHT_TORRENT_DEFAULT_SAVE_FILES_THRESHOLD, DHT_TORRENT_SOURCE,
};
pub use persist_torrent_route::{
    dht_persist_torrent_channel, DhtPersistTorrentInput, DhtPersistTorrentInputClosed,
    DhtPersistTorrentReceiver, DhtPersistTorrentRequest, DHT_PERSIST_TORRENT_ROUTE_CAPACITY,
};
pub use persist_torrent_worker::{
    DhtExistingV2Row, DhtPersistTorrentWorker, DhtPersistTorrentWorkerConfig,
    DhtPersistTorrentWorkerExit, DhtPersistTorrentWorkerStats, DhtPersistTorrentWorkerStatsHandle,
    DhtTorrentBatchWriteError, DhtTorrentBatchWriter, DhtTorrentV2Lookup,
    PersistTorrentCollaboratorError, DHT_PERSIST_TORRENT_BATCH_INTERVAL,
    DHT_PERSIST_TORRENT_BATCH_LIMIT, DHT_PERSIST_TORRENT_LOOKUP_CHUNK_LIMIT,
};
pub use pg_source_batch_writer::{
    PgDhtSourceBatchWriter, PgDhtSourceBatchWriterError, PG_DHT_SOURCE_WRITE_CHUNK_LIMIT,
};
pub use pg_torrent_batch_writer::{
    PgDhtTorrentBatchWriteStage, PgDhtTorrentBatchWriter, PgDhtTorrentBatchWriterError,
    PG_DHT_TORRENT_SMALL_WRITE_CHUNK_LIMIT, PG_DHT_TORRENT_WRITE_CHUNK_LIMIT,
};
pub use pg_torrent_triage_lookup::PgDhtTorrentTriageLookup;
pub use pg_torrent_v2_lookup::PgDhtTorrentV2Lookup;
pub use pipeline_supervisor::{
    DhtCrawlerPipelineBlockingResult, DhtCrawlerPipelineCompletedExit,
    DhtCrawlerPipelineDownstreamChild, DhtCrawlerPipelineDownstreamExits,
    DhtCrawlerPipelineDownstreamObservabilitySnapshot, DhtCrawlerPipelineExit,
    DhtCrawlerPipelineHandles, DhtCrawlerPipelineLifecycle, DhtCrawlerPipelineLifecycleHandle,
    DhtCrawlerPipelineMaintenanceObservabilitySnapshot, DhtCrawlerPipelineObservabilityHandle,
    DhtCrawlerPipelineObservabilitySnapshot, DhtCrawlerPipelineObservedLifecycle,
    DhtCrawlerPipelineRuntimeObservabilitySnapshot, DhtCrawlerPipelineSupervisor,
    DhtCrawlerPipelineTrigger,
};
pub use request_meta_info::{
    DefaultDhtMetaInfoBanningChecker, DhtInfoHashBlocker, DhtMetaInfoBanningChecker,
    DhtMetaInfoRequester, DhtRequestMetaInfoWorker, DhtRequestMetaInfoWorkerConfig,
    DhtRequestMetaInfoWorkerExit, DhtRequestMetaInfoWorkerStats,
    DhtRequestMetaInfoWorkerStatsHandle, RequestMetaInfoCollaboratorError,
};
pub use request_meta_info_route::{
    dht_request_meta_info_channel, dht_request_meta_info_channel_with_capacity, DhtMetaInfoRequest,
    DhtRequestMetaInfoInput, DhtRequestMetaInfoInputClosed, DhtRequestMetaInfoReceiver,
    DHT_REQUEST_META_INFO_ROUTE_CAPACITY,
};
pub use scrape::{
    DhtScrapeWorker, DhtScrapeWorkerConfig, DhtScrapeWorkerExit, DhtScrapeWorkerStats,
    DhtScrapeWorkerStatsHandle,
};
