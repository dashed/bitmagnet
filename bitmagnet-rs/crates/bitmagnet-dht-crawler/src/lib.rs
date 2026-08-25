//! Owned database- and policy-dependent DHT crawler behavior above the
//! protocol, runtime, scheduler, and maintenance primitives in `bitmagnet-dht`.
//!
//! This crate consumes typed DHT discovery products and makes database- and
//! policy-dependent routing decisions. The core worker still receives injected
//! collaborators, while this crate supplies concrete adapters for PostgreSQL
//! triage lookup and the persistent blocking manager. Application ownership,
//! lifecycle, and shutdown wiring remain deferred.

mod blocking_manager_filter;
mod get_peers;
mod info_hash_triage;
mod persist_source_route;
mod persist_torrent_route;
mod pg_torrent_triage_lookup;
mod request_meta_info_route;
mod scrape;

#[cfg(test)]
mod get_peers_parity;

#[cfg(test)]
mod scrape_parity;

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
pub use persist_source_route::{
    dht_persist_source_channel, DhtPersistSourceInput, DhtPersistSourceInputClosed,
    DhtPersistSourceReceiver, DhtPersistSourceRequest, DHT_PERSIST_SOURCE_ROUTE_CAPACITY,
};
pub use persist_torrent_route::{
    dht_persist_torrent_channel, DhtPersistTorrentInput, DhtPersistTorrentInputClosed,
    DhtPersistTorrentReceiver, DhtPersistTorrentRequest, DHT_PERSIST_TORRENT_ROUTE_CAPACITY,
};
pub use pg_torrent_triage_lookup::PgDhtTorrentTriageLookup;
pub use request_meta_info_route::{
    dht_request_meta_info_channel, DhtMetaInfoRequest, DhtRequestMetaInfoInput,
    DhtRequestMetaInfoInputClosed, DhtRequestMetaInfoReceiver,
    DHT_REQUEST_META_INFO_ROUTE_CAPACITY,
};
pub use scrape::{
    DhtScrapeWorker, DhtScrapeWorkerConfig, DhtScrapeWorkerExit, DhtScrapeWorkerStats,
    DhtScrapeWorkerStatsHandle,
};
