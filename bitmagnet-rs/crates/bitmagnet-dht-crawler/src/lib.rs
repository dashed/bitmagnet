//! Owned database- and policy-dependent DHT crawler behavior above the
//! protocol, runtime, scheduler, and maintenance primitives in `bitmagnet-dht`.
//!
//! This crate consumes typed DHT discovery products and makes database- and
//! policy-dependent routing decisions. PostgreSQL and the persistent blocking
//! manager remain injected boundaries so the core worker is deterministic,
//! testable without external services, and reusable by later application
//! composition.

mod info_hash_triage;
mod pg_torrent_triage_lookup;

pub use info_hash_triage::{
    DhtInfoHashBlockFilter, DhtInfoHashTriageClock, DhtInfoHashTriageConfig,
    DhtInfoHashTriageStats, DhtInfoHashTriageStatsHandle, DhtInfoHashTriageWorker,
    DhtInfoHashTriageWorkerExit, DhtTorrentTriageLookup, DhtTorrentTriageRow,
    SystemDhtInfoHashTriageClock, TriageCollaboratorError, DHT_INFO_HASH_TRIAGE_BATCH_INTERVAL,
    DHT_INFO_HASH_TRIAGE_BATCH_LIMIT, DHT_INFO_HASH_TRIAGE_RESCRAPE_THRESHOLD,
    DHT_INFO_HASH_TRIAGE_SAVE_FILES_THRESHOLD,
};
pub use pg_torrent_triage_lookup::PgDhtTorrentTriageLookup;
