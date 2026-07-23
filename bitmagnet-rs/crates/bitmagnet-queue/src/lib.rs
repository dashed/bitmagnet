//! Phase-3 Lane Q — PG job-queue substrate. Contract:
//! `docs/dev/rust-rewrite/phase3-contracts.md` §1.
//!
//! This milestone ports the two pure-logic pieces of the queue substrate whose
//! output must be byte/bound-identical to the Go oracle:
//!
//! - **Fingerprint** ([`job`] + [`message`]): `hex(sha256(queue || json(payload)))`
//!   over the three job payload types, reproducing Go's `encoding/json` exactly
//!   (per-type casing, `omitempty` on struct/array fields, sorted map keys).
//! - **Backoff** ([`backoff`]): the deterministic base + bounded jitter of
//!   `queue.CalculateBackoff`.
//!
//! Deferred to later milestones (need a live PG): the dequeue differential
//! (`ORDER BY (status='retry'), priority, run_after` + `SKIP LOCKED` +
//! literal `LIMIT 1`), the poll loop, the producer/consumer API, and the
//! scratch-queue shadow mirror-writer.

pub mod backoff;
pub mod id;
pub mod job;
pub mod message;

pub use id::ProtocolId;
pub use job::{
    fingerprint, new_queue_job, JobError, QueueJob, QueueJobOptions, QueueJobStatus,
    DEFAULT_ARCHIVAL_DURATION,
};
pub use message::{
    blob_migration_job, process_torrent_batch_job, process_torrent_job, BlobMigrationParams,
    GoTime, ProcessTorrentBatchParams, ProcessTorrentParams, BLOB_MIGRATION,
    BLOB_MIGRATION_DEFAULT_CHUNK_SIZE, CLASSIFY_MODE_DEFAULT, CLASSIFY_MODE_REMATCH,
    PROCESS_TORRENT, PROCESS_TORRENT_BATCH,
};
