//! Phase-3 Lane Q — PG job-queue substrate. Contract:
//! `docs/dev/rust-rewrite/phase3-contracts.md` §1.
//!
//! This milestone ports the pure-logic pieces of the queue substrate whose
//! output must be byte/bound-identical to the Go oracle:
//!
//! - **Fingerprint** ([`job`] + [`message`]): `hex(sha256(queue || json(payload)))`
//!   over the three job payload types, reproducing Go's `encoding/json` exactly
//!   (per-type casing, `omitempty` on struct/array fields, sorted map keys).
//! - **Backoff** ([`backoff`]): the deterministic base + bounded jitter of
//!   `queue.CalculateBackoff`.
//! - **Batch planning** ([`batch`]): ordered database pages to child jobs,
//!   priorities, keyset continuation, and the deliberately page-granular chunk
//!   boundary.
//!
//! The PostgreSQL runtime still owns candidate selection, the frozen
//! dequeue/settlement transaction, the poll-only consumer, and the bounded
//! processed-row shadow mirror.

pub mod backoff;
pub mod batch;
pub mod id;
pub mod job;
pub mod message;
pub mod pg;

pub use batch::{BatchPlan, BatchPlanError, BatchPlanner};
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
pub use pg::{
    ConsumeOutcome, Consumer, ConsumerConfig, DequeuedJob, MirrorBootstrap, MirrorConfig,
    MirrorCursor, MirrorIneligibleReason, MirrorReport, QueuePgError, QueueStore,
    PROCESS_TORRENT_SHADOW,
};
