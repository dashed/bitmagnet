//! SQLx-based PostgreSQL access layer for bitmagnet, mirroring the relevant
//! parts of Go's `internal/database/`.
//!
//! * [`DbConfig`] — connection settings, from `BITMAGNET_POSTGRES_*` env vars
//!   or explicit fields (mirrors `internal/database/postgres.Config`).
//! * [`connect`] / [`ping`] — build a [`PgPool`] and health-check it.
//! * [`stream_torrents_with_files`] + [`TorrentWithBlob`] — keyset-paginated
//!   read of torrents together with their compressed `files_data` blob.
//! * [`stream_torrents_for_index`] + [`TorrentForIndexPage`] — keyset-paginated
//!   read of `torrent_contents` joined with torrents + content (one row per
//!   search document), for the Phase 3 search backfill.
//! * [`batch_torrent_files_ext_agg`] + [`FileExtAgg`] — the per-(torrent,
//!   extension) `torrent_files` aggregate, the actual side of the L2 `verify`
//!   parity checker (Job A).
//! * [`read_deleted_torrents`] / [`prune_deleted_torrents`] — the deletion-audit
//!   window read and merge-base-gated retention prune.
//! * [`stream_changed_torrent_keys`] +
//!   [`stream_torrents_for_index_info_hashes`] — the 00024 incremental follow
//!   contract for the main Tantivy sidecar.
//!
//! All queries use the runtime [`sqlx::query`] API (not the compile-time
//! `query!` macros), so the crate builds and tests green without a live
//! database or `DATABASE_URL`.
//!
//! See `docs/rust-rewrite-plan.md`.

mod agg;
mod config;
mod deleted;
mod error;
mod pool;
mod stream;

pub use agg::{batch_torrent_files_ext_agg, FileExtAgg};
pub use config::DbConfig;
pub use deleted::{prune_deleted_torrents, read_deleted_torrents};
pub use error::{DbError, Result};
pub use pool::{connect, ping};
pub use stream::{
    stream_changed_torrent_keys, stream_changed_torrents, stream_torrents_for_index,
    stream_torrents_for_index_info_hashes, stream_torrents_with_files, ChangedTorrentKey,
    TorrentForIndex, TorrentForIndexPage, TorrentWithBlob,
};

/// Re-exported so callers can name the pool type without depending on `sqlx`
/// directly.
pub use sqlx::PgPool;
