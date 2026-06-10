//! SQLx-based PostgreSQL access layer for bitmagnet, mirroring the relevant
//! parts of Go's `internal/database/`.
//!
//! * [`DbConfig`] — connection settings, from `BITMAGNET_POSTGRES_*` env vars
//!   or explicit fields (mirrors `internal/database/postgres.Config`).
//! * [`connect`] / [`ping`] — build a [`PgPool`] and health-check it.
//! * [`stream_torrents_with_files`] + [`TorrentWithBlob`] — keyset-paginated
//!   read of torrents together with their compressed `files_data` blob.
//! * [`stream_torrents_for_index`] + [`TorrentForIndex`] — keyset-paginated read
//!   of `torrent_contents` joined with torrents + content (one row per search
//!   document), for the Phase 3 search backfill.
//!
//! All queries use the runtime [`sqlx::query`] API (not the compile-time
//! `query!` macros), so the crate builds and tests green without a live
//! database or `DATABASE_URL`.
//!
//! See `docs/rust-rewrite-plan.md`.

mod config;
mod error;
mod pool;
mod stream;

pub use config::DbConfig;
pub use error::{DbError, Result};
pub use pool::{connect, ping};
pub use stream::{
    stream_changed_torrents, stream_torrents_for_index, stream_torrents_with_files,
    TorrentForIndex, TorrentWithBlob,
};

/// Re-exported so callers can name the pool type without depending on `sqlx`
/// directly.
pub use sqlx::PgPool;
