//! `bitmagnet-parquet` — the L2 export/refresh library.
//!
//! Productionizes the throwaway `bench/blob_export`: it streams
//! `torrents.files_data` blobs and writes the immutable Parquet **generations**
//! the DuckDB file-search sidecar serves from.
//!
//! Pipeline (one blob stream, fanned out by [`export::Sinks`]):
//! 1. **decode** ([`decode`]) — blob → [`decode::FileRow`]s, G1 path-derived
//!    extension, decode errors counted (V3).
//! 2. **fact** ([`fact`]) — sorted-by-`(extension, size)` slim Parquet, ZSTD,
//!    1 M row groups, bloom OFF (zone-map pruning).
//! 3. **rollups** ([`rollup`]) — `agg_ext` + `agg_torrent_ext` Parquet (the
//!    `<3 ms` facet/collapse lever; serving Parquet beats a native DuckDB table
//!    100–1000× per the CB campaign).
//! 4. **generation** ([`generation`]) — versioned dirs + atomic `current`
//!    symlink swap + the delta watermark.
//! 5. **delta** ([`delta`]) — minute refresh with a tombstone key set
//!    (FB-B1a supersession incl. deletes); read-time anti-join is the sidecar's job.
//!
//! Jobs: [`export::run_base`], [`export::run_delta`], [`export::run_compaction`].
//!
//! The DROP-gate parity checker (Job A, blob ⟺ `torrent_files`) lives in
//! [`verify`] and runs as the `verify` CLI subcommand.

pub mod decode;
pub mod delta;
pub mod export;
pub mod fact;
pub mod generation;
pub mod manifest;
pub mod rollup;
pub mod schema;
pub mod seal;
pub mod verify;

pub use decode::{DecodeStats, FileRow};
pub use export::{BuildStats, Sinks};
pub use fact::SortMode;
pub use generation::{Kind, Layout};
pub use manifest::{BaseEntry, Manifest, SegmentEntry};
pub use verify::{VerifyOpts, VerifyStats};
