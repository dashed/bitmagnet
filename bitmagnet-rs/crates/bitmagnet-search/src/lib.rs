//! Tantivy search sidecar for bitmagnet.
//!
//! This crate implements the `bitmagnet.v1` `SearchService` (defined in
//! [`proto`]) over gRPC, backed by a Tantivy index. Phase 1 ships the server
//! skeleton and the module layout; the search/index logic lands in Phase 3.
//!
//! Modules:
//! - [`schema`] — Tantivy field definitions + resolved [`schema::Fields`].
//! - [`server`] — the [`SearchServer`] gRPC service (write RPCs + `HealthCheck`
//!   implemented; `Search`/`GetFacets` delegate to the read path).
//! - [`index`] — index open/create lifecycle, reader/writer handles.
//! - [`indexer`] — proto `TorrentDocument` → Tantivy document + upsert/delete.
//! - [`query`] — `tsquery`-to-Tantivy translation + `run_search` (read path).
//! - [`facets`] — the 14 search facets + `run_facets` (read path).
//! - [`tokenizer`] — the `TokenizeFlat` tokenizer (Task #1).
//! - [`transform`] — PG row → proto `TorrentDocument` for the backfill bin.

// `SearchServer` (in `server`) and `build_schema` (in `schema`) intentionally
// echo their module name; the clearer call sites are worth one allow.
#![allow(clippy::module_name_repetitions)]

pub mod facets;
pub mod index;
pub mod indexer;
pub mod pathsearch;
pub mod query;
pub mod schema;
pub mod server;
pub mod tokenizer;
pub mod transform;

/// The generated `bitmagnet.v1` protobuf + gRPC bindings this sidecar serves.
pub use bitmagnet_proto::v1 as proto;

pub use pathsearch::PathSearchServer;
pub use schema::Fields;
pub use server::SearchServer;
