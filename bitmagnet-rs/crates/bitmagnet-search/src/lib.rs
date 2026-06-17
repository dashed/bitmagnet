//! Tantivy search sidecar for bitmagnet.
//!
//! This crate implements the `bitmagnet.v1` `SearchService` (defined in
//! [`proto`]) over gRPC, backed by a Tantivy index. Phase 1 ships the server
//! skeleton and the module layout; the search/index logic lands in Phase 3.
//!
//! Modules:
//! - [`schema`] — Tantivy field definitions (implemented).
//! - [`server`] — the [`SearchServer`] gRPC service (`HealthCheck` only so far).
//! - [`index`] — index open/create lifecycle (Phase 3 stub).
//! - [`query`] — `tsquery`-to-Tantivy translation (Phase 3 stub).
//! - [`facets`] — the 14 search facets (enum + Phase 3 stub).
//! - [`tokenizer`] — the `TokenizeFlat` tokenizer (Phase 3 stub).

// `SearchServer` (in `server`) and `build_schema` (in `schema`) intentionally
// echo their module name; the clearer call sites are worth one allow.
#![allow(clippy::module_name_repetitions)]

pub mod facets;
pub mod index;
pub mod query;
pub mod schema;
pub mod server;
pub mod tokenizer;

/// The generated `bitmagnet.v1` protobuf + gRPC bindings this sidecar serves.
pub use bitmagnet_proto::v1 as proto;

pub use server::SearchServer;
