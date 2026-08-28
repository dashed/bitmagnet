//! `bitmagnet-filesearch` — the L2b DuckDB-on-Parquet file-search sidecar.
//!
//! Serves the `bitmagnet.v1` `FileSearchService` over gRPC from the immutable
//! Parquet **generations** produced by `bitmagnet-parquet`. The pieces:
//! * [`query`] — validated, engine-agnostic query intent (proto → domain).
//! * [`sql`] — the FB-B1d safe SQL builder (server-controlled paths +
//!   identifiers; user values are bound `?` params; ILIKE-escaped path search).
//! * [`generation`] — resolves the current base+segments+delta layer set and
//!   swaps it atomically on reload (FB-B1c: immutable, read-only).
//! * [`engine`] — the [`engine::Engine`] trait, with an in-memory reference
//!   engine (tests) and the feature-gated `DuckEngine` (production).
//! * [`service`] — the gRPC service: proto mapping, the CB concurrency gate
//!   (semaphore + `spawn_blocking`), per-query deadlines.

pub mod engine;
pub mod generation;
pub mod parity;
pub mod query;
pub mod service;
pub mod sql;

pub use engine::{Engine, InMemoryEngine};
pub use generation::{GenerationManager, LoadedGeneration};
pub use service::{FileSearchServer, ServiceConfig};

/// Re-export the generated proto module for the binary / clients.
pub use bitmagnet_proto::v1 as proto;
