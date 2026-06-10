//! **L3 path-FTS typeahead** — a second, narrow engine on the search sidecar.
//!
//! This module is independent of the torrent-grained main-search modules
//! ([`crate::schema`]/[`crate::query`]/[`crate::server`]): it builds and serves a
//! **per-torrent path-bag** index (one doc per torrent, holding all its file
//! paths as overlapping char-ngrams) for CJK-correct free-text *path* typeahead.
//! It does NOT replace PG-FTS main search, nor the DuckDB-on-Parquet structured
//! per-file tier — it adds the one thing neither offers cheaply: broad,
//! interactive, substring path search (PS-T3/PS-T4).
//!
//! Layout (mirrors the main-search module split):
//! - [`tokenizer`] — the single char-ngram(2,3) analyzer for writer + query.
//! - [`schema`] — the per-torrent path-bag Tantivy schema + [`schema::PathFields`].
//! - [`index`] — open/create + the single-thread, ≥2 GiB-arena writer.
//! - [`indexer`] — build the path-bag doc + upsert/delete.
//! - [`query`] — the `PathTypeahead` read path (guard + gram-conjunction + top-k).
//! - [`follow`] — the `--follow` PG-tail watermark loop + the DB-row→doc glue.
//! - [`server`] — the [`server::PathSearchServer`] gRPC `PathSearchService`.

pub mod follow;
pub mod index;
pub mod indexer;
pub mod query;
pub mod schema;
pub mod server;
pub mod tokenizer;

pub use server::PathSearchServer;
