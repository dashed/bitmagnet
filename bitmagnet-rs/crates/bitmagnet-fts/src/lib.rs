//! Shared FTS tokenizer + `tsquery` builder for the bitmagnet Rust rewrite.
//!
//! Skeleton crate (Phase-2 P0-1). The FTS tokenizer port currently living in
//! `bitmagnet-search-query::fts` moves here in P0-2 so every FTS consumer
//! (the search-query full builder, a future ingest-side indexer) shares ONE
//! tokenizer instead of copying it.
