//! Phase-1 Torznab/Newznab read adapter (roadmap 05 §Phase 1): axum handler +
//! quick-xml response structs (caps/categories/search/tv/movie/music/book),
//! translating Torznab params → search options → results via
//! `bitmagnet-search-query`. Reads blob/sidecars only — NEVER torrent_files.
//! Serves on the Phase-0 bitmagnet-common bootstrap (serve/metrics/config).
//!
//! Lane contract (phase1-tasks.md): this crate owns HTTP/XML/params/categories.
//! Query construction lives in bitmagnet-search-query.
