//! L3 pathsearch: a torrent-grained Tantivy path-bag candidate index.
//!
//! This is intentionally separate from the main [`crate::SearchServer`]. The
//! main search service indexes torrent-content documents for PG-FTS parity; L3
//! indexes one path-bag document per torrent and returns candidate `info_hash`
//! values for L1/L2 exact refinement.

pub mod document;
pub mod index;
pub mod indexer;
pub mod query;
pub mod schema;
pub mod server;
pub mod watermark;

pub use document::PathDocument;
pub use server::PathSearchServer;
