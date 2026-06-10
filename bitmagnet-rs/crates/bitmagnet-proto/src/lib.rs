//! Generated protobuf + gRPC bindings for the bitmagnet Rust services.
//!
//! The code in [`v1`] is generated at build time by `tonic-prost-build` from
//! the `.proto` files in `bitmagnet-rs/proto/`. Both `common.proto` and
//! `search.proto` share the `bitmagnet.v1` package, so prost emits a single
//! `bitmagnet.v1.rs` that this module includes.

/// The `bitmagnet.v1` protobuf package: the shared [`ContentType`] /
/// [`FileType`] enums, the [`TorrentDocument`] search document, and the
/// `SearchService` gRPC client/server.
#[allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    missing_docs,
    unreachable_pub,
    rust_2018_idioms
)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/bitmagnet.v1.rs"));
}

// Convenience re-exports of the most commonly used items.
pub use v1::search_service_client::SearchServiceClient;
pub use v1::search_service_server::{SearchService, SearchServiceServer};
pub use v1::{ContentType, FileType, TorrentDocument};

// L2b file-search service (DuckDB-on-Parquet sidecar).
pub use v1::file_search_service_client::FileSearchServiceClient;
pub use v1::file_search_service_server::{FileSearchService, FileSearchServiceServer};

#[cfg(test)]
mod tests {
    use super::v1::{ContentType, FileType};

    // These assertions lock the enum discriminants to the Go definitions in
    // `internal/protobuf/bitmagnet.proto`. If prost ever renames a variant or
    // a value drifts, the build breaks here rather than silently corrupting
    // the wire contract between Go and Rust.

    #[test]
    fn content_type_values_match_go() {
        assert_eq!(ContentType::Unknown as i32, 0);
        assert_eq!(ContentType::Movie as i32, 1);
        assert_eq!(ContentType::TvShow as i32, 2);
        assert_eq!(ContentType::Music as i32, 3);
        assert_eq!(ContentType::Ebook as i32, 4);
        assert_eq!(ContentType::Comic as i32, 5);
        assert_eq!(ContentType::Audiobook as i32, 6);
        assert_eq!(ContentType::Game as i32, 7);
        assert_eq!(ContentType::Software as i32, 8);
        assert_eq!(ContentType::Xxx as i32, 9);
    }

    #[test]
    fn file_type_values_match_go() {
        assert_eq!(FileType::Unknown as i32, 0);
        assert_eq!(FileType::Archive as i32, 1);
        assert_eq!(FileType::Audio as i32, 2);
        assert_eq!(FileType::Data as i32, 3);
        assert_eq!(FileType::Document as i32, 4);
        assert_eq!(FileType::Image as i32, 5);
        assert_eq!(FileType::Software as i32, 6);
        assert_eq!(FileType::Subtitles as i32, 7);
        assert_eq!(FileType::Video as i32, 8);
    }
}
