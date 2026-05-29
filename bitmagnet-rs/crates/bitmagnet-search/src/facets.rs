//! The 14 search facets exposed by bitmagnet, mirroring the Go `Facet`
//! implementations, and the `GetFacets` RPC entry point the server delegates to.

use tantivy::{Index, IndexReader};

use crate::proto::{GetFacetsRequest, GetFacetsResponse};
use crate::schema::Fields;

/// Run faceted aggregation for `request.facet_fields` over the documents
/// matching `request.query` + `request.filters`, returning one
/// [`crate::proto::Facet`] per requested field. This is the entry point
/// [`crate::server::SearchServer`] delegates the `GetFacets` RPC to.
///
/// Aggregate over the FAST keyword/numeric fields declared in [`crate::schema`]
/// using Tantivy's built-in term/range aggregations (`tantivy::aggregation`,
/// available without any cargo feature in 0.26).
///
/// Note: 5 of the 14 [`FacetType`]s (`Video3d`, `VideoModifier`,
/// `ReleaseGroup`, `TmdbId`, `AudioLanguage`) have no backing field on the
/// proto `TorrentDocument` yet and cannot be aggregated until it is extended.
///
/// # Errors
/// Returns an error if aggregation fails.
///
/// # Panics
/// Currently always panics — the read path (Task #3) fills this in.
pub fn run_facets(
    _index: &Index,
    _reader: &IndexReader,
    _fields: &Fields,
    _request: GetFacetsRequest,
) -> anyhow::Result<GetFacetsResponse> {
    unimplemented!("read path (Task #3): run_facets")
}

/// A search facet: a field whose distinct values are aggregated into counts
/// alongside a result set.
///
/// The ordering matches the Go facet registry so the `GetFacets` gRPC response
/// is stable across the Go and Rust implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FacetType {
    /// Primary content classification (movie, tv show, ...).
    ContentType,
    /// Number of files in the torrent, bucketed.
    FilesCount,
    /// Per-file content type.
    FileType,
    /// Detected content languages.
    Language,
    /// Release genre(s).
    Genre,
    /// Release year.
    ReleaseYear,
    /// Video resolution (1080p, 2160p, ...).
    VideoResolution,
    /// Video source (`BluRay`, `WEB-DL`, ...).
    VideoSource,
    /// Video codec (x264, x265, ...).
    VideoCodec,
    /// Stereoscopic 3D layout, when present.
    Video3d,
    /// Additional video modifiers (`REMUX`, `PROPER`, ...).
    VideoModifier,
    /// Scene / p2p release group.
    ReleaseGroup,
    /// TMDB identifier of the matched title.
    TmdbId,
    /// Audio track languages.
    AudioLanguage,
}

impl FacetType {
    /// Every facet, in the canonical Go ordering.
    pub const ALL: [FacetType; 14] = [
        FacetType::ContentType,
        FacetType::FilesCount,
        FacetType::FileType,
        FacetType::Language,
        FacetType::Genre,
        FacetType::ReleaseYear,
        FacetType::VideoResolution,
        FacetType::VideoSource,
        FacetType::VideoCodec,
        FacetType::Video3d,
        FacetType::VideoModifier,
        FacetType::ReleaseGroup,
        FacetType::TmdbId,
        FacetType::AudioLanguage,
    ];

    /// The stable string key used by the gRPC and GraphQL APIs.
    #[must_use]
    pub const fn key(self) -> &'static str {
        match self {
            FacetType::ContentType => "content_type",
            FacetType::FilesCount => "files_count",
            FacetType::FileType => "file_type",
            FacetType::Language => "language",
            FacetType::Genre => "genre",
            FacetType::ReleaseYear => "release_year",
            FacetType::VideoResolution => "video_resolution",
            FacetType::VideoSource => "video_source",
            FacetType::VideoCodec => "video_codec",
            FacetType::Video3d => "video_3d",
            FacetType::VideoModifier => "video_modifier",
            FacetType::ReleaseGroup => "release_group",
            FacetType::TmdbId => "tmdb_id",
            FacetType::AudioLanguage => "audio_language",
        }
    }
}

/// Aggregate the value-to-count buckets for `facet` over the current result set.
///
/// Phase 3 reads these from Tantivy's columnar fast-field store.
///
/// # Panics
/// Always panics — not implemented until Phase 3.
#[must_use]
pub fn aggregate(_facet: FacetType) -> Vec<(String, u64)> {
    unimplemented!("Phase 3: aggregate facet counts from the columnar store")
}

#[cfg(test)]
mod tests {
    use super::FacetType;
    use std::collections::HashSet;

    #[test]
    fn fourteen_facets_with_unique_keys() {
        assert_eq!(FacetType::ALL.len(), 14);
        let keys: HashSet<&str> = FacetType::ALL.iter().map(|facet| facet.key()).collect();
        assert_eq!(keys.len(), 14, "facet keys must be unique");
    }
}
