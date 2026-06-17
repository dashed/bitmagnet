//! The 14 search facets exposed by bitmagnet, mirroring the Go `Facet`
//! implementations.

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
