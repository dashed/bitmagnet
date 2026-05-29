//! [`Content`] and [`TorrentContent`], mirroring Go's `model.Content`
//! (`content.gen.go`) and `model.TorrentContent` (`torrent_contents.gen.go`).
//!
//! As with [`crate::Torrent`], the GORM association graph, full-text `tsv`
//! column, and DB-managed timestamps are omitted; the typed video enums
//! (`NullVideoResolution`, …) are represented as `Option<String>` for now,
//! since only [`ContentType`] / [`FileType`] are needed as first-class enums.

use serde::{Deserialize, Serialize};

use crate::enums::ContentType;
use crate::info_hash::InfoHash;

/// A row from the `content` table — externally-sourced metadata (TMDB, etc.)
/// keyed by `(type, source, id)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Content {
    /// Content type (part of the primary key); serialised as `"type"` to match
    /// the Go JSON tag.
    #[serde(rename = "type")]
    pub content_type: ContentType,
    /// Metadata source identifier, e.g. `"tmdb"` (part of the primary key).
    pub source: String,
    /// Source-specific id (part of the primary key).
    pub id: String,
    /// Display title.
    pub title: String,
    /// Release year, when known.
    #[serde(default)]
    pub release_year: Option<u32>,
    /// Original-language ISO code, when known.
    #[serde(default)]
    pub original_language: Option<String>,
    /// Original-language title, when known.
    #[serde(default)]
    pub original_title: Option<String>,
    /// Plot overview / synopsis.
    #[serde(default)]
    pub overview: Option<String>,
    /// Runtime in minutes, when known.
    #[serde(default)]
    pub runtime: Option<u32>,
    /// Popularity score from the metadata source.
    #[serde(default)]
    pub popularity: Option<f32>,
    /// Average vote / rating.
    #[serde(default)]
    pub vote_average: Option<f32>,
    /// Number of votes backing [`Self::vote_average`].
    #[serde(default)]
    pub vote_count: Option<u32>,
}

/// A row from the `torrent_contents` table — the join between a torrent and its
/// (optional) classified content, carrying the fields the search index needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentContent {
    /// Surrogate primary key.
    pub id: String,
    /// Info hash of the associated torrent.
    pub info_hash: InfoHash,
    /// Classified content type, when known.
    #[serde(default)]
    pub content_type: Option<ContentType>,
    /// Metadata source of the linked [`Content`], when classified.
    #[serde(default)]
    pub content_source: Option<String>,
    /// Source-specific id of the linked [`Content`], when classified.
    #[serde(default)]
    pub content_id: Option<String>,
    /// Detected languages (the `languages` JSON column).
    #[serde(default)]
    pub languages: Vec<String>,
    /// Video resolution, e.g. `"1080p"`.
    #[serde(default)]
    pub video_resolution: Option<String>,
    /// Video source, e.g. `"BluRay"`.
    #[serde(default)]
    pub video_source: Option<String>,
    /// Video codec, e.g. `"x265"`.
    #[serde(default)]
    pub video_codec: Option<String>,
    /// Scene release group, when detected.
    #[serde(default)]
    pub release_group: Option<String>,
    /// Swarm seeders, when known.
    #[serde(default)]
    pub seeders: Option<u32>,
    /// Swarm leechers, when known.
    #[serde(default)]
    pub leechers: Option<u32>,
    /// Publication time as a Unix timestamp in seconds (maps to the proto
    /// `published_at` / Tantivy Date field).
    pub published_at: i64,
    /// Total size in bytes (denormalised from the torrent).
    pub size: u64,
    /// Number of files, when known.
    #[serde(default)]
    pub files_count: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_serde_round_trips() {
        let c = Content {
            content_type: ContentType::Movie,
            source: "tmdb".to_owned(),
            id: "603".to_owned(),
            title: "The Matrix".to_owned(),
            release_year: Some(1999),
            original_language: Some("en".to_owned()),
            original_title: Some("The Matrix".to_owned()),
            overview: None,
            runtime: Some(136),
            popularity: Some(42.5),
            vote_average: Some(8.2),
            vote_count: Some(24000),
        };
        let bytes = rmp_serde::to_vec_named(&c).unwrap();
        let back: Content = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(c, back);
    }

    #[test]
    fn torrent_content_serde_round_trips() {
        let tc = TorrentContent {
            id: "abc".to_owned(),
            info_hash: "0123456789abcdef0123456789abcdef01234567".parse().unwrap(),
            content_type: Some(ContentType::TvShow),
            content_source: Some("tmdb".to_owned()),
            content_id: Some("1399".to_owned()),
            languages: vec!["en".to_owned(), "de".to_owned()],
            video_resolution: Some("1080p".to_owned()),
            video_source: Some("BluRay".to_owned()),
            video_codec: Some("x265".to_owned()),
            release_group: None,
            seeders: Some(123),
            leechers: Some(4),
            published_at: 1_700_000_000,
            size: 9_000_000_000,
            files_count: Some(10),
        };
        let bytes = rmp_serde::to_vec_named(&tc).unwrap();
        let back: TorrentContent = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(tc, back);
    }
}
