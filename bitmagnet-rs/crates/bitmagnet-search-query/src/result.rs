//! [`SearchResultItem`] — one hydrated result row, carrying everything Lane T
//! needs to render a Torznab `<item>` (see `internal/torznab/adapter/
//! search_result.go` for the fields consumed).
//!
//! The v1 parity gate (Q3) only compares the ordered **info-hash list**, so the
//! authoritative field for parity is [`SearchResultItem::info_hash`]. The
//! remaining fields exist so Lane T can build XML; their exact hydration
//! (torrents join for `name`/magnet, content join for year/identifiers) is Q2's
//! to implement and Lane G's XML goldens to pin. The struct is `non_exhaustive`
//! so fields can be added without breaking Lane T.

use crate::criteria::{Episodes, Video3D, VideoResolution};
use bitmagnet_model::{ContentType, InfoHash};

/// A single search result row.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SearchResultItem {
    /// The torrent info hash — the parity key and the Torznab `guid` /
    /// `infohash` attr.
    pub info_hash: InfoHash,
    /// `torrents.name` (hydrated). The Torznab item `title`.
    pub name: String,
    /// `torrent_contents.size` in bytes.
    pub size: u64,
    /// `torrent_contents.content_type`, if classified.
    pub content_type: Option<ContentType>,
    /// `torrent_contents.published_at` as a Unix epoch (seconds), mirroring the
    /// `EXTRACT(EPOCH ...)::bigint` house style in `bitmagnet-db`. Lane T
    /// formats it as the RSS date.
    pub published_at: i64,
    /// `torrent_contents.seeders`.
    pub seeders: Option<u32>,
    /// `torrent_contents.leechers`.
    pub leechers: Option<u32>,
    /// `torrent_contents.files_count`.
    pub files_count: Option<u32>,
    /// `torrent_contents.video_resolution`.
    pub video_resolution: Option<VideoResolution>,
    /// `torrent_contents.video_3d`.
    pub video_3d: Option<Video3D>,
    /// `torrent_contents.video_codec` label (raw string for v1; a typed enum
    /// can replace this later without a breaking change thanks to
    /// `non_exhaustive`).
    pub video_codec: Option<String>,
    /// `torrent_contents.release_group`.
    pub release_group: Option<String>,
    /// Episode map hydrated from `torrent_contents.episodes`.
    pub episodes: Episodes,
    /// Content release year (hydrated from `content`), if known.
    pub release_year: Option<i32>,
    /// `imdb` external id for the content, if any (hydrated from `content` /
    /// `content_attributes`).
    pub imdb_id: Option<String>,
    /// `tmdb` external id for the content, if any.
    pub tmdb_id: Option<String>,
}
