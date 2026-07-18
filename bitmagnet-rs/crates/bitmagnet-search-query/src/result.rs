//! Search result shapes for the Torznab row feed and Phase-2 GraphQL surface.
//!
//! [`SearchResultItem`] is one hydrated row carrying everything Lane T needs
//! to render a Torznab `<item>` (see `internal/torznab/adapter/search_result.go`
//! for the fields consumed). [`SearchResult`] wraps those rows with GraphQL
//! pagination, count, and aggregation metadata.
//!
//! The v1 parity gate (Q3) only compares the ordered **info-hash list**, so the
//! authoritative field for parity is [`SearchResultItem::info_hash`]. The
//! remaining fields exist so Lane T can build XML; their exact hydration
//! (torrents join for `name`/magnet, content join for year/identifiers) is Q2's
//! to implement and Lane G's XML goldens to pin. The struct is `non_exhaustive`
//! so fields can be added without breaking Lane T.

use crate::aggregations::Aggregations;
use crate::criteria::{Episodes, Video3D, VideoResolution};
use bitmagnet_model::{
    BlobFile, Content, ContentType, FilesStatus, InfoHash, Torrent, TorrentContent,
};
use serde::{Deserialize, Serialize};

/// The GraphQL-surface search result.
///
/// Mirrors Go `query.GenericResult[TorrentContentResultItem]` in
/// `internal/database/query/query.go` plus gqlmodel
/// `TorrentContentSearchResult`. Lane S S3-S5 fill the count, pagination, and
/// aggregation fields around the separately hydrated membership-query rows.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    /// Exact or budget-estimated total row count requested by `WithTotalCount`.
    pub total_count: u64,
    /// Whether the budgeted count SQL exceeded its budget and returned an
    /// estimate.
    pub total_count_is_estimate: bool,
    /// Whether the `limit + 1` membership query found another page.
    pub has_next_page: bool,
    /// Ordered, hydrated result rows.
    pub items: Vec<SearchResultItem>,
    /// Count-per-value facet groups keyed by facet key.
    pub aggregations: Aggregations,
}

/// A single search result row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
    /// Maximum source-row seeders, preserved for Torznab byte parity.
    pub seeders: Option<u32>,
    /// Maximum source-row leechers, preserved for Torznab byte parity.
    pub leechers: Option<u32>,
    /// `torrent_contents.files_count`. The denormalized file count carried for
    /// ordering and the GraphQL surface; NOT the source of the Torznab `files`
    /// attribute (see [`Self::files_attr_count`]).
    pub files_count: Option<u32>,
    /// Source of the Torznab `files` attribute, matching live Go's observable
    /// output. Go's `Torrent.AfterFind` deserialises `torrents.files_data` into
    /// `Torrent.Files`, so the live serve path emits `files = len(files_data
    /// entries)` when the blob is present and omits the attribute otherwise —
    /// it does NOT use `torrent_contents.files_count`. This field reproduces
    /// that with the presence-gated summary projection `CASE WHEN
    /// torrents.files_data IS NOT NULL THEN torrent_file_summary.file_count ELSE
    /// NULL END`: the summary's `file_count` is co-written with `files_data`
    /// from the same file slice, so it equals `len(files_data)` without
    /// decoding the heavyweight blob. `None` when the row has no `files_data`
    /// blob (e.g. single-file torrents with no enumerated file rows), which is
    /// exactly when Go omits the attribute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_attr_count: Option<u32>,
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
    /// `torrents.info_hash_v1` (20-byte SHA-1), if present. Drives the
    /// `xt=urn:btih:` magnet topic for v1/hybrid torrents (Go `Torrent.MagnetURI`).
    pub info_hash_v1: Option<[u8; 20]>,
    /// `torrents.info_hash_v2` (32-byte SHA-256), if present. Drives the
    /// `xt=urn:btmh:1220<hex>` multihash magnet topic for v2/hybrid torrents.
    pub info_hash_v2: Option<[u8; 32]>,
    /// The scalar `torrent_contents` row used by Go's embedded
    /// `model.TorrentContent`. Fields not represented by the shared Rust model
    /// remain available on this item (episodes/video 3D above and the explicit
    /// supplemental fields below).
    pub torrent_content: TorrentContent,
    /// `torrent_contents.video_modifier`, supplemental to [`TorrentContent`].
    pub torrent_content_video_modifier: Option<String>,
    /// `torrent_contents.created_at` as Unix seconds, supplemental to
    /// [`TorrentContent`].
    pub torrent_content_created_at: i64,
    /// `torrent_contents.updated_at` as Unix seconds, supplemental to
    /// [`TorrentContent`].
    pub torrent_content_updated_at: i64,
    /// The scalar `torrents` row hydrated by Go's
    /// `HydrateTorrentContentTorrent`. `files_data` is `None` unless
    /// [`crate::HydrateOptions::files_data`] is enabled.
    pub torrent: Torrent,
    /// Decoded file rows retained only by the L3/L1 composer while it serves an
    /// exact-refined result. Normal PostgreSQL search results leave this empty;
    /// [`Torrent`]'s `files_data` remains the optional hydration input from
    /// which the composer builds it. The GraphQL mapper, not serde, decides
    /// whether these internal rows are exposed through the API.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refine_files: Vec<BlobFile>,
    /// `torrents.created_at` as Unix seconds, supplemental to [`Torrent`].
    pub torrent_created_at: i64,
    /// `torrents.updated_at` as Unix seconds, supplemental to [`Torrent`].
    pub torrent_updated_at: i64,
    /// `torrents.meta_version`, supplemental to [`Torrent`].
    pub torrent_meta_version: Option<u16>,
    /// Source association rows normally preloaded into Go `Torrent.Sources`.
    pub torrent_sources: Vec<TorrentSourceInfo>,
    /// Tag names normally preloaded into Go `Torrent.Tags`.
    pub torrent_tags: Vec<String>,
    /// Hydrated content metadata. `None` when the content join has no row,
    /// matching gqlmodel's `if item.Content.ID != ""` guard.
    pub content: Option<Content>,
    /// Go `TorrentContent.Title()` derivation.
    pub title: String,
    /// DHT source observation count (Go `DHTSeenStatsFromTorrent`).
    pub dht_seen_count: i32,
    /// DHT source `created_at` as Unix seconds.
    pub dht_first_seen_at: Option<i64>,
    /// DHT source `updated_at` as Unix seconds.
    pub dht_last_seen_at: Option<i64>,
    /// `ts_rank_cd` from the lean membership query, or `0.0` for browse
    /// queries without a tsquery.
    pub query_string_rank: f64,
}

/// Crate-local equivalent of gqlmodel `TorrentSourceInfo`.
///
/// `bitmagnet_model::Torrent` intentionally omits GORM associations, so the
/// query crate carries the keyed source preload separately.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentSourceInfo {
    /// `torrents_torrent_sources.source`.
    pub key: String,
    /// Human-readable `torrent_sources.name`.
    pub name: String,
    /// Import identifier, when the source supplied one.
    pub import_id: Option<String>,
    /// Source-specific seeder count.
    pub seeders: Option<u32>,
    /// Source-specific leecher count.
    pub leechers: Option<u32>,
    /// Source-specific publication time as Unix seconds.
    pub published_at: Option<i64>,
    /// Number of times this source has observed the torrent.
    pub seen_count: u32,
    /// Source row `created_at` as Unix seconds.
    pub first_seen_at: i64,
    /// Source row `updated_at` as Unix seconds.
    pub last_seen_at: i64,
}

impl SearchResultItem {
    /// Test-only constructor for a baseline result row.
    ///
    /// `#[non_exhaustive]` blocks the struct *literal* outside this crate, so
    /// Lane T (`bitmagnet-torznab`) cannot build a `SearchResultItem` to drive
    /// its XML goldens for populated feeds without a live DB. This gives a
    /// minimal identity row (all other fields defaulted); every field is `pub`,
    /// so callers mutate what they need afterwards (e.g. `item.seeders =
    /// Some(5)`). Hidden from docs because it is not part of the runtime API.
    #[doc(hidden)]
    #[must_use]
    pub fn for_test(info_hash: InfoHash, name: impl Into<String>, size: u64) -> Self {
        let name = name.into();
        let torrent = Torrent {
            info_hash,
            name: name.clone(),
            size,
            private: false,
            files_status: FilesStatus::NoInfo,
            extension: None,
            files_count: None,
            files_data: None,
            file_extensions: Vec::new(),
        };
        let torrent_content = TorrentContent {
            id: String::new(),
            info_hash,
            content_type: None,
            content_source: None,
            content_id: None,
            languages: Vec::new(),
            video_resolution: None,
            video_source: None,
            video_codec: None,
            release_group: None,
            seeders: None,
            leechers: None,
            published_at: 0,
            size,
            files_count: None,
        };
        Self {
            info_hash,
            name: name.clone(),
            size,
            content_type: None,
            published_at: 0,
            seeders: None,
            leechers: None,
            files_count: None,
            files_attr_count: None,
            video_resolution: None,
            video_3d: None,
            video_codec: None,
            release_group: None,
            episodes: Episodes::new(),
            release_year: None,
            imdb_id: None,
            tmdb_id: None,
            info_hash_v1: None,
            info_hash_v2: None,
            torrent_content,
            torrent_content_video_modifier: None,
            torrent_content_created_at: 0,
            torrent_content_updated_at: 0,
            torrent,
            refine_files: Vec::new(),
            torrent_created_at: 0,
            torrent_updated_at: 0,
            torrent_meta_version: None,
            torrent_sources: Vec::new(),
            torrent_tags: Vec::new(),
            content: None,
            title: name,
            dht_seen_count: 0,
            dht_first_seen_at: None,
            dht_last_seen_at: None,
            query_string_rank: 0.0,
        }
    }
}

pub(crate) fn derive_title(name: &str, content: Option<&Content>, episodes: &Episodes) -> String {
    let Some(content) =
        content.filter(|content| !content.id.is_empty() && !content.title.is_empty())
    else {
        return name.to_owned();
    };

    let mut parts = vec![content.title.clone()];
    if let Some(original_title) = content
        .original_title
        .as_deref()
        .filter(|original_title| *original_title != content.title)
    {
        parts.push(format!("/ {original_title}"));
    }
    if let Some(release_year) = content.release_year {
        parts.push(format!("({release_year})"));
    }
    if !episodes.is_empty() {
        parts.push(episodes_label(episodes));
    }
    parts.join(" ")
}

pub(crate) fn dht_seen_stats(sources: &[TorrentSourceInfo]) -> (i32, Option<i64>, Option<i64>) {
    sources
        .iter()
        .find(|source| source.key == "dht")
        .map_or((0, None, None), |source| {
            (
                i32::try_from(source.seen_count).unwrap_or(i32::MAX),
                Some(source.first_seen_at),
                Some(source.last_seen_at),
            )
        })
}

fn episodes_label(episodes: &Episodes) -> String {
    let whole_seasons: Vec<i32> = episodes
        .0
        .iter()
        .filter_map(|(season, values)| values.is_empty().then_some(*season))
        .collect();
    let mut whole_ranges = std::collections::BTreeMap::new();
    for (start, end) in contiguous_ranges(&whole_seasons) {
        whole_ranges.insert(start, format!("S{}", format_range(start, end)));
    }

    episodes
        .0
        .iter()
        .filter_map(|(season, values)| {
            if values.is_empty() {
                whole_ranges.get(season).cloned()
            } else {
                let mut values = values.clone();
                values.sort_unstable();
                values.dedup();
                let episode_parts = contiguous_ranges(&values)
                    .into_iter()
                    .map(|(start, end)| format!("E{}", format_range(start, end)))
                    .collect::<Vec<_>>()
                    .join(",");
                Some(format!("S{season:02}{episode_parts}"))
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn contiguous_ranges(values: &[i32]) -> Vec<(i32, i32)> {
    let mut ranges = Vec::new();
    let Some(&first) = values.first() else {
        return ranges;
    };
    let mut start = first;
    let mut end = first;
    for &value in &values[1..] {
        if value == end + 1 {
            end = value;
        } else {
            ranges.push((start, end));
            start = value;
            end = value;
        }
    }
    ranges.push((start, end));
    ranges
}

fn format_range(start: i32, end: i32) -> String {
    if start == end {
        format!("{start:02}")
    } else {
        format!("{start:02}-{end:02}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content(title: &str) -> Content {
        Content {
            content_type: ContentType::TvShow,
            source: "tmdb".to_owned(),
            id: "123".to_owned(),
            title: title.to_owned(),
            release_year: Some(2024),
            original_language: Some("ja".to_owned()),
            original_title: Some("Original".to_owned()),
            overview: None,
            runtime: Some(24),
            popularity: Some(1.5),
            vote_average: Some(8.0),
            vote_count: Some(100),
        }
    }

    #[test]
    fn title_matches_go_content_and_torrent_fallbacks() {
        let episodes = Episodes::new()
            .add_season(1)
            .add_season(2)
            .add_episode(4, 3)
            .add_episode(4, 4)
            .add_episode(4, 6);
        assert_eq!(
            derive_title("raw torrent", Some(&content("Localized")), &episodes),
            "Localized / Original (2024) S01-02, S04E03-04,E06"
        );
        assert_eq!(derive_title("raw torrent", None, &episodes), "raw torrent");

        let mut empty = content("");
        empty.original_title = None;
        assert_eq!(
            derive_title("raw torrent", Some(&empty), &episodes),
            "raw torrent"
        );
    }

    #[test]
    fn dht_stats_use_only_dht_source() {
        let sources = vec![
            TorrentSourceInfo {
                key: "tracker".to_owned(),
                name: "Tracker".to_owned(),
                import_id: None,
                seeders: Some(1),
                leechers: Some(2),
                published_at: None,
                seen_count: 99,
                first_seen_at: 10,
                last_seen_at: 20,
            },
            TorrentSourceInfo {
                key: "dht".to_owned(),
                name: "DHT".to_owned(),
                import_id: None,
                seeders: None,
                leechers: None,
                published_at: None,
                seen_count: 7,
                first_seen_at: 30,
                last_seen_at: 40,
            },
        ];
        assert_eq!(dht_seen_stats(&sources), (7, Some(30), Some(40)));
    }

    #[test]
    fn expanded_item_json_round_trips() {
        let mut item = SearchResultItem::for_test(InfoHash::new([0x11; 20]), "name", 42);
        item.content = Some(content("Title"));
        item.title = derive_title(&item.name, item.content.as_ref(), &item.episodes);
        item.query_string_rank = 0.75;
        item.torrent_tags = vec!["trusted".to_owned()];

        let encoded = serde_json::to_vec(&item).unwrap();
        assert!(!encoded
            .windows(b"refineFiles".len())
            .any(|window| window == b"refineFiles"));
        let decoded: SearchResultItem = serde_json::from_slice(&encoded).unwrap();
        assert!(decoded.refine_files.is_empty());
        assert_eq!(decoded, item);
    }
}
