//! Lane Q search-row to Torznab RSS mapping.

use bitmagnet_search_query::{ContentType, Episodes, InfoHash, SearchResultItem, VideoResolution};
use chrono::{DateTime, Utc};

use crate::categories::{
    CATEGORY_AUDIO, CATEGORY_AUDIO_AUDIOBOOK, CATEGORY_BOOKS, CATEGORY_BOOKS_COMICS,
    CATEGORY_MOVIES, CATEGORY_OTHER, CATEGORY_PC, CATEGORY_PC_GAMES, CATEGORY_TV, CATEGORY_XXX,
};
use crate::config::Profile;
use crate::request::TorznabRequest;
use crate::response::{Channel, Enclosure, Item, Response, RssDate, SearchResult, TorznabAttr};

const MAGNET_MIME_TYPE: &str = "application/x-bittorrent;x-scheme-handler/magnet";

/// Builds the magnet URI, mirroring Go `model.Torrent.MagnetURI`
/// (`internal/model/torrents.go`): a `urn:btih` v1 topic for v1/hybrid torrents
/// (or the PK info_hash for legacy non-v2 rows), and a `urn:btmh:1220…`
/// SHA-256 multihash topic for v2/hybrid torrents; then `&dn=`/`&xl=`.
#[must_use]
pub fn magnet(
    info_hash: &InfoHash,
    info_hash_v1: Option<&[u8; 20]>,
    info_hash_v2: Option<&[u8; 32]>,
    name: &str,
    size: u64,
) -> String {
    let mut xts: Vec<String> = Vec::with_capacity(2);
    match info_hash_v1 {
        Some(v1) => xts.push(format!("xt=urn:btih:{}", InfoHash::new(*v1))),
        None if info_hash_v2.is_none() => xts.push(format!("xt=urn:btih:{info_hash}")),
        None => {} // pure-v2: no btih topic
    }
    if let Some(v2) = info_hash_v2 {
        xts.push(format!("xt=urn:btmh:1220{}", hex_lower(v2)));
    }
    format!(
        "magnet:?{}&dn={}&xl={size}",
        xts.join("&"),
        query_escape(name)
    )
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Maps a content classification to its Torznab numeric category ID.
#[must_use]
pub const fn content_type_category_id(content_type: Option<ContentType>) -> i32 {
    match content_type {
        Some(ContentType::Movie) => CATEGORY_MOVIES,
        Some(ContentType::TvShow) => CATEGORY_TV,
        Some(ContentType::Music) => CATEGORY_AUDIO,
        Some(ContentType::Ebook) => CATEGORY_BOOKS,
        Some(ContentType::Comic) => CATEGORY_BOOKS_COMICS,
        Some(ContentType::Audiobook) => CATEGORY_AUDIO_AUDIOBOOK,
        Some(ContentType::Software) => CATEGORY_PC,
        Some(ContentType::Game) => CATEGORY_PC_GAMES,
        Some(ContentType::Xxx) => CATEGORY_XXX,
        None => CATEGORY_OTHER,
    }
}

/// Converts one hydrated Lane Q row into a Torznab RSS item.
#[must_use]
pub fn to_item(row: &SearchResultItem) -> Item {
    map_fields(ResultFields::from(row))
}

/// Builds the complete Torznab channel for a search response.
#[must_use]
pub fn to_search_result(
    request: &TorznabRequest,
    profile: &Profile,
    rows: Vec<SearchResultItem>,
) -> SearchResult {
    SearchResult {
        channel: Channel {
            title: Some(profile.title.clone()),
            response: Response {
                offset: request.offset.unwrap_or_default(),
                total: 0,
            },
            items: rows.iter().map(to_item).collect(),
            ..Channel::default()
        },
    }
}

/// Owned field view used by the Lane G golden parity harness to build an
/// [`Item`] straight from `testdata/parity/torznab/fixtures.jsonl` — the
/// v1 [`SearchResultItem`] has no external constructor (it is only produced by
/// Lane Q's `SearchQuery::fetch`), so the parity test hands these fields to the
/// real [`map_fields`] to exercise byte-parity without a live query. Not part
/// of the supported API.
#[doc(hidden)]
pub struct FixtureItemFields {
    pub info_hash: [u8; 20],
    pub info_hash_v1: Option<[u8; 20]>,
    pub info_hash_v2: Option<[u8; 32]>,
    pub name: String,
    pub size: u64,
    pub content_type: Option<ContentType>,
    pub published_at: i64,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    pub files_count: Option<u32>,
    pub video_resolution: Option<VideoResolution>,
    pub video_codec: Option<String>,
    pub release_group: Option<String>,
    pub episodes: Episodes,
    pub release_year: Option<i32>,
    pub imdb_id: Option<String>,
    pub tmdb_id: Option<String>,
}

/// Builds a Torznab [`Item`] from fixture fields through the production
/// [`map_fields`] path. Parity-harness only (see [`FixtureItemFields`]).
#[doc(hidden)]
#[must_use]
pub fn item_from_fixture_fields(fields: &FixtureItemFields) -> Item {
    let info_hash = InfoHash::new(fields.info_hash);
    map_fields(ResultFields {
        info_hash: &info_hash,
        info_hash_v1: fields.info_hash_v1.as_ref(),
        info_hash_v2: fields.info_hash_v2.as_ref(),
        name: &fields.name,
        size: fields.size,
        content_type: fields.content_type,
        published_at: fields.published_at,
        seeders: fields.seeders,
        leechers: fields.leechers,
        files_count: fields.files_count,
        video_resolution: fields.video_resolution,
        video_codec: fields.video_codec.as_deref(),
        release_group: fields.release_group.as_deref(),
        episodes: &fields.episodes,
        release_year: fields.release_year,
        imdb_id: fields.imdb_id.as_deref(),
        tmdb_id: fields.tmdb_id.as_deref(),
    })
}

struct ResultFields<'a> {
    info_hash: &'a InfoHash,
    info_hash_v1: Option<&'a [u8; 20]>,
    info_hash_v2: Option<&'a [u8; 32]>,
    name: &'a str,
    size: u64,
    content_type: Option<ContentType>,
    published_at: i64,
    seeders: Option<u32>,
    leechers: Option<u32>,
    files_count: Option<u32>,
    video_resolution: Option<VideoResolution>,
    video_codec: Option<&'a str>,
    release_group: Option<&'a str>,
    episodes: &'a Episodes,
    release_year: Option<i32>,
    imdb_id: Option<&'a str>,
    tmdb_id: Option<&'a str>,
}

impl<'a> From<&'a SearchResultItem> for ResultFields<'a> {
    fn from(row: &'a SearchResultItem) -> Self {
        Self {
            info_hash: &row.info_hash,
            info_hash_v1: row.info_hash_v1.as_ref(),
            info_hash_v2: row.info_hash_v2.as_ref(),
            name: &row.name,
            size: row.size,
            content_type: row.content_type,
            published_at: row.published_at,
            seeders: row.seeders,
            leechers: row.leechers,
            files_count: row.files_count,
            video_resolution: row.video_resolution,
            video_codec: row.video_codec.as_deref(),
            release_group: row.release_group.as_deref(),
            episodes: &row.episodes,
            release_year: row.release_year,
            imdb_id: row.imdb_id.as_deref(),
            tmdb_id: row.tmdb_id.as_deref(),
        }
    }
}

fn map_fields(row: ResultFields<'_>) -> Item {
    let info_hash = row.info_hash.to_string();
    let magnet = magnet(
        row.info_hash,
        row.info_hash_v1,
        row.info_hash_v2,
        row.name,
        row.size,
    );
    let pub_date = DateTime::<Utc>::from_timestamp(row.published_at, 0)
        .map(|date| RssDate(date.fixed_offset()))
        .unwrap_or_default();
    let category = row
        .content_type
        .map(ContentType::as_str)
        .unwrap_or("Unknown")
        .to_owned();

    let mut attrs = vec![
        attr("infohash", info_hash.clone()),
        attr("magneturl", magnet.clone()),
        attr(
            "category",
            content_type_category_id(row.content_type).to_string(),
        ),
        attr("size", row.size.to_string()),
        attr("publishdate", pub_date.format()),
    ];

    if let Some(seeders) = row.seeders {
        attrs.push(attr("seeders", seeders.to_string()));
    }
    if let Some(leechers) = row.leechers {
        attrs.push(attr("leechers", leechers.to_string()));
    }
    if let (Some(seeders), Some(leechers)) = (row.seeders, row.leechers) {
        attrs.push(attr(
            "peers",
            (u64::from(seeders) + u64::from(leechers)).to_string(),
        ));
    }
    if let Some(files_count) = row.files_count.filter(|count| *count > 0) {
        attrs.push(attr("files", files_count.to_string()));
    }
    if let Some(release_year) = row.release_year {
        attrs.push(attr("year", release_year.to_string()));
    }
    if let Some((season, episodes)) = row.episodes.0.iter().next() {
        attrs.push(attr("season", season.to_string()));
        if let Some(episode) = episodes.first() {
            attrs.push(attr("episode", episode.to_string()));
        }
    }
    if let Some(video_codec) = row.video_codec {
        attrs.push(attr("video", video_codec));
    }
    if let Some(video_resolution) = row.video_resolution {
        let raw_resolution = video_resolution.as_str();
        attrs.push(attr(
            "resolution",
            raw_resolution.strip_prefix('V').unwrap_or(raw_resolution),
        ));
    }
    if let Some(release_group) = row.release_group {
        attrs.push(attr("team", release_group));
    }
    if let Some(tmdb_id) = row.tmdb_id {
        attrs.push(attr("tmdb", tmdb_id));
    }
    if let Some(imdb_id) = row.imdb_id {
        attrs.push(attr("imdb", imdb_id.strip_prefix("tt").unwrap_or(imdb_id)));
    }

    Item {
        title: row.name.to_owned(),
        guid: Some(info_hash),
        pub_date,
        category: Some(category),
        size: row.size,
        enclosure: Enclosure {
            url: magnet,
            length: row.size.to_string(),
            type_: MAGNET_MIME_TYPE.to_owned(),
        },
        torznab_attrs: attrs,
        ..Item::default()
    }
}

fn attr(name: &str, value: impl Into<String>) -> TorznabAttr {
    TorznabAttr {
        name: name.to_owned(),
        value: value.into(),
    }
}

fn query_escape(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut escaped = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                escaped.push(char::from(byte));
            }
            b' ' => escaped.push('+'),
            _ => {
                escaped.push('%');
                escaped.push(char::from(HEX[usize::from(byte >> 4)]));
                escaped.push(char::from(HEX[usize::from(byte & 0x0f)]));
            }
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use bitmagnet_search_query::{ContentType, Episodes, InfoHash, VideoResolution};

    use super::{magnet, map_fields, to_search_result, ResultFields, MAGNET_MIME_TYPE};
    use crate::config::Profile;
    use crate::request::TorznabRequest;
    use crate::response::RssDate;

    #[test]
    fn populated_result_preserves_torznab_attribute_order_and_values() {
        // Lane Q's non-exhaustive SearchResultItem currently has no public
        // constructor, so the unit fixture uses the identical private field
        // view consumed by the production SearchResultItem conversion.
        let info_hash = InfoHash::new([0xab; 20]);
        let episodes = Episodes::new().add_episode(2, 3);
        let item = map_fields(ResultFields {
            info_hash: &info_hash,
            info_hash_v1: None,
            info_hash_v2: None,
            name: "A name & more",
            size: 42,
            content_type: Some(ContentType::Movie),
            published_at: 0,
            seeders: Some(5),
            leechers: Some(7),
            files_count: Some(4),
            video_resolution: Some(VideoResolution::V1080p),
            video_codec: Some("x265"),
            release_group: Some("ExampleTeam"),
            episodes: &episodes,
            release_year: Some(2024),
            imdb_id: Some("tt456"),
            tmdb_id: Some("123"),
        });

        let hash = "abababababababababababababababababababab";
        let magnet = format!("magnet:?xt=urn:btih:{hash}&dn=A+name+%26+more&xl=42");
        assert_eq!(item.title, "A name & more");
        assert_eq!(item.guid.as_deref(), Some(hash));
        assert_eq!(item.category.as_deref(), Some("movie"));
        assert_eq!(item.enclosure.url, magnet);
        assert_eq!(item.enclosure.length, "42");
        assert_eq!(item.enclosure.type_, MAGNET_MIME_TYPE);

        let attrs = item
            .torznab_attrs
            .iter()
            .map(|attribute| (attribute.name.as_str(), attribute.value.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            attrs,
            [
                ("infohash", hash),
                ("magneturl", magnet.as_str()),
                ("category", "2000"),
                ("size", "42"),
                ("publishdate", "Thu, 01 Jan 1970 00:00:00 +0000"),
                ("seeders", "5"),
                ("leechers", "7"),
                ("peers", "12"),
                ("files", "4"),
                ("year", "2024"),
                ("season", "2"),
                ("episode", "3"),
                ("video", "x265"),
                ("resolution", "1080p"),
                ("team", "ExampleTeam"),
                ("tmdb", "123"),
                ("imdb", "456"),
            ]
        );
    }

    #[test]
    fn magnet_query_escape_matches_go_for_spaces_ampersands_and_utf8() {
        let info_hash = InfoHash::new([0; 20]);
        assert_eq!(
            magnet(&info_hash, None, None, "space & café", 4200),
            concat!(
                "magnet:?xt=urn:btih:0000000000000000000000000000000000000000",
                "&dn=space+%26+caf%C3%A9&xl=4200"
            )
        );
    }

    #[test]
    fn magnet_matches_go_magneturi_switch() {
        let pk = InfoHash::new([0x11; 20]);
        let v1 = [0x22u8; 20];
        let v2 = [0x33u8; 32];
        let pk_hex = "1111111111111111111111111111111111111111";
        let v1_hex = "2222222222222222222222222222222222222222";
        let v2_hex = "33".repeat(32); // 64 chars

        // hybrid: v1 + v2 → btih(v1) then btmh(1220+v2)
        assert_eq!(
            magnet(&pk, Some(&v1), Some(&v2), "n", 1),
            format!("magnet:?xt=urn:btih:{v1_hex}&xt=urn:btmh:1220{v2_hex}&dn=n&xl=1")
        );
        // pure-v2: v1 None + v2 Some → btmh only, NO btih
        assert_eq!(
            magnet(&pk, None, Some(&v2), "n", 1),
            format!("magnet:?xt=urn:btmh:1220{v2_hex}&dn=n&xl=1")
        );
        // v1-only: btih(v1), no btmh
        assert_eq!(
            magnet(&pk, Some(&v1), None, "n", 1),
            format!("magnet:?xt=urn:btih:{v1_hex}&dn=n&xl=1")
        );
        // legacy (v1 None + v2 None): btih from PK
        assert_eq!(
            magnet(&pk, None, None, "n", 1),
            format!("magnet:?xt=urn:btih:{pk_hex}&dn=n&xl=1")
        );
    }

    #[test]
    fn search_result_channel_uses_profile_offset_and_zero_total() {
        let request = TorznabRequest {
            offset: Some(25),
            ..TorznabRequest::default()
        };
        let result = to_search_result(&request, &Profile::default_profile(), Vec::new());

        assert_eq!(result.channel.title.as_deref(), Some("bitmagnet"));
        assert_eq!(result.channel.response.offset, 25);
        assert_eq!(result.channel.response.total, 0);
        assert!(result.channel.items.is_empty());
        assert_eq!(result.channel.pub_date, RssDate::default());
        assert_eq!(result.channel.last_build_date, RssDate::default());
    }
}
