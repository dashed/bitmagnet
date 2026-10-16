//! The search-domain predicate tree and its leaf value types.
//!
//! This is the Rust port of the `query.Criteria` tree assembled in Go by
//! `internal/database/search`'s `*Criteria` constructors and combined with
//! `query.And` / `query.Or` / `query.Not` (see `internal/database/query/
//! criteria.go`). Lane T (`bitmagnet-torznab`) builds a [`Criteria`] from a
//! parsed Torznab request — translating `t=`, `cat=`, `imdbid=`, `tmdbid=`,
//! `season`/`ep` into these leaves exactly as `internal/torznab/adapter/
//! search_options.go` does — and hands it to [`crate::build_query`].
//!
//! The v1 Torznab subset and the Phase-2 GraphQL contract are modelled here.
//! Every leaf maps to a documented SQL fragment against `torrent_contents`
//! (and, where noted, a required join); the Phase-2 SQL is implemented in
//! later Lane S tasks.

use bitmagnet_model::{ContentType, FileType, InfoHash};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A predicate over the `torrent_contents` search relation.
///
/// Combinators mirror Go's GORM scope semantics: [`Criteria::And`] chains
/// `WHERE`-joined conditions, [`Criteria::Or`] `OR`-joins them, [`Criteria::Not`]
/// negates. An empty `And`/`Or` is a no-op / always-false respectively — Q2
/// pins the exact degenerate-case SQL against the Go builder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Criteria {
    /// Conjunction. Go: `query.And` / successive `query.Where`.
    And(Vec<Criteria>),
    /// Disjunction. Go: `query.Or`.
    Or(Vec<Criteria>),
    /// Negation. Go: `query.Not`. (Not reached by Torznab today; included for
    /// the Phase-2 reuse surface and kept parity-tested.)
    Not(Box<Criteria>),
    /// `torrent_contents.content_type IN (...)`.
    /// Go: `search.TorrentContentTypeCriteria`.
    ContentTypeIn(Vec<ContentType>),
    /// Source-key membership. Go: `facet_torrent_source.go`
    /// `TorrentSourceCriteria`. Lane S S2 will emit a correlated
    /// `EXISTS (SELECT 1 FROM torrents_torrent_sources s WHERE s.info_hash =
    /// torrent_contents.info_hash AND s.source IN (...))` predicate.
    TorrentSourceIn(Vec<String>),
    /// Torrent file-type membership. Go: `criteria_torrent_file_type.go`
    /// `TorrentFileTypeCriteria`. Lane S S2 will expand each
    /// [`FileType::extensions`] set and delegate to file-extension criteria.
    TorrentFileTypeIn(Vec<FileType>),
    /// File-extension membership. Go: `criteria_torrent_file_extension.go`
    /// `TorrentFileExtensionCriteria`. Lane S S2 will OR
    /// `torrents.extension IN (...)` with either legacy
    /// `EXISTS (torrent_files ... extension IN (...))` or gated JSONB
    /// `torrents.file_extensions @> '["ext"]'::jsonb` predicates.
    FileExtensionIn(Vec<String>),
    /// Language-id membership. Go: `facet_torrent_content_language.go`
    /// `TorrentContentLanguageCriteria`. Lane S S2 will emit
    /// `torrent_contents.languages ?| array['<id>', ...]`.
    LanguageIn(Vec<String>),
    /// Genre collection membership. Go: `facet_torrent_content_genre.go`
    /// `TorrentContentGenreFacet` via `ContentCollectionCriteria`. Lane S S2
    /// will apply content-collection `EXISTS` predicates with
    /// `collection_type = 'genre'`.
    ContentGenre(Vec<ContentCollectionRef>),
    /// Content-collection membership. Go: `criteria_content_collection.go`
    /// `ContentCollectionCriteria`. Lane S S2 will require non-null
    /// `torrent_contents.content_id` and emit an OR of correlated
    /// `EXISTS (content_collections_content ...)` predicates.
    ContentCollection(Vec<ContentCollectionRef>),
    /// `content.release_year IN (...)`, with accepted years in `1000..=9999`.
    /// Go: `facet_release_year.go` `yearCondition`; Lane S S2 implements the
    /// range validation and SQL.
    ReleaseYearIn(Vec<u16>),
    /// `torrent_contents.video_resolution IN (...)`.
    /// Go: `search.VideoResolutionCriteria`.
    VideoResolutionIn(Vec<VideoResolution>),
    /// `torrent_contents.video_source IN (...)`. Go:
    /// `facet_torrent_content_video_source.go` `VideoSourceCriteria`; SQL is
    /// implemented in Lane S S2.
    VideoSourceIn(Vec<VideoSource>),
    /// `torrent_contents.video_codec IN (...)`. Go:
    /// `facet_torrent_content_video_codec.go` `VideoCodecCriteria`; SQL is
    /// implemented in Lane S S2.
    VideoCodecIn(Vec<VideoCodec>),
    /// `torrent_contents.video_3d IN (...)`.
    /// Go: `search.Video3DCriteria`.
    ///
    /// Explicitly renamed: the `snake_case` rule mangles `Video3DIn` to
    /// `video3_d_in`, but the Go parity generator (and the natural, column-
    /// aligned name) is `video_3d_in`.
    #[serde(rename = "video_3d_in")]
    Video3DIn(Vec<Video3D>),
    /// `torrent_contents.video_modifier IN (...)`. Go:
    /// `facet_torrent_content_video_modifier.go` `VideoModifierCriteria`; SQL
    /// is implemented in Lane S S2.
    VideoModifierIn(Vec<VideoModifier>),
    /// Inclusive byte bounds on `torrent_contents.size`. Go:
    /// `criteria_torrent_content_size.go` `TorrentContentSizeCriteria` and
    /// gqlmodel's `SizeRangeCriteria`. Lane S S2 will emit `size >= min`
    /// and/or `size <= max`.
    SizeRange {
        /// Inclusive minimum size in bytes.
        min: Option<i64>,
        /// Inclusive maximum size in bytes.
        max: Option<i64>,
    },
    /// Published-at time-frame text. Go:
    /// `criteria_torrent_content_published_at.go`
    /// `TorrentContentPublishedAtCriteria`. Lane S S2 will parse the value and
    /// emit inclusive `torrent_contents.published_at >= ...` / `<= ...`
    /// bounds.
    PublishedAt(String),
    /// Torrent-content info-hash membership. Go:
    /// `criteria_torrent_content_info_hash.go`
    /// `TorrentContentInfoHashCriteria`. Lane S S2 will emit
    /// `torrent_contents.info_hash IN (DECODE('<hex>', 'hex'), ...)`; an empty
    /// set becomes `FALSE`.
    TorrentContentInfoHashIn(Vec<InfoHash>),
    /// Null-bucket predicate for a facet attribute. Go:
    /// `facet_torrent_content_attribute.go` generic `Criteria` and
    /// `facet_release_year.go`. Lane S S2 will emit
    /// `torrent_contents.<column> IS NULL`, except release year uses
    /// `content.release_year IS NULL`.
    ///
    /// When a facet filter includes the literal `"null"`, callers compose
    /// `Criteria::or([<field>In(non_null_values), Criteria::IsNull(field)])`.
    IsNull(TorrentContentAttribute),
    /// Season/episode containment against the `torrent_contents.episodes`
    /// jsonb. Go: `search.TorrentContentEpisodesCriteria`.
    Episodes(Episodes),
    /// Canonical id match on the joined `content` row (`content.source` +
    /// `content.id`, optionally `content.type`). Go:
    /// `search.ContentCanonicalIdentifierCriteria` — used for `tmdbid=`.
    /// Requires the `content` join.
    CanonicalIdentifier(Vec<ContentRef>),
    /// Alternative id match via `EXISTS (content_attributes ...)`. Go:
    /// `search.ContentAlternativeIdentifierCriteria` — used for `imdbid=`.
    /// Requires the `content` join.
    AlternativeIdentifier(Vec<ContentRef>),
    /// `EXISTS (torrent_tags WHERE info_hash = torrents.info_hash AND name IN
    /// (...))`. Go: `search.TorrentTagCriteria` — used for profile tags.
    /// Requires the `torrents` join.
    TorrentTag(Vec<String>),
}

impl Criteria {
    /// Conjunction combinator (Go `query.And`).
    pub fn and(criteria: impl IntoIterator<Item = Criteria>) -> Self {
        Self::And(criteria.into_iter().collect())
    }

    /// Disjunction combinator (Go `query.Or`).
    pub fn or(criteria: impl IntoIterator<Item = Criteria>) -> Self {
        Self::Or(criteria.into_iter().collect())
    }

    /// Negation combinator (Go `query.Not`).
    #[allow(clippy::should_implement_trait)] // Frozen v1 constructor name.
    pub fn not(criteria: Criteria) -> Self {
        Self::Not(Box::new(criteria))
    }

    /// `content_type IN (...)` (Go `search.TorrentContentTypeCriteria`).
    pub fn content_type_in(types: impl IntoIterator<Item = ContentType>) -> Self {
        Self::ContentTypeIn(types.into_iter().collect())
    }

    /// Source-key membership (Go `facet_torrent_source.go`
    /// `TorrentSourceCriteria`); Lane S S2 emits a correlated
    /// `torrents_torrent_sources` `EXISTS` predicate.
    pub fn torrent_source_in(values: impl IntoIterator<Item = String>) -> Self {
        Self::TorrentSourceIn(values.into_iter().collect())
    }

    /// Torrent file-type membership (Go `criteria_torrent_file_type.go`
    /// `TorrentFileTypeCriteria`); Lane S S2 expands extensions and delegates
    /// to the file-extension SQL.
    pub fn torrent_file_type_in(values: impl IntoIterator<Item = FileType>) -> Self {
        Self::TorrentFileTypeIn(values.into_iter().collect())
    }

    /// File-extension membership (Go `criteria_torrent_file_extension.go`
    /// `TorrentFileExtensionCriteria`); Lane S S2 emits the `torrents` branch
    /// plus the feature-gated legacy-file or JSONB branch.
    pub fn file_extension_in(values: impl IntoIterator<Item = String>) -> Self {
        Self::FileExtensionIn(values.into_iter().collect())
    }

    /// Language-id membership (Go `facet_torrent_content_language.go`
    /// `TorrentContentLanguageCriteria`); Lane S S2 emits the PostgreSQL `?|`
    /// array-overlap predicate.
    pub fn language_in(values: impl IntoIterator<Item = String>) -> Self {
        Self::LanguageIn(values.into_iter().collect())
    }

    /// Genre collection membership (Go `facet_torrent_content_genre.go`
    /// `TorrentContentGenreFacet`); Lane S S2 emits collection `EXISTS`
    /// predicates restricted to `collection_type = 'genre'`.
    pub fn content_genre(refs: impl IntoIterator<Item = ContentCollectionRef>) -> Self {
        Self::ContentGenre(refs.into_iter().collect())
    }

    /// Content-collection membership (Go `criteria_content_collection.go`
    /// `ContentCollectionCriteria`); Lane S S2 emits the content-id guard and
    /// correlated `content_collections_content` `EXISTS` branches.
    pub fn content_collection(refs: impl IntoIterator<Item = ContentCollectionRef>) -> Self {
        Self::ContentCollection(refs.into_iter().collect())
    }

    /// Release-year membership (Go `facet_release_year.go` `yearCondition`);
    /// Lane S S2 validates `1000..=9999` and emits `content.release_year IN
    /// (...)`.
    pub fn release_year_in(years: impl IntoIterator<Item = u16>) -> Self {
        Self::ReleaseYearIn(years.into_iter().collect())
    }

    /// `video_resolution IN (...)` (Go `search.VideoResolutionCriteria`).
    pub fn video_resolution_in(values: impl IntoIterator<Item = VideoResolution>) -> Self {
        Self::VideoResolutionIn(values.into_iter().collect())
    }

    /// Video-source membership (Go `facet_torrent_content_video_source.go`
    /// `VideoSourceCriteria`); Lane S S2 emits
    /// `torrent_contents.video_source IN (...)`.
    pub fn video_source_in(values: impl IntoIterator<Item = VideoSource>) -> Self {
        Self::VideoSourceIn(values.into_iter().collect())
    }

    /// Video-codec membership (Go `facet_torrent_content_video_codec.go`
    /// `VideoCodecCriteria`); Lane S S2 emits
    /// `torrent_contents.video_codec IN (...)`.
    pub fn video_codec_in(values: impl IntoIterator<Item = VideoCodec>) -> Self {
        Self::VideoCodecIn(values.into_iter().collect())
    }

    /// `video_3d IN (...)` (Go `search.Video3DCriteria`).
    pub fn video_3d_in(values: impl IntoIterator<Item = Video3D>) -> Self {
        Self::Video3DIn(values.into_iter().collect())
    }

    /// Video-modifier membership (Go
    /// `facet_torrent_content_video_modifier.go` `VideoModifierCriteria`);
    /// Lane S S2 emits `torrent_contents.video_modifier IN (...)`.
    pub fn video_modifier_in(values: impl IntoIterator<Item = VideoModifier>) -> Self {
        Self::VideoModifierIn(values.into_iter().collect())
    }

    /// Inclusive size bounds (Go `criteria_torrent_content_size.go`
    /// `TorrentContentSizeCriteria`); Lane S S2 emits `size >= min` and/or
    /// `size <= max`.
    pub const fn size_range(min: Option<i64>, max: Option<i64>) -> Self {
        Self::SizeRange { min, max }
    }

    /// Published-at time-frame criterion (Go
    /// `criteria_torrent_content_published_at.go`
    /// `TorrentContentPublishedAtCriteria`); Lane S S2 parses it into inclusive
    /// timestamp bounds.
    pub fn published_at(value: impl Into<String>) -> Self {
        Self::PublishedAt(value.into())
    }

    /// Torrent-content info-hash membership (Go
    /// `criteria_torrent_content_info_hash.go`
    /// `TorrentContentInfoHashCriteria`); Lane S S2 emits decoded-bytea `IN`
    /// SQL, with an empty set lowering to `FALSE`.
    pub fn torrent_content_info_hash_in(values: impl IntoIterator<Item = InfoHash>) -> Self {
        Self::TorrentContentInfoHashIn(values.into_iter().collect())
    }

    /// Facet null-bucket criterion (Go `facet_torrent_content_attribute.go`
    /// and `facet_release_year.go`); Lane S S2 emits the selected nullable
    /// column's `IS NULL` predicate.
    pub const fn is_null(attribute: TorrentContentAttribute) -> Self {
        Self::IsNull(attribute)
    }

    /// Episodes containment (Go `search.TorrentContentEpisodesCriteria`).
    pub fn episodes(episodes: Episodes) -> Self {
        Self::Episodes(episodes)
    }

    /// Canonical identifier match (Go `search.ContentCanonicalIdentifierCriteria`).
    pub fn canonical_identifier(refs: impl IntoIterator<Item = ContentRef>) -> Self {
        Self::CanonicalIdentifier(refs.into_iter().collect())
    }

    /// Alternative identifier match (Go `search.ContentAlternativeIdentifierCriteria`).
    pub fn alternative_identifier(refs: impl IntoIterator<Item = ContentRef>) -> Self {
        Self::AlternativeIdentifier(refs.into_iter().collect())
    }

    /// Torrent-tag existence (Go `search.TorrentTagCriteria`).
    pub fn torrent_tag(names: impl IntoIterator<Item = String>) -> Self {
        Self::TorrentTag(names.into_iter().collect())
    }
}

/// A reference to a content record by external source + id, optionally scoped
/// to a content type. Mirrors Go `model.ContentRef` (`Type`, `Source`, `ID`).
///
/// `content_type == None` means the Go nil-type case (`ContentType.IsNil()`),
/// which drops the `type` predicate — Torznab emits this when the function is
/// ambiguous (`t=search`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<ContentType>,
    /// External metadata source, e.g. `"imdb"` or `"tmdb"`.
    pub source: String,
    /// The source-local id, e.g. `"tt0111161"` or `"603"`.
    pub id: String,
}

/// A reference to a content collection (genre etc.) by type/source/id.
/// Mirrors Go `model.ContentCollectionRef`, consumed by
/// `criteria_content_collection.go` `ContentCollectionCriteria`. Lane S S2
/// lowers it to correlated `content_collections_content` `EXISTS` SQL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentCollectionRef {
    /// Optional collection type, for example `"genre"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection_type: Option<String>,
    /// Collection namespace/source.
    pub source: String,
    /// Source-local collection identifier.
    pub id: String,
}

/// The nullable `torrent_contents` attribute columns that facets can filter on
/// with `IS NULL` when the facet filter includes the literal value `"null"`.
/// Mirrors the generic facet criteria in
/// `facet_torrent_content_attribute.go` and `facet_release_year.go`.
///
/// Callers compose `Criteria::or([<field>In(non_null_values),
/// Criteria::IsNull(TorrentContentAttribute::X)])`. Lane S S2 emits
/// `torrent_contents.<column> IS NULL`, except [`Self::ReleaseYear`] emits
/// `content.release_year IS NULL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentContentAttribute {
    /// `torrent_contents.content_type IS NULL`.
    ContentType,
    /// `torrent_contents.video_resolution IS NULL`.
    VideoResolution,
    /// `torrent_contents.video_source IS NULL`.
    VideoSource,
    /// `torrent_contents.video_codec IS NULL`.
    VideoCodec,
    /// `torrent_contents.video_3d IS NULL`.
    #[serde(rename = "video_3d")]
    Video3D,
    /// `torrent_contents.video_modifier IS NULL`.
    VideoModifier,
    /// `content.release_year IS NULL`.
    ReleaseYear,
}

/// The `torrent_contents.video_source` enum. Values mirror Go
/// `model.VideoSource` from `internal/model/video_source_enum.go`; Lane S S2
/// binds [`Self::as_str`] values into `torrent_contents.video_source IN (...)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VideoSource {
    /// PostgreSQL value `CAM`.
    #[serde(rename = "CAM")]
    Cam,
    /// PostgreSQL value `TELESYNC`.
    #[serde(rename = "TELESYNC")]
    Telesync,
    /// PostgreSQL value `TELECINE`.
    #[serde(rename = "TELECINE")]
    Telecine,
    /// PostgreSQL value `WORKPRINT`.
    #[serde(rename = "WORKPRINT")]
    Workprint,
    /// PostgreSQL value `DVD`.
    #[serde(rename = "DVD")]
    Dvd,
    /// PostgreSQL value `TV`.
    #[serde(rename = "TV")]
    Tv,
    /// PostgreSQL value `WEBDL`.
    #[serde(rename = "WEBDL")]
    WebDl,
    /// PostgreSQL value `WEBRip`.
    #[serde(rename = "WEBRip")]
    WebRip,
    /// PostgreSQL value `BluRay`.
    #[serde(rename = "BluRay")]
    BluRay,
}

impl VideoSource {
    /// Return the exact PostgreSQL column value from Go `model.VideoSource` for
    /// Lane S S2's `torrent_contents.video_source IN (...)` binds.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cam => "CAM",
            Self::Telesync => "TELESYNC",
            Self::Telecine => "TELECINE",
            Self::Workprint => "WORKPRINT",
            Self::Dvd => "DVD",
            Self::Tv => "TV",
            Self::WebDl => "WEBDL",
            Self::WebRip => "WEBRip",
            Self::BluRay => "BluRay",
        }
    }
}

/// The `torrent_contents.video_codec` enum. Values mirror Go
/// `model.VideoCodec` from `internal/model/video_codec_enum.go`; Lane S S2
/// binds [`Self::as_str`] values into `torrent_contents.video_codec IN (...)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VideoCodec {
    /// PostgreSQL value `H264`.
    #[serde(rename = "H264")]
    H264,
    /// PostgreSQL value `x264`.
    #[serde(rename = "x264")]
    X264,
    /// PostgreSQL value `x265`.
    #[serde(rename = "x265")]
    X265,
    /// PostgreSQL value `XviD`.
    #[serde(rename = "XviD")]
    XviD,
    /// PostgreSQL value `DivX`.
    #[serde(rename = "DivX")]
    DivX,
    /// PostgreSQL value `MPEG2`.
    #[serde(rename = "MPEG2")]
    Mpeg2,
    /// PostgreSQL value `MPEG4`.
    #[serde(rename = "MPEG4")]
    Mpeg4,
}

impl VideoCodec {
    /// Return the exact PostgreSQL column value from Go `model.VideoCodec` for
    /// Lane S S2's `torrent_contents.video_codec IN (...)` binds.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::H264 => "H264",
            Self::X264 => "x264",
            Self::X265 => "x265",
            Self::XviD => "XviD",
            Self::DivX => "DivX",
            Self::Mpeg2 => "MPEG2",
            Self::Mpeg4 => "MPEG4",
        }
    }
}

/// The `torrent_contents.video_modifier` enum. Values mirror Go
/// `model.VideoModifier` from `internal/model/video_modifier_enum.go`; Lane S
/// S2 binds [`Self::as_str`] values into
/// `torrent_contents.video_modifier IN (...)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VideoModifier {
    /// PostgreSQL value `REGIONAL`.
    #[serde(rename = "REGIONAL")]
    Regional,
    /// PostgreSQL value `SCREENER`.
    #[serde(rename = "SCREENER")]
    Screener,
    /// PostgreSQL value `RAWHD`.
    #[serde(rename = "RAWHD")]
    RawHd,
    /// PostgreSQL value `BRDISK`.
    #[serde(rename = "BRDISK")]
    BrDisk,
    /// PostgreSQL value `REMUX`.
    #[serde(rename = "REMUX")]
    Remux,
}

impl VideoModifier {
    /// Return the exact PostgreSQL column value from Go `model.VideoModifier`
    /// for Lane S S2's `torrent_contents.video_modifier IN (...)` binds.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Regional => "REGIONAL",
            Self::Screener => "SCREENER",
            Self::RawHd => "RAWHD",
            Self::BrDisk => "BRDISK",
            Self::Remux => "REMUX",
        }
    }
}

/// The `torrent_contents.video_resolution` enum. String values are the exact
/// PostgreSQL column values (Go `model.VideoResolution`).
///
/// NOTE: lives here for v1 because `bitmagnet-model` does not yet carry it;
/// once Phase 2 needs the full resolution/3d/codec enums they should move to
/// `bitmagnet-model` and this re-export from there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VideoResolution {
    #[serde(rename = "V360p")]
    V360p,
    #[serde(rename = "V480p")]
    V480p,
    #[serde(rename = "V540p")]
    V540p,
    #[serde(rename = "V576p")]
    V576p,
    #[serde(rename = "V720p")]
    V720p,
    #[serde(rename = "V1080p")]
    V1080p,
    #[serde(rename = "V1440p")]
    V1440p,
    #[serde(rename = "V2160p")]
    V2160p,
    #[serde(rename = "V4320p")]
    V4320p,
}

impl VideoResolution {
    /// The PostgreSQL column value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::V360p => "V360p",
            Self::V480p => "V480p",
            Self::V540p => "V540p",
            Self::V576p => "V576p",
            Self::V720p => "V720p",
            Self::V1080p => "V1080p",
            Self::V1440p => "V1440p",
            Self::V2160p => "V2160p",
            Self::V4320p => "V4320p",
        }
    }
}

/// The `torrent_contents.video_3d` enum (Go `model.Video3D`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Video3D {
    #[serde(rename = "V3D")]
    V3D,
    #[serde(rename = "V3DSBS")]
    V3DSBS,
    #[serde(rename = "V3DOU")]
    V3DOU,
}

impl Video3D {
    /// The PostgreSQL column value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::V3D => "V3D",
            Self::V3DSBS => "V3DSBS",
            Self::V3DOU => "V3DOU",
        }
    }
}

/// Season → episode containment set, mirroring Go `model.Episodes`.
///
/// Wire/fixture shape is `{"<season>": [<episode>, ...]}`. An empty episode
/// list for a season means "any episode in this season" — Go emits
/// `episodes #> '{<season>}' = '{}'::jsonb` (season present, episodes
/// unconstrained); a non-empty list emits
/// `episodes #> '{<season>}' @> '{"<ep>":{}, ...}'::jsonb`. Seasons are
/// AND-combined. The `BTreeMap`/`Vec` give deterministic iteration order so the
/// generated SQL is stable.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Episodes(pub BTreeMap<i32, Vec<i32>>);

impl Episodes {
    /// Empty episode set.
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Constrain to a whole season (Go `Episodes.AddSeason`).
    pub fn add_season(mut self, season: i32) -> Self {
        self.0.entry(season).or_default();
        self
    }

    /// Constrain to a specific episode within a season (Go
    /// `Episodes.AddEpisode`).
    pub fn add_episode(mut self, season: i32, episode: i32) -> Self {
        let eps = self.0.entry(season).or_default();
        if !eps.contains(&episode) {
            eps.push(episode);
        }
        self
    }

    /// True when no season constraint is present (Torznab omits the episodes
    /// criterion entirely in that case).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
