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
//! Only the subset the Torznab adapter exercises is modelled here (the v1
//! contract). Every leaf maps to a documented SQL fragment against
//! `torrent_contents` (and, where noted, a required join).

use bitmagnet_model::ContentType;
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
    /// `torrent_contents.video_resolution IN (...)`.
    /// Go: `search.VideoResolutionCriteria`.
    VideoResolutionIn(Vec<VideoResolution>),
    /// `torrent_contents.video_3d IN (...)`.
    /// Go: `search.Video3DCriteria`.
    ///
    /// Explicitly renamed: the `snake_case` rule mangles `Video3DIn` to
    /// `video3_d_in`, but the Go parity generator (and the natural, column-
    /// aligned name) is `video_3d_in`.
    #[serde(rename = "video_3d_in")]
    Video3DIn(Vec<Video3D>),
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

    /// `video_resolution IN (...)` (Go `search.VideoResolutionCriteria`).
    pub fn video_resolution_in(values: impl IntoIterator<Item = VideoResolution>) -> Self {
        Self::VideoResolutionIn(values.into_iter().collect())
    }

    /// `video_3d IN (...)` (Go `search.Video3DCriteria`).
    pub fn video_3d_in(values: impl IntoIterator<Item = Video3D>) -> Self {
        Self::Video3DIn(values.into_iter().collect())
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
