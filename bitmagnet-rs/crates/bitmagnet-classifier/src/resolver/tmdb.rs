//! The TMDB request/response DTOs — a port of Go `internal/tmdb/interface.go`,
//! with the JSON field names from `internal/tmdb/client.go`'s decode path
//! preserved exactly.
//!
//! # Why these live here, not in `bitmagnet-tmdb`
//!
//! `bitmagnet-tmdb` will hold the `reqwest` client, and that client will
//! *implement* [`crate::ContentResolver`] — so it must depend on this crate. If
//! the DTOs lived there too, the dependency would be a cycle. Keeping the seam's
//! whole vocabulary with the seam keeps every implementation crate
//! (`bitmagnet-tmdb`, `bitmagnet-content-search`, a tape) a leaf that depends on
//! the classifier and nothing depends back.
//!
//! # Fidelity notes
//!
//! * Responses derive `Default` so a missing/absent object decodes to Go's zero
//!   value rather than failing — Go's `encoding/json` leaves absent fields at
//!   their zero value, and several of these (`belongs_to_collection`,
//!   `external_ids`, `last_episode_to_air`) are routinely absent.
//! * 🚨 Every response field is `#[serde(default, deserialize_with =
//!   "null_to_default")]`, not merely `#[serde(default)]`. `default` alone covers
//!   an **absent** field; it does **not** cover an explicit `null`, and TMDB
//!   sends `null` freely for strings it has no value for (`poster_path`,
//!   `overview`, `belongs_to_collection`, …). Go's `encoding/json` documents
//!   that unmarshalling `null` into a non-pointer "has no effect on the value and
//!   produces no error", so a plain `default` decodes strictly *less* than Go
//!   accepts and fails the classification on data Go handles fine. This was found
//!   by the production corpus gate: four real subjects errored on
//!   `invalid type: null, expected a string`.
//! * Requests derive `Serialize`/`Deserialize` + `Eq`/`Hash` so a tape can key
//!   on them directly. Responses carry `f32` scores and so are `PartialEq` only.
//!   Requests keep a plain `default`: their fields are `Option`, which already
//!   maps `null` onto `None`, and they are tape *keys* rather than decoded
//!   payloads.
//! * Only the fields Go declares are modelled; TMDB sends more, and unknown
//!   fields are ignored on both sides.

use serde::{Deserialize, Deserializer, Serialize};

/// Decodes `null` as the type's default, mirroring Go's `encoding/json`.
///
/// See the module docs: this is the difference between "the field was absent"
/// (which `#[serde(default)]` already handles) and "the field was present and
/// `null`" (which it does not, but Go does).
fn null_to_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

// ---------------------------------------------------------------------------
// /search/movie
// ---------------------------------------------------------------------------

/// Go `tmdb.SearchMovieRequest`.
///
/// `language` / `region` are Go `model.NullString`, and the two year fields are
/// `model.Year` (zero == nil) — all four are `Option` here, and `None` means the
/// query parameter is omitted.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchMovieRequest {
    pub query: String,
    #[serde(default)]
    pub include_adult: bool,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub primary_release_year: Option<u16>,
    #[serde(default)]
    pub year: Option<u16>,
    #[serde(default)]
    pub region: Option<String>,
}

/// Go `tmdb.SearchMovieResult`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchMovieResult {
    #[serde(default, deserialize_with = "null_to_default")]
    pub vote_count: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub id: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub video: bool,
    #[serde(default, deserialize_with = "null_to_default")]
    pub vote_average: f32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub title: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub popularity: f32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub poster_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub original_language: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub original_title: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub genre_ids: Vec<i64>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub backdrop_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub adult: bool,
    #[serde(default, deserialize_with = "null_to_default")]
    pub overview: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub release_date: String,
}

/// Go `tmdb.SearchMovieResponse`. [`Self::results`] is TMDB's ordered relevance
/// ranking and must be preserved verbatim — the Levenshtein pick is first-wins
/// over it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchMovieResponse {
    #[serde(default, deserialize_with = "null_to_default")]
    pub page: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub total_results: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub total_pages: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub results: Vec<SearchMovieResult>,
}

// ---------------------------------------------------------------------------
// /search/tv
// ---------------------------------------------------------------------------

/// Go `tmdb.SearchTvRequest`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchTvRequest {
    pub query: String,
    #[serde(default)]
    pub include_adult: bool,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub first_air_date_year: Option<u16>,
    /// Present on the Go struct but never sent by `client.SearchTv`; kept so the
    /// request shape is a faithful mirror.
    #[serde(default)]
    pub year: Option<u16>,
}

/// Go `tmdb.SearchTvResult`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchTvResult {
    #[serde(default, deserialize_with = "null_to_default")]
    pub original_name: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub id: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub name: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub vote_count: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub vote_average: f32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub poster_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub first_air_date: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub popularity: f32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub genre_ids: Vec<i64>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub original_language: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub backdrop_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub overview: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub origin_country: Vec<String>,
}

/// Go `tmdb.SearchTvResponse`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchTvResponse {
    #[serde(default, deserialize_with = "null_to_default")]
    pub page: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub total_results: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub total_pages: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub results: Vec<SearchTvResult>,
}

// ---------------------------------------------------------------------------
// /movie/{id}
// ---------------------------------------------------------------------------

/// Go `tmdb.MovieDetailsRequest`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MovieDetailsRequest {
    pub id: i64,
    #[serde(default)]
    pub append_to_response: Vec<String>,
    #[serde(default)]
    pub language: Option<String>,
}

/// Go `tmdb.Genre` — the genre names that become `"genre"` content collections.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Genre {
    #[serde(default, deserialize_with = "null_to_default")]
    pub id: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub name: String,
}

/// The `belongs_to_collection` object.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BelongsToCollection {
    #[serde(default, deserialize_with = "null_to_default")]
    pub id: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub name: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub poster_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub backdrop_path: String,
}

/// An entry of `production_companies` / `networks`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductionCompany {
    #[serde(default, deserialize_with = "null_to_default")]
    pub name: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub id: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub logo_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub origin_country: String,
}

/// An entry of `production_countries`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductionCountry {
    #[serde(default, rename = "iso_3166_1")]
    pub iso_3166_1: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub name: String,
}

/// An entry of `spoken_languages`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpokenLanguage {
    #[serde(default, rename = "iso_639_1")]
    pub iso_639_1: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub name: String,
}

/// Go `tmdb.MovieDetailsResponse`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MovieDetailsResponse {
    #[serde(default, deserialize_with = "null_to_default")]
    pub adult: bool,
    #[serde(default, deserialize_with = "null_to_default")]
    pub backdrop_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub belongs_to_collection: BelongsToCollection,
    #[serde(default, deserialize_with = "null_to_default")]
    pub budget: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub genres: Vec<Genre>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub homepage: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub id: i64,
    /// Go's field is `IMDbID` with the `imdb_id` JSON tag — the external
    /// identifier that becomes a `key = 'id'` content attribute.
    #[serde(default, deserialize_with = "null_to_default")]
    pub imdb_id: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub original_language: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub original_title: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub overview: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub popularity: f32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub poster_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub production_companies: Vec<ProductionCompany>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub production_countries: Vec<ProductionCountry>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub release_date: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub revenue: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub runtime: i32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub spoken_languages: Vec<SpokenLanguage>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub status: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub tagline: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub title: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub video: bool,
    #[serde(default, deserialize_with = "null_to_default")]
    pub vote_average: f32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub vote_count: i64,
}

// ---------------------------------------------------------------------------
// /tv/{series_id}
// ---------------------------------------------------------------------------

/// Go `tmdb.TvDetailsRequest`. `append_to_response` is how the classifier asks
/// for `external_ids` in one round trip (`tmdb.go:87`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TvDetailsRequest {
    pub series_id: i64,
    #[serde(default)]
    pub append_to_response: Vec<String>,
    #[serde(default)]
    pub language: Option<String>,
}

/// An entry of `created_by`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TvCreatedBy {
    #[serde(default, deserialize_with = "null_to_default")]
    pub id: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub credit_id: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub name: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub gender: i32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub profile_path: String,
}

/// The `last_episode_to_air` / `next_episode_to_air` objects.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TvEpisode {
    #[serde(default, deserialize_with = "null_to_default")]
    pub air_date: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub episode_number: i32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub id: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub name: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub overview: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub production_code: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub season_number: i32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub show_id: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub still_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub vote_average: f32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub vote_count: i64,
}

/// An entry of `seasons`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TvSeason {
    #[serde(default, deserialize_with = "null_to_default")]
    pub air_date: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub episode_count: i32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub id: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub name: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub overview: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub poster_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub season_number: i32,
}

/// The `external_ids` object, requested via `append_to_response`. Its `imdb_id`
/// / `tvdb_id` become `key = 'id'` content attributes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TvExternalIds {
    #[serde(default, deserialize_with = "null_to_default")]
    pub imdb_id: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub freebase_mid: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub freebase_id: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub tvdb_id: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub tvrage_id: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub facebook_id: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub instagram_id: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub twitter_id: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub id: i64,
}

/// Go `tmdb.TvDetailsResponse`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TvDetailsResponse {
    #[serde(default, deserialize_with = "null_to_default")]
    pub backdrop_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub created_by: Vec<TvCreatedBy>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub episode_run_time: Vec<i32>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub first_air_date: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub genres: Vec<Genre>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub homepage: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub id: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub in_production: bool,
    #[serde(default, deserialize_with = "null_to_default")]
    pub languages: Vec<String>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub last_air_date: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub last_episode_to_air: TvEpisode,
    #[serde(default, deserialize_with = "null_to_default")]
    pub name: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub next_episode_to_air: TvEpisode,
    #[serde(default, deserialize_with = "null_to_default")]
    pub networks: Vec<ProductionCompany>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub number_of_episodes: i32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub number_of_seasons: i32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub origin_country: Vec<String>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub original_language: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub original_name: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub overview: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub popularity: f32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub poster_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub production_companies: Vec<ProductionCompany>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub production_countries: Vec<ProductionCountry>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub seasons: Vec<TvSeason>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub status: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub tagline: String,
    #[serde(default, rename = "type")]
    pub show_type: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub vote_average: f32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub vote_count: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub external_ids: TvExternalIds,
}

// ---------------------------------------------------------------------------
// /find/{external_id}
// ---------------------------------------------------------------------------

/// Go `tmdb.FindByIDRequest`. `external_source` is the TMDB-side source name
/// (`imdb_id`, `tvdb_id`, …) derived from a `model.ContentRef` by Go's
/// `tmdb.ExternalSource`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindByIdRequest {
    pub external_source: String,
    pub external_id: String,
    #[serde(default)]
    pub language: Option<String>,
}

/// An entry of `movie_results`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FindByIdMovieResult {
    #[serde(default, deserialize_with = "null_to_default")]
    pub adult: bool,
    #[serde(default, deserialize_with = "null_to_default")]
    pub backdrop_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub genre_ids: Vec<i64>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub id: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub original_language: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub original_title: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub overview: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub poster_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub release_date: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub title: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub video: bool,
    #[serde(default, deserialize_with = "null_to_default")]
    pub vote_average: f32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub vote_count: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub popularity: f32,
}

/// An entry of `tv_results`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FindByIdTvResult {
    #[serde(default, deserialize_with = "null_to_default")]
    pub original_name: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub id: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub name: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub vote_count: i64,
    #[serde(default, deserialize_with = "null_to_default")]
    pub vote_average: f32,
    #[serde(default, deserialize_with = "null_to_default")]
    pub first_air_date: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub poster_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub genre_ids: Vec<i64>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub original_language: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub backdrop_path: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub overview: String,
    #[serde(default, deserialize_with = "null_to_default")]
    pub origin_country: Vec<String>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub popularity: f32,
}

/// Go `tmdb.FindByIDResponse`. Go's `tmdbGetTMDBIDByExternalID` dispatches on
/// the content type and takes the **first** entry of the matching array, so the
/// array order is part of the observation.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FindByIdResponse {
    #[serde(default, deserialize_with = "null_to_default")]
    pub movie_results: Vec<FindByIdMovieResult>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub tv_results: Vec<FindByIdTvResult>,
}

// ---------------------------------------------------------------------------
// DTO -> model.Content — Go `internal/tmdb/transformers.go`
// ---------------------------------------------------------------------------
//
// These live here rather than in `bitmagnet-tmdb` (where that crate's
// placeholder doc-comment anticipated them) for the same dependency reason the
// DTOs do: `bitmagnet-tmdb` depends on this crate to implement `ContentResolver`,
// while the classifier's `attach_tmdb_*` actions need the transform at attach
// time. Putting it there would be a cycle. The transform is a pure function of
// a DTO, so it belongs with the DTO regardless.

use bitmagnet_model::{Content, ContentAttribute, ContentCollection, ContentType, Date};

/// Go `model.SourceTmdb` / `SourceImdb` / `SourceTvdb`.
const SOURCE_TMDB: &str = "tmdb";
const SOURCE_IMDB: &str = "imdb";
const SOURCE_TVDB: &str = "tvdb";

/// A TMDB date string that Go's `time.Parse("2006-01-02", …)` would reject.
///
/// Go propagates this as a hard error out of the transformer, which becomes an
/// `error` outcome for the classification — NOT an unmatched fallthrough — so it
/// is modelled as an error here too.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid TMDB date {value:?}: {reason}")]
pub struct DateParseError {
    pub value: String,
    pub reason: &'static str,
}

/// Go `model.NewDateFromIsoString` — `time.Parse` with the layout `2006-01-02`.
///
/// That layout is fixed-width and digits-only, and `time.Parse` validates the
/// day against the real length of the month (it rejects `2021-02-30`), so this
/// does the same. Accepting a date Go rejects would attach content Go errored
/// on, which is a divergence the tape cannot see — the tape records the API
/// response, and both sides decode the same bytes.
fn parse_iso_date(value: &str) -> Result<Date, DateParseError> {
    let invalid = |reason| DateParseError {
        value: value.to_owned(),
        reason,
    };

    let bytes = value.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return Err(invalid("expected exactly YYYY-MM-DD"));
    }

    // `str::parse` would accept a leading `+`; the layout's digit fields do not.
    let digits = |range: std::ops::Range<usize>| {
        bytes[range.clone()]
            .iter()
            .all(u8::is_ascii_digit)
            .then(|| value[range].parse::<u16>().ok())
            .flatten()
    };

    let (Some(year), Some(month), Some(day)) = (digits(0..4), digits(5..7), digits(8..10)) else {
        return Err(invalid("expected exactly YYYY-MM-DD"));
    };

    let month = u8::try_from(month).map_err(|_| invalid("month out of range"))?;
    let day = u8::try_from(day).map_err(|_| invalid("day out of range"))?;

    if month == 0 || month > 12 {
        return Err(invalid("month out of range"));
    }

    if day == 0 || day > days_in_month(year, month) {
        return Err(invalid("day out of range"));
    }

    Ok(Date { year, month, day })
}

/// Proleptic Gregorian, matching Go's `time` package.
const fn days_in_month(year: u16, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        _ => 28,
    }
}

/// Go's `Year` zero value is nil; `Content.release_year` carries that as `None`.
fn release_year(date: Option<Date>) -> Option<u32> {
    date.map(|d| u32::from(d.year)).filter(|year| *year != 0)
}

/// Builds a `content_attributes` row.
///
/// 🚨 Go leaves the three `content_*` key columns at their zero values here and
/// lets GORM fill them from the association on write. Rust's `ContentType` has no
/// zero variant to mirror that with, so the row is built already pointing at its
/// parent — which is what it becomes once persisted. No attach decision reads
/// these columns, so the difference is invisible to the parity gate; it is called
/// out because a future *result* diff against Go's in-memory value would see it.
fn attribute(
    content_type: ContentType,
    content_id: &str,
    source: &str,
    key: &str,
    value: String,
) -> ContentAttribute {
    ContentAttribute {
        content_type,
        content_source: SOURCE_TMDB.to_owned(),
        content_id: content_id.to_owned(),
        source: source.to_owned(),
        key: key.to_owned(),
        value,
    }
}

impl MovieDetailsResponse {
    /// Go `tmdb.MovieDetailsToMovieModel`.
    ///
    /// # Errors
    ///
    /// If `release_date` is present but not a date Go would parse.
    pub fn into_content(self) -> Result<Content, DateParseError> {
        let release_date = if self.release_date.is_empty() {
            None
        } else {
            Some(parse_iso_date(&self.release_date)?)
        };

        // Go: `details.Adult` promotes a movie to xxx.
        let content_type = if self.adult {
            ContentType::Xxx
        } else {
            ContentType::Movie
        };
        let id = self.id.to_string();

        let mut collections = Vec::new();
        if self.belongs_to_collection.id != 0 {
            collections.push(ContentCollection {
                collection_type: "franchise".to_owned(),
                source: SOURCE_TMDB.to_owned(),
                id: self.belongs_to_collection.id.to_string(),
                name: self.belongs_to_collection.name,
            });
        }
        collections.extend(self.genres.into_iter().map(|genre| ContentCollection {
            collection_type: "genre".to_owned(),
            source: SOURCE_TMDB.to_owned(),
            id: genre.id.to_string(),
            name: genre.name,
        }));

        // Order mirrors Go's appends: imdb id, then poster, then backdrop.
        let mut attributes = Vec::new();
        if !self.imdb_id.is_empty() {
            attributes.push(attribute(
                content_type,
                &id,
                SOURCE_IMDB,
                "id",
                self.imdb_id,
            ));
        }
        if !self.poster_path.is_empty() {
            attributes.push(attribute(
                content_type,
                &id,
                SOURCE_TMDB,
                "poster_path",
                self.poster_path,
            ));
        }
        if !self.backdrop_path.is_empty() {
            attributes.push(attribute(
                content_type,
                &id,
                SOURCE_TMDB,
                "backdrop_path",
                self.backdrop_path,
            ));
        }

        Ok(Content {
            content_type,
            source: SOURCE_TMDB.to_owned(),
            id,
            title: self.title,
            release_date,
            release_year: release_year(release_date),
            // Go `NewNullBool`/`NewNullFloat32`/`NewNullUint` are unconditionally
            // valid, so these are always Some even at their zero value. Overview
            // and runtime are the two Go guards explicitly.
            adult: Some(self.adult),
            original_language: bitmagnet_release::parse_language(&self.original_language),
            // Go `NewNullString` is valid even for "", unlike Overview below.
            original_title: Some(self.original_title),
            overview: (!self.overview.is_empty()).then_some(self.overview),
            runtime: (self.runtime > 0).then_some(self.runtime.unsigned_abs()),
            popularity: Some(self.popularity),
            vote_average: Some(self.vote_average),
            vote_count: Some(
                self.vote_count
                    .unsigned_abs()
                    .try_into()
                    .unwrap_or(u32::MAX),
            ),
            collections,
            attributes,
            created_at: None,
            updated_at: None,
            tsv: bitmagnet_fts::Tsvector::default(),
        })
    }
}

impl TvDetailsResponse {
    /// Go `tmdb.TvShowDetailsToTvShowModel`.
    ///
    /// # Errors
    ///
    /// If `first_air_date` is present but not a date Go would parse.
    pub fn into_content(self) -> Result<Content, DateParseError> {
        let first_air_date = if self.first_air_date.is_empty() {
            None
        } else {
            Some(parse_iso_date(&self.first_air_date)?)
        };

        let id = self.id.to_string();

        // Go builds this with slice.Map — genres only, no franchise collection.
        let collections = self
            .genres
            .into_iter()
            .map(|genre| ContentCollection {
                collection_type: "genre".to_owned(),
                source: SOURCE_TMDB.to_owned(),
                id: genre.id.to_string(),
                name: genre.name,
            })
            .collect();

        // Order mirrors Go's appends: imdb, tvdb, poster, backdrop.
        let mut attributes = Vec::new();
        if !self.external_ids.imdb_id.is_empty() {
            attributes.push(attribute(
                ContentType::TvShow,
                &id,
                SOURCE_IMDB,
                "id",
                self.external_ids.imdb_id,
            ));
        }
        if self.external_ids.tvdb_id != 0 {
            attributes.push(attribute(
                ContentType::TvShow,
                &id,
                SOURCE_TVDB,
                "id",
                self.external_ids.tvdb_id.to_string(),
            ));
        }
        if !self.poster_path.is_empty() {
            attributes.push(attribute(
                ContentType::TvShow,
                &id,
                SOURCE_TMDB,
                "poster_path",
                self.poster_path,
            ));
        }
        if !self.backdrop_path.is_empty() {
            attributes.push(attribute(
                ContentType::TvShow,
                &id,
                SOURCE_TMDB,
                "backdrop_path",
                self.backdrop_path,
            ));
        }

        Ok(Content {
            content_type: ContentType::TvShow,
            source: SOURCE_TMDB.to_owned(),
            id,
            title: self.name,
            release_date: first_air_date,
            release_year: release_year(first_air_date),
            // Go's TV transform sets neither Adult nor Runtime.
            adult: None,
            original_language: bitmagnet_release::parse_language(&self.original_language),
            original_title: Some(self.original_name),
            overview: (!self.overview.is_empty()).then_some(self.overview),
            runtime: None,
            popularity: Some(self.popularity),
            vote_average: Some(self.vote_average),
            vote_count: Some(
                self.vote_count
                    .unsigned_abs()
                    .try_into()
                    .unwrap_or(u32::MAX),
            ),
            collections,
            attributes,
            created_at: None,
            updated_at: None,
            tsv: bitmagnet_fts::Tsvector::default(),
        })
    }
}

/// Go `tmdb.ExternalSource`: which `/find/{id}` external source a content ref
/// maps onto, or `None` for Go's `ErrUnmatched`.
#[must_use]
pub fn external_source(content_type: ContentType, source: &str) -> Option<&'static str> {
    match (content_type, source) {
        (ContentType::Movie | ContentType::TvShow | ContentType::Xxx, SOURCE_IMDB) => {
            Some("imdb_id")
        }
        (ContentType::TvShow, SOURCE_TVDB) => Some("tvdb_id"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An absent `external_ids` (no `append_to_response`) must decode to the
    /// zero value, as Go's `encoding/json` does — not fail.
    #[test]
    fn absent_nested_objects_decode_to_the_zero_value() {
        let decoded: TvDetailsResponse =
            serde_json::from_str(r#"{"id":1399,"name":"Game of Thrones"}"#).unwrap();
        assert_eq!(decoded.id, 1399);
        assert_eq!(decoded.external_ids, TvExternalIds::default());
        assert_eq!(decoded.last_episode_to_air, TvEpisode::default());
    }

    /// `type` is a Rust keyword; the rename must keep the wire name.
    #[test]
    fn tv_show_type_keeps_its_wire_name() {
        let decoded: TvDetailsResponse = serde_json::from_str(r#"{"type":"Scripted"}"#).unwrap();
        assert_eq!(decoded.show_type, "Scripted");
    }

    /// Search results preserve TMDB's relevance order verbatim — the
    /// Levenshtein pick is first-wins over it.
    #[test]
    fn search_results_preserve_order() {
        let decoded: SearchMovieResponse = serde_json::from_str(
            r#"{"page":1,"results":[{"id":1,"title":"A"},{"id":2,"title":"B"}]}"#,
        )
        .unwrap();
        let ids: Vec<i64> = decoded.results.iter().map(|r| r.id).collect();
        assert_eq!(ids, vec![1, 2]);
    }

    /// 🚨 The regression the production corpus gate caught. TMDB sends `null`
    /// for strings it has no value for, and Go's `encoding/json` documents that
    /// unmarshalling `null` into a non-pointer leaves the zero value and
    /// produces no error. A plain `#[serde(default)]` rejects it, which failed
    /// four real subjects with `invalid type: null, expected a string`.
    #[test]
    fn explicit_nulls_decode_to_go_zero_values() {
        let decoded: MovieDetailsResponse = serde_json::from_str(
            r#"{"id":1,"title":"T","poster_path":null,"backdrop_path":null,
                "overview":null,"imdb_id":null,"belongs_to_collection":null,
                "genres":null,"original_language":null,"runtime":null}"#,
        )
        .expect("null decodes as Go's zero value");

        assert_eq!(decoded.poster_path, "");
        assert_eq!(decoded.imdb_id, "");
        assert_eq!(decoded.belongs_to_collection.id, 0);
        assert!(decoded.genres.is_empty());
        assert_eq!(decoded.runtime, 0);
    }

    #[test]
    fn explicit_nulls_decode_in_search_and_tv_payloads_too() {
        let movies: SearchMovieResponse = serde_json::from_str(
            r#"{"results":[{"id":1,"title":"T","original_title":null,"release_date":null}]}"#,
        )
        .expect("search results tolerate null");
        assert_eq!(movies.results[0].original_title, "");

        let tv: TvDetailsResponse = serde_json::from_str(
            r#"{"id":2,"name":"N","original_name":null,"first_air_date":null,
                "external_ids":{"imdb_id":null,"tvdb_id":null}}"#,
        )
        .expect("tv details tolerate null");
        assert_eq!(tv.original_name, "");
        assert_eq!(tv.external_ids.tvdb_id, 0);
    }

    // -- date parsing (Go `time.Parse("2006-01-02", …)`) ---------------------

    #[test]
    fn parses_a_well_formed_date() {
        assert_eq!(
            parse_iso_date("1950-02-15").unwrap(),
            Date {
                year: 1950,
                month: 2,
                day: 15
            }
        );
    }

    /// Go's `time.Parse` validates the day against the month's real length.
    #[test]
    fn rejects_a_day_the_month_does_not_have() {
        assert!(parse_iso_date("2021-02-30").is_err());
        assert!(parse_iso_date("2021-04-31").is_err());
        assert!(parse_iso_date("2021-13-01").is_err());
        assert!(parse_iso_date("2021-00-01").is_err());
        assert!(parse_iso_date("2021-01-00").is_err());
    }

    #[test]
    fn handles_leap_years_like_the_gregorian_calendar() {
        assert!(parse_iso_date("2020-02-29").is_ok(), "2020 is a leap year");
        assert!(parse_iso_date("2021-02-29").is_err(), "2021 is not");
        assert!(parse_iso_date("2000-02-29").is_ok(), "2000 is (div by 400)");
        assert!(
            parse_iso_date("1900-02-29").is_err(),
            "1900 is not (div by 100)"
        );
    }

    /// The layout is fixed-width and digits-only; `str::parse` alone would
    /// accept a leading sign.
    #[test]
    fn rejects_anything_that_is_not_exactly_yyyy_mm_dd() {
        for bad in [
            "",
            "1950",
            "1950-2-15",
            "1950/02/15",
            "+150-02-15",
            "1950-02-15T00:00:00Z",
        ] {
            assert!(parse_iso_date(bad).is_err(), "{bad:?} should not parse");
        }
    }

    // -- transformers --------------------------------------------------------

    /// Go promotes an adult movie to the `xxx` content type, and the collections
    /// come out franchise-first then genres, in TMDB's order.
    #[test]
    fn movie_transform_mirrors_gos_field_mapping() {
        let details = MovieDetailsResponse {
            id: 42,
            title: "A Movie".to_owned(),
            original_title: "Le Film".to_owned(),
            adult: true,
            release_date: "1999-03-31".to_owned(),
            imdb_id: "tt0133093".to_owned(),
            poster_path: "/p.jpg".to_owned(),
            original_language: "fr".to_owned(),
            runtime: 136,
            overview: String::new(),
            belongs_to_collection: BelongsToCollection {
                id: 7,
                name: "Franchise".to_owned(),
                ..Default::default()
            },
            genres: vec![Genre {
                id: 28,
                name: "Action".to_owned(),
            }],
            ..Default::default()
        };

        let content = details.into_content().expect("transforms");

        assert_eq!(
            content.content_type,
            ContentType::Xxx,
            "adult promotes to xxx"
        );
        assert_eq!(content.id, "42");
        assert_eq!(content.release_year, Some(1999));
        assert_eq!(content.original_language.as_deref(), Some("fr"));
        assert_eq!(content.runtime, Some(136));
        // Go's NewNullString is valid even for "", unlike Overview.
        assert_eq!(content.original_title.as_deref(), Some("Le Film"));
        assert_eq!(content.overview, None, "an empty overview is NULL in Go");

        let collections: Vec<_> = content
            .collections
            .iter()
            .map(|c| (c.collection_type.as_str(), c.id.as_str()))
            .collect();
        assert_eq!(collections, vec![("franchise", "7"), ("genre", "28")]);

        let attributes: Vec<_> = content
            .attributes
            .iter()
            .map(|a| (a.source.as_str(), a.key.as_str()))
            .collect();
        assert_eq!(attributes, vec![("imdb", "id"), ("tmdb", "poster_path")]);
    }

    /// Go's TV transform sets neither Adult nor Runtime, and pulls the imdb/tvdb
    /// ids out of the appended `external_ids`.
    #[test]
    fn tv_transform_mirrors_gos_field_mapping() {
        let details = TvDetailsResponse {
            id: 1399,
            name: "A Show".to_owned(),
            original_name: "Ein Show".to_owned(),
            first_air_date: "2011-04-17".to_owned(),
            external_ids: TvExternalIds {
                imdb_id: "tt0944947".to_owned(),
                tvdb_id: 121_361,
                ..Default::default()
            },
            genres: vec![Genre {
                id: 10765,
                name: "Sci-Fi".to_owned(),
            }],
            ..Default::default()
        };

        let content = details.into_content().expect("transforms");

        assert_eq!(content.content_type, ContentType::TvShow);
        assert_eq!(content.release_year, Some(2011));
        assert_eq!(content.adult, None, "Go's TV transform never sets Adult");
        assert_eq!(content.runtime, None, "nor Runtime");

        let attributes: Vec<_> = content
            .attributes
            .iter()
            .map(|a| (a.source.as_str(), a.value.as_str()))
            .collect();
        assert_eq!(
            attributes,
            vec![("imdb", "tt0944947"), ("tvdb", "121361")],
            "Go appends imdb then tvdb"
        );
    }

    /// A movie with no release date is Go's zero Date, and its year is nil.
    #[test]
    fn an_absent_release_date_yields_no_year() {
        let content = MovieDetailsResponse {
            id: 1,
            release_date: String::new(),
            ..Default::default()
        }
        .into_content()
        .expect("transforms");

        assert_eq!(content.release_date, None);
        assert_eq!(content.release_year, None);
    }

    /// Go returns the parse error out of the transformer, so this must be an
    /// error rather than a silently dropped date.
    #[test]
    fn a_malformed_release_date_is_an_error_not_a_dropped_field() {
        let result = MovieDetailsResponse {
            id: 1,
            release_date: "not-a-date".to_owned(),
            ..Default::default()
        }
        .into_content();

        assert!(result.is_err());
    }

    #[test]
    fn external_source_maps_only_the_pairs_go_maps() {
        assert_eq!(external_source(ContentType::Movie, "imdb"), Some("imdb_id"));
        assert_eq!(external_source(ContentType::Xxx, "imdb"), Some("imdb_id"));
        assert_eq!(
            external_source(ContentType::TvShow, "imdb"),
            Some("imdb_id")
        );
        assert_eq!(
            external_source(ContentType::TvShow, "tvdb"),
            Some("tvdb_id")
        );
        // Go's default arm is ErrUnmatched.
        assert_eq!(external_source(ContentType::Movie, "tvdb"), None);
        assert_eq!(external_source(ContentType::Music, "imdb"), None);
        assert_eq!(external_source(ContentType::Movie, ""), None);
    }

    /// Requests are tape keys: they must round-trip and compare by value.
    #[test]
    fn requests_round_trip_for_tape_keying() {
        let request = FindByIdRequest {
            external_source: "imdb_id".to_owned(),
            external_id: "tt0133093".to_owned(),
            language: None,
        };
        let json = serde_json::to_string(&request).unwrap();
        let back: FindByIdRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(request, back);
    }
}
