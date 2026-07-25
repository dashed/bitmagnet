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
//! * `#[serde(default)]` on every field reproduces that same tolerance
//!   field-by-field, including for `null` sent in place of a string.
//! * Requests derive `Serialize`/`Deserialize` + `Eq`/`Hash` so a tape can key
//!   on them directly. Responses carry `f32` scores and so are `PartialEq` only.
//! * Only the fields Go declares are modelled; TMDB sends more, and unknown
//!   fields are ignored on both sides.

use serde::{Deserialize, Serialize};

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
    #[serde(default)]
    pub vote_count: i64,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub video: bool,
    #[serde(default)]
    pub vote_average: f32,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub popularity: f32,
    #[serde(default)]
    pub poster_path: String,
    #[serde(default)]
    pub original_language: String,
    #[serde(default)]
    pub original_title: String,
    #[serde(default)]
    pub genre_ids: Vec<i64>,
    #[serde(default)]
    pub backdrop_path: String,
    #[serde(default)]
    pub adult: bool,
    #[serde(default)]
    pub overview: String,
    #[serde(default)]
    pub release_date: String,
}

/// Go `tmdb.SearchMovieResponse`. [`Self::results`] is TMDB's ordered relevance
/// ranking and must be preserved verbatim — the Levenshtein pick is first-wins
/// over it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchMovieResponse {
    #[serde(default)]
    pub page: i64,
    #[serde(default)]
    pub total_results: i64,
    #[serde(default)]
    pub total_pages: i64,
    #[serde(default)]
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
    #[serde(default)]
    pub original_name: String,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub vote_count: i64,
    #[serde(default)]
    pub vote_average: f32,
    #[serde(default)]
    pub poster_path: String,
    #[serde(default)]
    pub first_air_date: String,
    #[serde(default)]
    pub popularity: f32,
    #[serde(default)]
    pub genre_ids: Vec<i64>,
    #[serde(default)]
    pub original_language: String,
    #[serde(default)]
    pub backdrop_path: String,
    #[serde(default)]
    pub overview: String,
    #[serde(default)]
    pub origin_country: Vec<String>,
}

/// Go `tmdb.SearchTvResponse`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchTvResponse {
    #[serde(default)]
    pub page: i64,
    #[serde(default)]
    pub total_results: i64,
    #[serde(default)]
    pub total_pages: i64,
    #[serde(default)]
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
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub name: String,
}

/// The `belongs_to_collection` object.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BelongsToCollection {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub poster_path: String,
    #[serde(default)]
    pub backdrop_path: String,
}

/// An entry of `production_companies` / `networks`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductionCompany {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub logo_path: String,
    #[serde(default)]
    pub origin_country: String,
}

/// An entry of `production_countries`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductionCountry {
    #[serde(default, rename = "iso_3166_1")]
    pub iso_3166_1: String,
    #[serde(default)]
    pub name: String,
}

/// An entry of `spoken_languages`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpokenLanguage {
    #[serde(default, rename = "iso_639_1")]
    pub iso_639_1: String,
    #[serde(default)]
    pub name: String,
}

/// Go `tmdb.MovieDetailsResponse`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MovieDetailsResponse {
    #[serde(default)]
    pub adult: bool,
    #[serde(default)]
    pub backdrop_path: String,
    #[serde(default)]
    pub belongs_to_collection: BelongsToCollection,
    #[serde(default)]
    pub budget: i64,
    #[serde(default)]
    pub genres: Vec<Genre>,
    #[serde(default)]
    pub homepage: String,
    #[serde(default)]
    pub id: i64,
    /// Go's field is `IMDbID` with the `imdb_id` JSON tag — the external
    /// identifier that becomes a `key = 'id'` content attribute.
    #[serde(default)]
    pub imdb_id: String,
    #[serde(default)]
    pub original_language: String,
    #[serde(default)]
    pub original_title: String,
    #[serde(default)]
    pub overview: String,
    #[serde(default)]
    pub popularity: f32,
    #[serde(default)]
    pub poster_path: String,
    #[serde(default)]
    pub production_companies: Vec<ProductionCompany>,
    #[serde(default)]
    pub production_countries: Vec<ProductionCountry>,
    #[serde(default)]
    pub release_date: String,
    #[serde(default)]
    pub revenue: i64,
    #[serde(default)]
    pub runtime: i32,
    #[serde(default)]
    pub spoken_languages: Vec<SpokenLanguage>,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub tagline: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub video: bool,
    #[serde(default)]
    pub vote_average: f32,
    #[serde(default)]
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
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub credit_id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub gender: i32,
    #[serde(default)]
    pub profile_path: String,
}

/// The `last_episode_to_air` / `next_episode_to_air` objects.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TvEpisode {
    #[serde(default)]
    pub air_date: String,
    #[serde(default)]
    pub episode_number: i32,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub overview: String,
    #[serde(default)]
    pub production_code: String,
    #[serde(default)]
    pub season_number: i32,
    #[serde(default)]
    pub show_id: i64,
    #[serde(default)]
    pub still_path: String,
    #[serde(default)]
    pub vote_average: f32,
    #[serde(default)]
    pub vote_count: i64,
}

/// An entry of `seasons`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TvSeason {
    #[serde(default)]
    pub air_date: String,
    #[serde(default)]
    pub episode_count: i32,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub overview: String,
    #[serde(default)]
    pub poster_path: String,
    #[serde(default)]
    pub season_number: i32,
}

/// The `external_ids` object, requested via `append_to_response`. Its `imdb_id`
/// / `tvdb_id` become `key = 'id'` content attributes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TvExternalIds {
    #[serde(default)]
    pub imdb_id: String,
    #[serde(default)]
    pub freebase_mid: String,
    #[serde(default)]
    pub freebase_id: String,
    #[serde(default)]
    pub tvdb_id: i64,
    #[serde(default)]
    pub tvrage_id: i64,
    #[serde(default)]
    pub facebook_id: String,
    #[serde(default)]
    pub instagram_id: String,
    #[serde(default)]
    pub twitter_id: String,
    #[serde(default)]
    pub id: i64,
}

/// Go `tmdb.TvDetailsResponse`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TvDetailsResponse {
    #[serde(default)]
    pub backdrop_path: String,
    #[serde(default)]
    pub created_by: Vec<TvCreatedBy>,
    #[serde(default)]
    pub episode_run_time: Vec<i32>,
    #[serde(default)]
    pub first_air_date: String,
    #[serde(default)]
    pub genres: Vec<Genre>,
    #[serde(default)]
    pub homepage: String,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub in_production: bool,
    #[serde(default)]
    pub languages: Vec<String>,
    #[serde(default)]
    pub last_air_date: String,
    #[serde(default)]
    pub last_episode_to_air: TvEpisode,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub next_episode_to_air: TvEpisode,
    #[serde(default)]
    pub networks: Vec<ProductionCompany>,
    #[serde(default)]
    pub number_of_episodes: i32,
    #[serde(default)]
    pub number_of_seasons: i32,
    #[serde(default)]
    pub origin_country: Vec<String>,
    #[serde(default)]
    pub original_language: String,
    #[serde(default)]
    pub original_name: String,
    #[serde(default)]
    pub overview: String,
    #[serde(default)]
    pub popularity: f32,
    #[serde(default)]
    pub poster_path: String,
    #[serde(default)]
    pub production_companies: Vec<ProductionCompany>,
    #[serde(default)]
    pub production_countries: Vec<ProductionCountry>,
    #[serde(default)]
    pub seasons: Vec<TvSeason>,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub tagline: String,
    #[serde(default, rename = "type")]
    pub show_type: String,
    #[serde(default)]
    pub vote_average: f32,
    #[serde(default)]
    pub vote_count: i64,
    #[serde(default)]
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
    #[serde(default)]
    pub adult: bool,
    #[serde(default)]
    pub backdrop_path: String,
    #[serde(default)]
    pub genre_ids: Vec<i64>,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub original_language: String,
    #[serde(default)]
    pub original_title: String,
    #[serde(default)]
    pub overview: String,
    #[serde(default)]
    pub poster_path: String,
    #[serde(default)]
    pub release_date: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub video: bool,
    #[serde(default)]
    pub vote_average: f32,
    #[serde(default)]
    pub vote_count: i64,
    #[serde(default)]
    pub popularity: f32,
}

/// An entry of `tv_results`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FindByIdTvResult {
    #[serde(default)]
    pub original_name: String,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub vote_count: i64,
    #[serde(default)]
    pub vote_average: f32,
    #[serde(default)]
    pub first_air_date: String,
    #[serde(default)]
    pub poster_path: String,
    #[serde(default)]
    pub genre_ids: Vec<i64>,
    #[serde(default)]
    pub original_language: String,
    #[serde(default)]
    pub backdrop_path: String,
    #[serde(default)]
    pub overview: String,
    #[serde(default)]
    pub origin_country: Vec<String>,
    #[serde(default)]
    pub popularity: f32,
}

/// Go `tmdb.FindByIDResponse`. Go's `tmdbGetTMDBIDByExternalID` dispatches on
/// the content type and takes the **first** entry of the matching array, so the
/// array order is part of the observation.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FindByIdResponse {
    #[serde(default)]
    pub movie_results: Vec<FindByIdMovieResult>,
    #[serde(default)]
    pub tv_results: Vec<FindByIdTvResult>,
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
