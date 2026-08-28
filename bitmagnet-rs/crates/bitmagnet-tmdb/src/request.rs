//! Go's TMDB URL construction (`internal/tmdb/client.go`), as a **value**.
//!
//! # Why the request is a value and not a side effect
//!
//! The question this client asks is the only part of it a parity gate can
//! compare, and it is exactly what Go records: `requester_recorder.go` observes
//! `{method, path, queryParams}` per call, and a replay matches it **byte for
//! byte**. Building that triple without touching the network means the whole URL
//! contract is assertable offline, against requests Go actually issued — see
//! `tests/tape_conformance.rs`.
//!
//! 🚨 **The `api_key` is deliberately not here.** Go sets it once as a
//! client-level query parameter (`requester_lazy.go:66`, resty's
//! `SetQueryParam`) and never per request. That is not only fidelity: because no
//! credential passes through this struct, a recorded tape carries no secret.
//! [`crate::TmdbClient`] adds the key when it turns a spec into a URL, at the
//! transport layer, below the recording seam.
//!
//! # Relationship to the classifier's tape resolver
//!
//! `bitmagnet_classifier::resolver::tape` contains the same five builders, for
//! the replay side, pinned against the production tape. They are private to that
//! crate, so this is a deliberate second copy of a spec that must not drift —
//! which is why the tests here assert against the *same recorded fixtures*
//! rather than against that code. If one side is changed and the other is not,
//! the corpus gate and this crate's tests disagree, loudly.

use std::collections::BTreeMap;

use bitmagnet_classifier::resolver::tmdb::{
    FindByIdRequest, MovieDetailsRequest, SearchMovieRequest, SearchTvRequest, TvDetailsRequest,
};
use serde::Serialize;

/// One TMDB call, as Go records it (`tmdb.tapeRequest`).
///
/// 🚨 Field order and the [`BTreeMap`] are both load-bearing. Go emits struct
/// fields in declaration order and `encoding/json` sorts map keys; the recorded
/// request is compared by bytes, so a reordered field or a `HashMap` here
/// desyncs — the `HashMap` case nondeterministically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TmdbRequestSpec {
    /// Always `GET`: every endpoint Go calls is a read.
    pub method: &'static str,
    /// Path relative to the configured base URL, leading slash included.
    pub path: String,
    #[serde(rename = "queryParams")]
    pub query_params: BTreeMap<String, String>,
}

impl TmdbRequestSpec {
    /// A `GET` of `path` with `query_params`.
    #[must_use]
    pub fn get(path: String, query_params: BTreeMap<String, String>) -> Self {
        Self {
            method: "GET",
            path,
            query_params,
        }
    }
}

/// Query parameters, built the way `internal/tmdb/client.go` builds them.
///
/// Every method here mirrors one `if` in that file. A parameter Go omits must be
/// **omitted, not sent empty**: the recorded request carries only what Go sent,
/// so an extra key is a desync — and against the live API an extra parameter is
/// a different query.
struct QueryParams(BTreeMap<String, String>);

impl QueryParams {
    fn new() -> Self {
        // Go always builds a non-nil map, so "no parameters" and "an empty
        // parameter set" encode identically (`{}`, never `null`).
        Self(BTreeMap::new())
    }

    fn set(&mut self, key: &str, value: impl Into<String>) {
        self.0.insert(key.to_owned(), value.into());
    }

    /// Go `model.NullString` — sent iff `Valid`.
    fn set_opt(&mut self, key: &str, value: Option<&String>) {
        if let Some(value) = value {
            self.set(key, value.clone());
        }
    }

    /// Go `model.Year`, whose **zero value is nil** (`IsNil()`), so a year of 0
    /// is omitted just as `None` is — never sent as `"0"`. Rendered with
    /// `Year.String()`, i.e. plain decimal.
    fn set_year(&mut self, key: &str, year: Option<u16>) {
        if let Some(year) = year.filter(|y| *y != 0) {
            self.set(key, year.to_string());
        }
    }

    /// Go `strings.Join(request.AppendToResponse, ",")`, omitted when empty.
    fn set_append_to_response(&mut self, values: &[String]) {
        if !values.is_empty() {
            self.set("append_to_response", values.join(","));
        }
    }

    fn get(self, path: String) -> TmdbRequestSpec {
        TmdbRequestSpec::get(path, self.0)
    }
}

/// Go `client.ValidateAPIKey` — `GET /authentication`, no parameters.
///
/// Go discards the body: a 2xx *is* the answer.
#[must_use]
pub fn validate_api_key() -> TmdbRequestSpec {
    QueryParams::new().get("/authentication".to_owned())
}

/// Go `client.SearchMovie` — `GET /search/movie`.
#[must_use]
pub fn search_movie(request: &SearchMovieRequest) -> TmdbRequestSpec {
    let mut params = QueryParams::new();
    params.set("query", request.query.clone());
    if request.include_adult {
        params.set("include_adult", "true");
    }
    params.set_opt("language", request.language.as_ref());
    params.set_year("primary_release_year", request.primary_release_year);
    params.set_year("year", request.year);
    params.set_opt("region", request.region.as_ref());
    params.get("/search/movie".to_owned())
}

/// Go `client.SearchTv` — `GET /search/tv`.
///
/// 🚨 `SearchTvRequest` carries a `year`, and `client.SearchTv` **never sends
/// it** — only `first_air_date_year`. Sending it would be an extra query
/// parameter, i.e. a desync against every recorded TV search.
#[must_use]
pub fn search_tv(request: &SearchTvRequest) -> TmdbRequestSpec {
    let mut params = QueryParams::new();
    params.set("query", request.query.clone());
    params.set_year("first_air_date_year", request.first_air_date_year);
    if request.include_adult {
        params.set("include_adult", "true");
    }
    params.set_opt("language", request.language.as_ref());
    params.get("/search/tv".to_owned())
}

/// Go `client.MovieDetails` — `GET /movie/{id}`.
#[must_use]
pub fn movie_details(request: &MovieDetailsRequest) -> TmdbRequestSpec {
    let mut params = QueryParams::new();
    params.set_append_to_response(&request.append_to_response);
    params.set_opt("language", request.language.as_ref());
    params.get(format!("/movie/{}", request.id))
}

/// Go `client.TvDetails` — `GET /tv/{series_id}`.
///
/// The classifier always appends `external_ids` here (`tmdb.go:87`); the
/// transform reads the imdb/tvdb ids out of it.
#[must_use]
pub fn tv_details(request: &TvDetailsRequest) -> TmdbRequestSpec {
    let mut params = QueryParams::new();
    params.set_append_to_response(&request.append_to_response);
    params.set_opt("language", request.language.as_ref());
    params.get(format!("/tv/{}", request.series_id))
}

/// Go `client.FindByID` — `GET /find/{external_id}`.
#[must_use]
pub fn find_by_id(request: &FindByIdRequest) -> TmdbRequestSpec {
    let mut params = QueryParams::new();
    params.set("external_source", request.external_source.clone());
    params.set_opt("language", request.language.as_ref());
    params.get(format!("/find/{}", request.external_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fixtures below are **verbatim `request` objects from the production
    /// tape** (`testdata/parity/classifier-attach/prod-20260809`) — requests Go
    /// actually made and recorded — and are the same ones pinned by the
    /// classifier's tape resolver. Matching these strings exactly is the whole
    /// contract: an extra parameter or a differently rendered id is a desync
    /// that reads as a port bug. `tests/tape_conformance.rs` runs the same
    /// assertion over *every* recorded request; these spell out the interesting
    /// cases with their reasons.
    fn encoded(request: &TmdbRequestSpec) -> String {
        serde_json::to_string(request).expect("request serialises")
    }

    #[test]
    fn search_tv_matches_the_recorded_request() {
        let request = search_tv(&SearchTvRequest {
            query: "UFC Fight Night Kape vs Horiguchi 20 05".to_owned(),
            include_adult: true,
            first_air_date_year: Some(2026),
            ..Default::default()
        });

        assert_eq!(
            encoded(&request),
            r#"{"method":"GET","path":"/search/tv","queryParams":{"first_air_date_year":"2026","include_adult":"true","query":"UFC Fight Night Kape vs Horiguchi 20 05"}}"#
        );
    }

    #[test]
    fn tv_details_matches_the_recorded_request() {
        let request = tv_details(&TvDetailsRequest {
            series_id: 12271,
            append_to_response: vec!["external_ids".to_owned()],
            ..Default::default()
        });

        assert_eq!(
            encoded(&request),
            r#"{"method":"GET","path":"/tv/12271","queryParams":{"append_to_response":"external_ids"}}"#
        );
    }

    /// Movie details carries NO query parameters, and the empty map must encode
    /// as `{}` rather than `null` — Go builds a non-nil map for exactly this
    /// reason.
    #[test]
    fn movie_details_matches_the_recorded_request() {
        let request = movie_details(&MovieDetailsRequest {
            id: 1_673_194,
            ..Default::default()
        });

        assert_eq!(
            encoded(&request),
            r#"{"method":"GET","path":"/movie/1673194","queryParams":{}}"#
        );
    }

    /// Query parameters are a Go map, so `encoding/json` sorts the keys. A
    /// `HashMap` here would pass or fail at random.
    #[test]
    fn query_params_are_key_sorted() {
        let request = search_movie(&SearchMovieRequest {
            query: "Cinderella".to_owned(),
            include_adult: true,
            year: Some(1950),
            region: Some("US".to_owned()),
            ..Default::default()
        });

        assert_eq!(
            encoded(&request),
            r#"{"method":"GET","path":"/search/movie","queryParams":{"include_adult":"true","query":"Cinderella","region":"US","year":"1950"}}"#
        );
    }

    /// Go's `model.Year` zero value is nil and `client.SearchMovie` skips a nil
    /// year, so a zero must be omitted rather than sent as `"0"`. A `"0"` would
    /// also be a real query against the live API: TMDB filters on it.
    #[test]
    fn a_zero_year_is_omitted_like_gos_nil() {
        let request = search_movie(&SearchMovieRequest {
            query: "Cinderella".to_owned(),
            year: Some(0),
            primary_release_year: Some(0),
            ..Default::default()
        });

        assert_eq!(
            encoded(&request),
            r#"{"method":"GET","path":"/search/movie","queryParams":{"query":"Cinderella"}}"#
        );
    }

    /// `client.SearchTv` sends `first_air_date_year` only, however the request
    /// struct is populated.
    #[test]
    fn search_tv_never_sends_year() {
        let request = search_tv(&SearchTvRequest {
            query: "Cinderella".to_owned(),
            year: Some(1950),
            ..Default::default()
        });

        assert_eq!(
            encoded(&request),
            r#"{"method":"GET","path":"/search/tv","queryParams":{"query":"Cinderella"}}"#
        );
    }

    /// `include_adult` is a Go bool: sent as the string `"true"` when set, and
    /// omitted entirely when false — not sent as `"false"`.
    #[test]
    fn a_false_include_adult_is_omitted_not_sent_as_false() {
        let request = search_movie(&SearchMovieRequest {
            query: "Cinderella".to_owned(),
            include_adult: false,
            ..Default::default()
        });

        assert!(!encoded(&request).contains("include_adult"));
    }

    /// `append_to_response` is a comma join, and the classifier can ask for more
    /// than one appendage — the join, not repeated keys, is what Go sends.
    #[test]
    fn append_to_response_is_a_comma_join() {
        let request = tv_details(&TvDetailsRequest {
            series_id: 1399,
            append_to_response: vec!["external_ids".to_owned(), "credits".to_owned()],
            ..Default::default()
        });

        assert_eq!(
            encoded(&request),
            r#"{"method":"GET","path":"/tv/1399","queryParams":{"append_to_response":"external_ids,credits"}}"#
        );
    }

    /// The production corpus holds no `/find` observation, so this endpoint is
    /// pinned against `client.FindByID` directly (and against the same fixture
    /// the classifier's tape resolver uses).
    #[test]
    fn find_by_id_builds_the_external_source_query() {
        let request = find_by_id(&FindByIdRequest {
            external_source: "imdb_id".to_owned(),
            external_id: "tt0042332".to_owned(),
            language: None,
        });

        assert_eq!(
            encoded(&request),
            r#"{"method":"GET","path":"/find/tt0042332","queryParams":{"external_source":"imdb_id"}}"#
        );
    }

    /// 🚨 The credential must never reach a request spec: it is what keeps
    /// recorded tapes secret-free, and this is the assertion that keeps it true
    /// as builders are edited.
    #[test]
    fn no_builder_puts_the_api_key_in_the_parameters() {
        let specs = [
            validate_api_key(),
            search_movie(&SearchMovieRequest {
                query: "q".to_owned(),
                ..Default::default()
            }),
            search_tv(&SearchTvRequest {
                query: "q".to_owned(),
                ..Default::default()
            }),
            movie_details(&MovieDetailsRequest {
                id: 1,
                ..Default::default()
            }),
            tv_details(&TvDetailsRequest {
                series_id: 1,
                ..Default::default()
            }),
            find_by_id(&FindByIdRequest {
                external_source: "imdb_id".to_owned(),
                external_id: "tt1".to_owned(),
                language: None,
            }),
        ];

        for spec in &specs {
            assert!(
                !spec.query_params.contains_key("api_key"),
                "{} must not carry the credential",
                spec.path
            );
        }
    }
}
