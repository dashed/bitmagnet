//! Every TMDB request in the production tape, rebuilt from this crate's
//! builders and compared to what Go actually sent.
//!
//! # Why this and not a smoke test
//!
//! `testdata/parity/classifier-attach/prod-20260809` was recorded live from
//! `bitmagnet-0`, the only pod running the classifier with the enrichment flags
//! on. Its 48 `tmdb.request` observations are requests **Go issued against the
//! real API**, recorded as `{method, path, queryParams}` by
//! `internal/tmdb/requester_recorder.go`. A replay matches them **byte for
//! byte**, so reproducing those bytes is the entire URL contract — and the only
//! part of a live client that can be verified without a network or a key.
//!
//! The test walks the tape in both directions:
//!
//! 1. **Reverse**: turn each recorded request back into the DTO the classifier
//!    would have passed (`{"query":…,"include_adult":"true"}` → a
//!    [`SearchMovieRequest`]). Any parameter the reverse map does not recognise
//!    fails the test — that is what catches a parameter Go sends and this crate
//!    has never heard of, which a forward-only test would miss entirely.
//! 2. **Forward**: rebuild the request from that DTO and compare the encoding to
//!    the recorded bytes.
//!
//! The encoder is [`bitmagnet_tape::marshal`], the same canonical encoder the
//! replay comparison uses (Go's `encoding/json` with HTML escaping off, plus the
//! U+2028/U+2029 repair) — so a pass here means the bytes a real replay would
//! compare, not a `serde_json` approximation of them.
//!
//! # Known gap
//!
//! The corpus contains **no `/find/{external_id}` observation**: production
//! reached the TMDB find endpoint for none of these 300 subjects. That endpoint
//! is therefore pinned against Go's `client.FindByID` by unit test only, and
//! this test asserts the gap explicitly so it is visible rather than assumed.

use std::collections::BTreeMap;
use std::path::PathBuf;

use bitmagnet_classifier::resolver::tmdb::{
    FindByIdRequest, MovieDetailsRequest, SearchMovieRequest, SearchTvRequest, TvDetailsRequest,
};
use bitmagnet_tmdb::request;
use serde::Deserialize;
use serde_json::value::RawValue;

/// One line of the tape: a subject with the observations its classification
/// made, in order.
#[derive(Debug, Deserialize)]
struct Record {
    subject: String,
    #[serde(default)]
    observations: Vec<Observation>,
}

#[derive(Debug, Deserialize)]
struct Observation {
    kind: String,
    /// Kept as raw bytes: re-encoding it through `serde_json::Value` would
    /// canonicalise the very thing under test.
    request: Box<RawValue>,
}

/// Go's `tmdb.tapeRequest`, for reading.
#[derive(Debug, Deserialize)]
struct RecordedRequest {
    method: String,
    path: String,
    #[serde(rename = "queryParams")]
    query_params: BTreeMap<String, String>,
}

fn tape_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/classifier-attach/prod-20260809/tape.jsonl")
}

fn recorded_tmdb_requests() -> Vec<(String, Box<RawValue>)> {
    let tape = std::fs::read_to_string(tape_path()).expect("production tape is checked in");

    tape.lines()
        .filter(|line| !line.trim().is_empty())
        .flat_map(|line| {
            let record: Record = serde_json::from_str(line).expect("tape line decodes");
            let subject = record.subject;

            record
                .observations
                .into_iter()
                .filter(|observation| observation.kind == "tmdb.request")
                .map(move |observation| (subject.clone(), observation.request))
        })
        .collect()
}

/// Rebuilds the request from the DTO the classifier would have held.
///
/// Consumes the parameters it understands and hands back the remainder, so an
/// unrecognised parameter is a failure rather than a silent pass.
fn rebuild(recorded: &RecordedRequest) -> (String, Vec<String>) {
    let mut params = recorded.query_params.clone();

    let mut take = |key: &str| params.remove(key);
    let year = |value: Option<String>| value.map(|v| v.parse().expect("a year is decimal"));

    let spec = if recorded.path == "/search/movie" {
        request::search_movie(&SearchMovieRequest {
            query: take("query").unwrap_or_default(),
            include_adult: take("include_adult").as_deref() == Some("true"),
            language: take("language"),
            primary_release_year: year(take("primary_release_year")),
            year: year(take("year")),
            region: take("region"),
        })
    } else if recorded.path == "/search/tv" {
        request::search_tv(&SearchTvRequest {
            query: take("query").unwrap_or_default(),
            include_adult: take("include_adult").as_deref() == Some("true"),
            language: take("language"),
            first_air_date_year: year(take("first_air_date_year")),
            // Never sent by `client.SearchTv`, so it can never be recovered
            // from a recording — and populating it here would prove nothing.
            year: None,
        })
    } else if let Some(id) = recorded.path.strip_prefix("/movie/") {
        request::movie_details(&MovieDetailsRequest {
            id: id.parse().expect("a TMDB id is decimal"),
            append_to_response: append_to_response(take("append_to_response")),
            language: take("language"),
        })
    } else if let Some(id) = recorded.path.strip_prefix("/tv/") {
        request::tv_details(&TvDetailsRequest {
            series_id: id.parse().expect("a TMDB id is decimal"),
            append_to_response: append_to_response(take("append_to_response")),
            language: take("language"),
        })
    } else if let Some(external_id) = recorded.path.strip_prefix("/find/") {
        request::find_by_id(&FindByIdRequest {
            external_source: take("external_source").unwrap_or_default(),
            external_id: external_id.to_owned(),
            language: take("language"),
        })
    } else {
        panic!("the tape holds a TMDB endpoint this crate does not build: {recorded:?}");
    };

    let encoded = bitmagnet_tape::marshal(&spec).expect("request encodes");

    (encoded, params.into_keys().collect())
}

fn append_to_response(joined: Option<String>) -> Vec<String> {
    joined
        .map(|value| value.split(',').map(str::to_owned).collect())
        .unwrap_or_default()
}

/// 🚨 The gate. Every request Go made, rebuilt and compared by bytes.
#[test]
fn every_recorded_request_is_rebuilt_byte_for_byte() {
    let recorded = recorded_tmdb_requests();
    assert!(
        !recorded.is_empty(),
        "the corpus must hold TMDB observations, or this test proves nothing"
    );

    for (subject, raw) in &recorded {
        let parsed: RecordedRequest =
            serde_json::from_str(raw.get()).expect("a recorded request decodes");
        assert_eq!(parsed.method, "GET", "{subject}: every TMDB call is a GET");

        let (rebuilt, unrecognised) = rebuild(&parsed);

        assert!(
            unrecognised.is_empty(),
            "{subject}: Go sent query parameters this crate does not build: {unrecognised:?}"
        );
        assert_eq!(
            rebuilt,
            raw.get(),
            "{subject}: rebuilt request diverges from the recording"
        );
    }
}

/// The corpus is only an oracle for what it covers. This pins the coverage so a
/// future tape that exercises a new endpoint is noticed, and so the `/find` gap
/// is a stated fact rather than an omission.
#[test]
fn the_corpus_covers_four_of_the_five_endpoints() {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();

    for (_, raw) in recorded_tmdb_requests() {
        let parsed: RecordedRequest = serde_json::from_str(raw.get()).expect("decodes");
        let endpoint = match parsed.path.as_str() {
            "/search/movie" => "/search/movie",
            "/search/tv" => "/search/tv",
            path if path.starts_with("/movie/") => "/movie/{id}",
            path if path.starts_with("/tv/") => "/tv/{series_id}",
            path if path.starts_with("/find/") => "/find/{external_id}",
            path => panic!("unknown endpoint {path}"),
        };
        *counts.entry(endpoint).or_default() += 1;
    }

    assert_eq!(
        counts,
        BTreeMap::from([
            ("/search/movie", 25),
            ("/search/tv", 19),
            ("/movie/{id}", 2),
            ("/tv/{series_id}", 2),
        ]),
        "corpus coverage changed; /find is expected to be absent"
    );
}
