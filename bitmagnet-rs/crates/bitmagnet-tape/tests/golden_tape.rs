//! Replay the Go-recorded golden tape at
//! `testdata/parity/classifier-attach/example`.
//!
//! This is the real artifact `go test ./internal/classifier -update-tape-example`
//! writes, not a Rust-authored fixture. A Rust-authored fixture would only prove
//! this crate is self-consistent; the point is that it agrees with Go.

use std::path::PathBuf;

use bitmagnet_tape::{Answer, Replay, TapeError};
use serde::Serialize;

/// The digest the golden tape was recorded under. It is also the digest the
/// live Go processor reports, which is why the tape is a usable oracle.
const GOLDEN_DIGEST: &str =
    "sha256:95ffc278681f50fbcee2a3498e4388378ffe78156bc432d403d2acc3c2c809ae";

fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/classifier-attach/example")
}

fn load() -> Replay {
    Replay::load(golden_dir(), GOLDEN_DIGEST).expect("golden tape loads")
}

/// Mirrors Go's `localContentBySearchRequest`. Field ORDER is load-bearing:
/// Go's `encoding/json` emits struct fields in declaration order and the
/// request is compared byte for byte, so reordering these silently desyncs.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalContentBySearchRequest {
    content_type: &'static str,
    base_title: &'static str,
    search_string: &'static str,
    year: Option<u16>,
    release_date_range: Option<DateRange>,
    order_by: &'static str,
    limit: i64,
}

#[derive(Serialize)]
struct DateRange {
    start: &'static str,
    end: &'static str,
}

/// Mirrors the TMDB recorder's `tapeRequest`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TmdbRequest {
    method: &'static str,
    path: &'static str,
    /// A `BTreeMap` because Go sorts map keys; a `HashMap` would emit them in an
    /// arbitrary order and fail the byte comparison non-deterministically.
    query_params: std::collections::BTreeMap<&'static str, &'static str>,
}

fn cinderella_search() -> LocalContentBySearchRequest {
    LocalContentBySearchRequest {
        content_type: "movie",
        base_title: "Cinderella",
        search_string: "\"Cinderella\"",
        year: Some(1950),
        release_date_range: Some(DateRange {
            start: "1950-01-01",
            end: "1951-01-01",
        }),
        order_by: "queryStringRank,identity",
        limit: 10,
    }
}

#[test]
fn manifest_matches_the_recording() {
    let replay = load();
    let manifest = replay.manifest();

    assert_eq!(manifest.schema, bitmagnet_tape::SCHEMA);
    assert_eq!(manifest.effective_config_digest, GOLDEN_DIGEST);
    assert_eq!(manifest.record_count, 4);
    assert_eq!(manifest.observation_count, 5);
    assert_eq!(manifest.incomplete_record_count, 0);
    assert!(!manifest.truncated);
}

#[test]
fn load_rejects_a_digest_mismatch() {
    // The whole point of pinning: a tape recorded under another configuration
    // describes a classifier that no longer exists.
    let err = Replay::load(golden_dir(), "sha256:deadbeef").expect_err("must fail closed");
    assert!(
        format!("{err}").contains("recorded under effective classifier config digest"),
        "unexpected error: {err}"
    );
}

#[test]
fn load_accepts_an_empty_digest_as_an_explicit_opt_out() {
    Replay::load(golden_dir(), "").expect("empty digest skips the pin");
}

/// The three-observation subject: a local search that returns nothing, then two
/// TMDB calls. Exercises ordered consumption across two different seams.
#[test]
fn replays_the_full_observation_sequence() {
    let replay = load();
    let mut session = replay.begin("empty-then-tmdb", 0);
    assert_eq!(session.remaining(), 3);

    let Answer::Response(response) = session
        .next("local.content_by_search", &cinderella_search())
        .expect("observation 0")
    else {
        panic!("expected a response");
    };
    // A recorded EMPTY result — a real answer, not a gap.
    assert_eq!(response.get(), r#"{"items":[]}"#);

    let mut params = std::collections::BTreeMap::new();
    params.insert("include_adult", "true");
    params.insert("query", "Cinderella");
    params.insert("year", "1950");

    let Answer::Response(search) = session
        .next(
            "tmdb.request",
            &TmdbRequest {
                method: "GET",
                path: "/search/movie",
                query_params: params,
            },
        )
        .expect("observation 1")
    else {
        panic!("expected a response");
    };
    assert!(search.get().contains("\"statusCode\":200"));

    let Answer::Response(detail) = session
        .next(
            "tmdb.request",
            &TmdbRequest {
                method: "GET",
                path: "/movie/11224",
                query_params: std::collections::BTreeMap::new(),
            },
        )
        .expect("observation 2")
    else {
        panic!("expected a response");
    };
    assert!(detail.get().contains("\"statusCode\":200"));

    assert_eq!(session.remaining(), 0);
}

/// A recorded failure replays as an `Answer::Failure`, NOT as an error: it is a
/// successful replay of an unsuccessful call. The caller rebuilds the
/// dependency's error from `kind`, which is what the classifier's control flow
/// keys on.
#[test]
fn replays_a_recorded_failure_as_an_answer() {
    let replay = load();
    let mut session = replay.begin("tmdb-failure", 0);

    let mut params = std::collections::BTreeMap::new();
    params.insert("include_adult", "true");
    params.insert("query", "Cinderella");

    let Answer::Failure(error) = session
        .next(
            "tmdb.request",
            &TmdbRequest {
                method: "GET",
                path: "/search/movie",
                query_params: params,
            },
        )
        .expect("a recorded failure is not a replay error")
    else {
        panic!("expected a recorded failure");
    };

    assert_eq!(error.kind, "unauthorized");
    assert_eq!(error.message, "TMDB request failed: 401 Unauthorized");
}

/// A record with an EMPTY observation list is a legitimate record of a
/// classification that consulted nothing. The first question still misses.
#[test]
fn a_record_with_no_observations_misses_immediately() {
    let replay = load();
    let mut session = replay.begin("no-observations", 0);
    assert_eq!(session.remaining(), 0);

    let err = session
        .next("local.content_by_search", &cinderella_search())
        .expect_err("must miss");

    assert!(
        matches!(err, TapeError::Miss { ref subject, sequence, .. }
                 if subject == "no-observations" && sequence == 0),
        "unexpected error: {err}"
    );
}

/// A subject absent from the tape still gets a session, so the first question
/// reports a miss naming it. Returning no session would silently fall back to
/// the live dependency — the exact failure this crate prevents.
#[test]
fn an_unknown_subject_misses_rather_than_falling_through() {
    let replay = load();
    let mut session = replay.begin("never-recorded", 0);

    let err = session
        .next("local.content_by_search", &cinderella_search())
        .expect_err("must miss");

    assert!(matches!(err, TapeError::Miss { ref subject, .. } if subject == "never-recorded"));
}

/// Asking the right kind with the wrong request desyncs. This is the failure the
/// request half of the tape exists to produce.
#[test]
fn a_changed_request_desyncs() {
    let replay = load();
    let mut session = replay.begin("tied-window", 0);

    let mut wrong = cinderella_search();
    wrong.search_string = "Cinderella"; // recorded as "\"Cinderella\"" — quoted

    let err = session
        .next("local.content_by_search", &wrong)
        .expect_err("must desync");

    match err {
        TapeError::Desync(detail) => assert_eq!(
            detail.want_kind, detail.got_kind,
            "kinds match; only the request differs"
        ),
        other => panic!("unexpected error: {other}"),
    }
}

/// Asking a different KIND desyncs too, and reports the kind mismatch shape.
#[test]
fn a_changed_kind_desyncs() {
    let replay = load();
    let mut session = replay.begin("tied-window", 0);

    let err = session
        .next(
            "tmdb.request",
            &TmdbRequest {
                method: "GET",
                path: "/search/movie",
                query_params: std::collections::BTreeMap::new(),
            },
        )
        .expect_err("must desync");

    assert!(
        format!("{err}").contains("recorded kind \"local.content_by_search\""),
        "unexpected error: {err}"
    );
}

/// The populated-response subject: the tied candidate window that motivates the
/// whole tape. Its recorded order is what makes replay deterministic.
#[test]
fn replays_the_tied_candidate_window_in_recorded_order() {
    let replay = load();
    let mut session = replay.begin("tied-window", 0);

    let Answer::Response(response) = session
        .next("local.content_by_search", &cinderella_search())
        .expect("observation 0")
    else {
        panic!("expected a response");
    };

    let value: serde_json::Value = serde_json::from_str(response.get()).expect("valid JSON");
    let items = value["items"].as_array().expect("items array");
    assert!(items.len() > 1, "the tied window needs multiple candidates");

    // Every candidate ties at rank 1 — which is exactly why re-running the query
    // could not reproduce this order and the tape must.
    for item in items {
        assert_eq!(item["queryStringRank"], "1");
    }
}

#[test]
fn subjects_excludes_nothing_in_a_complete_tape() {
    let replay = load();
    let mut subjects: Vec<_> = replay.subjects().map(|r| r.subject.clone()).collect();
    subjects.sort();

    assert_eq!(
        subjects,
        vec![
            "empty-then-tmdb",
            "no-observations",
            "tied-window",
            "tmdb-failure"
        ]
    );
}
