//! The B′ oracle end to end: a [`ContentResolver`] answering from the tape Go
//! recorded.
//!
//! Everything here runs against the real artifact at
//! `testdata/parity/classifier-attach/example`, so a pass means Rust asked the
//! questions Go asked and read back what Go saw — including deserialising Go's
//! `model.Content` JSON, which is the part most likely to drift silently.

use std::path::PathBuf;

use bitmagnet_classifier::resolver::{tape::TapeContentResolver, ContentResolver, ResolveError};
use bitmagnet_model::ContentType;
use bitmagnet_tape::Replay;

const GOLDEN_DIGEST: &str =
    "sha256:95ffc278681f50fbcee2a3498e4388378ffe78156bc432d403d2acc3c2c809ae";

fn replay() -> Replay {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/classifier-attach/example");
    Replay::load(dir, GOLDEN_DIGEST).expect("golden tape loads")
}

/// The case the whole tape exists for: a candidate window whose rows all tie at
/// rank 1, where only the recorded ORDER is meaningful.
#[tokio::test]
async fn content_by_search_replays_the_tied_window_in_recorded_order() {
    let replay = replay();
    let resolver = TapeContentResolver::new(&replay, "tied-window", 0);

    let items = resolver
        .content_by_search(ContentType::Movie, "Cinderella", Some(1950))
        .await
        .expect("the tape answers");

    assert!(items.len() > 1, "the tied window needs multiple candidates");

    // Every candidate ties, which is exactly why re-running the query could not
    // reproduce this order and the tape must.
    for item in &items {
        assert!(
            (item.query_string_rank - 1.0).abs() < f64::EPSILON,
            "expected a tied rank, got {}",
            item.query_string_rank
        );
    }

    // Go's model.Content decoded into Rust's — the silent-drift risk.
    assert_eq!(items[0].content.source, "tmdb");
    assert_eq!(items[0].content.content_type, ContentType::Movie);
    assert!(
        !items[0].content.title.is_empty(),
        "content must round-trip with its title"
    );

    // The order is the payload: ids must come back in the recorded sequence.
    let ids: Vec<_> = items.iter().map(|i| i.content.id.as_str()).collect();
    assert_eq!(ids.first(), Some(&"1000"), "first recorded candidate wins");

    assert_eq!(resolver.remaining(), 0, "the record is fully consumed");
}

/// A recorded EMPTY candidate list is Go's `ErrUnmatched` — a real negative
/// answer, not a gap. It must not surface as an error.
#[tokio::test]
async fn an_empty_candidate_list_is_a_real_answer_not_a_miss() {
    let replay = replay();
    let resolver = TapeContentResolver::new(&replay, "empty-then-tmdb", 0);

    let items = resolver
        .content_by_search(ContentType::Movie, "Cinderella", Some(1950))
        .await
        .expect("an empty answer is still an answer");

    assert!(items.is_empty());
    // Two TMDB observations remain — the classification went on to consult TMDB.
    assert_eq!(resolver.remaining(), 2);
}

/// Asking about a subject the tape never recorded must miss, loudly, rather
/// than silently degrade to "nothing found".
#[tokio::test]
async fn an_unrecorded_subject_misses_rather_than_returning_empty() {
    let replay = replay();
    let resolver = TapeContentResolver::new(&replay, "never-recorded", 0);

    let err = resolver
        .content_by_search(ContentType::Movie, "Cinderella", Some(1950))
        .await
        .expect_err("must miss");

    assert!(
        matches!(err, ResolveError::TapeMiss(_)),
        "a gap must be a TapeMiss, not an empty result: {err}"
    );
}

/// The desync guarantee: asking a different question fails even though the
/// recorded answer would have decoded perfectly well.
#[tokio::test]
async fn a_different_question_desyncs() {
    let replay = replay();
    let resolver = TapeContentResolver::new(&replay, "tied-window", 0);

    let err = resolver
        .content_by_search(ContentType::Movie, "Cinderella", None) // recorded WITH year 1950
        .await
        .expect_err("dropping the year is a different question");

    let message = err.to_string();
    assert!(
        message.contains("desync"),
        "expected a desync, got: {message}"
    );
}

/// The year drives two fields at once — `year` and `releaseDateRange` — and Go
/// writes them together or not at all. This pins the derivation, since getting
/// the range wrong would desync every year-qualified search.
#[tokio::test]
async fn the_year_expands_to_gos_release_date_range() {
    let replay = replay();
    let resolver = TapeContentResolver::new(&replay, "tied-window", 0);

    // Succeeding at all proves the request matched byte for byte, including
    // releaseDateRange {1950-01-01, 1951-01-01}.
    resolver
        .content_by_search(ContentType::Movie, "Cinderella", Some(1950))
        .await
        .expect("year 1950 must expand to the recorded range");
}

/// TMDB is not wired, and says so specifically. It must NOT be reported as a
/// miss (which would read as a gap in the recording) or as an empty answer
/// (which would read as `ErrUnmatched`).
#[tokio::test]
async fn tmdb_methods_report_unsupported_rather_than_faking_an_answer() {
    let replay = replay();
    let resolver = TapeContentResolver::new(&replay, "empty-then-tmdb", 0);

    let err = resolver
        .tmdb_search_movie(&Default::default())
        .await
        .expect_err("tmdb replay is not wired");

    assert!(
        matches!(err, ResolveError::Unsupported(_)),
        "expected Unsupported, got: {err}"
    );
    assert!(
        err.to_string().contains("HTTP level"),
        "the error must explain WHY it is unwired: {err}"
    );
}
