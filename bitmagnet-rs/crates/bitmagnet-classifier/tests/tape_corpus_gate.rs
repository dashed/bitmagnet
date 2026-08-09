//! The B′ desync gate, run against the Go-recorded golden tape.
//!
//! 🚨 This gate does NOT fully pass yet, and the assertions pin the current
//! baseline rather than a pass — so a lane that changes behaviour moves the
//! numbers visibly instead of quietly satisfying a test written around a stub.
//! (It already worked once: implementing the local attach actions turned these
//! assertions red, which is exactly what was wanted.)
//!
//! # These fixtures are a smoke test, not a parity measurement
//!
//! The golden's four subjects were built to exercise the TAPE — an empty answer,
//! a populated window, a recorded failure, an empty record — not to be a
//! classifier corpus. Running one workflow over all of them therefore produces
//! honest-but-uninteresting verdicts for some, notably `tmdb-failure` (see
//! [`baseline`]). A real measurement needs a corpus of real torrents recorded
//! through the full workflow. What these tests prove is that the harness,
//! resolver and attach actions are wired correctly end to end.

use std::path::PathBuf;
use std::sync::Arc;

use bitmagnet_classifier::tape_corpus::{self, Verdict};
use bitmagnet_classifier::{Classifier, ClassifierInput, InputHint};
use bitmagnet_tape::Replay;

const GOLDEN_DIGEST: &str =
    "sha256:95ffc278681f50fbcee2a3498e4388378ffe78156bc432d403d2acc3c2c809ae";

fn replay() -> Replay {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/classifier-attach/example");
    Replay::load(dir, GOLDEN_DIGEST).expect("golden tape loads")
}

/// The golden's subjects are synthetic fixtures, not real torrents, so the
/// corpus supplies a minimal input per subject. What the gate measures is which
/// dependency calls the classification makes, not what it decides.
fn input_for(subject: &str) -> Option<ClassifierInput> {
    Some(ClassifierInput {
        id: subject.to_owned(),
        // Parses to base title "Cinderella" + year 1950, which is exactly the
        // question the golden recorded. Anything else would desync on purpose.
        name: "Cinderella (1950)".to_owned(),
        size: 1,
        // `no_info` keeps the file-extension rules from firing, so the content
        // type comes from the hint and the only thing left to observe is the
        // enrichment path this gate exists to measure.
        files_status: "no_info".to_owned(),
        extension: None,
        files_count: None,
        files: Vec::new(),
        // The hint supplies the content type the attach actions guard on.
        // Without it the classification never reaches them and the gate would
        // measure nothing — which is exactly the trap this test fell into first.
        hint: Some(InputHint {
            content_type: "movie".to_owned(),
            content_source: String::new(),
            content_id: String::new(),
        }),
    })
}

async fn run_gate() -> tape_corpus::CorpusReport {
    let replay = replay();

    tape_corpus::run(
        &replay,
        |resolver| Classifier::from_core_with(resolver as Arc<_>),
        input_for,
        16,
    )
    .await
    .expect("the gate runs")
}

#[tokio::test]
async fn gate_runs_over_every_recorded_subject() {
    let report = run_gate().await;

    assert_eq!(report.subjects, 4, "the golden holds four subjects");
    assert_eq!(
        report.recorded_observations, 5,
        "and five observations across them"
    );
}

/// The current baseline, with the LOCAL attach actions implemented and TMDB not.
///
/// Each verdict here is understood, not merely observed:
///
/// * `tied-window` — **match**. Its one recorded observation is a local search,
///   Rust asks exactly it, and the record is fully consumed.
/// * `no-observations` — **match**, trivially: nothing recorded, nothing asked.
/// * `empty-then-tmdb` — **unconsumed 2 of 3**. The local search matches; the two
///   TMDB observations go unasked because TMDB replay is unwired. This is the
///   shortfall the gate exists to report, and it closes when TMDB lands.
/// * `tmdb-failure` — **desync, and NOT a port bug.** That fixture's record holds
///   only a `tmdb.request`, because it was recorded by exercising the TMDB seam
///   directly rather than by running the whole workflow. Its flags omit
///   `local_search_enabled`, which `core.yml` DEFAULTS TO TRUE, so a full
///   workflow legitimately attempts a local search first and finds a
///   `tmdb.request` at that position. The gate is correctly reporting that this
///   fixture is not a workflow recording.
#[tokio::test]
async fn baseline() {
    let report = run_gate().await;

    assert_eq!(
        report.consumed_observations, 3,
        "the local attach actions consult the resolver"
    );
    assert_eq!(report.matched, 2, "tied-window and no-observations match");
    assert_eq!(
        report.unconsumed, 1,
        "empty-then-tmdb still owes its TMDB calls"
    );
    assert_eq!(report.desynced, 1, "tmdb-failure — see this test's docs");
    assert_eq!(report.errored, 0);

    assert!(
        !report.passed(),
        "not a pass while TMDB is unwired and a fixture desyncs"
    );
}

/// The one that proves the whole chain: a subject whose recorded observation is
/// a local search is asked for byte-identically and fully consumed.
#[tokio::test]
async fn a_local_search_subject_matches_exactly() {
    let report = run_gate().await;

    assert!(
        !report
            .failures
            .iter()
            .any(|failure| failure.subject == "tied-window"),
        "tied-window must not appear among the failures: {:?}",
        report.failures
    );
}

/// The per-subject detail has to name what was skipped, or the report is not
/// actionable when the numbers start moving.
#[tokio::test]
async fn failures_report_how_much_was_skipped() {
    let report = run_gate().await;

    let empty_then_tmdb = report
        .failures
        .iter()
        .find(|failure| failure.subject == "empty-then-tmdb")
        .expect("the three-observation subject is reported");

    assert_eq!(empty_then_tmdb.recorded, 3);
    assert_eq!(
        empty_then_tmdb.consumed, 1,
        "the local search is asked; the two TMDB calls are not"
    );
    assert_eq!(
        empty_then_tmdb.verdict,
        Verdict::Unconsumed { remaining: 2 },
        "it should say exactly how many observations went unasked"
    );
}

/// The artifact must carry its own scope, so a reader of the JSON cannot mistake
/// a desync gate for a proof that the two classifiers agree on their verdicts.
#[tokio::test]
async fn the_report_states_what_it_does_not_prove() {
    let report = run_gate().await;
    let json = serde_json::to_string(&report).expect("report serialises");

    assert!(json.contains("NOT a result diff"));
    assert!(
        json.contains("SEARCH STRING"),
        "the tsquery scope limit must travel with the numbers"
    );
}

/// Determinism: a gate whose output depends on hash-map iteration order cannot
/// be diffed between runs, which is most of what a baseline is for.
#[tokio::test]
async fn the_report_is_deterministic() {
    let first = serde_json::to_string(&run_gate().await).expect("serialises");
    let second = serde_json::to_string(&run_gate().await).expect("serialises");

    assert_eq!(first, second, "two runs must produce identical artifacts");
}
