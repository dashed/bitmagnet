//! The B′ desync gate, run against the Go-recorded golden tape.
//!
//! 🚨 This gate does NOT fully pass, and the assertions pin the current baseline
//! rather than a pass — so a lane that changes behaviour moves the numbers
//! visibly instead of quietly satisfying a test written around a stub. It has
//! now earned its keep twice: implementing the local attach actions turned these
//! assertions red (0/5 → 3/5 observations consumed), and wiring TMDB replay
//! turned them red again (3/5 → 5/5). The one remaining non-match is a defect in
//! the FIXTURE, not the port — see [`baseline`].
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
        contents: Vec::new(),
        // The hint supplies the content type the attach actions guard on.
        // Without it the classification never reaches them and the gate would
        // measure nothing — which is exactly the trap this test fell into first.
        hint: Some(InputHint {
            content_type: "movie".to_owned(),
            content_source: String::new(),
            content_id: String::new(),
            ..Default::default()
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

/// The current baseline, with all four attach actions — local AND TMDB —
/// implemented.
///
/// Each verdict here is understood, not merely observed:
///
/// * `tied-window` — **match**. Its one recorded observation is a local search,
///   Rust asks exactly it, and the record is fully consumed.
/// * `no-observations` — **match**, trivially: nothing recorded, nothing asked.
/// * `empty-then-tmdb` — **match**, and this is the one that moved when TMDB
///   replay landed. Its local search returns empty, so the workflow falls through
///   to TMDB and now makes both recorded calls (search, then details-by-id)
///   instead of stopping. It was `Unconsumed { remaining: 2 }` before.
/// * `tmdb-failure` — **desync, and NOT a port bug.** That fixture's record holds
///   only a `tmdb.request`, because it was recorded by exercising the TMDB seam
///   directly rather than by running the whole workflow. Its flags omit
///   `local_search_enabled`, which `core.yml` DEFAULTS TO TRUE, so a full
///   workflow legitimately attempts a local search first and finds a
///   `tmdb.request` at that position. The gate is correctly reporting that this
///   fixture is not a workflow recording — wiring TMDB does not and should not
///   change that.
#[tokio::test]
async fn baseline() {
    let report = run_gate().await;

    assert_eq!(
        report.consumed_observations, 5,
        "every recorded observation is now asked for"
    );
    assert_eq!(
        report.matched, 3,
        "tied-window, no-observations, and now empty-then-tmdb"
    );
    assert_eq!(
        report.unconsumed, 0,
        "TMDB replay closed the only under-consuming subject"
    );
    assert_eq!(report.desynced, 1, "tmdb-failure — see this test's docs");
    assert_eq!(report.errored, 0);

    assert!(
        !report.passed(),
        "still not a pass: the tmdb-failure fixture is not a workflow recording"
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

/// The multi-seam subject: a local search that finds nothing, then the TMDB
/// fallback. It exercises the full chain in one classification — empty local
/// answer, TMDB search, then details-by-id keyed on the search's winner — so it
/// is the fixture that proves the seams compose rather than merely each working
/// alone.
#[tokio::test]
async fn the_local_then_tmdb_fallback_chain_is_fully_replayed() {
    let report = run_gate().await;

    assert!(
        !report
            .failures
            .iter()
            .any(|failure| failure.subject == "empty-then-tmdb"),
        "empty-then-tmdb must now consume all three observations: {:?}",
        report.failures
    );
}

/// The per-subject detail still has to name what diverged, or the report is not
/// actionable when the numbers move. `tmdb-failure` is the standing example.
#[tokio::test]
async fn failures_report_what_diverged() {
    let report = run_gate().await;

    let tmdb_failure = report
        .failures
        .iter()
        .find(|failure| failure.subject == "tmdb-failure")
        .expect("the non-workflow fixture is reported");

    assert_eq!(tmdb_failure.recorded, 1);
    assert!(
        matches!(tmdb_failure.verdict, Verdict::Desync { .. }),
        "it should say the question differed, not merely that something failed: {:?}",
        tmdb_failure.verdict
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
