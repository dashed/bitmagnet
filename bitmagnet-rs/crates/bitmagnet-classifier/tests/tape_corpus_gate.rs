//! The B′ desync gate, run against the Go-recorded golden tape.
//!
//! 🚨 This gate does NOT pass today, and that is the point. Rust's four
//! `attach_*` actions are stubs (`engine.rs` maps them all to
//! `Action::AttachUnmatched`), so the classifier never consults the resolver and
//! every subject that recorded observations reports `unconsumed`.
//!
//! These tests therefore pin the **baseline**, not a pass. When an enrichment
//! lane lands, `unconsumed` falls and `matched` rises, and the assertions here
//! are what will notice. A test that asserted `passed()` today would either fail
//! permanently or, worse, be written to accept the stub and then never notice
//! the real thing.

use std::path::PathBuf;
use std::sync::Arc;

use bitmagnet_classifier::tape_corpus::{self, Verdict};
use bitmagnet_classifier::{Classifier, ClassifierInput};
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
        name: format!("{subject} (2020)"),
        size: 1,
        // `no_info` keeps the file-list rules from firing, so the only thing the
        // gate can observe is the enrichment path it exists to measure.
        files_status: "no_info".to_owned(),
        extension: None,
        files_count: None,
        files: Vec::new(),
        hint: None,
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

/// The baseline: with `attach_*` stubbed, Rust consults nothing.
///
/// Every subject that recorded observations is `unconsumed`; the one that
/// recorded none matches trivially, because "asked nothing, and nothing was
/// recorded" is agreement.
#[tokio::test]
async fn baseline_is_unconsumed_because_attach_is_stubbed() {
    let report = run_gate().await;

    assert_eq!(
        report.consumed_observations, 0,
        "attach_* is stubbed, so the resolver is never consulted"
    );
    assert_eq!(
        report.desynced, 0,
        "nothing should DESYNC: Rust asks no questions, it does not ask wrong ones"
    );

    // `no-observations` recorded nothing, so asking nothing agrees with it.
    assert_eq!(
        report.matched, 1,
        "only the zero-observation subject matches"
    );
    assert_eq!(
        report.unconsumed, 3,
        "the three subjects with observations are all unconsumed"
    );

    assert!(
        !report.passed(),
        "the gate must NOT report a pass while the enrichment path is stubbed"
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
    assert_eq!(empty_then_tmdb.consumed, 0);
    assert_eq!(
        empty_then_tmdb.verdict,
        Verdict::Unconsumed { remaining: 3 },
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
