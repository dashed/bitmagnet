//! The B′ desync gate against a **real production corpus**.
//!
//! The tape at `testdata/parity/classifier-attach/prod-20260809` was recorded
//! live from `bitmagnet-0` — the only pod that runs the classifier with the
//! enrichment flags ON against real traffic — under
//! `CLASSIFIER_TAPE_DIR`/`CLASSIFIER_TAPE_MAX_RECORDS=300`. Unlike the golden's
//! four synthetic fixtures, every subject here is a real torrent classified by
//! the production workflow, so its verdicts are a genuine measurement rather
//! than a smoke test of the wiring.
//!
//! # Known limitations of this corpus, which the numbers must be read against
//!
//! * **Truncated.** Reaching the record cap marks the tape truncated. It is a
//!   complete oracle for *these* subjects, not for the population — "300
//!   classifications replay" is supportable, "all traffic replays" is not.
//! * **9 of 300 subjects are gone.** Those torrents were deleted from the
//!   database between recording and export, so they have no input and are
//!   skipped. Ordinary churn, not a defect.
//! * 🚨 **File lists come from `torrent_files`, not the `files_data` blob the
//!   processor actually hydrates from.** They are expected to agree, but that is
//!   an assumption, not a proof — so a desync on a file-driven content-type rule
//!   could in principle be an export artifact rather than a port bug. Anything
//!   this gate reports as a desync should be checked against that before it is
//!   called a divergence.
//!
//! # The measurement
//!
//! ```text
//! subjects=284 matched=282 desynced=0 missed=0 unconsumed=0 errored=0
//! not_authoritative=2  observations=120/120
//! ```
//!
//! **Zero desyncs, and every recorded observation consumed.** Across 284 real
//! classifications Rust asked exactly the questions Go asked — all 72
//! `local.content_by_search` *and* all 48 `tmdb.request` calls — each matching
//! Go's recorded request byte for byte, in the same order, with none left over.
//!
//! For the TMDB half that means Rust rebuilt Go's HTTP requests exactly: the same
//! paths, the same query-parameter sets, the same rendering of years and
//! `append_to_response`. Because each request's arguments depend on the previous
//! response — search, Levenshtein-pick a winner, then fetch that winner's details
//! by id — reproducing the whole chain reproduces the decision logic, not merely
//! the call shapes.
//!
//! ## The 2 non-matching subjects are a RECORDING artifact, not a port divergence
//!
//! Both recorded **zero** observations while Rust asked for a
//! `local.content_by_search` at position 0:
//!
//! * `b78a66755eb9…` — "Sunny (2011) DC BluRay 1080p 5.1CH x264 SmallAndHD"
//! * `f2b4c073a129…` — "Mr.D.S07E10.1080p.WEBRip.x264-aAF[rarbg]"
//!
//! This was checked by **running Go's own classifier on the exact corpus
//! input**, with a recording `LocalSearch` in place of the mock. Go asks
//! `ContentBySearch(movie, "Sunny", 2011)` and `ContentBySearch(tv_show,
//! "Mr D", 0)` — byte-for-byte the questions Rust asks. Go does so with or
//! without the hint applied. So on this input the two implementations agree
//! exactly, and the port is not what diverges.
//!
//! What is ruled out, and how:
//!
//! * *A base-title divergence* (the original hypothesis). Go's
//!   `ParseVideoContent` returns `BaseTitle` "Sunny" / "Mr D" for these names.
//! * *A file-list export artifact.* `files_count` and `torrent_files` agree with
//!   the corpus, and the names alone drive the title.
//! * *A pre-seeded attachment.* `runner.go` DOES pre-attach existing content
//!   before the workflow (`cl.AttachContent` when the hint has a content
//!   SOURCE and a matching `torrent_contents` row exists), which would make
//!   `!result.hasAttachedContent` false and skip the search — and injecting that
//!   pre-seed reproduces the zero-observation behaviour exactly. But both hint
//!   rows were created in **January 2025** and never updated, with a NULL
//!   source, so the pre-seed cannot have fired at recording time.
//!
//! 🚨 What that left was a property of the TAPE FORMAT: a record is marked
//! `incomplete` only if its session is still open when the tape is written
//! (`recorder.go` — `Incomplete` is membership in `r.open`). A classification
//! that *began and then finished without reaching the enrichment step* —
//! cancelled at SIGTERM, or ended by an error outcome — closes its session and
//! is written as a COMPLETE record with zero observations, indistinguishable
//! from "the workflow legitimately asked nothing".
//!
//! **That gap is now fixed**: `tape.RecordOutcome` records how each
//! classification ended, and [`bitmagnet_tape::Record::authoritative`] reports
//! whether its observation list can be read as a complete account. These two
//! subjects therefore land in `not_authoritative` rather than being counted as
//! divergences.
//!
//! 🚨 But THIS corpus predates the fix, so every record's outcome is `unknown`
//! and *nothing in it is authoritative*. The reclassification above is correct
//! but blunt: it applies to the whole tape, not just these two. Re-recording is
//! what turns the distinction into a real measurement.
//!
//! Corroboration that Rust's question is the RIGHT one: both torrents carry
//! attached content that is exactly what it finds — `tmdb/77117` "Sunny" (2011)
//! and `tmdb/42382` "Mr. D". Some other classification of the same torrent asked
//! Rust's question and attached the answer.
//!
//! ## 🚨 How much of this measurement is positive evidence
//!
//! Only **72 of the 284 subjects made any observation at all**; the other 212
//! recorded nothing and "match" by both sides asking nothing. Per the section
//! above, a zero-observation record on an outcome-less tape is weak evidence —
//! it can also mean the recorded classification ended early. So the load-bearing
//! part of this gate is the **72 subjects / 120 observations that actually
//! exercised a seam**, where every request matched byte for byte. The 212
//! trivial agreements should not be read as 212 independent confirmations.
//!
//! A `Match` is deliberately NOT downgraded for a non-authoritative record:
//! agreement over a prefix is still agreement, it just proves less. The separate
//! `not_authoritative` count is what keeps that honest.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use bitmagnet_classifier::tape_corpus::{self, CorpusReport};
use bitmagnet_classifier::{Classifier, ClassifierInput, InputContent, InputFile, InputHint};
use bitmagnet_tape::Replay;
use serde::Deserialize;

const PROD_DIGEST: &str = "sha256:95ffc278681f50fbcee2a3498e4388378ffe78156bc432d403d2acc3c2c809ae";

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/classifier-attach/prod-20260809")
}

/// One exported torrent, in the shape the SQL export writes.
#[derive(Debug, Deserialize)]
struct ExportedInput {
    id: String,
    name: String,
    #[serde(default)]
    size: u64,
    files_status: String,
    #[serde(default)]
    extension: Option<String>,
    #[serde(default)]
    files_count: Option<u32>,
    #[serde(default)]
    files: Vec<ExportedFile>,
    #[serde(default)]
    hint: Option<ExportedHint>,
    /// T9: existing `torrent_contents` associations, with their `content` row
    /// hydrated. Absent from a corpus exported before T9, which is why it
    /// defaults rather than being required.
    #[serde(default)]
    contents: Vec<ExportedContent>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExportedContent {
    #[serde(default)]
    content_type: String,
    #[serde(default)]
    content_source: String,
    #[serde(default)]
    content_id: String,
    #[serde(default)]
    content: Option<bitmagnet_model::Content>,
}

#[derive(Debug, Deserialize)]
struct ExportedFile {
    #[serde(default)]
    index: u32,
    path: String,
    #[serde(default)]
    extension: Option<String>,
    #[serde(default)]
    size: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExportedHint {
    content_type: Option<String>,
    #[serde(default)]
    content_source: String,
    #[serde(default)]
    content_id: String,
}

fn load_inputs() -> HashMap<String, ClassifierInput> {
    let raw = std::fs::read(corpus_dir().join("inputs.json")).expect("corpus inputs");
    let exported: Vec<ExportedInput> = serde_json::from_slice(&raw).expect("inputs parse");

    exported
        .into_iter()
        .map(|input| {
            let classifier_input = ClassifierInput {
                id: input.id.clone(),
                name: input.name,
                size: input.size,
                files_status: input.files_status,
                extension: input.extension,
                files_count: input.files_count,
                files: input
                    .files
                    .into_iter()
                    .map(|file| InputFile {
                        index: file.index,
                        path: file.path,
                        extension: file.extension.unwrap_or_default(),
                        size: file.size,
                    })
                    .collect(),
                // A hint row with no content type is not a hint: Go treats
                // `Hint.IsNil()` (an empty content type) as absent.
                contents: input
                    .contents
                    .into_iter()
                    .map(|content| InputContent {
                        content_type: content.content_type,
                        content_source: content.content_source,
                        content_id: content.content_id,
                        content: content.content,
                    })
                    .collect(),
                hint: input.hint.and_then(|hint| {
                    hint.content_type
                        .filter(|t| !t.is_empty())
                        .map(|content_type| InputHint {
                            content_type,
                            content_source: hint.content_source,
                            content_id: hint.content_id,
                            ..Default::default()
                        })
                }),
            };

            (input.id, classifier_input)
        })
        .collect()
}

async fn run_gate() -> CorpusReport {
    let replay = Replay::load(corpus_dir(), PROD_DIGEST).expect("prod tape loads");
    let inputs = load_inputs();

    tape_corpus::run(
        &replay,
        |resolver| Classifier::from_core_with(resolver as Arc<_>),
        |subject| inputs.get(subject).cloned(),
        32,
    )
    .await
    .expect("the gate runs")
}

#[test]
fn the_tape_is_a_real_production_recording() {
    let replay = Replay::load(corpus_dir(), PROD_DIGEST).expect("prod tape loads");
    let manifest = replay.manifest();

    assert_eq!(manifest.record_count, 300);
    assert_eq!(manifest.observation_count, 120);
    assert!(
        manifest.truncated,
        "hitting the cap marks the tape truncated; the numbers describe THESE subjects"
    );
    assert_eq!(
        manifest.incomplete_record_count, 7,
        "records still classifying when the cap hit are excluded from replay"
    );
}

/// The measurement. This is not asserted to pass — it pins what the port
/// currently does against real traffic, so a lane that changes it has to say so.
#[tokio::test]
async fn prod_corpus_baseline() {
    let report = run_gate().await;

    // 300 recorded − 7 incomplete (excluded by the loader) − 9 deleted torrents.
    assert!(
        report.subjects >= 280 && report.subjects <= 293,
        "unexpected subject count {}: {:?}",
        report.subjects,
        report.by_verdict
    );

    // Printed so the artifact is visible in CI output, not just asserted on.
    println!(
        "PROD CORPUS GATE  subjects={} matched={} desynced={} missed={} unconsumed={} \
errored={} not_authoritative={} observations={}/{}",
        report.subjects,
        report.matched,
        report.desynced,
        report.missed,
        report.unconsumed,
        report.errored,
        report.not_authoritative,
        report.consumed_observations,
        report.recorded_observations,
    );

    for failure in report
        .failures
        .iter()
        .filter(|f| {
            !matches!(
                f.verdict,
                bitmagnet_classifier::tape_corpus::Verdict::Unconsumed { .. }
            )
        })
        .take(8)
    {
        println!(
            "  {} recorded={} consumed={} {:?}",
            failure.subject, failure.recorded, failure.consumed, failure.verdict
        );
    }

    assert_eq!(
        report.errored, 0,
        "no subject should fail for a non-tape reason"
    );

    // The load-bearing claim. A desync means the port asked a question Go never
    // asked — the one failure mode that says the decision logic itself diverged,
    // as opposed to stopping early (unconsumed) or running on (miss). It is zero
    // against real traffic today and must stay zero.
    assert_eq!(
        report.desynced, 0,
        "Rust asked a question Go did not: {:?}",
        report.failures
    );

    // Now that TMDB replay is wired, nothing legitimately under-consumes: every
    // recorded observation must be asked for. Asking FEWER questions than Go is
    // as much a divergence as asking the wrong one.
    assert_eq!(
        report.unconsumed, 0,
        "Rust skipped work Go performed: {:?}",
        report.failures
    );
    assert_eq!(
        report.consumed_observations, report.recorded_observations,
        "every recorded observation should be consumed"
    );

    // No misses. The two that used to appear here were a RECORDING artifact --
    // running Go's own classifier on their corpus input produces exactly the
    // request Rust makes -- and the tape now carries an outcome, so a record
    // that cannot support a verdict is reported as such instead of being
    // counted as a divergence.
    assert_eq!(
        report.missed, 0,
        "a miss against an authoritative record is a real divergence: {:?}",
        report.failures
    );

    // 🚨 This corpus was recorded BEFORE outcomes existed, so every record's
    // ending is "unknown" and nothing in it is authoritative. That is why the
    // two subjects above land here rather than in `missed`, and it is the honest
    // reading: their observation lists cannot be shown to be complete. Re-record
    // to turn this into a real number.
    assert_eq!(
        report.not_authoritative, 2,
        "expected exactly the two subjects whose recordings are not oracles: {:?}",
        report.failures
    );
}
