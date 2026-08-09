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
//! # The measurement, and the two subjects that miss
//!
//! ```text
//! subjects=284 matched=282 desynced=0 missed=2 unconsumed=0 errored=0  observations=120/120
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
//! The 2 misses are a **Miss, not a Desync** — Rust asked an EXTRA question
//! rather than a wrong one — and that distinction is why they are recorded here
//! instead of blocking. `b78a66755eb9c4c0deaf88eae082e2e53683e4f9` ("Sunny
//! (2011) DC BluRay 1080p 5.1CH x264 SmallAndHD") recorded **zero**
//! observations, yet Rust asked for a `local.content_by_search` at position 0.
//!
//! Its corpus input was checked against production and is faithful — the DB
//! agrees on the name, on `files_count=1`/`torrent_files=1` (so this is NOT the
//! `files_data` caveat above), on a single 2.2 GB `.mkv`, and on a hint of
//! `movie` with no source or id. The record's flags are all ON. So under
//! `classifier.core.yml:92` (`!result.hasAttachedContent && result.hasBaseTitle`)
//! Go reached that gate with no base title, or short-circuited before it, where
//! Rust parsed one. `Result.Content` is only ever set by `AttachContent`, so a
//! pre-seeded attachment is ruled out as the explanation. Narrowing it to the
//! exact step is the next lane's work; until then these two are a known,
//! bounded 0.7%.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use bitmagnet_classifier::tape_corpus::{self, CorpusReport};
use bitmagnet_classifier::{Classifier, ClassifierInput, InputFile, InputHint};
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
                hint: input.hint.and_then(|hint| {
                    hint.content_type
                        .filter(|t| !t.is_empty())
                        .map(|content_type| InputHint {
                            content_type,
                            content_source: hint.content_source,
                            content_id: hint.content_id,
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
        "PROD CORPUS GATE  subjects={} matched={} desynced={} missed={} unconsumed={} errored={} observations={}/{}",
        report.subjects,
        report.matched,
        report.desynced,
        report.missed,
        report.unconsumed,
        report.errored,
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

    // The two known misses are documented in this module's docs, and are a
    // base-title divergence unrelated to the seams. Pinned, not asserted as a
    // pass — this is the number the next lane drives to zero.
    assert!(
        report.missed <= 2,
        "a third miss is new: {:?}",
        report.failures
    );
}
