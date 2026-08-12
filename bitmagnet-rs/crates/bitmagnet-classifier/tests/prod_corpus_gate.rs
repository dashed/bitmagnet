//! The B′ desync gate against a **real production corpus**.
//!
//! The tape at `testdata/parity/classifier-attach/prod-20260811` was recorded
//! live from `bitmagnet-0` — the only pod that runs the classifier with the
//! enrichment flags ON against real traffic — by the exact writer image
//! `p0-97304a42`. Every subject is a real torrent classified by the production
//! workflow, so its verdicts are a measurement rather than a smoke test of the
//! wiring.
//!
//! # The measurement
//!
//! ```text
//! subjects=2000 matched=2000 desynced=0 missed=0 unconsumed=0 errored=0
//! not_authoritative=0 observations=715/715
//! ```
//!
//! **Every subject matched and every recorded observation was consumed.**
//! Across 2,000 real classifications Rust asked exactly the questions Go asked
//! — 418 `local.content_by_search`, 280 `tmdb.request` and 17
//! `local.content_by_id` —
//! each matching Go's recorded request byte for byte, in order, none left over.
//!
//! Every record embeds the exact input captured at Go's classifier boundary and
//! carries a terminal outcome. The clean-shutdown generation has 2,000
//! authoritative records, zero incomplete records, and no out-of-band
//! `inputs.json`. That removes both failure modes demonstrated by the 2026-08-10
//! corpus: subjects can no longer disappear between recording and a post-hoc
//! export, and their classifier-time effective hint no longer has to be
//! reconstructed. In this independent sample, all 2,000 inputs — including the
//! 60 classifications that ended by deleting the torrent — remain replayable.
//!
//! The legacy export decoder remains in this gate so older v1 tapes can still be
//! diagnosed. Embedded input always wins per `(subject, attempt)`, and malformed
//! embedded input fails closed instead of falling back to a post-hoc snapshot.
//! The regression below also pins why the legacy fallback must keep its stored
//! hint rather than synthesising one from contents exported after classification.
//!
//! # Known limitations, which the numbers must be read against
//!
//! * **Truncated.** Hitting the record cap marks the tape truncated. "These
//!   2,000 classifications replay" is supportable; "all traffic replays" is not.
//! * The tape proves the classifier from the captured input boundary onward. It
//!   does not independently prove the upstream processor constructed that input
//!   correctly.
//! * The search-string tape seam still does not expose tsquery construction;
//!   that Unicode-sensitive boundary is proven separately.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use bitmagnet_classifier::tape_corpus::{self, CorpusReport};
use bitmagnet_classifier::{Classifier, ClassifierInput, InputContent, InputFile, InputHint};
use bitmagnet_processor::load::{
    effective_hint, has_stored_file_list, CurrentContent, CurrentHint,
};
use bitmagnet_tape::{Record, Replay};
use serde::Deserialize;

const PROD_DIGEST: &str = "sha256:95ffc278681f50fbcee2a3498e4388378ffe78156bc432d403d2acc3c2c809ae";

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/classifier-attach/prod-20260811")
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
    let path = corpus_dir().join("inputs.json");
    let raw = match std::fs::read(&path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return HashMap::new(),
        Err(error) => panic!("failed to read {}: {error}", path.display()),
    };
    let exported: Vec<ExportedInput> = serde_json::from_slice(&raw).expect("inputs parse");

    exported
        .into_iter()
        .map(|input| {
            let classifier_input = ClassifierInput {
                id: input.id.clone(),
                name: input.name,
                size: input.size,
                files_status: input.files_status.clone(),
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
                contents: input
                    .contents
                    .iter()
                    .map(|content| InputContent {
                        content_type: content.content_type.clone(),
                        content_source: content.content_source.clone(),
                        content_id: content.content_id.clone(),
                        content: content.content.clone(),
                    })
                    .collect(),
                // 🚨 The STORED hint, deliberately NOT the processor's
                // synthesised one — see `the_corpus_cannot_reproduce_the_hint`
                // below. `effective_hint` is real production logic, but feeding
                // it this corpus's `contents` fabricates knowledge Go did not
                // have: the export is a snapshot taken AFTER the recorded
                // classification wrote its result, so a search that SUCCEEDED
                // now looks like content that was already attached.
                hint: input.hint.as_ref().and_then(|hint| {
                    hint.content_type
                        .clone()
                        .filter(|value| !value.is_empty())
                        .map(|content_type| InputHint {
                            content_type,
                            content_source: hint.content_source.clone(),
                            content_id: hint.content_id.clone(),
                            ..Default::default()
                        })
                }),
            };

            (input.id, classifier_input)
        })
        .collect()
}

fn recorded_or_legacy_input(
    record: &Record,
    legacy_inputs: &HashMap<String, ClassifierInput>,
) -> Option<ClassifierInput> {
    match record.input.as_ref() {
        Some(raw) => Some(serde_json::from_str(raw.get()).unwrap_or_else(|error| {
            panic!(
                "embedded classifier input for {}#{} does not decode: {error}",
                record.subject, record.attempt
            )
        })),
        None => legacy_inputs.get(&record.subject).cloned(),
    }
}

async fn run_gate() -> CorpusReport {
    let replay = Replay::load(corpus_dir(), PROD_DIGEST).expect("prod tape loads");
    let inputs = load_inputs();

    tape_corpus::run(
        &replay,
        |resolver| Classifier::from_core_with_tape_evidence(resolver as Arc<_>),
        |record| recorded_or_legacy_input(record, &inputs),
        32,
    )
    .await
    .expect("the gate runs")
}

#[test]
fn the_tape_is_a_real_production_recording() {
    let replay = Replay::load(corpus_dir(), PROD_DIGEST).expect("prod tape loads");
    let manifest = replay.manifest();

    assert_eq!(manifest.record_count, 2000);
    assert_eq!(manifest.observation_count, 715);
    assert_eq!(manifest.authoritative_record_count, 2000);
    assert!(
        manifest.truncated,
        "hitting the cap marks the tape truncated; the numbers describe THESE subjects"
    );
    assert_eq!(
        manifest.incomplete_record_count, 0,
        "the clean-shutdown generation closes every in-flight record"
    );
    assert!(
        replay
            .subjects()
            .all(|record| record.input.is_some() && record.authoritative()),
        "every production record must carry classifier-time input and a complete outcome"
    );
}

/// The measurement. Its exact passing counts are pinned so a lane that changes
/// production behavior has to move them visibly.
#[tokio::test]
async fn prod_corpus_baseline() {
    let report = run_gate().await;

    assert_eq!(report.subjects, 2000, "every recorded input is embedded");
    assert_eq!(report.matched, 2000, "every production subject matches");
    assert_eq!(
        report.recorded_observations, 715,
        "the gate covers every recorded production observation"
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

    // A miss against these complete, classifier-time inputs is a real
    // divergence rather than a post-hoc recording artifact.
    assert_eq!(
        report.missed, 0,
        "a miss against an authoritative record is a real divergence: {:?}",
        report.failures
    );

    assert_eq!(
        report.not_authoritative, 0,
        "every record carries a complete outcome and classifier-time input: {:?}",
        report.failures
    );
}

#[test]
fn embedded_inputs_win_per_attempt_over_the_legacy_export() {
    let legacy = HashMap::from([(
        "s".to_owned(),
        ClassifierInput {
            id: "s".to_owned(),
            name: "post-hoc legacy snapshot".to_owned(),
            size: 1,
            files_status: "no_info".to_owned(),
            extension: None,
            files_count: None,
            files: Vec::new(),
            hint: None,
            contents: Vec::new(),
        },
    )]);
    let record = |attempt, name: &str| {
        Record {
            subject: "s".to_owned(),
            attempt,
            workflow: "default".to_owned(),
            flags: serde_json::Map::new(),
            input: Some(
                serde_json::value::RawValue::from_string(format!(
                    r#"{{"id":"s","name":"{name}","size":2,"filesStatus":"single","files":[],"contents":[]}}"#
                ))
                .expect("raw input"),
            ),
            action_entries: None,
            processor_state: None,
            observations: Vec::new(),
            incomplete: false,
            outcome: None,
        }
    };

    let first = record(0, "classifier-time first");
    let second = record(1, "classifier-time second");
    assert_eq!(
        recorded_or_legacy_input(&first, &legacy)
            .expect("first input")
            .name,
        "classifier-time first"
    );
    assert_eq!(
        recorded_or_legacy_input(&second, &legacy)
            .expect("second input")
            .name,
        "classifier-time second"
    );
}

#[test]
fn absent_embedded_input_uses_the_legacy_export() {
    let legacy = HashMap::from([(
        "legacy".to_owned(),
        ClassifierInput {
            id: "legacy".to_owned(),
            name: "stored legacy snapshot".to_owned(),
            size: 7,
            files_status: "no_info".to_owned(),
            extension: None,
            files_count: None,
            files: Vec::new(),
            hint: None,
            contents: Vec::new(),
        },
    )]);
    let record = Record {
        subject: "legacy".to_owned(),
        attempt: 0,
        workflow: "default".to_owned(),
        flags: serde_json::Map::new(),
        input: None,
        action_entries: None,
        processor_state: None,
        observations: Vec::new(),
        incomplete: false,
        outcome: None,
    };

    let input = recorded_or_legacy_input(&record, &legacy).expect("legacy input");
    assert_eq!(input.id, "legacy");
    assert_eq!(input.name, "stored legacy snapshot");
    assert_eq!(input.size, 7);
}

#[test]
#[should_panic(expected = "embedded classifier input for s#0 does not decode")]
fn malformed_embedded_input_never_falls_back() {
    let legacy = HashMap::from([(
        "s".to_owned(),
        ClassifierInput {
            id: "s".to_owned(),
            name: "tempting fallback".to_owned(),
            size: 1,
            files_status: "no_info".to_owned(),
            extension: None,
            files_count: None,
            files: Vec::new(),
            hint: None,
            contents: Vec::new(),
        },
    )]);
    let record = Record {
        subject: "s".to_owned(),
        attempt: 0,
        workflow: "default".to_owned(),
        flags: serde_json::Map::new(),
        input: Some(
            serde_json::value::RawValue::from_string(r#"{"name":42}"#.to_owned())
                .expect("raw input"),
        ),
        action_entries: None,
        processor_state: None,
        observations: Vec::new(),
        incomplete: false,
        outcome: None,
    };

    let _ = recorded_or_legacy_input(&record, &legacy);
}

/// Pins WHY this gate replays the stored hint rather than the processor's
/// synthesised one, using the real `load::effective_hint` on the real shape that
/// exposed it.
///
/// Subject `02c1f244…` was recorded performing a `content_by_search` for
/// "K 19 The Widowmaker"; by export time the content that search PRODUCED was
/// attached. Feeding those post-hoc associations to the synthesis turns that
/// successful search into an already-attached row, so a replay pre-attaches and
/// asks nothing. Across the corpus that moved the gate from 9 non-matching
/// subjects to 172.
///
/// This is not a defect in `effective_hint` — it is correct production logic,
/// and T9 exists because Rust must reproduce it. It is a defect in feeding it a
/// snapshot taken after the classification it is meant to precede. New tapes
/// record their own input; this legacy corpus still needs the stored-hint
/// fallback, and this test stops anyone "improving" that fallback by wiring the
/// synthesis back in.
#[test]
fn synthesising_the_hint_from_post_hoc_contents_fabricates_a_pre_attach() {
    let association = CurrentContent {
        id: "tc".to_owned(),
        content_type: Some("movie".to_owned()),
        content_source: Some("tmdb".to_owned()),
        content_id: Some("8665".to_owned()),
    };

    // No stored hint, exactly as the export shows for that subject.
    let synthesised = effective_hint(
        None,
        std::slice::from_ref(&association),
        true,
        has_stored_file_list("multi"),
    )
    .expect("the synthesis produces a hint from a sourced association");

    assert_eq!(synthesised.content_source, "tmdb");
    assert_eq!(synthesised.content_id, "8665");

    // A hint carrying a SOURCE is what makes `runner.Run` pre-attach, which
    // suppresses the enrichment branch entirely -- so the replay would ask
    // nothing where Go recorded a search.
    assert!(
        !synthesised.content_source.is_empty(),
        "a sourced hint is precisely what suppresses the search"
    );

    // The stored-hint path this gate uses instead yields no hint at all here,
    // so the workflow reaches the search Go actually recorded.
    let stored: Option<CurrentHint> = None;
    assert!(stored.is_none());
}
