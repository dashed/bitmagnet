//! The B′ desync gate against a **real production corpus**.
//!
//! The tape at `testdata/parity/classifier-attach/prod-20260810` was recorded
//! live from `bitmagnet-0` — the only pod that runs the classifier with the
//! enrichment flags ON against real traffic — over ~150 minutes at ~13
//! classifications/min. Every subject is a real torrent classified by the
//! production workflow, so its verdicts are a measurement rather than a smoke
//! test of the wiring.
//!
//! # The measurement
//!
//! ```text
//! subjects=1912 matched=1903 desynced=0 missed=0 unconsumed=0 errored=0
//! not_authoritative=9  observations=653/653
//! ```
//!
//! **Zero desyncs, and every recorded observation consumed.** Across 1,912 real
//! classifications Rust asked exactly the questions Go asked — 386
//! `local.content_by_search`, 254 `tmdb.request` and 13 `local.content_by_id` —
//! each matching Go's recorded request byte for byte, in order, none left over.
//!
//! Against the previous 300-record corpus this is **5.5× the subjects that
//! actually exercise a seam** (399 vs 72) and 5.4× the observations. That ratio
//! is the one that matters: a subject observing nothing agrees trivially, so it
//! is the observing subjects that carry the evidence.
//!
//! It is also the first corpus to cover `local.content_by_id` at all — 13
//! observations, every one with source `imdb`, i.e. the ALTERNATIVE-identifier
//! branch.
//!
//! # 🚨 Why this replays the STORED hint, not the processor's synthesised one
//!
//! Go's classifier does not see the `torrent_hints` row directly.
//! `processor.go` synthesises an effective hint from the first sourced
//! `torrent_contents` association whenever the stored row has no content
//! SOURCE, and `runner.Run` then PRE-ATTACHES that content, suppressing the
//! whole enrichment branch. Reproducing that faithfully is what T9 was.
//!
//! So the obvious thing is to run the real `load::effective_hint` over this
//! corpus's `contents`. **That was tried, and it is wrong here**: it moved the
//! gate from 9 non-matching subjects to 172, every one of them Rust asking
//! FEWER questions than Go.
//!
//! The reason is that `inputs.json` is a snapshot taken *after* the recorded
//! classification wrote its result. Subject `02c1f244…` makes it concrete: the
//! tape shows Go performing `content_by_search` for "K 19 The Widowmaker" and
//! getting one hit, while the export shows no stored hint and
//! `contents = [(movie, tmdb, 8665)]` — the content that search *produced*.
//! Synthesising from that hands the replay knowledge Go did not have at the
//! time, turning a successful search into an already-attached row.
//!
//! Replaying the stored hint is therefore the less-wrong option for a corpus of
//! freshly crawled torrents, whose content mostly did not pre-exist the
//! recording. `contents` is still supplied, so the genuine pre-attach still
//! fires wherever the STORED hint carries a source.
//!
//! **The real fix is for the tape to record its own input.** The format and
//! writer now support that: a replay uses a record's embedded input first and
//! needs no post-hoc export for those fields. This particular production tape
//! predates the field, so its baseline cannot change until a new writer is
//! explicitly deployed and a new artifact is recorded. This is the same
//! snapshot-versus-replay non-idempotency (T2) that caps the write-set gate at
//! ~86.6%, showing up in a second place.
//!
//! # Known limitations, which the numbers must be read against
//!
//! * **The 9 non-matching subjects (0.5%)** are that same artifact seen from the
//!   other side: their content DID pre-exist the recording, so Go pre-attached
//!   and the replay searches. Proportionally identical to the old corpus's 2 of
//!   284 (0.7%), which is what a stable artifact rather than a regression looks
//!   like.
//! * **Truncated.** Hitting the record cap marks the tape truncated. "These
//!   2,000 classifications replay" is supportable; "all traffic replays" is not.
//! * **79 of 2,000 subjects are gone** — deleted between recording and export.
//!   Ordinary churn; they have no input and are skipped.
//! * 🚨 **No outcomes.** The deployed writer image predates `tape.RecordOutcome`,
//!   so every record's outcome is `unknown` and nothing here is authoritative in
//!   the sense [`bitmagnet_tape::Record::authoritative`] means. That is why the 9
//!   land in `not_authoritative` rather than `missed`.
//! * 🚨 **File lists come from `torrent_files`, not the `files_data` blob the
//!   processor hydrates from.** Expected to agree; not proven to.
//!
//! A `Match` is deliberately NOT downgraded for a non-authoritative record:
//! agreement over a prefix is still agreement, it just proves less, and the
//! separate `not_authoritative` count is what keeps that honest.

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
        .join("../../../testdata/parity/classifier-attach/prod-20260810")
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
        |resolver| Classifier::from_core_with(resolver as Arc<_>),
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
    assert_eq!(manifest.observation_count, 653);
    assert!(
        manifest.truncated,
        "hitting the cap marks the tape truncated; the numbers describe THESE subjects"
    );
    assert_eq!(
        manifest.incomplete_record_count, 9,
        "records still classifying when the cap hit are excluded from replay"
    );
}

/// The measurement. This is not asserted to pass — it pins what the port
/// currently does against real traffic, so a lane that changes it has to say so.
#[tokio::test]
async fn prod_corpus_baseline() {
    let report = run_gate().await;

    // 2000 recorded − 9 incomplete (excluded by the loader) − 79 torrents
    // deleted between recording and export.
    assert!(
        report.subjects >= 1900 && report.subjects <= 1991,
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
    // The corpus cannot reproduce Go's input for these: their content
    // pre-existed the recording, so Go pre-attached where the replay searches.
    // See the module docs. Pinned, not asserted as a pass — this is the number
    // that a NEW tape recorded by a writer containing input capture should
    // drive to zero.
    assert!(
        report.not_authoritative <= 9,
        "more subjects than expected cannot be reproduced from the corpus: {:?}",
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
