//! The B′ corpus harness: run the classifier over every subject in a tape and
//! report, per subject, whether Rust asked the questions Go asked.
//!
//! # What this gate actually proves
//!
//! It is a **desync gate**, not a result-diff. The tape records the observations
//! a classification made against its impure dependencies; it does not record the
//! classification's final verdict. So what this harness can establish — and it is
//! the thing worth establishing first — is that Rust's control flow through the
//! enrichment path is identical to Go's: the same lookups, with the same
//! arguments, in the same order, and no more or fewer of them.
//!
//! That is a strong property. Go's flags-ON path is a sequence of decisions
//! (search locally, then fall back to TMDB, then fetch details) where each step's
//! arguments depend on the previous step's answer. A port that reproduces the
//! whole sequence byte for byte has reproduced the decision logic, whatever it
//! then does with the result.
//!
//! # Where this stands
//!
//! All four `attach_*` actions are implemented against both seams, and against
//! the production corpus the gate reports **284 subjects, 0 desyncs, 120/120
//! observations consumed** — Rust asks exactly Go's questions, local and TMDB
//! alike, in order, with none left over.
//!
//! The two verdicts that are NOT `Match` there are
//! [`Verdict::Miss`](Verdict::Miss)es on a base-title divergence unrelated to
//! either seam. See `tests/prod_corpus_gate.rs`, which documents them.
//!
//! Its assertions deliberately pin the current numbers rather than asserting a
//! pass, so a lane that changes behaviour has to move them visibly. That has
//! caught the right thing twice: implementing the local attach actions, and
//! wiring TMDB replay, each turned the assertions red.

use std::collections::BTreeMap;
use std::sync::Arc;

use bitmagnet_tape::Replay;
use serde::Serialize;

use crate::resolver::tape::TapeContentResolver;
use crate::{Classifier, ClassifierInput, FlagValue, Flags};

/// What happened when one subject was replayed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "verdict")]
pub enum Verdict {
    /// Rust asked exactly the recorded questions, in order, and consumed them
    /// all. The desync gate passes for this subject.
    Match,
    /// Rust asked a question the recording does not have at that position — the
    /// highest-signal failure, because it means the port's decision logic
    /// diverged rather than merely stopping early.
    Desync { detail: String },
    /// Rust asked MORE questions than were recorded. Distinct from a desync: the
    /// prefix agreed and then Rust kept going.
    Miss { detail: String },
    /// Rust asked FEWER questions than Go did. The mirror image of a miss, and
    /// just as much a divergence — it means Rust skipped work Go performed.
    Unconsumed { remaining: usize },
    /// The classification failed for a reason that is not a tape disagreement,
    /// e.g. TMDB replay being unwired.
    Error { detail: String },
}

/// One subject's outcome.
#[derive(Debug, Clone, Serialize)]
pub struct SubjectReport {
    pub subject: String,
    pub attempt: i64,
    /// Observations the recording holds for this subject.
    pub recorded: usize,
    /// Observations the Rust run actually consumed.
    pub consumed: usize,
    #[serde(flatten)]
    pub verdict: Verdict,
}

/// The aggregate gate result.
#[derive(Debug, Clone, Serialize)]
pub struct CorpusReport {
    /// Restated in the artifact so a reader of the JSON alone cannot mistake a
    /// desync gate for a result diff.
    pub scope: &'static str,
    pub subjects: usize,
    pub matched: usize,
    pub desynced: usize,
    pub missed: usize,
    pub unconsumed: usize,
    pub errored: usize,
    pub recorded_observations: usize,
    pub consumed_observations: usize,
    /// Per-verdict counts, for a one-line summary.
    pub by_verdict: BTreeMap<String, usize>,
    /// Bounded sample of the non-matching subjects, so a large corpus still
    /// yields a readable artifact.
    pub failures: Vec<SubjectReport>,
}

const SCOPE_NOTE: &str = "Desync gate, NOT a result diff: the tape records the observations a \
classification made against its impure dependencies, not its final verdict. A Match means Rust \
asked exactly the questions Go asked, with the same arguments, in the same order, and consumed \
them all. It does NOT mean the two classifiers produced the same content attachment. Separately: \
the tape records the SEARCH STRING handed to the query builder, not the tsquery it compiles \
into, so implementations that agree on the string and disagree on the tsquery match here \
regardless.";

impl CorpusReport {
    /// The gate passes only when every subject matched.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.subjects > 0 && self.matched == self.subjects
    }
}

/// Runs the classifier over every replayable subject in `replay`.
///
/// `classifier_for` is handed the resolver for one subject and returns the
/// classifier to run with it — a closure rather than a prebuilt [`Classifier`],
/// because each subject needs its own resolver and the resolver is baked in at
/// construction.
///
/// `input_for` maps a subject id to the input to classify. Returning `None`
/// skips the subject, which is what a caller does when its corpus does not carry
/// that subject's torrent.
///
/// Each subject runs under **the flags the recording was made with**, taken from
/// the tape record rather than assumed: replaying a flags-ON recording under
/// flags-off would consult nothing and report a vacuous pass.
///
/// # Errors
///
/// Returns an error only if a classifier cannot be constructed. Per-subject
/// failures become verdicts — the point of a gate is to count them, not to stop
/// at the first.
pub async fn run<F, I>(
    replay: &Replay,
    mut classifier_for: F,
    mut input_for: I,
    failure_sample: usize,
) -> Result<CorpusReport, crate::ClassifierError>
where
    F: FnMut(Arc<TapeContentResolver>) -> Result<Classifier, crate::ClassifierError>,
    I: FnMut(&str) -> Option<ClassifierInput>,
{
    // Deterministic order so two runs of the same corpus produce comparable
    // artifacts; the tape's own index is a hash map.
    let mut subjects: Vec<_> = replay
        .subjects()
        .map(|record| {
            (
                record.subject.clone(),
                record.attempt,
                record.workflow.clone(),
                flags_from_record(&record.flags),
                record.observations.len(),
            )
        })
        .collect();
    subjects.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));

    let mut reports = Vec::with_capacity(subjects.len());

    for (subject, attempt, workflow, flags, recorded) in subjects {
        let Some(input) = input_for(&subject) else {
            continue;
        };

        let resolver = Arc::new(TapeContentResolver::new(replay, &subject, attempt));
        let classifier = classifier_for(Arc::clone(&resolver))?;

        let outcome = classifier.run(&workflow, &flags, &input).await;

        let remaining = resolver.remaining();
        let consumed = recorded.saturating_sub(remaining);
        let verdict = verdict_for(&outcome, remaining);

        reports.push(SubjectReport {
            subject,
            attempt,
            recorded,
            consumed,
            verdict,
        });
    }

    Ok(summarise(reports, failure_sample))
}

/// The flags the recording ran under, as the classifier wants them.
///
/// Only booleans are carried: every flag the enrichment path keys on
/// (`local_search_enabled`, `apis_enabled`, `tmdb_enabled`) is a boolean, and
/// silently coercing anything else would fabricate a flag state the recording
/// never had.
fn flags_from_record(recorded: &serde_json::Map<String, serde_json::Value>) -> Flags {
    recorded
        .iter()
        .filter_map(|(name, value)| {
            value
                .as_bool()
                .map(|flag| (name.clone(), FlagValue::Bool(flag)))
        })
        .collect()
}

/// Derives a verdict from the classification outcome and what the tape has left.
///
/// `Classifier::run` does not return a `Result` — it encodes failure as an
/// `error` outcome inside the JSON, mirroring Go — so the tape disagreements
/// have to be recognised from that payload.
fn verdict_for(outcome: &crate::Json, remaining: usize) -> Verdict {
    if let Some(detail) = error_outcome(outcome) {
        return classify_error(&detail);
    }

    if remaining > 0 {
        // Nothing failed, but Rust asked fewer questions than Go did. That is a
        // divergence in its own right, and with the `attach_*` actions stubbed
        // it is the expected verdict for every subject today.
        return Verdict::Unconsumed { remaining };
    }

    Verdict::Match
}

/// Pulls the message out of an `error` outcome, if that is what this is.
fn error_outcome(outcome: &crate::Json) -> Option<String> {
    let value: serde_json::Value = serde_json::to_value(outcome).ok()?;

    match value.get("outcome")? {
        serde_json::Value::String(text) if text == "error" => Some(
            value
                .get("error")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("error outcome without a message")
                .to_owned(),
        ),
        serde_json::Value::Object(map) => map
            .get("error")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        _ => None,
    }
}

/// Sorts a classification failure into the verdict taxonomy.
///
/// The distinctions matter: a desync says the port asked the wrong question, a
/// miss says it asked too many, and anything else is a plain error that should
/// not be counted as evidence about parity either way.
fn classify_error(detail: &str) -> Verdict {
    let detail = detail.to_owned();

    if detail.contains("desync") {
        Verdict::Desync { detail }
    } else if detail.contains("no observation recorded") || detail.contains("no recorded response")
    {
        Verdict::Miss { detail }
    } else {
        Verdict::Error { detail }
    }
}

fn summarise(reports: Vec<SubjectReport>, failure_sample: usize) -> CorpusReport {
    let mut by_verdict: BTreeMap<String, usize> = BTreeMap::new();
    let (mut matched, mut desynced, mut missed, mut unconsumed, mut errored) = (0, 0, 0, 0, 0);
    let (mut recorded_observations, mut consumed_observations) = (0, 0);

    for report in &reports {
        recorded_observations += report.recorded;
        consumed_observations += report.consumed;

        let key = match &report.verdict {
            Verdict::Match => {
                matched += 1;
                "match"
            }
            Verdict::Desync { .. } => {
                desynced += 1;
                "desync"
            }
            Verdict::Miss { .. } => {
                missed += 1;
                "miss"
            }
            Verdict::Unconsumed { .. } => {
                unconsumed += 1;
                "unconsumed"
            }
            Verdict::Error { .. } => {
                errored += 1;
                "error"
            }
        };

        *by_verdict.entry(key.to_owned()).or_default() += 1;
    }

    let failures = reports
        .iter()
        .filter(|report| report.verdict != Verdict::Match)
        .take(failure_sample)
        .cloned()
        .collect();

    CorpusReport {
        scope: SCOPE_NOTE,
        subjects: reports.len(),
        matched,
        desynced,
        missed,
        unconsumed,
        errored,
        recorded_observations,
        consumed_observations,
        by_verdict,
        failures,
    }
}
