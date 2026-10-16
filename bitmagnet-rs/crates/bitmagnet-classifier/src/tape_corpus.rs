//! The B′ corpus harness: run the classifier over every subject in a tape and
//! report, per subject, whether Rust entered the attach actions and asked the
//! dependency questions Go did, in the same order.
//!
//! # What this gate actually proves
//!
//! It is a **desync gate**, not a result-diff. New tapes record both the attach
//! actions a classification entered and its observations against impure
//! dependencies; legacy tapes carry only the observations. The harness can
//! therefore establish that Rust's control flow through the enrichment path is
//! identical to Go's: the same action entries and lookups, with the same
//! arguments, in the same order, and no more or fewer of them. It still does not
//! compare the two classifiers' final materialized results.
//!
//! That is a strong property. Go's flags-ON path is a sequence of decisions
//! (search locally, then fall back to TMDB, then fetch details) where each step's
//! arguments depend on the previous step's answer. A port that reproduces the
//! whole sequence byte for byte has reproduced the decision logic, whatever it
//! then does with the result.
//!
//! # Where this stands
//!
//! All four `attach_*` actions are implemented against both seams. Against the
//! 2026-08-11 production corpus the gate reports **2,000/2,000 subjects matched,
//! 0 desyncs and 715/715 observations consumed** — Rust asks exactly Go's
//! questions, local and TMDB alike, in order, with none left over. Every record
//! carries classifier-time input and a complete outcome, so none of those
//! verdicts is downgraded as non-authoritative. See
//! `tests/prod_corpus_gate.rs`.

use std::collections::BTreeMap;
use std::sync::Arc;

use bitmagnet_tape::{ActionEntry, Record, Replay};
use serde::Serialize;

use crate::resolver::tape::TapeContentResolver;
use crate::{Classifier, ClassifierInput, ContentType, FlagValue, Flags};

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
    /// The RECORDING is not an oracle for this subject, so the disagreement it
    /// would otherwise report is not evidence.
    ///
    /// A record whose classification ended early — cancelled at shutdown, or
    /// stopped by an error — holds a prefix of what it would have asked. A
    /// replay that runs further is doing the right thing, and counting that as a
    /// miss fabricates a divergence. See `bitmagnet_tape::RecordOutcome`.
    NotAuthoritative {
        /// The recorded ending, or `"unknown"` for a tape without outcomes.
        outcome: String,
        detail: String,
    },
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
    /// Subjects whose recording is a prefix, so no verdict about them is
    /// evidence either way. Counted separately rather than folded into
    /// `matched` or `missed`: both would be a claim the tape cannot support.
    pub not_authoritative: usize,
    pub recorded_observations: usize,
    pub consumed_observations: usize,
    /// Per-verdict counts, for a one-line summary.
    pub by_verdict: BTreeMap<String, usize>,
    /// Bounded sample of the non-matching subjects, so a large corpus still
    /// yields a readable artifact.
    pub failures: Vec<SubjectReport>,
}

const SCOPE_NOTE: &str = "Desync gate, NOT a result diff: a traced tape records the attach actions \
entered plus the observations a classification made against its impure dependencies. A legacy \
tape's absent action trace is unknown and only its observations are compared. A Match means Rust \
entered every known action and asked exactly the questions Go did, in the same order, consuming \
all recorded evidence. It does NOT mean the two classifiers produced the same content attachment. Separately: \
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
/// `input_for` maps one exact record to the input to classify. It receives the
/// whole [`Record`], rather than only a subject id, because repeat attempts may
/// carry different classifier-time inputs. Returning `None` skips the record,
/// which is what a legacy caller does when its out-of-band corpus lacks it.
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
    I: FnMut(&Record) -> Option<ClassifierInput>,
{
    // Deterministic order so two runs of the same corpus produce comparable
    // artifacts; the tape's own index is a hash map.
    let mut records: Vec<_> = replay.subjects().collect();
    records.sort_by(|a, b| (&a.subject, a.attempt).cmp(&(&b.subject, b.attempt)));
    // Manifest presence distinguishes a traced run whose per-record empty
    // arrays were omitted from a legacy run where absence means unknown.
    let action_entries_known = replay.manifest().action_entry_count.is_some();

    let mut reports = Vec::with_capacity(records.len());

    for record in records {
        let Some(input) = input_for(record) else {
            continue;
        };

        let flags = match flags_from_record(&record.flags) {
            Ok(flags) => flags,
            Err(detail) => {
                reports.push(SubjectReport {
                    subject: record.subject.clone(),
                    attempt: record.attempt,
                    recorded: record.observations.len(),
                    consumed: 0,
                    verdict: Verdict::Error { detail },
                });
                continue;
            }
        };
        let resolver = Arc::new(TapeContentResolver::new(
            replay,
            &record.subject,
            record.attempt,
        ));
        let classifier = classifier_for(Arc::clone(&resolver))?;

        let (result, actual_action_entries) = classifier
            .run_with_action_entries(&record.workflow, &flags, &input)
            .await;

        let remaining = resolver.remaining();
        let recorded = record.observations.len();
        let consumed = recorded.saturating_sub(remaining);
        let outcome = record.outcome.as_ref().map_or_else(
            || "unknown".to_owned(),
            |outcome| outcome.kind.as_str().to_owned(),
        );
        let authoritative = record.authoritative();
        let recorded_action_entries = (action_entries_known && authoritative)
            .then(|| record.action_entries.as_deref().unwrap_or_default());
        let outcome_verdict = authoritative.then(|| {
            let actual = normalized_outcome(&result);
            (actual != outcome).then(|| Verdict::Desync {
                detail: format!(
                    "terminal outcome mismatch: recorded {outcome}, Rust produced {actual}"
                ),
            })
        });
        let verdict = action_entry_verdict(recorded_action_entries, &actual_action_entries)
            .or_else(|| outcome_verdict.flatten())
            .unwrap_or_else(|| verdict_for(&result, remaining));
        let verdict = downgrade_if_not_an_oracle(verdict, authoritative, &outcome);

        reports.push(SubjectReport {
            subject: record.subject.clone(),
            attempt: record.attempt,
            recorded,
            consumed,
            verdict,
        });
    }

    Ok(summarise(reports, failure_sample))
}

/// Exact ordered action-entry comparison. `None` is a legacy/unknown trace and
/// deliberately does not claim that zero actions ran.
fn action_entry_verdict(
    recorded: Option<&[ActionEntry]>,
    actual: &[ActionEntry],
) -> Option<Verdict> {
    let recorded = recorded?;
    if recorded == actual {
        return None;
    }

    let first_difference = recorded
        .iter()
        .zip(actual)
        .position(|(expected, got)| expected != got)
        .unwrap_or_else(|| recorded.len().min(actual.len()));
    let names = |entries: &[ActionEntry]| {
        entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>()
    };

    Some(Verdict::Desync {
        detail: format!(
            "action-entry desync at sequence {first_difference}: recorded {:?}, replay entered {:?}",
            names(recorded),
            names(actual)
        ),
    })
}

/// The flags the recording ran under, as the classifier wants them.
///
/// Only booleans are carried: every flag the enrichment path keys on
/// (`local_search_enabled`, `apis_enabled`, `tmdb_enabled`) is a boolean, and
/// silently coercing anything else would fabricate a flag state the recording
/// never had.
fn flags_from_record(
    recorded: &serde_json::Map<String, serde_json::Value>,
) -> Result<Flags, String> {
    recorded
        .iter()
        .map(|(name, value)| {
            let value = if let Some(flag) = value.as_bool() {
                FlagValue::Bool(flag)
            } else if let Some(values) = value.as_array() {
                let content_types = values
                    .iter()
                    .map(|value| {
                        let raw = value.as_str().ok_or_else(|| {
                            format!("classifier flag {name:?} contains a non-string value")
                        })?;
                        ContentType::parse(raw).ok_or_else(|| {
                            format!("classifier flag {name:?} has unknown content type {raw:?}")
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                FlagValue::ContentTypeList(content_types)
            } else {
                return Err(format!(
                    "classifier flag {name:?} has unsupported recorded value {value}"
                ));
            };
            Ok((name.clone(), value))
        })
        .collect()
}

fn normalized_outcome(result: &crate::Json) -> String {
    match result.get("outcome").and_then(serde_json::Value::as_str) {
        Some("classified") => "completed".to_owned(),
        Some(value) => value.to_owned(),
        None => "unknown".to_owned(),
    }
}

/// Reclassifies a verdict when the RECORDING, not the port, is what cannot be
/// trusted.
///
/// A record whose classification ended early holds a prefix of the questions it
/// would have asked, so "the replay asked more" (a [`Verdict::Miss`]) or "the
/// replay asked fewer" ([`Verdict::Unconsumed`]) says nothing about parity. Both
/// become [`Verdict::NotAuthoritative`].
///
/// 🚨 A [`Verdict::Desync`] is deliberately NOT downgraded. A prefix is still a
/// prefix of the real sequence, so a *different* question inside it is a genuine
/// divergence no matter how the recording ended. Neither is a [`Verdict::Match`]:
/// agreement over a prefix is still agreement, it just proves less, and the
/// separate `not_authoritative` count is what keeps that honest.
fn downgrade_if_not_an_oracle(verdict: Verdict, authoritative: bool, outcome: &str) -> Verdict {
    if authoritative {
        return verdict;
    }

    match verdict {
        Verdict::Miss { detail } => Verdict::NotAuthoritative {
            outcome: outcome.to_owned(),
            detail,
        },
        Verdict::Unconsumed { remaining } => Verdict::NotAuthoritative {
            outcome: outcome.to_owned(),
            detail: format!("{remaining} recorded observation(s) went unasked"),
        },
        other => other,
    }
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
    let mut not_authoritative = 0;
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
            Verdict::NotAuthoritative { .. } => {
                not_authoritative += 1;
                "not_authoritative"
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
        not_authoritative,
        recorded_observations,
        consumed_observations,
        by_verdict,
        failures,
    }
}

#[cfg(test)]
mod action_entry_tests {
    use super::*;

    fn entries(names: &[&str]) -> Vec<ActionEntry> {
        names
            .iter()
            .map(|name| ActionEntry {
                name: (*name).to_owned(),
            })
            .collect()
    }

    #[test]
    fn legacy_absence_does_not_claim_zero_actions() {
        assert_eq!(
            action_entry_verdict(None, &entries(&["attach_local_content_by_id"])),
            None
        );
    }

    #[test]
    fn exact_action_order_is_part_of_the_gate() {
        let recorded = entries(&[
            "attach_local_content_by_id",
            "attach_local_content_by_search",
        ]);
        assert_eq!(action_entry_verdict(Some(&recorded), &recorded), None);

        let reversed = entries(&[
            "attach_local_content_by_search",
            "attach_local_content_by_id",
        ]);
        let verdict = action_entry_verdict(Some(&recorded), &reversed)
            .expect("same actions in a different order must desync");
        assert!(
            matches!(verdict, Verdict::Desync { ref detail }
                if detail.contains("sequence 0") && detail.contains("attach_local_content_by_id")),
            "unexpected verdict: {verdict:?}"
        );
    }

    #[test]
    fn missing_or_extra_action_entries_desync() {
        let recorded = entries(&["attach_local_content_by_id"]);
        for actual in [
            entries(&[]),
            entries(&["attach_local_content_by_id", "attach_tmdb_content_by_id"]),
        ] {
            assert!(matches!(
                action_entry_verdict(Some(&recorded), &actual),
                Some(Verdict::Desync { .. })
            ));
        }
    }
}
