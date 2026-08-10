//! The on-disk tape format — a direct port of Go's `internal/tape/format.go`.
//!
//! Field names, ordering and validation rules are Go's. Where a rule looks
//! pedantic it is load-bearing; the comments say which invariant each one
//! protects, because a reader that is laxer than the writer turns a corrupt
//! tape into wrong answers rather than an error.

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;

/// Identifies the on-disk format. A reader refuses a tape it does not recognise.
pub const SCHEMA: &str = "bitmagnet.classifier-attach-tape/v1";

pub const TAPE_FILE_NAME: &str = "tape.jsonl";
pub const MANIFEST_FILE_NAME: &str = "manifest.json";

/// Observation outcomes. Exactly one of `response` / `error` is populated.
pub const OUTCOME_OK: &str = "ok";
pub const OUTCOME_ERROR: &str = "error";

/// The tape header, written alongside the records.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub schema: String,
    /// Pins the classifier configuration the tape was recorded under. A replay
    /// under a different digest is comparing against a classifier that no
    /// longer exists, so [`crate::Replay::load`] fails closed on drift.
    pub effective_config_digest: String,
    pub generated_at: String,
    pub recorder: String,
    pub record_count: usize,
    pub observation_count: usize,
    /// Records still being classified when the tape was written. Excluded from
    /// replay — see [`Record::incomplete`].
    #[serde(default)]
    pub incomplete_record_count: usize,
    /// Records that are a complete account of what their classification asked —
    /// see [`Record::authoritative`]. This is the honest size of the oracle; a
    /// reader who only sees `record_count` will overstate it.
    ///
    /// Zero on a tape recorded before outcomes existed, where it is unknowable.
    #[serde(default)]
    pub authoritative_record_count: usize,
    /// Set when the recording hit its cap. A truncated tape is not a complete
    /// oracle and a replay of the full population will report misses.
    #[serde(default)]
    pub truncated: bool,
}

/// One classification: the subject, the flag state it ran under, and the
/// observations it made, **in the order it made them**.
///
/// A replay consumes observations by position, so a port that consults its
/// dependencies in a different order desyncs.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub subject: String,
    /// Disambiguates repeat classifications of one subject within a run. In a
    /// normal run every subject is classified once and this is 0.
    pub attempt: i64,
    pub workflow: String,
    pub flags: serde_json::Map<String, serde_json::Value>,
    /// The classifier input at the instant Go entered `runner.Run`, before the
    /// workflow could change what a later database snapshot would show.
    ///
    /// Absent on legacy tapes. When present, this record-and-attempt-specific
    /// value is authoritative and callers must not replace it with an
    /// out-of-band input merely because it fails to decode.
    #[serde(
        default,
        deserialize_with = "deserialize_present_raw",
        skip_serializing_if = "Option::is_none"
    )]
    pub input: Option<Box<RawValue>>,
    pub observations: Vec<Observation>,
    /// Marks a record whose classification had not finished when the tape was
    /// written. Its observation list is a prefix, so it is **not** an oracle for
    /// that subject: [`crate::Replay::load`] drops it, which turns a question
    /// about that subject into a miss naming it rather than a short answer.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub incomplete: bool,
    /// How the classification ended — Go `tape.RecordOutcome`.
    ///
    /// `None` means the outcome is **unknown**: either the record was still open
    /// when the tape was written, or the tape predates outcome recording. That
    /// is not the same as "it finished normally", and must not be read as such.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<RecordOutcome>,
}

// `Option<T>` normally collapses an explicit JSON null into `None`, which would
// make a malformed new record indistinguishable from a legacy record whose
// input field is absent. Missing fields take `default`; present fields always
// deserialize as RawValue so validation can reject null fail-closed.
fn deserialize_present_raw<'de, D>(deserializer: D) -> Result<Option<Box<RawValue>>, D::Error>
where
    D: Deserializer<'de>,
{
    Box::<RawValue>::deserialize(deserializer).map(Some)
}

/// How a classification ended — Go `tape.RecordOutcome`.
///
/// 🚨 Without this, a record holding an **empty** observation list is ambiguous
/// between two opposite claims: that the workflow ran to the end and
/// legitimately consulted nothing (so a replay consulting something has
/// diverged), and that the classification never got that far (so the list is a
/// prefix and proves nothing). Reading the second as the first manufactures a
/// divergence out of a recording artifact — which is exactly what happened to
/// two subjects of the 2026-08-09 production corpus.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct RecordOutcome {
    pub kind: RecordOutcomeKind,
    /// Diagnosis only. Consumers key on [`Self::kind`]; the text is not part of
    /// the contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The endings a classification can have.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordOutcomeKind {
    /// Ran to the end and returned a result.
    Completed,
    /// Ended with Go's `ErrUnmatched`.
    Unmatched,
    /// Ended with Go's `ErrDeleteTorrent`.
    Deleted,
    /// The context was cancelled or timed out — the process was going away.
    /// Not reproducible, so the observation list stops wherever it landed.
    Canceled,
    /// Anything else went wrong.
    Error,
    /// A kind this build does not know. Kept rather than rejected so a newer
    /// recorder cannot make an older reader fail closed on an unrelated tape —
    /// but it is deliberately NOT authoritative.
    #[serde(other)]
    Unknown,
}

impl Record {
    /// Whether this record's observation list can be read as a **complete**
    /// account of what the classification asked.
    ///
    /// True only for the endings a replay of the same input reaches too:
    /// completion, or stopping at `unmatched` / `delete`, which are the
    /// workflow's own deterministic vocabulary. A cancellation is not
    /// reproducible, an error may not be, and an unknown outcome is not a claim
    /// at all — for those the list is a prefix, so "the replay asked more" says
    /// nothing about parity.
    #[must_use]
    pub fn authoritative(&self) -> bool {
        if self.incomplete {
            return false;
        }

        matches!(
            self.outcome.as_ref().map(|outcome| outcome.kind),
            Some(
                RecordOutcomeKind::Completed
                    | RecordOutcomeKind::Unmatched
                    | RecordOutcomeKind::Deleted
            )
        )
    }
}

/// A single interaction with an impure dependency.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Observation {
    pub kind: String,
    /// The canonically encoded question that was asked. This is the field a
    /// replay asserts against — kept as raw bytes so the comparison is against
    /// what Go actually wrote, not a re-encoding of it.
    pub request: Box<RawValue>,
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<Box<RawValue>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ObservationError>,
}

/// A recorded failure, in a form a replay can turn back into the same error
/// value the dependency originally produced.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ObservationError {
    /// Stable discriminator owned by the recording package. The classifier's
    /// control flow depends on error *identity*, not error text — the TMDB
    /// recorder distinguishes unauthorized from not-found because `find_match`
    /// treats them differently — so this, not `message`, is what a replay keys on.
    pub kind: String,
    pub message: String,
    /// Optional evidence for a human reading the tape. A replay must never need
    /// it to reconstruct the error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<Box<RawValue>>,
}

/// Why a tape could not be loaded, or could not answer.
#[derive(Debug, thiserror::Error)]
pub enum TapeError {
    #[error("tape: {0}")]
    Io(#[from] std::io::Error),

    #[error("tape: {context}: {source}")]
    Decode {
        context: String,
        #[source]
        source: serde_json::Error,
    },

    #[error("tape: {0}")]
    Invalid(String),

    /// The tape holds no observation at the requested position.
    ///
    /// Deliberately distinct from a recorded empty response: a miss means the
    /// recording never saw this question, so the replay has no answer and must
    /// not invent one.
    #[error("tape: no observation recorded at {subject}#{attempt}[{sequence}] ({kind})")]
    Miss {
        subject: String,
        attempt: i64,
        sequence: usize,
        kind: String,
    },

    /// The caller asked a different question from the one recorded here.
    ///
    /// This is the highest-signal failure the replay produces: it says the port
    /// asked the wrong question, independently of whether the answer would have
    /// matched.
    /// Boxed: it carries both requests verbatim, which makes it far larger than
    /// every other variant, and an unboxed variant would inflate every `Result`
    /// in the crate.
    #[error("{0}")]
    Desync(Box<Desync>),
}

/// The detail of a [`TapeError::Desync`].
#[derive(Debug)]
pub struct Desync {
    pub subject: String,
    pub attempt: i64,
    pub sequence: usize,
    pub want_kind: String,
    pub got_kind: String,
    pub want_request: String,
    pub got_request: String,
}

impl std::fmt::Display for Desync {
    /// Mirrors Go's `DesyncError.Error()`, including its two shapes: a kind
    /// mismatch is reported without the requests, because the requests are not
    /// comparable when the questions are of different kinds.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            subject,
            attempt,
            sequence,
            want_kind,
            got_kind,
            want_request,
            got_request,
        } = self;

        if want_kind != got_kind {
            return write!(
                f,
                "tape: desync at {subject}#{attempt}[{sequence}]: recorded kind {want_kind:?}, replay asked {got_kind:?}"
            );
        }

        write!(
            f,
            "tape: desync at {subject}#{attempt}[{sequence}] ({want_kind}): recorded request {want_request}, replay asked {got_request}"
        )
    }
}

impl Record {
    /// Checks the invariants a reader relies on, so a malformed or hand-edited
    /// tape fails closed instead of silently answering questions wrongly.
    pub(crate) fn validate(&self) -> Result<(), TapeError> {
        if self.subject.is_empty() {
            return Err(TapeError::Invalid("record has an empty subject".into()));
        }

        if self.attempt < 0 {
            return Err(TapeError::Invalid(format!(
                "record {:?} has a negative attempt {}",
                self.subject, self.attempt
            )));
        }

        if self
            .input
            .as_ref()
            .is_some_and(|input| input.get().trim() == "null")
        {
            return Err(TapeError::Invalid(format!(
                "record {:?} has a null input; an unavailable legacy input must be absent",
                self.subject
            )));
        }

        for (i, observation) in self.observations.iter().enumerate() {
            observation.validate().map_err(|err| {
                TapeError::Invalid(format!("record {:?} observation {i}: {err}", self.subject))
            })?;
        }

        Ok(())
    }
}

impl Observation {
    fn validate(&self) -> Result<(), String> {
        if self.kind.is_empty() {
            return Err("observation has an empty kind".into());
        }

        if self.request.get().is_empty() {
            return Err(
                "observation has no request; the request is what a replay asserts against".into(),
            );
        }

        match self.outcome.as_str() {
            OUTCOME_OK => {
                if self.error.is_some() {
                    return Err(r#"observation has outcome "ok" but carries an error"#.into());
                }

                // A `null` response is rejected as well as an absent one: either
                // is indistinguishable from a gap, and a gap must never be read
                // as a legitimate empty answer.
                match self.response.as_ref() {
                    None => Err(r#"observation has outcome "ok" but no response; a genuine empty answer must encode its emptiness explicitly"#.into()),
                    Some(raw) if raw.get().trim() == "null" => Err(r#"observation has outcome "ok" but a null response; a genuine empty answer must encode its emptiness explicitly"#.into()),
                    Some(_) => Ok(()),
                }
            }
            OUTCOME_ERROR => {
                if self.response.is_some() {
                    return Err(r#"observation has outcome "error" but carries a response"#.into());
                }

                match self.error.as_ref() {
                    None => Err(r#"observation has outcome "error" but no error"#.into()),
                    Some(err) if err.kind.is_empty() => {
                        Err("observation error has an empty kind".into())
                    }
                    Some(_) => Ok(()),
                }
            }
            other => Err(format!("observation has an unknown outcome {other:?}")),
        }
    }
}

#[cfg(test)]
mod outcome_tests {
    use super::*;

    fn record(json: &str) -> Record {
        serde_json::from_str(json).expect("record decodes")
    }

    const BASE: &str =
        r#""subject":"s","attempt":0,"workflow":"default","flags":{},"observations":[]"#;

    /// The regression the outcome exists for: two records, both with an empty
    /// observation list, meaning opposite things.
    #[test]
    fn an_empty_observation_list_means_different_things() {
        let completed = record(&format!(r#"{{{BASE},"outcome":{{"kind":"completed"}}}}"#));
        let cancelled = record(&format!(r#"{{{BASE},"outcome":{{"kind":"canceled"}}}}"#));

        assert!(completed.observations.is_empty() && cancelled.observations.is_empty());
        assert!(
            !completed.incomplete && !cancelled.incomplete,
            "an early exit CLOSES its session, so `incomplete` cannot tell them apart"
        );

        assert!(
            completed.authoritative(),
            "asked nothing, and that is an answer"
        );
        assert!(
            !cancelled.authoritative(),
            "asked nothing YET; the list is a prefix"
        );
    }

    /// `unmatched` and `delete` are the workflow's own deterministic endings, so
    /// a replay of the same input stops in the same place.
    #[test]
    fn the_workflows_own_endings_are_authoritative() {
        for kind in ["completed", "unmatched", "deleted"] {
            let decoded = record(&format!(r#"{{{BASE},"outcome":{{"kind":"{kind}"}}}}"#));
            assert!(decoded.authoritative(), "{kind} should be authoritative");
        }

        for kind in ["canceled", "error"] {
            let decoded = record(&format!(r#"{{{BASE},"outcome":{{"kind":"{kind}"}}}}"#));
            assert!(
                !decoded.authoritative(),
                "{kind} should not be authoritative"
            );
        }
    }

    /// A tape written before outcomes existed must not be upgraded to
    /// "finished normally" by its own silence.
    #[test]
    fn an_absent_outcome_is_unknown_not_completed() {
        let decoded = record(&format!(r#"{{{BASE}}}"#));

        assert!(decoded.outcome.is_none());
        assert!(
            !decoded.authoritative(),
            "an unknown outcome is not a claim that the classification finished"
        );
    }

    #[test]
    fn classifier_input_is_optional_and_round_trips_raw() {
        let legacy = record(&format!(r#"{{{BASE}}}"#));
        assert!(legacy.input.is_none());

        let decoded = record(&format!(
            r#"{{{BASE},"input":{{"name":"Cinderella","files":[]}}}}"#
        ));
        assert_eq!(
            decoded.input.as_ref().map(|input| input.get()),
            Some(r#"{"name":"Cinderella","files":[]}"#)
        );

        let encoded = serde_json::to_string(&decoded).expect("record encodes");
        let round_tripped = record(&encoded);
        assert_eq!(
            round_tripped.input.as_ref().map(|input| input.get()),
            decoded.input.as_ref().map(|input| input.get())
        );
    }

    #[test]
    fn null_input_is_not_a_legacy_absence() {
        let decoded = record(&format!(r#"{{{BASE},"input":null}}"#));
        assert!(decoded.validate().is_err());
    }

    #[test]
    fn unknown_record_fields_fail_closed_like_the_go_reader() {
        let error = serde_json::from_str::<Record>(&format!(r#"{{{BASE},"extra":1}}"#))
            .expect_err("unknown record field should fail");
        assert!(error.to_string().contains("unknown field"));
    }

    /// An open record has no outcome and is never authoritative, whatever else
    /// the tape says.
    #[test]
    fn an_incomplete_record_is_never_authoritative() {
        let decoded = record(&format!(
            r#"{{{BASE},"incomplete":true,"outcome":{{"kind":"completed"}}}}"#
        ));

        assert!(!decoded.authoritative());
    }

    /// A kind from a newer recorder must not fail the decode — but must not be
    /// trusted either.
    #[test]
    fn an_unknown_kind_decodes_without_being_trusted() {
        let decoded = record(&format!(r#"{{{BASE},"outcome":{{"kind":"teleported"}}}}"#));

        assert_eq!(
            decoded.outcome.as_ref().map(|o| o.kind),
            Some(RecordOutcomeKind::Unknown)
        );
        assert!(!decoded.authoritative());
    }

    /// The error text is diagnosis only, but it has to survive so a human can
    /// read why a record was excluded.
    #[test]
    fn the_failure_message_survives_for_diagnosis() {
        let decoded = record(&format!(
            r#"{{{BASE},"outcome":{{"kind":"canceled","error":"context canceled"}}}}"#
        ));

        assert_eq!(
            decoded.outcome.and_then(|o| o.error).as_deref(),
            Some("context canceled")
        );
    }
}
