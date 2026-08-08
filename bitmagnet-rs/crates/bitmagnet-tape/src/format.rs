//! The on-disk tape format — a direct port of Go's `internal/tape/format.go`.
//!
//! Field names, ordering and validation rules are Go's. Where a rule looks
//! pedantic it is load-bearing; the comments say which invariant each one
//! protects, because a reader that is laxer than the writer turns a corrupt
//! tape into wrong answers rather than an error.

use serde::{Deserialize, Serialize};
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
pub struct Record {
    pub subject: String,
    /// Disambiguates repeat classifications of one subject within a run. In a
    /// normal run every subject is classified once and this is 0.
    pub attempt: i64,
    pub workflow: String,
    pub flags: serde_json::Map<String, serde_json::Value>,
    pub observations: Vec<Observation>,
    /// Marks a record whose classification had not finished when the tape was
    /// written. Its observation list is a prefix, so it is **not** an oracle for
    /// that subject: [`crate::Replay::load`] drops it, which turns a question
    /// about that subject into a miss naming it rather than a short answer.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub incomplete: bool,
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
