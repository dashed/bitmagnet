//! The on-disk tape format — a direct port of Go's `internal/tape/format.go`.
//!
//! Field names, ordering and validation rules are Go's. Where a rule looks
//! pedantic it is load-bearing; the comments say which invariant each one
//! protects, because a reader that is laxer than the writer turns a corrupt
//! tape into wrong answers rather than an error.

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

/// Identifies the on-disk format. A reader refuses a tape it does not recognise.
pub const SCHEMA: &str = "bitmagnet.classifier-attach-tape/v1";

pub const TAPE_FILE_NAME: &str = "tape.jsonl";
pub const MANIFEST_FILE_NAME: &str = "manifest.json";

/// Observation outcomes. Exactly one of `response` / `error` is populated.
pub const OUTCOME_OK: &str = "ok";
pub const OUTCOME_ERROR: &str = "error";

/// The tape header, written alongside the records.
#[derive(Clone, Debug, Serialize)]
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
    /// Exact reviewed acquisition plan used to seed rare action/outcome strata.
    /// Absent on organic/legacy recordings; explicit null is malformed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acquisition_plan_digest: Option<String>,
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
    /// Exact counts of the terminal outcomes carried by the records. Absent on
    /// tapes written before outcomes were added; an absent map means unknown,
    /// not an assertion that no outcomes occurred.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_outcome_counts: Option<BTreeMap<String, usize>>,
    /// Total number of ordered attach-action entries in the tape. Presence is
    /// also the run-level capability bit: old tapes omit this field, while a
    /// traced run writes it even when the total is zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_entry_count: Option<usize>,
    /// Per-action breakdown for [`Self::action_entry_count`]. The Go writer
    /// omits an empty map, so `None` is equivalent to an empty map only when
    /// `action_entry_count` is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_entry_counts: Option<BTreeMap<String, usize>>,
    /// Set when the recording hit its cap. A truncated tape is not a complete
    /// oracle and a replay of the full population will report misses.
    #[serde(default)]
    pub truncated: bool,
    /// An older manifest's absent authoritative count is unknown, not a claim
    /// of zero. Kept private because callers consume the normalized public
    /// value while load validation needs the original field presence.
    #[serde(skip)]
    pub(crate) authoritative_record_count_present: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestWire {
    schema: String,
    effective_config_digest: String,
    generated_at: String,
    recorder: String,
    record_count: usize,
    observation_count: usize,
    #[serde(default, deserialize_with = "deserialize_present")]
    acquisition_plan_digest: Option<String>,
    #[serde(default)]
    incomplete_record_count: usize,
    #[serde(default, deserialize_with = "deserialize_present")]
    authoritative_record_count: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_present")]
    record_outcome_counts: Option<BTreeMap<String, usize>>,
    #[serde(default, deserialize_with = "deserialize_present")]
    action_entry_count: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_present")]
    action_entry_counts: Option<BTreeMap<String, usize>>,
    #[serde(default)]
    truncated: bool,
}

fn deserialize_present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

impl<'de> Deserialize<'de> for Manifest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ManifestWire::deserialize(deserializer)?;
        let authoritative_record_count_present = wire.authoritative_record_count.is_some();
        Ok(Self {
            schema: wire.schema,
            effective_config_digest: wire.effective_config_digest,
            generated_at: wire.generated_at,
            recorder: wire.recorder,
            record_count: wire.record_count,
            observation_count: wire.observation_count,
            acquisition_plan_digest: wire.acquisition_plan_digest,
            incomplete_record_count: wire.incomplete_record_count,
            authoritative_record_count: wire.authoritative_record_count.unwrap_or_default(),
            record_outcome_counts: wire.record_outcome_counts,
            action_entry_count: wire.action_entry_count,
            action_entry_counts: wire.action_entry_counts,
            truncated: wire.truncated,
            authoritative_record_count_present,
        })
    }
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
    /// Attach actions entered by this classification, in execution order.
    ///
    /// The field is absent on legacy tapes. On a traced tape (identified by a
    /// present manifest `actionEntryCount`), an absent per-record field is the
    /// writer's `omitempty` encoding of an empty sequence.
    #[serde(
        rename = "actionEntries",
        default,
        deserialize_with = "deserialize_present_action_entries",
        skip_serializing_if = "Option::is_none"
    )]
    pub action_entries: Option<Vec<ActionEntry>>,
    /// Processor-owned state that is not classifier input but is required to
    /// reproduce the eventual write set. New writers emit it even when the id
    /// list is empty; legacy tapes omit it and therefore cannot prove stale-id
    /// deletion parity.
    #[serde(
        rename = "processorState",
        default,
        deserialize_with = "deserialize_present_processor_state",
        skip_serializing_if = "Option::is_none"
    )]
    pub processor_state: Option<ProcessorState>,
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

// As with embedded input, explicit null must not collapse into legacy absence.
// Missing fields take `default`; a present field must contain an actual array.
fn deserialize_present_action_entries<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<ActionEntry>>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<ActionEntry>::deserialize(deserializer).map(Some)
}

fn deserialize_present_processor_state<'de, D>(
    deserializer: D,
) -> Result<Option<ProcessorState>, D::Error>
where
    D: Deserializer<'de>,
{
    ProcessorState::deserialize(deserializer).map(Some)
}

/// One attach action entered by a classification. The array position is the
/// execution order and is therefore part of the parity contract.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActionEntry {
    pub name: String,
}

/// The pre-classification processor state needed by write-set replay. It stays
/// separate from classifier input because the classifier neither reads nor
/// owns these association row identifiers.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProcessorState {
    pub existing_content_ids: Vec<String>,
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
#[derive(Clone, Debug, PartialEq, Eq)]
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
    Unknown(String),
}

impl RecordOutcomeKind {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Completed => "completed",
            Self::Unmatched => "unmatched",
            Self::Deleted => "deleted",
            Self::Canceled => "canceled",
            Self::Error => "error",
            Self::Unknown(value) => value,
        }
    }
}

impl<'de> Deserialize<'de> for RecordOutcomeKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            "completed" => Self::Completed,
            "unmatched" => Self::Unmatched,
            "deleted" => Self::Deleted,
            "canceled" => Self::Canceled,
            "error" => Self::Error,
            _ => Self::Unknown(value),
        })
    }
}

impl Serialize for RecordOutcomeKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
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
            self.outcome.as_ref().map(|outcome| &outcome.kind),
            Some(RecordOutcomeKind::Completed)
                | Some(RecordOutcomeKind::Unmatched)
                | Some(RecordOutcomeKind::Deleted)
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

        if let Some(entries) = &self.action_entries {
            for (i, entry) in entries.iter().enumerate() {
                if entry.name.is_empty() {
                    return Err(TapeError::Invalid(format!(
                        "record {:?} action entry {i} has an empty name",
                        self.subject
                    )));
                }
            }
        }

        if let Some(state) = &self.processor_state {
            let mut seen = HashSet::with_capacity(state.existing_content_ids.len());
            for (i, id) in state.existing_content_ids.iter().enumerate() {
                if id.is_empty() {
                    return Err(TapeError::Invalid(format!(
                        "record {:?} processor state existing content id {i} is empty",
                        self.subject
                    )));
                }
                if !seen.insert(id) {
                    return Err(TapeError::Invalid(format!(
                        "record {:?} processor state repeats existing content id {id:?}",
                        self.subject
                    )));
                }
            }
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
mod manifest_tests {
    use super::*;

    const BASE: &str = r#""schema":"bitmagnet.classifier-attach-tape/v1","effectiveConfigDigest":"sha256:test","generatedAt":"2026-08-12T00:00:00Z","recorder":"test","recordCount":0,"observationCount":0"#;

    #[test]
    fn explicit_null_aggregate_capabilities_are_not_legacy_absence() {
        for field in [
            r#""acquisitionPlanDigest":null"#,
            r#""authoritativeRecordCount":null"#,
            r#""recordOutcomeCounts":null"#,
            r#""actionEntryCount":null"#,
            r#""actionEntryCounts":null"#,
        ] {
            serde_json::from_str::<Manifest>(&format!(r#"{{{BASE},{field}}}"#)).unwrap_err();
        }
    }

    #[test]
    fn unknown_and_misspelled_manifest_fields_fail_closed() {
        for field in [r#""futureAggregate":0"#, r#""actionEntriesCount":0"#] {
            let error = serde_json::from_str::<Manifest>(&format!(r#"{{{BASE},{field}}}"#))
                .expect_err("unknown manifest field should fail");
            assert!(
                error.to_string().contains("unknown field"),
                "unexpected error for {field}: {error}"
            );
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
    fn action_entries_are_optional_ordered_and_round_trip() {
        let legacy = record(&format!(r#"{{{BASE}}}"#));
        assert!(legacy.action_entries.is_none(), "legacy absence is unknown");

        let decoded = record(&format!(
            r#"{{{BASE},"actionEntries":[{{"name":"attach_local_content_by_id"}},{{"name":"attach_tmdb_content_by_search"}}]}}"#
        ));
        let names: Vec<_> = decoded
            .action_entries
            .as_ref()
            .expect("present action entries")
            .iter()
            .map(|entry| entry.name.as_str())
            .collect();
        assert_eq!(
            names,
            [
                "attach_local_content_by_id",
                "attach_tmdb_content_by_search"
            ],
            "array order is the execution order"
        );

        let encoded = serde_json::to_string(&decoded).expect("record encodes");
        let round_tripped = record(&encoded);
        assert_eq!(round_tripped.action_entries, decoded.action_entries);
    }

    #[test]
    fn null_action_entries_are_not_legacy_absence() {
        serde_json::from_str::<Record>(&format!(r#"{{{BASE},"actionEntries":null}}"#))
            .expect_err("explicit null is not an ordered action array");
    }

    #[test]
    fn empty_action_names_fail_validation() {
        let decoded = record(&format!(r#"{{{BASE},"actionEntries":[{{"name":""}}]}}"#));
        assert!(decoded.validate().is_err());
    }

    #[test]
    fn processor_state_is_optional_ordered_and_round_trips() {
        let legacy = record(&format!(r#"{{{BASE}}}"#));
        assert!(
            legacy.processor_state.is_none(),
            "legacy absence is unknown, not an empty prior write set"
        );

        let decoded = record(&format!(
            r#"{{{BASE},"processorState":{{"existingContentIds":["tc-2","tc-1"]}}}}"#
        ));
        assert_eq!(
            decoded
                .processor_state
                .as_ref()
                .expect("processor state")
                .existing_content_ids,
            ["tc-2", "tc-1"],
            "slice order is preserved"
        );

        let encoded = serde_json::to_string(&decoded).expect("record encodes");
        let round_tripped = record(&encoded);
        assert_eq!(round_tripped.processor_state, decoded.processor_state);
    }

    #[test]
    fn null_processor_state_is_not_legacy_absence() {
        serde_json::from_str::<Record>(&format!(r#"{{{BASE},"processorState":null}}"#))
            .expect_err("explicit null is not processor state");
    }

    #[test]
    fn processor_state_ids_must_be_nonempty_and_unique() {
        for ids in [r#"[""]"#, r#"["tc-1","tc-1"]"#] {
            let decoded = record(&format!(
                r#"{{{BASE},"processorState":{{"existingContentIds":{ids}}}}}"#
            ));
            assert!(decoded.validate().is_err(), "ids {ids} must fail");
        }
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
            decoded.outcome.as_ref().map(|o| &o.kind),
            Some(&RecordOutcomeKind::Unknown("teleported".into()))
        );
        assert_eq!(
            serde_json::to_value(decoded.outcome.as_ref().expect("outcome").kind.clone())
                .expect("kind serialises"),
            "teleported",
            "an older reader must preserve the newer writer's count key"
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
