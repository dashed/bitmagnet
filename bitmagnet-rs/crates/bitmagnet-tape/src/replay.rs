//! Answering observations from a recorded tape — a port of Go's
//! `internal/tape/replay.go` and the replay half of `session.go`.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::canonical;
use crate::format::{
    Desync, Manifest, Observation, ObservationError, Record, TapeError, MANIFEST_FILE_NAME,
    OUTCOME_ERROR, SCHEMA, TAPE_FILE_NAME,
};

/// What the tape recorded at a position: either a response, or the dependency's
/// original failure.
///
/// A recorded failure is **not** a Rust `Err`: it is a successful replay of an
/// unsuccessful call, and the caller rebuilds the dependency's error from it.
/// Conflating the two would make a recorded 401 indistinguishable from the tape
/// being unable to answer.
#[derive(Debug)]
pub enum Answer<'a> {
    Response(&'a RawValue),
    Failure(&'a ObservationError),
}

/// A loaded tape.
#[derive(Debug)]
pub struct Replay {
    manifest: Manifest,
    records: HashMap<(String, i64), Record>,
}

impl Replay {
    /// Reads the tape in `dir` and pins it to `effective_config_digest`.
    ///
    /// Fails closed on drift: a tape recorded under a different classifier
    /// configuration answers questions about a classifier that no longer
    /// exists. Pass an empty digest only when the caller has separately
    /// established that the configuration is irrelevant.
    pub fn load(dir: impl AsRef<Path>, effective_config_digest: &str) -> Result<Self, TapeError> {
        let dir = dir.as_ref();

        let manifest_bytes = std::fs::read(dir.join(MANIFEST_FILE_NAME))?;
        let manifest: Manifest =
            serde_json::from_slice(&manifest_bytes).map_err(|source| TapeError::Decode {
                context: "decode tape manifest".into(),
                source,
            })?;

        if manifest.schema != SCHEMA {
            return Err(TapeError::Invalid(format!(
                "tape schema is {:?}, want {:?}",
                manifest.schema, SCHEMA
            )));
        }

        if !effective_config_digest.is_empty()
            && manifest.effective_config_digest != effective_config_digest
        {
            return Err(TapeError::Invalid(format!(
                "tape was recorded under effective classifier config digest {}, but the current digest is {}",
                manifest.effective_config_digest, effective_config_digest
            )));
        }

        let tape_bytes = std::fs::read(dir.join(TAPE_FILE_NAME))?;
        let records = decode_records(&tape_bytes)?;

        let mut indexed = HashMap::with_capacity(records.len());
        let mut seen = HashSet::with_capacity(records.len());

        for record in &records {
            let key = (record.subject.clone(), record.attempt);

            if !seen.insert(key.clone()) {
                return Err(TapeError::Invalid(format!(
                    "tape has duplicate record for subject {:?} attempt {}",
                    record.subject, record.attempt
                )));
            }

            // An incomplete record holds a prefix of what that classification
            // observed. Serving it would answer the first questions and then run
            // out, which reads like the classification legitimately stopped
            // asking. Excluding it turns that into a miss naming the subject.
            if record.incomplete {
                continue;
            }

            indexed.insert(key, record.clone());
        }

        // Counted against ALL records, including the incomplete ones that were
        // just excluded — the manifest describes the file, not the index.
        if manifest.record_count != records.len() {
            return Err(TapeError::Invalid(format!(
                "tape manifest declares {} records but the tape holds {}",
                manifest.record_count,
                records.len()
            )));
        }

        Ok(Self {
            manifest,
            records: indexed,
        })
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// The replayable records, i.e. excluding incomplete ones.
    pub fn subjects(&self) -> impl Iterator<Item = &Record> {
        self.records.values()
    }

    /// Opens a replay session for one classification.
    ///
    /// A subject the tape has no record for still gets a session, holding no
    /// observations, so the first question asked of it reports a miss naming the
    /// subject. Returning no session instead would silently drop the replay back
    /// onto the live dependency, which is precisely the failure this crate
    /// exists to prevent.
    pub fn begin(&self, subject: impl Into<String>, attempt: i64) -> Session {
        let subject = subject.into();

        // The session OWNS its observations. It costs one clone of a handful of
        // records per classification — negligible against the classification
        // itself — and in exchange the resolver that wraps it can be `'static`,
        // which is what `Arc<dyn ContentResolver>` requires.
        let recorded = self
            .records
            .get(&(subject.clone(), attempt))
            .map(|record| record.observations.clone())
            .unwrap_or_default();

        Session {
            subject,
            attempt,
            recorded,
            cursor: 0,
        }
    }
}

/// Newline-delimited tape records, each validated.
pub fn decode_records(bytes: &[u8]) -> Result<Vec<Record>, TapeError> {
    let mut records = Vec::new();

    // A tape is newline-delimited; a single trailing newline is normal and does
    // not introduce a final empty record. Any OTHER blank line is a malformed
    // tape, which Go rejects rather than skipping.
    let body = bytes.strip_suffix(b"\n").unwrap_or(bytes);

    if body.is_empty() {
        return Ok(records);
    }

    for (index, line) in body.split(|byte| *byte == b'\n').enumerate() {
        let line_number = index + 1;

        if line.iter().all(u8::is_ascii_whitespace) {
            return Err(TapeError::Invalid(format!(
                "tape line {line_number} is empty"
            )));
        }

        // Go uses DisallowUnknownFields: an unrecognised field means the writer
        // and reader disagree about the format, which must fail rather than be
        // silently dropped.
        let mut deserializer = serde_json::Deserializer::from_slice(line);
        let record =
            Record::deserialize(&mut deserializer).map_err(|source| TapeError::Decode {
                context: format!("decode tape line {line_number}"),
                source,
            })?;
        deserializer.end().map_err(|source| TapeError::Decode {
            context: format!("decode tape line {line_number}: trailing content"),
            source,
        })?;

        record.validate().map_err(|err| match err {
            TapeError::Invalid(message) => {
                TapeError::Invalid(format!("tape line {line_number}: {message}"))
            }
            other => other,
        })?;

        records.push(record);
    }

    Ok(records)
}

/// The per-classification handle a seam replays through.
///
/// Observations are consumed **by position**, so the cursor advances even on a
/// miss or a desync — exactly as Go's does. That keeps a failure from silently
/// re-aligning the stream and turning one wrong question into a cascade of
/// misleading answers.
#[derive(Debug)]
pub struct Session {
    subject: String,
    attempt: i64,
    recorded: Vec<Observation>,
    cursor: usize,
}

impl Session {
    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub fn attempt(&self) -> i64 {
        self.attempt
    }

    /// How many observations remain unconsumed. A classification that ends with
    /// this non-zero asked *fewer* questions than the recording — the mirror of
    /// a miss, and just as much a divergence.
    pub fn remaining(&self) -> usize {
        self.recorded.len().saturating_sub(self.cursor)
    }

    /// Returns what was recorded at the current position, asserting that the
    /// caller is asking the recorded question.
    ///
    /// The request is compared **byte for byte** against the recording, which is
    /// why it must be encoded with [`canonical::marshal`] rather than
    /// `serde_json::to_string`.
    pub fn next<T: Serialize + ?Sized>(
        &mut self,
        kind: &str,
        request: &T,
    ) -> Result<Answer<'_>, TapeError> {
        let request_json = canonical::marshal(request).map_err(|source| TapeError::Decode {
            context: "encode replay request".into(),
            source,
        })?;

        let sequence = self.cursor;
        self.cursor += 1;

        let Some(observation) = self.recorded.get(sequence) else {
            return Err(TapeError::Miss {
                subject: self.subject.clone(),
                attempt: self.attempt,
                sequence,
                kind: kind.to_owned(),
            });
        };

        if observation.kind != kind || observation.request.get() != request_json {
            return Err(TapeError::Desync(Box::new(Desync {
                subject: self.subject.clone(),
                attempt: self.attempt,
                sequence,
                want_kind: observation.kind.clone(),
                got_kind: kind.to_owned(),
                want_request: observation.request.get().to_owned(),
                got_request: request_json,
            })));
        }

        if observation.outcome == OUTCOME_ERROR {
            // validate() guarantees the error is present for this outcome.
            return Ok(Answer::Failure(
                observation
                    .error
                    .as_ref()
                    .expect("validated: error outcome carries an error"),
            ));
        }

        Ok(Answer::Response(
            observation
                .response
                .as_deref()
                .expect("validated: ok outcome carries a response"),
        ))
    }
}
