//! Replay of the Go classifier's attach tape (`internal/tape`).
//!
//! # Why this crate exists
//!
//! The Rust classifier runs with the three enrichment flags OFF, because it does
//! not implement the `attach_*` actions. Production Go runs them ON. Until Rust
//! can reproduce flags-ON behaviour there is no parity for the enrichment path
//! and no write-path cutover — that is B′.
//!
//! The obstacle is the oracle. Flags-ON behaviour depends on a TMDB API and a
//! local content search over a live table, and it is not a pure function of a
//! database snapshot: the local search orders candidates by `ts_rank_cd`, which
//! is degenerate for the phrase queries the classifier issues, so dozens of rows
//! can tie at exactly 1.0. Which ones land inside `LIMIT 10`, and in what order,
//! is then the query planner's choice — and the levenshtein selection that
//! follows is first-wins with an early exit. Re-running the query against a
//! frozen snapshot re-rolls that dice.
//!
//! So the only replayable artifact is the ordered candidate list Go actually
//! observed. Go records it; this crate reads it back.
//!
//! # The three rules that make a replay evidence rather than theatre
//!
//! 1. **Requests are asserted, not just answers.** Every observation stores the
//!    question. A port that asks something different — a different search
//!    string, a dropped year filter — gets [`TapeError::Desync`] even when the
//!    recorded answer would have happened to fit.
//! 2. **Empty is not missing.** A recorded empty response is a real answer from
//!    the dependency; a position the recording never saw is [`TapeError::Miss`].
//!    Reading a gap as an empty answer would manufacture agreement.
//! 3. **Byte comparison.** Requests are compared as bytes, so Rust must encode
//!    them exactly as Go does. See [`canonical`].
//!
//! # What a green replay does NOT prove
//!
//! The seam records the *search string* handed to the query builder, not the
//! tsquery it compiles into. Two implementations that agree on the string and
//! disagree on the tsquery replay identically and never desync. That gap is
//! real and known — Rust's `char::is_alphanumeric` disagrees with Go's
//! `unicode.IsLetter || unicode.IsDigit` at 12,322 code points — and has to be
//! proven separately by an all-scalar test over the two predicates. See
//! `TapeScopeLimits` in `internal/classifier/tape_local_search.go`.

pub mod canonical;
mod format;
mod replay;

pub use canonical::marshal;
pub use format::{
    Desync, Manifest, Observation, ObservationError, Record, TapeError, MANIFEST_FILE_NAME,
    OUTCOME_ERROR, OUTCOME_OK, SCHEMA, TAPE_FILE_NAME,
};
pub use replay::{decode_records, Answer, Replay, Session};
