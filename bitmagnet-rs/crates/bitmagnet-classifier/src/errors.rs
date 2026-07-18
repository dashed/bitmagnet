//! Runtime flow-control errors — a faithful port of
//! `classification.WorkflowError` + `classification.RuntimeError`
//! (`classification/errors.go`), including the exact `Error()` strings and the
//! `errors.Is` unwrap semantics the corpus `error` field pins.

use std::fmt;

/// A workflow flow-control error. `Unmatched`/`Delete` are the two sentinels
/// (`ErrUnmatched` / `ErrDeleteTorrent`); `Runtime` wraps a cause with the
/// compile-time path (`workflows.default.[0].if_else...`); `Cel` carries a
/// program evaluation failure.
#[derive(Clone, Debug)]
pub enum FlowError {
    /// `ErrUnmatched` — `WorkflowError{key: "unmatched"}`.
    Unmatched,
    /// `ErrDeleteTorrent` — `WorkflowError{key: "delete_torrent"}`.
    Delete,
    /// `RuntimeError{Path, Cause}`.
    Runtime { path: String, cause: Box<FlowError> },
    /// A CEL program or other opaque runtime error (surfaced as `outcome:error`).
    Cel(String),
}

impl FlowError {
    /// Wrap a cause in a `RuntimeError` at `path` (mirrors the construction in
    /// `find_match` / `unmatched` / `delete`).
    #[must_use]
    pub fn runtime(path: &[String], cause: FlowError) -> FlowError {
        FlowError::Runtime {
            path: path.join("."),
            cause: Box::new(cause),
        }
    }

    /// `errors.Is(err, ErrDeleteTorrent)` — walks the `Runtime` unwrap chain.
    #[must_use]
    pub fn is_delete(&self) -> bool {
        match self {
            FlowError::Delete => true,
            FlowError::Runtime { cause, .. } => cause.is_delete(),
            _ => false,
        }
    }

    /// `errors.Is(err, ErrUnmatched)`.
    #[must_use]
    pub fn is_unmatched(&self) -> bool {
        match self {
            FlowError::Unmatched => true,
            FlowError::Runtime { cause, .. } => cause.is_unmatched(),
            _ => false,
        }
    }
}

impl fmt::Display for FlowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // WorkflowError.Error() with empty message.
            FlowError::Unmatched => write!(f, "workflow unmarshalError: unmatched"),
            FlowError::Delete => write!(f, "workflow unmarshalError: delete_torrent"),
            FlowError::Runtime { path, cause } => {
                write!(f, "runtime error at Path {path}: {cause}")
            }
            FlowError::Cel(msg) => write!(f, "{msg}"),
        }
    }
}
