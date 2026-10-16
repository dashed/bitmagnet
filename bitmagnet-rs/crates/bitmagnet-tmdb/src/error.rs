//! TMDB failures, kept apart by the distinction the classifier's control flow
//! actually depends on.
//!
//! Go models this with package sentinels (`tmdb.ErrUnauthorized`,
//! `tmdb.ErrNotFound`) that callers reach with `errors.Is`, and
//! `requester_recorder.go` records which one it was as a `TapeErrorKind*`. Those
//! constants exist precisely because the difference changes behaviour:
//!
//! * **404** on a *details* call is turned into "unmatched" by the caller, and
//!   `find_match` falls through to the next branch. Flattened into a generic
//!   failure it would abort the classification instead.
//! * **401** additionally latches a process-lifetime failure (see
//!   [`crate::TmdbClient`]), so it can never be retried into a storm.
//! * Anything else is fatal to the classification.
//!
//! So: never collapse these into one variant.

use bitmagnet_classifier::ResolveError;

/// Go's recorded error kinds (`internal/tmdb/requester_recorder.go`).
pub const ERROR_KIND_UNAUTHORIZED: &str = "unauthorized";
pub const ERROR_KIND_NOT_FOUND: &str = "not_found";
pub const ERROR_KIND_HTTP: &str = "http";
pub const ERROR_KIND_TRANSPORT: &str = "transport";

/// A TMDB call that did not return a decoded response.
///
/// # Display
///
/// The text is Go's error message **without** its `"TMDB request failed: "`
/// prefix, because [`ResolveError::Tmdb`]'s own `Display` adds exactly that
/// prefix. Rendering through the seam therefore reproduces Go's string, and
/// matches the classifier's tape resolver — which builds `Tmdb("401
/// Unauthorized")` — byte for byte. The one exception is [`Self::Disabled`],
/// whose Go message has no prefix to begin with; through the seam it gains one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TmdbError {
    /// Go `newRequester`: the client was consulted while TMDB is switched off.
    /// Latched by the lazy initialiser exactly as Go latches it.
    #[error("TMDB is disabled")]
    Disabled,

    /// HTTP 401. Latches the fail-fast gate for the process lifetime.
    #[error("401 Unauthorized")]
    Unauthorized,

    /// HTTP 404 — a *miss*, not a failure, for the callers that treat it as
    /// `ErrUnmatched`.
    #[error("404 Not Found")]
    NotFound,

    /// Any other non-2xx.
    ///
    /// `status_line` is Go's `res.Status()`, e.g. `"500 Internal Server
    /// Error"`. Its reason phrase comes from the canonical status table rather
    /// than the server's own bytes — hyper does not surface those — so a server
    /// sending a nonstandard phrase produces a different string here than in
    /// Go. Nothing branches on the text.
    #[error("{status_line}")]
    Http { status: u16, status_line: String },

    /// No usable HTTP response: connection refused, TLS failure, timeout,
    /// retries exhausted. Go's `err != nil` out of resty, and the only class of
    /// failure resty retries.
    #[error("{0}")]
    Transport(String),

    /// The response body was not the JSON the DTO expects.
    ///
    /// Go decodes inside resty, so a decode failure surfaces as a non-nil error
    /// with a *successful* response attached, which `requestErrorKind`
    /// classifies as `transport` — hence [`Self::error_kind`] reports the same,
    /// even though this crate decodes one layer higher. Nothing between the two
    /// layers inspects the decoded value, so the placement is not observable.
    #[error("{0}")]
    Decode(String),
}

impl TmdbError {
    /// The kind Go would have recorded for this failure
    /// (`requester_recorder.go`'s `requestErrorKind`).
    ///
    /// Exposed so a recording built on this client, or a test comparing against
    /// a tape, classifies failures the same way Go's recorder does.
    #[must_use]
    pub fn error_kind(&self) -> &'static str {
        match self {
            Self::Unauthorized => ERROR_KIND_UNAUTHORIZED,
            Self::NotFound => ERROR_KIND_NOT_FOUND,
            Self::Http { .. } => ERROR_KIND_HTTP,
            // Go reaches `transport` for a nil/successful response with an
            // error, which is what a disabled client and a decode failure both
            // look like from the recorder's seat.
            Self::Transport(_) | Self::Decode(_) | Self::Disabled => ERROR_KIND_TRANSPORT,
        }
    }

    /// Maps an HTTP status onto Go's `requester.Request` switch
    /// (`internal/tmdb/requester.go:29-38`), or `None` for a success.
    ///
    /// Go's success test is resty's `IsSuccess()`, i.e. 200..=299 — a 3xx that
    /// survived redirect handling is an error, not a success.
    #[must_use]
    pub fn from_status(status: u16) -> Option<Self> {
        match status {
            200..=299 => None,
            401 => Some(Self::Unauthorized),
            404 => Some(Self::NotFound),
            _ => Some(Self::Http {
                status,
                status_line: status_line(status),
            }),
        }
    }
}

/// Go's `res.Status()` — `"<code> <reason>"`.
fn status_line(status: u16) -> String {
    let reason = reqwest::StatusCode::from_u16(status)
        .ok()
        .and_then(|code| code.canonical_reason())
        .unwrap_or("Unknown");

    format!("{status} {reason}")
}

/// The seam's error type. Every distinction above is preserved by the *return
/// type* at the call site (a 404 details lookup becomes `Ok(None)`), so what
/// reaches here is genuinely fatal to the classification — matching how the
/// classifier's tape resolver reports the same failures.
impl From<TmdbError> for ResolveError {
    fn from(err: TmdbError) -> Self {
        ResolveError::Tmdb(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three status classes the classifier branches on must not collapse.
    #[test]
    fn status_mapping_keeps_the_classes_apart() {
        assert_eq!(TmdbError::from_status(200), None);
        assert_eq!(TmdbError::from_status(204), None);
        assert_eq!(TmdbError::from_status(401), Some(TmdbError::Unauthorized));
        assert_eq!(TmdbError::from_status(404), Some(TmdbError::NotFound));
        assert_eq!(
            TmdbError::from_status(500),
            Some(TmdbError::Http {
                status: 500,
                status_line: "500 Internal Server Error".to_owned(),
            })
        );
        // Go's IsSuccess() is 2xx only; a surfaced 3xx is an error.
        assert!(matches!(
            TmdbError::from_status(304),
            Some(TmdbError::Http { status: 304, .. })
        ));
        // TMDB's own rate-limit response must stay distinguishable from a miss.
        assert!(matches!(
            TmdbError::from_status(429),
            Some(TmdbError::Http { status: 429, .. })
        ));
    }

    /// The recorder's kinds are what a tape keys failures on; a wrong kind
    /// silently changes replay control flow.
    #[test]
    fn error_kinds_match_gos_recorder() {
        assert_eq!(TmdbError::Unauthorized.error_kind(), "unauthorized");
        assert_eq!(TmdbError::NotFound.error_kind(), "not_found");
        assert_eq!(
            TmdbError::Http {
                status: 500,
                status_line: "500 Internal Server Error".to_owned()
            }
            .error_kind(),
            "http"
        );
        assert_eq!(
            TmdbError::Transport("connection refused".to_owned()).error_kind(),
            "transport"
        );
        assert_eq!(
            TmdbError::Decode("expected value".to_owned()).error_kind(),
            "transport"
        );
    }

    /// Through the seam the message must read exactly as Go's does — and
    /// exactly as the classifier's tape resolver renders the replayed failure,
    /// so a live run and a replay are indistinguishable in the logs.
    #[test]
    fn seam_errors_render_gos_message() {
        assert_eq!(
            ResolveError::from(TmdbError::Unauthorized).to_string(),
            "TMDB request failed: 401 Unauthorized"
        );
        assert_eq!(
            ResolveError::from(TmdbError::NotFound).to_string(),
            "TMDB request failed: 404 Not Found"
        );
        assert_eq!(
            ResolveError::from(TmdbError::Http {
                status: 503,
                status_line: "503 Service Unavailable".to_owned()
            })
            .to_string(),
            "TMDB request failed: 503 Service Unavailable"
        );
    }
}
