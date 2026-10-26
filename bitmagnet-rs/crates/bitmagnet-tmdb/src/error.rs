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
use reqwest::Version;

use crate::status_text::go_status_text;

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
    /// `status_line` is Go's `res.Status()`. On HTTP/1.x that is the status
    /// code and the reason phrase the server sent, e.g. `"500 Internal Server
    /// Error"` or a Go server's `"599 status code 599"`; on HTTP/2 it is the
    /// code and Go's own `StatusText`, e.g. `"413 Request Entity Too Large"` or
    /// `"599 "`. The text reaches a failed job's stored error, so it is rebuilt
    /// byte for byte (see [`Self::from_status_line`]).
    #[error("{status_line}")]
    Http { status: u16, status_line: String },

    /// An HTTP/1.x non-2xx whose status line cannot be rebuilt as Go would
    /// render it: an empty reason phrase, one that is not UTF-8, or an unknown
    /// status with no phrase. Every HTTP/2 status renders.
    #[error("{status} with an unrenderable reason phrase")]
    UnrenderableStatus { status: u16 },

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
            Self::Http { .. } | Self::UnrenderableStatus { .. } => ERROR_KIND_HTTP,
            // Go reaches `transport` for a nil/successful response with an
            // error, which is what a disabled client and a decode failure both
            // look like from the recorder's seat.
            Self::Transport(_) | Self::Decode(_) | Self::Disabled => ERROR_KIND_TRANSPORT,
        }
    }

    /// [`Self::from_status_line`] for an HTTP/1.1 status line with its
    /// canonical reason phrase.
    #[must_use]
    pub fn from_status(status: u16) -> Option<Self> {
        Self::from_status_line(status, Version::HTTP_11, None)
    }

    /// Maps an HTTP status onto Go's `requester.Request` switch
    /// (`internal/tmdb/requester.go:29-38`), or `None` for a success.
    ///
    /// Go's success test is resty's `IsSuccess()`, i.e. 200..=299 — a 3xx that
    /// survived redirect handling is an error, not a success. 401 and 404 are
    /// Go's sentinels, whose text is fixed whatever phrase the server sent.
    ///
    /// `version` is [`crate::HttpResponse::version`], the protocol the
    /// response came over, and `reason_phrase` is
    /// [`crate::HttpResponse::reason_phrase`]: the server's phrase when it is
    /// not the canonical one, which only HTTP/1.x has.
    #[must_use]
    pub fn from_status_line(
        status: u16,
        version: Version,
        reason_phrase: Option<&[u8]>,
    ) -> Option<Self> {
        match status {
            200..=299 => None,
            401 => Some(Self::Unauthorized),
            404 => Some(Self::NotFound),
            _ => Some(match status_line(status, version, reason_phrase) {
                Some(status_line) => Self::Http {
                    status,
                    status_line,
                },
                None => Self::UnrenderableStatus { status },
            }),
        }
    }
}

/// Go's `res.Status()` for a response that came over `version`.
fn status_line(status: u16, version: Version, reason_phrase: Option<&[u8]>) -> Option<String> {
    if version == Version::HTTP_2 {
        http2_status_line(status)
    } else {
        http1_status_line(status, reason_phrase)
    }
}

/// Go's HTTP/2 `res.Status()`: the `:status` value, a space, and Go's
/// `StatusText`, which is empty for a code Go does not name (`"599 "`). The h2
/// crate accepts only a three-digit `:status`, so that value is the code as
/// printed here; see [`crate::status_text`].
fn http2_status_line(status: u16) -> Option<String> {
    go_status_text(status).map(|text| format!("{status} {text}"))
}

/// Go's HTTP/1.x `res.Status()`: the code, a space, and the reason phrase as
/// sent, which is the canonical one unless hyper kept a different phrase.
fn http1_status_line(status: u16, reason_phrase: Option<&[u8]>) -> Option<String> {
    let reason = match reason_phrase {
        // httparse reports `500`, `500 ` and a phrase with obs-text bytes all
        // as an empty phrase. Go renders `"500"`, `"500 "` and the bytes as
        // sent, so which one it was cannot be known.
        Some([]) => return None,
        Some(phrase) => std::str::from_utf8(phrase).ok()?,
        None => reqwest::StatusCode::from_u16(status)
            .ok()?
            .canonical_reason()?,
    };

    Some(format!("{status} {reason}"))
}

/// The seam's error type. Every distinction above is preserved by the *return
/// type* at the call site (a 404 details lookup becomes `Ok(None)`), so what
/// reaches here is genuinely fatal to the classification — matching how the
/// classifier's tape resolver reports the same failures.
///
/// An answered status keeps its code ([`ResolveError::TmdbStatus`]), so a
/// caller can treat it apart from a request that got no usable answer; both
/// render Go's message.
impl From<TmdbError> for ResolveError {
    fn from(err: TmdbError) -> Self {
        let status = match &err {
            TmdbError::Unauthorized => Some(401),
            TmdbError::NotFound => Some(404),
            TmdbError::Http { status, .. } => Some(*status),
            TmdbError::UnrenderableStatus { .. }
            | TmdbError::Transport(_)
            | TmdbError::Decode(_)
            | TmdbError::Disabled => None,
        };
        match status {
            Some(status) => ResolveError::TmdbStatus {
                status,
                status_line: err.to_string(),
            },
            None => ResolveError::Tmdb(err.to_string()),
        }
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

    /// Go's `res.Status()` is the status line as sent, and it reaches a failed
    /// job's stored error, so a non-canonical phrase must survive as is.
    #[test]
    fn status_lines_keep_the_servers_reason_phrase() {
        for (status, phrase, expected) in [
            // A Go server's text for a code without `StatusText`.
            (599, &b"status code 599"[..], "599 status code 599"),
            // Go's table, not hyper's: 413 is "Payload Too Large" there.
            (
                413,
                b"Request Entity Too Large",
                "413 Request Entity Too Large",
            ),
            (503, b"Slow Down", "503 Slow Down"),
        ] {
            assert_eq!(
                TmdbError::from_status_line(status, Version::HTTP_11, Some(phrase)),
                Some(TmdbError::Http {
                    status,
                    status_line: expected.to_owned(),
                }),
                "{status}"
            );
        }
        // Go's sentinels have fixed text whatever the server said.
        assert_eq!(
            TmdbError::from_status_line(401, Version::HTTP_11, Some(b"Go Away")),
            Some(TmdbError::Unauthorized)
        );
        assert_eq!(
            TmdbError::from_status_line(404, Version::HTTP_11, Some(b"Nope")),
            Some(TmdbError::NotFound)
        );
        // Text that cannot be Go's byte for byte is not passed off as it.
        assert_eq!(
            TmdbError::from_status_line(502, Version::HTTP_11, Some(b"Bad \xff Gateway")),
            Some(TmdbError::UnrenderableStatus { status: 502 })
        );
        // Go's `"500"` or `"500 "`, or an obs-text phrase: hyper cannot say which.
        assert_eq!(
            TmdbError::from_status_line(500, Version::HTTP_11, Some(b"")),
            Some(TmdbError::UnrenderableStatus { status: 500 })
        );
        assert_eq!(
            TmdbError::from_status(599),
            Some(TmdbError::UnrenderableStatus { status: 599 })
        );
    }

    /// HTTP/2 has no reason phrase: Go's text is the code, a space and its own
    /// `StatusText`, whatever hyper's table says, and a code Go does not name
    /// keeps the trailing space. HTTP/1.x renders the same statuses from the
    /// status line, as before.
    #[test]
    fn http2_status_lines_are_gos_status_text() {
        for (status, expected) in [
            (413, "413 Request Entity Too Large"),
            (414, "414 Request URI Too Long"),
            (416, "416 Requested Range Not Satisfiable"),
            (422, "422 Unprocessable Entity"),
            (429, "429 Too Many Requests"),
            (500, "500 Internal Server Error"),
            (503, "503 Service Unavailable"),
            (304, "304 Not Modified"),
            (520, "520 "),
            (599, "599 "),
            (999, "999 "),
        ] {
            assert_eq!(
                TmdbError::from_status_line(status, Version::HTTP_2, None),
                Some(TmdbError::Http {
                    status,
                    status_line: expected.to_owned(),
                }),
                "{status}"
            );
        }
        // A phrase cannot arrive over HTTP/2; if one did, Go would not use it.
        assert_eq!(
            TmdbError::from_status_line(503, Version::HTTP_2, Some(b"Slow Down")),
            Some(TmdbError::Http {
                status: 503,
                status_line: "503 Service Unavailable".to_owned(),
            })
        );
        // The sentinels and the success range do not depend on the protocol.
        assert_eq!(
            TmdbError::from_status_line(401, Version::HTTP_2, None),
            Some(TmdbError::Unauthorized)
        );
        assert_eq!(
            TmdbError::from_status_line(404, Version::HTTP_2, None),
            Some(TmdbError::NotFound)
        );
        assert_eq!(
            TmdbError::from_status_line(200, Version::HTTP_2, None),
            None
        );
        assert_eq!(
            TmdbError::from_status_line(299, Version::HTTP_2, None),
            None
        );

        // The same statuses over HTTP/1.x: hyper's canonical phrase for 413,
        // and no rendering at all for a 599 without one.
        assert_eq!(
            TmdbError::from_status(413),
            Some(TmdbError::Http {
                status: 413,
                status_line: "413 Payload Too Large".to_owned(),
            })
        );
        assert_eq!(
            TmdbError::from_status_line(599, Version::HTTP_10, None),
            Some(TmdbError::UnrenderableStatus { status: 599 })
        );
    }

    /// An answered status keeps its code through the seam, so the worker can
    /// tell it from a request that got no usable answer; the message is Go's
    /// either way.
    #[test]
    fn answered_statuses_keep_their_code_through_the_seam() {
        for (err, status, message) in [
            (
                TmdbError::Http {
                    status: 503,
                    status_line: "503 Service Unavailable".to_owned(),
                },
                503,
                "TMDB request failed: 503 Service Unavailable",
            ),
            (
                TmdbError::Unauthorized,
                401,
                "TMDB request failed: 401 Unauthorized",
            ),
            (
                TmdbError::NotFound,
                404,
                "TMDB request failed: 404 Not Found",
            ),
        ] {
            let resolved = ResolveError::from(err);
            assert_eq!(resolved.to_string(), message);
            assert!(
                matches!(resolved, ResolveError::TmdbStatus { status: s, .. } if s == status),
                "{resolved:?}"
            );
        }
        for err in [
            TmdbError::Transport("operation timed out".to_owned()),
            TmdbError::Decode("expected value".to_owned()),
            TmdbError::Disabled,
            TmdbError::UnrenderableStatus { status: 502 },
        ] {
            let resolved = ResolveError::from(err);
            assert!(matches!(resolved, ResolveError::Tmdb(_)), "{resolved:?}");
        }
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
