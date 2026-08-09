//! A [`ContentResolver`] backed by a recorded tape — the B′ oracle, wired in.
//!
//! This is what turns [`bitmagnet_tape`] from a reader into a parity run: the
//! classifier consults *this* resolver instead of PostgreSQL and TMDB, and every
//! answer comes from what Go actually observed.
//!
//! # One resolver per classification
//!
//! A tape session is stateful — observations are consumed by position — and the
//! [`ContentResolver`] trait takes `&self` with no context parameter, so there
//! is nowhere to hang "which subject is being classified". Go solves this by
//! carrying the session on the `context.Context`; here the resolver is instead
//! **bound to one subject at construction**. A harness builds one per subject,
//! runs the classifier, then asserts [`TapeContentResolver::remaining`] is zero.
//!
//! The cursor lives behind a `Mutex` because the trait promises `Sync` and Go
//! runs classifications under a worker pool; within a single classification the
//! calls are sequential, so the lock is uncontended.
//!
//! # What is wired, and what is not
//!
//! The **local** seams are complete: [`ContentResolver::content_by_id`] and
//! [`ContentResolver::content_by_search`] map onto the tape's
//! `local.content_by_id` and `local.content_by_search` observations, which is
//! where the load-bearing nondeterminism lives (the ordered candidate list).
//!
//! 🚨 The **TMDB** methods deliberately fail with [`ResolveError::Unsupported`].
//! The tape records TMDB at the *HTTP* level (`tmdb.request` carrying method,
//! path and query parameters), not at trait-method level, so replaying them
//! requires Rust to rebuild Go's exact request URLs and to decode base64 HTTP
//! bodies into the response DTOs. Rust has no TMDB client yet —
//! `bitmagnet-tmdb` is still a placeholder — and guessing the request shape
//! would produce desyncs that read as port bugs rather than as missing code.
//! Failing loudly is the honest behaviour until that lane lands.

use std::sync::Mutex;

use async_trait::async_trait;
use bitmagnet_fts::Tsvector;
use bitmagnet_model::{Content, ContentType};
use bitmagnet_tape::{Answer, Replay, Session, TapeError};
use serde::{Deserialize, Serialize};

use super::{ContentResolver, ContentResultItem, ResolveError};

/// Observation kinds, matching Go's `tape_local_search.go`.
const KIND_CONTENT_BY_SEARCH: &str = "local.content_by_search";
const KIND_CONTENT_BY_ID: &str = "local.content_by_id";

/// Query-shape constants shared with the recording. Go keeps these in
/// `internal/classifier/search.go` precisely so a recorded request cannot drift
/// from the query that produced it; the same reasoning applies here.
const CONTENT_BY_SEARCH_ORDER_BY: &str = "queryStringRank,identity";
const CONTENT_BY_SEARCH_LIMIT: i64 = 10;
const CONTENT_BY_ID_LIMIT: i64 = 1;
const CONTENT_BY_ID_ALTERNATIVE_ORDER_BY: &str = "identity";
const CANONICAL_IDENTIFIER_SOURCE: &str = "tmdb";
const IDENTIFIER_CANONICAL: &str = "canonical";
const IDENTIFIER_ALTERNATIVE: &str = "alternative";

/// Go's `localContentBySearchRequest`.
///
/// 🚨 Field order is load-bearing. Go emits struct fields in declaration order
/// and the request is compared byte for byte, so reordering these silently
/// desyncs every observation.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ContentBySearchRequest<'a> {
    content_type: &'a str,
    base_title: &'a str,
    /// The string actually handed to the query builder, not the base title it
    /// was derived from: a port that quotes or normalises differently is asking
    /// a different question even when its answer coincides.
    search_string: String,
    year: Option<u16>,
    release_date_range: Option<DateRange>,
    order_by: &'static str,
    limit: i64,
}

/// Go's `tapeDateRange`, formatted `2006-01-02`.
#[derive(Debug, Serialize)]
struct DateRange {
    start: String,
    end: String,
}

/// Go's `localContentByIDRequest`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ContentByIdRequest<'a> {
    content_type: &'a str,
    source: &'a str,
    id: &'a str,
    identifier: &'static str,
    order_by: &'static str,
    limit: i64,
}

/// Go's `model.Content` **as the tape encodes it**.
///
/// 🚨 This is deliberately NOT [`bitmagnet_model::Content`]. Go's struct carries
/// no JSON tags on its nested value types, so `encoding/json` emits them with Go
/// field names — `{"Year":1950,"Month":1,"Day":1}` for a date, and its
/// `sql.Null*`-style wrappers as `{"Bool":false,"Valid":false}`. The shared Rust
/// model represents the same information as plain `Option<T>`, because it is
/// built for a different wire format.
///
/// Rather than reshape the shared model — which other crates decode with their
/// own expectations — the Go-specific encoding lives here, in the adapter that
/// actually talks to Go. Translation happens once, at the boundary.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TapeContent {
    #[serde(rename = "type")]
    content_type: ContentType,
    source: String,
    id: String,
    title: String,
    #[serde(default)]
    release_date: Option<TapeDate>,
    #[serde(default)]
    release_year: Option<u32>,
    #[serde(default)]
    adult: Option<NullBool>,
    #[serde(default)]
    original_language: Option<NullLanguage>,
    #[serde(default)]
    original_title: Option<String>,
    #[serde(default)]
    overview: Option<String>,
    #[serde(default)]
    runtime: Option<NullUint16>,
    #[serde(default)]
    popularity: Option<NullFloat32>,
    #[serde(default)]
    vote_average: Option<NullFloat32>,
    #[serde(default)]
    vote_count: Option<NullUint>,
}

/// Go `model.Date` — untagged, so PascalCase on the wire.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TapeDate {
    year: u16,
    month: u8,
    day: u8,
}

/// Go's null wrappers. `Valid` false means SQL NULL regardless of the payload,
/// which is why the zero value must not be mistaken for a real reading.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct NullBool {
    bool: bool,
    valid: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct NullLanguage {
    language: String,
    valid: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct NullUint16 {
    uint16: u16,
    valid: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct NullUint {
    uint: u32,
    valid: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct NullFloat32 {
    float32: f32,
    valid: bool,
}

impl From<TapeContent> for Content {
    fn from(taped: TapeContent) -> Self {
        Content {
            content_type: taped.content_type,
            source: taped.source,
            id: taped.id,
            title: taped.title,
            release_date: taped.release_date.map(|date| bitmagnet_model::Date {
                year: date.year,
                month: date.month,
                day: date.day,
            }),
            release_year: taped.release_year,
            adult: taped.adult.and_then(|v| v.valid.then_some(v.bool)),
            original_language: taped
                .original_language
                .and_then(|v| v.valid.then_some(v.language)),
            original_title: taped.original_title,
            overview: taped.overview,
            runtime: taped
                .runtime
                .and_then(|v| v.valid.then_some(u32::from(v.uint16))),
            popularity: taped.popularity.and_then(|v| v.valid.then_some(v.float32)),
            vote_average: taped
                .vote_average
                .and_then(|v| v.valid.then_some(v.float32)),
            vote_count: taped.vote_count.and_then(|v| v.valid.then_some(v.uint)),
            // Timestamps and the search/derived columns are deliberately dropped:
            // Go writes its zero time ("0001-01-01T00:00:00Z") and nulls here, and
            // none of them is an input to the attach decision the tape exists to
            // compare. Carrying them would invent precision the oracle does not have.
            created_at: None,
            updated_at: None,
            tsv: Tsvector::default(),
            collections: Vec::new(),
            attributes: Vec::new(),
        }
    }
}

/// Go's `localContentResponse`.
#[derive(Debug, Deserialize)]
struct ContentResponse {
    items: Vec<ContentItem>,
}

/// Go's `localContentItem`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContentItem {
    /// Recorded as a STRING via Go's `strconv.FormatFloat(f, 'g', -1, 64)`,
    /// because the rank is diagnostic and its exact float formatting should not
    /// become part of the comparison surface.
    query_string_rank: String,
    content: TapeContent,
}

/// Go's `contentSearchString`: the base title wrapped in double quotes, making
/// it a phrase query.
fn content_search_string(base_title: &str) -> String {
    format!("\"{base_title}\"")
}

/// Go's `model.NewDateRangeFromYear`: `[Jan 1 of year, Jan 1 of year+1]`.
fn date_range_from_year(year: u16) -> DateRange {
    DateRange {
        start: format!("{year}-01-01"),
        end: format!("{}-01-01", year.saturating_add(1)),
    }
}

/// A [`ContentResolver`] that answers from a recorded tape. See the module docs.
pub struct TapeContentResolver {
    session: Mutex<Session>,
}

impl TapeContentResolver {
    /// Binds a resolver to one subject's recording.
    ///
    /// A subject the tape has no record for still yields a resolver; its first
    /// lookup reports a miss naming the subject, rather than silently falling
    /// through to a live backend.
    pub fn new(replay: &Replay, subject: &str, attempt: i64) -> Self {
        Self {
            session: Mutex::new(replay.begin(subject, attempt)),
        }
    }

    /// Observations the classification did not consume.
    ///
    /// A run that ends with this non-zero asked *fewer* questions than Go did —
    /// the mirror image of a miss, and just as much a divergence. A harness
    /// should assert it is zero.
    pub fn remaining(&self) -> usize {
        self.session
            .lock()
            .expect("tape session mutex poisoned")
            .remaining()
    }

    /// Consumes one observation, translating tape failures into [`ResolveError`].
    fn next_raw(&self, kind: &str, request: &impl Serialize) -> Result<RawAnswer, ResolveError> {
        let mut session = self.session.lock().expect("tape session mutex poisoned");

        match session.next(kind, request) {
            Ok(Answer::Response(raw)) => Ok(RawAnswer::Response(raw.get().to_owned())),
            Ok(Answer::Failure(err)) => Ok(RawAnswer::Failure {
                kind: err.kind.clone(),
                message: err.message.clone(),
            }),
            // A miss and a desync are both fatal, but they say different things:
            // a miss is "the recording never saw this", a desync is "you asked
            // something else". Both surface verbatim so the distinction survives.
            Err(err @ TapeError::Miss { .. }) => Err(ResolveError::TapeMiss(err.to_string())),
            Err(err) => Err(ResolveError::LocalSearch(err.to_string())),
        }
    }
}

enum RawAnswer {
    Response(String),
    Failure { kind: String, message: String },
}

/// Go's `rebuildLocalSearchError`. The classifier's control flow compares
/// against the context sentinels, so they are reconstructed by *kind* rather
/// than by message text.
fn rebuild_local_search_error(kind: &str, message: &str) -> ResolveError {
    match kind {
        "context_canceled" => ResolveError::LocalSearch("context canceled".into()),
        "context_deadline_exceeded" => {
            ResolveError::LocalSearch("context deadline exceeded".into())
        }
        _ => ResolveError::LocalSearch(message.to_owned()),
    }
}

#[async_trait]
impl ContentResolver for TapeContentResolver {
    async fn content_by_id(
        &self,
        content_type: ContentType,
        source: &str,
        id: &str,
    ) -> Result<Option<Content>, ResolveError> {
        // Go dispatches on the source: "tmdb" matches the canonical identifier
        // (the content primary key) and imposes no ordering, anything else
        // matches an alternative identifier and orders by identity.
        let canonical = source == CANONICAL_IDENTIFIER_SOURCE;

        let request = ContentByIdRequest {
            content_type: content_type.as_str(),
            source,
            id,
            identifier: if canonical {
                IDENTIFIER_CANONICAL
            } else {
                IDENTIFIER_ALTERNATIVE
            },
            order_by: if canonical {
                ""
            } else {
                CONTENT_BY_ID_ALTERNATIVE_ORDER_BY
            },
            limit: CONTENT_BY_ID_LIMIT,
        };

        match self.next_raw(KIND_CONTENT_BY_ID, &request)? {
            RawAnswer::Failure { kind, message } => {
                Err(rebuild_local_search_error(&kind, &message))
            }
            RawAnswer::Response(body) => {
                let response: ContentResponse = serde_json::from_str(&body).map_err(|err| {
                    ResolveError::LocalSearch(format!("decode taped content_by_id response: {err}"))
                })?;

                // An empty list is Go's ErrUnmatched — a real "no such content",
                // not a gap. The gap case is a miss, handled above.
                Ok(response
                    .items
                    .into_iter()
                    .next()
                    .map(|item| item.content.into()))
            }
        }
    }

    async fn content_by_search(
        &self,
        content_type: ContentType,
        base_title: &str,
        year: Option<u16>,
    ) -> Result<Vec<ContentResultItem>, ResolveError> {
        let request = ContentBySearchRequest {
            content_type: content_type.as_str(),
            base_title,
            search_string: content_search_string(base_title),
            year,
            // Go writes year and releaseDateRange together, or neither.
            release_date_range: year.map(date_range_from_year),
            order_by: CONTENT_BY_SEARCH_ORDER_BY,
            limit: CONTENT_BY_SEARCH_LIMIT,
        };

        match self.next_raw(KIND_CONTENT_BY_SEARCH, &request)? {
            RawAnswer::Failure { kind, message } => {
                Err(rebuild_local_search_error(&kind, &message))
            }
            RawAnswer::Response(body) => {
                let response: ContentResponse = serde_json::from_str(&body).map_err(|err| {
                    ResolveError::LocalSearch(format!(
                        "decode taped content_by_search response: {err}"
                    ))
                })?;

                response
                    .items
                    .into_iter()
                    .map(|item| {
                        // The rank is diagnostic; consumers must trust the ORDER.
                        // A malformed rank is still a corrupt tape, so it fails
                        // rather than silently defaulting to zero.
                        let rank = item.query_string_rank.parse::<f64>().map_err(|err| {
                            ResolveError::LocalSearch(format!(
                                "decode taped queryStringRank {:?}: {err}",
                                item.query_string_rank
                            ))
                        })?;

                        Ok(ContentResultItem {
                            content: item.content.into(),
                            query_string_rank: rank,
                        })
                    })
                    .collect()
            }
        }
    }

    async fn tmdb_find_by_external_id(
        &self,
        _request: &super::tmdb::FindByIdRequest,
    ) -> Result<super::tmdb::FindByIdResponse, ResolveError> {
        Err(tmdb_unsupported("find_by_external_id"))
    }

    async fn tmdb_movie_details(
        &self,
        _request: &super::tmdb::MovieDetailsRequest,
    ) -> Result<Option<super::tmdb::MovieDetailsResponse>, ResolveError> {
        Err(tmdb_unsupported("movie_details"))
    }

    async fn tmdb_tv_details(
        &self,
        _request: &super::tmdb::TvDetailsRequest,
    ) -> Result<Option<super::tmdb::TvDetailsResponse>, ResolveError> {
        Err(tmdb_unsupported("tv_details"))
    }

    async fn tmdb_search_movie(
        &self,
        _request: &super::tmdb::SearchMovieRequest,
    ) -> Result<super::tmdb::SearchMovieResponse, ResolveError> {
        Err(tmdb_unsupported("search_movie"))
    }

    async fn tmdb_search_tv(
        &self,
        _request: &super::tmdb::SearchTvRequest,
    ) -> Result<super::tmdb::SearchTvResponse, ResolveError> {
        Err(tmdb_unsupported("search_tv"))
    }
}

fn tmdb_unsupported(method: &str) -> ResolveError {
    ResolveError::Unsupported(format!(
        "tape replay of tmdb.{method} is not wired: the tape records TMDB at the HTTP level \
         (method/path/queryParams), so replaying it needs Rust to rebuild Go's request URLs and \
         decode base64 bodies into the response DTOs. bitmagnet-tmdb is still a placeholder; \
         guessing the request shape would desync and read as a port bug"
    ))
}
