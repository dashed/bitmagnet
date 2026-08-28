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
//! The **TMDB** seam is wired too, at the level Go records it: `tmdb.request`
//! carries `{method, path, queryParams}`, so replaying it means rebuilding Go's
//! exact request — not its trait-method arguments — and decoding a base64 HTTP
//! body into the response DTO. Both halves are ported from
//! `internal/tmdb/client.go` (URL construction) and
//! `internal/tmdb/requester_recorder.go` (the record shape).
//!
//! 🚨 Two details there are load-bearing:
//!
//! * `queryParams` is a Go **map**, and `encoding/json` **sorts map keys**. The
//!   request is compared byte for byte, so it is built in a [`BTreeMap`] — a
//!   `HashMap` would desync nondeterministically.
//! * The recorded `bodySha256` is **verified** before decoding, exactly as Go's
//!   `replayRequest` does. A tape whose body no longer hashes to its digest is
//!   corrupt, and silently decoding it would launder that into a parity result.

use std::collections::BTreeMap;
use std::sync::Mutex;

use base64::Engine as _;
use sha2::{Digest, Sha256};

use async_trait::async_trait;
use bitmagnet_fts::Tsvector;
use bitmagnet_model::{Content, ContentType};
use bitmagnet_tape::{Answer, Replay, Session, TapeError};
use serde::{Deserialize, Serialize};

use super::{ContentResolver, ContentResultItem, ResolveError};

/// Observation kinds, matching Go's `tape_local_search.go`.
const KIND_CONTENT_BY_SEARCH: &str = "local.content_by_search";
const KIND_CONTENT_BY_ID: &str = "local.content_by_id";
/// Go `tmdb.TapeKindRequest`.
const KIND_TMDB_REQUEST: &str = "tmdb.request";

/// Go's recorded TMDB error kinds (`internal/tmdb/requester_recorder.go`).
///
/// They are kept apart because the classifier's control flow depends on which
/// one it got: `not_found` becomes `ErrUnmatched` and `find_match` falls through
/// to the next branch, while everything else is fatal to the classification.
/// Flattening them would change control flow.
const TMDB_ERR_UNAUTHORIZED: &str = "unauthorized";
const TMDB_ERR_NOT_FOUND: &str = "not_found";

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
    #[serde(default)]
    collections: Option<Vec<bitmagnet_model::ContentCollection>>,
    #[serde(default)]
    attributes: Option<Vec<bitmagnet_model::ContentAttribute>>,
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
            collections: taped.collections.unwrap_or_default(),
            attributes: taped.attributes.unwrap_or_default(),
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
    ///
    /// `seam` only selects which [`ResolveError`] variant a non-miss tape failure
    /// (a desync, a corrupt record) is reported under, so the error names the
    /// dependency the caller was actually talking to.
    fn next_raw(
        &self,
        seam: Seam,
        kind: &str,
        request: &impl Serialize,
    ) -> Result<RawAnswer, ResolveError> {
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
            Err(err) => Err(seam.error(err.to_string())),
        }
    }

    /// Replays one `tmdb.request` — Go `tmdb.replayRequest`.
    ///
    /// Returns the raw response body, or [`None`] for a recorded 404, which Go's
    /// callers turn into `ErrUnmatched`.
    fn tmdb_next(&self, request: &TmdbRequest) -> Result<Option<Vec<u8>>, ResolveError> {
        match self.next_raw(Seam::Tmdb, KIND_TMDB_REQUEST, request)? {
            RawAnswer::Failure { kind, message } => match kind.as_str() {
                // Go returns the package sentinels themselves here, because
                // callers reach them with errors.Is and a look-alike would not
                // match. The Rust equivalent is this distinction in the return
                // type, which is why not_found is Ok(None) and not an error.
                TMDB_ERR_NOT_FOUND => Ok(None),
                TMDB_ERR_UNAUTHORIZED => Err(ResolveError::Tmdb("401 Unauthorized".to_owned())),
                _ => Err(ResolveError::Tmdb(message)),
            },
            RawAnswer::Response(raw) => {
                let response: TmdbResponse = serde_json::from_str(&raw).map_err(|err| {
                    ResolveError::Tmdb(format!("decode taped tmdb.request response: {err}"))
                })?;

                let body = base64::engine::general_purpose::STANDARD
                    .decode(&response.body_base64)
                    .map_err(|err| {
                        ResolveError::Tmdb(format!("decode taped tmdb.request body: {err}"))
                    })?;

                // Go verifies this before decoding, and so must we: a body that
                // no longer matches its digest is a corrupt tape, and decoding it
                // anyway would launder corruption into a parity verdict.
                let digest = format!("sha256:{:x}", Sha256::digest(&body));
                if digest != response.body_sha256 {
                    return Err(ResolveError::Tmdb(format!(
                        "taped tmdb.request response body digest is {digest}, but the recorded \
                         digest is {}",
                        response.body_sha256
                    )));
                }

                Ok(Some(body))
            }
        }
    }
}

/// Which dependency a tape lookup was for. See [`TapeContentResolver::next_raw`].
#[derive(Debug, Clone, Copy)]
enum Seam {
    Local,
    Tmdb,
}

impl Seam {
    fn error(self, message: String) -> ResolveError {
        match self {
            Self::Local => ResolveError::LocalSearch(message),
            Self::Tmdb => ResolveError::Tmdb(message),
        }
    }
}

/// Go's `tmdb.tapeRequest` (`internal/tmdb/requester_recorder.go`).
///
/// 🚨 `query_params` is a [`BTreeMap`] on purpose: Go records it as a `map` and
/// `encoding/json` sorts map keys, while the request is compared byte for byte.
/// Field order is likewise Go's declaration order.
#[derive(Debug, Serialize)]
struct TmdbRequest {
    method: &'static str,
    path: String,
    #[serde(rename = "queryParams")]
    query_params: BTreeMap<String, String>,
}

/// Go's `tmdb.tapeResponse`.
#[derive(Debug, Deserialize)]
struct TmdbResponse {
    #[serde(rename = "bodyBase64", default)]
    body_base64: String,
    #[serde(rename = "bodySha256", default)]
    body_sha256: String,
}

/// Query parameters, built the way `internal/tmdb/client.go` builds them.
///
/// Every `insert` here mirrors one `if` in that file. A parameter Go omits must
/// be omitted, not sent empty: the recorded request carries only what Go sent, so
/// an extra key is a desync.
struct QueryParams(BTreeMap<String, String>);

impl QueryParams {
    fn new() -> Self {
        // Go always builds a non-nil map, so an absent parameter set and an
        // empty one encode identically.
        Self(BTreeMap::new())
    }

    fn set(&mut self, key: &str, value: impl Into<String>) {
        self.0.insert(key.to_owned(), value.into());
    }

    /// Go `model.NullString` — present iff `Valid`.
    fn set_opt(&mut self, key: &str, value: Option<&String>) {
        if let Some(value) = value {
            self.set(key, value.clone());
        }
    }

    /// Go `model.Year` — `IsNil()` is the zero value, so 0 is omitted just as
    /// `None` is. Rendered with `Year.String()`, i.e. plain decimal.
    fn set_year(&mut self, key: &str, year: Option<u16>) {
        if let Some(year) = year.filter(|y| *y != 0) {
            self.set(key, year.to_string());
        }
    }

    /// Go `strings.Join(request.AppendToResponse, ",")`, omitted when empty.
    fn set_append_to_response(&mut self, values: &[String]) {
        if !values.is_empty() {
            self.set("append_to_response", values.join(","));
        }
    }

    fn get(self, path: String) -> TmdbRequest {
        TmdbRequest {
            method: "GET",
            path,
            query_params: self.0,
        }
    }
}

/// Go `client.SearchMovie`. Insertion order is irrelevant (the map sorts); the
/// SET of keys is what has to match.
fn search_movie_request(request: &super::tmdb::SearchMovieRequest) -> TmdbRequest {
    let mut params = QueryParams::new();
    params.set("query", request.query.clone());
    if request.include_adult {
        params.set("include_adult", "true");
    }
    params.set_opt("language", request.language.as_ref());
    params.set_year("primary_release_year", request.primary_release_year);
    params.set_year("year", request.year);
    params.set_opt("region", request.region.as_ref());
    params.get("/search/movie".to_owned())
}

/// Go `client.SearchTv`.
///
/// 🚨 `SearchTvRequest` carries a `year`, but `client.SearchTv` never sends it.
/// Sending it would be an extra query parameter, i.e. a desync.
fn search_tv_request(request: &super::tmdb::SearchTvRequest) -> TmdbRequest {
    let mut params = QueryParams::new();
    params.set("query", request.query.clone());
    params.set_year("first_air_date_year", request.first_air_date_year);
    if request.include_adult {
        params.set("include_adult", "true");
    }
    params.set_opt("language", request.language.as_ref());
    params.get("/search/tv".to_owned())
}

/// Go `client.MovieDetails`.
fn movie_details_request(request: &super::tmdb::MovieDetailsRequest) -> TmdbRequest {
    let mut params = QueryParams::new();
    params.set_append_to_response(&request.append_to_response);
    params.set_opt("language", request.language.as_ref());
    params.get(format!("/movie/{}", request.id))
}

/// Go `client.TvDetails`.
fn tv_details_request(request: &super::tmdb::TvDetailsRequest) -> TmdbRequest {
    let mut params = QueryParams::new();
    params.set_append_to_response(&request.append_to_response);
    params.set_opt("language", request.language.as_ref());
    params.get(format!("/tv/{}", request.series_id))
}

/// Go `client.FindByID`.
fn find_by_id_request(request: &super::tmdb::FindByIdRequest) -> TmdbRequest {
    let mut params = QueryParams::new();
    params.set("external_source", request.external_source.clone());
    params.set_opt("language", request.language.as_ref());
    params.get(format!("/find/{}", request.external_id))
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

        match self.next_raw(Seam::Local, KIND_CONTENT_BY_ID, &request)? {
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

        match self.next_raw(Seam::Local, KIND_CONTENT_BY_SEARCH, &request)? {
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

    /// Go `client.FindByID` → `GET /find/{external_id}`.
    async fn tmdb_find_by_external_id(
        &self,
        request: &super::tmdb::FindByIdRequest,
    ) -> Result<super::tmdb::FindByIdResponse, ResolveError> {
        let taped = self.tmdb_next(&find_by_id_request(request))?;

        // Go's FindByID has no 404 special case — the error propagates — so a
        // recorded not_found is a genuine failure of this call, not an absence.
        decode_tmdb(taped, "find")?.ok_or_else(|| ResolveError::Tmdb("404 Not Found".to_owned()))
    }

    /// Go `client.MovieDetails` → `GET /movie/{id}`.
    async fn tmdb_movie_details(
        &self,
        request: &super::tmdb::MovieDetailsRequest,
    ) -> Result<Option<super::tmdb::MovieDetailsResponse>, ResolveError> {
        let taped = self.tmdb_next(&movie_details_request(request))?;

        // A recorded 404 is Go's ErrNotFound, which `tmdbGetMovieByTMDBID` maps
        // to ErrUnmatched — hence Ok(None) rather than an error.
        decode_tmdb(taped, "movie details")
    }

    /// Go `client.TvDetails` → `GET /tv/{series_id}`.
    async fn tmdb_tv_details(
        &self,
        request: &super::tmdb::TvDetailsRequest,
    ) -> Result<Option<super::tmdb::TvDetailsResponse>, ResolveError> {
        let taped = self.tmdb_next(&tv_details_request(request))?;

        decode_tmdb(taped, "tv details")
    }

    /// Go `client.SearchMovie` → `GET /search/movie`.
    async fn tmdb_search_movie(
        &self,
        request: &super::tmdb::SearchMovieRequest,
    ) -> Result<super::tmdb::SearchMovieResponse, ResolveError> {
        let taped = self.tmdb_next(&search_movie_request(request))?;

        decode_tmdb(taped, "search movie")?
            .ok_or_else(|| ResolveError::Tmdb("404 Not Found".to_owned()))
    }

    /// Go `client.SearchTv` → `GET /search/tv`.
    async fn tmdb_search_tv(
        &self,
        request: &super::tmdb::SearchTvRequest,
    ) -> Result<super::tmdb::SearchTvResponse, ResolveError> {
        let taped = self.tmdb_next(&search_tv_request(request))?;

        decode_tmdb(taped, "search tv")?
            .ok_or_else(|| ResolveError::Tmdb("404 Not Found".to_owned()))
    }
}

/// Decodes a replayed TMDB body into its DTO, preserving the 404 → [`None`]
/// distinction the caller depends on.
///
/// Go hands the raw bytes to `json.Unmarshal` and leaves absent fields at their
/// zero value; the DTOs carry `#[serde(default)]` throughout for the same reason,
/// so this is a plain decode.
fn decode_tmdb<T: serde::de::DeserializeOwned>(
    body: Option<Vec<u8>>,
    what: &str,
) -> Result<Option<T>, ResolveError> {
    body.map(|body| {
        serde_json::from_slice(&body)
            .map_err(|err| ResolveError::Tmdb(format!("decode taped {what} response: {err}")))
    })
    .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::tmdb;

    /// The fixtures below are **verbatim `request` objects from the production
    /// tape** (`testdata/parity/classifier-attach/prod-20260809`), i.e. requests
    /// Go actually made and recorded. Go compares the replayed request to the
    /// recorded one BYTE FOR BYTE, so matching these strings exactly is the whole
    /// contract — a reordered key or an extra parameter is a desync that reads as
    /// a port bug.
    fn encoded(request: &TmdbRequest) -> String {
        serde_json::to_string(request).expect("request serialises")
    }

    #[test]
    fn search_tv_matches_the_recorded_request() {
        let request = search_tv_request(&tmdb::SearchTvRequest {
            query: "UFC Fight Night Kape vs Horiguchi 20 05".to_owned(),
            include_adult: true,
            first_air_date_year: Some(2026),
            ..Default::default()
        });

        assert_eq!(
            encoded(&request),
            r#"{"method":"GET","path":"/search/tv","queryParams":{"first_air_date_year":"2026","include_adult":"true","query":"UFC Fight Night Kape vs Horiguchi 20 05"}}"#
        );
    }

    /// Go always appends `external_ids` for TV details, and the transform reads
    /// the imdb/tvdb ids out of it.
    #[test]
    fn tv_details_matches_the_recorded_request() {
        let request = tv_details_request(&tmdb::TvDetailsRequest {
            series_id: 12271,
            append_to_response: vec!["external_ids".to_owned()],
            ..Default::default()
        });

        assert_eq!(
            encoded(&request),
            r#"{"method":"GET","path":"/tv/12271","queryParams":{"append_to_response":"external_ids"}}"#
        );
    }

    /// Movie details carries NO query parameters — and the empty map must encode
    /// as `{}`, never as `null`. Go always builds a non-nil map for exactly this
    /// reason.
    #[test]
    fn movie_details_matches_the_recorded_request() {
        let request = movie_details_request(&tmdb::MovieDetailsRequest {
            id: 1_673_194,
            ..Default::default()
        });

        assert_eq!(
            encoded(&request),
            r#"{"method":"GET","path":"/movie/1673194","queryParams":{}}"#
        );
    }

    /// Query parameters are a Go map, so `encoding/json` sorts the keys. A
    /// `HashMap` here would pass or fail at random.
    #[test]
    fn query_params_are_key_sorted() {
        let request = search_movie_request(&tmdb::SearchMovieRequest {
            query: "Cinderella".to_owned(),
            include_adult: true,
            year: Some(1950),
            region: Some("US".to_owned()),
            ..Default::default()
        });

        assert_eq!(
            encoded(&request),
            r#"{"method":"GET","path":"/search/movie","queryParams":{"include_adult":"true","query":"Cinderella","region":"US","year":"1950"}}"#
        );
    }

    /// Go's `model.Year` zero value is nil and `client.SearchMovie` skips a nil
    /// year, so a zero must be omitted rather than sent as `"0"`.
    #[test]
    fn a_zero_year_is_omitted_like_gos_nil() {
        let request = search_movie_request(&tmdb::SearchMovieRequest {
            query: "Cinderella".to_owned(),
            year: Some(0),
            ..Default::default()
        });

        assert_eq!(
            encoded(&request),
            r#"{"method":"GET","path":"/search/movie","queryParams":{"query":"Cinderella"}}"#
        );
    }

    /// `client.SearchTv` never sends the `year` field its request struct carries.
    #[test]
    fn search_tv_never_sends_year() {
        let request = search_tv_request(&tmdb::SearchTvRequest {
            query: "Cinderella".to_owned(),
            year: Some(1950),
            ..Default::default()
        });

        assert!(
            !encoded(&request).contains("\"year\""),
            "client.SearchTv sends first_air_date_year only: {}",
            encoded(&request)
        );
    }

    #[test]
    fn find_by_id_builds_the_external_source_query() {
        let request = find_by_id_request(&tmdb::FindByIdRequest {
            external_source: "imdb_id".to_owned(),
            external_id: "tt0042332".to_owned(),
            language: None,
        });

        assert_eq!(
            encoded(&request),
            r#"{"method":"GET","path":"/find/tt0042332","queryParams":{"external_source":"imdb_id"}}"#
        );
    }
}
