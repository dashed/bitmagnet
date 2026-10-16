//! The live TMDB client — Go's `internal/tmdb` requester chain.
//!
//! # The chain, in Go's order
//!
//! `factory.go` builds `NewClient(&requesterLazy{…})`, and `NewClient` wraps
//! that in the tape recorder, so a request descends:
//!
//! ```text
//! recorder            requester_recorder.go   <- NOT here; see below
//!   lazy              requester_lazy.go       once: build + ValidateAPIKey
//!     logger          requester_logger.go     debug/error with path + params
//!       fail-fast     requester_fail_fast.go  401 latch, process lifetime
//!         semaphore   requester_semaphore.go  2 concurrent requests
//!           limiter   requester_limiter.go    rate.Limiter.Wait
//!             resty   requester.go            retry(3) + 10s timeout, status -> error
//! ```
//!
//! Each layer below is a method named after its Go file, calling the next in
//! that order, so the nesting is checkable by reading rather than by inference.
//! Two places where the order is load-bearing:
//!
//! * The **limiter is inside the semaphore**, so at most two callers ever queue
//!   for a token.
//! * The **retry is inside the limiter**, so a retried attempt spends no token
//!   and can exceed the configured rate. Go gets this by putting retry inside
//!   resty; it is behaviour, not an accident of layering.
//!
//! # The recorder is deliberately absent
//!
//! Go's tape seam sits at the very top of the chain, above the lazy
//! initialisation, so a replay never builds a live requester. In Rust the replay
//! side is a *separate implementation* of the same seam
//! (`bitmagnet_classifier::resolver::tape::TapeContentResolver`), chosen at
//! construction instead of consulted per request. So this client is the live
//! branch only, and there is nothing here for a recording context to intercept.
//!
//! # 🚨 The credential
//!
//! The `api_key` is a **client-level** query parameter (Go
//! `requester_lazy.go:66`, resty's `SetQueryParam`), attached in
//! [`TmdbClient::url`] when a [`TmdbRequestSpec`] becomes a URL. It never enters
//! a request spec, is never logged, and [`ApiKey`]'s `Debug` redacts it. That is
//! why a recorded tape — which records specs — contains no secret.
//!
//! # Deliberate differences from Go, and why
//!
//! * **No fallback to the upstream default API key.** Go's `newRequester`
//!   answers a 401 by retrying with a hardcoded public key and downgrading the
//!   rate limit to 1/s burst 8 (`requester_lazy.go:84-93`). Reproducing that
//!   means embedding a credential in this source. The unauthorized error is
//!   surfaced instead; a deployment that wants the fallback configures the key
//!   and the rate limit it implies. Request fidelity is unaffected.
//! * **No `ctx` plumbing.** Go threads a `context.Context` through every layer
//!   for cancellation; Rust cancels by dropping the future, which stops the
//!   chain at its next await point. The observable difference is that Go's
//!   `limiter.Wait` and `semaphore.Acquire` return a context error where here
//!   the future simply never resumes.
//! * **Reason phrases** in [`TmdbError::Http`] come from the canonical status
//!   table, not the server's bytes (hyper discards those). No branch reads them.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bitmagnet_classifier::resolver::tmdb::{
    FindByIdRequest, FindByIdResponse, MovieDetailsRequest, MovieDetailsResponse,
    SearchMovieRequest, SearchMovieResponse, SearchTvRequest, SearchTvResponse, TvDetailsRequest,
    TvDetailsResponse,
};
use bitmagnet_classifier::ResolveError;
use reqwest::Url;
use serde::de::DeserializeOwned;
use tokio::sync::{OnceCell, Semaphore};

use crate::error::TmdbError;
use crate::limiter::RateLimiter;
use crate::request as requests;
use crate::request::TmdbRequestSpec;
use crate::transport::{
    execute_with_retry, HttpResponse, ReqwestTransport, RetryPolicy, Transport,
};

/// Go `semaphore.NewWeighted(2)` (`requester_lazy.go:76`).
const MAX_CONCURRENT_REQUESTS: usize = 2;

/// A TMDB API key.
///
/// A newtype rather than a `String` so it cannot be printed by accident: the
/// `Debug` is redacted and there is no `Display`. Reading it back out is
/// `pub(crate)`, and its one caller is [`TmdbClient::url`].
#[derive(Clone, PartialEq, Eq)]
pub struct ApiKey(String);

impl ApiKey {
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl From<String> for ApiKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for ApiKey {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiKey(<redacted>)")
    }
}

/// Go `tmdb.Config`, plus the two resty settings Go hardcodes.
///
/// The retry policy and timeout are configurable here only because a test that
/// had to wait out a real 2s backoff would not be written; production should
/// leave them at Go's values.
#[derive(Debug, Clone)]
pub struct TmdbConfig {
    pub enabled: bool,
    pub base_url: String,
    pub api_key: ApiKey,
    /// Go `rate.Every(RateLimit)`. Zero is `rate.Inf` — no limiting.
    pub rate_limit: Duration,
    pub rate_limit_burst: u32,
    /// Go `SetTimeout`, applied per attempt.
    pub timeout: Duration,
    pub retry: RetryPolicy,
}

impl TmdbConfig {
    /// Go `NewDefaultConfig`, minus its hardcoded default API key — see the
    /// module docs on the fallback.
    #[must_use]
    pub fn new(api_key: impl Into<ApiKey>) -> Self {
        Self {
            enabled: true,
            base_url: "https://api.themoviedb.org/3".to_owned(),
            api_key: api_key.into(),
            // `defaultRateLimit` / `defaultRateLimitBurst` (`config.go:25`).
            rate_limit: Duration::from_secs(1) / 20,
            rate_limit_burst: 5,
            timeout: Duration::from_secs(10),
            retry: RetryPolicy::default(),
        }
    }
}

/// The live TMDB client.
///
/// Holds the five calls `bitmagnet_classifier::ContentResolver` makes, with the
/// trait's exact signatures and error type, so a composite resolver can delegate
/// without adapting anything. It deliberately does **not** implement the trait:
/// the trait's other two methods are the local PostgreSQL search, which belongs
/// to a different crate.
pub struct TmdbClient<T = ReqwestTransport> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    enabled: bool,
    base_url: String,
    api_key: ApiKey,
    retry: RetryPolicy,
    limiter: RateLimiter,
    semaphore: Semaphore,
    /// Go `requesterFailFast.isUnauthorized`.
    unauthorized: AtomicBool,
    /// Go `requesterLazy.once` + its latched `err`.
    init: OnceCell<Result<(), TmdbError>>,
    transport: T,
}

impl<T> Clone for TmdbClient<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl TmdbClient<ReqwestTransport> {
    /// Builds a client over the live HTTP transport.
    ///
    /// No request is made here, and none is made until the first call: Go defers
    /// construction *and* API-key validation to first use so that a process
    /// which never touches TMDB never fails on it.
    ///
    /// # Errors
    ///
    /// If the HTTP client cannot be built (TLS initialisation).
    pub fn new(config: TmdbConfig) -> Result<Self, TmdbError> {
        let transport = ReqwestTransport::new(config.timeout)?;
        Ok(Self::with_transport(config, transport))
    }
}

impl<T: Transport> TmdbClient<T> {
    /// Builds a client over an arbitrary transport — the seam the offline
    /// conformance tests drive.
    pub fn with_transport(config: TmdbConfig, transport: T) -> Self {
        Self {
            inner: Arc::new(Inner {
                enabled: config.enabled,
                // resty's `SetBaseURL` trims trailing slashes; paths are built
                // with a leading one.
                base_url: config.base_url.trim_end_matches('/').to_owned(),
                api_key: config.api_key,
                retry: config.retry,
                limiter: RateLimiter::new(config.rate_limit, config.rate_limit_burst),
                semaphore: Semaphore::new(MAX_CONCURRENT_REQUESTS),
                unauthorized: AtomicBool::new(false),
                init: OnceCell::new(),
                transport,
            }),
        }
    }

    // -- the five seam calls -------------------------------------------------
    //
    // Signatures and error type match `ContentResolver`'s TMDB half exactly.
    // Where they return `Option`, `None` is Go's `ErrNotFound` reaching a caller
    // that turns it into `ErrUnmatched` — the same split the classifier's tape
    // resolver makes, so a live run and a replayed one are interchangeable.

    /// `GET /find/{external_id}` — Go `tmdb.Client.FindByID`.
    ///
    /// Go's `FindByID` has **no** 404 special case: the error propagates, so a
    /// 404 here is a failure and not an absence.
    ///
    /// # Errors
    ///
    /// Any TMDB failure, including a 404.
    pub async fn find_by_external_id(
        &self,
        request: &FindByIdRequest,
    ) -> Result<FindByIdResponse, ResolveError> {
        Ok(self.request(&requests::find_by_id(request)).await?)
    }

    /// `GET /movie/{id}` — Go `tmdb.Client.MovieDetails`. `Ok(None)` is
    /// `ErrNotFound`, which `tmdbGetMovieByTMDBID` maps to `ErrUnmatched`.
    ///
    /// # Errors
    ///
    /// Any TMDB failure other than a 404.
    pub async fn movie_details(
        &self,
        request: &MovieDetailsRequest,
    ) -> Result<Option<MovieDetailsResponse>, ResolveError> {
        optional(self.request(&requests::movie_details(request)).await)
    }

    /// `GET /tv/{series_id}` — Go `tmdb.Client.TvDetails`. `Ok(None)` is
    /// `ErrNotFound`.
    ///
    /// # Errors
    ///
    /// Any TMDB failure other than a 404.
    pub async fn tv_details(
        &self,
        request: &TvDetailsRequest,
    ) -> Result<Option<TvDetailsResponse>, ResolveError> {
        optional(self.request(&requests::tv_details(request)).await)
    }

    /// `GET /search/movie` — Go `tmdb.Client.SearchMovie`. Returns the raw
    /// ordered `results` array; the Levenshtein pick happens caller-side.
    ///
    /// # Errors
    ///
    /// Any TMDB failure.
    pub async fn search_movie(
        &self,
        request: &SearchMovieRequest,
    ) -> Result<SearchMovieResponse, ResolveError> {
        Ok(self.request(&requests::search_movie(request)).await?)
    }

    /// `GET /search/tv` — Go `tmdb.Client.SearchTv`.
    ///
    /// # Errors
    ///
    /// Any TMDB failure.
    pub async fn search_tv(
        &self,
        request: &SearchTvRequest,
    ) -> Result<SearchTvResponse, ResolveError> {
        Ok(self.request(&requests::search_tv(request)).await?)
    }

    /// Go `client.ValidateAPIKey` — `GET /authentication`.
    ///
    /// Calling this on a fresh client issues **two** requests, exactly as Go
    /// does: the lazy initialiser validates on the way through, and then this
    /// call is made.
    ///
    /// # Errors
    ///
    /// [`TmdbError::Unauthorized`] for a rejected key, or any other failure.
    pub async fn validate_api_key(&self) -> Result<(), TmdbError> {
        self.ensure_initialised().await?;
        self.logged(&requests::validate_api_key()).await.map(|_| ())
    }

    /// Issues `spec` and decodes the response.
    ///
    /// The four failure classes stay apart here — see [`TmdbError`]. Callers
    /// that need a 404 as an absence rather than a failure use [`optional`].
    ///
    /// # Errors
    ///
    /// Any of [`TmdbError`]'s variants.
    pub async fn request<R: DeserializeOwned + Default>(
        &self,
        spec: &TmdbRequestSpec,
    ) -> Result<R, TmdbError> {
        self.ensure_initialised().await?;
        let response = self.logged(spec).await?;
        decode(&response)
    }

    // -- the chain -----------------------------------------------------------

    /// Go `requesterLazy`: build once, validate once, and **latch whatever
    /// happened** — including a failure.
    ///
    /// 🚨 The latch covers errors, not just success. Go stores `r.err` under a
    /// `sync.Once`, so a single transport failure during validation disables
    /// TMDB for the life of the process; a `get_or_try_init` here would quietly
    /// retry and be *more* available than Go. That is a divergence in the
    /// direction that looks like an improvement, which is the kind that ends up
    /// in a parity report.
    async fn ensure_initialised(&self) -> Result<(), TmdbError> {
        self.inner
            .init
            .get_or_init(|| async {
                if !self.inner.enabled {
                    return Err(TmdbError::Disabled);
                }

                // Validation descends the rest of the chain, so a 401 here
                // latches the fail-fast gate too.
                self.logged(&requests::validate_api_key()).await.map(|_| ())
            })
            .await
            .clone()
    }

    /// Go `requesterLogger`.
    ///
    /// 🚨 Logs the path and parameters, never the URL: the URL carries the
    /// credential.
    async fn logged(&self, spec: &TmdbRequestSpec) -> Result<HttpResponse, TmdbError> {
        let result = self.fail_fast(spec).await;

        match &result {
            Ok(response) => tracing::debug!(
                path = %spec.path,
                query_params = ?spec.query_params,
                status = response.status,
                "request succeeded"
            ),
            Err(err) => tracing::error!(
                path = %spec.path,
                query_params = ?spec.query_params,
                error = %err,
                "request failed"
            ),
        }

        result
    }

    /// Go `requesterFailFast`: once unauthorized, every later call fails
    /// immediately without a request.
    ///
    /// The latch is process-lifetime because the requester it lives on is built
    /// once, under the lazy `sync.Once`. Its point is that a revoked key cannot
    /// produce a request storm.
    async fn fail_fast(&self, spec: &TmdbRequestSpec) -> Result<HttpResponse, TmdbError> {
        if self.inner.unauthorized.load(Ordering::Acquire) {
            return Err(TmdbError::Unauthorized);
        }

        let result = self.with_semaphore(spec).await;
        if matches!(result, Err(TmdbError::Unauthorized)) {
            self.inner.unauthorized.store(true, Ordering::Release);
        }

        result
    }

    /// Go `requesterSemaphore`: at most two requests in flight.
    async fn with_semaphore(&self, spec: &TmdbRequestSpec) -> Result<HttpResponse, TmdbError> {
        let _permit = self
            .inner
            .semaphore
            .acquire()
            .await
            .expect("semaphore is never closed");

        self.rate_limited(spec).await
    }

    /// Go `requesterLimiter`.
    async fn rate_limited(&self, spec: &TmdbRequestSpec) -> Result<HttpResponse, TmdbError> {
        self.inner.limiter.wait().await;
        self.send(spec).await
    }

    /// Go `requester` + resty: retry the transport, then map the status onto a
    /// sentinel.
    async fn send(&self, spec: &TmdbRequestSpec) -> Result<HttpResponse, TmdbError> {
        let url = self.url(spec)?;
        let response = execute_with_retry(&self.inner.transport, &url, &self.inner.retry)
            .await
            .map_err(TmdbError::Transport)?;

        match TmdbError::from_status(response.status) {
            Some(err) => Err(err),
            None => Ok(response),
        }
    }

    /// Turns a spec into the URL to fetch, adding the credential.
    ///
    /// 🚨 This is the *only* place the api_key is used, mirroring resty's
    /// client-level `SetQueryParam`. Go renders the query with
    /// `url.Values.Encode()`, which sorts keys, so the credential is merged into
    /// the sorted set rather than appended — same bytes on the wire.
    ///
    /// An unparseable base URL is a [`TmdbError::Transport`], which is where
    /// resty puts it too (the error comes back from `Get`, with no response).
    /// The message cannot contain the key: `url::ParseError` does not quote its
    /// input.
    fn url(&self, spec: &TmdbRequestSpec) -> Result<Url, TmdbError> {
        let mut url = Url::parse(&format!("{}{}", self.inner.base_url, spec.path))
            .map_err(|err| TmdbError::Transport(format!("invalid TMDB URL: {err}")))?;

        let mut params: BTreeMap<&str, &str> = spec
            .query_params
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        params.insert("api_key", self.inner.api_key.expose());

        {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in params {
                pairs.append_pair(key, value);
            }
        }

        Ok(url)
    }
}

/// Go's `ErrNotFound` → `ErrUnmatched` translation, as a return type.
fn optional<R>(result: Result<R, TmdbError>) -> Result<Option<R>, ResolveError> {
    match result {
        Ok(response) => Ok(Some(response)),
        Err(TmdbError::NotFound) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// resty's `parseResponseBody` (`middleware.go:397`), which is stricter about
/// *when* it decodes than it looks.
///
/// 🚨 A 204, or any response whose content type is not JSON, is **not** decoded
/// and is **not** an error: Go leaves the result at its zero value and returns
/// nil. So a proxy answering `200 text/html` yields an empty response and the
/// classifier reads that as "no results" — an unmatched, not an error. Returning
/// a decode error there would turn one parity outcome into another, which is why
/// this reproduces the rule rather than doing the obvious thing.
fn decode<R: DeserializeOwned + Default>(response: &HttpResponse) -> Result<R, TmdbError> {
    let content_type = response.content_type.as_deref().unwrap_or_default();
    if response.status == 204 || !is_json_content_type(content_type) {
        return Ok(R::default());
    }

    serde_json::from_slice(&response.body).map_err(|err| TmdbError::Decode(err.to_string()))
}

/// resty's `IsJSONType`, whose regex is `(?i:(application|text)/(.*json.*)(;|$))`.
fn is_json_content_type(content_type: &str) -> bool {
    let content_type = content_type.to_ascii_lowercase();

    content_type
        .split(';')
        .next()
        .and_then(|essence| essence.trim().split_once('/'))
        .is_some_and(|(kind, subtype)| {
            matches!(kind, "application" | "text") && subtype.contains("json")
        })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;

    const JSON: &str = "application/json;charset=utf-8";

    /// What a scripted transport answers with.
    #[derive(Clone)]
    enum Canned {
        Status(u16, &'static str),
        /// A transport failure — the only class resty retries.
        Failure,
    }

    /// A transport that answers from a script and records what it was asked.
    ///
    /// `/authentication` is answered separately so a script can be about the
    /// calls under test: every client validates its key on first use, and
    /// threading that through each script would obscure what each test pins.
    struct ScriptedTransport {
        auth: Canned,
        script: Mutex<Vec<Canned>>,
        calls: Mutex<Vec<Url>>,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
    }

    impl ScriptedTransport {
        /// `script` is consumed front to back; once it runs out every call gets
        /// an empty 200, so a test spells out only the answers it cares about.
        fn new(script: Vec<Canned>) -> Arc<Self> {
            Self::with_auth(Canned::Status(200, "{}"), script)
        }

        fn with_auth(auth: Canned, script: Vec<Canned>) -> Arc<Self> {
            Arc::new(Self {
                auth,
                script: Mutex::new(script.into_iter().rev().collect()),
                calls: Mutex::new(Vec::new()),
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
            })
        }

        fn calls(&self) -> Vec<Url> {
            self.calls.lock().expect("calls poisoned").clone()
        }

        fn call_count(&self) -> usize {
            self.calls().len()
        }

        fn paths(&self) -> Vec<String> {
            self.calls()
                .iter()
                .map(|url| url.path().to_owned())
                .collect()
        }
    }

    #[async_trait]
    impl Transport for ScriptedTransport {
        async fn execute(&self, url: &Url) -> Result<HttpResponse, String> {
            self.calls.lock().expect("calls poisoned").push(url.clone());

            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
            // Give the runtime a chance to start the other queued callers, so
            // the concurrency cap is exercised rather than assumed.
            tokio::task::yield_now().await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);

            let canned = if url.path().ends_with("/authentication") {
                self.auth.clone()
            } else {
                self.script
                    .lock()
                    .expect("script poisoned")
                    .pop()
                    .unwrap_or(Canned::Status(200, "{}"))
            };

            match canned {
                Canned::Status(status, body) => Ok(HttpResponse {
                    status,
                    content_type: Some(JSON.to_owned()),
                    body: body.as_bytes().to_vec(),
                }),
                Canned::Failure => Err("connection refused".to_owned()),
            }
        }
    }

    /// No rate limiting and no backoff: those layers have their own tests, and
    /// at Go's production values every test here would sleep.
    fn test_config() -> TmdbConfig {
        TmdbConfig {
            rate_limit: Duration::ZERO,
            retry: RetryPolicy {
                count: 3,
                wait: Duration::ZERO,
                max_wait: Duration::ZERO,
            },
            ..TmdbConfig::new("s3cret")
        }
    }

    fn client(transport: &Arc<ScriptedTransport>) -> TmdbClient<Arc<ScriptedTransport>> {
        TmdbClient::with_transport(test_config(), Arc::clone(transport))
    }

    fn movie(id: i64) -> MovieDetailsRequest {
        MovieDetailsRequest {
            id,
            ..Default::default()
        }
    }

    fn search(query: &str) -> SearchMovieRequest {
        SearchMovieRequest {
            query: query.to_owned(),
            ..Default::default()
        }
    }

    /// 🚨 The credential rides on the URL and nowhere else. If this ever fails
    /// the other way round — a key in the spec — recorded tapes start carrying
    /// secrets.
    #[tokio::test]
    async fn the_api_key_is_a_client_level_parameter_only() {
        let transport = ScriptedTransport::new(vec![]);
        let client = client(&transport);

        let request = search("Cinderella");
        let built = requests::search_movie(&request);
        client.search_movie(&request).await.expect("succeeds");

        assert!(!built.query_params.contains_key("api_key"));
        assert!(!serde_json::to_string(&built).unwrap().contains("s3cret"));

        for url in transport.calls() {
            let values: Vec<_> = url
                .query_pairs()
                .filter(|(key, _)| key == "api_key")
                .map(|(_, value)| value.into_owned())
                .collect();
            assert_eq!(values, vec!["s3cret".to_owned()], "{url}");
        }
    }

    /// Go builds the query with `url.Values.Encode()`, which sorts keys — the
    /// credential lands in sort order rather than appended at the end.
    #[tokio::test]
    async fn the_url_is_the_base_plus_the_sorted_query() {
        let transport = ScriptedTransport::new(vec![]);
        let client = client(&transport);

        client
            .search_movie(&SearchMovieRequest {
                query: "Cinderella".to_owned(),
                include_adult: true,
                year: Some(1950),
                ..Default::default()
            })
            .await
            .expect("succeeds");

        assert_eq!(
            transport.calls()[1].as_str(),
            "https://api.themoviedb.org/3/search/movie\
             ?api_key=s3cret&include_adult=true&query=Cinderella&year=1950"
        );
    }

    /// The redacted `Debug` is the last line of defence for a config that ends
    /// up in a log line or a panic message.
    #[test]
    fn the_config_debug_does_not_leak_the_key() {
        let rendered = format!("{:?}", TmdbConfig::new("s3cret"));

        assert!(!rendered.contains("s3cret"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
    }

    /// A 404 on a details lookup is Go's `ErrNotFound` → `ErrUnmatched`: the
    /// classifier falls through to its next branch. As an error it would abort
    /// the classification instead, which is the whole reason the kinds are kept
    /// apart.
    #[tokio::test]
    async fn a_details_404_is_an_absence_not_a_failure() {
        let transport = ScriptedTransport::new(vec![Canned::Status(404, "{}")]);

        assert_eq!(
            client(&transport)
                .movie_details(&movie(1))
                .await
                .expect("404 is not a failure here"),
            None
        );
    }

    /// Search and find have no 404 special case in Go — the error propagates.
    #[tokio::test]
    async fn a_search_404_is_a_failure() {
        let transport = ScriptedTransport::new(vec![Canned::Status(404, "{}")]);
        let err = client(&transport)
            .search_movie(&search("x"))
            .await
            .expect_err("404 propagates");

        assert_eq!(err.to_string(), "TMDB request failed: 404 Not Found");
    }

    /// Every other non-2xx is fatal, and must stay distinguishable from both a
    /// miss and an unauthorized.
    #[tokio::test]
    async fn other_statuses_are_hard_errors() {
        let transport = ScriptedTransport::new(vec![Canned::Status(500, "")]);
        let err = client(&transport)
            .movie_details(&movie(1))
            .await
            .expect_err("500 is fatal");

        assert_eq!(
            err.to_string(),
            "TMDB request failed: 500 Internal Server Error"
        );
    }

    /// 🚨 The latch. Once TMDB has said 401, no further request may leave the
    /// process — a revoked key must not turn into a request storm.
    #[tokio::test]
    async fn an_unauthorized_response_latches_for_the_process() {
        // The key validates and is then revoked: that isolates the fail-fast
        // latch from the lazy initialiser's own latch, covered by the next test.
        let transport = ScriptedTransport::new(vec![Canned::Status(401, "")]);
        let client = client(&transport);

        assert_eq!(
            client
                .movie_details(&movie(1))
                .await
                .expect_err("401")
                .to_string(),
            "TMDB request failed: 401 Unauthorized"
        );
        assert_eq!(transport.paths(), vec!["/3/authentication", "/3/movie/1"]);

        for _ in 0..3 {
            assert_eq!(
                client
                    .movie_details(&movie(2))
                    .await
                    .expect_err("latched")
                    .to_string(),
                "TMDB request failed: 401 Unauthorized"
            );
        }

        assert_eq!(
            transport.call_count(),
            2,
            "a latched client must issue no further requests"
        );
    }

    /// Go latches the lazy initialiser's *error* too, so a client whose key
    /// fails validation is disabled for the process — not retried per call.
    #[tokio::test]
    async fn a_failed_initialisation_latches() {
        let transport = ScriptedTransport::with_auth(Canned::Status(401, ""), vec![]);
        let client = client(&transport);

        for _ in 0..3 {
            assert!(client.movie_details(&movie(1)).await.is_err());
        }

        assert_eq!(transport.paths(), vec!["/3/authentication"]);
    }

    /// A disabled client never reaches the network, and says so — Go's
    /// `newRequester` refuses before building anything.
    #[tokio::test]
    async fn a_disabled_client_makes_no_request() {
        let transport = ScriptedTransport::new(vec![]);
        let client = TmdbClient::with_transport(
            TmdbConfig {
                enabled: false,
                ..test_config()
            },
            Arc::clone(&transport),
        );

        assert_eq!(
            client
                .movie_details(&movie(1))
                .await
                .expect_err("disabled")
                .to_string(),
            "TMDB request failed: TMDB is disabled"
        );
        assert_eq!(transport.call_count(), 0);
    }

    /// 🚨 resty retries transport failures only — bitmagnet registers no retry
    /// condition, so an HTTP error status comes back as-is. Retrying a 500 or a
    /// 429 would be the intuitive behaviour and a divergence.
    #[tokio::test]
    async fn transport_failures_retry_and_error_statuses_do_not() {
        let failing = ScriptedTransport::new(vec![Canned::Failure; 4]);
        let err = client(&failing)
            .movie_details(&movie(1))
            .await
            .expect_err("retries exhausted");

        assert_eq!(err.to_string(), "TMDB request failed: connection refused");
        // One validation + four attempts: `SetRetryCount(3)` is three RETRIES.
        assert_eq!(failing.call_count(), 5);

        let erroring = ScriptedTransport::new(vec![Canned::Status(500, "")]);
        assert!(client(&erroring).movie_details(&movie(1)).await.is_err());
        assert_eq!(erroring.call_count(), 2, "a 500 is not retried");
    }

    /// A transport failure that clears within the retry budget is invisible to
    /// the caller.
    #[tokio::test]
    async fn a_recovered_transport_failure_succeeds() {
        let transport = ScriptedTransport::new(vec![
            Canned::Failure,
            Canned::Failure,
            Canned::Status(200, r#"{"id":603,"title":"The Matrix"}"#),
        ]);

        let details = client(&transport)
            .movie_details(&movie(603))
            .await
            .expect("recovers")
            .expect("found");

        assert_eq!(details.title, "The Matrix");
    }

    /// Go bounds TMDB concurrency at two (`semaphore.NewWeighted(2)`); the
    /// worker pool above it is much wider, so this is what protects the API
    /// budget.
    #[tokio::test]
    async fn at_most_two_requests_are_in_flight() {
        let transport = ScriptedTransport::new(vec![]);
        let client = client(&transport);

        // Serialised, so the one-time initialisation is done before the fan-out
        // and cannot be what limits concurrency.
        client.movie_details(&movie(0)).await.expect("warms up");

        let calls = (1..=8).map(|id| {
            let client = client.clone();
            async move { client.movie_details(&movie(id)).await }
        });
        futures::future::join_all(calls).await;

        assert_eq!(transport.max_in_flight.load(Ordering::SeqCst), 2);
        // Validation + the warm-up + the eight fanned-out calls: the cap delays
        // requests, it never drops them.
        assert_eq!(transport.call_count(), 1 + 1 + 8);
    }

    /// TMDB sends explicit `null` for absent strings, and the DTOs decode that
    /// as Go's zero value. This proves the live path uses those DTOs unchanged
    /// rather than re-tightening them.
    #[tokio::test]
    async fn responses_decode_with_gos_json_tolerance() {
        let transport = ScriptedTransport::new(vec![Canned::Status(
            200,
            r#"{"id":603,"title":"The Matrix","poster_path":null,"overview":null}"#,
        )]);

        let details = client(&transport)
            .movie_details(&movie(603))
            .await
            .expect("decodes")
            .expect("found");

        assert_eq!(details.id, 603);
        assert_eq!(details.poster_path, "");
    }

    /// resty decodes only JSON content types, and a 204 not at all, leaving the
    /// result at its zero value in both cases — without an error. See
    /// [`decode`].
    #[test]
    fn a_non_json_body_is_gos_zero_value_not_an_error() {
        let html = HttpResponse {
            status: 200,
            content_type: Some("text/html".to_owned()),
            body: b"<html>nope</html>".to_vec(),
        };
        assert_eq!(
            decode::<MovieDetailsResponse>(&html),
            Ok(MovieDetailsResponse::default())
        );

        let no_content = HttpResponse {
            status: 204,
            content_type: Some(JSON.to_owned()),
            body: Vec::new(),
        };
        assert_eq!(
            decode::<SearchMovieResponse>(&no_content),
            Ok(SearchMovieResponse::default())
        );

        // A JSON content type with a broken body IS an error: Go's `Unmarshalc`
        // returns one and resty propagates it.
        let broken = HttpResponse {
            status: 200,
            content_type: Some(JSON.to_owned()),
            body: b"{".to_vec(),
        };
        assert!(decode::<MovieDetailsResponse>(&broken).is_err());
    }

    #[test]
    fn json_content_types_match_restys_regex() {
        for json in [
            "application/json",
            "application/json; charset=utf-8",
            "APPLICATION/JSON",
            "text/json",
            "application/vnd.api+json",
        ] {
            assert!(is_json_content_type(json), "{json}");
        }

        for other in ["", "text/html", "application/xml", "text/plain"] {
            assert!(!is_json_content_type(other), "{other}");
        }
    }
}
