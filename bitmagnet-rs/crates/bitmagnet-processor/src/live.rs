//! The guard between a live content resolver and the persisting worker.
//!
//! With the attach flags on, the classifier reaches PostgreSQL local search and
//! TMDB through a [`ContentResolver`]. The engine renders every
//! [`ResolveError`] into an `error` outcome, wrapped in Go's `find_match`
//! runtime error, and Go persists that outcome like any other classification
//! error: the torrent alone fails, and goes into the partial-failure retry job
//! or, when every torrent failed, into the job's own error. The other torrents
//! of the job keep resolving, as Go's independent goroutines do.
//!
//! The Rust worker does the same where it can reproduce Go's text byte for
//! byte: a TMDB call answered with its own status other than 401
//! ([`ResolveError::TmdbStatus`]), a 5xx, a 429, or a search or find 404, whose
//! message is the status line. Every other failure abstains from the whole job
//! before any side effect and leaves the row to Go:
//!
//! - a 401: at key validation Go falls back to its built-in default key, which
//!   is not ported, and both sides then latch TMDB off for the process;
//! - a failed key validation, whatever its status: both sides latch it for the
//!   process, so a transient 500 at start-up would otherwise fail every
//!   torrent, and burn every job's retries, until a restart;
//! - a transport failure or timeout after the retries, whose Go text embeds
//!   the request URL and so the API key;
//! - a status whose reason phrase cannot be rebuilt: hyper reports a missing,
//!   empty or obs-text phrase as empty, and Go renders the three differently;
//! - an undecodable body or a disabled client, whose Go text is Go's own;
//! - a local search query error, whose Go text is the driver's.
//!
//! [`AbstainingResolver`] wraps the live resolver and records the first
//! abstaining error of a composition in its [`ResolveScope`]; the materializer
//! then returns [`crate::MaterializeError::ResolverFailed`] rather than a
//! write-set. Errors are passed on unchanged, so the engine's error texts do
//! not change. A TMDB details 404 is not an error: the client answers
//! `Ok(None)`, which the engine treats as unmatched, as Go does.
//!
//! Each guarded call logs `content resolver call started` (debug) when it goes
//! ahead, or `content resolver call refused` (debug) once the scope has
//! stopped. An abstaining failure of a call that went ahead logs `content
//! resolver failure recorded; later calls are refused` (info). All three
//! carry `info_hash` and `call`. At several threads, calls that started
//! before the recorded failure may still be in flight, but none is logged as
//! started after it: the decision and its log line share the scope's lock.
//!
//! Classifications of one composition may run on several threads at once, so
//! the scope reaches the resolver through a task-local set around each
//! classification future rather than through the shared, compiled classifier.

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bitmagnet_classifier::resolver::tmdb;
use bitmagnet_classifier::{ContentResolver, ContentResultItem, ResolveError};
use bitmagnet_model::{Content, ContentType};

tokio::task_local! {
    static SLOT: ResolveSlot;
}

/// The first resolver failure of one composition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolverFailure {
    /// The torrent whose classification made the call.
    pub info_hash: String,
    /// The [`ContentResolver`] method, e.g. `tmdb_search_movie`.
    pub call: &'static str,
    /// The [`ResolveError`] as the engine would have rendered it.
    pub error: String,
}

impl fmt::Display for ResolverFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} for {} failed: {}",
            self.call, self.info_hash, self.error
        )
    }
}

/// Resolver failures of one composition, shared by its classifications.
#[derive(Debug, Default)]
pub(crate) struct ResolveScope {
    failure: Mutex<Option<ResolverFailure>>,
    stopped: AtomicBool,
}

impl ResolveScope {
    /// Fail every later resolver call of this composition at once.
    ///
    /// The first abstaining failure stops it, because the composition will be
    /// abstained whatever the remaining calls answer. So does the worker when
    /// it gives a composition up (job timeout or shutdown), so that the
    /// detached blocking task makes no further TMDB or database calls.
    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }

    pub(crate) fn failure(&self) -> Option<ResolverFailure> {
        self.failure.lock().expect("resolve scope lock").clone()
    }

    fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// Lets a call go ahead, or refuses it once the scope has stopped; logged
    /// either way. It holds the lock [`Self::record`] takes, so the log never
    /// shows a call started after the failure that stopped the scope.
    ///
    /// A refusal is kept as the failure when none was recorded, as when the
    /// worker stopped the scope, so a stopped scope always ends the composition
    /// abstained.
    fn admit(&self, info_hash: &str, call: &'static str) -> Result<(), ResolveError> {
        let mut failure = self.failure.lock().expect("resolve scope lock");
        if self.stopped() {
            tracing::debug!(info_hash = %info_hash, call, "content resolver call refused");
            let error =
                ResolveError::Unsupported(format!("{call} skipped: the job is already abstained"));
            failure.get_or_insert_with(|| ResolverFailure {
                info_hash: info_hash.to_owned(),
                call,
                error: error.to_string(),
            });
            return Err(error);
        }
        tracing::debug!(info_hash = %info_hash, call, "content resolver call started");
        Ok(())
    }

    fn record(&self, info_hash: &str, call: &'static str, error: &ResolveError) {
        let mut failure = self.failure.lock().expect("resolve scope lock");
        failure.get_or_insert_with(|| ResolverFailure {
            info_hash: info_hash.to_owned(),
            call,
            error: error.to_string(),
        });
        self.stop();
        tracing::info!(
            info_hash = %info_hash,
            call,
            %error,
            "content resolver failure recorded; later calls are refused"
        );
    }
}

/// One classification's view of its composition's [`ResolveScope`].
#[derive(Clone)]
pub(crate) struct ResolveSlot {
    scope: Arc<ResolveScope>,
    info_hash: Arc<str>,
}

impl ResolveSlot {
    pub(crate) fn new(scope: &Arc<ResolveScope>, info_hash: &str) -> Self {
        Self {
            scope: Arc::clone(scope),
            info_hash: info_hash.into(),
        }
    }

    /// Run one classification future with this slot visible to the resolver.
    pub(crate) fn scope<F: Future>(self, classification: F) -> impl Future<Output = F::Output> {
        SLOT.scope(self, classification)
    }
}

/// Records every abstaining [`ResolveError`] of the wrapped resolver in the
/// calling classification's [`ResolveScope`] (see [`abstains`]).
pub(crate) struct AbstainingResolver {
    inner: Arc<dyn ContentResolver>,
}

impl AbstainingResolver {
    pub(crate) fn new(inner: Arc<dyn ContentResolver>) -> Self {
        Self { inner }
    }

    async fn guard<T>(
        &self,
        call: &'static str,
        request: impl Future<Output = Result<T, ResolveError>>,
    ) -> Result<T, ResolveError> {
        // The materializer runs every live classification inside a slot; a call
        // outside one has no scope to abstain through, so it fails.
        let Ok(slot) = SLOT.try_with(ResolveSlot::clone) else {
            return Err(ResolveError::Unsupported(format!(
                "{call} outside a guarded classification"
            )));
        };
        slot.scope.admit(&slot.info_hash, call)?;
        let result = request.await;
        if let Err(error) = &result {
            if abstains(error) {
                slot.scope.record(&slot.info_hash, call, error);
            }
        }
        result
    }
}

/// Whether `error` abstains the whole job, rather than failing only the
/// torrent whose classification made the call, as Go does.
///
/// Only an answered TMDB status other than 401 fails just the torrent: its Go
/// message is the status line, which the client rebuilds exactly. See the
/// module docs for the rest.
pub(crate) fn abstains(error: &ResolveError) -> bool {
    !matches!(error, ResolveError::TmdbStatus { status, .. } if *status != 401)
}

#[async_trait]
impl ContentResolver for AbstainingResolver {
    async fn content_by_id(
        &self,
        content_type: ContentType,
        source: &str,
        id: &str,
    ) -> Result<Option<Content>, ResolveError> {
        self.guard(
            "content_by_id",
            self.inner.content_by_id(content_type, source, id),
        )
        .await
    }

    async fn content_by_search(
        &self,
        content_type: ContentType,
        base_title: &str,
        year: Option<u16>,
    ) -> Result<Vec<ContentResultItem>, ResolveError> {
        self.guard(
            "content_by_search",
            self.inner.content_by_search(content_type, base_title, year),
        )
        .await
    }

    async fn tmdb_find_by_external_id(
        &self,
        request: &tmdb::FindByIdRequest,
    ) -> Result<tmdb::FindByIdResponse, ResolveError> {
        self.guard(
            "tmdb_find_by_external_id",
            self.inner.tmdb_find_by_external_id(request),
        )
        .await
    }

    async fn tmdb_movie_details(
        &self,
        request: &tmdb::MovieDetailsRequest,
    ) -> Result<Option<tmdb::MovieDetailsResponse>, ResolveError> {
        self.guard("tmdb_movie_details", self.inner.tmdb_movie_details(request))
            .await
    }

    async fn tmdb_tv_details(
        &self,
        request: &tmdb::TvDetailsRequest,
    ) -> Result<Option<tmdb::TvDetailsResponse>, ResolveError> {
        self.guard("tmdb_tv_details", self.inner.tmdb_tv_details(request))
            .await
    }

    async fn tmdb_search_movie(
        &self,
        request: &tmdb::SearchMovieRequest,
    ) -> Result<tmdb::SearchMovieResponse, ResolveError> {
        self.guard("tmdb_search_movie", self.inner.tmdb_search_movie(request))
            .await
    }

    async fn tmdb_search_tv(
        &self,
        request: &tmdb::SearchTvRequest,
    ) -> Result<tmdb::SearchTvResponse, ResolveError> {
        self.guard("tmdb_search_tv", self.inner.tmdb_search_tv(request))
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use bitmagnet_classifier::resolver::tmdb;
    use bitmagnet_classifier::{
        ClassifierInput, ContentResolver, ContentResultItem, InputHint, ResolveError,
    };
    use bitmagnet_model::{Content, ContentType};
    use bitmagnet_queue::{ProcessTorrentParams, ProtocolId};
    use bitmagnet_tmdb::{HttpResponse, RetryPolicy, TmdbClient, TmdbConfig, Transport};
    use reqwest::Url;
    use serde_json::Value;

    use super::ResolverFailure;
    use crate::{
        ClassifierFailure, LoadedTorrent, MaterializeError, MaterializedWriterOutput, Materializer,
    };

    const MOVIE: &str = "1111111111111111111111111111111111111111";
    const OTHER: &str = "2222222222222222222222222222222222222222";
    const EMPTY_SEARCH: &str = r#"{"page":1,"results":[],"total_pages":0,"total_results":0}"#;

    type Scripted = Result<HttpResponse, String>;

    /// Go's path for the `find_match` that attaches by hinted id, and the one
    /// that searches by title.
    const BY_ID: &str = "runtime error at Path workflows.default.[3].if_else.if_action.find_match";
    const BY_SEARCH: &str =
        "runtime error at Path workflows.default.[5].if_else.if_action.find_match";

    fn status(status: u16, body: &str) -> Scripted {
        Ok(HttpResponse {
            status,
            reason_phrase: None,
            content_type: Some("application/json".to_owned()),
            body: body.as_bytes().to_vec(),
        })
    }

    /// A status line with a non-canonical reason phrase.
    fn status_line(code: u16, phrase: &[u8]) -> Scripted {
        let mut response = status(code, "{}")?;
        response.reason_phrase = Some(phrase.to_vec());
        Ok(response)
    }

    /// Answers each TMDB path suffix with a scripted response (anything else
    /// with an empty search page) and records every path asked for.
    #[derive(Default)]
    struct ScriptedTransport {
        responses: Vec<(&'static str, Scripted)>,
        asked: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl Transport for ScriptedTransport {
        async fn execute(&self, url: &Url) -> Result<HttpResponse, String> {
            let path = url.path().to_owned();
            self.asked.lock().expect("asked lock").push(path.clone());
            self.responses
                .iter()
                .find(|(suffix, _)| path.ends_with(suffix))
                .map_or_else(
                    || status(200, EMPTY_SEARCH),
                    |(_, response)| response.clone(),
                )
        }
    }

    /// The live resolver's shape: local search answering nothing (or failing),
    /// and the real TMDB client over a scripted transport.
    struct TestResolver {
        local_error: bool,
        tmdb: TmdbClient<Arc<ScriptedTransport>>,
    }

    impl TestResolver {
        fn local<T: Default>(&self) -> Result<T, ResolveError> {
            if self.local_error {
                Err(ResolveError::LocalSearch("connection refused".to_owned()))
            } else {
                Ok(T::default())
            }
        }
    }

    #[async_trait]
    impl ContentResolver for TestResolver {
        async fn content_by_id(
            &self,
            _content_type: ContentType,
            _source: &str,
            _id: &str,
        ) -> Result<Option<Content>, ResolveError> {
            self.local()
        }

        async fn content_by_search(
            &self,
            _content_type: ContentType,
            _base_title: &str,
            _year: Option<u16>,
        ) -> Result<Vec<ContentResultItem>, ResolveError> {
            self.local()
        }

        async fn tmdb_find_by_external_id(
            &self,
            request: &tmdb::FindByIdRequest,
        ) -> Result<tmdb::FindByIdResponse, ResolveError> {
            self.tmdb.find_by_external_id(request).await
        }

        async fn tmdb_movie_details(
            &self,
            request: &tmdb::MovieDetailsRequest,
        ) -> Result<Option<tmdb::MovieDetailsResponse>, ResolveError> {
            self.tmdb.movie_details(request).await
        }

        async fn tmdb_tv_details(
            &self,
            request: &tmdb::TvDetailsRequest,
        ) -> Result<Option<tmdb::TvDetailsResponse>, ResolveError> {
            self.tmdb.tv_details(request).await
        }

        async fn tmdb_search_movie(
            &self,
            request: &tmdb::SearchMovieRequest,
        ) -> Result<tmdb::SearchMovieResponse, ResolveError> {
            self.tmdb.search_movie(request).await
        }

        async fn tmdb_search_tv(
            &self,
            request: &tmdb::SearchTvRequest,
        ) -> Result<tmdb::SearchTvResponse, ResolveError> {
            self.tmdb.search_tv(request).await
        }
    }

    /// How one case's composition ends.
    enum Expected {
        /// A write-set with the torrent classified and nothing failed.
        Resolved,
        /// The whole job abstained on this `(call, error)`.
        Abstained(&'static str, &'static str),
        /// Only the torrent failed, with this rendered classifier error.
        Failed(String),
    }

    struct Case {
        name: &'static str,
        responses: Vec<(&'static str, Scripted)>,
        enabled: bool,
        local_error: bool,
        hint: Option<(&'static str, &'static str)>,
        expected: Expected,
        /// A path the classification must have asked TMDB for.
        asked: Option<&'static str>,
    }

    fn case(name: &'static str) -> Case {
        Case {
            name,
            responses: Vec::new(),
            enabled: true,
            local_error: false,
            hint: None,
            expected: Expected::Resolved,
            asked: Some("/search/movie"),
        }
    }

    fn movie_torrent(info_hash: &str, name: &str, hint: Option<(&str, &str)>) -> LoadedTorrent {
        LoadedTorrent {
            info_hash: info_hash.to_owned(),
            classifier_input: ClassifierInput {
                id: info_hash.to_owned(),
                name: name.to_owned(),
                size: 2_000_000_000,
                files_status: "single".to_owned(),
                extension: Some("mkv".to_owned()),
                files_count: None,
                files: Vec::new(),
                hint: hint.map(|(source, id)| InputHint {
                    content_type: "movie".to_owned(),
                    content_source: source.to_owned(),
                    content_id: id.to_owned(),
                    ..InputHint::default()
                }),
                contents: Vec::new(),
            },
            existing_content_ids: Vec::new(),
            hint_admission: crate::load::HintAdmission::Supported,
            content_ambiguity: None,
            source_backed_content_present: false,
        }
    }

    fn flags_on(info_hashes: &[&str]) -> ProcessTorrentParams {
        ProcessTorrentParams {
            classifier_workflow: "default".to_owned(),
            classifier_flags: Some(
                ["apis_enabled", "local_search_enabled", "tmdb_enabled"]
                    .into_iter()
                    .map(|flag| (flag.to_owned(), Value::Bool(true)))
                    .collect::<BTreeMap<_, _>>(),
            ),
            info_hashes: info_hashes
                .iter()
                .map(|hash| ProtocolId::from_hex(hash).expect("fixture hash"))
                .collect(),
            ..ProcessTorrentParams::default()
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("test runtime")
    }

    /// Materialize `torrents` serially and on a classify pool over one case's
    /// resolver; both must agree. An abstention is `Err("<hash> <call>:
    /// <error>")`.
    fn materialize(
        case: &Case,
        torrents: &[LoadedTorrent],
    ) -> (Result<MaterializedWriterOutput, String>, Vec<String>) {
        let runtime = runtime();
        let transport = Arc::new(ScriptedTransport {
            responses: case.responses.clone(),
            ..ScriptedTransport::default()
        });
        let mut results = Vec::new();
        for parallelism in [1, 4] {
            let config = TmdbConfig {
                enabled: case.enabled,
                base_url: "http://tmdb.invalid/3".to_owned(),
                retry: RetryPolicy {
                    count: 0,
                    ..RetryPolicy::default()
                },
                ..TmdbConfig::new("dummy-key")
            };
            let resolver = TestResolver {
                local_error: case.local_error,
                tmdb: TmdbClient::with_transport(config, Arc::clone(&transport)),
            };
            let materializer =
                Materializer::from_core_with_resolver(Arc::new(resolver), runtime.handle().clone())
                    .expect("compile core classifier")
                    .with_classify_parallelism(NonZeroUsize::new(parallelism).expect("nonzero"))
                    .expect("classify pool");
            let hashes: Vec<&str> = torrents.iter().map(|t| t.info_hash.as_str()).collect();
            results.push(
                materializer
                    .materialize_borrowed_with_failures(&flags_on(&hashes), torrents)
                    .map_err(|error| match error {
                        MaterializeError::ResolverFailed(ResolverFailure {
                            info_hash,
                            call,
                            error,
                        }) => format!("{info_hash} {call}: {error}"),
                        other => panic!("{}: unexpected error {other}", case.name),
                    }),
            );
        }
        let serial = results.remove(0);
        assert_eq!(
            results[0], serial,
            "{}: parallel differs from serial",
            case.name
        );
        let asked = transport.asked.lock().expect("asked lock").clone();
        (serial, asked)
    }

    #[test]
    fn answered_statuses_fail_only_the_torrent_and_every_other_error_abstains() {
        let failed =
            |status: &str| Expected::Failed(format!("{BY_SEARCH}: TMDB request failed: {status}"));
        let cases = [
            case("search answers nothing"),
            Case {
                responses: vec![("/movie/42", status(404, "{}"))],
                hint: Some(("tmdb", "42")),
                asked: Some("/movie/42"),
                ..case("details 404 is unmatched, as in Go")
            },
            Case {
                expected: failed("500 Internal Server Error"),
                responses: vec![("/search/movie", status(500, "{}"))],
                ..case("5xx")
            },
            Case {
                expected: failed("429 Too Many Requests"),
                responses: vec![("/search/movie", status(429, "{}"))],
                ..case("429, which Go does not retry")
            },
            Case {
                expected: failed("404 Not Found"),
                responses: vec![("/search/movie", status_line(404, b"Gone Fishing"))],
                ..case("search 404 is Go's sentinel text")
            },
            Case {
                expected: failed("599 status code 599"),
                responses: vec![("/search/movie", status_line(599, b"status code 599"))],
                ..case("the server's own reason phrase")
            },
            Case {
                expected: Expected::Failed(format!("{BY_ID}: TMDB request failed: 404 Not Found")),
                responses: vec![("/find/tt0000042", status(404, "{}"))],
                hint: Some(("imdb", "tt0000042")),
                asked: Some("/find/tt0000042"),
                ..case("find 404")
            },
            Case {
                expected: Expected::Abstained(
                    "tmdb_search_movie",
                    "TMDB request failed: 401 Unauthorized",
                ),
                responses: vec![("/search/movie", status(401, "{}"))],
                ..case("search 401 latches")
            },
            Case {
                expected: Expected::Abstained(
                    "tmdb_search_movie",
                    "TMDB request failed: 401 Unauthorized",
                ),
                responses: vec![("/authentication", status(401, "{}"))],
                asked: Some("/authentication"),
                ..case("key validation 401 latches")
            },
            Case {
                expected: Expected::Abstained(
                    "tmdb_search_movie",
                    "TMDB request failed: 500 Internal Server Error",
                ),
                responses: vec![("/authentication", status(500, "{}"))],
                asked: Some("/authentication"),
                ..case("a latched key validation abstains whatever its status")
            },
            Case {
                expected: Expected::Abstained(
                    "tmdb_movie_details",
                    "TMDB request failed: 404 Not Found",
                ),
                responses: vec![("/authentication", status(404, "{}"))],
                hint: Some(("tmdb", "42")),
                asked: Some("/authentication"),
                ..case("a latched key-validation 404 is not unmatched")
            },
            Case {
                expected: Expected::Abstained(
                    "tmdb_search_movie",
                    "TMDB request failed: TMDB is disabled",
                ),
                enabled: false,
                asked: None,
                ..case("disabled client")
            },
            Case {
                expected: Expected::Abstained(
                    "tmdb_search_movie",
                    "TMDB request failed: operation timed out",
                ),
                responses: vec![("/search/movie", Err("operation timed out".to_owned()))],
                ..case("transport timeout after retries")
            },
            Case {
                expected: Expected::Abstained(
                    "tmdb_search_movie",
                    "TMDB request failed: 502 with an unrenderable reason phrase",
                ),
                responses: vec![("/search/movie", status_line(502, b"Bad \xff Gateway"))],
                ..case("a reason phrase Go's text cannot be rebuilt from")
            },
            Case {
                expected: Expected::Abstained(
                    "tmdb_search_movie",
                    "TMDB request failed: 500 with an unrenderable reason phrase",
                ),
                responses: vec![("/search/movie", status_line(500, b""))],
                ..case("an empty reason phrase, which could be Go's \"500\" or \"500 \"")
            },
            Case {
                expected: Expected::Abstained(
                    "tmdb_search_movie",
                    "TMDB request failed: EOF while parsing a value at line 1 column 0",
                ),
                responses: vec![("/search/movie", status(200, ""))],
                ..case("undecodable body")
            },
            Case {
                expected: Expected::Abstained(
                    "content_by_search",
                    "local content search failed: connection refused",
                ),
                local_error: true,
                asked: None,
                ..case("local search failure")
            },
        ];
        for case in &cases {
            let torrent = movie_torrent(MOVIE, "Some Movie 1999 1080p BluRay x264-GRP", case.hint);
            let (result, asked) = materialize(case, std::slice::from_ref(&torrent));
            match &case.expected {
                Expected::Resolved => {
                    let output = result.unwrap_or_else(|error| panic!("{}: {error}", case.name));
                    assert_eq!(output.write_set.torrent_contents.len(), 1, "{}", case.name);
                    assert!(output.classifier_failures.is_empty(), "{}", case.name);
                }
                Expected::Failed(rendered_error) => {
                    let output = result.unwrap_or_else(|error| panic!("{}: {error}", case.name));
                    assert!(
                        output.write_set.torrent_contents.is_empty(),
                        "{}",
                        case.name
                    );
                    assert_eq!(
                        output.write_set.failed_info_hashes,
                        [MOVIE],
                        "{}",
                        case.name
                    );
                    assert_eq!(
                        output.classifier_failures,
                        [ClassifierFailure {
                            info_hash: MOVIE.to_owned(),
                            rendered_error: rendered_error.clone(),
                        }],
                        "{}",
                        case.name
                    );
                }
                Expected::Abstained(call, error) => {
                    assert_eq!(
                        result,
                        Err(format!("{MOVIE} {call}: {error}")),
                        "{}",
                        case.name
                    );
                }
            }
            if let Some(path) = case.asked {
                assert!(
                    asked.iter().any(|asked| asked.ends_with(path)),
                    "{}: TMDB was never asked for {path}: {asked:?}",
                    case.name
                );
            }
        }
    }

    const FOUND_SEARCH: &str = r#"{"page":1,"results":[{"id":603,"title":"Some Movie","original_title":"Some Movie","release_date":"1999-03-31"}],"total_pages":1,"total_results":1}"#;
    const FOUND_DETAILS: &str = r#"{"id":603,"title":"Some Movie","original_title":"Some Movie","original_language":"en","release_date":"1999-03-31","imdb_id":"tt0000603"}"#;

    /// Go classifies a job's torrents in independent goroutines, so one
    /// torrent's failed TMDB call stops only that torrent.
    #[test]
    fn one_failing_torrent_fails_alone_and_the_others_still_resolve() {
        let case = Case {
            responses: vec![
                ("/movie/42", status(500, "{}")),
                ("/search/movie", status(200, FOUND_SEARCH)),
                ("/movie/603", status(200, FOUND_DETAILS)),
            ],
            ..case("one failing torrent")
        };
        // The failing torrent comes first, so the serial composition reaches
        // the other one only after the failure.
        let torrents = [
            movie_torrent(OTHER, "Other Film 2004 720p WEB-DL", Some(("tmdb", "42"))),
            movie_torrent(MOVIE, "Some Movie 1999 1080p BluRay x264-GRP", None),
        ];
        let (result, asked) = materialize(&case, &torrents);
        let output = result.expect("no abstention");
        assert_eq!(
            output.classifier_failures,
            [ClassifierFailure {
                info_hash: OTHER.to_owned(),
                rendered_error: format!("{BY_ID}: TMDB request failed: 500 Internal Server Error"),
            }]
        );
        assert_eq!(output.write_set.failed_info_hashes, [OTHER]);
        let [resolved] = output.write_set.torrent_contents.as_slice() else {
            panic!("{:?}", output.write_set.torrent_contents);
        };
        assert_eq!(resolved.info_hash, MOVIE);
        assert_eq!(resolved.content_id.as_deref(), Some("603"));
        // Serial then parallel: each asked for the failing details once, and
        // nothing more for that torrent.
        assert_eq!(
            asked
                .iter()
                .filter(|path| path.ends_with("/movie/42"))
                .count(),
            2,
            "{asked:?}"
        );
        assert_eq!(
            asked
                .iter()
                .filter(|path| path.ends_with("/movie/603"))
                .count(),
            2,
            "{asked:?}"
        );
    }

    /// A failure only the torrent bears does not mask one that abstains.
    #[test]
    fn an_abstaining_error_abstains_the_job_whatever_else_failed() {
        let case = Case {
            expected: Expected::Resolved,
            responses: vec![
                ("/movie/42", status(500, "{}")),
                ("/search/movie", status(401, "{}")),
            ],
            ..case("one failing, one abstaining")
        };
        let torrents = [
            movie_torrent(OTHER, "Other Film 2004 720p WEB-DL", Some(("tmdb", "42"))),
            movie_torrent(MOVIE, "Some Movie 1999 1080p BluRay x264-GRP", None),
        ];
        let (result, _) = materialize(&case, &torrents);
        assert_eq!(
            result,
            Err(format!(
                "{MOVIE} tmdb_search_movie: TMDB request failed: 401 Unauthorized"
            ))
        );
    }

    #[test]
    fn only_an_answered_status_other_than_401_fails_just_the_torrent() {
        let status = |status, status_line: &str| ResolveError::TmdbStatus {
            status,
            status_line: status_line.to_owned(),
        };
        for error in [
            status(500, "500 Internal Server Error"),
            status(429, "429 Too Many Requests"),
            status(404, "404 Not Found"),
            status(599, "599 status code 599"),
        ] {
            assert!(!super::abstains(&error), "{error:?}");
        }
        for error in [
            status(401, "401 Unauthorized"),
            ResolveError::Tmdb("operation timed out".to_owned()),
            ResolveError::Tmdb("500 Internal Server Error".to_owned()),
            ResolveError::LocalSearch("connection refused".to_owned()),
            ResolveError::Unsupported("skipped".to_owned()),
            ResolveError::TapeMiss("search".to_owned()),
        ] {
            assert!(super::abstains(&error), "{error:?}");
        }
    }

    #[test]
    fn a_resolver_call_outside_a_classification_fails() {
        let runtime = runtime();
        let resolver =
            super::AbstainingResolver::new(Arc::new(bitmagnet_classifier::NullContentResolver));
        let error = runtime
            .block_on(resolver.content_by_id(ContentType::Movie, "tmdb", "42"))
            .expect_err("no scope");
        assert!(matches!(error, ResolveError::Unsupported(_)), "{error}");
    }

    #[test]
    fn a_stopped_scope_fails_every_later_call_and_keeps_the_first_failure() {
        let runtime = runtime();
        let resolver =
            super::AbstainingResolver::new(Arc::new(bitmagnet_classifier::NullContentResolver));
        let scope = Arc::new(super::ResolveScope::default());
        let call = |info_hash: &'static str| {
            super::ResolveSlot::new(&scope, info_hash).scope(resolver.content_by_id(
                ContentType::Movie,
                "tmdb",
                "42",
            ))
        };
        assert!(runtime.block_on(call(MOVIE)).is_ok());
        assert_eq!(scope.failure(), None);
        scope.stop();
        assert!(runtime.block_on(call(MOVIE)).is_err());
        assert!(runtime.block_on(call(OTHER)).is_err());
        let failure = scope.failure().expect("recorded");
        assert_eq!(
            (failure.info_hash.as_str(), failure.call),
            (MOVIE, "content_by_id")
        );
    }

    /// Two failures can race at several threads; the first one recorded is the
    /// one the worker reports.
    #[test]
    fn the_first_recorded_failure_is_kept() {
        let scope = super::ResolveScope::default();
        scope.record(
            MOVIE,
            "tmdb_search_movie",
            &ResolveError::Tmdb("operation timed out".to_owned()),
        );
        scope.record(
            OTHER,
            "content_by_search",
            &ResolveError::LocalSearch("connection refused".to_owned()),
        );
        let failure = scope.failure().expect("recorded");
        assert_eq!(
            (failure.info_hash.as_str(), failure.call),
            (MOVIE, "tmdb_search_movie")
        );
    }

    /// A writer the log capture below appends to.
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("capture lock")
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// The parity harness attributes TMDB requests from these lines, so their
    /// text, fields and order are part of the contract: a call that goes ahead
    /// is logged before its request, then the failure that stops the scope,
    /// then every later call as refused.
    #[test]
    fn the_guard_logs_started_recorded_and_refused_calls() {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&buffer);
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .without_time()
            .with_writer(move || Capture(Arc::clone(&sink)))
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let scope = super::ResolveScope::default();
            assert!(scope.admit(MOVIE, "tmdb_search_movie").is_ok());
            scope.record(
                MOVIE,
                "tmdb_search_movie",
                &ResolveError::Tmdb("operation timed out".to_owned()),
            );
            assert!(scope.admit(OTHER, "tmdb_movie_details").is_err());
        });

        let text = String::from_utf8(buffer.lock().expect("capture lock").clone()).expect("utf-8");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3, "{text}");
        for (line, (level, message, info_hash, call)) in lines.iter().zip([
            (
                "DEBUG",
                "content resolver call started",
                MOVIE,
                "tmdb_search_movie",
            ),
            (
                "INFO",
                "content resolver failure recorded; later calls are refused",
                MOVIE,
                "tmdb_search_movie",
            ),
            (
                "DEBUG",
                "content resolver call refused",
                OTHER,
                "tmdb_movie_details",
            ),
        ]) {
            assert!(
                line.contains(level)
                    && line.contains(message)
                    && line.contains(&format!("info_hash={info_hash}"))
                    && line.contains(&format!("call=\"{call}\"")),
                "{line}"
            );
        }
        assert!(
            lines[1].contains("error=TMDB request failed: operation timed out"),
            "{}",
            lines[1]
        );
    }
}
