//! The guard between a live content resolver and the persisting worker.
//!
//! With the attach flags on, the classifier reaches PostgreSQL local search and
//! TMDB through a [`ContentResolver`]. The engine renders every
//! [`ResolveError`] into an `error` outcome, so from the outcome alone a failed
//! TMDB call looks like any other classification error, and Go would persist
//! it as one: the torrent is republished in a retry job. The Rust worker does
//! not yet reproduce that path for resolver failures (a 401 latch, a disabled
//! client, a timeout, a 5xx or 429 after retries, a search or find 404, a local
//! search query error). It abstains from the whole job instead, before any
//! side effect, and leaves the row to Go.
//!
//! [`AbstainingResolver`] wraps the live resolver and records the first error
//! of a composition in its [`ResolveScope`]; the materializer then returns
//! [`crate::MaterializeError::ResolverFailed`] rather than a write-set. The
//! error is passed on unchanged, so the engine's error texts do not change. A
//! TMDB details 404 is not an error: the client answers `Ok(None)`, which the
//! engine treats as unmatched, as Go does.
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
    /// The first failure stops it, because the composition will be abstained
    /// whatever the remaining calls answer. So does the worker when it gives a
    /// composition up (job timeout or shutdown), so that the detached blocking
    /// task makes no further TMDB or database calls.
    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }

    pub(crate) fn failure(&self) -> Option<ResolverFailure> {
        self.failure.lock().expect("resolve scope lock").clone()
    }

    fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    fn record(&self, info_hash: &str, call: &'static str, error: &ResolveError) {
        self.failure
            .lock()
            .expect("resolve scope lock")
            .get_or_insert_with(|| ResolverFailure {
                info_hash: info_hash.to_owned(),
                call,
                error: error.to_string(),
            });
        self.stop();
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

/// Records every [`ResolveError`] of the wrapped resolver in the calling
/// classification's [`ResolveScope`].
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
        let result = if slot.scope.stopped() {
            Err(ResolveError::Unsupported(format!(
                "{call} skipped: the job is already abstained"
            )))
        } else {
            request.await
        };
        if let Err(error) = &result {
            slot.scope.record(&slot.info_hash, call, error);
        }
        result
    }
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
    use crate::{LoadedTorrent, MaterializeError, Materializer, WriteSet};

    const MOVIE: &str = "1111111111111111111111111111111111111111";
    const OTHER: &str = "2222222222222222222222222222222222222222";
    const EMPTY_SEARCH: &str = r#"{"page":1,"results":[],"total_pages":0,"total_results":0}"#;

    type Scripted = Result<HttpResponse, String>;

    fn status(status: u16, body: &str) -> Scripted {
        Ok(HttpResponse {
            status,
            content_type: Some("application/json".to_owned()),
            body: body.as_bytes().to_vec(),
        })
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

    struct Case {
        name: &'static str,
        responses: Vec<(&'static str, Scripted)>,
        enabled: bool,
        local_error: bool,
        hint: Option<(&'static str, &'static str)>,
        /// The recorded `(call, error)`, or `None` for a write-set.
        expected: Option<(&'static str, &'static str)>,
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
            expected: None,
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
            attach_hint_unsupported: false,
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
    /// resolver; both must agree.
    fn materialize(
        case: &Case,
        torrents: &[LoadedTorrent],
    ) -> (Result<WriteSet, String>, Vec<String>) {
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
                    .materialize_borrowed(&flags_on(&hashes), torrents)
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
    fn every_resolver_error_class_abstains_and_a_details_404_does_not() {
        let cases = [
            case("search answers nothing"),
            Case {
                expected: Some(("tmdb_search_movie", "TMDB request failed: 401 Unauthorized")),
                responses: vec![("/search/movie", status(401, "{}"))],
                ..case("search 401 latches")
            },
            Case {
                expected: Some(("tmdb_search_movie", "TMDB request failed: 401 Unauthorized")),
                responses: vec![("/authentication", status(401, "{}"))],
                asked: Some("/authentication"),
                ..case("key validation 401 latches")
            },
            Case {
                expected: Some(("tmdb_search_movie", "TMDB request failed: TMDB is disabled")),
                enabled: false,
                asked: None,
                ..case("disabled client")
            },
            Case {
                expected: Some((
                    "tmdb_search_movie",
                    "TMDB request failed: operation timed out",
                )),
                responses: vec![("/search/movie", Err("operation timed out".to_owned()))],
                ..case("transport timeout after retries")
            },
            Case {
                expected: Some((
                    "tmdb_search_movie",
                    "TMDB request failed: 500 Internal Server Error",
                )),
                responses: vec![("/search/movie", status(500, "{}"))],
                ..case("5xx")
            },
            Case {
                expected: Some((
                    "tmdb_search_movie",
                    "TMDB request failed: 429 Too Many Requests",
                )),
                responses: vec![("/search/movie", status(429, "{}"))],
                ..case("429")
            },
            Case {
                expected: Some(("tmdb_search_movie", "TMDB request failed: 404 Not Found")),
                responses: vec![("/search/movie", status(404, "{}"))],
                ..case("search 404 is a failure")
            },
            Case {
                expected: Some((
                    "tmdb_find_by_external_id",
                    "TMDB request failed: 404 Not Found",
                )),
                responses: vec![("/find/tt0000042", status(404, "{}"))],
                hint: Some(("imdb", "tt0000042")),
                asked: Some("/find/tt0000042"),
                ..case("find 404 is a failure")
            },
            Case {
                responses: vec![("/movie/42", status(404, "{}"))],
                hint: Some(("tmdb", "42")),
                asked: Some("/movie/42"),
                ..case("details 404 is unmatched, as in Go")
            },
            Case {
                expected: Some((
                    "content_by_search",
                    "local content search failed: connection refused",
                )),
                local_error: true,
                asked: None,
                ..case("local search failure")
            },
        ];
        for case in &cases {
            let torrent = movie_torrent(MOVIE, "Some Movie 1999 1080p BluRay x264-GRP", case.hint);
            let (result, asked) = materialize(case, std::slice::from_ref(&torrent));
            match case.expected {
                Some((call, error)) => {
                    assert_eq!(
                        result,
                        Err(format!("{MOVIE} {call}: {error}")),
                        "{}",
                        case.name
                    );
                }
                None => {
                    let write_set = result.unwrap_or_else(|error| panic!("{}: {error}", case.name));
                    assert_eq!(write_set.torrent_contents.len(), 1, "{}", case.name);
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

    #[test]
    fn one_failing_torrent_abstains_the_whole_job() {
        let case = Case {
            expected: None,
            responses: vec![("/movie/42", status(500, "{}"))],
            ..case("one failing torrent")
        };
        let torrents = [
            movie_torrent(MOVIE, "Some Movie 1999 1080p BluRay x264-GRP", None),
            movie_torrent(OTHER, "Other Film 2004 720p WEB-DL", Some(("tmdb", "42"))),
        ];
        let (result, _) = materialize(&case, &torrents);
        assert_eq!(
            result,
            Err(format!(
                "{OTHER} tmdb_movie_details: TMDB request failed: 500 Internal Server Error"
            ))
        );
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
}
