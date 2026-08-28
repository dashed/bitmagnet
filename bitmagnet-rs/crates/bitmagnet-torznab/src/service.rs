//! Axum routing and the Torznab HTTP request lifecycle.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{OriginalUri, RawQuery, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response as AxumResponse};
use axum::routing::get;
use axum::Router;
use bitmagnet_search_query::{SearchQueryError, SearchResultItem, TorznabSearchParams};
use thiserror::Error;

use crate::config::{Config, Profile};
use crate::mapping::to_search_params;
use crate::request::{parse, profile_name, FUNCTION_CAPS};
use crate::response::TorznabError;
use crate::result_map::to_search_result;
use crate::xml::XmlError;

const XML_CONTENT_TYPE: &str = "application/xml; charset=utf-8";

/// Search failures surfaced by a [`SearchClient`] implementation.
#[derive(Debug, Error)]
pub enum SearchError {
    #[error(transparent)]
    Query(#[from] SearchQueryError),
    #[error("{0}")]
    Backend(String),
}

/// Search execution boundary. T3 supplies the real Lane Q/PgPool client;
/// handler tests use an in-memory implementation.
#[async_trait]
pub trait SearchClient: Send + Sync + 'static {
    async fn search(
        &self,
        params: TorznabSearchParams,
    ) -> Result<Vec<SearchResultItem>, SearchError>;
}

struct ServiceState<C> {
    config: Config,
    client: Arc<C>,
}

impl<C> Clone for ServiceState<C> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            client: Arc::clone(&self.client),
        }
    }
}

/// Builds the dependency-injected Torznab and liveness routes.
pub fn router<C>(config: Config, client: Arc<C>) -> Router
where
    C: SearchClient,
{
    let state = ServiceState { config, client };

    Router::new()
        .route("/healthz", get(healthz))
        // Axum wildcards require a non-empty capture, so the explicit slash
        // route mirrors Gin's empty `*any` match.
        .route("/torznab/", get(handle::<C>))
        .route("/torznab/{*path}", get(handle::<C>))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

async fn handle<C>(
    State(state): State<ServiceState<C>>,
    OriginalUri(uri): OriginalUri,
    RawQuery(raw_query): RawQuery,
) -> AxumResponse
where
    C: SearchClient,
{
    let torznab_path = uri.path().strip_prefix("/torznab").unwrap_or(uri.path());
    let requested_profile = profile_name(torznab_path);
    let profile = match requested_profile.as_str() {
        "" | "api" | "default" => Profile::default_profile(),
        _ => match state.config.get_profile(&requested_profile) {
            Some(profile) => profile.clone(),
            None => {
                return plain_error(
                    StatusCode::NOT_FOUND,
                    format!("profile not found: {requested_profile}"),
                );
            }
        },
    };

    let raw_query = raw_query.unwrap_or_default();
    let request = parse(form_urlencoded::parse(raw_query.as_bytes()));

    if request.type_.is_empty() {
        return xml_response(
            TorznabError {
                code: 200,
                description: "missing parameter (t)".to_owned(),
            }
            .to_xml(),
        );
    }

    if request.type_ == FUNCTION_CAPS {
        return xml_response(profile.caps().to_xml());
    }

    let params = match to_search_params(&request, &profile) {
        Ok(params) => params,
        Err(error) => return xml_response(error.to_xml()),
    };
    let rows = match state.client.search(params).await {
        Ok(rows) => rows,
        Err(error) => {
            return plain_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to search: {error}"),
            );
        }
    };

    xml_response(to_search_result(&request, &profile, rows).to_xml())
}

fn xml_response(document: Result<Vec<u8>, XmlError>) -> AxumResponse {
    match document {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, XML_CONTENT_TYPE)],
            body,
        )
            .into_response(),
        Err(error) => plain_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode xml: {error}"),
        ),
    }
}

fn plain_error(status: StatusCode, message: String) -> AxumResponse {
    (status, format!("{message}\n")).into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use axum::body::{to_bytes, Body};
    use axum::http::{header, Request, StatusCode};
    use bitmagnet_search_query::{
        ContentRef, ContentType, Criteria, Episodes, SearchResultItem, TorrentContentAttribute,
        TorznabSearchParams, VideoResolution,
    };
    use tower::ServiceExt;

    use super::{router, SearchClient, SearchError, XML_CONTENT_TYPE};
    use crate::config::{Config, Profile};
    use crate::response::TorznabError;

    #[derive(Default)]
    struct MockClient {
        calls: Mutex<Vec<TorznabSearchParams>>,
    }

    impl MockClient {
        fn calls(&self) -> Vec<TorznabSearchParams> {
            self.calls
                .lock()
                .expect("mock call lock is not poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl SearchClient for MockClient {
        async fn search(
            &self,
            params: TorznabSearchParams,
        ) -> Result<Vec<SearchResultItem>, SearchError> {
            self.calls
                .lock()
                .expect("mock call lock is not poisoned")
                .push(params);
            Ok(Vec::new())
        }
    }

    struct FailingClient;

    #[async_trait]
    impl SearchClient for FailingClient {
        async fn search(
            &self,
            _params: TorznabSearchParams,
        ) -> Result<Vec<SearchResultItem>, SearchError> {
            Err(SearchError::Backend("backend unavailable".to_owned()))
        }
    }

    fn custom_profile() -> Profile {
        Profile {
            id: "test".to_owned(),
            title: "Test".to_owned(),
            disable_order_by_relevance: false,
            default_limit: 1_000,
            max_limit: 2_000,
            tags: vec!["test".to_owned()],
        }
    }

    fn config() -> Config {
        Config {
            profiles: vec![custom_profile()],
        }
        .merge_defaults()
    }

    async fn get<C>(uri: &str, client: Arc<C>) -> (StatusCode, axum::http::HeaderMap, Vec<u8>)
    where
        C: SearchClient,
    {
        let response = router(config(), client)
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("test request builds"),
            )
            .await
            .expect("router serves request");
        let status = response.status();
        let headers = response.headers().clone();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body is readable")
            .to_vec();
        (status, headers, body)
    }

    #[tokio::test]
    async fn caps_paths_use_the_resolved_profile_and_xml_content_type() {
        let cases = [
            ("/torznab/?t=caps", Profile::default_profile()),
            ("/torznab/api?t=caps", Profile::default_profile()),
            ("/torznab/default?t=caps", Profile::default_profile()),
            ("/torznab/test/api/?t=caps", custom_profile()),
        ];

        for (uri, profile) in cases {
            let client = Arc::new(MockClient::default());
            let (status, headers, body) = get(uri, Arc::clone(&client)).await;

            assert_eq!(status, StatusCode::OK, "{uri}");
            assert_eq!(
                headers
                    .get(header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok()),
                Some(XML_CONTENT_TYPE),
                "{uri}"
            );
            assert_eq!(
                body,
                profile.caps().to_xml().expect("caps XML renders"),
                "{uri}"
            );
            assert!(client.calls().is_empty(), "{uri}");
        }
    }

    #[tokio::test]
    async fn search_http_params_map_to_the_expected_lane_q_tree() {
        // F3: Torznab typed/category content-type filters admit NULL rows.
        let ct_or_null = |content_type: ContentType| {
            Criteria::or([
                Criteria::content_type_in([content_type]),
                Criteria::is_null(TorrentContentAttribute::ContentType),
            ])
        };
        let cases = [
            (
                "/torznab/?t=movie&cat=2000%2C2030&limit=10&offset=100",
                TorznabSearchParams {
                    query: None,
                    filter: Some(Criteria::and([
                        ct_or_null(ContentType::Movie),
                        Criteria::or([
                            Criteria::and([ct_or_null(ContentType::Movie)]),
                            Criteria::and([Criteria::video_resolution_in([
                                VideoResolution::V480p,
                            ])]),
                        ]),
                    ])),
                    order: None,
                    limit: 10,
                    offset: Some(100),
                },
            ),
            (
                "/torznab/default?t=tvsearch&imdbid=123&season=1",
                TorznabSearchParams {
                    query: None,
                    filter: Some(Criteria::and([
                        ct_or_null(ContentType::TvShow),
                        Criteria::episodes(Episodes::new().add_season(1)),
                        Criteria::alternative_identifier([ContentRef {
                            content_type: Some(ContentType::TvShow),
                            source: "imdb".to_owned(),
                            id: "tt123".to_owned(),
                        }]),
                    ])),
                    order: None,
                    limit: 100,
                    offset: None,
                },
            ),
            (
                "/torznab/default?t=tvsearch&tmdbid=123&season=2&ep=3",
                TorznabSearchParams {
                    query: None,
                    filter: Some(Criteria::and([
                        ct_or_null(ContentType::TvShow),
                        Criteria::episodes(Episodes::new().add_episode(2, 3)),
                        Criteria::canonical_identifier([ContentRef {
                            content_type: Some(ContentType::TvShow),
                            source: "tmdb".to_owned(),
                            id: "123".to_owned(),
                        }]),
                    ])),
                    order: None,
                    limit: 100,
                    offset: None,
                },
            ),
        ];

        for (uri, expected) in cases {
            let client = Arc::new(MockClient::default());
            let (status, headers, _) = get(uri, Arc::clone(&client)).await;

            assert_eq!(status, StatusCode::OK, "{uri}");
            assert_eq!(
                headers
                    .get(header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok()),
                Some(XML_CONTENT_TYPE),
                "{uri}"
            );
            assert_eq!(client.calls(), [expected], "{uri}");
        }
    }

    #[tokio::test]
    async fn torznab_errors_are_xml_with_http_200() {
        let cases = [
            (
                "/torznab/",
                TorznabError {
                    code: 200,
                    description: "missing parameter (t)".to_owned(),
                },
            ),
            (
                "/torznab/?t=foo",
                TorznabError {
                    code: 202,
                    description: "no such function (foo)".to_owned(),
                },
            ),
        ];

        for (uri, expected) in cases {
            let client = Arc::new(MockClient::default());
            let (status, headers, body) = get(uri, Arc::clone(&client)).await;

            assert_eq!(status, StatusCode::OK, "{uri}");
            assert_eq!(
                headers
                    .get(header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok()),
                Some(XML_CONTENT_TYPE),
                "{uri}"
            );
            assert_eq!(body, expected.to_xml().expect("error XML renders"), "{uri}");
            assert!(client.calls().is_empty(), "{uri}");
        }
    }

    #[tokio::test]
    async fn unknown_profile_is_plain_text_404() {
        let client = Arc::new(MockClient::default());
        let (status, headers, body) = get("/torznab/MISSING?t=caps", client).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_ne!(
            headers
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some(XML_CONTENT_TYPE)
        );
        assert_eq!(body, b"profile not found: missing\n");
    }

    #[tokio::test]
    async fn backend_errors_are_plain_text_500() {
        let (status, headers, body) = get("/torznab/?t=search", Arc::new(FailingClient)).await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_ne!(
            headers
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some(XML_CONTENT_TYPE)
        );
        assert_eq!(body, b"failed to search: backend unavailable\n");
    }

    #[tokio::test]
    async fn healthz_is_dependency_free() {
        let client = Arc::new(MockClient::default());
        let (status, _, body) = get("/healthz", Arc::clone(&client)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"ok");
        assert!(client.calls().is_empty());
    }
}
