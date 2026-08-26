//! HTTP health, status, and Prometheus projections for the observe-only soak.

use std::time::Duration;

use axum::extract::State;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use bitmagnet_common::metrics::register_computed_gauge;
use bitmagnet_dht::{DhtRuntimeHealthFailure, DhtRuntimeHealthStatus};
use serde::Serialize;

use bitmagnet_dht_crawler::{
    DhtCrawlerObserveOnlyObservabilityHandle, DhtCrawlerObserveOnlyObservabilitySnapshot,
    DhtCrawlerPipelineObservedLifecycle,
};

const MODE: &str = "observe_only";

/// Stable, deliberately compact JSON projection for the observe-only process.
///
/// This does not expose payloads, peer addresses, database state, or a writer
/// readiness claim. Counter fields are independently sampled atomics.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DhtCrawlerObserveOnlyStatusResponse {
    pub mode: &'static str,
    pub status: &'static str,
    pub ready: bool,
    pub lifecycle: &'static str,
    pub runtime: DhtCrawlerObserveOnlyRuntimeStatus,
    pub observations: DhtCrawlerObserveOnlyObservationStatus,
}

/// Outbound-health portion of the observe-only status response.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DhtCrawlerObserveOnlyRuntimeStatus {
    pub active: bool,
    pub health: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_failure: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub running_for_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_response_ago_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_ago_seconds: Option<f64>,
}

/// Minimal traffic and discard counters for operator soak admission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DhtCrawlerObserveOnlyObservationStatus {
    pub inbound_admitted: u64,
    pub discovery_queued: u64,
    pub sample_queries_started: u64,
    pub sample_queries_succeeded: u64,
    pub sample_queries_failed: u64,
    pub info_hashes_observed: u64,
}

impl DhtCrawlerObserveOnlyStatusResponse {
    #[must_use]
    fn from_snapshot(snapshot: DhtCrawlerObserveOnlyObservabilitySnapshot) -> Self {
        let ready = snapshot.is_ready();
        let (health, health_failure) = runtime_health_labels(snapshot.runtime.health.status());
        Self {
            mode: MODE,
            status: if ready { "ready" } else { "not_ready" },
            ready,
            lifecycle: lifecycle_label(snapshot.lifecycle),
            runtime: DhtCrawlerObserveOnlyRuntimeStatus {
                active: snapshot.runtime.health.active,
                health,
                health_failure,
                running_for_seconds: duration_seconds(snapshot.runtime.health.running_for),
                last_response_ago_seconds: duration_seconds(
                    snapshot.runtime.health.last_response_ago,
                ),
                last_success_ago_seconds: duration_seconds(
                    snapshot.runtime.health.last_success_ago,
                ),
            },
            observations: DhtCrawlerObserveOnlyObservationStatus {
                inbound_admitted: snapshot.runtime.inbound.admitted,
                discovery_queued: snapshot.runtime.discovery.queued,
                sample_queries_started: snapshot
                    .maintenance
                    .sample_infohashes_worker
                    .queries_started,
                sample_queries_succeeded: snapshot
                    .maintenance
                    .sample_infohashes_worker
                    .queries_succeeded,
                sample_queries_failed: snapshot.maintenance.sample_infohashes_worker.queries_failed,
                info_hashes_observed: snapshot.observation.observed,
            },
        }
    }
}

/// Build the sender-free health surface used by the observe-only executable.
pub fn observe_only_http_router(handle: DhtCrawlerObserveOnlyObservabilityHandle) -> Router {
    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/status", get(status))
        .with_state(handle)
}

/// Register fresh-on-scrape gauges for the non-parity observe-only process.
///
/// # Panics
///
/// Panics if called more than once in a process or if another collector has
/// already registered one of these names.
pub fn register_observe_only_metrics(handle: DhtCrawlerObserveOnlyObservabilityHandle) {
    register_snapshot_gauge(
        &handle,
        "bitmagnet_dht_observe_ready",
        "Whether the observe-only DHT graph has proven current readiness.",
        |snapshot| u8::from(snapshot.is_ready()) as f64,
    );
    register_snapshot_gauge(
        &handle,
        "bitmagnet_dht_observe_runtime_active",
        "Whether the observe-only DHT UDP runtime is active.",
        |snapshot| u8::from(snapshot.runtime.health.active) as f64,
    );
    register_snapshot_gauge(
        &handle,
        "bitmagnet_dht_observe_inbound_admitted",
        "Inbound DHT messages admitted by the observe-only runtime.",
        |snapshot| snapshot.runtime.inbound.admitted as f64,
    );
    register_snapshot_gauge(
        &handle,
        "bitmagnet_dht_observe_sample_queries_succeeded",
        "Successful sample_infohashes queries in the observe-only graph.",
        |snapshot| {
            snapshot
                .maintenance
                .sample_infohashes_worker
                .queries_succeeded as f64
        },
    );
    register_snapshot_gauge(
        &handle,
        "bitmagnet_dht_observe_info_hashes_observed",
        "Info-hash occurrences counted and discarded by the observe-only sink.",
        |snapshot| snapshot.observation.observed as f64,
    );
}

fn register_snapshot_gauge<F>(
    handle: &DhtCrawlerObserveOnlyObservabilityHandle,
    name: &str,
    help: &str,
    value: F,
) where
    F: Fn(DhtCrawlerObserveOnlyObservabilitySnapshot) -> f64 + Send + Sync + 'static,
{
    let handle = handle.clone();
    register_computed_gauge(name, help, move || value(handle.snapshot()));
}

async fn livez() -> Response {
    no_store(Json(LivenessResponse {
        mode: MODE,
        status: "up",
    }))
}

async fn readyz(State(handle): State<DhtCrawlerObserveOnlyObservabilityHandle>) -> Response {
    readiness_response(handle)
}

async fn status(State(handle): State<DhtCrawlerObserveOnlyObservabilityHandle>) -> Response {
    readiness_response(handle)
}

fn readiness_response(handle: DhtCrawlerObserveOnlyObservabilityHandle) -> Response {
    let response = DhtCrawlerObserveOnlyStatusResponse::from_snapshot(handle.snapshot());
    let status = if response.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    no_store((status, Json(response)))
}

#[derive(Serialize)]
struct LivenessResponse {
    mode: &'static str,
    status: &'static str,
}

fn no_store(response: impl IntoResponse) -> Response {
    let mut response = response.into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}

fn duration_seconds(duration: Option<Duration>) -> Option<f64> {
    duration.map(|duration| duration.as_secs_f64())
}

fn lifecycle_label(lifecycle: DhtCrawlerPipelineObservedLifecycle) -> &'static str {
    match lifecycle {
        DhtCrawlerPipelineObservedLifecycle::Starting => "starting",
        DhtCrawlerPipelineObservedLifecycle::Ready => "ready",
        DhtCrawlerPipelineObservedLifecycle::Stopping => "stopping",
        DhtCrawlerPipelineObservedLifecycle::Stopped => "stopped",
        DhtCrawlerPipelineObservedLifecycle::Cancelled { .. } => "cancelled",
    }
}

fn runtime_health_labels(status: DhtRuntimeHealthStatus) -> (&'static str, Option<&'static str>) {
    match status {
        DhtRuntimeHealthStatus::Inactive => ("inactive", None),
        DhtRuntimeHealthStatus::Up => ("up", None),
        DhtRuntimeHealthStatus::Down(DhtRuntimeHealthFailure::NoResponseWithinInitialGrace) => {
            ("down", Some("no_response_within_initial_grace"))
        }
        DhtRuntimeHealthStatus::Down(
            DhtRuntimeHealthFailure::NoSuccessfulResponseWithinFreshness,
        ) => ("down", Some("no_successful_response_within_freshness")),
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::time::Duration;

    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use bitmagnet_common::metrics::gather_text;
    use bitmagnet_dht::{
        DhtBootstrapPingProducerConfig, DhtCrawlerMaintenanceConfig, DhtRuntimeConfig,
        DhtRuntimeHealthSnapshot,
    };
    use serde_json::Value;
    use tokio::sync::oneshot;
    use tokio::time::timeout;
    use tower::ServiceExt as _;

    use super::*;
    use bitmagnet_dht_crawler::{DhtCrawlerObserveOnlyConfig, DhtCrawlerObserveOnlySupervisor};

    fn offline_config() -> DhtCrawlerObserveOnlyConfig {
        DhtCrawlerObserveOnlyConfig {
            runtime: DhtRuntimeConfig {
                bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
                ..DhtRuntimeConfig::default()
            },
            maintenance: DhtCrawlerMaintenanceConfig {
                bootstrap_ping: DhtBootstrapPingProducerConfig {
                    bootstrap_nodes: Vec::new(),
                    ..DhtBootstrapPingProducerConfig::default()
                },
                ..DhtCrawlerMaintenanceConfig::default()
            },
            ..DhtCrawlerObserveOnlyConfig::default()
        }
    }

    async fn request(app: Router, uri: &str) -> Response {
        app.oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("health request builds"),
        )
        .await
        .expect("health route responds")
    }

    #[tokio::test]
    async fn routes_are_no_store_and_readiness_fails_closed_without_success() {
        let (supervisor, handle) = DhtCrawlerObserveOnlySupervisor::start(offline_config())
            .await
            .expect("offline observe-only graph starts");
        let (stop, stop_rx) = oneshot::channel();
        let run = tokio::spawn(supervisor.run(async move {
            let _ = stop_rx.await;
        }));
        let mut lifecycle = handle.lifecycle();
        timeout(Duration::from_secs(5), async {
            while lifecycle.changed().await.is_some() {
                if lifecycle.is_ready() {
                    break;
                }
            }
        })
        .await
        .expect("offline graph reaches lifecycle ready");

        let app = observe_only_http_router(handle.clone());
        let live = request(app.clone(), "/livez").await;
        assert_eq!(live.status(), StatusCode::OK);
        assert_eq!(live.headers()[CACHE_CONTROL], "no-store");
        assert_eq!(
            live.headers()[CONTENT_TYPE],
            "application/json; charset=utf-8"
        );
        let live_body = to_bytes(live.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&live_body).unwrap(),
            serde_json::json!({"mode": "observe_only", "status": "up"})
        );

        let ready = request(app.clone(), "/readyz").await;
        assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(ready.headers()[CACHE_CONTROL], "no-store");
        let ready_body = to_bytes(ready.into_body(), usize::MAX).await.unwrap();
        let ready_json: Value = serde_json::from_slice(&ready_body).unwrap();
        assert_eq!(ready_json["mode"], MODE);
        assert_eq!(ready_json["status"], "not_ready");
        assert_eq!(ready_json["lifecycle"], "ready");
        assert_eq!(ready_json["runtime"]["health"], "up");
        assert_eq!(
            ready_json["runtime"]["last_success_ago_seconds"],
            Value::Null
        );

        let status = request(app, "/status").await;
        assert_eq!(status.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(status.headers()[CACHE_CONTROL], "no-store");

        stop.send(()).unwrap();
        timeout(Duration::from_secs(5), run)
            .await
            .expect("offline graph stops")
            .expect("observe-only run task joins");
    }

    #[tokio::test]
    async fn status_projection_requires_a_fresh_proven_success() {
        let (supervisor, handle) = DhtCrawlerObserveOnlySupervisor::start(offline_config())
            .await
            .expect("offline observe-only graph starts");
        let mut snapshot = handle.snapshot();
        snapshot.lifecycle = DhtCrawlerPipelineObservedLifecycle::Ready;
        snapshot.runtime.health = DhtRuntimeHealthSnapshot {
            active: true,
            running_for: Some(Duration::from_secs(31)),
            last_response_ago: Some(Duration::from_secs(1)),
            last_success_ago: Some(Duration::from_secs(1)),
        };

        let ready = DhtCrawlerObserveOnlyStatusResponse::from_snapshot(snapshot.clone());
        assert!(ready.ready);
        assert_eq!(ready.status, "ready");
        assert_eq!(ready.lifecycle, "ready");
        assert_eq!(ready.runtime.health, "up");
        assert_eq!(ready.runtime.health_failure, None);

        snapshot.runtime.health.last_success_ago = Some(Duration::from_secs(61));
        let stale = DhtCrawlerObserveOnlyStatusResponse::from_snapshot(snapshot);
        assert!(!stale.ready);
        assert_eq!(stale.status, "not_ready");
        assert_eq!(stale.runtime.health, "down");
        assert_eq!(
            stale.runtime.health_failure,
            Some("no_successful_response_within_freshness")
        );

        timeout(
            Duration::from_secs(5),
            supervisor.run(std::future::ready(())),
        )
        .await
        .expect("observe-only graph stops");
    }

    #[tokio::test]
    async fn metrics_are_fresh_sender_free_non_parity_gauges() {
        let (supervisor, handle) = DhtCrawlerObserveOnlySupervisor::start(offline_config())
            .await
            .expect("offline observe-only graph starts");
        register_observe_only_metrics(handle);
        let metrics = gather_text();
        for expected in [
            "bitmagnet_dht_observe_ready 0",
            "bitmagnet_dht_observe_runtime_active 1",
            "bitmagnet_dht_observe_inbound_admitted 0",
            "bitmagnet_dht_observe_sample_queries_succeeded 0",
            "bitmagnet_dht_observe_info_hashes_observed 0",
        ] {
            assert!(metrics.lines().any(|line| line == expected), "{expected}");
        }

        timeout(
            Duration::from_secs(5),
            supervisor.run(std::future::ready(())),
        )
        .await
        .expect("observe-only graph stops");
    }
}
