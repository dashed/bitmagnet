use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use bitmagnet_common::metrics::register_computed_gauge;
use bitmagnet_dht::{DhtRuntimeHealthFailure, DhtRuntimeHealthStatus};
use bitmagnet_dht_crawler::{
    DhtCrawlerPipelineObservabilityHandle, DhtCrawlerPipelineObservabilitySnapshot,
    DhtCrawlerPipelineObservedLifecycle,
};
use serde::Serialize;

const MODE: &str = "writer";
const DATABASE_HEALTH_TIMEOUT: Duration = Duration::from_secs(2);

type DatabaseHealthFuture = Pin<Box<dyn Future<Output = bool> + Send + 'static>>;

#[derive(Clone)]
struct WriterSnapshotSource {
    snapshot: Arc<dyn Fn() -> DhtCrawlerPipelineObservabilitySnapshot + Send + Sync>,
}

impl WriterSnapshotSource {
    fn from_handle(handle: DhtCrawlerPipelineObservabilityHandle) -> Self {
        Self {
            snapshot: Arc::new(move || handle.snapshot()),
        }
    }

    fn snapshot(&self) -> DhtCrawlerPipelineObservabilitySnapshot {
        (self.snapshot)()
    }

    #[cfg(test)]
    fn fixed(snapshot: DhtCrawlerPipelineObservabilitySnapshot) -> Self {
        Self {
            snapshot: Arc::new(move || snapshot.clone()),
        }
    }
}

#[derive(Clone)]
struct WriterStatusSource {
    snapshot: WriterSnapshotSource,
    database_health: Arc<dyn Fn() -> DatabaseHealthFuture + Send + Sync>,
}

impl WriterStatusSource {
    fn from_handle_and_pool(
        handle: DhtCrawlerPipelineObservabilityHandle,
        pool: bitmagnet_db::PgPool,
    ) -> Self {
        Self {
            snapshot: WriterSnapshotSource::from_handle(handle),
            database_health: Arc::new(move || {
                let pool = pool.clone();
                Box::pin(async move {
                    matches!(
                        tokio::time::timeout(DATABASE_HEALTH_TIMEOUT, bitmagnet_db::ping(&pool))
                            .await,
                        Ok(Ok(()))
                    )
                })
            }),
        }
    }

    fn snapshot(&self) -> DhtCrawlerPipelineObservabilitySnapshot {
        self.snapshot.snapshot()
    }

    async fn database_is_reachable(&self) -> bool {
        (self.database_health)().await
    }

    #[cfg(test)]
    fn fixed(snapshot: DhtCrawlerPipelineObservabilitySnapshot, database_reachable: bool) -> Self {
        Self {
            snapshot: WriterSnapshotSource::fixed(snapshot),
            database_health: Arc::new(move || Box::pin(async move { database_reachable })),
        }
    }
}

/// Stable, deliberately compact JSON projection for the writer process.
///
/// The response contains no database target, credentials, peer addresses,
/// torrent identities, payloads, or queue contents. Counter fields are sampled
/// independently and are not a transactional snapshot.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DhtCrawlerWriterStatusResponse {
    pub mode: &'static str,
    pub status: &'static str,
    pub ready: bool,
    pub lifecycle: &'static str,
    pub database: DhtCrawlerWriterDatabaseStatus,
    pub runtime: DhtCrawlerWriterRuntimeStatus,
    pub writes: DhtCrawlerWriterWriteStatus,
}

/// Bounded database-health portion of the writer status response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DhtCrawlerWriterDatabaseStatus {
    pub reachable: bool,
}

/// Outbound-health portion of the writer status response.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DhtCrawlerWriterRuntimeStatus {
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

/// Minimal logical-write and ambiguity counters for operator admission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DhtCrawlerWriterWriteStatus {
    pub info_hashes_triaged: u64,
    pub torrents_persisted: u64,
    pub torrent_writer_rejections: u64,
    pub torrent_writer_outcomes_unknown: u64,
    pub sources_persisted: u64,
    pub source_writer_rejections: u64,
    pub source_writer_outcomes_unknown: u64,
}

impl DhtCrawlerWriterStatusResponse {
    #[must_use]
    fn from_snapshot(
        snapshot: DhtCrawlerPipelineObservabilitySnapshot,
        database_reachable: bool,
    ) -> Self {
        let ready = snapshot.is_ready() && database_reachable;
        let (health, health_failure) = runtime_health_labels(snapshot.runtime.health.status());
        Self {
            mode: MODE,
            status: if ready { "ready" } else { "not_ready" },
            ready,
            lifecycle: lifecycle_label(snapshot.lifecycle),
            database: DhtCrawlerWriterDatabaseStatus {
                reachable: database_reachable,
            },
            runtime: DhtCrawlerWriterRuntimeStatus {
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
            writes: DhtCrawlerWriterWriteStatus {
                info_hashes_triaged: snapshot.downstream.triage.dequeued,
                torrents_persisted: snapshot.downstream.persist_torrent.projected_persisted,
                torrent_writer_rejections: snapshot.downstream.persist_torrent.writer_rejections,
                torrent_writer_outcomes_unknown: snapshot
                    .downstream
                    .persist_torrent
                    .writer_outcomes_unknown,
                sources_persisted: snapshot.downstream.persist_source.sources_persisted,
                source_writer_rejections: snapshot.downstream.persist_source.writer_rejections,
                source_writer_outcomes_unknown: snapshot
                    .downstream
                    .persist_source
                    .writer_outcomes_unknown,
            },
        }
    }
}

/// Build the sender-free health surface used by the writer executable.
pub fn writer_http_router(
    handle: DhtCrawlerPipelineObservabilityHandle,
    pool: bitmagnet_db::PgPool,
) -> Router {
    writer_http_router_from_source(WriterStatusSource::from_handle_and_pool(handle, pool))
}

fn writer_http_router_from_source(source: WriterStatusSource) -> Router {
    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/status", get(status))
        .with_state(source)
}

/// Register fresh-on-scrape gauges for the non-deployed writer process.
///
/// # Panics
///
/// Panics if called more than once in a process or if another collector has
/// already registered one of these names.
pub fn register_writer_metrics(handle: DhtCrawlerPipelineObservabilityHandle) {
    register_writer_metrics_from_source(WriterSnapshotSource::from_handle(handle));
}

fn register_writer_metrics_from_source(source: WriterSnapshotSource) {
    register_snapshot_gauge(
        &source,
        "bitmagnet_dht_crawler_pipeline_ready",
        "Whether the writer DHT pipeline has proven lifecycle and runtime readiness; database health is evaluated separately by HTTP readiness.",
        |snapshot| u8::from(snapshot.is_ready()) as f64,
    );
    register_snapshot_gauge(
        &source,
        "bitmagnet_dht_crawler_runtime_active",
        "Whether the writer-capable DHT UDP runtime is active.",
        |snapshot| u8::from(snapshot.runtime.health.active) as f64,
    );
    register_snapshot_gauge(
        &source,
        "bitmagnet_dht_crawler_info_hashes_triaged",
        "Info hashes dequeued by the writer-capable DHT triage worker.",
        |snapshot| snapshot.downstream.triage.dequeued as f64,
    );
    register_snapshot_gauge(
        &source,
        "bitmagnet_dht_crawler_torrents_persisted",
        "Projected torrents in confirmed-success writer calls.",
        |snapshot| snapshot.downstream.persist_torrent.projected_persisted as f64,
    );
    register_snapshot_gauge(
        &source,
        "bitmagnet_dht_crawler_torrent_writer_outcomes_unknown",
        "Torrent writer calls with an unknown database outcome.",
        |snapshot| snapshot.downstream.persist_torrent.writer_outcomes_unknown as f64,
    );
    register_snapshot_gauge(
        &source,
        "bitmagnet_dht_crawler_sources_persisted",
        "Source records in confirmed-success writer calls.",
        |snapshot| snapshot.downstream.persist_source.sources_persisted as f64,
    );
    register_snapshot_gauge(
        &source,
        "bitmagnet_dht_crawler_source_writer_outcomes_unknown",
        "Source writer calls with an unknown database outcome.",
        |snapshot| snapshot.downstream.persist_source.writer_outcomes_unknown as f64,
    );
}

fn register_snapshot_gauge<F>(source: &WriterSnapshotSource, name: &str, help: &str, value: F)
where
    F: Fn(DhtCrawlerPipelineObservabilitySnapshot) -> f64 + Send + Sync + 'static,
{
    let source = source.clone();
    register_computed_gauge(name, help, move || value(source.snapshot()));
}

async fn livez() -> Response {
    no_store(Json(LivenessResponse {
        mode: MODE,
        status: "up",
    }))
}

async fn readyz(State(source): State<WriterStatusSource>) -> Response {
    readiness_response(source).await
}

async fn status(State(source): State<WriterStatusSource>) -> Response {
    readiness_response(source).await
}

async fn readiness_response(source: WriterStatusSource) -> Response {
    let snapshot = source.snapshot();
    let database_reachable = source.database_is_reachable().await;
    let response = DhtCrawlerWriterStatusResponse::from_snapshot(snapshot, database_reachable);
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
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use bitmagnet_common::metrics::gather_text;
    use bitmagnet_dht::DhtRuntimeHealthSnapshot;
    use bitmagnet_dht_crawler::{
        DhtCrawlerPipelineDownstreamObservabilitySnapshot,
        DhtCrawlerPipelineMaintenanceObservabilitySnapshot,
        DhtCrawlerPipelineRuntimeObservabilitySnapshot, DhtInfoHashTriageStats,
        DhtPersistSourceWorkerStats, DhtPersistTorrentWorkerStats,
    };
    use serde_json::Value;
    use tower::ServiceExt as _;

    use super::*;

    fn snapshot(
        lifecycle: DhtCrawlerPipelineObservedLifecycle,
        health: DhtRuntimeHealthSnapshot,
    ) -> DhtCrawlerPipelineObservabilitySnapshot {
        DhtCrawlerPipelineObservabilitySnapshot {
            lifecycle,
            runtime: DhtCrawlerPipelineRuntimeObservabilitySnapshot {
                health,
                ..DhtCrawlerPipelineRuntimeObservabilitySnapshot::default()
            },
            maintenance: DhtCrawlerPipelineMaintenanceObservabilitySnapshot::default(),
            downstream: DhtCrawlerPipelineDownstreamObservabilitySnapshot {
                triage: DhtInfoHashTriageStats {
                    dequeued: 17,
                    ..DhtInfoHashTriageStats::default()
                },
                persist_torrent: DhtPersistTorrentWorkerStats {
                    projected_persisted: 11,
                    writer_rejections: 2,
                    writer_outcomes_unknown: 3,
                    ..DhtPersistTorrentWorkerStats::default()
                },
                persist_source: DhtPersistSourceWorkerStats {
                    sources_persisted: 13,
                    writer_rejections: 5,
                    writer_outcomes_unknown: 7,
                    ..DhtPersistSourceWorkerStats::default()
                },
                ..DhtCrawlerPipelineDownstreamObservabilitySnapshot::default()
            },
        }
    }

    fn ready_snapshot() -> DhtCrawlerPipelineObservabilitySnapshot {
        snapshot(
            DhtCrawlerPipelineObservedLifecycle::Ready,
            DhtRuntimeHealthSnapshot {
                active: true,
                running_for: Some(Duration::from_secs(31)),
                last_response_ago: Some(Duration::from_secs(1)),
                last_success_ago: Some(Duration::from_secs(1)),
            },
        )
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
    async fn liveness_is_content_free_and_readiness_exposes_only_bounded_status() {
        let source = WriterStatusSource::fixed(ready_snapshot(), true);
        let app = writer_http_router_from_source(source);

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
            serde_json::json!({"mode": "writer", "status": "up"})
        );

        let ready = request(app.clone(), "/readyz").await;
        assert_eq!(ready.status(), StatusCode::OK);
        assert_eq!(ready.headers()[CACHE_CONTROL], "no-store");
        let body = to_bytes(ready.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["mode"], MODE);
        assert_eq!(json["status"], "ready");
        assert_eq!(json["database"]["reachable"], true);
        assert_eq!(json["writes"]["info_hashes_triaged"], 17);
        assert_eq!(json["writes"]["torrents_persisted"], 11);
        assert_eq!(json["writes"]["sources_persisted"], 13);
        let serialized = String::from_utf8(body.to_vec()).unwrap();
        for forbidden in [
            "postgres://",
            "password",
            "database_url",
            "peer_address",
            "0123456789abcdef0123456789abcdef01234567",
        ] {
            assert!(!serialized.to_ascii_lowercase().contains(forbidden));
        }

        let status = request(app, "/status").await;
        assert_eq!(status.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readiness_fails_closed_before_ready_and_through_stopping_and_stale_health() {
        for snapshot in [
            snapshot(
                DhtCrawlerPipelineObservedLifecycle::Starting,
                DhtRuntimeHealthSnapshot::default(),
            ),
            snapshot(
                DhtCrawlerPipelineObservedLifecycle::Stopping,
                ready_snapshot().runtime.health,
            ),
            snapshot(
                DhtCrawlerPipelineObservedLifecycle::Ready,
                DhtRuntimeHealthSnapshot {
                    active: true,
                    running_for: Some(Duration::from_secs(61)),
                    last_response_ago: Some(Duration::from_secs(1)),
                    last_success_ago: Some(Duration::from_secs(61)),
                },
            ),
        ] {
            let response = request(
                writer_http_router_from_source(WriterStatusSource::fixed(snapshot, true)),
                "/readyz",
            )
            .await;
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
    }

    #[tokio::test]
    async fn database_failure_keeps_liveness_up_and_readiness_down() {
        let app =
            writer_http_router_from_source(WriterStatusSource::fixed(ready_snapshot(), false));
        assert_eq!(
            request(app.clone(), "/livez").await.status(),
            StatusCode::OK
        );
        let ready = request(app, "/readyz").await;
        assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(ready.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["database"]["reachable"], false);
        assert_eq!(json["status"], "not_ready");
    }

    #[test]
    fn metrics_are_fresh_sender_free_non_parity_gauges() {
        register_writer_metrics_from_source(WriterSnapshotSource::fixed(ready_snapshot()));
        let metrics = gather_text();
        for expected in [
            "bitmagnet_dht_crawler_pipeline_ready 1",
            "bitmagnet_dht_crawler_runtime_active 1",
            "bitmagnet_dht_crawler_info_hashes_triaged 17",
            "bitmagnet_dht_crawler_torrents_persisted 11",
            "bitmagnet_dht_crawler_torrent_writer_outcomes_unknown 3",
            "bitmagnet_dht_crawler_sources_persisted 13",
            "bitmagnet_dht_crawler_source_writer_outcomes_unknown 7",
        ] {
            assert!(metrics.lines().any(|line| line == expected), "{expected}");
        }
    }
}
