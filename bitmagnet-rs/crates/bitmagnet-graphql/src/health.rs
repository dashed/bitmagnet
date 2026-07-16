use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::schema::enums::HealthStatus;
use crate::schema::objects::{HealthCheck, Worker};
use crate::schema::scalars;

const HEALTH_PEER_CHECK_KEY: &str = "status_peer";
const DEFAULT_PEER_TIMEOUT: Duration = Duration::from_millis(1_500);
const POSTGRES_CHECK_INTERVAL: Duration = Duration::from_secs(30);
const POSTGRES_CHECK_TIMEOUT: Duration = Duration::from_secs(5);
const HEALTH_PEER_QUERY: &str = r"query HealthPeer {
  health {
    status
    checks {
      key
      status
      timestamp
      error
    }
  }
  workers {
    listAll {
      workers {
        key
        started
      }
    }
  }
}";

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub peer_graphql_urls: Vec<String>,
    pub peer_timeout: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            peer_graphql_urls: Vec::new(),
            peer_timeout: DEFAULT_PEER_TIMEOUT,
        }
    }
}

#[derive(Clone)]
pub(crate) struct HealthRuntime {
    postgres_health: watch::Receiver<HealthCheckSnapshot>,
    peer_graphql_urls: Vec<String>,
    peer_timeout: Duration,
    client: reqwest::Client,
}

impl HealthRuntime {
    pub(crate) fn new(pool: bitmagnet_db::PgPool, config: RuntimeConfig) -> Self {
        let (postgres_health_tx, postgres_health) = watch::channel(HealthCheckSnapshot {
            key: "postgres".to_owned(),
            status: HealthStatus::Unknown,
            timestamp: Utc::now(),
            error: None,
        });
        tokio::spawn(run_postgres_health_probe(pool, postgres_health_tx));

        Self::with_postgres_health(postgres_health, config)
    }

    fn with_postgres_health(
        postgres_health: watch::Receiver<HealthCheckSnapshot>,
        config: RuntimeConfig,
    ) -> Self {
        let peer_timeout = if config.peer_timeout.is_zero() {
            DEFAULT_PEER_TIMEOUT
        } else {
            config.peer_timeout
        };

        Self {
            postgres_health,
            peer_graphql_urls: config.peer_graphql_urls,
            peer_timeout,
            client: reqwest::Client::new(),
        }
    }

    pub(crate) async fn health(&self) -> HealthSnapshot {
        let local = self.local_health();
        let (snapshots, errors) = self.fetch_peer_snapshots().await;

        merge_peer_health(local, &snapshots, &errors)
    }

    pub(crate) async fn workers(&self) -> Vec<Worker> {
        let local = vec![Worker {
            key: "http_server".to_owned(),
            started: true,
        }];
        let (snapshots, _) = self.fetch_peer_snapshots().await;

        merge_peer_workers(local, &snapshots)
    }

    fn local_health(&self) -> HealthSnapshot {
        let check = self.postgres_health.borrow().clone();
        HealthSnapshot {
            status: check.status,
            checks: vec![check],
        }
    }

    async fn fetch_peer_snapshots(&self) -> (Vec<PeerSnapshot>, Vec<String>) {
        if self.peer_graphql_urls.is_empty() {
            return (Vec::new(), Vec::new());
        }

        let mut snapshots = Vec::with_capacity(self.peer_graphql_urls.len());
        let mut errors = Vec::new();

        for raw_url in &self.peer_graphql_urls {
            let url = raw_url.trim();
            if url.is_empty() {
                continue;
            }

            let fetch = fetch_peer_snapshot(&self.client, url);
            match tokio::time::timeout(self.peer_timeout, fetch).await {
                Ok(Ok(snapshot)) => snapshots.push(snapshot),
                Ok(Err(error)) => errors.push(format!("{url}: {error}")),
                Err(_) => errors.push(format!(
                    "{url}: request timed out after {:?}",
                    self.peer_timeout
                )),
            }
        }

        (snapshots, errors)
    }
}

async fn run_postgres_health_probe(
    pool: bitmagnet_db::PgPool,
    tx: watch::Sender<HealthCheckSnapshot>,
) {
    let mut interval = tokio::time::interval(POSTGRES_CHECK_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            _ = tx.closed() => break,
            _ = interval.tick() => {
                let snapshot = postgres_health_snapshot(
                    async {
                        sqlx::query("SELECT 1")
                            .execute(&pool)
                            .await
                            .map(|_| ())
                    },
                    POSTGRES_CHECK_TIMEOUT,
                )
                .await;
                if tx.send(snapshot).is_err() {
                    break;
                }
            }
        }
    }
}

async fn postgres_health_snapshot<F, E>(probe: F, timeout: Duration) -> HealthCheckSnapshot
where
    F: Future<Output = Result<(), E>>,
    E: std::fmt::Display,
{
    let result = tokio::time::timeout(timeout, probe).await;
    let (status, error) = match result {
        Ok(Ok(())) => (HealthStatus::Up, None),
        Ok(Err(error)) => (
            HealthStatus::Down,
            Some(format!("failed to ping database: {error}")),
        ),
        Err(_) => (
            HealthStatus::Down,
            Some(format!("postgres health check timed out after {timeout:?}")),
        ),
    };

    HealthCheckSnapshot {
        key: "postgres".to_owned(),
        status,
        timestamp: Utc::now(),
        error,
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HealthSnapshot {
    pub(crate) status: HealthStatus,
    pub(crate) checks: Vec<HealthCheckSnapshot>,
}

impl HealthSnapshot {
    pub(crate) fn into_graphql_checks(self) -> Vec<HealthCheck> {
        self.checks
            .into_iter()
            .map(HealthCheckSnapshot::into_graphql)
            .collect()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct HealthCheckSnapshot {
    key: String,
    status: HealthStatus,
    timestamp: DateTime<Utc>,
    error: Option<String>,
}

impl HealthCheckSnapshot {
    fn into_graphql(self) -> HealthCheck {
        HealthCheck {
            error: self.error,
            key: self.key,
            status: self.status,
            timestamp: scalars::DateTime(
                self.timestamp.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            ),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct WorkerSnapshot {
    key: String,
    started: bool,
}

#[derive(Clone, Debug)]
struct PeerSnapshot {
    health: HealthSnapshot,
    workers: Vec<WorkerSnapshot>,
}

#[derive(Serialize)]
struct PeerRequest<'a> {
    query: &'a str,
}

#[derive(Deserialize)]
struct PeerResponse {
    data: Option<PeerData>,
    #[serde(default)]
    errors: Vec<PeerError>,
}

#[derive(Deserialize)]
struct PeerData {
    health: PeerHealth,
    workers: PeerWorkers,
}

#[derive(Deserialize)]
struct PeerHealth {
    status: HealthStatus,
    checks: Vec<HealthCheckSnapshot>,
}

#[derive(Deserialize)]
struct PeerWorkers {
    #[serde(rename = "listAll")]
    list_all: PeerWorkersList,
}

#[derive(Deserialize)]
struct PeerWorkersList {
    workers: Vec<WorkerSnapshot>,
}

#[derive(Deserialize)]
struct PeerError {
    message: String,
}

async fn fetch_peer_snapshot(client: &reqwest::Client, url: &str) -> Result<PeerSnapshot, String> {
    let response = client
        .post(url)
        .json(&PeerRequest {
            query: HEALTH_PEER_QUERY,
        })
        .send()
        .await
        .map_err(|error| error.to_string())?;

    if !response.status().is_success() {
        return Err(format!("unexpected status {}", response.status()));
    }

    let response: PeerResponse = response
        .json()
        .await
        .map_err(|error| format!("invalid GraphQL response: {error}"))?;

    if !response.errors.is_empty() {
        let messages = response
            .errors
            .into_iter()
            .map(|error| error.message)
            .collect::<Vec<_>>()
            .join("; ");
        return Err(format!("graphql errors: {messages}"));
    }

    let data = response
        .data
        .ok_or_else(|| "GraphQL response did not contain data".to_owned())?;

    Ok(PeerSnapshot {
        health: HealthSnapshot {
            status: data.health.status,
            checks: data.health.checks,
        },
        workers: data.workers.list_all.workers,
    })
}

fn merge_peer_health(
    local: HealthSnapshot,
    peers: &[PeerSnapshot],
    errors: &[String],
) -> HealthSnapshot {
    let mut checks = local
        .checks
        .into_iter()
        .map(|check| (check.key.clone(), check))
        .collect::<BTreeMap<_, _>>();

    for peer in peers {
        for candidate in &peer.health.checks {
            merge_health_check(&mut checks, candidate.clone());
        }
    }

    if !errors.is_empty() {
        merge_health_check(
            &mut checks,
            HealthCheckSnapshot {
                key: HEALTH_PEER_CHECK_KEY.to_owned(),
                status: HealthStatus::Down,
                timestamp: Utc::now(),
                error: Some(errors.join("; ")),
            },
        );
    }

    let checks = checks.into_values().collect::<Vec<_>>();
    HealthSnapshot {
        status: aggregate_health_status(&checks),
        checks,
    }
}

fn merge_health_check(
    checks: &mut BTreeMap<String, HealthCheckSnapshot>,
    candidate: HealthCheckSnapshot,
) {
    match checks.get(&candidate.key) {
        Some(existing) if !should_replace_health_check(existing, &candidate) => {}
        _ => {
            checks.insert(candidate.key.clone(), candidate);
        }
    }
}

fn merge_peer_workers(local: Vec<Worker>, peers: &[PeerSnapshot]) -> Vec<Worker> {
    let mut workers = local
        .into_iter()
        .map(|worker| (worker.key.clone(), worker))
        .collect::<BTreeMap<_, _>>();

    for peer in peers {
        for candidate in &peer.workers {
            let worker = workers.entry(candidate.key.clone()).or_insert(Worker {
                key: candidate.key.clone(),
                started: candidate.started,
            });
            worker.started |= candidate.started;
        }
    }

    workers.into_values().collect()
}

fn should_replace_health_check(
    existing: &HealthCheckSnapshot,
    candidate: &HealthCheckSnapshot,
) -> bool {
    if existing.status == HealthStatus::Inactive && candidate.status != HealthStatus::Inactive {
        return true;
    }

    let existing_severity = health_status_severity(existing.status);
    let candidate_severity = health_status_severity(candidate.status);
    if candidate_severity != existing_severity {
        return candidate_severity > existing_severity;
    }

    candidate.timestamp > existing.timestamp
}

fn aggregate_health_status(checks: &[HealthCheckSnapshot]) -> HealthStatus {
    if checks.is_empty() {
        return HealthStatus::Unknown;
    }

    let mut status = HealthStatus::Up;
    for check in checks {
        match check.status {
            HealthStatus::Down => return HealthStatus::Down,
            HealthStatus::Unknown => status = HealthStatus::Unknown,
            HealthStatus::Inactive | HealthStatus::Up => {}
        }
    }

    status
}

const fn health_status_severity(status: HealthStatus) -> u8 {
    match status {
        HealthStatus::Down => 3,
        HealthStatus::Unknown => 2,
        HealthStatus::Up => 1,
        HealthStatus::Inactive => 0,
    }
}

#[cfg(test)]
mod tests {
    use axum::routing::post;
    use axum::{Json, Router};
    use chrono::{TimeZone, Timelike};
    use serde_json::{json, Value};

    use super::*;

    fn check(key: &str, status: HealthStatus, second: u32) -> HealthCheckSnapshot {
        HealthCheckSnapshot {
            key: key.to_owned(),
            status,
            timestamp: Utc
                .with_ymd_and_hms(2026, 7, 13, 1, 2, second)
                .single()
                .expect("valid timestamp"),
            error: None,
        }
    }

    #[test]
    fn empty_peer_set_is_a_sorted_single_instance_noop() {
        let local = HealthSnapshot {
            status: HealthStatus::Up,
            checks: vec![
                check("postgres", HealthStatus::Up, 2),
                check("dht", HealthStatus::Inactive, 1),
            ],
        };

        let merged = merge_peer_health(local, &[], &[]);

        assert_eq!(merged.status, HealthStatus::Up);
        assert_eq!(
            merged
                .checks
                .iter()
                .map(|check| check.key.as_str())
                .collect::<Vec<_>>(),
            ["dht", "postgres"]
        );
    }

    #[test]
    fn peer_merge_prefers_active_then_severity_then_recency() {
        let local = HealthSnapshot {
            status: HealthStatus::Unknown,
            checks: vec![
                check("dht", HealthStatus::Inactive, 5),
                check("postgres", HealthStatus::Up, 5),
                check("tmdb", HealthStatus::Unknown, 5),
            ],
        };
        let peer = PeerSnapshot {
            health: HealthSnapshot {
                status: HealthStatus::Down,
                checks: vec![
                    check("dht", HealthStatus::Up, 1),
                    check("postgres", HealthStatus::Down, 1),
                    check("tmdb", HealthStatus::Unknown, 6),
                ],
            },
            workers: Vec::new(),
        };

        let merged = merge_peer_health(local, &[peer], &[]);

        assert_eq!(merged.status, HealthStatus::Down);
        assert_eq!(merged.checks[0].key, "dht");
        assert_eq!(merged.checks[0].status, HealthStatus::Up);
        assert_eq!(merged.checks[1].key, "postgres");
        assert_eq!(merged.checks[1].status, HealthStatus::Down);
        assert_eq!(merged.checks[2].key, "tmdb");
        assert_eq!(merged.checks[2].timestamp.second(), 6);
    }

    #[test]
    fn peer_failures_add_a_down_status_peer_check_in_input_order() {
        let merged = merge_peer_health(
            HealthSnapshot {
                status: HealthStatus::Up,
                checks: vec![check("postgres", HealthStatus::Up, 1)],
            },
            &[],
            &["peer-a: 503".to_owned(), "peer-b: timeout".to_owned()],
        );

        assert_eq!(merged.status, HealthStatus::Down);
        let peer_check = merged
            .checks
            .iter()
            .find(|check| check.key == HEALTH_PEER_CHECK_KEY)
            .expect("status_peer check");
        assert_eq!(peer_check.status, HealthStatus::Down);
        assert_eq!(
            peer_check.error.as_deref(),
            Some("peer-a: 503; peer-b: timeout")
        );
    }

    #[test]
    fn peer_failure_check_obeys_existing_timestamp_precedence() {
        let future_timestamp = Utc::now() + chrono::Duration::hours(1);
        let merged = merge_peer_health(
            HealthSnapshot {
                status: HealthStatus::Down,
                checks: vec![HealthCheckSnapshot {
                    key: HEALTH_PEER_CHECK_KEY.to_owned(),
                    status: HealthStatus::Down,
                    timestamp: future_timestamp,
                    error: Some("newer peer failure".to_owned()),
                }],
            },
            &[],
            &["local peer failure".to_owned()],
        );

        let peer_check = merged
            .checks
            .iter()
            .find(|check| check.key == HEALTH_PEER_CHECK_KEY)
            .expect("status_peer check");
        assert_eq!(peer_check.timestamp, future_timestamp);
        assert_eq!(peer_check.error.as_deref(), Some("newer peer failure"));
    }

    #[tokio::test]
    async fn postgres_probe_reports_success_failure_and_timeout_deterministically() {
        let success =
            postgres_health_snapshot(async { Ok::<(), &'static str>(()) }, POSTGRES_CHECK_TIMEOUT)
                .await;
        assert_eq!(success.status, HealthStatus::Up);
        assert_eq!(success.error, None);

        let failure = postgres_health_snapshot(
            async { Err::<(), _>("database unavailable") },
            POSTGRES_CHECK_TIMEOUT,
        )
        .await;
        assert_eq!(failure.status, HealthStatus::Down);
        assert_eq!(
            failure.error.as_deref(),
            Some("failed to ping database: database unavailable")
        );

        let timeout = postgres_health_snapshot(
            std::future::pending::<Result<(), &'static str>>(),
            Duration::ZERO,
        )
        .await;
        assert_eq!(timeout.status, HealthStatus::Down);
        assert_eq!(
            timeout.error.as_deref(),
            Some("postgres health check timed out after 0ns")
        );
    }

    #[test]
    fn local_health_reads_the_cached_postgres_snapshot() {
        let (tx, rx) = watch::channel(check("postgres", HealthStatus::Up, 1));
        let runtime = HealthRuntime::with_postgres_health(rx, RuntimeConfig::default());

        let initial = runtime.local_health();
        assert_eq!(initial.status, HealthStatus::Up);
        assert_eq!(initial.checks[0].timestamp.second(), 1);

        tx.send(check("postgres", HealthStatus::Down, 2))
            .expect("update cached postgres health");
        let updated = runtime.local_health();
        assert_eq!(updated.status, HealthStatus::Down);
        assert_eq!(updated.checks[0].timestamp.second(), 2);
    }

    #[test]
    fn worker_merge_is_sorted_and_started_true_wins() {
        let peers = vec![PeerSnapshot {
            health: HealthSnapshot {
                status: HealthStatus::Up,
                checks: Vec::new(),
            },
            workers: vec![
                WorkerSnapshot {
                    key: "queue_server".to_owned(),
                    started: true,
                },
                WorkerSnapshot {
                    key: "http_server".to_owned(),
                    started: false,
                },
            ],
        }];

        let workers = merge_peer_workers(
            vec![Worker {
                key: "http_server".to_owned(),
                started: true,
            }],
            &peers,
        );

        assert_eq!(workers.len(), 2);
        assert_eq!(workers[0].key, "http_server");
        assert!(workers[0].started);
        assert_eq!(workers[1].key, "queue_server");
        assert!(workers[1].started);
    }

    #[tokio::test]
    async fn peer_fetch_uses_the_federation_query_and_decodes_snapshot() {
        let app = Router::new().route(
            "/graphql",
            post(|Json(request): Json<Value>| async move {
                assert_eq!(request["query"], HEALTH_PEER_QUERY);
                Json(json!({
                    "data": {
                        "health": {
                            "status": "up",
                            "checks": [{
                                "key": "postgres",
                                "status": "up",
                                "timestamp": "2026-07-13T01:02:03Z",
                                "error": null
                            }]
                        },
                        "workers": {
                            "listAll": {
                                "workers": [{"key": "queue_server", "started": true}]
                            }
                        }
                    }
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind peer fixture server");
        let addr = listener.local_addr().expect("peer fixture address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve peer fixture response");
        });

        let snapshot =
            fetch_peer_snapshot(&reqwest::Client::new(), &format!("http://{addr}/graphql"))
                .await
                .expect("fetch peer snapshot");
        server.abort();

        assert_eq!(snapshot.health.status, HealthStatus::Up);
        assert_eq!(snapshot.health.checks[0].key, "postgres");
        assert_eq!(snapshot.workers[0].key, "queue_server");
        assert!(snapshot.workers[0].started);
    }
}
