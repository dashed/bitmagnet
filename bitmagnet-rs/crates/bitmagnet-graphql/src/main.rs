//! Entry point for the PostgreSQL-backed GraphQL HTTP server.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, Response};
use axum::routing::get;
use axum::{Json, Router};
use bitmagnet_db::PgPool;
use clap::Parser;
use serde::Serialize;
use tracing::{info, warn};

const GRAPHQL_HANDLER_DURATION_HEADER: &str = "x-bitmagnet-graphql-handler-duration-us";

/// `bitmagnet-graphql` — the PostgreSQL-backed GraphQL HTTP server.
#[derive(Debug, Parser)]
#[command(
    name = "bitmagnet-graphql",
    about = "PostgreSQL-backed GraphQL HTTP server for bitmagnet"
)]
struct Args {
    /// Address on which the GraphQL HTTP server listens.
    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:3337")]
    listen_addr: String,

    /// Goose migration version the database must have applied.
    #[arg(long, env = "BITMAGNET_GRAPHQL_EXPECTED_GOOSE_VERSION")]
    expected_goose_version: Option<i64>,

    /// Comma-delimited peer GraphQL endpoints used for federated health.
    #[arg(long, env = "HEALTH_PEER_GRAPHQL_URLS", value_delimiter = ',')]
    health_peer_graphql_urls: Vec<String>,

    /// Per-peer health request timeout (Go duration syntax, for example 1500ms).
    #[arg(
        long,
        env = "HEALTH_PEER_TIMEOUT",
        default_value = "1500ms",
        value_parser = parse_go_duration
    )]
    health_peer_timeout: Duration,
}

#[derive(Clone)]
struct AppState {
    schema: bitmagnet_graphql::Schema,
    pool: PgPool,
    version: String,
}

#[derive(Serialize)]
struct StatusResponse {
    status: &'static str,
    goose_version: Option<i64>,
    version: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bitmagnet_common::init_tracing();
    let args = Args::parse();
    let addr = args
        .listen_addr
        .parse::<SocketAddr>()
        .with_context(|| format!("invalid listen address {:?}", args.listen_addr))?;

    let pool = bitmagnet_db::connect(&bitmagnet_db::DbConfig::from_env()?).await?;
    assert_goose_version(&pool, args.expected_goose_version).await?;

    let _metrics = bitmagnet_common::metrics::maybe_spawn_metrics_server().await?;
    let version =
        std::env::var("BITMAGNET_VERSION").unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_owned());
    let schema = bitmagnet_graphql::build_runtime_schema(
        version.clone(),
        pool.clone(),
        bitmagnet_graphql::RuntimeConfig {
            peer_graphql_urls: args.health_peer_graphql_urls,
            peer_timeout: args.health_peer_timeout,
        },
    );
    let state = AppState {
        schema,
        pool,
        version,
    };
    let app = app(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "bitmagnet-graphql starting");
    axum::serve(listener, app)
        .with_graceful_shutdown(bitmagnet_common::serve::shutdown_signal())
        .await?;

    info!("bitmagnet-graphql stopped");
    Ok(())
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/", get(playground))
        .route(
            "/graphql",
            get(graphql_handler)
                .post(graphql_handler)
                .layer(middleware::from_fn(graphql_handler_duration)),
        )
        .route("/livez", get(livez))
        .route("/status", get(status))
        .with_state(state)
}

async fn graphql_handler(
    State(state): State<AppState>,
    request: GraphQLRequest,
) -> GraphQLResponse {
    state.schema.execute(request.into_inner()).await.into()
}

/// Measures the complete Axum `/graphql` route-service boundary: request body
/// extraction, GraphQL parse/validation/execution, and eager response JSON
/// serialization all run inside `next`. Router matching and transport writes
/// happen outside this boundary.
async fn graphql_handler_duration(request: Request, next: Next) -> Response {
    let started = Instant::now();
    let mut response = next.run(request).await;
    let elapsed_us = started.elapsed().as_micros().max(1);

    if let Ok(value) = HeaderValue::from_str(&elapsed_us.to_string()) {
        response.headers_mut().insert(
            HeaderName::from_static(GRAPHQL_HANDLER_DURATION_HEADER),
            value,
        );
    }

    response
}

async fn playground() -> Html<String> {
    Html(playground_source(GraphQLPlaygroundConfig::new("/graphql")))
}

async fn livez() -> &'static str {
    "ok"
}

async fn status(State(state): State<AppState>) -> Result<Json<StatusResponse>, StatusCode> {
    let goose_version =
        sqlx::query_scalar::<_, Option<i64>>("SELECT max(version_id) FROM goose_db_version")
            .fetch_one(&state.pool)
            .await
            .map_err(|error| {
                warn!(%error, "GraphQL readiness query failed");
                StatusCode::SERVICE_UNAVAILABLE
            })?;

    Ok(Json(StatusResponse {
        status: "ok",
        goose_version,
        version: state.version,
    }))
}

async fn assert_goose_version(pool: &PgPool, expected: Option<i64>) -> anyhow::Result<()> {
    let Some(expected) = expected else {
        warn!(
            "goose schema version assertion skipped; no \
             BITMAGNET_GRAPHQL_EXPECTED_GOOSE_VERSION configured"
        );
        return Ok(());
    };

    let detected =
        match sqlx::query_scalar::<_, Option<i64>>("SELECT max(version_id) FROM goose_db_version")
            .fetch_one(pool)
            .await
        {
            Ok(detected) => detected,
            Err(error) => anyhow::bail!(
                "goose schema version assertion failed: detected=unavailable, \
             expected={expected}: {error}"
            ),
        };

    if let Some(message) = goose_mismatch(expected, detected) {
        anyhow::bail!(message);
    }

    info!(goose_version = expected, "goose schema version confirmed");
    Ok(())
}

fn goose_mismatch(expected: i64, detected: Option<i64>) -> Option<String> {
    match detected {
        Some(detected) if detected == expected => None,
        Some(detected) => Some(format!(
            "goose schema version mismatch: detected={detected}, expected={expected}"
        )),
        None => Some(format!(
            "goose schema version mismatch: detected=none, expected={expected}"
        )),
    }
}

fn parse_go_duration(value: &str) -> Result<Duration, String> {
    let value = value.trim();
    let (negative, mut remaining) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if remaining == "0" {
        return Ok(Duration::ZERO);
    }
    let mut nanos = 0_u128;
    let mut parsed_parts = 0_u32;

    while !remaining.is_empty() {
        let whole_len = remaining.bytes().take_while(u8::is_ascii_digit).count();
        let whole = &remaining[..whole_len];
        remaining = &remaining[whole_len..];

        let fraction = if let Some(after_dot) = remaining.strip_prefix('.') {
            let fraction_len = after_dot.bytes().take_while(u8::is_ascii_digit).count();
            let fraction = &after_dot[..fraction_len];
            remaining = &after_dot[fraction_len..];
            fraction
        } else {
            ""
        };
        if whole.is_empty() && fraction.is_empty() {
            return Err(format!("invalid duration {value:?}"));
        }
        let whole = if whole.is_empty() {
            0
        } else {
            whole
                .parse::<u128>()
                .map_err(|error| format!("invalid duration {value:?}: {error}"))?
        };

        let (unit, scale) = [
            ("ns", 1_u128),
            ("us", 1_000_u128),
            ("µs", 1_000_u128),
            ("μs", 1_000_u128),
            ("ms", 1_000_000_u128),
            ("s", 1_000_000_000_u128),
            ("m", 60_000_000_000_u128),
            ("h", 3_600_000_000_000_u128),
        ]
        .into_iter()
        .find(|(unit, _)| remaining.starts_with(unit))
        .ok_or_else(|| format!("invalid duration {value:?}"))?;
        remaining = &remaining[unit.len()..];

        let whole_nanos = whole
            .checked_mul(scale)
            .ok_or_else(|| format!("invalid duration {value:?}"))?;
        let fraction_nanos = fraction.bytes().rev().fold(0_u128, |acc, digit| {
            (scale * u128::from(digit - b'0') + acc) / 10
        });
        let part_nanos = whole_nanos
            .checked_add(fraction_nanos)
            .ok_or_else(|| format!("invalid duration {value:?}"))?;
        nanos = nanos
            .checked_add(part_nanos)
            .ok_or_else(|| format!("invalid duration {value:?}"))?;
        parsed_parts += 1;
    }

    let max_nanos = if negative {
        1_u128 << 63
    } else {
        (1_u128 << 63) - 1
    };
    if parsed_parts == 0 || nanos > max_nanos {
        return Err(format!("invalid duration {value:?}"));
    }

    // Go accepts negative durations; the peer configuration treats every
    // non-positive value as the 1500ms default. Duration cannot represent a
    // negative value, so preserve that effective behavior as zero here.
    if negative {
        return Ok(Duration::ZERO);
    }

    Ok(Duration::from_nanos(nanos as u64))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use clap::Parser;
    use serde_json::{json, Value};

    use super::{
        app, goose_mismatch, parse_go_duration, AppState, Args, GRAPHQL_HANDLER_DURATION_HEADER,
    };

    struct ArgsEnvRestore {
        listen_addr: Option<OsString>,
        expected_goose_version: Option<OsString>,
        health_peer_graphql_urls: Option<OsString>,
        health_peer_timeout: Option<OsString>,
    }

    impl ArgsEnvRestore {
        fn clear() -> Self {
            let restore = Self {
                listen_addr: std::env::var_os("LISTEN_ADDR"),
                expected_goose_version: std::env::var_os(
                    "BITMAGNET_GRAPHQL_EXPECTED_GOOSE_VERSION",
                ),
                health_peer_graphql_urls: std::env::var_os("HEALTH_PEER_GRAPHQL_URLS"),
                health_peer_timeout: std::env::var_os("HEALTH_PEER_TIMEOUT"),
            };
            std::env::remove_var("LISTEN_ADDR");
            std::env::remove_var("BITMAGNET_GRAPHQL_EXPECTED_GOOSE_VERSION");
            std::env::remove_var("HEALTH_PEER_GRAPHQL_URLS");
            std::env::remove_var("HEALTH_PEER_TIMEOUT");
            restore
        }
    }

    impl Drop for ArgsEnvRestore {
        fn drop(&mut self) {
            restore_env("LISTEN_ADDR", self.listen_addr.take());
            restore_env(
                "BITMAGNET_GRAPHQL_EXPECTED_GOOSE_VERSION",
                self.expected_goose_version.take(),
            );
            restore_env(
                "HEALTH_PEER_GRAPHQL_URLS",
                self.health_peer_graphql_urls.take(),
            );
            restore_env("HEALTH_PEER_TIMEOUT", self.health_peer_timeout.take());
        }
    }

    fn restore_env(name: &str, value: Option<OsString>) {
        match value {
            Some(value) => std::env::set_var(name, value),
            None => std::env::remove_var(name),
        }
    }

    fn assert_positive_duration_header(response: &reqwest::Response) {
        let raw = response
            .headers()
            .get(GRAPHQL_HANDLER_DURATION_HEADER)
            .expect("GraphQL handler duration header")
            .to_str()
            .expect("ASCII GraphQL handler duration header");
        let micros = raw
            .parse::<u128>()
            .expect("integer GraphQL handler duration header");
        assert!(micros > 0, "GraphQL handler duration must be positive");
    }

    #[test]
    fn args_parse_defaults_and_overrides() {
        let _restore = ArgsEnvRestore::clear();

        let defaults =
            Args::try_parse_from(["bitmagnet-graphql"]).expect("default GraphQL arguments parse");
        assert_eq!(defaults.listen_addr, "0.0.0.0:3337");
        assert_eq!(defaults.expected_goose_version, None);
        assert!(defaults.health_peer_graphql_urls.is_empty());
        assert_eq!(
            defaults.health_peer_timeout,
            std::time::Duration::from_millis(1_500)
        );

        let overridden = Args::try_parse_from([
            "bitmagnet-graphql",
            "--listen-addr",
            "127.0.0.1:43337",
            "--expected-goose-version",
            "2026071201",
            "--health-peer-graphql-urls",
            "http://peer-a/graphql,http://peer-b/graphql",
            "--health-peer-timeout",
            "2.5s",
        ])
        .expect("explicit GraphQL arguments parse");
        assert_eq!(overridden.listen_addr, "127.0.0.1:43337");
        assert_eq!(overridden.expected_goose_version, Some(2_026_071_201));
        assert_eq!(
            overridden.health_peer_graphql_urls,
            ["http://peer-a/graphql", "http://peer-b/graphql"]
        );
        assert_eq!(
            overridden.health_peer_timeout,
            std::time::Duration::from_millis(2_500)
        );
    }

    #[test]
    fn goose_mismatch_reports_missing_and_wrong_versions() {
        assert_eq!(goose_mismatch(42, Some(42)), None);
        assert_eq!(
            goose_mismatch(42, Some(41)).as_deref(),
            Some("goose schema version mismatch: detected=41, expected=42")
        );
        assert_eq!(
            goose_mismatch(42, None).as_deref(),
            Some("goose schema version mismatch: detected=none, expected=42")
        );
    }

    #[test]
    fn go_duration_parser_accepts_compound_fractional_and_non_positive_values() {
        assert_eq!(
            parse_go_duration("0.25s").expect("fractional seconds"),
            std::time::Duration::from_millis(250)
        );
        assert_eq!(
            parse_go_duration("1m30.5s").expect("compound duration"),
            std::time::Duration::from_millis(90_500)
        );
        assert_eq!(
            parse_go_duration("0").expect("zero"),
            std::time::Duration::ZERO
        );
        assert_eq!(
            parse_go_duration("-1s").expect("negative fallback"),
            std::time::Duration::ZERO
        );
        assert_eq!(
            parse_go_duration("+0").expect("positive signed zero"),
            std::time::Duration::ZERO
        );
        assert_eq!(
            parse_go_duration("-0").expect("negative signed zero"),
            std::time::Duration::ZERO
        );
        assert_eq!(
            parse_go_duration("0.9ns").expect("sub-nanosecond truncation"),
            std::time::Duration::ZERO
        );
        assert_eq!(
            parse_go_duration("1.9ns").expect("fractional nanosecond truncation"),
            std::time::Duration::from_nanos(1)
        );
        assert!(parse_go_duration("1500").is_err());
    }

    #[test]
    fn go_duration_parser_enforces_the_signed_int64_nanosecond_range() {
        assert_eq!(
            parse_go_duration("9223372036854775807ns").expect("maximum positive duration"),
            std::time::Duration::from_nanos(i64::MAX as u64)
        );
        assert!(parse_go_duration("9223372036854775808ns").is_err());
        assert_eq!(
            parse_go_duration("-9223372036854775808ns").expect("minimum negative duration"),
            std::time::Duration::ZERO
        );
        assert!(parse_go_duration("-9223372036854775809ns").is_err());
    }

    #[tokio::test]
    async fn graphql_route_reports_handler_duration_for_success_and_errors_only() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/bitmagnet")
            .expect("lazy postgres pool");
        let version = "test-version".to_owned();
        let router = app(AppState {
            schema: bitmagnet_graphql::build_schema(version.clone()),
            pool,
            version: version.clone(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind GraphQL test server");
        let address = listener.local_addr().expect("GraphQL test server address");
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("serve GraphQL test router");
        });
        let client = reqwest::Client::new();
        let graphql_url = format!("http://{address}/graphql");

        let success = client
            .post(&graphql_url)
            .json(&json!({"query": "{ version }"}))
            .send()
            .await
            .expect("successful GraphQL request");
        assert_eq!(success.status(), reqwest::StatusCode::OK);
        assert_positive_duration_header(&success);
        let success_body = success
            .json::<Value>()
            .await
            .expect("successful GraphQL response JSON");
        assert_eq!(success_body["data"]["version"], version);

        let error = client
            .post(&graphql_url)
            .json(&json!({"query": "{ fieldThatDoesNotExist }"}))
            .send()
            .await
            .expect("GraphQL validation-error request");
        assert_eq!(error.status(), reqwest::StatusCode::OK);
        assert_positive_duration_header(&error);
        let error_body = error
            .json::<Value>()
            .await
            .expect("GraphQL validation-error response JSON");
        assert!(
            error_body["errors"]
                .as_array()
                .is_some_and(|errors| !errors.is_empty()),
            "GraphQL validation error response must contain errors"
        );

        let livez = client
            .get(format!("http://{address}/livez"))
            .send()
            .await
            .expect("livez request");
        assert!(
            livez
                .headers()
                .get(GRAPHQL_HANDLER_DURATION_HEADER)
                .is_none(),
            "non-GraphQL routes must not expose the GraphQL duration header"
        );
        let playground = client
            .get(format!("http://{address}/"))
            .send()
            .await
            .expect("playground request");
        assert!(
            playground
                .headers()
                .get(GRAPHQL_HANDLER_DURATION_HEADER)
                .is_none(),
            "the playground must not expose the GraphQL duration header"
        );

        server.abort();
        let _ = server.await;
    }
}
