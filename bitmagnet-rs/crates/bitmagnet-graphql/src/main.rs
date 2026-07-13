//! Entry point for the PostgreSQL-backed GraphQL HTTP server.

use std::net::SocketAddr;

use anyhow::Context;
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use bitmagnet_db::PgPool;
use clap::Parser;
use serde::Serialize;
use tracing::{info, warn};

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
    let schema = bitmagnet_graphql::build_schema(version.clone());
    let state = AppState {
        schema,
        pool,
        version,
    };
    let app = Router::new()
        .route("/", get(playground))
        .route("/graphql", get(graphql_handler).post(graphql_handler))
        .route("/livez", get(livez))
        .route("/status", get(status))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "bitmagnet-graphql starting");
    axum::serve(listener, app)
        .with_graceful_shutdown(bitmagnet_common::serve::shutdown_signal())
        .await?;

    info!("bitmagnet-graphql stopped");
    Ok(())
}

async fn graphql_handler(
    State(state): State<AppState>,
    request: GraphQLRequest,
) -> GraphQLResponse {
    state.schema.execute(request.into_inner()).await.into()
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

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use clap::Parser;

    use super::{goose_mismatch, Args};

    struct ArgsEnvRestore {
        listen_addr: Option<OsString>,
        expected_goose_version: Option<OsString>,
    }

    impl ArgsEnvRestore {
        fn clear() -> Self {
            let restore = Self {
                listen_addr: std::env::var_os("LISTEN_ADDR"),
                expected_goose_version: std::env::var_os(
                    "BITMAGNET_GRAPHQL_EXPECTED_GOOSE_VERSION",
                ),
            };
            std::env::remove_var("LISTEN_ADDR");
            std::env::remove_var("BITMAGNET_GRAPHQL_EXPECTED_GOOSE_VERSION");
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
        }
    }

    fn restore_env(name: &str, value: Option<OsString>) {
        match value {
            Some(value) => std::env::set_var(name, value),
            None => std::env::remove_var(name),
        }
    }

    #[test]
    fn args_parse_defaults_and_overrides() {
        let _restore = ArgsEnvRestore::clear();

        let defaults =
            Args::try_parse_from(["bitmagnet-graphql"]).expect("default GraphQL arguments parse");
        assert_eq!(defaults.listen_addr, "0.0.0.0:3337");
        assert_eq!(defaults.expected_goose_version, None);

        let overridden = Args::try_parse_from([
            "bitmagnet-graphql",
            "--listen-addr",
            "127.0.0.1:43337",
            "--expected-goose-version",
            "2026071201",
        ])
        .expect("explicit GraphQL arguments parse");
        assert_eq!(overridden.listen_addr, "127.0.0.1:43337");
        assert_eq!(overridden.expected_goose_version, Some(2_026_071_201));
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
}
