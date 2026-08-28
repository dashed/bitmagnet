//! Entry point for the PostgreSQL-backed Torznab HTTP adapter.

use std::net::SocketAddr;
use std::time::Instant;

use anyhow::Context;
use axum::extract::Request;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::Router;
use bitmagnet_common::metrics::{
    maybe_spawn_metrics_server, register_histogram, register_int_counter,
};
use bitmagnet_db::DbConfig;
use bitmagnet_torznab::{admit_pg, pg_router, Config};
use clap::Parser;
use prometheus::{Histogram, IntCounter};
use tracing::info;

/// `bitmagnet-torznab` — the PostgreSQL-backed Torznab HTTP adapter.
#[derive(Debug, Parser)]
#[command(
    name = "bitmagnet-torznab",
    about = "PostgreSQL-backed Torznab HTTP adapter for bitmagnet"
)]
struct Args {
    /// Address on which the Torznab HTTP server listens.
    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:3336")]
    listen_addr: String,

    /// Goose migration version the database must have applied.
    #[arg(
        long,
        env = "BITMAGNET_TORZNAB_EXPECTED_GOOSE_VERSION",
        value_parser = parse_positive_i64
    )]
    expected_goose_version: i64,
}

/// Request metrics for the Rust Torznab adapter.
///
/// Go emits no Torznab-specific Prometheus series, so these are new
/// Rust-adapter observability rather than a Go parity port.
#[derive(Clone)]
struct TorznabMetrics {
    requests_total: IntCounter,
    request_duration_seconds: Histogram,
}

impl TorznabMetrics {
    fn register() -> Self {
        Self {
            requests_total: register_int_counter(
                "torznab_requests_total",
                "Total Torznab HTTP requests handled.",
            ),
            request_duration_seconds: register_histogram(
                "torznab_request_duration_seconds",
                "Torznab HTTP request handling latency in seconds.",
                vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
            ),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bitmagnet_common::init_tracing();
    let args = Args::parse();
    let addr = args
        .listen_addr
        .parse::<SocketAddr>()
        .with_context(|| format!("invalid listen address {:?}", args.listen_addr))?;

    let pool = bitmagnet_db::connect(&DbConfig::from_compatible_env()?).await?;
    let goose_head = admit_pg(&pool, args.expected_goose_version).await?;
    info!(
        goose_version = goose_head.version,
        "goose schema version confirmed"
    );

    let _metrics_server = maybe_spawn_metrics_server().await?;
    let request_metrics = TorznabMetrics::register();

    let app = pg_router(Config::default().merge_defaults(), pool);
    let app = with_request_metrics(app, request_metrics);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "bitmagnet-torznab starting");
    axum::serve(listener, app)
        .with_graceful_shutdown(bitmagnet_common::serve::shutdown_signal())
        .await?;

    info!("bitmagnet-torznab stopped");
    Ok(())
}

fn parse_positive_i64(value: &str) -> Result<i64, String> {
    let parsed = value
        .parse::<i64>()
        .map_err(|error| format!("invalid positive integer {value:?}: {error}"))?;
    if parsed <= 0 {
        return Err(format!("expected a positive integer, got {parsed}"));
    }
    Ok(parsed)
}

fn with_request_metrics(app: Router, metrics: TorznabMetrics) -> Router {
    app.layer(middleware::from_fn(move |request: Request, next: Next| {
        let metrics = metrics.clone();
        async move {
            let started = Instant::now();
            let response: Response = next.run(request).await;
            metrics.requests_total.inc();
            metrics
                .request_duration_seconds
                .observe(started.elapsed().as_secs_f64());
            response
        }
    }))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use axum::Router;
    use bitmagnet_common::metrics::gather_text;
    use clap::Parser;
    use tower::ServiceExt;

    use super::{with_request_metrics, Args, TorznabMetrics};

    struct ArgsEnvRestore {
        listen_addr: Option<OsString>,
        expected_goose_version: Option<OsString>,
    }

    impl ArgsEnvRestore {
        fn clear() -> Self {
            let restore = Self {
                listen_addr: std::env::var_os("LISTEN_ADDR"),
                expected_goose_version: std::env::var_os(
                    "BITMAGNET_TORZNAB_EXPECTED_GOOSE_VERSION",
                ),
            };
            std::env::remove_var("LISTEN_ADDR");
            std::env::remove_var("BITMAGNET_TORZNAB_EXPECTED_GOOSE_VERSION");
            restore
        }
    }

    impl Drop for ArgsEnvRestore {
        fn drop(&mut self) {
            restore_env("LISTEN_ADDR", self.listen_addr.take());
            restore_env(
                "BITMAGNET_TORZNAB_EXPECTED_GOOSE_VERSION",
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
    fn args_require_positive_goose_version_and_accept_overrides() {
        let _restore = ArgsEnvRestore::clear();

        assert!(Args::try_parse_from(["bitmagnet-torznab"]).is_err());
        for invalid in ["0", "-1"] {
            assert!(Args::try_parse_from([
                "bitmagnet-torznab",
                "--expected-goose-version",
                invalid,
            ])
            .is_err());
        }

        let overridden = Args::try_parse_from([
            "bitmagnet-torznab",
            "--listen-addr",
            "127.0.0.1:43336",
            "--expected-goose-version",
            "2026071101",
        ])
        .expect("explicit Torznab arguments parse");
        assert_eq!(overridden.listen_addr, "127.0.0.1:43336");
        assert_eq!(overridden.expected_goose_version, 2_026_071_101);
    }

    #[tokio::test]
    async fn middleware_records_request_count_and_duration() {
        let metrics = TorznabMetrics::register();
        let requests_before = metric_value(&gather_text(), "torznab_requests_total");
        let durations_before =
            metric_value(&gather_text(), "torznab_request_duration_seconds_count");
        let app = with_request_metrics(Router::new().route("/", get(|| async { "ok" })), metrics);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .expect("dummy request builds"),
            )
            .await
            .expect("dummy route responds");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            metric_value(&gather_text(), "torznab_requests_total"),
            requests_before + 1.0
        );
        assert_eq!(
            metric_value(&gather_text(), "torznab_request_duration_seconds_count"),
            durations_before + 1.0
        );
    }

    fn metric_value(text: &str, name: &str) -> f64 {
        let prefix = format!("{name} ");
        text.lines()
            .find_map(|line| line.strip_prefix(prefix.as_str()))
            .expect("metric sample is present")
            .parse()
            .expect("metric value is numeric")
    }
}
