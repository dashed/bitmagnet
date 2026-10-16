//! Process-wide Prometheus metrics registration and optional HTTP exposition.

use std::env;
use std::fmt::Display;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::OnceLock;

use prometheus::Encoder as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

const METRICS_ADDR_ENV: &str = "BITMAGNET_METRICS_ADDR";
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4";
const REQUEST_BUFFER_SIZE: usize = 8 * 1024;

static REGISTRY: OnceLock<prometheus::Registry> = OnceLock::new();

struct ComputedGauge<F> {
    gauge: prometheus::Gauge,
    value: F,
}

impl<F> prometheus::core::Collector for ComputedGauge<F>
where
    F: Fn() -> f64 + Send + Sync,
{
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        prometheus::core::Collector::desc(&self.gauge)
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.gauge.set((self.value)());
        prometheus::core::Collector::collect(&self.gauge)
    }
}

/// Return the process-wide Prometheus registry.
///
/// On Linux, the registry includes the current process collector.
#[must_use]
pub fn registry() -> &'static prometheus::Registry {
    REGISTRY.get_or_init(|| {
        let registry = prometheus::Registry::new();

        #[cfg(target_os = "linux")]
        match registry.register(Box::new(
            prometheus::process_collector::ProcessCollector::for_self(),
        )) {
            Ok(()) | Err(prometheus::Error::AlreadyReg) => {}
            Err(error) => panic!("failed to register Prometheus process collector: {error}"),
        }

        registry
    })
}

/// Create and register an integer gauge in the process-wide registry.
///
/// # Panics
///
/// Panics when the metric is invalid or conflicts with a registered collector.
#[must_use]
pub fn register_int_gauge(name: &str, help: &str) -> prometheus::IntGauge {
    let metric = prometheus::IntGauge::new(name, help).unwrap_or_else(|error| {
        panic!("failed to construct Prometheus integer gauge {name}: {error}")
    });
    registry()
        .register(Box::new(metric.clone()))
        .unwrap_or_else(|error| {
            panic!("failed to register Prometheus integer gauge {name}: {error}")
        });
    metric
}

/// Create and register a floating-point gauge in the process-wide registry.
///
/// # Panics
///
/// Panics when the metric is invalid or conflicts with a registered collector.
#[must_use]
pub fn register_gauge(name: &str, help: &str) -> prometheus::Gauge {
    let metric = prometheus::Gauge::new(name, help).unwrap_or_else(|error| {
        panic!("failed to construct Prometheus floating-point gauge {name}: {error}")
    });
    registry()
        .register(Box::new(metric.clone()))
        .unwrap_or_else(|error| {
            panic!("failed to register Prometheus floating-point gauge {name}: {error}")
        });
    metric
}

/// Register a floating-point gauge whose value is computed whenever metrics
/// are gathered.
///
/// This keeps gauges derived from shared runtime state current without a
/// background polling task. The callback runs synchronously during a scrape
/// and should therefore be fast and non-blocking.
///
/// # Panics
///
/// Panics when the metric is invalid or conflicts with a registered collector.
pub fn register_computed_gauge<F>(name: &str, help: &str, value: F)
where
    F: Fn() -> f64 + Send + Sync + 'static,
{
    let gauge = prometheus::Gauge::new(name, help).unwrap_or_else(|error| {
        panic!("failed to construct Prometheus computed gauge {name}: {error}")
    });
    registry()
        .register(Box::new(ComputedGauge { gauge, value }))
        .unwrap_or_else(|error| {
            panic!("failed to register Prometheus computed gauge {name}: {error}")
        });
}

/// Create and register an integer counter in the process-wide registry.
///
/// # Panics
///
/// Panics when the metric is invalid or conflicts with a registered collector.
#[must_use]
pub fn register_int_counter(name: &str, help: &str) -> prometheus::IntCounter {
    let metric = prometheus::IntCounter::new(name, help).unwrap_or_else(|error| {
        panic!("failed to construct Prometheus integer counter {name}: {error}")
    });
    registry()
        .register(Box::new(metric.clone()))
        .unwrap_or_else(|error| {
            panic!("failed to register Prometheus integer counter {name}: {error}")
        });
    metric
}

/// Create and register a histogram with explicit buckets in the process-wide registry.
///
/// # Panics
///
/// Panics when the metric is invalid or conflicts with a registered collector.
#[must_use]
pub fn register_histogram(name: &str, help: &str, buckets: Vec<f64>) -> prometheus::Histogram {
    let opts = prometheus::HistogramOpts::new(name, help).buckets(buckets);
    let metric = prometheus::Histogram::with_opts(opts)
        .unwrap_or_else(|error| panic!("failed to construct Prometheus histogram {name}: {error}"));
    registry()
        .register(Box::new(metric.clone()))
        .unwrap_or_else(|error| panic!("failed to register Prometheus histogram {name}: {error}"));
    metric
}

/// Gather the process-wide registry in Prometheus text exposition format.
#[must_use]
pub fn gather_text() -> String {
    encode_metric_families(registry().gather())
}

fn gather_text_with(mut extra: Vec<prometheus::proto::MetricFamily>) -> String {
    let mut metric_families = registry().gather();
    metric_families.append(&mut extra);
    metric_families.sort_by(|left, right| left.name().cmp(right.name()));
    encode_metric_families(metric_families)
}

fn encode_metric_families(metric_families: Vec<prometheus::proto::MetricFamily>) -> String {
    let mut buffer = Vec::new();
    prometheus::TextEncoder::new()
        .encode(&metric_families, &mut buffer)
        .expect("encoding Prometheus metrics into memory must succeed");
    String::from_utf8(buffer).expect("Prometheus text exposition must be valid UTF-8")
}

/// If `BITMAGNET_METRICS_ADDR` is a non-empty `HOST:PORT`, spawn a background
/// task serving `GET /metrics` and return its join handle and bound address.
///
/// An unset or empty variable returns `Ok(None)` without binding a socket.
pub async fn maybe_spawn_metrics_server(
) -> crate::Result<Option<(tokio::task::JoinHandle<()>, SocketAddr)>> {
    maybe_spawn_metrics_server_with_async_gatherer(|| async {
        Ok::<_, std::convert::Infallible>(Vec::new())
    })
    .await
}

/// Start the optional metrics server with metric families freshly awaited for
/// every successful scrape.
///
/// Gatherer errors omit only the async families and still return the normal
/// process registry with HTTP 200, matching Prometheus custom-collector
/// failure semantics without caching stale data.
pub async fn maybe_spawn_metrics_server_with_async_gatherer<G, Fut, E>(
    gatherer: G,
) -> crate::Result<Option<(tokio::task::JoinHandle<()>, SocketAddr)>>
where
    G: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<prometheus::proto::MetricFamily>, E>> + Send,
    E: Display,
{
    let configured_addr = match env::var(METRICS_ADDR_ENV) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(crate::Error::Config(format!(
                "{METRICS_ADDR_ENV} must contain valid Unicode"
            )));
        }
    };
    let configured_addr = configured_addr.trim();
    if configured_addr.is_empty() {
        return Ok(None);
    }

    let requested_addr = configured_addr.parse::<SocketAddr>().map_err(|error| {
        crate::Error::Config(format!(
            "invalid {METRICS_ADDR_ENV} value {configured_addr:?}: {error}"
        ))
    })?;
    let listener = tokio::net::TcpListener::bind(requested_addr).await?;
    let bound_addr = listener.local_addr()?;

    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _peer_addr)) => {
                    if let Err(error) = serve_metrics_connection(stream, &gatherer).await {
                        tracing::warn!(%error, "failed to serve metrics connection");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "failed to accept metrics connection");
                }
            }
        }
    });

    Ok(Some((handle, bound_addr)))
}

async fn serve_metrics_connection<G, Fut, E>(
    mut stream: tokio::net::TcpStream,
    gatherer: &G,
) -> std::io::Result<()>
where
    G: Fn() -> Fut,
    Fut: Future<Output = Result<Vec<prometheus::proto::MetricFamily>, E>>,
    E: Display,
{
    let mut request = [0_u8; REQUEST_BUFFER_SIZE];
    let bytes_read = stream.read(&mut request).await?;
    let target = request_target(&request[..bytes_read]);

    let (status_line, body) = if matches!(target, Some("/") | Some("/metrics")) {
        let extra = match gatherer().await {
            Ok(families) => families,
            Err(error) => {
                tracing::warn!(%error, "failed to gather asynchronous metrics");
                Vec::new()
            }
        };
        ("HTTP/1.1 200 OK", gather_text_with(extra).into_bytes())
    } else {
        ("HTTP/1.1 404 Not Found", Vec::new())
    };
    let response = build_http_response(status_line, PROMETHEUS_CONTENT_TYPE, &body);

    stream.write_all(&response).await?;
    stream.shutdown().await
}

fn request_target(request: &[u8]) -> Option<&str> {
    std::str::from_utf8(request)
        .ok()?
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)
}

fn build_http_response(status_line: &str, content_type: &str, body: &[u8]) -> Vec<u8> {
    let headers = format!(
        "{status_line}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut response = Vec::with_capacity(headers.len() + body.len());
    response.extend_from_slice(headers.as_bytes());
    response.extend_from_slice(body);
    response
}

#[cfg(test)]
mod tests {
    use super::{
        build_http_response, gather_text, maybe_spawn_metrics_server,
        maybe_spawn_metrics_server_with_async_gatherer, register_computed_gauge,
        register_int_gauge, METRICS_ADDR_ENV, PROMETHEUS_CONTENT_TYPE,
    };
    use prometheus::core::Collector as _;
    use std::ffi::OsString;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct MetricsAddrRestore(Option<OsString>);

    impl MetricsAddrRestore {
        fn clear() -> Self {
            let original = std::env::var_os(METRICS_ADDR_ENV);
            std::env::remove_var(METRICS_ADDR_ENV);
            Self(original)
        }

        fn set(value: &str) -> Self {
            let original = std::env::var_os(METRICS_ADDR_ENV);
            std::env::set_var(METRICS_ADDR_ENV, value);
            Self(original)
        }
    }

    impl Drop for MetricsAddrRestore {
        fn drop(&mut self) {
            match self.0.take() {
                Some(value) => std::env::set_var(METRICS_ADDR_ENV, value),
                None => std::env::remove_var(METRICS_ADDR_ENV),
            }
        }
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test Tokio runtime builds")
    }

    async fn request(addr: std::net::SocketAddr, target: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("metrics listener accepts TCP connections");
        stream
            .write_all(
                format!("GET {target} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes(),
            )
            .await
            .expect("metrics request writes");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("metrics response reads to connection close");
        String::from_utf8(response).expect("HTTP response is valid UTF-8")
    }

    #[test]
    fn metrics_server_is_disabled_by_default() {
        let _env_lock = ENV_LOCK.lock().expect("metrics env lock is not poisoned");
        let _restore = MetricsAddrRestore::clear();

        let server = test_runtime()
            .block_on(maybe_spawn_metrics_server())
            .expect("unset metrics address is valid");

        assert!(
            server.is_none(),
            "an unset address must not create a listener"
        );
    }

    #[test]
    fn metrics_server_exposes_registered_metrics() {
        const METRIC_NAME: &str = "bitmagnet_common_metrics_exposure_test_gauge";

        let _env_lock = ENV_LOCK.lock().expect("metrics env lock is not poisoned");
        let _restore = MetricsAddrRestore::set("127.0.0.1:0");
        let gauge = register_int_gauge(METRIC_NAME, "Metrics exposure test gauge.");
        gauge.set(37);

        test_runtime().block_on(async {
            let (handle, addr) = maybe_spawn_metrics_server()
                .await
                .expect("ephemeral metrics listener binds")
                .expect("configured metrics listener is enabled");
            let mut stream = tokio::net::TcpStream::connect(addr)
                .await
                .expect("metrics listener accepts TCP connections");
            stream
                .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                .await
                .expect("metrics request writes");

            let mut response = Vec::new();
            stream
                .read_to_end(&mut response)
                .await
                .expect("metrics response reads to connection close");
            let response = String::from_utf8(response).expect("HTTP response is valid UTF-8");

            assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
            assert!(response.contains(&format!("\r\nContent-Type: {PROMETHEUS_CONTENT_TYPE}\r\n")));
            let (_, body) = response
                .split_once("\r\n\r\n")
                .expect("HTTP response separates headers and body");
            let expected_sample = format!("{METRIC_NAME} 37");
            assert!(body.lines().any(|line| line == expected_sample.as_str()));

            handle.abort();
            let error = handle.await.expect_err("aborted metrics server stops");
            assert!(error.is_cancelled());
        });
    }

    #[test]
    fn async_metrics_are_fresh_per_scrape_and_skipped_for_404() {
        const METRIC_NAME: &str = "bitmagnet_common_metrics_async_test_gauge";

        let _env_lock = ENV_LOCK.lock().expect("metrics env lock is not poisoned");
        let _restore = MetricsAddrRestore::set("127.0.0.1:0");
        let calls = Arc::new(AtomicI64::new(0));

        test_runtime().block_on(async {
            let gather_calls = Arc::clone(&calls);
            let (handle, addr) = maybe_spawn_metrics_server_with_async_gatherer(move || {
                let gather_calls = Arc::clone(&gather_calls);
                async move {
                    let value = gather_calls.fetch_add(1, Ordering::SeqCst) + 1;
                    let gauge = prometheus::Gauge::new(METRIC_NAME, "Async test gauge.")
                        .expect("valid async test gauge");
                    gauge.set(value as f64);
                    Ok::<_, &'static str>(gauge.collect())
                }
            })
            .await
            .expect("ephemeral async metrics listener binds")
            .expect("configured async metrics listener is enabled");

            let not_found = request(addr, "/nope").await;
            assert!(not_found.starts_with("HTTP/1.1 404 Not Found\r\n"));
            assert_eq!(calls.load(Ordering::SeqCst), 0);

            let first = request(addr, "/metrics").await;
            assert!(first.lines().any(|line| line == format!("{METRIC_NAME} 1")));
            let second = request(addr, "/metrics").await;
            assert!(second
                .lines()
                .any(|line| line == format!("{METRIC_NAME} 2")));
            assert_eq!(calls.load(Ordering::SeqCst), 2);

            handle.abort();
            assert!(handle.await.unwrap_err().is_cancelled());
        });
    }

    #[test]
    fn async_metrics_failure_omits_only_async_families() {
        const BASE_NAME: &str = "bitmagnet_common_metrics_async_failure_base";
        const OMITTED_NAME: &str = "bitmagnet_common_metrics_async_failure_omitted";

        let _env_lock = ENV_LOCK.lock().expect("metrics env lock is not poisoned");
        let _restore = MetricsAddrRestore::set("127.0.0.1:0");
        register_int_gauge(BASE_NAME, "Async failure base gauge.").set(5);

        test_runtime().block_on(async {
            let (handle, addr) = maybe_spawn_metrics_server_with_async_gatherer(|| async {
                Err::<Vec<prometheus::proto::MetricFamily>, _>("database unavailable")
            })
            .await
            .expect("ephemeral failing metrics listener binds")
            .expect("configured failing metrics listener is enabled");

            let response = request(addr, "/metrics").await;
            assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
            assert!(response
                .lines()
                .any(|line| line == format!("{BASE_NAME} 5")));
            assert!(!response.contains(OMITTED_NAME));

            handle.abort();
            assert!(handle.await.unwrap_err().is_cancelled());
        });
    }

    #[test]
    fn gather_text_contains_registered_metric() {
        const METRIC_NAME: &str = "bitmagnet_common_metrics_gather_test_gauge";

        let gauge = register_int_gauge(METRIC_NAME, "Metrics gathering test gauge.");
        gauge.set(11);

        let text = gather_text();
        assert!(!text.is_empty());
        assert!(text.contains(METRIC_NAME));
    }

    #[test]
    fn computed_gauge_reads_shared_state_at_gather_time() {
        const METRIC_NAME: &str = "bitmagnet_common_metrics_computed_test_gauge";

        let value = Arc::new(AtomicI64::new(7));
        let shared_value = value.clone();
        register_computed_gauge(METRIC_NAME, "Computed metrics test gauge.", move || {
            shared_value.load(Ordering::Relaxed) as f64
        });

        let first = gather_text();
        assert!(first.lines().any(|line| line == format!("{METRIC_NAME} 7")));

        value.store(19, Ordering::Relaxed);
        let second = gather_text();
        assert!(second
            .lines()
            .any(|line| line == format!("{METRIC_NAME} 19")));
    }

    #[test]
    fn response_builder_sets_matching_content_length() {
        let response = build_http_response("HTTP/1.1 200 OK", PROMETHEUS_CONTENT_TYPE, b"hello");
        let response = String::from_utf8(response).expect("sample response is valid UTF-8");

        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("\r\nContent-Length: 5\r\n"));
        assert!(response.ends_with("\r\n\r\nhello"));
    }
}
