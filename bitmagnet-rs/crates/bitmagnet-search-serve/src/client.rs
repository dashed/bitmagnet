//! Tonic client for the L3 per-torrent path-bag candidate sidecar.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use bitmagnet_proto::v1::{
    HealthCheckRequest, PathCandidatesRequest, PathCandidatesResponse, PathSearchHealth,
    SuggestRequest, SuggestResponse,
};
use bitmagnet_proto::PathSearchServiceClient;
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

use crate::candidates::CandidateSource;
use crate::{Error, Result};

/// Connection settings for the L3 pathsearch sidecar, ported from Go's
/// `pathsearch.Config`.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Sidecar address: a Unix socket (`unix:///run/bitmagnet/pathsearch.sock`
    /// or a bare absolute path) or a TCP `host:port`. The production default is
    /// `bitmagnet-pathsearch.bitmagnet.svc:50053`.
    pub address: String,
    /// Per-unary-RPC timeout. Zero leaves the deadline to the caller.
    pub timeout: Duration,
}

#[derive(Debug)]
enum Target {
    Unix(PathBuf),
    Tcp(String),
}

/// Normalizes a sidecar address using Go `pathsearch.parseTarget` semantics.
///
/// Unix-prefixed targets are preserved, bare absolute paths gain a `unix://`
/// prefix, and TCP `host:port` targets are returned after trimming.
pub fn normalize_target(address: &str) -> Result<String> {
    let address = address.trim();
    if address.is_empty() {
        return Err(Error::Candidate("pathsearch: empty address".into()));
    }

    if address.starts_with("unix:") || !address.starts_with('/') {
        Ok(address.to_owned())
    } else {
        Ok(format!("unix://{address}"))
    }
}

fn classify_target(address: &str) -> Result<Target> {
    let normalized = normalize_target(address)?;

    if let Some(path) = normalized.strip_prefix("unix://") {
        Ok(Target::Unix(PathBuf::from(path)))
    } else if let Some(path) = normalized.strip_prefix("unix:") {
        Ok(Target::Unix(PathBuf::from(path)))
    } else {
        Ok(Target::Tcp(normalized))
    }
}

fn with_timeout(endpoint: Endpoint, timeout: Duration) -> Endpoint {
    if timeout.is_zero() {
        endpoint
    } else {
        endpoint.timeout(timeout)
    }
}

/// Thin, concurrency-safe wrapper over tonic's generated L3 client.
///
/// Like Go's `pathsearch.Client`, cloned generated clients share one
/// multiplexed channel. [`Self::connect`] creates that channel lazily; the
/// background health poll discovers whether the target is reachable.
pub struct Client {
    inner: PathSearchServiceClient<Channel>,
    timeout: Duration,
}

impl fmt::Debug for Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Client")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Builds a lazy TCP or Unix-socket channel, mirroring Go
    /// `pathsearch.NewClient`.
    ///
    /// This does not require a running server; the first RPC initiates the
    /// connection attempt.
    pub fn connect(config: ClientConfig) -> Result<Self> {
        let target = classify_target(&config.address)?;
        let timeout = config.timeout;

        let channel = match target {
            Target::Tcp(address) => {
                let uri = format!("http://{address}");
                let endpoint = Endpoint::from_shared(uri).map_err(|error| {
                    Error::Candidate(format!("pathsearch: dial {address:?}: {error}"))
                })?;
                with_timeout(endpoint, timeout).connect_lazy()
            }
            Target::Unix(path) => {
                // The URI supplies the HTTP/2 authority; the connector ignores
                // it and opens the configured filesystem socket instead.
                let endpoint = with_timeout(Endpoint::from_static("http://[::]:50053"), timeout);
                let connector = service_fn(move |_| {
                    let path = path.clone();
                    async move { UnixStream::connect(path).await.map(TokioIo::new) }
                });
                endpoint.connect_with_connector_lazy(connector)
            }
        };

        Ok(Self {
            inner: PathSearchServiceClient::new(channel),
            timeout,
        })
    }
}

fn rpc_error(rpc: &str, status: tonic::Status) -> Error {
    Error::Candidate(format!("pathsearch: {rpc} rpc: {status}"))
}

#[async_trait::async_trait]
impl CandidateSource for Client {
    async fn path_candidates(
        &self,
        request: PathCandidatesRequest,
    ) -> Result<PathCandidatesResponse> {
        let mut client = self.inner.clone();
        client
            .path_candidates(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| rpc_error("PathCandidates", status))
    }

    async fn suggest(&self, request: SuggestRequest) -> Result<SuggestResponse> {
        let mut client = self.inner.clone();
        client
            .suggest(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| rpc_error("Suggest", status))
    }

    async fn health_check(&self) -> Result<PathSearchHealth> {
        let mut client = self.inner.clone();
        client
            .health_check(HealthCheckRequest {})
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| rpc_error("HealthCheck", status))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_target_matches_go_parse_target() {
        let cases = [
            (
                "bitmagnet-pathsearch.bitmagnet.svc:50053",
                Some("bitmagnet-pathsearch.bitmagnet.svc:50053"),
            ),
            ("127.0.0.1:50053", Some("127.0.0.1:50053")),
            (
                "unix:/run/bitmagnet/pathsearch.sock",
                Some("unix:/run/bitmagnet/pathsearch.sock"),
            ),
            (
                "unix:///run/bitmagnet/pathsearch.sock",
                Some("unix:///run/bitmagnet/pathsearch.sock"),
            ),
            (
                "/run/bitmagnet/pathsearch.sock",
                Some("unix:///run/bitmagnet/pathsearch.sock"),
            ),
            ("  127.0.0.1:50053  ", Some("127.0.0.1:50053")),
            ("", None),
            ("   ", None),
        ];

        for (input, expected) in cases {
            match expected {
                Some(expected) => assert_eq!(
                    normalize_target(input).expect("target should normalize"),
                    expected
                ),
                None => assert!(normalize_target(input).is_err()),
            }
        }
    }

    #[tokio::test]
    async fn connect_is_lazy_for_unreachable_tcp_target() {
        let client = Client::connect(ClientConfig {
            address: "127.0.0.1:1".to_owned(),
            timeout: Duration::from_secs(1),
        })
        .expect("lazy channel construction should not dial");

        tokio::task::yield_now().await;
        drop(client);
    }
}
