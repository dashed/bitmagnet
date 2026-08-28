//! Shared gRPC listener, health, and graceful-shutdown helpers.

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;

/// The transport and address on which a gRPC server should listen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Listen {
    /// Listen on a TCP socket.
    Tcp(SocketAddr),
    /// Listen on a Unix-domain socket path.
    Unix(PathBuf),
}

impl Listen {
    /// Parse a TCP address or Unix-domain socket path.
    ///
    /// A leading `unix:` prefix is optional for Unix-domain socket paths.
    #[must_use]
    pub fn parse(addr: &str) -> Listen {
        let candidate = addr.strip_prefix("unix:").unwrap_or(addr);
        candidate
            .parse::<SocketAddr>()
            .map_or_else(|_| Listen::Unix(PathBuf::from(candidate)), Listen::Tcp)
    }
}

/// Resolve when the process receives `SIGINT` (Ctrl-C) or, on Unix, `SIGTERM`.
pub async fn shutdown_signal() {
    wait_for_shutdown_signal().await;
    tracing::info!("shutdown signal received");
}

/// Resolve when the process receives `SIGINT` or, on Unix, `SIGTERM`, without
/// emitting the standard shutdown log line.
///
/// This is for loops that detect the signal in one task but deliberately log
/// only after reaching their own safe shutdown boundary.
pub async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install ctrl-c handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => tracing::error!(%error, "failed to install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    wait_for_shutdown(ctrl_c, terminate).await;
}

async fn wait_for_shutdown<C, T>(ctrl_c: C, terminate: T)
where
    C: Future<Output = ()>,
    T: Future<Output = ()>,
{
    tokio::pin!(ctrl_c);
    tokio::pin!(terminate);
    tokio::select! {
        () = &mut ctrl_c => {}
        () = &mut terminate => {}
    }
}

/// Build a `grpc.health.v1` reporter and server.
///
/// The reporter starts every service at `NotServing`; flip each service to
/// `Serving` once it is ready.
#[must_use]
pub fn health() -> (
    tonic_health::server::HealthReporter,
    tonic_health::pb::health_server::HealthServer<tonic_health::server::HealthService>,
) {
    let reporter = tonic_health::server::HealthReporter::new();
    let service = tonic_health::pb::health_server::HealthServer::new(
        tonic_health::server::HealthService::from_health_reporter(reporter.clone()),
    );
    (reporter, service)
}

/// Serve an explicitly composed tonic router over TCP or a Unix-domain socket.
///
/// The server stops gracefully when `shutdown` resolves. Unix-domain socket
/// paths are removed before binding when they already exist.
pub async fn serve_router(
    router: tonic::transport::server::Router,
    listen: Listen,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> crate::Result<()> {
    match listen {
        Listen::Tcp(addr) => {
            tracing::info!(%addr, "serving gRPC over TCP");
            router
                .serve_with_shutdown(addr, shutdown)
                .await
                .map_err(|error| crate::Error::Other(error.to_string()))
        }
        #[cfg(unix)]
        Listen::Unix(path) => {
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
            let listener = tokio::net::UnixListener::bind(&path)?;
            tracing::info!(path = %path.display(), "serving gRPC over unix socket");
            router
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::UnixListenerStream::new(listener),
                    shutdown,
                )
                .await
                .map_err(|error| crate::Error::Other(error.to_string()))
        }
        #[cfg(not(unix))]
        Listen::Unix(path) => Err(crate::Error::Other(format!(
            "unix-socket listening ({}) is only supported on unix platforms",
            path.display()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{wait_for_shutdown, Listen};
    use std::net::SocketAddr;
    use std::path::PathBuf;

    #[test]
    fn listen_parse_distinguishes_tcp_and_unix_addresses() {
        assert_eq!(
            Listen::parse("127.0.0.1:50051"),
            Listen::Tcp(
                "127.0.0.1:50051"
                    .parse::<SocketAddr>()
                    .expect("valid IPv4 socket address")
            )
        );
        assert_eq!(
            Listen::parse("unix:/tmp/x.sock"),
            Listen::Unix(PathBuf::from("/tmp/x.sock"))
        );
        assert_eq!(
            Listen::parse("/tmp/y.sock"),
            Listen::Unix(PathBuf::from("/tmp/y.sock"))
        );
        assert_eq!(
            Listen::parse("[::1]:8080"),
            Listen::Tcp(
                "[::1]:8080"
                    .parse::<SocketAddr>()
                    .expect("valid IPv6 socket address")
            )
        );
    }

    #[tokio::test]
    async fn simulated_sigterm_resolves_shutdown_selector() {
        wait_for_shutdown(std::future::pending::<()>(), std::future::ready(())).await;
    }
}
