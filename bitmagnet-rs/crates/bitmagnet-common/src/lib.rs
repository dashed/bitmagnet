//! Shared utilities for the bitmagnet Rust workspace: a common [`Error`] type
//! and [`Result`] alias, runtime [`Config`], tracing, metrics, and server
//! bootstrap helpers.

pub mod config;
pub mod metrics;
pub mod serve;
pub mod strcase;

use std::net::SocketAddr;

use thiserror::Error;

/// The workspace-wide error type. Crates extend it as the port grows (e.g. a
/// `Db`/`Search` variant via `#[from]`).
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid or missing configuration.
    #[error("configuration error: {0}")]
    Config(String),

    /// An underlying I/O failure.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// A failure that does not (yet) have a dedicated variant.
    #[error("{0}")]
    Other(String),
}

/// Convenient `Result` alias defaulting the error type to [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Runtime configuration shared by the Rust services.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    /// Address the gRPC server binds to.
    pub listen_addr: SocketAddr,
    /// Tracing filter directive, e.g. `info` or `bitmagnet=debug,tower=warn`.
    pub log_level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 3030)),
            log_level: "info".to_owned(),
        }
    }
}

/// Initialise the global `tracing` subscriber from the `RUST_LOG` environment
/// variable, falling back to `info`. Call once at process startup; calling it
/// again will panic (a global subscriber can only be set once).
pub fn init_tracing() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer())
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_sane() {
        let cfg = Config::default();
        assert_eq!(cfg.log_level, "info");
        assert_eq!(cfg.listen_addr.port(), 3030);
        assert!(cfg.listen_addr.ip().is_loopback());
    }
}
