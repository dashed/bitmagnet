use std::fmt;
use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::str::FromStr;
use std::time::Duration;

use bitmagnet_dht_crawler::{
    DhtCrawlerAppConfig, DhtCrawlerAppConfigError, DhtCrawlerAppProjection,
};
use clap::Parser;

/// Default budget for either application startup or graceful shutdown.
pub const DHT_CRAWLER_WRITER_DEFAULT_PROCESS_TIMEOUT_SECONDS: u16 = 20;
/// Smallest lifecycle budget that leaves a forced-cleanup and pool-close tail.
pub const DHT_CRAWLER_WRITER_MIN_PROCESS_TIMEOUT_SECONDS: u16 = 10;
/// Largest accepted application startup or graceful-shutdown budget.
pub const DHT_CRAWLER_WRITER_MAX_PROCESS_TIMEOUT_SECONDS: u16 = 300;

/// Validated positive, operationally bounded application lifecycle budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DhtCrawlerWriterProcessTimeout(NonZeroU16);

impl DhtCrawlerWriterProcessTimeout {
    pub const DEFAULT: Self = Self(
        NonZeroU16::new(DHT_CRAWLER_WRITER_DEFAULT_PROCESS_TIMEOUT_SECONDS)
            .expect("the default writer-process timeout is nonzero"),
    );

    /// Validate a timeout expressed in whole seconds.
    pub const fn from_seconds(seconds: u64) -> Result<Self, DhtCrawlerWriterProcessTimeoutError> {
        if seconds < DHT_CRAWLER_WRITER_MIN_PROCESS_TIMEOUT_SECONDS as u64 {
            return Err(DhtCrawlerWriterProcessTimeoutError::TooSmall {
                seconds,
                minimum_seconds: DHT_CRAWLER_WRITER_MIN_PROCESS_TIMEOUT_SECONDS,
            });
        }
        if seconds > DHT_CRAWLER_WRITER_MAX_PROCESS_TIMEOUT_SECONDS as u64 {
            return Err(DhtCrawlerWriterProcessTimeoutError::TooLarge {
                seconds,
                maximum_seconds: DHT_CRAWLER_WRITER_MAX_PROCESS_TIMEOUT_SECONDS,
            });
        }
        Ok(Self(
            NonZeroU16::new(seconds as u16).expect("validated seconds are nonzero"),
        ))
    }

    #[must_use]
    pub const fn seconds(self) -> u16 {
        self.0.get()
    }

    #[must_use]
    pub const fn duration(self) -> Duration {
        Duration::from_secs(self.seconds() as u64)
    }
}

impl Default for DhtCrawlerWriterProcessTimeout {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl fmt::Display for DhtCrawlerWriterProcessTimeout {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.seconds().fmt(formatter)
    }
}

impl FromStr for DhtCrawlerWriterProcessTimeout {
    type Err = DhtCrawlerWriterProcessTimeoutError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let seconds = value
            .parse::<u64>()
            .map_err(|_| DhtCrawlerWriterProcessTimeoutError::NotUnsignedInteger)?;
        Self::from_seconds(seconds)
    }
}

/// Invalid application lifecycle budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DhtCrawlerWriterProcessTimeoutError {
    #[error("writer-process timeout must be an unsigned integer number of seconds")]
    NotUnsignedInteger,
    #[error("writer-process timeout {seconds}s is below minimum {minimum_seconds}s")]
    TooSmall { seconds: u64, minimum_seconds: u16 },
    #[error("writer-process timeout {seconds}s exceeds maximum {maximum_seconds}s")]
    TooLarge { seconds: u64, maximum_seconds: u16 },
}

/// Pure process configuration for the writer-capable DHT crawler.
///
/// Database connection material is intentionally absent. It must be loaded
/// through `bitmagnet_db::DbConfig::from_compatible_env` only after this pure
/// configuration has parsed and projected successfully.
#[derive(Clone, Debug, Parser)]
#[command(
    name = "bitmagnet-dht-crawler",
    about = "Writer-capable Rust DHT crawler",
    disable_help_subcommand = true,
    after_long_help = "PostgreSQL connection material is accepted only through the compatible \
BITMAGNET_POSTGRES_* or POSTGRES_* environment boundary. The optional common metrics listener is \
configured separately through BITMAGNET_METRICS_ADDR."
)]
pub struct DhtCrawlerWriterAppConfig {
    #[command(flatten)]
    crawler: DhtCrawlerAppConfig,

    /// HTTP liveness, readiness, and status listener.
    #[arg(
        long = "http-server-local-address",
        env = "HTTP_SERVER_LOCAL_ADDRESS",
        default_value = "0.0.0.0:3333",
        value_parser = parse_go_http_listen_addr
    )]
    http_listen_addr: SocketAddr,

    /// Whole-application startup budget.
    #[arg(
        long = "startup-timeout-seconds",
        env = "BITMAGNET_DHT_CRAWLER_STARTUP_TIMEOUT_SECONDS",
        default_value_t = DhtCrawlerWriterProcessTimeout::DEFAULT
    )]
    startup_timeout: DhtCrawlerWriterProcessTimeout,

    /// Whole-process graceful-drain budget after the first terminal event.
    #[arg(
        long = "graceful-shutdown-timeout-seconds",
        env = "BITMAGNET_DHT_CRAWLER_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS",
        default_value_t = DhtCrawlerWriterProcessTimeout::DEFAULT
    )]
    shutdown_timeout: DhtCrawlerWriterProcessTimeout,
}

impl DhtCrawlerWriterAppConfig {
    /// Validate and project the complete process policy without I/O.
    pub fn projection(
        &self,
    ) -> Result<DhtCrawlerWriterAppProjection, DhtCrawlerWriterAppConfigError> {
        Ok(DhtCrawlerWriterAppProjection {
            crawler: self
                .crawler
                .projection()
                .map_err(DhtCrawlerWriterAppConfigError::Crawler)?,
            http_listen_addr: self.http_listen_addr,
            startup_timeout: self.startup_timeout,
            shutdown_timeout: self.shutdown_timeout,
        })
    }
}

/// Fully validated, secret-free writer application policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtCrawlerWriterAppProjection {
    pub crawler: DhtCrawlerAppProjection,
    pub http_listen_addr: SocketAddr,
    pub startup_timeout: DhtCrawlerWriterProcessTimeout,
    pub shutdown_timeout: DhtCrawlerWriterProcessTimeout,
}

/// Writer application policy could not be projected safely.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DhtCrawlerWriterAppConfigError {
    #[error("invalid writer crawler policy: {0}")]
    Crawler(#[source] DhtCrawlerAppConfigError),
}

fn parse_go_http_listen_addr(value: &str) -> Result<SocketAddr, String> {
    let normalized = value
        .strip_prefix(':')
        .map_or_else(|| value.to_owned(), |port| format!("0.0.0.0:{port}"));
    normalized
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid HTTP listener address {value:?}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use clap::{CommandFactory, FromArgMatches};

    use super::*;

    fn try_parse_without_ambient_env(
        args: impl IntoIterator<Item = &'static str>,
    ) -> Result<DhtCrawlerWriterAppConfig, clap::Error> {
        let command =
            DhtCrawlerWriterAppConfig::command().mut_args(|argument| argument.env(None::<&str>));
        let matches = command.try_get_matches_from(args)?;
        DhtCrawlerWriterAppConfig::from_arg_matches(&matches)
    }

    fn required_args() -> [&'static str; 5] {
        [
            "bitmagnet-dht-crawler",
            "--expected-goose-version",
            "33",
            "--classifier-queue",
            "shadow",
        ]
    }

    #[test]
    fn required_writer_policy_projects_without_io() {
        let config = try_parse_without_ambient_env(required_args()).unwrap();
        let projection = config.projection().unwrap();
        assert_eq!(projection.crawler.expected_goose_version, 33);
        assert_eq!(projection.http_listen_addr, "0.0.0.0:3333".parse().unwrap());
        assert_eq!(projection.startup_timeout.seconds(), 20);
        assert_eq!(projection.shutdown_timeout.seconds(), 20);
    }

    #[test]
    fn bare_go_http_port_and_distinct_bounded_budgets_parse() {
        let mut args = required_args().to_vec();
        args.extend([
            "--http-server-local-address",
            ":4321",
            "--startup-timeout-seconds",
            "10",
            "--graceful-shutdown-timeout-seconds",
            "11",
        ]);
        let projection = try_parse_without_ambient_env(args)
            .unwrap()
            .projection()
            .unwrap();
        assert_eq!(projection.http_listen_addr, "0.0.0.0:4321".parse().unwrap());
        assert_eq!(projection.startup_timeout.seconds(), 10);
        assert_eq!(projection.shutdown_timeout.seconds(), 11);
    }

    #[test]
    fn mandatory_writer_mode_and_goose_pin_fail_closed() {
        assert!(try_parse_without_ambient_env(["bitmagnet-dht-crawler"]).is_err());
        assert!(try_parse_without_ambient_env([
            "bitmagnet-dht-crawler",
            "--expected-goose-version",
            "33",
        ])
        .is_err());
        assert!(try_parse_without_ambient_env([
            "bitmagnet-dht-crawler",
            "--classifier-queue",
            "shadow",
        ])
        .is_err());
    }

    #[test]
    fn lifecycle_budgets_are_positive_and_bounded() {
        for flag in [
            "--startup-timeout-seconds",
            "--graceful-shutdown-timeout-seconds",
        ] {
            for invalid in ["0", "9", "301", "-1", "not-a-number"] {
                let mut args = required_args().to_vec();
                args.extend([flag, invalid]);
                assert!(DhtCrawlerWriterAppConfig::command()
                    .mut_args(|argument| argument.env(None::<&str>))
                    .try_get_matches_from(args)
                    .is_err());
            }
        }
    }

    #[test]
    fn clap_and_debug_surfaces_contain_no_database_material() {
        let command = DhtCrawlerWriterAppConfig::command();
        let environment = command
            .get_arguments()
            .filter_map(|argument| argument.get_env())
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<BTreeSet<_>>();
        assert!(!environment
            .iter()
            .any(|value| value.contains("POSTGRES") || value.contains("PASSWORD")));

        let mut help = Vec::new();
        DhtCrawlerWriterAppConfig::command()
            .write_long_help(&mut help)
            .unwrap();
        let help = String::from_utf8(help).unwrap();
        for secret_surface in ["--postgres", "--database-url", "DSN=", "PASSWORD="] {
            assert!(
                !help.contains(secret_surface),
                "unexpected {secret_surface} in help"
            );
        }

        let debug = format!(
            "{:?}",
            try_parse_without_ambient_env(required_args()).unwrap()
        );
        assert!(!debug.contains("postgres://"));
        assert!(!debug.contains("password"));
    }
}
