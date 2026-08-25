use std::fmt::Debug;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;

use bitmagnet_dht::{
    DhtBootstrapPingProducerConfig, DhtCrawlerMaintenanceConfig, DhtRuntimeConfig,
};
use clap::Parser;

use crate::{
    DhtCrawlerDownstreamConfig, DhtInfoHashTriageConfig, DhtPeerWireMetaInfoRequesterConfig,
    DhtPersistTorrentWorkerConfig, DhtTorrentPlanConfig,
};

/// Go's ordered `dht_crawler.bootstrap_nodes` default.
///
/// Order is application behavior: the bootstrap producer traverses this list in
/// order. Keep this synchronized with `internal/dhtcrawler/config.go`.
pub const DEFAULT_BOOTSTRAP_NODES: [&str; 6] = [
    "router.utorrent.com:6881",
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.aelitis.com:6881",
    "router.silotis.us:6881",
    "dht.libtorrent.org:25401",
];

const DEFAULT_BOOTSTRAP_NODES_CSV: &str = "router.utorrent.com:6881,router.bittorrent.com:6881,dht.transmissionbt.com:6881,dht.aelitis.com:6881,router.silotis.us:6881,dht.libtorrent.org:25401";
const DEFAULT_SCALING_FACTOR: usize = 10;
#[cfg(test)]
const EFFECTIVE_BOOTSTRAP_RESEED_INTERVAL: Duration = Duration::from_secs(10 * 60);
const DEFAULT_SAVE_FILES_THRESHOLD: u64 = 100;
const DEFAULT_SAVE_PIECES: bool = false;
#[cfg(test)]
const DEFAULT_RESCRAPE_THRESHOLD: Duration = Duration::from_secs(30 * 24 * 60 * 60);
#[cfg(test)]
const DEFAULT_METAINFO_REQUEST_TIMEOUT: Duration = Duration::from_secs(6);
const DEFAULT_METAINFO_KEY_MUTEX_SIZE: usize = 1_000;

/// Pure application configuration for the first owned DHT crawler process.
///
/// This type only parses values; constructing it performs no database, DNS,
/// socket, or task work. Database connection material is intentionally not a
/// field: the executable must obtain its PostgreSQL secret through a separate
/// secret-only boundary and must never expose it as a CLI argument or through
/// this type's `Debug` implementation. The expected Goose version is a
/// non-secret admission pin and is therefore kept here.
///
/// Go-compatible knobs are represented even when the current Rust composition
/// still fixes their effective values. [`Self::validate`] rejects every
/// unsupported nondefault, preventing a deployment from silently accepting
/// configuration it cannot honor.
#[derive(Clone, Debug, Parser, PartialEq, Eq)]
#[command(
    name = "bitmagnet-dht-crawler",
    about = "Owned Rust DHT crawler configuration",
    disable_help_subcommand = true
)]
pub struct DhtCrawlerAppConfig {
    /// IPv4 UDP port used by the DHT server.
    #[arg(
        long = "dht-server-port",
        env = "DHT_SERVER_PORT",
        default_value_t = 3334
    )]
    dht_server_port: u16,

    /// DHT query timeout in Go duration syntax (for example `1500ms` or `4s`).
    #[arg(
        long = "dht-server-query-timeout",
        env = "DHT_SERVER_QUERY_TIMEOUT",
        default_value = "4s",
        value_parser = parse_go_duration
    )]
    dht_server_query_timeout: Duration,

    /// Required Goose migration version the database must already have applied.
    #[arg(
        long = "expected-goose-version",
        env = "BITMAGNET_DHT_CRAWLER_EXPECTED_GOOSE_VERSION",
        value_parser = parse_positive_i64
    )]
    expected_goose_version: i64,

    /// Go-compatible crawler scaling factor.
    #[arg(
        long = "dht-crawler-scaling-factor",
        env = "DHT_CRAWLER_SCALING_FACTOR",
        default_value_t = DEFAULT_SCALING_FACTOR
    )]
    scaling_factor: usize,

    /// Ordered, comma-delimited Go bootstrap node list.
    #[arg(
        long = "dht-crawler-bootstrap-nodes",
        env = "DHT_CRAWLER_BOOTSTRAP_NODES",
        value_delimiter = ',',
        default_value = DEFAULT_BOOTSTRAP_NODES_CSV
    )]
    bootstrap_nodes: Vec<String>,

    /// Effective bootstrap reseed interval in Go duration syntax.
    ///
    /// Go's config default is one minute, but its production factory currently
    /// ignores that field and uses ten minutes. Rust defaults to that effective
    /// behavior while honoring configured positive intervals.
    #[arg(
        long = "dht-crawler-reseed-bootstrap-nodes-interval",
        env = "DHT_CRAWLER_RESEED_BOOTSTRAP_NODES_INTERVAL",
        default_value = "10m",
        value_parser = parse_go_duration
    )]
    reseed_bootstrap_nodes_interval: Duration,

    /// Largest torrent file count retained by the current composition.
    #[arg(
        long = "dht-crawler-save-files-threshold",
        env = "DHT_CRAWLER_SAVE_FILES_THRESHOLD",
        default_value_t = DEFAULT_SAVE_FILES_THRESHOLD
    )]
    save_files_threshold: u64,

    /// Whether torrent piece hashes are persisted by the current composition.
    #[arg(
        long = "dht-crawler-save-pieces",
        env = "DHT_CRAWLER_SAVE_PIECES",
        default_value_t = DEFAULT_SAVE_PIECES,
        action = clap::ArgAction::Set
    )]
    save_pieces: bool,

    /// Minimum age before rescraping a known torrent, in Go duration syntax.
    #[arg(
        long = "dht-crawler-rescrape-threshold",
        env = "DHT_CRAWLER_RESCRAPE_THRESHOLD",
        default_value = "720h",
        value_parser = parse_go_duration
    )]
    rescrape_threshold: Duration,

    /// Peer-wire metadata request timeout in Go duration syntax.
    #[arg(
        long = "metainfo-requester-request-timeout",
        env = "METAINFO_REQUESTER_REQUEST_TIMEOUT",
        default_value = "6s",
        value_parser = parse_go_duration
    )]
    metainfo_request_timeout: Duration,

    /// Go-compatible metainfo requester keyed-mutex capacity.
    #[arg(
        long = "metainfo-requester-key-mutex-size",
        env = "METAINFO_REQUESTER_KEY_MUTEX_SIZE",
        default_value_t = DEFAULT_METAINFO_KEY_MUTEX_SIZE
    )]
    metainfo_key_mutex_size: usize,
}

impl DhtCrawlerAppConfig {
    /// Reject values the current application composition cannot yet honor.
    pub fn validate(&self) -> Result<(), DhtCrawlerAppConfigError> {
        require_supported(
            "DHT_CRAWLER_SCALING_FACTOR",
            &self.scaling_factor,
            &DEFAULT_SCALING_FACTOR,
        )?;
        require_supported(
            "METAINFO_REQUESTER_KEY_MUTEX_SIZE",
            &self.metainfo_key_mutex_size,
            &DEFAULT_METAINFO_KEY_MUTEX_SIZE,
        )?;
        Ok(())
    }

    /// Build the side-effect-free runtime value after checking application pins.
    pub fn dht_runtime_config(&self) -> Result<DhtRuntimeConfig, DhtCrawlerAppConfigError> {
        self.validate()?;
        Ok(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, self.dht_server_port),
            query_timeout: self.dht_server_query_timeout,
            ..DhtRuntimeConfig::default()
        })
    }

    /// Project validated application policy into the taskless maintenance
    /// composition without resolving bootstrap endpoints or starting work.
    pub fn maintenance_config(
        &self,
    ) -> Result<DhtCrawlerMaintenanceConfig, DhtCrawlerAppConfigError> {
        self.validate()?;
        Ok(DhtCrawlerMaintenanceConfig {
            bootstrap_ping: DhtBootstrapPingProducerConfig {
                bootstrap_nodes: self.bootstrap_nodes.clone(),
                reseed_interval: self.reseed_bootstrap_nodes_interval,
            },
            ..DhtCrawlerMaintenanceConfig::default()
        })
    }

    /// Project validated application policy into the taskless downstream
    /// composition while retaining every unrelated worker default.
    pub fn downstream_config(
        &self,
    ) -> Result<DhtCrawlerDownstreamConfig, DhtCrawlerAppConfigError> {
        self.validate()?;
        Ok(DhtCrawlerDownstreamConfig {
            triage: DhtInfoHashTriageConfig {
                save_files_threshold: self.save_files_threshold,
                rescrape_threshold: self.rescrape_threshold,
                ..DhtInfoHashTriageConfig::default()
            },
            persist_torrent: DhtPersistTorrentWorkerConfig {
                plan_config: DhtTorrentPlanConfig {
                    save_files_threshold: self.save_files_threshold,
                    save_pieces: self.save_pieces,
                },
                ..DhtPersistTorrentWorkerConfig::default()
            },
            metainfo_requester: DhtPeerWireMetaInfoRequesterConfig {
                request_timeout: self.metainfo_request_timeout,
                ..DhtPeerWireMetaInfoRequesterConfig::default()
            },
        })
    }

    pub const fn expected_goose_version(&self) -> i64 {
        self.expected_goose_version
    }

    pub const fn dht_server_port(&self) -> u16 {
        self.dht_server_port
    }

    pub const fn dht_server_query_timeout(&self) -> Duration {
        self.dht_server_query_timeout
    }

    pub const fn scaling_factor(&self) -> usize {
        self.scaling_factor
    }

    pub fn bootstrap_nodes(&self) -> &[String] {
        &self.bootstrap_nodes
    }

    pub const fn reseed_bootstrap_nodes_interval(&self) -> Duration {
        self.reseed_bootstrap_nodes_interval
    }

    pub const fn save_files_threshold(&self) -> u64 {
        self.save_files_threshold
    }

    pub const fn save_pieces(&self) -> bool {
        self.save_pieces
    }

    pub const fn rescrape_threshold(&self) -> Duration {
        self.rescrape_threshold
    }

    pub const fn metainfo_request_timeout(&self) -> Duration {
        self.metainfo_request_timeout
    }

    pub const fn metainfo_key_mutex_size(&self) -> usize {
        self.metainfo_key_mutex_size
    }
}

impl TryFrom<&DhtCrawlerAppConfig> for DhtRuntimeConfig {
    type Error = DhtCrawlerAppConfigError;

    fn try_from(value: &DhtCrawlerAppConfig) -> Result<Self, Self::Error> {
        value.dht_runtime_config()
    }
}

impl TryFrom<&DhtCrawlerAppConfig> for DhtCrawlerMaintenanceConfig {
    type Error = DhtCrawlerAppConfigError;

    fn try_from(value: &DhtCrawlerAppConfig) -> Result<Self, Self::Error> {
        value.maintenance_config()
    }
}

impl TryFrom<&DhtCrawlerAppConfig> for DhtCrawlerDownstreamConfig {
    type Error = DhtCrawlerAppConfigError;

    fn try_from(value: &DhtCrawlerAppConfig) -> Result<Self, Self::Error> {
        value.downstream_config()
    }
}

/// An application value differs from the only behavior currently wired.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unsupported {name} value {configured}; the current Rust composition requires {supported}")]
pub struct DhtCrawlerAppConfigError {
    name: &'static str,
    configured: String,
    supported: String,
}

fn require_supported<T>(
    name: &'static str,
    configured: &T,
    supported: &T,
) -> Result<(), DhtCrawlerAppConfigError>
where
    T: Debug + PartialEq,
{
    if configured == supported {
        return Ok(());
    }
    Err(DhtCrawlerAppConfigError {
        name,
        configured: format!("{configured:?}"),
        supported: format!("{supported:?}"),
    })
}

fn parse_positive_i64(value: &str) -> Result<i64, String> {
    let value = value
        .parse::<i64>()
        .map_err(|error| format!("invalid positive integer {value:?}: {error}"))?;
    if value <= 0 {
        return Err("expected Goose version must be positive".to_owned());
    }
    Ok(value)
}

/// Parse the positive subset of Go's `time.ParseDuration` syntax.
///
/// All documented Go units (`ns`, `us`, `µs`, `μs`, `ms`, `s`, `m`, and `h`),
/// compound values, fractions, and a leading plus sign are recognized. Zero
/// and negative durations are rejected explicitly because this application has
/// no useful non-positive timeout or interval semantics.
fn parse_go_duration(value: &str) -> Result<Duration, String> {
    let mut remaining = match value.as_bytes().first() {
        Some(b'-') => return Err(format!("negative duration {value:?} is unsupported")),
        Some(b'+') => &value[1..],
        _ => value,
    };
    if remaining == "0" {
        return Err(format!("duration {value:?} must be positive"));
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
            .ok_or_else(|| format!("duration {value:?} overflows"))?;
        let fraction_nanos = fraction.bytes().rev().fold(0_u128, |acc, digit| {
            (scale * u128::from(digit - b'0') + acc) / 10
        });
        nanos = nanos
            .checked_add(
                whole_nanos
                    .checked_add(fraction_nanos)
                    .ok_or_else(|| format!("duration {value:?} overflows"))?,
            )
            .ok_or_else(|| format!("duration {value:?} overflows"))?;
        parsed_parts += 1;
    }

    if parsed_parts == 0 || nanos > i64::MAX as u128 {
        return Err(format!("duration {value:?} overflows"));
    }
    if nanos == 0 {
        return Err(format!("duration {value:?} must be positive"));
    }
    Ok(Duration::from_nanos(nanos as u64))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::time::Duration;

    use bitmagnet_dht::{
        DhtBootstrapPingProducerConfig, DhtCrawlerMaintenanceConfig, DhtRuntimeConfig,
    };
    use clap::{CommandFactory, FromArgMatches, Parser};

    use super::{
        parse_go_duration, DhtCrawlerAppConfig, DhtCrawlerDownstreamConfig,
        DhtInfoHashTriageConfig, DhtPeerWireMetaInfoRequesterConfig, DhtPersistTorrentWorkerConfig,
        DhtTorrentPlanConfig, DEFAULT_BOOTSTRAP_NODES, DEFAULT_METAINFO_KEY_MUTEX_SIZE,
        DEFAULT_METAINFO_REQUEST_TIMEOUT, DEFAULT_RESCRAPE_THRESHOLD, DEFAULT_SAVE_FILES_THRESHOLD,
        DEFAULT_SAVE_PIECES, DEFAULT_SCALING_FACTOR, EFFECTIVE_BOOTSTRAP_RESEED_INTERVAL,
    };

    fn supported_args() -> Vec<&'static str> {
        vec![
            "bitmagnet-dht-crawler",
            "--dht-server-port",
            "4444",
            "--dht-server-query-timeout",
            "1m30.5s",
            "--expected-goose-version",
            "29",
            "--dht-crawler-scaling-factor",
            "10",
            "--dht-crawler-bootstrap-nodes",
            "router.utorrent.com:6881,router.bittorrent.com:6881,dht.transmissionbt.com:6881,dht.aelitis.com:6881,router.silotis.us:6881,dht.libtorrent.org:25401",
            "--dht-crawler-reseed-bootstrap-nodes-interval",
            "10m",
            "--dht-crawler-save-files-threshold",
            "100",
            "--dht-crawler-save-pieces",
            "false",
            "--dht-crawler-rescrape-threshold",
            "720h",
            "--metainfo-requester-request-timeout",
            "6s",
            "--metainfo-requester-key-mutex-size",
            "1000",
        ]
    }

    fn parse_without_environment(args: &[&str]) -> DhtCrawlerAppConfig {
        let command = DhtCrawlerAppConfig::command()
            .mut_args(|argument| argument.env(Option::<&'static str>::None));
        let matches = command
            .try_get_matches_from(args)
            .expect("parse config without ambient environment");
        DhtCrawlerAppConfig::from_arg_matches(&matches).expect("build typed config")
    }

    #[test]
    fn clap_defaults_match_the_effective_supported_contract() {
        let config =
            parse_without_environment(&["bitmagnet-dht-crawler", "--expected-goose-version", "29"]);

        config.validate().expect("default config is supported");
        assert_eq!(config.dht_server_port(), 3334);
        assert_eq!(config.dht_server_query_timeout(), Duration::from_secs(4));
        assert_eq!(
            config.bootstrap_nodes(),
            DEFAULT_BOOTSTRAP_NODES.map(str::to_owned)
        );
        assert_eq!(
            config.reseed_bootstrap_nodes_interval(),
            EFFECTIVE_BOOTSTRAP_RESEED_INTERVAL
        );
        assert_eq!(config.save_files_threshold(), DEFAULT_SAVE_FILES_THRESHOLD);
        assert_eq!(config.save_pieces(), DEFAULT_SAVE_PIECES);
        assert_eq!(config.rescrape_threshold(), DEFAULT_RESCRAPE_THRESHOLD);
        assert_eq!(
            config.metainfo_request_timeout(),
            DEFAULT_METAINFO_REQUEST_TIMEOUT
        );
        assert_eq!(
            config.metainfo_key_mutex_size(),
            DEFAULT_METAINFO_KEY_MUTEX_SIZE
        );
        assert_eq!(
            config.downstream_config().expect("downstream config"),
            DhtCrawlerDownstreamConfig::default(),
            "application defaults must preserve every downstream worker default"
        );
        assert_eq!(
            config.maintenance_config().expect("maintenance config"),
            DhtCrawlerMaintenanceConfig::default(),
            "application defaults must preserve the effective maintenance defaults"
        );
    }

    #[test]
    fn explicit_supported_cli_is_typed_validated_and_converts_without_side_effects() {
        let config = DhtCrawlerAppConfig::try_parse_from(supported_args()).expect("parse config");
        config.validate().expect("supported config");

        assert_eq!(config.expected_goose_version(), 29);
        assert_eq!(config.scaling_factor(), DEFAULT_SCALING_FACTOR);
        assert_eq!(
            config.bootstrap_nodes(),
            DEFAULT_BOOTSTRAP_NODES.map(str::to_owned)
        );
        assert_eq!(
            config.reseed_bootstrap_nodes_interval(),
            EFFECTIVE_BOOTSTRAP_RESEED_INTERVAL
        );
        assert_eq!(config.save_files_threshold(), DEFAULT_SAVE_FILES_THRESHOLD);
        assert_eq!(config.save_pieces(), DEFAULT_SAVE_PIECES);
        assert_eq!(config.rescrape_threshold(), DEFAULT_RESCRAPE_THRESHOLD);
        assert_eq!(
            config.metainfo_request_timeout(),
            DEFAULT_METAINFO_REQUEST_TIMEOUT
        );
        assert_eq!(
            config.metainfo_key_mutex_size(),
            DEFAULT_METAINFO_KEY_MUTEX_SIZE
        );

        let runtime = DhtRuntimeConfig::try_from(&config).expect("runtime config");
        assert_eq!(
            runtime.bind_addr,
            SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 4444)
        );
        assert_eq!(runtime.query_timeout, Duration::from_millis(90_500));
        assert_eq!(
            runtime.sample_infohashes_interval,
            DhtRuntimeConfig::default().sample_infohashes_interval
        );
    }

    #[test]
    fn every_cli_field_has_an_explicit_go_compatible_environment_name() {
        let environment_by_id = DhtCrawlerAppConfig::command()
            .get_arguments()
            .map(|argument| {
                (
                    argument.get_id().to_string(),
                    argument
                        .get_env()
                        .expect("application argument has explicit environment key")
                        .to_string_lossy()
                        .into_owned(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            environment_by_id,
            BTreeMap::from([
                (
                    "bootstrap_nodes".to_owned(),
                    "DHT_CRAWLER_BOOTSTRAP_NODES".to_owned()
                ),
                ("dht_server_port".to_owned(), "DHT_SERVER_PORT".to_owned()),
                (
                    "dht_server_query_timeout".to_owned(),
                    "DHT_SERVER_QUERY_TIMEOUT".to_owned()
                ),
                (
                    "expected_goose_version".to_owned(),
                    "BITMAGNET_DHT_CRAWLER_EXPECTED_GOOSE_VERSION".to_owned()
                ),
                (
                    "metainfo_key_mutex_size".to_owned(),
                    "METAINFO_REQUESTER_KEY_MUTEX_SIZE".to_owned()
                ),
                (
                    "metainfo_request_timeout".to_owned(),
                    "METAINFO_REQUESTER_REQUEST_TIMEOUT".to_owned()
                ),
                (
                    "rescrape_threshold".to_owned(),
                    "DHT_CRAWLER_RESCRAPE_THRESHOLD".to_owned()
                ),
                (
                    "reseed_bootstrap_nodes_interval".to_owned(),
                    "DHT_CRAWLER_RESEED_BOOTSTRAP_NODES_INTERVAL".to_owned()
                ),
                (
                    "save_files_threshold".to_owned(),
                    "DHT_CRAWLER_SAVE_FILES_THRESHOLD".to_owned()
                ),
                (
                    "save_pieces".to_owned(),
                    "DHT_CRAWLER_SAVE_PIECES".to_owned()
                ),
                (
                    "scaling_factor".to_owned(),
                    "DHT_CRAWLER_SCALING_FACTOR".to_owned()
                ),
            ])
        );
    }

    #[test]
    fn supported_downstream_nondefaults_project_to_every_consuming_policy() {
        for (threshold, save_pieces) in [("0", "false"), ("500000", "true")] {
            let mut args = supported_args();
            for (flag, replacement) in [
                ("--dht-crawler-save-files-threshold", threshold),
                ("--dht-crawler-save-pieces", save_pieces),
                ("--dht-crawler-rescrape-threshold", "48h"),
                ("--metainfo-requester-request-timeout", "1500ms"),
            ] {
                let value_index = args
                    .iter()
                    .position(|value| *value == flag)
                    .expect("supported args contain downstream flag")
                    + 1;
                args[value_index] = replacement;
            }

            let config = DhtCrawlerAppConfig::try_parse_from(args).expect("typed nondefaults");
            config.validate().expect("wired nondefaults are supported");
            let downstream = config.downstream_config().expect("downstream projection");
            let threshold = threshold.parse::<u64>().unwrap();
            let save_pieces = save_pieces.parse::<bool>().unwrap();

            assert_eq!(
                downstream.triage,
                DhtInfoHashTriageConfig {
                    save_files_threshold: threshold,
                    rescrape_threshold: Duration::from_secs(48 * 60 * 60),
                    ..DhtInfoHashTriageConfig::default()
                }
            );
            assert_eq!(
                downstream.persist_torrent,
                DhtPersistTorrentWorkerConfig {
                    plan_config: DhtTorrentPlanConfig {
                        save_files_threshold: threshold,
                        save_pieces,
                    },
                    ..DhtPersistTorrentWorkerConfig::default()
                }
            );
            assert_eq!(
                downstream.metainfo_requester,
                DhtPeerWireMetaInfoRequesterConfig {
                    request_timeout: Duration::from_millis(1_500),
                    ..DhtPeerWireMetaInfoRequesterConfig::default()
                }
            );
            assert_eq!(
                DhtCrawlerDownstreamConfig::try_from(&config).unwrap(),
                downstream,
                "the trait and method projections must be identical"
            );
        }
    }

    #[test]
    fn ordered_bootstrap_occurrences_and_positive_reseed_project_without_normalization() {
        let mut args = supported_args();
        for (flag, replacement) in [
            (
                "--dht-crawler-bootstrap-nodes",
                "first.example:1,,third.example:3,first.example:1",
            ),
            ("--dht-crawler-reseed-bootstrap-nodes-interval", "1m30.5s"),
        ] {
            let value_index = args
                .iter()
                .position(|value| *value == flag)
                .expect("supported args contain bootstrap flag")
                + 1;
            args[value_index] = replacement;
        }

        let config = DhtCrawlerAppConfig::try_parse_from(args).expect("typed bootstrap config");
        config
            .validate()
            .expect("wired bootstrap config is supported");
        let expected = DhtCrawlerMaintenanceConfig {
            bootstrap_ping: DhtBootstrapPingProducerConfig {
                bootstrap_nodes: vec![
                    "first.example:1".to_owned(),
                    String::new(),
                    "third.example:3".to_owned(),
                    "first.example:1".to_owned(),
                ],
                reseed_interval: Duration::from_millis(90_500),
            },
            ..DhtCrawlerMaintenanceConfig::default()
        };
        assert_eq!(config.maintenance_config().unwrap(), expected);
        assert_eq!(
            DhtCrawlerMaintenanceConfig::try_from(&config).unwrap(),
            expected
        );
    }

    #[test]
    fn unsupported_remaining_nondefaults_are_rejected_instead_of_ignored() {
        let cases = [
            (
                "--dht-crawler-scaling-factor",
                "11",
                "DHT_CRAWLER_SCALING_FACTOR",
            ),
            (
                "--metainfo-requester-key-mutex-size",
                "1001",
                "METAINFO_REQUESTER_KEY_MUTEX_SIZE",
            ),
        ];

        for (flag, replacement, expected_name) in cases {
            let mut args = supported_args();
            let value_index = args
                .iter()
                .position(|value| *value == flag)
                .expect("supported args contain flag")
                + 1;
            args[value_index] = replacement;
            let config = DhtCrawlerAppConfig::try_parse_from(args).expect("typed nondefault");
            let error = config.validate().expect_err("nondefault must be rejected");
            assert!(error.to_string().contains(expected_name), "{error}");
        }
    }

    #[test]
    fn goose_version_is_required_and_positive() {
        let command = DhtCrawlerAppConfig::command();
        let expected_goose_version = command
            .get_arguments()
            .find(|argument| argument.get_id() == "expected_goose_version")
            .expect("expected Goose argument");
        assert!(expected_goose_version.is_required_set());
        assert!(expected_goose_version.get_default_values().is_empty());

        let mut args = supported_args();
        let value_index = args
            .iter()
            .position(|value| *value == "--expected-goose-version")
            .expect("supported args contain expected Goose flag")
            + 1;
        args[value_index] = "0";
        let non_positive =
            DhtCrawlerAppConfig::try_parse_from(args).expect_err("Goose version must be positive");
        assert!(non_positive.to_string().contains("must be positive"));
    }

    #[test]
    fn go_duration_parser_covers_documented_units_and_rejects_lossy_inputs() {
        assert!(parse_go_duration("0").is_err());
        assert_eq!(
            parse_go_duration("1h2m3.004005006s").unwrap(),
            Duration::new(3_723, 4_005_006)
        );
        assert_eq!(parse_go_duration("1ms").unwrap(), Duration::from_millis(1));
        assert_eq!(parse_go_duration("1us").unwrap(), Duration::from_micros(1));
        assert_eq!(parse_go_duration("1µs").unwrap(), Duration::from_micros(1));
        assert_eq!(parse_go_duration("1μs").unwrap(), Duration::from_micros(1));
        assert_eq!(parse_go_duration("1ns").unwrap(), Duration::from_nanos(1));
        assert!(parse_go_duration("1500").is_err());
        assert!(parse_go_duration("-1s").is_err());
        assert!(parse_go_duration("0.9ns").is_err());
        assert!(parse_go_duration("9223372036854775808ns").is_err());
        assert!(parse_go_duration(" 4s").is_err());
        assert!(parse_go_duration("4s ").is_err());
    }
}
