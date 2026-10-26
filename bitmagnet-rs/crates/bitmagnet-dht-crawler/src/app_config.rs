use std::fmt::Debug;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroUsize;
use std::time::Duration;

use bitmagnet_dht::{
    DhtBootstrapPingProducerConfig, DhtCrawlerMaintenanceConfig, DhtCrawlerMaintenanceConfigError,
    DhtRuntimeConfig, DhtRuntimeConfigError, DHT_CHANNEL_MAX_CAPACITY,
};
use clap::Parser;

use crate::{
    DhtCrawlerClassifierQueue, DhtCrawlerDownstreamConfig, DhtCrawlerDownstreamConfigError,
    DhtCrawlerDownstreamLaneConfig, DhtCrawlerObserveOnlyConfig, DhtInfoHashTriageConfig,
    DhtPeerWireMetaInfoRequesterConfig, DhtPersistTorrentWorkerConfig, DhtTorrentPlanConfig,
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

/// Largest application scaling factor whose `100 * S` discovery route fits
/// Tokio's bounded-channel limit.
pub const DHT_CRAWLER_MAX_SCALING_FACTOR: usize = DHT_CHANNEL_MAX_CAPACITY / 100;

/// Complete side-effect-free projection for the observe-only executable.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DhtCrawlerObserveOnlyAppProjection {
    /// HTTP health and diagnostics listener.
    pub http_listen_addr: SocketAddr,
    /// Closed runtime, maintenance, and counter-only observation graph.
    pub graph: DhtCrawlerObserveOnlyConfig,
}

impl DhtCrawlerObserveOnlyAppProjection {
    /// Consume the projection into its listener and closed graph policies.
    #[must_use]
    pub fn into_parts(self) -> (SocketAddr, DhtCrawlerObserveOnlyConfig) {
        (self.http_listen_addr, self.graph)
    }
}

/// Pure CLI and environment policy for the PostgreSQL-nonmutating DHT soak.
///
/// The absence of database, Goose, blocker, peer-wire, and persistence fields
/// is deliberate. This type can project only the closed runtime-maintenance-
/// observer graph plus its HTTP listener.
#[derive(Clone, Debug, Parser, PartialEq, Eq)]
#[command(
    name = "bitmagnet-dht-observe",
    about = "PostgreSQL-nonmutating Rust DHT network observer",
    disable_help_subcommand = true
)]
pub struct DhtCrawlerObserveOnlyAppConfig {
    /// Address for liveness, readiness, and diagnostic HTTP endpoints.
    #[arg(
        long = "http-server-local-address",
        env = "HTTP_SERVER_LOCAL_ADDRESS",
        default_value = ":3333",
        value_parser = parse_go_http_listen_addr
    )]
    http_server_local_address: SocketAddr,

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

    /// Fresh delay between completed bootstrap resolution rounds.
    #[arg(
        long = "dht-crawler-reseed-bootstrap-nodes-interval",
        env = "DHT_CRAWLER_RESEED_BOOTSTRAP_NODES_INTERVAL",
        default_value = "10m",
        value_parser = parse_go_duration
    )]
    reseed_bootstrap_nodes_interval: Duration,
}

impl DhtCrawlerObserveOnlyAppConfig {
    /// Project and validate the entire closed graph before binding a socket.
    pub fn projection(
        &self,
    ) -> Result<DhtCrawlerObserveOnlyAppProjection, DhtCrawlerAppConfigError> {
        if self.scaling_factor == 0 {
            return Err(DhtCrawlerAppConfigError::new(
                DhtCrawlerAppConfigErrorKind::ScalingFactorZero,
            ));
        }
        let runtime = DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, self.dht_server_port),
            query_timeout: self.dht_server_query_timeout,
            discovery_capacity: checked_scaled_capacity(self.scaling_factor, 100)?,
            ..DhtRuntimeConfig::default()
        };
        runtime.validate().map_err(|source| {
            DhtCrawlerAppConfigError::new(DhtCrawlerAppConfigErrorKind::Runtime(source))
        })?;
        let maintenance = DhtCrawlerMaintenanceConfig {
            ping_capacity: checked_scaled_capacity(self.scaling_factor, 1)?,
            find_node_capacity: checked_scaled_capacity(self.scaling_factor, 10)?,
            sample_infohashes_capacity: checked_scaled_capacity(self.scaling_factor, 10)?,
            bootstrap_ping: DhtBootstrapPingProducerConfig {
                bootstrap_nodes: self.bootstrap_nodes.clone(),
                reseed_interval: self.reseed_bootstrap_nodes_interval,
            },
        };
        maintenance.validate().map_err(|source| {
            DhtCrawlerAppConfigError::new(DhtCrawlerAppConfigErrorKind::Maintenance(source))
        })?;
        let graph = DhtCrawlerObserveOnlyConfig {
            runtime,
            maintenance,
            observation_capacity: checked_scaled_capacity(self.scaling_factor, 10)?,
        };
        graph.validate().map_err(|source| match source {
            crate::DhtCrawlerObserveOnlyConfigError::Runtime(source) => {
                DhtCrawlerAppConfigError::new(DhtCrawlerAppConfigErrorKind::Runtime(source))
            }
            crate::DhtCrawlerObserveOnlyConfigError::Maintenance(source) => {
                DhtCrawlerAppConfigError::new(DhtCrawlerAppConfigErrorKind::Maintenance(source))
            }
            crate::DhtCrawlerObserveOnlyConfigError::ObservationCapacityOutOfRange { .. } => {
                DhtCrawlerAppConfigError::new(
                    DhtCrawlerAppConfigErrorKind::ScalingCapacityOverflow {
                        scaling_factor: self.scaling_factor,
                        multiplier: 10,
                    },
                )
            }
        })?;
        Ok(DhtCrawlerObserveOnlyAppProjection {
            http_listen_addr: self.http_server_local_address,
            graph,
        })
    }

    #[must_use]
    pub const fn http_server_local_address(&self) -> SocketAddr {
        self.http_server_local_address
    }
}

/// Complete side-effect-free policy for one owned DHT crawler graph.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DhtCrawlerAppProjection {
    /// Exact already-applied Goose migration head required before runtime bind.
    pub expected_goose_version: i64,
    /// UDP runtime and discovery-ingress policy.
    pub runtime: DhtRuntimeConfig,
    /// Discovery scheduler, maintenance workers, and bootstrap policy.
    pub maintenance: DhtCrawlerMaintenanceConfig,
    /// Root triage route, downstream lanes, and persistence policy.
    pub downstream: DhtCrawlerDownstreamConfig,
}

impl DhtCrawlerAppProjection {
    /// Revalidate the mutable public projection before any database or network
    /// effect.
    pub fn validate(&self) -> Result<(), DhtCrawlerAppProjectionError> {
        if self.expected_goose_version <= 0 {
            return Err(DhtCrawlerAppProjectionError::ExpectedGooseVersion {
                version: self.expected_goose_version,
            });
        }
        self.runtime
            .validate()
            .map_err(DhtCrawlerAppProjectionError::Runtime)?;
        self.maintenance
            .validate()
            .map_err(DhtCrawlerAppProjectionError::Maintenance)?;
        self.downstream
            .validate()
            .map_err(DhtCrawlerAppProjectionError::Downstream)
    }

    /// Consume the atomic projection into its three construction policies.
    /// The admission pin remains available from the projection before this
    /// compatibility-preserving decomposition.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        DhtRuntimeConfig,
        DhtCrawlerMaintenanceConfig,
        DhtCrawlerDownstreamConfig,
    ) {
        (self.runtime, self.maintenance, self.downstream)
    }
}

/// Invalid mutable writer-graph projection.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum DhtCrawlerAppProjectionError {
    #[error("expected Goose version must be positive, got {version}")]
    ExpectedGooseVersion { version: i64 },
    #[error(transparent)]
    Runtime(DhtRuntimeConfigError),
    #[error(transparent)]
    Maintenance(DhtCrawlerMaintenanceConfigError),
    #[error(transparent)]
    Downstream(DhtCrawlerDownstreamConfigError),
}

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

    /// Required classifier queue target for newly persisted torrents.
    ///
    /// `shadow` isolates classifier consumption but does not disable the
    /// crawler's PostgreSQL torrent, source, file, pieces, or queue writes.
    #[arg(
        long = "classifier-queue",
        env = "BITMAGNET_DHT_CRAWLER_CLASSIFIER_QUEUE",
        value_enum
    )]
    classifier_queue: DhtCrawlerClassifierQueue,

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
    /// Validate the complete application graph without constructing runtime
    /// state, queues, collaborators, or tasks.
    ///
    /// # Errors
    ///
    /// Returns the same typed, deterministic failure as [`Self::projection`]
    /// when any runtime, maintenance, downstream, or remaining compatibility
    /// policy is invalid.
    pub fn validate(&self) -> Result<(), DhtCrawlerAppConfigError> {
        self.projection().map(drop)
    }

    /// Project every application value into one fully validated crawler graph.
    ///
    /// # Errors
    ///
    /// Returns a typed error before exposing any partial config when scaling
    /// is zero, arithmetic overflows, a bounded route is out of range, an
    /// unsupported compatibility value is configured, or a child policy is
    /// invalid.
    pub fn projection(&self) -> Result<DhtCrawlerAppProjection, DhtCrawlerAppConfigError> {
        if self.scaling_factor == 0 {
            return Err(DhtCrawlerAppConfigError::new(
                DhtCrawlerAppConfigErrorKind::ScalingFactorZero,
            ));
        }
        let discovery_capacity = checked_scaled_capacity(self.scaling_factor, 100)?;
        let runtime = DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, self.dht_server_port),
            query_timeout: self.dht_server_query_timeout,
            discovery_capacity,
            ..DhtRuntimeConfig::default()
        };
        runtime.validate().map_err(|source| {
            DhtCrawlerAppConfigError::new(DhtCrawlerAppConfigErrorKind::Runtime(source))
        })?;

        require_supported(
            "METAINFO_REQUESTER_KEY_MUTEX_SIZE",
            &self.metainfo_key_mutex_size,
            &DEFAULT_METAINFO_KEY_MUTEX_SIZE,
        )?;

        let scaling_capacity = checked_scaled_capacity(self.scaling_factor, 1)?;
        let ten_scaling = checked_scaled_capacity(self.scaling_factor, 10)?;
        let twenty_scaling = checked_scaled_capacity(self.scaling_factor, 20)?;
        let forty_scaling = checked_scaled_capacity(self.scaling_factor, 40)?;

        let maintenance = DhtCrawlerMaintenanceConfig {
            ping_capacity: scaling_capacity,
            find_node_capacity: ten_scaling,
            sample_infohashes_capacity: ten_scaling,
            bootstrap_ping: DhtBootstrapPingProducerConfig {
                bootstrap_nodes: self.bootstrap_nodes.clone(),
                reseed_interval: self.reseed_bootstrap_nodes_interval,
            },
        };
        maintenance.validate().map_err(|source| {
            DhtCrawlerAppConfigError::new(DhtCrawlerAppConfigErrorKind::Maintenance(source))
        })?;

        let downstream = DhtCrawlerDownstreamConfig {
            root_triage_capacity: ten_scaling,
            get_peers_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: ten_scaling,
                worker_max_inflight: twenty_scaling,
            },
            scrape_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: ten_scaling,
                worker_max_inflight: twenty_scaling,
            },
            request_meta_info_lane: DhtCrawlerDownstreamLaneConfig {
                route_capacity: ten_scaling,
                worker_max_inflight: forty_scaling,
            },
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
                classifier_queue: self.classifier_queue,
                ..DhtPersistTorrentWorkerConfig::default()
            },
            metainfo_requester: DhtPeerWireMetaInfoRequesterConfig {
                request_timeout: self.metainfo_request_timeout,
                ..DhtPeerWireMetaInfoRequesterConfig::default()
            },
        };
        downstream.validate().map_err(|source| {
            DhtCrawlerAppConfigError::new(DhtCrawlerAppConfigErrorKind::Downstream(source))
        })?;

        Ok(DhtCrawlerAppProjection {
            expected_goose_version: self.expected_goose_version,
            runtime,
            maintenance,
            downstream,
        })
    }

    /// Return the runtime portion of the fully validated atomic graph policy.
    ///
    /// # Errors
    ///
    /// Returns any whole-graph projection error, including maintenance or
    /// downstream failures unrelated to the runtime fields themselves.
    pub fn dht_runtime_config(&self) -> Result<DhtRuntimeConfig, DhtCrawlerAppConfigError> {
        Ok(self.projection()?.runtime)
    }

    /// Project validated application policy into the taskless maintenance
    /// composition without resolving bootstrap endpoints or starting work.
    ///
    /// # Errors
    ///
    /// Returns any whole-graph projection error, including runtime or
    /// downstream failures unrelated to the maintenance fields themselves.
    pub fn maintenance_config(
        &self,
    ) -> Result<DhtCrawlerMaintenanceConfig, DhtCrawlerAppConfigError> {
        Ok(self.projection()?.maintenance)
    }

    /// Project validated application policy into the taskless downstream
    /// composition while retaining every unrelated worker default.
    ///
    /// # Errors
    ///
    /// Returns any whole-graph projection error, including runtime or
    /// maintenance failures unrelated to the downstream fields themselves.
    pub fn downstream_config(
        &self,
    ) -> Result<DhtCrawlerDownstreamConfig, DhtCrawlerAppConfigError> {
        Ok(self.projection()?.downstream)
    }

    pub const fn expected_goose_version(&self) -> i64 {
        self.expected_goose_version
    }

    pub const fn classifier_queue(&self) -> DhtCrawlerClassifierQueue {
        self.classifier_queue
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

/// A complete application policy could not be projected safely.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{kind}")]
pub struct DhtCrawlerAppConfigError {
    #[source]
    kind: DhtCrawlerAppConfigErrorKind,
}

impl DhtCrawlerAppConfigError {
    const fn new(kind: DhtCrawlerAppConfigErrorKind) -> Self {
        Self { kind }
    }

    /// Borrow the typed application projection failure.
    #[must_use]
    pub const fn kind(&self) -> &DhtCrawlerAppConfigErrorKind {
        &self.kind
    }

    /// Consume the wrapper and recover the typed application projection failure.
    #[must_use]
    pub fn into_kind(self) -> DhtCrawlerAppConfigErrorKind {
        self.kind
    }
}

/// Exact reason an application policy could not be projected.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DhtCrawlerAppConfigErrorKind {
    /// Zero would create unbuffered or non-running Go lanes and cannot form a
    /// positive Rust bounded-route policy.
    #[error("DHT crawler scaling factor must be positive")]
    ScalingFactorZero,
    /// Native-width scaling arithmetic overflowed before any config was exposed.
    #[error("DHT crawler scaling factor {scaling_factor} overflows multiplier {multiplier}")]
    ScalingCapacityOverflow {
        scaling_factor: usize,
        multiplier: usize,
    },
    /// A parsed compatibility knob is not yet honored by the Rust graph.
    #[error(
        "unsupported {name} value {configured}; the current Rust composition requires {supported}"
    )]
    UnsupportedValue {
        name: &'static str,
        configured: String,
        supported: String,
    },
    /// The projected runtime policy is outside its typed bounds.
    #[error("invalid DHT runtime configuration: {0}")]
    Runtime(#[source] DhtRuntimeConfigError),
    /// The projected maintenance policy is invalid.
    #[error("invalid DHT crawler maintenance configuration: {0}")]
    Maintenance(#[source] DhtCrawlerMaintenanceConfigError),
    /// The projected downstream policy is invalid.
    #[error("invalid DHT crawler downstream configuration: {0}")]
    Downstream(#[source] DhtCrawlerDownstreamConfigError),
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
    Err(DhtCrawlerAppConfigError::new(
        DhtCrawlerAppConfigErrorKind::UnsupportedValue {
            name,
            configured: format!("{configured:?}"),
            supported: format!("{supported:?}"),
        },
    ))
}

fn checked_scaled_capacity(
    scaling_factor: usize,
    multiplier: usize,
) -> Result<NonZeroUsize, DhtCrawlerAppConfigError> {
    scaling_factor
        .checked_mul(multiplier)
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| {
            DhtCrawlerAppConfigError::new(DhtCrawlerAppConfigErrorKind::ScalingCapacityOverflow {
                scaling_factor,
                multiplier,
            })
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

/// Parse Go's accepted bare-port listener form as an IPv4 wildcard address.
fn parse_go_http_listen_addr(value: &str) -> Result<SocketAddr, String> {
    let normalized = value
        .strip_prefix(':')
        .map_or_else(|| value.to_owned(), |port| format!("0.0.0.0:{port}"));
    normalized
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid HTTP listener address {value:?}: {error}"))
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
    use std::error::Error;
    use std::ffi::OsString;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::num::NonZeroUsize;
    use std::sync::Mutex;
    use std::time::Duration;

    use bitmagnet_dht::{
        DhtBootstrapPingProducerConfig, DhtBootstrapPingProducerConfigError,
        DhtCrawlerMaintenanceConfig, DhtCrawlerMaintenanceConfigError, DhtRuntimeConfig,
        DhtRuntimeConfigError, DHT_CHANNEL_MAX_CAPACITY,
    };
    use clap::{CommandFactory, FromArgMatches, Parser};

    use super::{
        parse_go_duration, parse_go_http_listen_addr, DhtCrawlerAppConfig,
        DhtCrawlerAppConfigErrorKind, DhtCrawlerAppProjection, DhtCrawlerClassifierQueue,
        DhtCrawlerDownstreamConfig, DhtCrawlerDownstreamLaneConfig, DhtCrawlerObserveOnlyAppConfig,
        DhtCrawlerObserveOnlyAppProjection, DhtCrawlerObserveOnlyConfig, DhtInfoHashTriageConfig,
        DhtPeerWireMetaInfoRequesterConfig, DhtPersistTorrentWorkerConfig, DhtTorrentPlanConfig,
        DEFAULT_BOOTSTRAP_NODES, DEFAULT_METAINFO_KEY_MUTEX_SIZE, DEFAULT_METAINFO_REQUEST_TIMEOUT,
        DEFAULT_RESCRAPE_THRESHOLD, DEFAULT_SAVE_FILES_THRESHOLD, DEFAULT_SAVE_PIECES,
        DEFAULT_SCALING_FACTOR, DHT_CRAWLER_MAX_SCALING_FACTOR,
        EFFECTIVE_BOOTSTRAP_RESEED_INTERVAL,
    };

    const HTTP_LISTEN_ENV: &str = "HTTP_SERVER_LOCAL_ADDRESS";
    static OBSERVE_ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvironmentRestore {
        name: &'static str,
        original: Option<OsString>,
    }

    impl EnvironmentRestore {
        fn set(name: &'static str, value: &str) -> Self {
            let original = std::env::var_os(name);
            std::env::set_var(name, value);
            Self { name, original }
        }
    }

    impl Drop for EnvironmentRestore {
        fn drop(&mut self) {
            match self.original.take() {
                Some(value) => std::env::set_var(self.name, value),
                None => std::env::remove_var(self.name),
            }
        }
    }

    fn supported_args() -> Vec<&'static str> {
        vec![
            "bitmagnet-dht-crawler",
            "--dht-server-port",
            "4444",
            "--dht-server-query-timeout",
            "1m30.5s",
            "--expected-goose-version",
            "29",
            "--classifier-queue",
            "shadow",
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

    fn parse_observe_without_environment(args: &[&str]) -> DhtCrawlerObserveOnlyAppConfig {
        let command = DhtCrawlerObserveOnlyAppConfig::command()
            .mut_args(|argument| argument.env(Option::<&'static str>::None));
        let matches = command
            .try_get_matches_from(args)
            .expect("parse observe-only config without ambient environment");
        DhtCrawlerObserveOnlyAppConfig::from_arg_matches(&matches)
            .expect("build typed observe-only config")
    }

    #[test]
    fn observe_only_defaults_project_only_the_closed_graph_and_http_listener() {
        let config = parse_observe_without_environment(&["bitmagnet-dht-observe"]);
        let projection = config
            .projection()
            .expect("default observe policy is valid");
        let http_listen_addr = "0.0.0.0:3333".parse::<SocketAddr>().unwrap();

        assert_eq!(config.http_server_local_address(), http_listen_addr);
        assert_eq!(
            projection,
            DhtCrawlerObserveOnlyAppProjection {
                http_listen_addr,
                graph: DhtCrawlerObserveOnlyConfig::default(),
            }
        );
        assert_eq!(
            projection.clone().into_parts(),
            (http_listen_addr, DhtCrawlerObserveOnlyConfig::default())
        );
    }

    #[test]
    fn observe_only_cli_projects_scaled_network_policy_without_writer_fields() {
        let config = parse_observe_without_environment(&[
            "bitmagnet-dht-observe",
            "--http-server-local-address",
            ":4445",
            "--dht-server-port",
            "4444",
            "--dht-server-query-timeout",
            "1500ms",
            "--dht-crawler-scaling-factor",
            "2",
            "--dht-crawler-bootstrap-nodes",
            "one.invalid:1,two.invalid:2",
            "--dht-crawler-reseed-bootstrap-nodes-interval",
            "1m30s",
        ]);
        let projection = config
            .projection()
            .expect("explicit observe policy is valid");

        assert_eq!(
            projection.http_listen_addr,
            "0.0.0.0:4445".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            projection.graph.runtime.bind_addr,
            SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 4444)
        );
        assert_eq!(
            projection.graph.runtime.query_timeout,
            Duration::from_millis(1_500)
        );
        assert_eq!(projection.graph.runtime.discovery_capacity.get(), 200);
        assert_eq!(projection.graph.maintenance.ping_capacity.get(), 2);
        assert_eq!(projection.graph.maintenance.find_node_capacity.get(), 20);
        assert_eq!(
            projection
                .graph
                .maintenance
                .sample_infohashes_capacity
                .get(),
            20
        );
        assert_eq!(projection.graph.observation_capacity.get(), 20);
        assert_eq!(
            projection.graph.maintenance.bootstrap_ping.bootstrap_nodes,
            ["one.invalid:1", "two.invalid:2"].map(str::to_owned)
        );
        assert_eq!(
            projection.graph.maintenance.bootstrap_ping.reseed_interval,
            Duration::from_secs(90)
        );

        let forbidden = DhtCrawlerObserveOnlyAppConfig::command()
            .mut_args(|argument| argument.env(Option::<&'static str>::None))
            .try_get_matches_from(["bitmagnet-dht-observe", "--expected-goose-version", "29"]);
        assert!(
            forbidden.is_err(),
            "observe-only CLI must expose no Goose pin"
        );
        let forbidden = DhtCrawlerObserveOnlyAppConfig::command()
            .mut_args(|argument| argument.env(Option::<&'static str>::None))
            .try_get_matches_from(["bitmagnet-dht-observe", "--classifier-queue", "shadow"]);
        assert!(
            forbidden.is_err(),
            "observe-only CLI must expose no classifier queue target"
        );
    }

    #[test]
    fn observe_only_fields_have_exact_environment_keys_and_typed_addresses() {
        let environment_by_id = DhtCrawlerObserveOnlyAppConfig::command()
            .get_arguments()
            .map(|argument| {
                (
                    argument.get_id().to_string(),
                    argument
                        .get_env()
                        .expect("observe argument has an explicit environment key")
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
                    "DHT_CRAWLER_BOOTSTRAP_NODES".to_owned(),
                ),
                ("dht_server_port".to_owned(), "DHT_SERVER_PORT".to_owned()),
                (
                    "dht_server_query_timeout".to_owned(),
                    "DHT_SERVER_QUERY_TIMEOUT".to_owned(),
                ),
                (
                    "http_server_local_address".to_owned(),
                    "HTTP_SERVER_LOCAL_ADDRESS".to_owned(),
                ),
                (
                    "reseed_bootstrap_nodes_interval".to_owned(),
                    "DHT_CRAWLER_RESEED_BOOTSTRAP_NODES_INTERVAL".to_owned(),
                ),
                (
                    "scaling_factor".to_owned(),
                    "DHT_CRAWLER_SCALING_FACTOR".to_owned(),
                ),
            ])
        );

        let invalid = DhtCrawlerObserveOnlyAppConfig::command()
            .mut_args(|argument| argument.env(Option::<&'static str>::None))
            .try_get_matches_from([
                "bitmagnet-dht-observe",
                "--http-server-local-address",
                "not-an-address",
            ]);
        assert!(invalid.is_err());
    }

    #[test]
    fn observe_only_go_listener_syntax_is_shared_by_cli_and_environment() {
        assert_eq!(
            parse_go_http_listen_addr(":3333").unwrap(),
            "0.0.0.0:3333".parse::<SocketAddr>().unwrap()
        );

        let _env_lock = OBSERVE_ENV_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _restore = EnvironmentRestore::set(HTTP_LISTEN_ENV, ":4555");
        let command = DhtCrawlerObserveOnlyAppConfig::command().mut_args(|argument| {
            if argument.get_id() == "http_server_local_address" {
                argument
            } else {
                argument.env(Option::<&'static str>::None)
            }
        });
        let matches = command
            .try_get_matches_from(["bitmagnet-dht-observe"])
            .expect("Go listener environment syntax parses");
        let config = DhtCrawlerObserveOnlyAppConfig::from_arg_matches(&matches)
            .expect("build typed observe-only environment config");
        assert_eq!(
            config.http_server_local_address(),
            "0.0.0.0:4555".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn observe_only_projection_rejects_invalid_scaling_and_reseed_policy() {
        let mut config = parse_observe_without_environment(&["bitmagnet-dht-observe"]);
        config.scaling_factor = 0;
        assert_eq!(
            config.projection().unwrap_err().into_kind(),
            DhtCrawlerAppConfigErrorKind::ScalingFactorZero
        );

        config.scaling_factor = DHT_CRAWLER_MAX_SCALING_FACTOR + 1;
        let over_max_capacity = NonZeroUsize::new(
            config
                .scaling_factor
                .checked_mul(100)
                .expect("one over Tokio's divided limit still fits usize"),
        )
        .unwrap();
        assert_eq!(
            config.projection().unwrap_err().into_kind(),
            DhtCrawlerAppConfigErrorKind::Runtime(
                DhtRuntimeConfigError::DiscoveryCapacityOutOfRange {
                    capacity: over_max_capacity,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                }
            )
        );

        config.scaling_factor = DHT_CRAWLER_MAX_SCALING_FACTOR;
        config.reseed_bootstrap_nodes_interval = Duration::MAX;
        assert_eq!(
            config.projection().unwrap_err().into_kind(),
            DhtCrawlerAppConfigErrorKind::Maintenance(
                DhtCrawlerMaintenanceConfigError::BootstrapPing(
                    DhtBootstrapPingProducerConfigError::ReseedIntervalOutOfRange
                )
            )
        );
    }

    #[test]
    fn clap_defaults_match_the_effective_supported_contract() {
        let config = parse_without_environment(&[
            "bitmagnet-dht-crawler",
            "--expected-goose-version",
            "29",
            "--classifier-queue",
            "live",
        ]);

        let projection = config.projection().expect("default config is supported");
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
        assert_eq!(config.classifier_queue(), DhtCrawlerClassifierQueue::Live);
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
            projection.clone().into_parts(),
            (
                DhtRuntimeConfig::default(),
                DhtCrawlerMaintenanceConfig::default(),
                DhtCrawlerDownstreamConfig::default(),
            ),
            "the committed three-part decomposition remains source-compatible"
        );
        assert_eq!(
            projection,
            DhtCrawlerAppProjection {
                expected_goose_version: 29,
                runtime: DhtRuntimeConfig::default(),
                maintenance: DhtCrawlerMaintenanceConfig::default(),
                downstream: DhtCrawlerDownstreamConfig::default(),
            },
            "application defaults must preserve every effective graph default"
        );
    }

    #[test]
    fn explicit_supported_cli_is_typed_validated_and_converts_without_side_effects() {
        let config = DhtCrawlerAppConfig::try_parse_from(supported_args()).expect("parse config");
        config.validate().expect("supported config");
        let projection = config.projection().expect("complete projection");

        assert_eq!(config.expected_goose_version(), 29);
        assert_eq!(config.classifier_queue(), DhtCrawlerClassifierQueue::Shadow);
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
        assert_eq!(runtime, projection.runtime);
        assert_eq!(config.dht_runtime_config().unwrap(), projection.runtime);
        assert_eq!(
            DhtCrawlerMaintenanceConfig::try_from(&config).unwrap(),
            projection.maintenance
        );
        assert_eq!(config.maintenance_config().unwrap(), projection.maintenance);
        assert_eq!(
            DhtCrawlerDownstreamConfig::try_from(&config).unwrap(),
            projection.downstream
        );
        assert_eq!(config.downstream_config().unwrap(), projection.downstream);
        assert_eq!(
            projection.downstream.persist_torrent.classifier_queue,
            DhtCrawlerClassifierQueue::Shadow
        );
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
                    "classifier_queue".to_owned(),
                    "BITMAGNET_DHT_CRAWLER_CLASSIFIER_QUEUE".to_owned()
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
                ("--dht-crawler-scaling-factor", "2"),
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
                downstream.get_peers_lane,
                DhtCrawlerDownstreamLaneConfig {
                    route_capacity: NonZeroUsize::new(20).unwrap(),
                    worker_max_inflight: NonZeroUsize::new(40).unwrap(),
                }
            );
            assert_eq!(downstream.scrape_lane, downstream.get_peers_lane);
            assert_eq!(
                downstream.request_meta_info_lane,
                DhtCrawlerDownstreamLaneConfig {
                    route_capacity: NonZeroUsize::new(20).unwrap(),
                    worker_max_inflight: NonZeroUsize::new(80).unwrap(),
                }
            );

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
                    classifier_queue: DhtCrawlerClassifierQueue::Shadow,
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
    fn scaling_factor_projects_every_graph_budget_and_keeps_unrelated_policy_fixed() {
        for (scaling_factor, discovery, ping, ten_scaling, twenty_scaling, forty_scaling) in
            [(2, 200, 2, 20, 40, 80), (10, 1_000, 10, 100, 200, 400)]
        {
            let mut config = parse_without_environment(&[
                "bitmagnet-dht-crawler",
                "--expected-goose-version",
                "29",
                "--classifier-queue",
                "live",
            ]);
            config.scaling_factor = scaling_factor;
            let projection = config.projection().expect("bounded scaling projection");

            assert_eq!(
                projection.runtime.discovery_capacity,
                NonZeroUsize::new(discovery).unwrap()
            );
            assert_eq!(
                projection.runtime.bind_addr,
                DhtRuntimeConfig::default().bind_addr
            );
            assert_eq!(
                projection.runtime.query_timeout,
                DhtRuntimeConfig::default().query_timeout
            );
            assert_eq!(
                projection.runtime.sample_infohashes_interval,
                DhtRuntimeConfig::default().sample_infohashes_interval
            );

            assert_eq!(
                projection.maintenance.ping_capacity,
                NonZeroUsize::new(ping).unwrap()
            );
            assert_eq!(
                projection.maintenance.find_node_capacity,
                NonZeroUsize::new(ten_scaling).unwrap()
            );
            assert_eq!(
                projection.maintenance.sample_infohashes_capacity,
                NonZeroUsize::new(ten_scaling).unwrap()
            );
            assert_eq!(
                projection.maintenance.bootstrap_ping,
                DhtBootstrapPingProducerConfig::default()
            );

            let ten_scaling = NonZeroUsize::new(ten_scaling).unwrap();
            let twenty_scaling = NonZeroUsize::new(twenty_scaling).unwrap();
            let forty_scaling = NonZeroUsize::new(forty_scaling).unwrap();
            assert_eq!(projection.downstream.root_triage_capacity, ten_scaling);
            assert_eq!(
                projection.downstream.get_peers_lane,
                DhtCrawlerDownstreamLaneConfig {
                    route_capacity: ten_scaling,
                    worker_max_inflight: twenty_scaling,
                }
            );
            assert_eq!(
                projection.downstream.scrape_lane,
                projection.downstream.get_peers_lane
            );
            assert_eq!(
                projection.downstream.request_meta_info_lane,
                DhtCrawlerDownstreamLaneConfig {
                    route_capacity: ten_scaling,
                    worker_max_inflight: forty_scaling,
                }
            );
            assert_eq!(
                projection.downstream.triage,
                DhtInfoHashTriageConfig::default()
            );
            assert_eq!(
                projection.downstream.persist_torrent,
                DhtPersistTorrentWorkerConfig::default()
            );
            assert_eq!(
                projection.downstream.metainfo_requester,
                DhtPeerWireMetaInfoRequesterConfig::default()
            );
        }
    }

    #[test]
    fn scaling_failures_are_typed_borrowed_and_have_deterministic_precedence() {
        let mut config = parse_without_environment(&[
            "bitmagnet-dht-crawler",
            "--expected-goose-version",
            "29",
            "--classifier-queue",
            "live",
        ]);
        config.scaling_factor = 0;
        config.metainfo_key_mutex_size = DEFAULT_METAINFO_KEY_MUTEX_SIZE + 1;
        let unchanged = config.clone();
        for error in [
            config.validate().unwrap_err(),
            config.projection().unwrap_err(),
            config.dht_runtime_config().unwrap_err(),
            config.maintenance_config().unwrap_err(),
            config.downstream_config().unwrap_err(),
            DhtRuntimeConfig::try_from(&config).unwrap_err(),
            DhtCrawlerMaintenanceConfig::try_from(&config).unwrap_err(),
            DhtCrawlerDownstreamConfig::try_from(&config).unwrap_err(),
        ] {
            assert_eq!(
                error.kind(),
                &DhtCrawlerAppConfigErrorKind::ScalingFactorZero
            );
        }
        assert_eq!(
            config, unchanged,
            "borrowed failure must preserve all policy"
        );

        config.scaling_factor = usize::MAX;
        assert_eq!(
            config.projection().unwrap_err().into_kind(),
            DhtCrawlerAppConfigErrorKind::ScalingCapacityOverflow {
                scaling_factor: usize::MAX,
                multiplier: 100,
            }
        );

        config.scaling_factor = DHT_CRAWLER_MAX_SCALING_FACTOR + 1;
        let over_max_capacity = NonZeroUsize::new(
            config
                .scaling_factor
                .checked_mul(100)
                .expect("one over Tokio's divided limit still fits usize"),
        )
        .unwrap();
        let error = config.projection().unwrap_err();
        assert_eq!(
            error.kind(),
            &DhtCrawlerAppConfigErrorKind::Runtime(
                DhtRuntimeConfigError::DiscoveryCapacityOutOfRange {
                    capacity: over_max_capacity,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                }
            )
        );
        let kind_source = error
            .source()
            .expect("the public wrapper exposes its typed kind");
        assert!(kind_source
            .downcast_ref::<DhtCrawlerAppConfigErrorKind>()
            .is_some());
        assert_eq!(
            kind_source
                .source()
                .and_then(|source| source.downcast_ref::<DhtRuntimeConfigError>()),
            Some(&DhtRuntimeConfigError::DiscoveryCapacityOutOfRange {
                capacity: over_max_capacity,
                maximum: DHT_CHANNEL_MAX_CAPACITY,
            })
        );
        assert_eq!(
            error.into_kind(),
            DhtCrawlerAppConfigErrorKind::Runtime(
                DhtRuntimeConfigError::DiscoveryCapacityOutOfRange {
                    capacity: over_max_capacity,
                    maximum: DHT_CHANNEL_MAX_CAPACITY,
                }
            )
        );

        config.scaling_factor = DHT_CRAWLER_MAX_SCALING_FACTOR;
        config.metainfo_key_mutex_size = DEFAULT_METAINFO_KEY_MUTEX_SIZE;
        let maximum = config
            .projection()
            .expect("maximum safe scaling projection");
        assert_eq!(
            maximum.runtime.discovery_capacity,
            NonZeroUsize::new(DHT_CRAWLER_MAX_SCALING_FACTOR * 100).unwrap()
        );

        config.reseed_bootstrap_nodes_interval = Duration::MAX;
        assert_eq!(
            config.projection().unwrap_err().into_kind(),
            DhtCrawlerAppConfigErrorKind::Maintenance(
                DhtCrawlerMaintenanceConfigError::BootstrapPing(
                    DhtBootstrapPingProducerConfigError::ReseedIntervalOutOfRange
                )
            )
        );
    }

    #[test]
    fn unsupported_remaining_nondefaults_are_rejected_instead_of_ignored() {
        let cases = [("--metainfo-requester-key-mutex-size", "1001")];

        for (flag, replacement) in cases {
            let mut args = supported_args();
            let value_index = args
                .iter()
                .position(|value| *value == flag)
                .expect("supported args contain flag")
                + 1;
            args[value_index] = replacement;
            let config = DhtCrawlerAppConfig::try_parse_from(args).expect("typed nondefault");
            let error = config.validate().expect_err("nondefault must be rejected");
            assert_eq!(
                error.into_kind(),
                DhtCrawlerAppConfigErrorKind::UnsupportedValue {
                    name: "METAINFO_REQUESTER_KEY_MUTEX_SIZE",
                    configured: "1001".to_owned(),
                    supported: "1000".to_owned(),
                }
            );
        }
    }

    #[test]
    fn goose_version_and_classifier_queue_are_required_and_typed() {
        let command = DhtCrawlerAppConfig::command();
        let expected_goose_version = command
            .get_arguments()
            .find(|argument| argument.get_id() == "expected_goose_version")
            .expect("expected Goose argument");
        assert!(expected_goose_version.is_required_set());
        assert!(expected_goose_version.get_default_values().is_empty());
        let classifier_queue = command
            .get_arguments()
            .find(|argument| argument.get_id() == "classifier_queue")
            .expect("classifier queue argument");
        assert!(classifier_queue.is_required_set());
        assert!(classifier_queue.get_default_values().is_empty());

        for (value, expected) in [
            ("shadow", DhtCrawlerClassifierQueue::Shadow),
            ("live", DhtCrawlerClassifierQueue::Live),
        ] {
            let mut args = supported_args();
            let value_index = args
                .iter()
                .position(|argument| *argument == "--classifier-queue")
                .unwrap()
                + 1;
            args[value_index] = value;
            assert_eq!(
                DhtCrawlerAppConfig::try_parse_from(args)
                    .expect("supported classifier queue")
                    .classifier_queue(),
                expected
            );
        }
        let missing = DhtCrawlerAppConfig::try_parse_from([
            "bitmagnet-dht-crawler",
            "--expected-goose-version",
            "29",
        ]);
        assert!(missing.is_err());
        let mut invalid_args = supported_args();
        let classifier_value = invalid_args
            .iter()
            .position(|argument| *argument == "--classifier-queue")
            .unwrap()
            + 1;
        invalid_args[classifier_value] = "other";
        assert!(DhtCrawlerAppConfig::try_parse_from(invalid_args).is_err());

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
