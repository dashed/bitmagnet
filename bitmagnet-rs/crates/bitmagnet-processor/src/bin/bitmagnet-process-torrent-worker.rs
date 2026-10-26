//! Standalone, non-deployed persisting Rust `process_torrent` queue worker.
//!
//! It claims only admitted supported-subset jobs (explicit `default` workflow,
//! all three attach flags explicitly false) with Go's `FOR UPDATE SKIP LOCKED`
//! order, so it can run beside the Go worker on the same queue: every other
//! row, and every claimed row the Rust kernel cannot reproduce, is left to Go.
//!
//! With `--admit-flags-on` it also claims the explicit flags-on payload (all
//! three attach flags true), classifying through PostgreSQL local search and
//! the TMDB API, and abstains from any job whose resolver calls fail.
//!
//! With `--admit-organic` as well it instead claims organic payloads (the DHT,
//! importer, batch, CLI and retry shapes) up to `--organic-max-priority`.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bitmagnet_blocking::BlockingManager as PersistentBlockingManager;
use bitmagnet_classifier::core_config_digest;
use bitmagnet_content_search::PgContentSearch;
use bitmagnet_processor::{
    register_blocking_failure_metrics, register_worker_metrics, Materializer, ProcessTorrentWorker,
    ProcessTorrentWorkerConfig,
};
use bitmagnet_queue::{QueueStore, PROCESS_TORRENT};
use bitmagnet_resolver::LiveContentResolver;
use bitmagnet_tmdb::{TmdbClient, TmdbConfig};
use clap::Parser;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// Oldest Goose head the worker runs against: the partial index that serves the
/// admitted claim (`00035_queue_admitted_process_torrent_claim_index.sql`).
/// Without it every poll reads the whole organic `process_torrent` backlog. The
/// loader, writer kernel, and generic queue SQL need head 26.
const MIN_GOOSE_VERSION: i64 = 35;

/// Oldest Goose head with `--admit-flags-on`: the partial index that serves
/// that claim (`00036_queue_admitted_flags_on_process_torrent_claim_index.sql`).
const MIN_FLAGS_ON_GOOSE_VERSION: i64 = 36;

const DEFAULT_CLASSIFY_PARALLELISM: usize = 4;

/// Go `tmdb.NewDefaultConfig`'s base URL.
const DEFAULT_TMDB_BASE_URL: &str = "https://api.themoviedb.org/3";

/// A TMDB API key, redacted from `Debug` so `Args` can never print it.
#[derive(Clone)]
struct TmdbApiKey(String);

impl std::str::FromStr for TmdbApiKey {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Debug for TmdbApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TmdbApiKey(<redacted>)")
    }
}

#[derive(Debug, Parser)]
#[command(name = "bitmagnet-process-torrent-worker")]
struct Args {
    #[arg(long, env = "BITMAGNET_POSTGRES_DSN")]
    postgres_dsn: String,
    #[arg(long, env = "BITMAGNET_POSTGRES_MAX_CONNECTIONS", default_value_t = 3)]
    postgres_max_connections: u32,
    #[arg(
        long,
        env = "BITMAGNET_POSTGRES_ACQUIRE_TIMEOUT_SECONDS",
        default_value_t = 5
    )]
    postgres_acquire_timeout_seconds: u64,
    /// Digest logged by the live Go processor (`classifier runner initialized`).
    #[arg(
        long,
        env = "BITMAGNET_PROCESS_TORRENT_WORKER_EXPECTED_CLASSIFIER_CONFIG_DIGEST"
    )]
    expected_classifier_config_digest: Option<String>,
    #[arg(long, default_value_t = 30)]
    check_interval_seconds: u64,
    #[arg(long, default_value_t = 600)]
    job_timeout_seconds: u64,
    #[arg(long, default_value_t = 600)]
    abstain_cooloff_seconds: u64,
    /// How long an in-flight job may run after SIGTERM before it is abandoned
    /// (0 abandons it at once). Keep it, plus the blocking-manager flush,
    /// under the pod's `terminationGracePeriodSeconds`.
    #[arg(long, default_value_t = 20)]
    shutdown_grace_seconds: u64,
    /// Threads that classify one job's torrents. Jobs stay serial; only the
    /// CPU-bound classification within a job is spread. Sized explicitly
    /// because `available_parallelism` sees the node's cores, not the pod's
    /// CPU request, and the worker shares its node with PostgreSQL.
    #[arg(
        long,
        env = "BITMAGNET_PROCESS_TORRENT_WORKER_CLASSIFY_PARALLELISM",
        default_value_t = DEFAULT_CLASSIFY_PARALLELISM
    )]
    classify_parallelism: usize,
    /// Also claim explicit flags-on payloads (all three attach flags true),
    /// classifying them through local search and TMDB. Requires Goose head 36,
    /// a TMDB API key, and three PostgreSQL connections.
    #[arg(long, env = "BITMAGNET_PROCESS_TORRENT_WORKER_ADMIT_FLAGS_ON")]
    admit_flags_on: bool,
    /// TMDB API key; required with `--admit-flags-on` and refused without it.
    /// There is no fallback key: Go's default key is not used.
    #[arg(long, env = "BITMAGNET_TMDB_API_KEY")]
    tmdb_api_key: Option<TmdbApiKey>,
    /// TMDB API base URL (Go's `tmdb.base_url`); used only with
    /// `--admit-flags-on`.
    #[arg(long, env = "BITMAGNET_TMDB_BASE_URL", default_value = DEFAULT_TMDB_BASE_URL)]
    tmdb_base_url: String,
    /// Claim organic payloads (workflow empty or `default`, attach flags
    /// omitted or booleans) up to `--organic-max-priority`, instead of only
    /// the explicit flags-on ones. Requires `--admit-flags-on`.
    #[arg(long, env = "BITMAGNET_PROCESS_TORRENT_WORKER_ADMIT_ORGANIC")]
    admit_organic: bool,
    /// Highest job priority claimed with `--admit-organic` (default 0: DHT
    /// and retry jobs); refused without it. An explicit flags-on payload
    /// above it stays with Go.
    #[arg(
        long,
        env = "BITMAGNET_PROCESS_TORRENT_WORKER_ORGANIC_MAX_PRIORITY",
        allow_negative_numbers = true
    )]
    organic_max_priority: Option<i32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    bitmagnet_common::init_tracing();
    let args = Args::parse();
    validate_args(&args)?;
    let expected = args
        .expected_classifier_config_digest
        .as_deref()
        .map(str::trim)
        .unwrap_or_default();
    let actual = core_config_digest().context("digesting embedded classifier configuration")?;
    anyhow::ensure!(
        expected == actual,
        "classifier configuration mismatch: expected live Go digest {expected}, Rust embedded digest {actual}"
    );

    let pool = PgPoolOptions::new()
        .max_connections(args.postgres_max_connections)
        .acquire_timeout(Duration::from_secs(args.postgres_acquire_timeout_seconds))
        .connect(&args.postgres_dsn)
        .await
        .context("connecting process_torrent worker to PostgreSQL")?;
    let head = bitmagnet_db::read_goose_applied_head(&pool)
        .await
        .context("reading Goose migration head")?
        .context("database has no applied Goose migration")?;
    let min_goose_version = if args.admit_flags_on {
        MIN_FLAGS_ON_GOOSE_VERSION
    } else {
        MIN_GOOSE_VERSION
    };
    anyhow::ensure!(
        head.version >= min_goose_version,
        "Goose migration head is {}; the process_torrent worker requires at least {min_goose_version}{}",
        head.version,
        if args.admit_flags_on { " with --admit-flags-on" } else { "" }
    );

    let metrics = register_worker_metrics(bitmagnet_common::metrics::registry())
        .context("registering worker metrics")?;
    let blocking_failures =
        register_blocking_failure_metrics(bitmagnet_common::metrics::registry())
            .context("registering blocking-failure metrics")?;
    let _metrics_server = bitmagnet_common::metrics::maybe_spawn_metrics_server()
        .await
        .context("starting process_torrent worker metrics listener")?;

    let blocking_manager = PersistentBlockingManager::new(pool.clone());
    let config = ProcessTorrentWorkerConfig {
        check_interval: Duration::from_secs(args.check_interval_seconds),
        job_timeout: Duration::from_secs(args.job_timeout_seconds),
        abstain_cooloff: Duration::from_secs(args.abstain_cooloff_seconds),
        shutdown_grace: Duration::from_secs(args.shutdown_grace_seconds),
        organic_max_priority: organic_max_priority(&args),
        ..ProcessTorrentWorkerConfig::default()
    };
    let worker = ProcessTorrentWorker::new(
        QueueStore::new(pool.clone()),
        materializer(&args, &pool)?
            .with_classify_parallelism(classify_parallelism(&args)?)
            .context("starting the classify thread pool")?,
        Arc::new(blocking_manager.clone()),
        config,
    )
    .with_metrics(metrics)
    .with_blocking_failure_metrics(blocking_failures)
    .with_timed_blocking_flush(
        Arc::new(blocking_manager.clone()),
        blocking_manager.max_flush_wait(),
    );

    tracing::info!(
        queue = PROCESS_TORRENT,
        admission = ?worker.admission(),
        tmdb_base_url = args.admit_flags_on.then_some(args.tmdb_base_url.as_str()),
        goose_version = head.version,
        classifier_config_digest = %actual,
        postgres_max_connections = args.postgres_max_connections,
        classify_parallelism = args.classify_parallelism,
        check_interval_seconds = args.check_interval_seconds,
        job_timeout_seconds = args.job_timeout_seconds,
        shutdown_grace_seconds = args.shutdown_grace_seconds,
        blocking_flush_interval_seconds = blocking_manager.max_flush_wait().as_secs(),
        "starting persisting Rust process_torrent worker"
    );
    // The flush runs before the pool closes.
    worker
        .run_then_flush(
            bitmagnet_common::serve::shutdown_signal(),
            &blocking_manager,
        )
        .await
        .context("flushing blocked info hashes on shutdown")?;
    Ok(())
}

/// The core classifier, over a live local-search + TMDB resolver with
/// `--admit-flags-on`.
fn materializer(args: &Args, pool: &PgPool) -> Result<Materializer> {
    if !args.admit_flags_on {
        return Materializer::from_core().context("compiling embedded classifier");
    }
    let api_key = args
        .tmdb_api_key
        .as_ref()
        .context("--admit-flags-on requires a TMDB API key")?;
    // Go validates the key lazily, on the first TMDB call, and so does the
    // client: a process that never reaches TMDB never calls it.
    let tmdb = TmdbClient::new(TmdbConfig {
        base_url: args.tmdb_base_url.clone(),
        ..TmdbConfig::new(api_key.0.as_str())
    })
    .context("building the TMDB client")?;
    let resolver = LiveContentResolver::new(
        Arc::new(PgContentSearch::with_associations(pool.clone())),
        Arc::new(tmdb),
    );
    Materializer::from_core_with_resolver(Arc::new(resolver), tokio::runtime::Handle::current())
        .context("compiling embedded classifier")
}

fn validate_args(args: &Args) -> Result<()> {
    anyhow::ensure!(
        args.postgres_max_connections >= 2,
        "process_torrent worker requires at least two PostgreSQL connections: one held by the claimed queue row and one for loading and persisting"
    );
    if args.admit_flags_on {
        // Within one job the stages are sequential, so two suffice. But a
        // composition abandoned at its job timeout keeps running read-only on
        // the blocking pool, and its local search (one query at a time per
        // process) can hold a connection while the next job claims and loads.
        anyhow::ensure!(
            args.postgres_max_connections >= 3,
            "process_torrent worker requires at least three PostgreSQL connections with --admit-flags-on: the claimed queue row, loading and persisting, and local content search"
        );
        anyhow::ensure!(
            args.tmdb_api_key
                .as_ref()
                .is_some_and(|key| !key.0.trim().is_empty()),
            "process_torrent worker requires --tmdb-api-key (or BITMAGNET_TMDB_API_KEY) with --admit-flags-on"
        );
        anyhow::ensure!(
            args.tmdb_base_url.starts_with("https://") || args.tmdb_base_url.starts_with("http://"),
            "tmdb-base-url must be an http(s) URL"
        );
    } else {
        anyhow::ensure!(
            args.tmdb_api_key
                .as_ref()
                .is_none_or(|key| key.0.trim().is_empty()),
            "--tmdb-api-key (or BITMAGNET_TMDB_API_KEY) is refused without --admit-flags-on"
        );
    }
    if args.admit_organic {
        anyhow::ensure!(
            args.admit_flags_on,
            "--admit-organic (or BITMAGNET_PROCESS_TORRENT_WORKER_ADMIT_ORGANIC) requires --admit-flags-on"
        );
        anyhow::ensure!(
            args.organic_max_priority
                .is_none_or(|priority| priority >= 0),
            "organic-max-priority must not be negative"
        );
    } else {
        anyhow::ensure!(
            args.organic_max_priority.is_none(),
            "--organic-max-priority (or BITMAGNET_PROCESS_TORRENT_WORKER_ORGANIC_MAX_PRIORITY) is refused without --admit-organic"
        );
    }
    anyhow::ensure!(
        args.expected_classifier_config_digest
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty()),
        "process_torrent worker requires --expected-classifier-config-digest (or BITMAGNET_PROCESS_TORRENT_WORKER_EXPECTED_CLASSIFIER_CONFIG_DIGEST)"
    );
    classify_parallelism(args)?;
    for (name, value) in [
        (
            "postgres-acquire-timeout-seconds",
            args.postgres_acquire_timeout_seconds,
        ),
        ("check-interval-seconds", args.check_interval_seconds),
        ("job-timeout-seconds", args.job_timeout_seconds),
    ] {
        anyhow::ensure!(value > 0, "{name} must be positive");
    }
    Ok(())
}

/// The organic priority bound, 0 unless given, with `--admit-organic`.
fn organic_max_priority(args: &Args) -> Option<i32> {
    args.admit_organic
        .then(|| args.organic_max_priority.unwrap_or(0))
}

fn classify_parallelism(args: &Args) -> Result<NonZeroUsize> {
    NonZeroUsize::new(args.classify_parallelism).context("classify-parallelism must be positive")
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{organic_max_priority, validate_args, Args, TmdbApiKey, DEFAULT_TMDB_BASE_URL};

    fn args() -> Args {
        Args {
            postgres_dsn: "postgres://example.invalid/bitmagnet".to_owned(),
            postgres_max_connections: 3,
            postgres_acquire_timeout_seconds: 5,
            expected_classifier_config_digest: Some("sha256:test".to_owned()),
            check_interval_seconds: 30,
            job_timeout_seconds: 600,
            abstain_cooloff_seconds: 600,
            shutdown_grace_seconds: 20,
            classify_parallelism: 4,
            admit_flags_on: false,
            tmdb_api_key: None,
            tmdb_base_url: DEFAULT_TMDB_BASE_URL.to_owned(),
            admit_organic: false,
            organic_max_priority: None,
        }
    }

    fn flags_on() -> Args {
        Args {
            admit_flags_on: true,
            tmdb_api_key: Some(TmdbApiKey("dummy-key".to_owned())),
            ..args()
        }
    }

    #[test]
    fn runtime_contract_requires_two_connections_a_digest_and_positive_durations() {
        assert!(validate_args(&args()).is_ok());
        let mut serial = args();
        serial.classify_parallelism = 1;
        assert!(validate_args(&serial).is_ok());

        let mut invalid = args();
        invalid.postgres_max_connections = 1;
        assert!(validate_args(&invalid)
            .expect_err("one connection would deadlock the claimed queue row")
            .to_string()
            .contains("at least two"));
        let mut two = args();
        two.postgres_max_connections = 2;
        assert!(validate_args(&two).is_ok());

        for mutate in [
            |args: &mut Args| args.expected_classifier_config_digest = None,
            |args: &mut Args| args.expected_classifier_config_digest = Some("  ".to_owned()),
            |args: &mut Args| args.postgres_acquire_timeout_seconds = 0,
            |args: &mut Args| args.check_interval_seconds = 0,
            |args: &mut Args| args.job_timeout_seconds = 0,
            |args: &mut Args| args.classify_parallelism = 0,
        ] {
            let mut invalid = args();
            mutate(&mut invalid);
            assert!(validate_args(&invalid).is_err());
        }
    }

    #[test]
    fn the_tmdb_key_is_refused_without_the_switch_and_required_with_it() {
        let mut keyed = args();
        keyed.tmdb_api_key = Some(TmdbApiKey("dummy-key".to_owned()));
        assert!(validate_args(&keyed)
            .expect_err("a key without the switch is a misconfiguration")
            .to_string()
            .contains("refused without --admit-flags-on"));
        // An empty variable is as good as unset, like the digest's.
        keyed.tmdb_api_key = Some(TmdbApiKey(String::new()));
        assert!(validate_args(&keyed).is_ok());
        // The base URL has a default and is unused without the switch.
        keyed.tmdb_base_url = "not a url".to_owned();
        assert!(validate_args(&keyed).is_ok());

        assert!(validate_args(&flags_on()).is_ok());
        type Mutation = fn(&mut Args);
        let cases: [(Mutation, &str); 5] = [
            (
                |args: &mut Args| args.tmdb_api_key = None,
                "requires --tmdb-api-key",
            ),
            (
                |args: &mut Args| args.tmdb_api_key = Some(TmdbApiKey(" ".to_owned())),
                "requires --tmdb-api-key",
            ),
            (
                |args: &mut Args| args.postgres_max_connections = 2,
                "at least three",
            ),
            (
                |args: &mut Args| "api.themoviedb.org/3".clone_into(&mut args.tmdb_base_url),
                "http(s) URL",
            ),
            (
                |args: &mut Args| args.expected_classifier_config_digest = None,
                "expected-classifier-config-digest",
            ),
        ];
        for (mutate, expected) in cases {
            let mut invalid = flags_on();
            mutate(&mut invalid);
            let error = validate_args(&invalid).expect_err(expected).to_string();
            assert!(error.contains(expected), "{error}");
        }
        let mut local = flags_on();
        "http://127.0.0.1:8080/3".clone_into(&mut local.tmdb_base_url);
        assert!(validate_args(&local).is_ok());
    }

    #[test]
    fn organic_admission_requires_flags_on_and_a_non_negative_bound() {
        let organic = Args {
            admit_organic: true,
            ..flags_on()
        };
        assert!(validate_args(&organic).is_ok());
        assert_eq!(organic_max_priority(&organic), Some(0));
        for priority in [0, 4, 20] {
            let bounded = Args {
                organic_max_priority: Some(priority),
                ..Args {
                    admit_organic: true,
                    ..flags_on()
                }
            };
            assert!(validate_args(&bounded).is_ok());
            assert_eq!(organic_max_priority(&bounded), Some(priority));
        }
        assert_eq!(organic_max_priority(&flags_on()), None);

        type Mutation = fn(&mut Args);
        let cases: [(Mutation, &str); 4] = [
            (
                |args: &mut Args| {
                    args.admit_flags_on = false;
                    args.tmdb_api_key = None;
                },
                "requires --admit-flags-on",
            ),
            (
                |args: &mut Args| args.organic_max_priority = Some(-1),
                "must not be negative",
            ),
            (
                |args: &mut Args| {
                    args.admit_organic = false;
                    args.organic_max_priority = Some(0);
                },
                "refused without --admit-organic",
            ),
            (
                |args: &mut Args| {
                    args.admit_organic = false;
                    args.admit_flags_on = false;
                    args.tmdb_api_key = None;
                    args.organic_max_priority = Some(10);
                },
                "refused without --admit-organic",
            ),
        ];
        for (mutate, expected) in cases {
            let mut invalid = Args {
                admit_organic: true,
                ..flags_on()
            };
            mutate(&mut invalid);
            let error = validate_args(&invalid).expect_err(expected).to_string();
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn the_switch_is_a_default_off_flag_and_the_key_never_prints() {
        let base = [
            "worker",
            "--postgres-dsn",
            "postgres://example.invalid/bitmagnet",
        ];
        let parsed = Args::try_parse_from(base).expect("parse defaults");
        assert!(!parsed.admit_flags_on);
        assert!(parsed.tmdb_api_key.is_none());
        assert_eq!(parsed.tmdb_base_url, "https://api.themoviedb.org/3");
        assert!(!parsed.admit_organic);
        assert_eq!(parsed.organic_max_priority, None);

        // A negative bound parses, so validation can refuse it by name.
        let parsed = Args::try_parse_from(base.into_iter().chain([
            "--admit-flags-on",
            "--admit-organic",
            "--organic-max-priority",
            "-1",
        ]))
        .expect("parse a negative bound");
        assert!(parsed.admit_organic);
        assert_eq!(parsed.organic_max_priority, Some(-1));

        let parsed = Args::try_parse_from(base.into_iter().chain([
            "--admit-flags-on",
            "--tmdb-api-key",
            "secret-key-value",
        ]))
        .expect("parse the switch");
        assert!(parsed.admit_flags_on);
        let printed = format!("{parsed:?}");
        assert!(!printed.contains("secret-key-value"), "{printed}");
        assert!(printed.contains("<redacted>"), "{printed}");
    }
}
