//! Entry point for the PostgreSQL-backed GraphQL HTTP server.

use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, Response};
use axum::routing::get;
use axum::{Json, Router};
use bitmagnet_db::PgPool;
use clap::{Args as ClapArgs, Parser};
use serde::Serialize;
use tracing::{info, warn};

const GRAPHQL_HANDLER_DURATION_HEADER: &str = "x-bitmagnet-graphql-handler-duration-us";

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
    #[arg(
        long,
        env = "BITMAGNET_GRAPHQL_EXPECTED_GOOSE_VERSION",
        allow_hyphen_values = true,
        value_parser = parse_positive_i64
    )]
    expected_goose_version: i64,

    /// Comma-delimited peer GraphQL endpoints used for federated health.
    #[arg(long, env = "HEALTH_PEER_GRAPHQL_URLS", value_delimiter = ',')]
    health_peer_graphql_urls: Vec<String>,

    /// Per-peer health request timeout (Go duration syntax, for example 1500ms).
    #[arg(
        long,
        env = "HEALTH_PEER_TIMEOUT",
        default_value = "1500ms",
        value_parser = parse_go_duration
    )]
    health_peer_timeout: Duration,

    #[command(flatten)]
    search: SearchArgs,

    #[command(flatten)]
    mutations: MutationArgs,

    #[command(flatten)]
    queue_mutations: QueueMutationArgs,
}

/// Explicitly gated writer configuration for the torrent-tag mutation family.
#[derive(Debug, ClapArgs)]
struct MutationArgs {
    /// Enable the served torrent-tag mutation runtime.
    #[arg(
        long,
        env = "BITMAGNET_GRAPHQL_MUTATIONS_ENABLED",
        default_value_t = false,
        value_parser = parse_go_bool
    )]
    mutations_enabled: bool,

    /// Separate PostgreSQL writer DSN, required exactly when mutations are enabled.
    #[arg(long, env = "BITMAGNET_GRAPHQL_MUTATION_POSTGRES_DSN")]
    mutation_postgres_dsn: Option<SecretDsn>,

    /// Maximum connections in the separately authenticated writer pool.
    #[arg(
        long,
        env = "BITMAGNET_GRAPHQL_MUTATION_POSTGRES_MAX_CONNECTIONS",
        default_value_t = 4,
        value_parser = parse_positive_u32
    )]
    mutation_postgres_max_connections: u32,
}

/// Explicitly gated writer configuration for the queue mutation family.
#[derive(Debug, ClapArgs)]
struct QueueMutationArgs {
    /// Enable the served queue mutation runtime.
    #[arg(
        long,
        env = "BITMAGNET_GRAPHQL_QUEUE_MUTATIONS_ENABLED",
        default_value_t = false,
        value_parser = parse_go_bool
    )]
    queue_mutations_enabled: bool,

    /// Separate PostgreSQL queue-writer DSN, required exactly when enabled.
    #[arg(long, env = "BITMAGNET_GRAPHQL_QUEUE_MUTATION_POSTGRES_DSN")]
    queue_mutation_postgres_dsn: Option<SecretDsn>,

    /// Maximum connections in the separately authenticated queue-writer pool.
    #[arg(
        long,
        env = "BITMAGNET_GRAPHQL_QUEUE_MUTATION_POSTGRES_MAX_CONNECTIONS",
        default_value_t = 2,
        value_parser = parse_positive_u32
    )]
    queue_mutation_postgres_max_connections: u32,
}

#[derive(Clone)]
struct SecretDsn(String);

impl SecretDsn {
    fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretDsn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl FromStr for SecretDsn {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err("mutation PostgreSQL DSN must not be empty".to_owned());
        }
        Ok(Self(value.to_owned()))
    }
}

/// Go-compatible search backend and feature configuration.
#[derive(Debug, ClapArgs)]
struct SearchArgs {
    /// Enable the L2 file-search backend.
    #[arg(
        long,
        env = "SEARCH_FILE_SEARCH_ENABLED",
        default_value_t = false,
        value_parser = parse_go_bool
    )]
    file_search_enabled: bool,

    /// L2 file-search gRPC endpoint.
    #[arg(
        long,
        env = "SEARCH_FILE_SEARCH_ADDRESS",
        default_value = "bitmagnet-filesearch.bitmagnet.svc:50052"
    )]
    file_search_address: String,

    /// Hard deadline for each L2 file-search RPC.
    #[arg(
        long,
        env = "SEARCH_FILE_SEARCH_TIMEOUT",
        default_value = "5s",
        value_parser = parse_go_duration
    )]
    file_search_timeout: Duration,

    /// Maximum cursorless L2 result window.
    #[arg(long, env = "SEARCH_FILE_SEARCH_MAX_ROWS", default_value_t = 500)]
    file_search_max_rows: u32,

    /// Route eligible file-search text through L3 exact refine.
    #[arg(
        long,
        env = "SEARCH_FILE_SEARCH_ROUTE_TEXT",
        default_value_t = true,
        value_parser = parse_go_bool
    )]
    file_search_route_text: bool,

    /// Enable the L3 pathsearch plus L1 exact-refine composer.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_ENABLED",
        default_value_t = false,
        value_parser = parse_go_bool
    )]
    pathsearch_enabled: bool,

    /// Enable candidate-derived path typeahead.
    #[arg(
        long,
        env = "SEARCH_PATH_TYPEAHEAD_ENABLED",
        default_value_t = false,
        value_parser = parse_go_bool
    )]
    path_typeahead_enabled: bool,

    /// Enable `collapse:path` through the L3 composer.
    #[arg(
        long,
        env = "SEARCH_PATH_COLLAPSE_ENABLED",
        default_value_t = false,
        value_parser = parse_go_bool
    )]
    path_collapse_enabled: bool,

    /// L3 pathsearch gRPC address.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_ADDRESS",
        default_value = "bitmagnet-pathsearch.bitmagnet.svc:50053"
    )]
    pathsearch_address: String,

    /// Per-RPC pathsearch deadline.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_TIMEOUT",
        default_value = "5s",
        value_parser = parse_go_duration
    )]
    pathsearch_timeout: Duration,

    /// Minimum broad-gram query length.
    #[arg(long, env = "SEARCH_PATHSEARCH_MIN_QUERY_LENGTH", default_value_t = 3)]
    pathsearch_min_query_length: i32,

    /// Candidate-page oversampling factor.
    #[arg(long, env = "SEARCH_PATHSEARCH_OVERSAMPLE", default_value_t = 4)]
    pathsearch_oversample: u32,

    /// Absolute candidate memory bound.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_MAX_CANDIDATES",
        default_value_t = 2_000
    )]
    pathsearch_max_candidates: u32,

    /// Per-request candidate decode bound.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_MAX_DECODE_CANDIDATES",
        default_value_t = 200
    )]
    pathsearch_max_decode_candidates: u32,

    /// Background L3 health-check cadence.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_HEALTH_INTERVAL",
        default_value = "15s",
        value_parser = parse_go_duration
    )]
    pathsearch_health_interval: Duration,

    /// Maximum accepted L3 watermark lag; zero disables the lag gate.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_MAX_WATERMARK_LAG",
        default_value = "0",
        value_parser = parse_go_duration
    )]
    pathsearch_max_watermark_lag: Duration,

    /// Per-torrent file-count refine cap.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_MAX_REFINE_FILES",
        default_value_t = 300_000
    )]
    pathsearch_max_refine_files: u32,

    /// Cumulative decoded-file budget per refine chunk.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_REFINE_FILE_BUDGET",
        default_value_t = 300_000
    )]
    pathsearch_refine_file_budget: u32,

    /// Maximum torrents in one refine chunk.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_MAX_CHUNK_TORRENTS",
        default_value_t = 1_024
    )]
    pathsearch_max_chunk_torrents: u32,

    /// Cumulative retained decoded-file budget per request.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_RETAINED_FILE_BUDGET",
        default_value_t = 1_000_000
    )]
    pathsearch_retained_file_budget: u32,

    /// Maximum compressed input and decompressed output bytes for one torrent blob.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_MAX_REFINE_DECOMPRESSED_BYTES",
        default_value_t = 67_108_864
    )]
    pathsearch_max_refine_decompressed_bytes: u64,

    /// Cumulative MessagePack plus owned path/extension bytes per refine chunk.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_REFINE_DECODED_BYTE_BUDGET",
        default_value_t = 134_217_728
    )]
    pathsearch_refine_decoded_byte_budget: u64,

    /// Cumulative retained path/extension bytes per request.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_RETAINED_BYTE_BUDGET",
        default_value_t = 67_108_864
    )]
    pathsearch_retained_byte_budget: u64,

    /// End-to-end L3 candidate plus exact-refine deadline.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_ROUTE_TIMEOUT",
        default_value = "8s",
        value_parser = parse_go_duration
    )]
    pathsearch_route_timeout: Duration,

    /// Maximum concurrent blob-decode refines; zero uses CPU count.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_MAX_CONCURRENT_REFINES",
        default_value_t = 0
    )]
    pathsearch_max_concurrent_refines: i32,

    /// Time a blob-decode refine waits for a concurrency slot.
    #[arg(
        long,
        env = "SEARCH_PATHSEARCH_SLOT_WAIT",
        default_value = "0",
        value_parser = parse_go_duration
    )]
    pathsearch_slot_wait: Duration,

    /// Enable the drop-compatible read path; this implies JSONB file-extension
    /// predicates exactly as it does in Go.
    #[arg(
        long,
        env = "SEARCH_FEATURES_DROP_COMPATIBLE_READS",
        default_value_t = false,
        value_parser = parse_go_bool
    )]
    features_drop_compatible_reads: bool,

    /// Enable Lane-S's JSONB file-extension predicate.
    #[arg(
        long,
        env = "SEARCH_FEATURES_GATE_FILE_EXTENSIONS_JSONB",
        default_value_t = false,
        value_parser = parse_go_bool
    )]
    features_file_extensions_jsonb: bool,

    /// Rewrite lone relevance ordering to popularity.
    #[arg(
        long,
        env = "SEARCH_FEATURES_POPULARITY_SORT_DEFAULT",
        default_value_t = false,
        value_parser = parse_go_bool
    )]
    features_popularity_sort_default: bool,

    /// Expose file-search GraphQL reads.
    #[arg(
        long,
        env = "SEARCH_FEATURES_FILE_SEARCH_ENABLED",
        default_value_t = false,
        value_parser = parse_go_bool
    )]
    features_file_search_enabled: bool,

    /// Expose file-search facet aggregation.
    #[arg(
        long,
        env = "SEARCH_FEATURES_FILE_SEARCH_FACETS_ENABLED",
        default_value_t = false,
        value_parser = parse_go_bool
    )]
    features_file_search_facets_enabled: bool,

    /// Prefer the L3 Suggest RPC for file-path typeahead.
    #[arg(
        long,
        env = "SEARCH_FEATURES_FILE_SEARCH_TYPEAHEAD_RPC_ENABLED",
        default_value_t = false,
        value_parser = parse_go_bool
    )]
    features_file_search_typeahead_rpc_enabled: bool,
}

fn parse_go_bool(value: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(format!("invalid Go boolean {value:?}")),
    }
}

struct SearchRuntimeLifecycle {
    runtime: Arc<dyn bitmagnet_graphql::SearchRuntime>,
    pathsearch_health_poller: Option<tokio::task::JoinHandle<()>>,
}

impl SearchRuntimeLifecycle {
    async fn shutdown(mut self) {
        if let Some(handle) = self.pathsearch_health_poller.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl Drop for SearchRuntimeLifecycle {
    fn drop(&mut self) {
        if let Some(handle) = &self.pathsearch_health_poller {
            handle.abort();
        }
    }
}

fn build_search_runtime(
    pool: PgPool,
    args: &SearchArgs,
    pathsearch_metrics: Arc<bitmagnet_search_serve::PathsearchMetrics>,
) -> anyhow::Result<SearchRuntimeLifecycle> {
    use bitmagnet_graphql::schema::file_search_client::L2FileSearchBackend;
    use bitmagnet_graphql::schema::lane_s::LaneSSearchBackend;
    use bitmagnet_graphql::schema::search::{SearchBuildConfig, SearchFeatures, SearchRuntime};
    use bitmagnet_search_serve::{CandidateSource, PgSearchBackend, SearchServe};

    let features = SearchFeatures {
        file_search_enabled: args.features_file_search_enabled,
        file_search_facets_enabled: args.features_file_search_facets_enabled,
        file_search_typeahead_rpc_enabled: args.features_file_search_typeahead_rpc_enabled,
    };
    let build_config = SearchBuildConfig {
        file_extensions_jsonb: args.features_drop_compatible_reads
            || args.features_file_extensions_jsonb,
        popularity_sort_default: args.features_popularity_sort_default,
    };

    let lane_s: Arc<dyn LaneSSearchBackend> =
        Arc::new(bitmagnet_graphql::SqlxLaneSSearchBackend::new(pool.clone()));
    let l2: Arc<dyn L2FileSearchBackend> = if args.file_search_enabled {
        let client = bitmagnet_graphql::TonicFileSearchClient::connect(
            bitmagnet_graphql::FileSearchClientConfig::new(
                args.file_search_address.clone(),
                args.file_search_timeout,
            )
            .with_max_rows(args.file_search_max_rows),
        )
        .context("construct L2 filesearch client")?;
        Arc::new(client)
    } else {
        Arc::new(bitmagnet_graphql::DisabledFileSearchBackend)
    };
    let base: Arc<dyn SearchRuntime> = Arc::new(bitmagnet_graphql::PgL2SearchRuntime::new(
        lane_s,
        l2,
        features,
        build_config,
    ));

    let (lane_c, pathsearch_health_poller): (
        Arc<dyn SearchServe>,
        Option<tokio::task::JoinHandle<()>>,
    ) = if args.pathsearch_enabled {
        let composer_config = bitmagnet_search_serve::ComposerConfig {
            min_query_length: u32::try_from(args.pathsearch_min_query_length).unwrap_or(0),
            oversample_factor: args.pathsearch_oversample,
            max_candidates: args.pathsearch_max_candidates,
            max_decode_candidates: args.pathsearch_max_decode_candidates,
            typeahead_enabled: args.path_typeahead_enabled,
            file_search_route_text: args.file_search_route_text,
            collapse_enabled: args.path_collapse_enabled,
            max_refine_files: args.pathsearch_max_refine_files,
            refine_file_budget: args.pathsearch_refine_file_budget,
            max_chunk_torrents: args.pathsearch_max_chunk_torrents,
            retained_file_budget: args.pathsearch_retained_file_budget,
            max_refine_decompressed_bytes: args.pathsearch_max_refine_decompressed_bytes,
            refine_decoded_byte_budget: args.pathsearch_refine_decoded_byte_budget,
            retained_byte_budget: args.pathsearch_retained_byte_budget,
            route_timeout: args.pathsearch_route_timeout,
            max_concurrent_refines: usize::try_from(args.pathsearch_max_concurrent_refines)
                .unwrap_or(0),
            slot_wait: args.pathsearch_slot_wait,
        };
        let normalized = composer_config.normalized();
        anyhow::ensure!(
            normalized.max_decode_candidates <= normalized.max_candidates,
            "pathsearch max_decode_candidates must not exceed max_candidates"
        );
        anyhow::ensure!(
            normalized.max_refine_files <= normalized.refine_file_budget,
            "pathsearch max_refine_files must not exceed refine_file_budget"
        );
        anyhow::ensure!(
            normalized.refine_file_budget <= normalized.retained_file_budget,
            "pathsearch refine_file_budget must not exceed retained_file_budget"
        );
        anyhow::ensure!(
            normalized.max_refine_decompressed_bytes <= normalized.refine_decoded_byte_budget,
            "pathsearch max_refine_decompressed_bytes must not exceed refine_decoded_byte_budget"
        );

        let candidate_client = Arc::new(
            bitmagnet_search_serve::Client::connect(bitmagnet_search_serve::ClientConfig {
                address: args.pathsearch_address.clone(),
                timeout: args.pathsearch_timeout,
            })
            .context("construct L3 pathsearch client")?,
        );
        let health_state = Arc::new(bitmagnet_search_serve::HealthState::new());
        let health_gate = bitmagnet_search_serve::gate(Arc::clone(&health_state));

        let lane_s_build = bitmagnet_search_serve::SearchBuildConfig {
            file_extensions_jsonb: build_config.file_extensions_jsonb,
            popularity_sort_default: build_config.popularity_sort_default,
        };
        let pg: Arc<dyn PgSearchBackend> = Arc::new(
            bitmagnet_search_serve::PgSearch::new(pool, lane_s_build)
                .with_metrics(Arc::clone(&pathsearch_metrics)),
        );
        let candidates: Arc<dyn CandidateSource> = candidate_client.clone();
        let composer = bitmagnet_search_serve::Composer::new(
            candidates,
            pg,
            composer_config,
            Some(health_gate),
        )
        .with_metrics(Arc::clone(&pathsearch_metrics));

        // Go's hostile/non-positive override falls back to the older shared
        // 30-second reporter cadence, even though the authored default is 15s.
        let health_interval = if args.pathsearch_health_interval.is_zero() {
            Duration::from_secs(30)
        } else {
            args.pathsearch_health_interval
        };
        let poller = bitmagnet_search_serve::spawn_health_poller_with_metrics(
            candidate_client,
            health_state,
            bitmagnet_search_serve::HealthConfig {
                interval: health_interval,
                max_watermark_lag: args.pathsearch_max_watermark_lag,
            },
            Some(pathsearch_metrics),
        );
        (Arc::new(composer), Some(poller))
    } else {
        (Arc::new(bitmagnet_search_serve::Disabled), None)
    };

    Ok(SearchRuntimeLifecycle {
        runtime: Arc::new(bitmagnet_graphql::LaneCSearchRuntime::new(base, lane_c)),
        pathsearch_health_poller,
    })
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

    let pool = bitmagnet_db::connect(&bitmagnet_db::DbConfig::from_compatible_env()?).await?;
    let goose_head = bitmagnet_graphql::admit_pg(&pool, args.expected_goose_version).await?;
    info!(
        goose_version = goose_head.version,
        "goose schema version confirmed"
    );
    let tag_mutations =
        build_tag_mutations_runtime(&args.mutations, args.expected_goose_version, &pool).await?;
    let queue_mutations =
        build_queue_mutations_runtime(&args.queue_mutations, args.expected_goose_version, &pool)
            .await?;
    let listener = tokio::net::TcpListener::bind(addr).await?;

    // Register both C6 metric families exactly once, even while the serving
    // routes are disabled, so their zero-valued series are present at startup.
    let pathsearch_metrics = Arc::new(bitmagnet_search_serve::PathsearchMetrics::register());
    let _serve_metrics = bitmagnet_search_serve::ServeMetrics::register();
    let _metrics = bitmagnet_common::metrics::maybe_spawn_metrics_server().await?;
    let search_runtime = build_search_runtime(pool.clone(), &args.search, pathsearch_metrics)?;
    let version =
        std::env::var("BITMAGNET_VERSION").unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_owned());
    let schema = bitmagnet_graphql::build_runtime_search_schema_with_mutations(
        version.clone(),
        pool.clone(),
        bitmagnet_graphql::RuntimeConfig {
            peer_graphql_urls: args.health_peer_graphql_urls,
            peer_timeout: args.health_peer_timeout,
        },
        Arc::clone(&search_runtime.runtime),
        tag_mutations,
        queue_mutations,
    );
    let state = AppState {
        schema,
        pool,
        version,
    };
    let app = app(state);

    info!(%addr, "bitmagnet-graphql starting");
    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(bitmagnet_common::serve::shutdown_signal())
        .await;
    search_runtime.shutdown().await;
    serve_result?;

    info!("bitmagnet-graphql stopped");
    Ok(())
}

async fn build_tag_mutations_runtime(
    args: &MutationArgs,
    expected_goose_version: i64,
    read_pool: &PgPool,
) -> anyhow::Result<bitmagnet_graphql::TorrentTagMutationsRuntimeData> {
    let Some(config) = mutation_db_config(args)? else {
        return Ok(bitmagnet_graphql::TorrentTagMutationsRuntimeData::disabled());
    };
    let pool = bitmagnet_db::connect(&config)
        .await
        .context("connect the separately authenticated GraphQL mutation writer")?;
    let goose_head = bitmagnet_graphql::admit_pg(&pool, expected_goose_version)
        .await
        .context("admit the GraphQL mutation writer against the expected Goose head")?;
    let read_identity = postgres_identity(read_pool)
        .await
        .context("read the GraphQL reader PostgreSQL identity")?;
    let writer_identity = postgres_identity(&pool)
        .await
        .context("read the GraphQL mutation writer PostgreSQL identity")?;
    anyhow::ensure!(
        writer_identity.0 == read_identity.0 && writer_identity.1 == read_identity.1,
        "GraphQL reader and mutation writer must target the same PostgreSQL system and database"
    );
    anyhow::ensure!(
        writer_identity.2 != read_identity.2,
        "GraphQL reader and mutation writer must use different PostgreSQL roles"
    );
    admit_mutation_writer_authority(&pool, MutationWriterFamily::TorrentTags).await?;
    info!(
        goose_version = goose_head.version,
        "torrent tag mutations enabled with a separate writer pool"
    );
    Ok(bitmagnet_graphql::TorrentTagMutationsRuntimeData::pg(pool))
}

async fn build_queue_mutations_runtime(
    args: &QueueMutationArgs,
    expected_goose_version: i64,
    read_pool: &PgPool,
) -> anyhow::Result<bitmagnet_graphql::QueueMutationsRuntimeData> {
    let Some(config) = queue_mutation_db_config(args)? else {
        return Ok(bitmagnet_graphql::QueueMutationsRuntimeData::disabled());
    };
    let pool = bitmagnet_db::connect(&config)
        .await
        .context("connect the separately authenticated GraphQL queue mutation writer")?;
    let goose_head = bitmagnet_graphql::admit_pg(&pool, expected_goose_version)
        .await
        .context("admit the GraphQL queue mutation writer against the expected Goose head")?;
    let read_identity = postgres_identity(read_pool)
        .await
        .context("read the GraphQL reader PostgreSQL identity")?;
    let writer_identity = postgres_identity(&pool)
        .await
        .context("read the GraphQL queue mutation writer PostgreSQL identity")?;
    anyhow::ensure!(
        writer_identity.0 == read_identity.0 && writer_identity.1 == read_identity.1,
        "GraphQL reader and queue mutation writer must target the same PostgreSQL system and database"
    );
    anyhow::ensure!(
        writer_identity.2 != read_identity.2,
        "GraphQL reader and queue mutation writer must use different PostgreSQL roles"
    );
    admit_mutation_writer_authority(&pool, MutationWriterFamily::Queue).await?;
    info!(
        goose_version = goose_head.version,
        "queue mutations enabled with a separate writer pool"
    );
    Ok(bitmagnet_graphql::QueueMutationsRuntimeData::pg(pool))
}

async fn postgres_identity(pool: &PgPool) -> anyhow::Result<(String, String, String)> {
    Ok(sqlx::query_as::<_, (String, String, String)>(
        "SELECT current_database()::text, system_identifier::text, current_user::text \
         FROM pg_control_system()",
    )
    .fetch_one(pool)
    .await?)
}

#[derive(Clone, Copy)]
enum MutationWriterFamily {
    TorrentTags,
    Queue,
}

async fn admit_mutation_writer_authority(
    pool: &PgPool,
    family: MutationWriterFamily,
) -> anyhow::Result<()> {
    let attributes = sqlx::query_as::<_, (bool, bool, bool, bool, bool, bool, bool)>(
        "SELECT rolcanlogin, rolinherit, rolsuper, rolcreatedb, rolcreaterole, \
         rolreplication, rolbypassrls FROM pg_roles WHERE rolname = current_user",
    )
    .fetch_one(pool)
    .await
    .context("read GraphQL mutation writer role attributes")?;
    anyhow::ensure!(
        attributes == (true, false, false, false, false, false, false),
        "GraphQL mutation writer must be LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
    );

    let creation = sqlx::query_as::<_, (bool, bool)>(
        "SELECT has_database_privilege(current_user, current_database(), 'CREATE'), \
                has_schema_privilege(current_user, 'public', 'CREATE')",
    )
    .fetch_one(pool)
    .await
    .context("read GraphQL mutation writer creation authority")?;
    anyhow::ensure!(
        creation == (false, false),
        "GraphQL mutation writer must not hold database or public-schema CREATE"
    );

    let grants = sqlx::query_as::<_, (String, String, String)>(
        "SELECT table_schema, table_name, privilege_type \
         FROM information_schema.role_table_grants \
         WHERE grantee = current_user \
           AND table_schema NOT IN ('pg_catalog', 'information_schema') \
         ORDER BY table_schema, table_name, privilege_type",
    )
    .fetch_all(pool)
    .await
    .context("read GraphQL mutation writer table grants")?;
    let expected = match family {
        MutationWriterFamily::TorrentTags => vec![
            (
                "public".to_owned(),
                "goose_db_version".to_owned(),
                "SELECT".to_owned(),
            ),
            (
                "public".to_owned(),
                "torrent_tags".to_owned(),
                "DELETE".to_owned(),
            ),
            (
                "public".to_owned(),
                "torrent_tags".to_owned(),
                "INSERT".to_owned(),
            ),
            (
                "public".to_owned(),
                "torrent_tags".to_owned(),
                "SELECT".to_owned(),
            ),
        ],
        MutationWriterFamily::Queue => vec![
            (
                "public".to_owned(),
                "goose_db_version".to_owned(),
                "SELECT".to_owned(),
            ),
            (
                "public".to_owned(),
                "queue_jobs".to_owned(),
                "DELETE".to_owned(),
            ),
            (
                "public".to_owned(),
                "queue_jobs".to_owned(),
                "INSERT".to_owned(),
            ),
            (
                "public".to_owned(),
                "queue_jobs".to_owned(),
                "TRUNCATE".to_owned(),
            ),
        ],
    };
    anyhow::ensure!(
        grants == expected,
        "GraphQL mutation writer does not have the exact family-specific table grants"
    );

    if matches!(family, MutationWriterFamily::Queue) {
        let selected_columns = sqlx::query_scalar::<_, String>(
            "SELECT column_name FROM information_schema.column_privileges \
             WHERE grantee = current_user AND table_schema = 'public' \
               AND table_name = 'queue_jobs' AND privilege_type = 'SELECT' \
             ORDER BY column_name",
        )
        .fetch_all(pool)
        .await
        .context("read GraphQL queue mutation writer column grants")?;
        anyhow::ensure!(
            selected_columns == ["queue".to_owned(), "status".to_owned()],
            "GraphQL queue mutation writer must SELECT only queue_jobs.queue and queue_jobs.status"
        );
    }

    Ok(())
}

fn mutation_db_config(args: &MutationArgs) -> anyhow::Result<Option<bitmagnet_db::DbConfig>> {
    match (args.mutations_enabled, args.mutation_postgres_dsn.as_ref()) {
        (false, None) => Ok(None),
        (false, Some(_)) => anyhow::bail!(
            "BITMAGNET_GRAPHQL_MUTATION_POSTGRES_DSN is forbidden while mutations are disabled"
        ),
        (true, None) => anyhow::bail!(
            "BITMAGNET_GRAPHQL_MUTATION_POSTGRES_DSN is required while mutations are enabled"
        ),
        (true, Some(dsn)) => Ok(Some(bitmagnet_db::DbConfig {
            dsn: dsn.expose().to_owned(),
            max_connections: args.mutation_postgres_max_connections,
            ..bitmagnet_db::DbConfig::default()
        })),
    }
}

fn queue_mutation_db_config(
    args: &QueueMutationArgs,
) -> anyhow::Result<Option<bitmagnet_db::DbConfig>> {
    match (
        args.queue_mutations_enabled,
        args.queue_mutation_postgres_dsn.as_ref(),
    ) {
        (false, None) => Ok(None),
        (false, Some(_)) => anyhow::bail!(
            "BITMAGNET_GRAPHQL_QUEUE_MUTATION_POSTGRES_DSN is forbidden while queue mutations are disabled"
        ),
        (true, None) => anyhow::bail!(
            "BITMAGNET_GRAPHQL_QUEUE_MUTATION_POSTGRES_DSN is required while queue mutations are enabled"
        ),
        (true, Some(dsn)) => Ok(Some(bitmagnet_db::DbConfig {
            dsn: dsn.expose().to_owned(),
            max_connections: args.queue_mutation_postgres_max_connections,
            ..bitmagnet_db::DbConfig::default()
        })),
    }
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/", get(playground))
        .route(
            "/graphql",
            get(graphql_handler)
                .post(graphql_handler)
                .layer(middleware::from_fn(graphql_handler_duration)),
        )
        .route("/livez", get(livez))
        .route("/status", get(status))
        .with_state(state)
}

async fn graphql_handler(
    State(state): State<AppState>,
    request: GraphQLRequest,
) -> GraphQLResponse {
    state.schema.execute(request.into_inner()).await.into()
}

/// Measures the complete Axum `/graphql` route-service boundary: request body
/// extraction, GraphQL parse/validation/execution, and eager response JSON
/// serialization all run inside `next`. Router matching and transport writes
/// happen outside this boundary.
async fn graphql_handler_duration(request: Request, next: Next) -> Response {
    let started = Instant::now();
    let mut response = next.run(request).await;
    let elapsed_us = started.elapsed().as_micros().max(1);

    if let Ok(value) = HeaderValue::from_str(&elapsed_us.to_string()) {
        response.headers_mut().insert(
            HeaderName::from_static(GRAPHQL_HANDLER_DURATION_HEADER),
            value,
        );
    }

    response
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

fn parse_positive_i64(value: &str) -> Result<i64, String> {
    let parsed = value
        .parse::<i64>()
        .map_err(|error| format!("invalid positive integer {value:?}: {error}"))?;
    if parsed <= 0 {
        return Err(format!("value must be positive, got {parsed}"));
    }
    Ok(parsed)
}

fn parse_positive_u32(value: &str) -> Result<u32, String> {
    let parsed = value
        .parse::<u32>()
        .map_err(|error| format!("invalid positive integer {value:?}: {error}"))?;
    if parsed == 0 {
        return Err("value must be greater than zero".to_owned());
    }
    Ok(parsed)
}

fn parse_go_duration(value: &str) -> Result<Duration, String> {
    let value = value.trim();
    let (negative, mut remaining) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if remaining == "0" {
        return Ok(Duration::ZERO);
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
            .ok_or_else(|| format!("invalid duration {value:?}"))?;
        let fraction_nanos = fraction.bytes().rev().fold(0_u128, |acc, digit| {
            (scale * u128::from(digit - b'0') + acc) / 10
        });
        let part_nanos = whole_nanos
            .checked_add(fraction_nanos)
            .ok_or_else(|| format!("invalid duration {value:?}"))?;
        nanos = nanos
            .checked_add(part_nanos)
            .ok_or_else(|| format!("invalid duration {value:?}"))?;
        parsed_parts += 1;
    }

    let max_nanos = if negative {
        1_u128 << 63
    } else {
        (1_u128 << 63) - 1
    };
    if parsed_parts == 0 || nanos > max_nanos {
        return Err(format!("invalid duration {value:?}"));
    }

    // Go accepts negative durations, while Duration cannot represent them.
    // Preserve the shared `<= 0` branch as zero; each consumer then applies
    // its own Go-compatible fallback or caller-owned-deadline behavior.
    if negative {
        return Ok(Duration::ZERO);
    }

    Ok(Duration::from_nanos(nanos as u64))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::sync::{Arc, Mutex};

    use clap::Parser;
    use serde_json::{json, Value};

    use super::{
        app, build_search_runtime, mutation_db_config, parse_go_duration, queue_mutation_db_config,
        AppState, Args, GRAPHQL_HANDLER_DURATION_HEADER,
    };

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const ARGS_ENV_KEYS: [&str; 42] = [
        "LISTEN_ADDR",
        "BITMAGNET_GRAPHQL_EXPECTED_GOOSE_VERSION",
        "BITMAGNET_GRAPHQL_MUTATIONS_ENABLED",
        "BITMAGNET_GRAPHQL_MUTATION_POSTGRES_DSN",
        "BITMAGNET_GRAPHQL_MUTATION_POSTGRES_MAX_CONNECTIONS",
        "BITMAGNET_GRAPHQL_QUEUE_MUTATIONS_ENABLED",
        "BITMAGNET_GRAPHQL_QUEUE_MUTATION_POSTGRES_DSN",
        "BITMAGNET_GRAPHQL_QUEUE_MUTATION_POSTGRES_MAX_CONNECTIONS",
        "HEALTH_PEER_GRAPHQL_URLS",
        "HEALTH_PEER_TIMEOUT",
        "SEARCH_FILE_SEARCH_ENABLED",
        "SEARCH_FILE_SEARCH_ADDRESS",
        "SEARCH_FILE_SEARCH_TIMEOUT",
        "SEARCH_FILE_SEARCH_MAX_ROWS",
        "SEARCH_FILE_SEARCH_ROUTE_TEXT",
        "SEARCH_PATHSEARCH_ENABLED",
        "SEARCH_PATH_TYPEAHEAD_ENABLED",
        "SEARCH_PATH_COLLAPSE_ENABLED",
        "SEARCH_PATHSEARCH_ADDRESS",
        "SEARCH_PATHSEARCH_TIMEOUT",
        "SEARCH_PATHSEARCH_MIN_QUERY_LENGTH",
        "SEARCH_PATHSEARCH_OVERSAMPLE",
        "SEARCH_PATHSEARCH_MAX_CANDIDATES",
        "SEARCH_PATHSEARCH_MAX_DECODE_CANDIDATES",
        "SEARCH_PATHSEARCH_HEALTH_INTERVAL",
        "SEARCH_PATHSEARCH_MAX_WATERMARK_LAG",
        "SEARCH_PATHSEARCH_MAX_REFINE_FILES",
        "SEARCH_PATHSEARCH_REFINE_FILE_BUDGET",
        "SEARCH_PATHSEARCH_MAX_CHUNK_TORRENTS",
        "SEARCH_PATHSEARCH_RETAINED_FILE_BUDGET",
        "SEARCH_PATHSEARCH_MAX_REFINE_DECOMPRESSED_BYTES",
        "SEARCH_PATHSEARCH_REFINE_DECODED_BYTE_BUDGET",
        "SEARCH_PATHSEARCH_RETAINED_BYTE_BUDGET",
        "SEARCH_PATHSEARCH_ROUTE_TIMEOUT",
        "SEARCH_PATHSEARCH_MAX_CONCURRENT_REFINES",
        "SEARCH_PATHSEARCH_SLOT_WAIT",
        "SEARCH_FEATURES_DROP_COMPATIBLE_READS",
        "SEARCH_FEATURES_GATE_FILE_EXTENSIONS_JSONB",
        "SEARCH_FEATURES_POPULARITY_SORT_DEFAULT",
        "SEARCH_FEATURES_FILE_SEARCH_ENABLED",
        "SEARCH_FEATURES_FILE_SEARCH_FACETS_ENABLED",
        "SEARCH_FEATURES_FILE_SEARCH_TYPEAHEAD_RPC_ENABLED",
    ];

    struct ArgsEnvRestore {
        values: Vec<(&'static str, Option<OsString>)>,
    }

    impl ArgsEnvRestore {
        fn clear() -> Self {
            let values = ARGS_ENV_KEYS
                .into_iter()
                .map(|name| {
                    let value = std::env::var_os(name);
                    std::env::remove_var(name);
                    (name, value)
                })
                .collect();
            Self { values }
        }
    }

    impl Drop for ArgsEnvRestore {
        fn drop(&mut self) {
            for (name, value) in self.values.drain(..) {
                restore_env(name, value);
            }
        }
    }

    fn restore_env(name: &str, value: Option<OsString>) {
        match value {
            Some(value) => std::env::set_var(name, value),
            None => std::env::remove_var(name),
        }
    }

    fn assert_positive_duration_header(response: &reqwest::Response) {
        let raw = response
            .headers()
            .get(GRAPHQL_HANDLER_DURATION_HEADER)
            .expect("GraphQL handler duration header")
            .to_str()
            .expect("ASCII GraphQL handler duration header");
        let micros = raw
            .parse::<u128>()
            .expect("integer GraphQL handler duration header");
        assert!(micros > 0, "GraphQL handler duration must be positive");
    }

    fn default_args() -> Args {
        Args::try_parse_from(["bitmagnet-graphql", "--expected-goose-version", "33"])
            .expect("GraphQL arguments with an explicit Goose version parse")
    }

    #[test]
    fn args_parse_defaults_and_overrides() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let _restore = ArgsEnvRestore::clear();

        let missing =
            Args::try_parse_from(["bitmagnet-graphql"]).expect_err("the Goose version is required");
        assert_eq!(
            missing.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
        for invalid in ["0", "-1"] {
            let error =
                Args::try_parse_from(["bitmagnet-graphql", "--expected-goose-version", invalid])
                    .expect_err("non-positive Goose versions are rejected");
            assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
        }

        let defaults = default_args();
        assert_eq!(defaults.listen_addr, "0.0.0.0:3337");
        assert_eq!(defaults.expected_goose_version, 33);
        assert!(!defaults.mutations.mutations_enabled);
        assert!(defaults.mutations.mutation_postgres_dsn.is_none());
        assert_eq!(defaults.mutations.mutation_postgres_max_connections, 4);
        assert!(!defaults.queue_mutations.queue_mutations_enabled);
        assert!(defaults
            .queue_mutations
            .queue_mutation_postgres_dsn
            .is_none());
        assert_eq!(
            defaults
                .queue_mutations
                .queue_mutation_postgres_max_connections,
            2
        );
        assert!(defaults.health_peer_graphql_urls.is_empty());
        assert_eq!(
            defaults.health_peer_timeout,
            std::time::Duration::from_millis(1_500)
        );
        assert!(!defaults.search.file_search_enabled);
        assert_eq!(
            defaults.search.file_search_address,
            "bitmagnet-filesearch.bitmagnet.svc:50052"
        );
        assert_eq!(defaults.search.file_search_timeout.as_secs(), 5);
        assert_eq!(defaults.search.file_search_max_rows, 500);
        assert!(defaults.search.file_search_route_text);
        assert!(!defaults.search.pathsearch_enabled);
        assert!(!defaults.search.path_typeahead_enabled);
        assert!(!defaults.search.path_collapse_enabled);
        assert_eq!(
            defaults.search.pathsearch_address,
            "bitmagnet-pathsearch.bitmagnet.svc:50053"
        );
        assert_eq!(defaults.search.pathsearch_timeout.as_secs(), 5);
        assert_eq!(defaults.search.pathsearch_min_query_length, 3);
        assert_eq!(defaults.search.pathsearch_oversample, 4);
        assert_eq!(defaults.search.pathsearch_max_candidates, 2_000);
        assert_eq!(defaults.search.pathsearch_max_decode_candidates, 200);
        assert_eq!(defaults.search.pathsearch_health_interval.as_secs(), 15);
        assert!(defaults.search.pathsearch_max_watermark_lag.is_zero());
        assert_eq!(defaults.search.pathsearch_max_refine_files, 300_000);
        assert_eq!(defaults.search.pathsearch_refine_file_budget, 300_000);
        assert_eq!(defaults.search.pathsearch_max_chunk_torrents, 1_024);
        assert_eq!(defaults.search.pathsearch_retained_file_budget, 1_000_000);
        assert_eq!(
            defaults.search.pathsearch_max_refine_decompressed_bytes,
            67_108_864
        );
        assert_eq!(
            defaults.search.pathsearch_refine_decoded_byte_budget,
            134_217_728
        );
        assert_eq!(defaults.search.pathsearch_retained_byte_budget, 67_108_864);
        assert_eq!(defaults.search.pathsearch_route_timeout.as_secs(), 8);
        assert_eq!(defaults.search.pathsearch_max_concurrent_refines, 0);
        assert!(defaults.search.pathsearch_slot_wait.is_zero());
        assert!(!defaults.search.features_drop_compatible_reads);
        assert!(!defaults.search.features_file_extensions_jsonb);
        assert!(!defaults.search.features_popularity_sort_default);
        assert!(!defaults.search.features_file_search_enabled);
        assert!(!defaults.search.features_file_search_facets_enabled);
        assert!(!defaults.search.features_file_search_typeahead_rpc_enabled);

        let overridden = Args::try_parse_from([
            "bitmagnet-graphql",
            "--listen-addr",
            "127.0.0.1:43337",
            "--expected-goose-version",
            "2026071201",
            "--health-peer-graphql-urls",
            "http://peer-a/graphql,http://peer-b/graphql",
            "--health-peer-timeout",
            "2.5s",
        ])
        .expect("explicit GraphQL arguments parse");
        assert_eq!(overridden.listen_addr, "127.0.0.1:43337");
        assert_eq!(overridden.expected_goose_version, 2_026_071_201);
        assert_eq!(
            overridden.health_peer_graphql_urls,
            ["http://peer-a/graphql", "http://peer-b/graphql"]
        );
        assert_eq!(
            overridden.health_peer_timeout,
            std::time::Duration::from_millis(2_500)
        );
    }

    #[test]
    fn mutation_writer_requires_the_explicit_double_gate() {
        let mut args = {
            let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
            let _restore = ArgsEnvRestore::clear();
            default_args()
        };
        assert!(mutation_db_config(&args.mutations)
            .expect("disabled config")
            .is_none());

        args.mutations.mutations_enabled = true;
        let missing =
            mutation_db_config(&args.mutations).expect_err("enabled writer requires a DSN");
        assert!(missing.to_string().contains("is required"));

        args.mutations.mutations_enabled = false;
        args.mutations.mutation_postgres_dsn = Some(
            "postgres://writer:secret@localhost/bitmagnet"
                .parse()
                .expect("test DSN"),
        );
        let debug = format!("{:?}", args.mutations);
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("writer:secret"));
        let forbidden =
            mutation_db_config(&args.mutations).expect_err("disabled writer forbids a DSN");
        assert!(forbidden.to_string().contains("is forbidden"));

        args.mutations.mutations_enabled = true;
        args.mutations.mutation_postgres_max_connections = 3;
        let config = mutation_db_config(&args.mutations)
            .expect("enabled config")
            .expect("writer config");
        assert_eq!(config.max_connections, 3);
        assert_eq!(config.dsn, "postgres://writer:secret@localhost/bitmagnet");
    }

    #[test]
    fn queue_mutation_writer_requires_a_separate_explicit_double_gate() {
        let mut args = {
            let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
            let _restore = ArgsEnvRestore::clear();
            default_args()
        };
        assert!(queue_mutation_db_config(&args.queue_mutations)
            .expect("disabled config")
            .is_none());

        args.queue_mutations.queue_mutations_enabled = true;
        let missing = queue_mutation_db_config(&args.queue_mutations)
            .expect_err("enabled queue writer requires a DSN");
        assert!(missing.to_string().contains("is required"));

        args.queue_mutations.queue_mutations_enabled = false;
        args.queue_mutations.queue_mutation_postgres_dsn = Some(
            "postgres://queue-writer:secret@localhost/bitmagnet"
                .parse()
                .expect("test DSN"),
        );
        let debug = format!("{:?}", args.queue_mutations);
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("queue-writer:secret"));
        let forbidden = queue_mutation_db_config(&args.queue_mutations)
            .expect_err("disabled queue writer forbids a DSN");
        assert!(forbidden.to_string().contains("is forbidden"));

        args.queue_mutations.queue_mutations_enabled = true;
        args.queue_mutations.queue_mutation_postgres_max_connections = 1;
        let config = queue_mutation_db_config(&args.queue_mutations)
            .expect("enabled config")
            .expect("queue writer config");
        assert_eq!(config.max_connections, 1);
        assert_eq!(
            config.dsn,
            "postgres://queue-writer:secret@localhost/bitmagnet"
        );
    }

    #[test]
    fn search_args_parse_go_compatible_environment_overrides() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let _restore = ArgsEnvRestore::clear();
        let values = [
            ("SEARCH_FILE_SEARCH_ENABLED", "1"),
            ("SEARCH_FILE_SEARCH_ADDRESS", "files.test:15052"),
            ("SEARCH_FILE_SEARCH_TIMEOUT", "-1s"),
            ("SEARCH_FILE_SEARCH_MAX_ROWS", "321"),
            ("SEARCH_FILE_SEARCH_ROUTE_TEXT", "0"),
            ("SEARCH_PATHSEARCH_ENABLED", "1"),
            ("SEARCH_PATH_TYPEAHEAD_ENABLED", "1"),
            ("SEARCH_PATH_COLLAPSE_ENABLED", "0"),
            ("SEARCH_PATHSEARCH_ADDRESS", "paths.test:15053"),
            ("SEARCH_PATHSEARCH_TIMEOUT", "2.5s"),
            ("SEARCH_PATHSEARCH_MIN_QUERY_LENGTH", "-3"),
            ("SEARCH_PATHSEARCH_OVERSAMPLE", "7"),
            ("SEARCH_PATHSEARCH_MAX_CANDIDATES", "701"),
            ("SEARCH_PATHSEARCH_MAX_DECODE_CANDIDATES", "77"),
            ("SEARCH_PATHSEARCH_HEALTH_INTERVAL", "0"),
            ("SEARCH_PATHSEARCH_MAX_WATERMARK_LAG", "1m"),
            ("SEARCH_PATHSEARCH_MAX_REFINE_FILES", "7001"),
            ("SEARCH_PATHSEARCH_REFINE_FILE_BUDGET", "7002"),
            ("SEARCH_PATHSEARCH_MAX_CHUNK_TORRENTS", "73"),
            ("SEARCH_PATHSEARCH_RETAINED_FILE_BUDGET", "7003"),
            ("SEARCH_PATHSEARCH_MAX_REFINE_DECOMPRESSED_BYTES", "7004"),
            ("SEARCH_PATHSEARCH_REFINE_DECODED_BYTE_BUDGET", "7005"),
            ("SEARCH_PATHSEARCH_RETAINED_BYTE_BUDGET", "7006"),
            ("SEARCH_PATHSEARCH_ROUTE_TIMEOUT", "7s"),
            ("SEARCH_PATHSEARCH_MAX_CONCURRENT_REFINES", "-4"),
            ("SEARCH_PATHSEARCH_SLOT_WAIT", "-2s"),
            ("SEARCH_FEATURES_DROP_COMPATIBLE_READS", "1"),
            ("SEARCH_FEATURES_GATE_FILE_EXTENSIONS_JSONB", "0"),
            ("SEARCH_FEATURES_POPULARITY_SORT_DEFAULT", "1"),
            ("SEARCH_FEATURES_FILE_SEARCH_ENABLED", "1"),
            ("SEARCH_FEATURES_FILE_SEARCH_FACETS_ENABLED", "1"),
            ("SEARCH_FEATURES_FILE_SEARCH_TYPEAHEAD_RPC_ENABLED", "0"),
        ];
        for (name, value) in values {
            std::env::set_var(name, value);
        }

        let args = default_args();
        assert!(args.search.file_search_enabled);
        assert_eq!(args.search.file_search_address, "files.test:15052");
        assert!(args.search.file_search_timeout.is_zero());
        assert_eq!(args.search.file_search_max_rows, 321);
        assert!(!args.search.file_search_route_text);
        assert!(args.search.pathsearch_enabled);
        assert!(args.search.path_typeahead_enabled);
        assert!(!args.search.path_collapse_enabled);
        assert_eq!(args.search.pathsearch_address, "paths.test:15053");
        assert_eq!(args.search.pathsearch_timeout.as_millis(), 2_500);
        assert_eq!(args.search.pathsearch_min_query_length, -3);
        assert_eq!(args.search.pathsearch_oversample, 7);
        assert_eq!(args.search.pathsearch_max_candidates, 701);
        assert_eq!(args.search.pathsearch_max_decode_candidates, 77);
        assert!(args.search.pathsearch_health_interval.is_zero());
        assert_eq!(args.search.pathsearch_max_watermark_lag.as_secs(), 60);
        assert_eq!(args.search.pathsearch_max_refine_files, 7_001);
        assert_eq!(args.search.pathsearch_refine_file_budget, 7_002);
        assert_eq!(args.search.pathsearch_max_chunk_torrents, 73);
        assert_eq!(args.search.pathsearch_retained_file_budget, 7_003);
        assert_eq!(args.search.pathsearch_max_refine_decompressed_bytes, 7_004);
        assert_eq!(args.search.pathsearch_refine_decoded_byte_budget, 7_005);
        assert_eq!(args.search.pathsearch_retained_byte_budget, 7_006);
        assert_eq!(args.search.pathsearch_route_timeout.as_secs(), 7);
        assert_eq!(args.search.pathsearch_max_concurrent_refines, -4);
        assert!(args.search.pathsearch_slot_wait.is_zero());
        assert!(args.search.features_drop_compatible_reads);
        assert!(!args.search.features_file_extensions_jsonb);
        assert!(args.search.features_popularity_sort_default);
        assert!(args.search.features_file_search_enabled);
        assert!(args.search.features_file_search_facets_enabled);
        assert!(!args.search.features_file_search_typeahead_rpc_enabled);
    }

    #[tokio::test]
    async fn disabled_builder_does_not_dial_or_spawn_and_preserves_double_gate() {
        let mut args = {
            let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
            let _restore = ArgsEnvRestore::clear();
            default_args()
        };
        args.search.features_drop_compatible_reads = true;
        args.search.features_file_search_enabled = true;
        args.search.features_file_search_facets_enabled = true;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/bitmagnet")
            .expect("lazy postgres pool");
        let lifecycle = build_search_runtime(
            pool,
            &args.search,
            Arc::new(bitmagnet_search_serve::PathsearchMetrics::new()),
        )
        .expect("disabled backends construct without dialing");

        assert!(lifecycle.pathsearch_health_poller.is_none());
        assert!(!lifecycle.runtime.healthy());
        assert!(!lifecycle.runtime.typeahead_enabled());
        assert!(lifecycle.runtime.features().file_search_enabled);
        assert!(lifecycle.runtime.features().file_search_facets_enabled);
        assert!(
            lifecycle
                .runtime
                .search_build_config()
                .file_extensions_jsonb
        );
        let error = lifecycle
            .runtime
            .file_search(bitmagnet_graphql::schema::search::FileSearchRequest::default())
            .await
            .expect_err("product-on plus backend-off must fail loud");
        assert!(error.to_string().contains("file search is disabled"));
    }

    #[tokio::test]
    async fn incompatible_pathsearch_budget_orderings_fail_before_dialing() {
        let mut args = {
            let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
            let _restore = ArgsEnvRestore::clear();
            default_args()
        };
        args.search.pathsearch_enabled = true;
        args.search.pathsearch_address = "127.0.0.1:1".to_owned();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/bitmagnet")
            .expect("lazy postgres pool");

        args.search.pathsearch_max_candidates = 100;
        args.search.pathsearch_max_decode_candidates = 101;
        let error = build_search_runtime(
            pool.clone(),
            &args.search,
            Arc::new(bitmagnet_search_serve::PathsearchMetrics::new()),
        )
        .err()
        .expect("decode budget ordering must fail closed");
        assert!(error.to_string().contains("max_decode_candidates"));

        args.search.pathsearch_max_candidates = 2_000;
        args.search.pathsearch_max_decode_candidates = 200;
        args.search.pathsearch_max_refine_files = 300_001;
        args.search.pathsearch_refine_file_budget = 300_000;
        let error = build_search_runtime(
            pool.clone(),
            &args.search,
            Arc::new(bitmagnet_search_serve::PathsearchMetrics::new()),
        )
        .err()
        .expect("per-torrent budget ordering must fail closed");
        assert!(error.to_string().contains("max_refine_files"));

        args.search.pathsearch_max_refine_files = 300_000;
        args.search.pathsearch_refine_file_budget = 1_000_001;
        args.search.pathsearch_retained_file_budget = 1_000_000;
        let error = build_search_runtime(
            pool.clone(),
            &args.search,
            Arc::new(bitmagnet_search_serve::PathsearchMetrics::new()),
        )
        .err()
        .expect("retained budget ordering must fail closed");
        assert!(error.to_string().contains("refine_file_budget"));

        args.search.pathsearch_refine_file_budget = 300_000;
        args.search.pathsearch_retained_file_budget = 1_000_000;
        args.search.pathsearch_max_refine_decompressed_bytes = 129;
        args.search.pathsearch_refine_decoded_byte_budget = 128;
        let error = build_search_runtime(
            pool,
            &args.search,
            Arc::new(bitmagnet_search_serve::PathsearchMetrics::new()),
        )
        .err()
        .expect("decompressed-byte ordering must fail closed");
        assert!(error.to_string().contains("max_refine_decompressed_bytes"));
    }

    #[tokio::test]
    async fn enabled_builder_owns_and_drains_the_health_poller() {
        let mut args = {
            let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
            let _restore = ArgsEnvRestore::clear();
            default_args()
        };
        args.search.pathsearch_enabled = true;
        args.search.pathsearch_address = "127.0.0.1:1".to_owned();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/bitmagnet")
            .expect("lazy postgres pool");
        let lifecycle = build_search_runtime(
            pool,
            &args.search,
            Arc::new(bitmagnet_search_serve::PathsearchMetrics::new()),
        )
        .expect("enabled builder constructs lazy pathsearch client");

        assert!(lifecycle.pathsearch_health_poller.is_some());
        tokio::time::timeout(std::time::Duration::from_secs(1), lifecycle.shutdown())
            .await
            .expect("health poller shutdown must not detach or hang");
    }

    #[test]
    fn go_duration_parser_accepts_compound_fractional_and_non_positive_values() {
        assert_eq!(
            parse_go_duration("0.25s").expect("fractional seconds"),
            std::time::Duration::from_millis(250)
        );
        assert_eq!(
            parse_go_duration("1m30.5s").expect("compound duration"),
            std::time::Duration::from_millis(90_500)
        );
        assert_eq!(
            parse_go_duration("0").expect("zero"),
            std::time::Duration::ZERO
        );
        assert_eq!(
            parse_go_duration("-1s").expect("negative fallback"),
            std::time::Duration::ZERO
        );
        assert_eq!(
            parse_go_duration("+0").expect("positive signed zero"),
            std::time::Duration::ZERO
        );
        assert_eq!(
            parse_go_duration("-0").expect("negative signed zero"),
            std::time::Duration::ZERO
        );
        assert_eq!(
            parse_go_duration("0.9ns").expect("sub-nanosecond truncation"),
            std::time::Duration::ZERO
        );
        assert_eq!(
            parse_go_duration("1.9ns").expect("fractional nanosecond truncation"),
            std::time::Duration::from_nanos(1)
        );
        assert!(parse_go_duration("1500").is_err());
    }

    #[test]
    fn go_duration_parser_enforces_the_signed_int64_nanosecond_range() {
        assert_eq!(
            parse_go_duration("9223372036854775807ns").expect("maximum positive duration"),
            std::time::Duration::from_nanos(i64::MAX as u64)
        );
        assert!(parse_go_duration("9223372036854775808ns").is_err());
        assert_eq!(
            parse_go_duration("-9223372036854775808ns").expect("minimum negative duration"),
            std::time::Duration::ZERO
        );
        assert!(parse_go_duration("-9223372036854775809ns").is_err());
    }

    #[tokio::test]
    async fn graphql_route_reports_handler_duration_for_success_and_errors_only() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/bitmagnet")
            .expect("lazy postgres pool");
        let version = "test-version".to_owned();
        let router = app(AppState {
            schema: bitmagnet_graphql::build_schema(version.clone()),
            pool,
            version: version.clone(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind GraphQL test server");
        let address = listener.local_addr().expect("GraphQL test server address");
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("serve GraphQL test router");
        });
        let client = reqwest::Client::new();
        let graphql_url = format!("http://{address}/graphql");

        let success = client
            .post(&graphql_url)
            .json(&json!({"query": "{ version }"}))
            .send()
            .await
            .expect("successful GraphQL request");
        assert_eq!(success.status(), reqwest::StatusCode::OK);
        assert_positive_duration_header(&success);
        let success_body = success
            .json::<Value>()
            .await
            .expect("successful GraphQL response JSON");
        assert_eq!(success_body["data"]["version"], version);

        let error = client
            .post(&graphql_url)
            .json(&json!({"query": "{ fieldThatDoesNotExist }"}))
            .send()
            .await
            .expect("GraphQL validation-error request");
        assert_eq!(error.status(), reqwest::StatusCode::OK);
        assert_positive_duration_header(&error);
        let error_body = error
            .json::<Value>()
            .await
            .expect("GraphQL validation-error response JSON");
        assert!(
            error_body["errors"]
                .as_array()
                .is_some_and(|errors| !errors.is_empty()),
            "GraphQL validation error response must contain errors"
        );

        let livez = client
            .get(format!("http://{address}/livez"))
            .send()
            .await
            .expect("livez request");
        assert!(
            livez
                .headers()
                .get(GRAPHQL_HANDLER_DURATION_HEADER)
                .is_none(),
            "non-GraphQL routes must not expose the GraphQL duration header"
        );
        let playground = client
            .get(format!("http://{address}/"))
            .send()
            .await
            .expect("playground request");
        assert!(
            playground
                .headers()
                .get(GRAPHQL_HANDLER_DURATION_HEADER)
                .is_none(),
            "the playground must not expose the GraphQL duration header"
        );

        server.abort();
        let _ = server.await;
    }
}
