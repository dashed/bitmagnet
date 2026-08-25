use std::collections::{BTreeMap, HashSet};
use std::future::pending;
use std::net::{IpAddr, SocketAddr, SocketAddrV6};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bitmagnet_dht::{Id20, ScrapeBloomFilter};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::{
    dht_persist_source_channel, DhtPersistSourceRequest, DhtPersistSourceWorker,
    DhtPersistSourceWorkerConfig, DhtPersistSourceWorkerExit, DhtPersistSourceWorkerStats,
    DhtSourceBatchWriter, DhtSourceWrite, PersistSourceCollaboratorError,
};

const FIXTURE_TEXT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/dht_crawler_persist_sources.jsonl"
));
const FIXTURE_BYTES: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/dht_crawler_persist_sources.jsonl"
));
const FIXTURE_SHA256: &str = "01acacdc5ccc425bda88e87643328101499af3873f3a52c7eef2f46a92697bd9";

const FIXTURE_IDS: [&str; 4] = [
    "production_source_factory_batcher_lifecycle_model_sql_and_schema_contract",
    "empty_and_directional_filters_project_valid_counts_null_optionals_and_bloom_direction",
    "one_bit_hash_collision_rounds_half_up_to_one_while_truncation_would_be_zero",
    "ordered_duplicate_batch_first_occurrence_wins_in_first_unique_order",
];

const ROW_CLASSIFICATIONS: [&str; 4] = [
    "SOURCE_ONLY",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT_TEST_HARNESS",
];

const RUST_EXECUTION_PARTITION: [(&str, &str); 4] = [
    (
        FIXTURE_IDS[0],
        "SOURCE_ONLY_NO_RUST_RUNTIME_OR_LIVE_SERVICE_REPLAY",
    ),
    (
        FIXTURE_IDS[1],
        "RUST_ACTUAL_WORKER_EMPTY_AND_DIRECTIONAL_PROJECTION_REPLAY",
    ),
    (
        FIXTURE_IDS[2],
        "RUST_ACTUAL_WORKER_ONE_BIT_COLLISION_ROUND_HALF_UP_REPLAY",
    ),
    (
        FIXTURE_IDS[3],
        "RUST_ACTUAL_WORKER_FIRST_WINS_BEHAVIOR_FROM_GO_TEST_HARNESS_NOT_GO_RUNPERSISTSOURCES_REPLAY",
    ),
];

const DELIBERATE_RUST_DELTAS: [&str; 6] = [
    "Rust_executes_the_actual_owned_worker_for_runtime_rows_while_Go_executes_conversion_or_a_source_pinned_loop_harness",
    "Rust_owns_first_item_relative_batching_without_a_detached_batcher_or_one_batch_output_buffer",
    "Rust_has_typed_input_EOF_and_flushes_the_final_partial_batch",
    "Rust_polls_biased_shutdown_then_deadline_before_every_additional_receive",
    "Rust_writer_calls_require_atomic_all_or_none_behavior_while_Go_100_row_chunks_can_partially_commit",
    "Rust_has_saturating_conservation_stats_and_truthful_shutdown_write_abandonment",
];

const RUST_NONCLAIMS: [&str; 13] = [
    "Go_runPersistSources_runtime_execution_or_closed_batcher_behavior",
    "live_PostgreSQL_SQL_schema_plan_index_locking_or_affected_row_count",
    "a_concrete_Rust_PostgreSQL_source_writer_or_transaction_implementation",
    "exact_Go_construction_phase_ticker_schedule_or_ready_select_tie_winner",
    "Go_detached_goroutine_shutdown_drain_join_or_total_retention_behavior",
    "writer_error_retry_requeue_or_exactly_once_delivery",
    "remote_database_commit_or_rollback_after_Rust_writer_future_cancellation",
    "all_ones_or_other_nonfinite_Bloom_ApproximatedSize_projection",
    "Bloom_mutation_after_request_construction_or_concurrent_Bloom_access",
    "source_node_or_raw_Bloom_database_durability",
    "live_DNS_UDP_DHT_scrape_or_upstream_worker_behavior",
    "these_fixture_rows_do_not_replay_Rust_shutdown_timer_writer_error_or_counter_saturation_paths",
    "application_supervisor_deployment_metrics_logs_or_production_readiness",
];

const NORMALIZED_AST: [(&str, &str); 16] = [
    (
        "batching.In",
        "f5ef939724dc08bc0fa39e9fa2e0863e45acd1c965609ad91fa7082fd6632b21",
    ),
    (
        "batching.NewBatchingChannel",
        "2c9a3fa894f82680a8cb8437d8dbad6d3bc2da9a7594c83553ef7650dd472dc6",
    ),
    (
        "batching.Out",
        "f677733fd65c621331747365d30bc29503cda90a21e5aba68ece706afd5d2e3c",
    ),
    (
        "batching.batch",
        "ebedd32544fc4a53c3cb016fd883da2e76267dd492a7c5f88ba2ebcf8232858c",
    ),
    (
        "batching.flush",
        "3c72fb1d8c6d52bfed5b60a796d5bfee0e13da3b745c220ac01467a88de1f274",
    ),
    (
        "bloom.FromScrape",
        "7298c86e1af2c667f8ae43775229426e70574a33dd4148ea2a71888bfe66f20b",
    ),
    (
        "crawler.infoHashWithScrape",
        "c9f4fdef915a61322eeaab348afd5896744000a5382416f474de44f21a6f835c",
    ),
    (
        "crawler.nodeHasPeersForHash",
        "1e2206b038dd5c1b70dff5a29cdf044ad7133b4876db75723081ab37c3d3da58",
    ),
    (
        "crawler.start",
        "d61a318ce626352ee4f5cd5dd48191d767bbfe45b6a9def673cd185eada4f67b",
    ),
    (
        "factory.New",
        "0204a00fd63b275339d63d622865858571c153bc81fc738784a78e1c150fec80",
    ),
    (
        "model.NewNullUint",
        "bba8e9dedb19e3e33c3dfed0ad327aa9e11892eaa6a08f3495b31062dfcff33f",
    ),
    (
        "model.TorrentsTorrentSource",
        "f71036cb64dfaa18994e0caa7fe63e394a93e3f29cf00312ce7f7d2e2cf358e5",
    ),
    (
        "persist.createTorrentSourceModel",
        "288ba786fbb6da0578c1164de0bba17bc5376e387996e3b02c54bdb2774f79f7",
    ),
    (
        "persist.persistScrapedTorrentSources",
        "e3b5338f2bd11789760caa263f1880535165ce58649bce0ef364941c04454097",
    ),
    (
        "persist.runPersistSources",
        "07ad92a09673d00523cc463c4c6b3cf6f31881c3ed279e0d77e3ce2c0659dc6a",
    ),
    (
        "protocol.ID.String",
        "c8e7761bfacaedb901406cffb17a1816adbc162e174f19a5678e20817f339126",
    ),
];

const GO_SOURCES: [(&str, &[u8], &str); 16] = [
    (
        "go.mod",
        include_bytes!("../../../../go.mod"),
        "bdeb8dff1aa2ee347af6d84614f8c4f79c15c75edb76d991c97150706385d71c",
    ),
    (
        "go.sum",
        include_bytes!("../../../../go.sum"),
        "4b1675395f71a90d7e2c761481165bb55cdba5e8d985269778fc60c2030b8096",
    ),
    (
        "internal/bloom/bloom.go",
        include_bytes!("../../../../internal/bloom/bloom.go"),
        "7fd2ef4970e108eb6b66d05f73aa0772864a93bdb49bee8e27697a321a8a9106",
    ),
    (
        "internal/concurrency/batching_channel.go",
        include_bytes!("../../../../internal/concurrency/batching_channel.go"),
        "72b3c9fd5fbc8ecbfb0ba2bc2ed5e6c1d45de01f03d3e015b2467f114ec70975",
    ),
    (
        "internal/dhtcrawler/config.go",
        include_bytes!("../../../../internal/dhtcrawler/config.go"),
        "b3cac15378cdca0f21c5f21f37aeb0679815d5bacd16bfa0c3bac2af56db87ef",
    ),
    (
        "internal/dhtcrawler/crawler.go",
        include_bytes!("../../../../internal/dhtcrawler/crawler.go"),
        "ae6ca2484a57231a08351629c21fdc0a875f2272bfd4ad42a4e5386be86500b6",
    ),
    (
        "internal/dhtcrawler/factory.go",
        include_bytes!("../../../../internal/dhtcrawler/factory.go"),
        "ed34129835773817736d70e74c7c884e5b9197e35741dee922ee9a5d691288a6",
    ),
    (
        "internal/dhtcrawler/persist.go",
        include_bytes!("../../../../internal/dhtcrawler/persist.go"),
        "8a76c32f1aeaad074ce22c53193a012ae447a34a3c07e9c2123e9e7e27ac2022",
    ),
    (
        "internal/dhtcrawler/scrape.go",
        include_bytes!("../../../../internal/dhtcrawler/scrape.go"),
        "8450576571bc044b1a85cb013ff6b330683b0b2b6e188110614120c3bafc320a",
    ),
    (
        "internal/model/null.go",
        include_bytes!("../../../../internal/model/null.go"),
        "b9c3762d286201140c51cd3ca2630361fb35fb76464c297a37d85037d1be782d",
    ),
    (
        "internal/model/torrents_torrent_sources.gen.go",
        include_bytes!("../../../../internal/model/torrents_torrent_sources.gen.go"),
        "a5431060dd68f51ac77aced27f4a3c1481124054bef43365d368bded4a405b41",
    ),
    (
        "internal/protocol/dht/scrape.go",
        include_bytes!("../../../../internal/protocol/dht/scrape.go"),
        "7dd152311451eb95c580bb7e49822a51b775bd532bc2add14c9feea8432af6bd",
    ),
    (
        "internal/protocol/id.go",
        include_bytes!("../../../../internal/protocol/id.go"),
        "e1947e2b4af4cc008f5bb8cf5000ebfe784a82e119cb0418c2a74c3ed5f8c26f",
    ),
    (
        "migrations/00001_init.sql",
        include_bytes!("../../../../migrations/00001_init.sql"),
        "32e729ca54f5140446cf313dc2207eee03c0eaae5ebe1dcbc2ffbcf1f4340d17",
    ),
    (
        "migrations/00017_ordering_fields.sql",
        include_bytes!("../../../../migrations/00017_ordering_fields.sql"),
        "63cedb8a21c89613aeab1cdd42b7d29378a08d47fad7a78462951b08d9b14955",
    ),
    (
        "migrations/00025_dht_seen_count.sql",
        include_bytes!("../../../../migrations/00025_dht_seen_count.sql"),
        "92c7571e4f1f1044c10c3854f551e28a2ed04b0f654c57404c71ed9777704f3f",
    ),
];

const PREREQUISITE_FIXTURES: [(&str, &[u8], &str); 2] = [
    (
        "testdata/parity/dht/dht_crawler_scrape.jsonl",
        include_bytes!("../../../../testdata/parity/dht/dht_crawler_scrape.jsonl"),
        "d434306fd60678be95cabd53d59ea152f6a013bf2e486f4bb2456aa8da2c6d9b",
    ),
    (
        "testdata/parity/dht/scrape_bloom.jsonl",
        include_bytes!("../../../../testdata/parity/dht/scrape_bloom.jsonl"),
        "760f868a2cb53d8342e02c84b99ec0335fa20df52d5d2695b00d3f7e2d7ac287",
    ),
];

const GO_NONCLAIMS: [&str; 16] = [
    "actual_runPersistSources_runtime_execution",
    "live_PostgreSQL_SQL_execution_schema_plan_index_locking_or_affected_row_count",
    "database_transactionality_beyond_source_observation_of_no_explicit_transaction",
    "exact_wall_clock_time_New_or_batching_ticker_elapsed_schedule",
    "ready_select_tie_winner_goroutine_scheduling_or_channel_fairness",
    "shutdown_drain_join_or_total_work_retention_guarantee",
    "closed_batcher_input_or_closed_output_runtime_execution",
    "all_ones_or_other_nonfinite_Bloom_ApproximatedSize_projection",
    "Bloom_mutation_after_model_conversion_or_concurrent_Bloom_access",
    "source_node_or_raw_Bloom_database_durability",
    "BfPeers_semantics_beyond_direct_projection_to_the_leechers_column",
    "repository_retry_requeue_idempotency_or_exactly_once_delivery",
    "metric_value_as_actual_inserted_updated_or_committed_database_rows",
    "log_delivery_format_level_or_ordering",
    "live_DNS_UDP_DHT_scrape_or_upstream_worker_behavior",
    "Rust_API_worker_repository_stats_shutdown_application_wiring_deployment_or_readiness",
];

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Fixture {
    id: String,
    subsystem: String,
    classification: String,
    oracle: Oracle,
    input: Input,
    expected: Expected,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Oracle {
    composition: String,
    determinism: String,
    harness: String,
    database: String,
    clock: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Input {
    kind: String,
    scrapes: Vec<Scrape>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Scrape {
    info_hash: String,
    node: Address,
    seeders: FilterInput,
    peers: FilterInput,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Address {
    ip: String,
    port: u16,
    scope: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FilterInput {
    raw_ips: Vec<String>,
    ranges: Vec<FilterRange>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FilterRange {
    base: String,
    count: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Expected {
    models: Vec<Model>,
    bloom_observations: Vec<BloomObservation>,
    converter_call_info_hashes: Vec<String>,
    duplicate_info_hashes: Vec<String>,
    conversion_errors: Vec<String>,
    first_occurrence_order: Vec<String>,
    run_persist_sources_executed: bool,
    source: Option<Source>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Model {
    source: String,
    info_hash: String,
    seeders: u32,
    seeders_valid: bool,
    leechers: u32,
    leechers_valid: bool,
    seen_count: u32,
    import_id_valid: bool,
    published_at_valid: bool,
    created_at_zero: bool,
    updated_at_zero: bool,
    source_node_retained: bool,
    raw_seeders_bloom_retained: bool,
    raw_peers_bloom_retained: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BloomObservation {
    info_hash: String,
    seeders_bloom_sha256: String,
    peers_bloom_sha256: String,
    seeders_approximated: u32,
    peers_approximated: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Source {
    factory: FactoryContract,
    batcher: BatcherContract,
    lifecycle: LifecycleContract,
    worker: WorkerContract,
    model: ModelContract,
    repository: RepositoryContract,
    schema: SchemaContract,
    dependencies: Dependencies,
    normalized_ast_sha256: BTreeMap<String, String>,
    source_sha256: BTreeMap<String, String>,
    prerequisite_sha256: BTreeMap<String, String>,
    nonclaims: Vec<String>,
    evidence: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FactoryContract {
    input_capacity: usize,
    maximum_batch_size: usize,
    batch_interval_millis: u64,
    output_capacity: usize,
    configuration_fields: Vec<String>,
    hardcoded: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BatcherContract {
    flush_at_maximum_size: bool,
    flush_on_nonempty_ticker: bool,
    ticker_starts_at_construction: bool,
    ticker_resets_after_flush: bool,
    flush_blocks_on_output: bool,
    context_aware: bool,
    input_close_exits_loop: bool,
    closed_input_source_outcome: String,
    output_receive_checks_open_boolean: bool,
    raw_input_capacity_is_total_retention: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LifecycleContract {
    start_launches_crawler_detached: bool,
    crawler_launches_worker_detached: bool,
    start_waits_only_for_stopped: bool,
    shared_context_cancelled_after_stopped: bool,
    stop_closes_batcher_input: bool,
    stop_drains_persist_sources: bool,
    stop_joins_worker_or_batcher: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkerContract {
    first_occurrence_wins: bool,
    first_unique_order_preserved: bool,
    duplicate_key: String,
    conversion_error_logged_and_skipped: bool,
    current_conversion_can_return_error: bool,
    repository_called_for_empty_models: bool,
    repository_error_logged: bool,
    repository_error_stops_worker: bool,
    repository_error_retried_or_requeued: bool,
    metric_entity: String,
    metric_counts_prepared_unique_models: bool,
    metric_counts_actual_affected_rows: bool,
    metric_incremented_on_repository_error: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModelContract {
    source: String,
    seeders_from: String,
    leechers_from: String,
    subtracts_seeders_from_peers: bool,
    counts_always_valid: bool,
    seen_count: u32,
    info_hash_encoding_for_sql: String,
    source_node_retained: bool,
    raw_blooms_retained: bool,
    import_id_set: bool,
    published_at_set: bool,
    model_created_updated_at_set: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RepositoryContract {
    chunk_size: usize,
    arguments_per_row: usize,
    one_timestamp_per_invocation: bool,
    explicit_transaction: bool,
    missing_parent_outcome: String,
    conflict_target: Vec<String>,
    conflict_updated_columns: Vec<String>,
    conflict_preserved_columns: Vec<String>,
    conflict_seen_count_expression: String,
    first_exec_error_stops_chunks: bool,
    earlier_chunks_can_remain_committed: bool,
    retry_or_requeue: bool,
    one_row_sql: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SchemaContract {
    table: String,
    columns: Vec<SchemaColumn>,
    primary_key: Vec<String>,
    raw_bloom_columns_present: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SchemaColumn {
    name: String,
    r#type: String,
    nullable: bool,
    default: String,
    reference: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Dependencies {
    go_mod_bloom_line: String,
    go_sum_bloom_line: String,
    go_sum_bloom_go_mod_line: String,
    bloom_approximation: String,
}

#[derive(Default)]
struct ScriptWriter {
    calls: Mutex<Vec<Vec<DhtSourceWrite>>>,
}

impl ScriptWriter {
    fn calls(&self) -> Vec<Vec<DhtSourceWrite>> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl DhtSourceBatchWriter for ScriptWriter {
    async fn write_batch(
        &self,
        sources: &[DhtSourceWrite],
    ) -> Result<(), PersistSourceCollaboratorError> {
        self.calls.lock().unwrap().push(sources.to_vec());
        Ok(())
    }
}

fn fixtures() -> Vec<Fixture> {
    FIXTURE_TEXT
        .lines()
        .enumerate()
        .map(|(index, line)| {
            serde_json::from_str(line)
                .unwrap_or_else(|error| panic!("parse persist-sources row {}: {error}", index + 1))
        })
        .collect()
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn expected_map<const N: usize>(entries: [(&str, &str); N]) -> BTreeMap<String, String> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

fn string_vec(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn assert_source_contract(source: &Source) {
    assert_eq!(
        source.factory,
        FactoryContract {
            input_capacity: 1_000,
            maximum_batch_size: 1_000,
            batch_interval_millis: 60_000,
            output_capacity: 1,
            configuration_fields: Vec::new(),
            hardcoded: true,
        }
    );
    assert_eq!(
        source.batcher,
        BatcherContract {
            flush_at_maximum_size: true,
            flush_on_nonempty_ticker: true,
            ticker_starts_at_construction: true,
            ticker_resets_after_flush: true,
            flush_blocks_on_output: true,
            context_aware: false,
            input_close_exits_loop: false,
            closed_input_source_outcome:
                "unlabeled_break_exits_select_only_and_closed_input_spins_without_closing_output"
                    .to_owned(),
            output_receive_checks_open_boolean: false,
            raw_input_capacity_is_total_retention: false,
        }
    );
    assert_eq!(
        source.lifecycle,
        LifecycleContract {
            start_launches_crawler_detached: true,
            crawler_launches_worker_detached: true,
            start_waits_only_for_stopped: true,
            shared_context_cancelled_after_stopped: true,
            stop_closes_batcher_input: false,
            stop_drains_persist_sources: false,
            stop_joins_worker_or_batcher: false,
        }
    );
    assert_eq!(
        source.worker,
        WorkerContract {
            first_occurrence_wins: true,
            first_unique_order_preserved: true,
            duplicate_key: "protocol.ID_info_hash".to_owned(),
            conversion_error_logged_and_skipped: true,
            current_conversion_can_return_error: false,
            repository_called_for_empty_models: true,
            repository_error_logged: true,
            repository_error_stops_worker: false,
            repository_error_retried_or_requeued: false,
            metric_entity: "TorrentsTorrentSource".to_owned(),
            metric_counts_prepared_unique_models: true,
            metric_counts_actual_affected_rows: false,
            metric_incremented_on_repository_error: false,
        }
    );
    assert_eq!(
        source.model,
        ModelContract {
            source: "dht".to_owned(),
            seeders_from: "bfsd.ApproximatedSize".to_owned(),
            leechers_from: "bfpe.ApproximatedSize".to_owned(),
            subtracts_seeders_from_peers: false,
            counts_always_valid: true,
            seen_count: 1,
            info_hash_encoding_for_sql: "lowercase_40_hex_then_PostgreSQL_decode_hex".to_owned(),
            source_node_retained: false,
            raw_blooms_retained: false,
            import_id_set: false,
            published_at_set: false,
            model_created_updated_at_set: false,
        }
    );
    assert_eq!(
        source.repository,
        RepositoryContract {
            chunk_size: 100,
            arguments_per_row: 8,
            one_timestamp_per_invocation: true,
            explicit_transaction: false,
            missing_parent_outcome: "silently_skipped_by_WHERE_EXISTS".to_owned(),
            conflict_target: string_vec(&["info_hash", "source"]),
            conflict_updated_columns: string_vec(&[
                "seeders",
                "leechers",
                "published_at",
                "updated_at",
                "seen_count",
            ]),
            conflict_preserved_columns: string_vec(&["created_at", "import_id"]),
            conflict_seen_count_expression: "torrents_torrent_sources.seen_count + 1".to_owned(),
            first_exec_error_stops_chunks: true,
            earlier_chunks_can_remain_committed: true,
            retry_or_requeue: false,
            one_row_sql: "INSERT INTO torrents_torrent_sources (source, info_hash, seeders, leechers, published_at, seen_count, created_at, updated_at) SELECT v.source, decode(v.info_hash, 'hex'), v.seeders, v.leechers, v.published_at, v.seen_count, v.created_at, v.updated_at FROM (VALUES (?,?,?::integer,?::integer,?::timestamptz,?::integer,?::timestamptz,?::timestamptz)) AS v(source, info_hash, seeders, leechers, published_at, seen_count, created_at, updated_at) WHERE EXISTS (SELECT 1 FROM torrents t WHERE t.info_hash = decode(v.info_hash, 'hex')) ON CONFLICT (info_hash, source) DO UPDATE SET seeders = excluded.seeders, leechers = excluded.leechers, published_at = excluded.published_at, updated_at = excluded.updated_at, seen_count = torrents_torrent_sources.seen_count + 1".to_owned(),
        }
    );
    assert_eq!(
        source.schema,
        SchemaContract {
            table: "torrents_torrent_sources".to_owned(),
            columns: vec![
                schema_column(
                    "source",
                    "text",
                    false,
                    "",
                    "torrent_sources_on_delete_cascade"
                ),
                schema_column(
                    "info_hash",
                    "bytea",
                    false,
                    "",
                    "torrents_on_delete_cascade"
                ),
                schema_column("import_id", "text", true, "", ""),
                schema_column("seeders", "integer", true, "", ""),
                schema_column("leechers", "integer", true, "", ""),
                schema_column("published_at", "timestamp_with_time_zone", true, "", ""),
                schema_column("created_at", "timestamp_with_time_zone", false, "", ""),
                schema_column("updated_at", "timestamp_with_time_zone", false, "", ""),
                schema_column("seen_count", "integer", false, "1", ""),
            ],
            primary_key: string_vec(&["source", "info_hash"]),
            raw_bloom_columns_present: false,
        }
    );
    assert_eq!(
        source.dependencies,
        Dependencies {
            go_mod_bloom_line: "github.com/bits-and-blooms/bloom/v3 v3.7.0".to_owned(),
            go_sum_bloom_line: "github.com/bits-and-blooms/bloom/v3 v3.7.0 h1:VfknkqV4xI+PsaDIsoHueyxVDZrfvMn56jeWUzvzdls=".to_owned(),
            go_sum_bloom_go_mod_line: "github.com/bits-and-blooms/bloom/v3 v3.7.0/go.mod h1:VKlUSvp0lFIYqxJjzdnSsZEw4iHb1kOL2tfHTgyJBHg=".to_owned(),
            bloom_approximation: "uint32_floor_negative_m_over_k_log_one_minus_x_over_m_plus_half_for_finite_filters".to_owned(),
        }
    );
    assert_eq!(source.normalized_ast_sha256, expected_map(NORMALIZED_AST));
    assert_eq!(
        source.source_sha256,
        expected_map(GO_SOURCES.map(|(path, _, digest)| (path, digest)))
    );
    assert_eq!(
        source.prerequisite_sha256,
        expected_map(PREREQUISITE_FIXTURES.map(|(path, _, digest)| (path, digest)))
    );
    assert_eq!(source.nonclaims, GO_NONCLAIMS);
    assert_eq!(
        source.evidence,
        "runtime rows call actual bloom.FromScrape, actual BloomFilter.ApproximatedSize through createTorrentSourceModel, and actual createTorrentSourceModel; the duplicate row executes only a source-pinned first-wins test harness; runPersistSources, persistScrapedTorrentSources, time.New, batching goroutines, metrics, logs, and PostgreSQL are never executed"
    );
}

fn schema_column(
    name: &str,
    r#type: &str,
    nullable: bool,
    default: &str,
    reference: &str,
) -> SchemaColumn {
    SchemaColumn {
        name: name.to_owned(),
        r#type: r#type.to_owned(),
        nullable,
        default: default.to_owned(),
        reference: reference.to_owned(),
    }
}

#[test]
fn source_contract_hashes_layout_partition_ast_dependencies_and_nonclaims_are_exact() {
    assert_eq!(sha256(FIXTURE_BYTES), FIXTURE_SHA256);
    assert!(FIXTURE_BYTES.ends_with(b"\n"));
    assert!(!FIXTURE_BYTES.contains(&b'\r'));
    assert_eq!(
        FIXTURE_BYTES.iter().filter(|byte| **byte == b'\n').count(),
        FIXTURE_IDS.len()
    );
    assert_eq!(
        RUST_EXECUTION_PARTITION,
        [
            (
                FIXTURE_IDS[0],
                "SOURCE_ONLY_NO_RUST_RUNTIME_OR_LIVE_SERVICE_REPLAY",
            ),
            (
                FIXTURE_IDS[1],
                "RUST_ACTUAL_WORKER_EMPTY_AND_DIRECTIONAL_PROJECTION_REPLAY",
            ),
            (
                FIXTURE_IDS[2],
                "RUST_ACTUAL_WORKER_ONE_BIT_COLLISION_ROUND_HALF_UP_REPLAY",
            ),
            (
                FIXTURE_IDS[3],
                "RUST_ACTUAL_WORKER_FIRST_WINS_BEHAVIOR_FROM_GO_TEST_HARNESS_NOT_GO_RUNPERSISTSOURCES_REPLAY",
            ),
        ]
    );
    assert_eq!(
        DELIBERATE_RUST_DELTAS,
        [
            "Rust_executes_the_actual_owned_worker_for_runtime_rows_while_Go_executes_conversion_or_a_source_pinned_loop_harness",
            "Rust_owns_first_item_relative_batching_without_a_detached_batcher_or_one_batch_output_buffer",
            "Rust_has_typed_input_EOF_and_flushes_the_final_partial_batch",
            "Rust_polls_biased_shutdown_then_deadline_before_every_additional_receive",
            "Rust_writer_calls_require_atomic_all_or_none_behavior_while_Go_100_row_chunks_can_partially_commit",
            "Rust_has_saturating_conservation_stats_and_truthful_shutdown_write_abandonment",
        ]
    );
    assert_eq!(
        RUST_NONCLAIMS,
        [
            "Go_runPersistSources_runtime_execution_or_closed_batcher_behavior",
            "live_PostgreSQL_SQL_schema_plan_index_locking_or_affected_row_count",
            "a_concrete_Rust_PostgreSQL_source_writer_or_transaction_implementation",
            "exact_Go_construction_phase_ticker_schedule_or_ready_select_tie_winner",
            "Go_detached_goroutine_shutdown_drain_join_or_total_retention_behavior",
            "writer_error_retry_requeue_or_exactly_once_delivery",
            "remote_database_commit_or_rollback_after_Rust_writer_future_cancellation",
            "all_ones_or_other_nonfinite_Bloom_ApproximatedSize_projection",
            "Bloom_mutation_after_request_construction_or_concurrent_Bloom_access",
            "source_node_or_raw_Bloom_database_durability",
            "live_DNS_UDP_DHT_scrape_or_upstream_worker_behavior",
            "these_fixture_rows_do_not_replay_Rust_shutdown_timer_writer_error_or_counter_saturation_paths",
            "application_supervisor_deployment_metrics_logs_or_production_readiness",
        ]
    );
    for (_, bytes, digest) in GO_SOURCES {
        assert_eq!(sha256(bytes), digest);
    }
    for (_, bytes, digest) in PREREQUISITE_FIXTURES {
        assert_eq!(sha256(bytes), digest);
    }

    let fixtures = fixtures();
    assert_eq!(fixtures.len(), FIXTURE_IDS.len());
    for (index, fixture) in fixtures.iter().enumerate() {
        assert_eq!(fixture.id, FIXTURE_IDS[index]);
        assert_eq!(fixture.subsystem, "dht_crawler_persist_sources");
        assert_eq!(fixture.classification, ROW_CLASSIFICATIONS[index]);
        assert_eq!(fixture.expected.source.is_some(), index == 0);
    }
    let classifications = fixtures.iter().fold(BTreeMap::new(), |mut counts, row| {
        *counts.entry(row.classification.as_str()).or_insert(0_usize) += 1;
        counts
    });
    assert_eq!(
        classifications,
        BTreeMap::from([
            ("RUNTIME_EXACT", 2),
            ("RUNTIME_EXACT_TEST_HARNESS", 1),
            ("SOURCE_ONLY", 1),
        ])
    );

    let source = &fixtures[0];
    assert_eq!(
        source.oracle,
        Oracle {
            composition: "exact_production_source_AST_dependency_schema_factory_batcher_lifecycle_model_and_SQL_freshness_gate".to_owned(),
            determinism: "normalized_AST_plus_source_prerequisite_and_fixture_SHA256".to_owned(),
            harness: "source_only_no_worker_or_database_execution".to_owned(),
            database: "source_contract_only_without_live_PostgreSQL".to_owned(),
            clock: "source_contract_only_for_time_New_and_ticker".to_owned(),
        }
    );
    assert_eq!(source.input.kind, "source_contract");
    assert!(source.input.scrapes.is_empty());
    assert!(source.expected.models.is_empty());
    assert!(source.expected.bloom_observations.is_empty());
    assert!(source.expected.converter_call_info_hashes.is_empty());
    assert!(source.expected.duplicate_info_hashes.is_empty());
    assert!(source.expected.conversion_errors.is_empty());
    assert!(source.expected.first_occurrence_order.is_empty());
    assert!(!source.expected.run_persist_sources_executed);
    assert_source_contract(source.expected.source.as_ref().unwrap());

    let go_mod = String::from_utf8_lossy(GO_SOURCES[0].1);
    let go_sum = String::from_utf8_lossy(GO_SOURCES[1].1);
    let dependencies = &source.expected.source.as_ref().unwrap().dependencies;
    assert!(go_mod
        .lines()
        .any(|line| line.trim() == dependencies.go_mod_bloom_line));
    assert!(go_sum
        .lines()
        .any(|line| line.trim() == dependencies.go_sum_bloom_line));
    assert!(go_sum
        .lines()
        .any(|line| line.trim() == dependencies.go_sum_bloom_go_mod_line));

    let runtime_oracle = Oracle {
        composition:
            "actual_createTorrentSourceModel_with_actual_bloom_FromScrape_and_ApproximatedSize"
                .to_owned(),
        determinism: "pure_fixed_inputs_without_clock_database_goroutine_or_channel_execution"
            .to_owned(),
        harness: "model_conversion".to_owned(),
        database: "not_executed".to_owned(),
        clock: "not_read".to_owned(),
    };
    assert_eq!(fixtures[1].oracle, runtime_oracle);
    assert_eq!(fixtures[2].oracle, runtime_oracle);
    assert_eq!(
        fixtures[3].oracle,
        Oracle {
            composition: "source_pinned_first_wins_loop_test_harness_plus_actual_createTorrentSourceModel_bloom_FromScrape_and_ApproximatedSize".to_owned(),
            determinism: "pure_fixed_inputs_without_clock_database_goroutine_or_channel_execution".to_owned(),
            harness: "source_pinned_first_wins_loop_harness".to_owned(),
            database: "not_executed".to_owned(),
            clock: "not_read".to_owned(),
        }
    );
}

#[test]
fn recursive_schema_rejects_unknown_fields() {
    let values = FIXTURE_TEXT
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let mutations: &[(usize, &[&str])] = &[
        (0, &[]),
        (0, &["oracle"]),
        (0, &["input"]),
        (0, &["expected"]),
        (0, &["expected", "source"]),
        (0, &["expected", "source", "factory"]),
        (0, &["expected", "source", "batcher"]),
        (0, &["expected", "source", "lifecycle"]),
        (0, &["expected", "source", "worker"]),
        (0, &["expected", "source", "model"]),
        (0, &["expected", "source", "repository"]),
        (0, &["expected", "source", "schema"]),
        (0, &["expected", "source", "schema", "columns", "0"]),
        (0, &["expected", "source", "dependencies"]),
        (1, &["input", "scrapes", "0"]),
        (1, &["input", "scrapes", "0", "node"]),
        (1, &["input", "scrapes", "0", "seeders"]),
        (1, &["input", "scrapes", "0", "peers"]),
        (1, &["expected", "models", "0"]),
        (1, &["expected", "bloomObservations", "0"]),
        (3, &["input", "scrapes", "2", "seeders", "ranges", "0"]),
    ];
    for (row, path) in mutations {
        let mut value = values[*row].clone();
        let mut target = &mut value;
        for component in *path {
            target = if let Ok(index) = component.parse::<usize>() {
                &mut target.as_array_mut().unwrap()[index]
            } else {
                target.as_object_mut().unwrap().get_mut(*component).unwrap()
            };
        }
        target
            .as_object_mut()
            .unwrap()
            .insert("unknownField".to_owned(), serde_json::Value::Bool(true));
        assert!(
            serde_json::from_value::<Fixture>(value).is_err(),
            "unknown field accepted at path {path:?}"
        );
    }
}

#[tokio::test]
async fn runtime_rows_replay_the_actual_worker_with_exact_filters_order_and_rounding() {
    let fixtures = fixtures();
    for row in &fixtures[1..] {
        replay_runtime_row(row).await;
    }
}

async fn replay_runtime_row(row: &Fixture) {
    assert!(!row.input.scrapes.is_empty());
    assert!(!row.expected.run_persist_sources_executed);
    assert!(row.expected.source.is_none());
    assert!(row.expected.conversion_errors.is_empty());
    assert_eq!(
        row.input.kind,
        if row.id == FIXTURE_IDS[3] {
            "source_pinned_first_wins_loop_harness"
        } else {
            "model_conversion"
        }
    );

    if row.id == FIXTURE_IDS[2] {
        assert_eq!(row.input.scrapes.len(), 1);
        let scrape = &row.input.scrapes[0];
        assert_eq!(scrape.seeders.raw_ips, ["0a0002ae"]);
        assert!(scrape.seeders.ranges.is_empty());
        assert!(scrape.peers.raw_ips.is_empty());
        assert!(scrape.peers.ranges.is_empty());
    }

    let mut requests = Vec::with_capacity(row.input.scrapes.len());
    let mut observations = Vec::with_capacity(row.input.scrapes.len());
    let mut seen = HashSet::with_capacity(row.input.scrapes.len());
    let mut duplicates = Vec::new();
    let mut first_order = Vec::new();
    for scrape in &row.input.scrapes {
        let (request, observation) = build_request(scrape);
        if !seen.insert(request.info_hash) {
            duplicates.push(scrape.info_hash.clone());
        } else {
            first_order.push(scrape.info_hash.clone());
        }
        requests.push(request);
        observations.push(observation);
    }
    assert_eq!(observations, row.expected.bloom_observations);
    assert_eq!(duplicates, row.expected.duplicate_info_hashes);
    assert_eq!(first_order, row.expected.first_occurrence_order);
    assert_eq!(first_order, row.expected.converter_call_info_hashes);

    if row.id == FIXTURE_IDS[2] {
        assert_eq!(
            requests[0]
                .seeders_bloom
                .as_bytes()
                .iter()
                .map(|byte| byte.count_ones())
                .sum::<u32>(),
            1
        );
        assert_eq!(requests[0].seeders_bloom.approximated_size(), 1);
        assert_eq!(requests[0].peers_bloom, ScrapeBloomFilter::EMPTY);
        assert_eq!(requests[0].peers_bloom.approximated_size(), 0);
    }

    let expected_writes = row
        .expected
        .models
        .iter()
        .map(assert_go_model_and_project_worker_write)
        .collect::<Vec<_>>();
    assert_eq!(
        expected_writes
            .iter()
            .map(|write| write.info_hash.to_string())
            .collect::<Vec<_>>(),
        first_order
    );

    if row.id == FIXTURE_IDS[1] {
        assert_eq!(
            requests[1].source_node_addr,
            SocketAddr::V6(SocketAddrV6::new("fe80::2".parse().unwrap(), 7002, 0, 42,))
        );
    }

    let writer = Arc::new(ScriptWriter::default());
    let (input, receiver) = dht_persist_source_channel();
    for request in requests {
        input.send(request).await.unwrap();
    }
    drop(input);
    let unique = expected_writes.len();
    let raw = row.input.scrapes.len();
    let (worker, stats) = DhtPersistSourceWorker::with_config(
        receiver,
        writer.clone(),
        DhtPersistSourceWorkerConfig {
            batch_limit: NonZeroUsize::new(raw).unwrap(),
            batch_interval: Duration::from_secs(60),
        },
    );

    assert_eq!(
        worker.run(pending()).await,
        DhtPersistSourceWorkerExit::InputClosed,
        "fixture {}",
        row.id
    );
    assert_eq!(writer.calls(), vec![expected_writes], "fixture {}", row.id);
    assert_eq!(
        stats.snapshot(),
        DhtPersistSourceWorkerStats {
            dequeued: raw as u64,
            batches: 1,
            input_duplicates_dropped: (raw - unique) as u64,
            writer_calls: 1,
            writer_successes: 1,
            writer_sources_submitted: unique as u64,
            sources_persisted: unique as u64,
            ..DhtPersistSourceWorkerStats::default()
        },
        "fixture {}",
        row.id
    );
}

fn build_request(scrape: &Scrape) -> (DhtPersistSourceRequest, BloomObservation) {
    let info_hash = parse_info_hash(&scrape.info_hash);
    let seeders = build_filter(&scrape.seeders);
    let peers = build_filter(&scrape.peers);
    (
        DhtPersistSourceRequest {
            info_hash,
            source_node_addr: parse_address(&scrape.node),
            seeders_bloom: seeders,
            peers_bloom: peers,
        },
        BloomObservation {
            info_hash: scrape.info_hash.clone(),
            seeders_bloom_sha256: sha256(seeders.as_bytes()),
            peers_bloom_sha256: sha256(peers.as_bytes()),
            seeders_approximated: seeders.approximated_size(),
            peers_approximated: peers.approximated_size(),
        },
    )
}

fn assert_go_model_and_project_worker_write(model: &Model) -> DhtSourceWrite {
    assert_eq!(model.source, "dht");
    assert!(model.seeders_valid);
    assert!(model.leechers_valid);
    assert_eq!(model.seen_count, 1);
    assert!(!model.import_id_valid);
    assert!(!model.published_at_valid);
    assert!(model.created_at_zero);
    assert!(model.updated_at_zero);
    assert!(!model.source_node_retained);
    assert!(!model.raw_seeders_bloom_retained);
    assert!(!model.raw_peers_bloom_retained);
    DhtSourceWrite {
        info_hash: parse_info_hash(&model.info_hash),
        seeders: model.seeders,
        leechers: model.leechers,
    }
}

fn build_filter(input: &FilterInput) -> ScrapeBloomFilter {
    let mut filter = ScrapeBloomFilter::EMPTY;
    for raw_ip in &input.raw_ips {
        filter.add_ip_bytes(&decode_ip_hex(raw_ip));
    }
    for range in &input.ranges {
        let base = decode_ip_hex(&range.base);
        for offset in 0..range.count {
            filter.add_ip_bytes(&add_big_endian(
                &base,
                u64::try_from(offset).expect("fixture filter range offset exceeds u64"),
            ));
        }
    }
    filter
}

fn parse_info_hash(value: &str) -> Id20 {
    assert_eq!(value.len(), 40, "fixture info hash is not 40 hex digits");
    Id20::from_slice(&decode_lower_hex(value))
        .unwrap_or_else(|error| panic!("invalid fixture info hash {value}: {error}"))
}

fn decode_ip_hex(value: &str) -> Vec<u8> {
    let decoded = decode_lower_hex(value);
    assert!(
        matches!(decoded.len(), 4 | 16),
        "fixture IP hex is {} bytes; expected 4 or 16",
        decoded.len()
    );
    decoded
}

fn decode_lower_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0, "odd fixture hex length: {value}");
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]))
        .collect()
}

fn hex_nibble(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        _ => panic!("fixture hex digit is not lowercase hexadecimal: {value:?}"),
    }
}

fn add_big_endian(base: &[u8], offset: u64) -> Vec<u8> {
    let mut result = base.to_vec();
    let mut carry = offset;
    for byte in result.iter_mut().rev() {
        if carry == 0 {
            break;
        }
        let sum = u128::from(*byte) + u128::from(carry);
        *byte = (sum & u128::from(u8::MAX)) as u8;
        carry = u64::try_from(sum >> 8).expect("fixture range carry exceeds u64");
    }
    assert_eq!(carry, 0, "fixture filter range overflows address width");
    result
}

#[test]
fn fixture_hex_width_case_and_range_overflow_are_rejected() {
    assert_eq!(decode_ip_hex("7f000001"), [127, 0, 0, 1]);
    assert_eq!(parse_info_hash(&"ab".repeat(20)).as_bytes(), &[0xab; 20]);

    for malformed in ["0A000001", "00", "0000000000", "abc"] {
        assert!(
            std::panic::catch_unwind(|| decode_ip_hex(malformed)).is_err(),
            "malformed fixture IP was accepted: {malformed}"
        );
    }
    assert!(std::panic::catch_unwind(|| parse_info_hash(&"AB".repeat(20))).is_err());
    assert!(std::panic::catch_unwind(|| parse_info_hash("00")).is_err());
    assert!(std::panic::catch_unwind(|| add_big_endian(&[u8::MAX; 4], 1)).is_err());
}

fn parse_address(address: &Address) -> SocketAddr {
    match IpAddr::from_str(&address.ip)
        .unwrap_or_else(|error| panic!("invalid fixture IP {}: {error}", address.ip))
    {
        IpAddr::V4(ip) => {
            assert_eq!(address.scope, 0, "IPv4 fixture address has a scope");
            SocketAddr::new(IpAddr::V4(ip), address.port)
        }
        IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(ip, address.port, 0, address.scope)),
    }
}
