use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::future::pending;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bitmagnet_dht::{
    dht_get_peers_channel, dht_info_hash_triage_channel, dht_scrape_channel, DhtGetPeersReceiver,
    DhtInfoHashTriageRequest, DhtScrapeReceiver, Id20, DHT_GET_PEERS_ROUTE_CAPACITY,
    DHT_SCRAPE_ROUTE_CAPACITY,
};
use bitmagnet_model::FilesStatus;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::{oneshot, Notify};

use super::*;

const FIXTURE_TEXT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/dht_crawler_info_hash_triage.jsonl"
));
const FIXTURE_BYTES: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/dht_crawler_info_hash_triage.jsonl"
));
const FIXTURE_SHA256: &str = "52eda840f872225cc34f8cf12edc2e4621e8a1fef569abf34a50f4a3bd9896f8";
const FIXTURE_IDS: [&str; 7] = [
    "production_source_factory_and_lifecycle_contract",
    "dedup_filter_lookup_and_decision_matrix",
    "empty_filter_result_skips_database_and_outputs",
    "filter_error_drops_batch_and_continues",
    "database_error_drops_batch_and_continues",
    "cancellation_at_blocked_get_peers_send",
    "cancellation_at_blocked_scrape_send",
];
const ROW_CLASSIFICATIONS: [&str; 7] = [
    "SOURCE_ONLY",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
];
const RUST_EXECUTION_PARTITION: [(&str, &str); 7] = [
    (
        FIXTURE_IDS[0],
        "SOURCE_ONLY_NO_RUST_RUNTIME_OR_GORM_SQL_REPLAY",
    ),
    (
        FIXTURE_IDS[1],
        "RUST_ACTUAL_WORKER_FILTER_LOOKUP_AND_SORTED_ACTION_MULTISET_REPLAY",
    ),
    (
        FIXTURE_IDS[2],
        "RUST_ACTUAL_WORKER_EMPTY_FILTER_AND_EOF_REPLAY",
    ),
    (
        FIXTURE_IDS[3],
        "RUST_ACTUAL_WORKER_FILTER_ERROR_CONTINUATION_AND_EOF_REPLAY",
    ),
    (
        FIXTURE_IDS[4],
        "RUST_ACTUAL_WORKER_LOOKUP_ERROR_CONTINUATION_AND_EOF_REPLAY",
    ),
    (
        FIXTURE_IDS[5],
        "RUST_ACTUAL_WORKER_BLOCKED_GET_PEERS_BIASED_SHUTDOWN_REPLAY",
    ),
    (
        FIXTURE_IDS[6],
        "RUST_ACTUAL_WORKER_BLOCKED_SCRAPE_BIASED_SHUTDOWN_REPLAY",
    ),
];
const DELIBERATE_RUST_DELTAS: [&str; 10] = [
    "Rust_owns_one_joined_worker_future_instead_of_a_detached_Go_goroutine",
    "Rust_input_EOF_is_typed_and_does_not_repeat_closed_output_zero_batches",
    "Rust_batching_is_first_item_relative_without_a_detached_batcher_or_buffered_batch_output",
    "Rust_routes_filtered_hashes_in_first_filtered_order_instead_of_Go_map_iteration_order",
    "Rust_filter_and_lookup_are_abstract_async_collaborators_not_a_GORM_DAO_or_live_Postgres",
    "Rust_collaborator_failures_are_counted_and_continue_without_claiming_Go_log_delivery",
    "Rust_output_closure_and_shutdown_return_typed_exact_suffix_accounting",
    "Rust_output_send_reservations_are_cancellation_safe_and_shutdown_biased",
    "Rust_rejects_foreign_filter_hashes_as_a_fail_closed_hardening_delta",
    "Rust_uses_an_injected_clock_while_preserving_strict_stale_before_for_reached_checks",
];
const RUST_NONCLAIMS: [&str; 14] = [
    "exact_Go_map_iteration_SQL_result_or_downstream_delivery_order",
    "exact_Go_rescrape_boundary_wall_clock_or_per_item_time_Now_schedule",
    "live_PostgreSQL_schema_query_plan_indexes_transactions_or_result_order",
    "production_blocking_bloom_state_buffering_or_flush_behavior",
    "production_Go_batching_ticker_input_close_or_output_close_behavior",
    "downstream_get_peers_or_scrape_worker_callbacks_concurrency_or_completion",
    "Go_select_tie_resolution_eager_In_operand_side_effects_or_fairness",
    "Go_log_messages_levels_fields_or_delivery",
    "cross_route_total_retention_throughput_backpressure_or_waiter_fairness",
    "closed_Go_infoHashTriage_output_runtime_behavior",
    "end_to_end_live_DHT_traffic_network_peers_or_external_services",
    "upstream_sample_infohashes_origin_has_peers_or_ignore_hash_provenance",
    "application_supervisor_database_adapter_deployment_or_production_readiness",
    "concurrent_upstream_send_permit_drain_accounting_outside_prequeued_fixture_inputs",
];

const GO_SOURCES: [(&str, &[u8], &str); 15] = [
    (
        "internal/blocking/manager.go",
        include_bytes!("../../../../internal/blocking/manager.go"),
        "d32ef7b0fb1eeadaeb1134f49b1046911c27312d2383b402d5989c8bc830130f",
    ),
    (
        "internal/concurrency/batching_channel.go",
        include_bytes!("../../../../internal/concurrency/batching_channel.go"),
        "72b3c9fd5fbc8ecbfb0ba2bc2ed5e6c1d45de01f03d3e015b2467f114ec70975",
    ),
    (
        "internal/concurrency/buffered_concurrent_channel.go",
        include_bytes!("../../../../internal/concurrency/buffered_concurrent_channel.go"),
        "4be882800ec66d0c1709319fe029d61773c3f4a37bdb409e3a2f7d5d415d954c",
    ),
    (
        "internal/database/dao/torrents.gen.go",
        include_bytes!("../../../../internal/database/dao/torrents.gen.go"),
        "59dd2534bdf02f356230ba602015a1ee8f9fc55d7203660776feeab4293981a3",
    ),
    (
        "internal/database/dao/torrents_torrent_sources.gen.go",
        include_bytes!("../../../../internal/database/dao/torrents_torrent_sources.gen.go"),
        "8efbb42ea9fa9aee021ef41528d0821600ebf703db8c76a4dc706a22e64ca31a",
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
        "internal/dhtcrawler/infohash_triage.go",
        include_bytes!("../../../../internal/dhtcrawler/infohash_triage.go"),
        "7950da30f12ec9d54ba830c7465a749d4625ad0fd7e0aa2bebbdc4cef2027f02",
    ),
    (
        "internal/dhtcrawler/sample_infohashes.go",
        include_bytes!("../../../../internal/dhtcrawler/sample_infohashes.go"),
        "483b9037673dce82f9026f2aec9448812f804c13484fd0bd2f55fcfc70a52983",
    ),
    (
        "internal/model/files_status_enum.go",
        include_bytes!("../../../../internal/model/files_status_enum.go"),
        "5f723e62282dcc82e2037c96d1423f81075cddca24b14e29a544340f5650e9a0",
    ),
    (
        "internal/model/null.go",
        include_bytes!("../../../../internal/model/null.go"),
        "b9c3762d286201140c51cd3ca2630361fb35fb76464c297a37d85037d1be782d",
    ),
    (
        "internal/model/torrents.gen.go",
        include_bytes!("../../../../internal/model/torrents.gen.go"),
        "3c3fb6debefdca25530b9f3cecd818e8b98817528f36ff87a76dfee79cad84e0",
    ),
    (
        "internal/model/torrents_torrent_sources.gen.go",
        include_bytes!("../../../../internal/model/torrents_torrent_sources.gen.go"),
        "a5431060dd68f51ac77aced27f4a3c1481124054bef43365d368bded4a405b41",
    ),
    (
        "internal/protocol/id.go",
        include_bytes!("../../../../internal/protocol/id.go"),
        "e1947e2b4af4cc008f5bb8cf5000ebfe784a82e119cb0418c2a74c3ed5f8c26f",
    ),
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
    database: String,
    clock: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Input {
    kind: String,
    batches: Vec<Vec<Request>>,
    filter_steps: Vec<FilterStep>,
    database_rows: Vec<DatabaseRow>,
    database_error: String,
    cancel_at_lane: String,
    save_files_threshold: u64,
    rescrape_threshold_seconds: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Request {
    info_hash: String,
    node: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FilterStep {
    result: Vec<String>,
    error: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DatabaseRow {
    info_hash: String,
    files_status: String,
    files_count: Option<u64>,
    seeders: Option<u64>,
    leechers: Option<u64>,
    updated_at: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Expected {
    filter_calls: Vec<Vec<String>>,
    sql_args: Vec<String>,
    database_query_calls: usize,
    actions: Vec<Action>,
    get_peers_in_calls: usize,
    scrape_in_calls: usize,
    block_calls: usize,
    flush_calls: usize,
    run_returned: bool,
    context_cancelled: bool,
    continued_after_error: bool,
    source: Option<Source>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Action {
    action: String,
    info_hash: String,
    node: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Source {
    input_capacity: usize,
    batch_limit: usize,
    batch_interval_seconds: u64,
    batch_output_capacity: usize,
    get_peers_input_capacity: usize,
    get_peers_concurrency: usize,
    scrape_input_capacity: usize,
    scrape_concurrency: usize,
    default_scaling_factor: usize,
    default_save_files_threshold: u64,
    default_rescrape_threshold_seconds: i64,
    first_duplicate_wins: bool,
    filter_receives_first_unique_order: bool,
    filter_before_database: bool,
    filtered_hashes_deduped_for_routing: bool,
    filtered_duplicates_remain_sql_args: bool,
    database_duplicate_last_wins: bool,
    selected_columns: Vec<String>,
    join_kind: String,
    join_source: String,
    get_peers_precedes_scrape: bool,
    strict_stale_before: bool,
    time_now_read_per_reached_staleness_check: bool,
    error_break_continues_outer_loop: bool,
    sends_cancellation_aware: bool,
    closed_out_checks_open_boolean: bool,
    worker_detached: bool,
    worker_joined: bool,
    no_stats: bool,
    normalized_ast_sha256: BTreeMap<String, String>,
    source_sha256: BTreeMap<String, String>,
    go_mod_sqlmock_line: String,
    go_sum_sqlmock_line: String,
    evidence: String,
    nonclaims: Vec<String>,
}

enum Step<T> {
    Ok(T),
    Err(String),
}

struct ScriptFilter {
    steps: Mutex<VecDeque<Step<Vec<Id20>>>>,
    calls: Mutex<Vec<Vec<Id20>>>,
}

impl ScriptFilter {
    fn new(steps: impl IntoIterator<Item = Step<Vec<Id20>>>) -> Arc<Self> {
        Arc::new(Self {
            steps: Mutex::new(steps.into_iter().collect()),
            calls: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> Vec<Vec<Id20>> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl DhtInfoHashBlockFilter for ScriptFilter {
    async fn filter(&self, info_hashes: &[Id20]) -> Result<Vec<Id20>, TriageCollaboratorError> {
        self.calls.lock().unwrap().push(info_hashes.to_vec());
        match self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted filter step")
        {
            Step::Ok(result) => Ok(result),
            Step::Err(message) => Err(Box::new(io::Error::other(message))),
        }
    }
}

struct ScriptLookup {
    steps: Mutex<VecDeque<Step<Vec<DhtTorrentTriageRow>>>>,
    calls: Mutex<Vec<Vec<Id20>>>,
}

impl ScriptLookup {
    fn new(steps: impl IntoIterator<Item = Step<Vec<DhtTorrentTriageRow>>>) -> Arc<Self> {
        Arc::new(Self {
            steps: Mutex::new(steps.into_iter().collect()),
            calls: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> Vec<Vec<Id20>> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl DhtTorrentTriageLookup for ScriptLookup {
    async fn lookup(
        &self,
        info_hashes: &[Id20],
    ) -> Result<Vec<DhtTorrentTriageRow>, TriageCollaboratorError> {
        self.calls.lock().unwrap().push(info_hashes.to_vec());
        match self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted lookup step")
        {
            Step::Ok(result) => Ok(result),
            Step::Err(message) => Err(Box::new(io::Error::other(message))),
        }
    }
}

struct FixedClock {
    now: i64,
    calls: AtomicUsize,
}

impl FixedClock {
    fn new(now: i64) -> Arc<Self> {
        Arc::new(Self {
            now,
            calls: AtomicUsize::new(0),
        })
    }
}

impl DhtInfoHashTriageClock for FixedClock {
    fn now_unix_micros(&self) -> i64 {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.now
    }
}

struct Replay {
    exit: DhtInfoHashTriageWorkerExit,
    filter_calls: Vec<Vec<Id20>>,
    lookup_calls: Vec<Vec<Id20>>,
    get_peers_actions: Vec<Action>,
    scrape_actions: Vec<Action>,
    actions: Vec<Action>,
    clock_calls: usize,
    stats: DhtInfoHashTriageStats,
}

fn fixtures() -> Vec<Fixture> {
    FIXTURE_TEXT
        .lines()
        .enumerate()
        .map(|(index, line)| {
            serde_json::from_str(line)
                .unwrap_or_else(|error| panic!("fixture line {} is invalid: {error}", index + 1))
        })
        .collect()
}

fn fixture(id: &str) -> Fixture {
    fixtures()
        .into_iter()
        .find(|fixture| fixture.id == id)
        .unwrap_or_else(|| panic!("fixture row {id} is missing"))
}

fn sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut value = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut value, "{byte:02x}").unwrap();
    }
    value
}

fn id(value: &str) -> Id20 {
    Id20::from_hex(value).unwrap_or_else(|error| panic!("invalid fixture ID {value}: {error}"))
}

fn request(value: &Request) -> DhtInfoHashTriageRequest {
    DhtInfoHashTriageRequest {
        info_hash: id(&value.info_hash),
        source_node_addr: value
            .node
            .parse::<SocketAddr>()
            .unwrap_or_else(|error| panic!("invalid fixture node {}: {error}", value.node)),
    }
}

fn fixture_hash(value: u8) -> String {
    format!("{value:040x}")
}

fn fixture_request(hash: u8, node: u8) -> Request {
    Request {
        info_hash: fixture_hash(hash),
        node: format!("192.0.2.{node}:{}", 7_000 + u16::from(node)),
    }
}

fn filter_step(values: &[u8], error: &str) -> FilterStep {
    FilterStep {
        result: values.iter().map(|value| fixture_hash(*value)).collect(),
        error: error.to_owned(),
    }
}

fn database_row(
    hash: u8,
    files_status: &str,
    files_count: Option<u64>,
    seeders: Option<u64>,
    leechers: Option<u64>,
    updated_at: &str,
) -> DatabaseRow {
    DatabaseRow {
        info_hash: fixture_hash(hash),
        files_status: files_status.to_owned(),
        files_count,
        seeders,
        leechers,
        updated_at: updated_at.to_owned(),
    }
}

fn action(kind: &str, hash: u8, node: u8) -> Action {
    let request = fixture_request(hash, node);
    Action {
        action: kind.to_owned(),
        info_hash: request.info_hash,
        node: request.node,
    }
}

fn runtime_expected(
    filter_calls: Vec<Vec<String>>,
    sql_args: Vec<String>,
    database_query_calls: usize,
    actions: Vec<Action>,
    get_peers_in_calls: usize,
    scrape_in_calls: usize,
    continued_after_error: bool,
) -> Expected {
    Expected {
        filter_calls,
        sql_args,
        database_query_calls,
        actions,
        get_peers_in_calls,
        scrape_in_calls,
        block_calls: 0,
        flush_calls: 0,
        run_returned: true,
        context_cancelled: true,
        continued_after_error,
        source: None,
    }
}

fn runtime_oracle(determinism: &str) -> Oracle {
    Oracle {
        composition: "actual_crawler_runInfoHashTriage_with_manual_interface_lanes_scripted_blocking_Manager_and_sqlmock_DAO".to_owned(),
        determinism: determinism.to_owned(),
        database: "actual_GORM_DAO_query_over_sqlmock_without_live_Postgres".to_owned(),
        clock: "production_time_Now_with_runtime_rows_far_from_the_staleness_boundary".to_owned(),
    }
}

fn input_base(kind: &str) -> Input {
    Input {
        kind: kind.to_owned(),
        batches: Vec::new(),
        filter_steps: Vec::new(),
        database_rows: Vec::new(),
        database_error: String::new(),
        cancel_at_lane: String::new(),
        save_files_threshold: 100,
        rescrape_threshold_seconds: 2_592_000,
    }
}

fn assert_runtime_fixture_contract(fixture: &Fixture) {
    let future = "9999-12-31T23:59:59Z";
    let epoch = "1970-01-01T00:00:00Z";
    let (oracle, input, expected) = match fixture.id.as_str() {
        "dedup_filter_lookup_and_decision_matrix" => {
            let mut input = input_base("dedup_filter_query_and_route");
            input.batches = vec![
                vec![
                    fixture_request(1, 1),
                    fixture_request(1, 11),
                    fixture_request(2, 2),
                    fixture_request(3, 3),
                    fixture_request(4, 4),
                    fixture_request(5, 5),
                    fixture_request(6, 6),
                    fixture_request(7, 7),
                    fixture_request(8, 8),
                    fixture_request(9, 9),
                    fixture_request(10, 10),
                ],
                vec![fixture_request(30, 30)],
            ];
            input.filter_steps = vec![
                filter_step(&[1, 3, 4, 5, 6, 7, 8, 9, 10], ""),
                filter_step(&[], ""),
            ];
            input.database_rows = vec![
                database_row(3, "no_info", None, None, None, future),
                database_row(4, "multi", None, None, None, future),
                database_row(5, "over_threshold", Some(100), Some(4), Some(5), future),
                database_row(6, "single", None, None, None, future),
                database_row(7, "multi", Some(1), Some(6), Some(7), epoch),
                database_row(8, "single", None, Some(8), Some(9), future),
                database_row(9, "over_threshold", Some(101), Some(10), Some(11), future),
                database_row(10, "over_threshold", None, Some(12), Some(13), future),
            ];
            let filter_calls = vec![(1..=10).map(fixture_hash).collect(), vec![fixture_hash(30)]];
            let sql_args = std::iter::once("dht".to_owned())
                .chain([1, 3, 4, 5, 6, 7, 8, 9, 10].into_iter().map(fixture_hash))
                .collect();
            let mut actions = vec![
                action("get_peers", 1, 1),
                action("get_peers", 3, 3),
                action("get_peers", 4, 4),
                action("get_peers", 5, 5),
                action("get_peers", 10, 10),
                action("scrape", 6, 6),
                action("scrape", 7, 7),
            ];
            actions.sort();
            (
                runtime_oracle("sorted_action_multiset_with_fixed_far_boundary_rows"),
                input,
                runtime_expected(filter_calls, sql_args, 1, actions, 5, 2, false),
            )
        }
        "empty_filter_result_skips_database_and_outputs" => {
            let mut input = input_base("empty_filter_result");
            input.batches = vec![vec![fixture_request(20, 20)], vec![fixture_request(31, 31)]];
            input.filter_steps = vec![filter_step(&[], ""), filter_step(&[], "")];
            (
                runtime_oracle("independent_GORM_query_counter_zero_and_no_queued_action"),
                input,
                runtime_expected(
                    vec![vec![fixture_hash(20)], vec![fixture_hash(31)]],
                    Vec::new(),
                    0,
                    Vec::new(),
                    0,
                    0,
                    false,
                ),
            )
        }
        "filter_error_drops_batch_and_continues" => {
            let mut input = input_base("filter_error_then_continue");
            input.batches = vec![vec![fixture_request(21, 21)], vec![fixture_request(22, 22)]];
            input.filter_steps = vec![
                filter_step(&[], "oracle filter failure"),
                filter_step(&[], ""),
            ];
            (
                runtime_oracle(
                    "two_observed_filter_calls_with_independent_GORM_query_counter_zero",
                ),
                input,
                runtime_expected(
                    vec![vec![fixture_hash(21)], vec![fixture_hash(22)]],
                    Vec::new(),
                    0,
                    Vec::new(),
                    0,
                    0,
                    true,
                ),
            )
        }
        "database_error_drops_batch_and_continues" => {
            let mut input = input_base("database_error_then_continue");
            input.batches = vec![vec![fixture_request(23, 23)], vec![fixture_request(24, 24)]];
            input.filter_steps = vec![filter_step(&[23], ""), filter_step(&[], "")];
            input.database_error = "oracle database failure".to_owned();
            (
                runtime_oracle(
                    "one_independently_counted_expected_query_then_second_observed_filter_call",
                ),
                input,
                runtime_expected(
                    vec![vec![fixture_hash(23)], vec![fixture_hash(24)]],
                    vec!["dht".to_owned(), fixture_hash(23)],
                    1,
                    Vec::new(),
                    0,
                    0,
                    true,
                ),
            )
        }
        "cancellation_at_blocked_get_peers_send" => {
            let mut input = input_base("cancel_blocked_send");
            input.batches = vec![vec![fixture_request(25, 25)]];
            input.filter_steps = vec![filter_step(&[25], "")];
            input.cancel_at_lane = "get_peers".to_owned();
            (
                runtime_oracle("one_independently_counted_query_then_unbuffered_send_access_observed_before_cancel_and_join"),
                input,
                runtime_expected(
                    vec![vec![fixture_hash(25)]],
                    vec!["dht".to_owned(), fixture_hash(25)],
                    1,
                    Vec::new(),
                    1,
                    0,
                    false,
                ),
            )
        }
        "cancellation_at_blocked_scrape_send" => {
            let mut input = input_base("cancel_blocked_send");
            input.batches = vec![vec![fixture_request(26, 26)]];
            input.filter_steps = vec![filter_step(&[26], "")];
            input.database_rows = vec![database_row(26, "single", None, None, None, future)];
            input.cancel_at_lane = "scrape".to_owned();
            (
                runtime_oracle("one_independently_counted_query_then_unbuffered_send_access_observed_before_cancel_and_join"),
                input,
                runtime_expected(
                    vec![vec![fixture_hash(26)]],
                    vec!["dht".to_owned(), fixture_hash(26)],
                    1,
                    Vec::new(),
                    0,
                    1,
                    false,
                ),
            )
        }
        id => panic!("unexpected runtime fixture {id}"),
    };
    assert_eq!(fixture.oracle, oracle);
    assert_eq!(fixture.input, input);
    assert_eq!(fixture.expected, expected);
}

fn parse_database_row(row: &DatabaseRow) -> DhtTorrentTriageRow {
    let files_status = row
        .files_status
        .parse::<FilesStatus>()
        .unwrap_or_else(|error| panic!("invalid files status {}: {error}", row.files_status));
    let updated_at = match row.updated_at.as_str() {
        "1970-01-01T00:00:00Z" => 0,
        "9999-12-31T23:59:59Z" => 253_402_300_799_000_000,
        value => panic!("unexpected fixture timestamp {value}"),
    };
    DhtTorrentTriageRow {
        info_hash: id(&row.info_hash),
        files_status,
        files_count: row.files_count,
        dht_seeders: row.seeders,
        dht_leechers: row.leechers,
        dht_updated_at_unix_micros: Some(updated_at),
    }
}

fn scripted_filter(input: &Input) -> Arc<ScriptFilter> {
    ScriptFilter::new(input.filter_steps.iter().map(|step| {
        if step.error.is_empty() {
            Step::Ok(step.result.iter().map(|value| id(value)).collect())
        } else {
            Step::Err(step.error.clone())
        }
    }))
}

fn scripted_lookup(input: &Input) -> Arc<ScriptLookup> {
    let calls = input
        .filter_steps
        .iter()
        .filter(|step| step.error.is_empty() && !step.result.is_empty())
        .count();
    assert!(calls <= 1, "fixture needs at most one scripted lookup");
    let steps = if calls == 0 {
        Vec::new()
    } else if input.database_error.is_empty() {
        vec![Step::Ok(
            input.database_rows.iter().map(parse_database_row).collect(),
        )]
    } else {
        vec![Step::Err(input.database_error.clone())]
    };
    ScriptLookup::new(steps)
}

fn worker_config(input: &Input) -> DhtInfoHashTriageConfig {
    DhtInfoHashTriageConfig {
        batch_limit: NonZeroUsize::new(input.batches[0].len()).unwrap(),
        batch_interval: Duration::from_secs(60 * 60),
        save_files_threshold: input.save_files_threshold,
        rescrape_threshold: Duration::from_secs(
            input.rescrape_threshold_seconds.try_into().unwrap(),
        ),
    }
}

async fn enqueue_fixture_batches(
    input: &bitmagnet_dht::DhtInfoHashTriageInput,
    batches: &[Vec<Request>],
) {
    for batch in batches {
        for value in batch {
            input.send(request(value)).await.unwrap();
        }
    }
}

fn action_from_request(kind: &str, request: DhtInfoHashTriageRequest) -> Action {
    Action {
        action: kind.to_owned(),
        info_hash: request.info_hash.to_hex(),
        node: request.source_node_addr.to_string(),
    }
}

fn drain_get_peers_actions(receiver: &mut DhtGetPeersReceiver) -> Vec<Action> {
    let mut actions = Vec::new();
    while let Ok(request) = receiver.try_recv() {
        actions.push(action_from_request("get_peers", request));
    }
    actions
}

fn drain_scrape_actions(receiver: &mut DhtScrapeReceiver) -> Vec<Action> {
    let mut actions = Vec::new();
    while let Ok(request) = receiver.try_recv() {
        actions.push(action_from_request("scrape", request));
    }
    actions
}

async fn replay(fixture: &Fixture) -> Replay {
    let filter = scripted_filter(&fixture.input);
    let lookup = scripted_lookup(&fixture.input);
    let total = fixture.input.batches.iter().map(Vec::len).sum::<usize>();
    let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(total.max(1)).unwrap());
    let (get_peers, mut get_peers_receiver) = dht_get_peers_channel();
    let (scrape, mut scrape_receiver) = dht_scrape_channel();
    let clock = FixedClock::new(1_700_000_000_000_000);
    let (worker, stats) = DhtInfoHashTriageWorker::with_config(
        receiver,
        get_peers,
        scrape,
        filter.clone(),
        lookup.clone(),
        clock.clone(),
        worker_config(&fixture.input),
    );
    enqueue_fixture_batches(&input, &fixture.input.batches).await;
    drop(input);
    let exit = worker.run(pending()).await;
    let get_peers_actions = drain_get_peers_actions(&mut get_peers_receiver);
    let scrape_actions = drain_scrape_actions(&mut scrape_receiver);
    let mut actions = get_peers_actions
        .iter()
        .chain(&scrape_actions)
        .cloned()
        .collect::<Vec<_>>();
    actions.sort();
    Replay {
        exit,
        filter_calls: filter.calls(),
        lookup_calls: lookup.calls(),
        get_peers_actions,
        scrape_actions,
        actions,
        clock_calls: clock.calls.load(Ordering::Relaxed),
        stats: stats.snapshot(),
    }
}

fn expected_id_calls(expected: &[Vec<String>]) -> Vec<Vec<Id20>> {
    expected
        .iter()
        .map(|call| call.iter().map(|value| id(value)).collect())
        .collect()
}

fn assert_lookup_calls(replay: &Replay, expected: &Expected) {
    assert_eq!(replay.lookup_calls.len(), expected.database_query_calls);
    if expected.database_query_calls == 0 {
        assert!(expected.sql_args.is_empty());
        assert!(replay.lookup_calls.is_empty());
    } else {
        assert_eq!(expected.sql_args[0], "dht");
        assert_eq!(
            replay.lookup_calls[0],
            expected.sql_args[1..]
                .iter()
                .map(|value| id(value))
                .collect::<Vec<_>>()
        );
    }
}

fn assert_conserved(stats: DhtInfoHashTriageStats) {
    assert_eq!(
        stats.dequeued,
        stats.input_duplicates_dropped
            + stats.filter_suppressed
            + stats.filter_failure_dropped
            + stats.filter_contract_dropped
            + stats.lookup_failure_dropped
            + stats.get_peers_queued
            + stats.scrape_queued
            + stats.discarded
            + stats.shutdown_batch_dropped
            + stats.route_closed_batch_dropped,
        "every dequeued occurrence has one terminal classification: {stats:?}"
    );
}

fn source_fixture_expected() -> Input {
    input_base("source_contract")
}

fn assert_source_contract(source: &Source) {
    assert_eq!(
        (
            source.input_capacity,
            source.batch_limit,
            source.batch_interval_seconds,
            source.batch_output_capacity,
            source.get_peers_input_capacity,
            source.get_peers_concurrency,
            source.scrape_input_capacity,
            source.scrape_concurrency,
        ),
        (100, 1_000, 20, 1, 100, 200, 100, 200)
    );
    assert_eq!(source.default_scaling_factor, 10);
    assert_eq!(source.default_save_files_threshold, 100);
    assert_eq!(source.default_rescrape_threshold_seconds, 2_592_000);
    assert!(source.first_duplicate_wins);
    assert!(source.filter_receives_first_unique_order);
    assert!(source.filter_before_database);
    assert!(source.filtered_hashes_deduped_for_routing);
    assert!(source.filtered_duplicates_remain_sql_args);
    assert!(source.database_duplicate_last_wins);
    assert_eq!(
        source.selected_columns,
        [
            "torrents.info_hash",
            "torrents.files_status",
            "torrents.files_count",
            "torrents_torrent_sources.seeders",
            "torrents_torrent_sources.leechers",
            "torrents_torrent_sources.updated_at",
        ]
    );
    assert_eq!(source.join_kind, "left_join");
    assert_eq!(source.join_source, "dht");
    assert!(source.get_peers_precedes_scrape);
    assert!(source.strict_stale_before);
    assert!(source.time_now_read_per_reached_staleness_check);
    assert!(source.error_break_continues_outer_loop);
    assert!(source.sends_cancellation_aware);
    assert!(!source.closed_out_checks_open_boolean);
    assert!(source.worker_detached);
    assert!(!source.worker_joined);
    assert!(source.no_stats);
    assert_eq!(
        source.normalized_ast_sha256,
        BTreeMap::from([
            (
                "config.NewDefaultConfig".to_owned(),
                "d044a4710817daf9a87dfab03ce22f138da3c6e1bf94d40bbbfd0fea70673f32".to_owned()
            ),
            (
                "crawler.nodeHasPeersForHash".to_owned(),
                "1e2206b038dd5c1b70dff5a29cdf044ad7133b4876db75723081ab37c3d3da58".to_owned()
            ),
            (
                "crawler.start".to_owned(),
                "d61a318ce626352ee4f5cd5dd48191d767bbfe45b6a9def673cd185eada4f67b".to_owned()
            ),
            (
                "factory.New".to_owned(),
                "0204a00fd63b275339d63d622865858571c153bc81fc738784a78e1c150fec80".to_owned()
            ),
            (
                "infohash.runInfoHashTriage".to_owned(),
                "1009e7775daf5ee49c53f7655130e98ec6a8f1e9574fb0f0b044ff3156f54b96".to_owned()
            ),
            (
                "infohash.triageResult".to_owned(),
                "fc0569527db2ab92684d7b4585f6971a95bb16e8698a7f80f2d68bbacb9e1435".to_owned()
            ),
        ])
    );
    let sources = BTreeMap::from_iter(
        GO_SOURCES
            .iter()
            .map(|(path, _, digest)| ((*path).to_owned(), (*digest).to_owned())),
    );
    assert_eq!(source.source_sha256, sources);
    for (_, bytes, digest) in GO_SOURCES {
        assert_eq!(sha256(bytes), digest);
    }
    assert_eq!(
        source.go_mod_sqlmock_line,
        "github.com/DATA-DOG/go-sqlmock v1.5.2"
    );
    assert_eq!(
        source.go_sum_sqlmock_line,
        "github.com/DATA-DOG/go-sqlmock v1.5.2 h1:OcvFkGmslmlZibjAjaHm3L//6LiuBgolP7OputlJIzU="
    );
    let go_mod = include_str!("../../../../go.mod");
    let go_sum = include_str!("../../../../go.sum");
    assert!(go_mod
        .lines()
        .any(|line| line.trim() == source.go_mod_sqlmock_line));
    assert!(go_sum
        .lines()
        .any(|line| line.trim() == source.go_sum_sqlmock_line));
    assert_eq!(
        source.evidence,
        "exact source freshness plus behavioral rows executing crawler.runInfoHashTriage with real DAO query construction"
    );
    assert_eq!(
        source.nonclaims,
        [
            "map iteration SQL result and downstream delivery order",
            "exact rescrape boundary behavior or wall-clock determinism",
            "live PostgreSQL schema query plan indexes or result ordering",
            "production blocking bloom state buffering or flush behavior",
            "production batching timer input close or output close behavior",
            "downstream consumer callbacks concurrency semaphore or completion",
            "select tie resolution scheduling fairness or side effects beyond recorded downstream In accessor evaluation",
            "log messages levels fields or delivery",
            "total work retention throughput or backpressure capacity",
            "closed infoHashTriage output behavior because production does not check receive openness",
            "end-to-end live DHT traffic network peers or external services",
            "upstream sample_infohashes response origin responding-node address has-peers or ignore-hash provenance",
            "Rust implementation API statistics supervision application wiring deployment or production readiness",
        ]
    );
}

#[test]
fn source_contract_schema_ids_hashes_ast_and_nonclaims_are_exact() {
    assert_eq!(sha256(FIXTURE_BYTES), FIXTURE_SHA256);
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), FIXTURE_IDS.len());
    assert_eq!(RUST_EXECUTION_PARTITION.map(|entry| entry.0), FIXTURE_IDS);
    assert_eq!(
        DELIBERATE_RUST_DELTAS,
        [
            "Rust_owns_one_joined_worker_future_instead_of_a_detached_Go_goroutine",
            "Rust_input_EOF_is_typed_and_does_not_repeat_closed_output_zero_batches",
            "Rust_batching_is_first_item_relative_without_a_detached_batcher_or_buffered_batch_output",
            "Rust_routes_filtered_hashes_in_first_filtered_order_instead_of_Go_map_iteration_order",
            "Rust_filter_and_lookup_are_abstract_async_collaborators_not_a_GORM_DAO_or_live_Postgres",
            "Rust_collaborator_failures_are_counted_and_continue_without_claiming_Go_log_delivery",
            "Rust_output_closure_and_shutdown_return_typed_exact_suffix_accounting",
            "Rust_output_send_reservations_are_cancellation_safe_and_shutdown_biased",
            "Rust_rejects_foreign_filter_hashes_as_a_fail_closed_hardening_delta",
            "Rust_uses_an_injected_clock_while_preserving_strict_stale_before_for_reached_checks",
        ]
    );
    assert_eq!(
        RUST_NONCLAIMS,
        [
            "exact_Go_map_iteration_SQL_result_or_downstream_delivery_order",
            "exact_Go_rescrape_boundary_wall_clock_or_per_item_time_Now_schedule",
            "live_PostgreSQL_schema_query_plan_indexes_transactions_or_result_order",
            "production_blocking_bloom_state_buffering_or_flush_behavior",
            "production_Go_batching_ticker_input_close_or_output_close_behavior",
            "downstream_get_peers_or_scrape_worker_callbacks_concurrency_or_completion",
            "Go_select_tie_resolution_eager_In_operand_side_effects_or_fairness",
            "Go_log_messages_levels_fields_or_delivery",
            "cross_route_total_retention_throughput_backpressure_or_waiter_fairness",
            "closed_Go_infoHashTriage_output_runtime_behavior",
            "end_to_end_live_DHT_traffic_network_peers_or_external_services",
            "upstream_sample_infohashes_origin_has_peers_or_ignore_hash_provenance",
            "application_supervisor_database_adapter_deployment_or_production_readiness",
            "concurrent_upstream_send_permit_drain_accounting_outside_prequeued_fixture_inputs",
        ]
    );
    assert!(FIXTURE_BYTES.ends_with(b"\n"));
    assert_eq!(FIXTURE_TEXT.lines().count(), 7);
    for (index, fixture) in fixtures.iter().enumerate() {
        assert_eq!(fixture.id, FIXTURE_IDS[index]);
        assert_eq!(fixture.subsystem, "dht_crawler_info_hash_triage");
        assert_eq!(fixture.classification, ROW_CLASSIFICATIONS[index]);
        if index > 0 {
            assert_runtime_fixture_contract(fixture);
            assert!(fixture.expected.source.is_none());
        }
    }

    let source_fixture = &fixtures[0];
    assert_eq!(
        source_fixture.oracle,
        Oracle {
            composition:
                "exact_Go_source_factory_configuration_model_DAO_and_channel_freshness_gate"
                    .to_owned(),
            determinism: "source_SHA256_plus_required_AST_and_factory_shapes".to_owned(),
            database: "source_contract_only_without_live_Postgres".to_owned(),
            clock: "source_contract_only_for_production_time_Now".to_owned(),
        }
    );
    assert_eq!(source_fixture.input, source_fixture_expected());
    assert!(source_fixture.expected.filter_calls.is_empty());
    assert!(source_fixture.expected.sql_args.is_empty());
    assert_eq!(source_fixture.expected.database_query_calls, 0);
    assert!(source_fixture.expected.actions.is_empty());
    assert_eq!(source_fixture.expected.get_peers_in_calls, 0);
    assert_eq!(source_fixture.expected.scrape_in_calls, 0);
    assert_eq!(source_fixture.expected.block_calls, 0);
    assert_eq!(source_fixture.expected.flush_calls, 0);
    assert!(!source_fixture.expected.run_returned);
    assert!(!source_fixture.expected.context_cancelled);
    assert!(!source_fixture.expected.continued_after_error);
    assert_source_contract(
        source_fixture
            .expected
            .source
            .as_ref()
            .expect("source row has a source contract"),
    );
}

#[tokio::test]
async fn decision_matrix_replays_through_actual_worker_as_an_action_multiset() {
    let fixture = fixture(FIXTURE_IDS[1]);
    assert_runtime_fixture_contract(&fixture);
    let replay = replay(&fixture).await;
    assert_eq!(replay.exit, DhtInfoHashTriageWorkerExit::InputClosed);
    assert_eq!(
        replay.filter_calls,
        expected_id_calls(&fixture.expected.filter_calls)
    );
    assert_lookup_calls(&replay, &fixture.expected);
    assert_eq!(
        replay.get_peers_actions,
        [
            action("get_peers", 1, 1),
            action("get_peers", 3, 3),
            action("get_peers", 4, 4),
            action("get_peers", 5, 5),
            action("get_peers", 10, 10),
        ]
    );
    assert_eq!(
        replay.scrape_actions,
        [action("scrape", 6, 6), action("scrape", 7, 7)]
    );
    assert_eq!(replay.actions, fixture.expected.actions);
    assert_eq!(replay.clock_calls, 3);
    assert_eq!(
        replay.stats,
        DhtInfoHashTriageStats {
            dequeued: 12,
            batches: 2,
            input_duplicates_dropped: 1,
            filter_calls: 2,
            filter_hashes_returned: 9,
            filter_suppressed: 2,
            lookup_calls: 1,
            get_peers_queued: 5,
            scrape_queued: 2,
            discarded: 2,
            ..DhtInfoHashTriageStats::default()
        }
    );
    assert_conserved(replay.stats);
}

#[tokio::test]
async fn empty_filter_replays_database_and_output_skip_then_continues_to_eof() {
    let fixture = fixture(FIXTURE_IDS[2]);
    assert_runtime_fixture_contract(&fixture);
    let replay = replay(&fixture).await;
    assert_eq!(replay.exit, DhtInfoHashTriageWorkerExit::InputClosed);
    assert_eq!(
        replay.filter_calls,
        expected_id_calls(&fixture.expected.filter_calls)
    );
    assert_lookup_calls(&replay, &fixture.expected);
    assert!(replay.actions.is_empty());
    assert_eq!(replay.clock_calls, 0);
    assert_eq!(
        replay.stats,
        DhtInfoHashTriageStats {
            dequeued: 2,
            batches: 2,
            filter_calls: 2,
            filter_suppressed: 2,
            ..DhtInfoHashTriageStats::default()
        }
    );
    assert_conserved(replay.stats);
}

#[tokio::test]
async fn filter_error_replays_batch_drop_and_continuation_to_eof() {
    let fixture = fixture(FIXTURE_IDS[3]);
    assert_runtime_fixture_contract(&fixture);
    let replay = replay(&fixture).await;
    assert_eq!(replay.exit, DhtInfoHashTriageWorkerExit::InputClosed);
    assert_eq!(
        replay.filter_calls,
        expected_id_calls(&fixture.expected.filter_calls)
    );
    assert_lookup_calls(&replay, &fixture.expected);
    assert!(replay.actions.is_empty());
    assert_eq!(replay.clock_calls, 0);
    assert_eq!(
        replay.stats,
        DhtInfoHashTriageStats {
            dequeued: 2,
            batches: 2,
            filter_calls: 2,
            filter_failures: 1,
            filter_suppressed: 1,
            filter_failure_dropped: 1,
            ..DhtInfoHashTriageStats::default()
        }
    );
    assert_conserved(replay.stats);
}

#[tokio::test]
async fn database_error_replays_batch_drop_and_continuation_to_eof() {
    let fixture = fixture(FIXTURE_IDS[4]);
    assert_runtime_fixture_contract(&fixture);
    let replay = replay(&fixture).await;
    assert_eq!(replay.exit, DhtInfoHashTriageWorkerExit::InputClosed);
    assert_eq!(
        replay.filter_calls,
        expected_id_calls(&fixture.expected.filter_calls)
    );
    assert_lookup_calls(&replay, &fixture.expected);
    assert!(replay.actions.is_empty());
    assert_eq!(replay.clock_calls, 0);
    assert_eq!(
        replay.stats,
        DhtInfoHashTriageStats {
            dequeued: 2,
            batches: 2,
            filter_calls: 2,
            filter_hashes_returned: 1,
            filter_suppressed: 1,
            lookup_calls: 1,
            lookup_failures: 1,
            lookup_failure_dropped: 1,
            ..DhtInfoHashTriageStats::default()
        }
    );
    assert_conserved(replay.stats);
}

async fn replay_blocked_route(fixture: &Fixture, expected_route: Route) -> Replay {
    let filter = scripted_filter(&fixture.input);
    let lookup = scripted_lookup(&fixture.input);
    let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
    let (get_peers, mut get_peers_receiver) = dht_get_peers_channel();
    let (scrape, mut scrape_receiver) = dht_scrape_channel();
    let mut prefix = Vec::new();
    let prefix_len = match expected_route {
        Route::GetPeers => DHT_GET_PEERS_ROUTE_CAPACITY,
        Route::Scrape => DHT_SCRAPE_ROUTE_CAPACITY,
    };
    for index in 0..prefix_len {
        let value = DhtInfoHashTriageRequest {
            info_hash: Id20::from_slice(&[index as u8; 20]).unwrap(),
            source_node_addr: format!("198.51.100.1:{}", 10_000 + index).parse().unwrap(),
        };
        match expected_route {
            Route::GetPeers => get_peers.send(value).await.unwrap(),
            Route::Scrape => scrape.send(value).await.unwrap(),
        }
        prefix.push(value);
    }
    let clock = FixedClock::new(1_700_000_000_000_000);
    let (worker, stats) = DhtInfoHashTriageWorker::with_config(
        receiver,
        get_peers,
        scrape,
        filter.clone(),
        lookup.clone(),
        clock.clone(),
        worker_config(&fixture.input),
    );
    enqueue_fixture_batches(&input, &fixture.input.batches).await;
    drop(input);
    let expected_request = request(&fixture.input.batches[0][0]);
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let hook_calls = Arc::new(AtomicUsize::new(0));
    let hook_counter = hook_calls.clone();
    let observed = Arc::new(Notify::new());
    let hook_observed = observed.clone();
    let task = tokio::spawn(worker.run_with(
        async move {
            let _ = shutdown_receiver.await;
        },
        move |call, route, observed_request| {
            assert_eq!(call, 1);
            assert_eq!(route, expected_route);
            assert_eq!(*observed_request, expected_request);
            hook_counter.fetch_add(1, Ordering::Relaxed);
            hook_observed.notify_one();
        },
    ));
    observed.notified().await;
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "the fixture target send must remain pending behind the full route"
    );
    shutdown_sender.send(()).unwrap();
    let exit = task.await.unwrap();
    assert_eq!(hook_calls.load(Ordering::Relaxed), 1);
    let observed_prefix = match expected_route {
        Route::GetPeers => {
            let values = drain_route(&mut get_peers_receiver);
            assert!(drain_route(&mut scrape_receiver).is_empty());
            values
        }
        Route::Scrape => {
            let values = drain_route(&mut scrape_receiver);
            assert!(drain_route(&mut get_peers_receiver).is_empty());
            values
        }
    };
    assert_eq!(observed_prefix, prefix);
    Replay {
        exit,
        filter_calls: filter.calls(),
        lookup_calls: lookup.calls(),
        get_peers_actions: Vec::new(),
        scrape_actions: Vec::new(),
        actions: Vec::new(),
        clock_calls: clock.calls.load(Ordering::Relaxed),
        stats: stats.snapshot(),
    }
}

fn drain_route<R>(receiver: &mut R) -> Vec<DhtInfoHashTriageRequest>
where
    R: TriageRouteReceiver,
{
    let mut values = Vec::new();
    while let Ok(value) = receiver.try_recv_request() {
        values.push(value);
    }
    values
}

trait TriageRouteReceiver {
    fn try_recv_request(
        &mut self,
    ) -> Result<DhtInfoHashTriageRequest, tokio::sync::mpsc::error::TryRecvError>;
}

impl TriageRouteReceiver for DhtGetPeersReceiver {
    fn try_recv_request(
        &mut self,
    ) -> Result<DhtInfoHashTriageRequest, tokio::sync::mpsc::error::TryRecvError> {
        self.try_recv()
    }
}

impl TriageRouteReceiver for DhtScrapeReceiver {
    fn try_recv_request(
        &mut self,
    ) -> Result<DhtInfoHashTriageRequest, tokio::sync::mpsc::error::TryRecvError> {
        self.try_recv()
    }
}

fn assert_blocked_replay(fixture: &Fixture, replay: Replay) {
    assert_eq!(
        replay.exit,
        DhtInfoHashTriageWorkerExit::Shutdown {
            queued_dropped: 0,
            batch_dropped: 1,
        }
    );
    assert_eq!(
        replay.filter_calls,
        expected_id_calls(&fixture.expected.filter_calls)
    );
    assert_lookup_calls(&replay, &fixture.expected);
    assert!(replay.actions.is_empty());
    assert_eq!(replay.clock_calls, 0);
    assert_eq!(
        replay.stats,
        DhtInfoHashTriageStats {
            dequeued: 1,
            batches: 1,
            filter_calls: 1,
            filter_hashes_returned: 1,
            lookup_calls: 1,
            shutdown_batch_dropped: 1,
            ..DhtInfoHashTriageStats::default()
        }
    );
    assert_conserved(replay.stats);
}

#[tokio::test]
async fn cancellation_at_blocked_get_peers_replays_through_run_with_hook() {
    let fixture = fixture(FIXTURE_IDS[5]);
    assert_runtime_fixture_contract(&fixture);
    assert_eq!(fixture.expected.get_peers_in_calls, 1);
    assert_eq!(fixture.expected.scrape_in_calls, 0);
    let replay = replay_blocked_route(&fixture, Route::GetPeers).await;
    assert_blocked_replay(&fixture, replay);
}

#[tokio::test]
async fn cancellation_at_blocked_scrape_replays_through_run_with_hook() {
    let fixture = fixture(FIXTURE_IDS[6]);
    assert_runtime_fixture_contract(&fixture);
    assert_eq!(fixture.expected.get_peers_in_calls, 0);
    assert_eq!(fixture.expected.scrape_in_calls, 1);
    let replay = replay_blocked_route(&fixture, Route::Scrape).await;
    assert_blocked_replay(&fixture, replay);
}
