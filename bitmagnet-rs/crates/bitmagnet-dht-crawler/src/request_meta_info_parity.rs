use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::future::{pending, ready, Future};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use bitmagnet_dht::Id20;
use bitmagnet_metainfo::{parse_info_bytes, Info, ParsedInfo};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::{oneshot, Notify};

use super::request_meta_info::{request_first_meta_info, RequestMetaInfoAttemptObserver};
use super::{
    dht_persist_torrent_channel, dht_request_meta_info_channel, DefaultDhtMetaInfoBanningChecker,
    DhtInfoHashBlocker, DhtMetaInfoBanningChecker, DhtMetaInfoRequest, DhtMetaInfoRequester,
    DhtPersistTorrentRequest, DhtRequestMetaInfoWorker, DhtRequestMetaInfoWorkerExit,
    DhtRequestMetaInfoWorkerStats, RequestMetaInfoCollaboratorError,
    DHT_PERSIST_TORRENT_ROUTE_CAPACITY,
};

const FIXTURE_TEXT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/dht_crawler_request_meta_info.jsonl"
));
const FIXTURE_BYTES: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/dht_crawler_request_meta_info.jsonl"
));
const FIXTURE_SHA256: &str = "03ce2ab0da2b0f9ba1173b8ba52481a903265ca6862f957b40490cf67a9e4ec5";

const HYBRID_TORRENT: &[u8] = include_bytes!(
    "../../../../internal/protocol/metainfo/testdata/bittorrent-v2-hybrid-test.torrent"
);

const FIXTURE_IDS: [&str; 8] = [
    "production_source_factory_and_lifecycle_contract",
    "zero_peers_returns_nil_error_and_emits_zero_parsed_info",
    "ordered_duplicate_peers_fail_through_to_first_allowed_hybrid_success",
    "all_peer_failures_join_in_attempt_order_and_preserve_causes",
    "banned_success_invokes_block_hash_false_ignores_block_error_stops_and_emits_none",
    "cancellation_during_first_request_error_continues_remaining_peers_with_same_cancelled_context",
    "cancelled_before_scripted_success_still_checks_ban_and_eagerly_evaluates_unavailable_persist_in",
    "lane_error_is_swallowed",
];

const ROW_CLASSIFICATIONS: [&str; 8] = [
    "SOURCE_ONLY",
    "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA",
    "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA",
    "GO_ONLY_LANE",
];

const RUST_EXECUTION_PARTITION: [(&str, &str); 8] = [
    (
        FIXTURE_IDS[0],
        "SOURCE_ONLY_NO_RUST_RUNTIME_OR_LIVE_SERVICE_REPLAY",
    ),
    (
        FIXTURE_IDS[1],
        "RUST_EMPTY_PEERS_HARDENING_DROPS_WITHOUT_ZERO_PARSED_INFO",
    ),
    (
        FIXTURE_IDS[2],
        "RUST_ACTUAL_WORKER_ORDERED_DUPLICATE_HYBRID_SUCCESS_REPLAY",
    ),
    (
        FIXTURE_IDS[3],
        "RUST_ACTUAL_WORKER_AND_ORDERED_FAILURE_CAUSE_REPLAY",
    ),
    (
        FIXTURE_IDS[4],
        "RUST_ACTUAL_WORKER_DEFAULT_TRIPLE_BAN_AND_BLOCK_REPLAY",
    ),
    (
        FIXTURE_IDS[5],
        "RUST_OWNED_WORKER_PENDING_REQUEST_SHUTDOWN_DELTA_REPLAY",
    ),
    (
        FIXTURE_IDS[6],
        "RUST_CONCEPTUAL_OWNED_PERSIST_BACKPRESSURE_SHUTDOWN_DELTA_REPLAY",
    ),
    (
        FIXTURE_IDS[7],
        "GO_ONLY_LANE_ERROR_WITH_RUST_TYPED_INPUT_EOF_REPLAY",
    ),
];

const DELIBERATE_RUST_DELTAS: [&str; 8] = [
    "Rust_owns_and_joins_at_most_400_tasks_instead_of_detaching_Go_callbacks",
    "Rust_empty_peer_lists_are_dropped_instead_of_emitting_zero_ParsedInfo",
    "Rust_input_EOF_is_typed_and_never_repeats_zero_value_callbacks",
    "Rust_shutdown_closes_and_drains_input_then_aborts_and_joins_accepted_tasks",
    "Rust_shutdown_during_a_pending_request_does_not_continue_the_peer_suffix",
    "Rust_shutdown_during_a_pending_request_does_not_check_banning_or_touch_persistence",
    "Rust_persistence_send_future_is_owned_and_cancelled_by_worker_shutdown",
    "Rust_has_typed_EOF_shutdown_and_accounting_instead_of_swallowing_a_lane_Run_error",
];

const RUST_NONCLAIMS: [&str; 17] = [
    "exact_Go_ready_select_tie_winner_or_eager_channel_operand_side_effects",
    "Go_goroutine_callback_scheduling_completion_order_or_semaphore_fairness",
    "closed_Go_buffered_input_runtime_execution_or_callback_join_guarantee",
    "send_to_closed_Go_channel_behavior",
    "metainfo_TCP_handshake_extension_piece_transfer_or_live_requester_behavior",
    "production_banning_rules_beyond_the_frozen_default_checker_row",
    "real_blocking_manager_buffer_Bloom_flush_database_or_durability_behavior",
    "runPersistTorrents_batching_deduplication_conversion_or_database_behavior",
    "batching_ticker_schedule_log_metrics_or_persisted_counter_delivery",
    "production_throughput_total_retention_or_waiter_fairness",
    "application_supervisor_deployment_or_production_readiness",
    "arbitrary_textual_IPv6_zones_beyond_numeric_scope",
    "Go_lane_Run_error_semantics_in_the_owned_Rust_input_route",
    "concurrent_external_pending_send_accounting_outside_prequeued_fixture_inputs",
    "scripted_banned_row_does_not_prove_end_to_end_requester_hash_verification",
    "U_FFFD_is_only_the_lossy_JSON_display_projection_while_Rust_retains_raw_name_bytes",
    "row7_blocked_persist_replay_is_a_conceptual_owned_output_cancellation_delta_not_an_exact_replay_of_the_Go_peer_sequence",
];

const NORMALIZED_AST: [(&str, &str); 20] = [
    (
        "banning.Checker",
        "4e63f1a6ec946417983d103e70b3bcd1f7ca28a2363ab616d99970ea528f135e",
    ),
    (
        "banning.New",
        "be3d2ed77f1c448fbd5c439cf8074d9af7fa6fc318c625a56149361c17080ac9",
    ),
    (
        "banning.combinedChecker.Check",
        "3d7e6507567670469050ea30493667d02ebaa3c65836b972187fa2aacb95b092",
    ),
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
        "blocking.Manager",
        "d4a130c8c8f8414c0522de3abfa7438c405b0ed93b6703e2945af5b4a83d250f",
    ),
    (
        "buffered.In",
        "47b8d0cda8a3039f6d0ea101430404511705d63aafe3ea9edf95e7883f17bedb",
    ),
    (
        "buffered.NewBufferedConcurrentChannel",
        "562428750b1aaf7a4811758daa63468461d995ac00f36e4d7b620fedfb4633ec",
    ),
    (
        "buffered.Run",
        "0a8f90020ab24fb50cad498fcf376777cde3b5f6aa6424da3e66b15b54e3292f",
    ),
    (
        "crawler.infoHashWithMetaInfo",
        "7de701e7f26b3dbbe7f82adc220ec88ffc362afd476bf5899fe20401afa0ce6d",
    ),
    (
        "crawler.infoHashWithPeers",
        "9effbfa014d73da2f826c0c78a8388c8260ff76474f12d47cbab434303bf345e",
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
        "metainfo.ParsedInfo",
        "51664f615ffbaff8382bef86eadfc7d0b1c722acfd76b2ca86705a921d3065d0",
    ),
    (
        "requester.Requester",
        "f57bed7d9fea486c6fa441a1576432f9ec03a0914e79784b2b6092e810dd76dd",
    ),
    (
        "requester.Response",
        "4b09076bf112c4fc5f81987da2fc81450f18cd6c5aed01ea5daf4e29b8e4cab1",
    ),
    (
        "requestmeta.doRequestMetaInfo",
        "f8ea6b497cfe359c313660b37c251a6396a83a186e8f83a42f0571ca0a901ca5",
    ),
    (
        "requestmeta.runRequestMetaInfo",
        "97bde956993ae99f1b52b5eac40e95da84b53a3ee10e1f7f16d6f0c0c8b54b91",
    ),
];

const GO_SOURCES: [(&str, &[u8], &str); 16] = [
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
        "internal/dhtcrawler/request_meta_info.go",
        include_bytes!("../../../../internal/dhtcrawler/request_meta_info.go"),
        "d20fc943aee947055dd5521235a55fd19c5fdd41a4203b8c09523b443c6ea0a6",
    ),
    (
        "internal/protocol/id.go",
        include_bytes!("../../../../internal/protocol/id.go"),
        "e1947e2b4af4cc008f5bb8cf5000ebfe784a82e119cb0418c2a74c3ed5f8c26f",
    ),
    (
        "internal/protocol/metainfo/banning/checker.go",
        include_bytes!("../../../../internal/protocol/metainfo/banning/checker.go"),
        "fc8293017ec0bd95925d7fba625c0bcc146a7b5b21ca6d35770331ea319737f6",
    ),
    (
        "internal/protocol/metainfo/banning/name_length.go",
        include_bytes!("../../../../internal/protocol/metainfo/banning/name_length.go"),
        "081bf48cb51a92083b5c5a3ca53a5f45b139fe41d566fafe8440414406e0d93e",
    ),
    (
        "internal/protocol/metainfo/banning/size.go",
        include_bytes!("../../../../internal/protocol/metainfo/banning/size.go"),
        "c1e94bf31704632f4d51fc53901826cfe7ba9fe31b04e160cf849a388643a5b6",
    ),
    (
        "internal/protocol/metainfo/banning/utf8.go",
        include_bytes!("../../../../internal/protocol/metainfo/banning/utf8.go"),
        "4b683a1e4a9b02ce6b11adb6d49b99794dee25a8f5a4345b2de62b3d50772f51",
    ),
    (
        "internal/protocol/metainfo/metainfo.go",
        include_bytes!("../../../../internal/protocol/metainfo/metainfo.go"),
        "b75c5f74d42431ad76fe2889f5ce6573cce89f5faedf66f27996dc458e3a7816",
    ),
    (
        "internal/protocol/metainfo/metainforequester/requester.go",
        include_bytes!("../../../../internal/protocol/metainfo/metainforequester/requester.go"),
        "dd99f3e7e593be707638ae17a76d0587a099e532d74b9785dc481881965e145b",
    ),
    (
        "internal/protocol/metainfo/parse.go",
        include_bytes!("../../../../internal/protocol/metainfo/parse.go"),
        "edda62d0c67ae79ded3a03b77b5d6108ac42eeb3df9d83f5fb73cf16cabaea0c",
    ),
];

const PREREQUISITE_FIXTURES: [(&str, &[u8], &str); 4] = [
    (
        "internal/protocol/metainfo/testdata/bittorrent-v2-hybrid-test.torrent",
        HYBRID_TORRENT,
        "8ba7575e64e9046cac74ca6523bff6445ff5c3e369d5d132607a793a1834e93f",
    ),
    (
        "testdata/parity/dht/dht_crawler_get_peers.jsonl",
        include_bytes!("../../../../testdata/parity/dht/dht_crawler_get_peers.jsonl"),
        "82b694fece9e46c05aefaab76bc05b78462bc04824bf6b83bb77eb544b7f0844",
    ),
    (
        "testdata/parity/dht/dht_crawler_info_hash_triage.jsonl",
        include_bytes!("../../../../testdata/parity/dht/dht_crawler_info_hash_triage.jsonl"),
        "52eda840f872225cc34f8cf12edc2e4621e8a1fef569abf34a50f4a3bd9896f8",
    ),
    (
        "testdata/parity/dht/dht_info_hash_block_filter.jsonl",
        include_bytes!("../../../../testdata/parity/dht/dht_info_hash_block_filter.jsonl"),
        "cc17edc11e5a21fe668d1067d2cf7413643bfdc8b81b0d5e97e5830afb1a51b4",
    ),
];

const EVIDENCE_COMMITS: [(&str, &str); 6] = [
    (
        "banning_checker_source",
        "f70352f4c540c6ba7e25f5aa9493766c5cc62f70",
    ),
    (
        "blocking_filter_oracle",
        "41f1e8cbe529d7a0bf464bb55011e0400d24b4e7",
    ),
    (
        "get_peers_oracle",
        "19f568e01c637a8ae1b94f38e3db2c9f95734d8c",
    ),
    (
        "info_hash_triage_oracle",
        "6aece7ac7605507aaf5ccdcc9adf2497170b071d",
    ),
    (
        "metainfo_v2_parser",
        "86017663f1b61908dd4792786081e179f7538e81",
    ),
    (
        "request_metainfo_route",
        "73a4d867b41f4a4e7933d527c633b044736300c6",
    ),
];

const GO_NONCLAIMS: [&str; 17] = [
    "goroutine_callback_scheduling_completion_or_order",
    "semaphore_or_channel_fairness",
    "closed_buffered_input_runtime_execution",
    "callback_join_guarantee",
    "ready_select_tie_winner",
    "arbitrary_side_effects_of_eagerly_evaluated_In_beyond_recorded_call_count",
    "send_to_closed_Go_channel_behavior",
    "metainfo_TCP_handshake_extension_piece_transfer_or_live_requester_behavior",
    "production_banning_checker_rules_beyond_the_actual_combined_banned_row",
    "Block_flush_false_argument_does_not_prove_real_manager_will_not_flush_when_shouldFlush_is_true",
    "blocking_manager_buffer_Bloom_flush_database_or_nonempty_durability",
    "runPersistTorrents_batching_deduplication_model_conversion_or_database_behavior",
    "batching_ticker_schedule_log_metrics_or_persisted_counter_delivery",
    "production_throughput_total_retention_or_waiter_fairness",
    "application_supervisor_deployment_or_production_readiness",
    "arbitrary_textual_IPv6_zones_runtime_rows_use_unscoped_or_numeric_scope_only",
    "Rust_public_API_owned_task_stats_or_shutdown_lifecycle_no_Rust_consumer_exists_in_this_slice",
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
    lane: String,
    requester: String,
    banning: String,
    blocking: String,
    handoff: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Input {
    kind: String,
    request: Option<Request>,
    outcomes: Vec<Outcome>,
    ban_error: String,
    block_error: String,
    cancel_requester_at_call: usize,
    blocker_pending: bool,
    handoff_mode: String,
    handoff_capacity: usize,
    cancel_at_handoff_in_call: usize,
    lane_return_error: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Request {
    info_hash: String,
    node: Address,
    peers: Vec<Address>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Outcome {
    kind: String,
    error: String,
    name: String,
    meta_version: u8,
    info_hash_v1: String,
    info_hash_v2: String,
    invalid_info: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Address {
    ip: String,
    port: u16,
    scope: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Expected {
    requester_calls: Vec<RequesterCall>,
    same_context: bool,
    banning_calls: Vec<String>,
    banning_errors: Vec<String>,
    block_calls: Vec<BlockCall>,
    handoff_in_calls: usize,
    handoff_deliveries: Vec<Handoff>,
    events: Vec<String>,
    do_result: Option<ParsedProjection>,
    do_error: String,
    do_error_identities: Option<Vec<bool>>,
    run_returned: bool,
    context_cancelled: bool,
    callback_completed: bool,
    source: Option<Source>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RequesterCall {
    info_hash: String,
    peer: Address,
    context_cancelled: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BlockCall {
    hashes: Vec<String>,
    flush: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Handoff {
    info_hash: String,
    node: Address,
    name: String,
    meta_version: u8,
    info_hash_v1: String,
    info_hash_v2: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ParsedProjection {
    name: String,
    meta_version: u8,
    info_hash_v1: String,
    info_hash_v2: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Source {
    run_error_ignored: bool,
    shared_callback_context: bool,
    peers_attempted_sequentially: bool,
    peer_order_and_duplicates_preserved: bool,
    requester_failure_falls_through: bool,
    first_requester_success_stops: bool,
    banning_checked_only_after_success: bool,
    banned_hash_block_flush_argument_false: bool,
    block_error_ignored: bool,
    banned_success_stops: bool,
    all_failures_joined_in_order: bool,
    zero_peers_returns_nil_error: bool,
    successful_handoff_uses_original_route: bool,
    successful_handoff_uses_parsed_info: bool,
    persist_in_eagerly_evaluated: bool,
    run_persist_torrents_executed: bool,
    production_input_capacity: usize,
    production_concurrency: usize,
    production_handoff_capacity: usize,
    production_handoff_max_batch_size: usize,
    production_handoff_interval_ms: u64,
    production_handoff_output_capacity: usize,
    default_scaling_factor: usize,
    consumer_dequeues_before_semaphore: bool,
    consumer_callbacks_detached: bool,
    consumer_callbacks_joined: bool,
    maximum_retained_work: String,
    closed_input_checks_open_boolean: bool,
    closed_input_outcome: String,
    start_launches_worker_detached: bool,
    start_waits_only_stopped: bool,
    start_defers_shared_context_cancel: bool,
    start_joins_worker_or_callbacks: bool,
    normalized_ast_sha256: BTreeMap<String, String>,
    prerequisite_fixture_sha256: BTreeMap<String, String>,
    evidence_commit: BTreeMap<String, String>,
    source_sha256: BTreeMap<String, String>,
    nonclaims: Vec<String>,
    evidence: String,
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
    format!("{:x}", Sha256::digest(bytes))
}

fn expected_map<const N: usize>(entries: [(&str, &str); N]) -> BTreeMap<String, String> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

fn assert_source_contract(source: &Source) {
    assert!(source.run_error_ignored);
    assert!(source.shared_callback_context);
    assert!(source.peers_attempted_sequentially);
    assert!(source.peer_order_and_duplicates_preserved);
    assert!(source.requester_failure_falls_through);
    assert!(source.first_requester_success_stops);
    assert!(source.banning_checked_only_after_success);
    assert!(source.banned_hash_block_flush_argument_false);
    assert!(source.block_error_ignored);
    assert!(source.banned_success_stops);
    assert!(source.all_failures_joined_in_order);
    assert!(source.zero_peers_returns_nil_error);
    assert!(source.successful_handoff_uses_original_route);
    assert!(source.successful_handoff_uses_parsed_info);
    assert!(source.persist_in_eagerly_evaluated);
    assert!(!source.run_persist_torrents_executed);
    assert_eq!(
        (
            source.production_input_capacity,
            source.production_concurrency,
            source.production_handoff_capacity,
            source.production_handoff_max_batch_size,
            source.production_handoff_interval_ms,
            source.production_handoff_output_capacity,
            source.default_scaling_factor,
        ),
        (100, 400, 1_000, 1_000, 60_000, 1, 10)
    );
    assert!(source.consumer_dequeues_before_semaphore);
    assert!(source.consumer_callbacks_detached);
    assert!(!source.consumer_callbacks_joined);
    assert_eq!(
        source.maximum_retained_work,
        "capacity_plus_concurrency_plus_one_acquire_waiter"
    );
    assert!(!source.closed_input_checks_open_boolean);
    assert_eq!(
        source.closed_input_outcome,
        "repeated_zero_value_callbacks_can_emit_zero_parsed_info_requests"
    );
    assert!(source.start_launches_worker_detached);
    assert!(source.start_waits_only_stopped);
    assert!(source.start_defers_shared_context_cancel);
    assert!(!source.start_joins_worker_or_callbacks);
    assert_eq!(source.normalized_ast_sha256, expected_map(NORMALIZED_AST));
    assert_eq!(
        source.prerequisite_fixture_sha256,
        expected_map(PREREQUISITE_FIXTURES.map(|(path, _, digest)| (path, digest)))
    );
    assert_eq!(source.evidence_commit, expected_map(EVIDENCE_COMMITS));
    assert_eq!(
        source.source_sha256,
        expected_map(GO_SOURCES.map(|(path, _, digest)| (path, digest)))
    );
    assert_eq!(source.nonclaims, GO_NONCLAIMS);
    assert_eq!(
        source.evidence,
        "runtime rows execute actual runRequestMetaInfo or doRequestMetaInfo through controlled interfaces; persistTorrents is observed only at raw input and runPersistTorrents is never executed"
    );
}

#[test]
fn source_schema_ids_hashes_metadata_nonclaims_and_execution_partition_are_exact() {
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
                "RUST_EMPTY_PEERS_HARDENING_DROPS_WITHOUT_ZERO_PARSED_INFO",
            ),
            (
                FIXTURE_IDS[2],
                "RUST_ACTUAL_WORKER_ORDERED_DUPLICATE_HYBRID_SUCCESS_REPLAY",
            ),
            (
                FIXTURE_IDS[3],
                "RUST_ACTUAL_WORKER_AND_ORDERED_FAILURE_CAUSE_REPLAY",
            ),
            (
                FIXTURE_IDS[4],
                "RUST_ACTUAL_WORKER_DEFAULT_TRIPLE_BAN_AND_BLOCK_REPLAY",
            ),
            (
                FIXTURE_IDS[5],
                "RUST_OWNED_WORKER_PENDING_REQUEST_SHUTDOWN_DELTA_REPLAY",
            ),
            (
                FIXTURE_IDS[6],
                "RUST_CONCEPTUAL_OWNED_PERSIST_BACKPRESSURE_SHUTDOWN_DELTA_REPLAY",
            ),
            (
                FIXTURE_IDS[7],
                "GO_ONLY_LANE_ERROR_WITH_RUST_TYPED_INPUT_EOF_REPLAY",
            ),
        ]
    );
    assert_eq!(
        DELIBERATE_RUST_DELTAS,
        [
            "Rust_owns_and_joins_at_most_400_tasks_instead_of_detaching_Go_callbacks",
            "Rust_empty_peer_lists_are_dropped_instead_of_emitting_zero_ParsedInfo",
            "Rust_input_EOF_is_typed_and_never_repeats_zero_value_callbacks",
            "Rust_shutdown_closes_and_drains_input_then_aborts_and_joins_accepted_tasks",
            "Rust_shutdown_during_a_pending_request_does_not_continue_the_peer_suffix",
            "Rust_shutdown_during_a_pending_request_does_not_check_banning_or_touch_persistence",
            "Rust_persistence_send_future_is_owned_and_cancelled_by_worker_shutdown",
            "Rust_has_typed_EOF_shutdown_and_accounting_instead_of_swallowing_a_lane_Run_error",
        ]
    );
    assert_eq!(
        RUST_NONCLAIMS,
        [
            "exact_Go_ready_select_tie_winner_or_eager_channel_operand_side_effects",
            "Go_goroutine_callback_scheduling_completion_order_or_semaphore_fairness",
            "closed_Go_buffered_input_runtime_execution_or_callback_join_guarantee",
            "send_to_closed_Go_channel_behavior",
            "metainfo_TCP_handshake_extension_piece_transfer_or_live_requester_behavior",
            "production_banning_rules_beyond_the_frozen_default_checker_row",
            "real_blocking_manager_buffer_Bloom_flush_database_or_durability_behavior",
            "runPersistTorrents_batching_deduplication_conversion_or_database_behavior",
            "batching_ticker_schedule_log_metrics_or_persisted_counter_delivery",
            "production_throughput_total_retention_or_waiter_fairness",
            "application_supervisor_deployment_or_production_readiness",
            "arbitrary_textual_IPv6_zones_beyond_numeric_scope",
            "Go_lane_Run_error_semantics_in_the_owned_Rust_input_route",
            "concurrent_external_pending_send_accounting_outside_prequeued_fixture_inputs",
            "scripted_banned_row_does_not_prove_end_to_end_requester_hash_verification",
            "U_FFFD_is_only_the_lossy_JSON_display_projection_while_Rust_retains_raw_name_bytes",
            "row7_blocked_persist_replay_is_a_conceptual_owned_output_cancellation_delta_not_an_exact_replay_of_the_Go_peer_sequence",
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
        assert_eq!(fixture.subsystem, "dht_crawler_request_meta_info");
        assert_eq!(fixture.classification, ROW_CLASSIFICATIONS[index]);
        assert_eq!(fixture.expected.source.is_some(), index == 0);
    }
    let counts = fixtures.iter().fold(BTreeMap::new(), |mut counts, row| {
        *counts.entry(row.classification.as_str()).or_insert(0_usize) += 1;
        counts
    });
    assert_eq!(
        counts,
        BTreeMap::from([
            ("GO_ONLY_LANE", 1),
            ("RUNTIME_EXACT", 3),
            ("RUNTIME_WITH_OWNED_SHUTDOWN_DELTA", 3),
            ("SOURCE_ONLY", 1),
        ])
    );

    let source = &fixtures[0];
    assert_eq!(
        source.oracle,
        Oracle {
            composition: "production_source_factory_and_lifecycle_freshness_gate".to_owned(),
            determinism: "exact_normalized_AST_source_and_prerequisite_fixture_SHA256".to_owned(),
            lane: "production_BufferedConcurrentChannel_source_shape".to_owned(),
            requester: "production_metainforequester_Requester_interface".to_owned(),
            banning: "production_banning_Checker_interface".to_owned(),
            blocking: "production_blocking_Manager_interface".to_owned(),
            handoff: "production_persistTorrents_BatchingChannel_input_shape_only".to_owned(),
        }
    );
    assert_eq!(source.input.kind, "source_contract");
    assert!(source.input.request.is_none());
    assert!(source.input.outcomes.is_empty());
    assert_eq!(source.input.handoff_mode, "source_only");
    assert!(!source.expected.same_context);
    assert!(!source.expected.run_returned);
    assert!(!source.expected.callback_completed);
    assert_source_contract(source.expected.source.as_ref().unwrap());

    let runtime_oracle = Oracle {
        composition: "actual_runRequestMetaInfo_or_doRequestMetaInfo_with_manual_lane_and_scripted_collaborators".to_owned(),
        determinism: "synchronous_peer_attempts_and_explicit_pending_cancellation_gates".to_owned(),
        lane: "manual_in_order_callback_interface".to_owned(),
        requester: "scripted_metainforequester_Requester".to_owned(),
        banning: "scripted_banning_Checker".to_owned(),
        blocking: "scripted_blocking_Manager".to_owned(),
        handoff: "buffered_accept_one".to_owned(),
    };
    for row in &fixtures[1..=6] {
        let mut expected_oracle = runtime_oracle.clone();
        if row.id == FIXTURE_IDS[3] {
            expected_oracle.handoff = "not_executed".to_owned();
        } else if matches!(row.id.as_str(), id if id == FIXTURE_IDS[5] || id == FIXTURE_IDS[6]) {
            expected_oracle.handoff = "unbuffered_no_receiver".to_owned();
        }
        assert_eq!(row.oracle, expected_oracle);
        assert!(row.input.request.is_some());
        assert!(!row.input.lane_return_error);
    }
    let go_only = &fixtures[7];
    let mut go_only_oracle = runtime_oracle;
    go_only_oracle.handoff = "unbuffered_no_receiver".to_owned();
    assert_eq!(go_only.oracle, go_only_oracle);
    assert!(go_only.input.request.is_none());
    assert!(go_only.input.lane_return_error);
    assert_eq!(go_only.expected.events, ["lane_return_error"]);
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
        (2, &["input", "request"]),
        (2, &["input", "request", "node"]),
        (2, &["input", "request", "peers", "0"]),
        (2, &["input", "outcomes", "0"]),
        (2, &["expected", "requesterCalls", "0"]),
        (2, &["expected", "requesterCalls", "0", "peer"]),
        (2, &["expected", "handoffDeliveries", "0"]),
        (2, &["expected", "handoffDeliveries", "0", "node"]),
        (3, &["expected", "doResult"]),
        (4, &["expected", "blockCalls", "0"]),
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

type RequestFuture = Pin<
    Box<dyn Future<Output = Result<ParsedInfo, RequestMetaInfoCollaboratorError>> + Send + 'static>,
>;
type BlockFuture =
    Pin<Box<dyn Future<Output = Result<(), RequestMetaInfoCollaboratorError>> + Send + 'static>>;
type CheckerFn =
    dyn Fn(&Info) -> Result<(), RequestMetaInfoCollaboratorError> + Send + Sync + 'static;

struct TestRequester {
    request: Arc<dyn Fn(Id20, SocketAddr) -> RequestFuture + Send + Sync>,
}

#[async_trait]
impl DhtMetaInfoRequester for TestRequester {
    async fn request(
        &self,
        info_hash: Id20,
        peer: SocketAddr,
    ) -> Result<ParsedInfo, RequestMetaInfoCollaboratorError> {
        (self.request)(info_hash, peer).await
    }
}

struct TestChecker {
    check: Arc<CheckerFn>,
}

impl DhtMetaInfoBanningChecker for TestChecker {
    fn check(&self, info: &Info) -> Result<(), RequestMetaInfoCollaboratorError> {
        (self.check)(info)
    }
}

struct TestBlocker {
    block: Arc<dyn Fn(Vec<Id20>, bool) -> BlockFuture + Send + Sync>,
}

#[async_trait]
impl DhtInfoHashBlocker for TestBlocker {
    async fn block(
        &self,
        hashes: &[Id20],
        flush: bool,
    ) -> Result<(), RequestMetaInfoCollaboratorError> {
        (self.block)(hashes.to_vec(), flush).await
    }
}

#[derive(Debug)]
struct FixtureError {
    ordinal: usize,
    message: String,
}

impl fmt::Display for FixtureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for FixtureError {}

fn fixture_error(ordinal: usize, message: impl Into<String>) -> RequestMetaInfoCollaboratorError {
    Box::new(FixtureError {
        ordinal,
        message: message.into(),
    })
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn requester<F>(request: F) -> Arc<dyn DhtMetaInfoRequester>
where
    F: Fn(Id20, SocketAddr) -> RequestFuture + Send + Sync + 'static,
{
    Arc::new(TestRequester {
        request: Arc::new(request),
    })
}

fn checker<F>(check: F) -> Arc<dyn DhtMetaInfoBanningChecker>
where
    F: Fn(&Info) -> Result<(), RequestMetaInfoCollaboratorError> + Send + Sync + 'static,
{
    Arc::new(TestChecker {
        check: Arc::new(check),
    })
}

fn blocker<F>(block: F) -> Arc<dyn DhtInfoHashBlocker>
where
    F: Fn(Vec<Id20>, bool) -> BlockFuture + Send + Sync + 'static,
{
    Arc::new(TestBlocker {
        block: Arc::new(block),
    })
}

fn panic_requester() -> Arc<dyn DhtMetaInfoRequester> {
    requester(|_, _| panic!("requester must not be called"))
}

fn panic_checker() -> Arc<dyn DhtMetaInfoBanningChecker> {
    checker(|_| panic!("checker must not be called"))
}

fn panic_blocker() -> Arc<dyn DhtInfoHashBlocker> {
    blocker(|_, _| panic!("blocker must not be called"))
}

fn id(value: &str) -> Id20 {
    Id20::from_slice(&decode_hex::<20>(value)).unwrap()
}

fn decode_hex<const N: usize>(value: &str) -> [u8; N] {
    assert_eq!(value.len(), N * 2);
    let mut decoded = [0; N];
    for (index, byte) in decoded.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).unwrap();
    }
    decoded
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn socket_addr(value: &Address) -> SocketAddr {
    match IpAddr::from_str(&value.ip).unwrap() {
        IpAddr::V4(ip) => {
            assert_eq!(value.scope, 0);
            SocketAddr::new(IpAddr::V4(ip), value.port)
        }
        IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(ip, value.port, 0, value.scope)),
    }
}

fn address(value: SocketAddr) -> Address {
    match value {
        SocketAddr::V4(value) => Address {
            ip: value.ip().to_string(),
            port: value.port(),
            scope: 0,
        },
        SocketAddr::V6(value) => Address {
            ip: value.ip().to_string(),
            port: value.port(),
            scope: value.scope_id(),
        },
    }
}

fn worker_request(value: &Request) -> DhtMetaInfoRequest {
    DhtMetaInfoRequest {
        info_hash: id(&value.info_hash),
        source_node_addr: socket_addr(&value.node),
        peers: value.peers.iter().map(socket_addr).collect(),
    }
}

fn projection(value: &ParsedInfo) -> ParsedProjection {
    ParsedProjection {
        name: String::from_utf8_lossy(value.info().best_name()).into_owned(),
        meta_version: value.meta_version().as_u8(),
        info_hash_v1: value
            .info_hash_v1()
            .map(|hash| encode_hex(&hash))
            .unwrap_or_default(),
        info_hash_v2: value
            .info_hash_v2()
            .map(|hash| encode_hex(&hash))
            .unwrap_or_default(),
    }
}

fn outcome_projection(value: &Outcome) -> ParsedProjection {
    ParsedProjection {
        name: value.name.clone(),
        meta_version: value.meta_version,
        info_hash_v1: value.info_hash_v1.clone(),
        info_hash_v2: value.info_hash_v2.clone(),
    }
}

fn handoff(info_hash: Id20, node: SocketAddr, meta_info: &ParsedInfo) -> Handoff {
    let parsed = projection(meta_info);
    Handoff {
        info_hash: encode_hex(info_hash.as_bytes()),
        node: address(node),
        name: parsed.name,
        meta_version: parsed.meta_version,
        info_hash_v1: parsed.info_hash_v1,
        info_hash_v2: parsed.info_hash_v2,
    }
}

fn hybrid_info() -> ParsedInfo {
    let raw = extract_info_dictionary(HYBRID_TORRENT);
    parse_info_bytes(decode_hex("631a31dd0a46257d5078c0dee4e66e26f73e42ac"), raw).unwrap()
}

fn invalid_info() -> ParsedInfo {
    let raw = b"d6:lengthi0e4:name1:\xffe";
    parse_info_bytes(decode_hex("80b26192d4afd1a76f8a52d1899bc59af904c0b8"), raw).unwrap()
}

fn allowed_after_cancel_info() -> ParsedInfo {
    let mut raw =
        b"d6:lengthi4096e4:name20:allowed.after.cancel12:piece lengthi32768e6:pieces20:".to_vec();
    raw.extend_from_slice(&[0; 20]);
    raw.push(b'e');
    parse_info_bytes(decode_hex("a83888153d6da2c33af736495ed645f290b09dd9"), &raw).unwrap()
}

fn extract_info_dictionary(torrent: &[u8]) -> &[u8] {
    assert_eq!(torrent.first(), Some(&b'd'));
    let mut cursor = 1;
    while torrent[cursor] != b'e' {
        let (key, value_start) = byte_string(torrent, cursor);
        let value_end = skip_value(torrent, value_start, 0);
        if key == b"info" {
            return &torrent[value_start..value_end];
        }
        cursor = value_end;
    }
    panic!("torrent fixture has no info dictionary")
}

fn byte_string(input: &[u8], start: usize) -> (&[u8], usize) {
    let colon = input[start..]
        .iter()
        .position(|byte| *byte == b':')
        .map(|offset| start + offset)
        .unwrap();
    let length = std::str::from_utf8(&input[start..colon])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let value_start = colon + 1;
    let value_end = value_start + length;
    (&input[value_start..value_end], value_end)
}

fn skip_value(input: &[u8], start: usize, depth: usize) -> usize {
    assert!(depth < 256);
    match input[start] {
        b'i' => input[start + 1..]
            .iter()
            .position(|byte| *byte == b'e')
            .map(|offset| start + offset + 2)
            .unwrap(),
        b'l' => {
            let mut cursor = start + 1;
            while input[cursor] != b'e' {
                cursor = skip_value(input, cursor, depth + 1);
            }
            cursor + 1
        }
        b'd' => {
            let mut cursor = start + 1;
            while input[cursor] != b'e' {
                let (_, value_start) = byte_string(input, cursor);
                cursor = skip_value(input, value_start, depth + 1);
            }
            cursor + 1
        }
        b'0'..=b'9' => byte_string(input, start).1,
        byte => panic!("invalid fixture bencode byte {byte:#x}"),
    }
}

fn assert_normal_conservation(stats: DhtRequestMetaInfoWorkerStats) {
    assert_eq!(stats.dequeued, stats.tasks_completed);
    assert_eq!(
        stats.peer_occurrences,
        stats.request_attempts_failed
            + stats.request_attempts_succeeded
            + stats.peer_occurrences_skipped
    );
    assert_eq!(
        stats.request_attempts_started,
        stats.request_attempts_failed + stats.request_attempts_succeeded
    );
    assert_eq!(
        stats.request_attempts_succeeded,
        stats.allowed + stats.banned
    );
    assert_eq!(
        stats.tasks_completed,
        stats.empty_peers_dropped + stats.all_peers_failed + stats.allowed + stats.banned
    );
    assert_eq!(stats.block_calls_started, stats.banned);
    assert_eq!(
        stats.banned,
        stats.block_succeeded + stats.block_failed_ignored
    );
    assert_eq!(
        stats.allowed,
        stats.persist_queued + stats.persist_closed_dropped
    );
    assert_eq!(stats.shutdown_queued_dropped, 0);
    assert_eq!(stats.shutdown_tasks_cancelled, 0);
    assert_eq!(stats.shutdown_peer_occurrences_dropped, 0);
    assert_eq!(stats.shutdown_request_attempts_cancelled, 0);
    assert_eq!(stats.shutdown_block_calls_cancelled, 0);
    assert_eq!(stats.shutdown_persist_requests_dropped, 0);
}

fn assert_shutdown_conservation(stats: DhtRequestMetaInfoWorkerStats) {
    assert_eq!(
        stats.dequeued,
        stats.tasks_completed + stats.shutdown_tasks_cancelled
    );
    assert_eq!(
        stats.peer_occurrences,
        stats.request_attempts_failed
            + stats.request_attempts_succeeded
            + stats.peer_occurrences_skipped
            + stats.shutdown_peer_occurrences_dropped
    );
    assert_eq!(
        stats.request_attempts_started,
        stats.request_attempts_failed
            + stats.request_attempts_succeeded
            + stats.shutdown_request_attempts_cancelled
    );
    assert_eq!(
        stats.tasks_completed,
        stats.empty_peers_dropped + stats.all_peers_failed + stats.allowed + stats.banned
    );
    assert_eq!(
        stats.request_attempts_succeeded,
        stats.allowed
            + stats.banned
            + stats.shutdown_block_calls_cancelled
            + stats.shutdown_persist_requests_dropped
    );
    assert_eq!(
        stats.block_calls_started,
        stats.block_succeeded + stats.block_failed_ignored + stats.shutdown_block_calls_cancelled
    );
    assert_eq!(
        stats.banned,
        stats.block_succeeded + stats.block_failed_ignored
    );
    assert_eq!(
        stats.allowed,
        stats.persist_queued + stats.persist_closed_dropped
    );
}

#[tokio::test]
async fn zero_peers_replays_as_owned_rust_hardening_without_zero_parsed_info() {
    let row = fixture(FIXTURE_IDS[1]);
    let request = worker_request(row.input.request.as_ref().unwrap());
    assert!(request.peers.is_empty());
    assert_eq!(row.expected.handoff_in_calls, 1);
    assert_eq!(row.expected.handoff_deliveries.len(), 1);
    assert_eq!(row.expected.handoff_deliveries[0].meta_version, 0);

    let (input, receiver) = dht_request_meta_info_channel();
    let (persist, mut output) = dht_persist_torrent_channel();
    let (worker, stats) = DhtRequestMetaInfoWorker::new(
        receiver,
        persist,
        panic_requester(),
        panic_checker(),
        panic_blocker(),
    );
    input.send(request).await.unwrap();
    drop(input);
    assert_eq!(
        worker.run(pending()).await,
        DhtRequestMetaInfoWorkerExit::InputClosed
    );
    assert!(output.try_recv().is_err());
    let snapshot = stats.snapshot();
    assert_eq!(
        snapshot,
        DhtRequestMetaInfoWorkerStats {
            dequeued: 1,
            tasks_completed: 1,
            empty_peers_dropped: 1,
            ..DhtRequestMetaInfoWorkerStats::default()
        }
    );
    assert_normal_conservation(snapshot);
}

#[tokio::test]
async fn ordered_duplicate_hybrid_success_replays_through_actual_worker() {
    let row = fixture(FIXTURE_IDS[2]);
    let fixture_request = row.input.request.as_ref().unwrap();
    let request = worker_request(fixture_request);
    let expected_calls = row.expected.requester_calls.clone();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed_calls = Arc::clone(&calls);
    let events = Arc::new(Mutex::new(Vec::new()));
    let request_events = Arc::clone(&events);
    let outcomes = row.input.outcomes.clone();
    let success_outcome = outcomes[2].clone();
    let attempt = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::clone(&attempt);
    let hybrid = hybrid_info();
    let successful = hybrid.clone();
    let requester_impl = requester(move |hash, peer| {
        observed_calls.lock().unwrap().push((hash, peer));
        let index = attempts.fetch_add(1, Ordering::Relaxed);
        request_events
            .lock()
            .unwrap()
            .push(format!("request:{}", index + 1));
        let outcome = outcomes[index].clone();
        let parsed = successful.clone();
        Box::pin(async move {
            match outcome.kind.as_str() {
                "error" => Err(fixture_error(index, outcome.error)),
                "success" => Ok(parsed),
                kind => panic!("unexpected outcome {kind}"),
            }
        })
    });
    let banning_calls = Arc::new(Mutex::new(Vec::new()));
    let observed_banning = Arc::clone(&banning_calls);
    let banning_events = Arc::clone(&events);
    let checker = checker(move |info| {
        banning_events
            .lock()
            .unwrap()
            .push("ban_check:1".to_owned());
        observed_banning
            .lock()
            .unwrap()
            .push(String::from_utf8_lossy(info.best_name()).into_owned());
        DefaultDhtMetaInfoBanningChecker.check(info)
    });

    let (input, receiver) = dht_request_meta_info_channel();
    let (persist, mut output) = dht_persist_torrent_channel();
    let (worker, stats) =
        DhtRequestMetaInfoWorker::new(receiver, persist, requester_impl, checker, panic_blocker());
    input.send(request.clone()).await.unwrap();
    drop(input);
    assert_eq!(
        worker.run(pending()).await,
        DhtRequestMetaInfoWorkerExit::InputClosed
    );

    let actual_calls = lock(&calls)
        .iter()
        .map(|(hash, peer)| RequesterCall {
            info_hash: encode_hex(hash.as_bytes()),
            peer: address(*peer),
            context_cancelled: false,
        })
        .collect::<Vec<_>>();
    assert_eq!(actual_calls, expected_calls);
    assert_eq!(*lock(&banning_calls), row.expected.banning_calls);
    let persisted = output.recv().await.unwrap();
    lock(&events).push("persist_in:1".to_owned());
    assert_eq!(
        handoff(
            persisted.info_hash,
            persisted.source_node_addr,
            persisted.meta_info.as_ref()
        ),
        row.expected.handoff_deliveries[0]
    );
    assert_eq!(persisted.meta_info.as_ref(), &hybrid);
    assert_eq!(projection(&hybrid), outcome_projection(&success_outcome));
    assert_eq!(*lock(&events), row.expected.events[1..]);
    assert_eq!(row.expected.events[0], "lane_callback:1");
    assert!(output.recv().await.is_none());
    let snapshot = stats.snapshot();
    assert_eq!(
        snapshot,
        DhtRequestMetaInfoWorkerStats {
            dequeued: 1,
            tasks_completed: 1,
            peer_occurrences: 4,
            request_attempts_started: 3,
            request_attempts_failed: 2,
            request_attempts_succeeded: 1,
            peer_occurrences_skipped: 1,
            allowed: 1,
            persist_queued: 1,
            ..DhtRequestMetaInfoWorkerStats::default()
        }
    );
    assert_normal_conservation(snapshot);
}

#[derive(Default)]
struct AttemptObserver {
    started: usize,
    failed: usize,
    succeeded: usize,
}

impl RequestMetaInfoAttemptObserver for AttemptObserver {
    fn request_started(&mut self) {
        self.started += 1;
    }

    fn request_failed(&mut self) {
        self.failed += 1;
    }

    fn request_succeeded(&mut self) {
        self.succeeded += 1;
    }
}

#[tokio::test]
async fn all_failures_replay_ordered_display_cause_identity_and_worker_drop() {
    let row = fixture(FIXTURE_IDS[3]);
    let fixture_request = row.input.request.as_ref().unwrap();
    let request = worker_request(fixture_request);
    let outcomes = row.input.outcomes.clone();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed_calls = Arc::clone(&calls);
    let events = Arc::new(Mutex::new(Vec::new()));
    let request_events = Arc::clone(&events);
    let attempt = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::clone(&attempt);
    let helper_requester = requester(move |hash, peer| {
        observed_calls.lock().unwrap().push((hash, peer));
        let index = attempts.fetch_add(1, Ordering::Relaxed);
        request_events
            .lock()
            .unwrap()
            .push(format!("request:{}", index + 1));
        let message = outcomes[index].error.clone();
        Box::pin(ready(Err(fixture_error(index, message))))
    });
    let mut observer = AttemptObserver::default();
    let failures = request_first_meta_info(
        helper_requester.as_ref(),
        request.info_hash,
        &request.peers,
        &mut observer,
    )
    .await
    .unwrap_err();
    assert_eq!(failures.to_string(), row.expected.do_error);
    assert_eq!(
        failures
            .errors()
            .iter()
            .enumerate()
            .map(|(index, error)| {
                error
                    .downcast_ref::<FixtureError>()
                    .is_some_and(|error| error.ordinal == index)
            })
            .collect::<Vec<_>>(),
        row.expected.do_error_identities.clone().unwrap()
    );
    assert_eq!(
        (observer.started, observer.failed, observer.succeeded),
        (3, 3, 0)
    );
    let actual_calls = lock(&calls)
        .iter()
        .map(|(hash, peer)| RequesterCall {
            info_hash: encode_hex(hash.as_bytes()),
            peer: address(*peer),
            context_cancelled: false,
        })
        .collect::<Vec<_>>();
    assert_eq!(actual_calls, row.expected.requester_calls);
    assert_eq!(*lock(&events), row.expected.events);
    assert_eq!(address(request.peers[1]), fixture_request.peers[1]);
    assert_eq!(request.peers[0], request.peers[2]);
    assert_eq!(
        request.peers[1],
        SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::from_str("2001:db8::7").unwrap(),
            7307,
            0,
            11
        ))
    );

    let outcomes = row.input.outcomes;
    let attempt = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::clone(&attempt);
    let worker_requester = requester(move |_, _| {
        let index = attempts.fetch_add(1, Ordering::Relaxed);
        Box::pin(ready(Err(fixture_error(
            index,
            outcomes[index].error.clone(),
        ))))
    });
    let (input, receiver) = dht_request_meta_info_channel();
    let (persist, mut output) = dht_persist_torrent_channel();
    let (worker, stats) = DhtRequestMetaInfoWorker::new(
        receiver,
        persist,
        worker_requester,
        panic_checker(),
        panic_blocker(),
    );
    input.send(request).await.unwrap();
    drop(input);
    assert_eq!(
        worker.run(pending()).await,
        DhtRequestMetaInfoWorkerExit::InputClosed
    );
    assert!(output.try_recv().is_err());
    let snapshot = stats.snapshot();
    assert_eq!(
        snapshot,
        DhtRequestMetaInfoWorkerStats {
            dequeued: 1,
            tasks_completed: 1,
            peer_occurrences: 3,
            request_attempts_started: 3,
            request_attempts_failed: 3,
            all_peers_failed: 1,
            ..DhtRequestMetaInfoWorkerStats::default()
        }
    );
    assert_normal_conservation(snapshot);
}

#[tokio::test]
async fn exact_default_triple_ban_blocks_original_hash_false_and_ignores_error() {
    let row = fixture(FIXTURE_IDS[4]);
    let request = worker_request(row.input.request.as_ref().unwrap());
    let parsed = invalid_info();
    assert_eq!(parsed.info().best_name(), [0xff]);
    assert_eq!(
        String::from_utf8_lossy(parsed.info().best_name()),
        row.expected.banning_calls[0]
    );
    assert_ne!(
        parsed.info_hash_v1().unwrap().as_slice(),
        request.info_hash.as_bytes()
    );
    let requester_calls = Arc::new(Mutex::new(Vec::new()));
    let observed_requests = Arc::clone(&requester_calls);
    let events = Arc::new(Mutex::new(Vec::new()));
    let request_events = Arc::clone(&events);
    let requester = requester(move |hash, peer| {
        observed_requests.lock().unwrap().push((hash, peer));
        request_events.lock().unwrap().push("request:1".to_owned());
        Box::pin(ready(Ok(parsed.clone())))
    });
    let banning_calls = Arc::new(Mutex::new(Vec::new()));
    let banning_errors = Arc::new(Mutex::new(Vec::new()));
    let observed_calls = Arc::clone(&banning_calls);
    let observed_errors = Arc::clone(&banning_errors);
    let banning_events = Arc::clone(&events);
    let checker = checker(move |info| {
        banning_events
            .lock()
            .unwrap()
            .push("ban_check:1".to_owned());
        observed_calls
            .lock()
            .unwrap()
            .push(String::from_utf8_lossy(info.best_name()).into_owned());
        let result = DefaultDhtMetaInfoBanningChecker.check(info);
        if let Err(error) = &result {
            observed_errors.lock().unwrap().push(error.to_string());
        }
        result
    });
    let block_calls = Arc::new(Mutex::new(Vec::new()));
    let observed_blocks = Arc::clone(&block_calls);
    let block_events = Arc::clone(&events);
    let block_error = row.input.block_error.clone();
    let blocker = blocker(move |hashes, flush| {
        block_events.lock().unwrap().push("block:1".to_owned());
        observed_blocks.lock().unwrap().push((hashes, flush));
        Box::pin(ready(Err(fixture_error(0, block_error.clone()))))
    });

    let (input, receiver) = dht_request_meta_info_channel();
    let (persist, mut output) = dht_persist_torrent_channel();
    let (worker, stats) =
        DhtRequestMetaInfoWorker::new(receiver, persist, requester, checker, blocker);
    input.send(request.clone()).await.unwrap();
    drop(input);
    assert_eq!(
        worker.run(pending()).await,
        DhtRequestMetaInfoWorkerExit::InputClosed
    );
    assert!(output.try_recv().is_err());
    let actual_requester_calls = lock(&requester_calls)
        .iter()
        .map(|(hash, peer)| RequesterCall {
            info_hash: encode_hex(hash.as_bytes()),
            peer: address(*peer),
            context_cancelled: false,
        })
        .collect::<Vec<_>>();
    assert_eq!(actual_requester_calls, row.expected.requester_calls);
    assert_eq!(*lock(&banning_calls), row.expected.banning_calls);
    assert_eq!(*lock(&banning_errors), row.expected.banning_errors);
    assert_eq!(row.expected.events[0], "lane_callback:1");
    assert_eq!(*lock(&events), row.expected.events[1..]);
    let actual_blocks = lock(&block_calls)
        .iter()
        .map(|(hashes, flush)| BlockCall {
            hashes: hashes
                .iter()
                .map(|hash| encode_hex(hash.as_bytes()))
                .collect(),
            flush: *flush,
        })
        .collect::<Vec<_>>();
    assert_eq!(actual_blocks, row.expected.block_calls);
    assert_eq!(
        actual_blocks[0].hashes,
        [encode_hex(request.info_hash.as_bytes())]
    );
    assert!(!actual_blocks[0].flush);
    let snapshot = stats.snapshot();
    assert_eq!(
        snapshot,
        DhtRequestMetaInfoWorkerStats {
            dequeued: 1,
            tasks_completed: 1,
            peer_occurrences: 2,
            request_attempts_started: 1,
            request_attempts_succeeded: 1,
            peer_occurrences_skipped: 1,
            banned: 1,
            block_calls_started: 1,
            block_failed_ignored: 1,
            ..DhtRequestMetaInfoWorkerStats::default()
        }
    );
    assert_normal_conservation(snapshot);
}

async fn replay_pending_request_shutdown(row: Fixture) {
    let request = worker_request(row.input.request.as_ref().unwrap());
    assert_eq!(request.peers.len(), 2);
    assert_eq!(row.input.cancel_requester_at_call, 1);
    assert_eq!(row.input.outcomes[0].kind, "pending_until_cancel");
    let started = Arc::new(Notify::new());
    let request_started = Arc::clone(&started);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed_calls = Arc::clone(&calls);
    let requester = requester(move |hash, peer| {
        observed_calls.lock().unwrap().push((hash, peer));
        request_started.notify_one();
        Box::pin(pending())
    });
    let (input, receiver) = dht_request_meta_info_channel();
    let (persist, mut output) = dht_persist_torrent_channel();
    let (worker, stats) = DhtRequestMetaInfoWorker::new(
        receiver,
        persist,
        requester,
        panic_checker(),
        panic_blocker(),
    );
    input.send(request.clone()).await.unwrap();
    drop(input);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let run = tokio::spawn(worker.run(async move {
        shutdown_rx.await.unwrap();
    }));
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("requester did not reach the pending shutdown barrier");
    shutdown_tx.send(()).unwrap();
    assert_eq!(
        run.await.unwrap(),
        DhtRequestMetaInfoWorkerExit::Shutdown {
            queued_dropped: 0,
            tasks_cancelled: 1,
            peer_occurrences_dropped: 2,
            request_attempts_cancelled: 1,
            block_calls_cancelled: 0,
            persist_requests_dropped: 0,
        }
    );
    assert!(output.try_recv().is_err());
    assert_eq!(*lock(&calls), vec![(request.info_hash, request.peers[0])]);
    assert_eq!(row.expected.requester_calls.len(), 2);
    assert!(row.expected.context_cancelled);
    let snapshot = stats.snapshot();
    assert_eq!(
        snapshot,
        DhtRequestMetaInfoWorkerStats {
            dequeued: 1,
            peer_occurrences: 2,
            request_attempts_started: 1,
            shutdown_tasks_cancelled: 1,
            shutdown_peer_occurrences_dropped: 2,
            shutdown_request_attempts_cancelled: 1,
            ..DhtRequestMetaInfoWorkerStats::default()
        }
    );
    assert_shutdown_conservation(snapshot);
}

#[tokio::test]
async fn pending_request_shutdown_replays_owned_lifecycle_delta() {
    replay_pending_request_shutdown(fixture(FIXTURE_IDS[5])).await;
}

#[tokio::test(flavor = "current_thread")]
async fn row7_conceptually_replays_owned_blocked_persist_cancellation() {
    let row = fixture(FIXTURE_IDS[6]);
    let request = worker_request(row.input.request.as_ref().unwrap());
    assert_eq!(row.input.outcomes[0].kind, "pending_until_cancel");
    assert_eq!(row.input.outcomes[1].kind, "success");
    assert_eq!(row.expected.banning_calls, ["allowed.after.cancel"]);
    assert_eq!(row.expected.handoff_in_calls, 1);
    assert!(row.expected.handoff_deliveries.is_empty());

    let parsed = allowed_after_cancel_info();
    let filler_hash = id("0000000000000000000000000000000000000001");
    let filler = DhtPersistTorrentRequest {
        info_hash: filler_hash,
        source_node_addr: socket_addr(&row.input.request.as_ref().unwrap().node),
        meta_info: Arc::new(parsed.clone()),
    };
    let (persist, mut output) = dht_persist_torrent_channel();
    for _ in 0..DHT_PERSIST_TORRENT_ROUTE_CAPACITY {
        persist.send(filler.clone()).await.unwrap();
    }

    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed_calls = Arc::clone(&calls);
    let requester = requester(move |hash, peer| {
        observed_calls.lock().unwrap().push((hash, peer));
        Box::pin(ready(Ok(parsed.clone())))
    });
    let checker_entered = Arc::new(Notify::new());
    let observed_checker = Arc::clone(&checker_entered);
    let checker = checker(move |info| {
        assert_eq!(info.best_name(), b"allowed.after.cancel");
        observed_checker.notify_one();
        DefaultDhtMetaInfoBanningChecker.check(info)
    });
    let (input, receiver) = dht_request_meta_info_channel();
    let (worker, stats) =
        DhtRequestMetaInfoWorker::new(receiver, persist, requester, checker, panic_blocker());
    input.send(request.clone()).await.unwrap();
    drop(input);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let run = tokio::spawn(worker.run(async move {
        shutdown_rx.await.unwrap();
    }));

    // This test-only notification marks the synchronous checker prefix; it is
    // not a production hook. On the current-thread runtime, this task cannot
    // observe it until the child next yields. Its next await is the send to a
    // full route whose receiver is deliberately not polled, so the request
    // cannot have committed before shutdown.
    tokio::time::timeout(Duration::from_secs(5), checker_entered.notified())
        .await
        .expect("checker did not reach the blocked-persist prefix");
    shutdown_tx.send(()).unwrap();
    assert_eq!(
        run.await.unwrap(),
        DhtRequestMetaInfoWorkerExit::Shutdown {
            queued_dropped: 0,
            tasks_cancelled: 1,
            peer_occurrences_dropped: 0,
            request_attempts_cancelled: 0,
            block_calls_cancelled: 0,
            persist_requests_dropped: 1,
        }
    );
    assert_eq!(*lock(&calls), vec![(request.info_hash, request.peers[0])]);

    let mut retained_prefix = Vec::with_capacity(DHT_PERSIST_TORRENT_ROUTE_CAPACITY);
    for _ in 0..DHT_PERSIST_TORRENT_ROUTE_CAPACITY {
        retained_prefix.push(output.recv().await.unwrap());
    }
    assert!(retained_prefix.iter().all(|item| item == &filler));
    assert!(retained_prefix
        .iter()
        .all(|item| item.info_hash != request.info_hash));
    assert!(output.recv().await.is_none());

    let snapshot = stats.snapshot();
    assert_eq!(
        snapshot,
        DhtRequestMetaInfoWorkerStats {
            dequeued: 1,
            peer_occurrences: 2,
            request_attempts_started: 1,
            request_attempts_succeeded: 1,
            peer_occurrences_skipped: 1,
            shutdown_tasks_cancelled: 1,
            shutdown_persist_requests_dropped: 1,
            ..DhtRequestMetaInfoWorkerStats::default()
        }
    );
    assert_shutdown_conservation(snapshot);
}

#[tokio::test]
async fn go_only_lane_error_is_partitioned_from_rust_typed_input_eof() {
    let row = fixture(FIXTURE_IDS[7]);
    assert_eq!(row.classification, "GO_ONLY_LANE");
    assert!(row.input.lane_return_error);
    assert_eq!(row.expected.events, ["lane_return_error"]);
    assert!(row.expected.run_returned);

    let (input, receiver) = dht_request_meta_info_channel();
    let (persist, mut output) = dht_persist_torrent_channel();
    let (worker, stats) = DhtRequestMetaInfoWorker::new(
        receiver,
        persist,
        panic_requester(),
        panic_checker(),
        panic_blocker(),
    );
    drop(input);
    assert_eq!(
        worker.run(pending()).await,
        DhtRequestMetaInfoWorkerExit::InputClosed
    );
    assert!(output.try_recv().is_err());
    assert_eq!(stats.snapshot(), DhtRequestMetaInfoWorkerStats::default());
}
