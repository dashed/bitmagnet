use std::collections::BTreeMap;
use std::future::{pending, Future};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bitmagnet_dht::{
    dht_discovery_channel, dht_get_peers_channel, DhtInfoHashTriageRequest, GetPeersResult, Id20,
    KTable, KTableCommand, KTableHashPeer, KTableNodeHandle, KTableNodeOption, RoutingNode,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::{oneshot, Notify};

use super::get_peers::DhtGetPeersWorkerCore;
use super::{
    dht_request_meta_info_channel, DhtGetPeersWorkerExit, DhtGetPeersWorkerStats,
    DhtGetPeersWorkerStatsHandle, DhtMetaInfoRequest,
};

const FIXTURE_TEXT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/dht_crawler_get_peers.jsonl"
));
const FIXTURE_BYTES: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/dht_crawler_get_peers.jsonl"
));
const FIXTURE_SHA256: &str = "82b694fece9e46c05aefaab76bc05b78462bc04824bf6b83bb77eb544b7f0844";

const FIXTURE_IDS: [&str; 8] = [
    "production_source_factory_and_lifecycle_contract",
    "query_error_drops_request_ip_and_preserves_cause",
    "success_nodes_without_values_puts_responder_and_fans_out_before_no_peers",
    "success_preserves_node_and_peer_order_duplicates_and_puts_hash_before_metadata",
    "cancelled_before_client_return_still_puts_responder_and_hash_but_abandons_fanout_and_metadata",
    "cancel_after_one_discovery_retains_prefix_and_hash_but_abandons_suffix_and_metadata",
    "cancellation_at_blocked_metadata_send_keeps_table_prefix",
    "lane_error_is_swallowed",
];

const ROW_CLASSIFICATIONS: [&str; 8] = [
    "SOURCE_ONLY",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA",
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
        "RUST_ACTUAL_WORKER_QUERY_ERROR_AND_KTABLE_DROP_ADDR_REPLAY",
    ),
    (
        FIXTURE_IDS[2],
        "RUST_ACTUAL_WORKER_EMPTY_VALUES_PUT_NODE_DISCOVERY_NO_METADATA_REPLAY",
    ),
    (
        FIXTURE_IDS[3],
        "RUST_ACTUAL_WORKER_SUCCESS_PUT_NODE_DISCOVERY_PUT_HASH_METADATA_REPLAY",
    ),
    (
        FIXTURE_IDS[4],
        "RUST_OWNED_WORKER_PENDING_QUERY_SHUTDOWN_DELTA_REPLAY",
    ),
    (
        FIXTURE_IDS[5],
        "RUST_OWNED_WORKER_DISCOVERY_PREFIX_SHUTDOWN_DELTA_REPLAY",
    ),
    (
        FIXTURE_IDS[6],
        "RUST_OWNED_WORKER_BLOCKED_METADATA_SHUTDOWN_DELTA_REPLAY",
    ),
    (
        FIXTURE_IDS[7],
        "GO_ONLY_MANUAL_LANE_ERROR_WITH_NO_RUST_ROUTE_ANALOGUE",
    ),
];

const DELIBERATE_RUST_DELTAS: [&str; 10] = [
    "Rust_owns_and_joins_bounded_query_tasks_instead_of_detaching_Go_callbacks",
    "Rust_input_EOF_is_typed_and_never_repeats_zero_value_callbacks",
    "Rust_shutdown_closes_and_drains_the_input_then_aborts_and_joins_accepted_tasks",
    "Rust_shutdown_during_a_pending_query_does_not_apply_Go_post_cancellation_table_updates",
    "Rust_shutdown_after_a_recursive_prefix_abandons_the_recursive_suffix_before_PutHash",
    "Rust_shutdown_at_metadata_backpressure_preserves_the_already_committed_PutHash",
    "Rust_discovery_uses_cancellation_safe_owned_reservations_with_one_whole_suffix_deadline",
    "Rust_metadata_send_future_is_owned_and_cancelled_by_worker_shutdown",
    "Rust_has_typed_EOF_shutdown_and_counter_accounting_instead_of_swallowing_a_lane_Run_error",
    "Rust_KTable_drops_by_IP_and_numeric_scope_but_does_not_store_the_Go_error_cause",
];

const RUST_NONCLAIMS: [&str; 17] = [
    "exact_Go_ready_select_tie_winner_or_eager_channel_operand_side_effects",
    "Go_goroutine_callback_scheduling_completion_order_or_semaphore_fairness",
    "closed_Go_buffered_input_runtime_execution_or_callback_join_guarantee",
    "actual_one_second_wall_clock_timeout_elapsed_in_fixture_replays",
    "send_to_closed_Go_channel_behavior",
    "exact_wall_clock_NodeResponded_timestamp",
    "KTable_map_iteration_eviction_or_internal_layout",
    "opaque_Go_NodeOption_function_identity_or_error_cause_storage_in_Rust_KTable",
    "live_DNS_UDP_DHT_network_or_client_wire_behavior",
    "downstream_discovered_node_deduplication_scheduling_or_routing",
    "metainfo_requester_banning_blocking_or_persistence_behavior",
    "torrent_source_database_or_nonempty_durability_behavior",
    "production_throughput_total_retention_or_waiter_fairness",
    "application_supervisor_deployment_or_production_readiness",
    "arbitrary_textual_IPv6_zones_beyond_numeric_scope",
    "Go_lane_Run_error_semantics_in_the_owned_Rust_input_route",
    "concurrent_external_pending_send_accounting_outside_prequeued_fixture_inputs",
];

const NORMALIZED_AST: [(&str, &str); 16] = [
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
        "client.GetPeersResult",
        "8d03c6b4898e797df9d24b99d4a7bdda8d4c62d6a02e8396e800ccbf65f9ae20",
    ),
    (
        "config.NewDefaultConfig",
        "d044a4710817daf9a87dfab03ce22f138da3c6e1bf94d40bbbfd0fea70673f32",
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
        "getpeers.requestPeersForHash",
        "aa0925cbdac93e87b66eb828d9e3cd170633625243e6eab987569a9bb30fc880",
    ),
    (
        "getpeers.runGetPeers",
        "82b1c5e9584e838a171a2d40acd1e5a0719d49a010d0e15076ea6d5e3d21a3b0",
    ),
    (
        "ktable.DropAddr",
        "ab8ca0a52e22a72b0e37325cbccccf98de5211fc415e0ae139015ccdc9e91cd3",
    ),
    (
        "ktable.HashPeer",
        "96c1c2acc982e6c5e6ded04aa22dcb83e7a49fe49032519fbad0a5c18f32f378",
    ),
    (
        "ktable.NodeResponded",
        "52c5c68a8e6125a6d89839181e4dcb69bd62a1c857d2cf33c2f935d9c521e3d4",
    ),
    (
        "ktable.PutHash",
        "75d31635e1942347ecffd0e0d7e3084c822a9526d6b34737ba954fc3c03250dd",
    ),
    (
        "ktable.PutNode",
        "f85a3fc30b4e45d98dadc9b26ff08b34a49e97d01757e4aa8d69757b0cacdc00",
    ),
];

const GO_SOURCES: [(&str, &[u8], &str); 16] = [
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
        "internal/dhtcrawler/discovered_nodes.go",
        include_bytes!("../../../../internal/dhtcrawler/discovered_nodes.go"),
        "22806cabf39173df71010a54d874a4319458f1715308834be828dbdb99767027",
    ),
    (
        "internal/dhtcrawler/factory.go",
        include_bytes!("../../../../internal/dhtcrawler/factory.go"),
        "ed34129835773817736d70e74c7c884e5b9197e35741dee922ee9a5d691288a6",
    ),
    (
        "internal/dhtcrawler/get_peers.go",
        include_bytes!("../../../../internal/dhtcrawler/get_peers.go"),
        "c90a41f46c322969188cde55a9e09f64025bc0d7192faca13f50c1bcc8d6cbbf",
    ),
    (
        "internal/protocol/id.go",
        include_bytes!("../../../../internal/protocol/id.go"),
        "e1947e2b4af4cc008f5bb8cf5000ebfe784a82e119cb0418c2a74c3ed5f8c26f",
    ),
    (
        "internal/protocol/dht/client/interface.go",
        include_bytes!("../../../../internal/protocol/dht/client/interface.go"),
        "477139d727ea685538bccfb0be114ab4fa43556cbdb70d5492a074f24482389f",
    ),
    (
        "internal/protocol/dht/ktable/command.go",
        include_bytes!("../../../../internal/protocol/dht/ktable/command.go"),
        "575e58a01856db0746281c3a66a95d6d5483452fb8ab20dc6379ffbc45cedf11",
    ),
    (
        "internal/protocol/dht/ktable/hash.go",
        include_bytes!("../../../../internal/protocol/dht/ktable/hash.go"),
        "05a350f23a2d21aa5b6fee431b9d065f912c0874b7f145ca59a90cf9dc9b3b8a",
    ),
    (
        "internal/protocol/dht/ktable/keyspace.go",
        include_bytes!("../../../../internal/protocol/dht/ktable/keyspace.go"),
        "fe0894e7df90dcfc85b10c72bba3c55d639fff3030735d78172d0b9fdf761573",
    ),
    (
        "internal/protocol/dht/ktable/node.go",
        include_bytes!("../../../../internal/protocol/dht/ktable/node.go"),
        "93ed9a76a7cd0f50ee3ad255c6e77a8d19e5fe17081edc6238c5efab4983b3c3",
    ),
    (
        "internal/protocol/dht/ktable/query.go",
        include_bytes!("../../../../internal/protocol/dht/ktable/query.go"),
        "103ec27a7904bdbbbd91f3ea1dae1f4d6ea3b3d6652757a6ab8ddbf598a7060e",
    ),
    (
        "internal/protocol/dht/ktable/reverse_map.go",
        include_bytes!("../../../../internal/protocol/dht/ktable/reverse_map.go"),
        "31e65f7b3b108e13c11772d375f97d7973b00dfc4df490d676a458d4f9a05213",
    ),
    (
        "internal/protocol/dht/ktable/table.go",
        include_bytes!("../../../../internal/protocol/dht/ktable/table.go"),
        "68e3caf4394b2692fd9358224cce2b70ae3d90d920097bd28885b6b3bb77848f",
    ),
];

const PREREQUISITE_FIXTURES: [(&str, &[u8], &str); 5] = [
    (
        "testdata/parity/dht/dht_crawler_discovered_nodes.jsonl",
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../testdata/parity/dht/dht_crawler_discovered_nodes.jsonl"
        )),
        "ae6d867378a227284aa0cd93e9120d70afbec1c5e3b19a9f64e09edace4190e0",
    ),
    (
        "testdata/parity/dht/dht_crawler_info_hash_triage.jsonl",
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../testdata/parity/dht/dht_crawler_info_hash_triage.jsonl"
        )),
        "52eda840f872225cc34f8cf12edc2e4621e8a1fef569abf34a50f4a3bd9896f8",
    ),
    (
        "testdata/parity/dht/ktable_core.jsonl",
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../testdata/parity/dht/ktable_core.jsonl"
        )),
        "b49854c20df24afec5f9bf76c22b2bdd12ca0a629cd3f199a742d44adf99844e",
    ),
    (
        "testdata/parity/dht/ktable_temporal.jsonl",
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../testdata/parity/dht/ktable_temporal.jsonl"
        )),
        "03178e62efbc40519ccc0496204a081469ef49cf6b1a2336cff39b474a745444",
    ),
    (
        "testdata/parity/dht/peer_sample_client.jsonl",
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../testdata/parity/dht/peer_sample_client.jsonl"
        )),
        "8c432a1555587a0c3dff51af3191c689adb3a2eda8b6515975ee1470b4bdfe51",
    ),
];

const EVIDENCE_COMMITS: [(&str, &str); 6] = [
    (
        "discovered_nodes_oracle",
        "069b3febcf1e270ffdaef9941bf56d494697bf2c",
    ),
    (
        "info_hash_triage_oracle",
        "6aece7ac7605507aaf5ccdcc9adf2497170b071d",
    ),
    (
        "ktable_core_oracle",
        "b345998fe0e3f3f99d35745588cbd8c375124ac8",
    ),
    (
        "ktable_temporal_oracle",
        "1df4d7a09f74e13e75ea2e1ab1dcfc67a130ed9d",
    ),
    (
        "peer_client_oracle",
        "1f00b40705ba527721208023ddec64220fb40729",
    ),
    (
        "typed_get_peers_route",
        "a5e2276ea9e2d93a75c3af8f4226bf2c333d27be",
    ),
];

const GO_NONCLAIMS: [&str; 18] = [
    "exact_ready_select_tie_winner",
    "goroutine_callback_scheduling_completion_or_order",
    "semaphore_fairness",
    "closed_buffered_input_runtime_execution",
    "callback_join_guarantee",
    "actual_one_second_timeout_elapsed_in_runtime_rows",
    "arbitrary_side_effects_of_eagerly_evaluated_channel_accessors_beyond_recorded_In_call_counts",
    "send_to_closed_Go_channel_behavior",
    "exact_wall_clock_NodeResponded_timestamp",
    "KTable_map_iteration_eviction_or_internal_layout",
    "opaque_NodeOption_function_identity",
    "live_DNS_UDP_DHT_network_or_client_wire_behavior",
    "downstream_discovered_node_deduplication_scheduling_or_routing",
    "metainfo_requester_banning_or_blocking_behavior",
    "torrent_source_persistence_or_database_behavior",
    "production_throughput_application_supervisor_or_deployment_wiring",
    "arbitrary_textual_IPv6_zones_runtime_rows_use_unscoped_or_numeric_scope_only",
    "Rust_public_API_or_owned_task_lifecycle_no_Rust_consumer_exists_in_this_slice",
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
    client: String,
    table: String,
    discovery: String,
    metadata: String,
    clock: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Input {
    kind: String,
    requests: Vec<Request>,
    outcomes: Vec<Outcome>,
    table_setup: Vec<TableSetup>,
    discovery_mode: Option<String>,
    discovery_capacity: usize,
    cancel_before_client_return: bool,
    cancel_after_discoveries: usize,
    metadata_mode: Option<String>,
    metadata_capacity: usize,
    cancel_at_metadata_in_call: usize,
    lane_return_error: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Request {
    info_hash: String,
    node: Address,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Outcome {
    kind: String,
    response_id: String,
    values: Vec<Address>,
    nodes: Vec<Node>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Node {
    id: String,
    addr: Address,
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
struct IpAddress {
    ip: String,
    scope: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TableSetup {
    kind: String,
    id: String,
    addr: Address,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Expected {
    client_calls: Vec<ClientCall>,
    same_context: bool,
    batch_calls: usize,
    commands: Vec<Command>,
    discovery_in_calls: usize,
    discoveries: Vec<Node>,
    metadata_in_calls: usize,
    metadata_deliveries: Vec<Metadata>,
    events: Vec<String>,
    table_post: TablePost,
    run_returned: bool,
    context_cancelled: bool,
    callback_completed: bool,
    source: Option<Source>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClientCall {
    node: Address,
    info_hash: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Command {
    kind: String,
    id: Option<String>,
    addr: Option<Address>,
    drop_ip: Option<IpAddress>,
    peers: Vec<Address>,
    option_count: usize,
    reason: Option<String>,
    error_identity_preserved: bool,
    stored_responded: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Metadata {
    info_hash: String,
    node: Address,
    peers: Vec<Address>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TablePost {
    nodes: Vec<NodePost>,
    hash: Option<HashPost>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodePost {
    id: String,
    present: bool,
    addr: Option<Address>,
    responded: bool,
    retained_dropped: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HashPost {
    id: String,
    found: bool,
    peers: Vec<Address>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Source {
    run_error_ignored: bool,
    shared_callback_context: bool,
    error_drops_request_ip_and_scope_without_port: bool,
    error_reason_wraps_cause: bool,
    success_uses_response_id: bool,
    success_uses_request_address: bool,
    success_uses_node_responded_option: bool,
    no_post_client_cancellation_before_put_node: bool,
    discovery_before_peer_presence_check: bool,
    empty_values_error: String,
    discovery_timeout_ms: u64,
    discovery_uses_response_order: bool,
    discovery_cancel_break_labelled: bool,
    discovery_cancel_break_scope: String,
    discovery_cancellation_retains_prefix: bool,
    discovery_cancellation_scans_suffix: bool,
    discovery_in_accessor_evaluated_for_suffix: bool,
    peer_order_and_duplicates_preserved: bool,
    put_hash_precedes_metadata: bool,
    metadata_cancellation_preserves_put_hash: bool,
    production_get_peers_capacity: usize,
    production_get_peers_concurrency: usize,
    production_metadata_capacity: usize,
    production_metadata_concurrency: usize,
    default_scaling_factor: usize,
    consumer_dequeues_before_semaphore: bool,
    consumer_callbacks_detached: bool,
    consumer_callbacks_joined: bool,
    maximum_retained_work: String,
    closed_input_checks_open_boolean: bool,
    closed_input_outcome: String,
    production_discovery_capacity: usize,
    production_discovery_max_batch_size: usize,
    production_discovery_interval_ms: u64,
    production_discovery_output_capacity: usize,
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
    assert!(source.error_drops_request_ip_and_scope_without_port);
    assert!(source.error_reason_wraps_cause);
    assert!(source.success_uses_response_id);
    assert!(source.success_uses_request_address);
    assert!(source.success_uses_node_responded_option);
    assert!(source.no_post_client_cancellation_before_put_node);
    assert!(source.discovery_before_peer_presence_check);
    assert_eq!(source.empty_values_error, "no peers found");
    assert_eq!(source.discovery_timeout_ms, 1_000);
    assert!(source.discovery_uses_response_order);
    assert!(!source.discovery_cancel_break_labelled);
    assert_eq!(
        source.discovery_cancel_break_scope,
        "select_only_not_for_loop"
    );
    assert!(source.discovery_cancellation_retains_prefix);
    assert!(source.discovery_cancellation_scans_suffix);
    assert!(source.discovery_in_accessor_evaluated_for_suffix);
    assert!(source.peer_order_and_duplicates_preserved);
    assert!(source.put_hash_precedes_metadata);
    assert!(source.metadata_cancellation_preserves_put_hash);
    assert_eq!(
        (
            source.production_get_peers_capacity,
            source.production_get_peers_concurrency,
            source.production_metadata_capacity,
            source.production_metadata_concurrency,
            source.default_scaling_factor,
        ),
        (100, 200, 100, 400, 10)
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
        "repeated_zero_value_callbacks_can_issue_invalid_zero_request_work"
    );
    assert_eq!(
        (
            source.production_discovery_capacity,
            source.production_discovery_max_batch_size,
            source.production_discovery_interval_ms,
            source.production_discovery_output_capacity,
        ),
        (1_000, 10, 10, 1)
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
        "runtime rows execute actual runGetPeers and requestPeersForHash synchronously through controlled interfaces and an actual KTable; source-only facts freeze full declarations and files"
    );
}

#[test]
fn source_schema_ids_hashes_nonclaims_and_execution_partition_are_exact() {
    assert_eq!(sha256(FIXTURE_BYTES), FIXTURE_SHA256);
    assert!(FIXTURE_BYTES.ends_with(b"\n"));
    assert_eq!(FIXTURE_TEXT.lines().count(), FIXTURE_IDS.len());
    assert_eq!(RUST_EXECUTION_PARTITION.map(|entry| entry.0), FIXTURE_IDS);
    assert_eq!(
        DELIBERATE_RUST_DELTAS,
        [
            "Rust_owns_and_joins_bounded_query_tasks_instead_of_detaching_Go_callbacks",
            "Rust_input_EOF_is_typed_and_never_repeats_zero_value_callbacks",
            "Rust_shutdown_closes_and_drains_the_input_then_aborts_and_joins_accepted_tasks",
            "Rust_shutdown_during_a_pending_query_does_not_apply_Go_post_cancellation_table_updates",
            "Rust_shutdown_after_a_recursive_prefix_abandons_the_recursive_suffix_before_PutHash",
            "Rust_shutdown_at_metadata_backpressure_preserves_the_already_committed_PutHash",
            "Rust_discovery_uses_cancellation_safe_owned_reservations_with_one_whole_suffix_deadline",
            "Rust_metadata_send_future_is_owned_and_cancelled_by_worker_shutdown",
            "Rust_has_typed_EOF_shutdown_and_counter_accounting_instead_of_swallowing_a_lane_Run_error",
            "Rust_KTable_drops_by_IP_and_numeric_scope_but_does_not_store_the_Go_error_cause",
        ]
    );
    assert_eq!(
        RUST_NONCLAIMS,
        [
            "exact_Go_ready_select_tie_winner_or_eager_channel_operand_side_effects",
            "Go_goroutine_callback_scheduling_completion_order_or_semaphore_fairness",
            "closed_Go_buffered_input_runtime_execution_or_callback_join_guarantee",
            "actual_one_second_wall_clock_timeout_elapsed_in_fixture_replays",
            "send_to_closed_Go_channel_behavior",
            "exact_wall_clock_NodeResponded_timestamp",
            "KTable_map_iteration_eviction_or_internal_layout",
            "opaque_Go_NodeOption_function_identity_or_error_cause_storage_in_Rust_KTable",
            "live_DNS_UDP_DHT_network_or_client_wire_behavior",
            "downstream_discovered_node_deduplication_scheduling_or_routing",
            "metainfo_requester_banning_blocking_or_persistence_behavior",
            "torrent_source_database_or_nonempty_durability_behavior",
            "production_throughput_total_retention_or_waiter_fairness",
            "application_supervisor_deployment_or_production_readiness",
            "arbitrary_textual_IPv6_zones_beyond_numeric_scope",
            "Go_lane_Run_error_semantics_in_the_owned_Rust_input_route",
            "concurrent_external_pending_send_accounting_outside_prequeued_fixture_inputs",
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
        assert_eq!(fixture.subsystem, "dht_crawler_get_peers");
        assert_eq!(fixture.classification, ROW_CLASSIFICATIONS[index]);
        assert_eq!(fixture.expected.source.is_some(), index == 0);
    }

    let source_fixture = &fixtures[0];
    assert_eq!(
        source_fixture.oracle,
        Oracle {
            composition: "production_source_factory_and_lifecycle_freshness_gate".to_owned(),
            determinism: "exact_normalized_AST_source_and_prerequisite_fixture_SHA256".to_owned(),
            lane: "production_BufferedConcurrentChannel_source_shape".to_owned(),
            client: "production_Client_GetPeers_interface".to_owned(),
            table: "production_KTable_command_and_query_source_shapes".to_owned(),
            discovery: "production_BatchingChannel_source_shape".to_owned(),
            metadata: "production_BufferedConcurrentChannel_source_shape".to_owned(),
            clock: "timeout_and_NodeResponded_source_only".to_owned(),
        }
    );
    assert_eq!(source_fixture.input.kind, "source_contract");
    assert!(source_fixture.input.requests.is_empty());
    assert!(source_fixture.input.outcomes.is_empty());
    assert!(source_fixture.input.table_setup.is_empty());
    assert!(source_fixture.input.discovery_mode.is_none());
    assert_eq!(source_fixture.input.discovery_capacity, 0);
    assert!(!source_fixture.input.cancel_before_client_return);
    assert_eq!(source_fixture.input.cancel_after_discoveries, 0);
    assert!(source_fixture.input.metadata_mode.is_none());
    assert_eq!(source_fixture.input.metadata_capacity, 0);
    assert_eq!(source_fixture.input.cancel_at_metadata_in_call, 0);
    assert!(!source_fixture.input.lane_return_error);
    assert!(source_fixture.expected.client_calls.is_empty());
    assert!(!source_fixture.expected.same_context);
    assert_eq!(source_fixture.expected.batch_calls, 0);
    assert!(source_fixture.expected.commands.is_empty());
    assert_eq!(source_fixture.expected.discovery_in_calls, 0);
    assert!(source_fixture.expected.discoveries.is_empty());
    assert_eq!(source_fixture.expected.metadata_in_calls, 0);
    assert!(source_fixture.expected.metadata_deliveries.is_empty());
    assert!(source_fixture.expected.events.is_empty());
    assert!(source_fixture.expected.table_post.nodes.is_empty());
    assert!(source_fixture.expected.table_post.hash.is_none());
    assert!(!source_fixture.expected.run_returned);
    assert!(!source_fixture.expected.context_cancelled);
    assert!(!source_fixture.expected.callback_completed);
    assert_source_contract(
        source_fixture
            .expected
            .source
            .as_ref()
            .expect("source row has source contract"),
    );

    for fixture in &fixtures[1..=6] {
        assert_eq!(fixture.input.kind, "run_get_peers");
        assert_eq!(fixture.input.requests.len(), 1);
        assert_eq!(fixture.input.outcomes.len(), 1);
        assert!(!fixture.input.lane_return_error);
        assert!(fixture.expected.run_returned);
        assert!(fixture.expected.same_context);
        assert!(fixture.expected.callback_completed);
    }
    let go_only = &fixtures[7];
    assert!(go_only.input.requests.is_empty());
    assert!(go_only.input.outcomes.is_empty());
    assert!(go_only.input.lane_return_error);
    assert!(go_only
        .expected
        .events
        .iter()
        .any(|event| event == "lane_return_error"));
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplayBarrier {
    None,
    PendingQuery,
    SecondRecursiveReserve,
    MetadataSend,
}

type QueryFuture =
    Pin<Box<dyn Future<Output = Result<GetPeersResult, &'static str>> + Send + 'static>>;
type HookFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

struct Replay {
    exit: DhtGetPeersWorkerExit,
    stats: DhtGetPeersWorkerStats,
    calls: Vec<ClientCall>,
    commands: Vec<KTableCommand>,
    events: Vec<String>,
    discoveries: Vec<Node>,
    metadata: Vec<Metadata>,
    table: KTable,
    retained: BTreeMap<String, KTableNodeHandle>,
    discovery_hook_calls: usize,
    metadata_hook_calls: usize,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn id(value: &str) -> Id20 {
    Id20::from_hex(value).unwrap_or_else(|error| panic!("invalid fixture ID {value}: {error}"))
}

fn socket_addr(value: &Address) -> SocketAddr {
    let ip = IpAddr::from_str(&value.ip)
        .unwrap_or_else(|error| panic!("invalid fixture IP {}: {error}", value.ip));
    match ip {
        IpAddr::V4(ip) => {
            assert_eq!(value.scope, 0, "IPv4 fixture address cannot carry scope");
            SocketAddr::V4(SocketAddrV4::new(ip, value.port))
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
        SocketAddr::V6(value) => {
            assert_eq!(
                value.flowinfo(),
                0,
                "fixture replay never creates IPv6 flowinfo"
            );
            Address {
                ip: value.ip().to_string(),
                port: value.port(),
                scope: value.scope_id(),
            }
        }
    }
}

fn routing_node(value: &Node) -> RoutingNode {
    RoutingNode {
        id: id(&value.id),
        addr: socket_addr(&value.addr),
    }
}

fn node(value: RoutingNode) -> Node {
    Node {
        id: value.id.to_hex(),
        addr: address(value.addr),
    }
}

fn worker_request(value: &Request) -> DhtInfoHashTriageRequest {
    DhtInfoHashTriageRequest {
        info_hash: id(&value.info_hash),
        source_node_addr: socket_addr(&value.node),
    }
}

fn query_result(value: &Outcome) -> Result<GetPeersResult, &'static str> {
    if value.kind == "error" {
        return Err("oracle get_peers failure");
    }
    assert_eq!(value.kind, "success");
    Ok(GetPeersResult {
        id: id(&value.response_id),
        values: value.values.iter().map(socket_addr).collect(),
        nodes: value.nodes.iter().map(routing_node).collect(),
    })
}

fn metadata(value: DhtMetaInfoRequest) -> Metadata {
    Metadata {
        info_hash: value.info_hash.to_hex(),
        node: address(value.source_node_addr),
        peers: value.peers.into_iter().map(address).collect(),
    }
}

fn placeholder_metadata() -> DhtMetaInfoRequest {
    DhtMetaInfoRequest {
        info_hash: Id20::ZERO,
        source_node_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1)),
        peers: Vec::new(),
    }
}

fn command_event(command: &KTableCommand) -> &'static str {
    match command {
        KTableCommand::DropAddr { .. } => "batch_drop_addr",
        KTableCommand::PutNode { .. } => "batch_put_node",
        KTableCommand::PutHash { .. } => "batch_put_hash",
        KTableCommand::DropNode { .. } => panic!("get-peers worker must not issue DropNode"),
    }
}

async fn replay(fixture: &Fixture, barrier: ReplayBarrier) -> Replay {
    assert_eq!(fixture.input.requests.len(), 1);
    assert_eq!(fixture.input.outcomes.len(), 1);
    let request = fixture.input.requests[0].clone();
    let outcome = fixture.input.outcomes[0].clone();

    let (input, input_receiver) = dht_get_peers_channel();
    input.send(worker_request(&request)).await.unwrap();
    drop(input);

    let table = KTable::new(Id20::ZERO);
    let mut retained = BTreeMap::new();
    for setup in &fixture.input.table_setup {
        assert_eq!(setup.kind, "put_same_node_twice_to_populate_reverse_map");
        let stored = RoutingNode {
            id: id(&setup.id),
            addr: socket_addr(&setup.addr),
        };
        table.put_node(stored);
        retained.insert(
            setup.id.clone(),
            table
                .node_handle(stored.id)
                .expect("first seed put creates a retained node handle"),
        );
        table.put_node(stored);
    }

    let discovery_capacity = NonZeroUsize::new(fixture.input.discovery_capacity.max(1)).unwrap();
    let (discovery, mut discovery_receiver) = dht_discovery_channel(discovery_capacity);
    let (meta_info, mut meta_info_receiver) = dht_request_meta_info_channel();
    let placeholder = placeholder_metadata();
    let prefilled_metadata: usize = if barrier == ReplayBarrier::MetadataSend {
        for _ in 0..100 {
            meta_info.send(placeholder.clone()).await.unwrap();
        }
        100
    } else {
        0
    };

    let stats = DhtGetPeersWorkerStatsHandle::default();
    let mut core = DhtGetPeersWorkerCore::new(
        input_receiver,
        table.clone(),
        meta_info,
        discovery,
        NonZeroUsize::new(1).unwrap(),
        stats.clone(),
    );
    let calls = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(Mutex::new(Vec::new()));
    let commands = Arc::new(Mutex::new(Vec::new()));
    let entered = Arc::new(Notify::new());
    let discovery_hook_calls = Arc::new(Mutex::new(0usize));
    let metadata_hook_calls = Arc::new(Mutex::new(0usize));

    let query_calls = Arc::clone(&calls);
    let query_events = Arc::clone(&events);
    let query_entered = Arc::clone(&entered);
    let query_outcome = outcome.clone();
    let query = move |remote: SocketAddr, info_hash: Id20| -> QueryFuture {
        let calls = Arc::clone(&query_calls);
        let events = Arc::clone(&query_events);
        let entered = Arc::clone(&query_entered);
        let result = query_result(&query_outcome);
        Box::pin(async move {
            lock(&events).push("lane_callback:1".to_owned());
            lock(&calls).push(ClientCall {
                node: address(remote),
                info_hash: info_hash.to_hex(),
            });
            lock(&events).push("client_get_peers:1".to_owned());
            if barrier == ReplayBarrier::PendingQuery {
                entered.notify_one();
                pending::<()>().await;
            }
            result
        })
    };

    let recursive_events = Arc::clone(&events);
    let recursive_entered = Arc::clone(&entered);
    let recursive_calls = Arc::clone(&discovery_hook_calls);
    let before_recursive = move |index: usize, _node: RoutingNode| -> HookFuture {
        let events = Arc::clone(&recursive_events);
        let entered = Arc::clone(&recursive_entered);
        let calls = Arc::clone(&recursive_calls);
        Box::pin(async move {
            {
                let mut count = lock(&calls);
                *count = count.saturating_add(1);
            }
            lock(&events).push(format!("discovery_in:{}", index + 1));
            if barrier == ReplayBarrier::SecondRecursiveReserve && index == 1 {
                entered.notify_one();
            }
        })
    };

    let metadata_events = Arc::clone(&events);
    let metadata_entered = Arc::clone(&entered);
    let metadata_calls = Arc::clone(&metadata_hook_calls);
    let before_metadata = move |_request: DhtMetaInfoRequest| -> HookFuture {
        let events = Arc::clone(&metadata_events);
        let entered = Arc::clone(&metadata_entered);
        let calls = Arc::clone(&metadata_calls);
        Box::pin(async move {
            {
                let mut count = lock(&calls);
                *count = count.saturating_add(1);
            }
            lock(&events).push("metadata_in:1".to_owned());
            if barrier == ReplayBarrier::MetadataSend {
                entered.notify_one();
            }
        })
    };

    let observed_commands = Arc::clone(&commands);
    let command_events = Arc::clone(&events);
    let observe_command = move |command: &KTableCommand| {
        lock(&observed_commands).push(command.clone());
        lock(&command_events).push(command_event(command).to_owned());
    };

    let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
    let mut shutdown_sender = Some(shutdown_sender);
    let run = tokio::spawn(async move {
        core.run_with(
            async move {
                let _ = shutdown_receiver.await;
            },
            query,
            || tokio::time::Instant::now() + Duration::from_secs(1),
            before_recursive,
            before_metadata,
            observe_command,
        )
        .await
    });
    if barrier != ReplayBarrier::None {
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("worker reached the requested deterministic barrier");
        shutdown_sender
            .take()
            .expect("shutdown sender is used once")
            .send(())
            .expect("worker still owns the shutdown receiver");
    }
    let exit = run.await.expect("get-peers worker replay task joins");
    drop(shutdown_sender);

    let mut discoveries = Vec::new();
    while let Ok(value) = discovery_receiver.try_recv() {
        discoveries.push(node(value));
    }
    let mut metadata_deliveries = Vec::new();
    let mut drained_metadata = 0usize;
    while let Ok(value) = meta_info_receiver.try_recv() {
        drained_metadata = drained_metadata.saturating_add(1);
        if value != placeholder {
            metadata_deliveries.push(metadata(value));
        }
    }
    assert_eq!(
        drained_metadata,
        prefilled_metadata.saturating_add(metadata_deliveries.len())
    );

    let calls = lock(&calls).clone();
    let commands = lock(&commands).clone();
    let events = lock(&events).clone();
    let discovery_hook_calls = *lock(&discovery_hook_calls);
    let metadata_hook_calls = *lock(&metadata_hook_calls);

    Replay {
        exit,
        stats: stats.snapshot(),
        calls,
        commands,
        events,
        discoveries,
        metadata: metadata_deliveries,
        table,
        retained,
        discovery_hook_calls,
        metadata_hook_calls,
    }
}

fn expected_commands(fixture: &Fixture) -> Vec<KTableCommand> {
    fixture
        .expected
        .commands
        .iter()
        .map(|command| match command.kind.as_str() {
            "drop_addr" => {
                let request_addr = socket_addr(&fixture.input.requests[0].node);
                let drop_ip = command
                    .drop_ip
                    .as_ref()
                    .expect("drop command has IP projection");
                assert_eq!(address(request_addr).ip, drop_ip.ip);
                assert_eq!(address(request_addr).scope, drop_ip.scope);
                assert_eq!(
                    command.reason.as_deref(),
                    Some("failed to get peers: oracle get_peers failure")
                );
                assert!(command.error_identity_preserved);
                KTableCommand::DropAddr { addr: request_addr }
            }
            "put_node" => {
                assert_eq!(command.option_count, 1);
                assert!(command.stored_responded);
                KTableCommand::PutNode {
                    node: RoutingNode {
                        id: id(command.id.as_deref().expect("PutNode ID")),
                        addr: socket_addr(command.addr.as_ref().expect("PutNode address")),
                    },
                    options: vec![KTableNodeOption::Responded],
                }
            }
            "put_hash" => {
                assert_eq!(command.option_count, 0);
                KTableCommand::PutHash {
                    id: id(command.id.as_deref().expect("PutHash ID")),
                    peers: command
                        .peers
                        .iter()
                        .map(|addr| KTableHashPeer {
                            addr: socket_addr(addr),
                        })
                        .collect(),
                }
            }
            kind => panic!("unexpected fixture command {kind}"),
        })
        .collect()
}

fn assert_table_post(replay: &Replay, expected: &TablePost) {
    for expected_node in &expected.nodes {
        let handle = replay.table.node_handle(id(&expected_node.id));
        assert_eq!(handle.is_some(), expected_node.present, "node presence");
        if let Some(handle) = handle {
            assert_eq!(expected_node.addr.as_ref(), Some(&address(handle.addr())));
            assert_eq!(
                handle.last_responded_at().is_some(),
                expected_node.responded
            );
        } else {
            assert!(expected_node.addr.is_none());
            assert!(!expected_node.responded);
        }
        let retained_dropped = replay
            .retained
            .get(&expected_node.id)
            .is_some_and(KTableNodeHandle::dropped);
        assert_eq!(retained_dropped, expected_node.retained_dropped);
    }
    if let Some(expected_hash) = &expected.hash {
        let hash = replay.table.hash(id(&expected_hash.id));
        assert_eq!(hash.is_some(), expected_hash.found, "hash presence");
        if let Some(hash) = hash {
            let mut peers = hash
                .peers
                .into_iter()
                .map(|peer| address(peer.addr))
                .collect::<Vec<_>>();
            peers.sort();
            let mut expected_peers = expected_hash.peers.clone();
            expected_peers.sort();
            assert_eq!(peers, expected_peers);
        } else {
            assert!(expected_hash.peers.is_empty());
        }
    }
}

fn exact_stats(id: &str) -> DhtGetPeersWorkerStats {
    match id {
        "query_error_drops_request_ip_and_preserves_cause" => DhtGetPeersWorkerStats {
            dequeued: 1,
            queries_started: 1,
            tasks_completed: 1,
            queries_failed: 1,
            drop_addr_commands: 1,
            ..DhtGetPeersWorkerStats::default()
        },
        "success_nodes_without_values_puts_responder_and_fans_out_before_no_peers" => {
            DhtGetPeersWorkerStats {
                dequeued: 1,
                queries_started: 1,
                tasks_completed: 1,
                queries_succeeded: 1,
                put_node_commands: 1,
                recursive_nodes: 2,
                recursive_nodes_queued: 2,
                responses_without_peers: 1,
                ..DhtGetPeersWorkerStats::default()
            }
        }
        "success_preserves_node_and_peer_order_duplicates_and_puts_hash_before_metadata" => {
            DhtGetPeersWorkerStats {
                dequeued: 1,
                queries_started: 1,
                tasks_completed: 1,
                queries_succeeded: 1,
                put_node_commands: 1,
                recursive_nodes: 2,
                recursive_nodes_queued: 2,
                responses_with_peers: 1,
                peer_values: 4,
                put_hash_commands: 1,
                meta_info_queued: 1,
                ..DhtGetPeersWorkerStats::default()
            }
        }
        id => panic!("no exact stats for {id}"),
    }
}

fn assert_exact_replay(fixture: &Fixture, replay: &Replay) {
    assert_eq!(replay.exit, DhtGetPeersWorkerExit::InputClosed);
    assert_eq!(replay.calls, fixture.expected.client_calls);
    assert_eq!(replay.commands, expected_commands(fixture));
    assert_eq!(replay.commands.len(), fixture.expected.batch_calls);
    assert_eq!(replay.events, fixture.expected.events);
    assert_eq!(
        replay.discovery_hook_calls,
        fixture.expected.discovery_in_calls
    );
    assert_eq!(replay.discoveries, fixture.expected.discoveries);
    assert_eq!(
        replay.metadata_hook_calls,
        fixture.expected.metadata_in_calls
    );
    assert_eq!(replay.metadata, fixture.expected.metadata_deliveries);
    assert_eq!(replay.stats, exact_stats(&fixture.id));
    assert_table_post(replay, &fixture.expected.table_post);
}

#[tokio::test(start_paused = true)]
async fn query_error_replays_through_actual_worker_and_real_ktable() {
    let fixture = fixture(FIXTURE_IDS[1]);
    let replay = replay(&fixture, ReplayBarrier::None).await;
    assert_exact_replay(&fixture, &replay);
}

#[tokio::test(start_paused = true)]
async fn empty_values_still_put_responder_and_fan_out_before_no_peers() {
    let fixture = fixture(FIXTURE_IDS[2]);
    let replay = replay(&fixture, ReplayBarrier::None).await;
    assert_exact_replay(&fixture, &replay);
}

#[tokio::test(start_paused = true)]
async fn success_preserves_recursive_and_peer_vectors_then_hash_before_metadata() {
    let fixture = fixture(FIXTURE_IDS[3]);
    let replay = replay(&fixture, ReplayBarrier::None).await;
    assert_exact_replay(&fixture, &replay);
}

#[tokio::test(start_paused = true)]
async fn owned_shutdown_during_pending_query_commits_none_of_the_go_post_cancel_prefix() {
    let fixture = fixture(FIXTURE_IDS[4]);
    let replay = replay(&fixture, ReplayBarrier::PendingQuery).await;
    assert_eq!(
        replay.exit,
        DhtGetPeersWorkerExit::Shutdown {
            queued_dropped: 0,
            tasks_cancelled: 1,
            recursive_nodes_dropped: 0,
            meta_info_requests_dropped: 0,
        }
    );
    assert_eq!(replay.calls, fixture.expected.client_calls);
    assert!(replay.commands.is_empty());
    assert_eq!(replay.events, ["lane_callback:1", "client_get_peers:1"]);
    assert_eq!(replay.discovery_hook_calls, 0);
    assert!(replay.discoveries.is_empty());
    assert_eq!(replay.metadata_hook_calls, 0);
    assert!(replay.metadata.is_empty());
    assert!(replay
        .table
        .node_handle(id(&fixture.input.outcomes[0].response_id))
        .is_none());
    assert!(replay
        .table
        .hash(id(&fixture.input.requests[0].info_hash))
        .is_none());
    assert_eq!(
        replay.stats,
        DhtGetPeersWorkerStats {
            dequeued: 1,
            queries_started: 1,
            shutdown_tasks_cancelled: 1,
            ..DhtGetPeersWorkerStats::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn owned_shutdown_after_one_recursive_commit_accounts_for_exact_suffix() {
    let fixture = fixture(FIXTURE_IDS[5]);
    let replay = replay(&fixture, ReplayBarrier::SecondRecursiveReserve).await;
    assert_eq!(
        replay.exit,
        DhtGetPeersWorkerExit::Shutdown {
            queued_dropped: 0,
            tasks_cancelled: 1,
            recursive_nodes_dropped: 2,
            meta_info_requests_dropped: 1,
        }
    );
    assert_eq!(replay.calls, fixture.expected.client_calls);
    assert_eq!(replay.commands, expected_commands(&fixture)[..1]);
    assert_eq!(
        replay.events,
        [
            "lane_callback:1",
            "client_get_peers:1",
            "batch_put_node",
            "discovery_in:1",
            "discovery_in:2",
        ]
    );
    assert_eq!(replay.discovery_hook_calls, 2);
    assert_eq!(replay.discoveries, fixture.expected.discoveries[..1]);
    assert_eq!(replay.metadata_hook_calls, 0);
    assert!(replay.metadata.is_empty());
    let response = replay
        .table
        .node_handle(id(&fixture.input.outcomes[0].response_id))
        .expect("successful query put responder before recursive fanout");
    assert_eq!(address(response.addr()), fixture.input.requests[0].node);
    assert!(response.last_responded_at().is_some());
    assert!(replay
        .table
        .hash(id(&fixture.input.requests[0].info_hash))
        .is_none());
    assert_eq!(
        replay.stats,
        DhtGetPeersWorkerStats {
            dequeued: 1,
            queries_started: 1,
            queries_succeeded: 1,
            put_node_commands: 1,
            recursive_nodes: 3,
            recursive_nodes_queued: 1,
            responses_with_peers: 1,
            peer_values: 1,
            shutdown_tasks_cancelled: 1,
            shutdown_recursive_nodes_dropped: 2,
            shutdown_meta_info_dropped: 1,
            ..DhtGetPeersWorkerStats::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn owned_shutdown_at_metadata_backpressure_preserves_table_prefix() {
    let fixture = fixture(FIXTURE_IDS[6]);
    let replay = replay(&fixture, ReplayBarrier::MetadataSend).await;
    assert_eq!(
        replay.exit,
        DhtGetPeersWorkerExit::Shutdown {
            queued_dropped: 0,
            tasks_cancelled: 1,
            recursive_nodes_dropped: 0,
            meta_info_requests_dropped: 1,
        }
    );
    assert_eq!(replay.calls, fixture.expected.client_calls);
    assert_eq!(replay.commands, expected_commands(&fixture));
    assert_eq!(replay.events, fixture.expected.events);
    assert_eq!(replay.discovery_hook_calls, 0);
    assert!(replay.discoveries.is_empty());
    assert_eq!(replay.metadata_hook_calls, 1);
    assert!(replay.metadata.is_empty());
    assert_table_post(&replay, &fixture.expected.table_post);
    assert_eq!(
        replay.stats,
        DhtGetPeersWorkerStats {
            dequeued: 1,
            queries_started: 1,
            queries_succeeded: 1,
            put_node_commands: 1,
            responses_with_peers: 1,
            peer_values: 1,
            put_hash_commands: 1,
            shutdown_tasks_cancelled: 1,
            shutdown_meta_info_dropped: 1,
            ..DhtGetPeersWorkerStats::default()
        }
    );
}
