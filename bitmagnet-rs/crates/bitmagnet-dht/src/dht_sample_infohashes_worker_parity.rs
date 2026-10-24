use std::collections::{BTreeMap, VecDeque};
use std::future::{pending, ready};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use super::*;
use crate::dht_discovered_node_scheduler::DhtDiscoveredNodeSampleInfoHashesInput;
use crate::{
    dht_discovery_channel, dht_info_hash_triage_channel, KTableBep51Support, KTableNodeHandle,
    RoutingPutResult,
};

const FIXTURE_TEXT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/dht_crawler_sample_infohashes_worker.jsonl"
));
const FIXTURE_BYTES: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/dht_crawler_sample_infohashes_worker.jsonl"
));
const FIXTURE_SHA256: &str = "8533c4644ceaed71a372ef52ec944f1b625f48c0042e1ef7f45990dbe0ef2744";
const FIXTURE_IDS: [&str; 5] = [
    "production_source_callback_interval_put_and_fanout_contract",
    "actual_buffered_lane_mutated_interface_node_candidate_skipped",
    "eligible_client_error_drops_advertised_node",
    "ordered_novel_prefix_cancel_after_full_dedupe",
    "clamp_put_then_detached_recursive_prefix_cancel",
];
const ROW_CLASSIFICATIONS: [&str; 5] = [
    "SOURCE_ONLY",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
];
const RUST_EXECUTION_PARTITION: [(&str, &str); 5] = [
    (FIXTURE_IDS[0], "SOURCE_ONLY_NO_WHOLE_WORKER_RUNTIME_REPLAY"),
    (
        FIXTURE_IDS[1],
        "RUST_RETAINED_HANDLE_CANDIDATE_RECHECK_WITH_GO_SEMAPHORE_TIMELINE_EXCLUDED",
    ),
    (FIXTURE_IDS[2], "RUST_TYPED_QUERY_ERROR_AND_DROP_REPLAY"),
    (
        FIXTURE_IDS[3],
        "RUST_FULL_DEDUPE_DYNAMIC_ADDRESS_TRIAGE_PREFIX_AND_SHUTDOWN_SUFFIX_REPLAY",
    ),
    (
        FIXTURE_IDS[4],
        "RUST_CLAMP_ATOMIC_PUT_OWNED_FANOUT_PREFIX_AND_SHUTDOWN_SUFFIX_REPLAY",
    ),
];
const DELIBERATE_RUST_DELTAS: [&str; 10] = [
    "Rust_owns_and_joins_every_accepted_task_in_one_JoinSet_instead_of_detaching_callbacks_and_fanout",
    "Rust_does_not_dequeue_while_max_inflight_is_exhausted",
    "Rust_shutdown_is_biased_ahead_of_join_and_receive_and_returns_typed_exact_suffix_accounting",
    "Rust_route_EOF_and_downstream_receiver_closure_are_typed",
    "Rust_triage_and_discovery_reservations_are_cancellation_safe",
    "Rust_preserves_an_explicit_Discovered_or_Retained_internal_provenance_variant",
    "Rust_shutdown_and_observed_child_panic_cleanup_abort_and_join_all_owned_tasks",
    "dropping_the_worker_or_run_future_closes_input_and_aborts_owned_tasks_without_terminal_accounting_or_a_join_guarantee",
    "Rust_saturates_unrepresentable_std_Instant_deadlines_after_preserving_Go_signed_duration_wrap",
    "Rust_ready_timeout_bias_may_skip_the_Go_eager_discoveredNodes_In_operand_evaluation",
];
const RUST_NONCLAIMS: [&str; 11] = [
    "exact_Go_runtime_event_schedule_callback_detachment_or_fanout_detachment_is_not_replayed",
    "Go_interface_or_pointer_ABI_identity_is_not_claimed_by_Rust_retained_generation_equality",
    "Go_semaphore_mutex_or_channel_waiter_fairness_is_not_claimed",
    "Go_ready_select_tie_winners_and_eager_send_operand_evaluation_are_not_claimed",
    "exact_wall_clock_values_and_unrepresentable_Go_deadline_overflow_are_not_claimed",
    "exact_BoomFilters_RNG_decrement_sequence_retention_or_false_positive_behavior_is_not_replayed",
    "production_batch_flush_output_database_triage_and_downstream_discovery_routing_are_not_replayed",
    "KTable_map_iteration_eviction_and_opaque_Go_NodeOption_function_identity_are_not_claimed",
    "live_DNS_UDP_DHT_network_behavior_is_not_exercised",
    "supervisor_application_deployment_and_production_wiring_are_deferred",
    "the_response_ID_is_not_used_as_the_advertised_node_identity",
];

const GO_SOURCES: [(&str, &[u8], &str); 17] = [
    (
        "internal/concurrency/atomic.go",
        include_bytes!("../../../../internal/concurrency/atomic.go"),
        "09cc4842dbdf516f8574f26b411130daba526f69dbf217e1f2867e829f781a4f",
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
        "internal/dhtcrawler/sample_infohashes.go",
        include_bytes!("../../../../internal/dhtcrawler/sample_infohashes.go"),
        "483b9037673dce82f9026f2aec9448812f804c13484fd0bd2f55fcfc70a52983",
    ),
    (
        "internal/protocol/dht/client/interface.go",
        include_bytes!("../../../../internal/protocol/dht/client/interface.go"),
        "477139d727ea685538bccfb0be114ab4fa43556cbdb70d5492a074f24482389f",
    ),
    (
        "internal/protocol/dht/client/server_adapter.go",
        include_bytes!("../../../../internal/protocol/dht/client/server_adapter.go"),
        "51334196660c0baeb730b1968f70db06af2622ea706de3e093fad39420539afa",
    ),
    (
        "internal/protocol/dht/ktable/command.go",
        include_bytes!("../../../../internal/protocol/dht/ktable/command.go"),
        "575e58a01856db0746281c3a66a95d6d5483452fb8ab20dc6379ffbc45cedf11",
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
        "internal/protocol/dht/ktable/table.go",
        include_bytes!("../../../../internal/protocol/dht/ktable/table.go"),
        "68e3caf4394b2692fd9358224cce2b70ae3d90d920097bd28885b6b3bb77848f",
    ),
    (
        "internal/protocol/dht/msg.go",
        include_bytes!("../../../../internal/protocol/dht/msg.go"),
        "a5129736a50eeb47cf955c075bec982a19d1d498c7bb9de6ce130b3c68118e70",
    ),
    (
        "internal/protocol/id.go",
        include_bytes!("../../../../internal/protocol/id.go"),
        "e1947e2b4af4cc008f5bb8cf5000ebfe784a82e119cb0418c2a74c3ed5f8c26f",
    ),
];

const PREREQUISITE_FIXTURES: [(&str, &[u8], &str); 4] = [
    (
        "testdata/parity/dht/dht_crawler_ignore_hashes.jsonl",
        include_bytes!("../../../../testdata/parity/dht/dht_crawler_ignore_hashes.jsonl"),
        "7900b4046d10037b9c7541d36d79370a92ceb3135f9c81be0adef985ac1f4621",
    ),
    (
        "testdata/parity/dht/dht_crawler_sample_infohashes_producer.jsonl",
        include_bytes!(
            "../../../../testdata/parity/dht/dht_crawler_sample_infohashes_producer.jsonl"
        ),
        "b0069a060b32edc4e1c6f5b2008f6b50f796eea6d162b4df3a148cad29745c1e",
    ),
    (
        "testdata/parity/dht/ktable_temporal.jsonl",
        include_bytes!("../../../../testdata/parity/dht/ktable_temporal.jsonl"),
        "03178e62efbc40519ccc0496204a081469ef49cf6b1a2336cff39b474a745444",
    ),
    (
        "testdata/parity/dht/peer_sample_client.jsonl",
        include_bytes!("../../../../testdata/parity/dht/peer_sample_client.jsonl"),
        "8c432a1555587a0c3dff51af3191c689adb3a2eda8b6515975ee1470b4bdfe51",
    ),
];

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Fixture {
    id: String,
    subsystem: String,
    classification: String,
    oracle: Oracle,
    input: Input,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Oracle {
    composition: String,
    determinism: String,
    lane: String,
    client: String,
    deduper: String,
    table: String,
    triage: String,
    fanout: String,
    clock: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Input {
    kind: String,
    lane_capacity: usize,
    lane_concurrency: usize,
    node: Option<Node>,
    response: Option<Response>,
    sought_target: Option<String>,
    preloaded_hashes: Option<Vec<String>>,
    oracle_rng_seed: Option<i64>,
    hash_indexes: Option<BTreeMap<String, Vec<u64>>>,
    mutate_candidate_after_take: Option<bool>,
    triage_capacity: usize,
    cancel_at_triage_in_call: usize,
    discovery_capacity: usize,
    cancel_at_discovery_in_call: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Node {
    token: String,
    id: String,
    addr: String,
    addr_returns: Option<Vec<String>>,
    initial_candidate: bool,
    final_candidate: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Response {
    kind: String,
    response_id: String,
    samples: Vec<String>,
    nodes: Vec<Node>,
    num: i64,
    interval: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Expected {
    node_calls: NodeCalls,
    client_calls: Vec<ClientCall>,
    same_context: bool,
    source_derived_deduper_call_order: Vec<String>,
    deduper_post_membership: BTreeMap<String, bool>,
    triage_in_calls: usize,
    triage_deliveries: Vec<TriageDelivery>,
    commands: Vec<Command>,
    discovery_in_calls: usize,
    discoveries: Vec<Node>,
    events: Vec<String>,
    run_returned: bool,
    context_cancelled: bool,
    callback_completion_observed: bool,
    fanout_completion_observed: bool,
    source: Option<Source>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodeCalls {
    id: usize,
    addr: usize,
    time: usize,
    dropped: usize,
    sample_infohashes_candidate: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClientCall {
    addr: String,
    target: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TriageDelivery {
    info_hash: String,
    node: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Command {
    kind: String,
    id: String,
    addr: Option<String>,
    option_count: usize,
    reason: Option<String>,
    error_identity_preserved: bool,
    stored_responded: bool,
    stored_candidate: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IntervalCase {
    name: String,
    raw_interval: i64,
    novel_count: usize,
    effective_interval: i64,
    duration_ns: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Source {
    run_error_ignored: bool,
    shared_callback_context: bool,
    candidate_checked_at_callback_time: bool,
    candidate_checked_before_client: bool,
    target_read_at_client_call: bool,
    response_id_ignored: bool,
    error_drops_advertised_id: bool,
    error_reason_wraps_cause: bool,
    samples_processed_in_response_order: bool,
    deduper_called_for_every_sample: bool,
    deduper_completes_before_triage: bool,
    only_novel_hashes_triaged: bool,
    node_address_reread_per_novel_hash: bool,
    triage_blocks_in_order: bool,
    triage_cancellation_aware: bool,
    triage_cancellation_branch_returns_before_put_fanout: bool,
    clamp_requires_novel_and_over300: bool,
    clamp_interval_seconds: i64,
    duration_conversion: String,
    go_int_bits: usize,
    interval_cases: Vec<IntervalCase>,
    put_uses_advertised_id_and_current_addr: bool,
    put_option_order: Vec<String>,
    put_discovered_count: String,
    put_total_count: String,
    put_deadline_expression: String,
    put_occurs_after_all_triage: bool,
    put_precedes_fanout_launch: bool,
    fanout_uses_response_order: bool,
    fanout_reads_captured_response_in_goroutine: bool,
    fanout_deep_copies_response_nodes: bool,
    fanout_detached: bool,
    fanout_joined: bool,
    fanout_whole_list_timeout_ms: i64,
    fanout_cancellation_aware: bool,
    production_capacity: usize,
    production_concurrency: usize,
    default_scaling_factor: usize,
    consumer_dequeues_before_semaphore: bool,
    acquire_cancellation_drops_dequeued_item: bool,
    maximum_retained_work: String,
    consumer_callbacks_detached: bool,
    consumer_callbacks_joined: bool,
    closed_input_checks_open_boolean: bool,
    closed_input_outcome: String,
    production_triage_capacity: usize,
    production_triage_max_batch_size: usize,
    production_triage_interval_ms: i64,
    production_triage_output_capacity: usize,
    production_discovery_capacity: usize,
    production_discovery_max_batch_size: usize,
    production_discovery_interval_ms: i64,
    production_discovery_output_capacity: usize,
    start_launches_worker_detached: bool,
    start_waits_only_stopped: bool,
    start_defers_shared_context_cancel: bool,
    start_joins_worker_callbacks_or_fanout: bool,
    fanout_can_outlive_worker_permit: bool,
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

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn id(hex: &str) -> Id20 {
    Id20::from_hex(hex).unwrap_or_else(|error| panic!("invalid fixture ID {hex}: {error}"))
}

fn addr(value: &str) -> SocketAddr {
    value
        .parse()
        .unwrap_or_else(|error| panic!("invalid fixture address {value}: {error}"))
}

fn routing_node(value: &Node) -> RoutingNode {
    RoutingNode {
        id: id(&value.id),
        addr: addr(&value.addr),
    }
}

fn retained(table: &KTable, value: &Node) -> KTableNodeHandle {
    let node = routing_node(value);
    assert_eq!(table.put_node(node), RoutingPutResult::Accepted);
    table.node_handle(node.id).expect("fixture node retained")
}

fn response(value: &Response) -> SampleInfoHashesResult {
    SampleInfoHashesResult {
        id: id(&value.response_id),
        samples: Some(value.samples.iter().map(|value| id(value)).collect()),
        nodes: value.nodes.iter().map(routing_node).collect(),
        num: value.num,
        interval: value.interval,
    }
}

fn core(
    input: DhtDiscoveredNodeSampleInfoHashesReceiver,
    table: KTable,
    triage: DhtInfoHashTriageInput,
    discovery: DhtDiscoverySender,
) -> (
    DhtSampleInfoHashesWorkerCore,
    DhtSampleInfoHashesWorkerStatsHandle,
) {
    let stats = DhtSampleInfoHashesWorkerStatsHandle::default();
    (
        DhtSampleInfoHashesWorkerCore::new(
            input,
            table,
            triage,
            discovery,
            NonZeroUsize::MIN,
            stats.clone(),
        ),
        stats,
    )
}

fn assert_shutdown_conservation(stats: DhtSampleInfoHashesWorkerStats) {
    assert_eq!(
        stats.dequeued,
        stats
            .tasks_completed
            .saturating_add(stats.shutdown_tasks_cancelled)
    );
    assert_eq!(
        stats.sample_hashes_returned,
        stats
            .sample_hashes_suppressed
            .saturating_add(stats.sample_hashes_novel)
    );
    assert_eq!(
        stats.sample_hashes_novel,
        stats
            .triage_queued
            .saturating_add(stats.triage_closed_dropped)
            .saturating_add(stats.shutdown_triage_hashes_dropped)
    );
    assert_eq!(
        stats.recursive_nodes,
        stats
            .recursive_nodes_queued
            .saturating_add(stats.recursive_nodes_closed_dropped)
            .saturating_add(stats.recursive_nodes_timed_out_dropped)
            .saturating_add(stats.shutdown_recursive_nodes_dropped)
    );
}

#[test]
fn fixture_schema_identity_sources_partition_and_metadata_are_frozen() {
    assert_eq!(sha256(FIXTURE_BYTES), FIXTURE_SHA256);
    let fixtures = fixtures();
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.id.as_str())
            .collect::<Vec<_>>(),
        FIXTURE_IDS
    );
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.classification.as_str())
            .collect::<Vec<_>>(),
        ROW_CLASSIFICATIONS
    );
    assert!(fixtures
        .iter()
        .all(|fixture| fixture.subsystem == "dht_crawler_sample_infohashes_worker"));
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| {
                (
                    fixture.oracle.composition.as_str(),
                    fixture.oracle.determinism.as_str(),
                    fixture.oracle.lane.as_str(),
                    fixture.oracle.client.as_str(),
                    fixture.oracle.deduper.as_str(),
                    fixture.oracle.table.as_str(),
                    fixture.oracle.triage.as_str(),
                    fixture.oracle.fanout.as_str(),
                    fixture.oracle.clock.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (
                "exact_production_worker_factory_start_channel_and_ktable_source_contract",
                "normalized_AST_exact_source_SHA256_prerequisite_fixture_SHA256_and_signed_integer_vectors",
                "production_buffered_concurrent_channel_source_only",
                "production_dht_client_interface_source_only",
                "composes_strict_actual_ignore_hashes_oracle",
                "production_ktable_commands_and_options_source_only",
                "production_shared_batching_channel_source_only",
                "production_shared_discovered_nodes_batching_channel_source_only",
                "duration_arithmetic_only_no_wall_clock_value_claim",
            ),
            (
                "actual_runSampleInfoHashes_with_actual_capacity_zero_concurrency_one_buffered_lane",
                "permit_blocker_proves_target_dequeued_then_mutated_before_callback",
                "actual_production_BufferedConcurrentChannel_implementation_with_oracle_dimensions",
                "must_not_be_called",
                "must_not_be_called",
                "must_not_be_called",
                "must_not_be_called",
                "must_not_be_called",
                "not_observed",
            ),
            (
                "actual_runSampleInfoHashes_with_manual_callback_lane_scripted_error_and_actual_ktable",
                "synchronous_callback_sentinel_error_and_no_downstream_sends",
                "manual_single_callback",
                "scripted_SampleInfoHashes_error",
                "actual_fresh_production_filter_not_reached",
                "tracing_wrapper_over_actual_ktable",
                "must_not_be_called",
                "must_not_be_called",
                "not_observed",
            ),
            (
                "actual_worker_actual_fresh_production_deduper_manual_lane_and_capacity_two_gated_triage",
                "fixed_RNG_seed_disjoint_hash_vectors_full_buffer_and_cancel_only_ready_after_third_In_gate",
                "manual_single_callback",
                "scripted_ordered_success",
                "actual_ignoreHashes_with_fresh_production_BoomFilters_and_preloaded_B",
                "tracing_actual_ktable_must_not_receive_command",
                "oracle_capacity_two_input_with_gate_inside_third_In",
                "must_not_be_launched_after_triage_cancellation",
                "interval_not_reached",
            ),
            (
                "actual_worker_actual_deduper_tracing_actual_ktable_capacity_one_triage_and_gated_capacity_two_discovery",
                "one_novel_forces_301_to_60_clamp_first_fanout_gate_proves_detachment_full_prefix_and_cancel_only_ready_at_third",
                "manual_single_callback",
                "scripted_success_with_one_novel_and_four_response_nodes",
                "actual_fresh_production_ignoreHashes",
                "tracing_wrapper_over_actual_ktable",
                "manual_capacity_one_input",
                "oracle_gates_inside_first_and_third_discoveredNodes_In_calls",
                "runtime_asserts_responded_and_not_candidate_but_not_absolute_time",
            ),
        ]
    );
    assert_source_row(&fixtures[0]);
    assert_candidate_row(&fixtures[1]);
    assert_error_row(&fixtures[2]);
    assert_dedupe_row(&fixtures[3]);
    assert_fanout_row(&fixtures[4]);
    assert_eq!(
        RUST_EXECUTION_PARTITION,
        [
            (FIXTURE_IDS[0], "SOURCE_ONLY_NO_WHOLE_WORKER_RUNTIME_REPLAY"),
            (
                FIXTURE_IDS[1],
                "RUST_RETAINED_HANDLE_CANDIDATE_RECHECK_WITH_GO_SEMAPHORE_TIMELINE_EXCLUDED",
            ),
            (FIXTURE_IDS[2], "RUST_TYPED_QUERY_ERROR_AND_DROP_REPLAY"),
            (
                FIXTURE_IDS[3],
                "RUST_FULL_DEDUPE_DYNAMIC_ADDRESS_TRIAGE_PREFIX_AND_SHUTDOWN_SUFFIX_REPLAY",
            ),
            (
                FIXTURE_IDS[4],
                "RUST_CLAMP_ATOMIC_PUT_OWNED_FANOUT_PREFIX_AND_SHUTDOWN_SUFFIX_REPLAY",
            ),
        ]
    );
    assert_eq!(
        DELIBERATE_RUST_DELTAS,
        [
            "Rust_owns_and_joins_every_accepted_task_in_one_JoinSet_instead_of_detaching_callbacks_and_fanout",
            "Rust_does_not_dequeue_while_max_inflight_is_exhausted",
            "Rust_shutdown_is_biased_ahead_of_join_and_receive_and_returns_typed_exact_suffix_accounting",
            "Rust_route_EOF_and_downstream_receiver_closure_are_typed",
            "Rust_triage_and_discovery_reservations_are_cancellation_safe",
            "Rust_preserves_an_explicit_Discovered_or_Retained_internal_provenance_variant",
            "Rust_shutdown_and_observed_child_panic_cleanup_abort_and_join_all_owned_tasks",
            "dropping_the_worker_or_run_future_closes_input_and_aborts_owned_tasks_without_terminal_accounting_or_a_join_guarantee",
            "Rust_saturates_unrepresentable_std_Instant_deadlines_after_preserving_Go_signed_duration_wrap",
            "Rust_ready_timeout_bias_may_skip_the_Go_eager_discoveredNodes_In_operand_evaluation",
        ]
    );
    assert_eq!(
        RUST_NONCLAIMS,
        [
            "exact_Go_runtime_event_schedule_callback_detachment_or_fanout_detachment_is_not_replayed",
            "Go_interface_or_pointer_ABI_identity_is_not_claimed_by_Rust_retained_generation_equality",
            "Go_semaphore_mutex_or_channel_waiter_fairness_is_not_claimed",
            "Go_ready_select_tie_winners_and_eager_send_operand_evaluation_are_not_claimed",
            "exact_wall_clock_values_and_unrepresentable_Go_deadline_overflow_are_not_claimed",
            "exact_BoomFilters_RNG_decrement_sequence_retention_or_false_positive_behavior_is_not_replayed",
            "production_batch_flush_output_database_triage_and_downstream_discovery_routing_are_not_replayed",
            "KTable_map_iteration_eviction_and_opaque_Go_NodeOption_function_identity_are_not_claimed",
            "live_DNS_UDP_DHT_network_behavior_is_not_exercised",
            "supervisor_application_deployment_and_production_wiring_are_deferred",
            "the_response_ID_is_not_used_as_the_advertised_node_identity",
        ]
    );
}

fn assert_source_row(fixture: &Fixture) {
    assert_eq!(fixture.input.kind, "source_contract");
    assert_eq!(
        (fixture.input.lane_capacity, fixture.input.lane_concurrency),
        (0, 0)
    );
    assert!(fixture.input.node.is_none());
    assert!(fixture.input.response.is_none());
    assert!(fixture.input.sought_target.is_none());
    assert!(fixture.input.preloaded_hashes.is_none());
    assert!(fixture.input.oracle_rng_seed.is_none());
    assert!(fixture.input.hash_indexes.is_none());
    assert!(fixture.input.mutate_candidate_after_take.is_none());
    assert_eq!(
        (
            fixture.input.triage_capacity,
            fixture.input.cancel_at_triage_in_call,
            fixture.input.discovery_capacity,
            fixture.input.cancel_at_discovery_in_call,
        ),
        (0, 0, 0, 0)
    );
    assert_empty_expected(&fixture.expected, true);
    let source = fixture.expected.source.as_ref().expect("source facts");
    assert_source_contract(source);
}

fn assert_source_contract(source: &Source) {
    assert!(source.run_error_ignored);
    assert!(source.shared_callback_context);
    assert!(source.candidate_checked_at_callback_time);
    assert!(source.candidate_checked_before_client);
    assert!(source.target_read_at_client_call);
    assert!(source.response_id_ignored);
    assert!(source.error_drops_advertised_id);
    assert!(source.error_reason_wraps_cause);
    assert!(source.samples_processed_in_response_order);
    assert!(source.deduper_called_for_every_sample);
    assert!(source.deduper_completes_before_triage);
    assert!(source.only_novel_hashes_triaged);
    assert!(source.node_address_reread_per_novel_hash);
    assert!(source.triage_blocks_in_order);
    assert!(source.triage_cancellation_aware);
    assert!(source.triage_cancellation_branch_returns_before_put_fanout);
    assert!(source.clamp_requires_novel_and_over300);
    assert_eq!(source.clamp_interval_seconds, 60);
    assert_eq!(
        source.duration_conversion,
        "time.Duration(effective_signed_Go_int)*time.Second_with_int64_nanosecond_wrap"
    );
    assert_eq!(source.go_int_bits, 64);
    assert_eq!(
        source
            .interval_cases
            .iter()
            .map(|case| (
                case.name.as_str(),
                case.raw_interval,
                case.novel_count,
                case.effective_interval,
                case.duration_ns,
            ))
            .collect::<Vec<_>>(),
        vec![
            ("negative_novel_unclamped", -7, 1, -7, -7_000_000_000),
            ("boundary_300_novel_unclamped", 300, 1, 300, 300_000_000_000),
            ("over_300_novel_clamped", 301, 1, 60, 60_000_000_000),
            (
                "over_300_zero_novel_unclamped",
                301,
                0,
                301,
                301_000_000_000
            ),
            (
                "max_int_novel_clamped_before_convert",
                i64::MAX,
                1,
                60,
                60_000_000_000
            ),
            (
                "max_int_zero_novel_wraps_duration",
                i64::MAX,
                0,
                i64::MAX,
                -1_000_000_000
            ),
            (
                "min_int_novel_unclamped_wraps_duration",
                i64::MIN,
                1,
                i64::MIN,
                0
            ),
            (
                "min_int_zero_novel_wraps_duration",
                i64::MIN,
                0,
                i64::MIN,
                0
            ),
        ]
    );
    for case in &source.interval_cases {
        assert_eq!(
            effective_interval_duration_ns(case.raw_interval, case.novel_count),
            case.duration_ns
        );
    }
    assert!(source.put_uses_advertised_id_and_current_addr);
    assert_eq!(
        source.put_option_order,
        [
            "NodeResponded",
            "NodeBep51Support(true)",
            "NodeSampleInfoHashesRes"
        ]
    );
    assert_eq!(source.put_discovered_count, "len(discoveredHashes)");
    assert_eq!(source.put_total_count, "res.Num");
    assert_eq!(
        source.put_deadline_expression,
        "time.Now().Add(time.Duration(interval)*time.Second)"
    );
    assert!(source.put_occurs_after_all_triage);
    assert!(source.put_precedes_fanout_launch);
    assert!(source.fanout_uses_response_order);
    assert!(source.fanout_reads_captured_response_in_goroutine);
    assert!(!source.fanout_deep_copies_response_nodes);
    assert!(source.fanout_detached);
    assert!(!source.fanout_joined);
    assert_eq!(source.fanout_whole_list_timeout_ms, 1_000);
    assert!(source.fanout_cancellation_aware);
    assert_eq!(
        (source.production_capacity, source.production_concurrency),
        (100, 100)
    );
    assert_eq!(source.default_scaling_factor, 10);
    assert!(source.consumer_dequeues_before_semaphore);
    assert!(source.acquire_cancellation_drops_dequeued_item);
    assert_eq!(
        source.maximum_retained_work,
        "capacity_plus_concurrency_plus_one_acquire_waiter"
    );
    assert!(source.consumer_callbacks_detached);
    assert!(!source.consumer_callbacks_joined);
    assert!(!source.closed_input_checks_open_boolean);
    assert_eq!(
        source.closed_input_outcome,
        "repeated_zero_value_callbacks_eventually_panic_on_nil_Node_accessor"
    );
    assert_eq!(
        (
            source.production_triage_capacity,
            source.production_triage_max_batch_size,
            source.production_triage_interval_ms,
            source.production_triage_output_capacity,
        ),
        (100, 1_000, 20_000, 1)
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
    assert!(!source.start_joins_worker_callbacks_or_fanout);
    assert!(source.fanout_can_outlive_worker_permit);
    let prerequisite = BTreeMap::from_iter(
        PREREQUISITE_FIXTURES
            .iter()
            .map(|(path, _, digest)| ((*path).to_owned(), (*digest).to_owned())),
    );
    assert_eq!(source.prerequisite_fixture_sha256, prerequisite);
    for (_, bytes, digest) in PREREQUISITE_FIXTURES {
        assert_eq!(sha256(bytes), digest);
    }
    assert_eq!(
        source.evidence_commit,
        BTreeMap::from([
            (
                "ignore_hashes_oracle".to_owned(),
                "684aedf68d9c07b96a362c470ec3619c0290b4f5".to_owned()
            ),
            (
                "ktable_temporal_oracle".to_owned(),
                "1df4d7a09f74e13e75ea2e1ab1dcfc67a130ed9d".to_owned()
            ),
            (
                "peer_sample_client_oracle".to_owned(),
                "1f00b40705ba527721208023ddec64220fb40729".to_owned()
            ),
            (
                "rust_info_hash_deduper".to_owned(),
                "accec9e0c0f89a3e5b64e8a60bb3f29393c13b52".to_owned()
            ),
            (
                "sample_infohashes_producer_oracle".to_owned(),
                "602dce3287795bbe2eee89bbcc1e0ebc6f9c7701".to_owned()
            ),
            (
                "shared_sample_input_seam".to_owned(),
                "e0fdd622f5869d092ff4322433d72bd17f783d11".to_owned()
            ),
            (
                "typed_info_hash_triage_route".to_owned(),
                "b98da5ae34524f4b45c1bd0eee2e0d41dbd3128e".to_owned()
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
        source.nonclaims,
        [
            "exact_wall_clock_NodeResponded_or_next_sample_timestamp",
            "ready_select_tie_winner",
            "goroutine_callback_or_fanout_scheduling_order",
            "semaphore_or_mutex_fairness",
            "closed_buffered_input_runtime_execution",
            "callback_or_fanout_join_guarantee",
            "one_second_timeout_elapsed_in_runtime_rows",
            "exact_BoomFilters_random_decrement_offsets_or_retention",
            "exact_set_or_false_positive_false_negative_semantics",
            "production_batch_flush_timing_or_output_batch_boundaries",
            "infohash_triage_database_blocking_or_downstream_route_behavior",
            "discovered_node_deduplication_filtering_or_downstream_routing",
            "KTable_map_iteration_order_or_eviction_behavior",
            "opaque_NodeOption_function_identity_or_internal_field_layout",
            "live_DNS_UDP_or_DHT_network_behavior",
            "response_ID_as_advertised_node_identity",
            "Rust_implementation_public_API_or_overlapping_task_lifecycle",
            "Rust_signed_overflow_parity_for_interval_or_deadline_arithmetic",
        ]
    );
    assert_eq!(
        source.evidence,
        "runtime rows execute the actual worker with controlled interfaces; source-only facts bind full normalized AST and exact file hashes"
    );
}

fn assert_empty_expected(expected: &Expected, source_row: bool) {
    assert_eq!(
        (
            expected.node_calls.id,
            expected.node_calls.addr,
            expected.node_calls.time,
            expected.node_calls.dropped,
            expected.node_calls.sample_infohashes_candidate,
        ),
        (0, 0, 0, 0, 0)
    );
    assert!(expected.client_calls.is_empty());
    assert!(!expected.same_context);
    assert!(expected.source_derived_deduper_call_order.is_empty());
    assert!(expected.deduper_post_membership.is_empty());
    assert_eq!(expected.triage_in_calls, 0);
    assert!(expected.triage_deliveries.is_empty());
    assert!(expected.commands.is_empty());
    assert_eq!(expected.discovery_in_calls, 0);
    assert!(expected.discoveries.is_empty());
    assert!(expected.events.is_empty());
    assert!(expected.run_returned);
    assert!(!expected.context_cancelled);
    assert!(!expected.callback_completion_observed);
    assert!(!expected.fanout_completion_observed);
    assert_eq!(expected.source.is_some(), source_row);
}

fn assert_node(
    value: &Node,
    token: &str,
    id: &str,
    addr: &str,
    addr_returns: Option<&[&str]>,
    final_candidate: bool,
) {
    assert_eq!(value.token, token);
    assert_eq!(value.id, id);
    assert_eq!(value.addr, addr);
    assert_eq!(
        value
            .addr_returns
            .as_ref()
            .map(|values| values.iter().map(String::as_str).collect::<Vec<_>>()),
        addr_returns.map(<[_]>::to_vec)
    );
    assert!(value.initial_candidate);
    assert_eq!(value.final_candidate, final_candidate);
}

fn assert_zero_downstream(expected: &Expected) {
    assert!(expected.source_derived_deduper_call_order.is_empty());
    assert!(expected.deduper_post_membership.is_empty());
    assert_eq!(expected.triage_in_calls, 0);
    assert!(expected.triage_deliveries.is_empty());
    assert_eq!(expected.discovery_in_calls, 0);
    assert!(expected.discoveries.is_empty());
}

fn assert_candidate_row(fixture: &Fixture) {
    let input = &fixture.input;
    assert_eq!(
        input.kind,
        "actual_buffered_lane_callback_time_interface_node_candidate_mutation"
    );
    assert_eq!((input.lane_capacity, input.lane_concurrency), (0, 1));
    let node = input.node.as_ref().expect("candidate row node");
    assert_node(
        node,
        "mutated_target",
        "000000000000000000000000000000000000002a",
        "198.51.100.42:6942",
        None,
        false,
    );
    assert!(node.initial_candidate);
    assert!(!node.final_candidate);
    assert!(input.response.is_none());
    assert!(input.sought_target.is_none());
    assert!(input.preloaded_hashes.is_none());
    assert!(input.oracle_rng_seed.is_none());
    assert!(input.hash_indexes.is_none());
    assert_eq!(input.mutate_candidate_after_take, Some(true));
    assert_eq!(
        (
            input.triage_capacity,
            input.cancel_at_triage_in_call,
            input.discovery_capacity,
            input.cancel_at_discovery_in_call,
        ),
        (0, 0, 0, 0)
    );
    let expected = &fixture.expected;
    assert_eq!(
        (
            expected.node_calls.id,
            expected.node_calls.addr,
            expected.node_calls.time,
            expected.node_calls.dropped,
            expected.node_calls.sample_infohashes_candidate,
        ),
        (0, 0, 0, 0, 1)
    );
    assert!(expected.client_calls.is_empty());
    assert!(!expected.same_context);
    assert_zero_downstream(expected);
    assert!(expected.commands.is_empty());
    assert_eq!(
        expected.events,
        [
            "node_candidate_enter:permit_blocker",
            "target_dequeued_before_permit",
            "node_candidate_mutated:mutated_target:false",
            "node_candidate_return:permit_blocker:false",
            "node_candidate_enter:mutated_target",
            "node_candidate_return:mutated_target:false",
        ]
    );
    assert!(expected.run_returned);
    assert!(expected.context_cancelled);
    assert!(!expected.callback_completion_observed);
    assert!(!expected.fanout_completion_observed);
    assert!(expected.source.is_none());
}

fn assert_response(
    value: &Response,
    kind: &str,
    response_id: &str,
    samples: &[&str],
    num: i64,
    interval: i64,
) {
    assert_eq!(value.kind, kind);
    assert_eq!(value.response_id, response_id);
    assert_eq!(
        value.samples.iter().map(String::as_str).collect::<Vec<_>>(),
        samples
    );
    assert_eq!((value.num, value.interval), (num, interval));
}

fn assert_error_row(fixture: &Fixture) {
    let input = &fixture.input;
    assert_eq!(input.kind, "eligible_client_error");
    assert_eq!((input.lane_capacity, input.lane_concurrency), (0, 0));
    assert_node(
        input.node.as_ref().expect("error row node"),
        "advertised_error_node",
        "0000000000000000000000000000000000000033",
        "198.51.100.51:6951",
        Some(&["198.51.100.51:6951"]),
        true,
    );
    let response = input.response.as_ref().expect("error row response");
    assert_response(
        response,
        "error",
        "00000000000000000000000000000000000000fb",
        &["0000000000000000000000000000000000000097"],
        700,
        301,
    );
    assert_eq!(response.nodes.len(), 1);
    assert_node(
        &response.nodes[0],
        "response_98",
        "0000000000000000000000000000000000000098",
        "203.0.113.152:7152",
        None,
        true,
    );
    assert_eq!(
        input.sought_target.as_deref(),
        Some("00000000000000000000000000000000000000d3")
    );
    assert!(input.preloaded_hashes.is_none());
    assert!(input.oracle_rng_seed.is_none());
    assert!(input.hash_indexes.is_none());
    assert!(input.mutate_candidate_after_take.is_none());
    assert_eq!(
        (
            input.triage_capacity,
            input.cancel_at_triage_in_call,
            input.discovery_capacity,
            input.cancel_at_discovery_in_call,
        ),
        (0, 0, 0, 0)
    );
    let expected = &fixture.expected;
    assert_eq!(
        (
            expected.node_calls.id,
            expected.node_calls.addr,
            expected.node_calls.time,
            expected.node_calls.dropped,
            expected.node_calls.sample_infohashes_candidate,
        ),
        (1, 1, 0, 0, 1)
    );
    assert_eq!(expected.client_calls.len(), 1);
    assert_eq!(expected.client_calls[0].addr, "198.51.100.51:6951");
    assert_eq!(
        expected.client_calls[0].target,
        "00000000000000000000000000000000000000d3"
    );
    assert!(expected.same_context);
    assert_zero_downstream(expected);
    assert_eq!(expected.commands.len(), 1);
    let command = &expected.commands[0];
    assert_eq!(command.kind, "drop_node");
    assert_eq!(command.id, "0000000000000000000000000000000000000033");
    assert!(command.addr.is_none());
    assert_eq!(command.option_count, 0);
    assert_eq!(
        command.reason.as_deref(),
        Some("sample_infohashes failed: oracle sample_infohashes failure")
    );
    assert!(command.error_identity_preserved);
    assert!(!command.stored_responded);
    assert!(!command.stored_candidate);
    assert_eq!(
        expected.events,
        [
            "callback_begin:0",
            "node_candidate_enter:advertised_error_node",
            "node_candidate_return:advertised_error_node:true",
            "node_addr:advertised_error_node",
            "client_sample_infohashes",
            "node_id:advertised_error_node",
            "table_drop_begin",
            "table_drop_complete",
            "callback_complete:0",
        ]
    );
    assert!(expected.run_returned);
    assert!(!expected.context_cancelled);
    assert!(expected.callback_completion_observed);
    assert!(!expected.fanout_completion_observed);
    assert!(expected.source.is_none());
}

fn assert_dedupe_row(fixture: &Fixture) {
    let input = &fixture.input;
    assert_eq!(
        input.kind,
        "ordered_samples_cancel_blocked_third_novel_triage"
    );
    assert_eq!((input.lane_capacity, input.lane_concurrency), (0, 0));
    assert_node(
        input.node.as_ref().expect("dedupe row node"),
        "ordered_samples_node",
        "000000000000000000000000000000000000003d",
        "198.51.100.61:6961",
        Some(&[
            "198.51.100.61:6961",
            "198.51.100.62:6962",
            "198.51.100.63:6963",
            "198.51.100.64:6964",
        ]),
        true,
    );
    let response = input.response.as_ref().expect("dedupe row response");
    assert_response(
        response,
        "success",
        "00000000000000000000000000000000000000fc",
        &[
            "00000000000000000000000000000000000000a1",
            "00000000000000000000000000000000000000b2",
            "00000000000000000000000000000000000000c3",
            "00000000000000000000000000000000000000d4",
        ],
        901,
        301,
    );
    assert_eq!(response.nodes.len(), 1);
    assert_node(
        &response.nodes[0],
        "response_a2",
        "00000000000000000000000000000000000000a2",
        "203.0.113.162:7162",
        None,
        true,
    );
    assert_eq!(
        input.sought_target.as_deref(),
        Some("00000000000000000000000000000000000000d4")
    );
    assert_eq!(
        input.preloaded_hashes.as_deref(),
        Some(&["00000000000000000000000000000000000000b2".to_owned()][..])
    );
    assert_eq!(input.oracle_rng_seed, Some(1));
    assert_eq!(
        input.hash_indexes,
        Some(BTreeMap::from([
            (
                "00000000000000000000000000000000000000a1".to_owned(),
                vec![4_110_100, 5_868_049, 7_625_998, 9_383_947, 1_141_896]
            ),
            (
                "00000000000000000000000000000000000000b2".to_owned(),
                vec![4_110_087, 5_868_036, 7_625_985, 9_383_934, 1_141_883]
            ),
            (
                "00000000000000000000000000000000000000c3".to_owned(),
                vec![4_110_198, 5_868_147, 7_626_096, 9_384_045, 1_141_994]
            ),
            (
                "00000000000000000000000000000000000000d4".to_owned(),
                vec![4_110_177, 5_868_126, 7_626_075, 9_384_024, 1_141_973]
            ),
        ]))
    );
    assert!(input.mutate_candidate_after_take.is_none());
    assert_eq!(
        (
            input.triage_capacity,
            input.cancel_at_triage_in_call,
            input.discovery_capacity,
            input.cancel_at_discovery_in_call,
        ),
        (2, 3, 0, 0)
    );
    let expected = &fixture.expected;
    assert_eq!(
        (
            expected.node_calls.id,
            expected.node_calls.addr,
            expected.node_calls.time,
            expected.node_calls.dropped,
            expected.node_calls.sample_infohashes_candidate,
        ),
        (0, 4, 0, 0, 1)
    );
    assert_eq!(expected.client_calls.len(), 1);
    assert_eq!(expected.client_calls[0].addr, "198.51.100.61:6961");
    assert_eq!(
        expected.client_calls[0].target,
        "00000000000000000000000000000000000000d4"
    );
    assert!(expected.same_context);
    assert_eq!(
        expected.source_derived_deduper_call_order,
        [
            "00000000000000000000000000000000000000a1",
            "00000000000000000000000000000000000000b2",
            "00000000000000000000000000000000000000c3",
            "00000000000000000000000000000000000000d4",
        ]
    );
    assert_eq!(
        expected.deduper_post_membership,
        BTreeMap::from([
            ("00000000000000000000000000000000000000a1".to_owned(), true),
            ("00000000000000000000000000000000000000b2".to_owned(), true),
            ("00000000000000000000000000000000000000c3".to_owned(), true),
            ("00000000000000000000000000000000000000d4".to_owned(), true),
        ])
    );
    assert_eq!(expected.triage_in_calls, 3);
    assert_eq!(expected.triage_deliveries.len(), 2);
    assert_eq!(
        expected.triage_deliveries[0].info_hash,
        "00000000000000000000000000000000000000a1"
    );
    assert_eq!(expected.triage_deliveries[0].node, "198.51.100.62:6962");
    assert_eq!(
        expected.triage_deliveries[1].info_hash,
        "00000000000000000000000000000000000000c3"
    );
    assert_eq!(expected.triage_deliveries[1].node, "198.51.100.63:6963");
    assert!(expected.commands.is_empty());
    assert_eq!(expected.discovery_in_calls, 0);
    assert!(expected.discoveries.is_empty());
    assert_eq!(
        expected.events,
        [
            "callback_begin:0",
            "node_candidate_enter:ordered_samples_node",
            "node_candidate_return:ordered_samples_node:true",
            "node_addr:ordered_samples_node",
            "client_sample_infohashes",
            "node_addr:ordered_samples_node",
            "node_addr:ordered_samples_node",
            "node_addr:ordered_samples_node",
            "triage_in:1",
            "triage_in:2",
            "triage_in:3",
            "all_samples_deduped_before_cancel",
            "context_cancelled",
            "callback_complete:0",
        ]
    );
    assert!(expected.run_returned);
    assert!(expected.context_cancelled);
    assert!(expected.callback_completion_observed);
    assert!(!expected.fanout_completion_observed);
    assert!(expected.source.is_none());
}

fn assert_fanout_row(fixture: &Fixture) {
    let input = &fixture.input;
    assert_eq!(
        input.kind,
        "clamp_put_then_detached_recursive_prefix_cancel"
    );
    assert_eq!((input.lane_capacity, input.lane_concurrency), (0, 0));
    assert_node(
        input.node.as_ref().expect("fanout row node"),
        "clamped_success_node",
        "0000000000000000000000000000000000000047",
        "198.51.100.71:6971",
        Some(&[
            "198.51.100.71:6971",
            "198.51.100.72:6972",
            "198.51.100.73:6973",
        ]),
        true,
    );
    let response = input.response.as_ref().expect("fanout row response");
    assert_response(
        response,
        "success",
        "00000000000000000000000000000000000000fd",
        &["00000000000000000000000000000000000000e5"],
        -17,
        301,
    );
    assert_eq!(response.nodes.len(), 4);
    for (actual, (token, id, addr)) in response.nodes.iter().zip([
        (
            "response_ab",
            "00000000000000000000000000000000000000ab",
            "203.0.113.171:7171",
        ),
        (
            "response_ac",
            "00000000000000000000000000000000000000ac",
            "203.0.113.172:7172",
        ),
        (
            "response_ad",
            "00000000000000000000000000000000000000ad",
            "203.0.113.173:7173",
        ),
        (
            "response_ae",
            "00000000000000000000000000000000000000ae",
            "203.0.113.174:7174",
        ),
    ]) {
        assert_node(actual, token, id, addr, None, true);
    }
    assert_eq!(
        input.sought_target.as_deref(),
        Some("00000000000000000000000000000000000000d5")
    );
    assert!(input.preloaded_hashes.is_none());
    assert!(input.oracle_rng_seed.is_none());
    assert!(input.hash_indexes.is_none());
    assert!(input.mutate_candidate_after_take.is_none());
    assert_eq!(
        (
            input.triage_capacity,
            input.cancel_at_triage_in_call,
            input.discovery_capacity,
            input.cancel_at_discovery_in_call,
        ),
        (1, 0, 2, 3)
    );
    let expected = &fixture.expected;
    assert_eq!(
        (
            expected.node_calls.id,
            expected.node_calls.addr,
            expected.node_calls.time,
            expected.node_calls.dropped,
            expected.node_calls.sample_infohashes_candidate,
        ),
        (1, 3, 0, 0, 1)
    );
    assert_eq!(expected.client_calls.len(), 1);
    assert_eq!(expected.client_calls[0].addr, "198.51.100.71:6971");
    assert_eq!(
        expected.client_calls[0].target,
        "00000000000000000000000000000000000000d5"
    );
    assert!(expected.same_context);
    assert_eq!(
        expected.source_derived_deduper_call_order,
        ["00000000000000000000000000000000000000e5"]
    );
    assert_eq!(
        expected.deduper_post_membership,
        BTreeMap::from([("00000000000000000000000000000000000000e5".to_owned(), true)])
    );
    assert_eq!(expected.triage_in_calls, 1);
    assert_eq!(expected.triage_deliveries.len(), 1);
    assert_eq!(
        expected.triage_deliveries[0].info_hash,
        "00000000000000000000000000000000000000e5"
    );
    assert_eq!(expected.triage_deliveries[0].node, "198.51.100.72:6972");
    assert_eq!(expected.commands.len(), 1);
    let command = &expected.commands[0];
    assert_eq!(command.kind, "put_node");
    assert_eq!(command.id, "0000000000000000000000000000000000000047");
    assert_eq!(command.addr.as_deref(), Some("198.51.100.73:6973"));
    assert_eq!(command.option_count, 3);
    assert!(command.reason.is_none());
    assert!(!command.error_identity_preserved);
    assert!(command.stored_responded);
    assert!(!command.stored_candidate);
    assert_eq!(expected.discovery_in_calls, 3);
    assert_eq!(expected.discoveries.len(), 2);
    assert_node(
        &expected.discoveries[0],
        "response_ab",
        "00000000000000000000000000000000000000ab",
        "203.0.113.171:7171",
        None,
        true,
    );
    assert_node(
        &expected.discoveries[1],
        "response_ac",
        "00000000000000000000000000000000000000ac",
        "203.0.113.172:7172",
        None,
        true,
    );
    assert_eq!(
        expected.events,
        [
            "callback_begin:0",
            "node_candidate_enter:clamped_success_node",
            "node_candidate_return:clamped_success_node:true",
            "node_addr:clamped_success_node",
            "client_sample_infohashes",
            "node_addr:clamped_success_node",
            "triage_in:1",
            "node_id:clamped_success_node",
            "node_addr:clamped_success_node",
            "table_put_begin",
            "table_put_complete",
            "callback_complete:0",
            "run_returned_before_fanout_send",
            "discovery_in:1",
            "discovery_in:2",
            "discovery_in:3",
            "context_cancelled",
            "fanout_observed_exited",
        ]
    );
    assert!(expected.run_returned);
    assert!(expected.context_cancelled);
    assert!(expected.callback_completion_observed);
    assert!(expected.fanout_completion_observed);
    assert!(expected.source.is_none());
}

#[tokio::test]
async fn mutated_interface_row_replays_retained_candidate_recheck_without_query() {
    let fixtures = fixtures();
    let row = &fixtures[1];
    let fixture_node = row.input.node.as_ref().expect("candidate row node");
    let table = KTable::new(Id20::ZERO);
    let handle = retained(&table, fixture_node);
    let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
    input.send(handle).await.unwrap();
    drop(input);
    table.put_node_with_options(
        routing_node(fixture_node),
        &[KTableNodeOption::Bep51Support(false)],
    );
    let (triage, mut triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
    let (discovery, mut discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
    let (mut core, stats) = core(receiver, table, triage, discovery);
    let query_calls = Arc::new(AtomicUsize::new(0));
    let query_calls_clone = Arc::clone(&query_calls);

    let exit = core
        .run_with(
            pending(),
            || id("00000000000000000000000000000000000000d1"),
            move |_, _| {
                query_calls_clone.fetch_add(1, Ordering::SeqCst);
                ready(Err::<SampleInfoHashesResult, ()>(()))
            },
            |_| panic!("candidate skip reached deduper"),
            Instant::now,
            || tokio::time::Instant::now() + Duration::from_secs(60),
            |_, _| {},
            |_, _| {},
        )
        .await;

    assert_eq!(exit, DhtSampleInfoHashesWorkerExit::InputClosed);
    assert_eq!(query_calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        triage_receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        discovery_receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(
        stats.snapshot(),
        DhtSampleInfoHashesWorkerStats {
            dequeued: 1,
            candidate_skipped: 1,
            tasks_completed: 1,
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn error_row_replays_typed_drop_without_downstream_work() {
    let fixtures = fixtures();
    let row = &fixtures[2];
    let fixture_node = row.input.node.as_ref().expect("error row node");
    let table = KTable::new(Id20::ZERO);
    let handle = retained(&table, fixture_node);
    let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
    input.send(handle.clone()).await.unwrap();
    drop(input);
    let (triage, mut triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
    let (discovery, mut discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
    let (mut core, stats) = core(receiver, table, triage, discovery);
    let query_calls = Arc::new(Mutex::new(Vec::new()));
    let query_calls_clone = Arc::clone(&query_calls);
    let target = id(row.input.sought_target.as_deref().expect("error target"));

    let exit = core
        .run_with(
            pending(),
            move || target,
            move |remote, actual_target| {
                query_calls_clone
                    .lock()
                    .unwrap()
                    .push((remote, actual_target));
                ready(Err::<SampleInfoHashesResult, &'static str>(
                    "oracle sample_infohashes failure",
                ))
            },
            |_| panic!("query error reached deduper"),
            Instant::now,
            || tokio::time::Instant::now() + Duration::from_secs(60),
            |_, _| {},
            |_, _| {},
        )
        .await;

    assert_eq!(exit, DhtSampleInfoHashesWorkerExit::InputClosed);
    assert_eq!(
        *query_calls.lock().unwrap(),
        vec![(
            addr("198.51.100.51:6951"),
            id("00000000000000000000000000000000000000d3")
        )]
    );
    assert!(handle.dropped());
    assert_eq!(handle.id(), id("0000000000000000000000000000000000000033"));
    assert!(matches!(
        triage_receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        discovery_receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(
        stats.snapshot(),
        DhtSampleInfoHashesWorkerStats {
            dequeued: 1,
            queries_started: 1,
            tasks_completed: 1,
            queries_failed: 1,
            drop_commands: 1,
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn ordered_dedupe_row_replays_triage_prefix_and_shutdown_suffix() {
    let fixtures = fixtures();
    let row = &fixtures[3];
    let fixture_node = row.input.node.as_ref().expect("dedupe row node");
    let fixture_response = row.input.response.as_ref().expect("dedupe response");
    let response = response(fixture_response);
    let dynamic_addrs = fixture_node
        .addr_returns
        .as_ref()
        .expect("dynamic addresses")
        .iter()
        .map(|value| addr(value))
        .collect::<Vec<_>>();
    let advertised_id = id("000000000000000000000000000000000000003d");
    let target = id("00000000000000000000000000000000000000d4");
    let table = KTable::new(Id20::ZERO);
    let handle = retained(&table, fixture_node);
    let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
    input.send(handle).await.unwrap();
    drop(input);
    let (triage, mut triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(2).unwrap());
    let (discovery, mut discovery_receiver) = dht_discovery_channel(NonZeroUsize::MIN);
    let (mut core, stats) = core(receiver, table.clone(), triage, discovery);
    let query_calls = Arc::new(Mutex::new(Vec::new()));
    let query_calls_clone = Arc::clone(&query_calls);
    let dedup_order = Arc::new(Mutex::new(Vec::new()));
    let dedup_order_clone = Arc::clone(&dedup_order);
    let dedup_results = Arc::new(Mutex::new(VecDeque::from([false, true, false, false])));
    let dedup_results_clone = Arc::clone(&dedup_results);
    let table_for_dedup = table.clone();
    let addrs_for_dedup = dynamic_addrs.clone();
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let attempts_clone = Arc::clone(&attempts);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));

    let run = tokio::spawn(async move {
        core.run_with(
            async {
                let _ = shutdown_rx.await;
            },
            move || target,
            move |remote, actual_target| {
                query_calls_clone
                    .lock()
                    .unwrap()
                    .push((remote, actual_target));
                ready(Ok::<_, ()>(response.clone()))
            },
            move |info_hash| {
                let mut order = dedup_order_clone.lock().unwrap();
                order.push(info_hash);
                let index = order.len() - 1;
                drop(order);
                match index {
                    0 => {
                        table_for_dedup.put_node(RoutingNode {
                            id: advertised_id,
                            addr: addrs_for_dedup[1],
                        });
                    }
                    2 => {
                        table_for_dedup.put_node(RoutingNode {
                            id: advertised_id,
                            addr: addrs_for_dedup[2],
                        });
                    }
                    3 => {
                        table_for_dedup.put_node(RoutingNode {
                            id: advertised_id,
                            addr: addrs_for_dedup[3],
                        });
                    }
                    _ => {}
                }
                dedup_results_clone.lock().unwrap().pop_front().unwrap()
            },
            Instant::now,
            || tokio::time::Instant::now() + Duration::from_secs(60),
            move |index, _| {
                attempts_clone.lock().unwrap().push(index);
                if index == 2 {
                    if let Some(sender) = shutdown_tx.lock().unwrap().take() {
                        let _ = sender.send(());
                    }
                }
            },
            |_, _| {},
        )
        .await
    });

    let exit = run.await.unwrap();
    assert_eq!(
        exit,
        DhtSampleInfoHashesWorkerExit::Shutdown {
            queued_dropped: 0,
            tasks_cancelled: 1,
            triage_hashes_dropped: 1,
            recursive_nodes_dropped: 0,
        }
    );
    assert_eq!(
        *query_calls.lock().unwrap(),
        vec![(addr("198.51.100.61:6961"), target)]
    );
    assert_eq!(
        *dedup_order.lock().unwrap(),
        vec![
            id("00000000000000000000000000000000000000a1"),
            id("00000000000000000000000000000000000000b2"),
            id("00000000000000000000000000000000000000c3"),
            id("00000000000000000000000000000000000000d4"),
        ]
    );
    assert_eq!(*attempts.lock().unwrap(), vec![0, 1, 2]);
    assert_eq!(
        triage_receiver.recv().await,
        Some(DhtInfoHashTriageRequest {
            info_hash: id("00000000000000000000000000000000000000a1"),
            source_node_addr: addr("198.51.100.62:6962"),
        })
    );
    assert_eq!(
        triage_receiver.recv().await,
        Some(DhtInfoHashTriageRequest {
            info_hash: id("00000000000000000000000000000000000000c3"),
            source_node_addr: addr("198.51.100.63:6963"),
        })
    );
    assert!(matches!(
        triage_receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
    ));
    assert!(matches!(
        discovery_receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
    ));
    let snapshot = stats.snapshot();
    assert_eq!(
        snapshot,
        DhtSampleInfoHashesWorkerStats {
            dequeued: 1,
            queries_started: 1,
            queries_succeeded: 1,
            sample_hashes_returned: 4,
            sample_hashes_suppressed: 1,
            sample_hashes_novel: 3,
            triage_queued: 2,
            shutdown_tasks_cancelled: 1,
            shutdown_triage_hashes_dropped: 1,
            ..Default::default()
        }
    );
    assert_shutdown_conservation(snapshot);
}

#[tokio::test]
async fn clamp_put_and_recursive_row_replays_owned_prefix_and_shutdown_suffix() {
    let fixtures = fixtures();
    let row = &fixtures[4];
    let fixture_node = row.input.node.as_ref().expect("fanout row node");
    let fixture_response = row.input.response.as_ref().expect("fanout response");
    let response = response(fixture_response);
    let dynamic_addrs = fixture_node
        .addr_returns
        .as_ref()
        .expect("dynamic addresses")
        .iter()
        .map(|value| addr(value))
        .collect::<Vec<_>>();
    let advertised_id = id("0000000000000000000000000000000000000047");
    let target = id("00000000000000000000000000000000000000d5");
    let table = KTable::new(Id20::ZERO);
    let handle = retained(&table, fixture_node);
    let (input, receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
    input.send(handle.clone()).await.unwrap();
    drop(input);
    let (triage, mut triage_receiver) = dht_info_hash_triage_channel(NonZeroUsize::MIN);
    let (discovery, mut discovery_receiver) = dht_discovery_channel(NonZeroUsize::new(2).unwrap());
    let (mut core, stats) = core(receiver, table.clone(), triage, discovery);
    let query_calls = Arc::new(Mutex::new(Vec::new()));
    let query_calls_clone = Arc::clone(&query_calls);
    let dedup_order = Arc::new(Mutex::new(Vec::new()));
    let dedup_order_clone = Arc::clone(&dedup_order);
    let table_for_dedup = table.clone();
    let table_before_triage = table.clone();
    let addrs_for_dedup = dynamic_addrs.clone();
    let addrs_before_triage = dynamic_addrs.clone();
    let recursive_attempts = Arc::new(Mutex::new(Vec::new()));
    let recursive_attempts_clone = Arc::clone(&recursive_attempts);
    let deadline_calls = Arc::new(AtomicUsize::new(0));
    let deadline_calls_clone = Arc::clone(&deadline_calls);
    let base = Instant::now();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));

    let run = tokio::spawn(async move {
        core.run_with(
            async {
                let _ = shutdown_rx.await;
            },
            move || target,
            move |remote, actual_target| {
                query_calls_clone
                    .lock()
                    .unwrap()
                    .push((remote, actual_target));
                ready(Ok::<_, ()>(response.clone()))
            },
            move |info_hash| {
                dedup_order_clone.lock().unwrap().push(info_hash);
                table_for_dedup.put_node(RoutingNode {
                    id: advertised_id,
                    addr: addrs_for_dedup[1],
                });
                false
            },
            move || base,
            move || {
                deadline_calls_clone.fetch_add(1, Ordering::SeqCst);
                tokio::time::Instant::now() + Duration::from_secs(60)
            },
            move |index, _| {
                assert_eq!(index, 0);
                table_before_triage.put_node(RoutingNode {
                    id: advertised_id,
                    addr: addrs_before_triage[2],
                });
            },
            move |index, _| {
                recursive_attempts_clone.lock().unwrap().push(index);
                if index == 2 {
                    if let Some(sender) = shutdown_tx.lock().unwrap().take() {
                        let _ = sender.send(());
                    }
                }
            },
        )
        .await
    });

    let exit = run.await.unwrap();
    assert_eq!(
        exit,
        DhtSampleInfoHashesWorkerExit::Shutdown {
            queued_dropped: 0,
            tasks_cancelled: 1,
            triage_hashes_dropped: 0,
            recursive_nodes_dropped: 2,
        }
    );
    assert_eq!(
        *query_calls.lock().unwrap(),
        vec![(addr("198.51.100.71:6971"), target)]
    );
    assert_eq!(
        *dedup_order.lock().unwrap(),
        vec![id("00000000000000000000000000000000000000e5")]
    );
    assert_eq!(*recursive_attempts.lock().unwrap(), vec![0, 1, 2]);
    assert_eq!(deadline_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        triage_receiver.recv().await,
        Some(DhtInfoHashTriageRequest {
            info_hash: id("00000000000000000000000000000000000000e5"),
            source_node_addr: addr("198.51.100.72:6972"),
        })
    );
    assert!(matches!(
        triage_receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
    ));
    assert_eq!(
        discovery_receiver.try_recv(),
        Ok(RoutingNode {
            id: id("00000000000000000000000000000000000000ab"),
            addr: addr("203.0.113.171:7171"),
        })
    );
    assert_eq!(
        discovery_receiver.try_recv(),
        Ok(RoutingNode {
            id: id("00000000000000000000000000000000000000ac"),
            addr: addr("203.0.113.172:7172"),
        })
    );
    assert!(matches!(
        discovery_receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
    ));
    assert_eq!(handle.id(), advertised_id);
    assert_eq!(handle.addr(), addr("198.51.100.73:6973"));
    assert!(handle.last_responded_at().is_some());
    assert_eq!(handle.bep51_support(), KTableBep51Support::Yes);
    assert_eq!(handle.sampled_num(), 1);
    assert_eq!(handle.last_discovered_num(), 1);
    assert_eq!(handle.total_num(), -17);
    assert_eq!(
        handle.next_sample_infohashes_at(),
        Some(base + Duration::from_secs(60))
    );
    assert!(!handle.is_sample_infohashes_candidate());
    let snapshot = stats.snapshot();
    assert_eq!(
        snapshot,
        DhtSampleInfoHashesWorkerStats {
            dequeued: 1,
            queries_started: 1,
            queries_succeeded: 1,
            sample_hashes_returned: 1,
            sample_hashes_novel: 1,
            triage_queued: 1,
            put_commands: 1,
            recursive_nodes: 4,
            recursive_nodes_queued: 2,
            shutdown_tasks_cancelled: 1,
            shutdown_recursive_nodes_dropped: 2,
            ..Default::default()
        }
    );
    assert_shutdown_conservation(snapshot);
}
