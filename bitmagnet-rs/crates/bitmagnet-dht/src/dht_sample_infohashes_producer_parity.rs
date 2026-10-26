use std::collections::BTreeMap;
use std::future::{poll_fn, ready, Future};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use super::*;
use crate::dht_discovered_node_scheduler::DhtDiscoveredNodeSampleInfoHashesWork;
use crate::{
    DhtDiscoveredNodeSchedulerConfig, Id20, KTableNodeHandle, RoutingNode, RoutingPutResult,
};

const FIXTURE_TEXT: &str =
    include_str!("../../../../testdata/parity/dht/dht_crawler_sample_infohashes_producer.jsonl");
const FIXTURE_BYTES: &[u8] =
    include_bytes!("../../../../testdata/parity/dht/dht_crawler_sample_infohashes_producer.jsonl");
const FIXTURE_SHA256: &str = "b0069a060b32edc4e1c6f5b2008f6b50f796eea6d162b4df3a148cad29745c1e";
const FIXTURE_IDS: [&str; 3] = [
    "production_source_factory_and_lifecycle_contract",
    "already_cancelled_still_queries_before_first_send",
    "ordered_prefix_then_cancel_at_blocked_third_send",
];
const ROW_CLASSIFICATIONS: [&str; 3] = ["SOURCE_ONLY", "RUNTIME_EXACT", "RUNTIME_EXACT"];

const GO_ONLY_METADATA: [&str; 18] = [
    "input.nodes[].token",
    "input.cancelAtLaneInCall",
    "expected.laneInCalls",
    "expected.deliveries[].sameGoInterfaceHandle",
    "expected.accessorCalls",
    "expected.events",
    "expected.runReturned and expected.contextCancelled",
    "expected.source.goLaneElementType",
    "expected.source.goLaneHasExplicitSourceTag",
    "expected.source.selectOperandsEvaluatedBeforeChoice",
    "expected.source.laneInEvaluatedWhenCancelWins",
    "expected.source.producerDetached and expected.source.producerJoined",
    "expected.source.sharedLaneWithSampleWorker",
    "expected.source.productionConcurrency",
    "expected.source.productionCapacityIsTotalRetentionBound",
    "expected.source.consumerDequeuesBeforeSemaphore",
    "expected.source.consumerCallbacksDetached",
    "expected.source.runtimeLaneGateIsOracleOnly",
];

const RUST_EXECUTION_PARTITION: [(&str, &str); 3] = [
    (FIXTURE_IDS[0], "SOURCE_ONLY_NO_RUST_RUNTIME_REPLAY"),
    (
        FIXTURE_IDS[1],
        "GO_RUNTIME_ONLY_WITH_SEPARATE_RUST_ZERO_WORK_PRE_READY_SHUTDOWN_DELTA",
    ),
    (
        FIXTURE_IDS[2],
        "RUST_RETAINED_HANDLE_PREFIX_SUFFIX_REPLAY_WITH_GO_LANE_ACCESSOR_CONCURRENCY_CALLBACK_METADATA_EXCLUDED",
    ),
];

const DELIBERATE_RUST_DELTAS: [&str; 8] = [
    "pre_ready_shutdown_then_input_close_are_biased_ahead_of_the_first_table_query",
    "positive_Tokio_test_route_replaces_the_oracle_only_unbuffered_Go_runtime_lane",
    "KTable_candidates_use_deterministic_ID_order_instead_of_a_Go_map_prefix",
    "shutdown_then_input_close_are_biased_ahead_of_each_send_and_fresh_delay",
    "retained_handles_keep_an_internal_source_variant_while_Go_has_no_explicit_source_tag",
    "empty_and_completed_round_delays_are_cancellation_aware_and_never_catch_up",
    "the_owned_taskless_run_future_and_typed_exits_replace_the_detached_unjoined_Go_producer",
    "five_saturating_component_local_counters_conserve_every_selected_occurrence_after_normal_exit",
];

const RUST_HARDENING_EVIDENCE: [(&str, &str); 8] = [
    (
        DELIBERATE_RUST_DELTAS[0],
        "ready_shutdown_wins_preclosed_input_before_query; preclosed_input_exits_before_query",
    ),
    (
        DELIBERATE_RUST_DELTAS[1],
        "go_pre_cancel_row_is_a_deliberate_rust_zero_work_pre_ready_shutdown_delta",
    ),
    (
        DELIBERATE_RUST_DELTAS[2],
        "ordered_prefix_replays_with_exact_retained_handles_and_attempted_third_send",
    ),
    (
        DELIBERATE_RUST_DELTAS[3],
        "ready_shutdown_beats_ready_capacity_for_blocked_send; closing_full_route_preserves_prefix_and_classifies_exact_suffix",
    ),
    (
        DELIBERATE_RUST_DELTAS[4],
        "ordered_prefix_replays_with_exact_retained_handles_and_attempted_third_send",
    ),
    (
        DELIBERATE_RUST_DELTAS[5],
        "delayed_first_poll_starts_immediate_query_then_fresh_exact_delay; empty_round_delay_is_shutdown_cancellation_aware; empty_round_delay_is_input_close_cancellation_aware",
    ),
    (
        DELIBERATE_RUST_DELTAS[6],
        "constructing_without_running_spawns_nothing_and_releases_eof; dropping_polled_run_releases_eof_without_terminal_classification",
    ),
    (
        DELIBERATE_RUST_DELTAS[7],
        "every_counter_saturates; capacity_two_preserves_exact_retained_prefix_and_shutdown_suffix",
    ),
];

const RUST_NONCLAIMS: [&str; 10] = [
    "the_exact_Go_production_map_candidate_prefix_is_not_replayed_by_Rust_ID_order",
    "the_nondeterministic_Go_ready_send_cancellation_choice_is_not_replayed_by_Rust_bias",
    "exact_Go_wall_clock_time_After_scheduling_is_not_replayed",
    "the_perpetual_empty_Go_runtime_path_is_source_only_and_not_executed",
    "cancellation_during_the_synchronous_Rust_table_query_is_not_claimed",
    "sample_worker_network_Bloom_triage_dequeue_semaphore_concurrency_and_callback_lifecycle_are_not_implemented_or_replayed",
    "Go_channel_capacity_100_is_not_claimed_as_a_total_Rust_retention_bound",
    "shared_route_cross_source_fairness_and_supervisor_composition_are_not_replayed_or_claimed",
    "Go_pointer_and_interface_ABI_identity_is_not_claimed_by_Rust_semantic_retained_generation_equality",
    "application_deployment_live_network_and_production_behavior_are_not_wired_or_claimed",
];

const GO_SOURCES: [(&str, &[u8], &str); 9] = [
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
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    id: String,
    subsystem: String,
    classification: String,
    oracle: Oracle,
    input: Input,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Oracle {
    composition: String,
    determinism: String,
    table: String,
    lane: String,
    clock: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Input {
    kind: String,
    context_initially_cancelled: bool,
    nodes: Vec<FixtureNode>,
    lane_capacity: usize,
    cancel_at_lane_in_call: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FixtureNode {
    token: String,
    id: String,
    addr: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Expected {
    get_calls: Vec<GetCall>,
    lane_in_calls: usize,
    deliveries: Vec<Delivery>,
    abandoned: Vec<FixtureNode>,
    accessor_calls: Vec<AccessorCalls>,
    events: Vec<String>,
    run_returned: bool,
    context_cancelled: bool,
    source: Option<Source>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct GetCall {
    limit: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Delivery {
    node: FixtureNode,
    same_go_interface_handle: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct AccessorCalls {
    token: String,
    id: usize,
    addr: usize,
    time: usize,
    dropped: usize,
    sample_infohashes_candidate: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Source {
    immediate_first_query: bool,
    limit: usize,
    production_selection_order: String,
    preserves_returned_order: bool,
    preserves_returned_handle_identity: bool,
    go_lane_element_type: String,
    producer_output_provenance: String,
    shared_lane_also_receives_discovered_nodes: bool,
    go_lane_has_explicit_source_tag: bool,
    select_operands_evaluated_before_choice: bool,
    lane_in_evaluated_when_cancel_wins: bool,
    per_node_send_cancellation_aware: bool,
    no_node_projection_or_recheck: bool,
    post_batch_delay_ms: u64,
    post_batch_sleep_cancellation_aware: bool,
    empty_table_cancellation_outcome: String,
    ready_send_cancel_outcome: String,
    producer_detached: bool,
    producer_joined: bool,
    shared_lane_with_sample_worker: bool,
    production_capacity: usize,
    production_concurrency: usize,
    production_capacity_is_total_retention_bound: bool,
    consumer_dequeues_before_semaphore: bool,
    consumer_callbacks_detached: bool,
    runtime_lane_gate_is_oracle_only: bool,
    post_batch_delay_runtime_observed: bool,
    empty_table_runtime_observed: bool,
    runtime_rows_return_before_sleep: bool,
    source_sha256: BTreeMap<String, String>,
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
        .all(|fixture| fixture.subsystem == "dht_crawler_sample_infohashes_producer"));
    assert_source_row(&fixtures[0]);
    assert_precancelled_go_row(&fixtures[1]);
    assert_ordered_prefix_go_row(&fixtures[2]);
    assert_eq!(
        RUST_EXECUTION_PARTITION,
        [
            (FIXTURE_IDS[0], "SOURCE_ONLY_NO_RUST_RUNTIME_REPLAY"),
            (
                FIXTURE_IDS[1],
                "GO_RUNTIME_ONLY_WITH_SEPARATE_RUST_ZERO_WORK_PRE_READY_SHUTDOWN_DELTA",
            ),
            (
                FIXTURE_IDS[2],
                "RUST_RETAINED_HANDLE_PREFIX_SUFFIX_REPLAY_WITH_GO_LANE_ACCESSOR_CONCURRENCY_CALLBACK_METADATA_EXCLUDED",
            ),
        ]
    );
    assert_eq!(
        GO_ONLY_METADATA,
        [
            "input.nodes[].token",
            "input.cancelAtLaneInCall",
            "expected.laneInCalls",
            "expected.deliveries[].sameGoInterfaceHandle",
            "expected.accessorCalls",
            "expected.events",
            "expected.runReturned and expected.contextCancelled",
            "expected.source.goLaneElementType",
            "expected.source.goLaneHasExplicitSourceTag",
            "expected.source.selectOperandsEvaluatedBeforeChoice",
            "expected.source.laneInEvaluatedWhenCancelWins",
            "expected.source.producerDetached and expected.source.producerJoined",
            "expected.source.sharedLaneWithSampleWorker",
            "expected.source.productionConcurrency",
            "expected.source.productionCapacityIsTotalRetentionBound",
            "expected.source.consumerDequeuesBeforeSemaphore",
            "expected.source.consumerCallbacksDetached",
            "expected.source.runtimeLaneGateIsOracleOnly",
        ]
    );
    assert_eq!(
        DELIBERATE_RUST_DELTAS,
        [
            "pre_ready_shutdown_then_input_close_are_biased_ahead_of_the_first_table_query",
            "positive_Tokio_test_route_replaces_the_oracle_only_unbuffered_Go_runtime_lane",
            "KTable_candidates_use_deterministic_ID_order_instead_of_a_Go_map_prefix",
            "shutdown_then_input_close_are_biased_ahead_of_each_send_and_fresh_delay",
            "retained_handles_keep_an_internal_source_variant_while_Go_has_no_explicit_source_tag",
            "empty_and_completed_round_delays_are_cancellation_aware_and_never_catch_up",
            "the_owned_taskless_run_future_and_typed_exits_replace_the_detached_unjoined_Go_producer",
            "five_saturating_component_local_counters_conserve_every_selected_occurrence_after_normal_exit",
        ]
    );
    assert_eq!(
        RUST_HARDENING_EVIDENCE,
        [
            (
                DELIBERATE_RUST_DELTAS[0],
                "ready_shutdown_wins_preclosed_input_before_query; preclosed_input_exits_before_query",
            ),
            (
                DELIBERATE_RUST_DELTAS[1],
                "go_pre_cancel_row_is_a_deliberate_rust_zero_work_pre_ready_shutdown_delta",
            ),
            (
                DELIBERATE_RUST_DELTAS[2],
                "ordered_prefix_replays_with_exact_retained_handles_and_attempted_third_send",
            ),
            (
                DELIBERATE_RUST_DELTAS[3],
                "ready_shutdown_beats_ready_capacity_for_blocked_send; closing_full_route_preserves_prefix_and_classifies_exact_suffix",
            ),
            (
                DELIBERATE_RUST_DELTAS[4],
                "ordered_prefix_replays_with_exact_retained_handles_and_attempted_third_send",
            ),
            (
                DELIBERATE_RUST_DELTAS[5],
                "delayed_first_poll_starts_immediate_query_then_fresh_exact_delay; empty_round_delay_is_shutdown_cancellation_aware; empty_round_delay_is_input_close_cancellation_aware",
            ),
            (
                DELIBERATE_RUST_DELTAS[6],
                "constructing_without_running_spawns_nothing_and_releases_eof; dropping_polled_run_releases_eof_without_terminal_classification",
            ),
            (
                DELIBERATE_RUST_DELTAS[7],
                "every_counter_saturates; capacity_two_preserves_exact_retained_prefix_and_shutdown_suffix",
            ),
        ]
    );
    assert_eq!(
        RUST_NONCLAIMS,
        [
            "the_exact_Go_production_map_candidate_prefix_is_not_replayed_by_Rust_ID_order",
            "the_nondeterministic_Go_ready_send_cancellation_choice_is_not_replayed_by_Rust_bias",
            "exact_Go_wall_clock_time_After_scheduling_is_not_replayed",
            "the_perpetual_empty_Go_runtime_path_is_source_only_and_not_executed",
            "cancellation_during_the_synchronous_Rust_table_query_is_not_claimed",
            "sample_worker_network_Bloom_triage_dequeue_semaphore_concurrency_and_callback_lifecycle_are_not_implemented_or_replayed",
            "Go_channel_capacity_100_is_not_claimed_as_a_total_Rust_retention_bound",
            "shared_route_cross_source_fairness_and_supervisor_composition_are_not_replayed_or_claimed",
            "Go_pointer_and_interface_ABI_identity_is_not_claimed_by_Rust_semantic_retained_generation_equality",
            "application_deployment_live_network_and_production_behavior_are_not_wired_or_claimed",
        ]
    );
}

fn assert_source_row(fixture: &Fixture) {
    assert_eq!(
        fixture.oracle.composition,
        "exact_production_source_factory_and_lifecycle_shapes"
    );
    assert_eq!(
        fixture.oracle.determinism,
        "normalized_ast_and_whole_source_sha256"
    );
    assert_eq!(
        fixture.oracle.table,
        "production_ktable_Table_GetNodesForSampleInfoHashes_interface"
    );
    assert_eq!(
        fixture.oracle.lane,
        "production_buffered_concurrent_channel"
    );
    assert_eq!(
        fixture.oracle.clock,
        "exact_source_unconditional_time_After_after_each_round"
    );
    assert_eq!(fixture.input.kind, "source_contract");
    assert!(!fixture.input.context_initially_cancelled);
    assert!(fixture.input.nodes.is_empty());
    assert_eq!(fixture.input.lane_capacity, 0);
    assert_eq!(fixture.input.cancel_at_lane_in_call, 0);
    assert!(fixture.expected.get_calls.is_empty());
    assert_eq!(fixture.expected.lane_in_calls, 0);
    assert!(fixture.expected.deliveries.is_empty());
    assert!(fixture.expected.abandoned.is_empty());
    assert!(fixture.expected.accessor_calls.is_empty());
    assert!(fixture.expected.events.is_empty());
    assert!(!fixture.expected.run_returned);
    assert!(!fixture.expected.context_cancelled);
    let source = fixture
        .expected
        .source
        .as_ref()
        .expect("source row has source facts");
    assert!(source.immediate_first_query);
    assert_eq!(source.limit, 60);
    assert_eq!(
        source.production_selection_order,
        "unspecified_map_iteration_prefix"
    );
    assert!(source.preserves_returned_order);
    assert!(source.preserves_returned_handle_identity);
    assert_eq!(source.go_lane_element_type, "ktable.Node");
    assert_eq!(source.producer_output_provenance, "retained_table_handle");
    assert!(source.shared_lane_also_receives_discovered_nodes);
    assert!(!source.go_lane_has_explicit_source_tag);
    assert!(source.select_operands_evaluated_before_choice);
    assert!(source.lane_in_evaluated_when_cancel_wins);
    assert!(source.per_node_send_cancellation_aware);
    assert!(source.no_node_projection_or_recheck);
    assert_eq!(source.post_batch_delay_ms, 1_000);
    assert!(!source.post_batch_sleep_cancellation_aware);
    assert_eq!(
        source.empty_table_cancellation_outcome,
        "while_every_query_remains_empty_queries_then_unconditionally_sleeps_one_second_forever_without_observing_cancellation"
    );
    assert_eq!(
        source.ready_send_cancel_outcome,
        "go_select_chooses_nondeterministically_when_send_and_cancellation_are_both_ready"
    );
    assert!(source.producer_detached);
    assert!(!source.producer_joined);
    assert!(source.shared_lane_with_sample_worker);
    assert_eq!(source.production_capacity, 100);
    assert_eq!(source.production_concurrency, 100);
    assert!(!source.production_capacity_is_total_retention_bound);
    assert!(source.consumer_dequeues_before_semaphore);
    assert!(source.consumer_callbacks_detached);
    assert!(source.runtime_lane_gate_is_oracle_only);
    assert!(!source.post_batch_delay_runtime_observed);
    assert!(!source.empty_table_runtime_observed);
    assert!(source.runtime_rows_return_before_sleep);
    assert_eq!(
        source.evidence,
        "runtime rows call the actual producer and return during its per-node select; the manual third-In gate only makes cancellation deterministic, while post-batch timing and perpetual-empty cancellation remain source-only"
    );
    assert_eq!(source.source_sha256.len(), GO_SOURCES.len());
    for (path, bytes, digest) in GO_SOURCES {
        assert_eq!(sha256(bytes), digest, "Go source drifted for {path}");
        assert_eq!(
            source.source_sha256.get(path).map(String::as_str),
            Some(digest)
        );
    }
    assert_eq!(QUERY_LIMIT.get(), source.limit);
    assert_eq!(
        ROUND_DELAY,
        Duration::from_millis(source.post_batch_delay_ms)
    );
    assert_eq!(
        DhtDiscoveredNodeSchedulerConfig::default()
            .sample_infohashes_capacity
            .get(),
        source.production_capacity
    );
}

fn assert_precancelled_go_row(fixture: &Fixture) {
    assert_runtime_oracle(fixture, "pre_cancelled_context_and_unbuffered_lane");
    assert_eq!(fixture.input.kind, "actual_getNodesForSampleInfoHashes");
    assert!(fixture.input.context_initially_cancelled);
    assert_eq!(fixture.input.lane_capacity, 0);
    assert_eq!(fixture.input.cancel_at_lane_in_call, 0);
    assert_eq!(
        fixture.input.nodes,
        [fixture_node(
            "A",
            "0000000000000000000000000000000000000001",
            "192.0.2.1:6001",
        )]
    );
    assert_eq!(fixture.expected.get_calls, [GetCall { limit: 60 }]);
    assert_eq!(fixture.expected.lane_in_calls, 1);
    assert!(fixture.expected.deliveries.is_empty());
    assert_eq!(fixture.expected.abandoned, fixture.input.nodes);
    assert_zero_accessors(&fixture.expected.accessor_calls, &["A"]);
    assert_eq!(
        fixture.expected.events,
        ["get_nodes_for_sample_infohashes", "lane_in:1", "return"]
    );
    assert!(fixture.expected.run_returned);
    assert!(fixture.expected.context_cancelled);
    assert!(fixture.expected.source.is_none());
}

fn assert_ordered_prefix_go_row(fixture: &Fixture) {
    assert_runtime_oracle(fixture, "capacity_two_lane_with_third_In_gate");
    assert_eq!(fixture.input.kind, "actual_getNodesForSampleInfoHashes");
    assert!(!fixture.input.context_initially_cancelled);
    assert_eq!(fixture.input.lane_capacity, 2);
    assert_eq!(fixture.input.cancel_at_lane_in_call, 3);
    let nodes = expected_fixture_nodes();
    assert_eq!(fixture.input.nodes, nodes);
    assert_eq!(fixture.expected.get_calls, [GetCall { limit: 60 }]);
    assert_eq!(fixture.expected.lane_in_calls, 3);
    assert_eq!(fixture.expected.deliveries.len(), 2);
    for (delivery, node) in fixture.expected.deliveries.iter().zip(&nodes[..2]) {
        assert_eq!(&delivery.node, node);
        assert!(delivery.same_go_interface_handle);
    }
    assert_eq!(fixture.expected.abandoned, nodes[2..]);
    assert_zero_accessors(&fixture.expected.accessor_calls, &["A", "B", "C", "D"]);
    assert_eq!(
        fixture.expected.events,
        [
            "get_nodes_for_sample_infohashes",
            "lane_in:1",
            "lane_in:2",
            "lane_in:3",
            "cancel",
            "return",
        ]
    );
    assert!(fixture.expected.run_returned);
    assert!(fixture.expected.context_cancelled);
    assert!(fixture.expected.source.is_none());
}

fn assert_runtime_oracle(fixture: &Fixture, determinism: &str) {
    assert_eq!(
        fixture.oracle.composition,
        "actual_crawler_getNodesForSampleInfoHashes_with_scripted_table_and_manual_lane"
    );
    assert_eq!(fixture.oracle.determinism, determinism);
    assert_eq!(
        fixture.oracle.table,
        "scripted_ktable_Table_GetNodesForSampleInfoHashes_override"
    );
    assert_eq!(
        fixture.oracle.lane,
        "capacity_controlled_BufferedConcurrentChannel_In_override"
    );
    assert_eq!(
        fixture.oracle.clock,
        "production_unconditional_time_After_not_reached_by_runtime_row"
    );
}

fn assert_zero_accessors(calls: &[AccessorCalls], tokens: &[&str]) {
    assert_eq!(calls.len(), tokens.len());
    for (calls, token) in calls.iter().zip(tokens) {
        assert_eq!(calls.token, *token);
        assert_eq!(
            (
                calls.id,
                calls.addr,
                calls.time,
                calls.dropped,
                calls.sample_infohashes_candidate,
            ),
            (0, 0, 0, 0, 0)
        );
    }
}

fn fixture_node(token: &str, id: &str, addr: &str) -> FixtureNode {
    FixtureNode {
        token: token.into(),
        id: id.into(),
        addr: addr.into(),
    }
}

fn expected_fixture_nodes() -> [FixtureNode; 4] {
    [
        fixture_node(
            "A",
            "0000000000000000000000000000000000000001",
            "192.0.2.1:6001",
        ),
        fixture_node(
            "B",
            "0000000000000000000000000000000000000002",
            "192.0.2.2:6002",
        ),
        fixture_node(
            "C",
            "0000000000000000000000000000000000000003",
            "192.0.2.3:6003",
        ),
        fixture_node(
            "D",
            "0000000000000000000000000000000000000004",
            "192.0.2.4:6004",
        ),
    ]
}

fn routing_node(value: &FixtureNode) -> RoutingNode {
    let addr = value
        .addr
        .parse::<SocketAddr>()
        .unwrap_or_else(|error| panic!("invalid fixture address {}: {error}", value.addr));
    assert!(
        matches!(addr.ip(), IpAddr::V4(_)),
        "producer fixture addresses are IPv4-only"
    );
    RoutingNode {
        id: Id20::from_hex(&value.id)
            .unwrap_or_else(|error| panic!("invalid fixture ID {}: {error}", value.id)),
        addr,
    }
}

fn retained(table: &KTable, node: RoutingNode) -> KTableNodeHandle {
    assert_eq!(table.put_node(node), RoutingPutResult::Accepted);
    table.node_handle(node.id).unwrap()
}

fn retained_work(work: DhtDiscoveredNodeSampleInfoHashesWork) -> KTableNodeHandle {
    match work {
        DhtDiscoveredNodeSampleInfoHashesWork::Retained(handle) => handle,
        DhtDiscoveredNodeSampleInfoHashesWork::Discovered(node) => {
            panic!("sample producer projected retained work as {node:?}")
        }
    }
}

fn assert_conservation(stats: DhtSampleInfoHashesProducerStats) {
    assert_eq!(
        stats.selected,
        stats
            .queued
            .saturating_add(stats.input_closed_dropped)
            .saturating_add(stats.shutdown_dropped)
    );
}

async fn poll_once_pending<F>(mut future: std::pin::Pin<&mut F>)
where
    F: Future,
{
    poll_fn(|context| match future.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("producer completed instead of registering as pending"),
    })
    .await;
}

#[tokio::test]
async fn go_pre_cancel_row_is_a_deliberate_rust_zero_work_pre_ready_shutdown_delta() {
    let fixtures = fixtures();
    let fixture = &fixtures[1];
    assert_precancelled_go_row(fixture);

    let advertised = routing_node(&fixture.input.nodes[0]);
    let table = KTable::new(Id20::ZERO);
    let retained = retained(&table, advertised);
    let (input, mut receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
    let (producer, stats) = DhtSampleInfoHashesProducer::new(table.clone(), input);

    assert_eq!(
        producer.run(ready(())).await,
        DhtSampleInfoHashesProducerExit::Shutdown {
            selected_dropped: 0,
        }
    );
    assert_eq!(
        stats.snapshot(),
        DhtSampleInfoHashesProducerStats::default()
    );
    assert_eq!(table.node_handle(advertised.id).unwrap(), retained);
    assert!(receiver.recv_work().await.is_none());
}

#[tokio::test]
async fn ordered_prefix_replays_with_exact_retained_handles_and_attempted_third_send() {
    let fixtures = fixtures();
    let fixture = &fixtures[2];
    assert_ordered_prefix_go_row(fixture);

    let nodes = fixture
        .input
        .nodes
        .iter()
        .map(routing_node)
        .collect::<Vec<_>>();
    let table = KTable::new(Id20::ZERO);
    let handles = nodes
        .iter()
        .copied()
        .map(|node| retained(&table, node))
        .collect::<Vec<_>>();
    let (input, mut receiver) =
        DhtDiscoveredNodeSampleInfoHashesInput::test_channel(fixture.input.lane_capacity);
    let (producer, stats) = DhtSampleInfoHashesProducer::new(table, input);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let attempted = Arc::new(Mutex::new(Vec::new()));
    let attempted_for_hook = Arc::clone(&attempted);
    let run = producer.run_with(
        async move {
            let _ = shutdown_rx.await;
        },
        |_| std::future::pending::<()>(),
        move |index, handle| {
            attempted_for_hook
                .lock()
                .unwrap()
                .push((index, handle.clone()));
        },
    );
    tokio::pin!(run);

    poll_once_pending(run.as_mut()).await;
    assert_eq!(
        attempted
            .lock()
            .unwrap()
            .iter()
            .map(|(index, _)| *index)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    for ((index, attempted), expected) in attempted.lock().unwrap().iter().zip(&handles[..3]) {
        assert_eq!(
            attempted, expected,
            "attempt {index} changed handle identity"
        );
    }
    let interim_stats = stats.snapshot();
    assert_eq!(
        interim_stats,
        DhtSampleInfoHashesProducerStats {
            table_queries: 1,
            selected: 4,
            queued: 2,
            input_closed_dropped: 0,
            shutdown_dropped: 0,
        }
    );

    shutdown_tx.send(()).unwrap();
    assert_eq!(
        run.await,
        DhtSampleInfoHashesProducerExit::Shutdown {
            selected_dropped: 2,
        }
    );
    let first = retained_work(receiver.recv_work().await.unwrap());
    let second = retained_work(receiver.recv_work().await.unwrap());
    assert_eq!(first, handles[0]);
    assert_eq!(second, handles[1]);
    assert_eq!(first.routing_node(), nodes[0]);
    assert_eq!(second.routing_node(), nodes[1]);
    assert!(receiver.recv_work().await.is_none());
    assert_eq!(fixture.expected.abandoned, fixture.input.nodes[2..]);
    assert_eq!(handles[2].routing_node(), nodes[2]);
    assert_eq!(handles[3].routing_node(), nodes[3]);
    let final_stats = stats.snapshot();
    assert_eq!(
        final_stats,
        DhtSampleInfoHashesProducerStats {
            table_queries: 1,
            selected: 4,
            queued: 2,
            input_closed_dropped: 0,
            shutdown_dropped: 2,
        }
    );
    assert_conservation(final_stats);
}
