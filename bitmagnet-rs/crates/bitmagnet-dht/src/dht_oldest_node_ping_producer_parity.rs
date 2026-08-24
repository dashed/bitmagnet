use std::collections::{BTreeMap, VecDeque};
use std::future::{pending, poll_fn, ready, Future};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use super::*;
use crate::{
    DhtDiscoveredNodePingInput, DhtDiscoveredNodePingWorkerConfig,
    DhtDiscoveredNodeSchedulerConfig, Id20, KTableClock, KTableNodeOption, RoutingNode,
    RoutingPutResult,
};

const FIXTURE_TEXT: &str =
    include_str!("../../../../testdata/parity/dht/dht_crawler_old_node_ping_producer.jsonl");
const FIXTURE_BYTES: &[u8] =
    include_bytes!("../../../../testdata/parity/dht/dht_crawler_old_node_ping_producer.jsonl");
const FIXTURE_SHA256: &str = "d300e4606f9811f402af6d835748d09dbc59434f733a28079ac0df5e2f99ae5a";
const FIXTURE_IDS: [&str; 3] = [
    "production_source_factory_and_lifecycle_contract",
    "already_cancelled_returns_before_initial_timer_and_query",
    "first_timer_ordered_prefix_then_cancel_at_blocked_third_send",
];
const ROW_CLASSIFICATIONS: [&str; 3] = ["SOURCE_ONLY", "RUNTIME_EXACT", "RUNTIME_EXACT"];

const GO_ONLY_METADATA: [&str; 9] = [
    "input.nodes[].token",
    "input.intervalMs on runtime rows",
    "expected.getCalls[].cutoffWindowMatched",
    "expected.getCalls[].waitedAtLeastInterval",
    "expected.laneInCalls",
    "expected.deliveries[].sameGoInterfaceHandle",
    "expected.accessorCalls",
    "expected.events",
    "source.consumerGuardAfterSemaphore",
];

const RUST_EXECUTION_PARTITION: [(&str, &str); 3] = [
    (FIXTURE_IDS[0], "SOURCE_ONLY_NO_RUST_RUNTIME_REPLAY"),
    (
        FIXTURE_IDS[1],
        "RUST_ZERO_WORK_PRE_READY_SHUTDOWN_REPLAY_WITH_GO_TIMER_CONSTRUCTION_METADATA_EXCLUDED",
    ),
    (
        FIXTURE_IDS[2],
        "RUST_INJECTED_DELAY_QUERY_PREFIX_SUFFIX_REPLAY_WITH_GO_WALL_CLOCK_LIVE_HANDLE_ACCESSOR_AND_EVENT_METADATA_EXCLUDED",
    ),
];

const DELIBERATE_RUST_DELTAS: [&str; 12] = [
    "ready_shutdown_and_input_close_are_biased_ahead_of_delay_query_and_capacity_progress",
    "the_owned_taskless_run_future_replaces_the_detached_unjoined_Go_producer",
    "a_fresh_cancellation_aware_monotonic_delay_never_catches_up",
    "Go_limit_zero_maps_to_Rust_None_for_an_uncapped_query",
    "cutoff_subtraction_floors_at_the_oldest_representable_Instant",
    "Rust_equal_time_order_uses_an_ID_tie_break_while_Go_is_unspecified",
    "positive_Tokio_capacity_and_typed_InputClosed_replace_raw_Go_channel_lifecycle",
    "Dropped_then_strict_recent_recheck_moves_from_the_Go_post_semaphore_worker_to_post_reserve_producer_code",
    "each_eligible_live_handle_becomes_one_immutable_RoutingNode_after_capacity_reservation",
    "the_blocked_Rust_occurrence_is_not_rechecked_or_snapshotted_until_reservation_while_Go_calls_In_before_its_blocked_send",
    "typed_Shutdown_and_InputClosed_terminal_exits_replace_the_detached_Go_producer_without_a_terminal_value",
    "seven_saturating_component_local_counters_with_terminal_selected_conservation_are_Rust_hardening",
];

const RUST_HARDENING_EVIDENCE: [(&str, &str); 12] = [
    (
        DELIBERATE_RUST_DELTAS[0],
        "ready_shutdown_wins_preclosed_input_before_delay_or_query; preclosed_input_exits_before_delay_or_query; tied_shutdown_wins_new_capacity_and_accounts_selected_suffix",
    ),
    (
        DELIBERATE_RUST_DELTAS[1],
        "DhtOldestNodePingProducer::run; dropping_run_blocked_on_capacity_has_no_terminal_accounting_and_releases_eof",
    ),
    (
        DELIBERATE_RUST_DELTAS[2],
        "first_query_waits_the_exact_ten_second_boundary; delayed_poll_starts_a_fresh_delay_without_catch_up",
    ),
    (
        DELIBERATE_RUST_DELTAS[3],
        "query_is_uncapped_and_preserves_deterministic_table_tie_order",
    ),
    (
        DELIBERATE_RUST_DELTAS[4],
        "constants_floor_and_public_handles_are_sound",
    ),
    (
        DELIBERATE_RUST_DELTAS[5],
        "KTable::get_oldest_nodes sorts by last response and ID; query_is_uncapped_and_preserves_deterministic_table_tie_order",
    ),
    (
        DELIBERATE_RUST_DELTAS[6],
        "DhtDiscoveredNodePingInput::reserve; close_after_reserve_keeps_commit_authority_then_exits_input_closed",
    ),
    (
        DELIBERATE_RUST_DELTAS[7],
        "post_reserve_rechecks_dropped_then_recent_and_snapshots_the_live_address; mutation_while_capacity_is_blocked_is_observed_only_after_reservation",
    ),
    (
        DELIBERATE_RUST_DELTAS[8],
        "address_mutation_while_capacity_is_blocked_is_snapshotted_on_commit",
    ),
    (
        DELIBERATE_RUST_DELTAS[9],
        "capacity_two_oracle_prefix_then_shutdown_drops_c_and_d; ordered-prefix parity replay observes post-reserve indices zero and one only",
    ),
    (
        DELIBERATE_RUST_DELTAS[10],
        "DhtOldestNodePingProducerExit::Shutdown; DhtOldestNodePingProducerExit::InputClosed",
    ),
    (
        DELIBERATE_RUST_DELTAS[11],
        "DhtOldestNodePingProducerStats has table_queries selected dropped_skipped recent_skipped queued input_closed_dropped shutdown_dropped; normal exit conserves every selected occurrence; all_counter_updates_saturate",
    ),
];

const GO_SOURCES: [(&str, &[u8], &str); 8] = [
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
        "internal/dhtcrawler/ping.go",
        include_bytes!("../../../../internal/dhtcrawler/ping.go"),
        "45561d97a79060e6b96bc81f7d83491195e4ff60fbdc9460d9973675547804a2",
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
    interval_ms: u64,
    old_peer_threshold_seconds: u64,
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
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct GetCall {
    limit: usize,
    cutoff_window_matched: bool,
    waited_at_least_interval: bool,
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
    initial_delay_before_first_query: bool,
    interval_seconds: u64,
    old_peer_threshold_seconds: u64,
    limit: usize,
    zero_limit_is_unbounded: bool,
    strict_cutoff: bool,
    production_selection_order: String,
    preserves_returned_order: bool,
    per_node_send_cancellation_aware: bool,
    no_node_projection_or_recheck: bool,
    fresh_leading_delay_per_loop: bool,
    leading_delay_cancellation_aware: bool,
    empty_table_cancellation_outcome: String,
    ready_timer_cancel_outcome: String,
    ready_send_cancel_outcome: String,
    producer_detached: bool,
    producer_joined: bool,
    default_scaling_factor: usize,
    production_capacity: usize,
    production_concurrency: usize,
    consumer_dropped_guard: bool,
    consumer_recent_guard_strict_after: bool,
    consumer_guard_after_semaphore: bool,
    cutoff_clock_runtime_bracketed: bool,
    positive_timer_runtime_observed: bool,
    factory_timer_runtime_observed: bool,
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
fn fixture_schema_identity_sources_and_metadata_are_frozen() {
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
        .all(|fixture| fixture.subsystem == "dht_crawler_old_node_ping_producer"));
    assert_source_row(&fixtures[0]);
    assert_precancelled_go_row(&fixtures[1]);
    assert_ordered_prefix_go_row(&fixtures[2]);
    assert_eq!(
        GO_ONLY_METADATA,
        [
            "input.nodes[].token",
            "input.intervalMs on runtime rows",
            "expected.getCalls[].cutoffWindowMatched",
            "expected.getCalls[].waitedAtLeastInterval",
            "expected.laneInCalls",
            "expected.deliveries[].sameGoInterfaceHandle",
            "expected.accessorCalls",
            "expected.events",
            "source.consumerGuardAfterSemaphore",
        ]
    );
    assert_eq!(
        RUST_EXECUTION_PARTITION,
        [
            (FIXTURE_IDS[0], "SOURCE_ONLY_NO_RUST_RUNTIME_REPLAY"),
            (
                FIXTURE_IDS[1],
                "RUST_ZERO_WORK_PRE_READY_SHUTDOWN_REPLAY_WITH_GO_TIMER_CONSTRUCTION_METADATA_EXCLUDED",
            ),
            (
                FIXTURE_IDS[2],
                "RUST_INJECTED_DELAY_QUERY_PREFIX_SUFFIX_REPLAY_WITH_GO_WALL_CLOCK_LIVE_HANDLE_ACCESSOR_AND_EVENT_METADATA_EXCLUDED",
            ),
        ]
    );
    assert_eq!(
        DELIBERATE_RUST_DELTAS,
        [
            "ready_shutdown_and_input_close_are_biased_ahead_of_delay_query_and_capacity_progress",
            "the_owned_taskless_run_future_replaces_the_detached_unjoined_Go_producer",
            "a_fresh_cancellation_aware_monotonic_delay_never_catches_up",
            "Go_limit_zero_maps_to_Rust_None_for_an_uncapped_query",
            "cutoff_subtraction_floors_at_the_oldest_representable_Instant",
            "Rust_equal_time_order_uses_an_ID_tie_break_while_Go_is_unspecified",
            "positive_Tokio_capacity_and_typed_InputClosed_replace_raw_Go_channel_lifecycle",
            "Dropped_then_strict_recent_recheck_moves_from_the_Go_post_semaphore_worker_to_post_reserve_producer_code",
            "each_eligible_live_handle_becomes_one_immutable_RoutingNode_after_capacity_reservation",
            "the_blocked_Rust_occurrence_is_not_rechecked_or_snapshotted_until_reservation_while_Go_calls_In_before_its_blocked_send",
            "typed_Shutdown_and_InputClosed_terminal_exits_replace_the_detached_Go_producer_without_a_terminal_value",
            "seven_saturating_component_local_counters_with_terminal_selected_conservation_are_Rust_hardening",
        ]
    );
    assert_eq!(
        RUST_HARDENING_EVIDENCE,
        [
            (
                DELIBERATE_RUST_DELTAS[0],
                "ready_shutdown_wins_preclosed_input_before_delay_or_query; preclosed_input_exits_before_delay_or_query; tied_shutdown_wins_new_capacity_and_accounts_selected_suffix",
            ),
            (
                DELIBERATE_RUST_DELTAS[1],
                "DhtOldestNodePingProducer::run; dropping_run_blocked_on_capacity_has_no_terminal_accounting_and_releases_eof",
            ),
            (
                DELIBERATE_RUST_DELTAS[2],
                "first_query_waits_the_exact_ten_second_boundary; delayed_poll_starts_a_fresh_delay_without_catch_up",
            ),
            (
                DELIBERATE_RUST_DELTAS[3],
                "query_is_uncapped_and_preserves_deterministic_table_tie_order",
            ),
            (
                DELIBERATE_RUST_DELTAS[4],
                "constants_floor_and_public_handles_are_sound",
            ),
            (
                DELIBERATE_RUST_DELTAS[5],
                "KTable::get_oldest_nodes sorts by last response and ID; query_is_uncapped_and_preserves_deterministic_table_tie_order",
            ),
            (
                DELIBERATE_RUST_DELTAS[6],
                "DhtDiscoveredNodePingInput::reserve; close_after_reserve_keeps_commit_authority_then_exits_input_closed",
            ),
            (
                DELIBERATE_RUST_DELTAS[7],
                "post_reserve_rechecks_dropped_then_recent_and_snapshots_the_live_address; mutation_while_capacity_is_blocked_is_observed_only_after_reservation",
            ),
            (
                DELIBERATE_RUST_DELTAS[8],
                "address_mutation_while_capacity_is_blocked_is_snapshotted_on_commit",
            ),
            (
                DELIBERATE_RUST_DELTAS[9],
                "capacity_two_oracle_prefix_then_shutdown_drops_c_and_d; ordered-prefix parity replay observes post-reserve indices zero and one only",
            ),
            (
                DELIBERATE_RUST_DELTAS[10],
                "DhtOldestNodePingProducerExit::Shutdown; DhtOldestNodePingProducerExit::InputClosed",
            ),
            (
                DELIBERATE_RUST_DELTAS[11],
                "DhtOldestNodePingProducerStats has table_queries selected dropped_skipped recent_skipped queued input_closed_dropped shutdown_dropped; normal exit conserves every selected occurrence; all_counter_updates_saturate",
            ),
        ]
    );
}

fn assert_source_row(fixture: &Fixture) {
    assert_eq!(
        fixture.oracle.composition,
        "exact_production_source_factory_query_consumer_and_lifecycle_shapes"
    );
    assert_eq!(
        fixture.oracle.determinism,
        "normalized_ast_and_whole_source_sha256"
    );
    assert_eq!(
        fixture.oracle.table,
        "production_ktable_Table_GetOldestNodes_strict_unbounded_query"
    );
    assert_eq!(
        fixture.oracle.lane,
        "production_buffered_concurrent_channel_shared_with_runPing"
    );
    assert_eq!(
        fixture.oracle.clock,
        "exact_source_time_After_then_time_Now_shapes"
    );
    assert_eq!(fixture.input.kind, "source_contract");
    assert!(!fixture.input.context_initially_cancelled);
    assert_eq!(fixture.input.interval_ms, 10_000);
    assert_eq!(fixture.input.old_peer_threshold_seconds, 900);
    assert!(fixture.input.nodes.is_empty());
    assert_eq!(fixture.input.lane_capacity, 10);
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
    assert!(source.initial_delay_before_first_query);
    assert_eq!(source.interval_seconds, 10);
    assert_eq!(source.old_peer_threshold_seconds, 900);
    assert_eq!(source.limit, 0);
    assert!(source.zero_limit_is_unbounded);
    assert!(source.strict_cutoff);
    assert_eq!(
        source.production_selection_order,
        "ascending_Time_with_unspecified_equal_time_order"
    );
    assert!(source.preserves_returned_order);
    assert!(source.per_node_send_cancellation_aware);
    assert!(source.no_node_projection_or_recheck);
    assert!(source.fresh_leading_delay_per_loop);
    assert!(source.leading_delay_cancellation_aware);
    assert_eq!(
        source.empty_table_cancellation_outcome,
        "after_an_empty_query_the_loop_returns_to_a_fresh_cancellation_aware_leading_select"
    );
    assert_eq!(
        source.ready_timer_cancel_outcome,
        "go_select_chooses_nondeterministically_when_both_are_ready"
    );
    assert_eq!(
        source.ready_send_cancel_outcome,
        "go_select_chooses_nondeterministically_when_both_are_ready"
    );
    assert!(source.producer_detached);
    assert!(!source.producer_joined);
    assert_eq!(source.default_scaling_factor, 10);
    assert_eq!(source.production_capacity, 10);
    assert_eq!(source.production_concurrency, 10);
    assert!(source.consumer_dropped_guard);
    assert!(source.consumer_recent_guard_strict_after);
    assert!(source.consumer_guard_after_semaphore);
    assert!(source.cutoff_clock_runtime_bracketed);
    assert!(source.positive_timer_runtime_observed);
    assert!(!source.factory_timer_runtime_observed);
    assert_eq!(
        source.evidence,
        "the actual method rows execute pre-cancel and a shortened positive timer; the factory ten-second timer, equal-ready select outcomes, production table order, and consumer callback scheduling remain source evidence"
    );
    assert_eq!(source.source_sha256.len(), GO_SOURCES.len());
    for (path, bytes, digest) in GO_SOURCES {
        assert_eq!(sha256(bytes), digest, "Go source drifted for {path}");
        assert_eq!(
            source.source_sha256.get(path).map(String::as_str),
            Some(digest)
        );
    }

    assert_eq!(QUERY_DELAY, Duration::from_secs(source.interval_seconds));
    assert_eq!(
        OLD_PEER_THRESHOLD,
        Duration::from_secs(source.old_peer_threshold_seconds)
    );
    assert_eq!(
        DhtDiscoveredNodeSchedulerConfig::default()
            .ping_capacity
            .get(),
        source.production_capacity
    );
    assert_eq!(
        DhtDiscoveredNodePingWorkerConfig::default()
            .max_inflight
            .get(),
        source.production_concurrency
    );
}

fn assert_precancelled_go_row(fixture: &Fixture) {
    assert_runtime_oracle(
        fixture,
        "pre_cancelled_context_with_positive_unobserved_timer",
    );
    assert_eq!(fixture.input.kind, "actual_getOldNodes");
    assert!(fixture.input.context_initially_cancelled);
    assert_eq!(fixture.input.interval_ms, 60_000);
    assert_eq!(fixture.input.old_peer_threshold_seconds, 900);
    assert!(fixture.input.nodes.is_empty());
    assert_eq!(fixture.input.lane_capacity, 0);
    assert_eq!(fixture.input.cancel_at_lane_in_call, 0);
    assert!(fixture.expected.get_calls.is_empty());
    assert_eq!(fixture.expected.lane_in_calls, 0);
    assert!(fixture.expected.deliveries.is_empty());
    assert!(fixture.expected.abandoned.is_empty());
    assert!(fixture.expected.accessor_calls.is_empty());
    assert_eq!(fixture.expected.events, ["run_start", "return"]);
    assert!(fixture.expected.run_returned);
    assert!(fixture.expected.context_cancelled);
    assert!(fixture.expected.source.is_none());
}

fn assert_ordered_prefix_go_row(fixture: &Fixture) {
    assert_runtime_oracle(
        fixture,
        "shortened_positive_timer_and_capacity_two_lane_with_third_In_gate",
    );
    assert_eq!(fixture.input.kind, "actual_getOldNodes");
    assert!(!fixture.input.context_initially_cancelled);
    assert_eq!(fixture.input.interval_ms, 10);
    assert_eq!(fixture.input.old_peer_threshold_seconds, 900);
    assert_eq!(fixture.input.lane_capacity, 2);
    assert_eq!(fixture.input.cancel_at_lane_in_call, 3);
    let nodes = [
        fixture_node(
            "A",
            "0000000000000000000000000000000000000001",
            "192.0.2.1:7001",
        ),
        fixture_node(
            "B",
            "0000000000000000000000000000000000000002",
            "192.0.2.2:7002",
        ),
        fixture_node(
            "C",
            "0000000000000000000000000000000000000003",
            "192.0.2.3:7003",
        ),
        fixture_node(
            "D",
            "0000000000000000000000000000000000000004",
            "192.0.2.4:7004",
        ),
    ];
    assert_eq!(fixture.input.nodes, nodes);
    assert_eq!(
        fixture.expected.get_calls,
        [GetCall {
            limit: 0,
            cutoff_window_matched: true,
            waited_at_least_interval: true,
        }]
    );
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
            "run_start",
            "get_oldest_nodes",
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
        "actual_crawler_getOldNodes_with_scripted_table_and_manual_lane"
    );
    assert_eq!(fixture.oracle.determinism, determinism);
    assert_eq!(
        fixture.oracle.table,
        "scripted_ktable_Table_GetOldestNodes_override"
    );
    assert_eq!(
        fixture.oracle.lane,
        "capacity_controlled_BufferedConcurrentChannel_In_override"
    );
    assert_eq!(
        fixture.oracle.clock,
        "production_time_After_and_time_Now_with_runtime_bracketed_cutoff"
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

struct ScriptedClock {
    values: Mutex<VecDeque<Instant>>,
}

impl KTableClock for ScriptedClock {
    fn now(&self) -> Instant {
        self.values
            .lock()
            .expect("scripted parity clock lock poisoned")
            .pop_front()
            .expect("scripted parity clock exhausted")
    }
}

#[tokio::test]
async fn pre_cancelled_go_row_replays_as_rust_biased_ready_shutdown_zero_work() {
    let fixtures = fixtures();
    let fixture = &fixtures[1];
    assert_precancelled_go_row(fixture);

    let table = KTable::new(Id20::ZERO);
    let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
    let (producer, stats) = DhtOldestNodePingProducer::new(table, input);
    assert_eq!(
        producer
            .run_with(
                ready(()),
                || panic!("ready shutdown must prevent a producer clock read"),
                |duration| {
                    assert_eq!(duration, QUERY_DELAY);
                    ready(())
                },
                |_, _| panic!("ready shutdown must prevent a post-reserve callback"),
            )
            .await,
        DhtOldestNodePingProducerExit::Shutdown {
            selected_dropped: 0,
        }
    );
    assert_eq!(stats.snapshot(), DhtOldestNodePingProducerStats::default());
    assert_eq!(receiver.recv().await, None);
}

#[tokio::test]
async fn ordered_prefix_row_replays_on_actual_rust_producer_with_explicit_deltas() {
    let fixtures = fixtures();
    let fixture = &fixtures[2];
    assert_ordered_prefix_go_row(fixture);

    let expected_nodes = [
        RoutingNode {
            id: Id20::from_hex("0000000000000000000000000000000000000001").unwrap(),
            addr: "192.0.2.1:7001".parse().unwrap(),
        },
        RoutingNode {
            id: Id20::from_hex("0000000000000000000000000000000000000002").unwrap(),
            addr: "192.0.2.2:7002".parse().unwrap(),
        },
        RoutingNode {
            id: Id20::from_hex("0000000000000000000000000000000000000003").unwrap(),
            addr: "192.0.2.3:7003".parse().unwrap(),
        },
        RoutingNode {
            id: Id20::from_hex("0000000000000000000000000000000000000004").unwrap(),
            addr: "192.0.2.4:7004".parse().unwrap(),
        },
    ];
    assert_eq!(
        fixture
            .input
            .nodes
            .iter()
            .map(routing_node)
            .collect::<Vec<_>>(),
        expected_nodes
    );

    let query_now = Instant::now()
        .checked_add(Duration::from_secs(20 * 60))
        .expect("twenty minutes fit the monotonic clock");
    let cutoff = query_now - OLD_PEER_THRESHOLD;
    let table = KTable::with_clock(
        Id20::ZERO,
        Arc::new(ScriptedClock {
            values: Mutex::new(VecDeque::from([
                cutoff - Duration::from_nanos(4),
                cutoff - Duration::from_nanos(3),
                cutoff - Duration::from_nanos(2),
                cutoff - Duration::from_nanos(1),
                cutoff,
            ])),
        }),
    );
    for node in expected_nodes {
        assert_eq!(
            table.put_node_with_options(node, &[KTableNodeOption::Responded]),
            RoutingPutResult::Accepted
        );
    }
    let exact_cutoff_sentinel = RoutingNode {
        id: Id20::from_hex("0000000000000000000000000000000000000005").unwrap(),
        addr: "192.0.2.5:7005".parse().unwrap(),
    };
    assert_eq!(
        table.put_node_with_options(exact_cutoff_sentinel, &[KTableNodeOption::Responded],),
        RoutingPutResult::Accepted
    );
    assert_eq!(
        table
            .get_oldest_nodes(cutoff, None)
            .iter()
            .map(|handle| handle.routing_node())
            .collect::<Vec<_>>(),
        expected_nodes
    );

    let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(2);
    let (producer, stats) = DhtOldestNodePingProducer::new(table, input);
    let (delay_tx, delay_rx) = oneshot::channel();
    let delay_rx = Arc::new(Mutex::new(Some(delay_rx)));
    let delay_calls = Arc::new(Mutex::new(Vec::new()));
    let delay_calls_for_run = Arc::clone(&delay_calls);
    let delay_rx_for_run = Arc::clone(&delay_rx);
    let now_calls = Arc::new(AtomicUsize::new(0));
    let now_calls_for_run = Arc::clone(&now_calls);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let post_reserve = Arc::new(Mutex::new(Vec::new()));
    let post_reserve_for_run = Arc::clone(&post_reserve);
    let run = producer.run_with(
        async move {
            let _ = shutdown_rx.await;
        },
        move || {
            now_calls_for_run.fetch_add(1, Ordering::Relaxed);
            query_now
        },
        move |duration| {
            delay_calls_for_run.lock().unwrap().push(duration);
            let gate = delay_rx_for_run.lock().unwrap().take();
            async move {
                match gate {
                    Some(gate) => {
                        let _ = gate.await;
                    }
                    None => pending::<()>().await,
                }
            }
        },
        move |index, _| post_reserve_for_run.lock().unwrap().push(index),
    );
    tokio::pin!(run);

    poll_once_pending(run.as_mut()).await;
    assert_eq!(*delay_calls.lock().unwrap(), [QUERY_DELAY]);
    assert_eq!(now_calls.load(Ordering::Relaxed), 0);
    assert_eq!(stats.snapshot(), DhtOldestNodePingProducerStats::default());
    delay_tx.send(()).unwrap();
    poll_once_pending(run.as_mut()).await;

    assert_eq!(*post_reserve.lock().unwrap(), [0, 1]);
    assert_eq!(now_calls.load(Ordering::Relaxed), 3);
    assert_eq!(
        stats.snapshot(),
        DhtOldestNodePingProducerStats {
            table_queries: 1,
            selected: 4,
            queued: 2,
            ..DhtOldestNodePingProducerStats::default()
        }
    );
    shutdown_tx.send(()).unwrap();
    assert_eq!(
        run.await,
        DhtOldestNodePingProducerExit::Shutdown {
            selected_dropped: 2,
        }
    );
    assert_eq!(receiver.recv().await, Some(expected_nodes[0]));
    assert_eq!(receiver.recv().await, Some(expected_nodes[1]));
    assert_eq!(receiver.recv().await, None);
    assert_eq!(
        stats.snapshot(),
        DhtOldestNodePingProducerStats {
            table_queries: 1,
            selected: 4,
            queued: 2,
            shutdown_dropped: 2,
            ..DhtOldestNodePingProducerStats::default()
        }
    );
}
