use std::collections::BTreeMap;
use std::future::{pending, poll_fn, ready, Future};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use super::*;
use crate::{DhtDiscoveredNodePingWorkerConfig, DhtDiscoveredNodeSchedulerConfig};

const FIXTURE_TEXT: &str =
    include_str!("../../../../testdata/parity/dht/dht_crawler_bootstrap_ping_producer.jsonl");
const FIXTURE_BYTES: &[u8] =
    include_bytes!("../../../../testdata/parity/dht/dht_crawler_bootstrap_ping_producer.jsonl");
const FIXTURE_SHA256: &str = "0616d53feb443d481d8d286d9c0d38ee14823b514c9075d0a4b367b938767cb4";
const FIXTURE_IDS: [&str; 4] = [
    "production_source_factory_defaults_and_lifecycle_contract",
    "ordered_numeric_ipv4_ipv6_delivery_then_cancel_before_second_round",
    "malformed_address_warns_and_continues_to_later_valid_address",
    "ordered_prefix_then_cancel_at_blocked_third_ping_send",
];
const ROW_CLASSIFICATIONS: [&str; 4] = [
    "SOURCE_ONLY",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
];

const GO_ONLY_METADATA: [&str; 9] = [
    "input.configuredReseedIntervalMs",
    "input.effectiveReseedIntervalMs on runtime rows",
    "expected.laneInCalls",
    "expected.deliveries[].timeIsZero",
    "expected.deliveries[].dropped",
    "expected.deliveries[].sampleInfohashesCandidate",
    "expected.warnings",
    "expected.events",
    "expected.runReturned and expected.contextCancelled",
];

const RUST_EXECUTION_PARTITION: [(&str, &str); 4] = [
    (FIXTURE_IDS[0], "SOURCE_ONLY_NO_RUST_RUNTIME_REPLAY"),
    (
        FIXTURE_IDS[1],
        "ACTUAL_RUST_NUMERIC_RESOLVER_REPLAY_WITH_MAPPED_IPV4_CANONICALIZATION",
    ),
    (
        FIXTURE_IDS[2],
        "ACTUAL_RUST_LOCALLY_REJECTED_ADDRESS_CONTINUATION_REPLAY_WITH_FAILURE_COUNTER",
    ),
    (
        FIXTURE_IDS[3],
        "RUST_READY_NUMERIC_RESOLVER_BLOCKED_THIRD_RESERVATION_PREFIX_SUFFIX_REPLAY",
    ),
];

const DELIBERATE_RUST_DELTAS: [&str; 11] = [
    "ready_shutdown_then_input_close_are_biased_ahead_of_the_immediate_first_round",
    "async_producer_level_cancellation_replaces_the_blocking_Go_resolver_without_claiming_OS_lookup_cancellation",
    "Go_address_family_selection_is_replayed_over_resolver_order_without_claiming_identical_DNS_answers",
    "native_and_mapped_IPv4_are_canonicalized_to_SocketAddr_V4_while_native_IPv6_is_retained",
    "the_public_constructor_currently_uses_fixed_defaults_while_Go_configured_bootstrap_nodes_remain_deferred_application_wiring",
    "the_fixed_fresh_600_second_delay_uses_the_effective_Go_factory_value_and_excludes_runtime_harness_overrides",
    "resolution_failures_are_counted_and_continue_without_freezing_Go_warning_text",
    "positive_Tokio_capacity_typed_InputClosed_and_irrevocable_owned_permits_replace_raw_Go_channel_lifecycle",
    "the_owned_taskless_run_future_and_typed_terminal_exits_replace_the_detached_unjoined_Go_producer",
    "immutable_RoutingNode_zero_ID_projection_has_no_Go_Time_Dropped_or_sample_candidate_state",
    "seven_saturating_component_local_counters_conserve_every_selected_occurrence_after_normal_exit",
];

const RUST_HARDENING_EVIDENCE: [(&str, &str); 11] = [
    (
        DELIBERATE_RUST_DELTAS[0],
        "ready_shutdown_wins_preclosed_input_before_starting_a_round; preclosed_input_exits_before_starting_a_round",
    ),
    (
        DELIBERATE_RUST_DELTAS[1],
        "cancelling_pending_resolution_drops_the_local_future_and_accounts_suffix",
    ),
    (
        DELIBERATE_RUST_DELTAS[2],
        "go_udp_family_preference_and_ipv4_canonicalization_are_exact",
    ),
    (
        DELIBERATE_RUST_DELTAS[3],
        "go_udp_family_preference_and_ipv4_canonicalization_are_exact; native_ipv6_zone_is_retained_and_mapped_ipv4_zone_is_discarded",
    ),
    (
        DELIBERATE_RUST_DELTAS[4],
        "constants_defaults_and_public_handles_are_sound",
    ),
    (
        DELIBERATE_RUST_DELTAS[5],
        "first_round_is_immediate_and_later_round_uses_a_fresh_ten_minute_delay; delayed_poll_does_not_catch_up_missed_rounds",
    ),
    (
        DELIBERATE_RUST_DELTAS[6],
        "resolution_is_sequential_preserves_occurrences_and_continues_after_failures",
    ),
    (
        DELIBERATE_RUST_DELTAS[7],
        "ready_shutdown_beats_ready_capacity_after_resolution; close_after_reserve_keeps_commit_authority_then_drops_later_suffix",
    ),
    (
        DELIBERATE_RUST_DELTAS[8],
        "constructing_without_running_spawns_nothing_and_drop_releases_route_eof; dropping_polled_run_releases_sender_without_terminal_classification",
    ),
    (
        DELIBERATE_RUST_DELTAS[9],
        "constants_defaults_and_public_handles_are_sound; native_ipv6_zone_is_retained_and_mapped_ipv4_zone_is_discarded",
    ),
    (
        DELIBERATE_RUST_DELTAS[10],
        "counters_and_terminal_classification_saturate; shutdown_while_capacity_is_blocked_preserves_prefix_and_drops_suffix",
    ),
];

const GO_SOURCES: [(&str, &[u8], &str); 7] = [
    (
        "internal/concurrency/buffered_concurrent_channel.go",
        include_bytes!("../../../../internal/concurrency/buffered_concurrent_channel.go"),
        "4be882800ec66d0c1709319fe029d61773c3f4a37bdb409e3a2f7d5d415d954c",
    ),
    (
        "internal/dhtcrawler/bootstrap.go",
        include_bytes!("../../../../internal/dhtcrawler/bootstrap.go"),
        "43c7f2d8bfb12b530c68a82dab270294cc500cc14ee64b459ad5db60b170a2a4",
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
    resolver: String,
    lane: String,
    timer: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Input {
    kind: String,
    context_initially_cancelled: bool,
    initial_interval_ms: u64,
    configured_reseed_interval_ms: u64,
    effective_reseed_interval_ms: u64,
    bootstrap_nodes: Vec<String>,
    lane_capacity: usize,
    cancel_at_lane_in_call: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Delivery {
    configured: String,
    id: String,
    addr: String,
    time_is_zero: bool,
    dropped: bool,
    sample_infohashes_candidate: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Expected {
    lane_in_calls: usize,
    deliveries: Vec<Delivery>,
    resolution_skipped: Vec<String>,
    abandoned: Vec<String>,
    warnings: Vec<String>,
    events: Vec<String>,
    run_returned: bool,
    context_cancelled: bool,
    source: Option<Source>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Source {
    initial_interval_ms: u64,
    initial_timer_cancellation_aware: bool,
    ready_initial_timer_cancel_outcome: String,
    resolves_sequentially: bool,
    resolver_network: String,
    resolution_cancellation_aware: bool,
    resolution_error_warns_and_continues: bool,
    resolves_one_address_per_configured_entry: bool,
    new_node_uses_zero_id: bool,
    new_node_default_time_is_zero: bool,
    new_node_default_dropped: bool,
    new_node_default_sample_candidate: bool,
    preserves_configured_order: bool,
    per_node_send_cancellation_aware: bool,
    ready_send_cancel_outcome: String,
    fresh_delay_after_round: bool,
    effective_reseed_interval_seconds: u64,
    config_default_reseed_interval_seconds: u64,
    factory_ignores_configured_reseed_interval: bool,
    factory_uses_configured_bootstrap_nodes: bool,
    default_bootstrap_nodes: Vec<String>,
    default_scaling_factor: usize,
    production_capacity: usize,
    production_concurrency: usize,
    lane_shared_with_run_ping: bool,
    producer_detached: bool,
    producer_joined: bool,
    runtime_avoids_public_dns: bool,
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

fn delivery(configured: &str, addr: &str) -> Delivery {
    Delivery {
        configured: configured.into(),
        id: "0000000000000000000000000000000000000000".into(),
        addr: addr.into(),
        time_is_zero: true,
        dropped: false,
        sample_infohashes_candidate: true,
    }
}

fn routing_node(addr: &str) -> RoutingNode {
    RoutingNode {
        id: Id20::ZERO,
        addr: addr
            .parse()
            .unwrap_or_else(|error| panic!("invalid hardcoded Rust address {addr}: {error}")),
    }
}

fn assert_conservation(stats: DhtBootstrapPingProducerStats) {
    assert_eq!(
        stats.selected,
        stats
            .resolution_failed
            .saturating_add(stats.queued)
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

#[test]
fn fixture_schema_identity_sources_metadata_and_partition_are_frozen() {
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
        .all(|fixture| fixture.subsystem == "dht_crawler_bootstrap_ping_producer"));
    assert_source_row(&fixtures[0]);
    assert_ordered_numeric_row(&fixtures[1]);
    assert_invalid_continues_row(&fixtures[2]);
    assert_ordered_prefix_row(&fixtures[3]);

    assert_eq!(
        GO_ONLY_METADATA,
        [
            "input.configuredReseedIntervalMs",
            "input.effectiveReseedIntervalMs on runtime rows",
            "expected.laneInCalls",
            "expected.deliveries[].timeIsZero",
            "expected.deliveries[].dropped",
            "expected.deliveries[].sampleInfohashesCandidate",
            "expected.warnings",
            "expected.events",
            "expected.runReturned and expected.contextCancelled",
        ]
    );
    assert_eq!(
        RUST_EXECUTION_PARTITION,
        [
            (FIXTURE_IDS[0], "SOURCE_ONLY_NO_RUST_RUNTIME_REPLAY"),
            (
                FIXTURE_IDS[1],
                "ACTUAL_RUST_NUMERIC_RESOLVER_REPLAY_WITH_MAPPED_IPV4_CANONICALIZATION",
            ),
            (
                FIXTURE_IDS[2],
                "ACTUAL_RUST_LOCALLY_REJECTED_ADDRESS_CONTINUATION_REPLAY_WITH_FAILURE_COUNTER",
            ),
            (
                FIXTURE_IDS[3],
                "RUST_READY_NUMERIC_RESOLVER_BLOCKED_THIRD_RESERVATION_PREFIX_SUFFIX_REPLAY",
            ),
        ]
    );
    assert_eq!(
        DELIBERATE_RUST_DELTAS,
        [
            "ready_shutdown_then_input_close_are_biased_ahead_of_the_immediate_first_round",
            "async_producer_level_cancellation_replaces_the_blocking_Go_resolver_without_claiming_OS_lookup_cancellation",
            "Go_address_family_selection_is_replayed_over_resolver_order_without_claiming_identical_DNS_answers",
            "native_and_mapped_IPv4_are_canonicalized_to_SocketAddr_V4_while_native_IPv6_is_retained",
            "the_public_constructor_currently_uses_fixed_defaults_while_Go_configured_bootstrap_nodes_remain_deferred_application_wiring",
            "the_fixed_fresh_600_second_delay_uses_the_effective_Go_factory_value_and_excludes_runtime_harness_overrides",
            "resolution_failures_are_counted_and_continue_without_freezing_Go_warning_text",
            "positive_Tokio_capacity_typed_InputClosed_and_irrevocable_owned_permits_replace_raw_Go_channel_lifecycle",
            "the_owned_taskless_run_future_and_typed_terminal_exits_replace_the_detached_unjoined_Go_producer",
            "immutable_RoutingNode_zero_ID_projection_has_no_Go_Time_Dropped_or_sample_candidate_state",
            "seven_saturating_component_local_counters_conserve_every_selected_occurrence_after_normal_exit",
        ]
    );
    assert_eq!(
        RUST_HARDENING_EVIDENCE,
        [
            (
                DELIBERATE_RUST_DELTAS[0],
                "ready_shutdown_wins_preclosed_input_before_starting_a_round; preclosed_input_exits_before_starting_a_round",
            ),
            (
                DELIBERATE_RUST_DELTAS[1],
                "cancelling_pending_resolution_drops_the_local_future_and_accounts_suffix",
            ),
            (
                DELIBERATE_RUST_DELTAS[2],
                "go_udp_family_preference_and_ipv4_canonicalization_are_exact",
            ),
            (
                DELIBERATE_RUST_DELTAS[3],
                "go_udp_family_preference_and_ipv4_canonicalization_are_exact; native_ipv6_zone_is_retained_and_mapped_ipv4_zone_is_discarded",
            ),
            (
                DELIBERATE_RUST_DELTAS[4],
                "constants_defaults_and_public_handles_are_sound",
            ),
            (
                DELIBERATE_RUST_DELTAS[5],
                "first_round_is_immediate_and_later_round_uses_a_fresh_ten_minute_delay; delayed_poll_does_not_catch_up_missed_rounds",
            ),
            (
                DELIBERATE_RUST_DELTAS[6],
                "resolution_is_sequential_preserves_occurrences_and_continues_after_failures",
            ),
            (
                DELIBERATE_RUST_DELTAS[7],
                "ready_shutdown_beats_ready_capacity_after_resolution; close_after_reserve_keeps_commit_authority_then_drops_later_suffix",
            ),
            (
                DELIBERATE_RUST_DELTAS[8],
                "constructing_without_running_spawns_nothing_and_drop_releases_route_eof; dropping_polled_run_releases_sender_without_terminal_classification",
            ),
            (
                DELIBERATE_RUST_DELTAS[9],
                "constants_defaults_and_public_handles_are_sound; native_ipv6_zone_is_retained_and_mapped_ipv4_zone_is_discarded",
            ),
            (
                DELIBERATE_RUST_DELTAS[10],
                "counters_and_terminal_classification_saturate; shutdown_while_capacity_is_blocked_preserves_prefix_and_drops_suffix",
            ),
        ]
    );
}

fn assert_source_row(fixture: &Fixture) {
    assert_eq!(
        fixture.oracle.composition,
        "exact_production_source_factory_defaults_and_lifecycle_shapes"
    );
    assert_eq!(
        fixture.oracle.determinism,
        "normalized_ast_and_whole_source_sha256"
    );
    assert_eq!(
        fixture.oracle.resolver,
        "production_net_ResolveUDPAddr_single_result_interface"
    );
    assert_eq!(
        fixture.oracle.lane,
        "production_buffered_concurrent_channel_shared_with_runPing"
    );
    assert_eq!(
        fixture.oracle.timer,
        "exact_source_zero_then_effective_factory_interval_time_After_shapes"
    );
    assert_eq!(fixture.input.kind, "source_contract");
    assert!(!fixture.input.context_initially_cancelled);
    assert_eq!(fixture.input.initial_interval_ms, 0);
    assert_eq!(fixture.input.configured_reseed_interval_ms, 60_000);
    assert_eq!(fixture.input.effective_reseed_interval_ms, 600_000);
    assert_eq!(
        fixture.input.bootstrap_nodes,
        DEFAULT_BOOTSTRAP_NODES.map(String::from)
    );
    assert_eq!(fixture.input.lane_capacity, 10);
    assert_eq!(fixture.input.cancel_at_lane_in_call, 0);
    assert!(fixture.expected.deliveries.is_empty());
    assert!(fixture.expected.resolution_skipped.is_empty());
    assert!(fixture.expected.abandoned.is_empty());
    assert!(fixture.expected.warnings.is_empty());
    assert!(fixture.expected.events.is_empty());
    assert_eq!(fixture.expected.lane_in_calls, 0);
    assert!(!fixture.expected.run_returned);
    assert!(!fixture.expected.context_cancelled);

    let source = fixture
        .expected
        .source
        .as_ref()
        .expect("source row has source facts");
    assert_eq!(source.initial_interval_ms, 0);
    assert!(source.initial_timer_cancellation_aware);
    assert_eq!(
        source.ready_initial_timer_cancel_outcome,
        "go_select_chooses_nondeterministically_when_zero_timer_and_cancel_are_both_ready"
    );
    assert!(source.resolves_sequentially);
    assert_eq!(source.resolver_network, "udp");
    assert!(!source.resolution_cancellation_aware);
    assert!(source.resolution_error_warns_and_continues);
    assert!(source.resolves_one_address_per_configured_entry);
    assert!(source.new_node_uses_zero_id);
    assert!(source.new_node_default_time_is_zero);
    assert!(!source.new_node_default_dropped);
    assert!(source.new_node_default_sample_candidate);
    assert!(source.preserves_configured_order);
    assert!(source.per_node_send_cancellation_aware);
    assert_eq!(
        source.ready_send_cancel_outcome,
        "go_select_chooses_nondeterministically_when_both_are_ready"
    );
    assert!(source.fresh_delay_after_round);
    assert_eq!(source.effective_reseed_interval_seconds, 600);
    assert_eq!(source.config_default_reseed_interval_seconds, 60);
    assert!(source.factory_ignores_configured_reseed_interval);
    assert!(source.factory_uses_configured_bootstrap_nodes);
    assert_eq!(
        source.default_bootstrap_nodes,
        DEFAULT_BOOTSTRAP_NODES.map(String::from)
    );
    assert_eq!(source.default_scaling_factor, 10);
    assert_eq!(source.production_capacity, 10);
    assert_eq!(source.production_concurrency, 10);
    assert!(source.lane_shared_with_run_ping);
    assert!(source.producer_detached);
    assert!(!source.producer_joined);
    assert!(source.runtime_avoids_public_dns);
    assert!(!source.factory_timer_runtime_observed);
    assert_eq!(
        source.evidence,
        "actual method rows use numeric literals plus one locally rejected malformed literal and return before the effective ten-minute delay; public DNS, the factory timer, synchronous resolver cancellation, and equal-ready Go select outcomes remain source evidence"
    );
    assert_eq!(source.source_sha256.len(), GO_SOURCES.len());
    for (path, bytes, digest) in GO_SOURCES {
        assert_eq!(sha256(bytes), digest, "Go source drifted for {path}");
        assert_eq!(
            source.source_sha256.get(path).map(String::as_str),
            Some(digest)
        );
    }

    assert_eq!(RESEED_DELAY, Duration::from_secs(600));
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
    let (input, _receiver) = DhtDiscoveredNodePingInput::test_channel(1);
    let (producer, _stats) = DhtBootstrapPingProducer::new(input);
    assert_eq!(
        producer.bootstrap_nodes.as_ref(),
        source.default_bootstrap_nodes.as_slice()
    );
}

fn assert_ordered_numeric_row(fixture: &Fixture) {
    assert_runtime_oracle(
        fixture,
        "numeric_ipv4_ipv6_resolution_and_controller_cancellation_after_committed_round",
    );
    assert_runtime_input(fixture, 2, 0);
    assert_eq!(
        fixture.input.bootstrap_nodes,
        ["192.0.2.10:6881", "[2001:db8::10]:6882"]
    );
    assert_eq!(fixture.expected.lane_in_calls, 2);
    assert_eq!(
        fixture.expected.deliveries,
        [
            delivery("192.0.2.10:6881", "[::ffff:192.0.2.10]:6881"),
            delivery("[2001:db8::10]:6882", "[2001:db8::10]:6882"),
        ]
    );
    assert!(fixture.expected.resolution_skipped.is_empty());
    assert!(fixture.expected.abandoned.is_empty());
    assert!(fixture.expected.warnings.is_empty());
    assert_eq!(
        fixture.expected.events,
        [
            "run_start",
            "lane_in:1",
            "lane_in:2",
            "cancel_before_second_round",
            "return",
        ]
    );
    assert_runtime_return(fixture);
}

fn assert_invalid_continues_row(fixture: &Fixture) {
    assert_runtime_oracle(
        fixture,
        "locally_rejected_malformed_literal_warning_then_later_numeric_delivery",
    );
    assert_runtime_input(fixture, 1, 0);
    assert_eq!(
        fixture.input.bootstrap_nodes,
        ["not-an-address", "192.0.2.11:6883"]
    );
    assert_eq!(fixture.expected.lane_in_calls, 1);
    assert_eq!(
        fixture.expected.deliveries,
        [delivery("192.0.2.11:6883", "[::ffff:192.0.2.11]:6883")]
    );
    assert_eq!(fixture.expected.resolution_skipped, ["not-an-address"]);
    assert!(fixture.expected.abandoned.is_empty());
    assert_eq!(
        fixture.expected.warnings,
        ["failed_to_resolve_bootstrap_node_address"]
    );
    assert_eq!(
        fixture.expected.events,
        [
            "run_start",
            "lane_in:1",
            "cancel_before_second_round",
            "return",
        ]
    );
    assert_runtime_return(fixture);
}

fn assert_ordered_prefix_row(fixture: &Fixture) {
    assert_runtime_oracle(
        fixture,
        "capacity_two_ping_lane_with_third_In_gate_and_numeric_resolution",
    );
    assert_runtime_input(fixture, 2, 3);
    assert_eq!(
        fixture.input.bootstrap_nodes,
        [
            "192.0.2.21:6891",
            "192.0.2.22:6892",
            "192.0.2.23:6893",
            "192.0.2.24:6894",
        ]
    );
    assert_eq!(fixture.expected.lane_in_calls, 3);
    assert_eq!(
        fixture.expected.deliveries,
        [
            delivery("192.0.2.21:6891", "[::ffff:192.0.2.21]:6891"),
            delivery("192.0.2.22:6892", "[::ffff:192.0.2.22]:6892"),
        ]
    );
    assert!(fixture.expected.resolution_skipped.is_empty());
    assert_eq!(
        fixture.expected.abandoned,
        ["192.0.2.23:6893", "192.0.2.24:6894"]
    );
    assert!(fixture.expected.warnings.is_empty());
    assert_eq!(
        fixture.expected.events,
        [
            "run_start",
            "lane_in:1",
            "lane_in:2",
            "lane_in:3",
            "cancel",
            "return",
        ]
    );
    assert_runtime_return(fixture);
}

fn assert_runtime_oracle(fixture: &Fixture, determinism: &str) {
    assert_eq!(
        fixture.oracle.composition,
        "actual_crawler_reseedBootstrapNodes_with_production_numeric_resolver_observer_logger_and_manual_lane"
    );
    assert_eq!(fixture.oracle.determinism, determinism);
    assert_eq!(
        fixture.oracle.resolver,
        "production_net_ResolveUDPAddr_with_numeric_or_locally_rejected_literals_only"
    );
    assert_eq!(
        fixture.oracle.lane,
        "manual_capacity_controlled_lane_implementing_BufferedConcurrentChannel_contract"
    );
    assert_eq!(
        fixture.oracle.timer,
        "production_initial_zero_time_After_then_cancel_before_positive_reseed_timer"
    );
}

fn assert_runtime_input(fixture: &Fixture, lane_capacity: usize, cancel_at: usize) {
    assert_eq!(fixture.input.kind, "actual_reseedBootstrapNodes");
    assert!(!fixture.input.context_initially_cancelled);
    assert_eq!(fixture.input.initial_interval_ms, 0);
    assert_eq!(fixture.input.configured_reseed_interval_ms, 0);
    assert_eq!(fixture.input.effective_reseed_interval_ms, 3_600_000);
    assert_eq!(fixture.input.lane_capacity, lane_capacity);
    assert_eq!(fixture.input.cancel_at_lane_in_call, cancel_at);
    assert!(fixture.expected.source.is_none());
}

fn assert_runtime_return(fixture: &Fixture) {
    assert!(fixture.expected.run_returned);
    assert!(fixture.expected.context_cancelled);
    assert!(fixture.expected.source.is_none());
}

#[tokio::test]
async fn ordered_numeric_row_replays_on_actual_rust_resolver_with_explicit_address_delta() {
    let fixtures = fixtures();
    let fixture = &fixtures[1];
    assert_ordered_numeric_row(fixture);

    let expected = [
        routing_node("192.0.2.10:6881"),
        routing_node("[2001:db8::10]:6882"),
    ];
    assert_eq!(
        fixture.expected.deliveries[0].addr,
        "[::ffff:192.0.2.10]:6881"
    );
    assert!(matches!(expected[0].addr, SocketAddr::V4(_)));
    assert!(matches!(expected[1].addr, SocketAddr::V6(_)));

    let (input, mut receiver) =
        DhtDiscoveredNodePingInput::test_channel(fixture.input.lane_capacity);
    let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(
        input,
        fixture.input.bootstrap_nodes.clone(),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let controller = async {
        let first = receiver.recv().await.expect("first bootstrap ping queued");
        let second = receiver.recv().await.expect("second bootstrap ping queued");
        shutdown_tx.send(()).expect("producer is still running");
        [first, second]
    };
    let shutdown = async move {
        let _ = shutdown_rx.await;
    };
    let (exit, actual) = tokio::join!(producer.run(shutdown), controller);

    assert_eq!(actual, expected);
    assert_eq!(
        exit,
        DhtBootstrapPingProducerExit::Shutdown {
            selected_dropped: 0
        }
    );
    assert_eq!(receiver.recv().await, None);
    assert_eq!(
        stats.snapshot(),
        DhtBootstrapPingProducerStats {
            rounds_started: 1,
            selected: 2,
            resolution_attempts: 2,
            queued: 2,
            ..DhtBootstrapPingProducerStats::default()
        }
    );
    assert_conservation(stats.snapshot());
}

#[tokio::test]
async fn invalid_continuation_row_replays_on_actual_rust_resolver_with_failure_counter_delta() {
    let fixtures = fixtures();
    let fixture = &fixtures[2];
    assert_invalid_continues_row(fixture);

    let expected = routing_node("192.0.2.11:6883");
    let (input, mut receiver) =
        DhtDiscoveredNodePingInput::test_channel(fixture.input.lane_capacity);
    let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(
        input,
        fixture.input.bootstrap_nodes.clone(),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let controller = async {
        let actual = receiver
            .recv()
            .await
            .expect("valid bootstrap ping after malformed address");
        shutdown_tx.send(()).expect("producer is still running");
        actual
    };
    let shutdown = async move {
        let _ = shutdown_rx.await;
    };
    let (exit, actual) = tokio::join!(producer.run(shutdown), controller);

    assert_eq!(actual, expected);
    assert_eq!(
        exit,
        DhtBootstrapPingProducerExit::Shutdown {
            selected_dropped: 0
        }
    );
    assert_eq!(receiver.recv().await, None);
    assert_eq!(
        stats.snapshot(),
        DhtBootstrapPingProducerStats {
            rounds_started: 1,
            selected: 2,
            resolution_attempts: 2,
            resolution_failed: 1,
            queued: 1,
            ..DhtBootstrapPingProducerStats::default()
        }
    );
    assert_conservation(stats.snapshot());
}

#[tokio::test]
async fn ordered_prefix_row_proves_blocked_third_reservation_before_shutdown() {
    let fixtures = fixtures();
    let fixture = &fixtures[3];
    assert_ordered_prefix_row(fixture);

    let (input, mut receiver) =
        DhtDiscoveredNodePingInput::test_channel(fixture.input.lane_capacity);
    let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(
        input,
        fixture.input.bootstrap_nodes.clone(),
    );
    let resolver_calls = Arc::new(Mutex::new(Vec::new()));
    let resolver_calls_for_run = Arc::clone(&resolver_calls);
    let after_reserve = Arc::new(Mutex::new(Vec::new()));
    let after_reserve_for_run = Arc::clone(&after_reserve);
    let delay_calls = Arc::new(Mutex::new(Vec::new()));
    let delay_calls_for_run = Arc::clone(&delay_calls);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let run = producer.run_with(
        async move {
            let _ = shutdown_rx.await;
        },
        move |configured| {
            resolver_calls_for_run
                .lock()
                .unwrap()
                .push(configured.clone());
            ready(
                configured
                    .parse::<SocketAddr>()
                    .map(|address| vec![address]),
            )
        },
        move |duration| {
            delay_calls_for_run.lock().unwrap().push(duration);
            pending::<()>()
        },
        move |index, configured, address| {
            after_reserve_for_run
                .lock()
                .unwrap()
                .push((index, configured.to_owned(), address));
        },
    );
    tokio::pin!(run);

    poll_once_pending(run.as_mut()).await;
    assert_eq!(
        *resolver_calls.lock().unwrap(),
        ["192.0.2.21:6891", "192.0.2.22:6892", "192.0.2.23:6893",]
    );
    assert_eq!(
        *after_reserve.lock().unwrap(),
        [
            (
                0,
                "192.0.2.21:6891".into(),
                "192.0.2.21:6891".parse().unwrap()
            ),
            (
                1,
                "192.0.2.22:6892".into(),
                "192.0.2.22:6892".parse().unwrap()
            ),
        ]
    );
    assert!(delay_calls.lock().unwrap().is_empty());
    assert_eq!(
        stats.snapshot(),
        DhtBootstrapPingProducerStats {
            rounds_started: 1,
            selected: 4,
            resolution_attempts: 3,
            queued: 2,
            ..DhtBootstrapPingProducerStats::default()
        }
    );

    shutdown_tx
        .send(())
        .expect("blocked producer is still running");
    assert_eq!(
        run.await,
        DhtBootstrapPingProducerExit::Shutdown {
            selected_dropped: 2
        }
    );
    assert_eq!(receiver.recv().await, Some(routing_node("192.0.2.21:6891")));
    assert_eq!(receiver.recv().await, Some(routing_node("192.0.2.22:6892")));
    assert_eq!(receiver.recv().await, None);
    assert_eq!(
        stats.snapshot(),
        DhtBootstrapPingProducerStats {
            rounds_started: 1,
            selected: 4,
            resolution_attempts: 3,
            queued: 2,
            shutdown_dropped: 2,
            ..DhtBootstrapPingProducerStats::default()
        }
    );
    assert_conservation(stats.snapshot());
}
