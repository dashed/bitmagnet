use std::collections::{BTreeMap, VecDeque};
use std::future::{pending, poll_fn, ready};
use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use super::*;
use crate::{dht_discovery_channel, DhtDiscoveryReceiver, DhtDiscoveryStatsHandle};

const FIXTURE_TEXT: &str =
    include_str!("../../../../testdata/parity/dht/dht_crawler_find_node_worker.jsonl");
const FIXTURE_BYTES: &[u8] =
    include_bytes!("../../../../testdata/parity/dht/dht_crawler_find_node_worker.jsonl");
const FIXTURE_SHA256: &str = "e126ad26fd342b14ae0416b3610d991f927dbe9381ac11609ebeba96d67870b7";
const FIXTURE_IDS: [&str; 8] = [
    "production_factory_producer_and_source_contract",
    "find_error_drops_advertised_id",
    "success_ignores_responder_id_and_marks_advertised_node_responded",
    "success_forwards_response_nodes_in_order_after_put",
    "cancelled_after_success_still_puts_then_abandons_blocked_discovery",
    "cancel_after_one_discovery_abandons_blocked_suffix",
    "sought_target_is_read_for_each_callback",
    "lane_error_is_swallowed",
];
const ROW_CLASSIFICATIONS: [(&str, &str); 8] = [
    (FIXTURE_IDS[0], "SOURCE_ONLY"),
    (FIXTURE_IDS[1], "RUNTIME_EXACT"),
    (FIXTURE_IDS[2], "RUNTIME_WITH_IMMUTABLE_ADDR_DELTA"),
    (FIXTURE_IDS[3], "RUNTIME_EXACT"),
    (FIXTURE_IDS[4], "RUNTIME_WITH_SHUTDOWN_BACKPRESSURE_DELTA"),
    (FIXTURE_IDS[5], "RUNTIME_WITH_SHUTDOWN_BACKPRESSURE_DELTA"),
    (FIXTURE_IDS[6], "RUNTIME_EXACT"),
    (FIXTURE_IDS[7], "GO_ONLY_LANE"),
];
const GO_ONLY_METADATA: [&str; 10] = [
    "input.nodes[].addrReturns",
    "input.discoveryMode=unbuffered and discoveryCapacity=0",
    "expected.nodeCalls",
    "expected.sameContext",
    "expected.batchCalls",
    "expected.commands[].optionCount",
    "expected.commands[].reason and errorIdentityPreserved",
    "expected.discoveries[].freshState",
    "input.laneReturnError",
    "error outcome responseId and nodes cannot inhabit Rust Err",
];
const DELIBERATE_RUST_DELTAS: [&str; 6] = [
    "state-free RoutingNode keeps the query and Put address immutable",
    "the controller snapshots the shared target once immediately before query-future construction",
    "query and recursive fanout remain one owned task joined on EOF and aborted on shutdown",
    "Tokio discovery uses positive capacity plus explicit prefill gates instead of an unbuffered channel",
    "Go retains 100 active + 100 queued + 1 acquire waiter; Rust gates before dequeue and retains 100 active + 100 queued with no hidden waiter",
    "Rust returns typed InputClosed on EOF; Go closed input can repeat nil receives and panic, while the Go lane error is swallowed",
];

const GO_SOURCES: [(&str, &[u8], &str); 13] = [
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
        "internal/dhtcrawler/find_node.go",
        include_bytes!("../../../../internal/dhtcrawler/find_node.go"),
        "cd5fab8aa078ad40ed82331dbbfd141a38badc018287dd13211d221b230087bb",
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
    oracle: Oracle,
    input: Input,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Oracle {
    composition: String,
    determinism: String,
    lane: String,
    client: String,
    table: String,
    discovery: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Input {
    kind: String,
    nodes: Option<Vec<InputNode>>,
    outcomes: Option<Vec<Outcome>>,
    initial_target: Option<String>,
    targets_before_callback: Option<Vec<String>>,
    discovery_mode: Option<String>,
    discovery_capacity: usize,
    cancel_before_return: Option<bool>,
    cancel_after_deliveries: Option<usize>,
    lane_return_error: Option<bool>,
    table_setup: Option<Vec<TableSetup>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InputNode {
    id: String,
    addr: FixtureAddress,
    addr_returns: Vec<FixtureAddress>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FixtureAddress {
    ip: String,
    port: u16,
    scope: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct FreshState {
    time_zero: bool,
    dropped: bool,
    sample_infohashes_candidate: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Outcome {
    kind: String,
    response_id: String,
    nodes: Vec<OutcomeNode>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct OutcomeNode {
    id: String,
    addr: FixtureAddress,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableSetup {
    id: String,
    addr: FixtureAddress,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Expected {
    node_calls: Vec<NodeCalls>,
    find_calls: Vec<FindCall>,
    same_context: bool,
    batch_calls: usize,
    commands: Vec<Command>,
    discoveries: Vec<DiscoveryNode>,
    events: Vec<String>,
    advertised_post: Vec<TablePost>,
    response_id_post: Vec<TablePost>,
    run_returned: bool,
    context_cancelled: bool,
    source: Option<Source>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct NodeCalls {
    id: usize,
    addr: usize,
    time: usize,
    dropped: usize,
    sample_infohashes_candidate: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FindCall {
    addr: FixtureAddress,
    target: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Command {
    kind: String,
    id: String,
    addr: Option<FixtureAddress>,
    option_count: usize,
    reason: String,
    error_identity_preserved: bool,
    stored_responded: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TablePost {
    id: String,
    present: bool,
    addr: Option<FixtureAddress>,
    responded: bool,
    retained_dropped: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DiscoveryNode {
    id: String,
    addr: FixtureAddress,
    fresh_state: FreshState,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Source {
    run_error_ignored: bool,
    shared_callback_context: bool,
    no_eligibility_recheck: bool,
    target_read_at_each_client_call: bool,
    response_id_ignored: bool,
    error_drops_advertised_id: bool,
    success_uses_node_responded_option: bool,
    no_post_query_cancellation_before_put: bool,
    put_precedes_recursive_discovery: bool,
    recursive_discovery_blocks_in_order: bool,
    recursive_discovery_cancel_aware: bool,
    production_capacity: usize,
    production_concurrency: usize,
    run_dequeues_before_acquire: bool,
    run_spawns_callbacks: bool,
    run_joins_callbacks: bool,
    generic_closed_input_repeats_receive: bool,
    closed_input_checks_open_boolean: bool,
    closed_input_find_node_outcome: String,
    maximum_retained_work: String,
    default_scaling_factor: usize,
    discovery_input_capacity: usize,
    discovery_max_batch_size: usize,
    discovery_batch_interval_ms: u64,
    discovery_output_capacity: usize,
    producer_initial_query_before_delay: bool,
    producer_cutoff_seconds: u64,
    producer_limit: usize,
    producer_interval_ms: u64,
    producer_sleep_cancellation_aware: bool,
    empty_table_cancellation_outcome: String,
    producer_evidence_scope: String,
    sought_id_rotation_seconds: u64,
    sought_id_initialized_before_start: bool,
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

fn fixture_id(value: &str) -> Id20 {
    Id20::from_hex(value).unwrap_or_else(|error| panic!("invalid fixture ID {value}: {error}"))
}

fn socket_addr(value: &FixtureAddress) -> SocketAddr {
    let ip = value
        .ip
        .parse::<IpAddr>()
        .unwrap_or_else(|error| panic!("invalid fixture IP {}: {error}", value.ip));
    match ip {
        IpAddr::V4(ip) => {
            assert_eq!(value.scope, 0, "IPv4 fixture address cannot carry a scope");
            SocketAddr::V4(SocketAddrV4::new(ip, value.port))
        }
        IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(ip, value.port, 0, value.scope)),
    }
}

fn routing_input_node(value: &InputNode) -> RoutingNode {
    RoutingNode {
        id: fixture_id(&value.id),
        addr: socket_addr(&value.addr),
    }
}

fn routing_outcome_node(value: &OutcomeNode) -> RoutingNode {
    RoutingNode {
        id: fixture_id(&value.id),
        addr: socket_addr(&value.addr),
    }
}

#[test]
fn fixture_schema_identity_sources_classifications_and_metadata_are_frozen() {
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
        ROW_CLASSIFICATIONS,
        [
            (FIXTURE_IDS[0], "SOURCE_ONLY"),
            (FIXTURE_IDS[1], "RUNTIME_EXACT"),
            (FIXTURE_IDS[2], "RUNTIME_WITH_IMMUTABLE_ADDR_DELTA"),
            (FIXTURE_IDS[3], "RUNTIME_EXACT"),
            (FIXTURE_IDS[4], "RUNTIME_WITH_SHUTDOWN_BACKPRESSURE_DELTA"),
            (FIXTURE_IDS[5], "RUNTIME_WITH_SHUTDOWN_BACKPRESSURE_DELTA"),
            (FIXTURE_IDS[6], "RUNTIME_EXACT"),
            (FIXTURE_IDS[7], "GO_ONLY_LANE"),
        ]
    );
    assert_eq!(
        GO_ONLY_METADATA,
        [
            "input.nodes[].addrReturns",
            "input.discoveryMode=unbuffered and discoveryCapacity=0",
            "expected.nodeCalls",
            "expected.sameContext",
            "expected.batchCalls",
            "expected.commands[].optionCount",
            "expected.commands[].reason and errorIdentityPreserved",
            "expected.discoveries[].freshState",
            "input.laneReturnError",
            "error outcome responseId and nodes cannot inhabit Rust Err",
        ]
    );
    assert_eq!(
        DELIBERATE_RUST_DELTAS,
        [
            "state-free RoutingNode keeps the query and Put address immutable",
            "the controller snapshots the shared target once immediately before query-future construction",
            "query and recursive fanout remain one owned task joined on EOF and aborted on shutdown",
            "Tokio discovery uses positive capacity plus explicit prefill gates instead of an unbuffered channel",
            "Go retains 100 active + 100 queued + 1 acquire waiter; Rust gates before dequeue and retains 100 active + 100 queued with no hidden waiter",
            "Rust returns typed InputClosed on EOF; Go closed input can repeat nil receives and panic, while the Go lane error is swallowed",
        ]
    );

    for fixture in &fixtures {
        assert_eq!(fixture.subsystem, "dht_crawler_find_node");
        assert_oracle(fixture);
    }
    assert_source_fixture(&fixtures[0]);
    for fixture in &fixtures[1..7] {
        assert_runtime_metadata(fixture);
    }
    assert_lane_fixture(&fixtures[7]);
}

fn assert_oracle(fixture: &Fixture) {
    let oracle = &fixture.oracle;
    if fixture.id == FIXTURE_IDS[0] {
        assert_eq!(
            oracle.composition,
            "source_factory_and_producer_freshness_gate"
        );
        assert_eq!(
            oracle.determinism,
            "exact_source_sha256_and_required_ast_shapes"
        );
        assert_eq!(oracle.lane, "production_buffered_concurrent_channel");
        assert_eq!(oracle.client, "production_dht_client_interface");
        assert_eq!(oracle.table, "production_ktable_batch_command");
        assert_eq!(oracle.discovery, "production_shared_batching_channel");
    } else {
        assert_eq!(
            oracle.composition,
            "actual_crawler_runFindNode_with_manual_callback_lane"
        );
        assert_eq!(
            oracle.determinism,
            "synchronous_callbacks_scripted_client_and_capacity_controlled_discovery"
        );
        assert_eq!(
            oracle.lane,
            "manual_ordered_callback_interface_implementation"
        );
        assert_eq!(oracle.client, "scripted_client_Client_findNode_override");
        assert_eq!(oracle.table, "tracing_wrapper_over_actual_ktable");
        assert_eq!(
            oracle.discovery,
            "manual_batching_channel_input_with_explicit_capacity"
        );
    }
}

fn assert_source_fixture(fixture: &Fixture) {
    let input = &fixture.input;
    assert_eq!(input.kind, "source_contract");
    assert!(input.nodes.is_none());
    assert!(input.outcomes.is_none());
    assert!(input.initial_target.is_none());
    assert!(input.targets_before_callback.is_none());
    assert!(input.discovery_mode.is_none());
    assert_eq!(input.discovery_capacity, 1_000);
    assert!(input.cancel_before_return.is_none());
    assert!(input.cancel_after_deliveries.is_none());
    assert!(input.lane_return_error.is_none());
    assert!(input.table_setup.is_none());

    let expected = &fixture.expected;
    assert!(expected.node_calls.is_empty());
    assert!(expected.find_calls.is_empty());
    assert!(!expected.same_context);
    assert_eq!(expected.batch_calls, 0);
    assert!(expected.commands.is_empty());
    assert!(expected.discoveries.is_empty());
    assert!(expected.events.is_empty());
    assert!(expected.advertised_post.is_empty());
    assert!(expected.response_id_post.is_empty());
    assert!(expected.run_returned);
    assert!(!expected.context_cancelled);
    let source = expected.source.as_ref().expect("source row facts");

    assert!(source.run_error_ignored);
    assert!(source.shared_callback_context);
    assert!(source.no_eligibility_recheck);
    assert!(source.target_read_at_each_client_call);
    assert!(source.response_id_ignored);
    assert!(source.error_drops_advertised_id);
    assert!(source.success_uses_node_responded_option);
    assert!(source.no_post_query_cancellation_before_put);
    assert!(source.put_precedes_recursive_discovery);
    assert!(source.recursive_discovery_blocks_in_order);
    assert!(source.recursive_discovery_cancel_aware);
    assert_eq!(source.production_capacity, 100);
    assert_eq!(source.production_concurrency, 100);
    assert!(source.run_dequeues_before_acquire);
    assert!(source.run_spawns_callbacks);
    assert!(!source.run_joins_callbacks);
    assert!(source.generic_closed_input_repeats_receive);
    assert!(!source.closed_input_checks_open_boolean);
    assert_eq!(
        source.closed_input_find_node_outcome,
        "zero_value_nil_node_callback_panics_on_p.Addr"
    );
    assert_eq!(
        source.maximum_retained_work,
        "capacity_plus_concurrency_plus_one_acquire_waiter"
    );
    assert_eq!(source.default_scaling_factor, 10);
    assert_eq!(source.discovery_input_capacity, 1_000);
    assert_eq!(source.discovery_max_batch_size, 10);
    assert_eq!(source.discovery_batch_interval_ms, 10);
    assert_eq!(source.discovery_output_capacity, 1);
    assert!(source.producer_initial_query_before_delay);
    assert_eq!(source.producer_cutoff_seconds, 5);
    assert_eq!(source.producer_limit, 10);
    assert_eq!(source.producer_interval_ms, 1_000);
    assert!(!source.producer_sleep_cancellation_aware);
    assert_eq!(
        source.empty_table_cancellation_outcome,
        "continues_query_and_sleep_loop_indefinitely"
    );
    assert_eq!(
        source.producer_evidence_scope,
        "ast_and_exact_source_digest_only_not_runtime_executed"
    );
    assert_eq!(source.sought_id_rotation_seconds, 10);
    assert!(source.sought_id_initialized_before_start);
    assert_eq!(
        source.evidence,
        "real_runFindNode_rows_plus_exact_Go_AST_and_source_freshness"
    );

    assert_eq!(source.source_sha256.len(), GO_SOURCES.len());
    for (path, bytes, digest) in GO_SOURCES {
        assert_eq!(sha256(bytes), digest, "Go source drifted for {path}");
        assert_eq!(
            source.source_sha256.get(path).map(String::as_str),
            Some(digest),
            "fixture source digest drifted for {path}"
        );
    }

    let scheduler = crate::DhtDiscoveredNodeSchedulerConfig::default();
    assert_eq!(
        DhtDiscoveredNodeFindWorkerConfig::default()
            .max_inflight
            .get(),
        source.production_concurrency
    );
    assert_eq!(
        scheduler.find_node_capacity.get(),
        source.production_capacity
    );
    assert_eq!(
        scheduler.max_batch_size.get(),
        source.discovery_max_batch_size
    );
    assert_eq!(
        scheduler.batch_interval,
        Duration::from_millis(source.discovery_batch_interval_ms)
    );
    assert_eq!(
        crate::DHT_DISCOVERY_QUEUE_CAPACITY,
        source.discovery_input_capacity
    );
}

fn assert_runtime_metadata(fixture: &Fixture) {
    let input = &fixture.input;
    assert_eq!(input.kind, "run_find_node");
    let nodes = input.nodes.as_ref().expect("runtime input nodes");
    let outcomes = input.outcomes.as_ref().expect("runtime outcomes");
    assert_eq!(nodes.len(), outcomes.len());
    assert!(!nodes.is_empty());
    let initial_target = input
        .initial_target
        .as_deref()
        .expect("runtime initial target");
    let _ = fixture_id(initial_target);
    let targets = input
        .targets_before_callback
        .as_ref()
        .expect("per-callback targets");
    assert_eq!(targets.len(), nodes.len());
    assert_eq!(targets.first().map(String::as_str), Some(initial_target));
    for target in targets {
        let _ = fixture_id(target);
    }
    assert_eq!(input.discovery_mode.as_deref(), Some("unbuffered"));
    assert_eq!(input.discovery_capacity, 0);
    assert_eq!(
        input.cancel_before_return,
        (fixture.id == FIXTURE_IDS[4]).then_some(true)
    );
    assert_eq!(
        input.cancel_after_deliveries,
        (fixture.id == FIXTURE_IDS[5]).then_some(1)
    );
    assert!(input.lane_return_error.is_none());

    for node in nodes {
        let _ = routing_input_node(node);
        let addr_returns = &node.addr_returns;
        match fixture.id.as_str() {
            id if id == FIXTURE_IDS[1] => {
                assert_eq!(addr_returns.as_slice(), std::slice::from_ref(&node.addr));
            }
            id if id == FIXTURE_IDS[2] => {
                assert_eq!(addr_returns.len(), 2);
                assert_eq!(addr_returns[0], node.addr);
                assert_ne!(addr_returns[1], node.addr);
            }
            _ => {
                assert_eq!(
                    addr_returns.as_slice(),
                    [node.addr.clone(), node.addr.clone()]
                );
            }
        }
    }
    for outcome in outcomes {
        assert!(matches!(outcome.kind.as_str(), "success" | "error"));
        assert_ne!(fixture_id(&outcome.response_id), Id20::ZERO);
        for node in &outcome.nodes {
            let _ = routing_outcome_node(node);
        }
    }
    if fixture.id == FIXTURE_IDS[1] {
        assert_eq!(outcomes[0].kind, "error");
        assert!(!outcomes[0].nodes.is_empty());
    } else {
        assert!(outcomes.iter().all(|outcome| outcome.kind == "success"));
    }

    if let Some(setup) = &input.table_setup {
        assert_eq!(setup.len(), 1);
        assert_eq!(setup[0].id, nodes[0].id);
        assert_eq!(setup[0].addr, nodes[0].addr);
        assert!(matches!(fixture.id.as_str(), id if id == FIXTURE_IDS[1] || id == FIXTURE_IDS[2]));
    } else {
        assert!(!matches!(fixture.id.as_str(), id if id == FIXTURE_IDS[1] || id == FIXTURE_IDS[2]));
    }

    let expected = &fixture.expected;
    assert_eq!(expected.node_calls.len(), nodes.len());
    assert_eq!(expected.find_calls.len(), nodes.len());
    assert!(expected.same_context);
    assert_eq!(expected.batch_calls, nodes.len());
    assert_eq!(expected.commands.len(), nodes.len());
    assert_eq!(expected.advertised_post.len(), nodes.len());
    assert_eq!(expected.response_id_post.len(), outcomes.len());
    assert!(expected.run_returned);
    assert_eq!(
        expected.context_cancelled,
        fixture.id == FIXTURE_IDS[4] || fixture.id == FIXTURE_IDS[5]
    );
    assert!(expected.source.is_none());

    for (index, ((node, outcome), target)) in nodes.iter().zip(outcomes).zip(targets).enumerate() {
        let calls = expected.node_calls[index];
        assert_eq!(calls.id, 1);
        assert_eq!(calls.addr, if outcome.kind == "success" { 2 } else { 1 });
        assert_eq!(calls.time, 0);
        assert_eq!(calls.dropped, 0);
        assert_eq!(calls.sample_infohashes_candidate, 0);

        let addr_returns = &node.addr_returns;
        assert_eq!(addr_returns.len(), calls.addr);
        assert_eq!(expected.find_calls[index].addr, addr_returns[0]);
        assert_eq!(expected.find_calls[index].target, *target);

        let command = &expected.commands[index];
        let advertised_post = &expected.advertised_post[index];
        assert_eq!(advertised_post.id, node.id);
        assert_eq!(advertised_post.retained_dropped, outcome.kind == "error");
        if outcome.kind == "success" {
            assert_eq!(command.kind, "put_node");
            assert_eq!(command.id, node.id);
            assert_eq!(command.addr.as_ref(), addr_returns.last());
            assert_eq!(command.option_count, 1);
            assert!(command.reason.is_empty());
            assert!(!command.error_identity_preserved);
            assert!(command.stored_responded);
            assert!(advertised_post.present);
            assert_eq!(advertised_post.addr.as_ref(), addr_returns.last());
            assert!(advertised_post.responded);
        } else {
            assert_eq!(command.kind, "drop_node");
            assert_eq!(command.id, node.id);
            assert!(command.addr.is_none());
            assert_eq!(command.option_count, 0);
            assert_eq!(command.reason, "find_node failed: oracle find_node failure");
            assert!(command.error_identity_preserved);
            assert!(!command.stored_responded);
            assert!(!advertised_post.present);
            assert!(advertised_post.addr.is_none());
            assert!(!advertised_post.responded);
        }

        let response_post = &expected.response_id_post[index];
        assert_eq!(response_post.id, outcome.response_id);
        assert!(!response_post.present);
        assert!(response_post.addr.is_none());
        assert!(!response_post.responded);
        assert!(!response_post.retained_dropped);
    }

    let expected_discoveries = match fixture.id.as_str() {
        id if id == FIXTURE_IDS[3] => outcomes[0].nodes.as_slice(),
        id if id == FIXTURE_IDS[5] => &outcomes[0].nodes[..1],
        _ => &[],
    };
    assert_eq!(expected.discoveries.len(), expected_discoveries.len());
    for (actual, source) in expected.discoveries.iter().zip(expected_discoveries) {
        assert_eq!(actual.id, source.id);
        assert_eq!(actual.addr, source.addr);
        assert_eq!(
            actual.fresh_state,
            FreshState {
                time_zero: true,
                dropped: false,
                sample_infohashes_candidate: true,
            }
        );
    }

    let mut events = Vec::new();
    for (index, node) in nodes.iter().enumerate() {
        events.push(format!(
            "table_{}_completed:{}",
            if outcomes[index].kind == "success" {
                "put_node"
            } else {
                "drop_node"
            },
            node.id
        ));
        if index == 0 {
            for discovery in expected_discoveries {
                events.push(format!("discovery_accepted:{}", discovery.id));
            }
        }
    }
    assert_eq!(expected.events, events);
}

fn assert_lane_fixture(fixture: &Fixture) {
    let input = &fixture.input;
    assert_eq!(input.kind, "run_find_node");
    assert!(input.nodes.is_none());
    assert!(input.outcomes.is_none());
    assert!(input.initial_target.is_none());
    assert!(input.targets_before_callback.is_none());
    assert_eq!(input.discovery_mode.as_deref(), Some("unbuffered"));
    assert_eq!(input.discovery_capacity, 0);
    assert!(input.cancel_before_return.is_none());
    assert!(input.cancel_after_deliveries.is_none());
    assert_eq!(input.lane_return_error, Some(true));
    assert!(input.table_setup.is_none());

    let expected = &fixture.expected;
    assert!(expected.node_calls.is_empty());
    assert!(expected.find_calls.is_empty());
    assert!(!expected.same_context);
    assert_eq!(expected.batch_calls, 0);
    assert!(expected.commands.is_empty());
    assert!(expected.discoveries.is_empty());
    assert!(expected.events.is_empty());
    assert!(expected.advertised_post.is_empty());
    assert!(expected.response_id_post.is_empty());
    assert!(expected.run_returned);
    assert!(!expected.context_cancelled);
    assert!(expected.source.is_none());
}

#[allow(clippy::type_complexity)]
fn core(
    route_capacity: usize,
    discovery_capacity: usize,
) -> (
    tokio::sync::mpsc::Sender<RoutingNode>,
    DhtDiscoveredNodeFindWorkerCore,
    DhtDiscoveredNodeFindStatsHandle,
    KTable,
    DhtDiscoveryReceiver,
    DhtDiscoverySender,
    DhtDiscoveryStatsHandle,
) {
    let (route_sender, route_receiver) =
        DhtDiscoveredNodeRouteReceiver::test_channel(route_capacity);
    let (discovery, discovery_receiver) =
        dht_discovery_channel(NonZeroUsize::new(discovery_capacity).unwrap());
    let discovery_probe = discovery.clone();
    let discovery_stats = discovery.stats_handle();
    let table = KTable::new(fixture_id("00000000000000000000000000000000000000fa"));
    let stats = DhtDiscoveredNodeFindStatsHandle::default();
    (
        route_sender,
        DhtDiscoveredNodeFindWorkerCore::new(
            route_receiver,
            table.clone(),
            discovery,
            NonZeroUsize::new(1).unwrap(),
            stats.clone(),
        ),
        stats,
        table,
        discovery_receiver,
        discovery_probe,
        discovery_stats,
    )
}

async fn wait_for(mut predicate: impl FnMut() -> bool, description: &'static str) {
    for _ in 0..1_000 {
        if predicate() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("timed out waiting for {description}");
}

fn scripted_results(outcomes: &[Outcome]) -> VecDeque<Result<FindNodeResult, &'static str>> {
    outcomes
        .iter()
        .map(|outcome| match outcome.kind.as_str() {
            "success" => Ok(FindNodeResult {
                id: fixture_id(&outcome.response_id),
                nodes: outcome.nodes.iter().map(routing_outcome_node).collect(),
            }),
            "error" => Err("oracle find_node failure"),
            kind => panic!("unexpected scripted outcome {kind}"),
        })
        .collect()
}

fn assert_query_calls(actual: &[(SocketAddr, Id20)], fixture: &Fixture) {
    assert_eq!(actual.len(), fixture.expected.find_calls.len());
    for ((addr, target), expected) in actual.iter().zip(&fixture.expected.find_calls) {
        assert_eq!(*addr, socket_addr(&expected.addr));
        assert_eq!(*target, fixture_id(&expected.target));
    }
}

fn assert_actual_success_table(table: &KTable, advertised: &InputNode, outcome: &Outcome) {
    let stored = table.node_handle(fixture_id(&advertised.id)).unwrap();
    assert_eq!(stored.id(), fixture_id(&advertised.id));
    assert_eq!(stored.addr(), socket_addr(&advertised.addr));
    assert!(stored.last_responded_at().is_some());
    assert!(!stored.dropped());
    assert!(table
        .node_handle(fixture_id(&outcome.response_id))
        .is_none());
}

fn assert_non_delta_success_table(
    table: &KTable,
    advertised: &InputNode,
    outcome: &Outcome,
    command: &Command,
    advertised_post: &TablePost,
) {
    assert_eq!(command.addr.as_ref(), Some(&advertised.addr));
    assert_eq!(advertised_post.addr.as_ref(), Some(&advertised.addr));
    assert_actual_success_table(table, advertised, outcome);
}

fn expected_input_closed_stats(id: &str) -> DhtDiscoveredNodeFindStats {
    match id {
        id if id == FIXTURE_IDS[1] => DhtDiscoveredNodeFindStats {
            dequeued: 1,
            queries_started: 1,
            tasks_completed: 1,
            queries_failed: 1,
            drop_commands: 1,
            ..DhtDiscoveredNodeFindStats::default()
        },
        id if id == FIXTURE_IDS[2] => DhtDiscoveredNodeFindStats {
            dequeued: 1,
            queries_started: 1,
            tasks_completed: 1,
            queries_succeeded: 1,
            put_commands: 1,
            ..DhtDiscoveredNodeFindStats::default()
        },
        id if id == FIXTURE_IDS[6] => DhtDiscoveredNodeFindStats {
            dequeued: 2,
            queries_started: 2,
            tasks_completed: 2,
            queries_succeeded: 2,
            put_commands: 2,
            ..DhtDiscoveredNodeFindStats::default()
        },
        id => panic!("no hard-coded InputClosed stats for {id}"),
    }
}

#[tokio::test]
async fn runtime_rows_replay_on_actual_worker_core_with_explicit_deltas() {
    let fixtures = fixtures();
    replay_input_closed_row(&fixtures[1]).await;
    replay_input_closed_row(&fixtures[2]).await;
    replay_full_ordered_fanout_row(&fixtures[3]).await;
    replay_same_poll_cancellation_row(&fixtures[4]).await;
    replay_one_prefix_cancellation_row(&fixtures[5]).await;
    replay_input_closed_row(&fixtures[6]).await;
}

#[tokio::test]
async fn rust_typed_eof_is_a_separate_hardening_contract() {
    assert_typed_eof_hardening().await;
}

async fn replay_input_closed_row(fixture: &Fixture) {
    assert!(
        matches!(fixture.id.as_str(), id if id == FIXTURE_IDS[1] || id == FIXTURE_IDS[2] || id == FIXTURE_IDS[6])
    );
    let nodes = fixture.input.nodes.as_ref().unwrap();
    let outcomes = fixture.input.outcomes.as_ref().unwrap();
    let targets = fixture.input.targets_before_callback.as_ref().unwrap();
    let (route_sender, mut core, stats, table, mut discovery_receiver, _probe, discovery_stats) =
        core(nodes.len().max(1), 1);

    let mut retained_handles = Vec::new();
    if let Some(setup) = &fixture.input.table_setup {
        for setup in setup {
            let seeded = RoutingNode {
                id: fixture_id(&setup.id),
                addr: socket_addr(&setup.addr),
            };
            assert_eq!(table.put_node(seeded), crate::RoutingPutResult::Accepted);
            let retained = table.node_handle(seeded.id).unwrap();
            assert_eq!(retained.id(), seeded.id);
            assert_eq!(retained.addr(), seeded.addr);
            assert!(retained.last_responded_at().is_none());
            assert!(!retained.dropped());
            retained_handles.push(retained);
        }
    }
    for node in nodes {
        route_sender.send(routing_input_node(node)).await.unwrap();
    }
    drop(route_sender);

    let scripted = Arc::new(Mutex::new(scripted_results(outcomes)));
    let target_values = Arc::new(Mutex::new(
        targets
            .iter()
            .map(|target| fixture_id(target))
            .collect::<VecDeque<_>>(),
    ));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(Mutex::new(Vec::new()));
    let scripted_for_query = Arc::clone(&scripted);
    let targets_for_reader = Arc::clone(&target_values);
    let calls_for_query = Arc::clone(&calls);
    let events_for_reader = Arc::clone(&events);
    let events_for_query = Arc::clone(&events);
    let exit = core
        .run_with_query(
            pending(),
            move || {
                let target = targets_for_reader.lock().unwrap().pop_front().unwrap();
                events_for_reader
                    .lock()
                    .unwrap()
                    .push(format!("target:{target}"));
                target
            },
            move |addr, target| {
                calls_for_query.lock().unwrap().push((addr, target));
                events_for_query
                    .lock()
                    .unwrap()
                    .push(format!("query:{addr}:{target}"));
                ready(scripted_for_query.lock().unwrap().pop_front().unwrap())
            },
        )
        .await;
    assert_eq!(exit, DhtDiscoveredNodeFindWorkerExit::InputClosed);
    assert!(scripted.lock().unwrap().is_empty());
    assert!(target_values.lock().unwrap().is_empty());
    assert_query_calls(&calls.lock().unwrap(), fixture);
    assert_eq!(stats.snapshot(), expected_input_closed_stats(&fixture.id));
    assert_eq!(
        discovery_stats.snapshot(),
        crate::DhtDiscoveryStats::default()
    );
    assert!(matches!(
        discovery_receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    for (index, (node, outcome)) in nodes.iter().zip(outcomes).enumerate() {
        if outcome.kind == "error" {
            assert!(table.node_handle(fixture_id(&node.id)).is_none());
            assert!(retained_handles[index].dropped());
            assert_eq!(retained_handles[index].addr(), socket_addr(&node.addr));
            assert!(retained_handles[index].last_responded_at().is_none());
            assert!(!outcome.nodes.is_empty());
            assert_ne!(fixture_id(&outcome.response_id), Id20::ZERO);
            assert!(table
                .node_handle(fixture_id(&outcome.response_id))
                .is_none());
        } else if fixture.id == FIXTURE_IDS[2] {
            assert_actual_success_table(&table, node, outcome);
        } else {
            assert_non_delta_success_table(
                &table,
                node,
                outcome,
                &fixture.expected.commands[index],
                &fixture.expected.advertised_post[index],
            );
        }
    }

    if fixture.id == FIXTURE_IDS[2] {
        let node = &nodes[0];
        assert_eq!(node.addr_returns.len(), 2);
        let query_addr = socket_addr(&node.addr_returns[0]);
        let go_put_addr = socket_addr(&node.addr_returns[1]);
        assert_ne!(query_addr, go_put_addr);
        assert_eq!(calls.lock().unwrap()[0].0, query_addr);
        assert_eq!(
            table.node_handle(fixture_id(&node.id)).unwrap().addr(),
            query_addr
        );
        assert!(!retained_handles[0].dropped());
        assert_eq!(retained_handles[0].addr(), query_addr);
        assert!(retained_handles[0].last_responded_at().is_some());
        assert_eq!(
            socket_addr(fixture.expected.commands[0].addr.as_ref().unwrap()),
            go_put_addr
        );
        assert_eq!(
            socket_addr(fixture.expected.advertised_post[0].addr.as_ref().unwrap()),
            go_put_addr
        );
    }

    if fixture.id == FIXTURE_IDS[6] {
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 4);
        for index in 0..2 {
            let target = fixture_id(&targets[index]);
            let addr = socket_addr(&nodes[index].addr);
            assert_eq!(events[index * 2], format!("target:{target}"));
            assert_eq!(events[index * 2 + 1], format!("query:{addr}:{target}"));
        }
    }
}

async fn replay_full_ordered_fanout_row(fixture: &Fixture) {
    assert_eq!(fixture.id, FIXTURE_IDS[3]);
    let node = &fixture.input.nodes.as_ref().unwrap()[0];
    let outcome = &fixture.input.outcomes.as_ref().unwrap()[0];
    let target = fixture_id(&fixture.input.targets_before_callback.as_ref().unwrap()[0]);
    let returned = outcome
        .nodes
        .iter()
        .map(routing_outcome_node)
        .collect::<Vec<_>>();
    let (route_sender, mut core, stats, table, mut receiver, probe, discovery_stats) = core(1, 1);
    let sentinel = RoutingNode {
        id: fixture_id("00000000000000000000000000000000000000f0"),
        addr: "192.0.2.240:7240".parse().unwrap(),
    };
    assert_eq!(probe.offer(sentinel), DhtDiscoveryOffer::Queued);
    route_sender.send(routing_input_node(node)).await.unwrap();
    drop(route_sender);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_for_query = Arc::clone(&calls);
    let returned_by_query = returned.clone();
    let response_id = fixture_id(&outcome.response_id);
    let worker = tokio::spawn(async move {
        core.run_with_query(
            pending(),
            move || target,
            move |addr, target| {
                calls_for_query.lock().unwrap().push((addr, target));
                ready(Ok::<_, ()>(FindNodeResult {
                    id: response_id,
                    nodes: returned_by_query.clone(),
                }))
            },
        )
        .await
    });

    wait_for(
        || {
            let snapshot = stats.snapshot();
            snapshot.put_commands == 1
                && snapshot.recursive_nodes == 4
                && snapshot.recursive_nodes_queued == 0
                && snapshot.tasks_completed == 0
        },
        "oracle row 3 Put before blocked discovery",
    )
    .await;
    assert_non_delta_success_table(
        &table,
        node,
        outcome,
        &fixture.expected.commands[0],
        &fixture.expected.advertised_post[0],
    );
    assert!(!worker.is_finished());
    assert_eq!(receiver.recv().await.unwrap(), sentinel);
    let mut discovered = Vec::new();
    for expected in &returned {
        let actual = receiver.recv().await.unwrap();
        assert_eq!(actual, *expected);
        discovered.push(actual);
    }
    assert_eq!(
        worker.await.unwrap(),
        DhtDiscoveredNodeFindWorkerExit::InputClosed
    );
    assert_eq!(discovered, returned);
    assert_query_calls(&calls.lock().unwrap(), fixture);
    assert_eq!(
        stats.snapshot(),
        DhtDiscoveredNodeFindStats {
            dequeued: 1,
            queries_started: 1,
            tasks_completed: 1,
            queries_succeeded: 1,
            put_commands: 1,
            recursive_nodes: 4,
            recursive_nodes_queued: 4,
            ..DhtDiscoveredNodeFindStats::default()
        }
    );
    assert_eq!(
        discovery_stats.snapshot(),
        crate::DhtDiscoveryStats {
            offered: 5,
            queued: 5,
            ..crate::DhtDiscoveryStats::default()
        }
    );
}

async fn replay_same_poll_cancellation_row(fixture: &Fixture) {
    assert_eq!(fixture.id, FIXTURE_IDS[4]);
    let node = &fixture.input.nodes.as_ref().unwrap()[0];
    let outcome = &fixture.input.outcomes.as_ref().unwrap()[0];
    let target = fixture_id(&fixture.input.targets_before_callback.as_ref().unwrap()[0]);
    let returned = outcome
        .nodes
        .iter()
        .map(routing_outcome_node)
        .collect::<Vec<_>>();
    let (route_sender, mut core, stats, table, mut receiver, probe, discovery_stats) = core(1, 1);
    let sentinel = RoutingNode {
        id: fixture_id("00000000000000000000000000000000000000f1"),
        addr: "192.0.2.241:7241".parse().unwrap(),
    };
    assert_eq!(probe.offer(sentinel), DhtDiscoveryOffer::Queued);
    route_sender.send(routing_input_node(node)).await.unwrap();
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let mut shutdown_sender = Some(shutdown_sender);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_for_query = Arc::clone(&calls);
    let response_id = fixture_id(&outcome.response_id);
    let exit = core
        .run_with_query(
            async move {
                let _ = shutdown_receiver.await;
            },
            move || target,
            move |addr, target| {
                calls_for_query.lock().unwrap().push((addr, target));
                let mut trigger = Some(shutdown_sender.take().unwrap());
                let returned = returned.clone();
                poll_fn(move |_| {
                    let _ = trigger.take().unwrap().send(());
                    Poll::Ready(Ok::<_, ()>(FindNodeResult {
                        id: response_id,
                        nodes: returned.clone(),
                    }))
                })
            },
        )
        .await;
    assert_eq!(
        exit,
        DhtDiscoveredNodeFindWorkerExit::Shutdown {
            queued_dropped: 0,
            tasks_cancelled: 1,
            recursive_nodes_dropped: 4,
        }
    );
    assert_non_delta_success_table(
        &table,
        node,
        outcome,
        &fixture.expected.commands[0],
        &fixture.expected.advertised_post[0],
    );
    assert_query_calls(&calls.lock().unwrap(), fixture);
    assert_eq!(receiver.try_recv().unwrap(), sentinel);
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(
        stats.snapshot(),
        DhtDiscoveredNodeFindStats {
            dequeued: 1,
            queries_started: 1,
            queries_succeeded: 1,
            put_commands: 1,
            recursive_nodes: 4,
            shutdown_tasks_cancelled: 1,
            shutdown_recursive_nodes_dropped: 4,
            ..DhtDiscoveredNodeFindStats::default()
        }
    );
    assert_eq!(
        discovery_stats.snapshot(),
        crate::DhtDiscoveryStats {
            offered: 1,
            queued: 1,
            ..crate::DhtDiscoveryStats::default()
        }
    );
}

async fn replay_one_prefix_cancellation_row(fixture: &Fixture) {
    assert_eq!(fixture.id, FIXTURE_IDS[5]);
    let node = &fixture.input.nodes.as_ref().unwrap()[0];
    let outcome = &fixture.input.outcomes.as_ref().unwrap()[0];
    let target = fixture_id(&fixture.input.targets_before_callback.as_ref().unwrap()[0]);
    let returned = outcome
        .nodes
        .iter()
        .map(routing_outcome_node)
        .collect::<Vec<_>>();
    let (route_sender, mut core, stats, table, mut receiver, _probe, discovery_stats) = core(1, 1);
    route_sender.send(routing_input_node(node)).await.unwrap();
    drop(route_sender);
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_for_query = Arc::clone(&calls);
    let returned_by_query = returned.clone();
    let response_id = fixture_id(&outcome.response_id);
    let worker = tokio::spawn(async move {
        core.run_with_query(
            async move {
                let _ = shutdown_receiver.await;
            },
            move || target,
            move |addr, target| {
                calls_for_query.lock().unwrap().push((addr, target));
                ready(Ok::<_, ()>(FindNodeResult {
                    id: response_id,
                    nodes: returned_by_query.clone(),
                }))
            },
        )
        .await
    });
    wait_for(
        || {
            let snapshot = stats.snapshot();
            snapshot.recursive_nodes_queued == 1 && snapshot.tasks_completed == 0
        },
        "oracle row 5 one recursive prefix",
    )
    .await;
    let _ = shutdown_sender.send(());
    assert_eq!(
        worker.await.unwrap(),
        DhtDiscoveredNodeFindWorkerExit::Shutdown {
            queued_dropped: 0,
            tasks_cancelled: 1,
            recursive_nodes_dropped: 3,
        }
    );
    assert_non_delta_success_table(
        &table,
        node,
        outcome,
        &fixture.expected.commands[0],
        &fixture.expected.advertised_post[0],
    );
    assert_query_calls(&calls.lock().unwrap(), fixture);
    assert_eq!(receiver.try_recv().unwrap(), returned[0]);
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(
        stats.snapshot(),
        DhtDiscoveredNodeFindStats {
            dequeued: 1,
            queries_started: 1,
            queries_succeeded: 1,
            put_commands: 1,
            recursive_nodes: 4,
            recursive_nodes_queued: 1,
            shutdown_tasks_cancelled: 1,
            shutdown_recursive_nodes_dropped: 3,
            ..DhtDiscoveredNodeFindStats::default()
        }
    );
    assert_eq!(
        discovery_stats.snapshot(),
        crate::DhtDiscoveryStats {
            offered: 1,
            queued: 1,
            ..crate::DhtDiscoveryStats::default()
        }
    );
}

async fn assert_typed_eof_hardening() {
    let (route_sender, mut core, stats, _table, mut receiver, _probe, discovery_stats) = core(1, 1);
    drop(route_sender);
    let exit = core
        .run_with_query(
            pending(),
            || -> Id20 { panic!("typed EOF must not read the crawler target") },
            |_, _| -> std::future::Ready<Result<FindNodeResult, ()>> {
                panic!("typed EOF must not construct a query")
            },
        )
        .await;
    assert_eq!(exit, DhtDiscoveredNodeFindWorkerExit::InputClosed);
    assert_eq!(stats.snapshot(), DhtDiscoveredNodeFindStats::default());
    assert_eq!(
        discovery_stats.snapshot(),
        crate::DhtDiscoveryStats::default()
    );
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
}
