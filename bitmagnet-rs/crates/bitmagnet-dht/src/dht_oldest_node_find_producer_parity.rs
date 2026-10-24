use std::collections::{BTreeMap, VecDeque};
use std::future::{poll_fn, ready, Future};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use super::*;
use crate::{
    DhtDiscoveredNodeFindWorkerConfig, DhtDiscoveredNodeSchedulerConfig, Id20, KTableClock,
    KTableNodeOption, RoutingNode, RoutingPutResult,
};

const FIXTURE_TEXT: &str =
    include_str!("../../../../testdata/parity/dht/dht_crawler_find_node_producer.jsonl");
const FIXTURE_BYTES: &[u8] =
    include_bytes!("../../../../testdata/parity/dht/dht_crawler_find_node_producer.jsonl");
const FIXTURE_SHA256: &str = "06e2ac78f73418038c946fdc5f3562654e130623fcf88e907c1c4e07112505cc";
const FIXTURE_IDS: [&str; 3] = [
    "production_source_factory_and_lifecycle_contract",
    "already_cancelled_still_queries_before_first_send",
    "ordered_prefix_then_cancel_at_blocked_third_send",
];
const ROW_CLASSIFICATIONS: [&str; 3] = ["SOURCE_ONLY", "RUNTIME_EXACT", "RUNTIME_EXACT"];

const GO_ONLY_METADATA: [&str; 6] = [
    "input.nodes[].token",
    "expected.getCalls[].cutoffWindowMatched",
    "expected.laneInCalls",
    "expected.deliveries[].sameGoInterfaceHandle",
    "expected.accessorCalls",
    "expected.events",
];

const RUST_EXECUTION_PARTITION: [(&str, &str); 3] = [
    (FIXTURE_IDS[0], "SOURCE_ONLY_NO_RUST_RUNTIME_REPLAY"),
    (
        FIXTURE_IDS[1],
        "GO_RUNTIME_ONLY_WITH_SEPARATE_RUST_PRE_READY_SHUTDOWN_DELTA",
    ),
    (
        FIXTURE_IDS[2],
        "RUST_OBSERVABLE_PREFIX_SUFFIX_REPLAY_WITH_GO_HANDLE_ACCESSOR_EVENT_AND_CUTOFF_METADATA_EXCLUDED",
    ),
];

const GO_SOURCES: [(&str, &[u8], &str); 6] = [
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
        "internal/dhtcrawler/find_node.go",
        include_bytes!("../../../../internal/dhtcrawler/find_node.go"),
        "cd5fab8aa078ad40ed82331dbbfd141a38badc018287dd13211d221b230087bb",
    ),
    (
        "internal/protocol/dht/ktable/table.go",
        include_bytes!("../../../../internal/protocol/dht/ktable/table.go"),
        "68e3caf4394b2692fd9358224cce2b70ae3d90d920097bd28885b6b3bb77848f",
    ),
];

const DELIBERATE_RUST_DELTAS: [&str; 8] = [
    "pre_ready_shutdown_wins_before_the_first_query",
    "positive_Tokio_capacity_replaces_the_Go_unbuffered_lane",
    "shutdown_is_biased_at_query_snapshot_send_and_delay_boundaries",
    "cutoff_uses_an_injectable_monotonic_clock",
    "post_batch_delay_is_cancellation_aware_and_never_catches_up",
    "empty_table_receiver_close_returns_typed_InputClosed",
    "each_live_handle_becomes_one_immutable_RoutingNode_before_its_send",
    "the_producer_run_future_is_owned_taskless_and_starts_no_detached_work",
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
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct GetCall {
    limit: usize,
    cutoff_window_matched: bool,
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
    cutoff_seconds: u64,
    limit: usize,
    preserves_returned_order: bool,
    per_node_send_cancellation_aware: bool,
    no_node_projection_or_recheck: bool,
    post_batch_delay_ms: u64,
    post_batch_sleep_cancellation_aware: bool,
    empty_table_cancellation_outcome: String,
    ready_send_cancel_outcome: String,
    producer_detached: bool,
    producer_joined: bool,
    production_capacity: usize,
    production_concurrency: usize,
    cutoff_clock_runtime_bracketed: bool,
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
        .all(|fixture| fixture.subsystem == "dht_crawler_find_node_producer"));
    assert_source_row(&fixtures[0]);
    assert_precancelled_go_row(&fixtures[1]);
    assert_ordered_prefix_go_row(&fixtures[2]);
    assert_eq!(
        DELIBERATE_RUST_DELTAS,
        [
            "pre_ready_shutdown_wins_before_the_first_query",
            "positive_Tokio_capacity_replaces_the_Go_unbuffered_lane",
            "shutdown_is_biased_at_query_snapshot_send_and_delay_boundaries",
            "cutoff_uses_an_injectable_monotonic_clock",
            "post_batch_delay_is_cancellation_aware_and_never_catches_up",
            "empty_table_receiver_close_returns_typed_InputClosed",
            "each_live_handle_becomes_one_immutable_RoutingNode_before_its_send",
            "the_producer_run_future_is_owned_taskless_and_starts_no_detached_work",
        ]
    );
    assert_eq!(
        GO_ONLY_METADATA,
        [
            "input.nodes[].token",
            "expected.getCalls[].cutoffWindowMatched",
            "expected.laneInCalls",
            "expected.deliveries[].sameGoInterfaceHandle",
            "expected.accessorCalls",
            "expected.events",
        ]
    );
    assert_eq!(
        RUST_EXECUTION_PARTITION,
        [
            (FIXTURE_IDS[0], "SOURCE_ONLY_NO_RUST_RUNTIME_REPLAY"),
            (
                FIXTURE_IDS[1],
                "GO_RUNTIME_ONLY_WITH_SEPARATE_RUST_PRE_READY_SHUTDOWN_DELTA",
            ),
            (
                FIXTURE_IDS[2],
                "RUST_OBSERVABLE_PREFIX_SUFFIX_REPLAY_WITH_GO_HANDLE_ACCESSOR_EVENT_AND_CUTOFF_METADATA_EXCLUDED",
            ),
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
        "production_ktable_Table_GetOldestNodes_interface"
    );
    assert_eq!(
        fixture.oracle.lane,
        "production_buffered_concurrent_channel"
    );
    assert_eq!(
        fixture.oracle.clock,
        "exact_source_time_Now_and_time_After_shapes"
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
    assert_eq!(source.cutoff_seconds, 5);
    assert_eq!(source.limit, 10);
    assert!(source.preserves_returned_order);
    assert!(source.per_node_send_cancellation_aware);
    assert!(source.no_node_projection_or_recheck);
    assert_eq!(source.post_batch_delay_ms, 1_000);
    assert!(!source.post_batch_sleep_cancellation_aware);
    assert_eq!(
        source.empty_table_cancellation_outcome,
        "while_every_query_remains_empty_queries_then_unconditionally_sleeps_one_second_forever"
    );
    assert_eq!(
        source.ready_send_cancel_outcome,
        "go_select_chooses_nondeterministically_when_both_are_ready"
    );
    assert!(source.producer_detached);
    assert!(!source.producer_joined);
    assert_eq!(source.production_capacity, 100);
    assert_eq!(source.production_concurrency, 100);
    assert!(source.cutoff_clock_runtime_bracketed);
    assert!(!source.post_batch_delay_runtime_observed);
    assert!(!source.empty_table_runtime_observed);
    assert!(source.runtime_rows_return_before_sleep);
    assert_eq!(source.evidence, "the cutoff clock is runtime-bracketed; post-batch timer timing and empty-table cancellation are source-only because runtime rows return from the actual method before its real sleep");
    assert_eq!(source.source_sha256.len(), GO_SOURCES.len());
    for (path, bytes, digest) in GO_SOURCES {
        assert_eq!(sha256(bytes), digest, "Go source drifted for {path}");
        assert_eq!(
            source.source_sha256.get(path).map(String::as_str),
            Some(digest)
        );
    }
    assert_eq!(OLDEST_AGE, Duration::from_secs(source.cutoff_seconds));
    assert_eq!(OLDEST_LIMIT.get(), source.limit);
    assert_eq!(
        QUERY_DELAY,
        Duration::from_millis(source.post_batch_delay_ms)
    );
    assert_eq!(
        DhtDiscoveredNodeSchedulerConfig::default()
            .find_node_capacity
            .get(),
        source.production_capacity
    );
    assert_eq!(
        DhtDiscoveredNodeFindWorkerConfig::default()
            .max_inflight
            .get(),
        source.production_concurrency
    );
}

fn assert_precancelled_go_row(fixture: &Fixture) {
    assert_runtime_oracle(fixture, "pre_cancelled_context_and_unbuffered_lane");
    assert_eq!(fixture.input.kind, "actual_getNodesForFindNode");
    assert!(fixture.input.context_initially_cancelled);
    assert_eq!(fixture.input.lane_capacity, 0);
    assert_eq!(fixture.input.cancel_at_lane_in_call, 0);
    assert_eq!(
        fixture.input.nodes,
        [fixture_node(
            "A",
            "0000000000000000000000000000000000000001",
            "192.0.2.1:6001"
        )]
    );
    assert_eq!(
        fixture.expected.get_calls,
        [GetCall {
            limit: 10,
            cutoff_window_matched: true
        }]
    );
    assert_eq!(fixture.expected.lane_in_calls, 1);
    assert!(fixture.expected.deliveries.is_empty());
    assert_eq!(fixture.expected.abandoned, fixture.input.nodes);
    assert_zero_accessors(&fixture.expected.accessor_calls, &["A"]);
    assert_eq!(
        fixture.expected.events,
        ["get_oldest_nodes", "lane_in:1", "return"]
    );
    assert!(fixture.expected.run_returned);
    assert!(fixture.expected.context_cancelled);
    assert!(fixture.expected.source.is_none());
}

fn assert_ordered_prefix_go_row(fixture: &Fixture) {
    assert_runtime_oracle(fixture, "capacity_two_lane_with_third_In_gate");
    assert_eq!(fixture.input.kind, "actual_getNodesForFindNode");
    assert!(!fixture.input.context_initially_cancelled);
    assert_eq!(fixture.input.lane_capacity, 2);
    assert_eq!(fixture.input.cancel_at_lane_in_call, 3);
    let nodes = [
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
    ];
    assert_eq!(fixture.input.nodes, nodes);
    assert_eq!(
        fixture.expected.get_calls,
        [GetCall {
            limit: 10,
            cutoff_window_matched: true
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
            "get_oldest_nodes",
            "lane_in:1",
            "lane_in:2",
            "lane_in:3",
            "cancel",
            "return"
        ]
    );
    assert!(fixture.expected.run_returned);
    assert!(fixture.expected.context_cancelled);
    assert!(fixture.expected.source.is_none());
}

fn assert_runtime_oracle(fixture: &Fixture, determinism: &str) {
    assert_eq!(
        fixture.oracle.composition,
        "actual_crawler_getNodesForFindNode_with_scripted_table_and_manual_lane"
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
        "production_time_Now_cutoff_runtime_bracketed_without_reaching_time_After"
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
                calls.sample_infohashes_candidate
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

#[tokio::test]
async fn go_pre_cancel_row_is_a_deliberate_rust_pre_ready_shutdown_delta() {
    let fixtures = fixtures();
    let fixture = &fixtures[1];
    assert_precancelled_go_row(fixture);

    let advertised = RoutingNode {
        id: Id20::from_hex("0000000000000000000000000000000000000001").unwrap(),
        addr: "192.0.2.1:6001".parse().unwrap(),
    };
    assert_eq!(routing_node(&fixture.input.nodes[0]), advertised);
    let table = KTable::new(Id20::ZERO);
    assert_eq!(table.put_node(advertised), RoutingPutResult::Accepted);
    let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(1);
    let (producer, stats) = DhtOldestNodeFindProducer::new(table.clone(), input);

    assert_eq!(
        producer.run(ready(())).await,
        DhtOldestNodeFindProducerExit::Shutdown {
            selected_dropped: 0,
        }
    );
    assert_eq!(stats.snapshot(), DhtOldestNodeFindProducerStats::default());
    assert_eq!(
        table.node_handle(advertised.id).unwrap().routing_node(),
        advertised
    );
    assert_eq!(receiver.recv().await, None);
}

#[tokio::test]
async fn ordered_prefix_outcome_replays_on_actual_rust_producer_with_go_metadata_excluded() {
    let fixtures = fixtures();
    let fixture = &fixtures[2];
    assert_ordered_prefix_go_row(fixture);

    let expected_nodes = [
        RoutingNode {
            id: Id20::from_hex("0000000000000000000000000000000000000001").unwrap(),
            addr: "192.0.2.1:6001".parse().unwrap(),
        },
        RoutingNode {
            id: Id20::from_hex("0000000000000000000000000000000000000002").unwrap(),
            addr: "192.0.2.2:6002".parse().unwrap(),
        },
        RoutingNode {
            id: Id20::from_hex("0000000000000000000000000000000000000003").unwrap(),
            addr: "192.0.2.3:6003".parse().unwrap(),
        },
        RoutingNode {
            id: Id20::from_hex("0000000000000000000000000000000000000004").unwrap(),
            addr: "192.0.2.4:6004".parse().unwrap(),
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
    struct ScriptedClock {
        values: Mutex<VecDeque<Instant>>,
    }

    impl KTableClock for ScriptedClock {
        fn now(&self) -> Instant {
            self.values
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted parity clock exhausted")
        }
    }

    let query_now = Instant::now()
        .checked_add(Duration::from_secs(10))
        .expect("ten seconds fit the monotonic clock");
    let old_response = query_now - Duration::from_secs(6);
    let exact_cutoff_response = query_now - OLDEST_AGE;
    let table = KTable::with_clock(
        Id20::ZERO,
        Arc::new(ScriptedClock {
            values: Mutex::new(VecDeque::from([
                old_response,
                old_response,
                old_response,
                old_response,
                exact_cutoff_response,
            ])),
        }),
    );
    for node in expected_nodes {
        assert_eq!(
            table.put_node_with_options(node, &[KTableNodeOption::Responded]),
            RoutingPutResult::Accepted
        );
    }
    let exact_cutoff_node = RoutingNode {
        id: Id20::from_hex("0000000000000000000000000000000000000005").unwrap(),
        addr: "192.0.2.5:6005".parse().unwrap(),
    };
    assert_eq!(
        table.put_node_with_options(exact_cutoff_node, &[KTableNodeOption::Responded]),
        RoutingPutResult::Accepted
    );
    let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(2);
    let (producer, stats) = DhtOldestNodeFindProducer::new(table, input);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let before_snapshot = Arc::new(Mutex::new(Vec::new()));
    let observed_before_snapshot = Arc::clone(&before_snapshot);
    let run = producer.run_with(
        async move {
            let _ = shutdown_rx.await;
        },
        move || query_now,
        |_| std::future::pending::<()>(),
        move |index, _| observed_before_snapshot.lock().unwrap().push(index),
    );
    tokio::pin!(run);

    poll_once_pending(run.as_mut()).await;
    assert_eq!(*before_snapshot.lock().unwrap(), [0, 1, 2]);
    assert_eq!(
        stats.snapshot(),
        DhtOldestNodeFindProducerStats {
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
        DhtOldestNodeFindProducerExit::Shutdown {
            selected_dropped: 2,
        }
    );
    assert_eq!(receiver.recv().await, Some(expected_nodes[0]));
    assert_eq!(receiver.recv().await, Some(expected_nodes[1]));
    assert_eq!(receiver.recv().await, None);
    assert_eq!(
        stats.snapshot(),
        DhtOldestNodeFindProducerStats {
            table_queries: 1,
            selected: 4,
            queued: 2,
            input_closed_dropped: 0,
            shutdown_dropped: 2,
        }
    );
}
