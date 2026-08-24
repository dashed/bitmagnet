use std::collections::BTreeMap;
use std::future::pending;
use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::num::NonZeroUsize;

use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use super::*;
use crate::RoutingPutResult;

const FIXTURE_TEXT: &str =
    include_str!("../../../../testdata/parity/dht/dht_crawler_ping_worker.jsonl");
const FIXTURE_BYTES: &[u8] =
    include_bytes!("../../../../testdata/parity/dht/dht_crawler_ping_worker.jsonl");
const FIXTURE_SHA256: &str = "26d403becff0caeb0a27ec9027a366d51e19cdb7129ff05715cf24a6d2e1b040";
const FIXTURE_IDS: [&str; 9] = [
    "production_factory_and_source_contract",
    "dropped_node_short_circuits_everything",
    "recent_node_skips_ping",
    "old_zero_id_success_learns_response_id",
    "old_matching_id_success_marks_responded",
    "old_mismatched_id_drops_advertised_id",
    "ping_error_drops_zero_not_advertised_id",
    "cancelled_after_success_still_puts",
    "lane_error_is_swallowed",
];
const DEFERRED_HANDLE_ROWS: [&str; 2] = [
    "dropped_node_short_circuits_everything",
    "recent_node_skips_ping",
];
const GO_ONLY_LANE_ROWS: [&str; 1] = ["lane_error_is_swallowed"];
const GO_ONLY_FIXTURE_METADATA: [&str; 6] = [
    "expected.nodeCalls",
    "expected.sameContext",
    "expected.batchCalls",
    "expected.commands[].optionCount",
    "expected.commands[].reason",
    "expected.commands[].errorIdentityPreserved",
];
const DELIBERATE_RUST_HARDENING_DELTAS: [&str; 3] = [
    "capacity blocks intake before dequeue; retained work is route capacity plus max_inflight",
    "query tasks are owned and joined on EOF or aborted and joined on shutdown",
    "closed input is a typed InputClosed exit rather than repeated nil receives and panic",
];

const GO_SOURCES: [(&str, &[u8]); 8] = [
    (
        "internal/concurrency/buffered_concurrent_channel.go",
        include_bytes!("../../../../internal/concurrency/buffered_concurrent_channel.go"),
    ),
    (
        "internal/dhtcrawler/config.go",
        include_bytes!("../../../../internal/dhtcrawler/config.go"),
    ),
    (
        "internal/dhtcrawler/crawler.go",
        include_bytes!("../../../../internal/dhtcrawler/crawler.go"),
    ),
    (
        "internal/dhtcrawler/factory.go",
        include_bytes!("../../../../internal/dhtcrawler/factory.go"),
    ),
    (
        "internal/dhtcrawler/ping.go",
        include_bytes!("../../../../internal/dhtcrawler/ping.go"),
    ),
    (
        "internal/protocol/dht/client/interface.go",
        include_bytes!("../../../../internal/protocol/dht/client/interface.go"),
    ),
    (
        "internal/protocol/dht/ktable/command.go",
        include_bytes!("../../../../internal/protocol/dht/ktable/command.go"),
    ),
    (
        "internal/protocol/dht/ktable/node.go",
        include_bytes!("../../../../internal/protocol/dht/ktable/node.go"),
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
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Oracle {
    composition: String,
    determinism: String,
    lane: String,
    client: String,
    table: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Input {
    kind: String,
    node: Option<FixtureNode>,
    ping_outcome: Option<String>,
    response_id: Option<String>,
    cancel_before_return: Option<bool>,
    lane_return_error: Option<bool>,
    table_setup: Option<Vec<TableSetup>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureNode {
    id: String,
    addr: FixtureAddress,
    state: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FixtureAddress {
    ip: String,
    port: u16,
    scope: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableSetup {
    kind: String,
    id: String,
    addr: FixtureAddress,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Expected {
    node_calls: NodeCalls,
    ping_calls: Vec<FixtureAddress>,
    same_context: bool,
    batch_calls: usize,
    commands: Vec<Command>,
    run_returned: bool,
    context_cancelled: bool,
    advertised_node_survived: bool,
    source: Option<Source>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct NodeCalls {
    dropped: usize,
    time: usize,
    id: usize,
    addr: usize,
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
struct Source {
    run_error_ignored: bool,
    guard_dropped_first: bool,
    guard_uses_strict_after: bool,
    threshold_uses_now_minus_configured: bool,
    node_id_initialized_zero: bool,
    error_before_response_projection: bool,
    success_uses_node_responded_option: bool,
    no_post_ping_cancellation_check: bool,
    production_capacity: usize,
    production_concurrency: usize,
    run_dequeues_before_acquire: bool,
    run_spawns_callbacks: bool,
    run_joins_callbacks: bool,
    generic_closed_input_repeats_receive: bool,
    ping_closed_input_outcome: String,
    maximum_retained_work: String,
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

fn table_contains(table: &KTable, id: Id20) -> bool {
    table.node_handle(id).is_some()
}

#[test]
fn fixture_schema_identity_sources_and_excluded_rows_are_frozen() {
    assert_eq!(sha256(FIXTURE_BYTES), FIXTURE_SHA256);
    let fixtures = fixtures();
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.id.as_str())
            .collect::<Vec<_>>(),
        FIXTURE_IDS
    );
    assert_eq!(DEFERRED_HANDLE_ROWS, [FIXTURE_IDS[1], FIXTURE_IDS[2]]);
    assert_eq!(GO_ONLY_LANE_ROWS, [FIXTURE_IDS[8]]);
    assert_eq!(
        GO_ONLY_FIXTURE_METADATA,
        [
            "expected.nodeCalls",
            "expected.sameContext",
            "expected.batchCalls",
            "expected.commands[].optionCount",
            "expected.commands[].reason",
            "expected.commands[].errorIdentityPreserved",
        ]
    );

    for fixture in &fixtures {
        assert_eq!(fixture.subsystem, "dht_crawler_ping");
        assert_oracle_shape(fixture);
    }
    assert_source_fixture(&fixtures[0]);
    assert_guard_fixture(
        &fixtures[1],
        "dropped",
        NodeCalls {
            dropped: 1,
            time: 0,
            id: 0,
            addr: 0,
        },
    );
    assert_guard_fixture(
        &fixtures[2],
        "recent",
        NodeCalls {
            dropped: 1,
            time: 1,
            id: 0,
            addr: 0,
        },
    );
    for fixture in &fixtures[3..8] {
        assert_state_free_worker_metadata(fixture);
    }
    assert_lane_error_fixture(&fixtures[8]);
}

fn assert_oracle_shape(fixture: &Fixture) {
    let Oracle {
        composition,
        determinism,
        lane,
        client,
        table,
    } = &fixture.oracle;
    if fixture.id == FIXTURE_IDS[0] {
        assert_eq!(composition, "source_and_factory_freshness_gate");
        assert_eq!(
            determinism,
            "exact_source_sha256_and_required_source_shapes"
        );
        assert_eq!(lane, "production_buffered_concurrent_channel");
        assert_eq!(client, "production_dht_client_interface");
        assert_eq!(table, "production_ktable_batch_command");
    } else {
        assert_eq!(
            composition,
            "actual_crawler_runPing_with_manual_single_callback_lane"
        );
        assert_eq!(determinism, "synchronous_callback_and_scripted_client");
        assert_eq!(lane, "manual_single_callback_interface_implementation");
        assert_eq!(client, "scripted_client_Client_ping_override");
        assert_eq!(table, "tracing_wrapper_over_actual_ktable");
    }
}

fn assert_source_fixture(fixture: &Fixture) {
    let Input {
        kind,
        node,
        ping_outcome,
        response_id,
        cancel_before_return,
        lane_return_error,
        table_setup,
    } = &fixture.input;
    assert_eq!(kind, "source_contract");
    assert!(node.is_none());
    assert!(ping_outcome.is_none());
    assert!(response_id.is_none());
    assert!(cancel_before_return.is_none());
    assert!(lane_return_error.is_none());
    assert!(table_setup.is_none());

    let expected = &fixture.expected;
    assert_eq!(expected.node_calls, NodeCalls::default());
    assert!(expected.ping_calls.is_empty());
    assert!(!expected.same_context);
    assert_eq!(expected.batch_calls, 0);
    assert!(expected.commands.is_empty());
    assert!(expected.run_returned);
    assert!(!expected.context_cancelled);
    assert!(!expected.advertised_node_survived);
    let source = expected.source.as_ref().expect("source row source facts");
    assert!(source.run_error_ignored);
    assert!(source.guard_dropped_first);
    assert!(source.guard_uses_strict_after);
    assert!(source.threshold_uses_now_minus_configured);
    assert!(source.node_id_initialized_zero);
    assert!(source.error_before_response_projection);
    assert!(source.success_uses_node_responded_option);
    assert!(source.no_post_ping_cancellation_check);
    assert_eq!(source.production_capacity, 10);
    assert_eq!(source.production_concurrency, 10);
    assert!(source.run_dequeues_before_acquire);
    assert!(source.run_spawns_callbacks);
    assert!(!source.run_joins_callbacks);
    assert!(source.generic_closed_input_repeats_receive);
    assert_eq!(
        source.ping_closed_input_outcome,
        "nil_node_callback_panics_process"
    );
    assert_eq!(
        source.maximum_retained_work,
        "capacity_plus_concurrency_plus_one_acquire_waiter"
    );
    assert_eq!(
        source.evidence,
        "real runPing rows plus exact Go source freshness; production executor facts are source-shaped because callback scheduling is nondeterministic"
    );
    assert_eq!(source.source_sha256.len(), GO_SOURCES.len());
    for (path, bytes) in GO_SOURCES {
        assert_eq!(
            source.source_sha256.get(path).map(String::as_str),
            Some(sha256(bytes).as_str()),
            "stale Go source {path}"
        );
    }
    assert_eq!(
        DhtDiscoveredNodePingWorkerConfig::default().max_inflight,
        NonZeroUsize::new(source.production_concurrency).unwrap()
    );
    assert_eq!(
        crate::DhtDiscoveredNodeSchedulerConfig::default()
            .ping_capacity
            .get(),
        source.production_capacity
    );
    assert_eq!(
        DELIBERATE_RUST_HARDENING_DELTAS,
        [
            "capacity blocks intake before dequeue; retained work is route capacity plus max_inflight",
            "query tasks are owned and joined on EOF or aborted and joined on shutdown",
            "closed input is a typed InputClosed exit rather than repeated nil receives and panic",
        ]
    );
}

fn assert_guard_fixture(fixture: &Fixture, state: &str, node_calls: NodeCalls) {
    let input = &fixture.input;
    assert_eq!(input.kind, "run_ping");
    let node = input.node.as_ref().expect("guard row node");
    assert_eq!(node.state, state);
    let _ = fixture_id(&node.id);
    let _ = socket_addr(&node.addr);
    assert!(input.ping_outcome.is_none());
    assert_eq!(
        input.response_id.as_deref(),
        Some(Id20::ZERO.to_hex().as_str())
    );
    assert!(input.cancel_before_return.is_none());
    assert!(input.lane_return_error.is_none());
    assert!(input.table_setup.is_none());
    let expected = &fixture.expected;
    assert_eq!(expected.node_calls, node_calls);
    assert!(expected.ping_calls.is_empty());
    assert!(!expected.same_context);
    assert_eq!(expected.batch_calls, 0);
    assert!(expected.commands.is_empty());
    assert!(expected.run_returned);
    assert!(!expected.context_cancelled);
    assert!(!expected.advertised_node_survived);
    assert!(expected.source.is_none());
}

fn assert_state_free_worker_metadata(fixture: &Fixture) {
    let input = &fixture.input;
    assert_eq!(input.kind, "run_ping");
    let node = input.node.as_ref().expect("worker row node");
    assert_eq!(node.state, "old");
    let _ = fixture_id(&node.id);
    assert_eq!(input.lane_return_error, None);
    let outcome = input.ping_outcome.as_deref().expect("ping outcome");
    assert!(matches!(outcome, "success" | "error"));
    let _ = fixture_id(input.response_id.as_deref().expect("response ID"));
    let cancelled = fixture.id == "cancelled_after_success_still_puts";
    assert_eq!(
        input.cancel_before_return,
        cancelled.then_some(true),
        "cancel field presence is part of the fixture contract"
    );
    if matches!(
        fixture.id.as_str(),
        "old_mismatched_id_drops_advertised_id" | "ping_error_drops_zero_not_advertised_id"
    ) {
        let setup = input.table_setup.as_ref().expect("seeded table setup");
        assert_eq!(setup.len(), 1);
        assert_eq!(setup[0].kind, "put_node");
        assert_eq!(setup[0].id, node.id);
        assert_eq!(setup[0].addr, node.addr);
    } else {
        assert!(input.table_setup.is_none());
    }

    let expected = &fixture.expected;
    assert_eq!(expected.node_calls, expected_go_node_calls(&fixture.id));
    assert_eq!(expected.ping_calls, vec![node.addr.clone()]);
    assert!(expected.same_context);
    assert_eq!(expected.batch_calls, 1);
    assert_eq!(expected.commands.len(), 1);
    assert!(expected.run_returned);
    assert_eq!(expected.context_cancelled, cancelled);
    assert!(expected.source.is_none());
    assert_go_command_metadata(fixture, &expected.commands[0]);
}

fn expected_go_node_calls(id: &str) -> NodeCalls {
    match id {
        "old_zero_id_success_learns_response_id" => NodeCalls {
            dropped: 1,
            time: 1,
            id: 1,
            addr: 2,
        },
        "old_matching_id_success_marks_responded" | "cancelled_after_success_still_puts" => {
            NodeCalls {
                dropped: 1,
                time: 1,
                id: 2,
                addr: 2,
            }
        }
        "old_mismatched_id_drops_advertised_id" => NodeCalls {
            dropped: 1,
            time: 1,
            id: 3,
            addr: 1,
        },
        "ping_error_drops_zero_not_advertised_id" => NodeCalls {
            dropped: 1,
            time: 1,
            id: 0,
            addr: 1,
        },
        id => panic!("no Go accessor counts for non-executable oracle row {id}"),
    }
}

fn assert_go_command_metadata(fixture: &Fixture, command: &Command) {
    let node = fixture.input.node.as_ref().unwrap();
    let response_id = fixture_id(fixture.input.response_id.as_deref().unwrap());
    match fixture.id.as_str() {
        "old_zero_id_success_learns_response_id"
        | "old_matching_id_success_marks_responded"
        | "cancelled_after_success_still_puts" => {
            assert_eq!(command.kind, "put_node");
            assert_eq!(fixture_id(&command.id), response_id);
            assert_eq!(command.addr.as_ref(), Some(&node.addr));
            assert_eq!(command.option_count, 1);
            assert!(command.reason.is_empty());
            assert!(!command.error_identity_preserved);
            assert!(command.stored_responded);
            assert_eq!(
                fixture.expected.advertised_node_survived,
                fixture.id != "old_zero_id_success_learns_response_id"
            );
        }
        "old_mismatched_id_drops_advertised_id" => {
            assert_eq!(command.kind, "drop_node");
            assert_eq!(command.id, node.id);
            assert!(command.addr.is_none());
            assert_eq!(command.option_count, 0);
            assert_eq!(
                command.reason,
                "failed to respond to ping: node responded with a mismatching ID"
            );
            assert!(!command.error_identity_preserved);
            assert!(!command.stored_responded);
            assert!(!fixture.expected.advertised_node_survived);
        }
        "ping_error_drops_zero_not_advertised_id" => {
            assert_eq!(command.kind, "drop_node");
            assert_eq!(fixture_id(&command.id), Id20::ZERO);
            assert!(command.addr.is_none());
            assert_eq!(command.option_count, 0);
            assert_eq!(
                command.reason,
                "failed to respond to ping: oracle ping failure"
            );
            assert!(command.error_identity_preserved);
            assert!(!command.stored_responded);
            assert!(fixture.expected.advertised_node_survived);
        }
        id => panic!("unexpected state-free worker row {id}"),
    }
}

fn assert_lane_error_fixture(fixture: &Fixture) {
    let input = &fixture.input;
    assert_eq!(input.kind, "run_ping");
    assert!(input.node.is_none());
    assert!(input.ping_outcome.is_none());
    assert_eq!(
        input.response_id.as_deref(),
        Some(Id20::ZERO.to_hex().as_str())
    );
    assert!(input.cancel_before_return.is_none());
    assert_eq!(input.lane_return_error, Some(true));
    assert!(input.table_setup.is_none());
    let expected = &fixture.expected;
    assert_eq!(expected.node_calls, NodeCalls::default());
    assert!(expected.ping_calls.is_empty());
    assert!(!expected.same_context);
    assert_eq!(expected.batch_calls, 0);
    assert!(expected.commands.is_empty());
    assert!(expected.run_returned);
    assert!(!expected.context_cancelled);
    assert!(!expected.advertised_node_survived);
    assert!(expected.source.is_none());
}

#[tokio::test]
async fn rust_worker_consumes_every_state_free_oracle_row_and_types_lane_eof() {
    let fixtures = fixtures();
    for fixture in &fixtures[3..8] {
        run_state_free_worker_row(fixture).await;
    }

    let (sender, receiver) = DhtDiscoveredNodeRouteReceiver::test_channel(1);
    drop(sender);
    let table = KTable::new(fixture_id("00000000000000000000000000000000000000fa"));
    let stats = DhtDiscoveredNodePingStatsHandle::default();
    let mut core = DhtDiscoveredNodePingWorkerCore::new(
        receiver,
        table,
        NonZeroUsize::new(1).unwrap(),
        stats.clone(),
    );
    let exit = core
        .run_with_query(
            pending(),
            |_| -> std::future::Ready<Result<PingResult, ()>> {
                panic!("closed Rust route must not construct a query")
            },
        )
        .await;
    assert_eq!(exit, DhtDiscoveredNodePingWorkerExit::InputClosed);
    assert_eq!(stats.snapshot(), DhtDiscoveredNodePingStats::default());
}

async fn run_state_free_worker_row(fixture: &Fixture) {
    let input = &fixture.input;
    let node = input.node.as_ref().unwrap();
    let input_id = fixture_id(&node.id);
    let input_addr = socket_addr(&node.addr);
    let response_id = fixture_id(input.response_id.as_deref().unwrap());
    let outcome = input.ping_outcome.clone().unwrap();
    let cancel_after_query = input.cancel_before_return.unwrap_or(false);
    let command = &fixture.expected.commands[0];

    let response = if outcome == "success" {
        Ok(response_id)
    } else {
        Err(())
    };
    let expected_decision = match command.kind.as_str() {
        "put_node" => PingDecision::Put {
            id: fixture_id(&command.id),
        },
        "drop_node" => PingDecision::Drop {
            id: fixture_id(&command.id),
            mismatch: fixture.id == "old_mismatched_id_drops_advertised_id",
        },
        kind => panic!("unexpected fixture command kind {kind}"),
    };
    assert_eq!(
        ping_decision(input_id, response),
        expected_decision,
        "fixture outcome must select the exact Rust command for {}",
        fixture.id
    );

    let table = KTable::new(fixture_id("00000000000000000000000000000000000000fa"));
    if let Some(setup) = &input.table_setup {
        for entry in setup {
            assert_eq!(entry.kind, "put_node");
            assert_eq!(
                table.put_node(RoutingNode {
                    id: fixture_id(&entry.id),
                    addr: socket_addr(&entry.addr),
                }),
                RoutingPutResult::Accepted
            );
            let seeded = table
                .node_handle(fixture_id(&entry.id))
                .expect("accepted setup node");
            assert_eq!(seeded.addr(), socket_addr(&entry.addr));
            assert!(seeded.last_responded_at().is_none());
        }
    }

    let (sender, receiver) = DhtDiscoveredNodeRouteReceiver::test_channel(1);
    sender
        .send(RoutingNode {
            id: input_id,
            addr: input_addr,
        })
        .await
        .unwrap();
    drop(sender);
    let stats = DhtDiscoveredNodePingStatsHandle::default();
    let mut core = DhtDiscoveredNodePingWorkerCore::new(
        receiver,
        table.clone(),
        NonZeroUsize::new(1).unwrap(),
        stats.clone(),
    );
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let mut shutdown_sender = Some(shutdown_sender);
    let exit = core
        .run_with_query(
            async move {
                if cancel_after_query {
                    let _ = shutdown_receiver.await;
                } else {
                    pending::<()>().await;
                }
            },
            move |remote| {
                assert_eq!(remote, input_addr);
                let outcome = outcome.clone();
                let shutdown_sender = shutdown_sender.take();
                async move {
                    if cancel_after_query {
                        let _ = shutdown_sender.unwrap().send(());
                    }
                    if outcome == "success" {
                        Ok(PingResult { id: response_id })
                    } else {
                        Err(())
                    }
                }
            },
        )
        .await;

    let expected_exit = if cancel_after_query {
        DhtDiscoveredNodePingWorkerExit::Shutdown {
            queued_dropped: 0,
            queries_cancelled: 0,
        }
    } else {
        DhtDiscoveredNodePingWorkerExit::InputClosed
    };
    assert_eq!(exit, expected_exit, "{}", fixture.id);

    let put = command.kind == "put_node";
    assert_eq!(
        stats.snapshot(),
        expected_rust_stats(&fixture.id),
        "{}",
        fixture.id
    );

    assert_eq!(
        table_contains(&table, input_id),
        fixture.expected.advertised_node_survived,
        "{}",
        fixture.id
    );
    if put {
        let stored_id = fixture_id(&command.id);
        let stored = table.node_handle(stored_id).expect("put response node");
        assert_eq!(stored.addr(), input_addr);
        assert!(stored.last_responded_at().is_some());
    } else {
        assert!(!table_contains(&table, response_id));
        assert!(!table_contains(&table, fixture_id(&command.id)));
        if fixture.id == "ping_error_drops_zero_not_advertised_id" {
            let surviving = table
                .node_handle(input_id)
                .expect("advertised node survives Go-compatible zero-ID drop");
            assert_eq!(surviving.addr(), input_addr);
            assert!(surviving.last_responded_at().is_none());
        }
    }
}

fn expected_rust_stats(id: &str) -> DhtDiscoveredNodePingStats {
    let mut stats = DhtDiscoveredNodePingStats {
        dequeued: 1,
        queries_started: 1,
        ..DhtDiscoveredNodePingStats::default()
    };
    match id {
        "old_zero_id_success_learns_response_id"
        | "old_matching_id_success_marks_responded"
        | "cancelled_after_success_still_puts" => {
            stats.queries_succeeded = 1;
            stats.put_commands = 1;
        }
        "old_mismatched_id_drops_advertised_id" => {
            stats.queries_succeeded = 1;
            stats.id_mismatches = 1;
            stats.drop_commands = 1;
        }
        "ping_error_drops_zero_not_advertised_id" => {
            stats.queries_failed = 1;
            stats.drop_commands = 1;
        }
        id => panic!("no Rust stats for non-executable oracle row {id}"),
    }
    stats
}
