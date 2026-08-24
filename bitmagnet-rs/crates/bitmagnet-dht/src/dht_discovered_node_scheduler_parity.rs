use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::num::NonZeroUsize;
use std::time::Duration;

use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::task::JoinHandle;

use super::*;
use crate::{
    dht_discovery_channel, DhtDiscoveryOffer, Id20, KTableHashPeer, RoutingPutResult,
    DHT_DISCOVERY_QUEUE_CAPACITY,
};

const FIXTURE_TEXT: &str =
    include_str!("../../../../testdata/parity/dht/dht_crawler_discovered_nodes.jsonl");
const FIXTURE_BYTES: &[u8] =
    include_bytes!("../../../../testdata/parity/dht/dht_crawler_discovered_nodes.jsonl");
const FIXTURE_SHA256: &str = "ae6d867378a227284aa0cd93e9120d70afbec1c5e3b19a9f64e09edace4190e0";
const FIXTURE_IDS: [&str; 10] = [
    "production_factory_defaults_and_source_lifecycle",
    "size_flush_order_and_output_backpressure",
    "first_ip_wins_known_filter_and_only_ping_ready",
    "cross_batch_dedupe_resets_and_all_known_continues",
    "only_find_node_ready",
    "only_sample_infohashes_ready",
    "all_ready_routes_each_node_exactly_once",
    "all_full_then_ping_drain_routes",
    "cancel_after_one_delivery_abandons_blocked_suffix",
    "blocked_route_cancellation_exits_without_delivery",
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
    ingress: String,
    routes: String,
    address_projection: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Input {
    kind: String,
    scaling_factor: Option<u64>,
    values: Option<Vec<i64>>,
    batches: Option<Vec<Vec<FixtureNode>>>,
    table_setup: Option<Vec<TableSetup>>,
    ready_lanes: Option<Vec<String>>,
    route_setup: Option<String>,
    cancel_phase: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureNode {
    ordinal: u64,
    id: String,
    addr: FixtureAddress,
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
#[serde(deny_unknown_fields)]
struct Expected {
    factory: Option<FactoryExpected>,
    source: Option<SourceExpected>,
    batching: Option<BatchingExpected>,
    crawler: Option<CrawlerExpected>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct FactoryExpected {
    input_capacity: usize,
    max_batch_size: usize,
    ticker_interval_ms: u64,
    output_capacity: usize,
    ping_capacity: usize,
    ping_concurrency: usize,
    find_node_capacity: usize,
    find_node_concurrency: usize,
    sample_infohashes_capacity: usize,
    sample_infohashes_concurrency: usize,
    timing_measured: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SourceExpected {
    ticker_starts_at_construction: bool,
    ticker_and_input_select_unbiased: bool,
    ticker_interval_is_strict_deadline: bool,
    ticker_flushes_nonempty_buffer: bool,
    empty_ticker_does_not_flush: bool,
    flush_resets_before_output_send: bool,
    input_close_breaks_only_select: bool,
    input_close_partial_buffer_outcome: String,
    output_close_deferred_unreached: bool,
    crawler_output_receive_checks_ok: bool,
    closed_output_can_spin: bool,
    routing_tie_outcome: String,
    filter_cancellation: String,
    filter_result_trust: String,
    closed_worker_selection: String,
    state_after_filter: String,
    source_sha256: BTreeMap<String, String>,
    evidence: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct BatchingExpected {
    size_batch: Vec<i64>,
    output_batch_while_blocked: Vec<i64>,
    held_batch_while_blocked: Vec<i64>,
    buffered_inputs_while_blocked: Vec<i64>,
    send_would_block_before_drain: Vec<i64>,
    send_completed_after_drain: Vec<i64>,
    remaining_batches: Vec<Vec<i64>>,
    dropped: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CrawlerExpected {
    batches: Vec<BatchExpected>,
    routing: RoutingExpected,
    table_mutator_calls: u64,
    filter_calls: u64,
    exited: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct BatchExpected {
    dedupe: Vec<DedupeExpected>,
    filter_input: Vec<FixtureAddress>,
    filter_output: Vec<FixtureAddress>,
    deliveries: Vec<DeliveryExpected>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DedupeExpected {
    key: String,
    winner_ordinal: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveryExpected {
    ordinal: u64,
    lane: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RoutingExpected {
    allowed_lanes: Vec<String>,
    exactly_once: bool,
    per_lane_preserves_order: bool,
    ready_tie_unspecified: bool,
    cancelled_undelivered: usize,
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
fn fixture_schema_identity_defaults_and_go_sources_are_frozen() {
    assert_eq!(sha256(FIXTURE_BYTES), FIXTURE_SHA256);
    let fixtures = fixtures();
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.id.as_str())
            .collect::<Vec<_>>(),
        FIXTURE_IDS
    );

    for fixture in &fixtures {
        assert_eq!(fixture.subsystem, "dht_crawler_discovered_nodes");
        let Oracle {
            composition,
            determinism,
            ingress,
            routes,
            address_projection,
        } = &fixture.oracle;
        assert!(!composition.is_empty());
        assert!(!determinism.is_empty());
        assert!(!ingress.is_empty());
        assert!(!routes.is_empty());
        assert!(matches!(
            address_projection.as_str(),
            "netip_address_plus_numeric_zone_port_discarded_textual_zones_and_rust_flowinfo_excluded"
                | "not_applicable"
        ));
        assert_fixture_shape(fixture);
    }

    assert_factory_and_source(&fixtures[0]);
    assert_batching_evidence(&fixtures[1]);
}

fn assert_fixture_shape(fixture: &Fixture) {
    let Input {
        kind,
        scaling_factor,
        values,
        batches,
        table_setup,
        ready_lanes,
        route_setup,
        cancel_phase,
    } = &fixture.input;
    let Expected {
        factory,
        source,
        batching,
        crawler,
    } = &fixture.expected;

    match fixture.id.as_str() {
        "production_factory_defaults_and_source_lifecycle" => {
            assert_eq!(kind, "factory");
            assert_eq!(*scaling_factor, Some(10));
            assert!(values.is_none());
            assert!(batches.is_none());
            assert!(table_setup.is_none());
            assert!(ready_lanes.is_none());
            assert!(route_setup.is_none());
            assert!(cancel_phase.is_none());
            assert!(factory.is_some());
            assert!(source.is_some());
            assert!(batching.is_none());
            assert!(crawler.is_none());
        }
        "size_flush_order_and_output_backpressure" => {
            assert_eq!(kind, "batching");
            assert!(scaling_factor.is_none());
            assert!(values.is_some());
            assert!(batches.is_none());
            assert!(table_setup.is_none());
            assert!(ready_lanes.is_none());
            assert!(route_setup.is_none());
            assert!(cancel_phase.is_none());
            assert!(factory.is_none());
            assert!(source.is_none());
            assert!(batching.is_some());
            assert!(crawler.is_none());
        }
        id => {
            assert_eq!(kind, "crawler");
            assert!(scaling_factor.is_none());
            assert!(values.is_none());
            assert!(batches.is_some());
            assert!(cancel_phase.is_some());
            assert!(factory.is_none());
            assert!(source.is_none());
            assert!(batching.is_none());
            assert!(crawler.is_some());
            let expected_cancel_phase = match id {
                "cancel_after_one_delivery_abandons_blocked_suffix" => {
                    "after_one_delivery_next_route_blocked"
                }
                "blocked_route_cancellation_exits_without_delivery" => "blocked_route_after_filter",
                _ => "after_all_deliveries",
            };
            assert_eq!(cancel_phase.as_deref(), Some(expected_cancel_phase));
            match id {
                "first_ip_wins_known_filter_and_only_ping_ready"
                | "cross_batch_dedupe_resets_and_all_known_continues" => {
                    assert!(table_setup.is_some());
                    assert!(ready_lanes.is_some());
                    assert!(route_setup.is_none());
                }
                "only_find_node_ready"
                | "only_sample_infohashes_ready"
                | "all_ready_routes_each_node_exactly_once"
                | "cancel_after_one_delivery_abandons_blocked_suffix" => {
                    assert!(table_setup.is_none());
                    assert!(ready_lanes.is_some());
                    assert!(route_setup.is_none());
                }
                "all_full_then_ping_drain_routes" => {
                    assert!(table_setup.is_none());
                    assert!(ready_lanes.is_none());
                    assert_eq!(
                        route_setup.as_deref(),
                        Some("all_three_capacity_one_prefilled_then_ping_drained")
                    );
                }
                "blocked_route_cancellation_exits_without_delivery" => {
                    assert!(table_setup.is_none());
                    assert!(ready_lanes.is_none());
                    assert!(route_setup.is_none());
                }
                _ => panic!("unexpected crawler fixture {id}"),
            }
        }
    }
}

fn assert_factory_and_source(fixture: &Fixture) {
    let factory = fixture.expected.factory.as_ref().unwrap();
    assert_eq!(factory.input_capacity, DHT_DISCOVERY_QUEUE_CAPACITY);
    assert_eq!(factory.max_batch_size, 10);
    assert_eq!(factory.ticker_interval_ms, 10);
    assert_eq!(factory.output_capacity, 1);
    assert_eq!(factory.ping_capacity, 10);
    assert_eq!(factory.ping_concurrency, 10);
    assert_eq!(factory.find_node_capacity, 100);
    assert_eq!(factory.find_node_concurrency, 100);
    assert_eq!(factory.sample_infohashes_capacity, 100);
    assert_eq!(factory.sample_infohashes_concurrency, 100);
    assert!(!factory.timing_measured);

    let rust = DhtDiscoveredNodeSchedulerConfig::default();
    assert_eq!(rust.max_batch_size.get(), factory.max_batch_size);
    assert_eq!(
        rust.batch_interval.as_millis(),
        u128::from(factory.ticker_interval_ms)
    );
    assert_eq!(rust.ping_capacity.get(), factory.ping_capacity);
    assert_eq!(rust.find_node_capacity.get(), factory.find_node_capacity);
    assert_eq!(
        rust.sample_infohashes_capacity.get(),
        factory.sample_infohashes_capacity
    );

    let source = fixture.expected.source.as_ref().unwrap();
    assert!(source.ticker_starts_at_construction);
    assert!(source.ticker_and_input_select_unbiased);
    assert!(!source.ticker_interval_is_strict_deadline);
    assert!(source.ticker_flushes_nonempty_buffer);
    assert!(source.empty_ticker_does_not_flush);
    assert!(source.flush_resets_before_output_send);
    assert!(source.input_close_breaks_only_select);
    assert_eq!(
        source.input_close_partial_buffer_outcome,
        "unspecified_tick_may_or_may_not_win_while_closed_input_spins"
    );
    assert!(source.output_close_deferred_unreached);
    assert!(!source.crawler_output_receive_checks_ok);
    assert!(source.closed_output_can_spin);
    assert_eq!(
        source.routing_tie_outcome,
        "cancel_vs_ready_lane_and_cancel_vs_batch_unspecified"
    );
    assert_eq!(
        source.filter_cancellation,
        "synchronous_filter_call_is_not_cancellation_aware"
    );
    assert_eq!(
        source.filter_result_trust,
        "returned_order_duplicates_and_unknown_keys_are_trusted"
    );
    assert_eq!(
        source.closed_worker_selection,
        "selected_send_to_closed_worker_channel_panics"
    );
    assert_eq!(source.state_after_filter, "not_rechecked_before_route");
    assert_eq!(
        source.evidence,
        "reflection_plus_go_ast_plus_exact_source_digests_no_busy_loop_execution"
    );

    let go_sources: [(&str, &[u8]); 5] = [
        (
            "internal/concurrency/batching_channel.go",
            include_bytes!("../../../../internal/concurrency/batching_channel.go"),
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
            "internal/dhtcrawler/discovered_nodes.go",
            include_bytes!("../../../../internal/dhtcrawler/discovered_nodes.go"),
        ),
        (
            "internal/dhtcrawler/factory.go",
            include_bytes!("../../../../internal/dhtcrawler/factory.go"),
        ),
    ];
    assert_eq!(source.source_sha256.len(), go_sources.len());
    for (path, bytes) in go_sources {
        assert_eq!(
            source.source_sha256.get(path).map(String::as_str),
            Some(sha256(bytes).as_str()),
            "Go source digest drifted for {path}"
        );
    }
}

fn assert_batching_evidence(fixture: &Fixture) {
    let values = fixture.input.values.as_ref().unwrap();
    let batching = fixture.expected.batching.as_ref().unwrap();
    assert_eq!(values, &(0_i64..=9).collect::<Vec<_>>());
    assert_eq!(batching.size_batch, *values);
    assert_eq!(batching.output_batch_while_blocked, vec![1]);
    assert_eq!(batching.held_batch_while_blocked, vec![2]);
    assert_eq!(batching.buffered_inputs_while_blocked, vec![3, 4]);
    assert_eq!(batching.send_would_block_before_drain, vec![5]);
    assert_eq!(batching.send_completed_after_drain, vec![5]);
    assert_eq!(
        batching.remaining_batches,
        vec![vec![2], vec![3], vec![4], vec![5]]
    );
    assert_eq!(batching.dropped, 0);
}

#[tokio::test]
async fn rust_scheduler_consumes_every_crawler_oracle_row() {
    let fixtures = fixtures();
    for fixture in &fixtures[2..] {
        execute_crawler_fixture(fixture).await;
    }
}

async fn execute_crawler_fixture(fixture: &Fixture) {
    let input_batches = fixture.input.batches.as_ref().unwrap();
    assert!(!input_batches.is_empty(), "{}", fixture.id);
    let batch_size = input_batches[0].len();
    assert!(batch_size > 0, "{}", fixture.id);
    assert!(
        input_batches.iter().all(|batch| batch.len() == batch_size),
        "fixture batches must have one fixed scheduler size: {}",
        fixture.id
    );
    let expected = fixture.expected.crawler.as_ref().unwrap();
    assert_eq!(
        expected.batches.len(),
        input_batches.len(),
        "{}",
        fixture.id
    );
    assert_eq!(expected.filter_calls, expected.batches.len() as u64);
    assert_eq!(expected.table_mutator_calls, 0);
    assert!(expected.exited);
    let allowed = expected
        .routing
        .allowed_lanes
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    if let Some(ready) = fixture.input.ready_lanes.as_ref() {
        assert_eq!(
            allowed,
            ready.iter().map(String::as_str).collect::<HashSet<_>>(),
            "{}",
            fixture.id
        );
    } else if fixture.id == "all_full_then_ping_drain_routes" {
        assert_eq!(allowed, HashSet::from(["ping"]));
    } else {
        assert!(allowed.is_empty(), "{}", fixture.id);
    }

    let table = KTable::new(Id20::ZERO);
    apply_table_setup(&table, fixture.input.table_setup.as_deref().unwrap_or(&[]));
    let table_before = table_shape(&table);
    validate_expected_batches(input_batches, expected, &table, &fixture.id);

    let raw_nodes = input_batches.iter().flatten().cloned().collect::<Vec<_>>();
    let expected_deliveries = expected
        .batches
        .iter()
        .flat_map(|batch| batch.deliveries.iter())
        .collect::<Vec<_>>();
    let route_capacity = if matches!(
        fixture.id.as_str(),
        "all_full_then_ping_drain_routes"
            | "cancel_after_one_delivery_abandons_blocked_suffix"
            | "blocked_route_cancellation_exits_without_delivery"
    ) {
        1
    } else {
        expected_deliveries.len().max(1)
    };
    let config = DhtDiscoveredNodeSchedulerConfig {
        max_batch_size: NonZeroUsize::new(batch_size).unwrap(),
        batch_interval: Duration::from_secs(60 * 60),
        ping_capacity: NonZeroUsize::new(route_capacity).unwrap(),
        find_node_capacity: NonZeroUsize::new(route_capacity).unwrap(),
        sample_infohashes_capacity: NonZeroUsize::new(route_capacity).unwrap(),
    };
    let (discovery, receiver) = dht_discovery_channel(NonZeroUsize::new(raw_nodes.len()).unwrap());
    let (scheduler, mut routes, stats) =
        DhtDiscoveredNodeScheduler::with_config(receiver, table.clone(), config).unwrap();
    configure_ready_lanes(&fixture.input, &mut routes);

    let dummy_nodes = if matches!(
        fixture.id.as_str(),
        "all_full_then_ping_drain_routes" | "blocked_route_cancellation_exits_without_delivery"
    ) {
        prefill_all_routes(&scheduler.routes)
    } else {
        Vec::new()
    };
    for fixture_node in &raw_nodes {
        assert_eq!(
            discovery.offer(routing_node(fixture_node)),
            DhtDiscoveryOffer::Queued,
            "{}",
            fixture.id
        );
    }
    drop(discovery);

    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(scheduler.run(async {
        let _ = shutdown_receiver.await;
    }));

    let exit = match fixture.id.as_str() {
        "all_full_then_ping_drain_routes" => {
            wait_for(&fixture.id, || {
                stats.snapshot().batches == 1 && routed_total(stats.snapshot()) == 0
            })
            .await;
            assert!(!task.is_finished(), "{}", fixture.id);
            assert_eq!(routes.ping.try_recv().unwrap(), dummy_nodes[0]);
            join_scheduler(&fixture.id, task).await
        }
        "cancel_after_one_delivery_abandons_blocked_suffix" => {
            wait_for(&fixture.id, || {
                let snapshot = stats.snapshot();
                snapshot.routed_ping == 1 && snapshot.route_attempts == 2
            })
            .await;
            assert!(!task.is_finished(), "{}", fixture.id);
            shutdown_sender.send(()).unwrap();
            join_scheduler(&fixture.id, task).await
        }
        "blocked_route_cancellation_exits_without_delivery" => {
            wait_for(&fixture.id, || {
                let snapshot = stats.snapshot();
                snapshot.filter_calls == 1
                    && snapshot.route_attempts == 1
                    && routed_total(snapshot) == 0
            })
            .await;
            assert!(!task.is_finished(), "{}", fixture.id);
            shutdown_sender.send(()).unwrap();
            join_scheduler(&fixture.id, task).await
        }
        _ => join_scheduler(&fixture.id, task).await,
    };

    match fixture.id.as_str() {
        "cancel_after_one_delivery_abandons_blocked_suffix"
        | "blocked_route_cancellation_exits_without_delivery" => {
            assert_eq!(
                exit,
                DhtDiscoveredNodeSchedulerExit::Shutdown {
                    pending_dropped: expected.routing.cancelled_undelivered
                },
                "{}",
                fixture.id
            );
        }
        _ => assert_eq!(
            exit,
            DhtDiscoveredNodeSchedulerExit::InputClosed,
            "{}",
            fixture.id
        ),
    }

    let dummy_ids = dummy_nodes
        .iter()
        .map(|node| node.id)
        .collect::<HashSet<_>>();
    let actual = drain_deliveries(&mut routes)
        .into_iter()
        .filter(|(_, node)| !dummy_ids.contains(&node.id))
        .collect::<Vec<_>>();
    assert_deliveries(fixture, &raw_nodes, expected, &actual);
    assert_eq!(table_shape(&table), table_before, "{}", fixture.id);
    assert_stats(fixture, &raw_nodes, expected, stats.snapshot(), &actual);
}

fn apply_table_setup(table: &KTable, setup: &[TableSetup]) {
    for item in setup {
        let id = Id20::from_hex(&item.id).unwrap();
        let addr = socket_addr(&item.addr);
        match item.kind.as_str() {
            "put_node_once" => {
                assert_eq!(
                    table.put_node(RoutingNode { id, addr }),
                    RoutingPutResult::Accepted
                );
            }
            "put_node_twice" => {
                assert_eq!(
                    table.put_node(RoutingNode { id, addr }),
                    RoutingPutResult::Accepted
                );
                assert_eq!(
                    table.put_node(RoutingNode { id, addr }),
                    RoutingPutResult::AlreadyExists
                );
            }
            "put_hash_peer" => {
                assert_eq!(
                    table.put_hash(id, &[KTableHashPeer { addr }]),
                    RoutingPutResult::Accepted
                );
            }
            kind => panic!("unexpected table setup kind {kind}"),
        }
    }
}

fn validate_expected_batches(
    input_batches: &[Vec<FixtureNode>],
    expected: &CrawlerExpected,
    table: &KTable,
    id: &str,
) {
    for (input, expected_batch) in input_batches.iter().zip(&expected.batches) {
        let mut seen = HashSet::new();
        let mut dedupe = Vec::new();
        let mut filter_input = Vec::new();
        for node in input {
            let key = fixture_address_key(&node.addr);
            if seen.insert(key.clone()) {
                dedupe.push(DedupeExpected {
                    key,
                    winner_ordinal: node.ordinal,
                });
                filter_input.push(normalized_address(&node.addr));
            }
        }
        assert_eq!(expected_batch.dedupe, dedupe, "{id}");
        assert_eq!(expected_batch.filter_input, filter_input, "{id}");
        let filtered = table
            .filter_known_addrs(
                &expected_batch
                    .filter_input
                    .iter()
                    .map(socket_addr)
                    .collect::<Vec<_>>(),
            )
            .into_iter()
            .map(fixture_address)
            .collect::<Vec<_>>();
        assert_eq!(expected_batch.filter_output, filtered, "{id}");

        let filtered_keys = expected_batch
            .filter_output
            .iter()
            .map(fixture_address_key)
            .collect::<HashSet<_>>();
        let expected_ordinals = expected_batch
            .dedupe
            .iter()
            .filter(|winner| filtered_keys.contains(&winner.key))
            .map(|winner| winner.winner_ordinal)
            .collect::<Vec<_>>();
        let delivered_ordinals = expected_batch
            .deliveries
            .iter()
            .map(|delivery| delivery.ordinal)
            .collect::<Vec<_>>();
        assert_eq!(
            &expected_ordinals[..delivered_ordinals.len()],
            delivered_ordinals,
            "{id}"
        );
    }
}

fn configure_ready_lanes(input: &Input, routes: &mut DhtDiscoveredNodeRoutes) {
    let Some(ready) = input.ready_lanes.as_ref() else {
        return;
    };
    if !ready.iter().any(|lane| lane == "ping") {
        routes.ping.close();
    }
    if !ready.iter().any(|lane| lane == "find_node") {
        routes.find_node.close();
    }
    if !ready.iter().any(|lane| lane == "sample_infohashes") {
        routes.sample_infohashes.close();
    }
}

fn prefill_all_routes(routes: &RouteSenders) -> Vec<RoutingNode> {
    let nodes = vec![dummy_node(240), dummy_node(241), dummy_node(242)];
    routes.ping.try_send(nodes[0]).unwrap();
    routes.find_node.try_send(nodes[1]).unwrap();
    routes.sample_infohashes.try_send(nodes[2]).unwrap();
    nodes
}

fn dummy_node(value: u8) -> RoutingNode {
    let mut id = [0_u8; 20];
    id[19] = value;
    RoutingNode {
        id: Id20::from_slice(&id).unwrap(),
        addr: SocketAddr::V4(SocketAddrV4::new(
            std::net::Ipv4Addr::LOCALHOST,
            u16::from(value),
        )),
    }
}

async fn wait_for(id: &str, mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("scheduler did not reach the fixture barrier: {id}"));
}

async fn join_scheduler(
    id: &str,
    task: JoinHandle<DhtDiscoveredNodeSchedulerExit>,
) -> DhtDiscoveredNodeSchedulerExit {
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap_or_else(|_| panic!("scheduler did not terminate for fixture {id}"))
        .unwrap_or_else(|error| panic!("scheduler task failed for fixture {id}: {error}"))
}

fn drain_deliveries(routes: &mut DhtDiscoveredNodeRoutes) -> Vec<(&'static str, RoutingNode)> {
    let mut output = Vec::new();
    while let Ok(node) = routes.ping.try_recv() {
        output.push(("ping", node));
    }
    while let Ok(node) = routes.find_node.try_recv() {
        output.push(("find_node", node));
    }
    while let Ok(node) = routes.sample_infohashes.try_recv() {
        output.push(("sample_infohashes", node));
    }
    output
}

fn assert_deliveries(
    fixture: &Fixture,
    raw_nodes: &[FixtureNode],
    expected: &CrawlerExpected,
    actual: &[(&str, RoutingNode)],
) {
    let by_id = raw_nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<HashMap<_, _>>();
    let expected_by_ordinal = expected
        .batches
        .iter()
        .flat_map(|batch| &batch.deliveries)
        .map(|delivery| (delivery.ordinal, delivery.lane.as_str()))
        .collect::<BTreeMap<_, _>>();
    let input_position = raw_nodes
        .iter()
        .enumerate()
        .map(|(position, node)| (node.ordinal, position))
        .collect::<HashMap<_, _>>();
    let mut actual_ordinals = HashSet::new();
    let mut per_lane_positions: HashMap<&str, Vec<usize>> = HashMap::new();

    for (lane, node) in actual {
        let id = hex::encode(node.id.as_bytes());
        let fixture_node = by_id
            .get(id.as_str())
            .unwrap_or_else(|| panic!("unexpected node {id} in {}", fixture.id));
        assert_eq!(*node, routing_node(fixture_node), "{}", fixture.id);
        assert!(
            actual_ordinals.insert(fixture_node.ordinal),
            "duplicate delivery in {}",
            fixture.id
        );
        let expected_lane = expected_by_ordinal
            .get(&fixture_node.ordinal)
            .unwrap_or_else(|| panic!("unexpected ordinal in {}", fixture.id));
        if *expected_lane == "one_of_ready" {
            assert!(
                expected
                    .routing
                    .allowed_lanes
                    .iter()
                    .any(|allowed| allowed == lane),
                "{}",
                fixture.id
            );
        } else {
            assert_eq!(*lane, *expected_lane, "{}", fixture.id);
        }
        per_lane_positions
            .entry(lane)
            .or_default()
            .push(input_position[&fixture_node.ordinal]);
    }

    assert_eq!(
        actual_ordinals,
        expected_by_ordinal.keys().copied().collect::<HashSet<_>>(),
        "{}",
        fixture.id
    );
    assert!(expected.routing.per_lane_preserves_order);
    for positions in per_lane_positions.values() {
        assert!(
            positions.windows(2).all(|pair| pair[0] < pair[1]),
            "{}",
            fixture.id
        );
    }
    if expected.routing.exactly_once {
        assert_eq!(actual.len(), expected_by_ordinal.len(), "{}", fixture.id);
    }
    assert_eq!(
        expected.routing.ready_tie_unspecified,
        expected_by_ordinal
            .values()
            .any(|lane| *lane == "one_of_ready"),
        "{}",
        fixture.id
    );
}

fn assert_stats(
    fixture: &Fixture,
    raw_nodes: &[FixtureNode],
    expected: &CrawlerExpected,
    stats: DhtDiscoveredNodeSchedulerStats,
    actual: &[(&str, RoutingNode)],
) {
    let unique = expected
        .batches
        .iter()
        .map(|batch| batch.dedupe.len())
        .sum::<usize>();
    let unknown = expected
        .batches
        .iter()
        .map(|batch| batch.filter_output.len())
        .sum::<usize>();
    assert_eq!(stats.received, raw_nodes.len() as u64, "{}", fixture.id);
    assert_eq!(
        stats.batches,
        expected.batches.len() as u64,
        "{}",
        fixture.id
    );
    assert_eq!(
        stats.duplicate_dropped,
        raw_nodes.len().saturating_sub(unique) as u64,
        "{}",
        fixture.id
    );
    assert_eq!(
        stats.known_filtered,
        unique.saturating_sub(unknown) as u64,
        "{}",
        fixture.id
    );
    assert_eq!(stats.filter_calls, expected.filter_calls, "{}", fixture.id);
    assert_eq!(stats.route_attempts, unknown as u64, "{}", fixture.id);
    assert_eq!(
        stats.routed_ping,
        lane_count(actual, "ping"),
        "{}",
        fixture.id
    );
    assert_eq!(
        stats.routed_find_node,
        lane_count(actual, "find_node"),
        "{}",
        fixture.id
    );
    assert_eq!(
        stats.routed_sample_infohashes,
        lane_count(actual, "sample_infohashes"),
        "{}",
        fixture.id
    );
    assert_eq!(
        stats.shutdown_dropped, expected.routing.cancelled_undelivered as u64,
        "{}",
        fixture.id
    );
    assert_eq!(stats.routes_closed_dropped, 0, "{}", fixture.id);
    assert_eq!(
        unknown.saturating_sub(actual.len()),
        expected.routing.cancelled_undelivered,
        "{}",
        fixture.id
    );
}

fn lane_count(actual: &[(&str, RoutingNode)], lane: &str) -> u64 {
    actual.iter().filter(|(actual, _)| *actual == lane).count() as u64
}

fn routed_total(stats: DhtDiscoveredNodeSchedulerStats) -> u64 {
    stats.routed_ping + stats.routed_find_node + stats.routed_sample_infohashes
}

fn table_shape(table: &KTable) -> (usize, usize, usize) {
    (
        table.node_count(),
        table.hash_count(),
        table.reverse_address_count(),
    )
}

fn routing_node(node: &FixtureNode) -> RoutingNode {
    RoutingNode {
        id: Id20::from_hex(&node.id).unwrap(),
        addr: socket_addr(&node.addr),
    }
}

fn socket_addr(addr: &FixtureAddress) -> SocketAddr {
    match addr.ip.parse::<IpAddr>().unwrap() {
        IpAddr::V4(ip) => {
            assert_eq!(addr.scope, 0);
            SocketAddr::V4(SocketAddrV4::new(ip, addr.port))
        }
        IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(ip, addr.port, 0, addr.scope)),
    }
}

fn fixture_address(addr: SocketAddr) -> FixtureAddress {
    match addr {
        SocketAddr::V4(addr) => FixtureAddress {
            ip: addr.ip().to_string(),
            port: addr.port(),
            scope: 0,
        },
        SocketAddr::V6(addr) => FixtureAddress {
            ip: addr.ip().to_string(),
            port: addr.port(),
            scope: addr.scope_id(),
        },
    }
}

fn normalized_address(addr: &FixtureAddress) -> FixtureAddress {
    FixtureAddress {
        ip: addr.ip.clone(),
        port: 0,
        scope: addr.scope,
    }
}

fn fixture_address_key(addr: &FixtureAddress) -> String {
    match addr.ip.parse::<IpAddr>().unwrap() {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) if addr.scope == 0 => ip.to_string(),
        IpAddr::V6(ip) => format!("{ip}%{}", addr.scope),
    }
}
