//! Differential composition and lifecycle gates for full DHT dispatch and one send.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::panic::{self, AssertUnwindSafe};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use bitmagnet_dht::{
    send_dht_reply, send_ping_find_node_reply, ByteString, CompactAddr, CompactNode,
    DatagramSender, DhtDispatchOutcome, DhtDispatcher, DhtReply, DhtResponder, DhtResponderError,
    DhtResponderLookup, DhtResponderSample, DhtResponderTable, DhtSendError, Id20, KTable,
    KTableCommand, KTableHashPeer, KrpcError, KrpcMessage, MessageArgs, MessageReturn, NodeTable,
    PingFindNodeDispatchOutcome, PingFindNodeDispatcher, PingFindNodeError, PingFindNodeReply,
    PingFindNodeSendError, RoutingNode, RoutingPutResult, MAX_INBOUND_DATAGRAM_BYTES,
};
use serde::Deserialize;
use tokio::sync::oneshot;

const RESPONDER_FIXTURE_IDS: [&str; 40] = [
    "global_nil_arguments_precede_unknown_method",
    "unknown_method_with_arguments",
    "ping_missing_arguments",
    "ping_zero_requester_and_ignored_fields",
    "find_node_zero_target",
    "find_node_empty",
    "find_node_ordered_duplicate_nodes",
    "find_node_native_scoped_ipv6_projection",
    "get_peers_zero_infohash",
    "get_peers_miss_empty",
    "get_peers_miss_ordered_duplicate_nodes",
    "get_peers_miss_native_scoped_ipv6_projection",
    "get_peers_found_empty_values",
    "get_peers_found_ordered_duplicate_values_ipv4_golden",
    "get_peers_ignores_scrape_want_noseed",
    "get_peers_zero_requester_token_sensitivity",
    "get_peers_token_port_independence",
    "get_peers_token_source_ip_sensitivity",
    "get_peers_token_infohash_sensitivity",
    "get_peers_token_requester_sensitivity",
    "get_peers_token_mapped_ipv6_golden",
    "get_peers_token_native_ipv6_numeric_zone7",
    "get_peers_token_native_ipv6_numeric_zone8",
    "announce_peer_zero_infohash_no_mutation",
    "announce_peer_bad_token_no_mutation",
    "announce_peer_get_token_roundtrip_port_independent",
    "announce_peer_implied_port_wins",
    "announce_peer_default_source_port",
    "announce_peer_explicit_port_zero",
    "announce_peer_explicit_port_65535",
    "announce_peer_explicit_port_negative_one_wraps",
    "announce_peer_explicit_port_65536_wraps",
    "announce_peer_explicit_port_i64_min_wraps",
    "announce_peer_explicit_port_i64_max_wraps",
    "sample_infohashes_nil_arguments",
    "sample_infohashes_zero_target_empty_present_fields",
    "sample_infohashes_ordered_duplicate_hashes_and_nodes",
    "sample_infohashes_native_scoped_ipv6_projection",
    "sample_infohashes_signed_i64_min_total_and_interval",
    "sample_infohashes_signed_i64_max_total_and_interval",
];

const NATIVE_IPV6_CASES: [&str; 3] = [
    "find_node_native_scoped_ipv6_projection",
    "get_peers_miss_native_scoped_ipv6_projection",
    "sample_infohashes_native_scoped_ipv6_projection",
];

const DISPATCH_SEND_FIXTURE_IDS: [&str; 26] = [
    "ping_success_empty_tid_mixed_request_y_ignored",
    "find_nodes_binary_tid_mapped_destination",
    "get_peers_values_and_binary_token",
    "get_peers_values_present_empty_and_empty_token",
    "get_peers_closest_nodes_and_token",
    "find_nodes_present_empty",
    "sample_populated_long_tid_signed_extremes",
    "sample_present_empty_and_zero_counts_scoped_destination",
    "announce_mutation_precedes_successful_send",
    "protocol_203_value_discards_partial_return",
    "protocol_203_wrapped_value",
    "protocol_204_value",
    "protocol_204_wrapped_value",
    "direct_protocol_pointer_is_generic_202",
    "wrapped_protocol_pointer_is_generic_202",
    "typed_nil_protocol_pointer_is_generic_202",
    "generic_error_binary_tid_discards_partial_return",
    "pre_cancelled_context_success_still_sent",
    "already_expired_context_success_still_sent",
    "expired_context_error_becomes_generic_202",
    "transport_error_one_call_is_logged_and_swallowed",
    "direct_send_returned_encode_error_zero_socket_calls",
    "compact_ipv4_native_ipv6_panics_before_socket",
    "responder_panic_is_not_recovered",
    "socket_panic_after_one_call_is_not_recovered",
    "announce_mutation_precedes_failed_send_and_survives",
];

const RUST_SUPPORTED_HANDLE_QUERY_ROWS: [&str; 5] = [
    "ping_success_empty_tid_mixed_request_y_ignored",
    "find_nodes_binary_tid_mapped_destination",
    "sample_populated_long_tid_signed_extremes",
    "sample_present_empty_and_zero_counts_scoped_destination",
    "protocol_204_value",
];

#[derive(Debug, Deserialize)]
struct ResponderFixture {
    id: String,
    subsystem: String,
    runtime: FixtureRuntime,
    config: FixtureConfig,
    input: FixtureInput,
    expected: FixtureExpected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureRuntime {
    int_bits: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureConfig {
    node_id: Id20,
    token_secret_hex: String,
    sample_info_hashes_interval: i64,
}

#[derive(Debug, Deserialize)]
struct FixtureInput {
    steps: Vec<FixtureStep>,
    table: FixtureTableScript,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureStep {
    source: FixtureAddr,
    method: String,
    args_presence: String,
    args: FixtureArgs,
    token_from_step: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureArgs {
    id: Id20,
    info_hash: Id20,
    target: Id20,
    token_hex: String,
    port_presence: String,
    port: i64,
    implied_port: bool,
    want_presence: String,
    want: Option<Vec<String>>,
    no_seed: i64,
    scrape: i64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureTableScript {
    closest_nodes: Option<Vec<FixtureNode>>,
    lookup_found: bool,
    lookup_hash_id: String,
    lookup_peers: Option<Vec<FixtureAddr>>,
    lookup_closest_nodes: Option<Vec<FixtureNode>>,
    sample_hashes: Option<Vec<Id20>>,
    sample_nodes: Option<Vec<FixtureNode>>,
    sample_total_hashes: i64,
}

#[derive(Debug, Deserialize)]
struct FixtureExpected {
    normalization: String,
    outcomes: Vec<FixtureOutcome>,
    #[serde(rename = "tableCalls")]
    table_calls: Vec<FixtureTableCall>,
    #[serde(rename = "tableState")]
    table_state: FixtureTableState,
}

#[derive(Debug, Deserialize)]
struct FixtureOutcome {
    #[serde(rename = "return")]
    returned: FixtureReturn,
    error: Option<FixtureError>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct FixtureReturn {
    id: Id20,
    nodes_presence: String,
    nodes: Option<Vec<FixtureNode>>,
    nodes6_presence: String,
    nodes6: Option<Vec<FixtureNode>>,
    values_presence: String,
    values: Option<Vec<FixtureAddr>>,
    token_presence: String,
    token_hex: String,
    samples_presence: String,
    samples: Option<Vec<Id20>>,
    num_presence: String,
    num: i64,
    interval_presence: String,
    interval: i64,
    peers_bloom_presence: String,
    seeders_bloom_presence: String,
    bep44_fields_are_zero: bool,
}

#[derive(Debug, Deserialize)]
struct FixtureError {
    code: i64,
    message: String,
    text: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct FixtureTableCall {
    method: String,
    id: String,
    command_count: usize,
}

#[derive(Debug, Deserialize)]
struct FixtureTableState {
    before: FixturePutState,
    after: FixturePutState,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct FixturePutState {
    put_hashes: Vec<FixturePutHash>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct FixturePutHash {
    id: Id20,
    peers: Vec<FixtureAddr>,
    options_count: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct FixtureNode {
    id: Id20,
    addr: FixtureAddr,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct FixtureAddr {
    ip: IpAddr,
    port: u16,
    #[serde(default)]
    zone: String,
}

#[derive(Default)]
struct ScriptedObserved {
    calls: Vec<FixtureTableCall>,
    put_hashes: Vec<FixturePutHash>,
}

#[derive(Clone)]
struct ScriptedTable {
    origin: Id20,
    script: FixtureTableScript,
    observed: Arc<Mutex<ScriptedObserved>>,
}

impl ScriptedTable {
    fn new(origin: Id20, script: FixtureTableScript) -> Self {
        Self {
            origin,
            script,
            observed: Arc::new(Mutex::new(ScriptedObserved::default())),
        }
    }

    fn calls(&self) -> Vec<FixtureTableCall> {
        self.observed.lock().unwrap().calls.clone()
    }

    fn put_state(&self) -> FixturePutState {
        FixturePutState {
            put_hashes: self.observed.lock().unwrap().put_hashes.clone(),
        }
    }

    fn record_call(&self, method: &str, id: String, command_count: usize) {
        self.observed.lock().unwrap().calls.push(FixtureTableCall {
            method: method.to_owned(),
            id,
            command_count,
        });
    }
}

impl DhtResponderTable for ScriptedTable {
    fn origin(&self) -> Id20 {
        self.origin
    }

    fn closest_nodes(&self, id: Id20) -> Vec<RoutingNode> {
        self.record_call("GetClosestNodes", id.to_hex(), 0);
        self.script
            .closest_nodes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(routing_node)
            .collect()
    }

    fn get_hash_or_closest_nodes(&self, id: Id20) -> DhtResponderLookup {
        self.record_call("GetHashOrClosestNodes", id.to_hex(), 0);
        if self.script.lookup_found {
            assert_eq!(
                Id20::from_hex(&self.script.lookup_hash_id).expect("scripted lookup hash ID"),
                id
            );
            DhtResponderLookup::Found {
                peers: self
                    .script
                    .lookup_peers
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|addr| KTableHashPeer {
                        addr: socket_addr(addr),
                    })
                    .collect(),
            }
        } else {
            assert!(self.script.lookup_hash_id.is_empty());
            assert!(self.script.lookup_peers.is_none());
            DhtResponderLookup::ClosestNodes(
                self.script
                    .lookup_closest_nodes
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(routing_node)
                    .collect(),
            )
        }
    }

    fn batch_command(&self, commands: &[KTableCommand]) {
        self.record_call("BatchCommand", String::new(), commands.len());
        let mut observed = self.observed.lock().unwrap();
        for command in commands {
            let KTableCommand::PutHash { id, peers } = command else {
                panic!("responder emitted a non-PutHash command: {command:?}")
            };
            observed.put_hashes.push(FixturePutHash {
                id: *id,
                peers: peers.iter().map(|peer| fixture_addr(peer.addr)).collect(),
                options_count: 0,
            });
        }
    }

    fn sample_hashes_and_nodes(&self) -> DhtResponderSample {
        self.record_call("SampleHashesAndNodes", String::new(), 0);
        DhtResponderSample {
            hashes: self.script.sample_hashes.clone().unwrap_or_default(),
            nodes: self
                .script
                .sample_nodes
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(routing_node)
                .collect(),
            total_hashes: self.script.sample_total_hashes,
        }
    }
}

fn responder_fixtures() -> Vec<ResponderFixture> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht/dht_responder.jsonl");
    BufReader::new(File::open(path).expect("checked DHT responder fixture"))
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect()
}

fn socket_addr(value: &FixtureAddr) -> SocketAddr {
    match value.ip {
        IpAddr::V4(ip) => {
            assert!(value.zone.is_empty());
            SocketAddr::V4(SocketAddrV4::new(ip, value.port))
        }
        IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(
            ip,
            value.port,
            0,
            if value.zone.is_empty() {
                0
            } else {
                value.zone.parse().expect("numeric IPv6 zone")
            },
        )),
    }
}

fn fixture_addr(value: SocketAddr) -> FixtureAddr {
    match value {
        SocketAddr::V4(value) => FixtureAddr {
            ip: IpAddr::V4(*value.ip()),
            port: value.port(),
            zone: String::new(),
        },
        SocketAddr::V6(value) => FixtureAddr {
            ip: IpAddr::V6(*value.ip()),
            port: value.port(),
            zone: if value.scope_id() == 0 {
                String::new()
            } else {
                value.scope_id().to_string()
            },
        },
    }
}

fn routing_node(value: &FixtureNode) -> RoutingNode {
    RoutingNode {
        id: value.id,
        addr: socket_addr(&value.addr),
    }
}

fn fixture_args(value: &FixtureArgs, token_override: Option<ByteString>) -> MessageArgs {
    let port = match value.port_presence.as_str() {
        "nil" => None,
        "present" => Some(value.port),
        other => panic!("unknown port presence {other}"),
    };
    let want = match value.want_presence.as_str() {
        "nil" => {
            assert!(value.want.is_none());
            None
        }
        "empty" | "present" => Some(
            value
                .want
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|want| ByteString::new(want.as_bytes()))
                .collect(),
        ),
        other => panic!("unknown want presence {other}"),
    };
    MessageArgs {
        id: value.id,
        info_hash: (value.info_hash != Id20::ZERO).then_some(value.info_hash),
        target: (value.target != Id20::ZERO).then_some(value.target),
        token: token_override.unwrap_or_else(|| {
            ByteString::new(hex::decode(&value.token_hex).expect("fixture token hex"))
        }),
        port,
        implied_port: value.implied_port,
        want,
        no_seed: value.no_seed,
        scrape: value.scrape,
    }
}

fn fixture_request(
    value: &FixtureStep,
    token_override: Option<ByteString>,
    transaction_id: Vec<u8>,
) -> KrpcMessage {
    let args = match value.args_presence.as_str() {
        "nil" => None,
        "present" => Some(fixture_args(&value.args, token_override)),
        other => panic!("unknown args presence {other}"),
    };
    KrpcMessage {
        transaction_id: ByteString::new(transaction_id),
        message_type: ByteString::new(b"already-routed-response"),
        query: ByteString::new(value.method.as_bytes()),
        args,
        response: Some(empty_return(Id20::ZERO)),
        error: Some(KrpcError {
            code: 999,
            message: ByteString::new(b"request-only"),
        }),
        observed_addr: Some(CompactAddr {
            ip: "198.51.100.1".parse().unwrap(),
            port: 9,
        }),
        read_only: true,
        client_id: ByteString::new(b"client"),
    }
}

fn fixture_node(value: &FixtureNode) -> CompactNode {
    CompactNode {
        id: value.id,
        addr: CompactAddr {
            ip: value.addr.ip,
            port: value.addr.port,
        },
    }
}

fn expected_return(value: &FixtureReturn) -> MessageReturn {
    assert!(value.bep44_fields_are_zero);
    assert_eq!(value.peers_bloom_presence, "nil");
    assert_eq!(value.seeders_bloom_presence, "nil");
    let list = |presence: &str, values: &Option<Vec<FixtureNode>>| match presence {
        "nil" => {
            assert!(values.is_none());
            None
        }
        "empty" | "present" => Some(
            values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(fixture_node)
                .collect(),
        ),
        other => panic!("unknown node presence {other}"),
    };
    let values = match value.values_presence.as_str() {
        "nil" => {
            assert!(value.values.is_none());
            None
        }
        "empty" | "present" => Some(
            value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|addr| {
                    assert!(addr.zone.is_empty());
                    CompactAddr {
                        ip: addr.ip,
                        port: addr.port,
                    }
                })
                .collect(),
        ),
        other => panic!("unknown values presence {other}"),
    };
    let token = match value.token_presence.as_str() {
        "nil" => None,
        "present" => Some(ByteString::new(hex::decode(&value.token_hex).unwrap())),
        other => panic!("unknown token presence {other}"),
    };
    let samples = match value.samples_presence.as_str() {
        "nil" => None,
        "empty" | "present" => Some(value.samples.clone().unwrap_or_default()),
        other => panic!("unknown samples presence {other}"),
    };
    let integer = |presence: &str, number| match presence {
        "nil" => None,
        "present" => Some(number),
        other => panic!("unknown integer presence {other}"),
    };
    MessageReturn {
        id: value.id,
        nodes: list(&value.nodes_presence, &value.nodes),
        nodes6: list(&value.nodes6_presence, &value.nodes6),
        token,
        values,
        interval: integer(&value.interval_presence, value.interval),
        num: integer(&value.num_presence, value.num),
        samples,
        seeders_bloom: None,
        peers_bloom: None,
    }
}

fn first_native_node(script: &FixtureTableScript) -> RoutingNode {
    script
        .closest_nodes
        .iter()
        .chain(&script.lookup_closest_nodes)
        .chain(&script.sample_nodes)
        .flat_map(|nodes| nodes.iter())
        .map(routing_node)
        .find(|node| match node.addr.ip() {
            IpAddr::V4(_) => false,
            IpAddr::V6(ip) => ip.to_ipv4_mapped().is_none(),
        })
        .expect("native IPv6 fixture node")
}

fn transaction_id(index: usize) -> Vec<u8> {
    match index % 5 {
        0 => Vec::new(),
        1 => vec![0xff],
        2 => vec![0, 0xff],
        3 => vec![0, 1, 2],
        _ => (0..257).map(|offset| offset as u8).collect(),
    }
}

fn clean_message(
    request: &KrpcMessage,
    response: Option<MessageReturn>,
    error: Option<KrpcError>,
) -> KrpcMessage {
    KrpcMessage {
        transaction_id: request.transaction_id.clone(),
        message_type: ByteString::new(b"r"),
        query: ByteString::default(),
        args: None,
        response,
        error,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    }
}

fn assert_clean(reply: &DhtReply, request: &KrpcMessage, source: SocketAddr) {
    assert_eq!(reply.destination, source);
    assert_eq!(reply.message.transaction_id, request.transaction_id);
    assert_eq!(reply.message.message_type.as_bytes(), b"r");
    assert!(reply.message.query.is_empty());
    assert!(reply.message.args.is_none());
    assert_ne!(
        reply.message.response.is_some(),
        reply.message.error.is_some()
    );
    assert!(reply.message.observed_addr.is_none());
    assert!(!reply.message.read_only);
    assert!(reply.message.client_id.is_empty());
}

#[test]
fn all_real_go_responder_rows_compose_clean_exact_dispatch_replies_and_effects() {
    let fixtures = responder_fixtures();
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.id.as_str())
            .collect::<Vec<_>>(),
        RESPONDER_FIXTURE_IDS
    );
    let mut protocol_partial_returns_discarded = 0;
    let mut native_deltas = 0;
    let mut transaction_widths = Vec::new();

    for (fixture_index, fixture) in fixtures.into_iter().enumerate() {
        assert_eq!(fixture.subsystem, "dht_responder", "{}", fixture.id);
        assert_eq!(fixture.runtime.int_bits, 64, "{}", fixture.id);
        assert_eq!(fixture.expected.normalization, "none", "{}", fixture.id);
        assert_eq!(fixture.input.steps.len(), fixture.expected.outcomes.len());
        let token_secret: [u8; 20] = hex::decode(&fixture.config.token_secret_hex)
            .unwrap()
            .try_into()
            .unwrap();
        let table = ScriptedTable::new(fixture.config.node_id, fixture.input.table.clone());
        assert_eq!(table.put_state(), fixture.expected.table_state.before);
        let dispatcher = DhtDispatcher::from_responder(DhtResponder::with_token_secret(
            table.clone(),
            token_secret,
            fixture.config.sample_info_hashes_interval,
        ));
        let mut step_tokens = Vec::<Option<ByteString>>::new();

        for (index, (step, expected)) in fixture
            .input
            .steps
            .iter()
            .zip(&fixture.expected.outcomes)
            .enumerate()
        {
            let token_override = step.token_from_step.map(|from| {
                assert!(from < index);
                assert!(step.args.token_hex.is_empty());
                step_tokens[from]
                    .clone()
                    .expect("referenced step returned no token")
            });
            let transaction_id = transaction_id(fixture_index + index);
            transaction_widths.push(transaction_id.len());
            let request = fixture_request(step, token_override, transaction_id);
            let source = socket_addr(&step.source);
            let outcome = dispatcher.dispatch(source, &request);
            let reply = outcome.reply();
            assert_clean(reply, &request, source);

            if NATIVE_IPV6_CASES.contains(&fixture.id.as_str()) {
                assert!(expected.error.is_none(), "{}", fixture.id);
                let DhtDispatchOutcome::LocalFailure { cause, .. } = &outcome else {
                    panic!("{} did not retain a local failure", fixture.id)
                };
                assert_eq!(
                    cause,
                    &DhtResponderError::NativeIpv6Node(first_native_node(&fixture.input.table)),
                    "{}",
                    fixture.id
                );
                let expected_message = clean_message(
                    &request,
                    None,
                    Some(KrpcError {
                        code: 202,
                        message: ByteString::new(b"server error"),
                    }),
                );
                assert_eq!(reply.message, expected_message, "{}", fixture.id);
                assert_eq!(reply.wire().unwrap(), expected_message.encode().unwrap());
                step_tokens.push(None);
                native_deltas += 1;
                continue;
            }

            match &expected.error {
                None => {
                    let expected_return = expected_return(&expected.returned);
                    let expected_message = clean_message(&request, Some(expected_return), None);
                    assert!(matches!(outcome, DhtDispatchOutcome::Reply(_)));
                    assert_eq!(
                        reply.message, expected_message,
                        "{} step {index}",
                        fixture.id
                    );
                    assert_eq!(reply.wire().unwrap(), expected_message.encode().unwrap());
                    step_tokens.push(
                        reply
                            .message
                            .response
                            .as_ref()
                            .and_then(|response| response.token.clone()),
                    );
                }
                Some(expected_error) => {
                    assert_eq!(
                        expected_error.text,
                        format!(
                            "KRPC error {}: {}",
                            expected_error.code, expected_error.message
                        )
                    );
                    let expected_message = clean_message(
                        &request,
                        None,
                        Some(KrpcError {
                            code: expected_error.code,
                            message: ByteString::new(expected_error.message.as_bytes()),
                        }),
                    );
                    assert!(matches!(outcome, DhtDispatchOutcome::Reply(_)));
                    assert_eq!(
                        reply.message, expected_message,
                        "{} step {index}",
                        fixture.id
                    );
                    assert_eq!(reply.wire().unwrap(), expected_message.encode().unwrap());
                    assert!(reply.message.response.is_none());
                    if expected.returned.id != Id20::ZERO {
                        protocol_partial_returns_discarded += 1;
                    }
                    step_tokens.push(None);
                }
            }
        }

        assert_eq!(
            table.calls(),
            fixture.expected.table_calls,
            "{}",
            fixture.id
        );
        assert_eq!(
            table.put_state(),
            fixture.expected.table_state.after,
            "{}",
            fixture.id
        );
    }

    assert_eq!(native_deltas, NATIVE_IPV6_CASES.len());
    assert_eq!(protocol_partial_returns_discarded, 5);
    transaction_widths.sort_unstable();
    transaction_widths.dedup();
    assert_eq!(transaction_widths, [0, 1, 2, 3, 257]);
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DispatchSendFixture {
    id: String,
    subsystem: String,
    runtime: DispatchRuntime,
    input: DispatchInput,
    expected: DispatchExpected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DispatchRuntime {
    int_bits: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DispatchInput {
    source: DispatchAddr,
    request: DispatchRequest,
    context: String,
    responder: DispatchResponderInput,
    socket: DispatchSocketInput,
    state: DispatchMutationState,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DispatchAddr {
    ip: IpAddr,
    port: u16,
    scope: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DispatchRequest {
    tid_hex: String,
    type_hex: String,
    method_hex: String,
    args_present: bool,
    mixed_fields: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DispatchResponderInput {
    kind: String,
    #[serde(rename = "return")]
    returned: DispatchReturn,
    error_code: Option<i64>,
    error_hex: Option<String>,
    mutation: Option<String>,
    unsupported_v: Option<bool>,
    returns_context_error: Option<bool>,
    panics: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DispatchSocketInput {
    kind: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DispatchExpected {
    responder_calls: usize,
    responder_input_exact: bool,
    context: DispatchContext,
    classification: String,
    destination: Option<DispatchAddr>,
    wire_hex: Option<String>,
    envelope: Option<DispatchEnvelope>,
    events: Vec<String>,
    send_calls: usize,
    logs: Vec<DispatchLog>,
    state: DispatchExpectedState,
    partial_return_discarded: Option<bool>,
    send_failure_swallowed: Option<bool>,
    returned_error: Option<DispatchReturnedError>,
    terminal: String,
    panic_text: Option<String>,
    panic_identity_exact: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DispatchContext {
    deadline_present: bool,
    err_at_respond: String,
    err_after: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DispatchEnvelope {
    tid_hex: String,
    type_hex: String,
    presence: DispatchWirePresence,
    #[serde(rename = "return")]
    returned: Option<DispatchReturn>,
    error: Option<DispatchWireError>,
    canonical: bool,
    tid_echoed: bool,
    request_fields_cleared: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DispatchWirePresence {
    #[serde(rename = "q")]
    query: bool,
    #[serde(rename = "a")]
    arguments: bool,
    #[serde(rename = "r")]
    returned: bool,
    #[serde(rename = "e")]
    error: bool,
    #[serde(rename = "ip")]
    ip: bool,
    #[serde(rename = "ro")]
    read_only: bool,
    #[serde(rename = "v")]
    client_id: bool,
    #[serde(rename = "id")]
    id: bool,
    #[serde(rename = "nodes")]
    nodes: bool,
    #[serde(rename = "nodes6")]
    nodes6: bool,
    #[serde(rename = "values")]
    values: bool,
    #[serde(rename = "token")]
    token: bool,
    #[serde(rename = "samples")]
    samples: bool,
    #[serde(rename = "num")]
    num: bool,
    #[serde(rename = "interval")]
    interval: bool,
    #[serde(rename = "BFsd")]
    seeders_bloom: bool,
    #[serde(rename = "BFpe")]
    peers_bloom: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DispatchReturn {
    id: Id20,
    nodes_present: bool,
    nodes: Vec<DispatchNode>,
    nodes6_present: bool,
    nodes6: Vec<DispatchNode>,
    values_present: bool,
    values: Vec<DispatchPeerAddr>,
    token_present: bool,
    token_hex: String,
    samples_present: bool,
    samples: Vec<Id20>,
    num_present: bool,
    num: i64,
    interval_present: bool,
    interval: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DispatchNode {
    id: Id20,
    addr: DispatchPeerAddr,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DispatchPeerAddr {
    ip: IpAddr,
    port: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DispatchWireError {
    code: i64,
    message_hex: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DispatchLog {
    level: String,
    message: String,
    ret_err_key: bool,
    ret_err_type: String,
    ret_err_text: String,
    ret_err_identity_exact: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DispatchReturnedError {
    #[serde(rename = "type")]
    error_type: String,
    text: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DispatchMutationState {
    mutations: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DispatchExpectedState {
    before: Vec<String>,
    at_send: Vec<String>,
    after: Vec<String>,
}

fn dispatch_send_fixtures() -> Vec<DispatchSendFixture> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht/dht_dispatch_send.jsonl");
    BufReader::new(File::open(path).expect("checked DHT dispatch/send fixture"))
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect()
}

fn assert_optional_omitempty_bool(value: Option<bool>, expected: bool, field: &str, id: &str) {
    assert_eq!(
        value,
        expected.then_some(true),
        "{id}: wrong {field} presence/value"
    );
}

fn dispatch_addr(value: &DispatchAddr) -> SocketAddr {
    let scope = match value.scope {
        None => 0,
        Some(scope) => {
            assert_ne!(scope, 0, "omitempty scope must not serialize zero");
            scope
        }
    };
    match value.ip {
        IpAddr::V4(ip) => {
            assert_eq!(scope, 0);
            SocketAddr::V4(SocketAddrV4::new(ip, value.port))
        }
        IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(ip, value.port, 0, scope)),
    }
}

fn dispatch_node(value: &DispatchNode) -> FixtureNode {
    let port = u16::try_from(value.addr.port).expect("oracle compact-address port");
    FixtureNode {
        id: value.id,
        addr: FixtureAddr {
            ip: value.addr.ip,
            port,
            zone: String::new(),
        },
    }
}

fn oracle_table_script(returned: &DispatchReturn, method: &[u8]) -> FixtureTableScript {
    let nodes = returned.nodes.iter().map(dispatch_node).collect::<Vec<_>>();
    FixtureTableScript {
        closest_nodes: (method == b"find_node").then_some(nodes.clone()),
        lookup_found: false,
        lookup_hash_id: String::new(),
        lookup_peers: None,
        lookup_closest_nodes: None,
        sample_hashes: (method == b"sample_infohashes").then_some(returned.samples.clone()),
        sample_nodes: (method == b"sample_infohashes").then_some(nodes),
        sample_total_hashes: returned.num,
    }
}

fn oracle_request(value: &DispatchRequest) -> KrpcMessage {
    let method = hex::decode(&value.method_hex).unwrap();
    let mut request_args = args(id(2));
    request_args.target = Some(id(3));
    request_args.info_hash = Some(id(4));
    let mut message = KrpcMessage {
        transaction_id: ByteString::new(hex::decode(&value.tid_hex).unwrap()),
        message_type: ByteString::new(hex::decode(&value.type_hex).unwrap()),
        query: ByteString::new(method),
        args: value.args_present.then_some(request_args),
        response: None,
        error: None,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    };
    if match value.mixed_fields {
        None => false,
        Some(true) => true,
        Some(false) => panic!("omitempty mixedFields serialized false"),
    } {
        message.response = Some(empty_return(id(0xaa)));
        message.error = Some(KrpcError {
            code: 999,
            message: ByteString::new(b"request-only"),
        });
        message.observed_addr = Some(CompactAddr {
            ip: "198.51.100.200".parse().unwrap(),
            port: 9,
        });
        message.read_only = true;
        message.client_id = ByteString::new([0xff, 0]);
    }
    message
}

fn dispatch_message_return(value: &DispatchReturn) -> MessageReturn {
    let nodes = |present: bool, values: &[DispatchNode]| {
        if present {
            Some(
                values
                    .iter()
                    .map(|node| CompactNode {
                        id: node.id,
                        addr: CompactAddr {
                            ip: node.addr.ip,
                            port: u16::try_from(node.addr.port).expect("oracle compact-node port"),
                        },
                    })
                    .collect(),
            )
        } else {
            assert!(values.is_empty());
            None
        }
    };
    let values = if value.values_present {
        Some(
            value
                .values
                .iter()
                .map(|peer| CompactAddr {
                    ip: peer.ip,
                    port: u16::try_from(peer.port).expect("oracle peer port"),
                })
                .collect(),
        )
    } else {
        assert!(value.values.is_empty());
        None
    };
    let token = if value.token_present {
        Some(ByteString::new(
            hex::decode(&value.token_hex).expect("oracle token hex"),
        ))
    } else {
        assert!(value.token_hex.is_empty());
        None
    };
    let samples = if value.samples_present {
        Some(value.samples.clone())
    } else {
        assert!(value.samples.is_empty());
        None
    };
    MessageReturn {
        id: value.id,
        nodes: nodes(value.nodes_present, &value.nodes),
        nodes6: nodes(value.nodes6_present, &value.nodes6),
        token,
        values,
        interval: value.interval_present.then_some(value.interval),
        num: value.num_present.then_some(value.num),
        samples,
        seeders_bloom: None,
        peers_bloom: None,
    }
}

fn dispatch_expected_message(envelope: &DispatchEnvelope) -> KrpcMessage {
    KrpcMessage {
        transaction_id: ByteString::new(
            hex::decode(&envelope.tid_hex).expect("oracle envelope TID hex"),
        ),
        message_type: ByteString::new(
            hex::decode(&envelope.type_hex).expect("oracle envelope type hex"),
        ),
        query: ByteString::default(),
        args: None,
        response: envelope.returned.as_ref().map(dispatch_message_return),
        error: envelope.error.as_ref().map(|error| KrpcError {
            code: error.code,
            message: ByteString::new(
                hex::decode(&error.message_hex).expect("oracle error message hex"),
            ),
        }),
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    }
}

fn outer_evidence_scope(id: &str) -> &'static str {
    match id {
        "get_peers_values_and_binary_token"
        | "get_peers_values_present_empty_and_empty_token"
        | "get_peers_closest_nodes_and_token"
        | "find_nodes_present_empty"
        | "announce_mutation_precedes_successful_send" => "scripted_return_only",
        "protocol_203_value_discards_partial_return"
        | "protocol_203_wrapped_value"
        | "protocol_204_wrapped_value"
        | "direct_protocol_pointer_is_generic_202"
        | "wrapped_protocol_pointer_is_generic_202"
        | "typed_nil_protocol_pointer_is_generic_202"
        | "generic_error_binary_tid_discards_partial_return" => "go_error_classification_only",
        "pre_cancelled_context_success_still_sent"
        | "already_expired_context_success_still_sent"
        | "expired_context_error_becomes_generic_202" => "go_context_policy_only",
        "transport_error_one_call_is_logged_and_swallowed"
        | "announce_mutation_precedes_failed_send_and_survives" => "go_log_swallow_policy_only",
        "direct_send_returned_encode_error_zero_socket_calls" => "go_direct_send_encoder_only",
        "compact_ipv4_native_ipv6_panics_before_socket" => "go_compact_encoder_panic_only",
        "responder_panic_is_not_recovered" | "socket_panic_after_one_call_is_not_recovered" => {
            "go_unwind_policy_only"
        }
        other => panic!("unclassified outer runtime evidence row {other}"),
    }
}

fn expected_responder_kind(id: &str) -> &'static str {
    match id {
        "protocol_203_value_discards_partial_return" | "protocol_204_value" => "protocol_value",
        "protocol_203_wrapped_value" => "wrapped_protocol_value",
        "protocol_204_wrapped_value" => "wrapped_protocol_value",
        "direct_protocol_pointer_is_generic_202" => "protocol_pointer",
        "wrapped_protocol_pointer_is_generic_202" => "wrapped_protocol_pointer",
        "typed_nil_protocol_pointer_is_generic_202" => "typed_nil_protocol_pointer",
        "generic_error_binary_tid_discards_partial_return" => "generic",
        "expired_context_error_becomes_generic_202" => "context_error",
        "direct_send_returned_encode_error_zero_socket_calls" => "not_called_direct_send",
        id if DISPATCH_SEND_FIXTURE_IDS.contains(&id) => "none",
        other => panic!("unknown dispatch fixture ID {other}"),
    }
}

fn expected_classification(id: &str) -> &'static str {
    match id {
        "protocol_203_value_discards_partial_return" | "protocol_203_wrapped_value" => {
            "protocol_203"
        }
        "protocol_204_value" | "protocol_204_wrapped_value" => "protocol_204",
        "direct_protocol_pointer_is_generic_202"
        | "wrapped_protocol_pointer_is_generic_202"
        | "typed_nil_protocol_pointer_is_generic_202"
        | "generic_error_binary_tid_discards_partial_return"
        | "expired_context_error_becomes_generic_202" => "generic_202",
        "direct_send_returned_encode_error_zero_socket_calls" => "direct_send_encode_error",
        "compact_ipv4_native_ipv6_panics_before_socket" => "success_encode_panic",
        "responder_panic_is_not_recovered" => "responder_panic",
        id if DISPATCH_SEND_FIXTURE_IDS.contains(&id) => "success",
        other => panic!("unknown dispatch fixture ID {other}"),
    }
}

fn expected_context_kind(id: &str) -> &'static str {
    match id {
        "pre_cancelled_context_success_still_sent" => "cancelled",
        "already_expired_context_success_still_sent"
        | "expired_context_error_becomes_generic_202" => "expired",
        "direct_send_returned_encode_error_zero_socket_calls" => "none_direct_send",
        id if DISPATCH_SEND_FIXTURE_IDS.contains(&id) => "active",
        other => panic!("unknown dispatch fixture ID {other}"),
    }
}

fn expected_socket_kind(id: &str) -> &'static str {
    match id {
        "transport_error_one_call_is_logged_and_swallowed"
        | "announce_mutation_precedes_failed_send_and_survives" => "error",
        "socket_panic_after_one_call_is_not_recovered" => "panic",
        id if DISPATCH_SEND_FIXTURE_IDS.contains(&id) => "success",
        other => panic!("unknown dispatch fixture ID {other}"),
    }
}

fn expected_source(id: &str) -> SocketAddr {
    match id {
        "find_nodes_binary_tid_mapped_destination" => "[::ffff:192.0.2.2]:6882".parse().unwrap(),
        "sample_present_empty_and_zero_counts_scoped_destination"
        | "protocol_203_value_discards_partial_return" => {
            SocketAddr::V6(SocketAddrV6::new("fe80::3".parse().unwrap(), 6883, 0, 7))
        }
        "get_peers_closest_nodes_and_token" | "sample_populated_long_tid_signed_extremes" => {
            "[2001:db8::4]:6884".parse().unwrap()
        }
        id if DISPATCH_SEND_FIXTURE_IDS.contains(&id) => "192.0.2.1:6881".parse().unwrap(),
        other => panic!("unknown dispatch fixture ID {other}"),
    }
}

fn expected_request_method(id: &str) -> &'static [u8] {
    match id {
        "find_nodes_binary_tid_mapped_destination"
        | "find_nodes_present_empty"
        | "compact_ipv4_native_ipv6_panics_before_socket" => b"find_node",
        "get_peers_values_and_binary_token"
        | "get_peers_values_present_empty_and_empty_token"
        | "get_peers_closest_nodes_and_token" => b"get_peers",
        "sample_populated_long_tid_signed_extremes"
        | "sample_present_empty_and_zero_counts_scoped_destination" => b"sample_infohashes",
        "announce_mutation_precedes_successful_send"
        | "announce_mutation_precedes_failed_send_and_survives" => b"announce_peer",
        "protocol_204_value" | "protocol_204_wrapped_value" => b"unknown",
        id if DISPATCH_SEND_FIXTURE_IDS.contains(&id) => b"ping",
        other => panic!("unknown dispatch fixture ID {other}"),
    }
}

fn expected_request_type(id: &str) -> &'static [u8] {
    match id {
        "ping_success_empty_tid_mixed_request_y_ignored" => b"e",
        "find_nodes_binary_tid_mapped_destination" => b"x",
        "protocol_203_value_discards_partial_return" => b"ignored",
        id if DISPATCH_SEND_FIXTURE_IDS.contains(&id) => b"q",
        other => panic!("unknown dispatch fixture ID {other}"),
    }
}

fn expected_responder_error_text(id: &str) -> Option<&'static str> {
    match id {
        "protocol_203_value_discards_partial_return" => Some("KRPC error 203: missing arguments"),
        "protocol_203_wrapped_value" => Some("outer 203: KRPC error 203: missing arguments"),
        "protocol_204_value" => Some("KRPC error 204: method Unknown"),
        "protocol_204_wrapped_value" => Some("outer 204: KRPC error 204: method Unknown"),
        "direct_protocol_pointer_is_generic_202" => Some("KRPC error 207: pointer protocol"),
        "wrapped_protocol_pointer_is_generic_202" => {
            Some("outer pointer: KRPC error 207: wrapped pointer")
        }
        "generic_error_binary_tid_discards_partial_return" => Some("dispatch generic sentinel"),
        id if DISPATCH_SEND_FIXTURE_IDS.contains(&id) => None,
        other => panic!("unknown dispatch fixture ID {other}"),
    }
}

fn assert_dispatch_return_shape(value: &DispatchReturn, id: &str, label: &str) {
    let DispatchReturn {
        id: return_id,
        nodes_present,
        nodes,
        nodes6_present,
        nodes6,
        values_present,
        values,
        token_present,
        token_hex,
        samples_present,
        samples,
        num_present,
        num,
        interval_present,
        interval,
    } = value;
    let _ = return_id;

    for node in nodes.iter().chain(nodes6) {
        let DispatchNode { id: node_id, addr } = node;
        let DispatchPeerAddr { ip, port } = addr;
        let _ = node_id;
        assert!(matches!(ip, IpAddr::V4(_) | IpAddr::V6(_)));
        assert!(
            u16::try_from(*port).is_ok(),
            "{id}: {label} node port {port}"
        );
    }
    for peer in values {
        let DispatchPeerAddr { ip, port } = peer;
        assert!(matches!(ip, IpAddr::V4(_) | IpAddr::V6(_)));
        assert!(
            u16::try_from(*port).is_ok(),
            "{id}: {label} value port {port}"
        );
    }
    if !nodes_present {
        assert!(
            nodes.is_empty(),
            "{id}: absent {label} nodes carried values"
        );
    }
    if !nodes6_present {
        assert!(
            nodes6.is_empty(),
            "{id}: absent {label} nodes6 carried values"
        );
    }
    if !values_present {
        assert!(
            values.is_empty(),
            "{id}: absent {label} values carried values"
        );
    }
    if *token_present {
        hex::decode(token_hex).expect("oracle token hex");
    } else {
        assert!(token_hex.is_empty(), "{id}: absent {label} token had bytes");
    }
    if !samples_present {
        assert!(
            samples.is_empty(),
            "{id}: absent {label} samples carried values"
        );
    }
    if !num_present {
        assert_eq!(*num, 0, "{id}: absent {label} num was nonzero");
    }
    if !interval_present {
        assert_eq!(*interval, 0, "{id}: absent {label} interval was nonzero");
    }
}

fn assert_dispatch_envelope(
    envelope: &DispatchEnvelope,
    request: &DispatchRequest,
    input_return: &DispatchReturn,
    classification: &str,
    id: &str,
) {
    let DispatchEnvelope {
        tid_hex,
        type_hex,
        presence,
        returned,
        error,
        canonical,
        tid_echoed,
        request_fields_cleared,
    } = envelope;
    assert_eq!(tid_hex, &request.tid_hex, "{id}: TID was not echoed");
    assert_eq!(type_hex, "72", "{id}: Go error reply was not y=r");
    assert!(*canonical, "{id}: noncanonical Go wire");
    assert!(*tid_echoed, "{id}: oracle did not confirm TID echo");
    assert!(
        *request_fields_cleared,
        "{id}: request fields survived response construction"
    );
    assert_ne!(returned.is_some(), error.is_some(), "{id}: r/e exclusivity");

    if let Some(returned) = returned {
        assert_dispatch_return_shape(returned, id, "envelope return");
        assert_eq!(
            returned, input_return,
            "{id}: successful projection drifted"
        );
    }
    if let Some(error) = error {
        let DispatchWireError { code, message_hex } = error;
        let (expected_code, expected_message) = match classification {
            "protocol_203" => (203, b"missing arguments".as_slice()),
            "protocol_204" => (204, b"method Unknown".as_slice()),
            "generic_202" => (202, b"server error".as_slice()),
            other => panic!("{id}: error envelope had classification {other}"),
        };
        assert_eq!(*code, expected_code, "{id}: error code");
        assert_eq!(
            hex::decode(message_hex).expect("oracle error message hex"),
            expected_message,
            "{id}: error message"
        );
    }

    let DispatchWirePresence {
        query,
        arguments,
        returned: return_present,
        error: error_present,
        ip,
        read_only,
        client_id,
        id: id_present,
        nodes,
        nodes6,
        values,
        token,
        samples,
        num,
        interval,
        seeders_bloom,
        peers_bloom,
    } = presence;
    assert_eq!(*return_present, returned.is_some(), "{id}: r presence");
    assert_eq!(*error_present, error.is_some(), "{id}: e presence");
    assert_eq!(*id_present, returned.is_some(), "{id}: id presence");
    assert_eq!(
        (*nodes, *nodes6, *values, *token, *samples, *num, *interval),
        returned
            .as_ref()
            .map_or((false, false, false, false, false, false, false), |ret| {
                (
                    ret.nodes_present,
                    ret.nodes6_present,
                    ret.values_present,
                    ret.token_present,
                    ret.samples_present,
                    ret.num_present,
                    ret.interval_present,
                )
            }),
        "{id}: projected return presence"
    );
    assert_eq!(
        (
            *query,
            *arguments,
            *ip,
            *read_only,
            *client_id,
            *seeders_bloom,
            *peers_bloom,
        ),
        (false, false, false, false, false, false, false),
        "{id}: cleared or bloom presence"
    );
}

fn assert_dispatch_fixture_schema(fixture: &DispatchSendFixture) {
    let DispatchSendFixture {
        id,
        subsystem,
        runtime,
        input,
        expected,
    } = fixture;
    let DispatchRuntime { int_bits } = runtime;
    assert_eq!(subsystem, "dht_dispatch_send", "{id}");
    assert_eq!(*int_bits, 64, "{id}");

    let DispatchInput {
        source,
        request,
        context,
        responder,
        socket,
        state,
    } = input;
    let DispatchAddr {
        ip: source_ip,
        port: source_port,
        scope: source_scope,
    } = source;
    assert!(matches!(source_ip, IpAddr::V4(_) | IpAddr::V6(_)));
    assert_ne!(*source_port, 0, "{id}: zero source port");
    assert_eq!(dispatch_addr(source), expected_source(id), "{id}: source");
    let expected_scope = matches!(
        id.as_str(),
        "sample_present_empty_and_zero_counts_scoped_destination"
            | "protocol_203_value_discards_partial_return"
    )
    .then_some(7);
    assert_eq!(*source_scope, expected_scope, "{id}: source scope");
    let DispatchRequest {
        tid_hex,
        type_hex,
        method_hex,
        args_present,
        mixed_fields,
    } = request;
    hex::decode(tid_hex).expect("oracle request TID hex");
    let request_type = hex::decode(type_hex).expect("oracle request type hex");
    let request_method = hex::decode(method_hex).expect("oracle request method hex");
    assert_eq!(
        request_type,
        expected_request_type(id),
        "{id}: request type"
    );
    assert_eq!(
        request_method,
        expected_request_method(id),
        "{id}: request method"
    );
    assert!(*args_present, "{id}: frozen request omitted arguments");
    assert_optional_omitempty_bool(
        *mixed_fields,
        matches!(
            id.as_str(),
            "ping_success_empty_tid_mixed_request_y_ignored"
                | "protocol_203_value_discards_partial_return"
        ),
        "mixedFields",
        id,
    );
    assert_eq!(context, expected_context_kind(id), "{id}: input context");

    let DispatchResponderInput {
        kind,
        returned: responder_return,
        error_code,
        error_hex,
        mutation,
        unsupported_v,
        returns_context_error,
        panics,
    } = responder;
    assert_eq!(kind, expected_responder_kind(id), "{id}: responder kind");
    assert_dispatch_return_shape(responder_return, id, "responder return");
    if let Some(code) = error_code {
        assert_ne!(*code, 0, "{id}: omitempty errorCode serialized zero");
    }
    let decoded_error = error_hex.as_ref().map(|encoded| {
        assert!(
            !encoded.is_empty(),
            "{id}: omitempty errorHex serialized empty"
        );
        String::from_utf8(hex::decode(encoded).expect("oracle responder error hex"))
            .expect("oracle responder error UTF-8")
    });
    if let Some(mutation) = mutation {
        assert!(
            !mutation.is_empty(),
            "{id}: omitempty mutation serialized empty"
        );
    }
    assert_optional_omitempty_bool(
        *unsupported_v,
        id == "direct_send_returned_encode_error_zero_socket_calls",
        "unsupportedV",
        id,
    );
    assert_optional_omitempty_bool(
        *returns_context_error,
        id == "expired_context_error_becomes_generic_202",
        "returnsContextError",
        id,
    );
    assert_optional_omitempty_bool(
        *panics,
        id == "responder_panic_is_not_recovered",
        "panics",
        id,
    );
    let expected_error_code = match id.as_str() {
        "protocol_203_value_discards_partial_return" | "protocol_203_wrapped_value" => Some(203),
        "protocol_204_value" | "protocol_204_wrapped_value" => Some(204),
        "direct_protocol_pointer_is_generic_202" | "wrapped_protocol_pointer_is_generic_202" => {
            Some(207)
        }
        _ => None,
    };
    assert_eq!(
        *error_code, expected_error_code,
        "{id}: responder errorCode"
    );
    assert_eq!(
        decoded_error.as_deref(),
        expected_responder_error_text(id),
        "{id}: responder errorHex"
    );

    let DispatchSocketInput { kind: socket_kind } = socket;
    assert_eq!(socket_kind, expected_socket_kind(id), "{id}: socket kind");
    let DispatchMutationState { mutations } = state;
    assert!(mutations.is_empty(), "{id}: nonempty initial input state");

    let DispatchExpected {
        responder_calls,
        responder_input_exact,
        context: expected_context,
        classification,
        destination,
        wire_hex,
        envelope,
        events,
        send_calls,
        logs,
        state: expected_state,
        partial_return_discarded,
        send_failure_swallowed,
        returned_error,
        terminal,
        panic_text,
        panic_identity_exact,
    } = expected;
    let direct_send = id == "direct_send_returned_encode_error_zero_socket_calls";
    assert_eq!(
        *responder_calls,
        usize::from(!direct_send),
        "{id}: responder calls"
    );
    assert_eq!(
        *responder_input_exact, !direct_send,
        "{id}: responder input exact"
    );
    let DispatchContext {
        deadline_present,
        err_at_respond,
        err_after,
    } = expected_context;
    let expected_context_values = match context.as_str() {
        "active" => (true, "none", "canceled"),
        "cancelled" => (true, "canceled", "canceled"),
        "expired" => (true, "deadline_exceeded", "deadline_exceeded"),
        "none_direct_send" => (false, "none", "none"),
        other => panic!("{id}: unknown context {other}"),
    };
    assert_eq!(
        (
            *deadline_present,
            err_at_respond.as_str(),
            err_after.as_str()
        ),
        expected_context_values,
        "{id}: context projection"
    );
    assert_eq!(
        classification,
        expected_classification(id),
        "{id}: classification"
    );
    let expected_send_calls = usize::from(!matches!(
        id.as_str(),
        "direct_send_returned_encode_error_zero_socket_calls"
            | "compact_ipv4_native_ipv6_panics_before_socket"
            | "responder_panic_is_not_recovered"
    ));
    assert_eq!(*send_calls, expected_send_calls, "{id}: send calls");
    assert_eq!(
        destination.is_some(),
        *send_calls == 1,
        "{id}: destination presence"
    );
    assert_eq!(wire_hex.is_some(), *send_calls == 1, "{id}: wire presence");
    assert_eq!(
        envelope.is_some(),
        *send_calls == 1,
        "{id}: envelope presence"
    );
    if let Some(destination) = destination {
        let DispatchAddr { ip, port, scope } = destination;
        let _ = (ip, port, scope);
        assert_eq!(
            dispatch_addr(destination),
            dispatch_addr(source),
            "{id}: destination"
        );
    }
    if let Some(wire_hex) = wire_hex {
        assert!(!wire_hex.is_empty(), "{id}: empty sent wire");
        let wire = hex::decode(wire_hex).expect("oracle wire hex");
        let decoded = KrpcMessage::decode(&wire).expect("oracle sent wire");
        let envelope = envelope.as_ref().expect("wire without envelope");
        assert_dispatch_envelope(envelope, request, responder_return, classification, id);
        assert_eq!(
            dispatch_expected_message(envelope).encode().unwrap(),
            wire,
            "{id}: exact wire projection"
        );
        assert_eq!(
            decoded.transaction_id.as_bytes(),
            hex::decode(&envelope.tid_hex).unwrap(),
            "{id}: decoded TID"
        );
        assert_eq!(decoded.message_type.as_bytes(), b"r", "{id}: decoded y");
    }

    let DispatchExpectedState {
        before,
        at_send,
        after,
    } = expected_state;
    assert!(before.is_empty(), "{id}: nonempty before state");
    assert_eq!(at_send, after, "{id}: mutation changed after send");
    let expected_mutation = matches!(
        id.as_str(),
        "announce_mutation_precedes_successful_send"
            | "announce_mutation_precedes_failed_send_and_survives"
    )
    .then_some("put_hash:0000000000000000000000000000000000000004@192.0.2.1:6881");
    assert_eq!(
        mutation.as_deref(),
        expected_mutation,
        "{id}: mutation metadata"
    );
    assert_eq!(
        expected_mutation
            .map(|mutation| vec![mutation.to_owned()])
            .unwrap_or_default(),
        *after,
        "{id}: responder mutation/state projection"
    );
    let mut expected_events = Vec::new();
    if *responder_calls == 1 {
        expected_events.push("respond");
    }
    if mutation.is_some() {
        expected_events.push("mutate");
    }
    if *send_calls == 1 {
        expected_events.push("send");
    }
    assert_eq!(
        events.iter().map(String::as_str).collect::<Vec<_>>(),
        expected_events,
        "{id}: events"
    );

    let expected_log = match (kind.as_str(), socket_kind.as_str()) {
        (_, "error") => Some((
            "debug",
            "could not send response",
            "*errors.errorString",
            "dispatch transport sentinel".to_owned(),
        )),
        ("protocol_pointer", _) => Some((
            "error",
            "server error",
            "*dht.Error",
            String::from_utf8(hex::decode(error_hex.as_ref().unwrap()).unwrap()).unwrap(),
        )),
        ("wrapped_protocol_pointer", _) => Some((
            "error",
            "server error",
            "*fmt.wrapError",
            String::from_utf8(hex::decode(error_hex.as_ref().unwrap()).unwrap()).unwrap(),
        )),
        ("typed_nil_protocol_pointer", _) => Some((
            "error",
            "server error",
            "*dht.Error",
            "<typed nil *dht.Error>".to_owned(),
        )),
        ("generic", _) => Some((
            "error",
            "server error",
            "*errors.errorString",
            String::from_utf8(hex::decode(error_hex.as_ref().unwrap()).unwrap()).unwrap(),
        )),
        ("context_error", _) => Some((
            "error",
            "server error",
            "context.deadlineExceededError",
            "context deadline exceeded".to_owned(),
        )),
        _ => None,
    };
    assert_eq!(
        logs.len(),
        usize::from(expected_log.is_some()),
        "{id}: log count"
    );
    if let Some((level, message, error_type, error_text)) = expected_log {
        let DispatchLog {
            level: actual_level,
            message: actual_message,
            ret_err_key,
            ret_err_type,
            ret_err_text,
            ret_err_identity_exact,
        } = &logs[0];
        assert_eq!(actual_level, level, "{id}: log level");
        assert_eq!(actual_message, message, "{id}: log message");
        assert!(*ret_err_key, "{id}: log retErr key");
        assert_eq!(ret_err_type, error_type, "{id}: log error type");
        assert_eq!(ret_err_text, &error_text, "{id}: log error text");
        assert!(*ret_err_identity_exact, "{id}: log error identity");
    }

    let partial_expected = matches!(
        kind.as_str(),
        "protocol_value"
            | "wrapped_protocol_value"
            | "protocol_pointer"
            | "wrapped_protocol_pointer"
            | "typed_nil_protocol_pointer"
            | "generic"
            | "context_error"
    );
    assert_optional_omitempty_bool(
        *partial_return_discarded,
        partial_expected,
        "partialReturnDiscarded",
        id,
    );
    assert_optional_omitempty_bool(
        *send_failure_swallowed,
        socket_kind == "error",
        "sendFailureSwallowed",
        id,
    );
    let expected_terminal = match id.as_str() {
        "direct_send_returned_encode_error_zero_socket_calls" => "direct_send_returned",
        "compact_ipv4_native_ipv6_panics_before_socket"
        | "responder_panic_is_not_recovered"
        | "socket_panic_after_one_call_is_not_recovered" => "panicked",
        _ => "returned",
    };
    assert_eq!(terminal, expected_terminal, "{id}: terminal");
    let expected_panic = match id.as_str() {
        "compact_ipv4_native_ipv6_panics_before_socket" => {
            Some(("marshalled 22 bytes, but expected 26", false))
        }
        "responder_panic_is_not_recovered" => Some(("dispatch responder panic sentinel", true)),
        "socket_panic_after_one_call_is_not_recovered" => {
            Some(("dispatch socket panic sentinel", true))
        }
        _ => None,
    };
    assert_eq!(
        panic_text.as_deref(),
        expected_panic.map(|(text, _)| text),
        "{id}: panic text"
    );
    assert_optional_omitempty_bool(
        *panic_identity_exact,
        expected_panic.is_some_and(|(_, identity)| identity),
        "panicIdentityExact",
        id,
    );
    if direct_send {
        let DispatchReturnedError { error_type, text } = returned_error
            .as_ref()
            .expect("direct-send row omitted returned error");
        assert_eq!(error_type, "*bencode.MarshalTypeError", "{id}");
        assert_eq!(text, "bencode: unsupported type: float64", "{id}");
    } else {
        assert!(returned_error.is_none(), "{id}: unexpected returned error");
    }
}

#[tokio::test]
async fn actual_go_handle_query_rows_replay_supported_wires_and_bound_outer_evidence() {
    let fixtures = dispatch_send_fixtures();
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.id.as_str())
            .collect::<Vec<_>>(),
        DISPATCH_SEND_FIXTURE_IDS
    );
    let mut supported = Vec::new();
    let mut outer = Vec::new();

    for fixture in &fixtures {
        assert_dispatch_fixture_schema(fixture);
        let fixture_id = fixture.id.as_str();
        let input = &fixture.input;
        let expected = &fixture.expected;
        let request = oracle_request(&input.request);
        assert_eq!(
            hex::encode(request.transaction_id.as_bytes()),
            input.request.tid_hex,
            "{fixture_id}: request TID projection"
        );
        assert_eq!(
            hex::encode(request.message_type.as_bytes()),
            input.request.type_hex,
            "{fixture_id}: request y projection"
        );
        assert_eq!(
            hex::encode(request.query.as_bytes()),
            input.request.method_hex,
            "{fixture_id}: request method projection"
        );
        assert_eq!(
            request.args.is_some(),
            input.request.args_present,
            "{fixture_id}"
        );

        if !RUST_SUPPORTED_HANDLE_QUERY_ROWS.contains(&fixture_id) {
            let scope = outer_evidence_scope(fixture_id);
            match scope {
                "scripted_return_only" => {
                    assert_eq!(input.responder.kind, "none");
                    assert_eq!(expected.classification, "success");
                }
                "go_error_classification_only" => {
                    assert_ne!(input.responder.kind, "none");
                    assert_eq!(expected.partial_return_discarded, Some(true));
                    let envelope = expected.envelope.as_ref().unwrap();
                    assert!(envelope.returned.is_none());
                    assert!(envelope.error.is_some());
                }
                "go_context_policy_only" => {
                    assert!(matches!(input.context.as_str(), "cancelled" | "expired"));
                    assert!(expected.context.deadline_present);
                }
                "go_log_swallow_policy_only" => {
                    assert_eq!(expected.send_failure_swallowed, Some(true));
                    assert_eq!(expected.send_calls, 1);
                    assert_eq!(expected.logs.len(), 1);
                }
                "go_direct_send_encoder_only" => {
                    assert_eq!(input.context, "none_direct_send");
                    assert_eq!(input.responder.unsupported_v, Some(true));
                    assert_eq!(expected.terminal, "direct_send_returned");
                }
                "go_compact_encoder_panic_only" | "go_unwind_policy_only" => {
                    assert_eq!(expected.terminal, "panicked");
                    assert!(expected
                        .panic_text
                        .as_ref()
                        .is_some_and(|text| !text.is_empty()));
                }
                other => panic!("unknown evidence scope {other}"),
            }
            outer.push((fixture_id, scope));
            continue;
        }

        let method = request.query.as_bytes();
        let returned = &input.responder.returned;
        let origin = returned.id;
        let table = ScriptedTable::new(origin, oracle_table_script(returned, method));
        let interval = returned.interval;
        let dispatcher = DhtDispatcher::from_responder(DhtResponder::with_token_secret(
            table.clone(),
            [0; 20],
            interval,
        ));
        let source = dispatch_addr(&input.source);
        let outcome = dispatcher.dispatch(source, &request);
        assert!(matches!(outcome, DhtDispatchOutcome::Reply(_)));
        assert_clean(outcome.reply(), &request, source);
        let expected_wire = hex::decode(expected.wire_hex.as_ref().unwrap()).unwrap();
        assert_eq!(
            outcome.reply().wire().unwrap(),
            expected_wire,
            "{fixture_id}"
        );
        assert_eq!(
            outcome.reply().message,
            KrpcMessage::decode(&expected_wire).unwrap(),
            "{fixture_id}"
        );
        let (mut sender, observations) = ImmediateSender::new(Ok(()));
        send_dht_reply(&mut sender, outcome.reply()).await.unwrap();
        let observations = observations.lock().unwrap();
        assert_eq!(observations.destinations, [source]);
        assert_eq!(observations.wires, [expected_wire]);
        assert_eq!(expected.send_calls, 1);
        supported.push(fixture_id);
    }

    assert_eq!(supported, RUST_SUPPORTED_HANDLE_QUERY_ROWS);
    assert_eq!(
        outer.len(),
        DISPATCH_SEND_FIXTURE_IDS.len() - supported.len()
    );
    assert!(outer
        .iter()
        .any(|(_, scope)| *scope == "go_error_classification_only"));
    assert!(outer
        .iter()
        .any(|(_, scope)| *scope == "go_context_policy_only"));
    assert!(outer
        .iter()
        .any(|(_, scope)| *scope == "go_log_swallow_policy_only"));
    assert!(outer
        .iter()
        .any(|(_, scope)| *scope == "go_direct_send_encoder_only"));
    assert!(outer
        .iter()
        .any(|(_, scope)| *scope == "go_compact_encoder_panic_only"));
    assert!(outer
        .iter()
        .any(|(_, scope)| *scope == "go_unwind_policy_only"));
    assert_eq!(
        fixtures
            .iter()
            .find(|fixture| fixture.id == "sample_populated_long_tid_signed_extremes")
            .unwrap()
            .input
            .request
            .tid_hex
            .len(),
        514,
        "257-byte transaction ID oracle"
    );
}

fn id(last: u8) -> Id20 {
    let mut bytes = [0; 20];
    bytes[19] = last;
    Id20::from_slice(&bytes).unwrap()
}

fn empty_return(local_id: Id20) -> MessageReturn {
    MessageReturn {
        id: local_id,
        nodes: None,
        nodes6: None,
        token: None,
        values: None,
        interval: None,
        num: None,
        samples: None,
        seeders_bloom: None,
        peers_bloom: None,
    }
}

fn args(sender_id: Id20) -> MessageArgs {
    MessageArgs {
        id: sender_id,
        info_hash: None,
        target: None,
        token: ByteString::default(),
        port: None,
        implied_port: false,
        want: None,
        no_seed: 0,
        scrape: 0,
    }
}

fn request(method: &[u8], transaction_id: &[u8], args: Option<MessageArgs>) -> KrpcMessage {
    KrpcMessage {
        transaction_id: ByteString::new(transaction_id),
        message_type: ByteString::new(b"q"),
        query: ByteString::new(method),
        args,
        response: None,
        error: None,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    }
}

struct PreparedAnnounce {
    table: KTable,
    info_hash: Id20,
    peer: SocketAddr,
    outcome: DhtDispatchOutcome,
}

fn prepare_announce(sequence: u8) -> PreparedAnnounce {
    let local_id = id(240);
    let sender_id = id(2);
    let info_hash = id(sequence);
    let table = KTable::new(local_id);
    let dispatcher = DhtDispatcher::from_responder(DhtResponder::with_token_secret(
        table.clone(),
        [0x5a; 20],
        10,
    ));
    let token_source = SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::new(198, 51, 100, sequence),
        4000,
    ));
    let peer = SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::new(198, 51, 100, sequence),
        5000,
    ));
    let mut get_args = args(sender_id);
    get_args.info_hash = Some(info_hash);
    let token_outcome =
        dispatcher.dispatch(token_source, &request(b"get_peers", b"G1", Some(get_args)));
    let token = token_outcome
        .reply()
        .message
        .response
        .as_ref()
        .and_then(|response| response.token.clone())
        .expect("announce token");
    let mut announce_args = args(sender_id);
    announce_args.info_hash = Some(info_hash);
    announce_args.token = token;
    let outcome = dispatcher.dispatch(peer, &request(b"announce_peer", b"A1", Some(announce_args)));
    assert!(matches!(outcome, DhtDispatchOutcome::Reply(_)));
    let prepared = PreparedAnnounce {
        table,
        info_hash,
        peer,
        outcome,
    };
    assert_announce(&prepared);
    prepared
}

fn assert_announce(prepared: &PreparedAnnounce) {
    assert_eq!(
        prepared.table.hash(prepared.info_hash).unwrap().peers,
        vec![KTableHashPeer {
            addr: prepared.peer
        }]
    );
}

fn reply_mut(outcome: &mut DhtDispatchOutcome) -> &mut DhtReply {
    match outcome {
        DhtDispatchOutcome::Reply(reply) | DhtDispatchOutcome::LocalFailure { reply, .. } => reply,
    }
}

#[derive(Clone, Debug)]
struct SendFailure(Arc<()>);

impl Display for SendFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("send sentinel")
    }
}

impl Error for SendFailure {}

#[derive(Default)]
struct SendObservations {
    destinations: Vec<SocketAddr>,
    wires: Vec<Vec<u8>>,
}

struct ImmediateSender {
    observations: Arc<Mutex<SendObservations>>,
    result: Option<Result<(), SendFailure>>,
}

impl ImmediateSender {
    fn new(result: Result<(), SendFailure>) -> (Self, Arc<Mutex<SendObservations>>) {
        let observations = Arc::new(Mutex::new(SendObservations::default()));
        (
            Self {
                observations: Arc::clone(&observations),
                result: Some(result),
            },
            observations,
        )
    }
}

impl DatagramSender for ImmediateSender {
    type Error = SendFailure;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let mut observations = self.observations.lock().unwrap();
        observations.destinations.push(destination);
        observations.wires.push(datagram.to_vec());
        drop(observations);
        let result = self.result.take().expect("exactly one send");
        Box::pin(async move { result })
    }
}

struct DropTrackedPending {
    drops: Arc<AtomicUsize>,
}

impl Future for DropTrackedPending {
    type Output = Result<(), SendFailure>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for DropTrackedPending {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct PendingSender {
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl DatagramSender for PendingSender {
    type Error = SendFailure;

    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        _datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(DropTrackedPending {
            drops: Arc::clone(&self.drops),
        })
    }
}

struct ConstructionPanicSender(Arc<AtomicUsize>);

impl DatagramSender for ConstructionPanicSender {
    type Error = SendFailure;

    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        _datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("sender construction panic")
    }
}

struct PollPanicFuture;

impl Future for PollPanicFuture {
    type Output = Result<(), SendFailure>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        panic!("sender poll panic")
    }
}

struct PollPanicSender(Arc<AtomicUsize>);

impl DatagramSender for PollPanicSender {
    type Error = SendFailure;

    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        _datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(PollPanicFuture)
    }
}

struct GatedSender {
    observations: Arc<Mutex<SendObservations>>,
    release: Option<oneshot::Receiver<()>>,
}

impl DatagramSender for GatedSender {
    type Error = SendFailure;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let mut observations = self.observations.lock().unwrap();
        observations.destinations.push(destination);
        observations.wires.push(datagram.to_vec());
        drop(observations);
        let release = self.release.take().expect("exactly one send");
        Box::pin(async move {
            release.await.expect("release gated sender");
            Ok(())
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
struct TooLarge {
    actual: usize,
}

impl Display for TooLarge {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "datagram too large: {}", self.actual)
    }
}

struct SizeRejectingSender {
    calls: usize,
}

impl DatagramSender for SizeRejectingSender {
    type Error = TooLarge;

    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        self.calls += 1;
        let actual = datagram.len();
        Box::pin(async move { Err(TooLarge { actual }) })
    }
}

#[tokio::test]
async fn announce_mutation_is_never_rolled_back_by_send_lifecycle() {
    let prepared = prepare_announce(11);
    let (mut sender, observations) = ImmediateSender::new(Ok(()));
    let future = send_dht_reply(&mut sender, prepared.outcome.reply());
    assert!(observations.lock().unwrap().wires.is_empty());
    drop(future);
    assert!(observations.lock().unwrap().wires.is_empty());
    assert_announce(&prepared);

    let mut prepared = prepare_announce(12);
    reply_mut(&mut prepared.outcome)
        .message
        .response
        .as_mut()
        .unwrap()
        .nodes = Some(vec![CompactNode {
        id: Id20::ZERO,
        addr: CompactAddr {
            ip: "2001:db8::1".parse().unwrap(),
            port: 6881,
        },
    }]);
    let (mut sender, observations) = ImmediateSender::new(Ok(()));
    let error = send_dht_reply(&mut sender, prepared.outcome.reply())
        .await
        .unwrap_err();
    assert!(matches!(error, DhtSendError::Encode(_)));
    assert!(Error::source(&error).is_some());
    assert!(observations.lock().unwrap().wires.is_empty());
    assert_announce(&prepared);

    let prepared = prepare_announce(13);
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut sender = PendingSender {
        calls: Arc::clone(&calls),
        drops: Arc::clone(&drops),
    };
    let mut future = Box::pin(send_dht_reply(&mut sender, prepared.outcome.reply()));
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(future);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_announce(&prepared);

    let prepared = prepare_announce(14);
    let table = prepared.table.clone();
    let info_hash = prepared.info_hash;
    let peer = prepared.peer;
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let task_drops = Arc::clone(&drops);
    let task = tokio::spawn(async move {
        let mut sender = PendingSender {
            calls: task_calls,
            drops: task_drops,
        };
        send_dht_reply(&mut sender, prepared.outcome.reply()).await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pending send was never constructed");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        table.hash(info_hash).unwrap().peers,
        vec![KTableHashPeer { addr: peer }]
    );

    let prepared = prepare_announce(15);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut sender = ConstructionPanicSender(Arc::clone(&calls));
    let mut future = Box::pin(send_dht_reply(&mut sender, prepared.outcome.reply()));
    let mut context = Context::from_waker(Waker::noop());
    let panicked = panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = future.as_mut().poll(&mut context);
    }));
    assert!(panicked.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(future);
    assert_announce(&prepared);

    let prepared = prepare_announce(16);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut sender = PollPanicSender(Arc::clone(&calls));
    let mut future = Box::pin(send_dht_reply(&mut sender, prepared.outcome.reply()));
    let mut context = Context::from_waker(Waker::noop());
    let panicked = panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = future.as_mut().poll(&mut context);
    }));
    assert!(panicked.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(future);
    assert_announce(&prepared);

    let prepared = prepare_announce(17);
    let sentinel = SendFailure(Arc::new(()));
    let (mut sender, observations) = ImmediateSender::new(Err(sentinel.clone()));
    let error = send_dht_reply(&mut sender, prepared.outcome.reply())
        .await
        .unwrap_err();
    let DhtSendError::Transport(actual) = error else {
        panic!("expected transport failure")
    };
    assert!(Arc::ptr_eq(&actual.0, &sentinel.0));
    assert!(Error::source(&DhtSendError::Transport(actual)).is_none());
    assert_eq!(observations.lock().unwrap().wires.len(), 1);
    assert_announce(&prepared);
}

#[tokio::test]
async fn one_send_preserves_backpressure_and_sender_owns_oversize_rejection() {
    let prepared = prepare_announce(18);
    let observations = Arc::new(Mutex::new(SendObservations::default()));
    let (release_tx, release_rx) = oneshot::channel();
    let mut sender = GatedSender {
        observations: Arc::clone(&observations),
        release: Some(release_rx),
    };
    let mut future = Box::pin(send_dht_reply(&mut sender, prepared.outcome.reply()));
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    {
        let observations = observations.lock().unwrap();
        assert_eq!(observations.destinations, [prepared.peer]);
        assert_eq!(
            observations.wires,
            [prepared.outcome.reply().wire().unwrap()]
        );
    }
    release_tx.send(()).unwrap();
    future.await.unwrap();
    assert_eq!(observations.lock().unwrap().wires.len(), 1);
    assert_announce(&prepared);

    let mut prepared = prepare_announce(19);
    reply_mut(&mut prepared.outcome).message.transaction_id =
        ByteString::new(vec![0xab; MAX_INBOUND_DATAGRAM_BYTES]);
    let wire_length = prepared.outcome.reply().wire().unwrap().len();
    assert!(wire_length > MAX_INBOUND_DATAGRAM_BYTES);
    let mut sender = SizeRejectingSender { calls: 0 };
    let error = send_dht_reply(&mut sender, prepared.outcome.reply())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        DhtSendError::Transport(TooLarge { actual }) if actual == wire_length
    ));
    assert_eq!(sender.calls, 1);
    assert_announce(&prepared);
}

#[tokio::test]
async fn prepared_local_cause_survives_exact_send_and_scoped_flow_destination() {
    let local_id = id(220);
    let node = RoutingNode {
        id: id(221),
        addr: SocketAddr::V6(SocketAddrV6::new(
            "2001:db8::1".parse().unwrap(),
            6881,
            0,
            0,
        )),
    };
    let table = KTable::new(local_id);
    assert_eq!(table.put_node(node), RoutingPutResult::Accepted);
    let dispatcher = DhtDispatcher::from_responder(DhtResponder::with_token_secret(
        table.clone(),
        [0x44; 20],
        10,
    ));
    let mut request_args = args(id(2));
    request_args.target = Some(node.id);
    let source = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 17, 9));
    let outcome = dispatcher.dispatch(source, &request(b"find_node", b"N1", Some(request_args)));
    let DhtDispatchOutcome::LocalFailure { reply, cause } = &outcome else {
        panic!("expected local failure")
    };
    assert_eq!(cause, &DhtResponderError::NativeIpv6Node(node));
    assert_eq!(reply.destination, source);
    assert_eq!(reply.message.error.as_ref().unwrap().code, 202);
    assert!(reply.message.response.is_none());
    let (mut sender, observations) = ImmediateSender::new(Ok(()));
    send_dht_reply(&mut sender, outcome.reply()).await.unwrap();
    let observations = observations.lock().unwrap();
    assert_eq!(observations.destinations, [source]);
    assert_eq!(observations.wires, [reply.wire().unwrap()]);
    assert_eq!(cause, &DhtResponderError::NativeIpv6Node(node));
    assert_eq!(table.closest_nodes(node.id)[0].routing_node(), node);
}

const fn legacy_dispatcher<'a>(table: &'a NodeTable) -> PingFindNodeDispatcher<'a> {
    PingFindNodeDispatcher::new(table)
}

fn legacy_responder_error(error: &PingFindNodeError) -> &'static str {
    match error {
        PingFindNodeError::Protocol(_) => "protocol",
        PingFindNodeError::NativeIpv6Node(_) => "native_ipv6",
    }
}

fn legacy_dispatch_outcome(outcome: &PingFindNodeDispatchOutcome) -> &'static str {
    match outcome {
        PingFindNodeDispatchOutcome::Reply(_) => "reply",
        PingFindNodeDispatchOutcome::LocalFailure { .. } => "local_failure",
    }
}

fn legacy_send_error(error: &PingFindNodeSendError<SendFailure>) -> &'static str {
    match error {
        PingFindNodeSendError::Encode(_) => "encode",
        PingFindNodeSendError::Transport(_) => "transport",
    }
}

#[tokio::test]
async fn legacy_partial_dispatch_and_send_remain_const_and_exhaustive() {
    let table = NodeTable::new(id(230));
    let dispatcher = legacy_dispatcher(&table);
    let query = request(b"ping", b"L1", Some(args(id(2))));
    let outcome = dispatcher
        .dispatch("192.0.2.1:1".parse().unwrap(), &query)
        .unwrap();
    assert_eq!(legacy_dispatch_outcome(&outcome), "reply");
    let reply = match &outcome {
        PingFindNodeDispatchOutcome::Reply(reply)
        | PingFindNodeDispatchOutcome::LocalFailure { reply, .. } => reply,
    };
    let cloned_literal = PingFindNodeReply {
        destination: reply.destination,
        message: reply.message.clone(),
    };
    let sentinel = SendFailure(Arc::new(()));
    let (mut sender, _) = ImmediateSender::new(Err(sentinel));
    let error = send_ping_find_node_reply(&mut sender, &cloned_literal)
        .await
        .unwrap_err();
    assert_eq!(legacy_send_error(&error), "transport");
    let protocol = PingFindNodeError::Protocol(KrpcError {
        code: 203,
        message: ByteString::new(b"missing arguments"),
    });
    assert_eq!(legacy_responder_error(&protocol), "protocol");
}
