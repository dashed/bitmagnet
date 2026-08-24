//! Differential replay and concurrency gates for the pure DHT responder.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::Path;
use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

use bitmagnet_dht::{
    ByteString, CompactAddr, CompactCodecError, CompactNode, DhtResponder, DhtResponderError,
    DhtResponderLookup, DhtResponderSample, DhtResponderTable, Id20, KTable, KTableCommand,
    KTableHashPeer, KrpcMessage, MessageArgs, MessageReturn, RoutingNode, RoutingPutResult,
    WireError,
};
use serde::Deserialize;

const FIXTURE_IDS: [&str; 40] = [
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

#[derive(Debug, Deserialize)]
struct Fixture {
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
                id,
                "fixture hash identity differs from the queried hash"
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

fn fixtures() -> Vec<Fixture> {
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
            assert!(value.zone.is_empty(), "IPv4 fixture address has a zone");
            SocketAddr::V4(SocketAddrV4::new(ip, value.port))
        }
        IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(
            ip,
            value.port,
            0,
            if value.zone.is_empty() {
                0
            } else {
                value.zone.parse().expect("numeric fixture IPv6 zone")
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

fn fixture_request(value: &FixtureStep, token_override: Option<ByteString>) -> KrpcMessage {
    let args = match value.args_presence.as_str() {
        "nil" => None,
        "present" => Some(fixture_args(&value.args, token_override)),
        other => panic!("unknown args presence {other}"),
    };
    KrpcMessage {
        transaction_id: ByteString::new(*b"R1"),
        message_type: ByteString::new(b"q"),
        query: ByteString::new(value.method.as_bytes()),
        args,
        response: None,
        error: None,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    }
}

fn fixture_node(value: CompactNode) -> FixtureNode {
    FixtureNode {
        id: value.id,
        addr: FixtureAddr {
            ip: value.addr.ip,
            port: value.addr.port,
            zone: String::new(),
        },
    }
}

fn optional_slice_presence<T>(value: &Option<Vec<T>>) -> String {
    match value {
        None => "nil",
        Some(values) if values.is_empty() => "empty",
        Some(_) => "present",
    }
    .to_owned()
}

fn optional_presence<T>(value: &Option<T>) -> String {
    if value.is_some() { "present" } else { "nil" }.to_owned()
}

fn project_return(value: &MessageReturn) -> FixtureReturn {
    FixtureReturn {
        id: value.id,
        nodes_presence: optional_slice_presence(&value.nodes),
        nodes: value
            .nodes
            .clone()
            .map(|nodes| nodes.into_iter().map(fixture_node).collect()),
        nodes6_presence: optional_slice_presence(&value.nodes6),
        nodes6: value
            .nodes6
            .clone()
            .map(|nodes| nodes.into_iter().map(fixture_node).collect()),
        values_presence: optional_slice_presence(&value.values),
        values: value.values.clone().map(|values| {
            values
                .into_iter()
                .map(|addr| FixtureAddr {
                    ip: addr.ip,
                    port: addr.port,
                    zone: String::new(),
                })
                .collect()
        }),
        token_presence: optional_presence(&value.token),
        token_hex: value
            .token
            .as_ref()
            .map_or_else(String::new, |token| hex::encode(token.as_bytes())),
        samples_presence: optional_slice_presence(&value.samples),
        samples: value.samples.clone(),
        num_presence: optional_presence(&value.num),
        num: value.num.unwrap_or_default(),
        interval_presence: optional_presence(&value.interval),
        interval: value.interval.unwrap_or_default(),
        peers_bloom_presence: optional_presence(&value.peers_bloom),
        seeders_bloom_presence: optional_presence(&value.seeders_bloom),
        bep44_fields_are_zero: true,
    }
}

fn expected_message_return(value: &FixtureReturn) -> MessageReturn {
    assert!(value.bep44_fields_are_zero);
    assert_eq!(value.peers_bloom_presence, "nil");
    assert_eq!(value.seeders_bloom_presence, "nil");
    let nodes = value.nodes.clone().map(|nodes| {
        nodes
            .into_iter()
            .map(|node| {
                assert!(node.addr.zone.is_empty());
                CompactNode {
                    id: node.id,
                    addr: CompactAddr {
                        ip: node.addr.ip,
                        port: node.addr.port,
                    },
                }
            })
            .collect()
    });
    let nodes6 = value.nodes6.clone().map(|nodes| {
        nodes
            .into_iter()
            .map(|node| CompactNode {
                id: node.id,
                addr: CompactAddr {
                    ip: node.addr.ip,
                    port: node.addr.port,
                },
            })
            .collect()
    });
    let values = value.values.clone().map(|values| {
        values
            .into_iter()
            .map(|addr| {
                assert!(addr.zone.is_empty());
                CompactAddr {
                    ip: addr.ip,
                    port: addr.port,
                }
            })
            .collect()
    });
    let token = match value.token_presence.as_str() {
        "nil" => None,
        "present" => Some(ByteString::new(
            hex::decode(&value.token_hex).expect("expected token hex"),
        )),
        other => panic!("unknown token presence {other}"),
    };
    let samples = match value.samples_presence.as_str() {
        "nil" => None,
        "empty" | "present" => Some(value.samples.clone().unwrap_or_default()),
        other => panic!("unknown samples presence {other}"),
    };
    let num = match value.num_presence.as_str() {
        "nil" => None,
        "present" => Some(value.num),
        other => panic!("unknown num presence {other}"),
    };
    let interval = match value.interval_presence.as_str() {
        "nil" => None,
        "present" => Some(value.interval),
        other => panic!("unknown interval presence {other}"),
    };
    let result = MessageReturn {
        id: value.id,
        nodes,
        nodes6,
        token,
        values,
        interval,
        num,
        samples,
        seeders_bloom: None,
        peers_bloom: None,
    };
    assert_eq!(project_return(&result), *value);
    result
}

fn response_envelope(response: MessageReturn) -> KrpcMessage {
    KrpcMessage {
        transaction_id: ByteString::new(*b"R1"),
        message_type: ByteString::new(b"r"),
        query: ByteString::default(),
        args: None,
        response: Some(response),
        error: None,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
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

fn id(last: u8) -> Id20 {
    let mut bytes = [0; 20];
    bytes[19] = last;
    Id20::from_slice(&bytes).expect("fixed-width ID")
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

fn request(method: &[u8], args: Option<MessageArgs>) -> KrpcMessage {
    KrpcMessage {
        transaction_id: ByteString::new(*b"R1"),
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

fn get_peers_request(sender_id: Id20, info_hash: Id20) -> KrpcMessage {
    let mut args = args(sender_id);
    args.info_hash = Some(info_hash);
    request(b"get_peers", Some(args))
}

fn announce_request(sender_id: Id20, info_hash: Id20, token: ByteString) -> KrpcMessage {
    let mut args = args(sender_id);
    args.info_hash = Some(info_hash);
    args.token = token;
    request(b"announce_peer", Some(args))
}

#[test]
fn real_go_full_responder_fixture_matches_exact_rust_projection_and_effects() {
    let fixtures = fixtures();
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.id.as_str())
            .collect::<Vec<_>>(),
        FIXTURE_IDS
    );
    let mut tokens = HashMap::<String, ByteString>::new();
    let mut native_ipv6_deltas = 0;

    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_responder", "{}", fixture.id);
        assert_eq!(fixture.runtime.int_bits, 64, "{}", fixture.id);
        assert_eq!(fixture.expected.normalization, "none", "{}", fixture.id);
        assert_eq!(
            fixture.input.steps.len(),
            fixture.expected.outcomes.len(),
            "{} outcome count",
            fixture.id
        );
        let token_secret: [u8; 20] = hex::decode(&fixture.config.token_secret_hex)
            .expect("fixture token secret hex")
            .try_into()
            .expect("20-byte fixture token secret");
        let table = ScriptedTable::new(fixture.config.node_id, fixture.input.table.clone());
        assert_eq!(
            table.put_state(),
            fixture.expected.table_state.before,
            "{} table before",
            fixture.id
        );
        let responder = DhtResponder::with_token_secret(
            table.clone(),
            token_secret,
            fixture.config.sample_info_hashes_interval,
        );
        let mut step_tokens = Vec::<Option<ByteString>>::new();

        for (index, (step, expected)) in fixture
            .input
            .steps
            .iter()
            .zip(&fixture.expected.outcomes)
            .enumerate()
        {
            let token_override = step.token_from_step.map(|from| {
                assert!(from < index, "{} step {index} future token", fixture.id);
                assert!(step.args.token_hex.is_empty());
                step_tokens[from]
                    .clone()
                    .expect("referenced step returned no token")
            });
            let actual = responder.respond(
                socket_addr(&step.source),
                &fixture_request(step, token_override),
            );

            if NATIVE_IPV6_CASES.contains(&fixture.id.as_str()) {
                assert!(expected.error.is_none(), "{} Go error metadata", fixture.id);
                let DhtResponderError::NativeIpv6Node(actual_node) = actual.unwrap_err() else {
                    panic!("{} did not retain the native IPv6 node", fixture.id)
                };
                let expected_node = first_native_node(&fixture.input.table);
                assert_eq!(actual_node, expected_node, "{} local cause", fixture.id);
                let SocketAddr::V6(actual_addr) = actual_node.addr else {
                    unreachable!()
                };
                assert_eq!(actual_addr.scope_id(), 7, "{} input zone", fixture.id);

                let go_return = expected_message_return(&expected.returned);
                let expected_nodes = expected
                    .returned
                    .nodes
                    .as_ref()
                    .expect("Go native projection nodes");
                assert!(expected_nodes.iter().all(|node| node.addr.zone.is_empty()));
                assert!(expected_nodes.iter().any(|node| {
                    node.id == actual_node.id
                        && node.addr.ip == actual_node.addr.ip()
                        && node.addr.port == actual_node.addr.port()
                }));
                let encoded = std::panic::catch_unwind(|| response_envelope(go_return).encode());
                assert!(encoded.is_ok(), "{} Rust encoder panicked", fixture.id);
                assert!(matches!(
                    encoded.unwrap(),
                    Err(WireError::Compact(CompactCodecError::WrongAddressFamily {
                        expected: "IPv4"
                    }))
                ));
                step_tokens.push(None);
                native_ipv6_deltas += 1;
                continue;
            }

            match (&expected.error, actual) {
                (None, Ok(actual_return)) => {
                    assert_eq!(
                        project_return(&actual_return),
                        expected.returned,
                        "{} step {index} return",
                        fixture.id
                    );
                    let token = actual_return.token.clone();
                    if index == 0 {
                        if let Some(token) = &token {
                            tokens.insert(fixture.id.clone(), token.clone());
                        }
                    }
                    step_tokens.push(token);
                }
                (Some(expected_error), Err(actual_error)) => {
                    let actual_text = actual_error.to_string();
                    let DhtResponderError::Protocol(actual_protocol) = actual_error else {
                        panic!("{} step {index} returned local error", fixture.id)
                    };
                    assert_eq!(
                        actual_protocol.code, expected_error.code,
                        "{} code",
                        fixture.id
                    );
                    assert_eq!(
                        actual_protocol.message.as_bytes(),
                        expected_error.message.as_bytes(),
                        "{} message",
                        fixture.id
                    );
                    assert_eq!(actual_text, expected_error.text, "{} text", fixture.id);
                    let partial_id = if step.args_presence == "present" {
                        fixture.config.node_id
                    } else {
                        Id20::ZERO
                    };
                    assert_eq!(
                        project_return(&empty_return(partial_id)),
                        expected.returned,
                        "{} Go partial return metadata",
                        fixture.id
                    );
                    step_tokens.push(None);
                }
                (None, Err(error)) => {
                    panic!("{} step {index} unexpected error: {error}", fixture.id)
                }
                (Some(error), Ok(returned)) => panic!(
                    "{} step {index} expected {} but returned {returned:?}",
                    fixture.id, error.text
                ),
            }
        }

        assert_eq!(
            table.calls(),
            fixture.expected.table_calls,
            "{} exact table calls",
            fixture.id
        );
        assert_eq!(
            table.put_state(),
            fixture.expected.table_state.after,
            "{} table after",
            fixture.id
        );
    }

    assert_eq!(native_ipv6_deltas, NATIVE_IPV6_CASES.len());
    let token = |case: &str| {
        tokens
            .get(case)
            .unwrap_or_else(|| panic!("missing {case} token"))
    };
    let base = token("get_peers_found_ordered_duplicate_values_ipv4_golden");
    assert_eq!(base.as_bytes(), b"266127f80b327ff927362ec21a79e923");
    assert_eq!(token("get_peers_token_port_independence"), base);
    for case in [
        "get_peers_zero_requester_token_sensitivity",
        "get_peers_token_source_ip_sensitivity",
        "get_peers_token_infohash_sensitivity",
        "get_peers_token_requester_sensitivity",
        "get_peers_token_mapped_ipv6_golden",
        "get_peers_token_native_ipv6_numeric_zone7",
        "get_peers_token_native_ipv6_numeric_zone8",
    ] {
        assert_ne!(token(case), base, "{case}");
    }
    assert_ne!(
        token("get_peers_token_native_ipv6_numeric_zone7"),
        token("get_peers_token_native_ipv6_numeric_zone8")
    );
}

#[test]
fn fixed_secret_token_is_repeatable_and_independent_of_source_port() {
    let local_id = id(240);
    let sender_id = id(2);
    let info_hash = id(3);
    let responder = DhtResponder::with_token_secret(KTable::new(local_id), [0x5a; 20], -7);
    let query = get_peers_request(sender_id, info_hash);
    let source_a = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 9), 4100));
    let source_b = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 9), 65000));
    let source_other = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 10), 4100));

    let first = responder
        .respond(source_a, &query)
        .expect("first peer lookup")
        .token
        .expect("announce token");
    let repeat = responder
        .respond(source_a, &query)
        .expect("repeated peer lookup")
        .token
        .expect("repeated announce token");
    let changed_port = responder
        .respond(source_b, &query)
        .expect("same IP with another source port")
        .token
        .expect("port-independent token");
    let changed_ip = responder
        .respond(source_other, &query)
        .expect("other source IP")
        .token
        .expect("IP-bound token");

    assert_eq!(first, repeat);
    assert_eq!(first, changed_port);
    assert_ne!(first, changed_ip);
    assert_eq!(first.as_bytes().len(), 32);
    assert!(first
        .as_bytes()
        .iter()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte)));
}

#[test]
fn production_constructor_and_responder_clones_retain_one_shared_table_and_secret() {
    let local_id = id(241);
    let sender_id = id(4);
    let info_hash = id(5);
    let table = KTable::new(local_id);
    let responder = DhtResponder::new(&table, 10).expect("random responder secret");
    let responder_clone = responder.clone();
    drop(table);

    let token_source: SocketAddr = "192.0.2.44:4000".parse().unwrap();
    let announce_source: SocketAddr = "192.0.2.44:5000".parse().unwrap();
    let token = responder
        .respond(token_source, &get_peers_request(sender_id, info_hash))
        .expect("token after caller table drop")
        .token
        .expect("announce token");
    assert_eq!(
        responder_clone
            .respond(announce_source, &get_peers_request(sender_id, info_hash))
            .unwrap()
            .token,
        Some(token.clone()),
        "cloned responder must retain the same secret and ignore source port"
    );
    assert_eq!(
        responder_clone
            .respond(
                announce_source,
                &announce_request(sender_id, info_hash, token),
            )
            .unwrap(),
        empty_return(local_id)
    );
    assert_eq!(
        responder
            .respond(token_source, &get_peers_request(sender_id, info_hash))
            .unwrap()
            .values,
        Some(vec![CompactAddr {
            ip: announce_source.ip(),
            port: announce_source.port(),
        }]),
        "both responders must observe the shared announce mutation"
    );
}

#[test]
fn cloned_ktable_concurrent_get_sample_and_announce_finish_with_consistent_state() {
    const ANNOUNCE_WORKERS: u8 = 2;
    const ANNOUNCES_PER_WORKER: u8 = 12;
    const READ_ITERATIONS: usize = 300;

    let local_id = id(250);
    let sender_id = id(1);
    let seed_hash = id(100);
    let seed_peer: SocketAddr = "203.0.113.1:6881".parse().unwrap();
    let node = RoutingNode {
        id: id(10),
        addr: "192.0.2.10:49001".parse().unwrap(),
    };
    let table = KTable::new(local_id);
    assert_eq!(table.put_node(node), RoutingPutResult::Accepted);
    assert_eq!(
        table.put_hash(seed_hash, &[KTableHashPeer { addr: seed_peer }]),
        RoutingPutResult::Accepted,
    );
    let responder = DhtResponder::with_token_secret(table.clone(), [0x33; 20], -19);
    let (done_tx, done_rx) = mpsc::channel();
    let start = Arc::new(Barrier::new(7));
    let mut workers = Vec::new();

    for _ in 0..2 {
        let responder = responder.clone();
        let done_tx = done_tx.clone();
        let start = Arc::clone(&start);
        workers.push(thread::spawn(move || {
            start.wait();
            for _ in 0..READ_ITERATIONS {
                let result = responder
                    .respond(
                        "198.51.100.240:3000".parse().unwrap(),
                        &get_peers_request(sender_id, seed_hash),
                    )
                    .expect("concurrent peer lookup");
                assert_eq!(result.id, local_id);
                assert_eq!(
                    result.values,
                    Some(vec![CompactAddr {
                        ip: seed_peer.ip(),
                        port: seed_peer.port(),
                    }])
                );
                assert!(result.token.is_some());
                assert!(result.nodes.is_none());
            }
            done_tx.send(()).unwrap();
        }));
    }

    for _ in 0..2 {
        let responder = responder.clone();
        let done_tx = done_tx.clone();
        let start = Arc::clone(&start);
        workers.push(thread::spawn(move || {
            start.wait();
            for _ in 0..READ_ITERATIONS {
                let result = responder
                    .respond(
                        "198.51.100.241:3001".parse().unwrap(),
                        &request(b"sample_infohashes", Some(args(sender_id))),
                    )
                    .expect("concurrent sample");
                assert_eq!(result.id, local_id);
                assert_eq!(result.interval, Some(-19));
                assert!(result
                    .samples
                    .as_ref()
                    .is_some_and(|samples| samples.len() <= 20));
                assert!(result.num.is_some_and(|num| (1..=25).contains(&num)));
                assert!(result.nodes.as_ref().is_some_and(|nodes| nodes.len() <= 39));
            }
            done_tx.send(()).unwrap();
        }));
    }

    for worker_index in 0..ANNOUNCE_WORKERS {
        let responder = responder.clone();
        let done_tx = done_tx.clone();
        let start = Arc::clone(&start);
        workers.push(thread::spawn(move || {
            start.wait();
            for offset in 0..ANNOUNCES_PER_WORKER {
                let sequence = worker_index * ANNOUNCES_PER_WORKER + offset;
                let info_hash = id(101 + sequence);
                let source_ip = Ipv4Addr::new(198, 51, 100, 1 + sequence);
                let token_source = SocketAddr::V4(SocketAddrV4::new(source_ip, 4000));
                let announce_source =
                    SocketAddr::V4(SocketAddrV4::new(source_ip, 5000 + u16::from(sequence)));
                let token = responder
                    .respond(token_source, &get_peers_request(sender_id, info_hash))
                    .expect("token lookup")
                    .token
                    .expect("announce token");
                let result = responder
                    .respond(
                        announce_source,
                        &announce_request(sender_id, info_hash, token),
                    )
                    .expect("concurrent announce");
                assert_eq!(result, empty_return(local_id));
            }
            done_tx.send(()).unwrap();
        }));
    }
    start.wait();
    drop(done_tx);

    for _ in 0..workers.len() {
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("responder worker deadlocked or panicked");
    }
    for worker in workers {
        worker.join().expect("responder worker panicked");
    }

    assert_eq!(table.node_count(), 1);
    assert_eq!(
        table.hash_count(),
        1 + usize::from(ANNOUNCE_WORKERS * ANNOUNCES_PER_WORKER)
    );
    assert_eq!(
        table.hash(seed_hash).unwrap().peers,
        vec![KTableHashPeer { addr: seed_peer }]
    );
    for sequence in 0..ANNOUNCE_WORKERS * ANNOUNCES_PER_WORKER {
        let source_ip = Ipv4Addr::new(198, 51, 100, 1 + sequence);
        let expected = SocketAddr::V4(SocketAddrV4::new(source_ip, 5000 + u16::from(sequence)));
        assert_eq!(
            table.hash(id(101 + sequence)).unwrap().peers,
            vec![KTableHashPeer { addr: expected }]
        );
    }
    assert_eq!(
        table.reverse_address_count(),
        1 + usize::from(ANNOUNCE_WORKERS * ANNOUNCES_PER_WORKER)
    );
    assert_eq!(table.sample_hashes_and_nodes().hashes.len(), 20);
    assert_eq!(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)), seed_peer.ip());
}
