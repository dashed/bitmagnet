//! Differential proof for pure ping/find-node dispatch and envelope composition.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::Path;

use bitmagnet_dht::{
    ByteString, Id20, KrpcError, KrpcMessage, MessageArgs, MessageReturn, NodeTable,
    PingFindNodeDispatchOutcome, PingFindNodeDispatcher, PingFindNodeError, RoutingNode,
    RoutingPutResult,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct DispatchFixture {
    id: String,
    subsystem: String,
    input: DispatchInput,
    expected: DispatchExpected,
}

#[derive(Deserialize)]
struct DispatchInput {
    source: FixtureAddr,
    request: DispatchRequest,
    script: DispatchScript,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DispatchRequest {
    tid_hex: String,
    type_hex: String,
    method_hex: String,
    args_present: bool,
    #[serde(default)]
    mixed_fields: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DispatchScript {
    kind: String,
    response_id: Option<Id20>,
    node: Option<FixtureNode>,
    #[serde(default)]
    error_code: i64,
    error_string: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DispatchExpected {
    destination: FixtureAddr,
    wire_hex: Option<String>,
    #[serde(default)]
    go_panicked: bool,
    generic202_wire_hex: Option<String>,
}

#[derive(Clone, Copy, Deserialize)]
struct FixtureNode {
    id: Id20,
    addr: FixtureAddr,
}

#[derive(Clone, Copy, Deserialize)]
struct FixtureAddr {
    ip: IpAddr,
    port: u16,
    #[serde(default)]
    scope: u32,
}

#[derive(Deserialize)]
struct ResponderFixture {
    id: String,
    input: ResponderInput,
    expected: ResponderExpected,
}

#[derive(Deserialize)]
struct ResponderInput {
    origin: Id20,
    nodes: Option<Vec<FixtureNode>>,
    request: ResponderRequest,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResponderRequest {
    method: String,
    args_present: bool,
    sender_id: Option<Id20>,
    #[serde(default)]
    target_present: bool,
    target: Option<Id20>,
    #[serde(default)]
    want: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResponderExpected {
    rust_outcome: String,
    native_ipv6_node: Option<FixtureNode>,
    wire_hex: Option<String>,
}

fn json_lines<T: for<'de> Deserialize<'de>>(name: &str) -> Vec<T> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht")
        .join(name);
    BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect()
}

fn socket_addr(value: FixtureAddr) -> SocketAddr {
    match value.ip {
        IpAddr::V4(ip) => {
            assert_eq!(value.scope, 0);
            SocketAddr::V4(SocketAddrV4::new(ip, value.port))
        }
        IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(ip, value.port, 0, value.scope)),
    }
}

fn routing_node(value: FixtureNode) -> RoutingNode {
    RoutingNode {
        id: value.id,
        addr: socket_addr(value.addr),
    }
}

fn empty_args(target: Option<Id20>) -> MessageArgs {
    MessageArgs {
        id: Id20::ZERO,
        info_hash: None,
        target,
        token: ByteString::default(),
        port: None,
        implied_port: false,
        want: None,
        no_seed: 0,
        scrape: 0,
    }
}

fn empty_return(id: Id20) -> MessageReturn {
    MessageReturn {
        id,
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

fn dispatch_request(input: &DispatchInput) -> KrpcMessage {
    let target = input.script.node.map(|node| node.id);
    let mut message = KrpcMessage {
        transaction_id: ByteString::new(hex::decode(&input.request.tid_hex).unwrap()),
        message_type: ByteString::new(hex::decode(&input.request.type_hex).unwrap()),
        query: ByteString::new(hex::decode(&input.request.method_hex).unwrap()),
        args: input.request.args_present.then(|| empty_args(target)),
        response: None,
        error: None,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    };
    if input.request.mixed_fields {
        message.response = Some(empty_return(Id20::ZERO));
        message.error = Some(KrpcError {
            code: 999,
            message: ByteString::new(b"request-only".to_vec()),
        });
        message.read_only = true;
        message.client_id = ByteString::new(b"client".to_vec());
    }
    message
}

fn responder_request(input: &ResponderRequest) -> KrpcMessage {
    let args = input.args_present.then(|| MessageArgs {
        id: input.sender_id.unwrap_or(Id20::ZERO),
        target: input
            .target_present
            .then_some(input.target.unwrap_or(Id20::ZERO)),
        want: (!input.want.is_empty()).then(|| {
            input
                .want
                .iter()
                .map(|want| ByteString::new(want.as_bytes().to_vec()))
                .collect()
        }),
        ..empty_args(None)
    });
    KrpcMessage {
        transaction_id: ByteString::new([1, 2]),
        message_type: ByteString::new(b"q".to_vec()),
        query: ByteString::new(input.method.as_bytes().to_vec()),
        args,
        response: None,
        error: None,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    }
}

fn assert_clean_reply(message: &KrpcMessage, request: &KrpcMessage) {
    assert_eq!(message.transaction_id, request.transaction_id);
    assert_eq!(message.message_type.as_bytes(), b"r");
    assert!(message.query.is_empty());
    assert!(message.args.is_none());
    assert!(message.observed_addr.is_none());
    assert!(!message.read_only);
    assert!(message.client_id.is_empty());
    assert_ne!(message.response.is_some(), message.error.is_some());
}

#[test]
fn real_go_handle_query_envelopes_match_the_partial_dispatcher() {
    let fixtures = json_lines::<DispatchFixture>("ping_find_node_dispatch.jsonl");
    assert_eq!(fixtures.len(), 10);
    let generic_reference = fixtures
        .iter()
        .find(|fixture| fixture.id == "generic_error_reference_tid")
        .unwrap()
        .expected
        .wire_hex
        .as_deref()
        .unwrap()
        .to_owned();

    for fixture in &fixtures {
        assert_eq!(fixture.subsystem, "dht_ping_find_node_dispatch");
        assert_eq!(fixture.input.source.ip, fixture.expected.destination.ip);
        assert_eq!(fixture.input.source.port, fixture.expected.destination.port);
        assert_eq!(
            fixture.input.source.scope,
            fixture.expected.destination.scope
        );

        let oracle_wire = fixture
            .expected
            .wire_hex
            .as_ref()
            .or(fixture.expected.generic202_wire_hex.as_ref())
            .unwrap();
        let decoded = KrpcMessage::decode(&hex::decode(oracle_wire).unwrap()).unwrap();
        assert_eq!(decoded.message_type.as_bytes(), b"r");
        assert!(decoded.query.is_empty());
        assert!(decoded.args.is_none());
        assert!(decoded.observed_addr.is_none());
        assert!(!decoded.read_only);
        assert!(decoded.client_id.is_empty());

        if fixture.input.script.kind == "wrapped" {
            let error = decoded.error.as_ref().unwrap();
            assert_eq!(error.code, fixture.input.script.error_code);
            assert_eq!(
                error.message.as_bytes(),
                fixture
                    .input
                    .script
                    .error_string
                    .as_ref()
                    .unwrap()
                    .as_bytes()
            );
        }
        if matches!(
            fixture.input.script.kind.as_str(),
            "generic" | "wrappedPointer"
        ) {
            assert_eq!(decoded.error.as_ref().unwrap().code, 202);
            assert_eq!(
                decoded.error.as_ref().unwrap().message.as_bytes(),
                b"server error"
            );
        }

        let direct_case = matches!(
            fixture.id.as_str(),
            "success_ping_two_byte_tid"
                | "protocol_error_empty_tid"
                | "success_node_three_byte_tid_mapped_source"
                | "mixed_request_fields_are_cleared"
                | "scoped_source_is_exact"
                | "native_ipv6_response_panics"
        );
        if !direct_case {
            continue;
        }

        let origin = fixture.input.script.response_id.unwrap_or(Id20::ZERO);
        let mut table = NodeTable::new(origin);
        if let Some(node) = fixture.input.script.node {
            assert_eq!(table.put(routing_node(node)), RoutingPutResult::Accepted);
        }
        let before = fixture.input.script.node.map(|node| table.closest(node.id));
        let source = socket_addr(fixture.input.source);
        let request = dispatch_request(&fixture.input);
        let outcome = PingFindNodeDispatcher::new(&table)
            .dispatch(source, &request)
            .unwrap();
        let reply = match outcome {
            PingFindNodeDispatchOutcome::Reply(reply) => {
                assert!(!fixture.expected.go_panicked);
                reply
            }
            PingFindNodeDispatchOutcome::LocalFailure { reply, cause } => {
                assert!(fixture.expected.go_panicked);
                assert_eq!(
                    cause,
                    PingFindNodeError::NativeIpv6Node(routing_node(
                        fixture.input.script.node.unwrap()
                    ))
                );
                assert_eq!(reply.message.error.as_ref().unwrap().code, 202);
                assert!(reply.message.response.is_none());
                reply
            }
        };
        assert_eq!(reply.destination, source);
        assert_clean_reply(&reply.message, &request);
        let expected_wire = if fixture.expected.go_panicked {
            fixture.expected.generic202_wire_hex.as_deref().unwrap()
        } else {
            fixture.expected.wire_hex.as_deref().unwrap()
        };
        assert_eq!(
            hex::encode(reply.wire().unwrap()),
            expected_wire,
            "{}",
            fixture.id
        );
        assert_eq!(
            fixture.input.script.node.map(|node| table.closest(node.id)),
            before,
            "{} mutated table",
            fixture.id,
        );
    }
    assert_eq!(
        generic_reference,
        "64313a656c693230326531323a736572766572206572726f7265313a74323a0102313a79313a7265"
    );
}

#[test]
fn prior_real_responder_matrix_composes_and_retains_all_native_causes() {
    let fixtures = json_lines::<ResponderFixture>("ping_find_node.jsonl");
    assert_eq!(fixtures.len(), 14);
    let generic_reference = json_lines::<DispatchFixture>("ping_find_node_dispatch.jsonl")
        .into_iter()
        .find(|fixture| fixture.id == "generic_error_reference_tid")
        .unwrap()
        .expected
        .wire_hex
        .unwrap();
    let source: SocketAddr = "[fe80::123%9]:456".parse().unwrap();
    let mut native_failures = 0;

    for fixture in fixtures {
        let mut table = NodeTable::new(fixture.input.origin);
        for node in fixture.input.nodes.as_deref().unwrap_or_default() {
            assert_eq!(table.put(routing_node(*node)), RoutingPutResult::Accepted);
        }
        let before = fixture
            .input
            .nodes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|node| (node.id, table.closest(node.id)))
            .collect::<Vec<_>>();
        let request = responder_request(&fixture.input.request);
        let outcome = PingFindNodeDispatcher::new(&table)
            .dispatch(source, &request)
            .unwrap();
        match fixture.expected.rust_outcome.as_str() {
            "ok" | "protocol" => {
                let PingFindNodeDispatchOutcome::Reply(reply) = outcome else {
                    panic!("{}: expected normal reply", fixture.id)
                };
                assert_eq!(reply.destination, source);
                assert_eq!(
                    hex::encode(reply.wire().unwrap()),
                    fixture.expected.wire_hex.as_deref().unwrap(),
                    "{}",
                    fixture.id,
                );
            }
            "nativeIpv6Node" => {
                let PingFindNodeDispatchOutcome::LocalFailure { reply, cause } = outcome else {
                    panic!("{}: expected local failure", fixture.id)
                };
                assert_eq!(
                    cause,
                    PingFindNodeError::NativeIpv6Node(routing_node(
                        fixture.expected.native_ipv6_node.unwrap()
                    ))
                );
                assert!(reply.message.response.is_none());
                assert_eq!(reply.message.error.as_ref().unwrap().code, 202);
                assert_eq!(hex::encode(reply.wire().unwrap()), generic_reference);
                native_failures += 1;
            }
            other => panic!("{}: unexpected outcome {other}", fixture.id),
        }
        for (id, expected) in before {
            assert_eq!(
                table.closest(id),
                expected,
                "{} mutated node {id}",
                fixture.id
            );
        }
    }
    assert_eq!(native_failures, 3);
}

#[test]
fn unowned_methods_return_none_before_arguments_or_envelope_type() {
    let table = NodeTable::new(Id20::ZERO);
    let dispatcher = PingFindNodeDispatcher::new(&table);
    for method in [b"".as_slice(), b"PING", b"get_peers", &[0, 255]] {
        let request = KrpcMessage {
            transaction_id: ByteString::new(b"raw".to_vec()),
            message_type: ByteString::new(b"e".to_vec()),
            query: ByteString::new(method.to_vec()),
            args: None,
            response: None,
            error: None,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        };
        assert_eq!(
            dispatcher.dispatch("127.0.0.1:1".parse().unwrap(), &request),
            None
        );
    }
}
