//! Differential replay of Go's pure ping/find-node responder core.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::Path;

use bitmagnet_dht::{
    ByteString, CompactAddr, CompactCodecError, CompactNode, Id20, KrpcError, KrpcMessage,
    MessageArgs, MessageReturn, NodeTable, PingFindNodeError, PingFindNodeResponder, RoutingNode,
    RoutingPutResult, WireError,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    id: String,
    subsystem: String,
    input: Input,
    expected: Expected,
}

#[derive(Deserialize)]
struct Input {
    origin: Id20,
    nodes: Option<Vec<FixtureNode>>,
    request: Request,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Request {
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
struct Expected {
    rust_outcome: String,
    go_response: FixtureResponse,
    protocol_error: Option<FixtureError>,
    native_ipv6_node: Option<FixtureNode>,
    wire_hex: Option<String>,
    #[serde(default)]
    go_wire_panicked: bool,
}

#[derive(Deserialize)]
struct FixtureResponse {
    id: Id20,
    nodes: Option<Vec<FixtureNode>>,
}

#[derive(Deserialize)]
struct FixtureError {
    code: i64,
    message: String,
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

fn fixtures() -> Vec<Fixture> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht/ping_find_node.jsonl");
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

fn compact_node(value: FixtureNode) -> CompactNode {
    CompactNode {
        id: value.id,
        addr: CompactAddr {
            ip: value.addr.ip,
            port: value.addr.port,
        },
    }
}

fn request(value: &Request) -> KrpcMessage {
    let args = value.args_present.then(|| MessageArgs {
        id: value.sender_id.unwrap_or(Id20::ZERO),
        info_hash: None,
        target: value
            .target_present
            .then_some(value.target.unwrap_or(Id20::ZERO)),
        token: ByteString::default(),
        port: None,
        implied_port: false,
        want: (!value.want.is_empty()).then(|| {
            value
                .want
                .iter()
                .map(|want| ByteString::new(want.as_bytes().to_vec()))
                .collect()
        }),
        no_seed: 0,
        scrape: 0,
    });
    KrpcMessage {
        transaction_id: ByteString::new([1, 2]),
        message_type: ByteString::new(b"q".to_vec()),
        query: ByteString::new(value.method.as_bytes().to_vec()),
        args,
        response: None,
        error: None,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
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

fn projected_response(value: &FixtureResponse) -> MessageReturn {
    MessageReturn {
        nodes: value
            .nodes
            .as_ref()
            .map(|nodes| nodes.iter().copied().map(compact_node).collect()),
        ..empty_return(value.id)
    }
}

fn protocol_error(value: &FixtureError) -> KrpcError {
    KrpcError {
        code: value.code,
        message: ByteString::new(hex::decode(&value.message).unwrap()),
    }
}

fn envelope(response: Option<MessageReturn>, error: Option<KrpcError>) -> KrpcMessage {
    KrpcMessage {
        transaction_id: ByteString::new([1, 2]),
        message_type: ByteString::new(b"r".to_vec()),
        query: ByteString::default(),
        args: None,
        response,
        error,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    }
}

#[test]
fn real_go_ping_find_node_responder_matches_rust() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 14);
    let mut saw_full_eight = false;
    let mut saw_mapped = false;
    let mut native_deltas = 0;

    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_ping_find_node");
        let mut table = NodeTable::new(fixture.input.origin);
        for node in fixture.input.nodes.as_deref().unwrap_or_default() {
            assert_eq!(
                table.put(routing_node(*node)),
                RoutingPutResult::Accepted,
                "{} seed {}",
                fixture.id,
                node.id,
            );
        }
        let before = table.count();
        let result = PingFindNodeResponder::new(&table)
            .respond(&request(&fixture.input.request))
            .expect("fixture contains only owned methods");
        assert_eq!(table.count(), before, "{} mutated table", fixture.id);
        for node in fixture.input.nodes.as_deref().unwrap_or_default() {
            assert_eq!(
                table.closest(node.id),
                vec![routing_node(*node)],
                "{} mutated node {}",
                fixture.id,
                node.id,
            );
        }

        match fixture.expected.rust_outcome.as_str() {
            "ok" => {
                let actual = result.unwrap();
                let expected = projected_response(&fixture.expected.go_response);
                assert_eq!(actual, expected, "{} typed response", fixture.id);
                assert!(!fixture.expected.go_wire_panicked);
                let wire = envelope(Some(actual), None).encode().unwrap();
                assert_eq!(
                    hex::encode(wire),
                    fixture.expected.wire_hex.as_deref().unwrap(),
                    "{} response wire",
                    fixture.id,
                );
                if expected
                    .nodes
                    .as_ref()
                    .is_some_and(|nodes| nodes.len() == 8)
                {
                    saw_full_eight = true;
                }
                if expected.nodes.as_ref().is_some_and(|nodes| {
                    nodes
                        .iter()
                        .any(|node| node.addr.ip == "192.0.2.2".parse::<IpAddr>().unwrap())
                }) {
                    saw_mapped = true;
                }
            }
            "protocol" => {
                let expected = protocol_error(fixture.expected.protocol_error.as_ref().unwrap());
                assert_eq!(result, Err(PingFindNodeError::Protocol(expected.clone())));
                assert!(fixture.expected.go_response.nodes.is_none());
                assert_eq!(
                    fixture.expected.go_response.id,
                    if fixture.input.request.args_present {
                        fixture.input.origin
                    } else {
                        Id20::ZERO
                    },
                    "{} Go partial response ID",
                    fixture.id,
                );
                assert!(!fixture.expected.go_wire_panicked);
                let wire = envelope(None, Some(expected)).encode().unwrap();
                assert_eq!(
                    hex::encode(wire),
                    fixture.expected.wire_hex.as_deref().unwrap(),
                    "{} error wire",
                    fixture.id,
                );
            }
            "nativeIpv6Node" => {
                let offender = routing_node(fixture.expected.native_ipv6_node.unwrap());
                assert_eq!(result, Err(PingFindNodeError::NativeIpv6Node(offender)));
                assert!(fixture.expected.go_wire_panicked);
                assert!(fixture.expected.wire_hex.is_none());

                let go_typed = projected_response(&fixture.expected.go_response);
                let rust_encode =
                    std::panic::catch_unwind(|| envelope(Some(go_typed), None).encode());
                assert!(rust_encode.is_ok(), "{} Rust encoder panicked", fixture.id);
                assert!(matches!(
                    rust_encode.unwrap(),
                    Err(WireError::Compact(CompactCodecError::WrongAddressFamily {
                        expected: "IPv4"
                    }))
                ));
                native_deltas += 1;
            }
            other => panic!("{}: unknown Rust outcome {other}", fixture.id),
        }
    }
    assert!(saw_full_eight);
    assert!(saw_mapped);
    assert_eq!(native_deltas, 3);
}

#[test]
fn method_ownership_is_exact_raw_bytes_and_precedes_arguments() {
    let table = NodeTable::new(Id20::ZERO);
    let responder = PingFindNodeResponder::new(&table);
    for method in [
        b"".as_slice(),
        b"PING",
        b"find-node",
        b"get_peers",
        &[0, 255],
    ] {
        let mut message = request(&Request {
            method: String::new(),
            args_present: false,
            sender_id: None,
            target_present: false,
            target: None,
            want: Vec::new(),
        });
        message.query = ByteString::new(method.to_vec());
        assert_eq!(responder.respond(&message), None);
    }
}
