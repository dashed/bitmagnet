//! Real-Go differential replay for the pure shared node/hash/reverse core.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::Path;

use bitmagnet_dht::{
    Id20, KTableCore, KTableHash, KTableHashLookup, KTableHashPeer, KTableReverseInfo, RoutingNode,
    RoutingPutResult, HASH_TABLE_CAPACITY,
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
#[serde(rename_all = "camelCase")]
struct Input {
    origin: Id20,
    address_universe: Vec<FixtureAddr>,
    operations: Vec<Operation>,
}

#[derive(Deserialize)]
struct Operation {
    kind: String,
    id: Option<Id20>,
    addr: Option<FixtureAddr>,
    #[serde(default)]
    peers: Vec<FixtureAddr>,
    #[serde(default)]
    addrs: Vec<FixtureAddr>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
struct FixtureAddr {
    ip: IpAddr,
    port: u16,
    #[serde(default)]
    scope: u32,
}

#[derive(Deserialize)]
struct Expected {
    results: Vec<ExpectedResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedResult {
    put_result: Option<String>,
    bool_result: Option<bool>,
    filtered: Option<Vec<FixtureAddr>>,
    lookup: Option<ExpectedLookup>,
    node_count: usize,
    hash_count: usize,
    reverse_count: usize,
    nodes: Vec<FixtureNode>,
    hashes: Vec<FixtureHash>,
    known_addrs: Vec<FixtureAddr>,
    reverse: Vec<FixtureReverse>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedLookup {
    found: bool,
    hash: Option<FixtureHash>,
    closest_nodes: Option<Vec<FixtureNode>>,
}

#[derive(Clone, Copy, Deserialize)]
struct FixtureNode {
    id: Id20,
    addr: FixtureAddr,
}

#[derive(Clone, Deserialize)]
struct FixtureHash {
    id: Id20,
    peers: Vec<FixtureAddr>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureReverse {
    addr: FixtureAddr,
    peer_id: Option<Id20>,
    hashes: Vec<Id20>,
}

fn fixtures() -> Vec<Fixture> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht/ktable_core.jsonl");
    BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect()
}

fn socket_addr(value: FixtureAddr) -> SocketAddr {
    match value.ip {
        IpAddr::V4(ip) => {
            assert_eq!(value.scope, 0, "IPv4 fixture cannot carry a scope");
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

fn hash_peer(value: FixtureAddr) -> KTableHashPeer {
    KTableHashPeer {
        addr: socket_addr(value),
    }
}

fn hash(value: &FixtureHash) -> KTableHash {
    KTableHash {
        id: value.id,
        peers: value.peers.iter().copied().map(hash_peer).collect(),
    }
}

fn put_label(result: RoutingPutResult) -> &'static str {
    match result {
        RoutingPutResult::Rejected => "rejected",
        RoutingPutResult::Accepted => "accepted",
        RoutingPutResult::AlreadyExists => "already exists",
    }
}

fn identity(value: FixtureAddr) -> (IpAddr, u32) {
    (value.ip, value.scope)
}

#[test]
fn real_go_shared_node_hash_and_reverse_trace_matches_rust() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 4);
    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_ktable_core");
        assert_eq!(
            fixture.input.operations.len(),
            fixture.expected.results.len()
        );
        let mut core = KTableCore::new(fixture.input.origin);
        for (index, (operation, expected)) in fixture
            .input
            .operations
            .iter()
            .zip(&fixture.expected.results)
            .enumerate()
        {
            let context = format!("{} operation {index}", fixture.id);
            match operation.kind.as_str() {
                "putNode" => {
                    let actual = core.put_node(RoutingNode {
                        id: operation.id.expect("node ID"),
                        addr: socket_addr(operation.addr.expect("node address")),
                    });
                    assert_eq!(
                        Some(put_label(actual)),
                        expected.put_result.as_deref(),
                        "{context}"
                    );
                }
                "dropNode" => assert_eq!(
                    Some(core.drop_node(operation.id.expect("node ID"))),
                    expected.bool_result,
                    "{context}"
                ),
                "dropAddr" => assert_eq!(
                    Some(core.drop_addr(socket_addr(operation.addr.expect("drop address")))),
                    expected.bool_result,
                    "{context}"
                ),
                "putHash" => {
                    let peers = operation
                        .peers
                        .iter()
                        .copied()
                        .map(hash_peer)
                        .collect::<Vec<_>>();
                    let actual = core.put_hash(operation.id.expect("hash ID"), &peers);
                    assert_eq!(
                        Some(put_label(actual)),
                        expected.put_result.as_deref(),
                        "{context}"
                    );
                }
                "filter" => {
                    let inputs = operation
                        .addrs
                        .iter()
                        .copied()
                        .map(socket_addr)
                        .collect::<Vec<_>>();
                    let actual = core.filter_known_addrs(&inputs);
                    let projected = actual
                        .iter()
                        .map(|addr| match addr {
                            SocketAddr::V4(addr) => (IpAddr::V4(*addr.ip()), 0),
                            SocketAddr::V6(addr) => (IpAddr::V6(*addr.ip()), addr.scope_id()),
                        })
                        .collect::<Vec<_>>();
                    let wanted = expected
                        .filtered
                        .as_ref()
                        .expect("filter result")
                        .iter()
                        .copied()
                        .map(identity)
                        .collect::<Vec<_>>();
                    assert_eq!(projected, wanted, "{context}");
                    let mut cursor = 0;
                    for retained in actual {
                        cursor += inputs[cursor..]
                            .iter()
                            .position(|input| *input == retained)
                            .expect("filtered address must retain an exact input")
                            + 1;
                    }
                }
                "lookup" => {
                    let expected = expected.lookup.as_ref().expect("lookup result");
                    match core.get_hash_or_closest_nodes(operation.id.expect("lookup ID")) {
                        KTableHashLookup::Found(actual) => {
                            assert!(expected.found, "{context}");
                            assert_eq!(
                                actual,
                                hash(expected.hash.as_ref().expect("found hash")),
                                "{context}"
                            );
                            assert!(
                                expected
                                    .closest_nodes
                                    .as_deref()
                                    .unwrap_or_default()
                                    .is_empty(),
                                "{context}"
                            );
                        }
                        KTableHashLookup::ClosestNodes(actual) => {
                            assert!(!expected.found, "{context}");
                            assert!(expected.hash.is_none(), "{context}");
                            assert_eq!(
                                actual,
                                expected
                                    .closest_nodes
                                    .as_deref()
                                    .unwrap_or_default()
                                    .iter()
                                    .copied()
                                    .map(routing_node)
                                    .collect::<Vec<_>>(),
                                "{context}"
                            );
                        }
                    }
                }
                other => panic!("{context}: unknown operation {other}"),
            }

            assert_eq!(
                core.node_count(),
                expected.node_count,
                "{context}: node count"
            );
            assert_eq!(
                core.hash_count(),
                expected.hash_count,
                "{context}: hash count"
            );
            assert_eq!(
                core.reverse_address_count(),
                expected.reverse_count,
                "{context}: reverse count"
            );
            assert_eq!(expected.nodes.len(), core.node_count(), "{context}: nodes");
            for node in expected.nodes.iter().copied() {
                assert_eq!(core.node(node.id), Some(routing_node(node)), "{context}");
            }
            assert_eq!(
                expected.hashes.len(),
                core.hash_count(),
                "{context}: hashes"
            );
            for expected_hash in &expected.hashes {
                assert_eq!(
                    core.hash(expected_hash.id),
                    Some(hash(expected_hash)),
                    "{context}"
                );
            }

            let known = expected
                .known_addrs
                .iter()
                .copied()
                .map(identity)
                .collect::<Vec<_>>();
            assert_eq!(known.len(), expected.reverse_count, "{context}: known keys");
            assert_eq!(expected.reverse.len(), expected.reverse_count, "{context}");
            for entry in &expected.reverse {
                assert_eq!(
                    core.reverse_info(socket_addr(entry.addr)),
                    Some(KTableReverseInfo {
                        peer_id: entry.peer_id,
                        hashes: entry.hashes.clone(),
                    }),
                    "{context}: reverse entry {:?}",
                    entry.addr
                );
            }
            for address in &fixture.input.address_universe {
                let socket = socket_addr(*address);
                let filtered = core.filter_known_addrs(&[socket]);
                if known.contains(&identity(*address)) {
                    assert!(filtered.is_empty(), "{context}: expected known {address:?}");
                } else {
                    assert_eq!(
                        filtered,
                        vec![socket],
                        "{context}: expected unknown {address:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn public_boundaries_are_fixed_and_filter_retains_ports_flowinfo_order_and_duplicates() {
    assert_eq!(HASH_TABLE_CAPACITY, 80);
    let mut core = KTableCore::new(Id20::ZERO);
    let known = SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 1, 7, 9));
    assert_eq!(
        core.put_hash(
            Id20::from_hex("0000000000000000000000000000000000000001").unwrap(),
            &[KTableHashPeer { addr: known }]
        ),
        RoutingPutResult::Accepted
    );
    let unknown = SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 2, 123, 10));
    let known_alias = SocketAddr::V6(SocketAddrV6::new(
        "fe80::1".parse().unwrap(),
        65535,
        u32::MAX,
        9,
    ));
    assert_eq!(
        core.filter_known_addrs(&[unknown, known_alias, unknown]),
        vec![unknown, unknown]
    );
}
