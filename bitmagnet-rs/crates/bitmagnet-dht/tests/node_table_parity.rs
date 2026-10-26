//! Differential replay of Go's deterministic current-state node keyspace.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::Path;

use bitmagnet_dht::{
    Id20, NodeTable, RoutingNode, RoutingPutResult, NODE_TABLE_CAPACITY, NODE_TABLE_CLOSEST_LIMIT,
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
    operations: Vec<Operation>,
}

#[derive(Deserialize)]
struct Operation {
    kind: String,
    id: Option<Id20>,
    addr: Option<FixtureAddr>,
}

#[derive(Clone, Copy, Deserialize)]
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
    drop_result: Option<bool>,
    closest: Option<Vec<FixtureNode>>,
    origin: Option<Id20>,
    #[serde(default)]
    rust_unsupported: bool,
    count: usize,
    state: Vec<FixtureNode>,
}

#[derive(Clone, Copy, Deserialize)]
struct FixtureNode {
    id: Id20,
    addr: FixtureAddr,
}

fn fixtures() -> Vec<Fixture> {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../testdata/parity/dht/node_table.jsonl");
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

#[test]
fn real_go_node_table_trace_matches_rust_exactly() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 6);
    let mut closest_by_scenario = BTreeMap::new();

    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_node_table");
        assert_eq!(
            fixture.input.operations.len(),
            fixture.expected.results.len()
        );
        let mut table = NodeTable::new(fixture.input.origin);
        let mut closest_results = Vec::new();

        for (index, (operation, expected)) in fixture
            .input
            .operations
            .iter()
            .zip(&fixture.expected.results)
            .enumerate()
        {
            let context = format!("{} operation {index}", fixture.id);
            match operation.kind.as_str() {
                "origin" => assert_eq!(Some(table.origin()), expected.origin, "{context}"),
                "put" => {
                    assert!(!expected.rust_unsupported, "{context}");
                    let result = table.put(RoutingNode {
                        id: operation.id.expect("put ID"),
                        addr: socket_addr(operation.addr.expect("put address")),
                    });
                    let label = match result {
                        RoutingPutResult::Rejected => "rejected",
                        RoutingPutResult::Accepted => "accepted",
                        RoutingPutResult::AlreadyExists => "already exists",
                    };
                    assert_eq!(Some(label), expected.put_result.as_deref(), "{context}");
                }
                "putInvalid" => {
                    assert!(expected.rust_unsupported, "{context}");
                    assert_eq!(
                        expected.put_result.as_deref(),
                        Some("rejected"),
                        "{context}"
                    );
                    assert!(operation.addr.is_none(), "{context}");
                }
                "drop" => assert_eq!(
                    Some(table.drop(operation.id.expect("drop ID"))),
                    expected.drop_result,
                    "{context}"
                ),
                "closest" => {
                    let actual = table.closest(operation.id.expect("closest ID"));
                    let want = expected
                        .closest
                        .as_ref()
                        .expect("closest projection")
                        .iter()
                        .copied()
                        .map(routing_node)
                        .collect::<Vec<_>>();
                    assert_eq!(actual, want, "{context}");
                    closest_results.push(actual);
                }
                other => panic!("{context}: unknown operation {other}"),
            }

            assert_eq!(table.count(), expected.count, "{context}: count");
            let expected_ids = expected
                .state
                .iter()
                .map(|node| node.id)
                .collect::<BTreeSet<_>>();
            assert_eq!(expected_ids.len(), expected.state.len(), "{context}: IDs");
            assert_eq!(expected_ids.len(), table.count(), "{context}: state size");
            for expected_node in expected.state.iter().copied() {
                let node = routing_node(expected_node);
                assert_eq!(table.closest(node.id), vec![node], "{context}: exact hit");
                if let SocketAddr::V6(addr) = node.addr {
                    assert_eq!(addr.flowinfo(), 0, "{context}: IPv6 flowinfo");
                }
            }
        }
        closest_by_scenario.insert(fixture.id, closest_results);
    }

    assert_eq!(
        closest_by_scenario["nonzero_origin_forward_traversal"],
        closest_by_scenario["nonzero_origin_reverse_traversal"],
        "node-table closest traversal must not depend on insertion order"
    );
}

#[test]
fn public_contract_is_fixed_and_invalid_go_addrport_is_unrepresentable() {
    assert_eq!(NODE_TABLE_CAPACITY, 80);
    assert_eq!(NODE_TABLE_CLOSEST_LIMIT, 8);

    let fixture = fixtures()
        .into_iter()
        .find(|fixture| fixture.id == "empty_origin_and_invalid_address")
        .unwrap();
    let invalid = fixture
        .input
        .operations
        .iter()
        .zip(&fixture.expected.results)
        .find(|(operation, _)| operation.kind == "putInvalid")
        .unwrap();
    assert!(invalid.0.addr.is_none());
    assert!(invalid.1.rust_unsupported);
    assert_eq!(invalid.1.put_result.as_deref(), Some("rejected"));
}
