//! Differential replay of the production Go Kademlia binary tree.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use bitmagnet_dht::{Id20, RoutingPutResult, RoutingTree, ROUTING_ID_BITS};
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
    k: usize,
    splitting_enabled: bool,
    operations: Vec<Operation>,
}

#[derive(Deserialize)]
struct Operation {
    kind: String,
    id: Option<Id20>,
    #[serde(default)]
    limit: usize,
}

#[derive(Deserialize)]
struct Expected {
    bits: usize,
    results: Vec<ExpectedResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedResult {
    put_result: Option<String>,
    bool_result: Option<bool>,
    closest: Option<Vec<Id20>>,
    count: usize,
    members: Vec<Id20>,
    target_present: Option<bool>,
}

fn fixtures() -> Vec<Fixture> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht/routing_tree.jsonl");
    BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect()
}

#[test]
fn real_go_routing_tree_trace_matches_rust_exactly() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 13);
    let mut closest_by_scenario = BTreeMap::new();

    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_routing_tree");
        assert_eq!(fixture.expected.bits, ROUTING_ID_BITS);
        assert_eq!(
            fixture.input.operations.len(),
            fixture.expected.results.len()
        );

        let mut tree = RoutingTree::new(
            fixture.input.origin,
            fixture.input.k,
            fixture.input.splitting_enabled,
        );
        assert_eq!(tree.bits(), ROUTING_ID_BITS);
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
                "put" => {
                    let id = operation.id.expect("put ID");
                    let actual = match tree.put(id) {
                        RoutingPutResult::Rejected => "rejected",
                        RoutingPutResult::Accepted => "accepted",
                        RoutingPutResult::AlreadyExists => "already exists",
                    };
                    assert_eq!(Some(actual), expected.put_result.as_deref(), "{context}");
                }
                "drop" => assert_eq!(
                    Some(tree.drop(operation.id.expect("drop ID"))),
                    expected.bool_result,
                    "{context}"
                ),
                "has" => assert_eq!(
                    Some(tree.contains(operation.id.expect("has ID"))),
                    expected.bool_result,
                    "{context}"
                ),
                "closest" => {
                    let actual = tree.closest(operation.id.expect("closest ID"), operation.limit);
                    assert_eq!(Some(&actual), expected.closest.as_ref(), "{context}");
                    closest_results.push(actual);
                }
                "count" => {}
                other => panic!("{context}: unknown operation {other}"),
            }

            assert_eq!(tree.count(), expected.count, "{context}: count");
            assert_eq!(
                tree.closest(fixture.input.origin, tree.count() + 1),
                expected.members,
                "{context}: exact membership snapshot"
            );
            if let Some(target_present) = expected.target_present {
                assert_eq!(
                    tree.contains(operation.id.expect("membership target")),
                    target_present,
                    "{context}: target membership"
                );
            }
        }
        closest_by_scenario.insert(fixture.id, closest_results);
    }

    assert_eq!(
        closest_by_scenario["closest_forward_insertion"],
        closest_by_scenario["closest_reverse_insertion"],
        "closest traversal must not depend on insertion order"
    );
}

#[test]
fn public_tree_is_fixed_to_exact_160_bit_ids_and_zero_capacity_rejects() {
    let mut tree = RoutingTree::new(Id20::ZERO, 0, false);
    let candidate = Id20::from_hex("0000000000000000000000000000000000000001").unwrap();
    assert_eq!(tree.bits(), 160);
    assert_eq!(tree.put(candidate), RoutingPutResult::Rejected);
    assert_eq!(tree.count(), 0);
    assert!(!tree.contains(candidate));
    assert!(tree.closest(Id20::ZERO, 0).is_empty());
    assert!(tree.closest(Id20::ZERO, usize::MAX).is_empty());
}
