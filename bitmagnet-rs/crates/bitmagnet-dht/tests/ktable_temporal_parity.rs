//! Real-Go differential replay for live temporal KTable node semantics.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bitmagnet_dht::{
    Id20, KTable, KTableBep51Support, KTableClock, KTableCommand, KTableNodeHandle,
    KTableNodeOption, KTableSampleHashesAndNodes, RoutingNode, RoutingPutResult,
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
#[serde(rename_all = "camelCase")]
struct Operation {
    kind: String,
    id: Option<Id20>,
    addr: Option<SocketAddr>,
    #[serde(default)]
    options: Vec<NodeOption>,
    capture: Option<String>,
    handle: Option<String>,
    cutoff_handle: Option<String>,
    #[serde(default)]
    cutoff_delay_seconds: i64,
    #[serde(default)]
    limit: usize,
    #[serde(default)]
    commands: Vec<Operation>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodeOption {
    kind: String,
    supported: Option<bool>,
    #[serde(default)]
    discovered_num: i64,
    #[serde(default)]
    total_num: i64,
    #[serde(default)]
    next_delay_seconds: i64,
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
    node_present: Option<bool>,
    query_ids: Option<Vec<Id20>>,
    handle: Option<ExpectedHandle>,
    node_count: usize,
    hash_count: usize,
    sample: Option<ExpectedSample>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedHandle {
    id: Id20,
    addr: SocketAddr,
    last_responded: bool,
    dropped: bool,
    bep51_support: String,
    sampled_num: i64,
    last_discovered_num: i64,
    total_num: i64,
    next_sample_state: String,
    candidate: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedSample {
    hash_count: usize,
    node_count: usize,
    total_hashes: usize,
    hash_ids_unique: bool,
    node_ids_unique: bool,
    hashes_are_subset: bool,
    nodes_are_subset: bool,
}

struct FixedClock(Instant);

impl KTableClock for FixedClock {
    fn now(&self) -> Instant {
        self.0
    }
}

fn fixtures() -> Vec<Fixture> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht/ktable_temporal.jsonl");
    BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect()
}

fn shifted(now: Instant, seconds: i64) -> Instant {
    if seconds < 0 {
        now.checked_sub(Duration::from_secs(seconds.unsigned_abs()))
            .expect("fixture timestamp must fit before the replay anchor")
    } else {
        now.checked_add(Duration::from_secs(seconds.unsigned_abs()))
            .expect("fixture timestamp must fit after the replay anchor")
    }
}

fn temporal_option(value: &NodeOption, now: Instant) -> KTableNodeOption {
    match value.kind.as_str() {
        "responded" => KTableNodeOption::Responded,
        "support" => KTableNodeOption::Bep51Support(value.supported.expect("support value")),
        "sample" => KTableNodeOption::SampleInfoHashesResponse {
            discovered_num: value.discovered_num,
            total_num: value.total_num,
            next_sample_at: shifted(now, value.next_delay_seconds),
        },
        other => panic!("unknown temporal node option {other}"),
    }
}

fn command(value: &Operation, now: Instant) -> KTableCommand {
    match value.kind.as_str() {
        "putNode" => KTableCommand::PutNode {
            node: RoutingNode {
                id: value.id.expect("command node ID"),
                addr: value.addr.expect("command node address"),
            },
            options: value
                .options
                .iter()
                .map(|option| temporal_option(option, now))
                .collect(),
        },
        "dropNode" => KTableCommand::DropNode {
            id: value.id.expect("command node ID"),
        },
        "dropAddr" => KTableCommand::DropAddr {
            addr: value.addr.expect("command drop address"),
        },
        "putHash" => KTableCommand::PutHash {
            id: value.id.expect("command hash ID"),
            peers: vec![],
        },
        other => panic!("unknown KTable command {other}"),
    }
}

fn put_label(result: RoutingPutResult) -> &'static str {
    match result {
        RoutingPutResult::Rejected => "rejected",
        RoutingPutResult::Accepted => "accepted",
        RoutingPutResult::AlreadyExists => "already exists",
    }
}

fn support_label(value: KTableBep51Support) -> &'static str {
    match value {
        KTableBep51Support::Unknown => "unknown",
        KTableBep51Support::Yes => "yes",
        KTableBep51Support::No => "no",
    }
}

fn next_sample_label(handle: &KTableNodeHandle, now: Instant) -> &'static str {
    match handle.next_sample_infohashes_at() {
        None => "zero",
        Some(next) if next < now => "past",
        Some(_) => "future",
    }
}

fn assert_handle(
    handle: &KTableNodeHandle,
    expected: &ExpectedHandle,
    now: Instant,
    context: &str,
) {
    assert_eq!(handle.id(), expected.id, "{context}: handle ID");
    assert_eq!(handle.addr(), expected.addr, "{context}: handle address");
    assert_eq!(
        handle.last_responded_at().is_some(),
        expected.last_responded,
        "{context}: last responded"
    );
    assert_eq!(handle.dropped(), expected.dropped, "{context}: dropped");
    assert_eq!(
        support_label(handle.bep51_support()),
        expected.bep51_support,
        "{context}: BEP-51 support"
    );
    assert_eq!(
        handle.sampled_num(),
        expected.sampled_num,
        "{context}: sampled count"
    );
    assert_eq!(
        handle.last_discovered_num(),
        expected.last_discovered_num,
        "{context}: last discovered count"
    );
    assert_eq!(
        handle.total_num(),
        expected.total_num,
        "{context}: total count"
    );
    assert_eq!(
        next_sample_label(handle, now),
        expected.next_sample_state,
        "{context}: next sample state"
    );
    assert_eq!(
        handle.is_sample_infohashes_candidate(),
        expected.candidate,
        "{context}: candidate"
    );
}

fn assert_sample(
    table: &KTable,
    actual: &KTableSampleHashesAndNodes,
    expected: &ExpectedSample,
    context: &str,
) {
    let hash_ids = actual
        .hashes
        .iter()
        .map(|hash| hash.id)
        .collect::<HashSet<_>>();
    let node_ids = actual
        .nodes
        .iter()
        .map(KTableNodeHandle::id)
        .collect::<HashSet<_>>();
    let hashes_are_subset = actual
        .hashes
        .iter()
        .all(|hash| table.hash(hash.id).as_ref() == Some(hash));
    let nodes_are_subset = actual.nodes.iter().all(|handle| {
        table
            .node_handle(handle.id())
            .is_some_and(|current| current == *handle)
    });
    assert_eq!(actual.hashes.len(), expected.hash_count, "{context}");
    assert_eq!(actual.nodes.len(), expected.node_count, "{context}");
    assert_eq!(actual.total_hashes, expected.total_hashes, "{context}");
    assert_eq!(
        hash_ids.len() == actual.hashes.len(),
        expected.hash_ids_unique,
        "{context}"
    );
    assert_eq!(
        node_ids.len() == actual.nodes.len(),
        expected.node_ids_unique,
        "{context}"
    );
    assert_eq!(hashes_are_subset, expected.hashes_are_subset, "{context}");
    assert_eq!(nodes_are_subset, expected.nodes_are_subset, "{context}");
    assert!(actual.hashes.windows(2).all(|pair| pair[0].id < pair[1].id));
    assert!(actual
        .nodes
        .windows(2)
        .all(|pair| pair[0].id() < pair[1].id()));
}

#[test]
fn real_go_live_handles_options_and_temporal_queries_match_rust() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 2);
    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_ktable_temporal");
        assert_eq!(
            fixture.input.operations.len(),
            fixture.expected.results.len()
        );
        let now = Instant::now();
        let table = KTable::with_clock(fixture.input.origin, Arc::new(FixedClock(now)));
        let mut handles = HashMap::<String, KTableNodeHandle>::new();
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
                    let id = operation.id.expect("node ID");
                    let options = operation
                        .options
                        .iter()
                        .map(|option| temporal_option(option, now))
                        .collect::<Vec<_>>();
                    let actual = table.put_node_with_options(
                        RoutingNode {
                            id,
                            addr: operation.addr.expect("node address"),
                        },
                        &options,
                    );
                    assert_eq!(
                        Some(put_label(actual)),
                        expected.put_result.as_deref(),
                        "{context}"
                    );
                    if let Some(label) = &operation.capture {
                        if let Some(handle) = table.node_handle(id) {
                            handles.insert(label.clone(), handle);
                        }
                    }
                }
                "dropNode" => assert_eq!(
                    Some(table.drop_node(operation.id.expect("node ID"))),
                    expected.bool_result,
                    "{context}"
                ),
                "dropAddr" => assert_eq!(
                    Some(table.drop_addr(operation.addr.expect("drop address"))),
                    expected.bool_result,
                    "{context}"
                ),
                "putHash" => {
                    let actual = table.put_hash(operation.id.expect("hash ID"), &[]);
                    assert_eq!(
                        Some(put_label(actual)),
                        expected.put_result.as_deref(),
                        "{context}"
                    );
                }
                "batch" => {
                    let commands = operation
                        .commands
                        .iter()
                        .map(|value| command(value, now))
                        .collect::<Vec<_>>();
                    table.batch_command(&commands);
                }
                "observe" => {
                    let handle = handles
                        .get(operation.handle.as_deref().expect("handle label"))
                        .expect("captured handle");
                    assert_handle(
                        handle,
                        expected.handle.as_ref().expect("handle observation"),
                        now,
                        &context,
                    );
                }
                "nodePresent" => assert_eq!(
                    Some(table.node_handle(operation.id.expect("node ID")).is_some(),),
                    expected.node_present,
                    "{context}"
                ),
                "oldest" => {
                    let cutoff = if let Some(label) = &operation.cutoff_handle {
                        handles
                            .get(label)
                            .expect("cutoff handle")
                            .last_responded_at()
                            .expect("cutoff handle response time")
                    } else {
                        shifted(now, operation.cutoff_delay_seconds)
                    };
                    let mut actual = table
                        .get_oldest_nodes(cutoff, NonZeroUsize::new(operation.limit))
                        .into_iter()
                        .map(|handle| handle.id())
                        .collect::<Vec<_>>();
                    actual.sort_unstable();
                    assert_eq!(Some(actual), expected.query_ids, "{context}");
                }
                "candidates" => {
                    let mut actual = table
                        .get_nodes_for_sample_infohashes(
                            NonZeroUsize::new(operation.limit).expect("positive candidate limit"),
                        )
                        .into_iter()
                        .map(|handle| handle.id())
                        .collect::<Vec<_>>();
                    actual.sort_unstable();
                    assert_eq!(Some(actual), expected.query_ids, "{context}");
                }
                "sample" => assert_sample(
                    &table,
                    &table.sample_hashes_and_nodes(),
                    expected.sample.as_ref().expect("sample result"),
                    &context,
                ),
                other => panic!("{context}: unknown operation {other}"),
            }
            assert_eq!(table.node_count(), expected.node_count, "{context}");
            assert_eq!(table.hash_count(), expected.hash_count, "{context}");
        }
    }
}
