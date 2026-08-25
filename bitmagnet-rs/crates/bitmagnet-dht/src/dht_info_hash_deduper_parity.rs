use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::thread;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::*;

const FIXTURE_TEXT: &str =
    include_str!("../../../../testdata/parity/dht/dht_crawler_ignore_hashes.jsonl");
const FIXTURE_BYTES: &[u8] =
    include_bytes!("../../../../testdata/parity/dht/dht_crawler_ignore_hashes.jsonl");
const GO_MOD: &str = include_str!("../../../../go.mod");
const GO_SUM: &str = include_str!("../../../../go.sum");
const FIXTURE_SHA256: &str = "7900b4046d10037b9c7541d36d79370a92ceb3135f9c81be0adef985ac1f4621";
const FIXTURE_IDS: [&str; 2] = [
    "production_source_filter_and_probabilistic_scope_contract",
    "fresh_production_filter_adjacent_duplicates",
];
const ROW_CLASSIFICATIONS: [&str; 2] = ["SOURCE_ONLY", "RUNTIME_EXACT"];

const RUST_EXECUTION_PARTITION: [(&str, &str); 2] = [
    (FIXTURE_IDS[0], "SOURCE_ONLY_NO_RUST_RUNTIME_REPLAY"),
    (
        FIXTURE_IDS[1],
        "EXACT_ADJACENT_DUPLICATE_AND_CONTENTION_AGGREGATE_REPLAY",
    ),
];

const DELIBERATE_RUST_DELTAS: [&str; 4] = [
    "one_per_instance_fastrand_stream_replaces_the_Go_process_global_math_rand_stream",
    "fatal_Rust_mutex_poison_replaces_the_Go_mutex_without_poison_state",
    "a_cloneable_fixed_public_capability_replaces_the_internal_Go_crawler_wrapper",
    "Rust_binds_but_does_not_implement_the_BoomFilters_serialization_format",
];

const RUST_NONCLAIMS: [&str; 7] = [
    "exact_Go_random_decrement_offsets_seed_or_sequence_are_not_replayed",
    "exact_set_membership_or_measured_false_positive_false_negative_rates_are_not_claimed",
    "long_run_eviction_age_retention_and_membership_sequences_are_not_claimed",
    "mutex_fairness_throughput_and_cross_thread_completion_order_are_not_claimed",
    "the_packed_payload_is_not_claimed_as_total_heap_or_allocator_footprint",
    "external_nonvendored_BoomFilters_sources_are_fixture_bound_but_not_rehashed_by_Rust",
    "sample_worker_triage_KTable_recursive_fanout_supervisor_app_and_live_behavior_are_not_implemented",
];

const SOURCE_DIGESTS: [(&str, &str); 7] = [
    (
        "github.com/tylertreat/BoomFilters@v0.0.0-20210315201527-1a82519a3e43/boom.go",
        "ce56167cde8bce69243cc48358184cba85b5848edd3b1143b763b3a95edccfe2",
    ),
    (
        "github.com/tylertreat/BoomFilters@v0.0.0-20210315201527-1a82519a3e43/buckets.go",
        "a9903d73dd69456f30230146a41cc3698acb65d63014f5758739881388b5b80a",
    ),
    (
        "github.com/tylertreat/BoomFilters@v0.0.0-20210315201527-1a82519a3e43/stable.go",
        "b2cf136135f9675441b887a552723815d806d58dba24ae2650c3c73469abfa48",
    ),
    (
        "internal/dhtcrawler/crawler.go",
        "ae6ca2484a57231a08351629c21fdc0a875f2272bfd4ad42a4e5386be86500b6",
    ),
    (
        "internal/dhtcrawler/factory.go",
        "ed34129835773817736d70e74c7c884e5b9197e35741dee922ee9a5d691288a6",
    ),
    (
        "internal/dhtcrawler/sample_infohashes.go",
        "483b9037673dce82f9026f2aec9448812f804c13484fd0bd2f55fcfc70a52983",
    ),
    (
        "internal/protocol/id.go",
        "e1947e2b4af4cc008f5bb8cf5000ebfe784a82e119cb0418c2a74c3ed5f8c26f",
    ),
];

const LOCAL_SOURCES: [(&str, &[u8]); 4] = [
    (
        "internal/dhtcrawler/crawler.go",
        include_bytes!("../../../../internal/dhtcrawler/crawler.go"),
    ),
    (
        "internal/dhtcrawler/factory.go",
        include_bytes!("../../../../internal/dhtcrawler/factory.go"),
    ),
    (
        "internal/dhtcrawler/sample_infohashes.go",
        include_bytes!("../../../../internal/dhtcrawler/sample_infohashes.go"),
    ),
    (
        "internal/protocol/id.go",
        include_bytes!("../../../../internal/protocol/id.go"),
    ),
];

const GO_NONCLAIMS: [&str; 16] = [
    "exact_random_decrement_offsets_or_cells",
    "exact_math_rand_seed_or_sequence",
    "exact_set_membership_semantics",
    "measured_or_guaranteed_false_positive_or_false_negative_rates",
    "long_run_false_positive_sequence",
    "long_run_false_negative_sequence",
    "exact_eviction_age_or_retention_window",
    "cross_goroutine_winner_or_completion_order",
    "mutex_lock_fairness",
    "mutex_lock_throughput",
    "packed_cell_payload_as_total_heap_or_allocator_footprint",
    "serialized_filter_contents",
    "process_restart_persistence",
    "Rust_implementation_or_public_API",
    "sample_infohashes_worker_end_to_end_behavior",
    "query_triage_KTable_recursive_fanout_supervisor_or_live_behavior",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    id: String,
    subsystem: String,
    classification: String,
    oracle: Oracle,
    input: Input,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Oracle {
    composition: String,
    determinism: String,
    filter: String,
    randomness: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    kind: String,
    operations: Vec<Operation>,
    contention: Option<ContentionInput>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Operation {
    token: String,
    id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ContentionInput {
    id: String,
    call_count: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    results: Vec<ExpectedResult>,
    contention: Option<ExpectedContention>,
    source: Option<Source>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ExpectedResult {
    token: String,
    already_present: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ExpectedContention {
    false_count: usize,
    true_count: usize,
    sequential_already_present: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Source {
    mutex_covers_test_and_add: bool,
    input_byte_length: usize,
    input_projection: String,
    test_precedes_random_decrement: bool,
    every_call_adds: bool,
    process_local: bool,
    persisted: bool,
    cells: usize,
    bits_per_cell: usize,
    target_false_positive_rate: f64,
    derived_hash_functions: usize,
    derived_decrement_cells: usize,
    derived_max_cell_value: u8,
    derived_index_buffer_length: usize,
    derived_cell_payload_bytes: usize,
    derived_serialized_bytes: usize,
    hash_kernel: String,
    random_decrement_source: String,
    stable_eviction: bool,
    false_positives_possible: bool,
    false_negatives_possible: bool,
    module_path: String,
    module_version: String,
    module_source_sum: String,
    module_go_mod_sum: String,
    dependency_source_pin: String,
    dependency_source_vendored: bool,
    go_mod_requirement: String,
    go_sum_module_line: String,
    go_sum_go_mod_line: String,
    adjacent_duplicate_scope: String,
    source_sha256: BTreeMap<String, String>,
    nonclaims: Vec<String>,
    evidence: String,
}

fn parse_fixtures() -> Vec<Fixture> {
    FIXTURE_TEXT
        .lines()
        .map(|line| serde_json::from_str(line).expect("ignore-hashes fixture row must deserialize"))
        .collect()
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[test]
fn fixture_schema_source_contract_and_partition_are_exact() {
    assert_eq!(digest(FIXTURE_BYTES), FIXTURE_SHA256);
    let fixtures = parse_fixtures();
    assert_eq!(fixtures.len(), FIXTURE_IDS.len());
    assert_eq!(
        fixtures
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        FIXTURE_IDS
    );
    assert_eq!(
        fixtures
            .iter()
            .map(|row| row.classification.as_str())
            .collect::<Vec<_>>(),
        ROW_CLASSIFICATIONS
    );
    assert!(fixtures
        .iter()
        .all(|row| row.subsystem == "dht_crawler_ignore_hashes"));
    assert_eq!(
        RUST_EXECUTION_PARTITION,
        [
            (FIXTURE_IDS[0], "SOURCE_ONLY_NO_RUST_RUNTIME_REPLAY"),
            (
                FIXTURE_IDS[1],
                "EXACT_ADJACENT_DUPLICATE_AND_CONTENTION_AGGREGATE_REPLAY",
            ),
        ]
    );
    assert_eq!(
        DELIBERATE_RUST_DELTAS,
        [
            "one_per_instance_fastrand_stream_replaces_the_Go_process_global_math_rand_stream",
            "fatal_Rust_mutex_poison_replaces_the_Go_mutex_without_poison_state",
            "a_cloneable_fixed_public_capability_replaces_the_internal_Go_crawler_wrapper",
            "Rust_binds_but_does_not_implement_the_BoomFilters_serialization_format",
        ]
    );
    assert_eq!(
        RUST_NONCLAIMS,
        [
            "exact_Go_random_decrement_offsets_seed_or_sequence_are_not_replayed",
            "exact_set_membership_or_measured_false_positive_false_negative_rates_are_not_claimed",
            "long_run_eviction_age_retention_and_membership_sequences_are_not_claimed",
            "mutex_fairness_throughput_and_cross_thread_completion_order_are_not_claimed",
            "the_packed_payload_is_not_claimed_as_total_heap_or_allocator_footprint",
            "external_nonvendored_BoomFilters_sources_are_fixture_bound_but_not_rehashed_by_Rust",
            "sample_worker_triage_KTable_recursive_fanout_supervisor_app_and_live_behavior_are_not_implemented",
        ]
    );

    let source_row = &fixtures[0];
    assert_eq!(
        source_row.oracle.composition,
        "exact_production_wrapper_factory_and_module_source_pin"
    );
    assert_eq!(
        source_row.oracle.determinism,
        "normalized_AST_exact_repo_and_module_source_SHA256_and_Go_module_lines"
    );
    assert_eq!(source_row.oracle.filter, "BoomFilters_StableBloomFilter");
    assert_eq!(
        source_row.oracle.randomness,
        "source_only_random_decrement_offsets_and_long_run_probabilistic_behavior"
    );
    assert_eq!(source_row.input.kind, "source_contract");
    assert!(source_row.input.operations.is_empty());
    assert!(source_row.input.contention.is_none());
    assert!(source_row.expected.results.is_empty());
    assert!(source_row.expected.contention.is_none());
    let source = source_row.expected.source.as_ref().unwrap();

    assert!(source.mutex_covers_test_and_add);
    assert_eq!(source.input_byte_length, 20);
    assert_eq!(source.input_projection, "full_protocol_ID_20_byte_slice");
    assert!(source.test_precedes_random_decrement);
    assert!(source.every_call_adds);
    assert!(source.process_local);
    assert!(!source.persisted);
    assert_eq!(source.cells, CELL_COUNT);
    assert_eq!(source.bits_per_cell, BITS_PER_CELL);
    assert_eq!(source.target_false_positive_rate, 0.001);
    assert_eq!(source.derived_hash_functions, HASH_FUNCTIONS);
    assert_eq!(source.derived_decrement_cells, DECREMENT_CELLS);
    assert_eq!(source.derived_max_cell_value, MAX_CELL_VALUE);
    assert_eq!(source.derived_index_buffer_length, HASH_FUNCTIONS);
    assert_eq!(source.derived_cell_payload_bytes, CELL_PAYLOAD_BYTES);
    assert_eq!(source.derived_serialized_bytes, 2_500_091);
    assert_eq!(
        source.hash_kernel,
        "FNV-1_64; index_i=(low32(sum)+high32(sum)*i)%10_000_000"
    );
    assert_eq!(
        source.random_decrement_source,
        "one_math/rand_Intn(10_000_000)_start_then_49_adjacent_cells_modulo_10_000_000"
    );
    assert!(source.stable_eviction);
    assert!(source.false_positives_possible);
    assert!(source.false_negatives_possible);
    assert_eq!(source.module_path, "github.com/tylertreat/BoomFilters");
    assert_eq!(source.module_version, "v0.0.0-20210315201527-1a82519a3e43");
    assert_eq!(
        source.module_source_sum,
        "h1:QEePdg0ty2r0t1+qwfZmQ4OOl/MB2UXIeJSpIZv56lg="
    );
    assert_eq!(
        source.module_go_mod_sum,
        "h1:OYRfF6eb5wY9VRFkXJH8FFBi3plw2v+giaIu7P054pM="
    );
    assert_eq!(source.dependency_source_pin, "Go_module_zip_h1_sum");
    assert!(!source.dependency_source_vendored);
    assert_eq!(
        source.go_mod_requirement,
        "github.com/tylertreat/BoomFilters v0.0.0-20210315201527-1a82519a3e43"
    );
    assert_eq!(source.go_sum_module_line, "github.com/tylertreat/BoomFilters v0.0.0-20210315201527-1a82519a3e43 h1:QEePdg0ty2r0t1+qwfZmQ4OOl/MB2UXIeJSpIZv56lg=");
    assert_eq!(source.go_sum_go_mod_line, "github.com/tylertreat/BoomFilters v0.0.0-20210315201527-1a82519a3e43/go.mod h1:OYRfF6eb5wY9VRFkXJH8FFBi3plw2v+giaIu7P054pM=");
    assert!(GO_MOD
        .lines()
        .any(|line| line.strip_prefix('\t') == Some(source.go_mod_requirement.as_str())));
    assert!(GO_SUM.lines().any(|line| line == source.go_sum_module_line));
    assert!(GO_SUM.lines().any(|line| line == source.go_sum_go_mod_line));
    assert_eq!(
        source.adjacent_duplicate_scope,
        "fresh_zero_filter_two_distinct_IDs_each_immediately_repeated"
    );
    assert_eq!(
        source
            .nonclaims
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        GO_NONCLAIMS
    );
    assert_eq!(source.evidence, "the runtime row calls the actual mutex wrapper over one fresh production-parameter filter; only adjacent-duplicate results and contention aggregates are exact");

    let expected_digests = SOURCE_DIGESTS
        .into_iter()
        .map(|(path, sha)| (path.to_owned(), sha.to_owned()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(source.source_sha256, expected_digests);
    for (path, bytes) in LOCAL_SOURCES {
        assert_eq!(
            digest(bytes),
            source.source_sha256[path],
            "source digest changed for {path}"
        );
    }

    let runtime_row = &fixtures[1];
    assert_eq!(
        runtime_row.oracle.composition,
        "actual_ignoreHashes_testAndAdd_with_fresh_production_BoomFilters_instance"
    );
    assert_eq!(
        runtime_row.oracle.determinism,
        "fresh_zero_filter_adjacent_duplicates_and_same_ID_contention_aggregate_only"
    );
    assert_eq!(
        runtime_row.oracle.filter,
        "actual_BoomFilters_StableBloomFilter"
    );
    assert_eq!(
        runtime_row.oracle.randomness,
        "random_decrement_does_not_change_this_membership_prefix"
    );
    assert_eq!(
        runtime_row.input.kind,
        "actual_fresh_production_ignore_hashes"
    );
    assert!(runtime_row.expected.source.is_none());
}

#[test]
fn runtime_row_replays_adjacent_duplicates_and_contention_aggregate() {
    let fixtures = parse_fixtures();
    let row = &fixtures[1];
    let expected_operations = [
        ("A:first", "00000000000000000000000000000000000000a1", false),
        (
            "A:adjacent_duplicate",
            "00000000000000000000000000000000000000a1",
            true,
        ),
        ("B:first", "00000000000000000000000000000000000000b2", false),
        (
            "B:adjacent_duplicate",
            "00000000000000000000000000000000000000b2",
            true,
        ),
    ];
    assert_eq!(row.input.operations.len(), expected_operations.len());
    assert_eq!(row.expected.results.len(), expected_operations.len());

    let deduper = DhtInfoHashDeduper::new();
    for ((operation, expected), (token, id_hex, already_present)) in row
        .input
        .operations
        .iter()
        .zip(&row.expected.results)
        .zip(expected_operations)
    {
        assert_eq!(operation.token, token);
        assert_eq!(operation.id, id_hex);
        assert_eq!(expected.token, token);
        assert_eq!(expected.already_present, already_present);
        assert_eq!(
            deduper.test_and_add(Id20::from_hex(&operation.id).unwrap()),
            already_present,
            "{}",
            operation.token
        );
    }

    let contention = row.input.contention.as_ref().unwrap();
    let expected = row.expected.contention.as_ref().unwrap();
    assert_eq!(contention.id, "00000000000000000000000000000000000000c3");
    assert_eq!(contention.call_count, 8);
    assert_eq!(expected.false_count, 1);
    assert_eq!(expected.true_count, 7);
    assert!(expected.sequential_already_present);

    let info_hash = Id20::from_hex(&contention.id).unwrap();
    let barrier = Arc::new(Barrier::new(contention.call_count));
    let handles = (0..contention.call_count)
        .map(|_| {
            let deduper = deduper.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                deduper.test_and_add(info_hash)
            })
        })
        .collect::<Vec<_>>();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        results.iter().filter(|&&present| !present).count(),
        expected.false_count
    );
    assert_eq!(
        results.iter().filter(|&&present| present).count(),
        expected.true_count
    );
    assert_eq!(
        deduper.test_and_add(info_hash),
        expected.sequential_already_present
    );
}
