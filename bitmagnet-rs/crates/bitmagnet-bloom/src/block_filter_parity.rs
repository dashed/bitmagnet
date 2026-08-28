use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::{fnv1_64, hash_indices, DecrementStartSource, StableBloomFilter, StableBloomGeometry};

const FIXTURE: &str =
    include_str!("../../../../testdata/parity/dht/dht_info_hash_block_filter.jsonl");
const FIXTURE_SHA256: &str = "cc17edc11e5a21fe668d1067d2cf7413643bfdc8b81b0d5e97e5830afb1a51b4";
const GO_MOD: &str = include_str!("../../../../go.mod");
const GO_SUM: &str = include_str!("../../../../go.sum");
const LOCAL_SOURCES: [(&str, &[u8]); 6] = [
    (
        "internal/blocking/factory.go",
        include_bytes!("../../../../internal/blocking/factory.go"),
    ),
    (
        "internal/blocking/manager.go",
        include_bytes!("../../../../internal/blocking/manager.go"),
    ),
    (
        "internal/bloom/stable.go",
        include_bytes!("../../../../internal/bloom/stable.go"),
    ),
    (
        "internal/lazy/lazy.go",
        include_bytes!("../../../../internal/lazy/lazy.go"),
    ),
    (
        "migrations/00005_bloom_filters.sql",
        include_bytes!("../../../../migrations/00005_bloom_filters.sql"),
    ),
    (
        "migrations/00020_bloom_filters_large_object.sql",
        include_bytes!("../../../../migrations/00020_bloom_filters_large_object.sql"),
    ),
];
const ROW_IDS: [&str; 2] = [
    "production_source_storage_and_lifecycle_contract",
    "fresh_filter_single_add_wire_roundtrip",
];
const ROW_CLASSIFICATIONS: [&str; 2] = ["SOURCE_ONLY", "RUNTIME_EXACT"];
const SOURCE_NONCLAIMS: [&str; 11] = [
    "multi-add serialized bytes or digest",
    "math/rand seed sequence decrement start or decremented cells",
    "maps.Keys order or stable-filter add order for buffered hashes",
    "long-run false-positive false-negative eviction or retention sequence",
    "live PostgreSQL schema permissions transactions large-object I/O or rollback",
    "exact PostgreSQL object ID timestamp query plan round trips or driver errors",
    "cross-process flush serialization lost-update prevention or replica behavior",
    "manager runtime filtering buffering threshold timing flush errors or shutdown execution",
    "mutex fairness throughput cancellation latency or caller scheduling",
    "metrics logs retries statement timeouts health checks or observability",
    "Rust implementation API hardening lifecycle wiring deployment or production readiness",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureRow {
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
    database: String,
    randomness: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Input {
    kind: String,
    #[serde(default)]
    info_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    #[serde(default)]
    source: Option<SourceEvidence>,
    #[serde(default)]
    wire: Option<WireEvidence>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SourceEvidence {
    filter_key: String,
    buffer_capacity: usize,
    max_buffer_size: usize,
    max_flush_wait_seconds: u64,
    input_byte_length: usize,
    filter_preserves_input_order: bool,
    filter_preserves_duplicates: bool,
    buffer_checked_before_bloom: bool,
    first_filter_loads_state: bool,
    block_deduplicates_buffer: bool,
    empty_flush_skips_database: bool,
    flush_transaction_mode: String,
    delete_precedes_bloom_load: bool,
    success_only_state_swap: bool,
    shutdown_flush_if_initialized: bool,
    filter_table: String,
    large_object_column: String,
    metrics: String,
    module_path: String,
    module_version: String,
    module_source_sum: String,
    module_go_mod_sum: String,
    go_mod_requirement: String,
    go_sum_module_line: String,
    go_sum_go_mod_line: String,
    normalized_ast_sha256: BTreeMap<String, String>,
    source_sha256: BTreeMap<String, String>,
    evidence: String,
    nonclaims: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireEvidence {
    cells: usize,
    bits_per_cell: u8,
    hash_functions: usize,
    decrement_cells: usize,
    max_cell_value: u8,
    index_buffer: Vec<usize>,
    bucket_size: u8,
    bucket_max: u8,
    bucket_count: usize,
    cell_payload_bytes: usize,
    header_bytes: usize,
    header_hex: String,
    serialized_bytes: usize,
    serialized_sha256: String,
    hash_kernel: String,
    hash_indices: Vec<usize>,
    nonzero_payload_bytes: Vec<NonzeroPayloadByte>,
    member_after_add: bool,
    absent_probe_info_hash: String,
    absent_probe_member: bool,
    read_bytes: usize,
    member_after_roundtrip: bool,
    absent_probe_member_after_roundtrip: bool,
    reencoded_identical: bool,
    reencoded_sha256: String,
    raw_fixture_bytes_embedded: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NonzeroPayloadByte {
    payload_offset: usize,
    serialized_offset: usize,
    value: u8,
}

#[derive(Debug)]
struct OneStart {
    start: Option<usize>,
    observed_cell_count: Option<usize>,
}

impl DecrementStartSource for OneStart {
    fn next_start(&mut self, cell_count: NonZeroUsize) -> usize {
        assert!(self.observed_cell_count.replace(cell_count.get()).is_none());
        self.start
            .take()
            .expect("single decrement start consumed once")
    }
}

#[test]
fn fixture_schema_identity_source_contract_and_nonclaims_are_exact() {
    let rows = load_fixture();
    assert_eq!(sha256_hex(FIXTURE.as_bytes()), FIXTURE_SHA256);
    assert_eq!(
        rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        ROW_IDS
    );
    assert_eq!(
        rows.iter()
            .map(|row| row.classification.as_str())
            .collect::<Vec<_>>(),
        ROW_CLASSIFICATIONS
    );
    assert!(rows
        .iter()
        .all(|row| row.subsystem == "dht_info_hash_block_filter"));

    let row = &rows[0];
    assert_eq!(
        row.oracle.composition,
        "exact_production_blocking_manager_factory_bloom_wrapper_migrations_and_pinned_BoomFilters_source"
    );
    assert_eq!(
        row.oracle.determinism,
        "normalized_AST_exact_source_SHA256_module_checksums_and_dependency_lines"
    );
    assert_eq!(
        row.oracle.database,
        "source_contract_only_without_live_PostgreSQL_or_large_object_execution"
    );
    assert_eq!(
        row.oracle.randomness,
        "source_only_for_multi_add_map_order_and_random_decrement_behavior"
    );
    assert_eq!(row.input.kind, "source_contract");
    assert!(row.input.info_hash.is_none());
    assert!(row.expected.wire.is_none());

    let source = row.expected.source.as_ref().expect("source evidence");
    assert_eq!(source.filter_key, "blocked_torrents");
    assert_eq!(source.buffer_capacity, 1_000);
    assert_eq!(source.max_buffer_size, 1_000);
    assert_eq!(source.max_flush_wait_seconds, 300);
    assert_eq!(source.input_byte_length, 20);
    assert!(source.filter_preserves_input_order);
    assert!(source.filter_preserves_duplicates);
    assert!(source.buffer_checked_before_bloom);
    assert!(source.first_filter_loads_state);
    assert!(source.block_deduplicates_buffer);
    assert!(source.empty_flush_skips_database);
    assert_eq!(source.flush_transaction_mode, "read_write");
    assert!(source.delete_precedes_bloom_load);
    assert!(source.success_only_state_swap);
    assert!(source.shutdown_flush_if_initialized);
    assert_eq!(source.filter_table, "bloom_filters");
    assert_eq!(source.large_object_column, "oid");
    assert_eq!(source.metrics, "none");
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
    assert_eq!(
        source.go_mod_requirement,
        "github.com/tylertreat/BoomFilters v0.0.0-20210315201527-1a82519a3e43"
    );
    assert_eq!(
        source.go_sum_module_line,
        "github.com/tylertreat/BoomFilters v0.0.0-20210315201527-1a82519a3e43 h1:QEePdg0ty2r0t1+qwfZmQ4OOl/MB2UXIeJSpIZv56lg="
    );
    assert_eq!(
        source.go_sum_go_mod_line,
        "github.com/tylertreat/BoomFilters v0.0.0-20210315201527-1a82519a3e43/go.mod h1:OYRfF6eb5wY9VRFkXJH8FFBi3plw2v+giaIu7P054pM="
    );
    assert_eq!(source.normalized_ast_sha256, expected_ast_digests());
    assert_eq!(source.source_sha256, expected_source_digests());
    for (path, bytes) in LOCAL_SOURCES {
        assert_eq!(
            source.source_sha256.get(path).map(String::as_str),
            Some(sha256_hex(bytes).as_str()),
            "repository-local source digest drifted for {path}"
        );
    }
    let go_mod_line = format!("\t{}", source.go_mod_requirement);
    assert!(GO_MOD.lines().any(|line| line == go_mod_line));
    assert!(GO_SUM.lines().any(|line| line == source.go_sum_module_line));
    assert!(GO_SUM.lines().any(|line| line == source.go_sum_go_mod_line));
    assert_eq!(
        source.evidence,
        "source-bound manager state machine plus a deterministic one-Add production codec row"
    );
    assert_eq!(
        source
            .nonclaims
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        SOURCE_NONCLAIMS
    );
}

#[test]
fn runtime_one_add_raw_wire_roundtrip_is_exact() {
    assert_eq!(
        usize::BITS,
        64,
        "the pinned Go oracle indexes use 64-bit uint arithmetic"
    );
    let rows = load_fixture();
    let row = &rows[1];
    assert_eq!(
        row.oracle.composition,
        "actual_internal_bloom_default_StableBloomFilter_single_Add_WriteTo_ReadFrom_WriteTo"
    );
    assert_eq!(
        row.oracle.determinism,
        "fresh_zero_cells_make_the_single_random_decrement_observationally_inert"
    );
    assert_eq!(
        row.oracle.database,
        "no_database_raw_BoomFilters_stream_wire_only"
    );
    assert_eq!(
        row.oracle.randomness,
        "one_random_decrement_occurs_but_cannot_change_any_initially_zero_payload_cell"
    );
    assert_eq!(row.input.kind, "fresh_filter_single_add_wire_roundtrip");
    assert!(row.expected.source.is_none());
    let info_hash = parse_hash(row.input.info_hash.as_deref().expect("runtime info hash"));
    let wire = row.expected.wire.as_ref().expect("wire evidence");

    assert_eq!(wire.cells, 100_000_000);
    assert_eq!(wire.bits_per_cell, 2);
    assert_eq!(wire.hash_functions, 5);
    assert_eq!(wire.decrement_cells, 49);
    assert_eq!(wire.max_cell_value, 3);
    assert_eq!(wire.index_buffer, [0, 0, 0, 0, 0]);
    assert_eq!(wire.bucket_size, 2);
    assert_eq!(wire.bucket_max, 3);
    assert_eq!(wire.bucket_count, 100_000_000);
    assert_eq!(wire.cell_payload_bytes, 25_000_000);
    assert_eq!(wire.header_bytes, 91);
    assert_eq!(
        wire.header_hex,
        "0000000005f5e100000000000000003100000000000000050300000000000000050000000000000000000000000000000000000000000000000000000000000000000000000000000002030000000005f5e10000000000017d7840"
    );
    assert_eq!(wire.serialized_bytes, 25_000_091);
    assert_eq!(
        wire.serialized_sha256,
        "e3aa29b65ca06e28b65e9434e13a12ed36c76ea2bbb6597579a9dba207fdece3"
    );
    assert_eq!(
        wire.hash_kernel,
        "FNV-1_64; index_i=(low32(sum)+high32(sum)*i)%100_000_000"
    );
    assert_eq!(
        wire.hash_indices,
        [94_110_100, 95_868_049, 97_625_998, 99_383_947, 1_141_896]
    );
    assert_eq!(
        wire.nonzero_payload_bytes,
        [
            NonzeroPayloadByte {
                payload_offset: 285_474,
                serialized_offset: 285_565,
                value: 3,
            },
            NonzeroPayloadByte {
                payload_offset: 23_527_525,
                serialized_offset: 23_527_616,
                value: 3,
            },
            NonzeroPayloadByte {
                payload_offset: 23_967_012,
                serialized_offset: 23_967_103,
                value: 12,
            },
            NonzeroPayloadByte {
                payload_offset: 24_406_499,
                serialized_offset: 24_406_590,
                value: 48,
            },
            NonzeroPayloadByte {
                payload_offset: 24_845_986,
                serialized_offset: 24_846_077,
                value: 192,
            },
        ]
    );
    assert!(wire.member_after_add);
    assert_eq!(
        wire.absent_probe_info_hash,
        "00000000000000000000000000000000000000b2"
    );
    assert!(!wire.absent_probe_member);
    assert_eq!(wire.read_bytes, 25_000_091);
    assert!(wire.member_after_roundtrip);
    assert!(!wire.absent_probe_member_after_roundtrip);
    assert!(wire.reencoded_identical);
    assert_eq!(wire.reencoded_sha256, wire.serialized_sha256);
    assert!(!wire.raw_fixture_bytes_embedded);

    let geometry = StableBloomGeometry::new(
        wire.cells,
        wire.bits_per_cell,
        wire.hash_functions,
        wire.decrement_cells,
    )
    .expect("oracle geometry");
    assert_eq!(geometry.max_cell_value(), wire.max_cell_value);
    assert_eq!(geometry.packed_bytes(), wire.cell_payload_bytes);
    assert_eq!(geometry.encoded_bytes(), wire.serialized_bytes);
    let hash_sum = fnv1_64(&info_hash);
    assert_eq!(hash_sum, 0xee85_fafd_354b_0994);
    assert_eq!(hash_sum as u32, 894_110_100);
    assert_eq!((hash_sum >> 32) as u32, 4_001_757_949);
    assert_eq!(hash_indices(&info_hash, geometry), wire.hash_indices);

    let absent_probe = parse_hash(&wire.absent_probe_info_hash);
    let mut filter = StableBloomFilter::new(geometry);
    assert!(!filter.test(&info_hash));
    let mut decrement = OneStart {
        start: Some(0),
        observed_cell_count: None,
    };
    filter.add(&info_hash, &mut decrement);
    assert!(decrement.start.is_none());
    assert_eq!(decrement.observed_cell_count, Some(wire.cells));
    assert_eq!(filter.test(&info_hash), wire.member_after_add);
    assert_eq!(filter.test(&absent_probe), wire.absent_probe_member);

    let mut encoded = Vec::with_capacity(wire.serialized_bytes);
    assert_eq!(
        filter.write_to(&mut encoded).unwrap(),
        wire.serialized_bytes
    );
    assert_eq!(encoded.len(), wire.serialized_bytes);
    let expected_header = decode_hex(&wire.header_hex);
    assert_eq!(expected_header.len(), wire.header_bytes);
    assert_eq!(&encoded[..wire.header_bytes], expected_header);
    assert_eq!(sha256_hex(&encoded), wire.serialized_sha256);
    let actual_nonzero = encoded[wire.header_bytes..]
        .iter()
        .enumerate()
        .filter_map(|(payload_offset, &value)| {
            (value != 0).then_some(NonzeroPayloadByte {
                payload_offset,
                serialized_offset: wire.header_bytes + payload_offset,
                value,
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(actual_nonzero, wire.nonzero_payload_bytes);

    drop(filter);
    let roundtrip = StableBloomFilter::read_from(encoded.as_slice(), geometry).unwrap();
    assert_eq!(encoded.len(), wire.read_bytes);
    assert_eq!(roundtrip.test(&info_hash), wire.member_after_roundtrip);
    assert_eq!(
        roundtrip.test(&absent_probe),
        wire.absent_probe_member_after_roundtrip
    );
    let mut reencoded = Vec::with_capacity(wire.serialized_bytes);
    assert_eq!(
        roundtrip.write_to(&mut reencoded).unwrap(),
        wire.serialized_bytes
    );
    assert_eq!(reencoded == encoded, wire.reencoded_identical);
    assert_eq!(sha256_hex(&reencoded), wire.reencoded_sha256);
}

fn load_fixture() -> Vec<FixtureRow> {
    assert!(FIXTURE.ends_with('\n'), "fixture must end with one newline");
    let lines = FIXTURE.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    lines
        .into_iter()
        .map(|line| serde_json::from_str(line).expect("strict block-filter fixture row"))
        .collect()
}

fn expected_ast_digests() -> BTreeMap<String, String> {
    BTreeMap::from(
        [
            (
                "blocking.New",
                "b0b08a55e7683980c3140ff4d8a9d41b0a70926e999fb22d83f5c5103e27362f",
            ),
            (
                "blocking.manager.Block",
                "d988679dd7182932f787e69c0bfa64e4245c2f945b1d7475bf5df0c1072ac50e",
            ),
            (
                "blocking.manager.Filter",
                "4a09ea55534e28d16dbda5d60c3f5c9d354ea6e120ff323674102214cdc42723",
            ),
            (
                "blocking.manager.Flush",
                "226940d1991ebe8ce1d717df5a7bde7014a2e3efee0f38ffb85ffd77472dda37",
            ),
            (
                "blocking.manager.flush",
                "7e961d417b2a7291f63ea6717a8842f48113d4fa10d7bae26eb6aedafafd0022",
            ),
            (
                "blocking.manager.shouldFlush",
                "ca350b7f4c1eb61204a141810c03bc6711b9989862f2eb1ac21ae98c8c95542e",
            ),
            (
                "bloom.NewDefaultStableBloomFilter",
                "c2aeda2f1a8ceb1a06a6a0026cd5af36444f8df2bbf6a6fa7d45d615ff5b922e",
            ),
            (
                "boom.Buckets.ReadFrom",
                "c5317a6855467803f10dfe93e77e6faa1852a8ac1e33e708c51a49cee3a4d7cf",
            ),
            (
                "boom.Buckets.WriteTo",
                "5243c65e678bbb67c74e4bab4d68e06adda181638a3c4a341f6e5e336996a919",
            ),
            (
                "boom.NewStableBloomFilter",
                "128981d2c0b266df53897ef3e5579a69229e84b0b3b25e8e884a5009cb049e13",
            ),
            (
                "boom.StableBloomFilter.Add",
                "77811b5ffb9d7c62ad08db95cc89305028f6eee4ceef4e84e15be05d42117af3",
            ),
            (
                "boom.StableBloomFilter.ReadFrom",
                "59d837787935241210bdd23a5389b287408a5f77ceb2ede806d153e74f278dc8",
            ),
            (
                "boom.StableBloomFilter.Test",
                "73ceeedfafc38b4ff4b02fc737d60c640b1464a9eae52d577fef6cbeaa703e1f",
            ),
            (
                "boom.StableBloomFilter.WriteTo",
                "fcc23206eec628b9cb95f4640ed06d5b2a95690070776269f29b23050bc48f5d",
            ),
            (
                "boom.hashKernel",
                "974271317e669fc2c2c00704a8d59628697d90ff972637185035decb515f9798",
            ),
            (
                "lazy.lazy.IfInitialized",
                "2644a4573d675bb21bca340c9672819b82d2460c102f98ecb7bc090dbbb7053a",
            ),
        ]
        .map(|(key, value)| (key.to_owned(), value.to_owned())),
    )
}

fn expected_source_digests() -> BTreeMap<String, String> {
    BTreeMap::from(
        [
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
                "internal/blocking/factory.go",
                "4761db2241277fdec317a15e53fb6f3d74ba79306fd9a1c04f3ef77d9fcbf8d9",
            ),
            (
                "internal/blocking/manager.go",
                "d32ef7b0fb1eeadaeb1134f49b1046911c27312d2383b402d5989c8bc830130f",
            ),
            (
                "internal/bloom/stable.go",
                "b038c1538c895e63efdecb86e1a16e5274591f8abd5a390697b183e16446d641",
            ),
            (
                "internal/lazy/lazy.go",
                "42984efd2a4a1934ad08186842b756230b60703c5bc3aa4c798820f90d087398",
            ),
            (
                "migrations/00005_bloom_filters.sql",
                "b34820c5722aeff6ae3b76bf5ae24446f97ead5bf134714e663f8928e7714316",
            ),
            (
                "migrations/00020_bloom_filters_large_object.sql",
                "910a3c9da021470540d2d10f812678d3d28ca788bedbe647df9d22d5730dc14e",
            ),
        ]
        .map(|(key, value)| (key.to_owned(), value.to_owned())),
    )
}

fn parse_hash(value: &str) -> [u8; 20] {
    decode_hex(value)
        .try_into()
        .expect("oracle info hash must be 20 bytes")
}

fn decode_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0, "hex must have complete bytes");
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| (hex_digit(pair[0]) << 4) | hex_digit(pair[1]))
        .collect()
}

fn hex_digit(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => panic!("oracle hex must be canonical lowercase"),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
