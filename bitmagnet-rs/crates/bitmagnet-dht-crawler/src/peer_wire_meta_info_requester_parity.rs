use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bitmagnet_dht::Id20;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use super::peer_wire_meta_info_requester::{
    handshake_request, perform_bit_torrent_handshake, perform_extension_handshake, read_all_pieces,
    read_message, request_all_pieces, DhtPeerWireMetaInfoRequesterError,
    DhtPeerWireMetaInfoRequesterStage, ExtensionHandshake, ADVERTISED_EXTENSION_BITS,
    EXTENSION_HANDSHAKE_REQUEST, HANDSHAKE_SIZE,
};

const FIXTURE_TEXT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/metainfo_requester.jsonl"
));
const FIXTURE_BYTES: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/metainfo_requester.jsonl"
));
const FIXTURE_SHA256: &str = "990f4d503065ed08689df37881817386874f12cda2fdaeaeb56c05e12bbcc80e";
const FIXTURE_BYTE_LENGTH: usize = 35_547;

const IDS: [&str; 7] = [
    "source_contract",
    "bt_handshake_and_extension_bits",
    "extension_handshake_boundaries",
    "piece_request_and_message_boundaries",
    "piece_reader_matrix",
    "requested_hash_parse_identity",
    "controlled_go_hazards",
];
const CLASSIFICATIONS: [&str; 7] = [
    "SOURCE_ONLY",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT_WITH_CONTROLLED_GO_HAZARD",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "GO_UNSAFE_CONTROLLED",
];
const EXECUTIONS: [&str; 7] = [
    "SOURCE_INSPECTION_ONLY",
    "ACTUAL_EXTENSION_BIT_AND_BT_HANDSHAKE_HELPERS",
    "ACTUAL_EXTENSION_HANDSHAKE_HELPERS_AND_IGNORED_WRITE_ERROR",
    "ACTUAL_REQUEST_ALL_PIECES_AND_READ_MESSAGE_HELPERS",
    "ACTUAL_READ_ALL_PIECES_MESSAGE_FILTER_AND_ERROR_HELPERS",
    "ACTUAL_PARSE_META_INFO_BYTES_REQUESTED_HASH_VERIFICATION",
    "CONTROLLED_RECOVERY_AROUND_ACTUAL_GO_PANIC_AND_HOLE_BEHAVIORS",
];
const RUST_EXECUTION_PARTITION: [(&str, &str); 7] = [
    (IDS[0], "SOURCE_AND_FRESHNESS_ASSERTIONS_ONLY"),
    (IDS[1], "RUST_WIRE_BUILDER_AND_HANDSHAKE_VALIDATION_REPLAY"),
    (
        IDS[2],
        "RUST_STRICT_EXTENSION_HANDSHAKE_REPLAY_WITH_GO_WRITE_HAZARD_REJECTED",
    ),
    (IDS[3], "RUST_PIECE_REQUEST_AND_FRAMED_READ_REPLAY"),
    (IDS[4], "RUST_UNIQUE_EXACT_PIECE_ASSEMBLY_REPLAY"),
    (IDS[5], "RUST_PARSE_INFO_BYTES_REQUESTED_IDENTITY_REPLAY"),
    (IDS[6], "GO_HAZARDS_ASSERTED_AND_RUST_HARDENING_REPLAYED"),
];
const RUST_NONCLAIMS: [&str; 8] = [
    "no_live_peer_or_external_network_execution",
    "no_DNS_resolution_or_hostname_support",
    "no_retry_factory_limiter_logging_or_metrics",
    "no_application_construction_supervision_or_production_wiring",
    "no_Go_transport_fragmentation_backpressure_or_concurrency_replay",
    "no_integrated_Go_forwarding_proof_from_extension_handshake_to_piece_requests",
    "no_v2_or_hybrid_parse_identity_runtime_replay",
    "no_acceptance_of_Go_duplicate_piece_hole_output_after_final_hash_validation",
];
const GO_NONCLAIMS: [&str; 15] = [
    "no_TCP_connect_or_socket_IO",
    "no_DNS_resolution",
    "no_context_or_socket_deadlines",
    "no_requester_factory_or_limiter_execution",
    "no_logger_or_metrics_execution",
    "no_live_network_or_remote_peer",
    "no_Request_or_connect_end_to_end_execution",
    "no_transport_fragmentation_backpressure_or_concurrency_claim",
    "out_of_order_success_is_proven_only_for_full_sized_pieces_not_an_early_short_final_piece",
    "direct_requestAllPieces_helper_boundary_proves_outbound_ut_metadata_ID_1_and_254_only",
    "no_integrated_runtime_proof_that_exHandshake_peer_advertised_ut_metadata_ID_is_forwarded_to_requestAllPieces",
    "incoming_response_filtering_is_runtime_proven_only_for_locally_advertised_ut_metadata_ID_1",
    "no_v2_or_hybrid_parse_identity_runtime_row",
    "duplicate_or_other_piece_reader_outputs_are_not_post_assembly_hash_verified_or_accepted_by_Request",
    "no_Rust_requester_parity_or_production_wiring_claim",
];

#[derive(Debug, Clone, PartialEq, Eq)]
struct StrictStringMap(BTreeMap<String, String>);

impl StrictStringMap {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<'de> Deserialize<'de> for StrictStringMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StrictStringMapVisitor;

        impl<'de> Visitor<'de> for StrictStringMapVisitor {
            type Value = StrictStringMap;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string map without duplicate keys")
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = BTreeMap::new();
                while let Some((key, value)) = access.next_entry::<String, String>()? {
                    if values.insert(key.clone(), value).is_some() {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate map key {key:?}"
                        )));
                    }
                }
                Ok(StrictStringMap(values))
            }
        }

        deserializer.deserialize_map(StrictStringMapVisitor)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Fixture {
    id: String,
    subsystem: String,
    classification: String,
    execution: String,
    oracle: Oracle,
    input: Input,
    expected: Expected,
    nonclaims: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Oracle {
    composition: String,
    determinism: String,
    in_memory_only: bool,
    tcp_executed: bool,
    dns_executed: bool,
    deadlines_executed: bool,
    factory_limiter_executed: bool,
    logging_executed: bool,
    metrics_executed: bool,
    actual_functions_executed: Vec<String>,
    source_pinned_harness_steps: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Input {
    kind: String,
    info_hash: String,
    client_id: String,
    peer_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Expected {
    source: Option<SourceContract>,
    extension_bits: Option<ExtensionBits>,
    handshakes: Vec<Handshake>,
    extension_handshakes: Vec<ExtensionHandshakeRow>,
    piece_requests: Vec<PieceRequest>,
    messages: Vec<MessageRead>,
    piece_reads: Vec<PieceRead>,
    parser: Option<ParserResult>,
    hazards: Vec<Hazard>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SourceContract {
    max_metadata_size: u64,
    piece_size: u64,
    handshake_size: u64,
    locally_advertised_ut_metadata_id: u64,
    incoming_response_ut_metadata_id: u64,
    remote_ut_metadata_minimum: u64,
    remote_ut_metadata_maximum: u64,
    advertised_extensions: Vec<String>,
    source_sha256: StrictStringMap,
    dependency_sha256: StrictStringMap,
    dependency_lines: Vec<String>,
    normalized_ast_sha256: StrictStringMap,
    controlled_go_hazards: Vec<String>,
    rust_hardening_allowed: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExtensionBits {
    replay_disposition: String,
    dht_bit: u64,
    ltep_bit: u64,
    dht_only_hex: String,
    ltep_only_hex: String,
    advertised_hex: String,
    dht_enabled: bool,
    ltep_enabled: bool,
    round_trip_disable_dht_hex: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Handshake {
    replay_disposition: String,
    label: String,
    response_wire_hex: String,
    attempted_request_hex: String,
    write_calls: u64,
    attempted_bytes: u64,
    reported_written_bytes: u64,
    peer_id: String,
    peer_extension_bits_hex: String,
    error: String,
    error_identity_preserved: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExtensionHandshakeRow {
    replay_disposition: String,
    label: String,
    response_wire_hex: String,
    ignored_frame_hex: Vec<String>,
    attempted_advertised_request_hex: String,
    write_calls: u64,
    attempted_bytes: u64,
    reported_written_bytes: u64,
    metadata_size_input: Option<i64>,
    ut_metadata_input: Option<i64>,
    metadata_size: u64,
    ut_metadata: u64,
    write_error_injected: bool,
    write_error_ignored: bool,
    error: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PieceRequest {
    replay_disposition: String,
    label: String,
    metadata_size: u64,
    ut_metadata: u64,
    piece_count: u64,
    frames_hex: Vec<String>,
    combined_hex: String,
    combined_sha256: String,
    error: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MessageRead {
    replay_disposition: String,
    label: String,
    declared_length: u64,
    payload_pattern_byte_hex: String,
    payload_pattern_length: u64,
    payload_sha256: String,
    returned: bool,
    returned_is_nil: bool,
    returned_length: u64,
    returned_sha256: String,
    error: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PieceRead {
    replay_disposition: String,
    label: String,
    metadata_size: u64,
    input_frame_hex: Vec<String>,
    input_frame_lengths: Vec<u64>,
    input_patterns: Vec<FramePattern>,
    input_byte_length: u64,
    input_sha256: String,
    returned: bool,
    returned_length: u64,
    returned_sha256: String,
    returned_prefix_hex: String,
    returned_suffix_hex: String,
    error: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FramePattern {
    label: String,
    header_hex: String,
    payload_encoding: String,
    payload_literal_hex: String,
    repeat_byte_hex: String,
    payload_length: u64,
    frame_length: u64,
    frame_sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ParserResult {
    replay_disposition: String,
    raw_info_hex: String,
    raw_info_sha256: String,
    requested_info_hash: String,
    wrong_requested_hash: String,
    meta_version: u64,
    info_hash_v1: String,
    info_hash_v2: Option<String>,
    name: String,
    length: i64,
    wrong_hash_error: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Hazard {
    replay_disposition: String,
    label: String,
    panic_observed: bool,
    panic_class: String,
    panic_type: String,
    panic_text: String,
    harness_contract_violation: bool,
    attempted_wire_hex: String,
    reported_written_bytes: u64,
    input_patterns: Vec<FramePattern>,
    input_byte_length: u64,
    input_sha256: String,
    metadata_size: u64,
    returned: bool,
    returned_length: u64,
    returned_sha256: String,
    duplicate_aggregate_count: u64,
    distinct_piece_indexes: u64,
    hole_offset: u64,
    hole_length: u64,
    hole_all_zero: bool,
    rust_may_reject: bool,
}

fn fixtures() -> Vec<Fixture> {
    FIXTURE_TEXT
        .lines()
        .map(|line| decode_fixture_line(line).expect("strict metainfo requester fixture row"))
        .collect()
}

fn decode_fixture_line(line: &str) -> Result<Fixture, String> {
    // Decode the typed representation first so duplicate struct and strict-map
    // keys are rejected before the generic Value representation can collapse
    // them.
    let fixture: Fixture = serde_json::from_str(line).map_err(|error| error.to_string())?;
    let raw: Value = serde_json::from_str(line).map_err(|error| error.to_string())?;
    let expected = raw
        .pointer("/expected")
        .and_then(Value::as_object)
        .ok_or_else(|| "expected must be an object".to_owned())?;
    for key in ["source", "extensionBits", "parser"] {
        if !expected.contains_key(key) {
            return Err(format!("expected.{key} must be explicitly present"));
        }
    }
    let extension_handshakes = expected
        .get("extensionHandshakes")
        .and_then(Value::as_array)
        .ok_or_else(|| "expected.extensionHandshakes must be an array".to_owned())?;
    for (index, handshake) in extension_handshakes.iter().enumerate() {
        let handshake = handshake
            .as_object()
            .ok_or_else(|| format!("expected.extensionHandshakes[{index}] must be an object"))?;
        for key in ["metadataSizeInput", "utMetadataInput"] {
            if !handshake.contains_key(key) {
                return Err(format!(
                    "expected.extensionHandshakes[{index}].{key} must be explicitly present"
                ));
            }
        }
    }
    if let Some(parser) = expected.get("parser").and_then(Value::as_object) {
        if !parser.contains_key("infoHashV2") {
            return Err("expected.parser.infoHashV2 must be explicitly present".to_owned());
        }
    }
    Ok(fixture)
}

fn hex_bytes(value: &str) -> Vec<u8> {
    assert!(value.len().is_multiple_of(2), "hex width");
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let digit = |byte: u8| match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                _ => panic!("fixture hex must be lowercase"),
            };
            digit(pair[0]) << 4 | digit(pair[1])
        })
        .collect()
}

fn hex_string(value: impl AsRef<[u8]>) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let value = value.as_ref();
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

fn id20(value: &str) -> Id20 {
    Id20::from_slice(&hex_bytes(value)).expect("20-byte fixture identity")
}

fn sha256(value: &[u8]) -> String {
    format!("{:x}", Sha256::digest(value))
}

fn frame_pattern(pattern: &FramePattern) -> Vec<u8> {
    let mut output = hex_bytes(&pattern.header_hex);
    match pattern.payload_encoding.as_str() {
        "none" => {
            assert!(pattern.payload_literal_hex.is_empty());
            assert!(pattern.repeat_byte_hex.is_empty());
            assert_eq!(pattern.payload_length, 0);
        }
        "literal_hex" => {
            assert!(pattern.repeat_byte_hex.is_empty());
            let payload = hex_bytes(&pattern.payload_literal_hex);
            assert_eq!(payload.len() as u64, pattern.payload_length);
            output.extend_from_slice(&payload);
        }
        "repeat_byte" => {
            assert!(pattern.payload_literal_hex.is_empty());
            let byte = hex_bytes(&pattern.repeat_byte_hex);
            assert_eq!(byte.len(), 1);
            output.resize(output.len() + pattern.payload_length as usize, byte[0]);
        }
        other => panic!("unknown payload encoding {other}"),
    }
    assert_eq!(
        output.len() as u64,
        pattern.frame_length,
        "{}",
        pattern.label
    );
    assert_eq!(sha256(&output), pattern.frame_sha256, "{}", pattern.label);
    output
}

fn piece_read_wire(row: &PieceRead) -> Vec<u8> {
    let pattern_frames = row
        .input_patterns
        .iter()
        .map(frame_pattern)
        .collect::<Vec<_>>();
    let literal_frames = row
        .input_frame_hex
        .iter()
        .map(|value| hex_bytes(value))
        .collect::<Vec<_>>();
    if !literal_frames.is_empty() {
        assert_eq!(
            literal_frames, pattern_frames,
            "{} dual encoding",
            row.label
        );
    }
    let frames = if literal_frames.is_empty() {
        pattern_frames
    } else {
        literal_frames
    };
    assert_eq!(
        frames
            .iter()
            .map(|frame| frame.len() as u64)
            .collect::<Vec<_>>(),
        row.input_frame_lengths
    );
    let output = frames.concat();
    assert_eq!(output.len() as u64, row.input_byte_length);
    assert_eq!(sha256(&output), row.input_sha256);
    output
}

fn test_peer() -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6881)
}

#[derive(Debug)]
struct WriteSentinel {
    label: &'static str,
    identity: Arc<()>,
}

impl fmt::Display for WriteSentinel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.label)
    }
}

impl std::error::Error for WriteSentinel {}

struct FailingWriteStream {
    sentinel: Option<WriteSentinel>,
    attempted: Arc<Mutex<Vec<u8>>>,
}

impl AsyncRead for FailingWriteStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for FailingWriteStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.attempted
            .lock()
            .expect("attempted write lock")
            .extend_from_slice(buffer);
        let sentinel = self
            .sentinel
            .take()
            .expect("fixture performs exactly one failing write");
        Poll::Ready(Err(io::Error::other(sentinel)))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn failing_write_stream(label: &'static str) -> (FailingWriteStream, Arc<()>, Arc<Mutex<Vec<u8>>>) {
    let identity = Arc::new(());
    let attempted = Arc::new(Mutex::new(Vec::new()));
    (
        FailingWriteStream {
            sentinel: Some(WriteSentinel {
                label,
                identity: Arc::clone(&identity),
            }),
            attempted: Arc::clone(&attempted),
        },
        identity,
        attempted,
    )
}

fn assert_write_error(
    error: DhtPeerWireMetaInfoRequesterError,
    stage: DhtPeerWireMetaInfoRequesterStage,
    label: &str,
    identity: &Arc<()>,
) {
    let DhtPeerWireMetaInfoRequesterError::Io {
        peer,
        stage: actual_stage,
        source,
    } = error
    else {
        panic!("unexpected write error: {error}");
    };
    assert_eq!(peer, test_peer());
    assert_eq!(actual_stage, stage);
    assert_eq!(source.kind(), io::ErrorKind::Other);
    assert_eq!(source.to_string(), label);
    let sentinel = source
        .get_ref()
        .and_then(|source| source.downcast_ref::<WriteSentinel>())
        .expect("typed write sentinel source");
    assert_eq!(sentinel.label, label);
    assert!(Arc::ptr_eq(&sentinel.identity, identity));
}

fn assert_empty_expected_except(row: &Fixture, branch: &str) {
    let expected = &row.expected;
    assert_eq!(expected.source.is_some(), branch == "source");
    assert_eq!(
        expected.extension_bits.is_some(),
        matches!(branch, "extensionBits" | "handshakes")
    );
    assert_eq!(expected.parser.is_some(), branch == "parser");
    assert_eq!(expected.handshakes.is_empty(), branch != "handshakes");
    assert_eq!(
        expected.extension_handshakes.is_empty(),
        branch != "extensionHandshakes"
    );
    assert_eq!(
        expected.piece_requests.is_empty(),
        branch != "pieceRequests"
    );
    assert_eq!(expected.messages.is_empty(), branch != "messages");
    assert_eq!(expected.piece_reads.is_empty(), branch != "pieceReads");
    assert_eq!(expected.hazards.is_empty(), branch != "hazards");
}

#[test]
fn fixture_envelope_source_and_execution_partition_are_exact() {
    assert_eq!(FIXTURE_BYTES.len(), FIXTURE_BYTE_LENGTH);
    assert_eq!(sha256(FIXTURE_BYTES), FIXTURE_SHA256);
    assert!(FIXTURE_BYTES.ends_with(b"\n"));
    assert!(!FIXTURE_BYTES.contains(&b'\r'));
    assert_eq!(
        FIXTURE_BYTES.iter().filter(|byte| **byte == b'\n').count(),
        7
    );

    let rows = fixtures();
    assert_eq!(rows.len(), IDS.len());
    let actual_functions: [&[&str]; 7] = [
        &[],
        &[
            "NewPeerExtensionBits",
            "PeerExtensionBits.WithBit",
            "PeerExtensionBits.GetBit",
            "btHandshake",
        ],
        &["exHandshake", "readExMessage", "readMessage"],
        &["requestAllPieces", "uintToBigEndian4", "readMessage"],
        &[
            "readAllPieces",
            "readUmMessage",
            "readExMessage",
            "readMessage",
        ],
        &["metainfo.ParseMetaInfoBytes"],
        &[
            "btHandshake",
            "readAllPieces",
            "readUmMessage",
            "readExMessage",
            "readMessage",
        ],
    ];
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.id, IDS[index]);
        assert_eq!(row.subsystem, "metainfo_requester");
        assert_eq!(row.classification, CLASSIFICATIONS[index]);
        assert_eq!(row.execution, EXECUTIONS[index]);
        assert_eq!(
            row.oracle.actual_functions_executed,
            actual_functions[index]
        );
        assert!(!row.oracle.composition.is_empty());
        assert!(!row.oracle.determinism.is_empty());
        assert!(!row.oracle.tcp_executed);
        assert!(!row.oracle.dns_executed);
        assert!(!row.oracle.deadlines_executed);
        assert!(!row.oracle.factory_limiter_executed);
        assert!(!row.oracle.logging_executed);
        assert!(!row.oracle.metrics_executed);
        assert_eq!(row.oracle.in_memory_only, index != 0);
        assert_eq!(
            row.oracle.source_pinned_harness_steps.is_empty(),
            index != 0
        );
        assert_eq!(row.nonclaims, GO_NONCLAIMS);
        assert_eq!(row.input.info_hash.len(), 40);
        assert_eq!(row.input.client_id.len(), 40);
        assert_eq!(row.input.peer_id.len(), 40);
        assert!(!row.input.kind.is_empty());
    }
    assert_eq!(
        rows[0].oracle.source_pinned_harness_steps,
        [
            "parse_and_format_exact_named_production_AST_functions",
            "hash_full_relevant_source_and_dependency_files",
            "extract_exact_anacrolix_torrent_dependency_lines",
        ]
    );
    assert_eq!(
        RUST_EXECUTION_PARTITION
            .iter()
            .map(|entry| entry.0)
            .collect::<Vec<_>>(),
        IDS
    );
    assert_eq!(RUST_NONCLAIMS.len(), 8);
    assert_empty_expected_except(&rows[0], "source");
    assert_empty_expected_except(&rows[1], "handshakes");
    assert_empty_expected_except(&rows[2], "extensionHandshakes");
    assert!(!rows[3].expected.piece_requests.is_empty());
    assert!(!rows[3].expected.messages.is_empty());
    assert_empty_expected_except(&rows[4], "pieceReads");
    assert_empty_expected_except(&rows[5], "parser");
    assert_empty_expected_except(&rows[6], "hazards");

    let source = rows[0].expected.source.as_ref().expect("source row");
    assert_eq!(source.max_metadata_size, 10 * 1024 * 1024);
    assert_eq!(source.piece_size, 16 * 1024);
    assert_eq!(source.handshake_size, 68);
    assert_eq!(source.locally_advertised_ut_metadata_id, 1);
    assert_eq!(source.incoming_response_ut_metadata_id, 1);
    assert_eq!(
        (
            source.remote_ut_metadata_minimum,
            source.remote_ut_metadata_maximum
        ),
        (1, 254)
    );
    assert_eq!(source.advertised_extensions, ["DHT", "LTEP"]);
    assert_eq!(source.source_sha256.len(), 5);
    assert_eq!(source.dependency_sha256.len(), 2);
    assert_eq!(source.normalized_ast_sha256.len(), 12);
    assert_eq!(
        source.source_sha256.keys().collect::<Vec<_>>(),
        [
            "internal/protocol/id.go",
            "internal/protocol/infohash_v2.go",
            "internal/protocol/metainfo/metainfo.go",
            "internal/protocol/metainfo/metainforequester/requester.go",
            "internal/protocol/metainfo/parse.go",
        ]
    );
    for (path, bytes) in [
        (
            "internal/protocol/id.go",
            include_bytes!("../../../../internal/protocol/id.go").as_slice(),
        ),
        (
            "internal/protocol/infohash_v2.go",
            include_bytes!("../../../../internal/protocol/infohash_v2.go").as_slice(),
        ),
        (
            "internal/protocol/metainfo/metainfo.go",
            include_bytes!("../../../../internal/protocol/metainfo/metainfo.go").as_slice(),
        ),
        (
            "internal/protocol/metainfo/metainforequester/requester.go",
            include_bytes!("../../../../internal/protocol/metainfo/metainforequester/requester.go")
                .as_slice(),
        ),
        (
            "internal/protocol/metainfo/parse.go",
            include_bytes!("../../../../internal/protocol/metainfo/parse.go").as_slice(),
        ),
    ] {
        assert_eq!(source.source_sha256.get(path), Some(sha256(bytes).as_str()));
    }
    assert_eq!(
        source.dependency_sha256.get("go.mod"),
        Some(sha256(include_bytes!("../../../../go.mod")).as_str())
    );
    assert_eq!(
        source.dependency_sha256.get("go.sum"),
        Some(sha256(include_bytes!("../../../../go.sum")).as_str())
    );
    assert_eq!(source.dependency_lines, [
        "github.com/anacrolix/torrent v1.58.0",
        "github.com/anacrolix/torrent v1.58.0 h1:cZGqEEEXYVXKIwnPfS56udd2BRaCH2iMPpct6Ao+Z8U=",
        "github.com/anacrolix/torrent v1.58.0/go.mod h1:n3SjHIE8oHXeH0Px0d5FXQ7cU4IgbEfTroen6B9KWJk=",
    ]);
    assert_eq!(
        source.controlled_go_hazards,
        [
            "extension_handshake_write_error_is_ignored_due_to_named_error_check",
            "short_bt_handshake_write_panics",
            "unchecked_metadata_piece_index_panics",
            "duplicate_piece_bytes_can_complete_aggregate_with_a_hole",
        ]
    );
    assert_eq!(
        source.rust_hardening_allowed,
        [
            "propagate_extension_handshake_write_error",
            "complete_partial_writes_without_panicking_and_type_actual_write_failures",
            "validate_piece_index_and_piece_coverage",
            "track_unique_piece_completion_instead_of_aggregate_bytes",
        ]
    );
    let ast_keys = [
        "metainfo.ParseMetaInfoBytes",
        "requester.NewPeerExtensionBits",
        "requester.PeerExtensionBits.GetBit",
        "requester.PeerExtensionBits.WithBit",
        "requester.btHandshake",
        "requester.exHandshake",
        "requester.readAllPieces",
        "requester.readExMessage",
        "requester.readMessage",
        "requester.readUmMessage",
        "requester.requestAllPieces",
        "requester.uintToBigEndian4",
    ];
    assert_eq!(
        source.normalized_ast_sha256.keys().collect::<Vec<_>>(),
        ast_keys
    );
    for (key, digest) in [
        (
            "metainfo.ParseMetaInfoBytes",
            "4de434e83335941b1db217f8cade3a09c7c01df133555f085baf70ad616f9b8b",
        ),
        (
            "requester.NewPeerExtensionBits",
            "232e27fb1211252b95bfa6b2066cf3e20a95d4f133e096cf013388e013eab1cf",
        ),
        (
            "requester.PeerExtensionBits.GetBit",
            "9e31be4aae704ce6c50d8a3ec144b1de65366842cf0173ffa1f9ce41a142e348",
        ),
        (
            "requester.PeerExtensionBits.WithBit",
            "f683b6610a2021d883f32985c661d471656e5dcf7da87f0dca3f02a7ae9ec515",
        ),
        (
            "requester.btHandshake",
            "7705b2ffd854a31cbd60761c89b97368cef037129c23110237a8d41ab60b2671",
        ),
        (
            "requester.exHandshake",
            "27f8b3d3f605c0e15c8792ebe65efe8c8ca0594ac0b86910e776251b36e97d21",
        ),
        (
            "requester.readAllPieces",
            "bf830b0788b0978cfb0ed1608ce5df59adce95615b0a4f7d4c20d3828d362363",
        ),
        (
            "requester.readExMessage",
            "88bede931c9c6d1d6799dfc405aae4367be571b26bbbf77a517fc1517131c190",
        ),
        (
            "requester.readMessage",
            "ccf0fe7dfb35db8da739586f26307b3bf2fc6d17937baa620751c1c15c458517",
        ),
        (
            "requester.readUmMessage",
            "feaef06a7dd1a2180b495d92ef7628c0b920680eb34606e810fbc94694f8bcff",
        ),
        (
            "requester.requestAllPieces",
            "f149e359a6f0d9d67fc57165d342b4b98a62811d7e0275e1e43ee25f9f957d8d",
        ),
        (
            "requester.uintToBigEndian4",
            "796958f9c8a84720c8b33053eeb93d377ad354715005306316ec2efa75723a9c",
        ),
    ] {
        assert_eq!(source.normalized_ast_sha256.get(key), Some(digest));
    }
}

#[test]
fn recursively_strict_schema_duplicate_maps_and_presence_are_enforced() {
    let mut row: Value =
        serde_json::from_str(FIXTURE_TEXT.lines().next().expect("row")).expect("JSON");
    row.as_object_mut()
        .expect("root")
        .insert("unknown".into(), Value::Bool(true));
    assert!(serde_json::from_value::<Fixture>(row).is_err());

    let nested_mutations = ["/oracle", "/input", "/expected/source"];
    for pointer in nested_mutations {
        let mut row: Value =
            serde_json::from_str(FIXTURE_TEXT.lines().next().expect("row")).expect("JSON");
        row.pointer_mut(pointer)
            .expect("pointer")
            .as_object_mut()
            .expect("object")
            .insert("unknown".into(), Value::Bool(true));
        assert!(serde_json::from_value::<Fixture>(row).is_err(), "{pointer}");
    }
    let runtime: Value =
        serde_json::from_str(FIXTURE_TEXT.lines().nth(4).expect("piece row")).expect("JSON");
    for pointer in [
        "/expected/pieceReads/0",
        "/expected/pieceReads/0/inputPatterns/0",
    ] {
        let mut row = runtime.clone();
        row.pointer_mut(pointer)
            .expect("pointer")
            .as_object_mut()
            .expect("object")
            .insert("unknown".into(), Value::Bool(true));
        assert!(serde_json::from_value::<Fixture>(row).is_err(), "{pointer}");
    }
    let duplicate_struct = FIXTURE_TEXT.lines().next().expect("row").replacen(
        "\"id\":\"source_contract\"",
        "\"id\":\"source_contract\",\"id\":\"source_contract\"",
        1,
    );
    assert!(serde_json::from_str::<Fixture>(&duplicate_struct).is_err());
    let duplicate_map = FIXTURE_TEXT.lines().next().expect("row").replacen(
        "\"go.mod\":\"bdeb8dff1aa2ee347af6d84614f8c4f79c15c75edb76d991c97150706385d71c\"",
        "\"go.mod\":\"bdeb8dff1aa2ee347af6d84614f8c4f79c15c75edb76d991c97150706385d71c\",\"go.mod\":\"bdeb8dff1aa2ee347af6d84614f8c4f79c15c75edb76d991c97150706385d71c\"",
        1,
    );
    assert!(serde_json::from_str::<Fixture>(&duplicate_map).is_err());

    for line in FIXTURE_TEXT.lines() {
        let raw: Value = serde_json::from_str(line).expect("raw row");
        let root_keys = raw
            .as_object()
            .expect("root")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        assert_eq!(root_keys.len(), 8);
        let expected = raw
            .pointer("/expected")
            .expect("expected")
            .as_object()
            .expect("object");
        assert_eq!(expected.len(), 9);
        for key in [
            "source",
            "extensionBits",
            "handshakes",
            "extensionHandshakes",
            "pieceRequests",
            "messages",
            "pieceReads",
            "parser",
            "hazards",
        ] {
            assert!(expected.contains_key(key), "missing explicit key {key}");
        }
    }
    let extension_raw: Value =
        serde_json::from_str(FIXTURE_TEXT.lines().nth(2).expect("extension row")).expect("JSON");
    for row in extension_raw
        .pointer("/expected/extensionHandshakes")
        .expect("extension rows")
        .as_array()
        .expect("array")
    {
        let row = row.as_object().expect("extension object");
        assert!(row.contains_key("metadataSizeInput"));
        assert!(row.contains_key("utMetadataInput"));
        assert_eq!(row.len(), 15);
    }
    let parser_raw: Value =
        serde_json::from_str(FIXTURE_TEXT.lines().nth(5).expect("parser row")).expect("JSON");
    let parser = parser_raw
        .pointer("/expected/parser")
        .expect("parser")
        .as_object()
        .expect("parser object");
    assert!(parser.contains_key("infoHashV2"));
    assert!(parser["infoHashV2"].is_null());
    assert_eq!(parser.len(), 11);

    for key in ["source", "extensionBits", "parser"] {
        let mut row: Value =
            serde_json::from_str(FIXTURE_TEXT.lines().next().expect("source row")).expect("JSON");
        row.pointer_mut("/expected")
            .expect("expected")
            .as_object_mut()
            .expect("object")
            .remove(key)
            .expect("required nullable key");
        assert!(
            decode_fixture_line(&serde_json::to_string(&row).expect("JSON")).is_err(),
            "missing expected.{key} was accepted"
        );
    }
    for key in ["metadataSizeInput", "utMetadataInput"] {
        let mut row: Value = serde_json::from_str(
            FIXTURE_TEXT
                .lines()
                .nth(2)
                .expect("extension handshake row"),
        )
        .expect("JSON");
        row.pointer_mut("/expected/extensionHandshakes/0")
            .expect("extension handshake")
            .as_object_mut()
            .expect("object")
            .remove(key)
            .expect("required nullable key");
        assert!(
            decode_fixture_line(&serde_json::to_string(&row).expect("JSON")).is_err(),
            "missing extension handshake {key} was accepted"
        );
    }
    let mut row: Value =
        serde_json::from_str(FIXTURE_TEXT.lines().nth(5).expect("parser runtime row"))
            .expect("JSON");
    row.pointer_mut("/expected/parser")
        .expect("parser")
        .as_object_mut()
        .expect("object")
        .remove("infoHashV2")
        .expect("required nullable key");
    assert!(
        decode_fixture_line(&serde_json::to_string(&row).expect("JSON")).is_err(),
        "missing parser.infoHashV2 was accepted"
    );
}

async fn replay_handshake(
    response: Vec<u8>,
    info_hash: Id20,
    client_id: Id20,
) -> (Result<[u8; 20], DhtPeerWireMetaInfoRequesterError>, Vec<u8>) {
    let (mut client, mut server) = tokio::io::duplex(256);
    let server_task = tokio::spawn(async move {
        server.write_all(&response).await.expect("script response");
        server.shutdown().await.expect("script EOF");
        let mut output = Vec::new();
        server
            .read_to_end(&mut output)
            .await
            .expect("script request");
        output
    });
    let result =
        perform_bit_torrent_handshake(&mut client, test_peer(), info_hash, client_id).await;
    drop(client);
    (result, server_task.await.expect("script task"))
}

async fn replay_extension_handshake(
    response: Vec<u8>,
) -> (
    Result<ExtensionHandshake, DhtPeerWireMetaInfoRequesterError>,
    Vec<u8>,
) {
    let (mut client, mut server) = tokio::io::duplex(response.len().max(256));
    let server_task = tokio::spawn(async move {
        server.write_all(&response).await.expect("script response");
        server.shutdown().await.expect("script EOF");
        let mut output = Vec::new();
        server
            .read_to_end(&mut output)
            .await
            .expect("script request");
        output
    });
    let result = perform_extension_handshake(&mut client, test_peer()).await;
    drop(client);
    (result, server_task.await.expect("script task"))
}

#[tokio::test]
async fn handshake_and_extension_handshake_rows_replay_exact_wire_and_boundaries() {
    let rows = fixtures();
    let handshake_row = &rows[1];
    let info_hash = id20(&handshake_row.input.info_hash);
    let client_id = id20(&handshake_row.input.client_id);
    let peer_id = id20(&handshake_row.input.peer_id);
    let bits = handshake_row
        .expected
        .extension_bits
        .as_ref()
        .expect("bits");
    assert_eq!(bits.replay_disposition, "MUST_MATCH");
    assert_eq!((bits.dht_bit, bits.ltep_bit), (0, 20));
    assert_eq!(bits.dht_only_hex, "0000000000000001");
    assert_eq!(bits.ltep_only_hex, "0000000000100000");
    assert_eq!(hex_bytes(&bits.advertised_hex), ADVERTISED_EXTENSION_BITS);
    assert!(bits.dht_enabled && bits.ltep_enabled);
    assert_eq!(bits.round_trip_disable_dht_hex, bits.ltep_only_hex);

    assert_eq!(handshake_row.expected.handshakes.len(), 6);
    for row in &handshake_row.expected.handshakes {
        assert_eq!(row.replay_disposition, "MUST_MATCH");
        assert_eq!(row.write_calls, 1);
        assert_eq!(row.attempted_bytes, HANDSHAKE_SIZE as u64);
        assert_eq!(
            row.attempted_request_hex,
            hex_string(handshake_request(info_hash, client_id))
        );
        assert_eq!(
            row.reported_written_bytes,
            if row.label == "write_error" { 0 } else { 68 }
        );
        assert_eq!(row.error_identity_preserved, row.label == "write_error");
        let response = hex_bytes(&row.response_wire_hex);
        if row.label == "write_error" {
            assert_eq!(row.error, "handshake write sentinel");
            let (mut stream, identity, attempted) =
                failing_write_stream("handshake write sentinel");
            let error =
                perform_bit_torrent_handshake(&mut stream, test_peer(), info_hash, client_id)
                    .await
                    .expect_err("Rust propagates the fixture-driven write error");
            assert_write_error(
                error,
                DhtPeerWireMetaInfoRequesterStage::BitTorrentHandshakeWrite,
                &row.error,
                &identity,
            );
            assert_eq!(
                *attempted.lock().expect("attempted write lock"),
                hex_bytes(&row.attempted_request_hex)
            );
            continue;
        }
        let (actual, attempted) = replay_handshake(response, info_hash, client_id).await;
        assert_eq!(attempted, handshake_request(info_hash, client_id));
        match row.label.as_str() {
            "valid_exact_68_bytes" => {
                assert_eq!(actual.expect("valid handshake"), *peer_id.as_bytes());
                assert_eq!(row.peer_id, peer_id.to_hex());
                assert_eq!(row.peer_extension_bits_hex, "0000000000100081");
                assert!(row.error.is_empty());
            }
            "invalid_protocol" => {
                assert!(matches!(
                    actual,
                    Err(DhtPeerWireMetaInfoRequesterError::InvalidHandshakeProtocol)
                ));
                assert_eq!(row.error, "invalid handshake response received");
            }
            "peer_without_ltep" => {
                assert!(matches!(
                    actual,
                    Err(DhtPeerWireMetaInfoRequesterError::ExtensionProtocolUnsupported)
                ));
                assert_eq!(row.error, "peer does not support the extension protocol");
            }
            "infohash_mismatch" => {
                assert!(matches!(
                    actual,
                    Err(DhtPeerWireMetaInfoRequesterError::InfoHashMismatch)
                ));
                assert_eq!(row.error, "infohash mismatch");
            }
            "short_response" => {
                let DhtPeerWireMetaInfoRequesterError::Io {
                    peer,
                    stage,
                    source,
                } = actual.expect_err("short response")
                else {
                    panic!("short response returned wrong error");
                };
                assert_eq!(peer, test_peer());
                assert_eq!(
                    stage,
                    DhtPeerWireMetaInfoRequesterStage::BitTorrentHandshakeRead
                );
                assert_eq!(source.kind(), io::ErrorKind::UnexpectedEof);
                assert_eq!(row.error, "failed to read all handshake bytes (67): unexpected EOF / 000102030405060708090a0b0c0d0e0f10111213");
            }
            other => panic!("unknown handshake case {other}"),
        }
    }

    let extension_row = &rows[2];
    assert_eq!(extension_row.expected.extension_handshakes.len(), 8);
    for row in &extension_row.expected.extension_handshakes {
        assert_eq!(
            row.attempted_advertised_request_hex,
            hex_string(EXTENSION_HANDSHAKE_REQUEST)
        );
        assert_eq!(row.write_calls, 1);
        assert_eq!(
            row.attempted_bytes,
            EXTENSION_HANDSHAKE_REQUEST.len() as u64
        );
        assert_eq!(
            row.reported_written_bytes,
            if row.write_error_injected { 0 } else { 30 }
        );
        if row.write_error_injected {
            assert_eq!(row.replay_disposition, "GO_HAZARD_RUST_HARDENING");
            assert!(row.write_error_ignored);
        } else {
            assert_eq!(row.replay_disposition, "MUST_MATCH");
            assert!(!row.write_error_ignored);
        }
        let wire = hex_bytes(&row.response_wire_hex);
        let ignored = row
            .ignored_frame_hex
            .iter()
            .map(|frame| hex_bytes(frame))
            .collect::<Vec<_>>()
            .concat();
        assert!(wire.starts_with(&ignored));
        let final_frame = &wire[ignored.len()..];
        let body_length = u32::from_be_bytes(final_frame[..4].try_into().expect("length")) as usize;
        assert_eq!(body_length + 4, final_frame.len());
        let final_extension_id = final_frame[5];
        if row.write_error_injected {
            assert_eq!(row.label, "write_error_is_ignored");
            assert!(row.error.is_empty());
            let (mut stream, identity, attempted) =
                failing_write_stream("extension handshake write sentinel");
            let error = perform_extension_handshake(&mut stream, test_peer())
                .await
                .expect_err("Rust hardening propagates extension write error");
            assert_write_error(
                error,
                DhtPeerWireMetaInfoRequesterStage::ExtensionHandshakeWrite,
                "extension handshake write sentinel",
                &identity,
            );
            assert_eq!(
                *attempted.lock().expect("attempted write lock"),
                hex_bytes(&row.attempted_advertised_request_hex)
            );
            continue;
        }
        let (actual, advertised) = replay_extension_handshake(wire).await;
        assert_eq!(advertised, EXTENSION_HANDSHAKE_REQUEST);
        match row.label.as_str() {
            "minimum_values_with_ignored_nonextension_frames"
            | "maximum_accepted_values"
            | "write_error_is_ignored" => {
                let actual = actual.expect("accepted extension handshake");
                assert_eq!(actual.metadata_size as u64, row.metadata_size);
                assert_eq!(actual.remote_ut_metadata_id as u64, row.ut_metadata);
                assert_eq!(row.metadata_size_input, Some(actual.metadata_size as i64));
                assert_eq!(
                    row.ut_metadata_input,
                    Some(i64::from(actual.remote_ut_metadata_id))
                );
                assert!(row.error.is_empty());
            }
            "zero_metadata_size" | "maximum_metadata_size_is_exclusive" => {
                assert!(matches!(
                    actual,
                    Err(DhtPeerWireMetaInfoRequesterError::InvalidMetadataSize(value))
                        if Some(value) == row.metadata_size_input
                ));
                assert_eq!(
                    row.error,
                    "metadata too big or its size is less than or equal zero"
                );
            }
            "zero_ut_metadata" | "ut_metadata_255_is_exclusive" => {
                assert!(matches!(
                    actual,
                    Err(DhtPeerWireMetaInfoRequesterError::InvalidRemoteUtMetadataId(value))
                        if Some(value) == row.ut_metadata_input
                ));
                assert_eq!(row.error, "ut_metadata is not an uint8");
            }
            "first_extension_message_not_handshake" => {
                assert!(matches!(
                    actual,
                    Err(
                        DhtPeerWireMetaInfoRequesterError::FirstExtensionMessageNotHandshake {
                            actual
                        }
                    ) if actual == final_extension_id
                ));
                assert_eq!(
                    row.error,
                    "first extension message is not an extension handshake"
                );
            }
            other => panic!("unknown extension case {other}"),
        }
        assert_eq!(
            row.metadata_size_input.is_none(),
            row.label == "first_extension_message_not_handshake"
        );
        assert_eq!(
            row.ut_metadata_input.is_none(),
            row.label == "first_extension_message_not_handshake"
        );
        for ignored in &row.ignored_frame_hex {
            assert!(!hex_bytes(ignored).is_empty());
        }
    }
}

#[tokio::test]
async fn piece_request_message_and_piece_reader_rows_replay() {
    let rows = fixtures();
    let request_row = &rows[3];
    assert_eq!(request_row.expected.piece_requests.len(), 3);
    for row in &request_row.expected.piece_requests {
        assert_eq!(row.replay_disposition, "MUST_MATCH");
        assert!(row.error.is_empty());
        let (mut client, mut server) = tokio::io::duplex(256);
        request_all_pieces(
            &mut client,
            test_peer(),
            row.metadata_size as usize,
            row.ut_metadata as u8,
        )
        .await
        .expect("piece requests");
        client.shutdown().await.expect("request EOF");
        let mut actual = Vec::new();
        server
            .read_to_end(&mut actual)
            .await
            .expect("request bytes");
        assert_eq!(actual, hex_bytes(&row.combined_hex), "{}", row.label);
        assert_eq!(sha256(&actual), row.combined_sha256);
        assert_eq!(
            row.frames_hex
                .iter()
                .map(|frame| hex_bytes(frame))
                .collect::<Vec<_>>()
                .concat(),
            actual
        );
        assert_eq!(row.piece_count as usize, row.frames_hex.len());
    }
    assert_eq!(
        request_row.expected.piece_requests[0].frames_hex[0],
        "0000001b140164383a6d73675f74797065693065353a706965636569306565"
    );
    assert_eq!(
        request_row.expected.piece_requests[2].frames_hex,
        [
            "0000001b14fe64383a6d73675f74797065693065353a706965636569306565",
            "0000001b14fe64383a6d73675f74797065693065353a706965636569316565",
        ]
    );

    assert_eq!(request_row.expected.messages.len(), 2);
    for row in &request_row.expected.messages {
        assert_eq!(row.replay_disposition, "MUST_MATCH");
        assert!(!row.label.is_empty());
        let pattern = hex_bytes(&row.payload_pattern_byte_hex);
        assert_eq!(pattern.len(), usize::from(row.payload_pattern_length != 0));
        let payload = if let Some(byte) = pattern.first() {
            vec![*byte; row.payload_pattern_length as usize]
        } else {
            Vec::new()
        };
        if payload.is_empty() {
            assert!(row.payload_sha256.is_empty());
        } else {
            assert_eq!(sha256(&payload), row.payload_sha256);
        }
        let mut wire = (row.declared_length as u32).to_be_bytes().to_vec();
        wire.extend_from_slice(&payload);
        let (mut reader, mut writer) = tokio::io::duplex(wire.len().max(1));
        let task =
            tokio::spawn(async move { writer.write_all(&wire).await.expect("message wire") });
        let actual = read_message(&mut reader, test_peer()).await;
        task.await.expect("writer");
        if row.returned {
            let actual = actual.expect("maximum accepted");
            assert!(!row.returned_is_nil);
            assert_eq!(actual.len() as u64, row.returned_length);
            assert_eq!(sha256(&actual), row.returned_sha256);
            assert!(row.error.is_empty());
        } else {
            assert!(row.returned_is_nil);
            assert!(matches!(
                actual,
                Err(DhtPeerWireMetaInfoRequesterError::MessageTooLong { length })
                    if length as u64 == row.declared_length
            ));
            assert_eq!(
                row.error,
                "message is longer than max allowed metadata size"
            );
        }
    }

    let piece_row = &rows[4];
    assert_eq!(piece_row.expected.piece_reads.len(), 6);
    for row in &piece_row.expected.piece_reads {
        assert_eq!(row.replay_disposition, "MUST_MATCH");
        let wire = piece_read_wire(row);
        let (mut reader, mut writer) = tokio::io::duplex(wire.len().max(1));
        let task = tokio::spawn(async move { writer.write_all(&wire).await.expect("piece wire") });
        let actual = read_all_pieces(&mut reader, test_peer(), row.metadata_size as usize).await;
        task.await.expect("writer");
        if row.returned {
            let actual = actual.expect("Go and Rust accepted piece sequence");
            assert_eq!(actual.len() as u64, row.returned_length);
            assert_eq!(sha256(&actual), row.returned_sha256);
            assert!(actual.starts_with(&hex_bytes(&row.returned_prefix_hex)));
            assert!(actual.ends_with(&hex_bytes(&row.returned_suffix_hex)));
            assert!(row.error.is_empty());
        } else {
            let error = actual.expect_err("both implementations reject malformed sequence");
            match row.label.as_str() {
                "remote_reject" => {
                    assert!(matches!(
                        error,
                        DhtPeerWireMetaInfoRequesterError::MetadataRejected { piece: 0 }
                    ));
                    assert_eq!(row.error, "remote peer rejected sending metadataBytes");
                }
                "oversized_piece" => {
                    assert!(matches!(
                        error,
                        DhtPeerWireMetaInfoRequesterError::InvalidPieceLength {
                            piece: 0,
                            actual: 16_385,
                            expected: 16_384,
                        }
                    ));
                    assert_eq!(row.error, "metadataPiece > 16kiB");
                }
                "short_incomplete_piece" => {
                    assert!(matches!(
                        error,
                        DhtPeerWireMetaInfoRequesterError::InvalidPieceLength {
                            piece: 0,
                            actual: 1,
                            expected: 16_384,
                        }
                    ));
                    assert_eq!(row.error, "metadataPiece < 16 kiB but incomplete");
                }
                "aggregate_size_overflow" => {
                    assert!(matches!(
                        error,
                        DhtPeerWireMetaInfoRequesterError::InvalidPieceLength {
                            piece: 1,
                            actual: 16_384,
                            expected: 1,
                        }
                    ));
                    assert_eq!(row.error, "receivedSize > metadataSize");
                }
                other => panic!("unexpected rejecting case {other}"),
            }
        }
    }
}

#[test]
fn parser_row_replays_requested_hash_and_exact_identity() {
    let rows = fixtures();
    let row = &rows[5];
    let expected = row.expected.parser.as_ref().expect("parser row");
    assert_eq!(expected.replay_disposition, "MUST_MATCH");
    assert_eq!(row.input.info_hash, expected.requested_info_hash);
    let raw = hex_bytes(&expected.raw_info_hex);
    assert_eq!(sha256(&raw), expected.raw_info_sha256);
    let parsed =
        bitmagnet_metainfo::parse_info_bytes(*id20(&expected.requested_info_hash).as_bytes(), &raw)
            .expect("requested identity");
    assert_eq!(
        u64::from(parsed.meta_version().as_u8()),
        expected.meta_version
    );
    assert_eq!(
        parsed.info_hash_v1().map(hex_string).as_deref(),
        Some(expected.info_hash_v1.as_str())
    );
    assert_eq!(parsed.info_hash_v2().map(hex_string), expected.info_hash_v2);
    assert_eq!(parsed.info().name(), expected.name.as_bytes());
    assert_eq!(parsed.info().length(), expected.length);
    let error = bitmagnet_metainfo::parse_info_bytes(
        *id20(&expected.wrong_requested_hash).as_bytes(),
        &raw,
    )
    .expect_err("wrong requested hash");
    assert_eq!(error.to_string(), expected.wrong_hash_error);
}

#[tokio::test]
async fn controlled_go_hazards_are_pinned_and_rust_hardenings_reject_corruption() {
    let rows = fixtures();
    let row = &rows[6];
    assert_eq!(row.expected.hazards.len(), 3);
    for hazard in &row.expected.hazards {
        assert_eq!(hazard.replay_disposition, "GO_HAZARD_RUST_HARDENING");
        assert!(hazard.rust_may_reject);
        match hazard.label.as_str() {
            "short_handshake_write_panics" => {
                assert!(hazard.panic_observed);
                assert_eq!(hazard.panic_class, "literal_panic");
                assert_eq!(hazard.panic_type, "string");
                assert_eq!(hazard.panic_text, "handshake bytes must have length 68");
                assert!(hazard.harness_contract_violation);
                assert_eq!(hazard.reported_written_bytes, 67);
                assert_eq!(hex_bytes(&hazard.attempted_wire_hex).len(), HANDSHAKE_SIZE);

                let info_hash = id20(&row.input.info_hash);
                let client_id = id20(&row.input.client_id);
                let peer_id = id20(&row.input.peer_id);
                let response = handshake_request(info_hash, peer_id);
                let (mut client, mut server) = tokio::io::duplex(1);
                let server_task = tokio::spawn(async move {
                    let mut request = [0; HANDSHAKE_SIZE];
                    server
                        .read_exact(&mut request)
                        .await
                        .expect("fragmented request");
                    server
                        .write_all(&response)
                        .await
                        .expect("fragmented response");
                    request
                });
                assert_eq!(
                    perform_bit_torrent_handshake(&mut client, test_peer(), info_hash, client_id)
                        .await
                        .expect("write_all completes partial readiness"),
                    *peer_id.as_bytes()
                );
                assert_eq!(
                    server_task.await.expect("server"),
                    handshake_request(info_hash, client_id)
                );
            }
            "unchecked_positive_piece_index_panics" => {
                assert!(hazard.panic_observed);
                assert_eq!(hazard.panic_class, "slice_bounds_out_of_range");
                assert!(hazard.panic_type.is_empty() && hazard.panic_text.is_empty());
                let wire = hazard
                    .input_patterns
                    .iter()
                    .map(frame_pattern)
                    .collect::<Vec<_>>()
                    .concat();
                assert_eq!(wire.len() as u64, hazard.input_byte_length);
                assert_eq!(sha256(&wire), hazard.input_sha256);
                let (mut reader, mut writer) = tokio::io::duplex(wire.len());
                writer.write_all(&wire).await.expect("hazard wire");
                assert!(matches!(
                    read_all_pieces(&mut reader, test_peer(), hazard.metadata_size as usize).await,
                    Err(DhtPeerWireMetaInfoRequesterError::InvalidPieceIndex {
                        piece: 1,
                        piece_count: 1,
                    })
                ));
            }
            "duplicate_piece_aggregate_completion_leaves_hole" => {
                assert!(!hazard.panic_observed);
                assert!(hazard.returned && hazard.hole_all_zero);
                assert_eq!(
                    (
                        hazard.duplicate_aggregate_count,
                        hazard.distinct_piece_indexes
                    ),
                    (2, 1)
                );
                assert_eq!((hazard.hole_offset, hazard.hole_length), (16_384, 16_384));
                let wire = hazard
                    .input_patterns
                    .iter()
                    .map(frame_pattern)
                    .collect::<Vec<_>>()
                    .concat();
                assert_eq!(wire.len() as u64, hazard.input_byte_length);
                assert_eq!(sha256(&wire), hazard.input_sha256);
                assert_eq!(hazard.returned_length, hazard.metadata_size);
                assert!(!hazard.returned_sha256.is_empty());
                let (mut reader, mut writer) = tokio::io::duplex(wire.len());
                writer.write_all(&wire).await.expect("hazard wire");
                assert!(matches!(
                    read_all_pieces(&mut reader, test_peer(), hazard.metadata_size as usize).await,
                    Err(DhtPeerWireMetaInfoRequesterError::DuplicatePiece { piece: 0 })
                ));
            }
            other => panic!("unknown hazard {other}"),
        }
    }
}
