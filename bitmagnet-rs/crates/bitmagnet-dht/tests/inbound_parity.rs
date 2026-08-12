//! Real-Go differential and adversarial gates for bounded inbound KRPC.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use bitmagnet_dht::{
    InboundError, KrpcMessage, MAX_INBOUND_DATAGRAM_BYTES, MAX_INBOUND_NESTING_DEPTH,
    MAX_INBOUND_VALUES,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    id: String,
    subsystem: String,
    input: InboundInput,
    expected: InboundExpected,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InboundInput {
    #[serde(default)]
    wire_hex: String,
    #[serde(default)]
    padding_datagram_bytes: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InboundExpected {
    go_accepted: bool,
    rust_accepted: bool,
    #[serde(default)]
    go_decoded: Option<serde_json::Value>,
    #[serde(default)]
    go_canonical_wire_hex: String,
    #[serde(default)]
    rust_projection_loss: bool,
    #[serde(default)]
    rust_error_class: String,
}

fn fixtures() -> Vec<Fixture> {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../testdata/parity/dht/inbound.jsonl");
    let file = File::open(&path).unwrap_or_else(|error| panic!("open {}: {error}", path.display()));
    BufReader::new(file)
        .lines()
        .map(|line| line.expect("read inbound fixture line"))
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(&line).expect("decode inbound fixture"))
        .collect()
}

fn fixture_wire(input: &InboundInput) -> Vec<u8> {
    if input.padding_datagram_bytes != 0 {
        return padding_datagram(input.padding_datagram_bytes);
    }
    hex::decode(&input.wire_hex).expect("fixture wire is hexadecimal")
}

fn padding_datagram(target: usize) -> Vec<u8> {
    let mut padding = target;
    loop {
        let header = format!("d1:t2:aa1:y1:q1:z{padding}:");
        let length = header.len() + padding + 1;
        match length.cmp(&target) {
            std::cmp::Ordering::Equal => {
                let mut wire = header.into_bytes();
                wire.extend(std::iter::repeat_n(b'x', padding));
                wire.push(b'e');
                return wire;
            }
            std::cmp::Ordering::Greater => padding -= length - target,
            std::cmp::Ordering::Less => padding += target - length,
        }
    }
}

fn error_class(error: &InboundError) -> &'static str {
    match error {
        InboundError::Limit { .. } => "limit",
        InboundError::Syntax { .. } => "syntax",
        InboundError::Shape { .. } => "shape",
        InboundError::Compact { .. } => "compact",
        InboundError::ScrapeBloom { .. } => "scrape_bloom",
        InboundError::Unsupported { .. } => "unsupported",
    }
}

#[test]
fn permissive_bounded_inbound_matches_real_go_decoder() {
    let fixtures = fixtures();
    assert_eq!(
        fixtures.len(),
        105,
        "expected one hundred five inbound cases"
    );
    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_krpc_inbound");
        let wire = fixture_wire(&fixture.input);
        let decoded = KrpcMessage::decode_inbound(&wire);
        assert_eq!(
            decoded.is_ok(),
            fixture.expected.rust_accepted,
            "[{}] Rust acceptance",
            fixture.id
        );
        match decoded {
            Ok(message) => {
                assert!(fixture.expected.go_accepted, "[{}] Go rejected", fixture.id);
                let go_decoded: KrpcMessage = serde_json::from_value(
                    fixture
                        .expected
                        .go_decoded
                        .clone()
                        .expect("shared acceptance has a Go projection"),
                )
                .unwrap_or_else(|error| panic!("[{}] decode Go projection: {error}", fixture.id));
                assert_eq!(
                    message, go_decoded,
                    "[{}] Go decoded projection",
                    fixture.id
                );
                if !fixture.expected.rust_projection_loss {
                    assert_eq!(
                        hex::encode(message.encode().expect("canonical encode")),
                        fixture.expected.go_canonical_wire_hex,
                        "[{}] Go canonical projection",
                        fixture.id
                    );
                }
            }
            Err(error) => assert_eq!(
                error_class(&error),
                fixture.expected.rust_error_class,
                "[{}] typed Rust error: {error}",
                fixture.id
            ),
        }
    }
}

#[test]
fn truncations_structural_mutations_and_limits_never_panic() {
    let fixture_wires = fixtures()
        .into_iter()
        .filter(|fixture| fixture.input.padding_datagram_bytes == 0)
        .map(|fixture| fixture_wire(&fixture.input))
        .collect::<Vec<_>>();

    for wire in &fixture_wires {
        assert!(std::panic::catch_unwind(|| KrpcMessage::decode_inbound(wire)).is_ok());
        for end in 0..wire.len() {
            let result = std::panic::catch_unwind(|| KrpcMessage::decode_inbound(&wire[..end]));
            assert!(result.is_ok(), "decoder panicked on truncation at {end}");
        }
        for suffix in [b"e".as_slice(), b"0:", b"i0e", b"\xff"] {
            let mut mutated = wire.clone();
            mutated.extend_from_slice(suffix);
            assert!(std::panic::catch_unwind(|| KrpcMessage::decode_inbound(&mutated)).is_ok());
        }
        for index in 0..wire.len() {
            let mut mutated = wire.clone();
            mutated[index] = 0xff;
            assert!(
                std::panic::catch_unwind(|| KrpcMessage::decode_inbound(&mutated)).is_ok(),
                "decoder panicked after mutation at {index}"
            );
        }
    }

    assert_eq!(MAX_INBOUND_DATAGRAM_BYTES, 65_507);
    assert_eq!(MAX_INBOUND_NESTING_DEPTH, 8);
    assert_eq!(MAX_INBOUND_VALUES, 32_768);
    assert!(KrpcMessage::decode_inbound(&padding_datagram(65_507)).is_ok());
    assert!(matches!(
        KrpcMessage::decode_inbound(&padding_datagram(65_508)),
        Err(InboundError::Limit { .. })
    ));
}
