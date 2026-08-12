//! Go-generated parity for BEP-33 bloom arithmetic and KRPC presence.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use bitmagnet_dht::{ByteString, Id20, KrpcMessage, MessageReturn, ScrapeBloomFilter};
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    id: String,
    subsystem: String,
    input: serde_json::Value,
    expected: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScrapeRange {
    base: ByteString,
    count: usize,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FilterInput {
    #[serde(default)]
    raw_ips: Vec<ByteString>,
    #[serde(default)]
    ranges: Vec<ScrapeRange>,
}

#[derive(Deserialize)]
struct ScrapeInput {
    #[serde(default)]
    seeders: Option<FilterInput>,
    #[serde(default)]
    peers: Option<FilterInput>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FilterExpected {
    bloom_hex: ScrapeBloomFilter,
    estimate_count: f64,
    approximated_size: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScrapeExpected {
    wire_hex: String,
    #[serde(default)]
    seeders: Option<FilterExpected>,
    #[serde(default)]
    peers: Option<FilterExpected>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CompatibilityInput {
    wire_hex: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CompatibilityExpected {
    go_accepted: bool,
    rust_accepted: bool,
    #[serde(default)]
    go_canonical_wire_hex: String,
    reason: String,
}

fn fixtures() -> Vec<Fixture> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht/scrape_bloom.jsonl");
    let file = File::open(&path).unwrap_or_else(|error| panic!("open {}: {error}", path.display()));
    BufReader::new(file)
        .lines()
        .map(|line| line.expect("read scrape fixture line"))
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(&line).expect("decode scrape fixture"))
        .collect()
}

#[test]
fn scrape_blooms_and_krpc_fields_match_go() {
    let fixtures = fixtures();
    let typed = fixtures
        .iter()
        .filter(|fixture| !fixture.id.starts_with("compat_"))
        .collect::<Vec<_>>();
    assert_eq!(typed.len(), 9, "expected nine typed scrape cases");

    let four_byte = expected_filter(&typed, "single_ipv4_4_byte");
    let mapped = expected_filter(&typed, "single_ipv4_mapped_16_byte");
    assert_ne!(
        four_byte, mapped,
        "Go hashes the raw four-byte and mapped sixteen-byte IPv4 differently"
    );

    for fixture in typed {
        assert_eq!(fixture.subsystem, "dht_scrape_bloom");
        let input: ScrapeInput = serde_json::from_value(fixture.input.clone())
            .unwrap_or_else(|error| panic!("[{}] decode input: {error}", fixture.id));
        let expected: ScrapeExpected = serde_json::from_value(fixture.expected.clone())
            .unwrap_or_else(|error| panic!("[{}] decode expected: {error}", fixture.id));
        let seeders = input.seeders.map(build_filter);
        let peers = input.peers.map(build_filter);
        assert_filter(fixture, seeders, expected.seeders);
        assert_filter(fixture, peers, expected.peers);

        let message = KrpcMessage {
            transaction_id: ByteString::new(b"bs"),
            message_type: ByteString::new(b"r"),
            query: ByteString::default(),
            args: None,
            response: Some(MessageReturn {
                id: Id20::from_slice(&[0x21; 20]).expect("fixed response ID"),
                nodes: None,
                nodes6: None,
                token: None,
                values: None,
                interval: None,
                num: None,
                samples: None,
                seeders_bloom: seeders,
                peers_bloom: peers,
            }),
            error: None,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        };
        let wire = message
            .encode()
            .unwrap_or_else(|error| panic!("[{}] encode: {error}", fixture.id));
        assert_eq!(hex::encode(&wire), expected.wire_hex, "[{}]", fixture.id);
        assert_eq!(
            KrpcMessage::decode(&wire).expect("decode exact-width scrape wire"),
            message,
            "[{}] exact KRPC round trip",
            fixture.id
        );
    }
}

#[test]
fn wrong_width_compatibility_delta_is_explicit() {
    let fixtures = fixtures();
    let compatibility = fixtures
        .iter()
        .filter(|fixture| fixture.id.starts_with("compat_"))
        .collect::<Vec<_>>();
    assert_eq!(compatibility.len(), 4, "expected four compatibility cases");

    for fixture in compatibility {
        let input: CompatibilityInput = serde_json::from_value(fixture.input.clone())
            .unwrap_or_else(|error| panic!("[{}] decode input: {error}", fixture.id));
        let expected: CompatibilityExpected = serde_json::from_value(fixture.expected.clone())
            .unwrap_or_else(|error| panic!("[{}] decode expected: {error}", fixture.id));
        let wire = hex::decode(input.wire_hex).expect("fixture wire is hexadecimal");
        assert_eq!(
            KrpcMessage::decode(&wire).is_ok(),
            expected.rust_accepted,
            "[{}] {}",
            fixture.id,
            expected.reason
        );
        if expected.go_accepted {
            assert!(fixture.id.contains("width"), "only width differs from Go");
            assert!(
                !expected.go_canonical_wire_hex.is_empty(),
                "Go-accepted wire must record canonical bytes"
            );
        } else {
            assert_eq!(fixture.id, "compat_wrong_type");
        }
    }
}

fn build_filter(input: FilterInput) -> ScrapeBloomFilter {
    let mut filter = ScrapeBloomFilter::default();
    for ip in input.raw_ips {
        filter.add_ip_bytes(ip.as_bytes());
    }
    for value_range in input.ranges {
        for offset in 0..value_range.count {
            let ip = add_big_endian(value_range.base.as_bytes(), offset);
            filter.add_ip_bytes(&ip);
        }
    }
    filter
}

fn add_big_endian(base: &[u8], offset: usize) -> Vec<u8> {
    let mut result = base.to_vec();
    let mut carry = offset;
    for byte in result.iter_mut().rev() {
        if carry == 0 {
            break;
        }
        let sum = usize::from(*byte) + carry;
        *byte = sum as u8;
        carry = sum >> 8;
    }
    assert_eq!(carry, 0, "scrape range overflows its address width");
    result
}

fn assert_filter(
    fixture: &Fixture,
    actual: Option<ScrapeBloomFilter>,
    expected: Option<FilterExpected>,
) {
    match (actual, expected) {
        (None, None) => {}
        (Some(actual), Some(expected)) => {
            assert_eq!(actual, expected.bloom_hex, "[{}] bloom bytes", fixture.id);
            assert_eq!(
                actual.approximated_size(),
                expected.approximated_size,
                "[{}] persistence estimate",
                fixture.id
            );
            assert!(
                (actual.estimate_count() - expected.estimate_count).abs() <= 1e-10,
                "[{}] floating estimate: Rust={} Go={}",
                fixture.id,
                actual.estimate_count(),
                expected.estimate_count
            );
        }
        _ => panic!("[{}] filter presence differs", fixture.id),
    }
}

fn expected_filter(fixtures: &[&Fixture], id: &str) -> ScrapeBloomFilter {
    let fixture = fixtures
        .iter()
        .find(|fixture| fixture.id == id)
        .unwrap_or_else(|| panic!("missing typed scrape fixture {id}"));
    let expected: ScrapeExpected = serde_json::from_value(fixture.expected.clone())
        .unwrap_or_else(|error| panic!("[{id}] decode expected: {error}"));
    expected
        .seeders
        .unwrap_or_else(|| panic!("[{id}] missing expected seeder filter"))
        .bloom_hex
}
