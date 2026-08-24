//! Real-Go projection parity plus no-socket lifecycle gates for the typed
//! `get_peers`, BEP-33 scrape, and `sample_infohashes` client methods.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use bitmagnet_dht::{
    ByteString, CompactAddr, CompactNode, DatagramReceiver, DatagramSender, DeliveryOutcome,
    DhtClient, DhtClientError, GetPeersResult, GetPeersScrapeResult, Id20, KrpcError, KrpcMessage,
    MessageReturn, QuerySendError, ReceiveDispatchOutcome, ReceiveDispatcher, ReceivedDatagram,
    RoutingNode, SampleInfoHashesResult, ScrapeBloomFilter, TransactionId, TransactionIdIssuer,
    TransactionIdSourceError, TransactionRegistry,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    id: String,
    subsystem: String,
    runtime: FixtureRuntime,
    input: FixtureInput,
    expected: FixtureExpected,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureRuntime {
    int_bits: u8,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureInput {
    operation: String,
    transaction_id_hex: String,
    local_id: Id20,
    remote: FixtureAddr,
    info_hash: Option<Id20>,
    target: Option<Id20>,
    response: FixtureResponse,
    #[serde(default)]
    failure: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureResponse {
    id: Id20,
    nodes_presence: String,
    nodes: Option<Vec<FixtureNode>>,
    nodes6_presence: String,
    nodes6: Option<Vec<FixtureNode>>,
    values_presence: String,
    values: Option<Vec<FixtureAddr>>,
    token_presence: String,
    token_hex: String,
    samples_presence: String,
    samples: Option<Vec<Id20>>,
    num_presence: String,
    num: i64,
    interval_presence: String,
    interval: i64,
    peers_bloom_presence: String,
    peers_bloom_hex: String,
    seeders_bloom_presence: String,
    seeders_bloom_hex: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureExpected {
    query_calls: usize,
    query_method: String,
    query_remote: FixtureAddr,
    query_args: FixtureQueryArgs,
    query_wire_hex: String,
    outcome: String,
    error_text: String,
    error_identity_preserved: bool,
    error_is_typed_nil: bool,
    result_was_zero: bool,
    result: FixtureResult,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureQueryArgs {
    id: Id20,
    info_hash: Id20,
    target: Id20,
    token_hex: String,
    port_presence: String,
    implied_port: bool,
    want_presence: String,
    want: Vec<String>,
    no_seed: i64,
    scrape: i64,
    bep44_fields_are_zero: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureResult {
    id: Id20,
    nodes_presence: String,
    nodes: Option<Vec<FixtureNode>>,
    values_presence: String,
    values: Option<Vec<FixtureAddr>>,
    samples_presence: String,
    samples: Option<Vec<Id20>>,
    num: i64,
    interval: i64,
    peers_bloom: Option<FixtureBloom>,
    seeders_bloom: Option<FixtureBloom>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureBloom {
    bloom_hex: String,
    capacity: usize,
    hashes: usize,
    approximated_size: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
struct FixtureAddr {
    ip: IpAddr,
    port: u16,
    #[serde(default)]
    scope: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
struct FixtureNode {
    id: Id20,
    addr: FixtureAddr,
}

struct ScriptedIssuer(VecDeque<Result<TransactionId, TransactionIdSourceError>>);

impl TransactionIdIssuer for ScriptedIssuer {
    fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
        self.0
            .pop_front()
            .unwrap_or_else(|| Err(TransactionIdSourceError::new("scripted issuer exhausted")))
    }
}

#[derive(Clone, Debug)]
struct TransportSentinel(Arc<()>);

impl Display for TransportSentinel {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("peer/sample client transport sentinel")
    }
}

impl Error for TransportSentinel {}

struct FixtureSender<I> {
    registry: TransactionRegistry<I>,
    response: KrpcMessage,
    response_source: SocketAddr,
    transport_error: Option<TransportSentinel>,
    calls: usize,
    destinations: Vec<SocketAddr>,
    wires: Vec<Vec<u8>>,
}

impl<I> DatagramSender for FixtureSender<I>
where
    I: TransactionIdIssuer + 'static,
{
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        self.calls += 1;
        self.destinations.push(destination);
        self.wires.push(datagram.to_vec());
        let query = KrpcMessage::decode(datagram).expect("decode captured fixture query");
        self.response.transaction_id = query.transaction_id;
        if self.transport_error.is_none() {
            assert_eq!(
                self.registry
                    .deliver(self.response_source, self.response.clone()),
                DeliveryOutcome::Delivered
            );
        }
        let error = self.transport_error.clone();
        Box::pin(async move { error.map_or(Ok(()), Err) })
    }
}

fn fixtures() -> Vec<Fixture> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht/peer_sample_client.jsonl");
    BufReader::new(File::open(path).expect("open real-Go peer/sample fixture"))
        .lines()
        .map(|line| {
            serde_json::from_str(&line.expect("read real-Go peer/sample fixture line"))
                .expect("decode real-Go peer/sample fixture line")
        })
        .collect()
}

fn transaction_id(value: &str) -> TransactionId {
    TransactionId::from_slice(&hex::decode(value).expect("transaction ID is hex"))
        .expect("transaction ID has exact width")
}

fn fixture_addr(value: FixtureAddr) -> SocketAddr {
    match value.ip {
        IpAddr::V4(ip) => SocketAddr::V4(SocketAddrV4::new(ip, value.port)),
        IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(ip, value.port, 0, value.scope)),
    }
}

fn fixture_node(value: FixtureNode) -> CompactNode {
    CompactNode {
        id: value.id,
        addr: CompactAddr {
            ip: value.addr.ip,
            port: value.addr.port,
        },
    }
}

fn optional_nodes(presence: &str, values: Option<&[FixtureNode]>) -> Option<Vec<CompactNode>> {
    match presence {
        "nil" => {
            assert!(values.is_none());
            None
        }
        "empty" => {
            assert!(values.is_some_and(<[FixtureNode]>::is_empty));
            Some(Vec::new())
        }
        "present" => {
            let values = values.expect("present nodes have values");
            assert!(!values.is_empty());
            Some(values.iter().copied().map(fixture_node).collect())
        }
        other => panic!("unexpected node presence {other:?}"),
    }
}

fn optional_values(presence: &str, values: Option<&[FixtureAddr]>) -> Option<Vec<CompactAddr>> {
    match presence {
        "nil" => {
            assert!(values.is_none());
            None
        }
        "empty" => {
            assert!(values.is_some_and(<[FixtureAddr]>::is_empty));
            Some(Vec::new())
        }
        "present" => {
            let values = values.expect("present values have addresses");
            assert!(!values.is_empty());
            Some(
                values
                    .iter()
                    .map(|value| CompactAddr {
                        ip: value.ip,
                        port: value.port,
                    })
                    .collect(),
            )
        }
        other => panic!("unexpected values presence {other:?}"),
    }
}

fn optional_samples(presence: &str, values: Option<&[Id20]>) -> Option<Vec<Id20>> {
    match presence {
        "nil" => {
            assert!(values.is_none());
            None
        }
        "empty" => {
            assert!(values.is_some_and(<[Id20]>::is_empty));
            Some(Vec::new())
        }
        "present" => {
            let values = values.expect("present samples have IDs");
            assert!(!values.is_empty());
            Some(values.to_vec())
        }
        other => panic!("unexpected samples presence {other:?}"),
    }
}

fn optional_i64(presence: &str, value: i64) -> Option<i64> {
    match presence {
        "nil" => {
            assert_eq!(value, 0);
            None
        }
        "present" => Some(value),
        other => panic!("unexpected integer presence {other:?}"),
    }
}

fn optional_bytes(presence: &str, value: &str) -> Option<ByteString> {
    match presence {
        "nil" => {
            assert!(value.is_empty());
            None
        }
        "present" => Some(ByteString::new(
            hex::decode(value).expect("present bytes are hex"),
        )),
        other => panic!("unexpected bytes presence {other:?}"),
    }
}

fn optional_bloom(presence: &str, value: &str) -> Option<ScrapeBloomFilter> {
    match presence {
        "nil" => {
            assert!(value.is_empty());
            None
        }
        "present" => Some(
            ScrapeBloomFilter::from_slice(&hex::decode(value).expect("present bloom is hex"))
                .expect("present bloom has BEP-33 width"),
        ),
        other => panic!("unexpected bloom presence {other:?}"),
    }
}

fn fixture_response(value: &FixtureResponse) -> MessageReturn {
    MessageReturn {
        id: value.id,
        nodes: optional_nodes(&value.nodes_presence, value.nodes.as_deref()),
        nodes6: optional_nodes(&value.nodes6_presence, value.nodes6.as_deref()),
        token: optional_bytes(&value.token_presence, &value.token_hex),
        values: optional_values(&value.values_presence, value.values.as_deref()),
        interval: optional_i64(&value.interval_presence, value.interval),
        num: optional_i64(&value.num_presence, value.num),
        samples: optional_samples(&value.samples_presence, value.samples.as_deref()),
        seeders_bloom: optional_bloom(&value.seeders_bloom_presence, &value.seeders_bloom_hex),
        peers_bloom: optional_bloom(&value.peers_bloom_presence, &value.peers_bloom_hex),
    }
}

fn expected_nodes(values: Option<&[FixtureNode]>) -> Vec<RoutingNode> {
    values
        .unwrap_or_default()
        .iter()
        .map(|value| RoutingNode {
            id: value.id,
            addr: fixture_addr(value.addr),
        })
        .collect()
}

fn expected_values(values: Option<&[FixtureAddr]>) -> Vec<SocketAddr> {
    values
        .unwrap_or_default()
        .iter()
        .copied()
        .map(fixture_addr)
        .collect()
}

fn assert_transport_error(
    error: DhtClientError<TransportSentinel>,
    sentinel: &TransportSentinel,
    fixture_id: &str,
) {
    assert!(Error::source(&error).is_some(), "{fixture_id}");
    let DhtClientError::QuerySend(QuerySendError::Transport(actual)) = error else {
        panic!("{fixture_id}: expected nested transport error")
    };
    assert!(Arc::ptr_eq(&actual.0, &sentinel.0), "{fixture_id}");
}

fn assert_go_zero_get_peers(result: &GetPeersResult, expected: bool, fixture_id: &str) {
    let actual = result.id == Id20::ZERO && result.nodes.is_empty() && result.values.is_empty();
    assert_eq!(actual, expected, "{fixture_id}");
}

fn assert_go_zero_sample(result: &SampleInfoHashesResult, expected: bool, fixture_id: &str) {
    let actual = result.id == Id20::ZERO
        && result.nodes.is_empty()
        && result.samples.is_none()
        && result.num == 0
        && result.interval == 0;
    assert_eq!(actual, expected, "{fixture_id}");
}

fn assert_bloom(actual: ScrapeBloomFilter, expected: &FixtureBloom, fixture_id: &str) {
    assert_eq!(
        hex::encode(actual.as_bytes()),
        expected.bloom_hex,
        "{fixture_id}"
    );
    assert_eq!(expected.capacity, 2048, "{fixture_id}");
    assert_eq!(expected.hashes, 2, "{fixture_id}");
    assert_eq!(
        actual.approximated_size(),
        expected.approximated_size,
        "{fixture_id}"
    );
}

fn assert_expected_error_metadata(
    expected: &FixtureExpected,
    outcome: &str,
    error_text: &str,
    error_identity_preserved: bool,
    error_is_typed_nil: bool,
    fixture_id: &str,
) {
    assert_eq!(expected.outcome, outcome, "{fixture_id}");
    assert_eq!(expected.error_text, error_text, "{fixture_id}");
    assert_eq!(
        expected.error_identity_preserved, error_identity_preserved,
        "{fixture_id}"
    );
    assert_eq!(
        expected.error_is_typed_nil, error_is_typed_nil,
        "{fixture_id}"
    );
}

fn assert_peer_result_projection(
    expected: &FixtureResult,
    actual_id: Id20,
    actual_nodes: &[RoutingNode],
    actual_values: &[SocketAddr],
    fixture_id: &str,
) {
    assert_eq!(expected.id, actual_id, "{fixture_id}");
    assert_eq!(
        expected_nodes(expected.nodes.as_deref()),
        actual_nodes,
        "{fixture_id}"
    );
    assert_eq!(
        expected.nodes_presence,
        if actual_nodes.is_empty() {
            "nil"
        } else {
            "present"
        },
        "{fixture_id}"
    );
    assert_eq!(
        expected_values(expected.values.as_deref()),
        actual_values,
        "{fixture_id}"
    );
    assert_eq!(
        expected.values_presence,
        if actual_values.is_empty() {
            "nil"
        } else {
            "present"
        },
        "{fixture_id}"
    );
    assert!(expected.samples_presence.is_empty(), "{fixture_id}");
    assert!(expected.samples.is_none(), "{fixture_id}");
    assert_eq!(expected.num, 0, "{fixture_id}");
    assert_eq!(expected.interval, 0, "{fixture_id}");
}

fn assert_no_expected_blooms(expected: &FixtureResult, fixture_id: &str) {
    assert!(expected.peers_bloom.is_none(), "{fixture_id}");
    assert!(expected.seeders_bloom.is_none(), "{fixture_id}");
}

fn assert_zero_peer_result(expected: &FixtureExpected, fixture_id: &str) {
    assert!(expected.result_was_zero, "{fixture_id}");
    assert_peer_result_projection(&expected.result, Id20::ZERO, &[], &[], fixture_id);
    assert_no_expected_blooms(&expected.result, fixture_id);
}

fn assert_sample_result_projection(
    expected: &FixtureResult,
    actual: &SampleInfoHashesResult,
    fixture_id: &str,
) {
    assert_eq!(expected.id, actual.id, "{fixture_id}");
    assert_eq!(
        expected_nodes(expected.nodes.as_deref()),
        actual.nodes,
        "{fixture_id}"
    );
    assert_eq!(
        expected.nodes_presence,
        if actual.nodes.is_empty() {
            "nil"
        } else {
            "present"
        },
        "{fixture_id}"
    );
    assert!(expected.values_presence.is_empty(), "{fixture_id}");
    assert!(expected.values.is_none(), "{fixture_id}");
    assert_eq!(expected.samples, actual.samples, "{fixture_id}");
    assert_eq!(
        expected.samples_presence,
        match actual.samples.as_deref() {
            None => "nil",
            Some([]) => "empty",
            Some(_) => "present",
        },
        "{fixture_id}"
    );
    assert_eq!(expected.num, actual.num, "{fixture_id}");
    assert_eq!(expected.interval, actual.interval, "{fixture_id}");
    assert_no_expected_blooms(expected, fixture_id);
}

fn assert_zero_sample_result(expected: &FixtureExpected, fixture_id: &str) {
    assert!(expected.result_was_zero, "{fixture_id}");
    assert_sample_result_projection(
        &expected.result,
        &SampleInfoHashesResult {
            id: Id20::ZERO,
            samples: None,
            nodes: Vec::new(),
            num: 0,
            interval: 0,
        },
        fixture_id,
    );
}

fn assert_query(fixture: &Fixture, sender: &FixtureSender<ScriptedIssuer>, remote: SocketAddr) {
    assert_eq!(sender.calls, fixture.expected.query_calls, "{}", fixture.id);
    assert_eq!(sender.destinations, vec![remote], "{}", fixture.id);
    assert_eq!(
        hex::encode(&sender.wires[0]),
        fixture.expected.query_wire_hex,
        "{}",
        fixture.id
    );
    let query = KrpcMessage::decode(&sender.wires[0]).expect("captured fixture query decodes");
    assert_eq!(
        query.transaction_id.as_bytes(),
        transaction_id(&fixture.input.transaction_id_hex).as_bytes(),
        "{}",
        fixture.id
    );
    assert_eq!(query.message_type.as_bytes(), b"q", "{}", fixture.id);
    assert_eq!(
        query.query.as_bytes(),
        fixture.expected.query_method.as_bytes(),
        "{}",
        fixture.id
    );
    let args = query.args.expect("fixture query has arguments");
    let expected = &fixture.expected.query_args;
    assert_eq!(args.id, expected.id, "{}", fixture.id);
    assert_eq!(args.info_hash.unwrap_or(Id20::ZERO), expected.info_hash);
    assert_eq!(args.target.unwrap_or(Id20::ZERO), expected.target);
    assert_eq!(hex::encode(args.token.as_bytes()), expected.token_hex);
    assert_eq!(args.port.is_none(), expected.port_presence == "nil");
    assert_eq!(args.implied_port, expected.implied_port);
    assert_eq!(args.want.is_none(), expected.want_presence == "nil");
    let actual_want = args
        .want
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|value| String::from_utf8(value.as_bytes().to_vec()).expect("fixture want is UTF-8"))
        .collect::<Vec<_>>();
    assert_eq!(actual_want, expected.want, "{}", fixture.id);
    assert_eq!(args.no_seed, expected.no_seed, "{}", fixture.id);
    assert_eq!(args.scrape, expected.scrape, "{}", fixture.id);
    assert!(expected.bep44_fields_are_zero, "{}", fixture.id);
}

fn id(last: u8) -> Id20 {
    let mut value = [0; 20];
    value[19] = last;
    Id20::from_slice(&value).expect("fixed-width ID")
}

#[tokio::test]
async fn real_go_peer_scrape_and_sample_projection_matches_the_typed_client() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 26);
    let mut saw_absent_samples = false;
    let mut saw_empty_samples = false;
    let mut saw_i64_min = false;
    let mut saw_i64_max = false;
    let mut saw_patterned_bloom_direction = false;
    let mut missing_bloom_combinations = [false; 4];
    let mut saw_ignored_get_fields = false;
    let mut saw_ignored_sample_fields = false;

    for fixture in fixtures {
        assert_eq!(
            fixture.subsystem, "dht_peer_sample_client",
            "{}",
            fixture.id
        );
        assert_eq!(fixture.runtime.int_bits, 64, "{}", fixture.id);
        assert_eq!(fixture.expected.query_calls, 1, "{}", fixture.id);
        assert_eq!(fixture.expected.query_remote, fixture.input.remote);
        assert_eq!(fixture.expected.query_args.id, fixture.input.local_id);
        assert_eq!(
            fixture.expected.query_args.info_hash,
            fixture.input.info_hash.unwrap_or(Id20::ZERO),
            "{}",
            fixture.id
        );
        assert_eq!(
            fixture.expected.query_args.target,
            fixture.input.target.unwrap_or(Id20::ZERO),
            "{}",
            fixture.id
        );
        let expected_method = if fixture.input.operation == "get_peers_scrape" {
            "get_peers"
        } else {
            fixture.input.operation.as_str()
        };
        assert_eq!(
            fixture.expected.query_method, expected_method,
            "{}",
            fixture.id
        );

        let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([Ok(
            transaction_id(&fixture.input.transaction_id_hex),
        )])));
        let remote = fixture_addr(fixture.input.remote);
        let sentinel = TransportSentinel(Arc::new(()));
        let mut sender = FixtureSender {
            registry: registry.clone(),
            response: response_message(Some(fixture_response(&fixture.input.response)), None),
            response_source: remote,
            // The Go-only pre-cancelled and typed-nil outcomes have no Rust
            // representation. A transport stop still captures their exact
            // method/argument/wire contract without inventing parity.
            transport_error: (!fixture.input.failure.is_empty()).then(|| sentinel.clone()),
            calls: 0,
            destinations: Vec::new(),
            wires: Vec::new(),
        };
        let client = DhtClient::new(fixture.input.local_id, &registry, Duration::from_secs(4));

        match fixture.input.operation.as_str() {
            "get_peers" => {
                let result = client
                    .get_peers(
                        &mut sender,
                        remote,
                        fixture.input.info_hash.expect("get_peers info hash"),
                    )
                    .await;
                match fixture.input.failure.as_str() {
                    "" => {
                        assert_expected_error_metadata(
                            &fixture.expected,
                            "success",
                            "",
                            false,
                            false,
                            &fixture.id,
                        );
                        let result = result.expect("real-Go success case succeeds in Rust");
                        assert_eq!(
                            result,
                            GetPeersResult {
                                id: fixture.expected.result.id,
                                nodes: expected_nodes(fixture.expected.result.nodes.as_deref()),
                                values: expected_values(fixture.expected.result.values.as_deref()),
                            },
                            "{}",
                            fixture.id
                        );
                        assert_go_zero_get_peers(
                            &result,
                            fixture.expected.result_was_zero,
                            &fixture.id,
                        );
                        assert_peer_result_projection(
                            &fixture.expected.result,
                            result.id,
                            &result.nodes,
                            &result.values,
                            &fixture.id,
                        );
                        assert_no_expected_blooms(&fixture.expected.result, &fixture.id);
                        if fixture.input.response.nodes6.is_some()
                            || fixture.input.response.token_presence == "present"
                            || fixture.input.response.samples.is_some()
                            || fixture.input.response.peers_bloom_presence == "present"
                        {
                            saw_ignored_get_fields = true;
                        }
                    }
                    "query_error" => {
                        assert_expected_error_metadata(
                            &fixture.expected,
                            "query_error",
                            "peer/sample client oracle sentinel",
                            true,
                            false,
                            &fixture.id,
                        );
                        assert_zero_peer_result(&fixture.expected, &fixture.id);
                        assert_transport_error(result.unwrap_err(), &sentinel, &fixture.id);
                    }
                    "pre_cancelled" => {
                        assert_expected_error_metadata(
                            &fixture.expected,
                            "context_cancelled",
                            "context canceled",
                            false,
                            false,
                            &fixture.id,
                        );
                        assert_zero_peer_result(&fixture.expected, &fixture.id);
                        assert_transport_error(result.unwrap_err(), &sentinel, &fixture.id);
                    }
                    "typed_nil_error" => {
                        assert_expected_error_metadata(
                            &fixture.expected,
                            "typed_nil_error",
                            "",
                            false,
                            true,
                            &fixture.id,
                        );
                        assert_zero_peer_result(&fixture.expected, &fixture.id);
                        assert_transport_error(result.unwrap_err(), &sentinel, &fixture.id);
                    }
                    other => panic!("{}: unexpected get_peers failure {other:?}", fixture.id),
                }
            }
            "get_peers_scrape" => {
                let result = client
                    .get_peers_scrape(
                        &mut sender,
                        remote,
                        fixture.input.info_hash.expect("scrape info hash"),
                    )
                    .await;
                if fixture.input.failure == "query_error" {
                    assert_expected_error_metadata(
                        &fixture.expected,
                        "query_error",
                        "peer/sample client oracle sentinel",
                        true,
                        false,
                        &fixture.id,
                    );
                    assert_zero_peer_result(&fixture.expected, &fixture.id);
                    assert_transport_error(result.unwrap_err(), &sentinel, &fixture.id);
                } else if fixture.expected.outcome == "missing_scrape_bloom" {
                    assert_expected_error_metadata(
                        &fixture.expected,
                        "missing_scrape_bloom",
                        "missing bloom filter in scrape response",
                        false,
                        false,
                        &fixture.id,
                    );
                    assert_zero_peer_result(&fixture.expected, &fixture.id);
                    let error = result.expect_err("missing bloom is a semantic error");
                    assert!(
                        error.to_string().starts_with(&fixture.expected.error_text),
                        "{}",
                        fixture.id
                    );
                    let DhtClientError::MissingScrapeBloomFilters {
                        response_source,
                        message,
                        missing_peers,
                        missing_seeders,
                    } = error
                    else {
                        panic!("{}: expected typed missing-bloom error", fixture.id)
                    };
                    assert_eq!(response_source, remote, "{}", fixture.id);
                    assert_eq!(message.as_ref(), &sender.response, "{}", fixture.id);
                    assert_eq!(
                        missing_peers,
                        fixture.input.response.peers_bloom_presence == "nil"
                    );
                    assert_eq!(
                        missing_seeders,
                        fixture.input.response.seeders_bloom_presence == "nil"
                    );
                    missing_bloom_combinations
                        [usize::from(missing_peers) * 2 + usize::from(missing_seeders)] = true;
                    assert_eq!(registry.pending_count(), 0, "{}", fixture.id);
                } else {
                    assert_expected_error_metadata(
                        &fixture.expected,
                        "success",
                        "",
                        false,
                        false,
                        &fixture.id,
                    );
                    let result: GetPeersScrapeResult = result.expect("successful scrape");
                    assert_peer_result_projection(
                        &fixture.expected.result,
                        result.id,
                        &result.nodes,
                        &result.values,
                        &fixture.id,
                    );
                    assert_bloom(
                        result.peers_bloom,
                        fixture
                            .expected
                            .result
                            .peers_bloom
                            .as_ref()
                            .expect("successful Go scrape has peer bloom"),
                        &fixture.id,
                    );
                    assert_bloom(
                        result.seeders_bloom,
                        fixture
                            .expected
                            .result
                            .seeders_bloom
                            .as_ref()
                            .expect("successful Go scrape has seeder bloom"),
                        &fixture.id,
                    );
                    saw_patterned_bloom_direction |=
                        result.peers_bloom.as_bytes() != result.seeders_bloom.as_bytes();
                    assert!(!fixture.expected.result_was_zero, "{}", fixture.id);
                }
            }
            "sample_infohashes" => {
                let result = client
                    .sample_infohashes(
                        &mut sender,
                        remote,
                        fixture.input.target.expect("sample target"),
                    )
                    .await;
                if fixture.input.failure == "query_error" {
                    assert_expected_error_metadata(
                        &fixture.expected,
                        "query_error",
                        "peer/sample client oracle sentinel",
                        true,
                        false,
                        &fixture.id,
                    );
                    assert_zero_sample_result(&fixture.expected, &fixture.id);
                    assert_transport_error(result.unwrap_err(), &sentinel, &fixture.id);
                } else {
                    assert_expected_error_metadata(
                        &fixture.expected,
                        "success",
                        "",
                        false,
                        false,
                        &fixture.id,
                    );
                    let result = result.expect("real-Go sample success case succeeds in Rust");
                    assert_eq!(
                        result,
                        SampleInfoHashesResult {
                            id: fixture.expected.result.id,
                            samples: fixture.expected.result.samples.clone(),
                            nodes: expected_nodes(fixture.expected.result.nodes.as_deref()),
                            num: fixture.expected.result.num,
                            interval: fixture.expected.result.interval,
                        },
                        "{}",
                        fixture.id
                    );
                    assert_go_zero_sample(&result, fixture.expected.result_was_zero, &fixture.id);
                    assert_sample_result_projection(&fixture.expected.result, &result, &fixture.id);
                    saw_absent_samples |= result.samples.is_none();
                    saw_empty_samples |= result.samples.as_ref().is_some_and(Vec::is_empty);
                    saw_i64_min |= result.num == i64::MIN && result.interval == i64::MIN;
                    saw_i64_max |= result.num == i64::MAX && result.interval == i64::MAX;
                    if fixture.input.response.nodes6.is_some()
                        || fixture.input.response.values.is_some()
                        || fixture.input.response.token_presence == "present"
                        || fixture.input.response.peers_bloom_presence == "present"
                    {
                        saw_ignored_sample_fields = true;
                    }
                }
            }
            other => panic!("{}: unexpected operation {other:?}", fixture.id),
        }

        assert_query(&fixture, &sender, remote);
        assert_eq!(registry.pending_count(), 0, "{}", fixture.id);
    }

    assert!(saw_absent_samples);
    assert!(saw_empty_samples);
    assert!(saw_i64_min);
    assert!(saw_i64_max);
    assert!(saw_patterned_bloom_direction);
    assert!(missing_bloom_combinations[1]);
    assert!(missing_bloom_combinations[2]);
    assert!(missing_bloom_combinations[3]);
    assert!(saw_ignored_get_fields);
    assert!(saw_ignored_sample_fields);
}

fn empty_return(response_id: Id20) -> MessageReturn {
    MessageReturn {
        id: response_id,
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

fn response_message(response: Option<MessageReturn>, error: Option<KrpcError>) -> KrpcMessage {
    KrpcMessage {
        transaction_id: ByteString::default(),
        message_type: ByteString::new(if error.is_some() { b"e" } else { b"r" }),
        query: ByteString::default(),
        args: None,
        response,
        error,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    }
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::new(NoopWake));
    future.poll(&mut Context::from_waker(&waker))
}

struct GateSender {
    released: Arc<AtomicBool>,
    calls: usize,
}

impl DatagramSender for GateSender {
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        _datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        self.calls += 1;
        let released = Arc::clone(&self.released);
        Box::pin(std::future::poll_fn(move |_| {
            if released.load(Ordering::SeqCst) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }))
    }
}

struct DeliverDuringSend<I> {
    registry: TransactionRegistry<I>,
    response: MessageReturn,
    released: Arc<AtomicBool>,
    fail_after_delivery: Option<TransportSentinel>,
    calls: usize,
}

impl<I> DatagramSender for DeliverDuringSend<I>
where
    I: TransactionIdIssuer + 'static,
{
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        self.calls += 1;
        let query = KrpcMessage::decode(datagram).expect("decode captured query");
        let mut response = response_message(Some(self.response.clone()), None);
        response.transaction_id = query.transaction_id;
        assert_eq!(
            self.registry.deliver(destination, response),
            DeliveryOutcome::Delivered
        );
        let released = Arc::clone(&self.released);
        let mut error = self.fail_after_delivery.clone();
        Box::pin(std::future::poll_fn(move |_| {
            if released.load(Ordering::SeqCst) {
                Poll::Ready(error.take().map_or(Ok(()), Err))
            } else {
                Poll::Pending
            }
        }))
    }
}

#[tokio::test]
async fn response_delivered_during_send_waits_for_send_and_transport_failure_wins() {
    let remote = "192.0.2.1:6881".parse().unwrap();
    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([
        Ok(TransactionId::from(*b"D1")),
        Ok(TransactionId::from(*b"D2")),
    ])));
    let released = Arc::new(AtomicBool::new(false));
    let mut sender = DeliverDuringSend {
        registry: registry.clone(),
        response: empty_return(id(9)),
        released: Arc::clone(&released),
        fail_after_delivery: None,
        calls: 0,
    };
    let client = DhtClient::new(id(1), &registry, Duration::from_secs(4));
    let mut query = Box::pin(client.get_peers(&mut sender, remote, id(2)));
    assert!(poll_once(query.as_mut()).is_pending());
    assert_eq!(registry.pending_count(), 1);
    released.store(true, Ordering::SeqCst);
    let result = match poll_once(query.as_mut()) {
        Poll::Ready(result) => result.expect("delivered response"),
        Poll::Pending => panic!("released send must settle"),
    };
    assert_eq!(result.id, id(9));
    drop(query);
    assert_eq!(sender.calls, 1);
    assert_eq!(registry.pending_count(), 0);

    let sentinel = TransportSentinel(Arc::new(()));
    let mut sender = DeliverDuringSend {
        registry: registry.clone(),
        response: empty_return(id(10)),
        released: Arc::new(AtomicBool::new(true)),
        fail_after_delivery: Some(sentinel.clone()),
        calls: 0,
    };
    let error = client
        .sample_infohashes(&mut sender, remote, id(3))
        .await
        .unwrap_err();
    let DhtClientError::QuerySend(QuerySendError::Transport(actual)) = error else {
        panic!("transport failure must beat a buffered response")
    };
    assert!(Arc::ptr_eq(&actual.0, &sentinel.0));
    assert_eq!(sender.calls, 1);
    assert_eq!(registry.pending_count(), 0);
}

#[test]
fn dropping_an_unpolled_client_future_does_nothing() {
    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([Ok(
        TransactionId::from(*b"U1"),
    )])));
    let mut sender = GateSender {
        released: Arc::new(AtomicBool::new(true)),
        calls: 0,
    };
    let client = DhtClient::new(id(1), &registry, Duration::from_secs(4));
    let future = client.get_peers(&mut sender, "192.0.2.1:1".parse().unwrap(), id(2));
    drop(future);
    assert_eq!(sender.calls, 0);
    assert_eq!(registry.pending_count(), 0);
}

#[test]
fn dropping_a_polled_future_during_send_cleans_the_registration() {
    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([Ok(
        TransactionId::from(*b"P1"),
    )])));
    let mut sender = GateSender {
        released: Arc::new(AtomicBool::new(false)),
        calls: 0,
    };
    let client = DhtClient::new(id(1), &registry, Duration::from_secs(4));
    let mut future =
        Box::pin(client.get_peers_scrape(&mut sender, "192.0.2.1:1".parse().unwrap(), id(2)));
    assert!(poll_once(future.as_mut()).is_pending());
    assert_eq!(registry.pending_count(), 1);
    drop(future);
    assert_eq!(sender.calls, 1);
    assert_eq!(registry.pending_count(), 0);
}

#[tokio::test]
async fn abort_during_send_or_wait_cleans_exactly_one_registration() {
    for block_send in [true, false] {
        let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([Ok(
            TransactionId::from(*b"A1"),
        )])));
        let task_registry = registry.clone();
        let released = Arc::new(AtomicBool::new(!block_send));
        let task_released = Arc::clone(&released);
        let task = tokio::spawn(async move {
            let mut sender = GateSender {
                released: task_released,
                calls: 0,
            };
            DhtClient::new(id(1), &task_registry, Duration::from_secs(60))
                .sample_infohashes(&mut sender, "192.0.2.1:1".parse().unwrap(), id(2))
                .await
        });
        for _ in 0..100 {
            if registry.pending_count() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(registry.pending_count(), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(registry.pending_count(), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn timeout_starts_after_send_and_zero_timeout_is_exact() {
    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([
        Ok(TransactionId::from(*b"T1")),
        Ok(TransactionId::from(*b"T2")),
    ])));
    let released = Arc::new(AtomicBool::new(false));
    let mut sender = GateSender {
        released: Arc::clone(&released),
        calls: 0,
    };
    let client = DhtClient::new(id(1), &registry, Duration::from_secs(4));
    let remote = "192.0.2.1:1".parse().unwrap();
    let mut query = Box::pin(client.get_peers(&mut sender, remote, id(2)));
    assert!(poll_once(query.as_mut()).is_pending());
    assert_eq!(registry.pending_count(), 1);
    tokio::time::advance(Duration::from_secs(400)).await;
    assert!(poll_once(query.as_mut()).is_pending());
    released.store(true, Ordering::SeqCst);
    assert!(poll_once(query.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(matches!(
        poll_once(query.as_mut()),
        Poll::Ready(Err(DhtClientError::Timeout))
    ));
    drop(query);
    assert_eq!(registry.pending_count(), 0);

    let mut sender = GateSender {
        released: Arc::new(AtomicBool::new(true)),
        calls: 0,
    };
    let zero = DhtClient::new(id(1), &registry, Duration::ZERO)
        .get_peers_scrape(&mut sender, remote, id(2))
        .await;
    assert!(matches!(zero, Err(DhtClientError::Timeout)));
    assert_eq!(registry.pending_count(), 0);
}

#[derive(Clone, Copy)]
enum TerminalMode {
    RemoteError,
    MissingReturn,
    MissingError,
    Close,
    WrongSource,
}

struct TerminalSender<I> {
    registry: TransactionRegistry<I>,
    mode: TerminalMode,
}

impl<I> DatagramSender for TerminalSender<I>
where
    I: TransactionIdIssuer + 'static,
{
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let query = KrpcMessage::decode(datagram).expect("decode captured query");
        if matches!(self.mode, TerminalMode::Close) {
            self.registry.close();
            return Box::pin(async { Ok(()) });
        }
        let (source, response) = match self.mode {
            TerminalMode::RemoteError => (
                destination,
                response_message(
                    None,
                    Some(KrpcError {
                        code: 201,
                        message: ByteString::new(b"remote"),
                    }),
                ),
            ),
            TerminalMode::MissingReturn => (destination, response_message(None, None)),
            TerminalMode::MissingError => {
                let mut message = response_message(None, None);
                message.message_type = ByteString::new(b"e");
                (destination, message)
            }
            TerminalMode::WrongSource => (
                "192.0.2.99:9".parse().unwrap(),
                response_message(Some(empty_return(id(9))), None),
            ),
            TerminalMode::Close => unreachable!(),
        };
        let mut response = response;
        response.transaction_id = query.transaction_id;
        let outcome = self.registry.deliver(source, response);
        if matches!(self.mode, TerminalMode::WrongSource) {
            assert!(matches!(outcome, DeliveryOutcome::AddressMismatch { .. }));
        } else {
            assert_eq!(outcome, DeliveryOutcome::Delivered);
        }
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(start_paused = true)]
async fn terminal_wait_outcomes_are_typed_and_always_cleanup() {
    let remote: SocketAddr = "192.0.2.1:1".parse().unwrap();
    for (index, mode) in [
        TerminalMode::RemoteError,
        TerminalMode::MissingReturn,
        TerminalMode::MissingError,
        TerminalMode::Close,
        TerminalMode::WrongSource,
    ]
    .into_iter()
    .enumerate()
    {
        let tid = TransactionId::from([(index + 1) as u8, 1]);
        let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([Ok(tid)])));
        let mut sender = TerminalSender {
            registry: registry.clone(),
            mode,
        };
        let result = DhtClient::new(id(1), &registry, Duration::ZERO)
            .get_peers(&mut sender, remote, id(2))
            .await;
        match (mode, result) {
            (
                TerminalMode::RemoteError,
                Err(DhtClientError::RemoteError {
                    response_source,
                    message,
                    error,
                }),
            ) => {
                assert_eq!(response_source, remote);
                assert_eq!(message.transaction_id.as_bytes(), tid.as_bytes());
                assert_eq!(error.code, 201);
            }
            (
                TerminalMode::MissingReturn,
                Err(DhtClientError::MissingReturnBody {
                    response_source,
                    message,
                }),
            ) => {
                assert_eq!(response_source, remote);
                assert_eq!(message.transaction_id.as_bytes(), tid.as_bytes());
            }
            (
                TerminalMode::MissingError,
                Err(DhtClientError::MissingErrorBody {
                    response_source,
                    message,
                }),
            ) => {
                assert_eq!(response_source, remote);
                assert_eq!(message.transaction_id.as_bytes(), tid.as_bytes());
            }
            (TerminalMode::Close, Err(DhtClientError::RegistryClosed))
            | (TerminalMode::WrongSource, Err(DhtClientError::Timeout)) => {}
            (_, other) => panic!("unexpected terminal outcome: {other:?}"),
        }
        assert_eq!(registry.pending_count(), 0);
    }
}

struct ImmediateResponseSender<I> {
    registry: TransactionRegistry<I>,
    response: MessageReturn,
}

impl<I> DatagramSender for ImmediateResponseSender<I>
where
    I: TransactionIdIssuer + 'static,
{
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let query = KrpcMessage::decode(datagram).expect("decode captured query");
        let mut response = response_message(Some(self.response.clone()), None);
        response.transaction_id = query.transaction_id;
        assert_eq!(
            self.registry.deliver(destination, response),
            DeliveryOutcome::Delivered
        );
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn missing_scrape_blooms_is_post_transaction_semantic_error() {
    let remote = "192.0.2.1:1".parse().unwrap();
    for (index, peers, seeders) in [
        (1, None, None),
        (2, Some(ScrapeBloomFilter::EMPTY), None),
        (3, None, Some(ScrapeBloomFilter::EMPTY)),
    ] {
        let registry =
            TransactionRegistry::new(ScriptedIssuer(VecDeque::from([Ok(TransactionId::from([
                b'B', index,
            ]))])));
        let mut response = empty_return(id(9));
        response.peers_bloom = peers;
        response.seeders_bloom = seeders;
        let mut sender = ImmediateResponseSender {
            registry: registry.clone(),
            response,
        };
        let error = DhtClient::new(id(1), &registry, Duration::from_secs(4))
            .get_peers_scrape(&mut sender, remote, id(2))
            .await
            .unwrap_err();
        let DhtClientError::MissingScrapeBloomFilters {
            response_source,
            message,
            missing_peers,
            missing_seeders,
        } = error
        else {
            panic!("expected missing-bloom semantic error")
        };
        assert_eq!(response_source, remote);
        assert_eq!(message.message_type.as_bytes(), b"r");
        assert_eq!(missing_peers, peers.is_none());
        assert_eq!(missing_seeders, seeders.is_none());
        assert_eq!(registry.pending_count(), 0);
    }
}

struct CaptureReadySender {
    captured: Arc<Mutex<Vec<KrpcMessage>>>,
}

impl DatagramSender for CaptureReadySender {
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        self.captured
            .lock()
            .unwrap()
            .push(KrpcMessage::decode(datagram).expect("decode captured query"));
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn shared_registry_correlates_mixed_methods_out_of_order() {
    let remote_a = "192.0.2.1:1".parse().unwrap();
    let remote_b = "192.0.2.2:2".parse().unwrap();
    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([
        Ok(TransactionId::from(*b"M1")),
        Ok(TransactionId::from(*b"M2")),
    ])));
    let captured_a = Arc::new(Mutex::new(Vec::new()));
    let captured_b = Arc::new(Mutex::new(Vec::new()));
    let mut sender_a = CaptureReadySender {
        captured: Arc::clone(&captured_a),
    };
    let mut sender_b = CaptureReadySender {
        captured: Arc::clone(&captured_b),
    };
    let client = DhtClient::new(id(1), &registry, Duration::from_secs(4));
    let mut get = Box::pin(client.get_peers(&mut sender_a, remote_a, id(2)));
    let mut sample = Box::pin(client.sample_infohashes(&mut sender_b, remote_b, id(3)));
    assert!(poll_once(get.as_mut()).is_pending());
    assert!(poll_once(sample.as_mut()).is_pending());
    assert_eq!(registry.pending_count(), 2);

    let get_tid = captured_a.lock().unwrap()[0].transaction_id.clone();
    let sample_tid = captured_b.lock().unwrap()[0].transaction_id.clone();
    let mut sample_response = empty_return(id(12));
    sample_response.samples = Some(vec![id(21)]);
    sample_response.num = Some(i64::MAX);
    sample_response.interval = Some(i64::MIN);
    let mut message = response_message(Some(sample_response), None);
    message.transaction_id = sample_tid;
    assert_eq!(
        registry.deliver(remote_b, message),
        DeliveryOutcome::Delivered
    );
    let mut message = response_message(Some(empty_return(id(11))), None);
    message.transaction_id = get_tid;
    assert_eq!(
        registry.deliver(remote_a, message),
        DeliveryOutcome::Delivered
    );

    let sample_result = sample.await.expect("sample response");
    assert_eq!(sample_result.id, id(12));
    assert_eq!(sample_result.samples, Some(vec![id(21)]));
    assert_eq!(sample_result.num, i64::MAX);
    assert_eq!(sample_result.interval, i64::MIN);
    assert_eq!(get.await.expect("get-peers response").id, id(11));
    assert_eq!(registry.pending_count(), 0);
}

struct PacketReceiver {
    wire: Vec<u8>,
    source: SocketAddr,
}

impl DatagramReceiver for PacketReceiver {
    type Error = std::convert::Infallible;

    fn receive<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ReceivedDatagram, Self::Error>> + Send + 'a>> {
        Box::pin(async move {
            buffer[..self.wire.len()].copy_from_slice(&self.wire);
            Ok(ReceivedDatagram {
                length: self.wire.len(),
                source: self.source,
            })
        })
    }
}

#[tokio::test(start_paused = true)]
async fn malformed_ignored_return_field_stays_pending_until_timeout() {
    let remote = "192.0.2.1:1".parse().unwrap();
    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([Ok(
        TransactionId::from(*b"R1"),
    )])));
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut sender = CaptureReadySender {
        captured: Arc::clone(&captured),
    };
    let client = DhtClient::new(id(1), &registry, Duration::from_secs(4));
    let mut query = Box::pin(client.get_peers(&mut sender, remote, id(2)));
    assert!(poll_once(query.as_mut()).is_pending());
    assert_eq!(registry.pending_count(), 1);
    let tid = captured.lock().unwrap()[0].transaction_id.clone();

    // `nodes6` is not projected by get_peers, but both production Go and the
    // bounded Rust inbound path validate its shape before transaction delivery.
    let mut wire = b"d1:rd2:id20:".to_vec();
    wire.extend_from_slice(id(9).as_bytes());
    wire.extend_from_slice(b"6:nodes6i1ee1:t2:");
    wire.extend_from_slice(tid.as_bytes());
    wire.extend_from_slice(b"1:y1:re");
    let mut dispatcher = ReceiveDispatcher::new(
        PacketReceiver {
            wire,
            source: remote,
        },
        registry.clone(),
    );
    assert!(matches!(
        dispatcher.receive_one().await.unwrap(),
        ReceiveDispatchOutcome::DecodeRejected { .. }
    ));
    assert_eq!(registry.pending_count(), 1);
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(matches!(query.await, Err(DhtClientError::Timeout)));
    assert_eq!(registry.pending_count(), 0);
}
