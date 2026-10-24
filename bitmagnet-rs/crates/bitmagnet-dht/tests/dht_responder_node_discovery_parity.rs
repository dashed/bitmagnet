//! Strict consumption of Go's node-discovery fixture and projection of the
//! ID/address fields owned by Rust's discovery event. Go-only wrapper metadata
//! and asynchronous ordering are retained and asserted as outer evidence, not
//! represented as Rust `RoutingNode` state or goroutine behavior.

use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::num::NonZeroUsize;

use bitmagnet_dht::{
    dht_discovery_channel, ByteString, DhtDiscoveryStats, DhtDispatchOutcome, DhtDispatcher,
    DhtResponder, Id20, KTable, KrpcMessage, MessageArgs, RoutingNode,
};
use serde::Deserialize;

const FIXTURE: &str =
    include_str!("../../../../testdata/parity/dht/responder_node_discovery.jsonl");
const ORIGIN: &str = "00112233445566778899aabbccddeeff10203040";
const INFO_HASH: &str = "11223344556677889900aabbccddeeff01020304";
const FIXTURE_IDS: [&str; 11] = [
    "ping_success_read_only_ipv4",
    "find_node_success_mapped_ipv4",
    "get_peers_success_scoped_ipv6",
    "announce_peer_success_mutates_before_notification",
    "sample_infohashes_success_native_ipv6",
    "ping_success_zero_requester_id",
    "duplicate_successes_are_preserved",
    "missing_arguments_suppresses_notification",
    "unknown_method_suppresses_notification",
    "missing_target_suppresses_notification",
    "invalid_announce_token_suppresses_notification",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    id: String,
    subsystem: String,
    oracle: Oracle,
    input: Input,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Oracle {
    composition: String,
    ingress: String,
    production_socket_reachable: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Input {
    method: String,
    args_present: bool,
    requester_id: Id20,
    info_hash: Option<Id20>,
    target: Option<Id20>,
    token: Option<String>,
    source: Address,
    read_only: bool,
    attempts: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Expected {
    outcome: String,
    return_id: Id20,
    protocol_error: Option<ProtocolError>,
    no_event_evidence: Option<String>,
    respond_returned_before_receive: bool,
    events: Vec<Node>,
    announce_stored_before_receive: bool,
    announce_peer_before_receive: Option<Address>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProtocolError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Node {
    id: Id20,
    addr: Address,
    #[serde(rename = "timeZero")]
    go_time_zero: bool,
    #[serde(rename = "dropped")]
    go_dropped: bool,
    #[serde(rename = "isSampleInfoHashesCandidate")]
    go_is_sample_info_hashes_candidate: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Address {
    ip: IpAddr,
    port: u16,
    scope: u32,
}

impl Address {
    fn socket_addr(&self) -> SocketAddr {
        match self.ip {
            IpAddr::V4(ip) => SocketAddr::V4(SocketAddrV4::new(ip, self.port)),
            IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(ip, self.port, 0, self.scope)),
        }
    }
}

#[test]
fn real_go_rows_project_owned_rust_fields_and_retain_outer_evidence() {
    let fixtures = FIXTURE
        .lines()
        .map(|line| {
            assert!(!line.is_empty(), "fixture must not contain blank rows");
            serde_json::from_str::<Fixture>(line).expect("strict Go node-discovery row")
        })
        .collect::<Vec<_>>();
    assert_eq!(fixtures.len(), FIXTURE_IDS.len());

    for (index, fixture) in fixtures.into_iter().enumerate() {
        assert_eq!(fixture.id, FIXTURE_IDS[index]);
        replay_fixture(fixture);
    }
}

fn replay_fixture(fixture: Fixture) {
    let Fixture {
        id,
        subsystem,
        oracle,
        input,
        expected,
    } = fixture;
    assert_eq!(subsystem, "dht_responder_node_discovery", "{id}");
    assert_eq!(
        oracle.composition, "private_core_then_actual_node_discovery_wrapper",
        "{id}"
    );
    assert_eq!(oracle.ingress, "direct_recv_msg", "{id}");
    assert_eq!(
        oracle.production_socket_reachable,
        input.source.ip.is_ipv4(),
        "{id}"
    );
    assert!(input.attempts > 0, "{id}");

    let origin = Id20::from_hex(ORIGIN).unwrap();
    let table = KTable::new(origin);
    let responder = DhtResponder::with_token_secret(table.clone(), *b"0123456789abcdefghij", 10);
    let source = input.source.socket_addr();
    let request = request_for_fixture(&input, source, &responder);
    let capacity = NonZeroUsize::new(input.attempts.max(1)).expect("positive attempts");
    let (discovery, mut discovered) = dht_discovery_channel(capacity);
    let dispatcher = DhtDispatcher::from_responder(responder).with_discovery(discovery.clone());

    for _ in 0..input.attempts {
        let outcome = dispatcher.dispatch(source, &request);
        match expected.outcome.as_str() {
            "success" => {
                let DhtDispatchOutcome::Reply(reply) = outcome else {
                    panic!("{id}: successful Go row became a local Rust failure")
                };
                assert!(reply.message.error.is_none(), "{id}");
                assert_eq!(
                    reply.message.response.as_ref().unwrap().id,
                    expected.return_id,
                    "{id}"
                );
                assert!(expected.protocol_error.is_none(), "{id}");
                assert!(expected.no_event_evidence.is_none(), "{id}");
                assert!(expected.respond_returned_before_receive, "{id}");
            }
            "protocol_error" => {
                let DhtDispatchOutcome::Reply(reply) = outcome else {
                    panic!("{id}: protocol Go row became a local Rust failure")
                };
                let expected_error = expected.protocol_error.as_ref().expect("Go protocol error");
                let actual_error = reply.message.error.as_ref().expect("Rust protocol error");
                assert_eq!(actual_error.code, expected_error.code, "{id}");
                assert_eq!(
                    actual_error.message.as_bytes(),
                    expected_error.message.as_bytes(),
                    "{id}"
                );
                assert!(reply.message.response.is_none(), "{id}");
                assert_eq!(
                    expected.no_event_evidence.as_deref(),
                    Some("source_predicate_err_non_nil"),
                    "{id}"
                );
                assert!(!expected.respond_returned_before_receive, "{id}");
                assert_eq!(
                    expected.return_id,
                    if input.args_present {
                        origin
                    } else {
                        Id20::ZERO
                    },
                    "{id}"
                );
            }
            other => panic!("{id}: unknown fixture outcome {other}"),
        }

        if expected.announce_stored_before_receive {
            let stored = table
                .hash(Id20::from_hex(INFO_HASH).unwrap())
                .expect("announce mutation before discovery receive");
            let expected_peer = expected
                .announce_peer_before_receive
                .as_ref()
                .expect("announce peer");
            assert_eq!(stored.peers.len(), 1, "{id}");
            assert_eq!(stored.peers[0].addr, expected_peer.socket_addr(), "{id}");
        } else {
            assert!(expected.announce_peer_before_receive.is_none(), "{id}");
            assert!(
                table.hash(Id20::from_hex(INFO_HASH).unwrap()).is_none(),
                "{id}: non-announce or failed announce unexpectedly mutated the hash table"
            );
        }
    }

    let actual_events = (0..expected.events.len())
        .map(|_| discovered.try_recv().expect("queued discovery event"))
        .collect::<Vec<_>>();
    assert_eq!(
        actual_events,
        expected
            .events
            .iter()
            .map(|node| {
                // Go's ktable.Node carries these extra wrapper defaults. Rust's
                // discovery event deliberately owns only the ID/address fields
                // consumed downstream, so retain the rest as outer evidence.
                assert!(node.go_time_zero, "{id}");
                assert!(!node.go_dropped, "{id}");
                assert!(node.go_is_sample_info_hashes_candidate, "{id}");
                RoutingNode {
                    id: node.id,
                    addr: node.addr.socket_addr(),
                }
            })
            .collect::<Vec<_>>(),
        "{id}"
    );
    assert_eq!(
        discovered.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty),
        "{id}"
    );
    let queued = u64::try_from(expected.events.len()).unwrap();
    assert_eq!(
        discovery.stats(),
        DhtDiscoveryStats {
            offered: queued,
            queued,
            full_dropped: 0,
            receiver_closed_dropped: 0,
        },
        "{id}"
    );
}

fn request_for_fixture(input: &Input, source: SocketAddr, responder: &DhtResponder) -> KrpcMessage {
    let args = input.args_present.then(|| {
        let mut args = MessageArgs {
            id: input.requester_id,
            info_hash: input.info_hash,
            target: input.target,
            token: ByteString::default(),
            port: None,
            implied_port: false,
            want: None,
            no_seed: 0,
            scrape: 0,
        };
        if input.method == "announce_peer" {
            args.port = Some(51_413);
            args.token = match input.token.as_deref() {
                Some("valid") => {
                    let token_request = query(
                        "get_peers",
                        Some(MessageArgs {
                            id: input.requester_id,
                            info_hash: input.info_hash,
                            target: None,
                            token: ByteString::default(),
                            port: None,
                            implied_port: false,
                            want: None,
                            no_seed: 0,
                            scrape: 0,
                        }),
                        input.read_only,
                    );
                    responder
                        .respond(source, &token_request)
                        .expect("valid token preflight")
                        .token
                        .expect("get_peers token")
                }
                Some(token) => ByteString::new(token.as_bytes().to_vec()),
                None => ByteString::default(),
            };
        }
        args
    });
    query(&input.method, args, input.read_only)
}

fn query(method: &str, args: Option<MessageArgs>, read_only: bool) -> KrpcMessage {
    KrpcMessage {
        transaction_id: ByteString::new(b"ND".to_vec()),
        message_type: ByteString::new(b"q".to_vec()),
        query: ByteString::new(method.as_bytes().to_vec()),
        args,
        response: None,
        error: None,
        observed_addr: None,
        read_only,
        client_id: ByteString::default(),
    }
}
