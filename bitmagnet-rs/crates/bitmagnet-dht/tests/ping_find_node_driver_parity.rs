//! Differential and boundary proof for one fake/no-socket driver step.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bitmagnet_dht::{
    ByteString, DatagramReceiver, DatagramSender, DeliveryOutcome, Id20, KrpcMessage, MessageArgs,
    NodeTable, PingFindNodeDispatchOutcome, PingFindNodeDriver, PingFindNodeDriverError,
    PingFindNodeDriverOutcome, PingFindNodeError, PingFindNodeSendError, ReceiveDispatchError,
    ReceiveDispatchOutcome, ReceivedDatagram, RoutingNode, RoutingPutResult, TransactionId,
    TransactionIdIssuer, TransactionIdSourceError, TransactionRegistry, MAX_INBOUND_DATAGRAM_BYTES,
};
use serde::Deserialize;
use tokio::sync::oneshot;

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
    wire_hex: String,
    source: FixtureAddr,
    origin: Id20,
    #[serde(default)]
    nodes: Vec<FixtureNode>,
    #[serde(default)]
    pending_tid_hex: String,
    expected_source: Option<FixtureAddr>,
    #[serde(default)]
    send_fails: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Expected {
    go_outcome: String,
    rust_outcome: String,
    events: Vec<String>,
    rust_events: Vec<String>,
    destination: Option<FixtureAddr>,
    #[serde(default)]
    wire_hex: String,
    send_calls: usize,
    receive_calls: usize,
    #[serde(default)]
    pending_after: bool,
    #[serde(default)]
    intentional_partial: bool,
    #[serde(default)]
    send_failure_logged: bool,
}

#[derive(Clone, Copy, Deserialize)]
struct FixtureAddr {
    ip: IpAddr,
    port: u16,
    #[serde(default)]
    scope: u32,
}

#[derive(Clone, Copy, Deserialize)]
struct FixtureNode {
    id: Id20,
    addr: FixtureAddr,
}

#[derive(Deserialize)]
struct ResponderFixture {
    id: String,
    input: ResponderInput,
    expected: ResponderExpected,
}

#[derive(Deserialize)]
struct ResponderInput {
    origin: Id20,
    nodes: Option<Vec<FixtureNode>>,
    request: ResponderRequest,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResponderRequest {
    method: String,
    args_present: bool,
    sender_id: Option<Id20>,
    #[serde(default)]
    target_present: bool,
    target: Option<Id20>,
    #[serde(default)]
    want: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResponderExpected {
    rust_outcome: String,
    native_ipv6_node: Option<FixtureNode>,
}

#[derive(Deserialize)]
struct DispatchFixture {
    id: String,
    expected: DispatchExpected,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DispatchExpected {
    #[serde(default)]
    wire_hex: String,
}

#[derive(Clone, Debug)]
struct Packet {
    wire: Vec<u8>,
    source: SocketAddr,
    reported: Option<usize>,
}

#[derive(Clone, Debug)]
struct TransportSentinel(Arc<()>);

impl Display for TransportSentinel {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("transport sentinel")
    }
}

impl Error for TransportSentinel {}

struct ScriptedIssuer(VecDeque<TransactionId>);

impl TransactionIdIssuer for ScriptedIssuer {
    fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
        self.0
            .pop_front()
            .ok_or_else(|| TransactionIdSourceError::new("scripted issuer exhausted"))
    }
}

#[derive(Default)]
struct Observations {
    events: Vec<&'static str>,
    receive_calls: usize,
    destinations: Vec<SocketAddr>,
    wires: Vec<Vec<u8>>,
}

struct FakeReceiver {
    packet: Option<Result<Packet, TransportSentinel>>,
    observations: Arc<Mutex<Observations>>,
}

impl DatagramReceiver for FakeReceiver {
    type Error = TransportSentinel;

    fn receive<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ReceivedDatagram, Self::Error>> + Send + 'a>> {
        let mut observations = self.observations.lock().unwrap();
        observations.events.push("receive");
        observations.receive_calls += 1;
        drop(observations);
        let packet = self.packet.take().expect("driver receives exactly once");
        Box::pin(async move {
            let packet = packet?;
            let copied = packet.wire.len().min(buffer.len());
            buffer[..copied].copy_from_slice(&packet.wire[..copied]);
            Ok(ReceivedDatagram {
                length: packet.reported.unwrap_or(packet.wire.len()),
                source: packet.source,
            })
        })
    }
}

struct FakeSender {
    observations: Arc<Mutex<Observations>>,
    error: Option<TransportSentinel>,
}

impl DatagramSender for FakeSender {
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let mut observations = self.observations.lock().unwrap();
        observations.events.push("send");
        observations.destinations.push(destination);
        observations.wires.push(datagram.to_vec());
        drop(observations);
        let error = self.error.clone();
        Box::pin(async move { error.map_or(Ok(()), Err) })
    }
}

fn json_lines<T: for<'de> Deserialize<'de>>(name: &str) -> Vec<T> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht")
        .join(name);
    BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect()
}

fn socket_addr(value: FixtureAddr) -> SocketAddr {
    match value.ip {
        IpAddr::V4(ip) => SocketAddr::V4(SocketAddrV4::new(ip, value.port)),
        IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(ip, value.port, 0, value.scope)),
    }
}

fn routing_node(value: FixtureNode) -> RoutingNode {
    RoutingNode {
        id: value.id,
        addr: socket_addr(value.addr),
    }
}

fn empty_args(target: Option<Id20>) -> MessageArgs {
    MessageArgs {
        id: Id20::ZERO,
        info_hash: None,
        target,
        token: ByteString::default(),
        port: None,
        implied_port: false,
        want: None,
        no_seed: 0,
        scrape: 0,
    }
}

fn responder_request(input: &ResponderRequest) -> KrpcMessage {
    let args = input.args_present.then(|| MessageArgs {
        id: input.sender_id.unwrap_or(Id20::ZERO),
        target: input
            .target_present
            .then_some(input.target.unwrap_or(Id20::ZERO)),
        want: (!input.want.is_empty()).then(|| {
            input
                .want
                .iter()
                .map(|want| ByteString::new(want.as_bytes().to_vec()))
                .collect()
        }),
        ..empty_args(None)
    });
    KrpcMessage {
        transaction_id: ByteString::new([1, 2]),
        message_type: ByteString::new(b"q".to_vec()),
        query: ByteString::new(input.method.as_bytes().to_vec()),
        args,
        response: None,
        error: None,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    }
}

#[tokio::test]
async fn actual_go_read_handle_and_send_matrix_matches_one_rust_driver_step() {
    let fixtures = json_lines::<Fixture>("ping_find_node_driver.jsonl");
    assert_eq!(fixtures.len(), 12);

    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_ping_find_node_driver");
        assert_eq!(fixture.expected.receive_calls, 1);
        assert!(fixture.expected.send_calls <= 1);
        assert_eq!(
            fixture.expected.events,
            if fixture.expected.send_calls == 0 {
                vec!["receive"]
            } else {
                vec!["receive", "respond", "send"]
            },
            "{}: Go lifecycle evidence",
            fixture.id,
        );
        if fixture.expected.intentional_partial {
            assert_eq!(fixture.expected.go_outcome, "reply_sent");
            assert_eq!(fixture.expected.rust_outcome, "unowned_query");
        } else if fixture.expected.go_outcome != "send_error_swallowed" {
            assert_eq!(fixture.expected.go_outcome, fixture.expected.rust_outcome);
        }
        assert_eq!(
            fixture.expected.send_failure_logged,
            fixture.expected.go_outcome == "send_error_swallowed",
            "{}: Go post-Send completion evidence",
            fixture.id,
        );

        let pending_tid = hex::decode(&fixture.input.pending_tid_hex).unwrap();
        let issuer = TransactionId::from_slice(&pending_tid).ok();
        let registry =
            TransactionRegistry::new(ScriptedIssuer(issuer.into_iter().collect::<VecDeque<_>>()));
        let pending = if let (Some(transaction_id), Some(expected_source)) =
            (issuer, fixture.input.expected_source)
        {
            let registered = registry
                .register(
                    socket_addr(expected_source),
                    ByteString::new(b"ping".to_vec()),
                    empty_args(None),
                )
                .unwrap();
            assert_eq!(registered.transaction_id(), transaction_id);
            Some(registered.mark_sent())
        } else {
            None
        };

        let mut table = NodeTable::new(fixture.input.origin);
        for node in &fixture.input.nodes {
            assert_eq!(table.put(routing_node(*node)), RoutingPutResult::Accepted);
        }
        let observations = Arc::new(Mutex::new(Observations::default()));
        let receiver = FakeReceiver {
            packet: Some(Ok(Packet {
                wire: hex::decode(&fixture.input.wire_hex).unwrap(),
                source: socket_addr(fixture.input.source),
                reported: None,
            })),
            observations: Arc::clone(&observations),
        };
        let marker = TransportSentinel(Arc::new(()));
        let sender = FakeSender {
            observations: Arc::clone(&observations),
            error: fixture.input.send_fails.then(|| marker.clone()),
        };
        let mut driver = PingFindNodeDriver::new(receiver, registry.clone(), sender, &table);
        let result = driver.drive_one().await;

        match fixture.expected.rust_outcome.as_str() {
            "zero" => assert!(matches!(
                result,
                Ok(PingFindNodeDriverOutcome::NoReply(
                    ReceiveDispatchOutcome::ZeroLength { .. }
                ))
            )),
            "decode_rejected" => assert!(matches!(
                result,
                Ok(PingFindNodeDriverOutcome::NoReply(
                    ReceiveDispatchOutcome::DecodeRejected { .. }
                ))
            )),
            "ignored" => assert!(matches!(
                result,
                Ok(PingFindNodeDriverOutcome::NoReply(
                    ReceiveDispatchOutcome::Ignored { .. }
                ))
            )),
            "response_delivered" => assert!(matches!(
                result,
                Ok(PingFindNodeDriverOutcome::NoReply(
                    ReceiveDispatchOutcome::Response {
                        delivery: DeliveryOutcome::Delivered,
                        ..
                    }
                ))
            )),
            "error_delivered" => assert!(matches!(
                result,
                Ok(PingFindNodeDriverOutcome::NoReply(
                    ReceiveDispatchOutcome::Error {
                        delivery: DeliveryOutcome::Delivered,
                        ..
                    }
                ))
            )),
            "reply_sent" => {
                let Ok(PingFindNodeDriverOutcome::Sent(prepared)) = result else {
                    panic!("{}: expected sent reply", fixture.id)
                };
                let PingFindNodeDispatchOutcome::Reply(reply) = *prepared else {
                    panic!("{}: expected normal prepared reply", fixture.id)
                };
                assert_eq!(
                    reply.destination,
                    socket_addr(fixture.expected.destination.unwrap())
                );
                assert_eq!(
                    hex::encode(reply.wire().unwrap()),
                    fixture.expected.wire_hex
                );
            }
            "unowned_query" => {
                let Ok(PingFindNodeDriverOutcome::NoReply(ReceiveDispatchOutcome::Query {
                    source,
                    message,
                })) = result
                else {
                    panic!("{}: expected intact unowned query", fixture.id)
                };
                assert_eq!(source, socket_addr(fixture.input.source));
                assert_eq!(message.query.as_bytes(), b"get_peers");
            }
            "send_error" => {
                let Err(PingFindNodeDriverError::Send {
                    prepared,
                    error: PingFindNodeSendError::Transport(error),
                }) = result
                else {
                    panic!("{}: expected typed send error", fixture.id)
                };
                let PingFindNodeDispatchOutcome::Reply(reply) = *prepared else {
                    panic!("{}: expected normal prepared reply", fixture.id)
                };
                assert!(Arc::ptr_eq(&error.0, &marker.0));
                assert_eq!(
                    reply.destination,
                    socket_addr(fixture.expected.destination.unwrap())
                );
                assert_eq!(
                    hex::encode(reply.wire().unwrap()),
                    fixture.expected.wire_hex
                );
            }
            other => panic!("{}: unsupported Rust outcome {other}", fixture.id),
        }

        let observations = observations.lock().unwrap();
        assert_eq!(
            observations.events,
            fixture
                .expected
                .rust_events
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            "{}",
            fixture.id,
        );
        assert_eq!(observations.receive_calls, 1, "{}", fixture.id);
        assert_eq!(
            observations.wires.len(),
            if fixture.expected.intentional_partial {
                0
            } else {
                fixture.expected.send_calls
            },
            "{}",
            fixture.id,
        );
        if !observations.wires.is_empty() {
            assert_eq!(
                hex::encode(&observations.wires[0]),
                fixture.expected.wire_hex
            );
            assert_eq!(
                observations.destinations,
                [socket_addr(fixture.expected.destination.unwrap())]
            );
        }
        assert_eq!(
            registry.pending_count() != 0,
            fixture.expected.pending_after
        );
        drop(observations);
        drop(pending);
    }
}

#[tokio::test]
async fn receive_transport_and_overreport_fail_before_dispatch_or_send() {
    let table = NodeTable::new(Id20::ZERO);
    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::new()));
    let source: SocketAddr = "127.0.0.1:1".parse().unwrap();

    let receive_marker = TransportSentinel(Arc::new(()));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let receiver = FakeReceiver {
        packet: Some(Err(receive_marker.clone())),
        observations: Arc::clone(&observations),
    };
    let sender = FakeSender {
        observations: Arc::clone(&observations),
        error: None,
    };
    let mut driver = PingFindNodeDriver::new(receiver, registry.clone(), sender, &table);
    let Err(PingFindNodeDriverError::Receive(ReceiveDispatchError::Transport(error))) =
        driver.drive_one().await
    else {
        panic!("expected receive transport error")
    };
    assert!(Arc::ptr_eq(&error.0, &receive_marker.0));
    assert_eq!(observations.lock().unwrap().events, ["receive"]);

    let observations = Arc::new(Mutex::new(Observations::default()));
    let receiver = FakeReceiver {
        packet: Some(Ok(Packet {
            wire: Vec::new(),
            source,
            reported: Some(MAX_INBOUND_DATAGRAM_BYTES + 1),
        })),
        observations: Arc::clone(&observations),
    };
    let sender = FakeSender {
        observations: Arc::clone(&observations),
        error: None,
    };
    let mut driver = PingFindNodeDriver::new(receiver, registry, sender, &table);
    assert!(matches!(
        driver.drive_one().await,
        Err(PingFindNodeDriverError::Receive(
            ReceiveDispatchError::OverreportedLength {
                reported,
                capacity: MAX_INBOUND_DATAGRAM_BYTES,
            }
        )) if reported == MAX_INBOUND_DATAGRAM_BYTES + 1
    ));
    assert_eq!(observations.lock().unwrap().events, ["receive"]);
    assert_eq!(table.count(), 0);
}

struct GatedReceiver {
    packet: Option<oneshot::Receiver<Packet>>,
    observations: Arc<Mutex<Observations>>,
}

impl DatagramReceiver for GatedReceiver {
    type Error = TransportSentinel;

    fn receive<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ReceivedDatagram, Self::Error>> + Send + 'a>> {
        let mut observations = self.observations.lock().unwrap();
        observations.events.push("receive");
        observations.receive_calls += 1;
        drop(observations);
        let packet = self.packet.take().expect("one receive");
        Box::pin(async move {
            let packet = packet.await.expect("test releases receive");
            buffer[..packet.wire.len()].copy_from_slice(&packet.wire);
            Ok(ReceivedDatagram {
                length: packet.wire.len(),
                source: packet.source,
            })
        })
    }
}

struct GatedSender {
    release: Option<oneshot::Receiver<()>>,
    observations: Arc<Mutex<Observations>>,
}

impl DatagramSender for GatedSender {
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let mut observations = self.observations.lock().unwrap();
        observations.events.push("send");
        observations.destinations.push(destination);
        observations.wires.push(datagram.to_vec());
        drop(observations);
        let release = self.release.take().expect("one send");
        Box::pin(async move {
            release.await.expect("test releases send");
            Ok(())
        })
    }
}

#[tokio::test]
async fn receive_then_send_order_and_backpressure_are_deterministic() {
    let table = NodeTable::new(Id20::ZERO);
    let observations = Arc::new(Mutex::new(Observations::default()));
    let (receive_tx, receive_rx) = oneshot::channel();
    let (send_tx, send_rx) = oneshot::channel();
    let receiver = GatedReceiver {
        packet: Some(receive_rx),
        observations: Arc::clone(&observations),
    };
    let sender = GatedSender {
        release: Some(send_rx),
        observations: Arc::clone(&observations),
    };
    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::new()));
    let mut driver = PingFindNodeDriver::new(receiver, registry, sender, &table);
    let mut drive = Box::pin(driver.drive_one());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(drive.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(observations.lock().unwrap().events, ["receive"]);
    let message = KrpcMessage {
        transaction_id: ByteString::new([1, 2]),
        message_type: ByteString::new(b"q".to_vec()),
        query: ByteString::new(b"ping".to_vec()),
        args: Some(empty_args(None)),
        response: None,
        error: None,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    };
    receive_tx
        .send(Packet {
            wire: message.encode().unwrap(),
            source: "192.0.2.1:6881".parse().unwrap(),
            reported: None,
        })
        .unwrap();
    assert!(matches!(drive.as_mut().poll(&mut context), Poll::Pending));
    {
        let observations = observations.lock().unwrap();
        assert_eq!(observations.events, ["receive", "send"]);
        assert_eq!(observations.receive_calls, 1);
        assert_eq!(observations.wires.len(), 1);
    }
    send_tx.send(()).unwrap();
    assert!(matches!(
        drive.as_mut().poll(&mut context),
        Poll::Ready(Ok(PingFindNodeDriverOutcome::Sent(_)))
    ));
    let observations = observations.lock().unwrap();
    assert_eq!(observations.events, ["receive", "send"]);
    assert_eq!(observations.receive_calls, 1);
    assert_eq!(observations.wires.len(), 1);
}

#[tokio::test]
async fn native_local_causes_and_replies_survive_success_and_send_failure() {
    let fixtures = json_lines::<ResponderFixture>("ping_find_node.jsonl")
        .into_iter()
        .filter(|fixture| fixture.expected.rust_outcome == "nativeIpv6Node")
        .collect::<Vec<_>>();
    assert_eq!(fixtures.len(), 3);
    let generic202 = json_lines::<DispatchFixture>("ping_find_node_dispatch.jsonl")
        .into_iter()
        .find(|fixture| fixture.id == "generic_error_reference_tid")
        .unwrap();
    let generic202_wire_hex = generic202.expected.wire_hex;
    let generic202 = KrpcMessage::decode(&hex::decode(&generic202_wire_hex).unwrap()).unwrap();
    let expected_error = generic202.error.unwrap();

    for fixture in fixtures {
        let mut table = NodeTable::new(fixture.input.origin);
        for node in fixture.input.nodes.as_deref().unwrap_or_default() {
            assert_eq!(table.put(routing_node(*node)), RoutingPutResult::Accepted);
        }
        let request = responder_request(&fixture.input.request);
        let packet = Packet {
            wire: request.encode().unwrap(),
            source: "192.0.2.55:6881".parse().unwrap(),
            reported: None,
        };
        for fail in [false, true] {
            let observations = Arc::new(Mutex::new(Observations::default()));
            let receiver = FakeReceiver {
                packet: Some(Ok(packet.clone())),
                observations: Arc::clone(&observations),
            };
            let marker = TransportSentinel(Arc::new(()));
            let sender = FakeSender {
                observations: Arc::clone(&observations),
                error: fail.then(|| marker.clone()),
            };
            let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::new()));
            let mut driver = PingFindNodeDriver::new(receiver, registry, sender, &table);
            let result = driver.drive_one().await;
            let expected_cause = PingFindNodeError::NativeIpv6Node(routing_node(
                fixture.expected.native_ipv6_node.unwrap(),
            ));
            let (reply, cause) = if fail {
                let Err(PingFindNodeDriverError::Send {
                    prepared,
                    error: PingFindNodeSendError::Transport(error),
                }) = result
                else {
                    panic!("{}: expected failed local reply", fixture.id)
                };
                assert!(Arc::ptr_eq(&error.0, &marker.0));
                let PingFindNodeDispatchOutcome::LocalFailure { reply, cause } = *prepared else {
                    panic!("{}: local cause became separable from reply", fixture.id)
                };
                (reply, cause)
            } else {
                let Ok(PingFindNodeDriverOutcome::Sent(prepared)) = result else {
                    panic!("{}: expected sent local reply", fixture.id)
                };
                let PingFindNodeDispatchOutcome::LocalFailure { reply, cause } = *prepared else {
                    panic!("{}: local cause became separable from reply", fixture.id)
                };
                (reply, cause)
            };
            assert_eq!(cause, expected_cause);
            assert!(reply.message.response.is_none());
            assert_eq!(reply.message.error.as_ref(), Some(&expected_error));
            let observations = observations.lock().unwrap();
            assert_eq!(observations.events, ["receive", "send"]);
            assert_eq!(observations.wires.len(), 1);
            assert_eq!(observations.wires[0], reply.wire().unwrap());
            assert_eq!(hex::encode(&observations.wires[0]), generic202_wire_hex);
        }
    }
}

#[tokio::test]
async fn raw_missing_binary_case_and_unowned_methods_are_preserved_without_send() {
    let table = NodeTable::new(Id20::ZERO);
    let source = SocketAddr::V6(SocketAddrV6::new(
        "2001:db8::7".parse().unwrap(),
        6881,
        0,
        9,
    ));

    for method in [
        b"".as_slice(),
        b"PING".as_slice(),
        b"get_peers".as_slice(),
        &[0, 255],
    ] {
        let message = KrpcMessage {
            transaction_id: ByteString::new([0, 255]),
            message_type: ByteString::new(b"q".to_vec()),
            query: ByteString::new(method.to_vec()),
            args: None,
            response: None,
            error: None,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        };
        let observations = Arc::new(Mutex::new(Observations::default()));
        let receiver = FakeReceiver {
            packet: Some(Ok(Packet {
                wire: message.encode().unwrap(),
                source,
                reported: None,
            })),
            observations: Arc::clone(&observations),
        };
        let sender = FakeSender {
            observations: Arc::clone(&observations),
            error: None,
        };
        let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::new()));
        let mut driver = PingFindNodeDriver::new(receiver, registry, sender, &table);

        let Ok(PingFindNodeDriverOutcome::NoReply(ReceiveDispatchOutcome::Query {
            source: actual_source,
            message: actual_message,
        })) = driver.drive_one().await
        else {
            panic!("unowned raw method must remain an intact query")
        };
        assert_eq!(actual_source, source);
        assert_eq!(*actual_message, message);
        let observations = observations.lock().unwrap();
        assert_eq!(observations.events, ["receive"]);
        assert_eq!(observations.receive_calls, 1);
        assert!(observations.wires.is_empty());
    }

    assert_eq!(table.count(), 0);
}
