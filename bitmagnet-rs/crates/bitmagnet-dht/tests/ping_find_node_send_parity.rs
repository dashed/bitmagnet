//! Differential and ownership proof for the fakeable prepared-reply sender.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bitmagnet_dht::{
    send_ping_find_node_reply, ByteString, CompactAddr, CompactNode, DatagramSender, Id20,
    KrpcError, KrpcMessage, MessageArgs, MessageReturn, NodeTable, PingFindNodeDispatchOutcome,
    PingFindNodeDispatcher, PingFindNodeError, PingFindNodeReply, PingFindNodeSendError,
    RoutingNode, RoutingPutResult,
};
use serde::Deserialize;
use tokio::sync::oneshot;

#[derive(Deserialize)]
struct SendFixture {
    id: String,
    subsystem: String,
    input: SendInput,
    expected: SendExpected,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SendInput {
    destination: FixtureAddr,
    tid_hex: String,
    kind: String,
    #[serde(default)]
    node_addrs: Vec<FixtureAddr>,
    #[serde(default)]
    transport_fail: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SendExpected {
    #[serde(default)]
    wire_hex: String,
    send_calls: usize,
    #[serde(default)]
    go_panicked: bool,
    #[serde(default)]
    transport_error_same: bool,
}

#[derive(Clone, Copy, Deserialize)]
struct FixtureAddr {
    ip: IpAddr,
    port: u16,
    #[serde(default)]
    scope: u32,
}

#[derive(Deserialize)]
struct PriorDispatchFixture {
    id: String,
    input: PriorDispatchInput,
    expected: PriorDispatchExpected,
}

#[derive(Deserialize)]
struct PriorDispatchInput {
    source: FixtureAddr,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PriorDispatchExpected {
    wire_hex: Option<String>,
    generic202_wire_hex: Option<String>,
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

#[derive(Clone, Copy, Deserialize)]
struct FixtureNode {
    id: Id20,
    addr: FixtureAddr,
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
    wire_hex: Option<String>,
}

#[derive(Clone, Debug)]
struct TransportSentinel(Arc<()>);

impl Display for TransportSentinel {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("transport sentinel")
    }
}

impl Error for TransportSentinel {}

#[derive(Default)]
struct CaptureState {
    destinations: Vec<SocketAddr>,
    wires: Vec<Vec<u8>>,
}

struct CaptureSender {
    state: Arc<Mutex<CaptureState>>,
    error: Option<TransportSentinel>,
}

impl CaptureSender {
    fn new(error: Option<TransportSentinel>) -> (Self, Arc<Mutex<CaptureState>>) {
        let state = Arc::new(Mutex::new(CaptureState::default()));
        (
            Self {
                state: Arc::clone(&state),
                error,
            },
            state,
        )
    }
}

impl DatagramSender for CaptureSender {
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let mut state = self.state.lock().unwrap();
        state.destinations.push(destination);
        state.wires.push(datagram.to_vec());
        drop(state);
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
        IpAddr::V4(ip) => {
            assert_eq!(value.scope, 0);
            SocketAddr::V4(SocketAddrV4::new(ip, value.port))
        }
        IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(ip, value.port, 0, value.scope)),
    }
}

fn id(last: u8) -> Id20 {
    let mut bytes = [0; 20];
    bytes[19] = last;
    Id20::from_slice(&bytes).unwrap()
}

fn empty_return(id: Id20) -> MessageReturn {
    MessageReturn {
        id,
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

fn send_fixture_reply(fixture: &SendFixture) -> PingFindNodeReply {
    let response = match fixture.input.kind.as_str() {
        "success" => {
            let mut response = empty_return(id(0x90));
            if !fixture.input.node_addrs.is_empty() {
                response.nodes = Some(
                    fixture
                        .input
                        .node_addrs
                        .iter()
                        .enumerate()
                        .map(|(index, addr)| CompactNode {
                            id: id(u8::try_from(index + 1).unwrap()),
                            addr: CompactAddr {
                                ip: addr.ip,
                                port: addr.port,
                            },
                        })
                        .collect(),
                );
            }
            Some(response)
        }
        "error" => None,
        other => panic!("unknown fixture kind {other}"),
    };
    let error = (fixture.input.kind == "error").then(|| KrpcError {
        code: 203,
        message: ByteString::new(b"missing arguments".to_vec()),
    });
    PingFindNodeReply {
        destination: socket_addr(fixture.input.destination),
        message: KrpcMessage {
            transaction_id: ByteString::new(hex::decode(&fixture.input.tid_hex).unwrap()),
            message_type: ByteString::new(b"r".to_vec()),
            query: ByteString::default(),
            args: None,
            response,
            error,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        },
    }
}

#[tokio::test]
async fn actual_go_server_send_matches_rust_call_order_bytes_and_errors() {
    let fixtures = json_lines::<SendFixture>("ping_find_node_send.jsonl");
    assert_eq!(fixtures.len(), 6);

    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_ping_find_node_send");
        let reply = send_fixture_reply(&fixture);
        let marker = TransportSentinel(Arc::new(()));
        let (mut sender, state) =
            CaptureSender::new(fixture.input.transport_fail.then(|| marker.clone()));
        let result = send_ping_find_node_reply(&mut sender, &reply).await;
        let state = state.lock().unwrap();
        assert_eq!(
            state.wires.len(),
            fixture.expected.send_calls,
            "{}",
            fixture.id
        );

        if fixture.expected.go_panicked {
            assert!(matches!(result, Err(PingFindNodeSendError::Encode(_))));
            assert!(state.wires.is_empty());
            continue;
        }

        assert_eq!(state.destinations, [reply.destination], "{}", fixture.id);
        assert_eq!(
            hex::encode(&state.wires[0]),
            fixture.expected.wire_hex,
            "{}",
            fixture.id,
        );
        if fixture.input.transport_fail {
            assert!(fixture.expected.transport_error_same);
            let Err(PingFindNodeSendError::Transport(error)) = result else {
                panic!("{}: expected transport error", fixture.id)
            };
            assert!(Arc::ptr_eq(&error.0, &marker.0));
        } else {
            result.unwrap();
        }
    }
}

struct GatedSender {
    calls: Arc<AtomicUsize>,
    observations: Arc<Mutex<CaptureState>>,
    release: Option<oneshot::Receiver<()>>,
}

impl DatagramSender for GatedSender {
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut observations = self.observations.lock().unwrap();
        observations.destinations.push(destination);
        observations.wires.push(datagram.to_vec());
        drop(observations);
        let release = self.release.take().expect("only one send");
        Box::pin(async move {
            release.await.expect("test releases sender");
            Ok(())
        })
    }
}

#[tokio::test]
async fn helper_awaits_backpressure_and_preserves_exact_flowinfo() {
    let reply = PingFindNodeReply {
        destination: SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 6881, 17, 9)),
        message: KrpcMessage {
            transaction_id: ByteString::new([0, 255, 1]),
            message_type: ByteString::new(b"r".to_vec()),
            query: ByteString::default(),
            args: None,
            response: Some(empty_return(id(0x90))),
            error: None,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        },
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let observations = Arc::new(Mutex::new(CaptureState::default()));
    let (release_tx, release_rx) = oneshot::channel();
    let mut sender = GatedSender {
        calls: Arc::clone(&calls),
        observations: Arc::clone(&observations),
        release: Some(release_rx),
    };
    let mut send = Box::pin(send_ping_find_node_reply(&mut sender, &reply));

    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(send.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        observations.lock().unwrap().destinations,
        [reply.destination]
    );
    release_tx.send(()).unwrap();
    send.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
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
async fn all_prior_dispatch_and_responder_replies_send_once_without_consuming_causes() {
    let dispatch = json_lines::<PriorDispatchFixture>("ping_find_node_dispatch.jsonl");
    assert_eq!(dispatch.len(), 10);
    for fixture in dispatch {
        let wire = fixture
            .expected
            .wire_hex
            .or(fixture.expected.generic202_wire_hex)
            .unwrap();
        let reply = PingFindNodeReply {
            destination: socket_addr(fixture.input.source),
            message: KrpcMessage::decode(&hex::decode(&wire).unwrap()).unwrap(),
        };
        let (mut sender, state) = CaptureSender::new(None);
        send_ping_find_node_reply(&mut sender, &reply)
            .await
            .unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.destinations, [reply.destination], "{}", fixture.id);
        assert_eq!(hex::encode(&state.wires[0]), wire, "{}", fixture.id);
    }

    let responder = json_lines::<ResponderFixture>("ping_find_node.jsonl");
    assert_eq!(responder.len(), 14);
    let generic202 = json_lines::<PriorDispatchFixture>("ping_find_node_dispatch.jsonl")
        .into_iter()
        .find(|fixture| fixture.id == "generic_error_reference_tid")
        .unwrap()
        .expected
        .wire_hex
        .unwrap();
    let destination: SocketAddr = "[fe80::123%9]:456".parse().unwrap();
    let mut native_count = 0;

    for fixture in responder {
        let mut table = NodeTable::new(fixture.input.origin);
        for node in fixture.input.nodes.as_deref().unwrap_or_default() {
            assert_eq!(table.put(routing_node(*node)), RoutingPutResult::Accepted);
        }
        let request = responder_request(&fixture.input.request);
        let outcome = PingFindNodeDispatcher::new(&table)
            .dispatch(destination, &request)
            .unwrap();
        let (reply, cause) = match &outcome {
            PingFindNodeDispatchOutcome::Reply(reply) => (reply, None),
            PingFindNodeDispatchOutcome::LocalFailure { reply, cause } => {
                native_count += 1;
                assert_eq!(
                    cause,
                    &PingFindNodeError::NativeIpv6Node(routing_node(
                        fixture.expected.native_ipv6_node.unwrap()
                    ))
                );
                (reply, Some(cause))
            }
        };
        let expected_wire = if fixture.expected.rust_outcome == "nativeIpv6Node" {
            generic202.as_str()
        } else {
            fixture.expected.wire_hex.as_deref().unwrap()
        };
        let (mut sender, state) = CaptureSender::new(None);
        send_ping_find_node_reply(&mut sender, reply).await.unwrap();
        assert_eq!(
            hex::encode(&state.lock().unwrap().wires[0]),
            expected_wire,
            "{}",
            fixture.id,
        );
        if let Some(cause) = cause {
            assert!(matches!(cause, PingFindNodeError::NativeIpv6Node(_)));
            let marker = TransportSentinel(Arc::new(()));
            let (mut failing_sender, failed_state) = CaptureSender::new(Some(marker.clone()));
            let Err(PingFindNodeSendError::Transport(error)) =
                send_ping_find_node_reply(&mut failing_sender, reply).await
            else {
                panic!("{}: expected one transport failure", fixture.id)
            };
            assert!(Arc::ptr_eq(&error.0, &marker.0));
            let failed_state = failed_state.lock().unwrap();
            assert_eq!(failed_state.destinations, [reply.destination]);
            assert_eq!(failed_state.wires.len(), 1);
            assert_eq!(hex::encode(&failed_state.wires[0]), expected_wire);
            assert!(matches!(cause, PingFindNodeError::NativeIpv6Node(_)));
        }
        assert_eq!(
            table.count(),
            fixture.input.nodes.as_deref().unwrap_or_default().len(),
            "{}",
            fixture.id,
        );
    }
    assert_eq!(native_count, 3);
}

#[test]
fn fixture_ids_are_unique() {
    let ids = json_lines::<SendFixture>("ping_find_node_send.jsonl")
        .into_iter()
        .map(|fixture| fixture.id)
        .collect::<VecDeque<_>>();
    let unique = ids.iter().collect::<std::collections::HashSet<_>>();
    assert_eq!(ids.len(), unique.len());
}
