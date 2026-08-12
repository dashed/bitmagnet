//! Real-Go projection parity and no-socket lifecycle gates for the typed client.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use bitmagnet_dht::{
    ByteString, CompactAddr, CompactNode, DatagramSender, DeliveryOutcome, FindNodeResult, Id20,
    KrpcError, KrpcMessage, MessageReturn, PingFindNodeClient, PingFindNodeClientError,
    QuerySendError, RegisterError, RoutingNode, TransactionId, TransactionIdIssuer,
    TransactionIdSourceError, TransactionRegistry,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    id: String,
    subsystem: String,
    input: FixtureInput,
    expected: FixtureExpected,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureInput {
    method: String,
    transaction_id_hex: String,
    local_id: Id20,
    remote: FixtureAddr,
    target: Option<Id20>,
    response_id: Id20,
    response_nodes_presence: String,
    response_nodes: Option<Vec<FixtureNode>>,
    response_nodes6_presence: String,
    response_nodes6: Option<Vec<FixtureNode>>,
    #[serde(default)]
    include_ignored_fields: bool,
    #[serde(default)]
    fail_query: bool,
    #[serde(default)]
    pre_cancelled: bool,
    #[serde(default)]
    typed_nil_error: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureExpected {
    query_calls: usize,
    query_method: String,
    query_local_id: Id20,
    query_target: Id20,
    query_remote: FixtureAddr,
    query_wire_hex: String,
    outcome: String,
    result_id: Id20,
    result_nodes: Option<Vec<FixtureNode>>,
    error_identity_preserved: bool,
    #[serde(default)]
    error_is_typed_nil: bool,
    result_was_zero: bool,
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
        formatter.write_str("ping/find-node client transport sentinel")
    }
}

impl Error for TransportSentinel {}

struct FixtureSender<I> {
    registry: TransactionRegistry<I>,
    response: KrpcMessage,
    response_source: SocketAddr,
    error: Option<TransportSentinel>,
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
        let query = KrpcMessage::decode(datagram).unwrap();
        self.response.transaction_id = query.transaction_id;
        if self.error.is_none() {
            assert_eq!(
                self.registry
                    .deliver(self.response_source, self.response.clone()),
                DeliveryOutcome::Delivered
            );
        }
        let error = self.error.clone();
        Box::pin(async move { error.map_or(Ok(()), Err) })
    }
}

fn fixtures() -> Vec<Fixture> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht/ping_find_node_client.jsonl");
    BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect()
}

fn transaction_id(value: &str) -> TransactionId {
    TransactionId::from_slice(&hex::decode(value).unwrap()).unwrap()
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

fn response_for(input: &FixtureInput) -> MessageReturn {
    let nodes = match input.response_nodes_presence.as_str() {
        "" => None,
        "empty" => Some(Vec::new()),
        "present" => Some(
            input
                .response_nodes
                .as_deref()
                .unwrap_or_default()
                .iter()
                .copied()
                .map(fixture_node)
                .collect(),
        ),
        other => panic!("unexpected nodes presence {other:?}"),
    };
    let nodes6 = match input.response_nodes6_presence.as_str() {
        "" => None,
        "empty" => Some(Vec::new()),
        "present" => Some(
            input
                .response_nodes6
                .as_deref()
                .unwrap_or_default()
                .iter()
                .copied()
                .map(fixture_node)
                .collect(),
        ),
        other => panic!("unexpected nodes6 presence {other:?}"),
    };
    MessageReturn {
        id: input.response_id,
        nodes,
        nodes6,
        token: input
            .include_ignored_fields
            .then(|| ByteString::new(b"ignored-token")),
        values: input.include_ignored_fields.then(|| {
            vec![CompactAddr {
                ip: "198.51.100.9".parse().unwrap(),
                port: 9999,
            }]
        }),
        interval: input.include_ignored_fields.then_some(17),
        num: input.include_ignored_fields.then_some(19),
        samples: input.include_ignored_fields.then(|| vec![id(0x55)]),
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

fn id(last: u8) -> Id20 {
    let mut value = [0; 20];
    value[19] = last;
    Id20::from_slice(&value).unwrap()
}

fn routing_nodes(values: Option<&[FixtureNode]>) -> Vec<RoutingNode> {
    values
        .unwrap_or_default()
        .iter()
        .map(|value| RoutingNode {
            id: value.id,
            addr: SocketAddr::new(value.addr.ip, value.addr.port),
        })
        .collect()
}

#[tokio::test]
async fn actual_go_server_adapter_projection_matches_the_typed_client() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 11);
    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_ping_find_node_client");
        assert_eq!(fixture.expected.query_calls, 1, "{}", fixture.id);
        assert_eq!(fixture.input.method, fixture.expected.query_method);
        assert_eq!(fixture.input.local_id, fixture.expected.query_local_id);
        assert_eq!(fixture.expected.query_remote, fixture.input.remote);

        if fixture.input.pre_cancelled {
            assert_eq!(fixture.expected.outcome, "context_cancelled");
            assert!(fixture.expected.result_was_zero);
            assert_eq!(fixture.expected.query_calls, 1);
            continue;
        }
        if fixture.input.typed_nil_error {
            assert_eq!(fixture.expected.outcome, "typed_nil_error");
            assert!(fixture.expected.error_is_typed_nil);
            assert!(fixture.expected.result_was_zero);
            assert_eq!(fixture.expected.query_calls, 1);
            continue;
        }

        let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([Ok(
            transaction_id(&fixture.input.transaction_id_hex),
        )])));
        let sentinel = TransportSentinel(Arc::new(()));
        let remote = fixture_addr(fixture.input.remote);
        let response = response_message(Some(response_for(&fixture.input)), None);
        let mut sender = FixtureSender {
            registry: registry.clone(),
            response,
            response_source: remote,
            error: fixture.input.fail_query.then(|| sentinel.clone()),
            calls: 0,
            destinations: Vec::new(),
            wires: Vec::new(),
        };
        let client =
            PingFindNodeClient::new(fixture.input.local_id, &registry, Duration::from_secs(4));

        match fixture.input.method.as_str() {
            "ping" => {
                let result = client.ping(&mut sender, remote).await;
                if fixture.input.fail_query {
                    let error = result.unwrap_err();
                    assert!(Error::source(&error).is_some());
                    let PingFindNodeClientError::QuerySend(QuerySendError::Transport(actual)) =
                        error
                    else {
                        panic!("{}: expected nested transport failure", fixture.id)
                    };
                    assert!(Arc::ptr_eq(&actual.0, &sentinel.0));
                    assert!(fixture.expected.error_identity_preserved);
                    assert!(fixture.expected.result_was_zero);
                    assert_eq!(fixture.expected.outcome, "query_error");
                } else {
                    let result = result.unwrap();
                    assert_eq!(result.id, fixture.expected.result_id, "{}", fixture.id);
                }
            }
            "find_node" => {
                let target = fixture.input.target.unwrap();
                let result = client.find_node(&mut sender, remote, target).await.unwrap();
                assert_eq!(
                    result,
                    FindNodeResult {
                        id: fixture.expected.result_id,
                        nodes: routing_nodes(fixture.expected.result_nodes.as_deref()),
                    },
                    "{}",
                    fixture.id
                );
            }
            other => panic!("unexpected method {other:?}"),
        }
        assert_eq!(sender.calls, fixture.expected.query_calls, "{}", fixture.id);
        assert_eq!(sender.destinations, vec![remote], "{}", fixture.id);
        assert_eq!(
            hex::encode(&sender.wires[0]),
            fixture.expected.query_wire_hex,
            "{}",
            fixture.id
        );
        let sent = KrpcMessage::decode(&sender.wires[0]).unwrap();
        assert_eq!(
            sent.query.as_bytes(),
            fixture.expected.query_method.as_bytes()
        );
        assert_eq!(
            sent.args.as_ref().unwrap().id,
            fixture.expected.query_local_id
        );
        assert_eq!(
            sent.args.as_ref().unwrap().target.unwrap_or(Id20::ZERO),
            fixture.expected.query_target
        );
        if fixture.input.method == "find_node" && fixture.input.target == Some(Id20::ZERO) {
            assert!(sent.args.unwrap().target.is_none());
            assert!(!sender.wires[0]
                .windows(b"target".len())
                .any(|window| window == b"target"));
        }
        assert_eq!(registry.pending_count(), 0, "{}", fixture.id);
    }
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

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::new(NoopWake));
    future.poll(&mut Context::from_waker(&waker))
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
    let client = PingFindNodeClient::new(id(1), &registry, Duration::from_secs(4));
    let remote = "192.0.2.1:1".parse().unwrap();
    let mut query = Box::pin(client.ping(&mut sender, remote));
    assert!(poll_once(query.as_mut()).is_pending());
    assert_eq!(registry.pending_count(), 1);
    tokio::time::advance(Duration::from_secs(400)).await;
    assert!(poll_once(query.as_mut()).is_pending());
    released.store(true, Ordering::SeqCst);
    assert!(poll_once(query.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(matches!(
        poll_once(query.as_mut()),
        Poll::Ready(Err(PingFindNodeClientError::Timeout))
    ));
    drop(query);
    assert_eq!(registry.pending_count(), 0);

    let mut sender = GateSender {
        released: Arc::new(AtomicBool::new(true)),
        calls: 0,
    };
    let zero = PingFindNodeClient::new(id(1), &registry, Duration::ZERO)
        .ping(&mut sender, remote)
        .await;
    assert!(matches!(zero, Err(PingFindNodeClientError::Timeout)));
    assert_eq!(registry.pending_count(), 0);
}

#[tokio::test]
async fn registration_failure_stays_nested_and_never_calls_the_sender() {
    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::new()));
    registry.close();
    let mut sender = GateSender {
        released: Arc::new(AtomicBool::new(true)),
        calls: 0,
    };
    let result = PingFindNodeClient::new(id(1), &registry, Duration::from_secs(4))
        .ping(&mut sender, "192.0.2.1:1".parse().unwrap())
        .await;
    let error = result.unwrap_err();
    assert!(Error::source(&error).is_some());
    assert!(matches!(
        error,
        PingFindNodeClientError::QuerySend(QuerySendError::Register(RegisterError::RegistryClosed))
    ));
    assert_eq!(sender.calls, 0);
    assert_eq!(registry.pending_count(), 0);
}

#[test]
fn dropping_an_unpolled_query_future_sends_and_registers_nothing() {
    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([Ok(
        TransactionId::from(*b"U1"),
    )])));
    let mut sender = GateSender {
        released: Arc::new(AtomicBool::new(true)),
        calls: 0,
    };
    let client = PingFindNodeClient::new(id(1), &registry, Duration::from_secs(4));
    let future = client.ping(&mut sender, "192.0.2.1:1".parse().unwrap());
    drop(future);
    assert_eq!(sender.calls, 0);
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
        let query = KrpcMessage::decode(datagram).unwrap();
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

#[tokio::test(start_paused = true)]
async fn terminal_wait_outcomes_are_typed_and_retain_missing_envelopes() {
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
        let expected = match mode {
            TerminalMode::RemoteError => "remote_error",
            TerminalMode::MissingReturn => "missing_return",
            TerminalMode::MissingError => "missing_error",
            TerminalMode::Close => "closed",
            TerminalMode::WrongSource => "timeout",
        };
        let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([Ok(tid)])));
        let mut sender = TerminalSender {
            registry: registry.clone(),
            mode,
        };
        let result = PingFindNodeClient::new(id(1), &registry, Duration::from_secs(4))
            .ping(&mut sender, remote)
            .await;
        match (expected, result) {
            (
                "remote_error",
                Err(PingFindNodeClientError::RemoteError {
                    response_source,
                    message,
                    error,
                }),
            ) => {
                assert_eq!(response_source, remote);
                assert_eq!(message.transaction_id.as_bytes(), tid.as_bytes());
                assert_eq!(message.message_type.as_bytes(), b"e");
                assert_eq!(error.code, 201);
            }
            (
                "missing_return",
                Err(PingFindNodeClientError::MissingReturnBody {
                    response_source,
                    message,
                }),
            ) => {
                assert_eq!(response_source, remote);
                assert_eq!(message.transaction_id.as_bytes(), tid.as_bytes());
                assert_eq!(message.message_type.as_bytes(), b"r");
            }
            (
                "missing_error",
                Err(PingFindNodeClientError::MissingErrorBody {
                    response_source,
                    message,
                }),
            ) => {
                assert_eq!(response_source, remote);
                assert_eq!(message.transaction_id.as_bytes(), tid.as_bytes());
                assert_eq!(message.message_type.as_bytes(), b"e");
            }
            ("closed", Err(PingFindNodeClientError::RegistryClosed)) => {}
            ("timeout", Err(PingFindNodeClientError::Timeout)) => {}
            (_, other) => panic!("unexpected {expected} terminal outcome: {other:?}"),
        }
        assert_eq!(registry.pending_count(), 0);
    }
}

#[tokio::test]
async fn response_correlation_normalizes_only_the_inbound_source() {
    struct NormalizingSender<I> {
        registry: TransactionRegistry<I>,
        response_source: SocketAddr,
        destination: Option<SocketAddr>,
    }
    impl<I: TransactionIdIssuer + 'static> DatagramSender for NormalizingSender<I> {
        type Error = TransportSentinel;
        fn send<'a>(
            &'a mut self,
            destination: SocketAddr,
            datagram: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            self.destination = Some(destination);
            let query = KrpcMessage::decode(datagram).unwrap();
            let mut response = response_message(Some(empty_return(id(9))), None);
            response.transaction_id = query.transaction_id;
            assert_eq!(
                self.registry.deliver(self.response_source, response),
                DeliveryOutcome::Delivered
            );
            Box::pin(async { Ok(()) })
        }
    }

    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([
        Ok(TransactionId::from(*b"N1")),
        Ok(TransactionId::from(*b"N2")),
    ])));
    let mapped = SocketAddr::V6(SocketAddrV6::new(
        Ipv6Addr::new(0, 0, 0, 0, 0, u16::MAX, 0xc000, 0x0201),
        6881,
        77,
        0,
    ));
    let mut sender = NormalizingSender {
        registry: registry.clone(),
        response_source: "192.0.2.1:6881".parse().unwrap(),
        destination: None,
    };
    PingFindNodeClient::new(id(1), &registry, Duration::from_secs(4))
        .ping(&mut sender, mapped)
        .await
        .unwrap();
    assert_eq!(sender.destination, Some(mapped));

    let scoped = SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 6882, 88, 7));
    sender.response_source =
        SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 6882, 0, 7));
    PingFindNodeClient::new(id(1), &registry, Duration::from_secs(4))
        .ping(&mut sender, scoped)
        .await
        .unwrap();
    assert_eq!(sender.destination, Some(scoped));
}

#[tokio::test]
async fn transport_after_delivery_wins_and_cleans_the_buffered_response() {
    struct DeliverThenFail<I> {
        registry: TransactionRegistry<I>,
        sentinel: TransportSentinel,
    }
    impl<I: TransactionIdIssuer + 'static> DatagramSender for DeliverThenFail<I> {
        type Error = TransportSentinel;
        fn send<'a>(
            &'a mut self,
            destination: SocketAddr,
            datagram: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            let query = KrpcMessage::decode(datagram).unwrap();
            let mut response = response_message(Some(empty_return(id(9))), None);
            response.transaction_id = query.transaction_id;
            assert_eq!(
                self.registry.deliver(destination, response),
                DeliveryOutcome::Delivered
            );
            let error = self.sentinel.clone();
            Box::pin(async move { Err(error) })
        }
    }
    let registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([Ok(
        TransactionId::from(*b"E1"),
    )])));
    let sentinel = TransportSentinel(Arc::new(()));
    let mut sender = DeliverThenFail {
        registry: registry.clone(),
        sentinel: sentinel.clone(),
    };
    let result = PingFindNodeClient::new(id(1), &registry, Duration::from_secs(4))
        .ping(&mut sender, "192.0.2.1:1".parse().unwrap())
        .await;
    let Err(PingFindNodeClientError::QuerySend(QuerySendError::Transport(actual))) = result else {
        panic!("transport failure must win")
    };
    assert!(Arc::ptr_eq(&actual.0, &sentinel.0));
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
            PingFindNodeClient::new(id(1), &task_registry, Duration::from_secs(60))
                .ping(&mut sender, "192.0.2.1:1".parse().unwrap())
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

#[test]
fn projected_compact_ipv6_addresses_have_zero_scope_and_flowinfo() {
    let node = FixtureNode {
        id: id(1),
        addr: FixtureAddr {
            ip: "2001:db8::1".parse().unwrap(),
            port: 6881,
            scope: 99,
        },
    };
    let projected = routing_nodes(Some(&[node]));
    let SocketAddr::V6(addr) = projected[0].addr else {
        panic!("expected IPv6")
    };
    assert_eq!(addr.flowinfo(), 0);
    assert_eq!(addr.scope_id(), 0);
}
