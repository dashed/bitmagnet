//! Differential and RAII proof for one registered asynchronous query send.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::future::{pending, Future};
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use bitmagnet_dht::{
    register_and_send_query, ByteString, DatagramSender, DeliveryOutcome, Id20, KrpcMessage,
    MessageArgs, MessageReturn, QuerySendError, RegisterError, TransactionId, TransactionIdIssuer,
    TransactionIdSourceError, TransactionRegistry, TransactionWaitOutcome,
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
    issuer_tids_hex: Vec<String>,
    #[serde(default)]
    preexisting_tid_hex: String,
    remote: FixtureAddr,
    query_hex: String,
    local_id: Id20,
    target: Option<Id20>,
    #[serde(default)]
    deliver_during_send: bool,
    #[serde(default)]
    fail_send: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Expected {
    tid_hex: String,
    wire_hex: String,
    destination: FixtureAddr,
    registered_at_send: bool,
    #[serde(default)]
    delivery_buffered: bool,
    send_calls: usize,
    issuer_calls: usize,
    outcome: String,
    #[serde(default)]
    response_id: Option<Id20>,
    #[serde(default)]
    transport_error_identity: bool,
    owned_pending_after: bool,
    total_pending_after: usize,
    events: Vec<String>,
}

#[derive(Clone, Copy, Deserialize)]
struct FixtureAddr {
    ip: IpAddr,
    port: u16,
    #[serde(default)]
    scope: u32,
}

struct ScriptedIssuer {
    ids: VecDeque<Result<TransactionId, TransactionIdSourceError>>,
    calls: Arc<AtomicUsize>,
}

impl TransactionIdIssuer for ScriptedIssuer {
    fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.ids
            .pop_front()
            .unwrap_or_else(|| Err(TransactionIdSourceError::new("scripted issuer exhausted")))
    }
}

#[derive(Clone, Debug)]
struct TransportSentinel(Arc<()>);

impl Display for TransportSentinel {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("query send transport sentinel")
    }
}

impl Error for TransportSentinel {}

#[derive(Default)]
struct Observations {
    events: Vec<&'static str>,
    destinations: Vec<SocketAddr>,
    wires: Vec<Vec<u8>>,
    registered_at_send: Vec<bool>,
}

struct OracleSender<I> {
    registry: TransactionRegistry<I>,
    observations: Arc<Mutex<Observations>>,
    deliver_during_send: bool,
    response_id: Id20,
    error: Option<TransportSentinel>,
}

impl<I> DatagramSender for OracleSender<I>
where
    I: TransactionIdIssuer + 'static,
{
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let message = KrpcMessage::decode(datagram).expect("query sender receives canonical wire");
        let transaction_id = TransactionId::from_slice(message.transaction_id.as_bytes()).unwrap();
        let registered = self.registry.is_pending(transaction_id);
        let mut observations = self.observations.lock().unwrap();
        observations.events.push("send");
        observations.destinations.push(destination);
        observations.wires.push(datagram.to_vec());
        observations.registered_at_send.push(registered);
        drop(observations);
        if self.deliver_during_send {
            let response = KrpcMessage {
                transaction_id: message.transaction_id,
                message_type: ByteString::new(b"r"),
                query: ByteString::default(),
                args: None,
                response: Some(empty_return(self.response_id)),
                error: None,
                observed_addr: None,
                read_only: false,
                client_id: ByteString::default(),
            };
            assert_eq!(
                self.registry.deliver(destination, response),
                DeliveryOutcome::Delivered
            );
            self.observations.lock().unwrap().events.push("deliver");
        }
        let error = self.error.clone();
        Box::pin(async move { error.map_or(Ok(()), Err) })
    }
}

fn fixtures() -> Vec<Fixture> {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../testdata/parity/dht/query_send.jsonl");
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

fn query_args(id: Id20, target: Option<Id20>) -> MessageArgs {
    MessageArgs {
        id,
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

fn transaction_ids(values: &[String]) -> VecDeque<Result<TransactionId, TransactionIdSourceError>> {
    values
        .iter()
        .map(|value| {
            TransactionId::from_slice(&hex::decode(value).unwrap()).map_err(|error| {
                TransactionIdSourceError::new(format!("invalid fixture transaction ID: {error}"))
            })
        })
        .collect()
}

#[tokio::test]
async fn actual_go_server_query_matrix_matches_registered_async_send() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 6);

    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_query_send");
        assert!(fixture.expected.registered_at_send);
        assert_eq!(
            fixture.expected.delivery_buffered, fixture.input.deliver_during_send,
            "{}: Go response-during-Send evidence",
            fixture.id,
        );
        assert_eq!(fixture.expected.send_calls, 1);
        assert!(!fixture.expected.owned_pending_after);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut issuer_ids = transaction_ids(&fixture.input.issuer_tids_hex);
        if !fixture.input.preexisting_tid_hex.is_empty() {
            issuer_ids.push_front(Ok(TransactionId::from_slice(
                &hex::decode(&fixture.input.preexisting_tid_hex).unwrap(),
            )
            .unwrap()));
        }
        let registry = TransactionRegistry::new(ScriptedIssuer {
            ids: issuer_ids,
            calls: Arc::clone(&calls),
        });
        let preexisting = if fixture.input.preexisting_tid_hex.is_empty() {
            None
        } else {
            let registered = registry
                .register(
                    "198.51.100.1:1".parse().unwrap(),
                    ByteString::new(b"occupied"),
                    query_args(Id20::ZERO, None),
                )
                .unwrap();
            assert_eq!(
                registered.transaction_id().to_hex(),
                fixture.input.preexisting_tid_hex
            );
            Some(registered)
        };
        let observations = Arc::new(Mutex::new(Observations::default()));
        let marker = TransportSentinel(Arc::new(()));
        let mut sender = OracleSender {
            registry: registry.clone(),
            observations: Arc::clone(&observations),
            deliver_during_send: fixture.input.deliver_during_send,
            response_id: fixture.expected.response_id.unwrap_or(Id20::ZERO),
            error: fixture.input.fail_send.then(|| marker.clone()),
        };
        let result = register_and_send_query(
            &registry,
            &mut sender,
            socket_addr(fixture.input.remote),
            ByteString::new(hex::decode(&fixture.input.query_hex).unwrap()),
            query_args(fixture.input.local_id, fixture.input.target),
        )
        .await;

        match fixture.expected.outcome.as_str() {
            "response" => {
                let pending = result.unwrap();
                assert_eq!(pending.transaction_id().to_hex(), fixture.expected.tid_hex);
                assert!(registry.is_pending(pending.transaction_id()));
                let TransactionWaitOutcome::Response { response, .. } =
                    pending.wait(Duration::from_secs(1)).await
                else {
                    panic!("{}: expected buffered response", fixture.id)
                };
                assert_eq!(response.id, fixture.expected.response_id.unwrap());
            }
            "transport_error" => {
                let Err(QuerySendError::Transport(error)) = result else {
                    panic!("{}: expected typed transport error", fixture.id)
                };
                assert!(fixture.expected.transport_error_identity);
                assert!(Arc::ptr_eq(&error.0, &marker.0));
            }
            outcome => panic!("{}: unexpected outcome {outcome}", fixture.id),
        }
        observations.lock().unwrap().events.push("return");

        assert_eq!(
            calls.load(Ordering::SeqCst) - usize::from(preexisting.is_some()),
            fixture.expected.issuer_calls,
        );
        assert_eq!(
            registry.pending_count(),
            fixture.expected.total_pending_after
        );
        let observations = observations.lock().unwrap();
        assert_eq!(observations.registered_at_send, [true]);
        assert_eq!(
            observations.events,
            fixture
                .expected
                .events
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            "{}",
            fixture.id,
        );
        assert_eq!(
            observations.destinations,
            [socket_addr(fixture.expected.destination)]
        );
        assert_eq!(
            hex::encode(&observations.wires[0]),
            fixture.expected.wire_hex
        );
        drop(observations);
        drop(preexisting);
        assert_eq!(registry.pending_count(), 0);
    }
}

struct GatedSender<I> {
    registry: TransactionRegistry<I>,
    release: Option<oneshot::Receiver<Result<(), TransportSentinel>>>,
    calls: Arc<AtomicUsize>,
}

impl<I> DatagramSender for GatedSender<I>
where
    I: TransactionIdIssuer + 'static,
{
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let message = KrpcMessage::decode(datagram).unwrap();
        let transaction_id = TransactionId::from_slice(message.transaction_id.as_bytes()).unwrap();
        assert!(self.registry.is_pending(transaction_id));
        let release = self.release.take().unwrap();
        Box::pin(async move { release.await.expect("test releases sender") })
    }
}

fn one_id_registry(id: [u8; 2]) -> TransactionRegistry<ScriptedIssuer> {
    TransactionRegistry::new(ScriptedIssuer {
        ids: VecDeque::from([Ok(TransactionId::from(id))]),
        calls: Arc::new(AtomicUsize::new(0)),
    })
}

#[tokio::test]
async fn sender_backpressure_buffers_a_response_until_successful_send() {
    let registry = one_id_registry(*b"A1");
    let calls = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = oneshot::channel();
    let mut sender = GatedSender {
        registry: registry.clone(),
        release: Some(release_rx),
        calls: Arc::clone(&calls),
    };
    let mut future = Box::pin(register_and_send_query(
        &registry,
        &mut sender,
        "192.0.2.1:1".parse().unwrap(),
        ByteString::new(b"ping"),
        query_args(Id20::ZERO, None),
    ));
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(registry.pending_count(), 1);
    let response = KrpcMessage {
        transaction_id: ByteString::new(b"A1"),
        message_type: ByteString::new(b"r"),
        query: ByteString::default(),
        args: None,
        response: Some(empty_return(Id20::ZERO)),
        error: None,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    };
    assert_eq!(
        registry.deliver("192.0.2.1:1".parse().unwrap(), response),
        DeliveryOutcome::Delivered
    );
    assert_eq!(registry.pending_count(), 1);
    release_tx.send(Ok(())).unwrap();
    let Poll::Ready(Ok(pending)) = future.as_mut().poll(&mut context) else {
        panic!("released query send did not complete")
    };
    assert!(matches!(
        pending.wait(Duration::from_secs(1)).await,
        TransactionWaitOutcome::Response { .. }
    ));
    assert_eq!(registry.pending_count(), 0);
}

#[tokio::test]
async fn dropping_a_backpressured_send_future_cleans_the_registration() {
    let registry = one_id_registry(*b"A1");
    let calls = Arc::new(AtomicUsize::new(0));
    let (_release_tx, release_rx) = oneshot::channel();
    let mut sender = GatedSender {
        registry: registry.clone(),
        release: Some(release_rx),
        calls: Arc::clone(&calls),
    };
    let mut future = Box::pin(register_and_send_query(
        &registry,
        &mut sender,
        "192.0.2.1:1".parse().unwrap(),
        ByteString::new(b"ping"),
        query_args(Id20::ZERO, None),
    ));
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(registry.pending_count(), 1);
    drop(future);
    assert_eq!(registry.pending_count(), 0);
}

struct NeverSender {
    calls: Arc<AtomicUsize>,
    entered: Option<oneshot::Sender<()>>,
}

impl DatagramSender for NeverSender {
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        _datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.take().unwrap().send(()).unwrap();
        Box::pin(pending())
    }
}

#[tokio::test]
async fn aborting_a_pending_send_future_cleans_the_registration() {
    let registry = one_id_registry(*b"A1");
    let outside = registry.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = oneshot::channel();
    let task = tokio::spawn({
        let calls = Arc::clone(&calls);
        async move {
            let mut sender = NeverSender {
                calls,
                entered: Some(entered_tx),
            };
            register_and_send_query(
                &registry,
                &mut sender,
                "192.0.2.1:1".parse().unwrap(),
                ByteString::new(b"ping"),
                query_args(Id20::ZERO, None),
            )
            .await
        }
    });
    entered_rx.await.unwrap();
    assert_eq!(outside.pending_count(), 1);
    task.abort();
    let Err(error) = task.await else {
        panic!("aborted query send unexpectedly completed")
    };
    assert!(error.is_cancelled());
    assert_eq!(outside.pending_count(), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

struct PanicSender;

impl DatagramSender for PanicSender {
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        _datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        panic!("synthetic sender panic")
    }
}

struct FuturePanicSender;

impl DatagramSender for FuturePanicSender {
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        _datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        Box::pin(async { panic!("synthetic sender future panic") })
    }
}

#[tokio::test]
async fn sender_panic_unwinds_the_registration_guard() {
    let registry = one_id_registry(*b"A1");
    let outside = registry.clone();
    let task = tokio::spawn(async move {
        let mut sender = PanicSender;
        register_and_send_query(
            &registry,
            &mut sender,
            "192.0.2.1:1".parse().unwrap(),
            ByteString::new(b"ping"),
            query_args(Id20::ZERO, None),
        )
        .await
    });
    let Err(error) = task.await else {
        panic!("panicking query sender unexpectedly completed")
    };
    assert!(error.is_panic());
    assert_eq!(outside.pending_count(), 0);

    let registry = one_id_registry(*b"B2");
    let outside = registry.clone();
    let task = tokio::spawn(async move {
        let mut sender = FuturePanicSender;
        register_and_send_query(
            &registry,
            &mut sender,
            "192.0.2.1:1".parse().unwrap(),
            ByteString::new(b"ping"),
            query_args(Id20::ZERO, None),
        )
        .await
    });
    let Err(error) = task.await else {
        panic!("panicking sender future unexpectedly completed")
    };
    assert!(error.is_panic());
    assert_eq!(outside.pending_count(), 0);
}

struct CountingSender {
    calls: Arc<AtomicUsize>,
}

impl DatagramSender for CountingSender {
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        _datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

struct ConstantIssuer(TransactionId);

impl TransactionIdIssuer for ConstantIssuer {
    fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
        Ok(self.0)
    }
}

struct SequentialIssuer(u32);

impl TransactionIdIssuer for SequentialIssuer {
    fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
        let value = u16::try_from(self.0)
            .map_err(|_| TransactionIdSourceError::new("sequential issuer exhausted"))?;
        self.0 += 1;
        Ok(TransactionId::from(value.to_be_bytes()))
    }
}

async fn assert_register_error_sends_nothing<I>(
    registry: &TransactionRegistry<I>,
    expected: RegisterError,
) where
    I: TransactionIdIssuer,
{
    let calls = Arc::new(AtomicUsize::new(0));
    let mut sender = CountingSender {
        calls: Arc::clone(&calls),
    };
    let result = register_and_send_query(
        registry,
        &mut sender,
        "192.0.2.1:1".parse().unwrap(),
        ByteString::new(b"ping"),
        query_args(Id20::ZERO, None),
    )
    .await;
    assert!(matches!(result, Err(QuerySendError::Register(error)) if error == expected));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn closed_issuer_collision_and_full_registration_fail_before_send() {
    let closed = one_id_registry(*b"A1");
    closed.close();
    assert_register_error_sends_nothing(&closed, RegisterError::RegistryClosed).await;

    let issuer_failure = TransactionRegistry::new(ScriptedIssuer {
        ids: VecDeque::from([Err(TransactionIdSourceError::new("entropy unavailable"))]),
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let mut sender = CountingSender {
        calls: Arc::clone(&calls),
    };
    assert!(matches!(
        register_and_send_query(
            &issuer_failure,
            &mut sender,
            "192.0.2.1:1".parse().unwrap(),
            ByteString::new(b"ping"),
            query_args(Id20::ZERO, None),
        )
        .await,
        Err(QuerySendError::Register(RegisterError::IdSource(_)))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let collision = TransactionRegistry::new(ConstantIssuer(TransactionId::from(*b"A1")));
    let occupied = collision
        .register(
            "192.0.2.1:1".parse().unwrap(),
            ByteString::new(b"occupied"),
            query_args(Id20::ZERO, None),
        )
        .unwrap();
    assert_register_error_sends_nothing(&collision, RegisterError::CollisionRetryExhausted).await;
    drop(occupied);

    let full = TransactionRegistry::new(SequentialIssuer(0));
    let mut registrations = Vec::with_capacity(usize::from(u16::MAX) + 1);
    for _ in 0..=u16::MAX {
        registrations.push(
            full.register(
                "192.0.2.1:1".parse().unwrap(),
                ByteString::new(b"occupied"),
                query_args(Id20::ZERO, None),
            )
            .unwrap(),
        );
    }
    assert_register_error_sends_nothing(&full, RegisterError::TransactionIdSpaceFull).await;
    drop(registrations);
    assert_eq!(full.pending_count(), 0);
}

struct NormalizingSender<I> {
    registry: TransactionRegistry<I>,
    response_source: SocketAddr,
    exact_destination: SocketAddr,
}

impl<I> DatagramSender for NormalizingSender<I>
where
    I: TransactionIdIssuer + 'static,
{
    type Error = TransportSentinel;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        assert_eq!(destination, self.exact_destination);
        let message = KrpcMessage::decode(datagram).unwrap();
        let response = KrpcMessage {
            transaction_id: message.transaction_id,
            message_type: ByteString::new(b"r"),
            query: ByteString::default(),
            args: None,
            response: Some(empty_return(Id20::ZERO)),
            error: None,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        };
        assert_eq!(
            self.registry.deliver(self.response_source, response),
            DeliveryOutcome::Delivered
        );
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn outbound_address_is_exact_while_response_correlation_is_normalized() {
    let cases = [
        (
            SocketAddr::V6(SocketAddrV6::new(
                Ipv4Addr::new(192, 0, 2, 9).to_ipv6_mapped(),
                6881,
                99,
                0,
            )),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 9), 6881)),
        ),
        (
            SocketAddr::V6(SocketAddrV6::new(
                "fe80::9".parse::<Ipv6Addr>().unwrap(),
                6882,
                77,
                7,
            )),
            SocketAddr::V6(SocketAddrV6::new(
                "fe80::9".parse::<Ipv6Addr>().unwrap(),
                6882,
                0,
                7,
            )),
        ),
    ];

    for (index, (remote, response_source)) in cases.into_iter().enumerate() {
        let registry = one_id_registry([b'A', b'1' + u8::try_from(index).unwrap()]);
        let mut sender = NormalizingSender {
            registry: registry.clone(),
            response_source,
            exact_destination: remote,
        };
        let pending = register_and_send_query(
            &registry,
            &mut sender,
            remote,
            ByteString::new(b"ping"),
            query_args(Id20::ZERO, None),
        )
        .await
        .unwrap();
        assert!(matches!(
            pending.wait(Duration::from_secs(1)).await,
            TransactionWaitOutcome::Response { source, .. } if source == response_source
        ));
        assert_eq!(registry.pending_count(), 0);
    }
}
