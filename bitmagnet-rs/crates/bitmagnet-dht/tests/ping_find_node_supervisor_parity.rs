//! Finite supervisor parity, lifecycle, and resource-boundary proofs.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::future::{pending, ready, Future};
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, SocketAddr, SocketAddrV6};
use std::num::NonZeroU8;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use bitmagnet_dht::{
    ByteString, DatagramReceiver, DatagramSender, DeliveryOutcome, Id20, KrpcError, KrpcMessage,
    MessageArgs, MessageReturn, NodeTable, PingFindNodeClient, PingFindNodeDispatchOutcome,
    PingFindNodeDriverError, PingFindNodeDriverOutcome, PingFindNodeError, PingFindNodeSendError,
    PingFindNodeSupervisor, PingFindNodeSupervisorExit, QuerySendError, ReceiveDispatchError,
    ReceiveDispatchOutcome, ReceivedDatagram, RoutingNode, RoutingPutResult, TransactionId,
    TransactionIdIssuer, TransactionIdSourceError, TransactionRegistry, TransactionWaitOutcome,
};
use serde::Deserialize;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
struct Sentinel(Arc<()>);

impl Display for Sentinel {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("supervisor sentinel")
    }
}

impl Error for Sentinel {}

struct Issuer(VecDeque<TransactionId>);

impl TransactionIdIssuer for Issuer {
    fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
        self.0
            .pop_front()
            .ok_or_else(|| TransactionIdSourceError::new("scripted issuer exhausted"))
    }
}

#[derive(Clone)]
struct Packet {
    wire: Vec<u8>,
    source: SocketAddr,
}

#[derive(Default)]
struct Counts {
    receives: usize,
    sends: usize,
    events: Vec<&'static str>,
}

struct QueueReceiver {
    packets: VecDeque<Result<Packet, Sentinel>>,
    counts: Arc<Mutex<Counts>>,
}

impl DatagramReceiver for QueueReceiver {
    type Error = Sentinel;

    fn receive<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ReceivedDatagram, Self::Error>> + Send + 'a>> {
        let packet = self.packets.pop_front().expect("bounded receive script");
        let mut counts = self.counts.lock().unwrap();
        counts.receives += 1;
        counts.events.push("receive");
        drop(counts);
        Box::pin(async move {
            let packet = packet?;
            buffer[..packet.wire.len()].copy_from_slice(&packet.wire);
            Ok(ReceivedDatagram {
                length: packet.wire.len(),
                source: packet.source,
            })
        })
    }
}

struct QueueSender {
    errors: VecDeque<Option<Sentinel>>,
    gate: Option<Arc<AtomicBool>>,
    counts: Arc<Mutex<Counts>>,
}

impl DatagramSender for QueueSender {
    type Error = Sentinel;

    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        _datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let error = self.errors.pop_front().unwrap_or(None);
        let gate = self.gate.take();
        let mut counts = self.counts.lock().unwrap();
        counts.sends += 1;
        counts.events.push("send");
        drop(counts);
        Box::pin(std::future::poll_fn(move |_| {
            if gate
                .as_ref()
                .is_some_and(|gate| !gate.load(Ordering::SeqCst))
            {
                Poll::Pending
            } else {
                Poll::Ready(error.clone().map_or(Ok(()), Err))
            }
        }))
    }
}

#[derive(Default)]
struct SendCancellationState {
    ready: bool,
    active: bool,
    calls: usize,
    completions: usize,
    cancellations: usize,
    wires: Vec<Vec<u8>>,
}

struct CancellationSafeSender {
    state: Arc<Mutex<SendCancellationState>>,
}

struct PendingSendGuard {
    state: Arc<Mutex<SendCancellationState>>,
    completed: bool,
}

impl PendingSendGuard {
    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for PendingSendGuard {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        state.active = false;
        if !self.completed {
            state.cancellations += 1;
        }
    }
}

impl DatagramSender for CancellationSafeSender {
    type Error = Sentinel;

    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        {
            let mut state = self.state.lock().unwrap();
            assert!(!state.active, "a cancelled send poisoned sender reuse");
            state.active = true;
            state.calls += 1;
            state.wires.push(datagram.to_vec());
        }
        let state = Arc::clone(&self.state);
        let mut guard = PendingSendGuard {
            state: Arc::clone(&state),
            completed: false,
        };
        Box::pin(std::future::poll_fn(move |_| {
            let ready = state.lock().unwrap().ready;
            if !ready {
                return Poll::Pending;
            }
            {
                let mut state = state.lock().unwrap();
                state.completions += 1;
            }
            guard.complete();
            Poll::Ready(Ok(()))
        }))
    }
}

fn id(last: u8) -> Id20 {
    let mut bytes = [0; 20];
    bytes[19] = last;
    Id20::from_slice(&bytes).unwrap()
}

fn args(target: Option<Id20>) -> MessageArgs {
    MessageArgs {
        id: id(1),
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

fn query(tid: &[u8], method: &[u8], target: Option<Id20>) -> Vec<u8> {
    KrpcMessage {
        transaction_id: ByteString::new(tid.to_vec()),
        message_type: ByteString::new(b"q"),
        query: ByteString::new(method.to_vec()),
        args: Some(args(target)),
        response: None,
        error: None,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    }
    .encode()
    .unwrap()
}

fn packet(wire: Vec<u8>) -> Packet {
    Packet {
        wire,
        source: "192.0.2.1:6881".parse().unwrap(),
    }
}

fn make_supervisor<'a>(
    packets: VecDeque<Result<Packet, Sentinel>>,
    errors: VecDeque<Option<Sentinel>>,
    gate: Option<Arc<AtomicBool>>,
    counts: Arc<Mutex<Counts>>,
    table: &'a NodeTable,
) -> PingFindNodeSupervisor<'a, QueueReceiver, QueueSender, Issuer> {
    PingFindNodeSupervisor::new(
        QueueReceiver {
            packets,
            counts: Arc::clone(&counts),
        },
        TransactionRegistry::new(Issuer(VecDeque::new())),
        QueueSender {
            errors,
            gate,
            counts,
        },
        table,
    )
}

struct NoopWake;
impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::new(NoopWake));
    future.poll(&mut Context::from_waker(&waker))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OracleFixture {
    id: String,
    subsystem: String,
    input: OracleInput,
    expected: OracleExpected,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OracleInput {
    #[serde(default)]
    wire_hex: String,
    source: OracleAddr,
    #[serde(default)]
    receive_fails: bool,
    #[serde(default)]
    send_fails: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OracleExpected {
    go_terminal: String,
    rust_terminal: String,
    go_receive_calls: usize,
    go_send_calls: usize,
    #[serde(default)]
    go_reply_wire_hex: String,
    #[serde(default)]
    go_panicked: bool,
    #[serde(default)]
    panic_retained_transport: bool,
    #[serde(default)]
    send_failure_logged: bool,
    #[serde(default)]
    send_failure_retained_transport: bool,
}

#[derive(Clone, Copy, Deserialize)]
struct OracleAddr {
    ip: IpAddr,
    port: u16,
}

fn fixtures() -> Vec<OracleFixture> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/dht/ping_find_node_supervisor.jsonl");
    BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect()
}

#[tokio::test]
async fn actual_go_read_deltas_match_typed_rust_stops() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 3);
    for fixture in fixtures {
        assert_eq!(fixture.subsystem, "dht_ping_find_node_supervisor");
        let source = SocketAddr::new(fixture.input.source.ip, fixture.input.source.port);
        let sentinel = Sentinel(Arc::new(()));
        let packets = VecDeque::from([if fixture.input.receive_fails {
            Err(sentinel.clone())
        } else {
            Ok(Packet {
                wire: hex::decode(&fixture.input.wire_hex).unwrap(),
                source,
            })
        }]);
        let counts = Arc::new(Mutex::new(Counts::default()));
        let table = NodeTable::new(id(0x90));
        let mut supervisor = make_supervisor(
            packets,
            VecDeque::from([fixture.input.send_fails.then(|| sentinel.clone())]),
            None,
            Arc::clone(&counts),
            &table,
        );
        let exit = supervisor
            .drive_batch(NonZeroU8::new(1).unwrap(), pending())
            .await;
        match fixture.expected.rust_terminal.as_str() {
            "unowned_query" => {
                let PingFindNodeSupervisorExit::UnownedQuery { completed, query } = exit else {
                    panic!("{}: expected intact unowned query", fixture.id)
                };
                assert!(completed.is_empty());
                let ReceiveDispatchOutcome::Query {
                    source: actual,
                    message,
                } = query
                else {
                    panic!("{}: unowned payload changed shape", fixture.id)
                };
                assert_eq!(actual, source);
                assert_eq!(message.query.as_bytes(), b"get_peers");
            }
            "failed_send" => {
                let PingFindNodeSupervisorExit::Failed {
                    completed,
                    error:
                        PingFindNodeDriverError::Send {
                            prepared,
                            error: PingFindNodeSendError::Transport(actual),
                        },
                } = exit
                else {
                    panic!("{}: expected typed send stop", fixture.id)
                };
                assert!(completed.is_empty());
                assert!(Arc::ptr_eq(&actual.0, &sentinel.0));
                assert!(matches!(*prepared, PingFindNodeDispatchOutcome::Reply(_)));
            }
            "failed_receive" => {
                let PingFindNodeSupervisorExit::Failed {
                    completed,
                    error: PingFindNodeDriverError::Receive(ReceiveDispatchError::Transport(actual)),
                } = exit
                else {
                    panic!("{}: expected typed receive stop", fixture.id)
                };
                assert!(completed.is_empty());
                assert!(Arc::ptr_eq(&actual.0, &sentinel.0));
            }
            other => panic!("unexpected terminal {other}"),
        }
        let counts = counts.lock().unwrap();
        assert_eq!(counts.receives, 1);
        assert_eq!(counts.sends, usize::from(fixture.input.send_fails));
        assert_eq!(
            fixture.expected.go_receive_calls,
            if fixture.input.receive_fails { 1 } else { 2 }
        );
        assert_eq!(
            fixture.expected.go_send_calls,
            usize::from(!fixture.input.receive_fails)
        );
        assert_eq!(fixture.expected.go_panicked, fixture.input.receive_fails);
        assert_eq!(
            fixture.expected.panic_retained_transport,
            fixture.input.receive_fails
        );
        assert_eq!(
            fixture.expected.send_failure_logged,
            fixture.input.send_fails
        );
        assert_eq!(
            fixture.expected.send_failure_retained_transport,
            fixture.input.send_fails
        );
        assert!(!fixture.expected.go_terminal.is_empty());
        if !fixture.expected.go_reply_wire_hex.is_empty() {
            KrpcMessage::decode(&hex::decode(fixture.expected.go_reply_wire_hex).unwrap()).unwrap();
        }
    }
}

#[tokio::test]
async fn budget_is_exact_ordered_and_supervisor_resumes_across_batches() {
    let source = "192.0.2.1:6881".parse().unwrap();
    let zero = Packet {
        wire: Vec::new(),
        source,
    };
    let malformed = packet(b"d1:t2:X1".to_vec());
    let ignored = packet(
        KrpcMessage {
            transaction_id: ByteString::new(b"I1"),
            message_type: ByteString::new(b"x"),
            query: ByteString::new(b"ping"),
            args: Some(args(None)),
            response: None,
            error: None,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        }
        .encode()
        .unwrap(),
    );
    let ping = packet(query(b"P1", b"ping", None));
    let counts = Arc::new(Mutex::new(Counts::default()));
    let table = NodeTable::new(id(9));
    let mut supervisor = make_supervisor(
        VecDeque::from([Ok(zero), Ok(malformed), Ok(ignored), Ok(ping)]),
        VecDeque::new(),
        None,
        Arc::clone(&counts),
        &table,
    );
    for expected in ["zero", "decode", "ignored", "sent"] {
        let PingFindNodeSupervisorExit::BudgetExhausted { completed } = supervisor
            .drive_batch(NonZeroU8::new(1).unwrap(), pending())
            .await
        else {
            panic!()
        };
        assert_eq!(completed.len(), 1);
        match (expected, &completed[0]) {
            (
                "zero",
                PingFindNodeDriverOutcome::NoReply(ReceiveDispatchOutcome::ZeroLength { .. }),
            )
            | (
                "decode",
                PingFindNodeDriverOutcome::NoReply(ReceiveDispatchOutcome::DecodeRejected {
                    ..
                }),
            )
            | (
                "ignored",
                PingFindNodeDriverOutcome::NoReply(ReceiveDispatchOutcome::Ignored { .. }),
            )
            | ("sent", PingFindNodeDriverOutcome::Sent(_)) => {}
            _ => panic!("outcome order changed"),
        }
    }
    assert_eq!(counts.lock().unwrap().receives, 4);
    assert_eq!(counts.lock().unwrap().sends, 1);

    let counts = Arc::new(Mutex::new(Counts::default()));
    let packets = (0..u8::MAX)
        .map(|_| {
            Ok(Packet {
                wire: Vec::new(),
                source,
            })
        })
        .collect();
    let mut supervisor =
        make_supervisor(packets, VecDeque::new(), None, Arc::clone(&counts), &table);
    let PingFindNodeSupervisorExit::BudgetExhausted { completed } = supervisor
        .drive_batch(NonZeroU8::new(u8::MAX).unwrap(), pending())
        .await
    else {
        panic!()
    };
    assert_eq!(completed.len(), usize::from(u8::MAX));
    assert_eq!(counts.lock().unwrap().receives, usize::from(u8::MAX));
}

struct ReusablePendingReceiver {
    ready: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    counts: Arc<Mutex<Counts>>,
    packet: Packet,
}

struct CancelFlag(Arc<AtomicBool>);
impl Drop for CancelFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl DatagramReceiver for ReusablePendingReceiver {
    type Error = Sentinel;
    fn receive<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ReceivedDatagram, Self::Error>> + Send + 'a>> {
        self.counts.lock().unwrap().receives += 1;
        let ready = Arc::clone(&self.ready);
        let packet = self.packet.clone();
        let flag = CancelFlag(Arc::clone(&self.cancelled));
        Box::pin(std::future::poll_fn(move |_| {
            let _keep = &flag;
            if !ready.load(Ordering::SeqCst) {
                return Poll::Pending;
            }
            buffer[..packet.wire.len()].copy_from_slice(&packet.wire);
            Poll::Ready(Ok(ReceivedDatagram {
                length: packet.wire.len(),
                source: packet.source,
            }))
        }))
    }
}

#[tokio::test]
async fn biased_shutdown_cancels_pending_receive_and_receiver_is_reusable() {
    let ready_flag = Arc::new(AtomicBool::new(false));
    let cancelled = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(Mutex::new(Counts::default()));
    let table = NodeTable::new(id(9));
    let receiver = ReusablePendingReceiver {
        ready: Arc::clone(&ready_flag),
        cancelled: Arc::clone(&cancelled),
        counts: Arc::clone(&counts),
        packet: packet(query(b"P1", b"ping", None)),
    };
    let sender = QueueSender {
        errors: VecDeque::new(),
        gate: None,
        counts: Arc::clone(&counts),
    };
    let mut supervisor = PingFindNodeSupervisor::new(
        receiver,
        TransactionRegistry::new(Issuer(VecDeque::new())),
        sender,
        &table,
    );
    let PingFindNodeSupervisorExit::Shutdown { completed } = supervisor
        .drive_batch(NonZeroU8::new(1).unwrap(), ready(()))
        .await
    else {
        panic!()
    };
    assert!(completed.is_empty());
    assert_eq!(
        counts.lock().unwrap().receives,
        0,
        "biased ready shutdown wins"
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let mut batch = Box::pin(
        supervisor.drive_batch(NonZeroU8::new(1).unwrap(), async move {
            let _ = shutdown_rx.await;
        }),
    );
    assert!(poll_once(batch.as_mut()).is_pending());
    assert_eq!(counts.lock().unwrap().receives, 1);
    shutdown_tx.send(()).unwrap();
    assert!(matches!(
        poll_once(batch.as_mut()),
        Poll::Ready(PingFindNodeSupervisorExit::Shutdown { .. })
    ));
    drop(batch);
    assert!(cancelled.load(Ordering::SeqCst));
    ready_flag.store(true, Ordering::SeqCst);
    assert!(matches!(
        supervisor
            .drive_batch(NonZeroU8::new(1).unwrap(), pending())
            .await,
        PingFindNodeSupervisorExit::BudgetExhausted { .. }
    ));
    assert_eq!(counts.lock().unwrap().sends, 1);
}

#[tokio::test]
async fn sender_backpressure_blocks_the_next_receive() {
    let gate = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(Mutex::new(Counts::default()));
    let table = NodeTable::new(id(9));
    let mut supervisor = make_supervisor(
        VecDeque::from([
            Ok(packet(query(b"P1", b"ping", None))),
            Ok(Packet {
                wire: Vec::new(),
                source: "192.0.2.1:1".parse().unwrap(),
            }),
        ]),
        VecDeque::new(),
        Some(Arc::clone(&gate)),
        Arc::clone(&counts),
        &table,
    );
    let mut batch = Box::pin(supervisor.drive_batch(NonZeroU8::new(2).unwrap(), pending()));
    assert!(poll_once(batch.as_mut()).is_pending());
    assert_eq!(counts.lock().unwrap().events, ["receive", "send"]);
    gate.store(true, Ordering::SeqCst);
    let Poll::Ready(PingFindNodeSupervisorExit::BudgetExhausted { completed }) =
        poll_once(batch.as_mut())
    else {
        panic!()
    };
    assert_eq!(completed.len(), 2);
    assert_eq!(
        counts.lock().unwrap().events,
        ["receive", "send", "receive"]
    );
}

fn cancellation_safe_supervisor<'a>(
    table: &'a NodeTable,
    state: Arc<Mutex<SendCancellationState>>,
    counts: Arc<Mutex<Counts>>,
) -> PingFindNodeSupervisor<'a, QueueReceiver, CancellationSafeSender, Issuer> {
    PingFindNodeSupervisor::new(
        QueueReceiver {
            packets: VecDeque::from([
                Ok(packet(query(b"C1", b"ping", None))),
                Ok(packet(query(b"C2", b"ping", None))),
            ]),
            counts,
        },
        TransactionRegistry::new(Issuer(VecDeque::new())),
        CancellationSafeSender { state },
        table,
    )
}

fn assert_one_cancelled_send(state: &Arc<Mutex<SendCancellationState>>) {
    let state = state.lock().unwrap();
    assert_eq!(state.calls, 1);
    assert_eq!(state.completions, 0);
    assert_eq!(state.cancellations, 1);
    assert!(!state.active);
    assert_eq!(state.wires.len(), 1);
}

fn assert_reused_without_retry(state: &Arc<Mutex<SendCancellationState>>) {
    let state = state.lock().unwrap();
    assert_eq!(state.calls, 2, "cancelled send was retried or duplicated");
    assert_eq!(state.completions, 1);
    assert_eq!(state.cancellations, 1);
    assert!(!state.active);
    assert_eq!(state.wires.len(), 2);
    assert_ne!(state.wires[0], state.wires[1], "first reply was retried");
}

#[tokio::test]
async fn shutdown_drops_a_pending_send_and_the_next_batch_reuses_sender_once() {
    let table = NodeTable::new(id(9));
    let state = Arc::new(Mutex::new(SendCancellationState::default()));
    let counts = Arc::new(Mutex::new(Counts::default()));
    let mut supervisor =
        cancellation_safe_supervisor(&table, Arc::clone(&state), Arc::clone(&counts));
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let mut batch = Box::pin(
        supervisor.drive_batch(NonZeroU8::new(2).unwrap(), async move {
            let _ = shutdown_rx.await;
        }),
    );
    assert!(poll_once(batch.as_mut()).is_pending());
    assert_eq!(counts.lock().unwrap().receives, 1);
    shutdown_tx.send(()).unwrap();
    let Poll::Ready(PingFindNodeSupervisorExit::Shutdown { completed }) = poll_once(batch.as_mut())
    else {
        panic!()
    };
    assert!(completed.is_empty(), "cancelled send is not settled");
    drop(batch);
    assert_one_cancelled_send(&state);

    state.lock().unwrap().ready = true;
    let PingFindNodeSupervisorExit::BudgetExhausted { completed } = supervisor
        .drive_batch(NonZeroU8::new(1).unwrap(), pending())
        .await
    else {
        panic!()
    };
    assert_eq!(completed.len(), 1);
    assert_eq!(counts.lock().unwrap().receives, 2);
    assert_reused_without_retry(&state);
}

#[test]
fn dropping_a_batch_during_send_cancels_once_and_preserves_sender_reuse() {
    let table = NodeTable::new(id(9));
    let state = Arc::new(Mutex::new(SendCancellationState::default()));
    let counts = Arc::new(Mutex::new(Counts::default()));
    let mut supervisor =
        cancellation_safe_supervisor(&table, Arc::clone(&state), Arc::clone(&counts));
    let mut batch = Box::pin(supervisor.drive_batch(NonZeroU8::new(2).unwrap(), pending()));
    assert!(poll_once(batch.as_mut()).is_pending());
    drop(batch);
    assert_one_cancelled_send(&state);

    state.lock().unwrap().ready = true;
    let mut resumed = Box::pin(supervisor.drive_batch(NonZeroU8::new(1).unwrap(), pending()));
    assert!(matches!(
        poll_once(resumed.as_mut()),
        Poll::Ready(PingFindNodeSupervisorExit::BudgetExhausted { .. })
    ));
    drop(resumed);
    assert_eq!(counts.lock().unwrap().receives, 2);
    assert_reused_without_retry(&state);
}

#[tokio::test]
async fn aborting_a_batch_during_send_drops_exactly_one_pending_send() {
    let state = Arc::new(Mutex::new(SendCancellationState::default()));
    let counts = Arc::new(Mutex::new(Counts::default()));
    let table = Box::leak(Box::new(NodeTable::new(id(9))));
    let mut supervisor =
        cancellation_safe_supervisor(table, Arc::clone(&state), Arc::clone(&counts));
    let task = tokio::spawn(async move {
        supervisor
            .drive_batch(NonZeroU8::new(2).unwrap(), pending())
            .await
    });
    while state.lock().unwrap().calls == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_one_cancelled_send(&state);
    assert_eq!(counts.lock().unwrap().receives, 1);
}

#[tokio::test]
async fn unowned_and_failure_stop_before_a_later_read_and_retain_prefix() {
    let counts = Arc::new(Mutex::new(Counts::default()));
    let table = NodeTable::new(id(9));
    let mut supervisor = make_supervisor(
        VecDeque::from([
            Ok(Packet {
                wire: Vec::new(),
                source: "192.0.2.1:1".parse().unwrap(),
            }),
            Ok(packet(query(b"U1", b"get_peers", None))),
            Ok(packet(query(b"P1", b"ping", None))),
        ]),
        VecDeque::new(),
        None,
        Arc::clone(&counts),
        &table,
    );
    let PingFindNodeSupervisorExit::UnownedQuery {
        completed,
        query: unowned,
    } = supervisor
        .drive_batch(NonZeroU8::new(3).unwrap(), pending())
        .await
    else {
        panic!()
    };
    assert_eq!(completed.len(), 1);
    assert!(matches!(unowned, ReceiveDispatchOutcome::Query { .. }));
    assert_eq!(counts.lock().unwrap().receives, 2);
    assert!(matches!(
        supervisor
            .drive_batch(NonZeroU8::new(1).unwrap(), pending())
            .await,
        PingFindNodeSupervisorExit::BudgetExhausted { .. }
    ));
    assert_eq!(counts.lock().unwrap().receives, 3);

    let marker = Sentinel(Arc::new(()));
    let counts = Arc::new(Mutex::new(Counts::default()));
    let mut supervisor = make_supervisor(
        VecDeque::from([
            Ok(Packet {
                wire: Vec::new(),
                source: "192.0.2.1:1".parse().unwrap(),
            }),
            Err(marker.clone()),
            Ok(packet(query(b"P2", b"ping", None))),
        ]),
        VecDeque::new(),
        None,
        Arc::clone(&counts),
        &table,
    );
    let PingFindNodeSupervisorExit::Failed {
        completed,
        error: PingFindNodeDriverError::Receive(ReceiveDispatchError::Transport(actual)),
    } = supervisor
        .drive_batch(NonZeroU8::new(3).unwrap(), pending())
        .await
    else {
        panic!()
    };
    assert_eq!(completed.len(), 1);
    assert!(Arc::ptr_eq(&actual.0, &marker.0));
    assert_eq!(counts.lock().unwrap().receives, 2);
}

fn response(tid: &[u8], kind: &[u8]) -> Vec<u8> {
    KrpcMessage {
        transaction_id: ByteString::new(tid.to_vec()),
        message_type: ByteString::new(kind.to_vec()),
        query: ByteString::default(),
        args: None,
        response: (kind == b"r").then(|| MessageReturn {
            id: id(8),
            nodes: None,
            nodes6: None,
            token: None,
            values: None,
            interval: None,
            num: None,
            samples: None,
            seeders_bloom: None,
            peers_bloom: None,
        }),
        error: (kind == b"e").then(|| KrpcError {
            code: 202,
            message: ByteString::new(b"remote"),
        }),
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    }
    .encode()
    .unwrap()
}

#[tokio::test]
async fn response_and_error_registry_outcomes_are_ordered_and_cleanup_after_wait() {
    let source = "192.0.2.1:6881".parse().unwrap();
    let registry = TransactionRegistry::new(Issuer(VecDeque::from([
        TransactionId::from(*b"R1"),
        TransactionId::from(*b"E1"),
    ])));
    let response_pending = registry
        .register(source, ByteString::new(b"ping"), args(None))
        .unwrap()
        .mark_sent();
    let error_pending = registry
        .register(source, ByteString::new(b"ping"), args(None))
        .unwrap()
        .mark_sent();
    let counts = Arc::new(Mutex::new(Counts::default()));
    let table = NodeTable::new(id(9));
    let receiver = QueueReceiver {
        packets: VecDeque::from([
            Ok(Packet {
                wire: response(b"R1", b"r"),
                source,
            }),
            Ok(Packet {
                wire: response(b"E1", b"e"),
                source,
            }),
        ]),
        counts: Arc::clone(&counts),
    };
    let sender = QueueSender {
        errors: VecDeque::new(),
        gate: None,
        counts,
    };
    let mut supervisor = PingFindNodeSupervisor::new(receiver, registry.clone(), sender, &table);
    let PingFindNodeSupervisorExit::BudgetExhausted { completed } = supervisor
        .drive_batch(NonZeroU8::new(2).unwrap(), pending())
        .await
    else {
        panic!()
    };
    assert!(matches!(
        completed.as_slice(),
        [
            PingFindNodeDriverOutcome::NoReply(ReceiveDispatchOutcome::Response {
                delivery: DeliveryOutcome::Delivered,
                ..
            }),
            PingFindNodeDriverOutcome::NoReply(ReceiveDispatchOutcome::Error {
                delivery: DeliveryOutcome::Delivered,
                ..
            })
        ]
    ));
    assert!(matches!(
        response_pending
            .wait(std::time::Duration::from_secs(1))
            .await,
        TransactionWaitOutcome::Response { .. }
    ));
    assert!(matches!(
        error_pending.wait(std::time::Duration::from_secs(1)).await,
        TransactionWaitOutcome::RemoteError { .. }
    ));
    assert_eq!(registry.pending_count(), 0);
}

#[tokio::test]
async fn local_failure_send_error_keeps_exact_cause_and_transport_identity() {
    let mut table = NodeTable::new(id(9));
    let native = RoutingNode {
        id: id(2),
        addr: SocketAddr::V6(SocketAddrV6::new(
            "2001:db8::1".parse().unwrap(),
            6881,
            0,
            7,
        )),
    };
    assert_eq!(table.put(native), RoutingPutResult::Accepted);
    let stored = table.closest(id(2))[0];
    let marker = Sentinel(Arc::new(()));
    let counts = Arc::new(Mutex::new(Counts::default()));
    let mut supervisor = make_supervisor(
        VecDeque::from([Ok(packet(query(b"F1", b"find_node", Some(id(2)))))]),
        VecDeque::from([Some(marker.clone())]),
        None,
        counts,
        &table,
    );
    let PingFindNodeSupervisorExit::Failed {
        completed,
        error:
            PingFindNodeDriverError::Send {
                prepared,
                error: PingFindNodeSendError::Transport(actual),
            },
    } = supervisor
        .drive_batch(NonZeroU8::new(1).unwrap(), pending())
        .await
    else {
        panic!()
    };
    assert!(completed.is_empty());
    assert!(Arc::ptr_eq(&actual.0, &marker.0));
    let PingFindNodeDispatchOutcome::LocalFailure { cause, .. } = *prepared else {
        panic!()
    };
    assert_eq!(cause, PingFindNodeError::NativeIpv6Node(stored));
}

struct ChannelReceiver {
    packets: mpsc::UnboundedReceiver<Packet>,
}
impl DatagramReceiver for ChannelReceiver {
    type Error = Sentinel;
    fn receive<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ReceivedDatagram, Self::Error>> + Send + 'a>> {
        Box::pin(async move {
            let packet = self
                .packets
                .recv()
                .await
                .expect("client supplies bounded query");
            buffer[..packet.wire.len()].copy_from_slice(&packet.wire);
            Ok(ReceivedDatagram {
                length: packet.wire.len(),
                source: packet.source,
            })
        })
    }
}

#[tokio::test]
async fn shutdown_after_a_settled_step_retains_the_exact_prefix() {
    let source = "192.0.2.1:1".parse().unwrap();
    let (packet_tx, packet_rx) = mpsc::unbounded_channel();
    packet_tx
        .send(Packet {
            wire: Vec::new(),
            source,
        })
        .unwrap();
    let counts = Arc::new(Mutex::new(Counts::default()));
    let sender = QueueSender {
        errors: VecDeque::new(),
        gate: None,
        counts,
    };
    let table = NodeTable::new(id(9));
    let mut supervisor = PingFindNodeSupervisor::new(
        ChannelReceiver { packets: packet_rx },
        TransactionRegistry::new(Issuer(VecDeque::new())),
        sender,
        &table,
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let mut batch = Box::pin(
        supervisor.drive_batch(NonZeroU8::new(2).unwrap(), async move {
            let _ = shutdown_rx.await;
        }),
    );
    assert!(poll_once(batch.as_mut()).is_pending());
    shutdown_tx.send(()).unwrap();
    let Poll::Ready(PingFindNodeSupervisorExit::Shutdown { completed }) = poll_once(batch.as_mut())
    else {
        panic!()
    };
    assert!(matches!(
        completed.as_slice(),
        [PingFindNodeDriverOutcome::NoReply(
            ReceiveDispatchOutcome::ZeroLength { source: actual }
        )] if *actual == source
    ));
}

struct ClientToSupervisorSender {
    tx: mpsc::UnboundedSender<Packet>,
    source: SocketAddr,
}
impl DatagramSender for ClientToSupervisorSender {
    type Error = Sentinel;
    fn send<'a>(
        &'a mut self,
        _destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let result = self.tx.send(Packet {
            wire: datagram.to_vec(),
            source: self.source,
        });
        Box::pin(async move { result.map_err(|_| Sentinel(Arc::new(()))) })
    }
}

struct SupervisorToClientSender<I> {
    registry: TransactionRegistry<I>,
    source: SocketAddr,
    destination: SocketAddr,
}
impl<I: TransactionIdIssuer + 'static> DatagramSender for SupervisorToClientSender<I> {
    type Error = Sentinel;
    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        assert_eq!(destination, self.destination);
        let message = KrpcMessage::decode(datagram).unwrap();
        assert_eq!(
            self.registry.deliver(self.source, message),
            DeliveryOutcome::Delivered
        );
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn fake_concurrent_client_ping_and_find_round_trip_through_supervisor() {
    let client_addr = "192.0.2.10:40000".parse().unwrap();
    let server_addr = "192.0.2.20:6881".parse().unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    let client_registry = TransactionRegistry::new(Issuer(VecDeque::from([
        TransactionId::from(*b"P1"),
        TransactionId::from(*b"F1"),
    ])));
    let mut table = NodeTable::new(id(9));
    let node = RoutingNode {
        id: id(2),
        addr: "192.0.2.30:6881".parse().unwrap(),
    };
    assert_eq!(table.put(node), RoutingPutResult::Accepted);
    let server_sender = SupervisorToClientSender {
        registry: client_registry.clone(),
        source: server_addr,
        destination: client_addr,
    };
    let mut supervisor = PingFindNodeSupervisor::new(
        ChannelReceiver { packets: rx },
        TransactionRegistry::new(Issuer(VecDeque::new())),
        server_sender,
        &table,
    );
    let mut client_sender = ClientToSupervisorSender {
        tx,
        source: client_addr,
    };
    let client =
        PingFindNodeClient::new(id(1), &client_registry, std::time::Duration::from_secs(10));
    let (exit, results) = tokio::join!(
        supervisor.drive_batch(NonZeroU8::new(2).unwrap(), pending()),
        async {
            let ping = client.ping(&mut client_sender, server_addr).await.unwrap();
            let find = client
                .find_node(&mut client_sender, server_addr, id(2))
                .await
                .unwrap();
            (ping, find)
        }
    );
    let PingFindNodeSupervisorExit::BudgetExhausted { completed } = exit else {
        panic!()
    };
    assert_eq!(completed.len(), 2);
    assert_eq!(results.0.id, id(9));
    assert_eq!(results.1.id, id(9));
    assert_eq!(results.1.nodes, [node]);
    assert_eq!(client_registry.pending_count(), 0);
}

#[test]
fn dropping_an_inflight_batch_drops_the_receive_future_without_panicking() {
    let ready_flag = Arc::new(AtomicBool::new(false));
    let cancelled = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(Mutex::new(Counts::default()));
    let table = NodeTable::new(id(9));
    let receiver = ReusablePendingReceiver {
        ready: ready_flag,
        cancelled: Arc::clone(&cancelled),
        counts: Arc::clone(&counts),
        packet: Packet {
            wire: Vec::new(),
            source: "192.0.2.1:1".parse().unwrap(),
        },
    };
    let sender = QueueSender {
        errors: VecDeque::new(),
        gate: None,
        counts,
    };
    let mut supervisor = PingFindNodeSupervisor::new(
        receiver,
        TransactionRegistry::new(Issuer(VecDeque::new())),
        sender,
        &table,
    );
    let mut future = Box::pin(supervisor.drive_batch(NonZeroU8::new(1).unwrap(), pending()));
    assert!(poll_once(future.as_mut()).is_pending());
    drop(future);
    assert!(cancelled.load(Ordering::SeqCst));
}

#[tokio::test]
async fn aborting_an_inflight_batch_drops_the_receive_future() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(Mutex::new(Counts::default()));
    let receiver = ReusablePendingReceiver {
        ready: Arc::new(AtomicBool::new(false)),
        cancelled: Arc::clone(&cancelled),
        counts: Arc::clone(&counts),
        packet: Packet {
            wire: Vec::new(),
            source: "192.0.2.1:1".parse().unwrap(),
        },
    };
    let sender = QueueSender {
        errors: VecDeque::new(),
        gate: None,
        counts: Arc::clone(&counts),
    };
    let table = Box::leak(Box::new(NodeTable::new(id(9))));
    let mut supervisor = PingFindNodeSupervisor::new(
        receiver,
        TransactionRegistry::new(Issuer(VecDeque::new())),
        sender,
        table,
    );
    let task = tokio::spawn(async move {
        supervisor
            .drive_batch(NonZeroU8::new(1).unwrap(), pending())
            .await
    });
    while counts.lock().unwrap().receives == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(cancelled.load(Ordering::SeqCst));
}

#[test]
fn public_error_sources_remain_nested() {
    fn assert_error<E: Error>() {}
    assert_error::<PingFindNodeDriverError<Sentinel, Sentinel>>();
    assert_error::<QuerySendError<Sentinel>>();
}
