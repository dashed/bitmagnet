//! Finite full-DHT supervisor ordering, shutdown, and cancellation gates.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::{pending, Future};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroU8;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use bitmagnet_dht::{
    ByteString, DatagramReceiver, DatagramSender, DhtDispatchOutcome, DhtDriver, DhtDriverError,
    DhtDriverOutcome, DhtResponder, DhtSendError, DhtSupervisor, DhtSupervisorExit, Id20, KTable,
    KrpcMessage, MessageArgs, MessageReturn, PingFindNodeSupervisorExit, ReceiveDispatchError,
    ReceiveDispatchOutcome, ReceivedDatagram, TokioIpv4UdpTransport, TransactionId,
    TransactionIdIssuer, TransactionIdSourceError, TransactionRegistry, MAX_INBOUND_DATAGRAM_BYTES,
};

const TOKEN_SECRET: [u8; 20] = [0x5a; 20];

#[derive(Clone, Debug)]
struct Sentinel(Arc<()>);

impl Display for Sentinel {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("full supervisor sentinel")
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
    reported: Option<usize>,
}

enum ReceiveAction {
    Complete(Result<Packet, Sentinel>),
    Pending,
}

enum SendAction {
    Complete(Result<(), Sentinel>),
    Pending,
    Gate(Arc<AtomicBool>),
    PanicConstruction(&'static str),
    PanicPoll(&'static str),
}

#[derive(Default)]
struct Observations {
    events: Vec<&'static str>,
    receive_calls: usize,
    receive_active: bool,
    receive_completions: usize,
    receive_cancellations: usize,
    send_calls: usize,
    send_active: bool,
    send_completions: usize,
    send_cancellations: usize,
    destinations: Vec<SocketAddr>,
    wires: Vec<Vec<u8>>,
}

struct ScriptedReceiver {
    actions: Arc<Mutex<VecDeque<ReceiveAction>>>,
    observations: Arc<Mutex<Observations>>,
}

struct ReceiveFuture<'a> {
    action: Option<ReceiveAction>,
    buffer: &'a mut [u8],
    observations: Arc<Mutex<Observations>>,
    completed: bool,
}

impl Future for ReceiveFuture<'_> {
    type Output = Result<ReceivedDatagram, Sentinel>;

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        let action = self.action.take().expect("receive polled after completion");
        match action {
            ReceiveAction::Pending => {
                self.action = Some(ReceiveAction::Pending);
                Poll::Pending
            }
            ReceiveAction::Complete(result) => {
                let outcome = result.map(|packet| {
                    let copied = packet.wire.len().min(self.buffer.len());
                    self.buffer[..copied].copy_from_slice(&packet.wire[..copied]);
                    ReceivedDatagram {
                        length: packet.reported.unwrap_or(packet.wire.len()),
                        source: packet.source,
                    }
                });
                self.completed = true;
                let mut observations = self.observations.lock().unwrap();
                observations.receive_active = false;
                observations.receive_completions += 1;
                Poll::Ready(outcome)
            }
        }
    }
}

impl Drop for ReceiveFuture<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut observations = self.observations.lock().unwrap();
        observations.receive_active = false;
        observations.receive_cancellations += 1;
    }
}

impl DatagramReceiver for ScriptedReceiver {
    type Error = Sentinel;

    fn receive<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ReceivedDatagram, Self::Error>> + Send + 'a>> {
        let action = self
            .actions
            .lock()
            .unwrap()
            .pop_front()
            .expect("bounded supervisor receive script");
        {
            let mut observations = self.observations.lock().unwrap();
            assert!(!observations.receive_active, "receiver was not reusable");
            observations.events.push("receive");
            observations.receive_calls += 1;
            observations.receive_active = true;
        }
        Box::pin(ReceiveFuture {
            action: Some(action),
            buffer,
            observations: Arc::clone(&self.observations),
            completed: false,
        })
    }
}

struct ScriptedSender {
    actions: Arc<Mutex<VecDeque<SendAction>>>,
    observations: Arc<Mutex<Observations>>,
}

struct SendFuture {
    action: SendAction,
    observations: Arc<Mutex<Observations>>,
    completed: bool,
}

impl Future for SendFuture {
    type Output = Result<(), Sentinel>;

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        let outcome = match &self.action {
            SendAction::Complete(result) => Poll::Ready(result.clone()),
            SendAction::Pending => Poll::Pending,
            SendAction::Gate(gate) if gate.load(Ordering::SeqCst) => Poll::Ready(Ok(())),
            SendAction::Gate(_) => Poll::Pending,
            SendAction::PanicPoll(message) => panic!("{message}"),
            SendAction::PanicConstruction(_) => unreachable!("construction panic has no future"),
        };
        if outcome.is_ready() {
            self.completed = true;
            let mut observations = self.observations.lock().unwrap();
            observations.send_active = false;
            observations.send_completions += 1;
        }
        outcome
    }
}

impl Drop for SendFuture {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut observations = self.observations.lock().unwrap();
        observations.send_active = false;
        observations.send_cancellations += 1;
    }
}

impl DatagramSender for ScriptedSender {
    type Error = Sentinel;

    fn send<'a>(
        &'a mut self,
        destination: SocketAddr,
        datagram: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let action = self
            .actions
            .lock()
            .unwrap()
            .pop_front()
            .expect("bounded supervisor send script");
        {
            let mut observations = self.observations.lock().unwrap();
            assert!(!observations.send_active, "sender was not reusable");
            observations.events.push("send");
            observations.send_calls += 1;
            observations.destinations.push(destination);
            observations.wires.push(datagram.to_vec());
        }
        if let SendAction::PanicConstruction(message) = action {
            panic!("{message}");
        }
        self.observations.lock().unwrap().send_active = true;
        Box::pin(SendFuture {
            action,
            observations: Arc::clone(&self.observations),
            completed: false,
        })
    }
}

fn id(last: u8) -> Id20 {
    let mut bytes = [0; 20];
    bytes[19] = last;
    Id20::from_slice(&bytes).unwrap()
}

fn source() -> SocketAddr {
    "192.0.2.1:6881".parse().unwrap()
}

fn args() -> MessageArgs {
    MessageArgs {
        id: id(2),
        info_hash: None,
        target: None,
        token: ByteString::default(),
        port: None,
        implied_port: false,
        want: None,
        no_seed: 0,
        scrape: 0,
    }
}

fn message(tid: &[u8], kind: &[u8], method: &[u8], args: Option<MessageArgs>) -> KrpcMessage {
    KrpcMessage {
        transaction_id: ByteString::new(tid),
        message_type: ByteString::new(kind),
        query: ByteString::new(method),
        args,
        response: None,
        error: None,
        observed_addr: None,
        read_only: false,
        client_id: ByteString::default(),
    }
}

fn packet(message: KrpcMessage) -> Packet {
    Packet {
        wire: message.encode().unwrap(),
        source: source(),
        reported: None,
    }
}

fn zero_packet() -> Packet {
    Packet {
        wire: Vec::new(),
        source: source(),
        reported: None,
    }
}

fn make_supervisor(
    receive_actions: Arc<Mutex<VecDeque<ReceiveAction>>>,
    send_actions: Arc<Mutex<VecDeque<SendAction>>>,
    observations: Arc<Mutex<Observations>>,
    table: &KTable,
) -> DhtSupervisor<ScriptedReceiver, ScriptedSender, Issuer> {
    let dispatcher = bitmagnet_dht::DhtDispatcher::from_responder(DhtResponder::with_token_secret(
        table.clone(),
        TOKEN_SECRET,
        300,
    ));
    let driver = DhtDriver::from_dispatcher(
        ScriptedReceiver {
            actions: receive_actions,
            observations: Arc::clone(&observations),
        },
        TransactionRegistry::new(Issuer(VecDeque::new())),
        ScriptedSender {
            actions: send_actions,
            observations,
        },
        dispatcher,
    );
    DhtSupervisor::from_driver(driver)
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::new(NoopWake));
    future.poll(&mut Context::from_waker(&waker))
}

#[tokio::test]
async fn budgets_are_exact_resume_is_ordered_and_unknown_queries_continue() {
    let table = KTable::new(id(0x90));
    let receive_actions = Arc::new(Mutex::new(VecDeque::from([
        ReceiveAction::Complete(Ok(packet(message(b"U1", b"q", b"unknown", Some(args()))))),
        ReceiveAction::Complete(Ok(zero_packet())),
        ReceiveAction::Complete(Ok(packet(message(b"P1", b"q", b"ping", Some(args()))))),
        ReceiveAction::Complete(Ok(zero_packet())),
        ReceiveAction::Complete(Ok(zero_packet())),
    ])));
    let send_actions = Arc::new(Mutex::new(VecDeque::from([
        SendAction::Complete(Ok(())),
        SendAction::Complete(Ok(())),
    ])));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut supervisor = make_supervisor(
        Arc::clone(&receive_actions),
        Arc::clone(&send_actions),
        Arc::clone(&observations),
        &table,
    );

    let DhtSupervisorExit::BudgetExhausted { completed } = supervisor
        .drive_batch(NonZeroU8::new(3).unwrap(), pending())
        .await
    else {
        panic!("three-step batch did not exhaust its budget")
    };
    assert_eq!(completed.len(), 3);
    let DhtDriverOutcome::Sent(unknown) = &completed[0] else {
        panic!("unknown query was not owned")
    };
    assert_eq!(unknown.reply().message.error.as_ref().unwrap().code, 204);
    assert!(matches!(
        completed[1],
        DhtDriverOutcome::NoReply(ReceiveDispatchOutcome::ZeroLength { .. })
    ));
    assert!(matches!(completed[2], DhtDriverOutcome::Sent(_)));

    for _ in 0..2 {
        let DhtSupervisorExit::BudgetExhausted { completed } = supervisor
            .drive_batch(NonZeroU8::new(1).unwrap(), pending())
            .await
        else {
            panic!("resumed batch did not exhaust")
        };
        assert_eq!(completed.len(), 1);
    }
    assert_eq!(observations.lock().unwrap().receive_calls, 5);

    let max_receive_actions = Arc::new(Mutex::new(
        std::iter::repeat_with(|| ReceiveAction::Complete(Ok(zero_packet())))
            .take(255)
            .collect(),
    ));
    let max_observations = Arc::new(Mutex::new(Observations::default()));
    let mut max_supervisor = make_supervisor(
        max_receive_actions,
        Arc::new(Mutex::new(VecDeque::new())),
        Arc::clone(&max_observations),
        &table,
    );
    let DhtSupervisorExit::BudgetExhausted { completed } = max_supervisor
        .drive_batch(NonZeroU8::new(255).unwrap(), pending())
        .await
    else {
        panic!("255-step batch did not exhaust")
    };
    assert_eq!(completed.len(), 255);
    assert_eq!(max_observations.lock().unwrap().receive_calls, 255);
}

#[test]
fn sender_backpressure_blocks_the_next_receive() {
    let table = KTable::new(id(0x90));
    let receive_actions = Arc::new(Mutex::new(VecDeque::from([
        ReceiveAction::Complete(Ok(packet(message(b"P1", b"q", b"ping", Some(args()))))),
        ReceiveAction::Complete(Ok(zero_packet())),
    ])));
    let gate = Arc::new(AtomicBool::new(false));
    let send_actions = Arc::new(Mutex::new(VecDeque::from([SendAction::Gate(Arc::clone(
        &gate,
    ))])));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut supervisor = make_supervisor(
        receive_actions,
        send_actions,
        Arc::clone(&observations),
        &table,
    );
    let mut batch = Box::pin(supervisor.drive_batch(NonZeroU8::new(2).unwrap(), pending()));
    assert!(poll_once(batch.as_mut()).is_pending());
    assert_eq!(observations.lock().unwrap().receive_calls, 1);
    gate.store(true, Ordering::SeqCst);
    let Poll::Ready(DhtSupervisorExit::BudgetExhausted { completed }) = poll_once(batch.as_mut())
    else {
        panic!("released batch did not finish")
    };
    assert_eq!(completed.len(), 2);
    assert_eq!(observations.lock().unwrap().receive_calls, 2);
}

#[tokio::test]
async fn biased_shutdown_cancels_receive_and_send_and_both_handles_are_reusable() {
    let table = KTable::new(id(0x90));
    let receive_actions = Arc::new(Mutex::new(VecDeque::from([
        ReceiveAction::Pending,
        ReceiveAction::Complete(Ok(zero_packet())),
    ])));
    let send_actions = Arc::new(Mutex::new(VecDeque::new()));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut supervisor = make_supervisor(
        Arc::clone(&receive_actions),
        Arc::clone(&send_actions),
        Arc::clone(&observations),
        &table,
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let mut batch = Box::pin(
        supervisor.drive_batch(NonZeroU8::new(1).unwrap(), async move {
            let _ = shutdown_rx.await;
        }),
    );
    assert!(poll_once(batch.as_mut()).is_pending());
    shutdown_tx.send(()).unwrap();
    assert!(matches!(
        poll_once(batch.as_mut()),
        Poll::Ready(DhtSupervisorExit::Shutdown { completed }) if completed.is_empty()
    ));
    drop(batch);
    assert_eq!(observations.lock().unwrap().receive_cancellations, 1);
    assert!(matches!(
        supervisor
            .drive_batch(NonZeroU8::new(1).unwrap(), pending())
            .await,
        DhtSupervisorExit::BudgetExhausted { completed } if completed.len() == 1
    ));

    let receive_actions = Arc::new(Mutex::new(VecDeque::from([
        ReceiveAction::Complete(Ok(packet(message(b"P1", b"q", b"ping", Some(args()))))),
        ReceiveAction::Complete(Ok(packet(message(b"P2", b"q", b"ping", Some(args()))))),
    ])));
    let send_actions = Arc::new(Mutex::new(VecDeque::from([
        SendAction::Pending,
        SendAction::Complete(Ok(())),
    ])));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut supervisor = make_supervisor(
        receive_actions,
        send_actions,
        Arc::clone(&observations),
        &table,
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let mut batch = Box::pin(
        supervisor.drive_batch(NonZeroU8::new(1).unwrap(), async move {
            let _ = shutdown_rx.await;
        }),
    );
    assert!(poll_once(batch.as_mut()).is_pending());
    shutdown_tx.send(()).unwrap();
    assert!(matches!(
        poll_once(batch.as_mut()),
        Poll::Ready(DhtSupervisorExit::Shutdown { completed }) if completed.is_empty()
    ));
    drop(batch);
    assert_eq!(observations.lock().unwrap().send_cancellations, 1);
    assert!(matches!(
        supervisor
            .drive_batch(NonZeroU8::new(1).unwrap(), pending())
            .await,
        DhtSupervisorExit::BudgetExhausted { completed } if completed.len() == 1
    ));
    let observations = observations.lock().unwrap();
    assert_eq!(observations.send_calls, 2);
    assert_eq!(observations.send_completions, 1);
}

fn announce(table: &KTable) -> KrpcMessage {
    let responder = DhtResponder::with_token_secret(table.clone(), TOKEN_SECRET, 300);
    let mut get_args = args();
    get_args.info_hash = Some(id(4));
    let get = message(b"G1", b"q", b"get_peers", Some(get_args));
    let token = responder.respond(source(), &get).unwrap().token.unwrap();
    let mut announce_args = args();
    announce_args.info_hash = Some(id(4));
    announce_args.token = token;
    message(b"A1", b"q", b"announce_peer", Some(announce_args))
}

#[tokio::test]
async fn announce_effect_survives_shutdown_abort_and_sender_panics_without_retry() {
    let table = KTable::new(id(0x90));
    let receive_actions = Arc::new(Mutex::new(VecDeque::from([ReceiveAction::Complete(Ok(
        packet(announce(&table)),
    ))])));
    let send_actions = Arc::new(Mutex::new(VecDeque::from([SendAction::Pending])));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut supervisor = make_supervisor(
        receive_actions,
        send_actions,
        Arc::clone(&observations),
        &table,
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let mut batch = Box::pin(
        supervisor.drive_batch(NonZeroU8::new(1).unwrap(), async move {
            let _ = shutdown_rx.await;
        }),
    );
    assert!(poll_once(batch.as_mut()).is_pending());
    assert_eq!(table.hash(id(4)).unwrap().peers[0].addr, source());
    shutdown_tx.send(()).unwrap();
    assert!(matches!(
        poll_once(batch.as_mut()),
        Poll::Ready(DhtSupervisorExit::Shutdown { completed }) if completed.is_empty()
    ));
    drop(batch);
    assert_eq!(observations.lock().unwrap().send_cancellations, 1);
    assert_eq!(table.hash(id(4)).unwrap().peers[0].addr, source());

    let table = KTable::new(id(0x90));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let supervisor = make_supervisor(
        Arc::new(Mutex::new(VecDeque::from([ReceiveAction::Complete(Ok(
            packet(announce(&table)),
        ))]))),
        Arc::new(Mutex::new(VecDeque::from([SendAction::Pending]))),
        Arc::clone(&observations),
        &table,
    );
    let task = tokio::spawn(async move {
        let mut supervisor = supervisor;
        supervisor
            .drive_batch(NonZeroU8::new(1).unwrap(), pending())
            .await
    });
    while !observations.lock().unwrap().send_active {
        tokio::task::yield_now().await;
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(observations.lock().unwrap().send_cancellations, 1);
    assert_eq!(table.hash(id(4)).unwrap().peers[0].addr, source());

    for action in [
        SendAction::PanicConstruction("supervisor construction panic"),
        SendAction::PanicPoll("supervisor poll panic"),
    ] {
        let table = KTable::new(id(0x90));
        let observations = Arc::new(Mutex::new(Observations::default()));
        let supervisor = make_supervisor(
            Arc::new(Mutex::new(VecDeque::from([ReceiveAction::Complete(Ok(
                packet(announce(&table)),
            ))]))),
            Arc::new(Mutex::new(VecDeque::from([action]))),
            Arc::clone(&observations),
            &table,
        );
        let task = tokio::spawn(async move {
            let mut supervisor = supervisor;
            supervisor
                .drive_batch(NonZeroU8::new(1).unwrap(), pending())
                .await
        });
        assert!(task.await.unwrap_err().is_panic());
        assert_eq!(observations.lock().unwrap().send_calls, 1);
        assert_eq!(table.hash(id(4)).unwrap().peers[0].addr, source());
    }
}

#[tokio::test]
async fn failures_stop_before_later_reads_and_retain_prefix_and_identity() {
    let table = KTable::new(id(0x90));
    let receive_sentinel = Sentinel(Arc::new(()));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut supervisor = make_supervisor(
        Arc::new(Mutex::new(VecDeque::from([
            ReceiveAction::Complete(Ok(zero_packet())),
            ReceiveAction::Complete(Err(receive_sentinel.clone())),
            ReceiveAction::Complete(Ok(zero_packet())),
        ]))),
        Arc::new(Mutex::new(VecDeque::new())),
        Arc::clone(&observations),
        &table,
    );
    let DhtSupervisorExit::Failed { completed, error } = supervisor
        .drive_batch(NonZeroU8::new(3).unwrap(), pending())
        .await
    else {
        panic!("receive failure did not stop batch")
    };
    assert_eq!(completed.len(), 1);
    let DhtDriverError::Receive(ReceiveDispatchError::Transport(actual)) = error else {
        panic!("receive identity was not retained")
    };
    assert!(Arc::ptr_eq(&actual.0, &receive_sentinel.0));
    assert_eq!(observations.lock().unwrap().receive_calls, 2);

    let send_sentinel = Sentinel(Arc::new(()));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut supervisor = make_supervisor(
        Arc::new(Mutex::new(VecDeque::from([
            ReceiveAction::Complete(Ok(zero_packet())),
            ReceiveAction::Complete(Ok(packet(message(b"P1", b"q", b"ping", Some(args()))))),
            ReceiveAction::Complete(Ok(zero_packet())),
        ]))),
        Arc::new(Mutex::new(VecDeque::from([SendAction::Complete(Err(
            send_sentinel.clone(),
        ))]))),
        Arc::clone(&observations),
        &table,
    );
    let DhtSupervisorExit::Failed { completed, error } = supervisor
        .drive_batch(NonZeroU8::new(3).unwrap(), pending())
        .await
    else {
        panic!("send failure did not stop batch")
    };
    assert_eq!(completed.len(), 1);
    let DhtDriverError::Send { prepared, error } = error else {
        panic!("send failure changed outer type")
    };
    assert!(matches!(*prepared, DhtDispatchOutcome::Reply(_)));
    let DhtSendError::Transport(actual) = error else {
        panic!("send identity changed nested type")
    };
    assert!(Arc::ptr_eq(&actual.0, &send_sentinel.0));
    let observations = observations.lock().unwrap();
    assert_eq!(observations.receive_calls, 2);
    assert_eq!(observations.send_calls, 1);
}

fn full_exit_exhaustive(exit: &DhtSupervisorExit<Sentinel, Sentinel>) -> &'static str {
    match exit {
        DhtSupervisorExit::BudgetExhausted { completed: _ } => "budget",
        DhtSupervisorExit::Shutdown { completed: _ } => "shutdown",
        DhtSupervisorExit::Failed {
            completed: _,
            error: _,
        } => "failed",
    }
}

fn legacy_exit_exhaustive(exit: &PingFindNodeSupervisorExit<Sentinel, Sentinel>) -> &'static str {
    match exit {
        PingFindNodeSupervisorExit::BudgetExhausted { completed: _ } => "budget",
        PingFindNodeSupervisorExit::Shutdown { completed: _ } => "shutdown",
        PingFindNodeSupervisorExit::UnownedQuery {
            completed: _,
            query: _,
        } => "unowned",
        PingFindNodeSupervisorExit::Failed {
            completed: _,
            error: _,
        } => "failed",
    }
}

#[test]
fn public_exit_and_legacy_exit_remain_separate_and_exhaustive() {
    let driver = DhtDriver::from_dispatcher(
        ScriptedReceiver {
            actions: Arc::new(Mutex::new(VecDeque::new())),
            observations: Arc::new(Mutex::new(Observations::default())),
        },
        TransactionRegistry::new(Issuer(VecDeque::new())),
        ScriptedSender {
            actions: Arc::new(Mutex::new(VecDeque::new())),
            observations: Arc::new(Mutex::new(Observations::default())),
        },
        bitmagnet_dht::DhtDispatcher::from_responder(DhtResponder::with_token_secret(
            KTable::new(id(0x90)),
            TOKEN_SECRET,
            300,
        )),
    );
    let _supervisor = DhtSupervisor::from_driver(driver);
    let _ = full_exit_exhaustive;
    let _ = legacy_exit_exhaustive;
}

#[tokio::test]
async fn real_tokio_ipv4_loopback_completes_one_bounded_full_supervisor_step() {
    let server_transport = TokioIpv4UdpTransport::bind_loopback().await.unwrap();
    let server_addr = server_transport.local_addr();
    let (server_receiver, server_sender) = server_transport.into_parts();
    let peer_transport = TokioIpv4UdpTransport::bind_loopback().await.unwrap();
    let (mut peer_receiver, mut peer_sender) = peer_transport.into_parts();

    let request = message(b"P1", b"q", b"ping", Some(args()));
    let request_wire = request.encode().unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        peer_sender.send(SocketAddr::V4(server_addr), &request_wire),
    )
    .await
    .expect("bounded loopback request send")
    .unwrap();

    let dispatcher = bitmagnet_dht::DhtDispatcher::from_responder(DhtResponder::with_token_secret(
        KTable::new(id(0x90)),
        TOKEN_SECRET,
        300,
    ));
    let driver = DhtDriver::from_dispatcher(
        server_receiver,
        TransactionRegistry::new(Issuer(VecDeque::new())),
        server_sender,
        dispatcher,
    );
    let mut supervisor = DhtSupervisor::from_driver(driver);
    let DhtSupervisorExit::BudgetExhausted { completed } = tokio::time::timeout(
        Duration::from_secs(2),
        supervisor.drive_batch(NonZeroU8::new(1).unwrap(), pending()),
    )
    .await
    .expect("bounded loopback supervisor") else {
        panic!("loopback supervisor did not finish")
    };
    assert_eq!(completed.len(), 1);
    assert!(matches!(completed[0], DhtDriverOutcome::Sent(_)));

    let mut buffer = vec![0; MAX_INBOUND_DATAGRAM_BYTES];
    let received = tokio::time::timeout(Duration::from_secs(2), peer_receiver.receive(&mut buffer))
        .await
        .expect("bounded loopback response receive")
        .unwrap();
    let response = KrpcMessage::decode(&buffer[..received.length]).unwrap();
    assert_eq!(response.transaction_id.as_bytes(), b"P1");
    assert_eq!(response.message_type.as_bytes(), b"r");
    assert!(response.response.is_some());
    assert!(response.error.is_none());
    assert_eq!(received.source, SocketAddr::V4(server_addr));

    let _ = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let _ = MessageReturn {
        id: id(0x90),
        nodes: None,
        nodes6: None,
        token: None,
        values: None,
        interval: None,
        num: None,
        samples: None,
        seeders_bloom: None,
        peers_bloom: None,
    };
}
