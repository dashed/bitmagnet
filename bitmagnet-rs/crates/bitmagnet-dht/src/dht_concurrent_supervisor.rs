use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::Arc;

use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

use crate::{
    send_dht_reply, DatagramReceiver, DatagramSender, DhtDispatchOutcome, DhtDispatcher,
    DhtDriverError, DhtInboundRateLimitDenial, DhtInboundRateLimiter, DhtInboundStats, DhtReply,
    DhtResponderTable, DhtSendError, KTable, ReceiveDispatchOutcome, ReceiveDispatcher,
    TransactionIdIssuer, TransactionRegistry,
};

/// A synchronous receive-order admission boundary for inbound DHT queries.
///
/// Implementations run on the supervisor task before responder dispatch. They
/// must therefore return promptly and must not retain the source address. A
/// typed denial selects the matching monotonic counter and Go-compatible 201
/// rejection; it never reaches the responder.
pub trait DhtInboundAdmissionPolicy: Send + Sync + 'static {
    fn admit(&self, source: SocketAddr) -> Result<(), DhtInboundRateLimitDenial>;
}

impl DhtInboundAdmissionPolicy for DhtInboundRateLimiter {
    fn admit(&self, source: SocketAddr) -> Result<(), DhtInboundRateLimitDenial> {
        DhtInboundRateLimiter::admit(self, source)
    }
}

/// The terminal state of the bounded concurrent DHT supervisor.
#[derive(Debug)]
pub enum DhtConcurrentSupervisorExit<R, S> {
    /// Shutdown won before another receive or joined handler was processed.
    Shutdown,
    /// The exact receive or reply-send boundary stopped the supervisor.
    Failed(DhtDriverError<R, S>),
}

struct HandlerFailure<E> {
    prepared: Box<DhtDispatchOutcome>,
    error: DhtSendError<E>,
}

struct RejectionWork {
    prepared: Box<DhtDispatchOutcome>,
    _outstanding: OwnedSemaphorePermit,
}

struct InboundMode {
    policy: Arc<dyn DhtInboundAdmissionPolicy>,
    max_outstanding_rejections: NonZeroUsize,
}

enum QueueRejectionOutcome {
    Queued,
    Full,
    WorkerClosed,
}

/// A continuous receive supervisor with bounded concurrent query handlers.
///
/// Responses and errors are delivered inline by [`ReceiveDispatcher`] and do
/// not consume query capacity. [`Self::from_dispatcher`] retains the legacy
/// silent drop at capacity. [`Self::with_inbound_policy`] instead applies a
/// capacity-first gate, then a synchronous receive-order policy, and queues
/// exact 201 rejections through one owned bounded FIFO worker. Capacity denial
/// deliberately preserves the peer's next rate-policy admission.
pub struct DhtConcurrentSupervisor<R, S, I, T = KTable>
where
    S: DatagramSender,
{
    receiver: ReceiveDispatcher<R, I>,
    sender: S,
    dispatcher: Arc<DhtDispatcher<T>>,
    handler_permits: Arc<Semaphore>,
    inbound: Option<InboundMode>,
    inbound_stats: DhtInboundStats,
    handlers: JoinSet<Result<(), HandlerFailure<S::Error>>>,
}

impl<R, S, I, T> DhtConcurrentSupervisor<R, S, I, T>
where
    S: DatagramSender,
{
    /// Construct a bounded supervisor around one production-shaped dispatcher.
    #[must_use]
    pub fn from_dispatcher(
        receiver: R,
        registry: TransactionRegistry<I>,
        sender: S,
        dispatcher: DhtDispatcher<T>,
        max_inflight_queries: NonZeroUsize,
    ) -> Self {
        Self {
            receiver: ReceiveDispatcher::new(receiver, registry),
            sender,
            dispatcher: Arc::new(dispatcher),
            handler_permits: Arc::new(Semaphore::new(max_inflight_queries.get())),
            inbound: None,
            inbound_stats: DhtInboundStats::new(),
            handlers: JoinSet::new(),
        }
    }

    /// Construct the production-shaped inbound policy and rejection lane.
    ///
    /// `max_outstanding_rejections` counts both the rejection currently being
    /// sent and every queued rejection. Once that total is reached, the newest
    /// rejection is dropped without cloning or polling another sender. Handler
    /// capacity is checked before `policy`, so local saturation consumes no
    /// per-peer or global policy token.
    #[must_use]
    pub fn with_inbound_policy<P>(
        receiver: R,
        registry: TransactionRegistry<I>,
        sender: S,
        dispatcher: DhtDispatcher<T>,
        policy: P,
        max_inflight_queries: NonZeroUsize,
        max_outstanding_rejections: NonZeroUsize,
    ) -> Self
    where
        P: DhtInboundAdmissionPolicy,
    {
        Self {
            receiver: ReceiveDispatcher::new(receiver, registry),
            sender,
            dispatcher: Arc::new(dispatcher),
            handler_permits: Arc::new(Semaphore::new(max_inflight_queries.get())),
            inbound: Some(InboundMode {
                policy: Arc::new(policy),
                max_outstanding_rejections,
            }),
            inbound_stats: DhtInboundStats::new(),
            handlers: JoinSet::new(),
        }
    }

    /// Clone the shared monotonic inbound admission and rejection counters.
    #[must_use]
    pub fn inbound_stats(&self) -> DhtInboundStats {
        self.inbound_stats.clone()
    }

    async fn abort_and_drain_all(
        &mut self,
        rejection_workers: &mut JoinSet<Result<(), HandlerFailure<S::Error>>>,
    ) {
        self.handlers.abort_all();
        rejection_workers.abort_all();
        while self.handlers.join_next().await.is_some() {}
        while rejection_workers.join_next().await.is_some() {}
    }

    async fn finish_rejection_worker(
        &mut self,
        rejection_workers: &mut JoinSet<Result<(), HandlerFailure<S::Error>>>,
        joined: Result<Result<(), HandlerFailure<S::Error>>, tokio::task::JoinError>,
    ) -> DhtConcurrentSupervisorExit<R::Error, S::Error>
    where
        R: DatagramReceiver,
    {
        enum Terminal<E> {
            Failure(HandlerFailure<E>),
            Panic(Box<dyn std::any::Any + Send + 'static>),
            Cancelled,
            UnexpectedCleanExit,
        }

        let terminal = match joined {
            Ok(Err(failure)) => Terminal::Failure(failure),
            Err(error) if error.is_panic() => Terminal::Panic(error.into_panic()),
            Err(error) => {
                debug_assert!(error.is_cancelled());
                Terminal::Cancelled
            }
            Ok(Ok(())) => Terminal::UnexpectedCleanExit,
        };
        self.abort_and_drain_all(rejection_workers).await;
        match terminal {
            Terminal::Failure(failure) => {
                DhtConcurrentSupervisorExit::Failed(DhtDriverError::Send {
                    prepared: failure.prepared,
                    error: failure.error,
                })
            }
            Terminal::Panic(payload) => resume_unwind(payload),
            Terminal::Cancelled => {
                panic!("DHT rejection worker was cancelled outside supervisor cleanup")
            }
            Terminal::UnexpectedCleanExit => {
                panic!("DHT rejection worker exited while its queue sender remained live")
            }
        }
    }
}

impl<R, S, I, T> DhtConcurrentSupervisor<R, S, I, T>
where
    R: DatagramReceiver,
    S: DatagramSender + Clone + Send + 'static,
    S::Error: Send + 'static,
    I: TransactionIdIssuer,
    T: DhtResponderTable + 'static,
{
    /// Continuously receive datagrams while bounding admitted queries.
    ///
    /// Shutdown is biased ahead of an admitted-handler join, which is biased
    /// ahead of the rejection-worker join and the next receive. Response and
    /// error envelopes therefore reach the shared registry even while every
    /// send is backpressured. A receive or child send failure aborts and fully
    /// drains both owned lanes before returning its exact typed error. A child
    /// panic is resumed with its original payload after the same cleanup; an
    /// externally cancelled child is an invariant violation.
    pub async fn run<F>(&mut self, shutdown: F) -> DhtConcurrentSupervisorExit<R::Error, S::Error>
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);

        let inbound_policy = self
            .inbound
            .as_ref()
            .map(|inbound| Arc::clone(&inbound.policy));
        let mut rejection_workers = JoinSet::new();
        let rejection_lane = if let Some(inbound) = self.inbound.as_ref() {
            let capacity = inbound.max_outstanding_rejections.get();
            let (tx, rx) = mpsc::channel(capacity);
            let outstanding = Arc::new(Semaphore::new(capacity));
            let sender = match catch_unwind(AssertUnwindSafe(|| self.sender.clone())) {
                Ok(sender) => sender,
                Err(payload) => {
                    self.abort_and_drain_all(&mut rejection_workers).await;
                    resume_unwind(payload);
                }
            };
            let stats = self.inbound_stats.clone();
            rejection_workers.spawn(rejection_worker(sender, rx, stats));
            Some((tx, outstanding))
        } else {
            None
        };

        loop {
            enum Next<R, H, W> {
                Shutdown,
                HandlerJoined(H),
                RejectionJoined(W),
                Received(R),
            }

            let next = tokio::select! {
                biased;
                () = &mut shutdown => Next::Shutdown,
                joined = self.handlers.join_next(), if !self.handlers.is_empty() => {
                    Next::HandlerJoined(joined.expect("guarded handler join remains present"))
                }
                joined = rejection_workers.join_next(), if !rejection_workers.is_empty() => {
                    Next::RejectionJoined(joined.expect("guarded rejection worker remains present"))
                }
                received = self.receiver.receive_one() => Next::Received(received),
            };

            match next {
                Next::Shutdown => {
                    self.abort_and_drain_all(&mut rejection_workers).await;
                    return DhtConcurrentSupervisorExit::Shutdown;
                }
                Next::HandlerJoined(Ok(Ok(()))) => {}
                Next::HandlerJoined(Ok(Err(failure))) => {
                    self.abort_and_drain_all(&mut rejection_workers).await;
                    return DhtConcurrentSupervisorExit::Failed(DhtDriverError::Send {
                        prepared: failure.prepared,
                        error: failure.error,
                    });
                }
                Next::HandlerJoined(Err(error)) if error.is_panic() => {
                    let payload = error.into_panic();
                    self.abort_and_drain_all(&mut rejection_workers).await;
                    resume_unwind(payload);
                }
                Next::HandlerJoined(Err(error)) => {
                    debug_assert!(error.is_cancelled());
                    self.abort_and_drain_all(&mut rejection_workers).await;
                    panic!("DHT concurrent handler was cancelled outside supervisor cleanup");
                }
                Next::RejectionJoined(joined) => {
                    return self
                        .finish_rejection_worker(&mut rejection_workers, joined)
                        .await;
                }
                Next::Received(Err(error)) => {
                    self.abort_and_drain_all(&mut rejection_workers).await;
                    return DhtConcurrentSupervisorExit::Failed(DhtDriverError::Receive(error));
                }
                Next::Received(Ok(ReceiveDispatchOutcome::Query { source, message })) => {
                    let permit = match Arc::clone(&self.handler_permits).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) if inbound_policy.is_none() => continue,
                        Err(_) => {
                            self.inbound_stats.record_denied_handler_capacity();
                            let outcome = queue_rejection(
                                rejection_lane
                                    .as_ref()
                                    .expect("inbound policy owns a rejection lane"),
                                source,
                                message.transaction_id.clone(),
                                &self.inbound_stats,
                            );
                            if matches!(outcome, QueueRejectionOutcome::WorkerClosed) {
                                let joined = rejection_workers
                                    .join_next()
                                    .await
                                    .expect("closed rejection queue retains its worker result");
                                return self
                                    .finish_rejection_worker(&mut rejection_workers, joined)
                                    .await;
                            }
                            continue;
                        }
                    };

                    if let Some(policy) = inbound_policy.as_ref() {
                        let admission = catch_unwind(AssertUnwindSafe(|| policy.admit(source)));
                        let admission = match admission {
                            Ok(admission) => admission,
                            Err(payload) => {
                                drop(permit);
                                self.abort_and_drain_all(&mut rejection_workers).await;
                                resume_unwind(payload);
                            }
                        };
                        if let Err(denial) = admission {
                            drop(permit);
                            match denial {
                                DhtInboundRateLimitDenial::PerIp => {
                                    self.inbound_stats.record_denied_per_ip();
                                }
                                DhtInboundRateLimitDenial::Global => {
                                    self.inbound_stats.record_denied_global();
                                }
                            }
                            let outcome = queue_rejection(
                                rejection_lane
                                    .as_ref()
                                    .expect("inbound policy owns a rejection lane"),
                                source,
                                message.transaction_id.clone(),
                                &self.inbound_stats,
                            );
                            if matches!(outcome, QueueRejectionOutcome::WorkerClosed) {
                                let joined = rejection_workers
                                    .join_next()
                                    .await
                                    .expect("closed rejection queue retains its worker result");
                                return self
                                    .finish_rejection_worker(&mut rejection_workers, joined)
                                    .await;
                            }
                            continue;
                        }
                        self.inbound_stats.record_admitted();
                    }

                    let dispatcher = Arc::clone(&self.dispatcher);
                    let mut sender = match catch_unwind(AssertUnwindSafe(|| self.sender.clone())) {
                        Ok(sender) => sender,
                        Err(payload) => {
                            drop(permit);
                            self.abort_and_drain_all(&mut rejection_workers).await;
                            resume_unwind(payload);
                        }
                    };
                    self.handlers.spawn(async move {
                        let _permit = permit;
                        let prepared = Box::new(dispatcher.dispatch(source, &message));
                        match send_dht_reply(&mut sender, prepared.reply()).await {
                            Ok(()) => Ok(()),
                            Err(error) => Err(HandlerFailure { prepared, error }),
                        }
                    });
                }
                Next::Received(Ok(_)) => {}
            }
        }
    }
}

fn queue_rejection(
    rejection_lane: &(mpsc::Sender<RejectionWork>, Arc<Semaphore>),
    source: SocketAddr,
    transaction_id: crate::ByteString,
    stats: &DhtInboundStats,
) -> QueueRejectionOutcome {
    let outstanding = match Arc::clone(&rejection_lane.1).try_acquire_owned() {
        Ok(outstanding) => outstanding,
        Err(_) => {
            stats.record_rejection_queue_full_dropped();
            return QueueRejectionOutcome::Full;
        }
    };
    let prepared = Box::new(DhtDispatchOutcome::Reply(DhtReply::too_many_requests(
        source,
        transaction_id,
    )));
    match rejection_lane.0.try_send(RejectionWork {
        prepared,
        _outstanding: outstanding,
    }) {
        Ok(()) => {
            stats.record_rejection_queued();
            QueueRejectionOutcome::Queued
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            stats.record_rejection_queue_full_dropped();
            QueueRejectionOutcome::Full
        }
        Err(mpsc::error::TrySendError::Closed(_)) => QueueRejectionOutcome::WorkerClosed,
    }
}

async fn rejection_worker<S>(
    mut sender: S,
    mut receiver: mpsc::Receiver<RejectionWork>,
    stats: DhtInboundStats,
) -> Result<(), HandlerFailure<S::Error>>
where
    S: DatagramSender,
{
    while let Some(RejectionWork {
        prepared,
        _outstanding: outstanding,
    }) = receiver.recv().await
    {
        let result = send_dht_reply(&mut sender, prepared.reply()).await;
        drop(outstanding);
        match result {
            Ok(()) => stats.record_rejection_sent(),
            Err(error) => {
                match error {
                    DhtSendError::Encode(_) => stats.record_rejection_encode_failed(),
                    DhtSendError::Transport(_) => stats.record_rejection_transport_failed(),
                }
                return Err(HandlerFailure { prepared, error });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::pending;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::panic::panic_any;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::sync::{mpsc, oneshot};

    use crate::{
        ByteString, DhtInboundStatsSnapshot, DhtResponder, DhtResponderLookup, DhtResponderSample,
        Id20, KTableCommand, KrpcMessage, MessageArgs, MessageReturn, ReceivedDatagram,
        RoutingNode, TransactionId, TransactionIdSourceError, TransactionWaitOutcome,
        MAX_INBOUND_DATAGRAM_BYTES,
    };

    use super::*;

    const QUERY_SOURCE: SocketAddr =
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 1), 6_881));
    const REMOTE: SocketAddr =
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(198, 51, 100, 2), 6_882));

    #[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
    #[error("{0}")]
    struct TestReceiveError(&'static str);

    #[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
    #[error("{0}")]
    struct TestSendError(&'static str);

    struct Datagram {
        source: SocketAddr,
        wire: Vec<u8>,
    }

    struct ChannelReceiver {
        datagrams: mpsc::UnboundedReceiver<Result<Datagram, TestReceiveError>>,
        received: mpsc::UnboundedSender<()>,
    }

    impl DatagramReceiver for ChannelReceiver {
        type Error = TestReceiveError;

        fn receive<'a>(
            &'a mut self,
            buffer: &'a mut [u8],
        ) -> Pin<Box<dyn Future<Output = Result<ReceivedDatagram, Self::Error>> + Send + 'a>>
        {
            Box::pin(async move {
                let datagram = self
                    .datagrams
                    .recv()
                    .await
                    .ok_or(TestReceiveError("input closed"))??;
                buffer[..datagram.wire.len()].copy_from_slice(&datagram.wire);
                let _ = self.received.send(());
                Ok(ReceivedDatagram {
                    length: datagram.wire.len(),
                    source: datagram.source,
                })
            })
        }
    }

    fn channel_receiver() -> (
        mpsc::UnboundedSender<Result<Datagram, TestReceiveError>>,
        mpsc::UnboundedReceiver<()>,
        ChannelReceiver,
    ) {
        let (datagram_tx, datagram_rx) = mpsc::unbounded_channel();
        let (received_tx, received_rx) = mpsc::unbounded_channel();
        (
            datagram_tx,
            received_rx,
            ChannelReceiver {
                datagrams: datagram_rx,
                received: received_tx,
            },
        )
    }

    enum SendAction {
        Wait {
            release: oneshot::Receiver<Result<(), TestSendError>>,
            dropped: Arc<AtomicBool>,
        },
        Pending {
            dropped: Arc<AtomicBool>,
        },
        Return(Result<(), TestSendError>),
        Panic(&'static str),
    }

    struct SendState {
        actions: Mutex<VecDeque<SendAction>>,
        started: mpsc::UnboundedSender<usize>,
        finished: mpsc::UnboundedSender<usize>,
        calls: AtomicUsize,
        records: Mutex<Vec<(SocketAddr, Vec<u8>)>>,
    }

    #[derive(Clone)]
    struct ScriptedSender(Arc<SendState>);

    impl ScriptedSender {
        fn new(
            actions: impl IntoIterator<Item = SendAction>,
        ) -> (
            Self,
            mpsc::UnboundedReceiver<usize>,
            mpsc::UnboundedReceiver<usize>,
        ) {
            let (started_tx, started_rx) = mpsc::unbounded_channel();
            let (finished_tx, finished_rx) = mpsc::unbounded_channel();
            (
                Self(Arc::new(SendState {
                    actions: Mutex::new(actions.into_iter().collect()),
                    started: started_tx,
                    finished: finished_tx,
                    calls: AtomicUsize::new(0),
                    records: Mutex::new(Vec::new()),
                })),
                started_rx,
                finished_rx,
            )
        }

        fn records(&self) -> Vec<(SocketAddr, Vec<u8>)> {
            self.0.records.lock().unwrap().clone()
        }
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    impl DatagramSender for ScriptedSender {
        type Error = TestSendError;

        fn send<'a>(
            &'a mut self,
            destination: SocketAddr,
            datagram: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            self.0
                .records
                .lock()
                .unwrap()
                .push((destination, datagram.to_vec()));
            let call = self.0.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let action = self
                .0
                .actions
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(SendAction::Return(Ok(())));
            let _ = self.0.started.send(call);
            let finished = self.0.finished.clone();
            Box::pin(async move {
                let result = match action {
                    SendAction::Wait { release, dropped } => {
                        let _drop_flag = DropFlag(dropped);
                        release.await.expect("scripted release remains live")
                    }
                    SendAction::Pending { dropped } => {
                        let _drop_flag = DropFlag(dropped);
                        pending().await
                    }
                    SendAction::Return(result) => result,
                    SendAction::Panic(payload) => panic_any(payload),
                };
                let _ = finished.send(call);
                result
            })
        }
    }

    #[derive(Clone)]
    struct TestTable {
        batch_calls: Arc<AtomicUsize>,
    }

    impl TestTable {
        fn new() -> Self {
            Self {
                batch_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl DhtResponderTable for TestTable {
        fn origin(&self) -> Id20 {
            local_id()
        }

        fn closest_nodes(&self, _id: Id20) -> Vec<RoutingNode> {
            Vec::new()
        }

        fn get_hash_or_closest_nodes(&self, _id: Id20) -> DhtResponderLookup {
            DhtResponderLookup::ClosestNodes(Vec::new())
        }

        fn batch_command(&self, _commands: &[KTableCommand]) {
            self.batch_calls.fetch_add(1, Ordering::SeqCst);
        }

        fn sample_hashes_and_nodes(&self) -> DhtResponderSample {
            DhtResponderSample {
                hashes: Vec::new(),
                nodes: Vec::new(),
                total_hashes: 0,
            }
        }
    }

    struct TestIssuer(u16);

    impl TransactionIdIssuer for TestIssuer {
        fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
            let id = self.0.to_be_bytes();
            self.0 = self.0.wrapping_add(1);
            Ok(TransactionId::from(id))
        }
    }

    #[derive(Clone)]
    struct ScriptedPolicy {
        state: Arc<Mutex<ScriptedPolicyState>>,
    }

    struct ScriptedPolicyState {
        outcomes: VecDeque<Result<(), DhtInboundRateLimitDenial>>,
        calls: Vec<SocketAddr>,
    }

    impl ScriptedPolicy {
        fn new(outcomes: impl IntoIterator<Item = Result<(), DhtInboundRateLimitDenial>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(ScriptedPolicyState {
                    outcomes: outcomes.into_iter().collect(),
                    calls: Vec::new(),
                })),
            }
        }

        fn calls(&self) -> Vec<SocketAddr> {
            self.state.lock().unwrap().calls.clone()
        }
    }

    impl DhtInboundAdmissionPolicy for ScriptedPolicy {
        fn admit(&self, source: SocketAddr) -> Result<(), DhtInboundRateLimitDenial> {
            let mut state = self.state.lock().unwrap();
            state.calls.push(source);
            state
                .outcomes
                .pop_front()
                .unwrap_or(Err(DhtInboundRateLimitDenial::PerIp))
        }
    }

    #[tokio::test]
    async fn inbound_policy_denials_are_typed_and_evaluated_in_receive_order() {
        let registry = TransactionRegistry::new(TestIssuer(1));
        let (input, _received, receiver) = channel_receiver();
        let policy = ScriptedPolicy::new([
            Err(DhtInboundRateLimitDenial::PerIp),
            Err(DhtInboundRateLimitDenial::Global),
            Ok(()),
        ]);
        let policy_observer = policy.clone();
        let (sender, mut started, _finished) = ScriptedSender::new([
            SendAction::Return(Ok(())),
            SendAction::Return(Ok(())),
            SendAction::Return(Ok(())),
        ]);
        let sender_observer = sender.clone();
        let mut supervisor =
            supervisor_with_policy(receiver, registry, sender, TestTable::new(), policy, 3, 3);
        let stats = supervisor.inbound_stats();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            supervisor
                .run(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        feed(&input, QUERY_SOURCE, ping_query(b"P1"));
        feed(&input, REMOTE, ping_query(b"G1"));
        feed(&input, QUERY_SOURCE, ping_query(b"A1"));
        for _ in 0..3 {
            started.recv().await.expect("three reply sends start");
        }
        wait_for_stats(&stats, |snapshot| {
            snapshot.rejection_sent == 2 && snapshot.admitted == 1
        })
        .await;

        assert_eq!(
            policy_observer.calls(),
            vec![QUERY_SOURCE, REMOTE, QUERY_SOURCE]
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.admitted, 1);
        assert_eq!(snapshot.denied_per_ip, 1);
        assert_eq!(snapshot.denied_global, 1);
        assert_eq!(snapshot.rejection_queued, 2);
        assert_eq!(snapshot.rejection_sent, 2);
        let records = sender_observer.records();
        assert_eq!(records.len(), 3);
        assert_rejection_for_tid(&records, b"P1");
        assert_rejection_for_tid(&records, b"G1");
        let admitted = record_for_tid(&records, b"A1");
        assert!(admitted.response.is_some());
        assert!(admitted.error.is_none());

        shutdown_tx.send(()).unwrap();
        assert!(matches!(
            task.await.unwrap(),
            DhtConcurrentSupervisorExit::Shutdown
        ));
    }

    #[tokio::test]
    async fn capacity_denial_precedes_policy_and_preserves_its_next_admission() {
        let registry = TransactionRegistry::new(TestIssuer(1));
        let (input, _received, receiver) = channel_receiver();
        let policy = ScriptedPolicy::new([Ok(()), Ok(())]);
        let policy_observer = policy.clone();
        let (release_tx, release_rx) = oneshot::channel();
        let blocked_dropped = Arc::new(AtomicBool::new(false));
        let (sender, mut started, _finished) = ScriptedSender::new([
            SendAction::Wait {
                release: release_rx,
                dropped: Arc::clone(&blocked_dropped),
            },
            SendAction::Return(Ok(())),
            SendAction::Return(Ok(())),
        ]);
        let sender_observer = sender.clone();
        let mut supervisor =
            supervisor_with_policy(receiver, registry, sender, TestTable::new(), policy, 1, 2);
        let handler_permits = Arc::clone(&supervisor.handler_permits);
        let stats = supervisor.inbound_stats();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            supervisor
                .run(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        feed(&input, QUERY_SOURCE, ping_query(b"H1"));
        assert_eq!(started.recv().await, Some(1));
        feed(&input, QUERY_SOURCE, ping_query(b"C1"));
        assert_eq!(started.recv().await, Some(2));
        wait_for_stats(&stats, |snapshot| snapshot.rejection_sent == 1).await;
        assert_eq!(policy_observer.calls(), vec![QUERY_SOURCE]);

        release_tx.send(Ok(())).unwrap();
        wait_for_permit(&handler_permits).await;
        feed(&input, QUERY_SOURCE, ping_query(b"A2"));
        assert_eq!(started.recv().await, Some(3));
        wait_for_stats(&stats, |snapshot| snapshot.admitted == 2).await;
        assert_eq!(policy_observer.calls(), vec![QUERY_SOURCE, QUERY_SOURCE]);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.denied_handler_capacity, 1);
        assert_eq!(snapshot.denied_per_ip, 0);
        assert_eq!(snapshot.denied_global, 0);
        let records = sender_observer.records();
        assert_rejection_for_tid(&records, b"C1");
        assert!(record_for_tid(&records, b"A2").response.is_some());

        shutdown_tx.send(()).unwrap();
        assert!(matches!(
            task.await.unwrap(),
            DhtConcurrentSupervisorExit::Shutdown
        ));
        assert!(blocked_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn rate_denied_announce_sends_exact_go_201_without_mutation() {
        let registry = TransactionRegistry::new(TestIssuer(1));
        let (input, _received, receiver) = channel_receiver();
        let source: SocketAddr = "203.0.113.9:6999".parse().unwrap();
        let policy = ScriptedPolicy::new([Err(DhtInboundRateLimitDenial::PerIp)]);
        let (sender, mut started, _finished) = ScriptedSender::new([SendAction::Return(Ok(()))]);
        let sender_observer = sender.clone();
        let table = TestTable::new();
        let batch_calls = Arc::clone(&table.batch_calls);
        let mut supervisor =
            supervisor_with_policy(receiver, registry, sender, table, policy, 1, 1);
        let stats = supervisor.inbound_stats();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            supervisor
                .run(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        feed(&input, source, announce_query(b"L1"));
        assert_eq!(started.recv().await, Some(1));
        wait_for_stats(&stats, |snapshot| snapshot.rejection_sent == 1).await;
        assert_eq!(batch_calls.load(Ordering::SeqCst), 0);
        let records = sender_observer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, source);
        assert_eq!(
            records[0].1,
            b"d1:eli201e17:too many requestse1:t2:L11:y1:re"
        );
        assert_eq!(
            stats.snapshot(),
            DhtInboundStatsSnapshot {
                denied_per_ip: 1,
                rejection_queued: 1,
                rejection_sent: 1,
                ..DhtInboundStatsSnapshot::default()
            }
        );

        shutdown_tx.send(()).unwrap();
        assert!(matches!(
            task.await.unwrap(),
            DhtConcurrentSupervisorExit::Shutdown
        ));
    }

    #[tokio::test]
    async fn blocked_rejection_keeps_response_delivery_and_allowed_query_live() {
        let registry = TransactionRegistry::new(TestIssuer(1));
        let registered = registry
            .register(REMOTE, ByteString::new(b"ping"), ping_args())
            .unwrap();
        let transaction_id = *registered.transaction_id().as_bytes();
        let pending_query = registered.mark_sent();
        let (input, _received, receiver) = channel_receiver();
        let policy = ScriptedPolicy::new([Err(DhtInboundRateLimitDenial::PerIp), Ok(())]);
        let rejection_dropped = Arc::new(AtomicBool::new(false));
        let (sender, mut started, _finished) = ScriptedSender::new([
            SendAction::Pending {
                dropped: Arc::clone(&rejection_dropped),
            },
            SendAction::Return(Ok(())),
        ]);
        let sender_observer = sender.clone();
        let mut supervisor =
            supervisor_with_policy(receiver, registry, sender, TestTable::new(), policy, 1, 1);
        let stats = supervisor.inbound_stats();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            supervisor
                .run(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        feed(&input, QUERY_SOURCE, ping_query(b"D1"));
        assert_eq!(started.recv().await, Some(1));
        feed(&input, QUERY_SOURCE, ping_query(b"A1"));
        assert_eq!(started.recv().await, Some(2));
        feed(&input, REMOTE, response(&transaction_id));
        assert!(matches!(
            tokio::time::timeout(
                Duration::from_secs(1),
                pending_query.wait(Duration::from_secs(60))
            )
            .await
            .expect("registered response bypasses blocked rejection"),
            TransactionWaitOutcome::Response { source: REMOTE, .. }
        ));
        wait_for_stats(&stats, |snapshot| snapshot.admitted == 1).await;
        let records = sender_observer.records();
        assert_rejection_for_tid(&records, b"D1");
        assert!(record_for_tid(&records, b"A1").response.is_some());

        shutdown_tx.send(()).unwrap();
        assert!(matches!(
            task.await.unwrap(),
            DhtConcurrentSupervisorExit::Shutdown
        ));
        assert!(rejection_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn bound_one_rejection_lane_drops_newest_then_recovers_with_exact_counters() {
        let registry = TransactionRegistry::new(TestIssuer(1));
        let (input, _received, receiver) = channel_receiver();
        let policy = ScriptedPolicy::new([Ok(())]);
        let policy_observer = policy.clone();
        let admitted_dropped = Arc::new(AtomicBool::new(false));
        let rejection_dropped = Arc::new(AtomicBool::new(false));
        let (rejection_release_tx, rejection_release_rx) = oneshot::channel();
        let (sender, mut started, _finished) = ScriptedSender::new([
            SendAction::Pending {
                dropped: Arc::clone(&admitted_dropped),
            },
            SendAction::Wait {
                release: rejection_release_rx,
                dropped: Arc::clone(&rejection_dropped),
            },
            SendAction::Return(Ok(())),
        ]);
        let mut supervisor =
            supervisor_with_policy(receiver, registry, sender, TestTable::new(), policy, 1, 1);
        let stats = supervisor.inbound_stats();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            supervisor
                .run(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        feed(&input, QUERY_SOURCE, ping_query(b"H1"));
        assert_eq!(started.recv().await, Some(1));
        feed(&input, QUERY_SOURCE, ping_query(b"R1"));
        assert_eq!(started.recv().await, Some(2));
        feed(&input, QUERY_SOURCE, ping_query(b"R2"));
        wait_for_stats(&stats, |snapshot| {
            snapshot.rejection_queue_full_dropped == 1
        })
        .await;
        assert!(matches!(
            started.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        rejection_release_tx.send(Ok(())).unwrap();
        wait_for_stats(&stats, |snapshot| snapshot.rejection_sent == 1).await;
        feed(&input, QUERY_SOURCE, ping_query(b"R3"));
        assert_eq!(started.recv().await, Some(3));
        wait_for_stats(&stats, |snapshot| snapshot.rejection_sent == 2).await;
        assert_eq!(policy_observer.calls(), vec![QUERY_SOURCE]);
        assert_eq!(
            stats.snapshot(),
            DhtInboundStatsSnapshot {
                admitted: 1,
                denied_handler_capacity: 3,
                rejection_queued: 2,
                rejection_queue_full_dropped: 1,
                rejection_sent: 2,
                ..DhtInboundStatsSnapshot::default()
            }
        );

        shutdown_tx.send(()).unwrap();
        assert!(matches!(
            task.await.unwrap(),
            DhtConcurrentSupervisorExit::Shutdown
        ));
        assert!(admitted_dropped.load(Ordering::SeqCst));
        assert!(rejection_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn rejection_send_failure_is_terminal_and_drains_admitted_sibling() {
        let registry = TransactionRegistry::new(TestIssuer(1));
        let (input, _received, receiver) = channel_receiver();
        let policy = ScriptedPolicy::new([Ok(()), Err(DhtInboundRateLimitDenial::PerIp)]);
        let admitted_dropped = Arc::new(AtomicBool::new(false));
        let (sender, mut started, _finished) = ScriptedSender::new([
            SendAction::Pending {
                dropped: Arc::clone(&admitted_dropped),
            },
            SendAction::Return(Err(TestSendError("exact rejection failure"))),
        ]);
        let table = TestTable::new();
        let batch_calls = Arc::clone(&table.batch_calls);
        let mut supervisor =
            supervisor_with_policy(receiver, registry, sender, table, policy, 2, 1);
        let stats = supervisor.inbound_stats();
        let task = tokio::spawn(async move { supervisor.run(pending()).await });

        feed(&input, QUERY_SOURCE, announce_query(b"A1"));
        assert_eq!(started.recv().await, Some(1));
        assert_eq!(batch_calls.load(Ordering::SeqCst), 1);
        feed(&input, QUERY_SOURCE, announce_query(b"D1"));
        assert_eq!(started.recv().await, Some(2));

        let DhtConcurrentSupervisorExit::Failed(DhtDriverError::Send { prepared, error }) =
            task.await.unwrap()
        else {
            panic!("rejection send failure must stop the supervisor");
        };
        assert!(matches!(
            error,
            DhtSendError::Transport(TestSendError("exact rejection failure"))
        ));
        assert_eq!(prepared.reply().message.transaction_id.as_bytes(), b"D1");
        assert_eq!(prepared.reply().message.error.as_ref().unwrap().code, 201);
        assert_eq!(batch_calls.load(Ordering::SeqCst), 1);
        assert!(admitted_dropped.load(Ordering::SeqCst));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.admitted, 1);
        assert_eq!(snapshot.denied_per_ip, 1);
        assert_eq!(snapshot.rejection_queued, 1);
        assert_eq!(snapshot.rejection_transport_failed, 1);
        assert_eq!(snapshot.rejection_sent, 0);
    }

    #[tokio::test]
    async fn rejection_sender_panic_preserves_payload_and_drains_admitted_sibling() {
        let registry = TransactionRegistry::new(TestIssuer(1));
        let (input, _received, receiver) = channel_receiver();
        let policy = ScriptedPolicy::new([Ok(()), Err(DhtInboundRateLimitDenial::Global)]);
        let admitted_dropped = Arc::new(AtomicBool::new(false));
        let (sender, mut started, _finished) = ScriptedSender::new([
            SendAction::Pending {
                dropped: Arc::clone(&admitted_dropped),
            },
            SendAction::Panic("exact rejection panic"),
        ]);
        let mut supervisor =
            supervisor_with_policy(receiver, registry, sender, TestTable::new(), policy, 2, 1);
        let stats = supervisor.inbound_stats();
        let task = tokio::spawn(async move { supervisor.run(pending()).await });

        feed(&input, QUERY_SOURCE, ping_query(b"A1"));
        assert_eq!(started.recv().await, Some(1));
        feed(&input, QUERY_SOURCE, ping_query(b"D1"));
        assert_eq!(started.recv().await, Some(2));

        let join_error = task.await.unwrap_err();
        assert!(join_error.is_panic());
        let payload = join_error.into_panic();
        assert_eq!(
            payload.downcast_ref::<&'static str>(),
            Some(&"exact rejection panic")
        );
        assert!(admitted_dropped.load(Ordering::SeqCst));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.admitted, 1);
        assert_eq!(snapshot.denied_global, 1);
        assert_eq!(snapshot.rejection_queued, 1);
        assert_eq!(snapshot.rejection_sent, 0);
        assert_eq!(snapshot.rejection_transport_failed, 0);
    }

    #[tokio::test]
    async fn blocked_query_send_does_not_block_registered_response_delivery() {
        let registry = TransactionRegistry::new(TestIssuer(1));
        let registered = registry
            .register(REMOTE, ByteString::new(b"ping"), ping_args())
            .unwrap();
        let transaction_id = *registered.transaction_id().as_bytes();
        let pending_query = registered.mark_sent();
        let (input, _received, receiver) = channel_receiver();
        let send_dropped = Arc::new(AtomicBool::new(false));
        let (sender, mut started, _finished) = ScriptedSender::new([SendAction::Pending {
            dropped: Arc::clone(&send_dropped),
        }]);
        let mut supervisor = supervisor(receiver, registry, sender, TestTable::new(), 1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            supervisor
                .run(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        feed(&input, QUERY_SOURCE, ping_query(b"Q1"));
        assert_eq!(started.recv().await, Some(1));
        feed(&input, REMOTE, response(&transaction_id));

        assert!(matches!(
            tokio::time::timeout(
                Duration::from_secs(1),
                pending_query.wait(Duration::from_secs(60))
            )
            .await
            .expect("registered response is delivered while send blocks"),
            TransactionWaitOutcome::Response { source: REMOTE, .. }
        ));

        shutdown_tx.send(()).unwrap();
        assert!(matches!(
            task.await.unwrap(),
            DhtConcurrentSupervisorExit::Shutdown
        ));
        assert!(send_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn capacity_drops_newest_before_mutation_then_allows_later_query() {
        let registry = TransactionRegistry::new(TestIssuer(1));
        let (input, mut received, receiver) = channel_receiver();
        let (release_tx, release_rx) = oneshot::channel();
        let first_dropped = Arc::new(AtomicBool::new(false));
        let (sender, mut started, mut finished) = ScriptedSender::new([
            SendAction::Wait {
                release: release_rx,
                dropped: Arc::clone(&first_dropped),
            },
            SendAction::Return(Ok(())),
        ]);
        let table = TestTable::new();
        let batch_calls = Arc::clone(&table.batch_calls);
        let mut supervisor = supervisor(receiver, registry, sender, table, 1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            supervisor
                .run(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        feed(&input, QUERY_SOURCE, announce_query(b"A1"));
        assert_eq!(started.recv().await, Some(1));
        assert_eq!(batch_calls.load(Ordering::SeqCst), 1);

        feed(&input, QUERY_SOURCE, announce_query(b"A2"));
        received.recv().await.unwrap();
        received.recv().await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(batch_calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            started.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        release_tx.send(Ok(())).unwrap();
        assert_eq!(finished.recv().await, Some(1));
        feed(&input, QUERY_SOURCE, announce_query(b"A3"));
        assert_eq!(started.recv().await, Some(2));
        assert_eq!(batch_calls.load(Ordering::SeqCst), 2);

        shutdown_tx.send(()).unwrap();
        assert!(matches!(
            task.await.unwrap(),
            DhtConcurrentSupervisorExit::Shutdown
        ));
        assert!(first_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn send_failure_aborts_and_drains_blocked_sibling() {
        let registry = TransactionRegistry::new(TestIssuer(1));
        let (input, _received, receiver) = channel_receiver();
        let sibling_dropped = Arc::new(AtomicBool::new(false));
        let (sender, mut started, _finished) = ScriptedSender::new([
            SendAction::Pending {
                dropped: Arc::clone(&sibling_dropped),
            },
            SendAction::Return(Err(TestSendError("exact send failure"))),
        ]);
        let mut supervisor = supervisor(receiver, registry, sender, TestTable::new(), 2);
        let task = tokio::spawn(async move { supervisor.run(pending()).await });

        feed(&input, QUERY_SOURCE, ping_query(b"F1"));
        assert_eq!(started.recv().await, Some(1));
        feed(&input, QUERY_SOURCE, ping_query(b"F2"));
        assert_eq!(started.recv().await, Some(2));

        let DhtConcurrentSupervisorExit::Failed(DhtDriverError::Send { error, .. }) =
            task.await.unwrap()
        else {
            panic!("send failure must stop the supervisor");
        };
        assert!(matches!(
            error,
            DhtSendError::Transport(TestSendError("exact send failure"))
        ));
        assert!(sibling_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn handler_panic_resumes_the_original_payload_after_cleanup() {
        let registry = TransactionRegistry::new(TestIssuer(1));
        let (input, _received, receiver) = channel_receiver();
        let (sender, _started, _finished) =
            ScriptedSender::new([SendAction::Panic("exact handler panic")]);
        let mut supervisor = supervisor(receiver, registry, sender, TestTable::new(), 1);
        let task = tokio::spawn(async move { supervisor.run(pending()).await });

        feed(&input, QUERY_SOURCE, ping_query(b"P1"));
        let join_error = task.await.unwrap_err();
        assert!(join_error.is_panic());
        let payload = join_error.into_panic();
        assert_eq!(
            payload.downcast_ref::<&'static str>(),
            Some(&"exact handler panic")
        );
    }

    #[tokio::test]
    async fn shutdown_aborts_and_fully_drains_blocked_handler() {
        let registry = TransactionRegistry::new(TestIssuer(1));
        let (input, _received, receiver) = channel_receiver();
        let handler_dropped = Arc::new(AtomicBool::new(false));
        let (sender, mut started, _finished) = ScriptedSender::new([SendAction::Pending {
            dropped: Arc::clone(&handler_dropped),
        }]);
        let mut supervisor = supervisor(receiver, registry, sender, TestTable::new(), 1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            supervisor
                .run(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        feed(&input, QUERY_SOURCE, ping_query(b"S1"));
        assert_eq!(started.recv().await, Some(1));
        shutdown_tx.send(()).unwrap();
        assert!(matches!(
            task.await.unwrap(),
            DhtConcurrentSupervisorExit::Shutdown
        ));
        assert!(handler_dropped.load(Ordering::SeqCst));
    }

    fn supervisor_with_policy(
        receiver: ChannelReceiver,
        registry: TransactionRegistry<TestIssuer>,
        sender: ScriptedSender,
        table: TestTable,
        policy: ScriptedPolicy,
        max_inflight_queries: usize,
        max_outstanding_rejections: usize,
    ) -> DhtConcurrentSupervisor<ChannelReceiver, ScriptedSender, TestIssuer, TestTable> {
        let responder = DhtResponder::with_token_secret(table, *b"0123456789abcdefghij", 10);
        DhtConcurrentSupervisor::with_inbound_policy(
            receiver,
            registry,
            sender,
            DhtDispatcher::from_responder(responder),
            policy,
            NonZeroUsize::new(max_inflight_queries).unwrap(),
            NonZeroUsize::new(max_outstanding_rejections).unwrap(),
        )
    }

    async fn wait_for_stats(
        stats: &DhtInboundStats,
        predicate: impl Fn(DhtInboundStatsSnapshot) -> bool,
    ) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if predicate(stats.snapshot()) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("inbound stats reach the expected state");
    }

    async fn wait_for_permit(permits: &Semaphore) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while permits.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("handler permit becomes available");
    }

    fn record_for_tid(records: &[(SocketAddr, Vec<u8>)], transaction_id: &[u8]) -> KrpcMessage {
        records
            .iter()
            .map(|(_, wire)| KrpcMessage::decode_inbound(wire).expect("decode captured reply"))
            .find(|message| message.transaction_id.as_bytes() == transaction_id)
            .unwrap_or_else(|| panic!("missing captured reply for transaction {transaction_id:?}"))
    }

    fn assert_rejection_for_tid(records: &[(SocketAddr, Vec<u8>)], transaction_id: &[u8]) {
        let message = record_for_tid(records, transaction_id);
        assert_eq!(message.message_type.as_bytes(), b"r");
        assert!(message.response.is_none());
        let error = message.error.expect("201 rejection contains an error");
        assert_eq!(error.code, 201);
        assert_eq!(error.message.as_bytes(), b"too many requests");
        assert!(message.query.is_empty());
        assert!(message.args.is_none());
        assert!(message.observed_addr.is_none());
        assert!(!message.read_only);
        assert!(message.client_id.is_empty());
    }

    fn supervisor(
        receiver: ChannelReceiver,
        registry: TransactionRegistry<TestIssuer>,
        sender: ScriptedSender,
        table: TestTable,
        max_inflight_queries: usize,
    ) -> DhtConcurrentSupervisor<ChannelReceiver, ScriptedSender, TestIssuer, TestTable> {
        let responder = DhtResponder::with_token_secret(table, *b"0123456789abcdefghij", 10);
        DhtConcurrentSupervisor::from_dispatcher(
            receiver,
            registry,
            sender,
            DhtDispatcher::from_responder(responder),
            NonZeroUsize::new(max_inflight_queries).unwrap(),
        )
    }

    fn feed(
        input: &mpsc::UnboundedSender<Result<Datagram, TestReceiveError>>,
        source: SocketAddr,
        message: KrpcMessage,
    ) {
        let wire = message.encode().unwrap();
        assert!(wire.len() <= MAX_INBOUND_DATAGRAM_BYTES);
        input.send(Ok(Datagram { source, wire })).unwrap();
    }

    fn ping_query(transaction_id: &[u8]) -> KrpcMessage {
        query(transaction_id, b"ping", ping_args())
    }

    fn announce_query(transaction_id: &[u8]) -> KrpcMessage {
        let mut args = ping_args();
        args.info_hash = Some(info_hash());
        args.token = ByteString::new(b"266127f80b327ff927362ec21a79e923");
        args.port = Some(51_413);
        query(transaction_id, b"announce_peer", args)
    }

    fn query(transaction_id: &[u8], method: &[u8], args: MessageArgs) -> KrpcMessage {
        KrpcMessage {
            transaction_id: ByteString::new(transaction_id),
            message_type: ByteString::new(b"q"),
            query: ByteString::new(method),
            args: Some(args),
            response: None,
            error: None,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        }
    }

    fn response(transaction_id: &[u8]) -> KrpcMessage {
        KrpcMessage {
            transaction_id: ByteString::new(transaction_id),
            message_type: ByteString::new(b"r"),
            query: ByteString::default(),
            args: None,
            response: Some(MessageReturn {
                id: local_id(),
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
            error: None,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        }
    }

    fn ping_args() -> MessageArgs {
        MessageArgs {
            id: requester_id(),
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

    fn local_id() -> Id20 {
        Id20::from_hex("00112233445566778899aabbccddeeff10203040").unwrap()
    }

    fn requester_id() -> Id20 {
        Id20::from_hex("ffeeddccbbaa0099887766554433221100abcdef").unwrap()
    }

    fn info_hash() -> Id20 {
        Id20::from_hex("11223344556677889900aabbccddeeff01020304").unwrap()
    }
}
