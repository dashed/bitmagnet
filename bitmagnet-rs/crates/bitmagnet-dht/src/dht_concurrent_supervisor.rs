use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::resume_unwind;
use std::sync::Arc;

use tokio::task::JoinSet;

use crate::{
    send_dht_reply, DatagramReceiver, DatagramSender, DhtDispatchOutcome, DhtDispatcher,
    DhtDriverError, DhtResponderTable, DhtSendError, KTable, ReceiveDispatchOutcome,
    ReceiveDispatcher, TransactionIdIssuer, TransactionRegistry,
};

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

/// A continuous receive supervisor with bounded concurrent query handlers.
///
/// Responses and errors are delivered inline by [`ReceiveDispatcher`] and do
/// not consume query capacity. At capacity, the newest query is silently
/// dropped before responder dispatch. This checkpoint deliberately adds no
/// inbound limiter, rejection reply, queue, retry, or detached task.
pub struct DhtConcurrentSupervisor<R, S, I, T = KTable>
where
    S: DatagramSender,
{
    receiver: ReceiveDispatcher<R, I>,
    sender: S,
    dispatcher: Arc<DhtDispatcher<T>>,
    max_inflight_queries: NonZeroUsize,
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
            max_inflight_queries,
            handlers: JoinSet::new(),
        }
    }

    async fn abort_and_drain_handlers(&mut self) {
        self.handlers.abort_all();
        while self.handlers.join_next().await.is_some() {}
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
    /// Continuously receive datagrams while bounding only admitted queries.
    ///
    /// Shutdown is biased ahead of a guarded child join, which is biased ahead
    /// of the next receive. Response and error envelopes therefore reach the
    /// shared registry even while every query handler is backpressured. A
    /// receive or child send failure aborts and fully drains all siblings before
    /// returning its exact typed error. A child panic is resumed with its
    /// original payload after the same cleanup; an externally cancelled child
    /// is an invariant violation and panics after cleanup.
    pub async fn run<F>(&mut self, shutdown: F) -> DhtConcurrentSupervisorExit<R::Error, S::Error>
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);

        loop {
            enum Next<R, J> {
                Shutdown,
                Joined(J),
                Received(R),
            }

            let next = tokio::select! {
                biased;
                () = &mut shutdown => Next::Shutdown,
                joined = self.handlers.join_next(), if !self.handlers.is_empty() => {
                    Next::Joined(joined.expect("guarded handler join remains present"))
                }
                received = self.receiver.receive_one() => Next::Received(received),
            };

            match next {
                Next::Shutdown => {
                    self.abort_and_drain_handlers().await;
                    return DhtConcurrentSupervisorExit::Shutdown;
                }
                Next::Joined(Ok(Ok(()))) => {}
                Next::Joined(Ok(Err(failure))) => {
                    self.abort_and_drain_handlers().await;
                    return DhtConcurrentSupervisorExit::Failed(DhtDriverError::Send {
                        prepared: failure.prepared,
                        error: failure.error,
                    });
                }
                Next::Joined(Err(error)) if error.is_panic() => {
                    let payload = error.into_panic();
                    self.abort_and_drain_handlers().await;
                    resume_unwind(payload);
                }
                Next::Joined(Err(error)) => {
                    debug_assert!(error.is_cancelled());
                    self.abort_and_drain_handlers().await;
                    panic!("DHT concurrent handler was cancelled outside supervisor cleanup");
                }
                Next::Received(Err(error)) => {
                    self.abort_and_drain_handlers().await;
                    return DhtConcurrentSupervisorExit::Failed(DhtDriverError::Receive(error));
                }
                Next::Received(Ok(ReceiveDispatchOutcome::Query { source, message })) => {
                    if self.handlers.len() >= self.max_inflight_queries.get() {
                        continue;
                    }

                    let dispatcher = Arc::clone(&self.dispatcher);
                    let mut sender = self.sender.clone();
                    self.handlers.spawn(async move {
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
        ByteString, DhtResponder, DhtResponderLookup, DhtResponderSample, Id20, KTableCommand,
        KrpcMessage, MessageArgs, MessageReturn, ReceivedDatagram, RoutingNode, TransactionId,
        TransactionIdSourceError, TransactionWaitOutcome, MAX_INBOUND_DATAGRAM_BYTES,
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
                })),
                started_rx,
                finished_rx,
            )
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
            _destination: SocketAddr,
            _datagram: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
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
