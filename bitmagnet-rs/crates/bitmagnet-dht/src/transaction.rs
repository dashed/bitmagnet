use std::collections::HashMap;
use std::fmt;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::{ByteString, KrpcError, KrpcMessage, MessageArgs, MessageReturn, WireError};

pub(crate) const TRANSACTION_ID_BYTES: usize = 2;
const TRANSACTION_ID_SPACE: usize = 1 << (TRANSACTION_ID_BYTES * 8);

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionId([u8; TRANSACTION_ID_BYTES]);

impl TransactionId {
    pub fn from_slice(value: &[u8]) -> Result<Self, TransactionIdError> {
        let bytes = value
            .try_into()
            .map_err(|_| TransactionIdError::InvalidLength(value.len()))?;
        Ok(Self(bytes))
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; TRANSACTION_ID_BYTES] {
        &self.0
    }

    #[must_use]
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

impl From<[u8; TRANSACTION_ID_BYTES]> for TransactionId {
    fn from(value: [u8; TRANSACTION_ID_BYTES]) -> Self {
        Self(value)
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TransactionIdError {
    #[error("KRPC transaction ID has length {0}; expected 2")]
    InvalidLength(usize),
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[error("transaction ID source failed: {message}")]
pub struct TransactionIdSourceError {
    message: String,
}

impl TransactionIdSourceError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Injectable fallible transaction-ID source. Implementations are serialized
/// with registration so scripted tests can force collisions deterministically.
pub trait TransactionIdIssuer: Send {
    fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CryptoTransactionIdIssuer;

impl TransactionIdIssuer for CryptoTransactionIdIssuer {
    fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
        let mut bytes = [0; TRANSACTION_ID_BYTES];
        getrandom::fill(&mut bytes)
            .map_err(|error| TransactionIdSourceError::new(error.to_string()))?;
        Ok(TransactionId(bytes))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AcceptedResponse {
    source: SocketAddr,
    message: KrpcMessage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EntryState {
    Awaiting,
    Delivered,
}

struct PendingEntry {
    generation: u64,
    expected_source: SocketAddr,
    sender: mpsc::Sender<AcceptedResponse>,
    state: EntryState,
}

#[derive(Default)]
struct RegistryState {
    closed: bool,
    next_generation: u64,
    pending: HashMap<TransactionId, PendingEntry>,
}

struct RegistryInner {
    state: Mutex<RegistryState>,
}

/// Pure transaction correlation without socket or receive-loop ownership.
pub struct TransactionRegistry<I> {
    issuer: Arc<Mutex<I>>,
    inner: Arc<RegistryInner>,
}

impl<I> Clone for TransactionRegistry<I> {
    fn clone(&self) -> Self {
        Self {
            issuer: Arc::clone(&self.issuer),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<I: TransactionIdIssuer> TransactionRegistry<I> {
    #[must_use]
    pub fn new(issuer: I) -> Self {
        Self {
            issuer: Arc::new(Mutex::new(issuer)),
            inner: Arc::new(RegistryInner {
                state: Mutex::new(RegistryState::default()),
            }),
        }
    }

    /// Atomically allocate a unique TID and insert its expected source. The
    /// returned typestate owns that registration immediately, before any wire
    /// send can occur.
    pub fn register(
        &self,
        remote: SocketAddr,
        query: ByteString,
        args: MessageArgs,
    ) -> Result<RegisteredQuery, RegisterError> {
        let (transaction_id, generation, receiver) = {
            let mut issuer = lock(&self.issuer);
            let mut state = lock(&self.inner.state);
            if state.closed {
                return Err(RegisterError::RegistryClosed);
            }
            if state.pending.len() == TRANSACTION_ID_SPACE {
                return Err(RegisterError::TransactionIdSpaceFull);
            }

            let mut selected = None;
            for _ in 0..TRANSACTION_ID_SPACE {
                let candidate = issuer.issue().map_err(RegisterError::IdSource)?;
                if !state.pending.contains_key(&candidate) {
                    selected = Some(candidate);
                    break;
                }
            }
            let transaction_id = selected.ok_or(RegisterError::CollisionRetryExhausted)?;
            let generation = state.next_generation;
            state.next_generation = state
                .next_generation
                .checked_add(1)
                .ok_or(RegisterError::GenerationExhausted)?;
            let (sender, receiver) = mpsc::channel(1);
            state.pending.insert(
                transaction_id,
                PendingEntry {
                    generation,
                    expected_source: normalize_socket_addr(remote),
                    sender,
                    state: EntryState::Awaiting,
                },
            );
            (transaction_id, generation, receiver)
        };

        let message = KrpcMessage {
            transaction_id: ByteString::new(transaction_id.as_bytes()),
            message_type: ByteString::new(b"q"),
            query,
            args: Some(args),
            response: None,
            error: None,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        };
        Ok(RegisteredQuery {
            guard: Some(RegistrationGuard {
                inner: Arc::clone(&self.inner),
                transaction_id,
                generation,
            }),
            receiver: Some(receiver),
            remote,
            message,
        })
    }

    /// Convenience wrapper that keeps registration live while `send` builds
    /// and transmits the canonical query. Send failure drops it immediately.
    pub fn register_before_send<E>(
        &self,
        remote: SocketAddr,
        query: ByteString,
        args: MessageArgs,
        send: impl FnOnce(&RegisteredQuery) -> Result<(), E>,
    ) -> Result<PendingTransaction, RegisterSendError<E>> {
        self.register(remote, query, args)
            .map_err(RegisterSendError::Register)?
            .send(send)
    }

    /// Deliver only response/error envelopes. Validation deliberately follows
    /// type, TID lookup, normalized source, then duplicate order.
    pub fn deliver(&self, source: SocketAddr, message: KrpcMessage) -> DeliveryOutcome {
        if !matches!(message.message_type.as_bytes(), b"r" | b"e") {
            return DeliveryOutcome::InvalidMessageType;
        }
        let transaction_id = match TransactionId::from_slice(message.transaction_id.as_bytes()) {
            Ok(transaction_id) => transaction_id,
            Err(_) => return DeliveryOutcome::InvalidTransactionId,
        };
        let source = normalize_socket_addr(source);

        let mut state = lock(&self.inner.state);
        if state.closed {
            return DeliveryOutcome::RegistryClosed;
        }
        let Some(pending) = state.pending.get_mut(&transaction_id) else {
            return DeliveryOutcome::UnknownTransaction;
        };
        if source != pending.expected_source {
            return DeliveryOutcome::AddressMismatch {
                expected_source: pending.expected_source,
            };
        }
        if pending.state == EntryState::Delivered {
            return DeliveryOutcome::Duplicate;
        }

        match pending
            .sender
            .try_send(AcceptedResponse { source, message })
        {
            Ok(()) => {
                pending.state = EntryState::Delivered;
                DeliveryOutcome::Delivered
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                pending.state = EntryState::Delivered;
                DeliveryOutcome::Duplicate
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                let generation = pending.generation;
                if state
                    .pending
                    .get(&transaction_id)
                    .is_some_and(|entry| entry.generation == generation)
                {
                    state.pending.remove(&transaction_id);
                }
                DeliveryOutcome::WaiterGone
            }
        }
    }

    pub fn close(&self) {
        let mut state = lock(&self.inner.state);
        state.closed = true;
        state.pending.clear();
    }

    #[must_use]
    pub fn pending_count(&self) -> usize {
        lock(&self.inner.state).pending.len()
    }

    #[must_use]
    pub fn is_pending(&self, transaction_id: TransactionId) -> bool {
        lock(&self.inner.state)
            .pending
            .contains_key(&transaction_id)
    }
}

impl Default for TransactionRegistry<CryptoTransactionIdIssuer> {
    fn default() -> Self {
        Self::new(CryptoTransactionIdIssuer)
    }
}

struct RegistrationGuard {
    inner: Arc<RegistryInner>,
    transaction_id: TransactionId,
    generation: u64,
}

impl Drop for RegistrationGuard {
    fn drop(&mut self) {
        remove_registration(&self.inner, self.transaction_id, self.generation);
    }
}

/// A query already present in the correlation map but not yet marked sent.
pub struct RegisteredQuery {
    guard: Option<RegistrationGuard>,
    receiver: Option<mpsc::Receiver<AcceptedResponse>>,
    remote: SocketAddr,
    message: KrpcMessage,
}

impl RegisteredQuery {
    #[must_use]
    pub fn transaction_id(&self) -> TransactionId {
        self.guard
            .as_ref()
            .expect("registered query guard is present until ownership transfer")
            .transaction_id
    }

    #[must_use]
    pub const fn remote(&self) -> SocketAddr {
        self.remote
    }

    #[must_use]
    pub const fn message(&self) -> &KrpcMessage {
        &self.message
    }

    pub fn wire(&self) -> Result<Vec<u8>, WireError> {
        self.message.encode()
    }

    pub fn send<E>(
        self,
        send: impl FnOnce(&RegisteredQuery) -> Result<(), E>,
    ) -> Result<PendingTransaction, RegisterSendError<E>> {
        send(&self).map_err(RegisterSendError::Send)?;
        Ok(self.mark_sent())
    }

    #[must_use]
    pub fn mark_sent(mut self) -> PendingTransaction {
        PendingTransaction {
            guard: self.guard.take(),
            receiver: self.receiver.take(),
        }
    }
}

pub struct PendingTransaction {
    guard: Option<RegistrationGuard>,
    receiver: Option<mpsc::Receiver<AcceptedResponse>>,
}

impl PendingTransaction {
    #[must_use]
    pub fn transaction_id(&self) -> TransactionId {
        self.guard
            .as_ref()
            .expect("pending query retains its registration guard")
            .transaction_id
    }

    pub async fn wait(mut self, timeout: Duration) -> TransactionWaitOutcome {
        let receiver = self
            .receiver
            .as_mut()
            .expect("pending query retains its response receiver");
        let outcome = match tokio::time::timeout(timeout, receiver.recv()).await {
            Ok(Some(accepted)) => classify_response(accepted),
            Ok(None) => TransactionWaitOutcome::RegistryClosed,
            Err(_) => TransactionWaitOutcome::Timeout,
        };
        self.finish();
        outcome
    }

    #[must_use]
    pub fn cancel(mut self) -> TransactionWaitOutcome {
        self.finish();
        TransactionWaitOutcome::Cancelled
    }

    fn finish(&mut self) {
        self.receiver.take();
        self.guard.take();
    }
}

impl Drop for PendingTransaction {
    fn drop(&mut self) {
        self.finish();
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum TransactionWaitOutcome {
    Response {
        source: SocketAddr,
        message: Box<KrpcMessage>,
        response: Box<MessageReturn>,
    },
    RemoteError {
        source: SocketAddr,
        message: Box<KrpcMessage>,
        error: KrpcError,
    },
    MissingReturnBody {
        source: SocketAddr,
        message: Box<KrpcMessage>,
    },
    MissingErrorBody {
        source: SocketAddr,
        message: Box<KrpcMessage>,
    },
    Timeout,
    Cancelled,
    RegistryClosed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryOutcome {
    Delivered,
    Duplicate,
    UnknownTransaction,
    InvalidTransactionId,
    InvalidMessageType,
    AddressMismatch { expected_source: SocketAddr },
    WaiterGone,
    RegistryClosed,
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum RegisterError {
    #[error("transaction registry is closed")]
    RegistryClosed,
    #[error("all 65,536 two-byte transaction IDs are registered")]
    TransactionIdSpaceFull,
    #[error("transaction ID source produced only collisions in 65,536 attempts")]
    CollisionRetryExhausted,
    #[error(transparent)]
    IdSource(#[from] TransactionIdSourceError),
    #[error("transaction registration generation counter is exhausted")]
    GenerationExhausted,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RegisterSendError<E> {
    #[error(transparent)]
    Register(#[from] RegisterError),
    #[error("send failed: {0}")]
    Send(E),
}

fn classify_response(accepted: AcceptedResponse) -> TransactionWaitOutcome {
    if accepted.message.message_type.as_bytes() == b"e" {
        match accepted.message.error.clone() {
            Some(error) => TransactionWaitOutcome::RemoteError {
                source: accepted.source,
                message: Box::new(accepted.message),
                error,
            },
            None => TransactionWaitOutcome::MissingErrorBody {
                source: accepted.source,
                message: Box::new(accepted.message),
            },
        }
    } else {
        match accepted.message.response.clone() {
            Some(response) => TransactionWaitOutcome::Response {
                source: accepted.source,
                message: Box::new(accepted.message),
                response: Box::new(response),
            },
            None => TransactionWaitOutcome::MissingReturnBody {
                source: accepted.source,
                message: Box::new(accepted.message),
            },
        }
    }
}

fn normalize_socket_addr(address: SocketAddr) -> SocketAddr {
    match address {
        SocketAddr::V6(address) if address.scope_id() == 0 => address
            .ip()
            .to_ipv4_mapped()
            .map(|ip| SocketAddr::V4(SocketAddrV4::new(ip, address.port())))
            .unwrap_or_else(|| {
                SocketAddr::V6(SocketAddrV6::new(*address.ip(), address.port(), 0, 0))
            }),
        SocketAddr::V6(address) => SocketAddr::V6(SocketAddrV6::new(
            *address.ip(),
            address.port(),
            0,
            address.scope_id(),
        )),
        address => address,
    }
}

fn remove_registration(inner: &RegistryInner, transaction_id: TransactionId, generation: u64) {
    let mut state = lock(&inner.state);
    if state
        .pending
        .get(&transaction_id)
        .is_some_and(|pending| pending.generation == generation)
    {
        state.pending.remove(&transaction_id);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, VecDeque};
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::sync::Barrier;

    use super::*;
    use crate::{ByteString, Id20, MessageReturn};

    struct ScriptedIssuer(VecDeque<Result<TransactionId, TransactionIdSourceError>>);

    impl TransactionIdIssuer for ScriptedIssuer {
        fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
            self.0
                .pop_front()
                .unwrap_or_else(|| Err(TransactionIdSourceError::new("scripted issuer exhausted")))
        }
    }

    struct ConstantIssuer(TransactionId);

    impl TransactionIdIssuer for ConstantIssuer {
        fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
            Ok(self.0)
        }
    }

    struct CountingIssuer(u16);

    impl TransactionIdIssuer for CountingIssuer {
        fn issue(&mut self) -> Result<TransactionId, TransactionIdSourceError> {
            let issued = self.0;
            self.0 = self.0.checked_add(1).unwrap();
            Ok(TransactionId(issued.to_be_bytes()))
        }
    }

    fn scripted(values: impl IntoIterator<Item = [u8; 2]>) -> ScriptedIssuer {
        ScriptedIssuer(values.into_iter().map(|value| Ok(value.into())).collect())
    }

    fn tid(value: [u8; 2]) -> TransactionId {
        TransactionId::from(value)
    }

    fn query_name() -> ByteString {
        ByteString::new(b"ping")
    }

    fn query_args() -> MessageArgs {
        MessageArgs {
            id: Id20::ZERO,
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

    fn message(tid: &[u8], kind: &[u8], has_body: bool) -> KrpcMessage {
        KrpcMessage {
            transaction_id: ByteString::new(tid),
            message_type: ByteString::new(kind),
            query: ByteString::default(),
            args: None,
            response: (kind == b"r" && has_body).then_some(MessageReturn {
                id: Id20::ZERO,
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
            error: (kind == b"e" && has_body).then(|| KrpcError {
                code: 201,
                message: ByteString::new(b"remote"),
            }),
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        }
    }

    #[test]
    fn registered_typestate_is_visible_before_send_and_transfers_cleanup() {
        let registry = TransactionRegistry::new(scripted([*b"A1", *b"B2", *b"C3"]));
        let remote: SocketAddr = "1.2.3.4:6881".parse().unwrap();

        let registered = registry
            .register(remote, query_name(), query_args())
            .unwrap();
        assert!(registry.is_pending(tid(*b"A1")));
        drop(registered);
        assert_eq!(registry.pending_count(), 0);

        let pending = registry
            .register_before_send(remote, query_name(), query_args(), |registered| {
                assert_eq!(registered.transaction_id(), tid(*b"B2"));
                assert!(registry.is_pending(registered.transaction_id()));
                Ok::<_, ()>(())
            })
            .unwrap();
        assert_eq!(registry.pending_count(), 1);
        drop(pending);
        assert_eq!(registry.pending_count(), 0);

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = registry.register_before_send(
                remote,
                query_name(),
                query_args(),
                |_| -> Result<(), ()> { panic!("synthetic send unwind") },
            );
        }));
        assert!(unwind.is_err());
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn send_failure_cancel_and_stale_generation_cleanup_are_safe() {
        let registry = TransactionRegistry::new(scripted([*b"A1", *b"A1", *b"A1"]));
        let remote: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        assert!(matches!(
            registry.register_before_send(remote, query_name(), query_args(), |_| Err("send")),
            Err(RegisterSendError::Send("send"))
        ));
        assert_eq!(registry.pending_count(), 0);

        let old = registry
            .register(remote, query_name(), query_args())
            .unwrap();
        let old_generation = old.guard.as_ref().unwrap().generation;
        remove_registration(&registry.inner, tid(*b"A1"), old_generation);
        let current = registry
            .register(remote, query_name(), query_args())
            .unwrap();
        let current_generation = current.guard.as_ref().unwrap().generation;
        assert_ne!(old_generation, current_generation);
        std::thread::spawn(move || drop(old)).join().unwrap();
        assert!(registry.is_pending(tid(*b"A1")));
        assert_eq!(
            current.mark_sent().cancel(),
            TransactionWaitOutcome::Cancelled
        );
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn cloned_registry_serializes_high_contention_registration() {
        const CALLERS: usize = 64;
        let registry = TransactionRegistry::new(CountingIssuer(0));
        let barrier = Arc::new(Barrier::new(CALLERS + 1));
        let (sender, receiver) = std::sync::mpsc::channel();
        let remote: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let threads = (0..CALLERS)
            .map(|_| {
                let registry = registry.clone();
                let barrier = Arc::clone(&barrier);
                let sender = sender.clone();
                std::thread::spawn(move || {
                    let registered = registry
                        .register(remote, query_name(), query_args())
                        .unwrap();
                    sender.send(registered.transaction_id()).unwrap();
                    barrier.wait();
                    drop(registered);
                })
            })
            .collect::<Vec<_>>();
        drop(sender);
        let tids = receiver.iter().take(CALLERS).collect::<HashSet<_>>();
        assert_eq!(tids.len(), CALLERS);
        assert_eq!(registry.pending_count(), CALLERS);
        barrier.wait();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn guard_drop_recovers_a_poisoned_registry_without_panicking() {
        let registry = TransactionRegistry::new(scripted([*b"A1"]));
        let remote: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let registered = registry
            .register(remote, query_name(), query_args())
            .unwrap();
        let inner = Arc::clone(&registry.inner);
        assert!(std::thread::spawn(move || {
            let _state = inner.state.lock().unwrap();
            panic!("synthetic registry poison");
        })
        .join()
        .is_err());
        assert!(std::panic::catch_unwind(|| drop(registered)).is_ok());
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn bounded_issue_failures_are_distinct() {
        let remote: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let source_registry = TransactionRegistry::new(ScriptedIssuer(VecDeque::from([Err(
            TransactionIdSourceError::new("entropy unavailable"),
        )])));
        assert!(matches!(
            source_registry.register(remote, query_name(), query_args()),
            Err(RegisterError::IdSource(_))
        ));

        let collision_registry = TransactionRegistry::new(ConstantIssuer(tid(*b"A1")));
        let occupied = collision_registry
            .register(remote, query_name(), query_args())
            .unwrap();
        assert!(matches!(
            collision_registry.register(remote, query_name(), query_args()),
            Err(RegisterError::CollisionRetryExhausted)
        ));
        drop(occupied);

        let full_registry = TransactionRegistry::new(ConstantIssuer(tid(*b"A1")));
        {
            let mut state = lock(&full_registry.inner.state);
            for raw in 0..=u16::MAX {
                let (sender, _receiver) = mpsc::channel(1);
                state.pending.insert(
                    TransactionId(raw.to_be_bytes()),
                    PendingEntry {
                        generation: u64::from(raw),
                        expected_source: remote,
                        sender,
                        state: EntryState::Awaiting,
                    },
                );
            }
        }
        assert!(matches!(
            full_registry.register(remote, query_name(), query_args()),
            Err(RegisterError::TransactionIdSpaceFull)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_abort_and_close_cleanup() {
        let registry = Arc::new(TransactionRegistry::new(scripted([*b"A1", *b"B2", *b"C3"])));
        let remote: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let pending = registry
            .register(remote, query_name(), query_args())
            .unwrap()
            .mark_sent();
        let waiter = tokio::spawn(pending.wait(Duration::from_secs(10)));
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(waiter.await.unwrap(), TransactionWaitOutcome::Timeout);
        assert_eq!(registry.pending_count(), 0);

        let pending = registry
            .register(remote, query_name(), query_args())
            .unwrap()
            .mark_sent();
        let waiter = tokio::spawn(pending.wait(Duration::from_secs(10)));
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(waiter.await.is_err());
        assert_eq!(registry.pending_count(), 0);

        let pending = registry
            .register(remote, query_name(), query_args())
            .unwrap()
            .mark_sent();
        registry.close();
        assert_eq!(
            pending.wait(Duration::from_secs(1)).await,
            TransactionWaitOutcome::RegistryClosed
        );
    }

    #[tokio::test]
    async fn delivery_order_first_wins_and_body_outcomes_are_typed() {
        let registry = TransactionRegistry::new(scripted([*b"A1", *b"B2", *b"C3"]));
        let remote: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let pending = registry
            .register(remote, query_name(), query_args())
            .unwrap()
            .mark_sent();
        assert_eq!(
            registry.deliver(remote, message(b"A1", b"q", true)),
            DeliveryOutcome::InvalidMessageType
        );
        assert_eq!(
            registry.deliver(remote, message(b"X", b"r", true)),
            DeliveryOutcome::InvalidTransactionId
        );
        assert_eq!(
            registry.deliver(remote, message(b"ZZ", b"r", true)),
            DeliveryOutcome::UnknownTransaction
        );
        assert!(matches!(
            registry.deliver("1.2.3.4:6882".parse().unwrap(), message(b"A1", b"r", true)),
            DeliveryOutcome::AddressMismatch { .. }
        ));
        assert_eq!(
            registry.deliver(remote, message(b"A1", b"r", true)),
            DeliveryOutcome::Delivered
        );
        assert!(registry.is_pending(tid(*b"A1")));
        assert!(matches!(
            registry.deliver("1.2.3.4:6882".parse().unwrap(), message(b"A1", b"r", true)),
            DeliveryOutcome::AddressMismatch { .. }
        ));
        assert_eq!(
            registry.deliver(remote, message(b"A1", b"r", true)),
            DeliveryOutcome::Duplicate
        );
        assert!(matches!(
            pending.wait(Duration::from_secs(1)).await,
            TransactionWaitOutcome::Response { source, .. } if source == remote
        ));

        let pending = registry
            .register(remote, query_name(), query_args())
            .unwrap()
            .mark_sent();
        assert_eq!(
            registry.deliver(remote, message(b"B2", b"e", true)),
            DeliveryOutcome::Delivered
        );
        assert!(matches!(
            pending.wait(Duration::from_secs(1)).await,
            TransactionWaitOutcome::RemoteError { source, error, .. }
                if source == remote && error.code == 201
        ));

        let pending = registry
            .register(remote, query_name(), query_args())
            .unwrap()
            .mark_sent();
        assert_eq!(
            registry.deliver(remote, message(b"C3", b"r", false)),
            DeliveryOutcome::Delivered
        );
        assert!(matches!(
            pending.wait(Duration::from_secs(1)).await,
            TransactionWaitOutcome::MissingReturnBody { source, message }
                if source == remote
                    && message.transaction_id.as_bytes() == b"C3"
                    && message.message_type.as_bytes() == b"r"
        ));

        let registry = TransactionRegistry::new(scripted([*b"D4"]));
        let pending = registry
            .register(remote, query_name(), query_args())
            .unwrap()
            .mark_sent();
        assert_eq!(
            registry.deliver(remote, message(b"D4", b"e", false)),
            DeliveryOutcome::Delivered
        );
        assert!(matches!(
            pending.wait(Duration::from_secs(1)).await,
            TransactionWaitOutcome::MissingErrorBody { source, message }
                if source == remote
                    && message.transaction_id.as_bytes() == b"D4"
                    && message.message_type.as_bytes() == b"e"
        ));
    }

    #[test]
    fn concurrent_duplicates_are_nonblocking_and_only_one_is_delivered() {
        const RESPONSES: usize = 32;
        let registry = TransactionRegistry::new(scripted([*b"A1"]));
        let remote: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let pending = registry
            .register(remote, query_name(), query_args())
            .unwrap()
            .mark_sent();
        let barrier = Arc::new(Barrier::new(RESPONSES + 1));
        let threads = (0..RESPONSES)
            .map(|_| {
                let registry = registry.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    registry.deliver(remote, message(b"A1", b"r", true))
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let outcomes = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == DeliveryOutcome::Delivered)
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == DeliveryOutcome::Duplicate)
                .count(),
            RESPONSES - 1
        );
        assert!(registry.is_pending(tid(*b"A1")));
        drop(pending);
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn closed_waiter_delivery_removes_only_its_generation() {
        let registry = TransactionRegistry::new(scripted([*b"A1"]));
        let remote: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let mut registered = registry
            .register(remote, query_name(), query_args())
            .unwrap();
        registered.receiver.take();
        assert_eq!(
            registry.deliver(remote, message(b"A1", b"r", true)),
            DeliveryOutcome::WaiterGone
        );
        assert_eq!(registry.pending_count(), 0);
        drop(registered);
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn address_normalization_is_scope_aware_and_ignores_flowinfo() {
        let plain = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 6881));
        let mapped = SocketAddr::V6(SocketAddrV6::new(
            Ipv4Addr::new(1, 2, 3, 4).to_ipv6_mapped(),
            6881,
            17,
            0,
        ));
        assert_eq!(normalize_socket_addr(plain), normalize_socket_addr(mapped));

        let scoped_mapped = SocketAddr::V6(SocketAddrV6::new(
            Ipv4Addr::new(1, 2, 3, 4).to_ipv6_mapped(),
            6881,
            17,
            3,
        ));
        assert_ne!(
            normalize_socket_addr(plain),
            normalize_socket_addr(scoped_mapped)
        );

        let ip: Ipv6Addr = "fe80::1".parse().unwrap();
        let native_a = SocketAddr::V6(SocketAddrV6::new(ip, 6881, 1, 3));
        let native_b = SocketAddr::V6(SocketAddrV6::new(ip, 6881, 99, 3));
        let native_wrong_scope = SocketAddr::V6(SocketAddrV6::new(ip, 6881, 1, 4));
        assert_eq!(
            normalize_socket_addr(native_a),
            normalize_socket_addr(native_b)
        );
        assert_ne!(
            normalize_socket_addr(native_a),
            normalize_socket_addr(native_wrong_scope)
        );
    }

    #[test]
    fn crypto_issuer_is_fallible_and_always_returns_two_bytes() {
        let mut issuer = CryptoTransactionIdIssuer;
        for _ in 0..1_000 {
            assert_eq!(
                issuer.issue().unwrap().as_bytes().len(),
                TRANSACTION_ID_BYTES
            );
        }
    }
}
