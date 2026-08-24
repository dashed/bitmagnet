use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroUsize;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle};

use crate::{
    CryptoTransactionIdIssuer, DhtClient, DhtClientError, DhtConcurrentSupervisor,
    DhtConcurrentSupervisorExit, DhtDispatcher, DhtDriverError, DhtOutboundRateLimiter,
    DhtResponder, FindNodeResult, GetPeersResult, GetPeersScrapeResult, Id20, KTable, PingResult,
    SampleInfoHashesResult, TokioIpv4UdpError, TokioIpv4UdpTransport, TokioIpv4UdpWeakSendError,
    TokioIpv4UdpWeakSender, TransactionRegistry,
};

const CLIENT_SUFFIX: &[u8; 8] = b"-BM0001-";
const MAX_INFLIGHT_QUERIES: NonZeroUsize = NonZeroUsize::new(64).unwrap();

/// Configuration for the initial owned IPv4 DHT runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtRuntimeConfig {
    /// The IPv4 UDP address to bind.
    pub bind_addr: SocketAddrV4,
    /// Time to await a response after an outbound query has been sent.
    pub query_timeout: Duration,
    /// BEP-51 interval advertised by `sample_infohashes` responses.
    pub sample_infohashes_interval: i64,
}

impl Default for DhtRuntimeConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 3334),
            query_timeout: Duration::from_secs(4),
            sample_infohashes_interval: 10,
        }
    }
}

/// Failures while constructing the owned runtime before its task is spawned.
#[derive(Debug, thiserror::Error)]
pub enum DhtRuntimeStartError {
    /// The process could not generate the random portion of its local node ID.
    #[error("could not generate DHT local node ID: {0}")]
    LocalId(getrandom::Error),
    /// The production responder could not generate its announce-token secret.
    #[error("could not generate DHT announce-token secret: {0}")]
    TokenSecret(getrandom::Error),
    /// The IPv4 UDP transport could not bind its configured address.
    #[error("could not start DHT IPv4 UDP transport: {0}")]
    Transport(#[source] TokioIpv4UdpError),
}

/// The exact driver failure type owned by the production runtime task.
pub type DhtRuntimeDriverError = DhtDriverError<TokioIpv4UdpError, TokioIpv4UdpError>;

/// A terminal result from the owned DHT task.
#[derive(Debug)]
pub enum DhtRuntimeExit {
    /// Graceful shutdown won before another receive or joined query handler.
    Shutdown,
    /// The receive, reply-encode, or reply-send boundary failed.
    Failed(DhtRuntimeDriverError),
}

/// The typed query failure surfaced by a runtime client handle.
pub type DhtRuntimeClientError = DhtClientError<TokioIpv4UdpWeakSendError>;

/// A cloneable typed query client that does not own the runtime's UDP socket.
///
/// Clones share the production transaction registry and outbound rate limiter.
/// The weak sender upgrades the socket only for the duration of an admitted
/// send, so retained client handles cannot keep the runtime's bound port open.
/// Each convenience method uses the limiter's unbounded `wait` admission; this
/// surface does not yet expose admission deadlines or typed cancellation.
/// Dropping or selecting away the whole method still cancels an in-flight
/// admission reservation or query registration through their owned guards.
#[derive(Clone)]
pub struct DhtRuntimeClient {
    client: DhtClient<CryptoTransactionIdIssuer>,
    sender: TokioIpv4UdpWeakSender,
    outbound_rate_limiter: DhtOutboundRateLimiter,
}

impl DhtRuntimeClient {
    fn new(
        local_id: Id20,
        registry: &TransactionRegistry<CryptoTransactionIdIssuer>,
        query_timeout: Duration,
        sender: TokioIpv4UdpWeakSender,
    ) -> Self {
        Self {
            client: DhtClient::new(local_id, registry, query_timeout),
            sender,
            outbound_rate_limiter: DhtOutboundRateLimiter::new(),
        }
    }

    /// The cached local address from which this client sends while the runtime
    /// remains live. The value itself is not a liveness check.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddrV4 {
        self.sender.local_addr()
    }

    /// Admit, send, and await one typed `ping` query.
    pub async fn ping(&self, remote: SocketAddr) -> Result<PingResult, DhtRuntimeClientError> {
        self.outbound_rate_limiter.wait(remote).await;
        self.client.ping(&mut self.sender.clone(), remote).await
    }

    /// Admit, send, and await one typed `find_node` query.
    pub async fn find_node(
        &self,
        remote: SocketAddr,
        target: Id20,
    ) -> Result<FindNodeResult, DhtRuntimeClientError> {
        self.outbound_rate_limiter.wait(remote).await;
        self.client
            .find_node(&mut self.sender.clone(), remote, target)
            .await
    }

    /// Admit, send, and await one typed `get_peers` query.
    pub async fn get_peers(
        &self,
        remote: SocketAddr,
        info_hash: Id20,
    ) -> Result<GetPeersResult, DhtRuntimeClientError> {
        self.outbound_rate_limiter.wait(remote).await;
        self.client
            .get_peers(&mut self.sender.clone(), remote, info_hash)
            .await
    }

    /// Admit, send, and await one typed BEP-33 scrape query.
    pub async fn get_peers_scrape(
        &self,
        remote: SocketAddr,
        info_hash: Id20,
    ) -> Result<GetPeersScrapeResult, DhtRuntimeClientError> {
        self.outbound_rate_limiter.wait(remote).await;
        self.client
            .get_peers_scrape(&mut self.sender.clone(), remote, info_hash)
            .await
    }

    /// Admit, send, and await one typed BEP-51 query.
    pub async fn sample_infohashes(
        &self,
        remote: SocketAddr,
        target: Id20,
    ) -> Result<SampleInfoHashesResult, DhtRuntimeClientError> {
        self.outbound_rate_limiter.wait(remote).await;
        self.client
            .sample_infohashes(&mut self.sender.clone(), remote, target)
            .await
    }
}

/// The initial owned production DHT composition over one shared IPv4 socket.
///
/// The background task admits at most 64 concurrent query handlers. Response
/// and error envelopes bypass that capacity and are delivered inline to the
/// transaction registry even while reply sends are backpressured. At capacity,
/// the newest query is silently dropped before responder dispatch. The inbound
/// rate limiter and an outer responder timeout are not wired here yet.
pub struct DhtRuntime {
    local_addr: SocketAddrV4,
    local_id: Id20,
    table: KTable,
    client: DhtRuntimeClient,
    registry: TransactionRegistry<CryptoTransactionIdIssuer>,
    shutdown_tx: watch::Sender<bool>,
    task: Option<JoinHandle<DhtRuntimeExit>>,
}

impl DhtRuntime {
    /// Construct the production table/responder/registry/supervisor composition,
    /// bind its shared IPv4 UDP socket, and spawn the owned bounded-concurrent
    /// task.
    pub async fn start(config: DhtRuntimeConfig) -> Result<Self, DhtRuntimeStartError> {
        let local_id = random_local_id().map_err(DhtRuntimeStartError::LocalId)?;
        let table = KTable::new(local_id);
        let registry = TransactionRegistry::default();
        let responder = DhtResponder::new(&table, config.sample_infohashes_interval)
            .map_err(DhtRuntimeStartError::TokenSecret)?;
        let dispatcher = DhtDispatcher::from_responder(responder);

        let transport = TokioIpv4UdpTransport::bind(config.bind_addr)
            .await
            .map_err(DhtRuntimeStartError::Transport)?;
        let local_addr = transport.local_addr();
        let (receiver, sender) = transport.into_parts();
        let weak_sender = sender.downgrade();
        let client = DhtRuntimeClient::new(local_id, &registry, config.query_timeout, weak_sender);

        let mut supervisor = DhtConcurrentSupervisor::from_dispatcher(
            receiver,
            registry.clone(),
            sender,
            dispatcher,
            MAX_INFLIGHT_QUERIES,
        );
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task_registry = registry.clone();
        let task = tokio::spawn(async move {
            let _registry_guard = RegistryCloseGuard(task_registry);
            match supervisor.run(wait_for_shutdown(&mut shutdown_rx)).await {
                DhtConcurrentSupervisorExit::Shutdown => DhtRuntimeExit::Shutdown,
                DhtConcurrentSupervisorExit::Failed(error) => DhtRuntimeExit::Failed(error),
            }
        });

        Ok(Self {
            local_addr,
            local_id,
            table,
            client,
            registry,
            shutdown_tx,
            task: Some(task),
        })
    }

    /// The actual bound IPv4 address, including an OS-assigned port.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddrV4 {
        self.local_addr
    }

    /// The random local node ID, including the Bitmagnet client suffix.
    #[must_use]
    pub const fn local_id(&self) -> Id20 {
        self.local_id
    }

    /// Borrow the shared production routing/hash table.
    #[must_use]
    pub const fn table(&self) -> &KTable {
        &self.table
    }

    /// Clone a non-owning typed query handle.
    #[must_use]
    pub fn client(&self) -> DhtRuntimeClient {
        self.client.clone()
    }

    /// Request graceful shutdown and await the exact task terminal result.
    ///
    /// Cancelling this consuming future drops the runtime, which closes the
    /// registry and aborts the task instead of detaching it.
    pub async fn shutdown(mut self) -> Result<DhtRuntimeExit, JoinError> {
        let _ = self.shutdown_tx.send(true);
        self.take_task().await
    }

    /// Await a natural task exit without requesting shutdown.
    ///
    /// Cancelling this consuming future drops the runtime and therefore closes
    /// the registry and aborts the owned task.
    pub async fn wait(mut self) -> Result<DhtRuntimeExit, JoinError> {
        self.take_task().await
    }

    async fn take_task(&mut self) -> Result<DhtRuntimeExit, JoinError> {
        let result = self
            .task
            .as_mut()
            .expect("DHT runtime task is present until the runtime is consumed")
            .await;
        self.task.take();
        result
    }
}

impl Drop for DhtRuntime {
    fn drop(&mut self) {
        self.registry.close();
        let _ = self.shutdown_tx.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct RegistryCloseGuard(TransactionRegistry<CryptoTransactionIdIssuer>);

impl Drop for RegistryCloseGuard {
    fn drop(&mut self) {
        self.0.close();
    }
}

async fn wait_for_shutdown(shutdown_rx: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown_rx.borrow_and_update() {
            return;
        }
        if shutdown_rx.changed().await.is_err() {
            return;
        }
    }
}

fn random_local_id() -> Result<Id20, getrandom::Error> {
    let mut bytes = [0; 20];
    getrandom::fill(&mut bytes)?;
    let suffix_start = bytes.len() - CLIENT_SUFFIX.len();
    bytes[suffix_start..].copy_from_slice(CLIENT_SUFFIX);
    Ok(Id20::from_slice(&bytes).expect("a 20-byte node ID always has valid length"))
}

#[cfg(test)]
mod tests {
    use std::future::{pending, poll_fn, Future};
    use std::task::Poll;

    use tokio::net::UdpSocket;
    use tokio::sync::oneshot;

    use crate::{
        QuerySendError, RegisterError, TokioIpv4UdpWeakSendError, TransactionRegistry,
        MAX_INBOUND_DATAGRAM_BYTES,
    };

    use super::*;

    #[test]
    fn defaults_and_random_local_id_match_production_contract() {
        let config = DhtRuntimeConfig::default();
        assert_eq!(
            config.bind_addr,
            SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 3334)
        );
        assert_eq!(config.query_timeout, Duration::from_secs(4));
        assert_eq!(config.sample_infohashes_interval, 10);

        let first = random_local_id().unwrap();
        assert_eq!(&first.as_bytes()[12..], CLIENT_SUFFIX);
    }

    #[tokio::test]
    async fn shared_socket_self_query_shutdown_and_weak_client_lifecycle() {
        let runtime = DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            query_timeout: Duration::from_secs(1),
            ..DhtRuntimeConfig::default()
        })
        .await
        .unwrap();
        let local_addr = runtime.local_addr();
        let local_id = runtime.local_id();
        let client = runtime.client();

        assert_eq!(runtime.table().origin(), local_id);
        assert_eq!(client.local_addr(), local_addr);
        assert_eq!(
            client.ping(SocketAddr::V4(local_addr)).await.unwrap(),
            PingResult { id: local_id }
        );

        assert!(matches!(
            runtime.shutdown().await.unwrap(),
            DhtRuntimeExit::Shutdown
        ));

        let stopped = client.ping(SocketAddr::V4(local_addr)).await;
        assert!(matches!(
            stopped,
            Err(DhtClientError::QuerySend(QuerySendError::Register(
                RegisterError::RegistryClosed
            )))
        ));

        let rebound = TokioIpv4UdpTransport::bind(local_addr).await.unwrap();
        assert_eq!(rebound.local_addr(), local_addr);
    }

    #[tokio::test]
    async fn drop_closes_registry_for_retained_weak_client() {
        let runtime = DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            query_timeout: Duration::from_secs(1),
            ..DhtRuntimeConfig::default()
        })
        .await
        .unwrap();
        let remote = SocketAddr::V4(runtime.local_addr());
        let local_addr = runtime.local_addr();
        let client = runtime.client();

        drop(runtime);
        tokio::task::yield_now().await;

        let rebound = tokio::time::timeout(
            Duration::from_secs(1),
            TokioIpv4UdpTransport::bind(local_addr),
        )
        .await
        .expect("aborted runtime task releases its socket promptly")
        .unwrap();
        assert_eq!(rebound.local_addr(), local_addr);

        assert!(matches!(
            client.ping(remote).await,
            Err(DhtClientError::QuerySend(QuerySendError::Register(
                RegisterError::RegistryClosed
            )))
        ));
    }

    #[tokio::test]
    async fn cancelling_consuming_wait_and_shutdown_abort_instead_of_detaching() {
        for shutdown in [false, true] {
            let (runtime, client, local_addr) = stubborn_runtime().await;
            let remote = SocketAddr::V4(local_addr);

            if shutdown {
                let mut future = Box::pin(runtime.shutdown());
                let first_poll = poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await;
                assert!(first_poll.is_pending());
                drop(future);
            } else {
                let mut future = Box::pin(runtime.wait());
                let first_poll = poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await;
                assert!(first_poll.is_pending());
                drop(future);
            }

            tokio::task::yield_now().await;
            let rebound = tokio::time::timeout(
                Duration::from_secs(1),
                TokioIpv4UdpTransport::bind(local_addr),
            )
            .await
            .expect("cancelled consuming runtime future releases its socket promptly")
            .unwrap();
            assert_eq!(rebound.local_addr(), local_addr);

            assert!(matches!(
                client.ping(remote).await,
                Err(DhtClientError::QuerySend(QuerySendError::Register(
                    RegisterError::RegistryClosed
                )))
            ));
        }
    }

    #[tokio::test]
    async fn pending_blackhole_query_closes_on_graceful_shutdown_and_drop() {
        for graceful in [true, false] {
            let PendingBlackhole {
                runtime,
                registry,
                retained_client,
                query,
                local_addr,
                remote,
                _blackhole,
            } = pending_blackhole_query().await;

            assert_eq!(registry.pending_count(), 1);
            if graceful {
                assert!(matches!(
                    runtime.shutdown().await.unwrap(),
                    DhtRuntimeExit::Shutdown
                ));
            } else {
                drop(runtime);
            }

            assert!(matches!(
                query.await.unwrap(),
                Err(DhtClientError::RegistryClosed)
            ));
            assert_eq!(registry.pending_count(), 0);
            assert_registry_closed(&retained_client, remote).await;

            let rebound = rebind_after_task_drop(local_addr).await;
            assert_eq!(rebound.local_addr(), local_addr);
        }
    }

    #[tokio::test]
    async fn task_panic_closes_pending_query_registry_and_socket() {
        let registry = TransactionRegistry::default();
        let local_id = Id20::ZERO;
        let table = KTable::new(local_id);
        let transport = TokioIpv4UdpTransport::bind_loopback().await.unwrap();
        let local_addr = transport.local_addr();
        let (receiver, sender) = transport.into_parts();
        let client = DhtRuntimeClient::new(
            local_id,
            &registry,
            Duration::from_secs(1),
            sender.downgrade(),
        );
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let (panic_tx, panic_rx) = oneshot::channel();
        let task_registry = registry.clone();
        let task: JoinHandle<DhtRuntimeExit> = tokio::spawn(async move {
            let _registry_guard = RegistryCloseGuard(task_registry);
            let socket_owners = (receiver, sender);
            panic_rx.await.expect("panic trigger remains live");
            let _ = &socket_owners;
            panic!("synthetic DHT runtime task panic");
        });
        let runtime = DhtRuntime {
            local_addr,
            local_id,
            table,
            client: client.clone(),
            registry: registry.clone(),
            shutdown_tx,
            task: Some(task),
        };

        let blackhole = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let remote = blackhole.local_addr().unwrap();
        let query_client = client.clone();
        let query = tokio::spawn(async move { query_client.ping(remote).await });
        receive_blackhole_query(&blackhole).await;
        wait_for_pending(&registry, 1).await;

        panic_tx.send(()).unwrap();
        let join_error = runtime.wait().await.unwrap_err();
        assert_eq!(classify_join_error(&join_error), "panic");
        assert!(matches!(
            query.await.unwrap(),
            Err(DhtClientError::RegistryClosed)
        ));
        assert_eq!(registry.pending_count(), 0);
        assert_registry_closed(&client, remote).await;

        let rebound = rebind_after_task_drop(local_addr).await;
        assert_eq!(rebound.local_addr(), local_addr);
    }

    #[test]
    fn runtime_public_error_surfaces_remain_exhaustive() {
        let _: fn(DhtRuntimeStartError) -> &'static str = classify_start_error;
        let _: fn(DhtRuntimeExit) -> &'static str = classify_runtime_exit;
        let _: fn(DhtRuntimeClientError) -> &'static str = classify_client_error;
        let _: fn(&JoinError) -> &'static str = classify_join_error;

        assert_eq!(classify_runtime_exit(DhtRuntimeExit::Shutdown), "shutdown");
    }

    fn classify_start_error(error: DhtRuntimeStartError) -> &'static str {
        match error {
            DhtRuntimeStartError::LocalId(_) => "local_id",
            DhtRuntimeStartError::TokenSecret(_) => "token_secret",
            DhtRuntimeStartError::Transport(_) => "transport",
        }
    }

    fn classify_runtime_exit(exit: DhtRuntimeExit) -> &'static str {
        match exit {
            DhtRuntimeExit::Shutdown => "shutdown",
            DhtRuntimeExit::Failed(_) => "failed",
        }
    }

    fn classify_client_error(error: DhtRuntimeClientError) -> &'static str {
        match error {
            DhtClientError::QuerySend(error) => match error {
                QuerySendError::Register(_) => "query_register",
                QuerySendError::Encode(_) => "query_encode",
                QuerySendError::Transport(error) => match error {
                    TokioIpv4UdpWeakSendError::Stopped => "transport_stopped",
                    TokioIpv4UdpWeakSendError::Transport(_) => "transport_live",
                },
            },
            DhtClientError::RemoteError { .. } => "remote",
            DhtClientError::MissingReturnBody { .. } => "missing_return",
            DhtClientError::MissingErrorBody { .. } => "missing_error",
            DhtClientError::MissingScrapeBloomFilters { .. } => "missing_blooms",
            DhtClientError::Timeout => "timeout",
            DhtClientError::RegistryClosed => "registry_closed",
        }
    }

    fn classify_join_error(error: &JoinError) -> &'static str {
        if error.is_panic() {
            "panic"
        } else if error.is_cancelled() {
            "cancelled"
        } else {
            "unknown"
        }
    }

    struct PendingBlackhole {
        runtime: DhtRuntime,
        registry: TransactionRegistry<CryptoTransactionIdIssuer>,
        retained_client: DhtRuntimeClient,
        query: JoinHandle<Result<PingResult, DhtRuntimeClientError>>,
        local_addr: SocketAddrV4,
        remote: SocketAddr,
        _blackhole: UdpSocket,
    }

    async fn pending_blackhole_query() -> PendingBlackhole {
        let runtime = DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            query_timeout: Duration::from_secs(60),
            ..DhtRuntimeConfig::default()
        })
        .await
        .unwrap();
        let local_addr = runtime.local_addr();
        let registry = runtime.registry.clone();
        let retained_client = runtime.client();
        let blackhole = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let remote = blackhole.local_addr().unwrap();
        let query_client = runtime.client();
        let query = tokio::spawn(async move { query_client.ping(remote).await });

        receive_blackhole_query(&blackhole).await;
        wait_for_pending(&registry, 1).await;

        PendingBlackhole {
            runtime,
            registry,
            retained_client,
            query,
            local_addr,
            remote,
            _blackhole: blackhole,
        }
    }

    async fn receive_blackhole_query(blackhole: &UdpSocket) {
        let mut buffer = [0; MAX_INBOUND_DATAGRAM_BYTES];
        let (length, _) =
            tokio::time::timeout(Duration::from_secs(1), blackhole.recv_from(&mut buffer))
                .await
                .expect("runtime client sends to blackhole promptly")
                .unwrap();
        assert!(length > 0);
    }

    async fn wait_for_pending(
        registry: &TransactionRegistry<CryptoTransactionIdIssuer>,
        expected: usize,
    ) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if registry.pending_count() == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("registry reaches expected pending cardinality");
    }

    async fn assert_registry_closed(client: &DhtRuntimeClient, remote: SocketAddr) {
        assert!(matches!(
            client.ping(remote).await,
            Err(DhtClientError::QuerySend(QuerySendError::Register(
                RegisterError::RegistryClosed
            )))
        ));
    }

    async fn rebind_after_task_drop(local_addr: SocketAddrV4) -> TokioIpv4UdpTransport {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match TokioIpv4UdpTransport::bind(local_addr).await {
                    Ok(transport) => return transport,
                    Err(TokioIpv4UdpError::Bind(error))
                        if error.kind() == std::io::ErrorKind::AddrInUse =>
                    {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("unexpected rebind failure: {error}"),
                }
            }
        })
        .await
        .expect("runtime task releases its socket promptly")
    }

    async fn stubborn_runtime() -> (DhtRuntime, DhtRuntimeClient, SocketAddrV4) {
        let local_id = Id20::ZERO;
        let table = KTable::new(local_id);
        let registry = TransactionRegistry::default();
        let transport = TokioIpv4UdpTransport::bind_loopback().await.unwrap();
        let local_addr = transport.local_addr();
        let (receiver, sender) = transport.into_parts();
        let client = DhtRuntimeClient::new(
            local_id,
            &registry,
            Duration::from_secs(1),
            sender.downgrade(),
        );
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let socket_owners = (receiver, sender);
            pending::<()>().await;
            drop(socket_owners);
            DhtRuntimeExit::Shutdown
        });

        (
            DhtRuntime {
                local_addr,
                local_id,
                table,
                client: client.clone(),
                registry,
                shutdown_tx,
                task: Some(task),
            },
            client,
            local_addr,
        )
    }
}
