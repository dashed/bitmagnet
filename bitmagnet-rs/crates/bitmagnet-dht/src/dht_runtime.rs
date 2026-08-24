use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroUsize;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle};

use crate::{
    dht_discovery_channel, CryptoTransactionIdIssuer, DhtClient, DhtClientError,
    DhtConcurrentSupervisor, DhtConcurrentSupervisorExit, DhtDiscoveryReceiver,
    DhtDiscoveryStatsHandle, DhtDispatcher, DhtDriverError, DhtInboundRateLimiter, DhtInboundStats,
    DhtOutboundRateLimiter, DhtRateLimitWaitError, DhtResponder, FindNodeResult, GetPeersResult,
    GetPeersScrapeResult, Id20, KTable, PingResult, SampleInfoHashesResult, TokioIpv4UdpError,
    TokioIpv4UdpTransport, TokioIpv4UdpWeakSendError, TokioIpv4UdpWeakSender, TransactionRegistry,
};

const CLIENT_SUFFIX: &[u8; 8] = b"-BM0001-";
const MAX_INFLIGHT_QUERIES: NonZeroUsize = NonZeroUsize::new(64).unwrap();
const MAX_OUTSTANDING_REJECTIONS: NonZeroUsize = NonZeroUsize::new(64).unwrap();
/// Fixed production discovery ingress capacity, matching Go's default
/// `100 * ScalingFactor` with its default scaling factor of ten.
pub const DHT_DISCOVERY_QUEUE_CAPACITY: usize = 1_000;

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

/// A controlled runtime query failed before or after outbound admission.
#[derive(Debug, thiserror::Error)]
pub enum DhtRuntimeControlledQueryError {
    /// The caller's admission cancellation or deadline prevented a query.
    #[error("DHT outbound admission failed: {0}")]
    Admission(
        #[from]
        #[source]
        DhtRateLimitWaitError,
    ),
    /// Admission succeeded and the existing typed query path failed.
    #[error("DHT runtime query failed after admission: {0}")]
    Query(
        #[from]
        #[source]
        DhtRuntimeClientError,
    ),
}

/// A cloneable typed query client that does not own the runtime's UDP socket.
///
/// Clones share the production transaction registry and outbound rate limiter.
/// The weak sender upgrades the socket only for the duration of an admitted
/// send, so retained client handles cannot keep the runtime's bound port open.
/// Each convenience method uses the limiter's unbounded `wait` admission. The
/// corresponding `*_with_admission` method exposes only the admission wait's
/// deadline and cancellation: once admission succeeds, that cancellation
/// future is dropped and the configured response timeout starts only after the
/// datagram send succeeds. Dropping or selecting away the whole method still
/// cancels whichever admission reservation or query registration it then owns.
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

    /// Admit with caller controls, then immediately send and await `ping`.
    pub async fn ping_with_admission<F>(
        &self,
        remote: SocketAddr,
        admission_deadline: Option<tokio::time::Instant>,
        admission_cancellation: F,
    ) -> Result<PingResult, DhtRuntimeControlledQueryError>
    where
        F: Future<Output = ()>,
    {
        let mut sender = self
            .admitted_sender(remote, admission_deadline, admission_cancellation)
            .await?;
        self.client
            .ping(&mut sender, remote)
            .await
            .map_err(DhtRuntimeControlledQueryError::Query)
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

    /// Admit with caller controls, then immediately send and await `find_node`.
    pub async fn find_node_with_admission<F>(
        &self,
        remote: SocketAddr,
        target: Id20,
        admission_deadline: Option<tokio::time::Instant>,
        admission_cancellation: F,
    ) -> Result<FindNodeResult, DhtRuntimeControlledQueryError>
    where
        F: Future<Output = ()>,
    {
        let mut sender = self
            .admitted_sender(remote, admission_deadline, admission_cancellation)
            .await?;
        self.client
            .find_node(&mut sender, remote, target)
            .await
            .map_err(DhtRuntimeControlledQueryError::Query)
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

    /// Admit with caller controls, then immediately send and await `get_peers`.
    pub async fn get_peers_with_admission<F>(
        &self,
        remote: SocketAddr,
        info_hash: Id20,
        admission_deadline: Option<tokio::time::Instant>,
        admission_cancellation: F,
    ) -> Result<GetPeersResult, DhtRuntimeControlledQueryError>
    where
        F: Future<Output = ()>,
    {
        let mut sender = self
            .admitted_sender(remote, admission_deadline, admission_cancellation)
            .await?;
        self.client
            .get_peers(&mut sender, remote, info_hash)
            .await
            .map_err(DhtRuntimeControlledQueryError::Query)
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

    /// Admit with caller controls, then immediately send and await BEP-33 scrape.
    pub async fn get_peers_scrape_with_admission<F>(
        &self,
        remote: SocketAddr,
        info_hash: Id20,
        admission_deadline: Option<tokio::time::Instant>,
        admission_cancellation: F,
    ) -> Result<GetPeersScrapeResult, DhtRuntimeControlledQueryError>
    where
        F: Future<Output = ()>,
    {
        let mut sender = self
            .admitted_sender(remote, admission_deadline, admission_cancellation)
            .await?;
        self.client
            .get_peers_scrape(&mut sender, remote, info_hash)
            .await
            .map_err(DhtRuntimeControlledQueryError::Query)
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

    /// Admit with caller controls, then immediately send and await BEP-51.
    pub async fn sample_infohashes_with_admission<F>(
        &self,
        remote: SocketAddr,
        target: Id20,
        admission_deadline: Option<tokio::time::Instant>,
        admission_cancellation: F,
    ) -> Result<SampleInfoHashesResult, DhtRuntimeControlledQueryError>
    where
        F: Future<Output = ()>,
    {
        let mut sender = self
            .admitted_sender(remote, admission_deadline, admission_cancellation)
            .await?;
        self.client
            .sample_infohashes(&mut sender, remote, target)
            .await
            .map_err(DhtRuntimeControlledQueryError::Query)
    }

    async fn admitted_sender<F>(
        &self,
        remote: SocketAddr,
        admission_deadline: Option<tokio::time::Instant>,
        admission_cancellation: F,
    ) -> Result<TokioIpv4UdpWeakSender, DhtRuntimeControlledQueryError>
    where
        F: Future<Output = ()>,
    {
        self.outbound_rate_limiter
            .wait_with(remote, admission_deadline, admission_cancellation)
            .await
            .map_err(DhtRuntimeControlledQueryError::Admission)?;
        Ok(self.sender.clone())
    }
}

/// The initial owned production DHT composition over one shared IPv4 socket.
///
/// The background task checks its fixed 64-handler capacity before consulting
/// the production inbound limiter. A capacity denial therefore preserves the
/// peer and global limiter tokens; an admitted rate-policy check still precedes
/// responder dispatch and every table effect. Both denial paths queue Go's
/// exact `y=r`, error-201 response through one FIFO lane bounded to 64 total
/// active-plus-queued rejections. A saturated lane drops the newest rejection.
/// Response and error envelopes bypass both query lanes and are delivered inline
/// to the transaction registry even while sends are backpressured.
/// Successful responder calls offer their exact requester node to a fixed
/// 1,000-item discovery queue. The offer owns no task and never waits for queue
/// capacity; saturation or a dropped receiver discards the newest event without
/// changing the reply. The take-once receiver is intentionally not a crawler:
/// batching, known-node filtering, and query scheduling remain downstream.
///
/// Unlike Go's swallowed query-reply send errors, an admitted or rejection send
/// failure terminates this owned task through its typed driver error. No outer
/// responder timeout is applied by this runtime yet.
pub struct DhtRuntime {
    local_addr: SocketAddrV4,
    local_id: Id20,
    table: KTable,
    client: DhtRuntimeClient,
    registry: TransactionRegistry<CryptoTransactionIdIssuer>,
    inbound_stats: DhtInboundStats,
    discovery_stats: DhtDiscoveryStatsHandle,
    discovered_nodes: Option<DhtDiscoveryReceiver>,
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
        let (discovery, discovered_nodes) = dht_discovery_channel(
            NonZeroUsize::new(DHT_DISCOVERY_QUEUE_CAPACITY)
                .expect("production discovery capacity is nonzero"),
        );
        let discovery_stats = discovery.stats_handle();
        let dispatcher = DhtDispatcher::from_responder(responder).with_discovery(discovery);

        let transport = TokioIpv4UdpTransport::bind(config.bind_addr)
            .await
            .map_err(DhtRuntimeStartError::Transport)?;
        let local_addr = transport.local_addr();
        let (receiver, sender) = transport.into_parts();
        let weak_sender = sender.downgrade();
        let client = DhtRuntimeClient::new(local_id, &registry, config.query_timeout, weak_sender);

        let mut supervisor = DhtConcurrentSupervisor::with_inbound_policy(
            receiver,
            registry.clone(),
            sender,
            dispatcher,
            DhtInboundRateLimiter::new(),
            MAX_INFLIGHT_QUERIES,
            MAX_OUTSTANDING_REJECTIONS,
        );
        let inbound_stats = supervisor.inbound_stats();
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
            inbound_stats,
            discovery_stats,
            discovered_nodes: Some(discovered_nodes),
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

    /// Clone the live monotonic inbound admission and rejection counters.
    ///
    /// Each snapshot loads its fields independently and is not transactional
    /// across counters.
    #[must_use]
    pub fn inbound_stats(&self) -> DhtInboundStats {
        self.inbound_stats.clone()
    }

    /// Clone live discovery counters without retaining a queue sender.
    #[must_use]
    pub fn discovery_stats(&self) -> DhtDiscoveryStatsHandle {
        self.discovery_stats.clone()
    }

    /// Take exclusive ownership of the production discovered-node stream.
    ///
    /// The first call returns the receiver; later calls return `None`. Once the
    /// runtime task exits, the receiver drains any queued nodes and then reaches
    /// EOF even while a discovery-stats handle remains alive.
    pub fn take_discovered_nodes(&mut self) -> Option<DhtDiscoveryReceiver> {
        self.discovered_nodes.take()
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
    use std::future::{pending, poll_fn, ready, Future};
    use std::task::Poll;
    use std::time::Instant as WallInstant;

    use tokio::net::UdpSocket;
    use tokio::sync::oneshot;

    use crate::{
        ByteString, DhtDiscoveryStats, DhtInboundStatsSnapshot, KrpcMessage, MessageArgs,
        QuerySendError, RegisterError, RoutingNode, TokioIpv4UdpWeakSendError, TransactionRegistry,
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
        assert_eq!(MAX_INFLIGHT_QUERIES.get(), 64);
        assert_eq!(MAX_OUTSTANDING_REJECTIONS.get(), 64);
        assert_eq!(DHT_DISCOVERY_QUEUE_CAPACITY, 1_000);

        let first = random_local_id().unwrap();
        assert_eq!(&first.as_bytes()[12..], CLIENT_SUFFIX);
    }

    #[tokio::test]
    async fn controlled_admission_precedes_closed_registry_and_query_errors_remain_nested() {
        let (closed_client, closed_registry, remote) = stopped_runtime_client().await;
        closed_registry.close();

        assert!(matches!(
            closed_client
                .ping_with_admission(remote, None, ready(()))
                .await,
            Err(DhtRuntimeControlledQueryError::Admission(
                DhtRateLimitWaitError::Cancelled
            ))
        ));
        assert_eq!(closed_registry.pending_count(), 0);

        let expired = tokio::time::Instant::now()
            .checked_sub(Duration::from_nanos(1))
            .expect("Tokio instant has a predecessor");
        assert!(matches!(
            closed_client
                .find_node_with_admission(remote, id(1), Some(expired), pending())
                .await,
            Err(DhtRuntimeControlledQueryError::Admission(
                DhtRateLimitWaitError::WouldExceedDeadline
            ))
        ));
        assert_eq!(closed_registry.pending_count(), 0);

        assert!(matches!(
            closed_client
                .get_peers_with_admission(remote, id(2), None, pending())
                .await,
            Err(DhtRuntimeControlledQueryError::Query(
                DhtClientError::QuerySend(QuerySendError::Register(RegisterError::RegistryClosed))
            ))
        ));
        assert_eq!(closed_registry.pending_count(), 0);

        let (stopped_client, open_registry, remote) = stopped_runtime_client().await;
        assert!(matches!(
            stopped_client
                .ping_with_admission(remote, None, pending())
                .await,
            Err(DhtRuntimeControlledQueryError::Query(
                DhtClientError::QuerySend(QuerySendError::Transport(
                    TokioIpv4UdpWeakSendError::Stopped
                ))
            ))
        ));
        assert_eq!(open_registry.pending_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_controlled_admission_never_registers_and_cancellation_rolls_back() {
        let (client, registry, remote) = stopped_runtime_client().await;
        for _ in 0..4 {
            client.outbound_rate_limiter.wait(remote).await;
        }

        let (cancel_tx, cancel_rx) = oneshot::channel();
        let mut blocked = Box::pin(client.ping_with_admission(remote, None, async move {
            let _ = cancel_rx.await;
        }));
        let first_poll = poll_fn(|cx| Poll::Ready(blocked.as_mut().poll(cx))).await;
        assert!(first_poll.is_pending());
        assert_eq!(registry.pending_count(), 0);

        cancel_tx.send(()).unwrap();
        assert!(matches!(
            blocked.await,
            Err(DhtRuntimeControlledQueryError::Admission(
                DhtRateLimitWaitError::Cancelled
            ))
        ));
        assert_eq!(registry.pending_count(), 0);

        let mut replacement = Box::pin(client.ping_with_admission(remote, None, pending()));
        let first_poll = poll_fn(|cx| Poll::Ready(replacement.as_mut().poll(cx))).await;
        assert!(first_poll.is_pending());
        tokio::time::advance(Duration::from_millis(999)).await;
        let early_poll = poll_fn(|cx| Poll::Ready(replacement.as_mut().poll(cx))).await;
        assert!(early_poll.is_pending());
        assert_eq!(registry.pending_count(), 0);

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(matches!(
            replacement.await,
            Err(DhtRuntimeControlledQueryError::Query(
                DhtClientError::QuerySend(QuerySendError::Transport(
                    TokioIpv4UdpWeakSendError::Stopped
                ))
            ))
        ));
        assert_eq!(registry.pending_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn admitted_control_drops_its_cancellation_and_whole_query_drop_cleans_registration() {
        let registry = TransactionRegistry::default();
        let transport = TokioIpv4UdpTransport::bind_loopback().await.unwrap();
        let (_receiver, sender) = transport.into_parts();
        let client = DhtRuntimeClient::new(
            id(1),
            &registry,
            Duration::from_secs(60),
            sender.downgrade(),
        );
        let blackhole = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let remote = blackhole.local_addr().unwrap();
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let mut query = Box::pin(client.ping_with_admission(remote, None, async move {
            let _ = cancel_rx.await;
        }));

        let first_poll = poll_fn(|cx| Poll::Ready(query.as_mut().poll(cx))).await;
        assert!(first_poll.is_pending());
        assert_eq!(registry.pending_count(), 1);
        assert!(cancel_tx.send(()).is_err());

        drop(query);
        assert_eq!(registry.pending_count(), 0);

        // The successful admission remains committed even though the query was
        // later dropped: only the other three burst tokens are still ready.
        for _ in 0..3 {
            client.outbound_rate_limiter.wait(remote).await;
        }
        let mut fifth = Box::pin(client.outbound_rate_limiter.wait(remote));
        let fifth_poll = poll_fn(|cx| Poll::Ready(fifth.as_mut().poll(cx))).await;
        assert!(fifth_poll.is_pending());
    }

    #[tokio::test]
    async fn controlled_methods_forward_all_five_typed_queries() {
        let runtime = DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            query_timeout: Duration::from_secs(1),
            ..DhtRuntimeConfig::default()
        })
        .await
        .unwrap();
        let remote = SocketAddr::V4(runtime.local_addr());
        let local_id = runtime.local_id();
        let client = runtime.client();
        let second_client = DhtRuntimeClient::new(
            local_id,
            &runtime.registry,
            Duration::from_secs(1),
            client.sender.clone(),
        );

        assert_eq!(
            client
                .ping_with_admission(remote, None, pending())
                .await
                .unwrap(),
            PingResult { id: local_id }
        );
        assert_eq!(
            client
                .find_node_with_admission(remote, id(2), None, pending())
                .await
                .unwrap(),
            FindNodeResult {
                id: local_id,
                nodes: Vec::new(),
            }
        );
        assert_eq!(
            client
                .get_peers_with_admission(remote, id(3), None, pending())
                .await
                .unwrap(),
            GetPeersResult {
                id: local_id,
                values: Vec::new(),
                nodes: Vec::new(),
            }
        );
        assert_eq!(
            client
                .sample_infohashes_with_admission(remote, id(4), None, pending())
                .await
                .unwrap(),
            SampleInfoHashesResult {
                id: local_id,
                samples: Some(Vec::new()),
                nodes: Vec::new(),
                num: 0,
                interval: 10,
            }
        );

        assert!(matches!(
            second_client
                .get_peers_scrape_with_admission(remote, id(5), None, pending())
                .await,
            Err(DhtRuntimeControlledQueryError::Query(
                DhtClientError::MissingScrapeBloomFilters {
                    response_source,
                    missing_peers: true,
                    missing_seeders: true,
                    ..
                }
            )) if response_source == remote
        ));

        assert!(matches!(
            runtime.shutdown().await.unwrap(),
            DhtRuntimeExit::Shutdown
        ));
    }

    #[tokio::test]
    async fn raw_loopback_burst_allows_ten_then_sends_exact_go_201_and_releases_port() {
        const ALLOWED_TIDS: [[u8; 2]; 10] = [
            *b"A0", *b"A1", *b"A2", *b"A3", *b"A4", *b"A5", *b"A6", *b"A7", *b"A8", *b"A9",
        ];
        const DENIAL_WIRE: &[u8] = b"d1:eli201e17:too many requestse1:t2:L11:y1:re";

        let mut runtime = DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            query_timeout: Duration::from_secs(1),
            ..DhtRuntimeConfig::default()
        })
        .await
        .unwrap();
        let local_addr = runtime.local_addr();
        let local_id = runtime.local_id();
        let stats = runtime.inbound_stats();
        let discovery_stats = runtime.discovery_stats();
        let mut discovered = runtime
            .take_discovered_nodes()
            .expect("production discovery receiver");
        assert!(runtime.take_discovered_nodes().is_none());
        let peer = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let peer_addr = peer.local_addr().unwrap();

        // Freeze only the Tokio clock. The raw nonblocking receive helper below
        // uses a wall-clock deadline, so this gate cannot accidentally refill
        // the one-token-per-second inbound bucket or auto-advance to a timeout.
        tokio::time::pause();

        for transaction_id in ALLOWED_TIDS {
            let wire = fixed_ping_query(&transaction_id);
            assert_eq!(peer.send_to(&wire, local_addr).await.unwrap(), wire.len());
        }

        let mut observed_tids = Vec::with_capacity(ALLOWED_TIDS.len());
        let mut buffer = [0; MAX_INBOUND_DATAGRAM_BYTES];
        for _ in ALLOWED_TIDS {
            let (length, source) = recv_raw_while_time_paused(&peer, &mut buffer).await;
            assert_eq!(source, SocketAddr::V4(local_addr));
            let message = KrpcMessage::decode_inbound(&buffer[..length]).unwrap();
            assert_eq!(message.message_type.as_bytes(), b"r");
            assert_eq!(message.response.as_ref().unwrap().id, local_id);
            assert!(message.error.is_none());
            observed_tids.push(message.transaction_id.as_bytes().to_vec());
        }
        observed_tids.sort();
        assert_eq!(
            observed_tids,
            ALLOWED_TIDS.into_iter().map(Vec::from).collect::<Vec<_>>()
        );
        assert_eq!(
            stats.snapshot(),
            DhtInboundStatsSnapshot {
                admitted: 10,
                ..DhtInboundStatsSnapshot::default()
            }
        );
        for _ in ALLOWED_TIDS {
            assert_eq!(
                discovered.recv().await,
                Some(RoutingNode {
                    id: Id20::from_hex("0000000000000000000000000000000000000044").unwrap(),
                    addr: peer_addr,
                })
            );
        }
        assert_eq!(
            discovery_stats.snapshot(),
            DhtDiscoveryStats {
                offered: 10,
                queued: 10,
                full_dropped: 0,
                receiver_closed_dropped: 0,
            }
        );

        let denied_query = fixed_ping_query(b"L1");
        assert_eq!(
            peer.send_to(&denied_query, local_addr).await.unwrap(),
            denied_query.len()
        );
        let (length, source) = recv_raw_while_time_paused(&peer, &mut buffer).await;
        assert_eq!(source, SocketAddr::V4(local_addr));
        assert_eq!(&buffer[..length], DENIAL_WIRE);
        wait_for_rejection_sent_while_time_paused(&stats).await;
        assert_eq!(
            stats.snapshot(),
            DhtInboundStatsSnapshot {
                admitted: 10,
                denied_per_ip: 1,
                rejection_queued: 1,
                rejection_sent: 1,
                ..DhtInboundStatsSnapshot::default()
            }
        );
        assert_eq!(
            discovered.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );
        assert_eq!(
            discovery_stats.snapshot(),
            DhtDiscoveryStats {
                offered: 10,
                queued: 10,
                full_dropped: 0,
                receiver_closed_dropped: 0,
            }
        );

        tokio::time::resume();
        assert!(matches!(
            runtime.shutdown().await.unwrap(),
            DhtRuntimeExit::Shutdown
        ));
        assert_eq!(discovered.recv().await, None);
        assert_eq!(discovery_stats.snapshot().queued, 10);
        let rebound = rebind_after_task_drop(local_addr).await;
        assert_eq!(rebound.local_addr(), local_addr);
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
    async fn dropping_discovery_receiver_never_terminates_or_changes_runtime_replies() {
        let mut runtime = DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            query_timeout: Duration::from_secs(1),
            ..DhtRuntimeConfig::default()
        })
        .await
        .unwrap();
        let local_addr = runtime.local_addr();
        let client = runtime.client();
        let discovery_stats = runtime.discovery_stats();
        drop(
            runtime
                .take_discovered_nodes()
                .expect("production discovery receiver"),
        );

        assert_eq!(
            client.ping(SocketAddr::V4(local_addr)).await.unwrap(),
            PingResult {
                id: runtime.local_id()
            }
        );
        assert_eq!(
            discovery_stats.snapshot(),
            DhtDiscoveryStats {
                offered: 1,
                queued: 0,
                full_dropped: 0,
                receiver_closed_dropped: 1,
            }
        );
        assert!(matches!(
            runtime.shutdown().await.unwrap(),
            DhtRuntimeExit::Shutdown
        ));
        let rebound = rebind_after_task_drop(local_addr).await;
        assert_eq!(rebound.local_addr(), local_addr);
    }

    #[tokio::test]
    async fn runtime_drop_closes_taken_discovery_receiver_while_stats_remain_readable() {
        let mut runtime = DhtRuntime::start(DhtRuntimeConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            query_timeout: Duration::from_secs(1),
            ..DhtRuntimeConfig::default()
        })
        .await
        .unwrap();
        let local_addr = runtime.local_addr();
        let stats = runtime.discovery_stats();
        let mut discovered = runtime
            .take_discovered_nodes()
            .expect("production discovery receiver");

        drop(runtime);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), discovered.recv())
                .await
                .expect("runtime abort closes discovery promptly"),
            None
        );
        assert_eq!(stats.snapshot(), DhtDiscoveryStats::default());
        let rebound = rebind_after_task_drop(local_addr).await;
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
        let (discovery_stats, discovered_nodes) = empty_discovery_state();
        let runtime = DhtRuntime {
            local_addr,
            local_id,
            table,
            client: client.clone(),
            registry: registry.clone(),
            inbound_stats: DhtInboundStats::new(),
            discovery_stats,
            discovered_nodes,
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
        let _: fn(DhtRuntimeControlledQueryError) -> &'static str = classify_controlled_query_error;
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

    fn classify_controlled_query_error(error: DhtRuntimeControlledQueryError) -> &'static str {
        match error {
            DhtRuntimeControlledQueryError::Admission(error) => match error {
                DhtRateLimitWaitError::Cancelled => "admission_cancelled",
                DhtRateLimitWaitError::WouldExceedDeadline => "admission_deadline",
            },
            DhtRuntimeControlledQueryError::Query(error) => classify_client_error(error),
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

    fn fixed_ping_query(transaction_id: &[u8]) -> Vec<u8> {
        KrpcMessage {
            transaction_id: ByteString::new(transaction_id),
            message_type: ByteString::new(b"q"),
            query: ByteString::new(b"ping"),
            args: Some(MessageArgs {
                id: Id20::from_hex("0000000000000000000000000000000000000044").unwrap(),
                info_hash: None,
                target: None,
                token: ByteString::default(),
                port: None,
                implied_port: false,
                want: None,
                no_seed: 0,
                scrape: 0,
            }),
            response: None,
            error: None,
            observed_addr: None,
            read_only: false,
            client_id: ByteString::default(),
        }
        .encode()
        .unwrap()
    }

    async fn recv_raw_while_time_paused(
        socket: &UdpSocket,
        buffer: &mut [u8],
    ) -> (usize, SocketAddr) {
        let deadline = WallInstant::now() + Duration::from_secs(5);
        loop {
            match socket.try_recv_from(buffer) {
                Ok(received) => return received,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        WallInstant::now() < deadline,
                        "raw loopback reply did not arrive before wall-clock deadline"
                    );
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("raw loopback receive failed: {error}"),
            }
        }
    }

    async fn wait_for_rejection_sent_while_time_paused(stats: &DhtInboundStats) {
        let deadline = WallInstant::now() + Duration::from_secs(5);
        loop {
            if stats.snapshot().rejection_sent == 1 {
                return;
            }
            assert!(
                WallInstant::now() < deadline,
                "rejection send counter did not settle before wall-clock deadline"
            );
            tokio::task::yield_now().await;
        }
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

    fn empty_discovery_state() -> (DhtDiscoveryStatsHandle, Option<DhtDiscoveryReceiver>) {
        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(1).expect("nonzero"));
        let stats = sender.stats_handle();
        drop(sender);
        (stats, Some(receiver))
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
        let (discovery_stats, discovered_nodes) = empty_discovery_state();

        (
            DhtRuntime {
                local_addr,
                local_id,
                table,
                client: client.clone(),
                registry,
                inbound_stats: DhtInboundStats::new(),
                discovery_stats,
                discovered_nodes,
                shutdown_tx,
                task: Some(task),
            },
            client,
            local_addr,
        )
    }

    async fn stopped_runtime_client() -> (
        DhtRuntimeClient,
        TransactionRegistry<CryptoTransactionIdIssuer>,
        SocketAddr,
    ) {
        let registry = TransactionRegistry::default();
        let transport = TokioIpv4UdpTransport::bind_loopback().await.unwrap();
        let remote = SocketAddr::V4(transport.local_addr());
        let (receiver, sender) = transport.into_parts();
        let client =
            DhtRuntimeClient::new(id(9), &registry, Duration::from_secs(4), sender.downgrade());
        drop(receiver);
        drop(sender);
        (client, registry, remote)
    }

    fn id(byte: u8) -> Id20 {
        Id20::from_slice(&[byte; 20]).unwrap()
    }
}
