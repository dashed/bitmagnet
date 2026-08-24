use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::{Instant, Interval, MissedTickBehavior};

use crate::{DhtDiscoveryReceiver, KTable, RoutingNode};

const DEFAULT_MAX_BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(10).unwrap();
const DEFAULT_PING_CAPACITY: NonZeroUsize = NonZeroUsize::new(10).unwrap();
const DEFAULT_FIND_NODE_CAPACITY: NonZeroUsize = NonZeroUsize::new(100).unwrap();
const DEFAULT_SAMPLE_INFOHASHES_CAPACITY: NonZeroUsize = NonZeroUsize::new(100).unwrap();
const DEFAULT_BATCH_INTERVAL: Duration = Duration::from_millis(10);

/// Batching and bounded-route capacities for discovered DHT nodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtDiscoveredNodeSchedulerConfig {
    pub max_batch_size: NonZeroUsize,
    pub batch_interval: Duration,
    pub ping_capacity: NonZeroUsize,
    pub find_node_capacity: NonZeroUsize,
    pub sample_infohashes_capacity: NonZeroUsize,
}

impl Default for DhtDiscoveredNodeSchedulerConfig {
    fn default() -> Self {
        Self {
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            batch_interval: DEFAULT_BATCH_INTERVAL,
            ping_capacity: DEFAULT_PING_CAPACITY,
            find_node_capacity: DEFAULT_FIND_NODE_CAPACITY,
            sample_infohashes_capacity: DEFAULT_SAMPLE_INFOHASHES_CAPACITY,
        }
    }
}

/// Invalid discovered-node scheduler configuration.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum DhtDiscoveredNodeSchedulerConfigError {
    #[error("the discovered-node batch interval must be nonzero")]
    ZeroBatchInterval,
    #[error("the discovered-node batch interval exceeds the monotonic clock range")]
    BatchIntervalOutOfRange,
}

/// The terminal state of the owned discovered-node scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtDiscoveredNodeSchedulerExit {
    /// Caller shutdown won; the count covers every uncommitted local or queued
    /// ingress node synchronously discarded after closing the ingress.
    Shutdown { pending_dropped: usize },
    /// Every discovery producer is gone and the final partial batch was routed.
    InputClosed,
    /// Every downstream route closed; the count covers every uncommitted local
    /// or queued ingress node synchronously discarded after closing the ingress.
    RoutesClosed { pending_dropped: usize },
}

/// Three unique bounded consumers for crawler work.
pub struct DhtDiscoveredNodeRoutes {
    pub ping: DhtDiscoveredNodeRouteReceiver,
    pub find_node: DhtDiscoveredNodeRouteReceiver,
    pub sample_infohashes: DhtDiscoveredNodeRouteReceiver,
}

/// Unique consumer for one bounded discovered-node work route.
pub struct DhtDiscoveredNodeRouteReceiver {
    receiver: mpsc::Receiver<RoutingNode>,
}

impl DhtDiscoveredNodeRouteReceiver {
    /// Receive the next routed node, or `None` after scheduler exit and drain.
    pub async fn recv(&mut self) -> Option<RoutingNode> {
        self.receiver.recv().await
    }

    /// Receive one currently queued node without waiting.
    pub fn try_recv(&mut self) -> Result<RoutingNode, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    /// Reject later routing while retaining already queued nodes to drain.
    pub fn close(&mut self) {
        self.receiver.close();
    }
}

#[derive(Default)]
struct DhtDiscoveredNodeSchedulerStatsInner {
    received: AtomicU64,
    batches: AtomicU64,
    duplicate_dropped: AtomicU64,
    known_filtered: AtomicU64,
    filter_calls: AtomicU64,
    route_attempts: AtomicU64,
    routed_ping: AtomicU64,
    routed_find_node: AtomicU64,
    routed_sample_infohashes: AtomicU64,
    shutdown_dropped: AtomicU64,
    routes_closed_dropped: AtomicU64,
}

/// Cloneable, sender-free view of discovered-node scheduler counters.
#[derive(Clone, Default)]
pub struct DhtDiscoveredNodeSchedulerStatsHandle {
    inner: Arc<DhtDiscoveredNodeSchedulerStatsInner>,
}

/// One non-transactional snapshot of monotonic scheduler counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtDiscoveredNodeSchedulerStats {
    pub received: u64,
    pub batches: u64,
    pub duplicate_dropped: u64,
    pub known_filtered: u64,
    /// Completed calls to the synchronous KTable filter.
    pub filter_calls: u64,
    /// Nodes that reached downstream route-capacity selection.
    pub route_attempts: u64,
    pub routed_ping: u64,
    pub routed_find_node: u64,
    pub routed_sample_infohashes: u64,
    pub shutdown_dropped: u64,
    pub routes_closed_dropped: u64,
}

impl DhtDiscoveredNodeSchedulerStatsHandle {
    /// Read each monotonic counter independently with relaxed ordering.
    #[must_use]
    pub fn snapshot(&self) -> DhtDiscoveredNodeSchedulerStats {
        DhtDiscoveredNodeSchedulerStats {
            received: self.inner.received.load(Ordering::Relaxed),
            batches: self.inner.batches.load(Ordering::Relaxed),
            duplicate_dropped: self.inner.duplicate_dropped.load(Ordering::Relaxed),
            known_filtered: self.inner.known_filtered.load(Ordering::Relaxed),
            filter_calls: self.inner.filter_calls.load(Ordering::Relaxed),
            route_attempts: self.inner.route_attempts.load(Ordering::Relaxed),
            routed_ping: self.inner.routed_ping.load(Ordering::Relaxed),
            routed_find_node: self.inner.routed_find_node.load(Ordering::Relaxed),
            routed_sample_infohashes: self.inner.routed_sample_infohashes.load(Ordering::Relaxed),
            shutdown_dropped: self.inner.shutdown_dropped.load(Ordering::Relaxed),
            routes_closed_dropped: self.inner.routes_closed_dropped.load(Ordering::Relaxed),
        }
    }
}

/// Owned batching, filtering, and bounded-routing stage for discovered nodes.
pub struct DhtDiscoveredNodeScheduler {
    input: DhtDiscoveryReceiver,
    table: KTable,
    config: DhtDiscoveredNodeSchedulerConfig,
    first_tick_at: Instant,
    routes: RouteSenders,
    stats: DhtDiscoveredNodeSchedulerStatsHandle,
}

struct RouteSenders {
    ping: mpsc::Sender<RoutingNode>,
    find_node: mpsc::Sender<RoutingNode>,
    sample_infohashes: mpsc::Sender<RoutingNode>,
}

impl DhtDiscoveredNodeScheduler {
    /// Construct the production-compatible fixed-capacity scheduler.
    #[must_use]
    pub fn new(
        input: DhtDiscoveryReceiver,
        table: KTable,
    ) -> (
        Self,
        DhtDiscoveredNodeRoutes,
        DhtDiscoveredNodeSchedulerStatsHandle,
    ) {
        Self::with_config(input, table, DhtDiscoveredNodeSchedulerConfig::default())
            .expect("the fixed scheduler defaults are valid")
    }

    /// Construct a scheduler with explicit batching and route capacities.
    pub fn with_config(
        input: DhtDiscoveryReceiver,
        table: KTable,
        config: DhtDiscoveredNodeSchedulerConfig,
    ) -> Result<
        (
            Self,
            DhtDiscoveredNodeRoutes,
            DhtDiscoveredNodeSchedulerStatsHandle,
        ),
        DhtDiscoveredNodeSchedulerConfigError,
    > {
        if config.batch_interval.is_zero() {
            return Err(DhtDiscoveredNodeSchedulerConfigError::ZeroBatchInterval);
        }
        let first_tick_at = Instant::now()
            .checked_add(config.batch_interval)
            .ok_or(DhtDiscoveredNodeSchedulerConfigError::BatchIntervalOutOfRange)?;

        let (ping, ping_receiver) = mpsc::channel(config.ping_capacity.get());
        let (find_node, find_node_receiver) = mpsc::channel(config.find_node_capacity.get());
        let (sample_infohashes, sample_infohashes_receiver) =
            mpsc::channel(config.sample_infohashes_capacity.get());
        let stats = DhtDiscoveredNodeSchedulerStatsHandle::default();
        Ok((
            Self {
                input,
                table,
                config,
                first_tick_at,
                routes: RouteSenders {
                    ping,
                    find_node,
                    sample_infohashes,
                },
                stats: stats.clone(),
            },
            DhtDiscoveredNodeRoutes {
                ping: DhtDiscoveredNodeRouteReceiver {
                    receiver: ping_receiver,
                },
                find_node: DhtDiscoveredNodeRouteReceiver {
                    receiver: find_node_receiver,
                },
                sample_infohashes: DhtDiscoveredNodeRouteReceiver {
                    receiver: sample_infohashes_receiver,
                },
            },
            stats,
        ))
    }

    /// Consume discovered nodes until shutdown, producer EOF, or route EOF.
    ///
    /// No task is spawned. Batches retain discovery order, deduplicate by IP
    /// and IPv6 scope, and call the real KTable filter once. Each unknown node
    /// then waits for capacity on exactly one open route. Shutdown is biased
    /// around both input/timer waits and route waits; selection among ready
    /// routes remains deliberately unbiased.
    pub async fn run<F>(mut self, shutdown: F) -> DhtDiscoveredNodeSchedulerExit
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let mut pending = Vec::with_capacity(self.config.max_batch_size.get());
        let mut interval = tokio::time::interval_at(self.first_tick_at, self.config.batch_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut open_routes = [true; 3];

        loop {
            let event = tokio::select! {
                biased;
                () = &mut shutdown => SchedulerEvent::Shutdown,
                event = next_scheduler_event(&mut self.input, &mut interval, &self.routes) => event,
            };

            match event {
                SchedulerEvent::Shutdown => {
                    return self.finish_shutdown(pending.len());
                }
                SchedulerEvent::Input(Some(node)) => {
                    increment_saturating(&self.stats.inner.received);
                    pending.push(node);
                    if pending.len() < self.config.max_batch_size.get() {
                        continue;
                    }
                    interval.reset();
                }
                SchedulerEvent::Tick if pending.is_empty() => continue,
                SchedulerEvent::Tick => interval.reset(),
                SchedulerEvent::RoutesClosed => {
                    return self.finish_routes_closed(pending.len());
                }
                SchedulerEvent::Input(None) if pending.is_empty() => {
                    return DhtDiscoveredNodeSchedulerExit::InputClosed;
                }
                SchedulerEvent::Input(None) => {
                    return match self
                        .route_batch(pending, &mut open_routes, shutdown.as_mut())
                        .await
                    {
                        RouteBatchResult::Complete => DhtDiscoveredNodeSchedulerExit::InputClosed,
                        RouteBatchResult::Shutdown { pending_dropped } => {
                            self.finish_shutdown(pending_dropped)
                        }
                        RouteBatchResult::RoutesClosed { pending_dropped } => {
                            self.finish_routes_closed(pending_dropped)
                        }
                    };
                }
            }

            let batch = std::mem::replace(
                &mut pending,
                Vec::with_capacity(self.config.max_batch_size.get()),
            );
            match self
                .route_batch(batch, &mut open_routes, shutdown.as_mut())
                .await
            {
                RouteBatchResult::Complete => {}
                RouteBatchResult::Shutdown { pending_dropped } => {
                    return self.finish_shutdown(pending_dropped);
                }
                RouteBatchResult::RoutesClosed { pending_dropped } => {
                    return self.finish_routes_closed(pending_dropped);
                }
            }
        }
    }

    fn finish_shutdown(&mut self, local_dropped: usize) -> DhtDiscoveredNodeSchedulerExit {
        let pending_dropped = local_dropped.saturating_add(self.close_and_drain_ingress());
        increment_saturating_by(&self.stats.inner.shutdown_dropped, pending_dropped);
        DhtDiscoveredNodeSchedulerExit::Shutdown { pending_dropped }
    }

    fn finish_routes_closed(&mut self, local_dropped: usize) -> DhtDiscoveredNodeSchedulerExit {
        let pending_dropped = local_dropped.saturating_add(self.close_and_drain_ingress());
        increment_saturating_by(&self.stats.inner.routes_closed_dropped, pending_dropped);
        DhtDiscoveredNodeSchedulerExit::RoutesClosed { pending_dropped }
    }

    fn close_and_drain_ingress(&mut self) -> usize {
        self.input.close();
        let mut dropped = 0_usize;
        while self.input.try_recv().is_ok() {
            dropped = dropped.saturating_add(1);
        }
        dropped
    }

    async fn route_batch<F>(
        &self,
        batch: Vec<RoutingNode>,
        open_routes: &mut [bool; 3],
        mut shutdown: std::pin::Pin<&mut F>,
    ) -> RouteBatchResult
    where
        F: Future<Output = ()>,
    {
        increment_saturating(&self.stats.inner.batches);
        let batch_len = batch.len();
        let mut seen = HashSet::with_capacity(batch_len);
        let mut unique = HashMap::with_capacity(batch_len);
        let mut ordered_keys = Vec::with_capacity(batch_len);

        for node in batch {
            let key = NodeAddressKey::from(node.addr);
            if seen.insert(key) {
                ordered_keys.push(key);
                unique.insert(key, node);
            } else {
                increment_saturating(&self.stats.inner.duplicate_dropped);
            }
        }

        let addrs = ordered_keys
            .iter()
            .map(|key| key.filter_addr())
            .collect::<Vec<_>>();
        let unknown_addrs = self.table.filter_known_addrs(&addrs);
        increment_saturating(&self.stats.inner.filter_calls);
        increment_saturating_by(
            &self.stats.inner.known_filtered,
            addrs.len().saturating_sub(unknown_addrs.len()),
        );
        let mut unknown = unknown_addrs
            .into_iter()
            .map(|addr| {
                unique
                    .remove(&NodeAddressKey::from(addr))
                    .expect("KTable preserves each unknown input address")
            })
            .collect::<Vec<_>>()
            .into_iter();
        let total_unknown = unknown.len();

        for (index, node) in unknown.by_ref().enumerate() {
            let remaining = total_unknown - index;
            increment_saturating(&self.stats.inner.route_attempts);
            let routed = tokio::select! {
                biased;
                () = shutdown.as_mut() => RouteOneResult::Shutdown,
                routed = route_one_unbiased(node, &self.routes, open_routes) => routed,
            };
            match routed {
                RouteOneResult::Ping => increment_saturating(&self.stats.inner.routed_ping),
                RouteOneResult::FindNode => {
                    increment_saturating(&self.stats.inner.routed_find_node);
                }
                RouteOneResult::SampleInfoHashes => {
                    increment_saturating(&self.stats.inner.routed_sample_infohashes);
                }
                RouteOneResult::Shutdown => {
                    return RouteBatchResult::Shutdown {
                        pending_dropped: remaining,
                    };
                }
                RouteOneResult::RoutesClosed => {
                    return RouteBatchResult::RoutesClosed {
                        pending_dropped: remaining,
                    };
                }
            }
        }

        RouteBatchResult::Complete
    }
}

enum SchedulerEvent {
    Shutdown,
    Input(Option<RoutingNode>),
    Tick,
    RoutesClosed,
}

async fn next_scheduler_event(
    input: &mut DhtDiscoveryReceiver,
    interval: &mut Interval,
    routes: &RouteSenders,
) -> SchedulerEvent {
    tokio::select! {
        node = input.recv() => SchedulerEvent::Input(node),
        _ = interval.tick() => SchedulerEvent::Tick,
        () = all_routes_closed(routes) => SchedulerEvent::RoutesClosed,
    }
}

async fn all_routes_closed(routes: &RouteSenders) {
    tokio::join!(
        routes.ping.closed(),
        routes.find_node.closed(),
        routes.sample_infohashes.closed(),
    );
}

enum RouteBatchResult {
    Complete,
    Shutdown { pending_dropped: usize },
    RoutesClosed { pending_dropped: usize },
}

enum RouteOneResult {
    Ping,
    FindNode,
    SampleInfoHashes,
    Shutdown,
    RoutesClosed,
}

async fn route_one_unbiased(
    node: RoutingNode,
    routes: &RouteSenders,
    open: &mut [bool; 3],
) -> RouteOneResult {
    loop {
        if !open.iter().any(|open| *open) {
            return RouteOneResult::RoutesClosed;
        }

        let selected = tokio::select! {
            permit = routes.ping.reserve(), if open[0] => (0, permit),
            permit = routes.find_node.reserve(), if open[1] => (1, permit),
            permit = routes.sample_infohashes.reserve(), if open[2] => (2, permit),
        };
        let (route, permit) = selected;
        match permit {
            Ok(permit) => {
                permit.send(node);
                return match route {
                    0 => RouteOneResult::Ping,
                    1 => RouteOneResult::FindNode,
                    2 => RouteOneResult::SampleInfoHashes,
                    _ => unreachable!("the route selector has exactly three branches"),
                };
            }
            Err(_) => open[route] = false,
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum NodeAddressKey {
    V4(std::net::Ipv4Addr),
    V6(std::net::Ipv6Addr, u32),
}

impl NodeAddressKey {
    fn filter_addr(self) -> SocketAddr {
        match self {
            Self::V4(ip) => SocketAddr::V4(std::net::SocketAddrV4::new(ip, 0)),
            Self::V6(ip, scope_id) => {
                SocketAddr::V6(std::net::SocketAddrV6::new(ip, 0, 0, scope_id))
            }
        }
    }
}

impl From<SocketAddr> for NodeAddressKey {
    fn from(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(addr) => Self::V4(*addr.ip()),
            SocketAddr::V6(addr) => Self::V6(*addr.ip(), addr.scope_id()),
        }
    }
}

fn increment_saturating(counter: &AtomicU64) {
    increment_saturating_by(counter, 1);
}

fn increment_saturating_by(counter: &AtomicU64, amount: usize) {
    let amount = u64::try_from(amount).unwrap_or(u64::MAX);
    let _previous = counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(amount))
        })
        .expect("a saturating counter update always supplies a replacement");
}

#[cfg(test)]
#[path = "dht_discovered_node_scheduler_parity.rs"]
mod parity_tests;

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

    use super::*;
    use crate::{dht_discovery_channel, DhtDiscoveryOffer, Id20};

    fn node(value: u8, addr: SocketAddr) -> RoutingNode {
        let mut bytes = [0_u8; 20];
        bytes[19] = value;
        RoutingNode {
            id: Id20::from_slice(&bytes).unwrap(),
            addr,
        }
    }

    fn v4(value: u8, port: u16) -> RoutingNode {
        node(
            value,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, value), port)),
        )
    }

    fn config(max_batch_size: usize, route_capacity: usize) -> DhtDiscoveredNodeSchedulerConfig {
        DhtDiscoveredNodeSchedulerConfig {
            max_batch_size: NonZeroUsize::new(max_batch_size).unwrap(),
            batch_interval: Duration::from_secs(60 * 60),
            ping_capacity: NonZeroUsize::new(route_capacity).unwrap(),
            find_node_capacity: NonZeroUsize::new(route_capacity).unwrap(),
            sample_infohashes_capacity: NonZeroUsize::new(route_capacity).unwrap(),
        }
    }

    #[test]
    fn defaults_and_validation_are_fixed() {
        assert_eq!(
            DhtDiscoveredNodeSchedulerConfig::default(),
            DhtDiscoveredNodeSchedulerConfig {
                max_batch_size: NonZeroUsize::new(10).unwrap(),
                batch_interval: Duration::from_millis(10),
                ping_capacity: NonZeroUsize::new(10).unwrap(),
                find_node_capacity: NonZeroUsize::new(100).unwrap(),
                sample_infohashes_capacity: NonZeroUsize::new(100).unwrap(),
            }
        );

        let (_sender, receiver) = dht_discovery_channel(NonZeroUsize::new(1).unwrap());
        let invalid = DhtDiscoveredNodeSchedulerConfig {
            batch_interval: Duration::ZERO,
            ..DhtDiscoveredNodeSchedulerConfig::default()
        };
        assert!(matches!(
            DhtDiscoveredNodeScheduler::with_config(receiver, KTable::new(Id20::ZERO), invalid),
            Err(DhtDiscoveredNodeSchedulerConfigError::ZeroBatchInterval)
        ));

        let (_sender, receiver) = dht_discovery_channel(NonZeroUsize::new(1).unwrap());
        let invalid = DhtDiscoveredNodeSchedulerConfig {
            batch_interval: Duration::MAX,
            ..DhtDiscoveredNodeSchedulerConfig::default()
        };
        assert!(matches!(
            DhtDiscoveredNodeScheduler::with_config(receiver, KTable::new(Id20::ZERO), invalid),
            Err(DhtDiscoveredNodeSchedulerConfigError::BatchIntervalOutOfRange)
        ));
    }

    #[tokio::test]
    async fn max_batch_deduplicates_first_ip_and_filters_known_without_mutation() {
        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(8).unwrap());
        let table = KTable::new(Id20::ZERO);
        let known = v4(9, 1);
        assert_eq!(table.put_node(known), crate::RoutingPutResult::Accepted);
        assert_eq!(
            table.put_node(known),
            crate::RoutingPutResult::AlreadyExists
        );
        let count_before = table.node_count();
        let (scheduler, mut routes, stats) =
            DhtDiscoveredNodeScheduler::with_config(receiver, table.clone(), config(4, 8)).unwrap();

        let first = v4(1, 1001);
        let duplicate = node(
            2,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 1), 2002)),
        );
        assert_eq!(sender.offer(first), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(duplicate), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(known), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(v4(3, 1003)), DhtDiscoveryOffer::Queued);
        drop(sender);

        assert_eq!(
            scheduler.run(std::future::pending()).await,
            DhtDiscoveredNodeSchedulerExit::InputClosed
        );
        let mut delivered = Vec::new();
        while let Ok(node) = routes.ping.try_recv() {
            delivered.push(node);
        }
        while let Ok(node) = routes.find_node.try_recv() {
            delivered.push(node);
        }
        while let Ok(node) = routes.sample_infohashes.try_recv() {
            delivered.push(node);
        }
        delivered.sort_by_key(|node| node.id);
        assert_eq!(delivered.len(), 2);
        assert!(delivered.contains(&first));
        assert!(delivered.contains(&v4(3, 1003)));
        assert!(!delivered.contains(&duplicate));
        assert_eq!(table.node_count(), count_before);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.received, 4);
        assert_eq!(snapshot.batches, 1);
        assert_eq!(snapshot.duplicate_dropped, 1);
        assert_eq!(snapshot.known_filtered, 1);
        assert_eq!(snapshot.filter_calls, 1);
        assert_eq!(snapshot.route_attempts, 2);
        assert_eq!(snapshot.shutdown_dropped, 0);
        assert_eq!(snapshot.routes_closed_dropped, 0);
        assert_eq!(
            snapshot.routed_ping + snapshot.routed_find_node + snapshot.routed_sample_infohashes,
            2
        );
    }

    #[tokio::test(start_paused = true)]
    async fn partial_batch_flushes_on_tick_and_empty_ticks_emit_nothing() {
        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(2).unwrap());
        let (scheduler, mut routes, stats) = DhtDiscoveredNodeScheduler::with_config(
            receiver,
            KTable::new(Id20::ZERO),
            DhtDiscoveredNodeSchedulerConfig {
                max_batch_size: NonZeroUsize::new(10).unwrap(),
                batch_interval: Duration::from_millis(10),
                ping_capacity: NonZeroUsize::new(1).unwrap(),
                find_node_capacity: NonZeroUsize::new(1).unwrap(),
                sample_infohashes_capacity: NonZeroUsize::new(1).unwrap(),
            },
        )
        .unwrap();
        routes.find_node.close();
        routes.sample_infohashes.close();
        let task = tokio::spawn(scheduler.run(std::future::pending()));

        tokio::time::advance(Duration::from_millis(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(stats.snapshot().batches, 0);
        assert_eq!(sender.offer(v4(1, 1)), DhtDiscoveryOffer::Queued);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(9)).await;
        tokio::task::yield_now().await;
        assert!(matches!(
            routes.ping.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(routes.ping.try_recv().unwrap(), v4(1, 1));
        drop(sender);
        assert_eq!(
            task.await.unwrap(),
            DhtDiscoveredNodeSchedulerExit::InputClosed
        );
    }

    #[tokio::test(start_paused = true)]
    async fn first_tick_is_anchored_at_scheduler_construction() {
        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(1).unwrap());
        let (scheduler, mut routes, _stats) = DhtDiscoveredNodeScheduler::with_config(
            receiver,
            KTable::new(Id20::ZERO),
            DhtDiscoveredNodeSchedulerConfig {
                max_batch_size: NonZeroUsize::new(10).unwrap(),
                batch_interval: Duration::from_millis(10),
                ping_capacity: NonZeroUsize::new(1).unwrap(),
                find_node_capacity: NonZeroUsize::new(1).unwrap(),
                sample_infohashes_capacity: NonZeroUsize::new(1).unwrap(),
            },
        )
        .unwrap();
        routes.find_node.close();
        routes.sample_infohashes.close();

        tokio::time::advance(Duration::from_millis(9)).await;
        assert_eq!(sender.offer(v4(1, 1)), DhtDiscoveryOffer::Queued);
        let task = tokio::spawn(scheduler.run(std::future::pending()));
        tokio::task::yield_now().await;
        assert!(matches!(
            routes.ping.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(routes.ping.try_recv().unwrap(), v4(1, 1));

        drop(sender);
        assert_eq!(
            task.await.unwrap(),
            DhtDiscoveredNodeSchedulerExit::InputClosed
        );
    }

    #[tokio::test(start_paused = true)]
    async fn size_flush_resets_timer_before_the_next_partial_batch() {
        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(3).unwrap());
        let (scheduler, mut routes, stats) = DhtDiscoveredNodeScheduler::with_config(
            receiver,
            KTable::new(Id20::ZERO),
            DhtDiscoveredNodeSchedulerConfig {
                max_batch_size: NonZeroUsize::new(2).unwrap(),
                batch_interval: Duration::from_millis(10),
                ping_capacity: NonZeroUsize::new(3).unwrap(),
                find_node_capacity: NonZeroUsize::new(1).unwrap(),
                sample_infohashes_capacity: NonZeroUsize::new(1).unwrap(),
            },
        )
        .unwrap();
        routes.find_node.close();
        routes.sample_infohashes.close();
        let task = tokio::spawn(scheduler.run(std::future::pending()));

        tokio::time::advance(Duration::from_millis(9)).await;
        assert_eq!(sender.offer(v4(1, 1)), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(v4(2, 2)), DhtDiscoveryOffer::Queued);
        tokio::task::yield_now().await;
        assert_eq!(stats.snapshot().batches, 1);
        assert_eq!(sender.offer(v4(3, 3)), DhtDiscoveryOffer::Queued);
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(stats.snapshot().batches, 1);
        tokio::time::advance(Duration::from_millis(9)).await;
        tokio::task::yield_now().await;
        assert_eq!(stats.snapshot().batches, 2);

        drop(sender);
        assert_eq!(
            task.await.unwrap(),
            DhtDiscoveredNodeSchedulerExit::InputClosed
        );
        assert_eq!(routes.ping.try_recv().unwrap(), v4(1, 1));
        assert_eq!(routes.ping.try_recv().unwrap(), v4(2, 2));
        assert_eq!(routes.ping.try_recv().unwrap(), v4(3, 3));
    }

    #[tokio::test]
    async fn dedupe_resets_across_batches_and_eof_flushes_partial_batch() {
        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(3).unwrap());
        let (scheduler, mut routes, stats) = DhtDiscoveredNodeScheduler::with_config(
            receiver,
            KTable::new(Id20::ZERO),
            config(2, 3),
        )
        .unwrap();
        routes.find_node.close();
        routes.sample_infohashes.close();

        let first = v4(1, 1);
        let first_duplicate = node(2, first.addr);
        let later_winner = node(3, first.addr);
        assert_eq!(sender.offer(first), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(first_duplicate), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(later_winner), DhtDiscoveryOffer::Queued);
        drop(sender);

        assert_eq!(
            scheduler.run(std::future::pending()).await,
            DhtDiscoveredNodeSchedulerExit::InputClosed
        );
        assert_eq!(routes.ping.try_recv().unwrap(), first);
        assert_eq!(routes.ping.try_recv().unwrap(), later_winner);
        assert!(matches!(
            routes.ping.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        assert_eq!(stats.snapshot().batches, 2);
        assert_eq!(stats.snapshot().duplicate_dropped, 1);
    }

    #[tokio::test]
    async fn full_routes_backpressure_then_resume_exactly_one_lane() {
        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(4).unwrap());
        let (scheduler, mut routes, stats) = DhtDiscoveredNodeScheduler::with_config(
            receiver,
            KTable::new(Id20::ZERO),
            config(1, 1),
        )
        .unwrap();

        assert_eq!(sender.offer(v4(1, 1)), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(v4(2, 2)), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(v4(3, 3)), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(v4(4, 4)), DhtDiscoveryOffer::Queued);
        drop(sender);
        let task = tokio::spawn(scheduler.run(std::future::pending()));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                tokio::task::yield_now().await;
                let snapshot = stats.snapshot();
                if snapshot.routed_ping
                    + snapshot.routed_find_node
                    + snapshot.routed_sample_infohashes
                    == 3
                {
                    break;
                }
            }
        })
        .await
        .expect("three open unit-capacity routes must fill");
        assert!(!task.is_finished());
        let mut delivered = Vec::new();
        delivered.push(routes.ping.try_recv().unwrap());
        delivered.push(routes.find_node.try_recv().unwrap());
        delivered.push(routes.sample_infohashes.try_recv().unwrap());
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                tokio::task::yield_now().await;
                let snapshot = stats.snapshot();
                if snapshot.routed_ping
                    + snapshot.routed_find_node
                    + snapshot.routed_sample_infohashes
                    == 4
                {
                    break;
                }
            }
        })
        .await
        .expect("draining route capacity must resume the blocked suffix");
        assert_eq!(
            task.await.unwrap(),
            DhtDiscoveredNodeSchedulerExit::InputClosed
        );
        while let Ok(node) = routes.ping.try_recv() {
            delivered.push(node);
        }
        while let Ok(node) = routes.find_node.try_recv() {
            delivered.push(node);
        }
        while let Ok(node) = routes.sample_infohashes.try_recv() {
            delivered.push(node);
        }
        delivered.sort_by_key(|node| node.id);
        assert_eq!(delivered, vec![v4(1, 1), v4(2, 2), v4(3, 3), v4(4, 4)]);
    }

    #[tokio::test]
    async fn shutdown_abandons_only_uncommitted_suffix_and_closes_routes() {
        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(3).unwrap());
        let (scheduler, mut routes, stats) = DhtDiscoveredNodeScheduler::with_config(
            receiver,
            KTable::new(Id20::ZERO),
            config(3, 1),
        )
        .unwrap();
        routes.find_node.close();
        routes.sample_infohashes.close();
        for value in 1..=3 {
            assert_eq!(
                sender.offer(v4(value, u16::from(value))),
                DhtDiscoveryOffer::Queued
            );
        }
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(scheduler.run(async {
            let _ = shutdown_receiver.await;
        }));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                tokio::task::yield_now().await;
                if stats.snapshot().routed_ping == 1 {
                    break;
                }
            }
        })
        .await
        .expect("the first ping route must commit");
        shutdown_sender.send(()).unwrap();
        assert_eq!(
            task.await.unwrap(),
            DhtDiscoveredNodeSchedulerExit::Shutdown { pending_dropped: 2 }
        );
        assert_eq!(routes.ping.try_recv().unwrap(), v4(1, 1));
        assert_eq!(routes.ping.recv().await, None);
        assert_eq!(stats.snapshot().shutdown_dropped, 2);
        assert_eq!(stats.snapshot().filter_calls, 1);
        assert_eq!(stats.snapshot().route_attempts, 2);
        drop(sender);
    }

    #[tokio::test]
    async fn all_closed_routes_report_exact_uncommitted_count() {
        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(2).unwrap());
        let (scheduler, routes, stats) = DhtDiscoveredNodeScheduler::with_config(
            receiver,
            KTable::new(Id20::ZERO),
            config(10, 1),
        )
        .unwrap();
        assert_eq!(sender.offer(v4(1, 1)), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(v4(2, 2)), DhtDiscoveryOffer::Queued);
        let task = tokio::spawn(scheduler.run(std::future::pending()));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                tokio::task::yield_now().await;
                if stats.snapshot().received == 2 {
                    break;
                }
            }
        })
        .await
        .expect("both nodes must enter the local partial batch");
        drop(routes);
        assert_eq!(
            task.await.unwrap(),
            DhtDiscoveredNodeSchedulerExit::RoutesClosed { pending_dropped: 2 }
        );
        assert_eq!(stats.snapshot().routes_closed_dropped, 2);
    }

    #[tokio::test]
    async fn terminal_events_close_and_account_for_queued_ingress() {
        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(2).unwrap());
        let (scheduler, routes, stats) = DhtDiscoveredNodeScheduler::with_config(
            receiver,
            KTable::new(Id20::ZERO),
            config(10, 1),
        )
        .unwrap();
        assert_eq!(sender.offer(v4(1, 1)), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(v4(2, 2)), DhtDiscoveryOffer::Queued);
        drop(routes);
        assert_eq!(
            scheduler.run(std::future::pending()).await,
            DhtDiscoveredNodeSchedulerExit::RoutesClosed { pending_dropped: 2 }
        );
        assert_eq!(stats.snapshot().routes_closed_dropped, 2);
        assert_eq!(sender.offer(v4(3, 3)), DhtDiscoveryOffer::ReceiverClosed);

        let (sender, receiver) = dht_discovery_channel(NonZeroUsize::new(2).unwrap());
        let (scheduler, _routes, stats) = DhtDiscoveredNodeScheduler::with_config(
            receiver,
            KTable::new(Id20::ZERO),
            config(10, 1),
        )
        .unwrap();
        assert_eq!(sender.offer(v4(4, 4)), DhtDiscoveryOffer::Queued);
        assert_eq!(sender.offer(v4(5, 5)), DhtDiscoveryOffer::Queued);
        assert_eq!(
            scheduler.run(std::future::ready(())).await,
            DhtDiscoveredNodeSchedulerExit::Shutdown { pending_dropped: 2 }
        );
        assert_eq!(stats.snapshot().received, 0);
        assert_eq!(stats.snapshot().shutdown_dropped, 2);
        assert_eq!(sender.offer(v4(6, 6)), DhtDiscoveryOffer::ReceiverClosed);
    }

    #[test]
    fn address_key_ignores_port_flow_and_id_but_keeps_family_and_scope() {
        let v4 = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1));
        let v4_other_port = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 2));
        let mapped = SocketAddr::V6(SocketAddrV6::new(
            Ipv4Addr::LOCALHOST.to_ipv6_mapped(),
            1,
            0,
            0,
        ));
        let scoped_7 = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1, 99, 7));
        let scoped_7_alias = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 2, 42, 7));
        let scoped_8 = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1, 99, 8));

        assert_eq!(
            NodeAddressKey::from(v4),
            NodeAddressKey::from(v4_other_port)
        );
        assert_ne!(NodeAddressKey::from(v4), NodeAddressKey::from(mapped));
        assert_eq!(
            NodeAddressKey::from(scoped_7),
            NodeAddressKey::from(scoped_7_alias)
        );
        assert_ne!(
            NodeAddressKey::from(scoped_7),
            NodeAddressKey::from(scoped_8)
        );
        assert_eq!(NodeAddressKey::from(v4).filter_addr().port(), 0);
        let SocketAddr::V6(normalized) = NodeAddressKey::from(scoped_7).filter_addr() else {
            panic!("the native IPv6 key must remain IPv6");
        };
        assert_eq!(normalized.port(), 0);
        assert_eq!(normalized.flowinfo(), 0);
        assert_eq!(normalized.scope_id(), 7);
    }
}
