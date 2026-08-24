use std::future::Future;
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::{DhtDiscoveredNodePingInput, Id20, RoutingNode};

const RESEED_DELAY: Duration = Duration::from_secs(10 * 60);
const DEFAULT_BOOTSTRAP_NODES: [&str; 6] = [
    "router.utorrent.com:6881",
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.aelitis.com:6881",
    "router.silotis.us:6881",
    "dht.libtorrent.org:25401",
];

/// Terminal state of the owned bootstrap-node ping producer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtBootstrapPingProducerExit {
    /// Caller shutdown won. The count is the selected suffix not classified or
    /// committed.
    Shutdown { selected_dropped: usize },
    /// The shared ping route closed. The count is the selected suffix not
    /// classified or committed.
    InputClosed { selected_dropped: usize },
}

#[derive(Default)]
struct DhtBootstrapPingProducerStatsInner {
    rounds_started: AtomicU64,
    selected: AtomicU64,
    resolution_attempts: AtomicU64,
    resolution_failed: AtomicU64,
    queued: AtomicU64,
    input_closed_dropped: AtomicU64,
    shutdown_dropped: AtomicU64,
}

/// Cloneable, sender-free view of bootstrap ping producer counters.
#[derive(Clone, Default)]
pub struct DhtBootstrapPingProducerStatsHandle {
    inner: Arc<DhtBootstrapPingProducerStatsInner>,
}

/// One non-transactional snapshot of monotonic bootstrap ping counters.
///
/// After normal exit, `selected` equals the saturating sum of
/// `resolution_failed`, `queued`, `input_closed_dropped`, and
/// `shutdown_dropped`. `resolution_attempts` deliberately overlaps those
/// terminal outcomes: an attempt is counted immediately before its resolver
/// future is polled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtBootstrapPingProducerStats {
    /// Immediate or timer-triggered passes over the configured node list.
    pub rounds_started: u64,
    /// Configured endpoint occurrences admitted by started rounds.
    pub selected: u64,
    /// Endpoint occurrences whose resolver future was constructed for polling.
    pub resolution_attempts: u64,
    /// Resolver errors or successful resolver results containing no addresses.
    pub resolution_failed: u64,
    /// Resolved endpoint occurrences committed to the shared ping-route queue.
    pub queued: u64,
    /// Selected suffix occurrences abandoned because the ping route closed.
    pub input_closed_dropped: u64,
    /// Selected suffix occurrences abandoned because caller shutdown won.
    pub shutdown_dropped: u64,
}

impl DhtBootstrapPingProducerStatsHandle {
    /// Read each saturating counter independently with relaxed ordering.
    ///
    /// Cross-field conservation is guaranteed only after normal producer exit.
    #[must_use]
    pub fn snapshot(&self) -> DhtBootstrapPingProducerStats {
        DhtBootstrapPingProducerStats {
            rounds_started: self.inner.rounds_started.load(Ordering::Relaxed),
            selected: self.inner.selected.load(Ordering::Relaxed),
            resolution_attempts: self.inner.resolution_attempts.load(Ordering::Relaxed),
            resolution_failed: self.inner.resolution_failed.load(Ordering::Relaxed),
            queued: self.inner.queued.load(Ordering::Relaxed),
            input_closed_dropped: self.inner.input_closed_dropped.load(Ordering::Relaxed),
            shutdown_dropped: self.inner.shutdown_dropped.load(Ordering::Relaxed),
        }
    }
}

/// Owned periodic producer of bootstrap endpoints for the shared ping route.
///
/// Construction uses the six production Go bootstrap strings in exact order.
/// The first round is immediate. Every subsequent round starts after one fresh
/// ten-minute, cancellation-aware delay created only after the preceding round
/// finishes; missed periods never catch up. Endpoints are resolved and queued
/// sequentially, preserving every configured occurrence and never fanning one
/// endpoint out into multiple pings. Resolver failures are skipped so later
/// configured endpoints still run.
///
/// Address selection mirrors Go's `net.ResolveUDPAddr("udp", endpoint)` over
/// resolver-order results: an endpoint containing `[` prefers the first native
/// IPv6 address; every other endpoint prefers the first IPv4-compatible
/// address; either path falls back to the first result. Native IPv4 and
/// IPv4-mapped IPv6 are emitted as [`SocketAddr::V4`] so the owned IPv4 DHT
/// transport can send them. Native IPv6 is retained and reaches the worker's
/// existing typed transport-family failure.
///
/// Shutdown wins every ready tie. Input closure wins each remaining tie before
/// a round, resolver, capacity reservation, or delay. Once capacity is
/// reserved, the zero-ID routing node is committed synchronously and receiver
/// close cannot revoke that commit. This type owns and spawns no task.
#[must_use = "the producer must be run to seed the shared ping route"]
pub struct DhtBootstrapPingProducer {
    bootstrap_nodes: Box<[String]>,
    input: DhtDiscoveredNodePingInput,
    stats: DhtBootstrapPingProducerStatsHandle,
}

impl DhtBootstrapPingProducer {
    /// Construct the fixed six-node, ten-minute production producer.
    pub fn new(input: DhtDiscoveredNodePingInput) -> (Self, DhtBootstrapPingProducerStatsHandle) {
        Self::from_bootstrap_nodes(
            input,
            DEFAULT_BOOTSTRAP_NODES
                .map(String::from)
                .into_iter()
                .collect(),
        )
    }

    #[cfg(test)]
    pub(crate) fn with_bootstrap_nodes(
        input: DhtDiscoveredNodePingInput,
        bootstrap_nodes: Vec<String>,
    ) -> (Self, DhtBootstrapPingProducerStatsHandle) {
        Self::from_bootstrap_nodes(input, bootstrap_nodes)
    }

    fn from_bootstrap_nodes(
        input: DhtDiscoveredNodePingInput,
        bootstrap_nodes: Vec<String>,
    ) -> (Self, DhtBootstrapPingProducerStatsHandle) {
        let stats = DhtBootstrapPingProducerStatsHandle::default();
        (
            Self {
                bootstrap_nodes: bootstrap_nodes.into_boxed_slice(),
                input,
                stats: stats.clone(),
            },
            stats,
        )
    }

    /// Run until caller shutdown or closure of the shared ping route.
    ///
    /// DNS lookup is asynchronous so shutdown and route closure stop awaiting
    /// it and let this producer return. Tokio may already have dispatched an
    /// operating-system lookup on its blocking pool; dropping `lookup_host`
    /// detaches that internal job, which can finish after this producer exits.
    /// The producer itself owns and detaches no task.
    ///
    /// On normal terminal return, `selected` equals the saturating sum of
    /// `resolution_failed`, `queued`, `input_closed_dropped`, and
    /// `shutdown_dropped`. Dropping this future is not a terminal return and
    /// carries no cross-counter promise.
    pub async fn run<F>(self, shutdown: F) -> DhtBootstrapPingProducerExit
    where
        F: Future<Output = ()>,
    {
        self.run_with(
            shutdown,
            |endpoint| async move {
                tokio::net::lookup_host(endpoint)
                    .await
                    .map(|addresses| addresses.collect::<Vec<_>>())
            },
            tokio::time::sleep,
            |_, _, _| {},
        )
        .await
    }

    async fn run_with<F, Resolve, ResolveFuture, ResolveError, Delay, DelayFuture, AfterReserve>(
        self,
        shutdown: F,
        mut resolve: Resolve,
        mut delay: Delay,
        mut after_reserve: AfterReserve,
    ) -> DhtBootstrapPingProducerExit
    where
        F: Future<Output = ()>,
        Resolve: FnMut(String) -> ResolveFuture,
        ResolveFuture: Future<Output = Result<Vec<SocketAddr>, ResolveError>>,
        Delay: FnMut(Duration) -> DelayFuture,
        DelayFuture: Future<Output = ()>,
        AfterReserve: FnMut(usize, &str, SocketAddr),
    {
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => return self.finish_shutdown(0),
                () = self.input.closed() => return self.finish_input_closed(0),
                () = async {} => {}
            }

            increment_saturating(&self.stats.inner.rounds_started);
            increment_saturating_by(&self.stats.inner.selected, self.bootstrap_nodes.len());

            for (index, configured) in self.bootstrap_nodes.iter().enumerate() {
                let remaining = self.bootstrap_nodes.len() - index;
                let result = tokio::select! {
                    biased;
                    () = &mut shutdown => return self.finish_shutdown(remaining),
                    () = self.input.closed() => return self.finish_input_closed(remaining),
                    result = async {
                        increment_saturating(&self.stats.inner.resolution_attempts);
                        resolve(configured.clone()).await
                    } => result,
                };
                let Some(address) = result
                    .ok()
                    .and_then(|addresses| select_resolved_address(configured, addresses))
                else {
                    increment_saturating(&self.stats.inner.resolution_failed);
                    continue;
                };

                let permit = tokio::select! {
                    biased;
                    () = &mut shutdown => return self.finish_shutdown(remaining),
                    () = self.input.closed() => return self.finish_input_closed(remaining),
                    permit = self.input.reserve() => match permit {
                        Ok(permit) => permit,
                        Err(_closed) => return self.finish_input_closed(remaining),
                    },
                };

                after_reserve(index, configured, address);
                permit.deliver(RoutingNode {
                    id: Id20::ZERO,
                    addr: address,
                });
                increment_saturating(&self.stats.inner.queued);
            }

            tokio::select! {
                biased;
                () = &mut shutdown => return self.finish_shutdown(0),
                () = self.input.closed() => return self.finish_input_closed(0),
                () = delay(RESEED_DELAY) => {}
            }
        }
    }

    fn finish_shutdown(&self, selected_dropped: usize) -> DhtBootstrapPingProducerExit {
        increment_saturating_by(&self.stats.inner.shutdown_dropped, selected_dropped);
        DhtBootstrapPingProducerExit::Shutdown { selected_dropped }
    }

    fn finish_input_closed(&self, selected_dropped: usize) -> DhtBootstrapPingProducerExit {
        increment_saturating_by(&self.stats.inner.input_closed_dropped, selected_dropped);
        DhtBootstrapPingProducerExit::InputClosed { selected_dropped }
    }
}

fn select_resolved_address(
    configured: &str,
    addresses: impl IntoIterator<Item = SocketAddr>,
) -> Option<SocketAddr> {
    let prefer_native_v6 = configured.contains('[');
    let mut first = None;

    for address in addresses {
        let ipv4_compatible = ipv4_compatible(address).is_some();
        let canonical = canonicalize_address(address);
        if first.is_none() {
            first = Some(canonical);
        }
        if prefer_native_v6 != ipv4_compatible {
            return Some(canonical);
        }
    }

    first
}

fn ipv4_compatible(address: SocketAddr) -> Option<SocketAddrV4> {
    match address {
        SocketAddr::V4(address) => Some(address),
        SocketAddr::V6(address) => address
            .ip()
            .to_ipv4_mapped()
            .map(|ip| SocketAddrV4::new(ip, address.port())),
    }
}

fn canonicalize_address(address: SocketAddr) -> SocketAddr {
    ipv4_compatible(address).map_or(address, SocketAddr::V4)
}

fn increment_saturating(counter: &AtomicU64) {
    let _previous = counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(1))
        })
        .expect("a saturating counter update always supplies a replacement");
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
#[path = "dht_bootstrap_ping_producer_parity.rs"]
mod parity_tests;

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::{pending, poll_fn, ready};
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV6};
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    use tokio::sync::oneshot;

    use super::*;

    struct ScriptedDelay {
        ready: bool,
    }

    struct PendingResolve {
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    impl Future for PendingResolve {
        type Output = Result<Vec<SocketAddr>, ()>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::Relaxed);
            Poll::Pending
        }
    }

    impl Drop for PendingResolve {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl Future for ScriptedDelay {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.ready {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    fn pending_delay(duration: Duration) -> ScriptedDelay {
        assert_eq!(duration, RESEED_DELAY);
        ScriptedDelay { ready: false }
    }

    fn one_delay_then_pending() -> impl FnMut(Duration) -> ScriptedDelay {
        let mut first = true;
        move |duration| {
            assert_eq!(duration, RESEED_DELAY);
            let ready = first;
            first = false;
            ScriptedDelay { ready }
        }
    }

    fn v4(last: u8, port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, last), port))
    }

    fn v6(last: u16, port: u16) -> SocketAddr {
        let ip = Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, last);
        SocketAddr::V6(SocketAddrV6::new(ip, port, 0, 0))
    }

    fn mapped_v4(last: u8, port: u16) -> SocketAddr {
        let ip = Ipv4Addr::new(192, 0, 2, last).to_ipv6_mapped();
        SocketAddr::V6(SocketAddrV6::new(ip, port, 0, 0))
    }

    fn node(address: SocketAddr) -> RoutingNode {
        RoutingNode {
            id: Id20::ZERO,
            addr: address,
        }
    }

    async fn poll_once_pending<F>(mut future: Pin<&mut F>)
    where
        F: Future,
    {
        poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }

    fn assert_conservation(stats: DhtBootstrapPingProducerStats) {
        assert_eq!(
            stats.selected,
            stats
                .resolution_failed
                .saturating_add(stats.queued)
                .saturating_add(stats.input_closed_dropped)
                .saturating_add(stats.shutdown_dropped)
        );
    }

    #[test]
    fn constants_defaults_and_public_handles_are_sound() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_eq!(RESEED_DELAY, Duration::from_secs(10 * 60));
        assert_eq!(
            DEFAULT_BOOTSTRAP_NODES,
            [
                "router.utorrent.com:6881",
                "router.bittorrent.com:6881",
                "dht.transmissionbt.com:6881",
                "dht.aelitis.com:6881",
                "router.silotis.us:6881",
                "dht.libtorrent.org:25401",
            ]
        );
        let (input, _receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtBootstrapPingProducer::new(input);
        assert_eq!(
            producer.bootstrap_nodes.as_ref(),
            &DEFAULT_BOOTSTRAP_NODES.map(String::from)
        );
        assert_eq!(stats.snapshot(), DhtBootstrapPingProducerStats::default());
        assert_send_sync::<DhtBootstrapPingProducer>();
        assert_send_sync::<DhtBootstrapPingProducerStatsHandle>();

        fn assert_send<T: Send>(_value: T) {}
        let (input, _receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, _stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(input, Vec::new());
        assert_send(producer.run(pending()));
    }

    #[test]
    fn go_udp_family_preference_and_ipv4_canonicalization_are_exact() {
        let native_v4 = v4(1, 1001);
        let mapped = mapped_v4(2, 1002);
        let native_v6 = v6(3, 1003);
        let later_v6 = v6(4, 1004);

        assert_eq!(
            select_resolved_address("example.test:1", [native_v6, mapped, native_v4]),
            Some(v4(2, 1002))
        );
        assert_eq!(
            select_resolved_address("[example.test]:1", [native_v4, mapped, native_v6, later_v6]),
            Some(native_v6)
        );
        assert_eq!(
            select_resolved_address("example.test:1", [native_v6, later_v6]),
            Some(native_v6)
        );
        assert_eq!(
            select_resolved_address("[example.test]:1", [mapped, native_v4]),
            Some(v4(2, 1002))
        );
        assert_eq!(
            select_resolved_address("example.test:1", [native_v4, mapped]),
            Some(native_v4)
        );
        assert_eq!(
            select_resolved_address("example.test:1", std::iter::empty()),
            None
        );
    }

    #[test]
    fn native_ipv6_zone_is_retained_and_mapped_ipv4_zone_is_discarded() {
        let native = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 6881, 7, 11));
        let mapped = SocketAddr::V6(SocketAddrV6::new(
            Ipv4Addr::new(192, 0, 2, 9).to_ipv6_mapped(),
            6882,
            7,
            11,
        ));

        assert_eq!(canonicalize_address(native), native);
        assert_eq!(canonicalize_address(mapped), v4(9, 6882));
    }

    #[tokio::test]
    async fn ready_shutdown_wins_preclosed_input_before_starting_a_round() {
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        receiver.close();
        let (producer, stats) =
            DhtBootstrapPingProducer::with_bootstrap_nodes(input, vec!["first".to_owned()]);

        assert_eq!(
            producer
                .run_with(
                    ready(()),
                    |_| -> std::future::Pending<Result<Vec<SocketAddr>, ()>> {
                        panic!("pre-ready shutdown must not construct a resolver future")
                    },
                    |_| -> std::future::Pending<()> {
                        panic!("pre-ready shutdown must not construct a delay")
                    },
                    |_, _, _| {},
                )
                .await,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_eq!(stats.snapshot(), DhtBootstrapPingProducerStats::default());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn preclosed_input_exits_before_starting_a_round() {
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        receiver.close();
        let (producer, stats) =
            DhtBootstrapPingProducer::with_bootstrap_nodes(input, vec!["first".to_owned()]);

        assert_eq!(
            producer
                .run_with(
                    pending(),
                    |_| -> std::future::Pending<Result<Vec<SocketAddr>, ()>> {
                        panic!("preclosed input must not construct a resolver future")
                    },
                    |_| -> std::future::Pending<()> {
                        panic!("preclosed input must not construct a delay")
                    },
                    |_, _, _| {},
                )
                .await,
            DhtBootstrapPingProducerExit::InputClosed {
                selected_dropped: 0
            }
        );
        assert_eq!(stats.snapshot(), DhtBootstrapPingProducerStats::default());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn resolution_is_sequential_preserves_occurrences_and_continues_after_failures() {
        let configured = vec![
            "first".to_owned(),
            "error".to_owned(),
            "empty".to_owned(),
            "fourth".to_owned(),
            "first".to_owned(),
        ];
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(3);
        let (producer, stats) =
            DhtBootstrapPingProducer::with_bootstrap_nodes(input, configured.clone());
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_for_resolver = Arc::clone(&observed);
        let results = Arc::new(Mutex::new(VecDeque::from([
            Ok(vec![v6(1, 1001), mapped_v4(2, 1002), v4(3, 1003)]),
            Err("rejected"),
            Ok(Vec::new()),
            Ok(vec![v6(4, 1004), v6(5, 1005)]),
            Ok(vec![v4(6, 1006)]),
        ])));
        let results_for_resolver = Arc::clone(&results);
        let mut run = Box::pin(producer.run_with(
            pending(),
            move |endpoint| {
                observed_for_resolver.lock().unwrap().push(endpoint);
                ready(results_for_resolver.lock().unwrap().pop_front().unwrap())
            },
            pending_delay,
            |_, _, _| {},
        ));

        poll_once_pending(run.as_mut()).await;
        assert_eq!(*observed.lock().unwrap(), configured);
        assert_eq!(receiver.recv().await, Some(node(v4(2, 1002))));
        assert_eq!(receiver.recv().await, Some(node(v6(4, 1004))));
        assert_eq!(receiver.recv().await, Some(node(v4(6, 1006))));
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 5,
                resolution_attempts: 5,
                resolution_failed: 2,
                queued: 3,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        drop(run);
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn first_round_is_immediate_and_later_round_uses_a_fresh_ten_minute_delay() {
        let selected = v4(1, 1001);
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(2);
        let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(
            input,
            vec!["127.0.0.1:1001".to_owned()],
        );
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run(async move {
            let _ = shutdown_rx.await;
        });
        tokio::pin!(run);

        // The production resolver is deliberately bypassed in the timing
        // assertion below; this public-run smoke instead proves pre-ready
        // shutdown remains cancellation-aware without a lookup.
        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_eq!(stats.snapshot(), DhtBootstrapPingProducerStats::default());
        assert_eq!(receiver.recv().await, None);

        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(2);
        let (producer, stats) =
            DhtBootstrapPingProducer::with_bootstrap_nodes(input, vec!["first".to_owned()]);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            move |_| ready(Ok::<_, ()>(vec![selected])),
            tokio::time::sleep,
            |_, _, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(node(selected)));
        assert_eq!(stats.snapshot().rounds_started, 1);
        tokio::time::advance(RESEED_DELAY - Duration::from_millis(1)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot().rounds_started, 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(node(selected)));
        assert_eq!(stats.snapshot().rounds_started, 2);

        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test(start_paused = true)]
    async fn long_resolution_starts_fresh_delay_only_after_the_round_finishes() {
        let selected = v4(1, 1001);
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(2);
        let (producer, stats) =
            DhtBootstrapPingProducer::with_bootstrap_nodes(input, vec!["first".to_owned()]);
        let (resolution_tx, resolution_rx) = oneshot::channel();
        let mut resolution_rx = Some(resolution_rx);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            move |_| {
                let first = resolution_rx.take();
                async move {
                    if let Some(first) = first {
                        let _ = first.await;
                    }
                    Ok::<_, ()>(vec![selected])
                }
            },
            tokio::time::sleep,
            |_, _, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        tokio::time::advance(Duration::from_secs(60 * 60)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot().rounds_started, 1);
        assert_eq!(stats.snapshot().queued, 0);

        resolution_tx.send(()).unwrap();
        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(node(selected)));
        assert_eq!(stats.snapshot().rounds_started, 1);

        tokio::time::advance(RESEED_DELAY - Duration::from_millis(1)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot().rounds_started, 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(node(selected)));
        assert_eq!(stats.snapshot().rounds_started, 2);

        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn delayed_poll_does_not_catch_up_missed_rounds() {
        let selected = v4(1, 1001);
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(2);
        let (producer, stats) =
            DhtBootstrapPingProducer::with_bootstrap_nodes(input, vec!["first".to_owned()]);
        let mut run = Box::pin(producer.run_with(
            pending(),
            move |_| ready(Ok::<_, ()>(vec![selected])),
            one_delay_then_pending(),
            |_, _, _| {},
        ));

        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(node(selected)));
        assert_eq!(receiver.recv().await, Some(node(selected)));
        assert_eq!(stats.snapshot().rounds_started, 2);
        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot().rounds_started, 2);
        drop(run);
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn empty_configuration_starts_a_round_then_waits_without_spinning() {
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(input, Vec::new());
        let mut run = Box::pin(producer.run_with(
            pending(),
            |_| -> std::future::Pending<Result<Vec<SocketAddr>, ()>> {
                panic!("an empty configuration must not resolve")
            },
            pending_delay,
            |_, _, _| {},
        ));

        poll_once_pending(run.as_mut()).await;
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        drop(run);
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn resolution_attempt_is_counted_before_its_future_is_polled_pending() {
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(
            input,
            vec!["first".to_owned(), "second".to_owned()],
        );
        let mut run = Box::pin(producer.run_with(
            pending(),
            |_| pending::<Result<Vec<SocketAddr>, ()>>(),
            pending_delay,
            |_, _, _| {},
        ));

        poll_once_pending(run.as_mut()).await;
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 2,
                resolution_attempts: 1,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        drop(run);
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn shutdown_during_resolution_accounts_current_and_suffix() {
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(
            input,
            vec!["first".to_owned(), "second".to_owned(), "third".to_owned()],
        );
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            |_| pending::<Result<Vec<SocketAddr>, ()>>(),
            pending_delay,
            |_, _, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 3
            }
        );
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 3,
                resolution_attempts: 1,
                shutdown_dropped: 3,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn input_close_during_resolution_accounts_current_and_suffix() {
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(
            input,
            vec!["first".to_owned(), "second".to_owned(), "third".to_owned()],
        );
        let run = producer.run_with(
            pending(),
            |_| pending::<Result<Vec<SocketAddr>, ()>>(),
            pending_delay,
            |_, _, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        receiver.close();
        assert_eq!(
            run.await,
            DhtBootstrapPingProducerExit::InputClosed {
                selected_dropped: 3
            }
        );
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 3,
                resolution_attempts: 1,
                input_closed_dropped: 3,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn mid_round_ready_shutdown_prevents_the_next_resolver_call_and_attempt() {
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(
            input,
            vec!["first".to_owned(), "second".to_owned()],
        );
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let mut shutdown_tx = Some(shutdown_tx);
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_run = Arc::clone(&calls);

        let exit = producer
            .run_with(
                async move {
                    let _ = shutdown_rx.await;
                },
                |endpoint| {
                    assert_eq!(endpoint, "first");
                    assert_eq!(calls_for_run.fetch_add(1, Ordering::Relaxed), 0);
                    shutdown_tx.take().unwrap().send(()).unwrap();
                    ready(Err::<Vec<SocketAddr>, _>(()))
                },
                pending_delay,
                |_, _, _| {},
            )
            .await;

        assert_eq!(
            exit,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 1
            }
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 2,
                resolution_attempts: 1,
                resolution_failed: 1,
                shutdown_dropped: 1,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn mid_round_ready_input_close_prevents_the_next_resolver_call_and_attempt() {
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(
            input,
            vec!["first".to_owned(), "second".to_owned()],
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_run = Arc::clone(&calls);

        let exit = producer
            .run_with(
                pending(),
                |endpoint| {
                    assert_eq!(endpoint, "first");
                    assert_eq!(calls_for_run.fetch_add(1, Ordering::Relaxed), 0);
                    receiver.close();
                    ready(Err::<Vec<SocketAddr>, _>(()))
                },
                pending_delay,
                |_, _, _| {},
            )
            .await;

        assert_eq!(
            exit,
            DhtBootstrapPingProducerExit::InputClosed {
                selected_dropped: 1
            }
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 2,
                resolution_attempts: 1,
                resolution_failed: 1,
                input_closed_dropped: 1,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn ready_shutdown_beats_ready_capacity_after_resolution() {
        let selected = v4(1, 1001);
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) =
            DhtBootstrapPingProducer::with_bootstrap_nodes(input, vec!["first".to_owned()]);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let mut shutdown_tx = Some(shutdown_tx);
        let reserve_hooks = Arc::new(AtomicUsize::new(0));
        let reserve_hooks_for_run = Arc::clone(&reserve_hooks);

        let exit = producer
            .run_with(
                async move {
                    let _ = shutdown_rx.await;
                },
                |_| {
                    shutdown_tx.take().unwrap().send(()).unwrap();
                    ready(Ok::<_, ()>(vec![selected]))
                },
                pending_delay,
                move |_, _, _| {
                    reserve_hooks_for_run.fetch_add(1, Ordering::Relaxed);
                },
            )
            .await;

        assert_eq!(
            exit,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 1
            }
        );
        assert_eq!(reserve_hooks.load(Ordering::Relaxed), 0);
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 1,
                resolution_attempts: 1,
                shutdown_dropped: 1,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn ready_input_close_beats_ready_reserve_error_after_resolution() {
        let selected = v4(1, 1001);
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) =
            DhtBootstrapPingProducer::with_bootstrap_nodes(input, vec!["first".to_owned()]);
        let reserve_hooks = Arc::new(AtomicUsize::new(0));
        let reserve_hooks_for_run = Arc::clone(&reserve_hooks);

        let exit = producer
            .run_with(
                pending(),
                |_| {
                    receiver.close();
                    ready(Ok::<_, ()>(vec![selected]))
                },
                pending_delay,
                move |_, _, _| {
                    reserve_hooks_for_run.fetch_add(1, Ordering::Relaxed);
                },
            )
            .await;

        assert_eq!(
            exit,
            DhtBootstrapPingProducerExit::InputClosed {
                selected_dropped: 1
            }
        );
        assert_eq!(reserve_hooks.load(Ordering::Relaxed), 0);
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 1,
                resolution_attempts: 1,
                input_closed_dropped: 1,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn cancelling_pending_resolution_drops_the_local_future_and_accounts_suffix() {
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(
            input,
            vec!["first".to_owned(), "second".to_owned()],
        );
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let polls_for_run = Arc::clone(&polls);
        let drops_for_run = Arc::clone(&drops);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            move |_| PendingResolve {
                polls: Arc::clone(&polls_for_run),
                drops: Arc::clone(&drops_for_run),
            },
            pending_delay,
            |_, _, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 2
            }
        );
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 2,
                resolution_attempts: 1,
                shutdown_dropped: 2,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn shutdown_while_capacity_is_blocked_preserves_prefix_and_drops_suffix() {
        let sentinel = node(v4(9, 9009));
        let selected = [v4(1, 1001), v4(2, 1002), v4(3, 1003)];
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        input.send(sentinel).await.unwrap();
        let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(
            input,
            vec!["first".to_owned(), "second".to_owned(), "third".to_owned()],
        );
        let results = Arc::new(Mutex::new(VecDeque::from(
            selected.map(|address| Ok::<_, ()>(vec![address])),
        )));
        let results_for_run = Arc::clone(&results);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            move |_| ready(results_for_run.lock().unwrap().pop_front().unwrap()),
            pending_delay,
            |_, _, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(sentinel));
        poll_once_pending(run.as_mut()).await;
        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 2
            }
        );
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 3,
                resolution_attempts: 2,
                queued: 1,
                shutdown_dropped: 2,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
        assert_eq!(receiver.recv().await, Some(node(selected[0])));
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn input_close_while_capacity_is_blocked_preserves_prefix_and_drops_suffix() {
        let sentinel = node(v4(9, 9009));
        let selected = [v4(1, 1001), v4(2, 1002), v4(3, 1003)];
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        input.send(sentinel).await.unwrap();
        let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(
            input,
            vec!["first".to_owned(), "second".to_owned(), "third".to_owned()],
        );
        let results = Arc::new(Mutex::new(VecDeque::from(
            selected.map(|address| Ok::<_, ()>(vec![address])),
        )));
        let results_for_run = Arc::clone(&results);
        let run = producer.run_with(
            pending(),
            move |_| ready(results_for_run.lock().unwrap().pop_front().unwrap()),
            pending_delay,
            |_, _, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(sentinel));
        poll_once_pending(run.as_mut()).await;
        receiver.close();
        assert_eq!(
            run.await,
            DhtBootstrapPingProducerExit::InputClosed {
                selected_dropped: 2
            }
        );
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 3,
                resolution_attempts: 2,
                queued: 1,
                input_closed_dropped: 2,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
        assert_eq!(receiver.recv().await, Some(node(selected[0])));
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn close_after_reserve_keeps_commit_authority_then_drops_later_suffix() {
        let selected = [v4(1, 1001), v4(2, 1002)];
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(
            input,
            vec!["first".to_owned(), "second".to_owned()],
        );
        let results = Arc::new(Mutex::new(VecDeque::from(
            selected.map(|address| Ok::<_, ()>(vec![address])),
        )));
        let results_for_run = Arc::clone(&results);

        let exit = producer
            .run_with(
                pending(),
                move |_| ready(results_for_run.lock().unwrap().pop_front().unwrap()),
                pending_delay,
                |index, configured, address| {
                    assert_eq!(index, 0);
                    assert_eq!(configured, "first");
                    assert_eq!(address, selected[0]);
                    receiver.close();
                },
            )
            .await;

        assert_eq!(
            exit,
            DhtBootstrapPingProducerExit::InputClosed {
                selected_dropped: 1
            }
        );
        assert_eq!(receiver.recv().await, Some(node(selected[0])));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 2,
                resolution_attempts: 1,
                queued: 1,
                input_closed_dropped: 1,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn shutdown_after_reserve_keeps_commit_authority_then_drops_later_suffix() {
        let selected = [v4(1, 1001), v4(2, 1002)];
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(
            input,
            vec!["first".to_owned(), "second".to_owned()],
        );
        let results = Arc::new(Mutex::new(VecDeque::from(
            selected.map(|address| Ok::<_, ()>(vec![address])),
        )));
        let results_for_run = Arc::clone(&results);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let mut shutdown_tx = Some(shutdown_tx);

        let exit = producer
            .run_with(
                async move {
                    let _ = shutdown_rx.await;
                },
                move |_| ready(results_for_run.lock().unwrap().pop_front().unwrap()),
                pending_delay,
                |index, _, _| {
                    assert_eq!(index, 0);
                    shutdown_tx.take().unwrap().send(()).unwrap();
                },
            )
            .await;

        assert_eq!(
            exit,
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: 1
            }
        );
        assert_eq!(receiver.recv().await, Some(node(selected[0])));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 2,
                resolution_attempts: 1,
                queued: 1,
                shutdown_dropped: 1,
                ..DhtBootstrapPingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn shutdown_and_input_close_during_delay_drop_no_selected_nodes() {
        for close_input in [false, true] {
            let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
            let (producer, stats) =
                DhtBootstrapPingProducer::with_bootstrap_nodes(input, vec!["bad".to_owned()]);
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let run = producer.run_with(
                async move {
                    let _ = shutdown_rx.await;
                },
                |_| ready(Err::<Vec<SocketAddr>, _>(())),
                pending_delay,
                |_, _, _| {},
            );
            tokio::pin!(run);

            poll_once_pending(run.as_mut()).await;
            if close_input {
                receiver.close();
                assert_eq!(
                    run.await,
                    DhtBootstrapPingProducerExit::InputClosed {
                        selected_dropped: 0
                    }
                );
            } else {
                shutdown_tx.send(()).unwrap();
                assert_eq!(
                    run.await,
                    DhtBootstrapPingProducerExit::Shutdown {
                        selected_dropped: 0
                    }
                );
            }
            assert_eq!(
                stats.snapshot(),
                DhtBootstrapPingProducerStats {
                    rounds_started: 1,
                    selected: 1,
                    resolution_attempts: 1,
                    resolution_failed: 1,
                    ..DhtBootstrapPingProducerStats::default()
                }
            );
            assert_conservation(stats.snapshot());
            assert_eq!(receiver.recv().await, None);
        }
    }

    #[tokio::test]
    async fn constructing_without_running_spawns_nothing_and_drop_releases_route_eof() {
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtBootstrapPingProducer::new(input);

        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(stats.snapshot(), DhtBootstrapPingProducerStats::default());
        drop(producer);
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn dropping_polled_run_releases_sender_without_terminal_classification() {
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) =
            DhtBootstrapPingProducer::with_bootstrap_nodes(input, vec!["first".to_owned()]);
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let polls_for_run = Arc::clone(&polls);
        let drops_for_run = Arc::clone(&drops);
        let mut run = Box::pin(producer.run_with(
            pending(),
            move |_| PendingResolve {
                polls: Arc::clone(&polls_for_run),
                drops: Arc::clone(&drops_for_run),
            },
            pending_delay,
            |_, _, _| {},
        ));

        poll_once_pending(run.as_mut()).await;
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        assert_eq!(
            stats.snapshot(),
            DhtBootstrapPingProducerStats {
                rounds_started: 1,
                selected: 1,
                resolution_attempts: 1,
                ..DhtBootstrapPingProducerStats::default()
            }
        );

        drop(run);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(receiver.recv().await, None);
        assert_eq!(stats.snapshot().shutdown_dropped, 0);
        assert_eq!(stats.snapshot().input_closed_dropped, 0);
    }

    #[test]
    fn counters_and_terminal_classification_saturate() {
        let (input, _receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtBootstrapPingProducer::with_bootstrap_nodes(input, Vec::new());
        for counter in [
            &stats.inner.rounds_started,
            &stats.inner.selected,
            &stats.inner.resolution_attempts,
            &stats.inner.resolution_failed,
            &stats.inner.queued,
            &stats.inner.input_closed_dropped,
            &stats.inner.shutdown_dropped,
        ] {
            counter.store(u64::MAX, Ordering::Relaxed);
            increment_saturating(counter);
            increment_saturating_by(counter, usize::MAX);
            assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
        }

        assert_eq!(
            producer.finish_shutdown(usize::MAX),
            DhtBootstrapPingProducerExit::Shutdown {
                selected_dropped: usize::MAX
            }
        );
        assert_eq!(
            producer.finish_input_closed(usize::MAX),
            DhtBootstrapPingProducerExit::InputClosed {
                selected_dropped: usize::MAX
            }
        );
        assert_eq!(stats.snapshot().shutdown_dropped, u64::MAX);
        assert_eq!(stats.snapshot().input_closed_dropped, u64::MAX);
    }
}
