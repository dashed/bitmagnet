use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{DhtDiscoveredNodePingInput, KTable, KTableNodeHandle};

const OLD_PEER_THRESHOLD: Duration = Duration::from_secs(15 * 60);
const QUERY_DELAY: Duration = Duration::from_secs(10);

/// Terminal state of the owned periodic oldest-node ping producer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtOldestNodePingProducerExit {
    /// Caller shutdown won. The count is the selected suffix not yet
    /// classified or committed.
    Shutdown { selected_dropped: usize },
    /// The shared ping route closed. The count is the selected suffix not yet
    /// classified or committed.
    InputClosed { selected_dropped: usize },
}

#[derive(Default)]
struct DhtOldestNodePingProducerStatsInner {
    table_queries: AtomicU64,
    selected: AtomicU64,
    dropped_skipped: AtomicU64,
    recent_skipped: AtomicU64,
    queued: AtomicU64,
    input_closed_dropped: AtomicU64,
    shutdown_dropped: AtomicU64,
}

/// Cloneable, sender-free view of periodic oldest-node ping counters.
#[derive(Clone, Default)]
pub struct DhtOldestNodePingProducerStatsHandle {
    inner: Arc<DhtOldestNodePingProducerStatsInner>,
}

/// One non-transactional snapshot of monotonic oldest-node ping counters.
///
/// After normal exit, `selected` equals the saturating sum of the five outcome
/// counters: `dropped_skipped`, `recent_skipped`, `queued`,
/// `input_closed_dropped`, and `shutdown_dropped`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtOldestNodePingProducerStats {
    /// Completed synchronous KTable oldest-node queries.
    pub table_queries: u64,
    /// Node occurrences returned by those queries.
    pub selected: u64,
    /// Selected occurrences whose retained live handle was already dropped.
    pub dropped_skipped: u64,
    /// Non-dropped selected occurrences that responded strictly after the
    /// fresh fifteen-minute cutoff.
    pub recent_skipped: u64,
    /// Selected occurrences committed to the shared ping-route queue.
    pub queued: u64,
    /// Selected suffix occurrences abandoned because the ping route closed.
    pub input_closed_dropped: u64,
    /// Selected suffix occurrences abandoned because caller shutdown won.
    pub shutdown_dropped: u64,
}

impl DhtOldestNodePingProducerStatsHandle {
    /// Read each saturating counter independently with relaxed ordering.
    ///
    /// Cross-field conservation is guaranteed only after normal producer exit.
    #[must_use]
    pub fn snapshot(&self) -> DhtOldestNodePingProducerStats {
        DhtOldestNodePingProducerStats {
            table_queries: self.inner.table_queries.load(Ordering::Relaxed),
            selected: self.inner.selected.load(Ordering::Relaxed),
            dropped_skipped: self.inner.dropped_skipped.load(Ordering::Relaxed),
            recent_skipped: self.inner.recent_skipped.load(Ordering::Relaxed),
            queued: self.inner.queued.load(Ordering::Relaxed),
            input_closed_dropped: self.inner.input_closed_dropped.load(Ordering::Relaxed),
            shutdown_dropped: self.inner.shutdown_dropped.load(Ordering::Relaxed),
        }
    }
}

/// Owned periodic producer of KTable oldest nodes for the shared ping route.
///
/// This producer sends directly through the scheduler's shared ping capacity;
/// it does not re-enter scheduler batching, filtering, routing, or counters.
/// Its live sender delays ping-route EOF until this value or its `run` future
/// is dropped or exits. This type owns and spawns no task.
///
/// Every loop begins with a fresh ten-second, cancellation-aware delay. Missed
/// periods never catch up. After the delay, one synchronous, uncapped
/// `get_oldest_nodes` query uses a safely floored fifteen-minute cutoff. The
/// returned live handles are processed sequentially in deterministic KTable
/// order, preserving every selected occurrence.
///
/// Each occurrence first reserves shared ping capacity. Shutdown wins every
/// ready tie, input closure wins the remaining ties, and capacity acquisition
/// is last. Once capacity is reserved, the retained handle is synchronously
/// rechecked: dropped state wins over recentness; a response strictly newer
/// than a fresh fifteen-minute cutoff is skipped, while an exact-cutoff
/// response remains eligible. An eligible handle is then snapshotted as an
/// immutable routing node and synchronously committed. Receiver close after
/// permit acquisition cannot revoke that commit authority.
pub struct DhtOldestNodePingProducer {
    table: KTable,
    input: DhtDiscoveredNodePingInput,
    stats: DhtOldestNodePingProducerStatsHandle,
}

impl DhtOldestNodePingProducer {
    /// Construct the fixed ten-second, fifteen-minute, uncapped producer.
    #[must_use]
    pub fn new(
        table: KTable,
        input: DhtDiscoveredNodePingInput,
    ) -> (Self, DhtOldestNodePingProducerStatsHandle) {
        let stats = DhtOldestNodePingProducerStatsHandle::default();
        (
            Self {
                table,
                input,
                stats: stats.clone(),
            },
            stats,
        )
    }

    /// Run until caller shutdown or closure of the shared ping route.
    ///
    /// On normal terminal return, `selected` equals the saturating sum of
    /// `dropped_skipped`, `recent_skipped`, `queued`,
    /// `input_closed_dropped`, and `shutdown_dropped`. Dropping this future is
    /// not a terminal return and carries no cross-counter promise.
    pub async fn run<F>(self, shutdown: F) -> DhtOldestNodePingProducerExit
    where
        F: Future<Output = ()>,
    {
        self.run_with(shutdown, Instant::now, tokio::time::sleep, |_, _| {})
            .await
    }

    async fn run_with<F, N, D, DF, B>(
        self,
        shutdown: F,
        mut now: N,
        mut delay: D,
        mut after_reserve: B,
    ) -> DhtOldestNodePingProducerExit
    where
        F: Future<Output = ()>,
        N: FnMut() -> Instant,
        D: FnMut(Duration) -> DF,
        DF: Future<Output = ()>,
        B: FnMut(usize, &KTableNodeHandle),
    {
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => return self.finish_shutdown(0),
                () = self.input.closed() => return self.finish_input_closed(0),
                () = delay(QUERY_DELAY) => {}
            }

            let cutoff = floor_sub_instant(now(), OLD_PEER_THRESHOLD);
            let nodes = self.table.get_oldest_nodes(cutoff, None);
            increment_saturating(&self.stats.inner.table_queries);
            increment_saturating_by(&self.stats.inner.selected, nodes.len());

            for (index, handle) in nodes.iter().enumerate() {
                let remaining = nodes.len() - index;
                let permit = tokio::select! {
                    biased;
                    () = &mut shutdown => return self.finish_shutdown(remaining),
                    () = self.input.closed() => return self.finish_input_closed(remaining),
                    permit = self.input.reserve() => match permit {
                        Ok(permit) => permit,
                        Err(_closed) => return self.finish_input_closed(remaining),
                    },
                };

                after_reserve(index, handle);
                if handle.dropped() {
                    increment_saturating(&self.stats.inner.dropped_skipped);
                    drop(permit);
                    continue;
                }

                let recent_cutoff = floor_sub_instant(now(), OLD_PEER_THRESHOLD);
                if handle
                    .last_responded_at()
                    .is_some_and(|responded| responded > recent_cutoff)
                {
                    increment_saturating(&self.stats.inner.recent_skipped);
                    drop(permit);
                    continue;
                }

                let node = handle.routing_node();
                permit.deliver(node);
                increment_saturating(&self.stats.inner.queued);
            }
        }
    }

    fn finish_shutdown(&self, selected_dropped: usize) -> DhtOldestNodePingProducerExit {
        increment_saturating_by(&self.stats.inner.shutdown_dropped, selected_dropped);
        DhtOldestNodePingProducerExit::Shutdown { selected_dropped }
    }

    fn finish_input_closed(&self, selected_dropped: usize) -> DhtOldestNodePingProducerExit {
        increment_saturating_by(&self.stats.inner.input_closed_dropped, selected_dropped);
        DhtOldestNodePingProducerExit::InputClosed { selected_dropped }
    }
}

/// Subtract with the oldest representable `Instant` as a non-panicking floor.
fn floor_sub_instant(now: Instant, duration: Duration) -> Instant {
    if let Some(result) = now.checked_sub(duration) {
        return result;
    }

    let mut valid_nanos = 0_u128;
    let mut invalid_nanos = duration.as_nanos();
    while valid_nanos + 1 < invalid_nanos {
        let candidate_nanos = valid_nanos + (invalid_nanos - valid_nanos) / 2;
        if now
            .checked_sub(duration_from_nanos(candidate_nanos))
            .is_some()
        {
            valid_nanos = candidate_nanos;
        } else {
            invalid_nanos = candidate_nanos;
        }
    }
    now.checked_sub(duration_from_nanos(valid_nanos))
        .unwrap_or(now)
}

fn duration_from_nanos(nanos: u128) -> Duration {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let seconds = u64::try_from(nanos / NANOS_PER_SECOND).unwrap_or(u64::MAX);
    let subsecond = u32::try_from(nanos % NANOS_PER_SECOND)
        .expect("a nanosecond remainder is below one second");
    Duration::new(seconds, subsecond)
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
mod tests {
    use std::collections::VecDeque;
    use std::future::{pending, poll_fn, ready};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    use tokio::sync::oneshot;

    use super::*;
    use crate::{Id20, KTableClock, KTableNodeOption, RoutingNode, RoutingPutResult};

    struct ScriptedClock {
        values: Mutex<VecDeque<Instant>>,
    }

    impl KTableClock for ScriptedClock {
        fn now(&self) -> Instant {
            self.values
                .lock()
                .expect("scripted clock lock poisoned")
                .pop_front()
                .expect("scripted clock exhausted")
        }
    }

    struct ScriptedDelay {
        ready: bool,
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

    fn one_tick_then_pending() -> impl FnMut(Duration) -> ScriptedDelay {
        let mut first = true;
        move |duration| {
            assert_eq!(duration, QUERY_DELAY);
            let ready = first;
            first = false;
            ScriptedDelay { ready }
        }
    }

    fn node(value: u8, port: u16) -> RoutingNode {
        let mut id = [0_u8; 20];
        id[19] = value;
        RoutingNode {
            id: Id20::from_slice(&id).unwrap(),
            addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, value), port)),
        }
    }

    fn with_port(node: RoutingNode, port: u16) -> RoutingNode {
        RoutingNode {
            addr: SocketAddr::new(node.addr.ip(), port),
            ..node
        }
    }

    fn put(table: &KTable, node: RoutingNode) {
        assert_eq!(table.put_node(node), RoutingPutResult::Accepted);
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

    fn assert_conservation(stats: DhtOldestNodePingProducerStats) {
        assert_eq!(
            stats.selected,
            stats
                .dropped_skipped
                .saturating_add(stats.recent_skipped)
                .saturating_add(stats.queued)
                .saturating_add(stats.input_closed_dropped)
                .saturating_add(stats.shutdown_dropped)
        );
    }

    #[test]
    fn constants_floor_and_public_handles_are_sound() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_eq!(OLD_PEER_THRESHOLD, Duration::from_secs(15 * 60));
        assert_eq!(QUERY_DELAY, Duration::from_secs(10));
        let now = Instant::now();
        assert_eq!(floor_sub_instant(now, Duration::ZERO), now);
        let floor = floor_sub_instant(now, Duration::MAX);
        assert_eq!(floor_sub_instant(floor, OLD_PEER_THRESHOLD), floor);
        assert_send_sync::<DhtOldestNodePingProducer>();
        assert_send_sync::<DhtOldestNodePingProducerStatsHandle>();
    }

    #[tokio::test]
    async fn ready_shutdown_wins_preclosed_input_before_delay_or_query() {
        let table = KTable::new(Id20::ZERO);
        put(&table, node(1, 1001));
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        receiver.close();
        let (producer, stats) = DhtOldestNodePingProducer::new(table, input);

        assert_eq!(
            producer
                .run_with(
                    ready(()),
                    || panic!("ready shutdown must prevent a clock read"),
                    |_| ready(()),
                    |_, _| {},
                )
                .await,
            DhtOldestNodePingProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_eq!(stats.snapshot(), DhtOldestNodePingProducerStats::default());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn preclosed_input_exits_before_delay_or_query() {
        let table = KTable::new(Id20::ZERO);
        put(&table, node(1, 1001));
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        receiver.close();
        let (producer, stats) = DhtOldestNodePingProducer::new(table, input);

        assert_eq!(
            producer
                .run_with(
                    pending(),
                    || panic!("ready input close must prevent a clock read"),
                    |_| ready(()),
                    |_, _| {},
                )
                .await,
            DhtOldestNodePingProducerExit::InputClosed {
                selected_dropped: 0
            }
        );
        assert_eq!(stats.snapshot(), DhtOldestNodePingProducerStats::default());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn first_query_waits_the_exact_ten_second_boundary() {
        let table = KTable::new(Id20::ZERO);
        let selected = node(1, 1001);
        put(&table, selected);
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtOldestNodePingProducer::new(table, input);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run(async move {
            let _ = shutdown_rx.await;
        });
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot(), DhtOldestNodePingProducerStats::default());
        tokio::time::advance(Duration::from_millis(9_999)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot(), DhtOldestNodePingProducerStats::default());
        tokio::time::advance(Duration::from_millis(1)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(selected));
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 1,
                queued: 1,
                ..DhtOldestNodePingProducerStats::default()
            }
        );

        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtOldestNodePingProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test(start_paused = true)]
    async fn input_close_during_the_leading_delay_queries_nothing() {
        let table = KTable::new(Id20::ZERO);
        put(&table, node(1, 1001));
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtOldestNodePingProducer::new(table, input);
        let run = producer.run(pending());
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        tokio::time::advance(Duration::from_secs(9)).await;
        poll_once_pending(run.as_mut()).await;
        receiver.close();
        assert_eq!(
            run.await,
            DhtOldestNodePingProducerExit::InputClosed {
                selected_dropped: 0
            }
        );
        assert_eq!(stats.snapshot(), DhtOldestNodePingProducerStats::default());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_poll_starts_a_fresh_delay_without_catch_up() {
        let table = KTable::new(Id20::ZERO);
        let selected = node(1, 1001);
        put(&table, selected);
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtOldestNodePingProducer::new(table, input);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run(async move {
            let _ = shutdown_rx.await;
        });
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        tokio::time::advance(Duration::from_secs(30)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(selected));
        assert_eq!(stats.snapshot().table_queries, 1);
        tokio::time::advance(Duration::from_millis(9_999)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot().table_queries, 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(selected));
        assert_eq!(stats.snapshot().table_queries, 2);

        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtOldestNodePingProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn query_is_uncapped_and_preserves_deterministic_table_tie_order() {
        let table = KTable::new(Id20::ZERO);
        for value in (1..=12).rev() {
            put(&table, node(value, 1000 + u16::from(value)));
        }
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(12);
        let (producer, stats) = DhtOldestNodePingProducer::new(table, input);
        let mut run = Box::pin(producer.run_with(
            pending(),
            Instant::now,
            one_tick_then_pending(),
            |_, _| {},
        ));

        poll_once_pending(run.as_mut()).await;
        for value in 1..=12 {
            assert_eq!(
                receiver.recv().await,
                Some(node(value, 1000 + u16::from(value)))
            );
        }
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 12,
                queued: 12,
                ..DhtOldestNodePingProducerStats::default()
            }
        );
        drop(run);
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn actual_query_uses_the_strict_fifteen_minute_cutoff() {
        let query_now = Instant::now()
            .checked_add(Duration::from_secs(20 * 60))
            .expect("twenty minutes fit the monotonic clock");
        let old = node(1, 1001);
        let at_cutoff = node(2, 1002);
        let recent = node(3, 1003);
        let clock = Arc::new(ScriptedClock {
            values: Mutex::new(VecDeque::from([
                query_now - OLD_PEER_THRESHOLD - Duration::from_nanos(1),
                query_now - OLD_PEER_THRESHOLD,
                query_now - OLD_PEER_THRESHOLD + Duration::from_nanos(1),
            ])),
        });
        let table = KTable::with_clock(Id20::ZERO, clock);
        for selected in [old, at_cutoff, recent] {
            assert_eq!(
                table.put_node_with_options(selected, &[KTableNodeOption::Responded]),
                RoutingPutResult::Accepted
            );
        }
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtOldestNodePingProducer::new(table, input);
        let mut times = VecDeque::from([query_now, query_now]);
        let mut run = Box::pin(producer.run_with(
            pending(),
            move || times.pop_front().expect("producer clock exhausted"),
            one_tick_then_pending(),
            |_, _| {},
        ));

        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(old));
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 1,
                queued: 1,
                ..DhtOldestNodePingProducerStats::default()
            }
        );
        drop(run);
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn post_reserve_rechecks_dropped_then_recent_and_snapshots_the_live_address() {
        let recheck_now = Instant::now()
            .checked_add(Duration::from_secs(20 * 60))
            .expect("twenty minutes fit the monotonic clock");
        let exact_response = recheck_now - OLD_PEER_THRESHOLD;
        let recent_response = exact_response + Duration::from_nanos(1);
        let clock = Arc::new(ScriptedClock {
            values: Mutex::new(VecDeque::from([exact_response, recent_response])),
        });
        let table = KTable::with_clock(Id20::ZERO, clock);
        let dropped = node(1, 1001);
        let exact = node(2, 1002);
        let recent = node(3, 1003);
        let stable = node(4, 1004);
        for selected in [dropped, exact, recent, stable] {
            put(&table, selected);
        }
        let exact_updated = with_port(exact, 2002);
        let recent_updated = with_port(recent, 2003);
        let stable_updated = with_port(stable, 2004);
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(2);
        let (producer, stats) = DhtOldestNodePingProducer::new(table.clone(), input);
        let table_for_hook = table.clone();
        let now_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let now_calls_for_run = Arc::clone(&now_calls);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            move || {
                now_calls_for_run.fetch_add(1, Ordering::Relaxed);
                recheck_now
            },
            one_tick_then_pending(),
            move |index, _handle| match index {
                0 => assert!(table_for_hook.drop_node(dropped.id)),
                1 => assert_eq!(
                    table_for_hook
                        .put_node_with_options(exact_updated, &[KTableNodeOption::Responded]),
                    RoutingPutResult::AlreadyExists
                ),
                2 => assert_eq!(
                    table_for_hook
                        .put_node_with_options(recent_updated, &[KTableNodeOption::Responded]),
                    RoutingPutResult::AlreadyExists
                ),
                3 => assert_eq!(
                    table_for_hook.put_node(stable_updated),
                    RoutingPutResult::AlreadyExists
                ),
                _ => unreachable!(),
            },
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(exact_updated));
        assert_eq!(receiver.recv().await, Some(stable_updated));
        assert_eq!(now_calls.load(Ordering::Relaxed), 4);
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 4,
                dropped_skipped: 1,
                recent_skipped: 1,
                queued: 2,
                ..DhtOldestNodePingProducerStats::default()
            }
        );

        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtOldestNodePingProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_conservation(stats.snapshot());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn mutation_while_capacity_is_blocked_is_observed_only_after_reservation() {
        let recheck_now = Instant::now()
            .checked_add(Duration::from_secs(20 * 60))
            .expect("twenty minutes fit the monotonic clock");
        let clock = Arc::new(ScriptedClock {
            values: Mutex::new(VecDeque::from([recheck_now])),
        });
        let table = KTable::with_clock(Id20::ZERO, clock);
        let original = node(1, 1001);
        let updated = with_port(original, 2001);
        put(&table, original);
        let sentinel = node(9, 9009);
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        input.send(sentinel).await.unwrap();
        let (producer, stats) = DhtOldestNodePingProducer::new(table.clone(), input);
        let mut times = VecDeque::from([recheck_now, recheck_now]);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            move || times.pop_front().expect("producer clock exhausted"),
            one_tick_then_pending(),
            |_, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 1,
                ..DhtOldestNodePingProducerStats::default()
            }
        );
        assert_eq!(
            table.put_node_with_options(updated, &[KTableNodeOption::Responded]),
            RoutingPutResult::AlreadyExists
        );
        assert_eq!(receiver.recv().await, Some(sentinel));
        poll_once_pending(run.as_mut()).await;
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 1,
                recent_skipped: 1,
                ..DhtOldestNodePingProducerStats::default()
            }
        );

        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtOldestNodePingProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_conservation(stats.snapshot());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn address_mutation_while_capacity_is_blocked_is_snapshotted_on_commit() {
        let table = KTable::new(Id20::ZERO);
        let original = node(1, 1001);
        let updated = with_port(original, 2001);
        put(&table, original);
        let sentinel = node(9, 9009);
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        input.send(sentinel).await.unwrap();
        let (producer, stats) = DhtOldestNodePingProducer::new(table.clone(), input);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            Instant::now,
            one_tick_then_pending(),
            |_, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(table.put_node(updated), RoutingPutResult::AlreadyExists);
        assert_eq!(receiver.recv().await, Some(sentinel));
        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(updated));
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 1,
                queued: 1,
                ..DhtOldestNodePingProducerStats::default()
            }
        );

        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtOldestNodePingProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_conservation(stats.snapshot());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn close_after_reserve_keeps_commit_authority_then_exits_input_closed() {
        let table = KTable::new(Id20::ZERO);
        let selected = node(1, 1001);
        put(&table, selected);
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtOldestNodePingProducer::new(table, input);

        let exit = producer
            .run_with(pending(), Instant::now, one_tick_then_pending(), |_, _| {
                receiver.close()
            })
            .await;
        assert_eq!(
            exit,
            DhtOldestNodePingProducerExit::InputClosed {
                selected_dropped: 0
            }
        );
        assert_eq!(receiver.recv().await, Some(selected));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 1,
                queued: 1,
                ..DhtOldestNodePingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn input_close_preserves_queued_prefix_and_accounts_selected_suffix() {
        let table = KTable::new(Id20::ZERO);
        let selected = [node(1, 1001), node(2, 1002), node(3, 1003)];
        for node in selected {
            put(&table, node);
        }
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtOldestNodePingProducer::new(table, input);
        let run = producer.run_with(pending(), Instant::now, one_tick_then_pending(), |_, _| {});
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        receiver.close();
        assert_eq!(
            run.await,
            DhtOldestNodePingProducerExit::InputClosed {
                selected_dropped: 2
            }
        );
        assert_eq!(receiver.recv().await, Some(selected[0]));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 3,
                queued: 1,
                input_closed_dropped: 2,
                ..DhtOldestNodePingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn tied_shutdown_wins_new_capacity_and_accounts_selected_suffix() {
        let table = KTable::new(Id20::ZERO);
        let selected = [node(1, 1001), node(2, 1002), node(3, 1003)];
        for node in selected {
            put(&table, node);
        }
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        let (producer, stats) = DhtOldestNodePingProducer::new(table, input);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            Instant::now,
            one_tick_then_pending(),
            |_, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        shutdown_tx.send(()).unwrap();
        assert_eq!(receiver.recv().await, Some(selected[0]));
        receiver.close();
        assert_eq!(
            run.await,
            DhtOldestNodePingProducerExit::Shutdown {
                selected_dropped: 2
            }
        );
        assert_eq!(receiver.recv().await, None);
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 3,
                queued: 1,
                shutdown_dropped: 2,
                ..DhtOldestNodePingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn capacity_two_oracle_prefix_then_shutdown_drops_c_and_d() {
        let table = KTable::new(Id20::ZERO);
        let selected = [node(1, 1001), node(2, 1002), node(3, 1003), node(4, 1004)];
        for node in selected {
            put(&table, node);
        }
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(2);
        let (producer, stats) = DhtOldestNodePingProducer::new(table, input);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            Instant::now,
            one_tick_then_pending(),
            |_, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 4,
                queued: 2,
                ..DhtOldestNodePingProducerStats::default()
            }
        );
        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtOldestNodePingProducerExit::Shutdown {
                selected_dropped: 2
            }
        );
        assert_eq!(receiver.recv().await, Some(selected[0]));
        assert_eq!(receiver.recv().await, Some(selected[1]));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 4,
                queued: 2,
                shutdown_dropped: 2,
                ..DhtOldestNodePingProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn dropping_run_blocked_on_capacity_has_no_terminal_accounting_and_releases_eof() {
        let table = KTable::new(Id20::ZERO);
        put(&table, node(1, 1001));
        let sentinel = node(9, 9009);
        let (input, mut receiver) = DhtDiscoveredNodePingInput::test_channel(1);
        input.send(sentinel).await.unwrap();
        let (producer, stats) = DhtOldestNodePingProducer::new(table, input);
        let mut run = Box::pin(producer.run_with(
            pending(),
            Instant::now,
            one_tick_then_pending(),
            |_, _| {},
        ));

        poll_once_pending(run.as_mut()).await;
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 1,
                ..DhtOldestNodePingProducerStats::default()
            }
        );
        drop(run);
        assert_eq!(receiver.recv().await, Some(sentinel));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: 1,
                selected: 1,
                ..DhtOldestNodePingProducerStats::default()
            }
        );
    }

    #[test]
    fn all_counter_updates_saturate() {
        let stats = DhtOldestNodePingProducerStatsHandle::default();
        for counter in [
            &stats.inner.table_queries,
            &stats.inner.selected,
            &stats.inner.dropped_skipped,
            &stats.inner.recent_skipped,
            &stats.inner.queued,
            &stats.inner.input_closed_dropped,
            &stats.inner.shutdown_dropped,
        ] {
            counter.store(u64::MAX, Ordering::Relaxed);
            increment_saturating(counter);
            increment_saturating_by(counter, usize::MAX);
        }
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodePingProducerStats {
                table_queries: u64::MAX,
                selected: u64::MAX,
                dropped_skipped: u64::MAX,
                recent_skipped: u64::MAX,
                queued: u64::MAX,
                input_closed_dropped: u64::MAX,
                shutdown_dropped: u64::MAX,
            }
        );
    }
}
