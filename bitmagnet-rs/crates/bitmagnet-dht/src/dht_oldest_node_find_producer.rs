use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{DhtDiscoveredNodeFindInput, KTable, KTableNodeHandle};

const OLDEST_AGE: Duration = Duration::from_secs(5);
const OLDEST_LIMIT: NonZeroUsize = NonZeroUsize::new(10).unwrap();
const QUERY_DELAY: Duration = Duration::from_secs(1);

/// Terminal state of the owned periodic oldest-node `find_node` producer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtOldestNodeFindProducerExit {
    /// Caller shutdown won. The count is the selected suffix not queued.
    Shutdown { selected_dropped: usize },
    /// The shared find route closed. The count is the selected suffix not queued.
    InputClosed { selected_dropped: usize },
}

#[derive(Default)]
struct DhtOldestNodeFindProducerStatsInner {
    table_queries: AtomicU64,
    selected: AtomicU64,
    queued: AtomicU64,
    input_closed_dropped: AtomicU64,
    shutdown_dropped: AtomicU64,
}

/// Cloneable, sender-free view of periodic oldest-node producer counters.
#[derive(Clone, Default)]
pub struct DhtOldestNodeFindProducerStatsHandle {
    inner: Arc<DhtOldestNodeFindProducerStatsInner>,
}

/// One non-transactional snapshot of monotonic oldest-node producer counters.
///
/// After normal exit, `selected` equals the saturating sum of `queued`,
/// `input_closed_dropped`, and `shutdown_dropped`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtOldestNodeFindProducerStats {
    /// Completed synchronous KTable oldest-node queries.
    pub table_queries: u64,
    /// Node occurrences returned by those queries.
    pub selected: u64,
    /// Selected occurrences committed to the shared find-route queue.
    pub queued: u64,
    /// Selected suffix occurrences abandoned because the route closed.
    pub input_closed_dropped: u64,
    /// Selected suffix occurrences abandoned because caller shutdown won.
    pub shutdown_dropped: u64,
}

impl DhtOldestNodeFindProducerStatsHandle {
    /// Read each saturating counter independently with relaxed ordering.
    ///
    /// Cross-field conservation is guaranteed only after normal producer exit.
    #[must_use]
    pub fn snapshot(&self) -> DhtOldestNodeFindProducerStats {
        DhtOldestNodeFindProducerStats {
            table_queries: self.inner.table_queries.load(Ordering::Relaxed),
            selected: self.inner.selected.load(Ordering::Relaxed),
            queued: self.inner.queued.load(Ordering::Relaxed),
            input_closed_dropped: self.inner.input_closed_dropped.load(Ordering::Relaxed),
            shutdown_dropped: self.inner.shutdown_dropped.load(Ordering::Relaxed),
        }
    }
}

/// Owned periodic producer of KTable oldest nodes for the shared find route.
///
/// This producer sends directly through the scheduler's shared find capacity;
/// it does not re-enter scheduler batching, filtering, routing, or counters.
/// Its live sender delays find-route EOF until this value or its `run` future is
/// dropped or exits.
///
/// The first table query is immediate unless pre-ready shutdown or input close
/// wins first. Ready-event ties are explicitly biased: shutdown wins every
/// tie; before a query or snapshot, input closure wins the remaining tie.
/// During a blocked send, shutdown also wins a tie with newly available
/// capacity or input closure. Selected handles are processed sequentially in
/// KTable return order. Retained selected handles are not rechecked for table
/// membership, eligibility, or recentness. Each live handle is projected
/// separately immediately before constructing that item's capacity-waiting
/// send future, so later handle changes cannot alter an already pending
/// immutable snapshot. A fresh, cancellation-aware delay starts only after the
/// complete batch is queued and never catches up missed periods. This type
/// spawns no task.
pub struct DhtOldestNodeFindProducer {
    table: KTable,
    input: DhtDiscoveredNodeFindInput,
    stats: DhtOldestNodeFindProducerStatsHandle,
}

impl DhtOldestNodeFindProducer {
    /// Construct the fixed five-second, ten-node, one-second producer.
    #[must_use]
    pub fn new(
        table: KTable,
        input: DhtDiscoveredNodeFindInput,
    ) -> (Self, DhtOldestNodeFindProducerStatsHandle) {
        let stats = DhtOldestNodeFindProducerStatsHandle::default();
        (
            Self {
                table,
                input,
                stats: stats.clone(),
            },
            stats,
        )
    }

    /// Run until caller shutdown or closure of the shared find route.
    ///
    /// On normal terminal return, `selected` equals the saturating sum of
    /// `queued`, `input_closed_dropped`, and `shutdown_dropped`. Dropping this
    /// future is not a terminal return and carries no cross-counter promise.
    pub async fn run<F>(self, shutdown: F) -> DhtOldestNodeFindProducerExit
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
        mut before_snapshot: B,
    ) -> DhtOldestNodeFindProducerExit
    where
        F: Future<Output = ()>,
        N: FnMut() -> Instant,
        D: FnMut(Duration) -> DF,
        DF: Future<Output = ()>,
        B: FnMut(usize, &KTableNodeHandle),
    {
        tokio::pin!(shutdown);

        loop {
            let nodes = tokio::select! {
                biased;
                () = &mut shutdown => return self.finish_shutdown(0),
                () = self.input.closed() => return self.finish_input_closed(0),
                nodes = async {
                    let cutoff = floor_sub_instant(now(), OLDEST_AGE);
                    self.table.get_oldest_nodes(cutoff, Some(OLDEST_LIMIT))
                } => nodes,
            };
            increment_saturating(&self.stats.inner.table_queries);
            increment_saturating_by(&self.stats.inner.selected, nodes.len());

            for (index, handle) in nodes.iter().enumerate() {
                let remaining = nodes.len() - index;
                let node = tokio::select! {
                    biased;
                    () = &mut shutdown => return self.finish_shutdown(remaining),
                    () = self.input.closed() => return self.finish_input_closed(remaining),
                    node = async {
                        before_snapshot(index, handle);
                        handle.routing_node()
                    } => node,
                };
                let sent = tokio::select! {
                    biased;
                    () = &mut shutdown => return self.finish_shutdown(remaining),
                    sent = self.input.send(node) => sent,
                };
                if sent.is_err() {
                    return self.finish_input_closed(remaining);
                }
                increment_saturating(&self.stats.inner.queued);
            }

            tokio::select! {
                biased;
                () = &mut shutdown => return self.finish_shutdown(0),
                () = self.input.closed() => return self.finish_input_closed(0),
                () = delay(QUERY_DELAY) => {}
            }
        }
    }

    fn finish_shutdown(&self, selected_dropped: usize) -> DhtOldestNodeFindProducerExit {
        increment_saturating_by(&self.stats.inner.shutdown_dropped, selected_dropped);
        DhtOldestNodeFindProducerExit::Shutdown { selected_dropped }
    }

    fn finish_input_closed(&self, selected_dropped: usize) -> DhtOldestNodeFindProducerExit {
        increment_saturating_by(&self.stats.inner.input_closed_dropped, selected_dropped);
        DhtOldestNodeFindProducerExit::InputClosed { selected_dropped }
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
    use std::future::{pending, ready};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::sync::Mutex;

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
                .unwrap()
                .pop_front()
                .expect("scripted clock exhausted")
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
            addr: SocketAddr::V4(SocketAddrV4::new(
                match node.addr.ip() {
                    std::net::IpAddr::V4(ip) => ip,
                    std::net::IpAddr::V6(_) => unreachable!(),
                },
                port,
            )),
            ..node
        }
    }

    fn put(table: &KTable, node: RoutingNode) {
        assert_eq!(table.put_node(node), RoutingPutResult::Accepted);
    }

    async fn poll_once_pending<F>(mut future: std::pin::Pin<&mut F>)
    where
        F: Future,
    {
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }

    fn assert_conservation(stats: DhtOldestNodeFindProducerStats) {
        assert_eq!(
            stats.selected,
            stats
                .queued
                .saturating_add(stats.input_closed_dropped)
                .saturating_add(stats.shutdown_dropped)
        );
    }

    #[test]
    fn constants_and_monotonic_floor_are_nonpanicking() {
        assert_eq!(OLDEST_AGE, Duration::from_secs(5));
        assert_eq!(OLDEST_LIMIT, NonZeroUsize::new(10).unwrap());
        assert_eq!(QUERY_DELAY, Duration::from_secs(1));

        let now = Instant::now();
        assert_eq!(floor_sub_instant(now, Duration::ZERO), now);
        let floor = floor_sub_instant(now, Duration::MAX);
        assert_eq!(floor_sub_instant(floor, OLDEST_AGE), floor);
    }

    #[tokio::test]
    async fn ready_shutdown_queries_nothing_and_releases_the_sender() {
        let table = KTable::new(Id20::ZERO);
        put(&table, node(1, 1001));
        let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(1);
        let (producer, stats) = DhtOldestNodeFindProducer::new(table, input);

        assert_eq!(
            producer.run(ready(())).await,
            DhtOldestNodeFindProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_eq!(stats.snapshot(), DhtOldestNodeFindProducerStats::default());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn preclosed_input_queries_nothing_and_releases_the_sender() {
        let table = KTable::new(Id20::ZERO);
        put(&table, node(1, 1001));
        let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(1);
        receiver.close();
        let (producer, stats) = DhtOldestNodeFindProducer::new(table, input);

        assert_eq!(
            producer.run(pending()).await,
            DhtOldestNodeFindProducerExit::InputClosed {
                selected_dropped: 0
            }
        );
        assert_eq!(stats.snapshot(), DhtOldestNodeFindProducerStats::default());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn ready_shutdown_wins_preclosed_input_without_querying() {
        let table = KTable::new(Id20::ZERO);
        put(&table, node(1, 1001));
        let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(1);
        receiver.close();
        let (producer, stats) = DhtOldestNodeFindProducer::new(table, input);

        assert_eq!(
            producer.run(ready(())).await,
            DhtOldestNodeFindProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_eq!(stats.snapshot(), DhtOldestNodeFindProducerStats::default());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn empty_table_observes_input_close_during_fresh_delay() {
        let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(1);
        let (producer, stats) = DhtOldestNodeFindProducer::new(KTable::new(Id20::ZERO), input);
        let run = producer.run(pending());
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodeFindProducerStats {
                table_queries: 1,
                ..DhtOldestNodeFindProducerStats::default()
            }
        );
        receiver.close();
        assert_eq!(
            run.await,
            DhtOldestNodeFindProducerExit::InputClosed {
                selected_dropped: 0
            }
        );
        assert_conservation(stats.snapshot());
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn input_close_preserves_prefix_and_accounts_current_suffix() {
        let table = KTable::new(Id20::ZERO);
        let first = node(1, 1001);
        let second = node(2, 1002);
        let third = node(3, 1003);
        for selected in [first, second, third] {
            put(&table, selected);
        }
        let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(1);
        let (producer, stats) = DhtOldestNodeFindProducer::new(table, input);
        let run = producer.run(pending());
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodeFindProducerStats {
                table_queries: 1,
                selected: 3,
                queued: 1,
                ..DhtOldestNodeFindProducerStats::default()
            }
        );
        receiver.close();
        assert_eq!(
            run.await,
            DhtOldestNodeFindProducerExit::InputClosed {
                selected_dropped: 2
            }
        );
        assert_eq!(receiver.recv().await, Some(first));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodeFindProducerStats {
                table_queries: 1,
                selected: 3,
                queued: 1,
                input_closed_dropped: 2,
                shutdown_dropped: 0,
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn tied_shutdown_preserves_prefix_and_accounts_selected_suffix() {
        let table = KTable::new(Id20::ZERO);
        let first = node(1, 1001);
        for selected in [first, node(2, 1002), node(3, 1003)] {
            put(&table, selected);
        }
        let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(1);
        let (producer, stats) = DhtOldestNodeFindProducer::new(table, input);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run(async move {
            let _ = shutdown_rx.await;
        });
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        shutdown_tx.send(()).unwrap();
        assert_eq!(receiver.recv().await, Some(first));
        assert_eq!(
            run.await,
            DhtOldestNodeFindProducerExit::Shutdown {
                selected_dropped: 2
            }
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ));
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodeFindProducerStats {
                table_queries: 1,
                selected: 3,
                queued: 1,
                input_closed_dropped: 0,
                shutdown_dropped: 2,
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
        let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(2);
        let (producer, stats) = DhtOldestNodeFindProducer::new(table, input);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run(async move {
            let _ = shutdown_rx.await;
        });
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodeFindProducerStats {
                table_queries: 1,
                selected: 4,
                queued: 2,
                ..DhtOldestNodeFindProducerStats::default()
            }
        );
        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtOldestNodeFindProducerExit::Shutdown {
                selected_dropped: 2
            }
        );
        assert_eq!(receiver.recv().await, Some(selected[0]));
        assert_eq!(receiver.recv().await, Some(selected[1]));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodeFindProducerStats {
                table_queries: 1,
                selected: 4,
                queued: 2,
                input_closed_dropped: 0,
                shutdown_dropped: 2,
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn first_query_is_immediate_and_selects_at_most_ten_in_table_order() {
        let table = KTable::new(Id20::ZERO);
        for value in 1..=12 {
            put(&table, node(value, 1000 + u16::from(value)));
        }
        let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(10);
        let (producer, stats) = DhtOldestNodeFindProducer::new(table, input);
        let mut run =
            Box::pin(producer.run_with(pending(), Instant::now, |_| pending::<()>(), |_, _| {}));

        poll_once_pending(run.as_mut()).await;
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodeFindProducerStats {
                table_queries: 1,
                selected: 10,
                queued: 10,
                ..DhtOldestNodeFindProducerStats::default()
            }
        );
        for value in 1..=10 {
            assert_eq!(
                receiver.recv().await,
                Some(node(value, 1000 + u16::from(value)))
            );
        }
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        drop(run);
        assert_eq!(receiver.recv().await, None);
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn actual_table_selection_uses_the_strict_five_second_cutoff() {
        let query_now = Instant::now()
            .checked_add(Duration::from_secs(10))
            .expect("ten seconds fit the monotonic clock");
        let clock = Arc::new(ScriptedClock {
            values: Mutex::new(VecDeque::from([
                query_now - OLDEST_AGE - Duration::from_nanos(1),
                query_now - OLDEST_AGE,
                query_now - Duration::from_secs(4),
            ])),
        });
        let table = KTable::with_clock(Id20::ZERO, clock);
        let old = node(1, 1001);
        let at_cutoff = node(2, 1002);
        let recent = node(3, 1003);
        for selected in [old, at_cutoff, recent] {
            assert_eq!(
                table.put_node_with_options(selected, &[KTableNodeOption::Responded]),
                RoutingPutResult::Accepted
            );
        }
        let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(1);
        let (producer, stats) = DhtOldestNodeFindProducer::new(table, input);
        let mut run = Box::pin(producer.run_with(
            pending(),
            move || query_now,
            |_| pending::<()>(),
            |_, _| {},
        ));

        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(old));
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodeFindProducerStats {
                table_queries: 1,
                selected: 1,
                queued: 1,
                ..DhtOldestNodeFindProducerStats::default()
            }
        );
        drop(run);
        assert_eq!(receiver.recv().await, None);
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn snapshot_is_per_item_immediately_before_its_send_attempt() {
        let table = KTable::new(Id20::ZERO);
        let first_old = node(1, 1001);
        let second_old = node(2, 1002);
        let first_new = with_port(first_old, 2001);
        let second_new = with_port(second_old, 2002);
        put(&table, first_old);
        put(&table, second_old);
        let sentinel = node(9, 9009);
        let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(1);
        input.send(sentinel).await.unwrap();
        let (producer, stats) = DhtOldestNodeFindProducer::new(table.clone(), input);
        let table_for_hook = table.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            Instant::now,
            |_| pending::<()>(),
            move |index, _| {
                if index == 1 {
                    assert_eq!(
                        table_for_hook.put_node(second_new),
                        RoutingPutResult::AlreadyExists
                    );
                }
            },
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(table.put_node(first_new), RoutingPutResult::AlreadyExists);
        assert_eq!(receiver.recv().await, Some(sentinel));
        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(first_old));
        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(second_new));

        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtOldestNodeFindProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodeFindProducerStats {
                table_queries: 1,
                selected: 2,
                queued: 2,
                ..DhtOldestNodeFindProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test(start_paused = true)]
    async fn query_delay_waits_the_full_one_second_boundary() {
        let table = KTable::new(Id20::ZERO);
        let selected = node(1, 1001);
        put(&table, selected);
        let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(1);
        let (producer, stats) = DhtOldestNodeFindProducer::new(table, input);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run(async move {
            let _ = shutdown_rx.await;
        });
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(selected));
        assert_eq!(stats.snapshot().table_queries, 1);
        tokio::time::advance(Duration::from_millis(999)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot().table_queries, 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(selected));
        assert_eq!(stats.snapshot().table_queries, 2);

        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtOldestNodeFindProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_poll_starts_a_fresh_delay_without_catch_up() {
        let table = KTable::new(Id20::ZERO);
        let selected = node(1, 1001);
        put(&table, selected);
        let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(2);
        let (producer, stats) = DhtOldestNodeFindProducer::new(table, input);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run(async move {
            let _ = shutdown_rx.await;
        });
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(selected));
        assert_eq!(stats.snapshot().table_queries, 1);
        tokio::time::advance(Duration::from_secs(5)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(receiver.recv().await, Some(selected));
        assert_eq!(stats.snapshot().table_queries, 2);
        tokio::time::advance(Duration::from_millis(999)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot().table_queries, 2);

        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtOldestNodeFindProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn dropping_run_blocked_on_selected_send_has_no_terminal_accounting() {
        let table = KTable::new(Id20::ZERO);
        put(&table, node(1, 1001));
        let (input, mut receiver) = DhtDiscoveredNodeFindInput::test_channel(1);
        let sentinel = node(9, 9009);
        input.send(sentinel).await.unwrap();
        let (producer, stats) = DhtOldestNodeFindProducer::new(table, input);
        let mut run = Box::pin(producer.run(pending()));
        poll_once_pending(run.as_mut()).await;
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodeFindProducerStats {
                table_queries: 1,
                selected: 1,
                ..DhtOldestNodeFindProducerStats::default()
            }
        );
        drop(run);
        assert_eq!(receiver.recv().await, Some(sentinel));
        assert_eq!(receiver.recv().await, None);
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodeFindProducerStats {
                table_queries: 1,
                selected: 1,
                ..DhtOldestNodeFindProducerStats::default()
            }
        );
    }

    #[test]
    fn counter_updates_saturate() {
        let stats = DhtOldestNodeFindProducerStatsHandle::default();
        stats.inner.table_queries.store(u64::MAX, Ordering::Relaxed);
        stats.inner.selected.store(u64::MAX, Ordering::Relaxed);
        stats.inner.queued.store(u64::MAX, Ordering::Relaxed);
        stats
            .inner
            .input_closed_dropped
            .store(u64::MAX, Ordering::Relaxed);
        stats
            .inner
            .shutdown_dropped
            .store(u64::MAX, Ordering::Relaxed);
        increment_saturating(&stats.inner.table_queries);
        increment_saturating_by(&stats.inner.selected, usize::MAX);
        increment_saturating(&stats.inner.queued);
        increment_saturating_by(&stats.inner.input_closed_dropped, usize::MAX);
        increment_saturating_by(&stats.inner.shutdown_dropped, usize::MAX);
        assert_eq!(
            stats.snapshot(),
            DhtOldestNodeFindProducerStats {
                table_queries: u64::MAX,
                selected: u64::MAX,
                queued: u64::MAX,
                input_closed_dropped: u64::MAX,
                shutdown_dropped: u64::MAX,
            }
        );
    }
}
