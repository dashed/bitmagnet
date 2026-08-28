use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::{DhtDiscoveredNodeSampleInfoHashesInput, KTable, KTableNodeHandle};

const QUERY_LIMIT: NonZeroUsize = NonZeroUsize::new(60).unwrap();
const ROUND_DELAY: Duration = Duration::from_secs(1);

/// Terminal state of the owned periodic sample-infohashes candidate producer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtSampleInfoHashesProducerExit {
    /// Caller shutdown won. The count is the selected suffix not committed.
    Shutdown { selected_dropped: usize },
    /// The shared sample route closed. The count is the selected suffix not
    /// committed.
    InputClosed { selected_dropped: usize },
}

#[derive(Default)]
struct DhtSampleInfoHashesProducerStatsInner {
    table_queries: AtomicU64,
    selected: AtomicU64,
    queued: AtomicU64,
    input_closed_dropped: AtomicU64,
    shutdown_dropped: AtomicU64,
}

/// Cloneable, sender-free view of sample-infohashes producer counters.
#[derive(Clone, Default)]
pub struct DhtSampleInfoHashesProducerStatsHandle {
    inner: Arc<DhtSampleInfoHashesProducerStatsInner>,
}

/// One non-transactional snapshot of monotonic sample producer counters.
///
/// After normal exit, `selected` equals the saturating sum of `queued`,
/// `input_closed_dropped`, and `shutdown_dropped`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtSampleInfoHashesProducerStats {
    /// Completed synchronous KTable candidate queries.
    pub table_queries: u64,
    /// Candidate occurrences returned by completed queries.
    pub selected: u64,
    /// Retained handles committed to the shared sample route.
    pub queued: u64,
    /// Selected suffix occurrences abandoned because the route closed.
    pub input_closed_dropped: u64,
    /// Selected suffix occurrences abandoned because caller shutdown won.
    pub shutdown_dropped: u64,
}

impl DhtSampleInfoHashesProducerStatsHandle {
    /// Read each saturating counter independently with relaxed ordering.
    ///
    /// Cross-field conservation is guaranteed only after normal producer exit.
    #[must_use]
    pub fn snapshot(&self) -> DhtSampleInfoHashesProducerStats {
        DhtSampleInfoHashesProducerStats {
            table_queries: self.inner.table_queries.load(Ordering::Relaxed),
            selected: self.inner.selected.load(Ordering::Relaxed),
            queued: self.inner.queued.load(Ordering::Relaxed),
            input_closed_dropped: self.inner.input_closed_dropped.load(Ordering::Relaxed),
            shutdown_dropped: self.inner.shutdown_dropped.load(Ordering::Relaxed),
        }
    }
}

/// Owned periodic producer of retained KTable sample-infohashes candidates.
///
/// The first limit-sixty query is immediate. Returned generation-specific
/// handles follow the KTable's deterministic ID order and are sent sequentially
/// through the scheduler's existing shared sample route without projection or
/// a second eligibility check. The route's internal work tag retains their
/// producer provenance. A completed round is followed by one fresh,
/// cancellation-aware one-second delay; missed periods never catch up.
///
/// Shutdown wins every ready tie and route closure wins each remaining tie
/// before a query, send, or delay. A successful send is the irrevocable commit
/// boundary. A terminal condition ready before the first query therefore
/// produces no query or selection, a deliberate Rust lifecycle hardening over
/// eager select-operand evaluation. This producer owns and spawns no task.
#[must_use = "the producer must be run to feed the shared sample route"]
pub struct DhtSampleInfoHashesProducer {
    table: KTable,
    input: DhtDiscoveredNodeSampleInfoHashesInput,
    stats: DhtSampleInfoHashesProducerStatsHandle,
}

impl DhtSampleInfoHashesProducer {
    /// Construct the fixed immediate, limit-sixty, one-second producer.
    pub fn new(
        table: KTable,
        input: DhtDiscoveredNodeSampleInfoHashesInput,
    ) -> (Self, DhtSampleInfoHashesProducerStatsHandle) {
        let stats = DhtSampleInfoHashesProducerStatsHandle::default();
        (
            Self {
                table,
                input,
                stats: stats.clone(),
            },
            stats,
        )
    }

    /// Run until caller shutdown or closure of the shared sample route.
    ///
    /// On normal terminal return, `selected` equals the saturating sum of
    /// `queued`, `input_closed_dropped`, and `shutdown_dropped`. Dropping this
    /// future is not a terminal return and carries no cross-counter promise.
    pub async fn run<F>(self, shutdown: F) -> DhtSampleInfoHashesProducerExit
    where
        F: Future<Output = ()>,
    {
        self.run_with(shutdown, tokio::time::sleep, |_, _| {}).await
    }

    async fn run_with<F, D, DF, B>(
        self,
        shutdown: F,
        mut delay: D,
        mut before_send: B,
    ) -> DhtSampleInfoHashesProducerExit
    where
        F: Future<Output = ()>,
        D: FnMut(Duration) -> DF,
        DF: Future<Output = ()>,
        B: FnMut(usize, &KTableNodeHandle),
    {
        tokio::pin!(shutdown);

        loop {
            let handles = tokio::select! {
                biased;
                () = &mut shutdown => return self.finish_shutdown(0),
                () = self.input.closed() => return self.finish_input_closed(0),
                handles = async {
                    self.table.get_nodes_for_sample_infohashes(QUERY_LIMIT)
                } => handles,
            };
            increment_saturating(&self.stats.inner.table_queries);
            increment_saturating_by(&self.stats.inner.selected, handles.len());
            let selected = handles.len();

            for (index, handle) in handles.into_iter().enumerate() {
                let remaining = selected - index;
                let sent = tokio::select! {
                    biased;
                    () = &mut shutdown => return self.finish_shutdown(remaining),
                    () = self.input.closed() => return self.finish_input_closed(remaining),
                    sent = async {
                        before_send(index, &handle);
                        self.input.send(handle).await
                    } => sent,
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
                () = delay(ROUND_DELAY) => {}
            }
        }
    }

    fn finish_shutdown(&self, selected_dropped: usize) -> DhtSampleInfoHashesProducerExit {
        increment_saturating_by(&self.stats.inner.shutdown_dropped, selected_dropped);
        DhtSampleInfoHashesProducerExit::Shutdown { selected_dropped }
    }

    fn finish_input_closed(&self, selected_dropped: usize) -> DhtSampleInfoHashesProducerExit {
        increment_saturating_by(&self.stats.inner.input_closed_dropped, selected_dropped);
        DhtSampleInfoHashesProducerExit::InputClosed { selected_dropped }
    }
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
#[path = "dht_sample_infohashes_producer_parity.rs"]
mod parity_tests;

#[cfg(test)]
mod tests {
    use std::future::{pending, poll_fn, ready};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    use tokio::sync::oneshot;

    use super::*;
    use crate::dht_discovered_node_scheduler::DhtDiscoveredNodeSampleInfoHashesWork;
    use crate::{
        DhtDiscoveredNodeSampleInfoHashesInput, Id20, KTableNodeHandle, RoutingNode,
        RoutingPutResult,
    };

    struct PendingDelay {
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    impl Future for PendingDelay {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::Relaxed);
            Poll::Pending
        }
    }

    impl Drop for PendingDelay {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
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

    fn retained(table: &KTable, node: RoutingNode) -> KTableNodeHandle {
        assert_eq!(table.put_node(node), RoutingPutResult::Accepted);
        table.node_handle(node.id).unwrap()
    }

    fn assert_retained(work: DhtDiscoveredNodeSampleInfoHashesWork, expected: &KTableNodeHandle) {
        match work {
            DhtDiscoveredNodeSampleInfoHashesWork::Retained(actual) => {
                assert_eq!(&actual, expected);
            }
            DhtDiscoveredNodeSampleInfoHashesWork::Discovered(node) => {
                panic!("sample producer projected a retained handle as {node:?}");
            }
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

    fn assert_conservation(stats: DhtSampleInfoHashesProducerStats) {
        assert_eq!(
            stats.selected,
            stats
                .queued
                .saturating_add(stats.input_closed_dropped)
                .saturating_add(stats.shutdown_dropped)
        );
    }

    #[test]
    fn constants_public_handles_and_run_future_are_send() {
        fn assert_send_sync<T: Send + Sync>() {}
        fn assert_send<T: Send>(_value: T) {}

        assert_eq!(QUERY_LIMIT.get(), 60);
        assert_eq!(ROUND_DELAY, Duration::from_secs(1));
        assert_send_sync::<DhtSampleInfoHashesProducer>();
        assert_send_sync::<DhtSampleInfoHashesProducerStatsHandle>();
        assert_send_sync::<DhtSampleInfoHashesProducerStats>();
        assert_send_sync::<DhtSampleInfoHashesProducerExit>();

        let (input, _receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        let (producer, _stats) = DhtSampleInfoHashesProducer::new(KTable::new(Id20::ZERO), input);
        assert_send(producer.run(pending()));
    }

    #[test]
    fn every_counter_saturates() {
        let stats = DhtSampleInfoHashesProducerStatsHandle::default();
        for counter in [
            &stats.inner.table_queries,
            &stats.inner.selected,
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
            DhtSampleInfoHashesProducerStats {
                table_queries: u64::MAX,
                selected: u64::MAX,
                queued: u64::MAX,
                input_closed_dropped: u64::MAX,
                shutdown_dropped: u64::MAX,
            }
        );
    }

    #[tokio::test]
    async fn ready_shutdown_wins_preclosed_input_before_query() {
        let table = KTable::new(Id20::ZERO);
        retained(&table, node(1, 1001));
        let (input, mut receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        receiver.close();
        let (producer, stats) = DhtSampleInfoHashesProducer::new(table, input);

        assert_eq!(
            producer
                .run_with(
                    ready(()),
                    |_| ready(()),
                    |_, _| panic!("ready shutdown reached send"),
                )
                .await,
            DhtSampleInfoHashesProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_eq!(
            stats.snapshot(),
            DhtSampleInfoHashesProducerStats::default()
        );
        assert!(receiver.recv_work().await.is_none());
    }

    #[tokio::test]
    async fn preclosed_input_exits_before_query() {
        let table = KTable::new(Id20::ZERO);
        retained(&table, node(1, 1001));
        let (input, mut receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        receiver.close();
        let (producer, stats) = DhtSampleInfoHashesProducer::new(table, input);

        assert_eq!(
            producer
                .run_with(
                    pending(),
                    |_| ready(()),
                    |_, _| panic!("preclosed input reached send"),
                )
                .await,
            DhtSampleInfoHashesProducerExit::InputClosed {
                selected_dropped: 0
            }
        );
        assert_eq!(
            stats.snapshot(),
            DhtSampleInfoHashesProducerStats::default()
        );
        assert!(receiver.recv_work().await.is_none());
    }

    #[tokio::test]
    async fn capacity_two_preserves_exact_retained_prefix_and_shutdown_suffix() {
        let table = KTable::new(Id20::ZERO);
        let handles = (1..=4)
            .map(|value| retained(&table, node(value, 6000 + u16::from(value))))
            .collect::<Vec<_>>();
        let (input, mut receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(2);
        let (producer, stats) = DhtSampleInfoHashesProducer::new(table, input);
        let attempted = Arc::new(Mutex::new(Vec::new()));
        let attempted_for_hook = Arc::clone(&attempted);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            |_| pending(),
            move |index, _| attempted_for_hook.lock().unwrap().push(index),
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(
            stats.snapshot(),
            DhtSampleInfoHashesProducerStats {
                table_queries: 1,
                selected: 4,
                queued: 2,
                ..DhtSampleInfoHashesProducerStats::default()
            }
        );
        assert_eq!(*attempted.lock().unwrap(), [0, 1, 2]);
        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtSampleInfoHashesProducerExit::Shutdown {
                selected_dropped: 2
            }
        );
        assert_retained(receiver.recv_work().await.unwrap(), &handles[0]);
        assert_retained(receiver.recv_work().await.unwrap(), &handles[1]);
        assert!(receiver.recv_work().await.is_none());
        assert_eq!(
            stats.snapshot(),
            DhtSampleInfoHashesProducerStats {
                table_queries: 1,
                selected: 4,
                queued: 2,
                shutdown_dropped: 2,
                ..DhtSampleInfoHashesProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn ready_shutdown_beats_ready_capacity_for_blocked_send() {
        let table = KTable::new(Id20::ZERO);
        let handles = (1..=2)
            .map(|value| retained(&table, node(value, 6000 + u16::from(value))))
            .collect::<Vec<_>>();
        let (input, mut receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        let (producer, stats) = DhtSampleInfoHashesProducer::new(table, input);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            |_| pending(),
            |_, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_retained(receiver.recv_work().await.unwrap(), &handles[0]);
        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtSampleInfoHashesProducerExit::Shutdown {
                selected_dropped: 1
            }
        );
        assert!(receiver.recv_work().await.is_none());
        assert_eq!(
            stats.snapshot(),
            DhtSampleInfoHashesProducerStats {
                table_queries: 1,
                selected: 2,
                queued: 1,
                shutdown_dropped: 1,
                ..DhtSampleInfoHashesProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn closing_full_route_preserves_prefix_and_classifies_exact_suffix() {
        let table = KTable::new(Id20::ZERO);
        let handles = (1..=4)
            .map(|value| retained(&table, node(value, 6000 + u16::from(value))))
            .collect::<Vec<_>>();
        let (input, mut receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(2);
        let (producer, stats) = DhtSampleInfoHashesProducer::new(table, input);
        let run = producer.run_with(pending(), |_| pending(), |_, _| {});
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        receiver.close();
        assert_eq!(
            run.await,
            DhtSampleInfoHashesProducerExit::InputClosed {
                selected_dropped: 2
            }
        );
        assert_retained(receiver.recv_work().await.unwrap(), &handles[0]);
        assert_retained(receiver.recv_work().await.unwrap(), &handles[1]);
        assert!(receiver.recv_work().await.is_none());
        assert_eq!(
            stats.snapshot(),
            DhtSampleInfoHashesProducerStats {
                table_queries: 1,
                selected: 4,
                queued: 2,
                input_closed_dropped: 2,
                ..DhtSampleInfoHashesProducerStats::default()
            }
        );
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn blocked_handle_is_not_rechecked_or_replaced_before_commit() {
        let table = KTable::new(Id20::ZERO);
        let first_node = node(1, 6001);
        let second_node = node(2, 6002);
        let first = retained(&table, first_node);
        let old_second = retained(&table, second_node);
        let (input, mut receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        let (producer, stats) = DhtSampleInfoHashesProducer::new(table.clone(), input);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            |_| pending(),
            |_, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert!(table.drop_node(second_node.id));
        let replacement = RoutingNode {
            addr: SocketAddr::new(second_node.addr.ip(), 7002),
            ..second_node
        };
        assert_eq!(table.put_node(replacement), RoutingPutResult::Accepted);
        let new_second = table.node_handle(second_node.id).unwrap();
        assert_ne!(old_second, new_second);
        assert!(old_second.dropped());

        assert_retained(receiver.recv_work().await.unwrap(), &first);
        poll_once_pending(run.as_mut()).await;
        assert_retained(receiver.recv_work().await.unwrap(), &old_second);
        assert_eq!(old_second.routing_node(), second_node);
        assert_eq!(stats.snapshot().queued, 2);

        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtSampleInfoHashesProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert!(receiver.recv_work().await.is_none());
        assert_conservation(stats.snapshot());
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_first_poll_starts_immediate_query_then_fresh_exact_delay() {
        let table = KTable::new(Id20::ZERO);
        let (input, mut receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        let (producer, stats) = DhtSampleInfoHashesProducer::new(table, input);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run(async move {
            let _ = shutdown_rx.await;
        });

        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::pin!(run);
        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot().table_queries, 1);

        tokio::time::advance(ROUND_DELAY - Duration::from_nanos(1)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot().table_queries, 1);
        tokio::time::advance(Duration::from_nanos(1)).await;
        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot().table_queries, 2);

        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtSampleInfoHashesProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert!(receiver.recv_work().await.is_none());
    }

    #[tokio::test]
    async fn empty_round_delay_is_shutdown_cancellation_aware() {
        let table = KTable::new(Id20::ZERO);
        let (input, mut receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        let (producer, stats) = DhtSampleInfoHashesProducer::new(table, input);
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let polls_for_delay = Arc::clone(&polls);
        let drops_for_delay = Arc::clone(&drops);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = producer.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            move |duration| {
                assert_eq!(duration, ROUND_DELAY);
                PendingDelay {
                    polls: Arc::clone(&polls_for_delay),
                    drops: Arc::clone(&drops_for_delay),
                }
            },
            |_, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot().table_queries, 1);
        assert_eq!(stats.snapshot().selected, 0);
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        shutdown_tx.send(()).unwrap();
        assert_eq!(
            run.await,
            DhtSampleInfoHashesProducerExit::Shutdown {
                selected_dropped: 0
            }
        );
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert!(receiver.recv_work().await.is_none());
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn empty_round_delay_is_input_close_cancellation_aware() {
        let table = KTable::new(Id20::ZERO);
        let (input, mut receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        let (producer, stats) = DhtSampleInfoHashesProducer::new(table, input);
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let polls_for_delay = Arc::clone(&polls);
        let drops_for_delay = Arc::clone(&drops);
        let run = producer.run_with(
            pending(),
            move |duration| {
                assert_eq!(duration, ROUND_DELAY);
                PendingDelay {
                    polls: Arc::clone(&polls_for_delay),
                    drops: Arc::clone(&drops_for_delay),
                }
            },
            |_, _| {},
        );
        tokio::pin!(run);

        poll_once_pending(run.as_mut()).await;
        assert_eq!(stats.snapshot().table_queries, 1);
        assert_eq!(stats.snapshot().selected, 0);
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        receiver.close();
        assert_eq!(
            run.await,
            DhtSampleInfoHashesProducerExit::InputClosed {
                selected_dropped: 0
            }
        );
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert!(receiver.recv_work().await.is_none());
        assert_conservation(stats.snapshot());
    }

    #[tokio::test]
    async fn constructing_without_running_spawns_nothing_and_releases_eof() {
        let table = KTable::new(Id20::ZERO);
        retained(&table, node(1, 6001));
        let (input, mut receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(1);
        let (producer, stats) = DhtSampleInfoHashesProducer::new(table, input);

        drop(producer);
        assert_eq!(
            stats.snapshot(),
            DhtSampleInfoHashesProducerStats::default()
        );
        assert!(receiver.recv_work().await.is_none());
    }

    #[tokio::test]
    async fn dropping_polled_run_releases_eof_without_terminal_classification() {
        let table = KTable::new(Id20::ZERO);
        let handles = (1..=4)
            .map(|value| retained(&table, node(value, 6000 + u16::from(value))))
            .collect::<Vec<_>>();
        let (input, mut receiver) = DhtDiscoveredNodeSampleInfoHashesInput::test_channel(2);
        let (producer, stats) = DhtSampleInfoHashesProducer::new(table, input);
        let mut run = Box::pin(producer.run_with(pending(), |_| pending(), |_, _| {}));

        poll_once_pending(run.as_mut()).await;
        drop(run);
        assert_retained(receiver.recv_work().await.unwrap(), &handles[0]);
        assert_retained(receiver.recv_work().await.unwrap(), &handles[1]);
        assert!(receiver.recv_work().await.is_none());
        assert_eq!(
            stats.snapshot(),
            DhtSampleInfoHashesProducerStats {
                table_queries: 1,
                selected: 4,
                queued: 2,
                ..DhtSampleInfoHashesProducerStats::default()
            }
        );
    }
}
