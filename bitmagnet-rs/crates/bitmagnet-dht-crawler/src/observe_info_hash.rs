//! PostgreSQL-nonmutating terminal consumer for DHT info-hash observations.
//!
//! This worker is the deliberately narrow end of the first Rust DHT network
//! soak. It discards each observed hash and source address after counting the
//! occurrence. It retains no payload, opens no database, contacts no peer, and
//! cannot persist or block a hash. The upstream runtime and maintenance graph
//! can still perform DNS and public DHT network traffic.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bitmagnet_dht::DhtInfoHashTriageReceiver;

/// Terminal state of one observation-worker run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DhtInfoHashObservationWorkerExit {
    /// The route reached EOF and every queued observation was consumed.
    InputClosed,
    /// External shutdown won before another observation was consumed.
    Shutdown {
        /// Observations already accepted by the bounded route and discarded
        /// after closing it to later sends.
        queued_dropped: usize,
    },
}

#[derive(Default)]
struct DhtInfoHashObservationStatsInner {
    observed: AtomicU64,
    input_closed: AtomicU64,
    shutdowns: AtomicU64,
    shutdown_queued_dropped: AtomicU64,
}

/// Cloneable sender-free view of discard-only observation counters.
#[derive(Clone, Default)]
pub struct DhtInfoHashObservationStatsHandle {
    inner: Arc<DhtInfoHashObservationStatsInner>,
}

/// One independently read snapshot of saturating observation counters.
///
/// After a terminal exit, `input_closed + shutdowns = 1`. On natural EOF all
/// accepted occurrences contribute to `observed`; on shutdown each accepted
/// occurrence contributes to either `observed` or `shutdown_queued_dropped`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtInfoHashObservationStats {
    /// Occurrences removed from the route and deliberately discarded.
    pub observed: u64,
    /// Runs whose input route reached EOF.
    pub input_closed: u64,
    /// Runs whose external shutdown signal won.
    pub shutdowns: u64,
    /// Accepted queued occurrences discarded after shutdown closed the route.
    pub shutdown_queued_dropped: u64,
}

impl DhtInfoHashObservationStatsHandle {
    /// Read the current counters without retaining the route or worker.
    #[must_use]
    pub fn snapshot(&self) -> DhtInfoHashObservationStats {
        DhtInfoHashObservationStats {
            observed: self.inner.observed.load(Ordering::Relaxed),
            input_closed: self.inner.input_closed.load(Ordering::Relaxed),
            shutdowns: self.inner.shutdowns.load(Ordering::Relaxed),
            shutdown_queued_dropped: self.inner.shutdown_queued_dropped.load(Ordering::Relaxed),
        }
    }
}

/// Owned terminal consumer for the first PostgreSQL-nonmutating DHT soak.
#[must_use = "the observation worker must be run or deliberately dropped"]
pub struct DhtInfoHashObservationWorker {
    receiver: DhtInfoHashTriageReceiver,
    stats: DhtInfoHashObservationStatsHandle,
}

impl DhtInfoHashObservationWorker {
    /// Construct a taskless worker around the unique route receiver.
    pub fn new(receiver: DhtInfoHashTriageReceiver) -> (Self, DhtInfoHashObservationStatsHandle) {
        let stats = DhtInfoHashObservationStatsHandle::default();
        (
            Self {
                receiver,
                stats: stats.clone(),
            },
            stats,
        )
    }

    /// Consume and discard observations until producer EOF or shutdown.
    ///
    /// Shutdown has deterministic priority. It closes the bounded route to
    /// later sends and drains every already accepted occurrence before
    /// returning, without retaining any hash or source address.
    ///
    /// Dropping this future returns no exit and makes no drain or accounting
    /// claim. Accepted queued observations may then be dropped with the owned
    /// receiver without contributing to either terminal counter.
    pub async fn run<F>(mut self, shutdown: F) -> DhtInfoHashObservationWorkerExit
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => {
                    self.receiver.close();
                    let mut queued_dropped = 0_usize;
                    while self.receiver.recv().await.is_some() {
                        queued_dropped = queued_dropped.saturating_add(1);
                    }
                    saturating_add(&self.stats.inner.shutdowns, 1);
                    saturating_add_usize(
                        &self.stats.inner.shutdown_queued_dropped,
                        queued_dropped,
                    );
                    return DhtInfoHashObservationWorkerExit::Shutdown { queued_dropped };
                }
                request = self.receiver.recv() => match request {
                    Some(_request) => saturating_add(&self.stats.inner.observed, 1),
                    None => {
                        saturating_add(&self.stats.inner.input_closed, 1);
                        return DhtInfoHashObservationWorkerExit::InputClosed;
                    }
                }
            }
        }
    }
}

fn saturating_add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn saturating_add_usize(counter: &AtomicU64, value: usize) {
    saturating_add(counter, u64::try_from(value).unwrap_or(u64::MAX));
}

#[cfg(test)]
mod tests {
    use std::future::{pending, poll_fn, Future};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::num::NonZeroUsize;
    use std::pin::Pin;
    use std::task::Poll;

    use bitmagnet_dht::{dht_info_hash_triage_channel, DhtInfoHashTriageRequest, Id20};

    use super::*;

    fn request(value: u8) -> DhtInfoHashTriageRequest {
        let mut bytes = [0_u8; 20];
        bytes[19] = value;
        DhtInfoHashTriageRequest {
            info_hash: Id20::from_slice(&bytes).unwrap(),
            source_node_addr: SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                10_000 + u16::from(value),
            )),
        }
    }

    async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
        poll_fn(|context| match future.as_mut().poll(context) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(_) => panic!("future completed instead of registering as pending"),
        })
        .await;
    }

    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn public_types_have_only_the_required_concurrency_capabilities() {
        assert_send::<DhtInfoHashObservationWorker>();
        assert_send_sync::<DhtInfoHashObservationStats>();
        assert_send_sync::<DhtInfoHashObservationStatsHandle>();
        assert_send_sync::<DhtInfoHashObservationWorkerExit>();
    }

    #[tokio::test]
    async fn natural_eof_observes_every_occurrence_without_retaining_payloads() {
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(3).unwrap());
        let (worker, stats) = DhtInfoHashObservationWorker::new(receiver);
        for value in 1..=3 {
            input.send(request(value)).await.unwrap();
        }
        drop(input);

        assert_eq!(
            worker.run(pending()).await,
            DhtInfoHashObservationWorkerExit::InputClosed
        );
        assert_eq!(
            stats.snapshot(),
            DhtInfoHashObservationStats {
                observed: 3,
                input_closed: 1,
                shutdowns: 0,
                shutdown_queued_dropped: 0,
            }
        );
    }

    #[tokio::test]
    async fn ready_shutdown_wins_and_accounts_for_the_exact_queued_prefix() {
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(2).unwrap());
        let (worker, stats) = DhtInfoHashObservationWorker::new(receiver);
        input.send(request(1)).await.unwrap();
        input.send(request(2)).await.unwrap();

        assert_eq!(
            worker.run(std::future::ready(())).await,
            DhtInfoHashObservationWorkerExit::Shutdown { queued_dropped: 2 }
        );
        assert_eq!(
            stats.snapshot(),
            DhtInfoHashObservationStats {
                observed: 0,
                input_closed: 0,
                shutdowns: 1,
                shutdown_queued_dropped: 2,
            }
        );
        assert_eq!(
            input.send(request(3)).await.unwrap_err().into_request(),
            request(3)
        );
    }

    #[tokio::test]
    async fn shutdown_after_an_observed_prefix_conserves_the_accepted_suffix() {
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(3).unwrap());
        let (worker, stats) = DhtInfoHashObservationWorker::new(receiver);
        for value in 1..=3 {
            input.send(request(value)).await.unwrap();
        }

        let shutdown_stats = stats.clone();
        let exit = worker
            .run(async move {
                while shutdown_stats.snapshot().observed == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await;

        assert_eq!(
            exit,
            DhtInfoHashObservationWorkerExit::Shutdown { queued_dropped: 2 }
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.observed, 1);
        assert_eq!(snapshot.shutdown_queued_dropped, 2);
        assert_eq!(
            snapshot
                .observed
                .saturating_add(snapshot.shutdown_queued_dropped),
            3
        );
    }

    #[tokio::test]
    async fn shutdown_recovers_a_blocked_sender_commit_before_finishing_drain() {
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        let (worker, stats) = DhtInfoHashObservationWorker::new(receiver);
        input.send(request(1)).await.unwrap();
        let mut blocked = Box::pin(input.send(request(2)));
        assert_pending(blocked.as_mut()).await;

        let exit = worker.run(std::future::ready(())).await;
        assert_eq!(
            exit,
            DhtInfoHashObservationWorkerExit::Shutdown { queued_dropped: 1 }
        );
        assert_eq!(blocked.await.unwrap_err().into_request(), request(2));
        assert_eq!(stats.snapshot().shutdown_queued_dropped, 1);
    }

    #[tokio::test]
    async fn cancelling_run_claims_neither_eof_nor_clean_shutdown() {
        let (input, receiver) = dht_info_hash_triage_channel(NonZeroUsize::new(1).unwrap());
        let (worker, stats) = DhtInfoHashObservationWorker::new(receiver);
        let mut run = Box::pin(worker.run(pending()));
        assert_pending(run.as_mut()).await;
        input.send(request(1)).await.unwrap();
        drop(run);

        assert_eq!(stats.snapshot(), DhtInfoHashObservationStats::default());
        assert_eq!(
            input.send(request(2)).await.unwrap_err().into_request(),
            request(2)
        );
    }
}
