use std::future::Future;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use thiserror::Error;

use crate::Id20;

const TARGET_BYTES: usize = 20;
const ROTATION_DELAY: Duration = Duration::from_secs(10);

/// Cloneable node-ID target shared by DHT crawler query workers.
///
/// This handle stores only the current target. It does not generate IDs,
/// rotate the value, start a task, or own a timer. Reads and module-owned
/// replacements synchronize the entire 20-byte ID, so callers never observe a
/// torn value. A poisoned lock is recovered because the protected invariant is
/// only one copyable [`Id20`] value.
#[derive(Clone)]
pub struct DhtCrawlerTarget {
    inner: Arc<RwLock<Id20>>,
}

impl DhtCrawlerTarget {
    /// Construct a shared target with an explicit initial node ID.
    #[must_use]
    pub fn new(initial: Id20) -> Self {
        Self {
            inner: Arc::new(RwLock::new(initial)),
        }
    }

    /// Read one stable snapshot of the current target.
    #[must_use]
    pub fn current(&self) -> Id20 {
        *self.inner.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Replace the target observed by every clone.
    fn set(&self, next: Id20) {
        *self.inner.write().unwrap_or_else(PoisonError::into_inner) = next;
    }
}

/// Failure to obtain entropy for a DHT crawler target.
#[derive(Debug, Error)]
pub enum DhtCrawlerTargetError {
    /// The operating-system entropy source failed before a complete ID could
    /// be published.
    #[error("failed to generate DHT crawler target: {0}")]
    Entropy(getrandom::Error),
}

/// Unique owner of periodic replacements for a [`DhtCrawlerTarget`].
///
/// A rotator is created only together with its read-only target handle and is
/// deliberately not cloneable. Calling [`Self::run`] owns no task and spawns
/// none; dropping that future stops rotation immediately.
///
/// ```compile_fail
/// use bitmagnet_dht::DhtCrawlerTargetRotator;
///
/// let (_, rotator) = DhtCrawlerTargetRotator::new().unwrap();
/// let _other_writer = rotator.clone();
/// ```
#[must_use = "the rotator must be run for the shared target to change"]
pub struct DhtCrawlerTargetRotator {
    target: DhtCrawlerTarget,
}

impl DhtCrawlerTargetRotator {
    /// Generate an initial raw 20-byte target and return it with its unique
    /// writer.
    ///
    /// If entropy fails, neither component is returned and any partially
    /// filled local byte buffer is discarded.
    pub fn new() -> Result<(DhtCrawlerTarget, Self), DhtCrawlerTargetError> {
        Self::new_with_fill(fill_random).map_err(DhtCrawlerTargetError::Entropy)
    }

    /// Rotate every ten seconds until caller shutdown.
    ///
    /// Each delay is created only after the preceding replacement completes,
    /// so delayed polling cannot cause catch-up rotations. Shutdown is biased
    /// ahead of a simultaneously ready timer. Once a timer wins, entropy is
    /// obtained synchronously and the whole ID is replaced; an entropy failure
    /// returns without changing the last published target.
    pub async fn run<F>(self, shutdown: F) -> Result<(), DhtCrawlerTargetError>
    where
        F: Future<Output = ()>,
    {
        self.run_with(shutdown, fill_random, tokio::time::sleep)
            .await
            .map_err(DhtCrawlerTargetError::Entropy)
    }

    fn new_with_fill<Fill, E>(mut fill: Fill) -> Result<(DhtCrawlerTarget, Self), E>
    where
        Fill: FnMut(&mut [u8; TARGET_BYTES]) -> Result<(), E>,
    {
        let initial = generate_target(&mut fill)?;
        let target = DhtCrawlerTarget::new(initial);
        let rotator = Self {
            target: target.clone(),
        };
        Ok((target, rotator))
    }

    async fn run_with<F, Fill, E, Delay, DelayFuture>(
        self,
        shutdown: F,
        mut fill: Fill,
        mut delay: Delay,
    ) -> Result<(), E>
    where
        F: Future<Output = ()>,
        Fill: FnMut(&mut [u8; TARGET_BYTES]) -> Result<(), E>,
        Delay: FnMut(Duration) -> DelayFuture,
        DelayFuture: Future<Output = ()>,
    {
        tokio::pin!(shutdown);

        loop {
            let wait = delay(ROTATION_DELAY);
            tokio::pin!(wait);
            tokio::select! {
                biased;
                () = &mut shutdown => return Ok(()),
                () = &mut wait => {}
            }

            let next = generate_target(&mut fill)?;
            self.target.set(next);
        }
    }
}

fn generate_target<Fill, E>(fill: &mut Fill) -> Result<Id20, E>
where
    Fill: FnMut(&mut [u8; TARGET_BYTES]) -> Result<(), E>,
{
    let mut bytes = [0; TARGET_BYTES];
    fill(&mut bytes)?;
    Ok(Id20::from_slice(&bytes).expect("a 20-byte node ID always has valid length"))
}

fn fill_random(bytes: &mut [u8; TARGET_BYTES]) -> Result<(), getrandom::Error> {
    getrandom::fill(bytes)
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::{pending, poll_fn, ready};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Barrier;
    use std::task::{Context, Poll, Wake, Waker};

    use super::*;

    fn id(value: u8) -> Id20 {
        let mut bytes = [0; 20];
        bytes[19] = value;
        Id20::from_slice(&bytes).unwrap()
    }

    #[test]
    fn clones_observe_module_owned_replacements() {
        let target = DhtCrawlerTarget::new(id(1));
        let clone = target.clone();

        target.set(id(2));
        assert_eq!(clone.current(), id(2));
    }

    #[test]
    fn handle_is_send_sync_and_concurrent_reads_are_whole_ids() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DhtCrawlerTarget>();

        let first = Id20::from_slice(&[0x00; 20]).unwrap();
        let second = Id20::from_slice(&[0xff; 20]).unwrap();
        let target = DhtCrawlerTarget::new(first);
        let start = Arc::new(Barrier::new(5));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let reader = target.clone();
            let reader_start = start.clone();
            readers.push(std::thread::spawn(move || {
                reader_start.wait();
                for _ in 0..10_000 {
                    assert!(matches!(reader.current(), value if value == first || value == second));
                }
            }));
        }

        start.wait();
        for index in 0..10_000 {
            target.set(if index % 2 == 0 { second } else { first });
        }
        for reader in readers {
            reader.join().unwrap();
        }

        target.set(second);
        let observer = target.clone();
        assert_eq!(observer.current(), second);
    }

    #[test]
    fn returned_snapshot_remains_stable_after_replacement() {
        let target = DhtCrawlerTarget::new(id(1));
        let snapshot = target.current();

        target.set(id(2));

        assert_eq!(snapshot, id(1));
        assert_eq!(target.current(), id(2));
    }

    #[test]
    fn zero_is_an_accepted_explicit_target() {
        let target = DhtCrawlerTarget::new(Id20::ZERO);

        assert_eq!(target.current(), Id20::ZERO);
    }

    #[test]
    fn poisoned_writer_does_not_prevent_reads_or_replacement() {
        let target = DhtCrawlerTarget::new(id(1));
        let poisoner = target.clone();

        let panic = std::thread::spawn(move || {
            let _guard = poisoner.inner.write().unwrap();
            panic!("poison target lock");
        })
        .join();
        assert!(panic.is_err());

        assert_eq!(target.current(), id(1));
        target.set(id(2));
        assert_eq!(target.current(), id(2));
    }

    #[test]
    fn injected_raw_initial_id_is_published_before_run() {
        let raw = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff, 0x10, 0x20, 0x30, 0x40,
        ];
        let (target, _rotator) =
            DhtCrawlerTargetRotator::new_with_fill(|bytes| -> Result<(), Infallible> {
                *bytes = raw;
                Ok(())
            })
            .unwrap();

        assert_eq!(target.current(), Id20::from_slice(&raw).unwrap());
    }

    #[test]
    fn injected_zero_initial_id_is_accepted_by_the_pair_constructor() {
        let (target, _rotator) = fixed_pair([0; TARGET_BYTES]);

        assert_eq!(target.current(), Id20::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn first_rotation_occurs_at_exactly_ten_seconds_and_is_visible_to_clones() {
        let initial = [0x11; TARGET_BYTES];
        let replacement = [0x22; TARGET_BYTES];
        let (target, rotator) =
            DhtCrawlerTargetRotator::new_with_fill(|bytes| -> Result<(), Infallible> {
                *bytes = initial;
                Ok(())
            })
            .unwrap();
        let observer = target.clone();
        let fill_calls = Arc::new(AtomicUsize::new(0));
        let run_fill_calls = fill_calls.clone();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let mut run = Box::pin(rotator.run_with(
            async move {
                let _ = shutdown_rx.await;
            },
            move |bytes| -> Result<(), Infallible> {
                run_fill_calls.fetch_add(1, Ordering::SeqCst);
                *bytes = replacement;
                Ok(())
            },
            tokio::time::sleep,
        ));

        assert!(poll_once(run.as_mut()).is_pending());

        tokio::time::advance(Duration::from_millis(9_999)).await;
        assert!(poll_once(run.as_mut()).is_pending());
        assert_eq!(target.current(), Id20::from_slice(&initial).unwrap());
        assert_eq!(fill_calls.load(Ordering::SeqCst), 0);

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(poll_once(run.as_mut()).is_pending());
        assert_eq!(observer.current(), Id20::from_slice(&replacement).unwrap());
        assert_eq!(fill_calls.load(Ordering::SeqCst), 1);

        shutdown_tx.send(()).unwrap();
        assert!(matches!(poll_once(run.as_mut()), Poll::Ready(Ok(()))));
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_poll_rotates_once_and_starts_a_fresh_delay_without_catch_up() {
        let initial = [0x11; TARGET_BYTES];
        let second = [0x22; TARGET_BYTES];
        let third = [0x33; TARGET_BYTES];
        let (target, rotator) =
            DhtCrawlerTargetRotator::new_with_fill(|bytes| -> Result<(), Infallible> {
                *bytes = initial;
                Ok(())
            })
            .unwrap();
        let generated = Arc::new(AtomicUsize::new(0));
        let generated_by_run = generated.clone();
        let delays = Arc::new(AtomicUsize::new(0));
        let delays_by_run = delays.clone();
        let run = rotator.run_with(
            pending(),
            move |bytes| -> Result<(), Infallible> {
                let call = generated_by_run.fetch_add(1, Ordering::SeqCst);
                *bytes = if call == 0 { second } else { third };
                Ok(())
            },
            move |duration| {
                assert_eq!(duration, ROTATION_DELAY);
                delays_by_run.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(duration)
            },
        );
        tokio::pin!(run);

        assert!(poll_once(run.as_mut()).is_pending());
        assert_eq!(delays.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_secs(35)).await;
        assert!(poll_once(run.as_mut()).is_pending());
        assert_eq!(generated.load(Ordering::SeqCst), 1);
        assert_eq!(delays.load(Ordering::SeqCst), 2);
        assert_eq!(target.current(), Id20::from_slice(&second).unwrap());

        tokio::time::advance(Duration::from_millis(9_999)).await;
        assert!(poll_once(run.as_mut()).is_pending());
        assert_eq!(generated.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(poll_once(run.as_mut()).is_pending());
        assert_eq!(generated.load(Ordering::SeqCst), 2);
        assert_eq!(target.current(), Id20::from_slice(&third).unwrap());
    }

    #[tokio::test]
    async fn ready_shutdown_wins_without_generation() {
        let (target, rotator) = fixed_pair([0x11; TARGET_BYTES]);
        let generated = Arc::new(AtomicUsize::new(0));
        let generated_by_run = generated.clone();
        let delays = Arc::new(AtomicUsize::new(0));
        let delays_by_run = delays.clone();

        let result = rotator
            .run_with(
                ready(()),
                move |_| -> Result<(), Infallible> {
                    generated_by_run.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                move |_| {
                    delays_by_run.fetch_add(1, Ordering::SeqCst);
                    pending()
                },
            )
            .await;

        assert!(result.is_ok());
        assert_eq!(generated.load(Ordering::SeqCst), 0);
        assert_eq!(delays.load(Ordering::SeqCst), 1);
        assert_eq!(
            target.current(),
            Id20::from_slice(&[0x11; TARGET_BYTES]).unwrap()
        );
    }

    #[tokio::test]
    async fn tied_shutdown_wins_without_generation() {
        let (target, rotator) = fixed_pair([0x11; TARGET_BYTES]);
        let generated = Arc::new(AtomicUsize::new(0));
        let generated_by_run = generated.clone();

        let result = rotator
            .run_with(
                ready(()),
                move |_| -> Result<(), Infallible> {
                    generated_by_run.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                |_| ready(()),
            )
            .await;

        assert!(result.is_ok());
        assert_eq!(generated.load(Ordering::SeqCst), 0);
        assert_eq!(
            target.current(),
            Id20::from_slice(&[0x11; TARGET_BYTES]).unwrap()
        );
    }

    #[tokio::test]
    async fn shutdown_becoming_ready_during_generation_keeps_the_completed_replacement() {
        let initial = [0x11; TARGET_BYTES];
        let replacement = [0x22; TARGET_BYTES];
        let (target, rotator) = fixed_pair(initial);
        let shutdown_ready = Arc::new(AtomicBool::new(false));
        let shutdown_for_wait = shutdown_ready.clone();
        let shutdown_from_fill = shutdown_ready.clone();
        let generated = Arc::new(AtomicUsize::new(0));
        let generated_by_run = generated.clone();

        let result = rotator
            .run_with(
                poll_fn(move |_| {
                    if shutdown_for_wait.load(Ordering::SeqCst) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }),
                move |bytes| -> Result<(), Infallible> {
                    generated_by_run.fetch_add(1, Ordering::SeqCst);
                    *bytes = replacement;
                    shutdown_from_fill.store(true, Ordering::SeqCst);
                    Ok(())
                },
                |_| ready(()),
            )
            .await;

        assert!(result.is_ok());
        assert_eq!(generated.load(Ordering::SeqCst), 1);
        assert_eq!(target.current(), Id20::from_slice(&replacement).unwrap());
    }

    #[tokio::test]
    async fn rotation_entropy_failure_preserves_last_published_target() {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        struct EntropyFailed;

        let initial = [0x11; TARGET_BYTES];
        let (target, rotator) = fixed_pair(initial);
        let result = rotator
            .run_with(
                pending(),
                |bytes| {
                    bytes[..7].fill(0xff);
                    Err(EntropyFailed)
                },
                |_| ready(()),
            )
            .await;

        assert_eq!(result, Err(EntropyFailed));
        assert_eq!(target.current(), Id20::from_slice(&initial).unwrap());
    }

    #[tokio::test]
    async fn later_rotation_entropy_failure_preserves_the_last_successful_replacement() {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        struct EntropyFailed;

        let initial = [0x11; TARGET_BYTES];
        let replacement = [0x22; TARGET_BYTES];
        let (target, rotator) = fixed_pair(initial);
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_by_run = calls.clone();
        let result = rotator
            .run_with(
                pending(),
                move |bytes| {
                    if calls_by_run.fetch_add(1, Ordering::SeqCst) == 0 {
                        *bytes = replacement;
                        Ok(())
                    } else {
                        bytes[..7].fill(0xff);
                        Err(EntropyFailed)
                    }
                },
                |_| ready(()),
            )
            .await;

        assert_eq!(result, Err(EntropyFailed));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(target.current(), Id20::from_slice(&replacement).unwrap());
    }

    #[test]
    fn constructor_entropy_failure_returns_no_pair() {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        struct EntropyFailed;

        let result = DhtCrawlerTargetRotator::new_with_fill(|bytes| {
            bytes[..7].fill(0xff);
            Err(EntropyFailed)
        });

        assert!(matches!(result, Err(EntropyFailed)));
    }

    #[tokio::test]
    async fn dropping_a_polled_run_drops_its_delay_without_detaching_work() {
        let (target, rotator) = fixed_pair([0x11; TARGET_BYTES]);
        let polled = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let polled_by_delay = polled.clone();
        let dropped_by_delay = dropped.clone();
        let generated = Arc::new(AtomicUsize::new(0));
        let generated_by_run = generated.clone();
        let mut run = Box::pin(rotator.run_with(
            pending(),
            move |_| -> Result<(), Infallible> {
                generated_by_run.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            move |duration| {
                assert_eq!(duration, ROTATION_DELAY);
                TrackedPending {
                    polled: polled_by_delay.clone(),
                    dropped: dropped_by_delay.clone(),
                }
            },
        ));

        assert!(poll_once(run.as_mut()).is_pending());
        assert!(polled.load(Ordering::SeqCst));
        drop(run);

        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(generated.load(Ordering::SeqCst), 0);
        assert_eq!(
            target.current(),
            Id20::from_slice(&[0x11; TARGET_BYTES]).unwrap()
        );
    }

    fn fixed_pair(initial: [u8; TARGET_BYTES]) -> (DhtCrawlerTarget, DhtCrawlerTargetRotator) {
        DhtCrawlerTargetRotator::new_with_fill(|bytes| -> Result<(), Infallible> {
            *bytes = initial;
            Ok(())
        })
        .unwrap()
    }

    #[test]
    fn rotator_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<DhtCrawlerTargetRotator>();
    }

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        let waker = Waker::from(Arc::new(NoopWake));
        future.poll(&mut Context::from_waker(&waker))
    }

    struct TrackedPending {
        polled: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }

    impl Future for TrackedPending {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            self.polled.store(true, Ordering::SeqCst);
            Poll::Pending
        }
    }

    impl Drop for TrackedPending {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
}
