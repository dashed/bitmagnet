//! PostgreSQL cleanup for terminal queue rows.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::{QueuePgError, QueueStore};

/// Go's completion-to-next-attempt queue garbage-collection interval.
pub const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(10 * 60);

impl QueueStore {
    /// Delete terminal jobs whose per-row archival window ended before `cutoff`.
    ///
    /// The strict inequality, terminal statuses, and caller-supplied clock match
    /// Go's queue garbage-collection statement. Rows with a null `ran_at` are
    /// retained by PostgreSQL's null predicate semantics.
    pub async fn delete_expired_terminal_jobs(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<u64, QueuePgError> {
        let result = sqlx::query(
            "DELETE FROM queue_jobs \
             WHERE status IN ('processed', 'failed') \
               AND ran_at + archival_duration < $1",
        )
        .bind(cutoff)
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected())
    }

    /// Sweep immediately, then ten minutes after each completed attempt, until shutdown.
    ///
    /// This loop is intentionally not activated by a runtime yet. Go remains
    /// the single global garbage-collection owner during partial queue cutover.
    /// Shutdown interrupts the wait but drains an already-started sweep because
    /// dropping an SQLx query future does not guarantee server-side cancellation.
    pub async fn run_terminal_gc_until<S>(&self, shutdown: S)
    where
        S: Future<Output = ()>,
    {
        run_gc_loop(
            Utc::now,
            |cutoff| self.delete_expired_terminal_jobs(cutoff),
            tokio::time::sleep,
            shutdown,
            |result| match result {
                Ok(0) => {}
                Ok(count) => tracing::debug!(count, "deleted old queue jobs"),
                Err(error) => tracing::error!(%error, "error deleting old queue jobs"),
            },
        )
        .await;
    }
}

async fn run_gc_loop<N, Sweep, SweepFuture, Sleep, SleepFuture, Shutdown, Observe>(
    mut now: N,
    mut sweep: Sweep,
    mut sleep: Sleep,
    shutdown: Shutdown,
    mut observe: Observe,
) where
    N: FnMut() -> DateTime<Utc>,
    Sweep: FnMut(DateTime<Utc>) -> SweepFuture,
    SweepFuture: Future<Output = Result<u64, QueuePgError>>,
    Sleep: FnMut(Duration) -> SleepFuture,
    SleepFuture: Future<Output = ()>,
    Shutdown: Future<Output = ()>,
    Observe: FnMut(&Result<u64, QueuePgError>),
{
    tokio::pin!(shutdown);
    loop {
        let attempt_started = Arc::new(AtomicBool::new(false));
        let observed_attempt_started = Arc::clone(&attempt_started);
        let attempt = async {
            observed_attempt_started.store(true, Ordering::Release);
            sweep(now()).await
        };
        tokio::pin!(attempt);
        let result = tokio::select! {
            biased;
            () = &mut shutdown => {
                if attempt_started.load(Ordering::Acquire) {
                    let result = attempt.await;
                    observe(&result);
                }
                return;
            }
            result = &mut attempt => result,
        };
        observe(&result);

        let wait = sleep(DEFAULT_GC_INTERVAL);
        tokio::pin!(wait);
        tokio::select! {
            biased;
            () = &mut shutdown => return,
            () = &mut wait => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{run_gc_loop, DEFAULT_GC_INTERVAL};
    use crate::QueuePgError;
    use chrono::{DateTime, TimeDelta, Utc};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::oneshot;

    #[tokio::test(start_paused = true)]
    async fn cadence_is_immediate_completion_relative_and_error_tolerant() {
        let first = DateTime::parse_from_rfc3339("2026-08-12T07:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let clock_calls = Arc::new(Mutex::new(0_i64));
        let cutoffs = Arc::new(Mutex::new(Vec::new()));
        let results = Arc::new(Mutex::new(VecDeque::from([
            Err(QueuePgError::InvalidStatus("broken".to_owned())),
            Ok(3),
        ])));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let task = tokio::spawn({
            let clock_calls = Arc::clone(&clock_calls);
            let cutoffs = Arc::clone(&cutoffs);
            let results = Arc::clone(&results);
            let observed = Arc::clone(&observed);
            async move {
                run_gc_loop(
                    move || {
                        let mut calls = clock_calls.lock().unwrap();
                        let value = first + TimeDelta::seconds(*calls);
                        *calls += 1;
                        value
                    },
                    move |cutoff| {
                        cutoffs.lock().unwrap().push(cutoff);
                        std::future::ready(results.lock().unwrap().pop_front().unwrap_or(Ok(0)))
                    },
                    tokio::time::sleep,
                    async {
                        let _ = shutdown_rx.await;
                    },
                    move |result| observed.lock().unwrap().push(result.is_ok()),
                )
                .await;
            }
        });

        tokio::task::yield_now().await;
        assert_eq!(cutoffs.lock().unwrap().as_slice(), &[first]);
        assert_eq!(observed.lock().unwrap().as_slice(), &[false]);

        tokio::time::advance(DEFAULT_GC_INTERVAL - Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(cutoffs.lock().unwrap().len(), 1);

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            cutoffs.lock().unwrap().as_slice(),
            &[first, first + TimeDelta::seconds(1)]
        );
        assert_eq!(observed.lock().unwrap().as_slice(), &[false, true]);

        shutdown_tx.send(()).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_drains_the_immediate_attempt() {
        let (attempt_tx, attempt_rx) = oneshot::channel();
        let attempt_rx = Arc::new(Mutex::new(Some(attempt_rx)));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let observations = Arc::new(Mutex::new(0_u32));
        let task = tokio::spawn({
            let observations = Arc::clone(&observations);
            let attempt_rx = Arc::clone(&attempt_rx);
            async move {
                run_gc_loop(
                    Utc::now,
                    move |_| {
                        let attempt_rx = attempt_rx.lock().unwrap().take().unwrap();
                        async move {
                            attempt_rx.await.unwrap();
                            Ok(1)
                        }
                    },
                    tokio::time::sleep,
                    async {
                        let _ = shutdown_rx.await;
                    },
                    move |_| *observations.lock().unwrap() += 1,
                )
                .await;
            }
        });
        tokio::task::yield_now().await;
        shutdown_tx.send(()).unwrap();
        tokio::task::yield_now().await;
        assert!(!task.is_finished(), "shutdown must drain the current sweep");
        attempt_tx.send(()).unwrap();
        task.await.unwrap();
        assert_eq!(*observations.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn pre_signaled_shutdown_starts_no_attempt() {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        shutdown_tx.send(()).unwrap();
        let clock_calls = Arc::new(Mutex::new(0_u32));
        let sweep_calls = Arc::new(Mutex::new(0_u32));
        run_gc_loop(
            {
                let clock_calls = Arc::clone(&clock_calls);
                move || {
                    *clock_calls.lock().unwrap() += 1;
                    Utc::now()
                }
            },
            {
                let sweep_calls = Arc::clone(&sweep_calls);
                move |_| {
                    *sweep_calls.lock().unwrap() += 1;
                    std::future::ready(Ok(0))
                }
            },
            tokio::time::sleep,
            async {
                let _ = shutdown_rx.await;
            },
            |_| panic!("pre-signaled shutdown must observe no sweep"),
        )
        .await;
        assert_eq!(*clock_calls.lock().unwrap(), 0);
        assert_eq!(*sweep_calls.lock().unwrap(), 0);
    }
}
