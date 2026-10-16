//! Persisting `process_torrent` queue worker.
//!
//! This is the live-queue lifecycle around the dormant writer seam: claim one
//! admitted job with Go's `FOR UPDATE SKIP LOCKED` order, compose its
//! [`crate::WriterPlan`] with [`load_writer_plan`], persist it through
//! [`persist_writer_plan`] (retry publication before the blocker and the
//! transaction kernel), then settle the row through Go's processed / retry /
//! failed lifecycle while still holding the claim lock.
//!
//! Go's handler (`internal/processor/queue/handler.go`) runs with concurrency
//! one, a ten-minute job timeout, and the queue server's 30-second idle poll.
//! The worker keeps those defaults and Go's error text:
//!
//! - a handler error is stored as `job failed to process: <error>`;
//! - a timeout is stored as `job exceeded its <Go duration> timeout: context
//!   deadline exceeded`.
//!
//! Only payloads that explicitly request the `default` workflow with all three
//! attach flags false are ever claimed (see
//! [`bitmagnet_queue::QueueStore::consume_admitted_process_torrent`]). A claimed
//! job that the Rust kernel still cannot reproduce — for example a sourced or
//! enriched explicit hint, or an input over the loader's bounds — is abstained
//! before any side effect, which leaves the row untouched for the Go worker.
//! So is a payload serde cannot decode: Go's decoder accepts inputs serde
//! rejects. Only transient database errors and Go-equivalent processor
//! outcomes consume one of the job's retries.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bitmagnet_queue::{
    AdmittedConsumeOutcome, ConsumeOutcome, DequeuedJob, JobVerdict, ProcessTorrentParams,
    QueuePgError, QueueStore,
};
use futures::FutureExt;
use prometheus::IntCounterVec;

use crate::{
    load_writer_plan, persist_writer_plan, BlockingManager, LoadError, Materializer,
    WriterLoadError, WriterPlanError,
};

/// Go's `handler.JobTimeout(time.Second*60*10)` for `process_torrent`.
pub const DEFAULT_JOB_TIMEOUT: Duration = Duration::from_secs(600);
/// Go's `handler.New` default `CheckInterval`.
pub const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(30);
/// How long an abstained job stays excluded from this worker's claims.
pub const DEFAULT_ABSTAIN_COOLOFF: Duration = Duration::from_secs(600);
/// Upper bound on remembered abstentions; the oldest is forgotten first.
pub const DEFAULT_MAX_EXCLUDED: usize = 1_024;
/// How long an in-flight job may keep running after shutdown is requested.
///
/// Kubernetes sends `SIGKILL` 30 seconds after `SIGTERM` by default; this
/// leaves room to flush the blocking manager before that.
pub const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(20);

const GO_HANDLER_ERROR_PREFIX: &str = "job failed to process: ";

/// Runtime knobs for [`ProcessTorrentWorker`].
#[derive(Debug, Clone)]
pub struct ProcessTorrentWorkerConfig {
    pub check_interval: Duration,
    pub job_timeout: Duration,
    pub abstain_cooloff: Duration,
    pub max_excluded: usize,
    pub shutdown_grace: Duration,
}

impl Default for ProcessTorrentWorkerConfig {
    fn default() -> Self {
        Self {
            check_interval: DEFAULT_CHECK_INTERVAL,
            job_timeout: DEFAULT_JOB_TIMEOUT,
            abstain_cooloff: DEFAULT_ABSTAIN_COOLOFF,
            max_excluded: DEFAULT_MAX_EXCLUDED,
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
        }
    }
}

/// One serial persisting `process_torrent` consumer.
pub struct ProcessTorrentWorker {
    store: QueueStore,
    materializer: Materializer,
    blocking_manager: Arc<dyn BlockingManager>,
    config: ProcessTorrentWorkerConfig,
    excluded: Mutex<VecDeque<(String, Instant)>>,
    metrics: Option<IntCounterVec>,
}

impl ProcessTorrentWorker {
    #[must_use]
    pub fn new(
        store: QueueStore,
        materializer: Materializer,
        blocking_manager: Arc<dyn BlockingManager>,
        config: ProcessTorrentWorkerConfig,
    ) -> Self {
        Self {
            store,
            materializer,
            blocking_manager,
            config,
            excluded: Mutex::new(VecDeque::new()),
            metrics: None,
        }
    }

    /// Count settled and abstained jobs by `outcome` label.
    #[must_use]
    pub fn with_metrics(mut self, metrics: IntCounterVec) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Claim, run, and settle at most one admitted job.
    pub async fn run_once(&self) -> Result<AdmittedConsumeOutcome, QueuePgError> {
        let excluded = self.excluded_ids();
        let outcome = self
            .store
            .consume_admitted_process_torrent(&excluded, |job| self.handle(job))
            .await?;
        self.observe(&outcome);
        Ok(outcome)
    }

    /// Poll until `shutdown` resolves.
    ///
    /// The loop self-chains after a claim and sleeps `check_interval` only when
    /// no admitted job was eligible, like Go's `serverHandler.start`. Database
    /// errors are returned to the caller.
    ///
    /// A job in flight when `shutdown` resolves gets `shutdown_grace` to
    /// settle. After that it is dropped, which rolls back its claim
    /// transaction: the row keeps its status, retries, and error, so it is
    /// claimed again later. This is what Go does when its queue context is
    /// cancelled on stop; side effects already committed by the writer stay,
    /// and the next run of the job redoes them.
    pub async fn run_until<S>(&self, shutdown: S) -> Result<(), QueuePgError>
    where
        S: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        loop {
            if (&mut shutdown).now_or_never().is_some() {
                return Ok(());
            }
            let run = self.run_once();
            tokio::pin!(run);
            let outcome = tokio::select! {
                biased;
                outcome = &mut run => outcome?,
                () = &mut shutdown => return self.drain(run).await,
            };
            if matches!(
                outcome,
                AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty)
            ) {
                tokio::select! {
                    biased;
                    () = &mut shutdown => return Ok(()),
                    () = tokio::time::sleep(self.config.check_interval) => {}
                }
            } else {
                tokio::task::yield_now().await;
            }
        }
    }

    /// Let the in-flight `run` settle within `shutdown_grace`, else drop it.
    async fn drain<F>(&self, run: Pin<&mut F>) -> Result<(), QueuePgError>
    where
        F: Future<Output = Result<AdmittedConsumeOutcome, QueuePgError>>,
    {
        if let Ok(outcome) = tokio::time::timeout(self.config.shutdown_grace, run).await {
            return outcome.map(drop);
        }
        tracing::warn!(
            shutdown_grace = %go_duration(self.config.shutdown_grace),
            "abandoning the in-flight process_torrent job at shutdown; its claim rolls back and the row stays queued"
        );
        Ok(())
    }

    async fn handle(&self, job: DequeuedJob) -> JobVerdict {
        match tokio::time::timeout(self.config.job_timeout, self.process(&job)).await {
            Ok(verdict) => verdict,
            Err(_) => JobVerdict::Fail(format!(
                "job exceeded its {} timeout: context deadline exceeded",
                go_duration(self.config.job_timeout)
            )),
        }
    }

    async fn process(&self, job: &DequeuedJob) -> JobVerdict {
        // Go's decoder is more lenient than serde (a `0x` hash prefix, JSON
        // null for a slice, case-insensitive keys), so a payload serde rejects
        // may be one Go processes. Leave it to Go rather than spend a retry.
        let params: ProcessTorrentParams = match serde_json::from_str(&job.payload) {
            Ok(params) => params,
            Err(error) => {
                tracing::info!(job_id = %job.id, %error, "abstaining from undecodable process_torrent payload");
                return JobVerdict::Abstain(format!("payload: {error}"));
            }
        };
        let pool = self.store.pool();
        let plan = match load_writer_plan(pool, &self.materializer, &params).await {
            Ok(plan) => plan,
            Err(error) if is_transient_plan_error(&error) => return go_handler_failure(&error),
            Err(error) => {
                tracing::info!(job_id = %job.id, %error, "abstaining from unsupported process_torrent job");
                return JobVerdict::Abstain(error.to_string());
            }
        };
        match persist_writer_plan(pool, &plan, &self.store, self.blocking_manager.as_ref()).await {
            Ok(receipt) => {
                tracing::debug!(
                    job_id = %job.id,
                    persisted = receipt.persisted,
                    retry = ?receipt.retry,
                    "process_torrent job persisted"
                );
                JobVerdict::Complete
            }
            Err(error) => go_handler_failure(&error),
        }
    }

    fn excluded_ids(&self) -> Vec<String> {
        let mut excluded = self.excluded.lock().expect("exclusion list lock");
        let now = Instant::now();
        while excluded
            .front()
            .is_some_and(|(_, at)| now.duration_since(*at) >= self.config.abstain_cooloff)
        {
            excluded.pop_front();
        }
        excluded.iter().map(|(id, _)| id.clone()).collect()
    }

    fn observe(&self, outcome: &AdmittedConsumeOutcome) {
        let label = match outcome {
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty) => return,
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) => {
                tracing::info!(job_id = %job.id, retries = job.retries, "process_torrent job processed");
                "processed"
            }
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::RetryScheduled { job, delay }) => {
                tracing::warn!(job_id = %job.id, retries = job.retries, ?delay, "process_torrent job failed; retry scheduled");
                "retry"
            }
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Failed { job, error }) => {
                tracing::error!(job_id = %job.id, retries = job.retries, %error, "process_torrent job failed");
                "failed"
            }
            AdmittedConsumeOutcome::Abstained { job, .. } => {
                let mut excluded = self.excluded.lock().expect("exclusion list lock");
                excluded.push_back((job.id.clone(), Instant::now()));
                while excluded.len() > self.config.max_excluded {
                    excluded.pop_front();
                }
                "abstained"
            }
        };
        if let Some(metrics) = &self.metrics {
            metrics.with_label_values(&[label]).inc();
        }
    }
}

/// Register the worker's outcome counter in `registry`.
pub fn register_worker_metrics(
    registry: &prometheus::Registry,
) -> Result<IntCounterVec, prometheus::Error> {
    let counter = IntCounterVec::new(
        prometheus::Opts::new(
            "bitmagnet_process_torrent_worker_jobs_total",
            "process_torrent jobs claimed by the Rust worker, by outcome",
        ),
        &["outcome"],
    )?;
    registry.register(Box::new(counter.clone()))?;
    Ok(counter)
}

fn go_handler_failure(error: &dyn std::fmt::Display) -> JobVerdict {
    JobVerdict::Fail(format!("{GO_HANDLER_ERROR_PREFIX}{error}"))
}

/// Only database transport/query failures are retried as Go would retry them.
/// Every other loader or composer error is a Rust capability or invariant
/// limit, and occurs before any mutation, so the job is abstained.
fn is_transient_plan_error(error: &WriterPlanError) -> bool {
    matches!(
        error,
        WriterPlanError::Load(
            WriterLoadError::Database(_) | WriterLoadError::Load(LoadError::Database(_))
        )
    )
}

/// Render a duration like Go's `time.Duration.String`.
fn go_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos == 0 {
        return "0s".to_owned();
    }
    if nanos < 1_000_000_000 {
        let (unit, suffix) = match nanos {
            0..=999 => (1, "ns"),
            1_000..=999_999 => (1_000, "\u{b5}s"),
            _ => (1_000_000, "ms"),
        };
        return format!("{}{suffix}", go_fraction(nanos, unit));
    }
    let total = duration.as_secs();
    let (hours, minutes) = (total / 3_600, (total % 3_600) / 60);
    let seconds = go_fraction(nanos % 60_000_000_000, 1_000_000_000);
    if hours > 0 {
        format!("{hours}h{minutes}m{seconds}s")
    } else if minutes > 0 {
        format!("{minutes}m{seconds}s")
    } else {
        format!("{seconds}s")
    }
}

/// `value / unit` with Go's trailing-zero-free decimal fraction.
fn go_fraction(value: u128, unit: u128) -> String {
    let (whole, remainder) = (value / unit, value % unit);
    if remainder == 0 {
        return whole.to_string();
    }
    let width = unit.ilog10() as usize;
    let fraction = format!("{remainder:0width$}");
    format!("{whole}.{}", fraction.trim_end_matches('0'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_duration_matches_time_duration_string() {
        for (duration, expected) in [
            (Duration::from_secs(600), "10m0s"),
            (Duration::from_secs(30), "30s"),
            (Duration::from_secs(90), "1m30s"),
            (Duration::from_secs(3_600), "1h0m0s"),
            (Duration::from_secs(3_725), "1h2m5s"),
            (Duration::from_millis(1_500), "1.5s"),
            (Duration::from_millis(250), "250ms"),
            (Duration::from_micros(1_500), "1.5ms"),
            (Duration::from_nanos(1_500), "1.5\u{b5}s"),
            (Duration::from_nanos(1), "1ns"),
            (Duration::from_nanos(61_000_000_001), "1m1.000000001s"),
            (Duration::ZERO, "0s"),
        ] {
            assert_eq!(go_duration(duration), expected, "{duration:?}");
        }
    }

    #[test]
    fn only_database_load_errors_are_transient() {
        assert!(is_transient_plan_error(&WriterPlanError::Load(
            WriterLoadError::Database(sqlx::Error::PoolTimedOut)
        )));
        assert!(is_transient_plan_error(&WriterPlanError::Load(
            WriterLoadError::Load(LoadError::Database(sqlx::Error::PoolTimedOut))
        )));
        assert!(!is_transient_plan_error(
            &WriterPlanError::ClassifierWorkflowUnsupported
        ));
        assert!(!is_transient_plan_error(&WriterPlanError::Load(
            WriterLoadError::RequestTooLarge {
                actual: 101,
                limit: 100
            }
        )));
        assert!(!is_transient_plan_error(
            &WriterPlanError::AttachHintUnsupported {
                info_hash: "aa".repeat(20)
            }
        ));
    }

    #[test]
    fn handler_failures_carry_go_prefix() {
        assert_eq!(
            go_handler_failure(&"missing 2 info hashes"),
            JobVerdict::Fail("job failed to process: missing 2 info hashes".to_owned())
        );
    }
}
