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
//! Jobs stay serial. Within a job the [`Materializer`] may classify the
//! torrents on its own thread pool, as Go's classifier runs a batch's torrents
//! concurrently; the write-set is still folded in requested order, so the
//! plan is the same for any parallelism.
//!
//! Only payloads that explicitly request the `default` workflow with all three
//! attach flags false are ever claimed (see
//! [`bitmagnet_queue::QueueStore::consume_admitted_process_torrent`]). A claimed
//! job that the Rust kernel still cannot reproduce — for example a sourced
//! explicit hint without a live resolver, a hint value Go would write back
//! differently, or an input over the loader's bounds — is abstained
//! before any side effect, which leaves the row untouched for the Go worker.
//! So is a payload serde cannot decode: Go's decoder accepts inputs serde
//! rejects. Only transient database errors and Go-equivalent processor
//! outcomes consume one of the job's retries.
//!
//! Given a live materializer ([`Materializer::from_core_with_resolver`]), the
//! worker also claims the explicit flags-on payload (all three attach flags
//! true; [`ProcessTorrentAdmission::FlagsOffOrOn`]). With
//! [`ProcessTorrentWorkerConfig::organic_max_priority`] set it instead claims
//! organic payloads up to that priority ([`ProcessTorrentAdmission::Organic`]):
//! the DHT, importer, batch, CLI and retry shapes, whose omitted attach flags
//! the classifier resolves to the core defaults. Its composition then runs
//! on tokio's blocking pool, since classification awaits local search and
//! TMDB. A TMDB call answered with a status other than 401 fails only its
//! torrent, as in Go: the torrent goes into the retry job, or into the job's
//! error when every torrent failed, and the others keep resolving. Any other
//! resolver failure (a 401, a failed key validation, a transport error or
//! timeout, a status whose reason phrase cannot be rebuilt, an undecodable
//! body, a local search error)
//! abstains the job before any side effect; the `live` module says why for
//! each.
//!
//! Each abstention is counted with a `reason` label: `payload`, `hint`,
//! `ambiguous`, `resolver`, `conflict`, or `unsupported`.
//!
//! A database error while claiming or settling never stops the worker. Like
//! Go's `serverHandler`, it logs `error handling job`, waits one check
//! interval, and polls again; the claim transaction rolls back, so the row is
//! claimed again once the database recovers.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bitmagnet_queue::{
    AdmittedConsumeOutcome, ConsumeOutcome, DequeuedJob, JobVerdict, ProcessTorrentAdmission,
    ProcessTorrentParams, QueuePgError, QueueStore,
};
use futures::FutureExt;
use prometheus::IntCounterVec;

use crate::writer_plan::load_live_writer_plan;
use crate::{
    load_writer_plan, persist_writer_plan, BlockingManager, LoadError, MaterializeError,
    Materializer, WriterLoadError, WriterPlanError,
};

/// Why the worker left a claimed job to Go: the `reason` label of
/// `outcome="abstained"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AbstainReason {
    /// The payload does not decode, or its workflow or flags are outside the
    /// admission.
    Payload,
    /// An explicit hint the writer cannot reproduce, or a sourced one on a
    /// worker without a live resolver.
    Hint,
    /// Associations Go picks among by row order; unhinted torrents included,
    /// so it is kept apart from `Hint`.
    Ambiguous,
    /// A content resolver failure that Go's text cannot be rebuilt for.
    Resolver,
    /// Two torrents reuse one content key with different images.
    Conflict,
    /// Any other loader or composer limit.
    Unsupported,
}

impl AbstainReason {
    const fn label(self) -> &'static str {
        match self {
            Self::Payload => "payload",
            Self::Hint => "hint",
            Self::Ambiguous => "ambiguous",
            Self::Resolver => "resolver",
            Self::Conflict => "conflict",
            Self::Unsupported => "unsupported",
        }
    }

    fn of(error: &WriterPlanError) -> Self {
        match error {
            WriterPlanError::ClassifierWorkflowUnsupported
            | WriterPlanError::AttachFlagsNotExplicitlyDisabled
            | WriterPlanError::AttachFlagsNotExplicitlyUniform
            | WriterPlanError::OrganicWorkflowUnsupported
            | WriterPlanError::OrganicFlagsUnsupported
            | WriterPlanError::Materialize(MaterializeError::UnsupportedFlag { .. }) => {
                Self::Payload
            }
            WriterPlanError::AttachHintUnsupported { .. } => Self::Hint,
            WriterPlanError::AmbiguousContentReuse { .. }
            | WriterPlanError::AmbiguousPreservedContentType { .. } => Self::Ambiguous,
            WriterPlanError::Materialize(MaterializeError::ResolverFailed(_)) => Self::Resolver,
            WriterPlanError::Load(WriterLoadError::ConflictingReusableContent { .. }) => {
                Self::Conflict
            }
            _ => Self::Unsupported,
        }
    }
}

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
    /// With a live materializer, claim organic payloads up to this priority
    /// instead of the explicit flags-on ones
    /// ([`ProcessTorrentAdmission::Organic`]). Ignored without one.
    pub organic_max_priority: Option<i32>,
}

impl Default for ProcessTorrentWorkerConfig {
    fn default() -> Self {
        Self {
            check_interval: DEFAULT_CHECK_INTERVAL,
            job_timeout: DEFAULT_JOB_TIMEOUT,
            abstain_cooloff: DEFAULT_ABSTAIN_COOLOFF,
            max_excluded: DEFAULT_MAX_EXCLUDED,
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
            organic_max_priority: None,
        }
    }
}

/// One serial persisting `process_torrent` consumer; only classification
/// within a job is parallel (see [`Materializer::with_classify_parallelism`]).
pub struct ProcessTorrentWorker {
    store: QueueStore,
    materializer: Arc<Materializer>,
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
            materializer: Arc::new(materializer),
            blocking_manager,
            config,
            excluded: Mutex::new(VecDeque::new()),
            metrics: None,
        }
    }

    /// Count settled and abstained jobs by `outcome` label, and abstained ones
    /// by `reason` too.
    #[must_use]
    pub fn with_metrics(mut self, metrics: IntCounterVec) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// The payloads this worker claims: flags-on or organic ones only with a
    /// live materializer.
    #[must_use]
    pub fn admission(&self) -> ProcessTorrentAdmission {
        match (
            self.materializer.is_live(),
            self.config.organic_max_priority,
        ) {
            (false, _) => ProcessTorrentAdmission::FlagsOff,
            (true, None) => ProcessTorrentAdmission::FlagsOffOrOn,
            (true, Some(max_priority)) => ProcessTorrentAdmission::Organic { max_priority },
        }
    }

    /// Claim, run, and settle at most one admitted job.
    ///
    /// A database error is logged and counted before it is returned.
    pub async fn run_once(&self) -> Result<AdmittedConsumeOutcome, QueuePgError> {
        let excluded = self.excluded_ids();
        let admission = self.admission();
        let abstained = OnceLock::new();
        let outcome = self
            .store
            .consume_admitted_process_torrent_with(admission, &excluded, |job| {
                self.handle(job, admission, &abstained)
            })
            .await;
        match &outcome {
            Ok(outcome) => self.observe(outcome, abstained.get().copied()),
            Err(error) => {
                tracing::error!(%error, "error handling process_torrent job");
                self.count("error");
            }
        }
        outcome
    }

    /// Poll until `shutdown` resolves.
    ///
    /// The loop self-chains after a claim and sleeps `check_interval` when no
    /// admitted job was eligible or the attempt failed with a database error,
    /// like Go's `serverHandler.start`. Errors never end the loop.
    ///
    /// A job in flight when `shutdown` resolves gets `shutdown_grace` to
    /// settle. After that it is dropped, which rolls back its claim
    /// transaction: the row keeps its status, retries, and error, so it is
    /// claimed again later. This is what Go does when its queue context is
    /// cancelled on stop; side effects already committed by the writer stay,
    /// and the next run of the job redoes them.
    pub async fn run_until<S>(&self, shutdown: S)
    where
        S: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        loop {
            if (&mut shutdown).now_or_never().is_some() {
                return;
            }
            let run = self.run_once();
            tokio::pin!(run);
            let outcome = tokio::select! {
                biased;
                outcome = &mut run => outcome,
                () = &mut shutdown => return self.drain(run).await,
            };
            if matches!(
                outcome,
                Ok(AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty)) | Err(_)
            ) {
                tokio::select! {
                    biased;
                    () = &mut shutdown => return,
                    () = tokio::time::sleep(self.config.check_interval) => {}
                }
            } else {
                tokio::task::yield_now().await;
            }
        }
    }

    /// Let the in-flight `run` settle within `shutdown_grace`, else drop it.
    async fn drain<F>(&self, run: Pin<&mut F>)
    where
        F: Future<Output = Result<AdmittedConsumeOutcome, QueuePgError>>,
    {
        // `run_once` has already logged and counted a database error.
        if tokio::time::timeout(self.config.shutdown_grace, run)
            .await
            .is_ok()
        {
            return;
        }
        tracing::warn!(
            shutdown_grace = %go_duration(self.config.shutdown_grace),
            "abandoning the in-flight process_torrent job at shutdown; its claim rolls back and the row stays queued"
        );
    }

    /// Run `job`; on an abstention, record its reason in `abstained`.
    async fn handle(
        &self,
        job: DequeuedJob,
        admission: ProcessTorrentAdmission,
        abstained: &OnceLock<AbstainReason>,
    ) -> JobVerdict {
        match tokio::time::timeout(self.config.job_timeout, self.process(&job, admission)).await {
            Ok(Ok(verdict)) => verdict,
            Ok(Err((reason, text))) => {
                let _ = abstained.set(reason);
                JobVerdict::Abstain(text)
            }
            Err(_) => JobVerdict::Fail(format!(
                "job exceeded its {} timeout: context deadline exceeded",
                go_duration(self.config.job_timeout)
            )),
        }
    }

    /// The job's verdict, or why and how it is abstained.
    async fn process(
        &self,
        job: &DequeuedJob,
        admission: ProcessTorrentAdmission,
    ) -> Result<JobVerdict, (AbstainReason, String)> {
        // Go's decoder is more lenient than serde (a `0x` hash prefix, JSON
        // null for a slice, case-insensitive keys), so a payload serde rejects
        // may be one Go processes. Leave it to Go rather than spend a retry.
        let params: ProcessTorrentParams = match serde_json::from_str(&job.payload) {
            Ok(params) => params,
            Err(error) => {
                tracing::info!(job_id = %job.id, %error, "abstaining from undecodable process_torrent payload");
                return Err((AbstainReason::Payload, format!("payload: {error}")));
            }
        };
        let pool = self.store.pool();
        let plan = if self.materializer.is_live() {
            load_live_writer_plan(pool, &self.materializer, &params, admission).await
        } else {
            load_writer_plan(pool, &self.materializer, &params).await
        };
        let plan = match plan {
            Ok(plan) => plan,
            Err(error) if is_transient_plan_error(&error) => return Ok(go_handler_failure(&error)),
            Err(WriterPlanError::Materialize(MaterializeError::ResolverFailed(failure))) => {
                tracing::warn!(
                    job_id = %job.id,
                    info_hash = %failure.info_hash,
                    call = failure.call,
                    error = %failure.error,
                    "abstaining from process_torrent job after a content resolver failure"
                );
                return Err((
                    AbstainReason::Resolver,
                    format!("content resolver: {failure}"),
                ));
            }
            Err(error) => {
                let reason = AbstainReason::of(&error);
                tracing::info!(
                    job_id = %job.id,
                    %error,
                    reason = reason.label(),
                    detail = error.abstain_detail(),
                    "abstaining from unsupported process_torrent job"
                );
                return Err((reason, error.to_string()));
            }
        };
        Ok(
            match persist_writer_plan(pool, &plan, &self.store, self.blocking_manager.as_ref())
                .await
            {
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
            },
        )
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

    fn observe(&self, outcome: &AdmittedConsumeOutcome, abstained: Option<AbstainReason>) {
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
                drop(excluded);
                // Only the handler abstains, and it always records why.
                let reason = abstained.unwrap_or(AbstainReason::Unsupported);
                self.count_reason("abstained", reason.label());
                return;
            }
        };
        self.count(label);
    }

    fn count(&self, outcome: &str) {
        self.count_reason(outcome, "");
    }

    fn count_reason(&self, outcome: &str, reason: &str) {
        if let Some(metrics) = &self.metrics {
            metrics.with_label_values(&[outcome, reason]).inc();
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
            "process_torrent jobs claimed by the Rust worker, by outcome, and abstained ones by reason (payload, hint, ambiguous, resolver, conflict, unsupported); `error` counts attempts that failed with a database error",
        ),
        &["outcome", "reason"],
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
                info_hash: "aa".repeat(20),
                detail: "languages",
            }
        ));
    }

    #[test]
    fn abstentions_are_labelled_by_their_cause() {
        let hash = "aa".repeat(20);
        for (error, reason) in [
            (
                WriterPlanError::ClassifierWorkflowUnsupported,
                AbstainReason::Payload,
            ),
            (
                WriterPlanError::AttachFlagsNotExplicitlyDisabled,
                AbstainReason::Payload,
            ),
            (
                WriterPlanError::AttachFlagsNotExplicitlyUniform,
                AbstainReason::Payload,
            ),
            (
                WriterPlanError::OrganicWorkflowUnsupported,
                AbstainReason::Payload,
            ),
            (
                WriterPlanError::OrganicFlagsUnsupported,
                AbstainReason::Payload,
            ),
            (
                WriterPlanError::Materialize(MaterializeError::UnsupportedFlag {
                    name: "tmdb_enabled".to_owned(),
                    value: serde_json::Value::from(1),
                }),
                AbstainReason::Payload,
            ),
            (
                WriterPlanError::AttachHintUnsupported {
                    info_hash: hash.clone(),
                    detail: "episodes",
                },
                AbstainReason::Hint,
            ),
            (
                WriterPlanError::AmbiguousContentReuse {
                    info_hash: hash.clone(),
                },
                AbstainReason::Ambiguous,
            ),
            (
                WriterPlanError::AmbiguousPreservedContentType {
                    info_hash: hash.clone(),
                },
                AbstainReason::Ambiguous,
            ),
            (
                WriterPlanError::Materialize(MaterializeError::ResolverFailed(
                    crate::ResolverFailure {
                        info_hash: hash.clone(),
                        call: "tmdb_search_movie",
                        error: "operation timed out".to_owned(),
                    },
                )),
                AbstainReason::Resolver,
            ),
            (
                WriterPlanError::Load(WriterLoadError::ConflictingReusableContent {
                    key: "movie:tmdb:42".to_owned(),
                }),
                AbstainReason::Conflict,
            ),
            (
                WriterPlanError::Load(WriterLoadError::RequestTooLarge {
                    actual: 101,
                    limit: 100,
                }),
                AbstainReason::Unsupported,
            ),
            (
                WriterPlanError::ReusableContentNotFullyHydrated { info_hash: hash },
                AbstainReason::Unsupported,
            ),
        ] {
            assert_eq!(AbstainReason::of(&error), reason, "{error}");
        }
        assert_eq!(
            [
                AbstainReason::Payload,
                AbstainReason::Hint,
                AbstainReason::Ambiguous,
                AbstainReason::Resolver,
                AbstainReason::Conflict,
                AbstainReason::Unsupported,
            ]
            .map(AbstainReason::label),
            [
                "payload",
                "hint",
                "ambiguous",
                "resolver",
                "conflict",
                "unsupported"
            ]
        );
    }

    #[test]
    fn handler_failures_carry_go_prefix() {
        assert_eq!(
            go_handler_failure(&"missing 2 info hashes"),
            JobVerdict::Fail("job failed to process: missing 2 info hashes".to_owned())
        );
    }
}
