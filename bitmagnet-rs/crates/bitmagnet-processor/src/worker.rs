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
//! A failed whole-torrent block fails the attempt with Go's text, `blocking
//! whole-torrent deletes failed`, which names no cause. The worker logs the
//! cause with the job's ID, the failed step, and the SQLSTATE, and counts the
//! attempt by step and SQLSTATE class, with a missing grant (`42501`) apart
//! from the rest of its class: it would otherwise show only as failed jobs.
//!
//! The binary also flushes the blocking manager's buffer on its
//! `max_flush_wait`, between jobs or while idle. A blocked hash then reaches
//! the persisted filter Go's crawler reads within that period, without
//! another blocking job or a shutdown. A failed timed flush is logged and
//! counted the same way, without a job.
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

use bitmagnet_blocking::{BlockingFinalizeOutcome, BlockingFinalizer, BlockingStoreOp};
use bitmagnet_queue::{
    AdmittedConsumeOutcome, ConsumeOutcome, DequeuedJob, JobVerdict, ProcessTorrentAdmission,
    ProcessTorrentParams, QueuePgError, QueueStore,
};
use futures::FutureExt;
use prometheus::IntCounterVec;

use crate::writer_plan::load_live_writer_plan;
use crate::{
    check_payload_key_set, load_writer_plan, persist_writer_plan, BlockingFailure, BlockingManager,
    LoadError, MaterializeError, Materializer, PersistWriterPlanError, WriterLoadError,
    WriterPlanError,
};

/// Why the worker left a claimed job to Go: the `reason` label of
/// `outcome="abstained"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AbstainReason {
    /// The payload has a key outside Go's key set, does not decode, or its
    /// workflow or flags are outside the admission.
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
    blocking_failures: Option<IntCounterVec>,
    timed_flush: Option<TimedFlush>,
}

/// The blocking manager's flush on a timer: see
/// [`ProcessTorrentWorker::with_timed_blocking_flush`].
struct TimedFlush {
    finalizer: Arc<dyn BlockingFinalizer>,
    every: Duration,
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
            blocking_failures: None,
            timed_flush: None,
        }
    }

    /// Flush `finalizer`, the worker's blocking manager, every `every` (the
    /// manager's `max_flush_wait`) while [`Self::run_until`] runs.
    ///
    /// A job deletes the torrents it blocks itself, but the manager adds
    /// their hashes to the persisted filter only when an operation finds the
    /// wait elapsed. This worker's only operation is a job's `block`, so
    /// without the timer a hash stays out of the filter until the next
    /// blocking job or shutdown. Meanwhile Go's crawler, which reloads the
    /// filter, can re-admit the torrent. Go's own crawler bounds that with
    /// frequent `Filter` calls. A zero period leaves it off.
    #[must_use]
    pub fn with_timed_blocking_flush(
        mut self,
        finalizer: Arc<dyn BlockingFinalizer>,
        every: Duration,
    ) -> Self {
        self.timed_flush = (!every.is_zero()).then_some(TimedFlush { finalizer, every });
        self
    }

    /// Count settled and abstained jobs by `outcome` label, and abstained ones
    /// by `reason` too.
    #[must_use]
    pub fn with_metrics(mut self, metrics: IntCounterVec) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Count whole-torrent blocking failures by `op` and `sqlstate_class`
    /// (see [`register_blocking_failure_metrics`]).
    #[must_use]
    pub fn with_blocking_failure_metrics(mut self, blocking_failures: IntCounterVec) -> Self {
        self.blocking_failures = Some(blocking_failures);
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
    ///
    /// With [`Self::with_timed_blocking_flush`], a flush falls due a period
    /// after the loop starts and after each timed flush, and runs between jobs
    /// or while idle: never beside a job, so it cannot wait on a `block` that
    /// job holds, and never cut short by `shutdown`. One in flight at shutdown
    /// completes, then this returns without claiming another job.
    pub async fn run_until<S>(&self, shutdown: S)
    where
        S: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let mut flush_at = self
            .timed_flush
            .as_ref()
            .map(|timed| tokio::time::Instant::now() + timed.every);
        loop {
            if (&mut shutdown).now_or_never().is_some() {
                return;
            }
            // A shutdown during the flush claims nothing more.
            if self.flush_if_due(&mut flush_at).await && (&mut shutdown).now_or_never().is_some() {
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
                // A timed flush due while idle runs without moving the poll.
                let poll_at = tokio::time::Instant::now() + self.config.check_interval;
                loop {
                    tokio::select! {
                        biased;
                        () = &mut shutdown => return,
                        () = tokio::time::sleep_until(poll_at) => break,
                        () = sleep_until_some(flush_at) => {
                            self.flush_if_due(&mut flush_at).await;
                        }
                    }
                }
            } else {
                tokio::task::yield_now().await;
            }
        }
    }

    /// [`Self::run_until`] `shutdown`, then flush `finalizer`, the blocking
    /// manager, once: Go flushes it when the application stops. A timed flush
    /// never outlives the loop, so this one never overlaps it and finds only
    /// what is still buffered.
    ///
    /// # Errors
    ///
    /// The flush's failure; the manager keeps its buffer.
    pub async fn run_then_flush<S>(
        &self,
        shutdown: S,
        finalizer: &dyn BlockingFinalizer,
    ) -> Result<BlockingFinalizeOutcome, bitmagnet_blocking::BlockingError>
    where
        S: Future<Output = ()>,
    {
        self.run_until(shutdown).await;
        let outcome = finalizer.finalize().await?;
        if let BlockingFinalizeOutcome::Flushed { unique_hash_count } = outcome {
            tracing::info!(
                unique_hash_count,
                "flushed buffered whole-torrent blocks on shutdown"
            );
        }
        Ok(outcome)
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
        // serde ignores a key it does not know, where Go may bind it, so the
        // keys are checked first, under every admission.
        if let Err(error) = check_payload_key_set(&job.payload) {
            tracing::info!(job_id = %job.id, %error, "abstaining from process_torrent payload outside Go's key set");
            return Err((AbstainReason::Payload, format!("payload: {error}")));
        }
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
                Err(error) => self.persist_failure(&job.id, &error),
            },
        )
    }

    /// Go's handler failure for a persist error. A blocking failure's cause,
    /// which that text leaves out, is logged and counted first.
    fn persist_failure(&self, job_id: &str, error: &PersistWriterPlanError) -> JobVerdict {
        if let PersistWriterPlanError::Persist { source, .. } = error {
            if let Some(failure) = BlockingFailure::of(source) {
                let op = self.count_blocking_failure(&failure);
                tracing::error!(
                    job_id = %job_id,
                    op,
                    sqlstate = failure.sqlstate.as_deref(),
                    error = %failure.error,
                    "blocking whole-torrent deletes failed"
                );
            }
        }
        go_handler_failure(error)
    }

    /// Count `failure` by step and SQLSTATE class; returns its `op` label.
    fn count_blocking_failure(&self, failure: &BlockingFailure) -> &'static str {
        let op = failure.op.map_or(OTHER_LABEL, BlockingStoreOp::label);
        if let Some(blocking_failures) = &self.blocking_failures {
            blocking_failures
                .with_label_values(&[op, sqlstate_class(failure.sqlstate.as_deref())])
                .inc();
        }
        op
    }

    /// Flush the blocking manager's buffer if the timed flush is due, then
    /// schedule the next one a period after it ends; whether it was due. An
    /// empty buffer is not flushed. A failure keeps the buffer, as a failed
    /// `block` does, for the next flush, and is logged and counted like a
    /// job's.
    async fn flush_if_due(&self, flush_at: &mut Option<tokio::time::Instant>) -> bool {
        let (Some(timed), Some(at)) = (&self.timed_flush, *flush_at) else {
            return false;
        };
        if tokio::time::Instant::now() < at {
            return false;
        }
        match timed.finalizer.finalize().await {
            Ok(BlockingFinalizeOutcome::NothingPending) => {}
            Ok(BlockingFinalizeOutcome::Flushed { unique_hash_count }) => {
                tracing::info!(
                    unique_hash_count,
                    "flushed buffered whole-torrent blocks on the flush interval"
                );
            }
            Err(error) => {
                let failure = BlockingFailure::of_flush(&error);
                let op = self.count_blocking_failure(&failure);
                tracing::error!(
                    op,
                    sqlstate = failure.sqlstate.as_deref(),
                    error = %failure.error,
                    "blocking whole-torrent deletes failed on the flush interval"
                );
            }
        }
        *flush_at = Some(tokio::time::Instant::now() + timed.every);
        true
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

/// The `op` and `sqlstate_class` label of a failure that names neither.
const OTHER_LABEL: &str = "other";
/// The `sqlstate_class` of a missing grant (insufficient_privilege), kept
/// apart from the rest of class `42`, which is schema drift (`42P01`,
/// `42703`, ...).
const INSUFFICIENT_PRIVILEGE: &str = "42501";
/// The SQLSTATE classes the blocking-failure counter names: connection,
/// transaction rollback, access rule, resources, and operator intervention.
/// Any other class is `other`.
const SQLSTATE_CLASSES: [&str; 5] = ["08", "40", "42", "53", "57"];
/// The `sqlstate_class` of a failure without a SQLSTATE.
const NO_SQLSTATE_LABEL: &str = "none";

fn sqlstate_class(sqlstate: Option<&str>) -> &'static str {
    match sqlstate {
        None => NO_SQLSTATE_LABEL,
        Some(INSUFFICIENT_PRIVILEGE) => INSUFFICIENT_PRIVILEGE,
        Some(sqlstate) => SQLSTATE_CLASSES
            .into_iter()
            .find(|class| sqlstate.starts_with(class))
            .unwrap_or(OTHER_LABEL),
    }
}

/// Register the worker's whole-torrent blocking-failure counter in
/// `registry`, with every label pair at zero, so it is scraped before the
/// first failure.
pub fn register_blocking_failure_metrics(
    registry: &prometheus::Registry,
) -> Result<IntCounterVec, prometheus::Error> {
    let counter = IntCounterVec::new(
        prometheus::Opts::new(
            "bitmagnet_process_torrent_worker_blocking_failures_total",
            "Whole-torrent blocking failures per failed process_torrent attempt, each failing it with `blocking whole-torrent deletes failed`, and per failed timed flush, by failed step (transaction, delete_torrents, bloom_filter, other) and SQLSTATE class (08, 40, 42501 = missing grant, 42 = rest of class 42, 53, 57, other, none)",
        ),
        &["op", "sqlstate_class"],
    )?;
    for op in BlockingStoreOp::ALL
        .map(BlockingStoreOp::label)
        .into_iter()
        .chain([OTHER_LABEL])
    {
        for class in [INSUFFICIENT_PRIVILEGE]
            .into_iter()
            .chain(SQLSTATE_CLASSES)
            .chain([OTHER_LABEL, NO_SQLSTATE_LABEL])
        {
            counter.with_label_values(&[op, class]);
        }
    }
    registry.register(Box::new(counter.clone()))?;
    Ok(counter)
}

/// Sleep until `at`, or forever without one.
async fn sleep_until_some(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
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

    struct UnusedBlocker;

    impl BlockingManager for UnusedBlocker {
        fn block<'a>(
            &'a self,
            _info_hashes: &'a [String],
        ) -> futures::future::BoxFuture<'a, Result<(), crate::BoxError>> {
            panic!("no job here reaches the writer")
        }
    }

    /// Every admission checks the keys before serde and before the database: no
    /// server listens on the pool's address, so a job that reached the
    /// database would fail as Go's handler does, not abstain.
    #[tokio::test]
    async fn a_payload_outside_gos_key_set_abstains_before_any_database_access() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(200))
            .connect_lazy("postgres://postgres@127.0.0.1:1/unreachable")
            .expect("lazy pool");
        let worker = ProcessTorrentWorker::new(
            QueueStore::new(pool),
            Materializer::from_core().expect("compile embedded classifier"),
            Arc::new(UnusedBlocker),
            ProcessTorrentWorkerConfig::default(),
        );
        let job = |payload: String| DequeuedJob {
            id: "job".to_owned(),
            fingerprint: String::new(),
            queue: bitmagnet_queue::PROCESS_TORRENT.to_owned(),
            original_status: bitmagnet_queue::QueueJobStatus::Pending,
            payload,
            retries: 0,
            max_retries: 2,
            priority: 0,
        };
        let hash = "a1".repeat(20);
        let off = r#"{"apis_enabled":false,"local_search_enabled":false,"tmdb_enabled":false}"#;
        let explicit = format!(r#""ClassifierWorkflow":"default","ClassifierFlags":{off}"#);
        let refused = [
            (
                format!(r#"{{{explicit},"InfoHashes":["{hash}"],"classifierworkflow":"custom"}}"#),
                r#"payload: unexpected key "classifierworkflow""#,
            ),
            (
                format!(r#"{{"InfoHashes":["{hash}"],"Extra":true}}"#),
                r#"payload: unexpected key "Extra""#,
            ),
            // serde refuses this one too; the key is what is reported.
            (
                format!(r#"{{"InfoHashes":"{hash}","Extra":true}}"#),
                r#"payload: unexpected key "Extra""#,
            ),
            (
                format!(
                    r#"{{"ClassifierWorkflow":"default","ClassifierFlags":{},"InfoHashes":["{hash}"]}}"#,
                    off.replace('}', r#","delete_xxx":false}"#)
                ),
                r#"payload: classifier flag "delete_xxx" is not an attach flag"#,
            ),
            (
                format!(r#"{{"ClassifierFlags":null,"InfoHashes":["{hash}"]}}"#),
                "payload: ClassifierFlags is not an object",
            ),
        ];
        for admission in [
            ProcessTorrentAdmission::FlagsOff,
            ProcessTorrentAdmission::FlagsOffOrOn,
            ProcessTorrentAdmission::Organic { max_priority: 0 },
        ] {
            for (payload, text) in &refused {
                match worker.process(&job(payload.clone()), admission).await {
                    Err((AbstainReason::Payload, abstained)) => {
                        assert_eq!(abstained, *text, "{admission:?} {payload}");
                    }
                    other => panic!("{admission:?} {payload}: {other:?}"),
                }
            }
            // The control: a payload inside the key set goes on to the database.
            let control = format!(r#"{{{explicit},"InfoHashes":["{hash}"]}}"#);
            assert!(
                matches!(
                    worker.process(&job(control), admission).await,
                    Ok(JobVerdict::Fail(ref error)) if error.starts_with(GO_HANDLER_ERROR_PREFIX)
                ),
                "{admission:?}"
            );
        }
    }

    #[test]
    fn handler_failures_carry_go_prefix() {
        assert_eq!(
            go_handler_failure(&"missing 2 info hashes"),
            JobVerdict::Fail("job failed to process: missing 2 info hashes".to_owned())
        );
    }

    #[test]
    fn sqlstates_count_by_named_class() {
        for (sqlstate, class) in [
            (Some("08006"), "08"),
            (Some("40001"), "40"),
            (Some("42501"), "42501"),
            (Some("42P01"), "42"),
            (Some("42703"), "42"),
            (Some("53100"), "53"),
            (Some("57014"), "57"),
            (Some("23505"), "other"),
            (None, "none"),
        ] {
            assert_eq!(sqlstate_class(sqlstate), class, "{sqlstate:?}");
        }
    }

    /// A writer the log capture below appends to.
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("capture lock")
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Without PostgreSQL, a store failure with no SQLSTATE: logged and
    /// counted as `other`/`none` under Go's job text. Another persist error
    /// keeps its own text and logs nothing here. `worker_pg` covers the
    /// PostgreSQL store's permission failures.
    #[tokio::test]
    async fn a_blocking_failure_is_logged_and_counted_under_gos_job_text() {
        let registry = prometheus::Registry::new();
        let failures = register_blocking_failure_metrics(&registry).unwrap();
        let exposition = prometheus::TextEncoder::new()
            .encode_to_string(&registry.gather())
            .unwrap();
        let series = |text: &str| {
            text.lines()
                .filter(|line| {
                    line.starts_with("bitmagnet_process_torrent_worker_blocking_failures_total{")
                })
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        assert_eq!(series(&exposition).len(), 4 * 8, "{exposition}");
        assert!(series(&exposition).iter().all(|line| line.ends_with(" 0")));

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://postgres@127.0.0.1:1/unreachable")
            .expect("lazy pool");
        let worker = ProcessTorrentWorker::new(
            QueueStore::new(pool),
            Materializer::from_core().expect("compile embedded classifier"),
            Arc::new(UnusedBlocker),
            ProcessTorrentWorkerConfig::default(),
        )
        .with_blocking_failure_metrics(failures.clone());
        let blocking = PersistWriterPlanError::Persist {
            retry: None,
            source: crate::PersistError::Blocking(Box::new(
                bitmagnet_blocking::BlockingError::Store(Box::new(std::io::Error::other(
                    "store unavailable",
                ))),
            )),
        };
        let database = PersistWriterPlanError::Persist {
            retry: None,
            source: crate::PersistError::Database(sqlx::Error::PoolTimedOut),
        };

        let buffer = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&buffer);
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(move || Capture(Arc::clone(&sink)))
            .finish();
        let (blocked, failed) = tracing::subscriber::with_default(subscriber, || {
            // A callsite's interest cached before this subscriber existed, on
            // another test's thread, would hide its lines.
            tracing::callsite::rebuild_interest_cache();
            (
                worker.persist_failure("job-1", &blocking),
                worker.persist_failure("job-2", &database),
            )
        });

        assert_eq!(
            blocked,
            JobVerdict::Fail("job failed to process: blocking whole-torrent deletes failed".into())
        );
        assert_eq!(
            failed,
            JobVerdict::Fail(format!(
                "job failed to process: {}",
                sqlx::Error::PoolTimedOut
            ))
        );
        let text = String::from_utf8(buffer.lock().expect("capture lock").clone()).unwrap();
        assert_eq!(
            text.lines().map(str::trim_start).collect::<Vec<_>>(),
            [
                "ERROR bitmagnet_processor::worker: blocking whole-torrent deletes failed \
                 job_id=job-1 op=\"other\" error=atomic blocking-store commit failed: \
                 store unavailable"
            ]
        );
        let exposition = prometheus::TextEncoder::new()
            .encode_to_string(&registry.gather())
            .unwrap();
        assert_eq!(
            series(&exposition)
                .into_iter()
                .filter(|line| !line.ends_with(" 0"))
                .collect::<Vec<_>>(),
            ["bitmagnet_process_torrent_worker_blocking_failures_total\
                 {op=\"other\",sqlstate_class=\"none\"} 1"]
        );
    }

    /// A finalizer that returns scripted outcomes, then `NothingPending`. With
    /// `hold`, its first call waits for `release` after signalling `entered`.
    #[derive(Default)]
    struct ScriptedFinalizer {
        outcomes:
            Mutex<VecDeque<Result<BlockingFinalizeOutcome, bitmagnet_blocking::BlockingError>>>,
        calls: std::sync::atomic::AtomicUsize,
        hold: bool,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl ScriptedFinalizer {
        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl BlockingFinalizer for ScriptedFinalizer {
        async fn finalize(
            &self,
        ) -> Result<BlockingFinalizeOutcome, bitmagnet_blocking::BlockingError> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.hold && call == 0 {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.outcomes
                .lock()
                .expect("outcomes lock")
                .pop_front()
                .unwrap_or(Ok(BlockingFinalizeOutcome::NothingPending))
        }
    }

    /// A worker whose every poll fails at once: no server listens on the
    /// pool's address, so it idles a full `check_interval` after each.
    fn idle_worker(check_interval: Duration) -> ProcessTorrentWorker {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(20))
            .connect_lazy("postgres://postgres@127.0.0.1:1/unreachable")
            .expect("lazy pool");
        ProcessTorrentWorker::new(
            QueueStore::new(pool),
            Materializer::from_core().expect("compile embedded classifier"),
            Arc::new(UnusedBlocker),
            ProcessTorrentWorkerConfig {
                check_interval,
                ..ProcessTorrentWorkerConfig::default()
            },
        )
    }

    /// While idle the timed flush runs every period without an extra poll; a
    /// failure is logged and counted without a job, and the next period
    /// retries; a flush is logged with its hash count; an empty one is silent.
    #[tokio::test]
    async fn the_timed_flush_runs_while_idle_logs_counts_and_retries() {
        let registry = prometheus::Registry::new();
        let jobs = register_worker_metrics(&registry).unwrap();
        let failures = register_blocking_failure_metrics(&registry).unwrap();
        let finalizer = Arc::new(ScriptedFinalizer {
            outcomes: Mutex::new(VecDeque::from([
                Err(bitmagnet_blocking::BlockingError::Store(Box::new(
                    std::io::Error::other("store unavailable"),
                ))),
                Ok(BlockingFinalizeOutcome::Flushed {
                    unique_hash_count: 2,
                }),
            ])),
            ..ScriptedFinalizer::default()
        });
        let worker = idle_worker(Duration::from_secs(3_600))
            .with_metrics(jobs.clone())
            .with_blocking_failure_metrics(failures)
            .with_timed_blocking_flush(finalizer.clone(), Duration::from_millis(40));
        assert!(idle_worker(Duration::from_secs(1))
            .with_timed_blocking_flush(finalizer.clone(), Duration::ZERO)
            .timed_flush
            .is_none());

        let buffer = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&buffer);
        let _logs = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_ansi(false)
                .without_time()
                .with_writer(move || Capture(Arc::clone(&sink)))
                .finish(),
        );
        // A callsite's interest cached before this subscriber existed, on
        // another test's thread, would hide its lines.
        tracing::callsite::rebuild_interest_cache();
        tokio::time::timeout(
            Duration::from_secs(5),
            worker.run_until(async {
                while finalizer.calls() < 4 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }),
        )
        .await
        .expect("four timed flushes");

        assert_eq!(
            jobs.with_label_values(&["error", ""]).get(),
            1,
            "one poll: the flushes did not move it"
        );
        let text = String::from_utf8(buffer.lock().expect("capture lock").clone()).unwrap();
        let flush_lines = text
            .lines()
            .map(str::trim_start)
            .filter(|line| line.contains("flush interval"))
            .collect::<Vec<_>>();
        assert_eq!(
            flush_lines,
            [
                "ERROR bitmagnet_processor::worker: blocking whole-torrent deletes failed \
                 on the flush interval op=\"other\" error=atomic blocking-store commit \
                 failed: store unavailable",
                "INFO bitmagnet_processor::worker: flushed buffered whole-torrent blocks \
                 on the flush interval unique_hash_count=2",
            ]
        );
        let exposition = prometheus::TextEncoder::new()
            .encode_to_string(&registry.gather())
            .unwrap();
        assert_eq!(
            exposition
                .lines()
                .filter(|line| line
                    .starts_with("bitmagnet_process_torrent_worker_blocking_failures_total{"))
                .filter(|line| !line.ends_with(" 0"))
                .collect::<Vec<_>>(),
            ["bitmagnet_process_torrent_worker_blocking_failures_total\
                 {op=\"other\",sqlstate_class=\"none\"} 1"]
        );
    }

    /// With a period shorter than the check interval, as in production (300 s
    /// against the canary's 600 s), the flushes neither delay nor add polls,
    /// and each re-arms the timer rather than spinning.
    #[tokio::test]
    async fn a_short_period_neither_moves_the_poll_nor_spins() {
        let registry = prometheus::Registry::new();
        let jobs = register_worker_metrics(&registry).unwrap();
        let finalizer = Arc::new(ScriptedFinalizer::default());
        let worker = idle_worker(Duration::from_millis(200))
            .with_metrics(jobs.clone())
            .with_timed_blocking_flush(finalizer.clone(), Duration::from_millis(30));

        tokio::time::timeout(
            Duration::from_secs(5),
            worker.run_until(tokio::time::sleep(Duration::from_secs(1))),
        )
        .await
        .expect("the loop stops at shutdown");

        // One poll per 200 ms interval: at 0, 200, 400, 600, 800 ms, and maybe
        // at 1 s.
        let polls = jobs.with_label_values(&["error", ""]).get();
        assert!((4..=7).contains(&polls), "{polls} polls");
        // At most one flush per 30 ms period.
        let flushes = finalizer.calls();
        assert!((10..=34).contains(&flushes), "{flushes} flushes");
    }

    /// Shutdown does not cut a timed flush short or start another: the loop
    /// returns once the flush in flight completes, after exactly one call.
    #[tokio::test]
    async fn a_timed_flush_in_flight_at_shutdown_completes_once() {
        let finalizer = Arc::new(ScriptedFinalizer {
            hold: true,
            ..ScriptedFinalizer::default()
        });
        let worker = idle_worker(Duration::from_secs(3_600))
            .with_timed_blocking_flush(finalizer.clone(), Duration::from_millis(20));
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let run = worker.run_until(async {
            let _ = stopped.await;
        });
        tokio::pin!(run);

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                () = &mut run => panic!("the loop returned before shutdown"),
                () = finalizer.entered.notified() => {}
            }
        })
        .await
        .expect("a timed flush starts");
        stop.send(()).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut run)
                .await
                .is_err(),
            "shutdown waits for the flush in flight"
        );
        finalizer.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), &mut run)
            .await
            .expect("the loop returns once the flush completes");
        assert_eq!(finalizer.calls(), 1);
    }
}
