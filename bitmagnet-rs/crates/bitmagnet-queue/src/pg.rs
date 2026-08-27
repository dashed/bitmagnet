//! PostgreSQL queue consumer and processed-row shadow mirror.

use std::collections::BTreeMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};

use crate::backoff::calculate_backoff;
use crate::{fingerprint, QueueJobStatus, PROCESS_TORRENT_BATCH};

pub const PROCESS_TORRENT_SHADOW: &str = "process_torrent_shadow";
pub const SHADOW_JOB_ENVELOPE_VERSION: u8 = 1;
const DEADLINE_ERROR: &str = "the job did not complete before its deadline";

/// Exact identity of the settled Go job mirrored into the scratch queue.
///
/// The raw JSON payload is retained instead of normalizing it through
/// [`crate::ProcessTorrentParams`], so the consumer can prove that the archived
/// source row still has exactly the payload that the mirror observed. The
/// workflow, flags, classify mode, and requested hashes remain inside that
/// source-owned document.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ShadowJobEnvelopeV1 {
    pub schema_version: u8,
    pub source_job_id: String,
    /// PostgreSQL's exact textual `timestamptz` representation.
    pub source_ran_at: String,
    pub source_payload: Value,
}

impl ShadowJobEnvelopeV1 {
    #[must_use]
    pub fn new(source_job_id: String, source_ran_at: String, source_payload: Value) -> Self {
        Self {
            schema_version: SHADOW_JOB_ENVELOPE_VERSION,
            source_job_id,
            source_ran_at,
            source_payload,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QueuePgError {
    #[error("queue database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("invalid queue status '{0}'")]
    InvalidStatus(String),
    #[error("queue integer '{field}' is outside the supported range: {value}")]
    InvalidInteger { field: &'static str, value: i64 },
    #[error("invalid mirror configuration: {0}")]
    InvalidMirrorConfig(&'static str),
    #[error("invalid batch selection: {0}")]
    InvalidBatchSelection(&'static str),
    #[error("invalid batch payload: {0}")]
    InvalidBatchPayload(&'static str),
    #[error("selected info hash is not exactly 20 bytes: {0}")]
    InvalidInfoHashLength(usize),
    #[error("invalid producer payload JSON")]
    InvalidProducerPayload {
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid producer job: {0}")]
    InvalidProducerJob(&'static str),
    #[error("producer integer '{field}' is outside PostgreSQL integer range: {value}")]
    InvalidProducerInteger { field: &'static str, value: u64 },
    #[error("producer duration '{field}' is outside PostgreSQL interval range: {microseconds} microseconds")]
    InvalidProducerDuration {
        field: &'static str,
        microseconds: u128,
    },
    #[error("producer duration '{field}' has sub-microsecond precision that PostgreSQL interval cannot represent: {submicro_nanoseconds} nanoseconds")]
    InvalidProducerDurationPrecision {
        field: &'static str,
        submicro_nanoseconds: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DequeuedJob {
    pub id: String,
    pub fingerprint: String,
    pub queue: String,
    pub original_status: QueueJobStatus,
    pub payload: String,
    pub retries: u32,
    pub max_retries: u32,
    pub priority: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumeOutcome {
    Empty,
    Processed { job: DequeuedJob },
    RetryScheduled { job: DequeuedJob, delay: Duration },
    Failed { job: DequeuedJob, error: String },
}

#[derive(Clone)]
pub struct QueueStore {
    pool: PgPool,
}

impl QueueStore {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Claim, execute, and settle one job while retaining the row lock.
    pub async fn consume_one<H, Fut, E>(
        &self,
        queue: &str,
        handler: H,
    ) -> Result<ConsumeOutcome, QueuePgError>
    where
        H: FnOnce(DequeuedJob) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: std::fmt::Display,
    {
        self.consume_one_observed(queue, || {}, handler).await
    }

    async fn consume_one_observed<O, H, Fut, E>(
        &self,
        queue: &str,
        on_claim: O,
        handler: H,
    ) -> Result<ConsumeOutcome, QueuePgError>
    where
        O: FnOnce(),
        H: FnOnce(DequeuedJob) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: std::fmt::Display,
    {
        let mut tx = self.pool.begin().await?;
        let row = if queue == PROCESS_TORRENT_SHADOW {
            sqlx::query("SELECT * FROM public.ingest_shadow_claim_job()")
                .fetch_optional(&mut *tx)
                .await?
        } else if queue == PROCESS_TORRENT_BATCH {
            sqlx::query("SELECT * FROM public.process_torrent_batch_claim_job()")
                .fetch_optional(&mut *tx)
                .await?
        } else {
            sqlx::query(
                "SELECT id, fingerprint, queue, status::text AS status, payload::text AS payload, \
                        retries::bigint AS retries, max_retries::bigint AS max_retries, priority, \
                        deadline IS NOT NULL AND deadline < clock_timestamp() AS deadline_exceeded \
                 FROM queue_jobs \
                 WHERE queue = $1 AND status IN ('pending','retry') \
                   AND run_after <= clock_timestamp() \
                 ORDER BY (status = 'retry'), priority, run_after \
                 FOR UPDATE SKIP LOCKED \
                 LIMIT 1",
            )
            .bind(queue)
            .fetch_optional(&mut *tx)
            .await?
        };
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(ConsumeOutcome::Empty);
        };

        let status_text: String = row.try_get("status")?;
        let original_status = parse_status(&status_text)?;
        let mut job = DequeuedJob {
            id: row.try_get("id")?,
            fingerprint: row.try_get("fingerprint")?,
            queue: row.try_get("queue")?,
            original_status,
            payload: row.try_get("payload")?,
            retries: to_u32("retries", row.try_get("retries")?)?,
            max_retries: to_u32("max_retries", row.try_get("max_retries")?)?,
            priority: row.try_get("priority")?,
        };
        let deadline_exceeded: bool = row.try_get("deadline_exceeded")?;
        on_claim();

        let handler_error = if deadline_exceeded {
            Some(DEADLINE_ERROR.to_owned())
        } else {
            if original_status == QueueJobStatus::Retry {
                job.retries = job.retries.saturating_add(1);
            }
            match std::panic::catch_unwind(AssertUnwindSafe(|| handler(job.clone()))) {
                Ok(future) => match AssertUnwindSafe(future).catch_unwind().await {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error.to_string()),
                    Err(payload) => {
                        Some(format!("job handler panicked: {}", panic_message(&payload)))
                    }
                },
                Err(payload) => Some(format!("job handler panicked: {}", panic_message(&payload))),
            }
        };

        let outcome = if let Some(error) = handler_error {
            if job.retries < job.max_retries {
                let delay = calculate_backoff(job.retries);
                let delay_seconds = i64::try_from(delay.as_secs()).unwrap_or(i64::MAX);
                if queue == PROCESS_TORRENT_SHADOW {
                    sqlx::query("SELECT public.ingest_shadow_settle_retry($1, $2, $3, $4)")
                        .bind(&job.id)
                        .bind(i64::from(job.retries))
                        .bind(&error)
                        .bind(delay_seconds)
                        .execute(&mut *tx)
                        .await?;
                } else if queue == PROCESS_TORRENT_BATCH {
                    sqlx::query(
                        "SELECT public.process_torrent_batch_settle_retry(\
                           $1::text, $2::bigint, $3::text, $4::bigint\
                         )",
                    )
                    .bind(&job.id)
                    .bind(i64::from(job.retries))
                    .bind(&error)
                    .bind(delay_seconds)
                    .execute(&mut *tx)
                    .await?;
                } else {
                    sqlx::query(
                        "UPDATE queue_jobs \
                         SET status = 'retry', retries = $2, ran_at = clock_timestamp(), \
                             error = $3, \
                             run_after = clock_timestamp() + make_interval(secs => $4) \
                         WHERE id = $1",
                    )
                    .bind(&job.id)
                    .bind(i64::from(job.retries))
                    .bind(&error)
                    .bind(delay_seconds)
                    .execute(&mut *tx)
                    .await?;
                }
                ConsumeOutcome::RetryScheduled { job, delay }
            } else {
                if queue == PROCESS_TORRENT_SHADOW {
                    sqlx::query("SELECT public.ingest_shadow_settle_failed($1, $2, $3)")
                        .bind(&job.id)
                        .bind(i64::from(job.retries))
                        .bind(&error)
                        .execute(&mut *tx)
                        .await?;
                } else if queue == PROCESS_TORRENT_BATCH {
                    sqlx::query(
                        "SELECT public.process_torrent_batch_settle_failed(\
                           $1::text, $2::bigint, $3::text\
                         )",
                    )
                    .bind(&job.id)
                    .bind(i64::from(job.retries))
                    .bind(&error)
                    .execute(&mut *tx)
                    .await?;
                } else {
                    sqlx::query(
                        "UPDATE queue_jobs SET status = 'failed', retries = $2, \
                         ran_at = clock_timestamp(), error = $3 WHERE id = $1",
                    )
                    .bind(&job.id)
                    .bind(i64::from(job.retries))
                    .bind(&error)
                    .execute(&mut *tx)
                    .await?;
                }
                ConsumeOutcome::Failed { job, error }
            }
        } else {
            if queue == PROCESS_TORRENT_SHADOW {
                sqlx::query("SELECT public.ingest_shadow_settle_processed($1, $2)")
                    .bind(&job.id)
                    .bind(i64::from(job.retries))
                    .execute(&mut *tx)
                    .await?;
            } else if queue == PROCESS_TORRENT_BATCH {
                sqlx::query(
                    "SELECT public.process_torrent_batch_settle_processed(\
                       $1::text, $2::bigint\
                     )",
                )
                .bind(&job.id)
                .bind(i64::from(job.retries))
                .execute(&mut *tx)
                .await?;
            } else {
                sqlx::query(
                    "UPDATE queue_jobs SET status = 'processed', retries = $2, \
                     ran_at = clock_timestamp() WHERE id = $1",
                )
                .bind(&job.id)
                .bind(i64::from(job.retries))
                .execute(&mut *tx)
                .await?;
            }
            ConsumeOutcome::Processed { job }
        };
        tx.commit().await?;
        Ok(outcome)
    }
}

pub(crate) fn parse_status(value: &str) -> Result<QueueJobStatus, QueuePgError> {
    match value {
        "pending" => Ok(QueueJobStatus::Pending),
        "processed" => Ok(QueueJobStatus::Processed),
        "retry" => Ok(QueueJobStatus::Retry),
        "failed" => Ok(QueueJobStatus::Failed),
        other => Err(QueuePgError::InvalidStatus(other.to_owned())),
    }
}

fn to_u32(field: &'static str, value: i64) -> Result<u32, QueuePgError> {
    u32::try_from(value).map_err(|_| QueuePgError::InvalidInteger { field, value })
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub queue: String,
    pub check_interval: Duration,
    pub job_timeout: Duration,
}

impl ConsumerConfig {
    #[must_use]
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            check_interval: Duration::from_secs(30),
            job_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone)]
pub struct Consumer {
    store: QueueStore,
    config: ConsumerConfig,
}

impl Consumer {
    #[must_use]
    pub const fn new(store: QueueStore, config: ConsumerConfig) -> Self {
        Self { store, config }
    }

    pub async fn run_until<H, Fut, E, S>(&self, handler: H, shutdown: S) -> Result<(), QueuePgError>
    where
        H: Fn(DequeuedJob) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: std::fmt::Display,
        S: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        loop {
            let queue = self.config.queue.clone();
            let timeout = self.config.job_timeout;
            let claimed = Arc::new(AtomicBool::new(false));
            let consume_claimed = Arc::clone(&claimed);
            let consume = self.store.consume_one_observed(
                &queue,
                move || consume_claimed.store(true, Ordering::Release),
                |job| async {
                    match tokio::time::timeout(timeout, handler(job)).await {
                        Ok(result) => result.map_err(|error| error.to_string()),
                        Err(_) => Err(format!("job exceeded its {timeout:?} timeout")),
                    }
                },
            );
            tokio::pin!(consume);
            let outcome = tokio::select! {
                biased;
                () = &mut shutdown => {
                    if claimed.load(Ordering::Acquire) {
                        consume.await?;
                    }
                    return Ok(());
                }
                result = &mut consume => result?,
            };
            if matches!(outcome, ConsumeOutcome::Empty) {
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorCursor {
    /// PostgreSQL's exact textual `timestamptz` representation.
    pub ran_at: String,
    pub id: String,
}

#[derive(Debug, Clone)]
pub struct MirrorConfig {
    pub source_queue: String,
    pub shadow_queue: String,
    pub bootstrap: MirrorBootstrap,
    pub sample_basis_points: u16,
    pub page_size: u32,
    pub active_depth_cap: u32,
    pub delay: Duration,
    pub archival_duration: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MirrorBootstrap {
    /// Start at the database clock when the durable mirror identity is first
    /// created. This is the production-safe default and does not replay the
    /// retained processed-row archive.
    Latest,
    /// Deliberately scan retained history from the oldest available row.
    ArchiveStart,
    /// Start strictly after an explicitly approved source position.
    Cursor(MirrorCursor),
}

impl Default for MirrorConfig {
    fn default() -> Self {
        Self {
            source_queue: crate::message::PROCESS_TORRENT.to_owned(),
            shadow_queue: PROCESS_TORRENT_SHADOW.to_owned(),
            bootstrap: MirrorBootstrap::Latest,
            sample_basis_points: 100,
            page_size: 100,
            active_depth_cap: 1_000,
            delay: Duration::from_secs(30),
            archival_duration: Duration::from_secs(60 * 60),
        }
    }
}

/// The closed set of reasons a processed source job is refused by the mirror's
/// supported-subset predicate.
///
/// The variants are ordered exactly as the predicate's conjuncts are written in
/// SQL, so `MirrorEligibility::ineligible_reason` reports the *first* failing
/// conjunct and every scanned candidate is attributed to at most one reason.
/// The set is fixed and small: it bounds the `reason` label cardinality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MirrorIneligibleReason {
    /// The payload is not the flags-off `ClassifierWorkflow: default` shape.
    PayloadShape,
    /// The payload carries no `InfoHashes` array entries.
    NoInfoHashes,
    /// A requested info hash has no `torrents` row.
    TorrentMissing,
    /// A requested torrent was updated after the source job settled, so the
    /// live image is no longer the image the source job wrote.
    TorrentUpdatedAfterRanAt,
    /// A requested torrent carries a sourced or enriched hint the shadow
    /// cannot reproduce.
    UnsupportedHint,
    /// A supported type-only hint changed after the source job settled.
    HintUpdatedAfterRanAt,
    /// A requested torrent has content with a non-null `content_source`.
    HasContentSource,
}

impl MirrorIneligibleReason {
    /// Every reason, so the metric children can be materialized at startup
    /// instead of only appearing once a reason is first observed.
    pub const ALL: [Self; 7] = [
        Self::PayloadShape,
        Self::NoInfoHashes,
        Self::TorrentMissing,
        Self::TorrentUpdatedAfterRanAt,
        Self::UnsupportedHint,
        Self::HintUpdatedAfterRanAt,
        Self::HasContentSource,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PayloadShape => "payload_shape",
            Self::NoInfoHashes => "no_infohashes",
            Self::TorrentMissing => "torrent_missing",
            Self::TorrentUpdatedAfterRanAt => "torrent_updated_after_ran_at",
            Self::UnsupportedHint => "unsupported_hint",
            Self::HintUpdatedAfterRanAt => "hint_updated_after_ran_at",
            Self::HasContentSource => "has_content_source",
        }
    }
}

/// The mirror's supported-subset predicate, decomposed into the individual
/// conjuncts the SQL evaluates.
///
/// This is observability only. `ineligible_reason().is_none()` is exactly the
/// single `shadow_eligible` boolean the query used to return: the SQL now
/// returns the same conjuncts separately so a refusal can be attributed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MirrorEligibility {
    payload_shape_ok: bool,
    has_infohashes: bool,
    torrent_missing: bool,
    torrent_updated_after_ran_at: bool,
    has_unsupported_hint: bool,
    hint_updated_after_ran_at: bool,
    has_content_source: bool,
}

impl MirrorEligibility {
    const fn ineligible_reason(self) -> Option<MirrorIneligibleReason> {
        if !self.payload_shape_ok {
            return Some(MirrorIneligibleReason::PayloadShape);
        }
        if !self.has_infohashes {
            return Some(MirrorIneligibleReason::NoInfoHashes);
        }
        if self.torrent_missing {
            return Some(MirrorIneligibleReason::TorrentMissing);
        }
        if self.torrent_updated_after_ran_at {
            return Some(MirrorIneligibleReason::TorrentUpdatedAfterRanAt);
        }
        if self.has_unsupported_hint {
            return Some(MirrorIneligibleReason::UnsupportedHint);
        }
        if self.hint_updated_after_ran_at {
            return Some(MirrorIneligibleReason::HintUpdatedAfterRanAt);
        }
        if self.has_content_source {
            return Some(MirrorIneligibleReason::HasContentSource);
        }
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorReport {
    pub cursor: Option<MirrorCursor>,
    pub scanned: u32,
    pub inserted: u32,
    pub active_depth: u32,
    pub capped: bool,
    /// Scanned candidates that passed both eligibility and the deterministic
    /// sample gate. `sampled - inserted` is the scratch-dedupe rate.
    pub sampled: u32,
    /// Scanned candidates refused by the supported-subset predicate, attributed
    /// to the first failing conjunct. Sums to `scanned - eligible`.
    pub ineligible: BTreeMap<MirrorIneligibleReason, u32>,
}

impl MirrorReport {
    const fn empty(cursor: Option<MirrorCursor>, active_depth: u32, capped: bool) -> Self {
        Self {
            cursor,
            scanned: 0,
            inserted: 0,
            active_depth,
            capped,
            sampled: 0,
            ineligible: BTreeMap::new(),
        }
    }
}

struct MirrorCandidate {
    cursor: MirrorCursor,
    payload: String,
    max_retries: u32,
    priority: i32,
    eligibility: MirrorEligibility,
}

impl QueueStore {
    pub async fn mirror_processed_page(
        &self,
        config: &MirrorConfig,
    ) -> Result<MirrorReport, QueuePgError> {
        validate_mirror(config)?;
        let mut tx = self.pool.begin().await?;
        let (bootstrap_latest, bootstrap_ran_at, bootstrap_id) = match &config.bootstrap {
            MirrorBootstrap::Latest => (true, None, None),
            MirrorBootstrap::ArchiveStart => (false, None, None),
            MirrorBootstrap::Cursor(cursor) => (
                false,
                Some(cursor.ran_at.as_str()),
                Some(cursor.id.as_str()),
            ),
        };
        // This SECURITY DEFINER capability hardcodes both queue identities,
        // acquires the target advisory lock, initializes the fixed cursor when
        // absent, and returns it under FOR UPDATE.
        let cursor_row = sqlx::query(
            "SELECT ran_at, source_job_id \
             FROM public.ingest_shadow_lock_cursor($1, $2::timestamptz, $3)",
        )
        .bind(bootstrap_latest)
        .bind(bootstrap_ran_at)
        .bind(bootstrap_id)
        .fetch_one(&mut *tx)
        .await?;
        let cursor = match (
            cursor_row.try_get::<Option<String>, _>("ran_at")?,
            cursor_row.try_get::<Option<String>, _>("source_job_id")?,
        ) {
            (Some(ran_at), Some(id)) => Some(MirrorCursor { ran_at, id }),
            (None, None) => None,
            _ => {
                return Err(QueuePgError::InvalidMirrorConfig(
                    "durable mirror cursor is internally inconsistent",
                ));
            }
        };

        let active: i64 = sqlx::query_scalar(
            "SELECT count(*)::bigint FROM queue_jobs \
             WHERE queue = $1 AND status IN ('pending','retry')",
        )
        .bind(&config.shadow_queue)
        .fetch_one(&mut *tx)
        .await?;
        let mut active = to_u32("active_depth", active)?;
        if active >= config.active_depth_cap {
            tx.commit().await?;
            return Ok(MirrorReport::empty(cursor, active, true));
        }

        let cursor_ran_at = cursor
            .as_ref()
            .map_or("-infinity", |value| value.ran_at.as_str());
        let cursor_id = cursor.as_ref().map_or("", |value| value.id.as_str());
        // The supported-subset predicate is returned as its individual
        // conjuncts rather than one `shadow_eligible` boolean, so a refused
        // candidate can be attributed to a reason. The logic is unchanged:
        // eligible is still the conjunction of the same terms, and
        // `NOT EXISTS(rows WHERE a OR b OR c OR d)` is exactly
        // `NOT a AND NOT b AND NOT c AND NOT d` where each term is
        // `EXISTS(rows WHERE …)`, computed here in one lateral pass as
        // `coalesce(bool_or(…), false)` (a `WHERE` only passes TRUE, and
        // `bool_or` over no TRUE row yields NULL or no rows).
        let rows = sqlx::query(
            "SELECT source_job.id, source_job.payload::text AS payload, \
                    source_job.max_retries::bigint AS max_retries, source_job.priority, \
                    source_job.ran_at::text AS ran_at, \
                    source_job.payload @> \
                      '{\"ClassifierWorkflow\":\"default\",\
                      \"ClassifierFlags\":{\"local_search_enabled\":false,\
                      \"apis_enabled\":false,\"tmdb_enabled\":false}}'::jsonb \
                      AS shadow_payload_shape_ok, \
                    jsonb_array_length(\
                      CASE WHEN jsonb_typeof(source_job.payload->'InfoHashes') = 'array' \
                           THEN source_job.payload->'InfoHashes' ELSE '[]'::jsonb END\
                    ) > 0 AS shadow_has_infohashes, \
                    coalesce(requested_torrent.torrent_missing, false) \
                      AS shadow_torrent_missing, \
                    coalesce(requested_torrent.torrent_updated_after_ran_at, false) \
                      AS shadow_torrent_updated_after_ran_at, \
                    coalesce(requested_torrent.has_unsupported_hint, false) \
                      AS shadow_has_unsupported_hint, \
                    coalesce(requested_torrent.hint_updated_after_ran_at, false) \
                      AS shadow_hint_updated_after_ran_at, \
                    coalesce(requested_torrent.has_content_source, false) \
                      AS shadow_has_content_source \
             FROM queue_jobs AS source_job \
             LEFT JOIN LATERAL (\
               SELECT bool_or(source_torrent.info_hash IS NULL) AS torrent_missing, \
                      bool_or(source_torrent.updated_at > source_job.ran_at) \
                        AS torrent_updated_after_ran_at, \
                      bool_or(EXISTS (\
                        SELECT 1 FROM torrent_hints AS source_hint \
                        WHERE source_hint.info_hash = source_torrent.info_hash\
                          AND (source_hint.content_source IS NOT NULL \
                            OR source_hint.content_id IS NOT NULL \
                            OR source_hint.title IS NOT NULL \
                            OR source_hint.release_year IS NOT NULL \
                            OR coalesce(source_hint.languages, '[]'::jsonb) \
                              <> '[]'::jsonb \
                            OR coalesce(source_hint.episodes, '{}'::jsonb) \
                              <> '{}'::jsonb \
                            OR source_hint.video_resolution IS NOT NULL \
                            OR source_hint.video_source IS NOT NULL \
                            OR source_hint.video_codec IS NOT NULL \
                            OR source_hint.video_3d IS NOT NULL \
                            OR source_hint.video_modifier IS NOT NULL \
                            OR source_hint.release_group IS NOT NULL)\
                      )) AS has_unsupported_hint, \
                      bool_or(EXISTS (\
                        SELECT 1 FROM torrent_hints AS source_hint \
                        WHERE source_hint.info_hash = source_torrent.info_hash \
                          AND source_hint.updated_at > source_job.ran_at\
                      )) AS hint_updated_after_ran_at, \
                      bool_or(EXISTS (\
                        SELECT 1 FROM torrent_contents AS source_content \
                        WHERE source_content.info_hash = source_torrent.info_hash \
                          AND source_content.content_source IS NOT NULL\
                      )) AS has_content_source \
               FROM jsonb_array_elements(\
                 CASE WHEN jsonb_typeof(source_job.payload->'InfoHashes') = 'array' \
                      THEN source_job.payload->'InfoHashes' ELSE '[]'::jsonb END\
               ) AS requested(value) \
               LEFT JOIN torrents AS source_torrent \
                 ON source_torrent.info_hash = CASE \
                   WHEN jsonb_typeof(requested.value) = 'string' \
                    AND (requested.value #>> '{}') ~ '^[0-9A-Fa-f]{40}$' \
                   THEN decode(requested.value #>> '{}', 'hex') \
                   ELSE NULL \
                 END \
             ) AS requested_torrent ON TRUE \
             WHERE source_job.queue = $1 AND source_job.status = 'processed' \
               AND source_job.ran_at IS NOT NULL \
               AND (source_job.ran_at, source_job.id) > ($2::timestamptz, $3) \
             ORDER BY source_job.ran_at, source_job.id LIMIT $4",
        )
        .bind(&config.source_queue)
        .bind(cursor_ran_at)
        .bind(cursor_id)
        .bind(i64::from(config.page_size))
        .fetch_all(&mut *tx)
        .await?;

        let candidates = rows
            .into_iter()
            .map(|row| {
                Ok(MirrorCandidate {
                    cursor: MirrorCursor {
                        ran_at: row.try_get("ran_at")?,
                        id: row.try_get("id")?,
                    },
                    payload: row.try_get("payload")?,
                    max_retries: to_u32("max_retries", row.try_get("max_retries")?)?,
                    priority: row.try_get("priority")?,
                    eligibility: MirrorEligibility {
                        payload_shape_ok: row.try_get("shadow_payload_shape_ok")?,
                        has_infohashes: row.try_get("shadow_has_infohashes")?,
                        torrent_missing: row.try_get("shadow_torrent_missing")?,
                        torrent_updated_after_ran_at: row
                            .try_get("shadow_torrent_updated_after_ran_at")?,
                        has_unsupported_hint: row.try_get("shadow_has_unsupported_hint")?,
                        hint_updated_after_ran_at: row
                            .try_get("shadow_hint_updated_after_ran_at")?,
                        has_content_source: row.try_get("shadow_has_content_source")?,
                    },
                })
            })
            .collect::<Result<Vec<_>, QueuePgError>>()?;

        let mut report = MirrorReport::empty(cursor, active, false);
        for candidate in candidates {
            if let Some(reason) = candidate.eligibility.ineligible_reason() {
                *report.ineligible.entry(reason).or_default() += 1;
            } else if sampled(&candidate.cursor.id, config.sample_basis_points) {
                if active >= config.active_depth_cap {
                    report.capped = true;
                    break;
                }
                report.sampled += 1;
                let source_payload = serde_json::from_str(&candidate.payload)
                    .map_err(|source| QueuePgError::InvalidProducerPayload { source })?;
                let envelope = ShadowJobEnvelopeV1::new(
                    candidate.cursor.id.clone(),
                    candidate.cursor.ran_at.clone(),
                    source_payload,
                );
                let scratch_payload = serde_json::to_string(&envelope)
                    .map_err(|source| QueuePgError::InvalidProducerPayload { source })?;
                let scratch_fingerprint = fingerprint(&config.shadow_queue, &scratch_payload);
                let inserted: bool = sqlx::query_scalar(
                    "SELECT public.ingest_shadow_enqueue_job(\
                       $1, $2::jsonb, $3, $4, $5, $6\
                     )",
                )
                .bind(scratch_fingerprint)
                .bind(&scratch_payload)
                .bind(i64::from(candidate.max_retries))
                .bind(i64::try_from(config.delay.as_secs()).unwrap_or(i64::MAX))
                .bind(i64::try_from(config.archival_duration.as_secs()).unwrap_or(i64::MAX))
                .bind(candidate.priority)
                .fetch_one(&mut *tx)
                .await?;
                if inserted {
                    active += 1;
                    report.inserted += 1;
                }
            }
            report.scanned += 1;
            report.cursor = Some(candidate.cursor);
        }
        report.active_depth = active;
        if let Some(cursor) = &report.cursor {
            sqlx::query("SELECT public.ingest_shadow_advance_cursor($1::timestamptz, $2)")
                .bind(&cursor.ran_at)
                .bind(&cursor.id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(report)
    }
}

fn sampled(id: &str, basis_points: u16) -> bool {
    if basis_points == 10_000 {
        return true;
    }
    let hash = Sha256::digest(id.as_bytes());
    let value = u64::from_be_bytes(hash[..8].try_into().expect("SHA-256 prefix is 8 bytes"));
    let threshold = (u128::from(basis_points) * (u128::from(u64::MAX) + 1)) / 10_000;
    u128::from(value) < threshold
}

fn validate_mirror(config: &MirrorConfig) -> Result<(), QueuePgError> {
    if config.source_queue != crate::message::PROCESS_TORRENT {
        return Err(QueuePgError::InvalidMirrorConfig(
            "source_queue must be process_torrent",
        ));
    }
    if config.shadow_queue != PROCESS_TORRENT_SHADOW {
        return Err(QueuePgError::InvalidMirrorConfig(
            "shadow_queue must be process_torrent_shadow",
        ));
    }
    if config.sample_basis_points > 10_000 {
        return Err(QueuePgError::InvalidMirrorConfig(
            "sample_basis_points must be <= 10000",
        ));
    }
    if config.page_size == 0 {
        return Err(QueuePgError::InvalidMirrorConfig(
            "page_size must be positive",
        ));
    }
    if config.active_depth_cap == 0 {
        return Err(QueuePgError::InvalidMirrorConfig(
            "active_depth_cap must be positive",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        sampled, validate_mirror, MirrorConfig, MirrorEligibility, MirrorIneligibleReason,
        ShadowJobEnvelopeV1, SHADOW_JOB_ENVELOPE_VERSION,
    };

    const ELIGIBLE: MirrorEligibility = MirrorEligibility {
        payload_shape_ok: true,
        has_infohashes: true,
        torrent_missing: false,
        torrent_updated_after_ran_at: false,
        has_unsupported_hint: false,
        hint_updated_after_ran_at: false,
        has_content_source: false,
    };

    #[test]
    fn shadow_envelope_round_trips_exact_source_json_and_rejects_schema_drift() {
        let source_payload = serde_json::json!({
            "ClassifierWorkflow": "default",
            "ClassifierFlags": {
                "local_search_enabled": false,
                "apis_enabled": false,
                "tmdb_enabled": false
            },
            "InfoHashes": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
        });
        let envelope = ShadowJobEnvelopeV1::new(
            "source-job".to_owned(),
            "2026-08-26 12:34:56.123456+00".to_owned(),
            source_payload.clone(),
        );
        let encoded = serde_json::to_string(&envelope).expect("encode envelope");
        let decoded: ShadowJobEnvelopeV1 = serde_json::from_str(&encoded).expect("decode envelope");
        assert_eq!(decoded, envelope);
        assert_eq!(decoded.schema_version, SHADOW_JOB_ENVELOPE_VERSION);
        assert_eq!(decoded.source_payload, source_payload);

        let with_unknown = encoded.replacen('{', "{\"unexpected\":true,", 1);
        assert!(serde_json::from_str::<ShadowJobEnvelopeV1>(&with_unknown).is_err());
    }

    #[test]
    fn reason_labels_are_a_bounded_distinct_set() {
        let labels = MirrorIneligibleReason::ALL
            .iter()
            .map(|reason| reason.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(labels.len(), MirrorIneligibleReason::ALL.len());
    }

    #[test]
    fn decomposed_predicate_matches_the_original_conjunction() {
        // The original single `shadow_eligible` boolean was
        // `shape AND infohashes AND NOT (missing OR updated OR unsupported hint
        // OR updated hint OR source)`.
        // Attributing a reason must not change which candidates are admitted,
        // so assert the equivalence over the whole 128-value truth table.
        for bits in 0_u8..128 {
            let eligibility = MirrorEligibility {
                payload_shape_ok: bits & 1 != 0,
                has_infohashes: bits & 2 != 0,
                torrent_missing: bits & 4 != 0,
                torrent_updated_after_ran_at: bits & 8 != 0,
                has_unsupported_hint: bits & 16 != 0,
                hint_updated_after_ran_at: bits & 32 != 0,
                has_content_source: bits & 64 != 0,
            };
            let original = eligibility.payload_shape_ok
                && eligibility.has_infohashes
                && !(eligibility.torrent_missing
                    || eligibility.torrent_updated_after_ran_at
                    || eligibility.has_unsupported_hint
                    || eligibility.hint_updated_after_ran_at
                    || eligibility.has_content_source);
            assert_eq!(
                eligibility.ineligible_reason().is_none(),
                original,
                "eligibility decomposition drifted for {eligibility:?}"
            );
        }
    }

    #[test]
    fn every_reason_is_reachable_and_reported_first_failure_first() {
        assert_eq!(ELIGIBLE.ineligible_reason(), None);

        let cases = [
            (
                MirrorEligibility {
                    payload_shape_ok: false,
                    ..ELIGIBLE
                },
                MirrorIneligibleReason::PayloadShape,
            ),
            (
                MirrorEligibility {
                    has_infohashes: false,
                    ..ELIGIBLE
                },
                MirrorIneligibleReason::NoInfoHashes,
            ),
            (
                MirrorEligibility {
                    torrent_missing: true,
                    ..ELIGIBLE
                },
                MirrorIneligibleReason::TorrentMissing,
            ),
            (
                MirrorEligibility {
                    torrent_updated_after_ran_at: true,
                    ..ELIGIBLE
                },
                MirrorIneligibleReason::TorrentUpdatedAfterRanAt,
            ),
            (
                MirrorEligibility {
                    has_unsupported_hint: true,
                    ..ELIGIBLE
                },
                MirrorIneligibleReason::UnsupportedHint,
            ),
            (
                MirrorEligibility {
                    hint_updated_after_ran_at: true,
                    ..ELIGIBLE
                },
                MirrorIneligibleReason::HintUpdatedAfterRanAt,
            ),
            (
                MirrorEligibility {
                    has_content_source: true,
                    ..ELIGIBLE
                },
                MirrorIneligibleReason::HasContentSource,
            ),
        ];
        for (eligibility, expected) in cases {
            assert_eq!(eligibility.ineligible_reason(), Some(expected));
        }

        // A candidate failing several conjuncts is attributed exactly once, to
        // the first, so the reason counters never double-count a scan.
        let all_failing = MirrorEligibility {
            payload_shape_ok: false,
            has_infohashes: false,
            torrent_missing: true,
            torrent_updated_after_ran_at: true,
            has_unsupported_hint: true,
            hint_updated_after_ran_at: true,
            has_content_source: true,
        };
        assert_eq!(
            all_failing.ineligible_reason(),
            Some(MirrorIneligibleReason::PayloadShape)
        );
    }

    #[test]
    fn sampling_extremes_are_exact() {
        assert!(!sampled("job", 0));
        assert!(sampled("job", 10_000));
    }

    #[test]
    fn mirror_queue_separation_is_fail_closed() {
        let config = MirrorConfig {
            shadow_queue: crate::message::PROCESS_TORRENT.to_owned(),
            ..MirrorConfig::default()
        };
        assert!(validate_mirror(&config).is_err());

        let config = MirrorConfig {
            source_queue: "process_torrent_batch".to_owned(),
            ..MirrorConfig::default()
        };
        assert!(validate_mirror(&config).is_err());
    }
}
