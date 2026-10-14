//! Admitted `process_torrent` claim for a persisting Rust worker.
//!
//! Go's queue server claims the next eligible `process_torrent` row with
//! `FOR UPDATE SKIP LOCKED`, keeps that row lock while the handler runs on other
//! connections, and settles the row in the same transaction. This module keeps
//! that claim order and settlement exactly, with two additions that let a Rust
//! worker run beside the Go worker on the same table:
//!
//! - **Payload admission.** Only payloads whose `ClassifierWorkflow` is
//!   explicitly `default` and whose `local_search_enabled`, `apis_enabled`, and
//!   `tmdb_enabled` flags are all explicitly JSON `false` are candidates. Every
//!   other row is never locked, so the Go worker keeps processing it in its own
//!   order. The Rust classifier embeds the Go core configuration, where all
//!   three flags default to `true`, so an omitted flag can never be resolved to
//!   the supported value.
//! - **Abstention.** A handler may decline a claimed job before it has caused
//!   any side effect. The claim transaction then rolls back: the row keeps its
//!   status, retry count, `run_after`, and error, and the lock is released for
//!   Go. The caller supplies already-abstained IDs as an exclusion list so a
//!   lone Rust worker does not reclaim the same row in a tight loop.
//!
//! Admission selects a subset; it does not reorder it. Among admitted rows the
//! claim order is Go's `(status = 'retry'), priority, run_after`.

use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures::FutureExt;
use sqlx::Row;

use crate::backoff::calculate_backoff;
use crate::pg::{
    panic_message, parse_status, to_u32, ConsumeOutcome, DequeuedJob, DEADLINE_ERROR,
    GENERIC_SETTLE_FAILED_SQL, GENERIC_SETTLE_PROCESSED_SQL, GENERIC_SETTLE_RETRY_SQL,
};
use crate::{QueueJobStatus, QueuePgError, QueueStore};

/// Go's claim with the fixed supported-subset admission predicate.
///
/// The queue name is hardcoded rather than bound so this statement can never be
/// pointed at another queue's payload shape. `$1` is the exclusion list.
const ADMITTED_PROCESS_TORRENT_CLAIM_SQL: &str = "\
SELECT id, fingerprint, queue, status::text AS status, payload::text AS payload, \
       retries::bigint AS retries, max_retries::bigint AS max_retries, priority, \
       deadline IS NOT NULL AND deadline < clock_timestamp() AS deadline_exceeded \
FROM queue_jobs \
WHERE queue = 'process_torrent' AND status IN ('pending','retry') \
  AND run_after <= clock_timestamp() \
  AND payload->>'ClassifierWorkflow' = 'default' \
  AND payload->'ClassifierFlags'->'local_search_enabled' = 'false'::jsonb \
  AND payload->'ClassifierFlags'->'apis_enabled' = 'false'::jsonb \
  AND payload->'ClassifierFlags'->'tmdb_enabled' = 'false'::jsonb \
  AND id <> ALL($1::text[]) \
ORDER BY (status = 'retry'), priority, run_after \
FOR UPDATE SKIP LOCKED \
LIMIT 1";

/// A handler's decision for one claimed job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobVerdict {
    /// The job succeeded; settle it `processed`.
    Complete,
    /// The job failed with this exact error text; settle it through Go's
    /// retry/failed lifecycle.
    Fail(String),
    /// The handler cannot reproduce Go for this job and has caused no side
    /// effect. Roll back the claim and leave the row to another worker.
    Abstain(String),
}

/// Result of one admitted claim attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmittedConsumeOutcome {
    /// The job was settled exactly as the generic consumer settles it.
    Settled(ConsumeOutcome),
    /// The handler abstained; the row was left untouched and unlocked.
    Abstained { job: DequeuedJob, reason: String },
}

impl QueueStore {
    /// Claim, execute, and settle one admitted `process_torrent` job.
    ///
    /// Deadline, retry-count, backoff, and settlement semantics are those of
    /// [`QueueStore::consume_one`]. The handler's error text is stored verbatim,
    /// so callers own Go's `job failed to process: ` wrapping. A panic is
    /// recorded as a failure, never as an abstention, because the handler may
    /// already have caused side effects.
    pub async fn consume_admitted_process_torrent<H, Fut>(
        &self,
        excluded_ids: &[String],
        handler: H,
    ) -> Result<AdmittedConsumeOutcome, QueuePgError>
    where
        H: FnOnce(DequeuedJob) -> Fut,
        Fut: Future<Output = JobVerdict>,
    {
        let mut tx = self.pool().begin().await?;
        let row = sqlx::query(ADMITTED_PROCESS_TORRENT_CLAIM_SQL)
            .bind(excluded_ids)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty));
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

        let handler_error = if deadline_exceeded {
            Some(DEADLINE_ERROR.to_owned())
        } else {
            if original_status == QueueJobStatus::Retry {
                job.retries = job.retries.saturating_add(1);
            }
            let verdict = match std::panic::catch_unwind(AssertUnwindSafe(|| handler(job.clone())))
            {
                Ok(future) => match AssertUnwindSafe(future).catch_unwind().await {
                    Ok(verdict) => verdict,
                    Err(payload) => JobVerdict::Fail(format!(
                        "job handler panicked: {}",
                        panic_message(&payload)
                    )),
                },
                Err(payload) => {
                    JobVerdict::Fail(format!("job handler panicked: {}", panic_message(&payload)))
                }
            };
            match verdict {
                JobVerdict::Complete => None,
                JobVerdict::Fail(error) => Some(error),
                JobVerdict::Abstain(reason) => {
                    tx.rollback().await?;
                    // Report the row as it still is: the retry increment above
                    // was never written.
                    job.retries = job
                        .retries
                        .saturating_sub(u32::from(original_status == QueueJobStatus::Retry));
                    return Ok(AdmittedConsumeOutcome::Abstained { job, reason });
                }
            }
        };

        let outcome = if let Some(error) = handler_error {
            if job.retries < job.max_retries {
                let delay = calculate_backoff(job.retries);
                let delay_seconds = i64::try_from(delay.as_secs()).unwrap_or(i64::MAX);
                sqlx::query(GENERIC_SETTLE_RETRY_SQL)
                    .bind(&job.id)
                    .bind(i64::from(job.retries))
                    .bind(&error)
                    .bind(delay_seconds)
                    .execute(&mut *tx)
                    .await?;
                ConsumeOutcome::RetryScheduled { job, delay }
            } else {
                sqlx::query(GENERIC_SETTLE_FAILED_SQL)
                    .bind(&job.id)
                    .bind(i64::from(job.retries))
                    .bind(&error)
                    .execute(&mut *tx)
                    .await?;
                ConsumeOutcome::Failed { job, error }
            }
        } else {
            sqlx::query(GENERIC_SETTLE_PROCESSED_SQL)
                .bind(&job.id)
                .bind(i64::from(job.retries))
                .execute(&mut *tx)
                .await?;
            ConsumeOutcome::Processed { job }
        };
        tx.commit().await?;
        Ok(AdmittedConsumeOutcome::Settled(outcome))
    }
}
