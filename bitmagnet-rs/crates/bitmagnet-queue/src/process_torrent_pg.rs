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
//!   the supported value. The payload may carry no top-level key other than
//!   `MessageParams`' four exact names: Go's decoder also binds keys that
//!   match case-insensitively (`classifierWorkflow`), which serde ignores.
//! - **Abstention.** A handler may decline a claimed job before it has caused
//!   any side effect. The claim transaction then rolls back: the row keeps its
//!   status, retry count, `run_after`, and error, and the lock is released for
//!   Go. The caller supplies already-abstained IDs as an exclusion list so a
//!   lone Rust worker does not reclaim the same row in a tight loop.
//!
//! Admission selects a subset; it does not reorder it. Among admitted rows the
//! claim order is Go's `(status = 'retry'), priority, run_after`, served by a
//! partial index that holds only admitted rows (migration 00035).
//!
//! A worker whose classifier has a live content resolver may also admit the
//! explicit flags-on payload: the same shape with all three attach flags JSON
//! `true` ([`ProcessTorrentAdmission::FlagsOffOrOn`]). It is a separate claim
//! with its own partial index (migration 00036), so the flags-off worker's
//! claim and plan stay exactly as they were.
//!
//! Such a worker may instead admit organic payloads up to a priority bound
//! ([`ProcessTorrentAdmission::Organic`]): any `MessageParams` whose workflow
//! resolves to `default` and whose attach flags, explicit or inherited from
//! the core defaults, the live classifier resolves. It runs on Go's own
//! `queue_jobs_claim_idx`, with the bound checked on the index tuples.

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
///
/// Admission is one jsonb containment rather than four `->` comparisons, with
/// the top-level key allow-list nested under it in a `CASE`. That accepts
/// exactly the payloads whose `ClassifierWorkflow` is the string `default`,
/// whose `ClassifierFlags` object holds the three flags as JSON `false`, and
/// which carry no other top-level key. Written as separate `AND`ed payload
/// comparisons, the planner multiplies their default selectivities to about
/// zero rows and then prefers a sequential scan that detoasts and sorts every
/// pending payload on each claim (28 ms with 1,224 pending jobs).
///
/// The `queue`, `status`, and admission conjuncts are, verbatim, the predicate
/// of migration 00035's partial `queue_jobs_admitted_process_torrent_claim_idx`,
/// keyed in this `ORDER BY`. The planner uses a partial index only when it can
/// prove the query implies the index predicate, so the two must change
/// together. Organic payloads are never in that index, so a claim reads only
/// admitted rows however large the organic backlog is. On Go's
/// `queue_jobs_claim_idx` it would step over every organic row ahead of the
/// first admitted one, and read the whole backlog when none is admitted.
///
/// Public only so the migration-backed plan test can `EXPLAIN` it.
pub const ADMITTED_PROCESS_TORRENT_CLAIM_SQL: &str = "\
SELECT id, fingerprint, queue, status::text AS status, payload::text AS payload, \
       retries::bigint AS retries, max_retries::bigint AS max_retries, priority, \
       deadline IS NOT NULL AND deadline < clock_timestamp() AS deadline_exceeded \
FROM queue_jobs \
WHERE queue = 'process_torrent' AND status IN ('pending','retry') \
  AND run_after <= clock_timestamp() \
  AND CASE WHEN payload @> '{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":\
{\"local_search_enabled\":false,\"apis_enabled\":false,\"tmdb_enabled\":false}}'::jsonb \
      THEN payload - 'ClassifyMode' - 'ClassifierWorkflow' - 'ClassifierFlags' \
           - 'InfoHashes' = '{}'::jsonb \
      ELSE false END \
  AND id <> ALL($1::text[]) \
ORDER BY (status = 'retry'), priority, run_after \
FOR UPDATE SKIP LOCKED \
LIMIT 1";

/// [`ADMITTED_PROCESS_TORRENT_CLAIM_SQL`], also admitting the explicit flags-on
/// payload: `ClassifierWorkflow` the string `default` and the three attach flags
/// all JSON `true`.
///
/// The two containments are `OR`ed inside the `CASE` condition, so the top-level
/// key allow-list still applies to both and stays nested, for the selectivity
/// reason above. One claim over both shapes keeps Go's claim order across them.
///
/// The `queue`, `status`, and admission conjuncts are, verbatim, the predicate
/// of migration 00036's partial
/// `queue_jobs_admitted_flags_on_process_torrent_claim_idx`, keyed in this
/// `ORDER BY`; the two must change together. Used only by a worker started with
/// the flags-on switch, which requires that migration.
///
/// Public only so the migration-backed plan test can `EXPLAIN` it.
pub const ADMITTED_FLAGS_ON_PROCESS_TORRENT_CLAIM_SQL: &str = "\
SELECT id, fingerprint, queue, status::text AS status, payload::text AS payload, \
       retries::bigint AS retries, max_retries::bigint AS max_retries, priority, \
       deadline IS NOT NULL AND deadline < clock_timestamp() AS deadline_exceeded \
FROM queue_jobs \
WHERE queue = 'process_torrent' AND status IN ('pending','retry') \
  AND run_after <= clock_timestamp() \
  AND CASE WHEN payload @> '{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":\
{\"local_search_enabled\":false,\"apis_enabled\":false,\"tmdb_enabled\":false}}'::jsonb \
        OR payload @> '{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":\
{\"local_search_enabled\":true,\"apis_enabled\":true,\"tmdb_enabled\":true}}'::jsonb \
      THEN payload - 'ClassifyMode' - 'ClassifierWorkflow' - 'ClassifierFlags' \
           - 'InfoHashes' = '{}'::jsonb \
      ELSE false END \
  AND id <> ALL($1::text[]) \
ORDER BY (status = 'retry'), priority, run_after \
FOR UPDATE SKIP LOCKED \
LIMIT 1";

/// The organic admission predicate over a `payload` column: Go's
/// `MessageParams` exactly as its producers write it, with a workflow that
/// resolves to `default` and only the three attach flags.
///
/// - **Keys.** Only `MessageParams`' four exact names. Go's decoder also binds
///   case-insensitive matches (`classifierFlags`), which serde ignores.
/// - **ClassifierWorkflow:** absent, `""` or `"default"`.
/// - **ClassifyMode:** absent, `0` or `1`. It is compared as jsonb text,
///   not with jsonb's numeric equality, which would also admit `1.0`, a
///   value neither Go's `int` nor serde's `i32` decodes.
/// - **InfoHashes:** an array of 1 to 100 (the loader's bound) lowercase
///   40-digit hex strings, a strict subset of what both decoders accept.
/// - **ClassifierFlags:** absent, or an object holding only
///   `local_search_enabled`, `apis_enabled` and `tmdb_enabled`, each a JSON
///   boolean. An absent flag takes the core default, `true`.
/// - **Nulls.** A JSON `null` anywhere is rejected; no producer writes one,
///   since `message.go` uses `omitempty`.
///
/// It is a `CASE`, not `AND`ed conditions. The `WHEN`s run in order, so
/// `jsonb -` (an error on a scalar) and the array functions only see the
/// type a previous `WHEN` established; PostgreSQL may reorder `AND`ed quals.
/// And an absent key makes its `WHEN` NULL, which is not taken, where a
/// negated `AND` operand would make the whole predicate NULL. `-` binds more
/// tightly than `->`, so `->` operands of `-` are parenthesized.
macro_rules! organic_payload_predicate {
    () => {
        "CASE \
WHEN jsonb_typeof(payload) <> 'object' THEN false \
WHEN payload - 'ClassifyMode' - 'ClassifierWorkflow' - 'ClassifierFlags' - 'InfoHashes' \
<> '{}'::jsonb THEN false \
WHEN payload->'ClassifierWorkflow' NOT IN ('\"\"'::jsonb, '\"default\"'::jsonb) THEN false \
WHEN (payload->'ClassifyMode')::text NOT IN ('0', '1') THEN false \
WHEN jsonb_typeof(payload->'InfoHashes') IS DISTINCT FROM 'array' THEN false \
WHEN jsonb_array_length(payload->'InfoHashes') NOT BETWEEN 1 AND 100 THEN false \
WHEN EXISTS (SELECT 1 FROM jsonb_array_elements(payload->'InfoHashes') AS h(value) \
WHERE jsonb_typeof(h.value) <> 'string' OR (h.value #>> '{}') !~ '^[0-9a-f]{40}$') THEN false \
WHEN jsonb_typeof(payload->'ClassifierFlags') <> 'object' THEN false \
WHEN (payload->'ClassifierFlags') - 'local_search_enabled' - 'apis_enabled' - 'tmdb_enabled' \
<> '{}'::jsonb THEN false \
WHEN EXISTS (SELECT 1 FROM jsonb_each(payload->'ClassifierFlags') AS f(key, value) \
WHERE jsonb_typeof(f.value) <> 'boolean') THEN false \
ELSE true END"
    };
}

/// The organic admission predicate as a value, so a test can evaluate it over
/// a table's `payload` column.
pub const ORGANIC_PAYLOAD_PREDICATE: &str = organic_payload_predicate!();

/// Go's claim over organic payloads ([`ORGANIC_PAYLOAD_PREDICATE`]) with
/// `priority <= $2`. `$1` is the exclusion list.
///
/// The select list, `ORDER BY` and locking are those of
/// [`ADMITTED_PROCESS_TORRENT_CLAIM_SQL`]. There is no partial index:
/// PostgreSQL allows no subquery in an index predicate. The claim runs on
/// Go's `queue_jobs_claim_idx`, whose key order is the `ORDER BY`. The bound
/// is checked on its index tuples, so a higher-priority backlog is stepped
/// over without a heap fetch; the payload predicate reads the heap only for
/// rows within the bound.
///
/// Public only so the migration-backed plan test can `EXPLAIN` it.
pub const ORGANIC_PROCESS_TORRENT_CLAIM_SQL: &str = concat!(
    "\
SELECT id, fingerprint, queue, status::text AS status, payload::text AS payload, \
       retries::bigint AS retries, max_retries::bigint AS max_retries, priority, \
       deadline IS NOT NULL AND deadline < clock_timestamp() AS deadline_exceeded \
FROM queue_jobs \
WHERE queue = 'process_torrent' AND status IN ('pending','retry') \
  AND run_after <= clock_timestamp() \
  AND priority <= $2 \
  AND ",
    organic_payload_predicate!(),
    " \
  AND id <> ALL($1::text[]) \
ORDER BY (status = 'retry'), priority, run_after \
FOR UPDATE SKIP LOCKED \
LIMIT 1"
);

/// Which payloads an admitted claim takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessTorrentAdmission {
    /// Explicit `default` workflow with all three attach flags JSON `false`
    /// ([`ADMITTED_PROCESS_TORRENT_CLAIM_SQL`], migration 00035).
    FlagsOff,
    /// [`Self::FlagsOff`], or the same with all three flags JSON `true`
    /// ([`ADMITTED_FLAGS_ON_PROCESS_TORRENT_CLAIM_SQL`], migration 00036).
    FlagsOffOrOn,
    /// Every organic payload ([`ORGANIC_PAYLOAD_PREDICATE`]) at a priority of
    /// at most `max_priority` ([`ORGANIC_PROCESS_TORRENT_CLAIM_SQL`]).
    ///
    /// Not a superset of [`Self::FlagsOffOrOn`]: an explicit payload above
    /// the bound stays with Go.
    Organic { max_priority: i32 },
}

impl ProcessTorrentAdmission {
    /// The claim statement for this admission.
    #[must_use]
    pub const fn claim_sql(self) -> &'static str {
        match self {
            Self::FlagsOff => ADMITTED_PROCESS_TORRENT_CLAIM_SQL,
            Self::FlagsOffOrOn => ADMITTED_FLAGS_ON_PROCESS_TORRENT_CLAIM_SQL,
            Self::Organic { .. } => ORGANIC_PROCESS_TORRENT_CLAIM_SQL,
        }
    }
}

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
        self.consume_admitted_process_torrent_with(
            ProcessTorrentAdmission::FlagsOff,
            excluded_ids,
            handler,
        )
        .await
    }

    /// [`Self::consume_admitted_process_torrent`] with an explicit admission.
    pub async fn consume_admitted_process_torrent_with<H, Fut>(
        &self,
        admission: ProcessTorrentAdmission,
        excluded_ids: &[String],
        handler: H,
    ) -> Result<AdmittedConsumeOutcome, QueuePgError>
    where
        H: FnOnce(DequeuedJob) -> Fut,
        Fut: Future<Output = JobVerdict>,
    {
        let mut tx = self.pool().begin().await?;
        let mut claim = sqlx::query(admission.claim_sql()).bind(excluded_ids);
        if let ProcessTorrentAdmission::Organic { max_priority } = admission {
            claim = claim.bind(max_priority);
        }
        let row = claim.fetch_optional(&mut *tx).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The explicit claims repeat migrations 00035 and 00036's index
    /// predicates, and the organic admission must never move them.
    #[test]
    fn the_explicit_claims_are_pinned_byte_for_byte() {
        assert_eq!(
            ADMITTED_PROCESS_TORRENT_CLAIM_SQL,
            r#"SELECT id, fingerprint, queue, status::text AS status, payload::text AS payload, retries::bigint AS retries, max_retries::bigint AS max_retries, priority, deadline IS NOT NULL AND deadline < clock_timestamp() AS deadline_exceeded FROM queue_jobs WHERE queue = 'process_torrent' AND status IN ('pending','retry') AND run_after <= clock_timestamp() AND CASE WHEN payload @> '{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false}}'::jsonb THEN payload - 'ClassifyMode' - 'ClassifierWorkflow' - 'ClassifierFlags' - 'InfoHashes' = '{}'::jsonb ELSE false END AND id <> ALL($1::text[]) ORDER BY (status = 'retry'), priority, run_after FOR UPDATE SKIP LOCKED LIMIT 1"#
        );
        assert_eq!(
            ADMITTED_FLAGS_ON_PROCESS_TORRENT_CLAIM_SQL,
            r#"SELECT id, fingerprint, queue, status::text AS status, payload::text AS payload, retries::bigint AS retries, max_retries::bigint AS max_retries, priority, deadline IS NOT NULL AND deadline < clock_timestamp() AS deadline_exceeded FROM queue_jobs WHERE queue = 'process_torrent' AND status IN ('pending','retry') AND run_after <= clock_timestamp() AND CASE WHEN payload @> '{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false}}'::jsonb OR payload @> '{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":true,"apis_enabled":true,"tmdb_enabled":true}}'::jsonb THEN payload - 'ClassifyMode' - 'ClassifierWorkflow' - 'ClassifierFlags' - 'InfoHashes' = '{}'::jsonb ELSE false END AND id <> ALL($1::text[]) ORDER BY (status = 'retry'), priority, run_after FOR UPDATE SKIP LOCKED LIMIT 1"#
        );
    }

    #[test]
    fn the_organic_claim_keeps_the_admitted_select_list_order_and_locking() {
        let (head, _) = ADMITTED_PROCESS_TORRENT_CLAIM_SQL
            .split_once(" AND CASE ")
            .expect("the admitted claim's admission conjunct");
        let (_, tail) = ADMITTED_PROCESS_TORRENT_CLAIM_SQL
            .split_once(" AND id <> ALL(")
            .expect("the admitted claim's exclusion conjunct");
        assert_eq!(
            ORGANIC_PROCESS_TORRENT_CLAIM_SQL,
            format!(
                "{head} AND priority <= $2 AND {ORGANIC_PAYLOAD_PREDICATE} AND id <> ALL({tail}"
            )
        );
    }

    #[test]
    fn each_admission_has_its_own_claim() {
        assert_eq!(
            ProcessTorrentAdmission::FlagsOff.claim_sql(),
            ADMITTED_PROCESS_TORRENT_CLAIM_SQL
        );
        assert_eq!(
            ProcessTorrentAdmission::FlagsOffOrOn.claim_sql(),
            ADMITTED_FLAGS_ON_PROCESS_TORRENT_CLAIM_SQL
        );
        assert_eq!(
            ProcessTorrentAdmission::Organic { max_priority: 0 }.claim_sql(),
            ORGANIC_PROCESS_TORRENT_CLAIM_SQL
        );
    }
}
