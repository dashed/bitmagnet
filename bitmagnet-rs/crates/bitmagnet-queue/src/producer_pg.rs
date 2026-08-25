//! Strict PostgreSQL insertion for constructed queue jobs.

use chrono::{DateTime, Utc};
use sqlx::postgres::types::PgInterval;
use sqlx::{Postgres, QueryBuilder};

use crate::{
    fingerprint, QueueJob, QueueJobStatus, QueuePgError, QueueStore, DEFAULT_ARCHIVAL_DURATION,
    PROCESS_TORRENT, PROCESS_TORRENT_BATCH,
};

/// A logical queue job whose application-clock eligibility timestamp has been
/// materialized. Batch callers prepare each child when its source page is
/// planned, preserving Go's page-by-page `RunAfter` ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedQueueJob {
    job: QueueJob,
    run_after: DateTime<Utc>,
}

impl PreparedQueueJob {
    /// Materialize the Go constructor-time `run_after` value, including the
    /// logical job's configured delay.
    pub fn materialize_at(
        job: QueueJob,
        materialized_at: DateTime<Utc>,
    ) -> Result<Self, QueuePgError> {
        let delay = chrono::TimeDelta::from_std(job.delay).map_err(|_| {
            QueuePgError::InvalidProducerDuration {
                field: "delay",
                microseconds: job.delay.as_micros(),
            }
        })?;
        let run_after = materialized_at.checked_add_signed(delay).ok_or(
            QueuePgError::InvalidProducerDuration {
                field: "delay",
                microseconds: job.delay.as_micros(),
            },
        )?;
        Ok(Self { job, run_after })
    }
}

/// PostgreSQL-representable values derived from a borrowed [`QueueJob`].
///
/// This preparation is transaction-neutral: it does not own or access a pool,
/// choose a timestamp, execute SQL, or impose queue-specific classifier
/// semantics. Callers can therefore validate a complete application plan
/// before beginning their own transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgQueueJobValues<'a> {
    /// The validated logical job. Its payload and text fields remain borrowed.
    pub job: &'a QueueJob,
    /// `max_retries` checked for PostgreSQL's signed `integer` type.
    pub max_retries: i32,
    /// The relative eligibility delay checked for PostgreSQL `interval`.
    pub delay: PgInterval,
    /// The exact seven-day retention default projected to PostgreSQL `interval`.
    pub archival_duration: PgInterval,
}

/// Validate and project a logical queue job into PostgreSQL-representable
/// values without starting a transaction or materializing timestamps.
///
/// Validation covers JSON syntax, recursively decoded JSON strings and object
/// keys containing NUL, PostgreSQL text NUL safety, the constructor invariants
/// for pending status/fingerprint/seven-day archival duration, and signed
/// PostgreSQL bounds for retry and relative-delay values. The archival value is
/// not independently variable: the seven-day invariant is checked before that
/// fixed default is projected to an interval.
///
/// # Errors
///
/// Returns [`QueuePgError`] when any logical value cannot be represented by the
/// generic PostgreSQL insertion contract.
pub fn prepare_pg_queue_job_values(job: &QueueJob) -> Result<PgQueueJobValues<'_>, QueuePgError> {
    let payload = validate_common_job(job)?;
    if job.queue.contains('\0') {
        return Err(QueuePgError::InvalidProducerJob(
            "queue contains a NUL byte",
        ));
    }
    if job.fingerprint.contains('\0') {
        return Err(QueuePgError::InvalidProducerJob(
            "fingerprint contains a NUL byte",
        ));
    }
    if json_contains_nul(&payload) {
        return Err(QueuePgError::InvalidProducerJob(
            "payload JSON contains a decoded NUL string",
        ));
    }

    let max_retries =
        i32::try_from(job.max_retries).map_err(|_| QueuePgError::InvalidProducerInteger {
            field: "max_retries",
            value: u64::from(job.max_retries),
        })?;
    let delay = duration_interval("delay", job.delay)?;
    let archival_duration = duration_interval("archival_duration", job.archival_duration)?;

    Ok(PgQueueJobValues {
        job,
        max_retries,
        delay,
        archival_duration,
    })
}

struct PreparedJob<'a> {
    job: &'a PreparedQueueJob,
    max_retries: i32,
    archival_duration: PgInterval,
}

fn prepare_materialized_jobs(
    jobs: &[PreparedQueueJob],
) -> Result<Vec<PreparedJob<'_>>, QueuePgError> {
    jobs.iter()
        .map(|prepared| {
            let job = &prepared.job;
            validate_common_job(job)?;
            let max_retries = i32::try_from(job.max_retries).map_err(|_| {
                QueuePgError::InvalidProducerInteger {
                    field: "max_retries",
                    value: u64::from(job.max_retries),
                }
            })?;
            let archival_duration =
                duration_interval("archival_duration", DEFAULT_ARCHIVAL_DURATION)?;
            Ok(PreparedJob {
                job: prepared,
                max_retries,
                archival_duration,
            })
        })
        .collect()
}

impl QueueStore {
    /// Insert the closed output shape of one `process_torrent_batch` plan
    /// through migration 32's fixed-queue capability.
    pub(crate) async fn insert_process_torrent_batch_plan_strict(
        &self,
        jobs: &[PreparedQueueJob],
    ) -> Result<(), QueuePgError> {
        let mut child_payloads = Vec::new();
        let mut child_run_afters = Vec::new();
        let mut child_priorities = Vec::new();
        let mut continuation_payload = None;
        let mut continuation_run_after = None;

        for (index, prepared) in jobs.iter().enumerate() {
            let job = &prepared.job;
            let payload = validate_common_job(job)?;
            if !payload.is_object() {
                return Err(QueuePgError::InvalidProducerJob(
                    "batch plan payload must be a JSON object",
                ));
            }
            if job.max_retries != 2 {
                return Err(QueuePgError::InvalidProducerJob(
                    "batch plan max_retries must be two",
                ));
            }
            let is_last = index + 1 == jobs.len();
            match job.queue.as_str() {
                PROCESS_TORRENT if matches!(job.priority, 4 | 10) => {
                    child_payloads.push(job.payload.as_str());
                    child_run_afters.push(prepared.run_after);
                    child_priorities.push(job.priority);
                }
                PROCESS_TORRENT_BATCH if is_last && job.priority == 0 => {
                    continuation_payload = Some(job.payload.as_str());
                    continuation_run_after = Some(prepared.run_after);
                }
                PROCESS_TORRENT => {
                    return Err(QueuePgError::InvalidProducerJob(
                        "batch child priority must be four or ten",
                    ));
                }
                PROCESS_TORRENT_BATCH => {
                    return Err(QueuePgError::InvalidProducerJob(
                        "batch continuation must be the final job with priority zero",
                    ));
                }
                _ => {
                    return Err(QueuePgError::InvalidProducerJob(
                        "batch plan contains an unsupported queue",
                    ));
                }
            }
        }

        let inserted: i64 = sqlx::query_scalar(
            "SELECT public.process_torrent_batch_enqueue_plan(\
               $1::text[], $2::timestamptz[], $3::integer[], \
               $4::text, $5::timestamptz, $6::timestamptz\
             )",
        )
        .bind(child_payloads)
        .bind(child_run_afters)
        .bind(child_priorities)
        .bind(continuation_payload)
        .bind(continuation_run_after)
        .bind(Utc::now())
        .fetch_one(self.pool())
        .await?;
        if usize::try_from(inserted).ok() != Some(jobs.len()) {
            return Err(QueuePgError::InvalidProducerJob(
                "batch capability inserted an unexpected row count",
            ));
        }
        Ok(())
    }

    /// Insert constructed jobs in one atomic statement without conflict suppression.
    ///
    /// PostgreSQL assigns the UUID. One application-clock `created_at` value is
    /// shared by the entire insert, while each caller-materialized `run_after`
    /// remains exact. This mirrors GORM's slice-create timestamp behavior.
    /// Active-fingerprint conflicts are returned unchanged (normally SQLSTATE
    /// `23505`), so any conflict rolls back the complete statement.
    pub async fn insert_jobs_strict(&self, jobs: &[PreparedQueueJob]) -> Result<(), QueuePgError> {
        if jobs.is_empty() {
            return Ok(());
        }

        // Validate every application value before PostgreSQL sees any row.
        let prepared = prepare_materialized_jobs(jobs)?;

        let created_at = Utc::now();
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO queue_jobs (fingerprint, queue, status, payload, retries, \
             max_retries, run_after, ran_at, error, deadline, archival_duration, \
             created_at, priority) ",
        );
        query.push_values(&prepared, |mut row, prepared| {
            row.push_bind(&prepared.job.job.fingerprint)
                .push_bind(&prepared.job.job.queue)
                .push("'pending'::queue_job_status")
                .push_bind(&prepared.job.job.payload)
                .push_unseparated("::jsonb")
                .push("0")
                .push_bind(prepared.max_retries)
                .push_bind(prepared.job.run_after)
                .push("NULL")
                .push("NULL")
                .push("NULL")
                .push_bind(prepared.archival_duration)
                .push_bind(created_at)
                .push_bind(prepared.job.job.priority);
        });

        query.build().execute(self.pool()).await?;
        Ok(())
    }
}

fn validate_common_job(job: &QueueJob) -> Result<serde_json::Value, QueuePgError> {
    let payload = parse_payload(job)?;
    if job.status != QueueJobStatus::Pending {
        return Err(QueuePgError::InvalidProducerJob("status must be pending"));
    }
    if job.archival_duration != DEFAULT_ARCHIVAL_DURATION {
        return Err(QueuePgError::InvalidProducerJob(
            "archival_duration must be seven days",
        ));
    }
    if fingerprint(&job.queue, &job.payload) != job.fingerprint {
        return Err(QueuePgError::InvalidProducerJob(
            "fingerprint does not match queue and payload bytes",
        ));
    }
    Ok(payload)
}

fn parse_payload(job: &QueueJob) -> Result<serde_json::Value, QueuePgError> {
    serde_json::from_str::<serde_json::Value>(&job.payload)
        .map_err(|source| QueuePgError::InvalidProducerPayload { source })
}

fn json_contains_nul(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(value) => value.contains('\0'),
        serde_json::Value::Array(values) => values.iter().any(json_contains_nul),
        serde_json::Value::Object(values) => values
            .iter()
            .any(|(key, value)| key.contains('\0') || json_contains_nul(value)),
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            false
        }
    }
}

fn duration_interval(
    field: &'static str,
    duration: std::time::Duration,
) -> Result<PgInterval, QueuePgError> {
    let submicro_nanoseconds = duration.subsec_nanos() % 1_000;
    if submicro_nanoseconds != 0 {
        return Err(QueuePgError::InvalidProducerDurationPrecision {
            field,
            submicro_nanoseconds,
        });
    }
    let microseconds =
        i64::try_from(duration.as_micros()).map_err(|_| QueuePgError::InvalidProducerDuration {
            field,
            microseconds: duration.as_micros(),
        })?;
    Ok(PgInterval {
        months: 0,
        days: 0,
        microseconds,
    })
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::time::Duration;

    use chrono::{TimeZone, Utc};

    use super::*;

    fn valid_job() -> QueueJob {
        let queue = "process_torrent".to_owned();
        let payload = r#"{"infoHash":"00112233445566778899aabbccddeeff00112233"}"#.to_owned();
        QueueJob {
            fingerprint: fingerprint(&queue, &payload),
            queue,
            status: QueueJobStatus::Pending,
            payload,
            max_retries: 2,
            priority: 10,
            archival_duration: DEFAULT_ARCHIVAL_DURATION,
            delay: Duration::from_secs(60),
        }
    }

    fn assert_send_sync<T: Send + Sync>() {}

    fn assert_send_future<F: Future + Send>(future: F) {
        drop(future);
    }

    #[test]
    fn valid_job_projects_exact_borrowed_and_postgres_values() {
        assert_send_sync::<PgQueueJobValues<'static>>();

        let job = valid_job();
        let values = prepare_pg_queue_job_values(&job).unwrap();
        assert!(std::ptr::eq(values.job, &job));
        assert_eq!(values.max_retries, 2);
        assert_eq!(
            values.delay,
            PgInterval {
                months: 0,
                days: 0,
                microseconds: 60_000_000,
            }
        );
        assert_eq!(
            values.archival_duration,
            PgInterval {
                months: 0,
                days: 0,
                microseconds: 604_800_000_000,
            }
        );
        assert_eq!(values.job.priority, 10);
        assert_eq!(values.job.payload, job.payload);
    }

    #[test]
    fn invalid_json_and_recursively_decoded_nul_strings_are_rejected() {
        let mut invalid = valid_job();
        invalid.payload = "{".to_owned();
        invalid.fingerprint = fingerprint(&invalid.queue, &invalid.payload);
        assert!(matches!(
            prepare_pg_queue_job_values(&invalid),
            Err(QueuePgError::InvalidProducerPayload { .. })
        ));

        for payload in [
            r#"{"nested":[{"value":"\u0000"}]}"#,
            r#"{"nested":{"key\u0000":"value"}}"#,
        ] {
            let mut nul = valid_job();
            nul.payload = payload.to_owned();
            nul.fingerprint = fingerprint(&nul.queue, &nul.payload);
            assert!(matches!(
                prepare_pg_queue_job_values(&nul),
                Err(QueuePgError::InvalidProducerJob(
                    "payload JSON contains a decoded NUL string"
                ))
            ));
        }
    }

    #[test]
    fn postgres_text_nul_is_rejected_without_bypassing_common_fingerprint_precedence() {
        let mut queue_nul = valid_job();
        queue_nul.queue.push('\0');
        queue_nul.fingerprint = fingerprint(&queue_nul.queue, &queue_nul.payload);
        assert!(matches!(
            prepare_pg_queue_job_values(&queue_nul),
            Err(QueuePgError::InvalidProducerJob(
                "queue contains a NUL byte"
            ))
        ));

        let mut fingerprint_nul = valid_job();
        fingerprint_nul.fingerprint.push('\0');
        assert!(matches!(
            prepare_pg_queue_job_values(&fingerprint_nul),
            Err(QueuePgError::InvalidProducerJob(
                "fingerprint does not match queue and payload bytes"
            ))
        ));
    }

    #[test]
    fn pending_fingerprint_and_default_archival_invariants_are_exact() {
        let mut status = valid_job();
        status.status = QueueJobStatus::Retry;
        assert!(matches!(
            prepare_pg_queue_job_values(&status),
            Err(QueuePgError::InvalidProducerJob("status must be pending"))
        ));

        let mut fingerprint_mismatch = valid_job();
        fingerprint_mismatch.fingerprint = "0".repeat(64);
        assert!(matches!(
            prepare_pg_queue_job_values(&fingerprint_mismatch),
            Err(QueuePgError::InvalidProducerJob(
                "fingerprint does not match queue and payload bytes"
            ))
        ));

        let mut archival = valid_job();
        archival.archival_duration += Duration::from_micros(1);
        assert!(matches!(
            prepare_pg_queue_job_values(&archival),
            Err(QueuePgError::InvalidProducerJob(
                "archival_duration must be seven days"
            ))
        ));
    }

    #[test]
    fn signed_retry_and_duration_bounds_are_checked() {
        let mut retries = valid_job();
        retries.max_retries = u32::try_from(i32::MAX).unwrap() + 1;
        assert!(matches!(
            prepare_pg_queue_job_values(&retries),
            Err(QueuePgError::InvalidProducerInteger {
                field: "max_retries",
                value: 2_147_483_648,
            })
        ));

        let mut delay = valid_job();
        let overflowing_microseconds = u64::try_from(i64::MAX).unwrap() + 1;
        delay.delay = Duration::from_micros(overflowing_microseconds);
        assert!(matches!(
            prepare_pg_queue_job_values(&delay),
            Err(QueuePgError::InvalidProducerDuration {
                field: "delay",
                microseconds,
            }) if microseconds == u128::from(overflowing_microseconds)
        ));

        let mut archival = valid_job();
        archival.archival_duration = Duration::from_micros(overflowing_microseconds);
        assert!(matches!(
            prepare_pg_queue_job_values(&archival),
            Err(QueuePgError::InvalidProducerJob(
                "archival_duration must be seven days"
            ))
        ));
    }

    #[test]
    fn submicrosecond_duration_precision_is_rejected_without_truncation() {
        let mut delay = valid_job();
        delay.delay = Duration::from_nanos(1);
        assert!(matches!(
            prepare_pg_queue_job_values(&delay),
            Err(QueuePgError::InvalidProducerDurationPrecision {
                field: "delay",
                submicro_nanoseconds: 1,
            })
        ));

        let mut delay = valid_job();
        delay.delay = Duration::from_nanos(999);
        assert!(matches!(
            prepare_pg_queue_job_values(&delay),
            Err(QueuePgError::InvalidProducerDurationPrecision {
                field: "delay",
                submicro_nanoseconds: 999,
            })
        ));

        let mut exact = valid_job();
        exact.delay = Duration::from_micros(1);
        let values = prepare_pg_queue_job_values(&exact).unwrap();
        assert_eq!(
            values.delay,
            PgInterval {
                months: 0,
                days: 0,
                microseconds: 1,
            }
        );
    }

    #[tokio::test]
    async fn strict_insert_preparation_preserves_absolute_materialized_timestamp() {
        let materialized_at = Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, 0).unwrap();
        let job = valid_job();
        let expected_run_after = materialized_at + chrono::TimeDelta::seconds(60);
        let materialized = PreparedQueueJob::materialize_at(job, materialized_at).unwrap();
        let rows = [materialized];
        let prepared = prepare_materialized_jobs(&rows).unwrap();

        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].job.run_after, expected_run_after);
        assert_eq!(prepared[0].max_retries, 2);
        assert_eq!(prepared[0].archival_duration.microseconds, 604_800_000_000);

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@127.0.0.1/bitmagnet")
            .unwrap();
        assert_send_future(QueueStore::new(pool).insert_jobs_strict(&rows));
    }

    #[test]
    fn materialized_insert_preserves_archive_fingerprint_and_integer_error_precedence() {
        let run_after = Utc.with_ymd_and_hms(2026, 8, 25, 12, 1, 0).unwrap();
        let mut archive_first = valid_job();
        archive_first.archival_duration += Duration::from_micros(1);
        archive_first.fingerprint = "mismatch".to_owned();
        archive_first.max_retries = u32::MAX;
        let rows = [PreparedQueueJob {
            job: archive_first,
            run_after,
        }];
        assert!(matches!(
            prepare_materialized_jobs(&rows),
            Err(QueuePgError::InvalidProducerJob(
                "archival_duration must be seven days"
            ))
        ));

        let mut fingerprint_before_integer = valid_job();
        fingerprint_before_integer.fingerprint = "mismatch".to_owned();
        fingerprint_before_integer.max_retries = u32::MAX;
        let rows = [PreparedQueueJob {
            job: fingerprint_before_integer,
            run_after,
        }];
        assert!(matches!(
            prepare_materialized_jobs(&rows),
            Err(QueuePgError::InvalidProducerJob(
                "fingerprint does not match queue and payload bytes"
            ))
        ));
    }

    #[test]
    fn materialized_insert_does_not_project_the_logical_relative_delay() {
        let overflowing_microseconds = u64::try_from(i64::MAX).unwrap() + 1;
        let mut job = valid_job();
        job.delay = Duration::from_micros(overflowing_microseconds);
        let run_after = Utc.with_ymd_and_hms(2026, 8, 25, 12, 2, 0).unwrap();
        let rows = [PreparedQueueJob { job, run_after }];

        let prepared = prepare_materialized_jobs(&rows).unwrap();
        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].job.run_after, run_after);
        assert_eq!(
            prepared[0].job.job.delay,
            Duration::from_micros(overflowing_microseconds)
        );
    }
}
