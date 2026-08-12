//! Strict PostgreSQL insertion for constructed queue jobs.

use chrono::{DateTime, Utc};
use sqlx::postgres::types::PgInterval;
use sqlx::{Postgres, QueryBuilder};

use crate::{
    fingerprint, QueueJob, QueueJobStatus, QueuePgError, QueueStore, DEFAULT_ARCHIVAL_DURATION,
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

struct PreparedJob<'a> {
    job: &'a PreparedQueueJob,
    max_retries: i32,
    archival_duration: PgInterval,
}

impl QueueStore {
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
        let prepared = jobs
            .iter()
            .map(|prepared| {
                let job = &prepared.job;
                serde_json::from_str::<serde_json::Value>(&job.payload)
                    .map_err(|source| QueuePgError::InvalidProducerPayload { source })?;
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
            .collect::<Result<Vec<_>, QueuePgError>>()?;

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

fn duration_interval(
    field: &'static str,
    duration: std::time::Duration,
) -> Result<PgInterval, QueuePgError> {
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
