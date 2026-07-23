//! PostgreSQL queue consumer and processed-row shadow mirror.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures::FutureExt;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};

use crate::backoff::calculate_backoff;
use crate::{fingerprint, QueueJobStatus};

pub const PROCESS_TORRENT_SHADOW: &str = "process_torrent_shadow";
const DEADLINE_ERROR: &str = "the job did not complete before its deadline";

#[derive(Debug, thiserror::Error)]
pub enum QueuePgError {
    #[error("queue database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("invalid queue status '{0}'")]
    InvalidStatus(String),
    #[error("queue integer '{field}' is outside the supported range: {value}")]
    InvalidInteger { field: &'static str, value: i64 },
    #[error("invalid mirror configuration: {0}")]
    InvalidMirrorConfig(&'static str),
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
        let mut tx = self.pool.begin().await?;
        let row = if queue == PROCESS_TORRENT_SHADOW {
            sqlx::query("SELECT * FROM public.ingest_shadow_claim_job()")
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

fn parse_status(value: &str) -> Result<QueueJobStatus, QueuePgError> {
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
            let consume = self.store.consume_one(&queue, |job| async {
                match tokio::time::timeout(timeout, handler(job)).await {
                    Ok(result) => result.map_err(|error| error.to_string()),
                    Err(_) => Err(format!("job exceeded its {timeout:?} timeout")),
                }
            });
            let outcome = tokio::select! {
                () = &mut shutdown => return Ok(()),
                result = consume => result?,
            };
            if matches!(outcome, ConsumeOutcome::Empty) {
                tokio::select! {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorReport {
    pub cursor: Option<MirrorCursor>,
    pub scanned: u32,
    pub inserted: u32,
    pub active_depth: u32,
    pub capped: bool,
}

struct MirrorCandidate {
    cursor: MirrorCursor,
    payload: String,
    max_retries: u32,
    priority: i32,
    eligible: bool,
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
            return Ok(MirrorReport {
                cursor,
                scanned: 0,
                inserted: 0,
                active_depth: active,
                capped: true,
            });
        }

        let cursor_ran_at = cursor
            .as_ref()
            .map_or("-infinity", |value| value.ran_at.as_str());
        let cursor_id = cursor.as_ref().map_or("", |value| value.id.as_str());
        let rows = sqlx::query(
            "SELECT source_job.id, source_job.payload::text AS payload, \
                    source_job.max_retries::bigint AS max_retries, source_job.priority, \
                    source_job.ran_at::text AS ran_at, \
                    source_job.payload @> \
                      '{\"ClassifierWorkflow\":\"default\",\
                      \"ClassifierFlags\":{\"local_search_enabled\":false,\
                      \"apis_enabled\":false,\"tmdb_enabled\":false}}'::jsonb \
                    AND jsonb_array_length(\
                      CASE WHEN jsonb_typeof(source_job.payload->'InfoHashes') = 'array' \
                           THEN source_job.payload->'InfoHashes' ELSE '[]'::jsonb END\
                    ) > 0 \
                    AND NOT EXISTS (\
                      SELECT 1 \
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
                      WHERE source_torrent.info_hash IS NULL \
                         OR source_torrent.updated_at > source_job.ran_at \
                         OR EXISTS (\
                           SELECT 1 FROM torrent_hints AS source_hint \
                           WHERE source_hint.info_hash = source_torrent.info_hash\
                         ) \
                         OR EXISTS (\
                           SELECT 1 FROM torrent_contents AS source_content \
                           WHERE source_content.info_hash = source_torrent.info_hash \
                             AND source_content.content_source IS NOT NULL\
                         )\
                    ) \
                      AS shadow_eligible \
             FROM queue_jobs AS source_job \
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
                    eligible: row.try_get("shadow_eligible")?,
                })
            })
            .collect::<Result<Vec<_>, QueuePgError>>()?;

        let mut report = MirrorReport {
            cursor,
            scanned: 0,
            inserted: 0,
            active_depth: active,
            capped: false,
        };
        for candidate in candidates {
            if candidate.eligible && sampled(&candidate.cursor.id, config.sample_basis_points) {
                if active >= config.active_depth_cap {
                    report.capped = true;
                    break;
                }
                let scratch_fingerprint = fingerprint(&config.shadow_queue, &candidate.payload);
                let inserted: bool = sqlx::query_scalar(
                    "SELECT public.ingest_shadow_enqueue_job(\
                       $1, $2::jsonb, $3, $4, $5, $6\
                     )",
                )
                .bind(scratch_fingerprint)
                .bind(&candidate.payload)
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
    use super::{sampled, validate_mirror, MirrorConfig};

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
