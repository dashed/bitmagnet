//! `QueueJob` construction + fingerprint, mirroring `model.NewQueueJob`
//! (`internal/model/queue_jobs.go:11-38`) and the `queue_jobs` row shape
//! (contract §1.1, §1.3).

use std::time::Duration;

use serde::Serialize;
use sha2::{Digest, Sha256};

/// Job status enum (`internal/model/queue_job_status_enum.go:19-22`, DDL
/// `00012_queue.sql`). New jobs default to `Pending`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueJobStatus {
    Pending,
    Processed,
    Retry,
    Failed,
}

impl QueueJobStatus {
    /// The DB enum label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processed => "processed",
            Self::Retry => "retry",
            Self::Failed => "failed",
        }
    }
}

/// Default archival duration: 7 days (`queue_jobs.go:31`).
pub const DEFAULT_ARCHIVAL_DURATION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Constructor options, mirroring the Go functional options
/// (`queue_jobs.go:40-58`): `QueueJobMaxRetries`, `QueueJobPriority`,
/// `QueueJobDelayBy`.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueueJobOptions {
    pub max_retries: u32,
    pub priority: i32,
    pub delay: Duration,
}

impl QueueJobOptions {
    /// Set `max_retries` (the message constructors inject `2`).
    #[must_use]
    pub const fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set `priority` (importer enqueues at 20; default 0).
    #[must_use]
    pub const fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Delay `run_after` by `delay` (`QueueJobDelayBy`).
    #[must_use]
    pub const fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

/// A constructed queue job. Fields that are time-derived at insert
/// (`run_after`, `ran_at`, `created_at`) or DB-assigned (`id`) are deferred to
/// the DB-integration milestone; this struct captures the fingerprint-relevant
/// and constructor-set fields (contract §1.1, §1.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueJob {
    pub queue: String,
    pub status: QueueJobStatus,
    /// The exact JSON bytes hashed into the fingerprint.
    pub payload: String,
    pub fingerprint: String,
    pub max_retries: u32,
    pub priority: i32,
    pub archival_duration: Duration,
    pub delay: Duration,
}

/// Error building a job — a payload that fails JSON marshaling.
#[derive(Debug)]
pub enum JobError {
    Marshal(serde_json::Error),
}

impl std::fmt::Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Marshal(e) => write!(f, "marshal queue job payload: {e}"),
        }
    }
}

impl std::error::Error for JobError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Marshal(e) => Some(e),
        }
    }
}

impl From<serde_json::Error> for JobError {
    fn from(e: serde_json::Error) -> Self {
        Self::Marshal(e)
    }
}

/// Compute the job fingerprint: `hex(sha256(queue || payload))`
/// (`queue_jobs.go:18-26`). Lowercase hex, matching Go's `fmt.Sprintf("%x")`.
#[must_use]
pub fn fingerprint(queue: &str, payload: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(queue.as_bytes());
    hasher.update(payload.as_bytes());
    hex::encode(hasher.finalize())
}

/// Build a `QueueJob` from a queue name + serializable payload, mirroring
/// `model.NewQueueJob` (`queue_jobs.go:11-38`): marshal the payload to JSON,
/// fingerprint `queue || payload`, apply constructor defaults + options.
///
/// The payload's JSON must be byte-identical to Go's `encoding/json` output;
/// [`crate::message`] defines the payload types that guarantee this.
///
/// # Errors
/// Returns [`JobError::Marshal`] if the payload fails to serialize.
pub fn new_queue_job<T: Serialize>(
    queue: &str,
    payload: &T,
    opts: QueueJobOptions,
) -> Result<QueueJob, JobError> {
    let payload = serde_json::to_string(payload)?;
    let fingerprint = fingerprint(queue, &payload);
    Ok(QueueJob {
        queue: queue.to_string(),
        status: QueueJobStatus::Pending,
        payload,
        fingerprint,
        max_retries: opts.max_retries,
        priority: opts.priority,
        archival_duration: DEFAULT_ARCHIVAL_DURATION,
        delay: opts.delay,
    })
}
