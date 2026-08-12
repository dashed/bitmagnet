//! PostgreSQL queue-depth snapshots for metrics exposition.

use sqlx::Row;

use crate::{QueueJobStatus, QueuePgError, QueueStore};

/// One nonempty `(queue, status)` group from the live queue table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueStatusCount {
    pub queue: String,
    pub status: QueueJobStatus,
    pub count: u64,
}

impl QueueStore {
    /// Read the current nonempty queue/status groups.
    ///
    /// Go runs this query during every Prometheus scrape and emits no synthetic
    /// zero groups. This async primitive preserves that database snapshot; a
    /// scrape adapter must await it rather than expose a stale cache.
    pub async fn status_counts(&self) -> Result<Vec<QueueStatusCount>, QueuePgError> {
        sqlx::query(
            "SELECT queue, status::text AS status, count(*)::bigint AS count \
             FROM queue_jobs GROUP BY queue, status",
        )
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(|row| {
            let status: String = row.try_get("status")?;
            let count: i64 = row.try_get("count")?;
            Ok(QueueStatusCount {
                queue: row.try_get("queue")?,
                status: super::pg::parse_status(&status)?,
                count: u64::try_from(count).map_err(|_| QueuePgError::InvalidInteger {
                    field: "count",
                    value: count,
                })?,
            })
        })
        .collect()
    }
}
