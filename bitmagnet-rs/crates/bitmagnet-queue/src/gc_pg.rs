//! PostgreSQL cleanup for terminal queue rows.

use chrono::{DateTime, Utc};

use crate::{QueuePgError, QueueStore};

impl QueueStore {
    /// Delete terminal jobs whose per-row archival window ended before `cutoff`.
    ///
    /// The strict inequality, terminal statuses, and caller-supplied clock match
    /// Go's queue garbage-collection statement. Rows with a null `ran_at` are
    /// retained by PostgreSQL's null predicate semantics.
    pub async fn delete_expired_terminal_jobs(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<u64, QueuePgError> {
        let result = sqlx::query(
            "DELETE FROM queue_jobs \
             WHERE status IN ('processed', 'failed') \
               AND ran_at + archival_duration < $1",
        )
        .bind(cutoff)
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected())
    }
}
