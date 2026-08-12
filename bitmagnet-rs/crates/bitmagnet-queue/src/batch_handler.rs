//! Callable `process_torrent_batch` orchestration without runtime registration.

use chrono::{DateTime, Utc};

use crate::{
    BatchPlanError, BatchPlanner, PreparedQueueJob, ProcessTorrentBatchParams, ProtocolId,
    QueuePgError, QueueStore,
};

/// Observable result of one batch payload without exposing DB-assigned row IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchHandleReport {
    pub selected: u64,
    pub child_jobs: u64,
    pub continuation_inserted: bool,
    pub max_info_hash: ProtocolId,
    pub done: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum BatchHandleError {
    #[error("decode process_torrent_batch payload")]
    Decode(#[source] serde_json::Error),
    #[error(transparent)]
    Plan(#[from] BatchPlanError),
    #[error(transparent)]
    Queue(#[from] QueuePgError),
}

impl QueueStore {
    /// Reproduce the Go batch handler's select-plan-insert boundary.
    ///
    /// Each non-empty page is timestamped immediately after planning its child;
    /// a continuation is timestamped only after finalization. All jobs are then
    /// inserted by one strict statement through this store's independent pool.
    pub async fn handle_process_torrent_batch_payload_with_clock<C>(
        &self,
        payload: &str,
        mut now: C,
    ) -> Result<BatchHandleReport, BatchHandleError>
    where
        C: FnMut() -> DateTime<Utc>,
    {
        let message: ProcessTorrentBatchParams =
            serde_json::from_str(payload).map_err(BatchHandleError::Decode)?;
        if message.batch_size == 0 {
            return Err(QueuePgError::InvalidBatchPayload(
                "BatchSize must be constructor-normalized and positive",
            )
            .into());
        }
        if message.chunk_size == 0 {
            return Err(QueuePgError::InvalidBatchPayload(
                "ChunkSize must be constructor-normalized and positive",
            )
            .into());
        }

        let mut planner = BatchPlanner::new(message);
        let mut materialized_at = Vec::new();
        let mut selected = 0_u64;
        while planner.should_query() {
            let page = self
                .select_process_torrent_batch_page(&planner.selection())
                .await?;
            let page_len = u64::try_from(page.len()).unwrap_or(u64::MAX);
            planner.add_page(&page)?;
            if !page.is_empty() {
                selected = selected.saturating_add(page_len);
                materialized_at.push(now());
            }
        }

        let plan = planner.finalize()?;
        let continuation_inserted = !plan.done;
        if continuation_inserted {
            materialized_at.push(now());
        }
        if materialized_at.len() != plan.jobs.len() {
            return Err(QueuePgError::InvalidBatchPayload(
                "planner jobs and materialization timestamps diverged",
            )
            .into());
        }
        let prepared = plan
            .jobs
            .into_iter()
            .zip(materialized_at)
            .map(|(job, at)| PreparedQueueJob::materialize_at(job, at))
            .collect::<Result<Vec<_>, QueuePgError>>()?;
        self.insert_jobs_strict(&prepared).await?;

        let job_count = u64::try_from(prepared.len()).unwrap_or(u64::MAX);
        Ok(BatchHandleReport {
            selected,
            child_jobs: job_count.saturating_sub(u64::from(continuation_inserted)),
            continuation_inserted,
            max_info_hash: plan.max_info_hash,
            done: plan.done,
        })
    }
}
