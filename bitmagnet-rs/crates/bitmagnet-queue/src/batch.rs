//! Pure `process_torrent_batch` page-to-job planning.
//!
//! PostgreSQL owns candidate selection. This module owns the deterministic
//! boundary after each ordered page is returned: child job grouping, the
//! API-disabled priority, chunk overshoot, keyset cursor, and continuation job.

use crate::{
    process_torrent_batch_job, process_torrent_job, JobError, ProcessTorrentBatchParams,
    ProcessTorrentParams, ProtocolId, QueueJob, QueueJobOptions,
};

#[derive(Debug, thiserror::Error)]
pub enum BatchPlanError {
    #[error(transparent)]
    Job(#[from] JobError),
    #[error("batch planner is already finalized")]
    Finalized,
    #[error("batch planner page violates the ordered keyset contract")]
    InvalidPage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchPlan {
    pub jobs: Vec<QueueJob>,
    pub max_info_hash: ProtocolId,
    pub chunk_size: u64,
    pub done: bool,
}

pub struct BatchPlanner {
    message: ProcessTorrentBatchParams,
    jobs: Vec<QueueJob>,
    max_info_hash: ProtocolId,
    chunk_size: u64,
    done: bool,
    finalized: bool,
}

impl BatchPlanner {
    #[must_use]
    pub fn new(message: ProcessTorrentBatchParams) -> Self {
        Self {
            max_info_hash: message.info_hash_greater_than,
            message,
            jobs: Vec::new(),
            chunk_size: 0,
            done: false,
            finalized: false,
        }
    }

    #[must_use]
    pub const fn max_info_hash(&self) -> ProtocolId {
        self.max_info_hash
    }

    #[must_use]
    pub const fn should_query(&self) -> bool {
        !self.done && (self.chunk_size == 0 || self.chunk_size < self.message.chunk_size)
    }

    pub fn add_page(&mut self, info_hashes: &[ProtocolId]) -> Result<(), BatchPlanError> {
        if self.finalized {
            return Err(BatchPlanError::Finalized);
        }
        if info_hashes.is_empty() {
            self.done = true;
            return Ok(());
        }
        if self.message.batch_size > 0
            && u64::try_from(info_hashes.len()).unwrap_or(u64::MAX) > self.message.batch_size
        {
            return Err(BatchPlanError::InvalidPage);
        }
        let mut previous = self.max_info_hash;
        for info_hash in info_hashes {
            if *info_hash <= previous {
                return Err(BatchPlanError::InvalidPage);
            }
            previous = *info_hash;
        }

        let priority = if self.message.apis_disabled() { 4 } else { 10 };
        self.jobs.push(process_torrent_job(
            &ProcessTorrentParams {
                classify_mode: self.message.classify_mode,
                classifier_workflow: self.message.classifier_workflow.clone(),
                classifier_flags: self.message.classifier_flags.clone(),
                info_hashes: info_hashes.to_vec(),
            },
            QueueJobOptions::default().with_priority(priority),
        )?);
        self.max_info_hash = *info_hashes.last().expect("non-empty page checked above");
        self.chunk_size = self
            .chunk_size
            .saturating_add(u64::try_from(info_hashes.len()).unwrap_or(u64::MAX));
        if info_hashes.len() < usize::try_from(self.message.batch_size).unwrap_or(usize::MAX) {
            self.done = true;
        }
        Ok(())
    }

    pub fn finalize(&mut self) -> Result<BatchPlan, BatchPlanError> {
        if self.finalized {
            return Err(BatchPlanError::Finalized);
        }
        self.finalized = true;
        if !self.done {
            let mut continuation = self.message.clone();
            continuation.info_hash_greater_than = self.max_info_hash;
            self.jobs.push(process_torrent_batch_job(
                &continuation,
                QueueJobOptions::default(),
            )?);
        }
        Ok(BatchPlan {
            jobs: self.jobs.clone(),
            max_info_hash: self.max_info_hash,
            chunk_size: self.chunk_size,
            done: self.done,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchPlanError, BatchPlanner};
    use crate::{ProcessTorrentBatchParams, ProtocolId};

    fn id(last: u8) -> ProtocolId {
        ProtocolId::from_hex(&format!("{last:040x}")).expect("valid fixture id")
    }

    #[test]
    fn rejects_pages_outside_the_ordered_keyset_contract() {
        for page in [
            vec![id(9), id(11)],
            vec![id(11), id(11)],
            vec![id(12), id(11)],
            vec![id(11), id(12), id(13)],
        ] {
            let mut planner = BatchPlanner::new(ProcessTorrentBatchParams {
                info_hash_greater_than: id(10),
                batch_size: 2,
                chunk_size: 10,
                ..ProcessTorrentBatchParams::default()
            });
            assert!(matches!(
                planner.add_page(&page),
                Err(BatchPlanError::InvalidPage)
            ));
        }
    }
}
