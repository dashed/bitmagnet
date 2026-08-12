//! PostgreSQL queue-depth snapshots for metrics exposition.

use prometheus::core::Collector as _;
use sqlx::Row;

use crate::{QueueJobStatus, QueuePgError, QueueStore};

pub const QUEUE_JOBS_METRIC_NAME: &str = "bitmagnet_queue_jobs_total";
pub const QUEUE_JOBS_METRIC_HELP: &str =
    "Number of tasks enqueued; broken down by queue and status.";

/// One nonempty `process_torrent_batch` status group from the live queue table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessTorrentBatchStatusCount {
    pub queue: String,
    pub status: QueueJobStatus,
    pub count: u64,
}

impl QueueStore {
    /// Read the current nonempty `process_torrent_batch` status groups.
    ///
    /// Go runs this query during every Prometheus scrape and emits no synthetic
    /// zero groups. This async primitive preserves that database snapshot; a
    /// scrape adapter must await it rather than expose a stale cache.
    pub async fn process_torrent_batch_status_counts(
        &self,
    ) -> Result<Vec<ProcessTorrentBatchStatusCount>, QueuePgError> {
        sqlx::query("SELECT queue, status, count FROM public.process_torrent_batch_status_counts()")
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .map(|row| {
                let status: String = row.try_get("status")?;
                let count: i64 = row.try_get("count")?;
                Ok(ProcessTorrentBatchStatusCount {
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

    /// Build the fresh batch-only unregistered gauge family for one
    /// asynchronous scrape.
    pub async fn status_metric_families(
        &self,
    ) -> Result<Vec<prometheus::proto::MetricFamily>, QueuePgError> {
        let counts = self.process_torrent_batch_status_counts().await?;
        if counts.is_empty() {
            return Ok(Vec::new());
        }

        let gauges = prometheus::GaugeVec::new(
            prometheus::Opts::new(QUEUE_JOBS_METRIC_NAME, QUEUE_JOBS_METRIC_HELP),
            &["queue", "status"],
        )
        .expect("the canonical queue metric descriptor must be valid");
        for item in counts {
            gauges
                .with_label_values(&[&item.queue, item.status.as_str()])
                .set(item.count as f64);
        }
        let mut families = gauges.collect();
        for family in &mut families {
            family.mut_metric().sort_by(|left, right| {
                let key = |metric: &prometheus::proto::Metric| {
                    metric
                        .get_label()
                        .iter()
                        .map(|label| (label.name().to_owned(), label.value().to_owned()))
                        .collect::<Vec<_>>()
                };
                key(left).cmp(&key(right))
            });
        }
        Ok(families)
    }
}
