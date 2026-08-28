//! PostgreSQL queue-depth snapshots for metrics exposition.

use prometheus::core::Collector as _;
use sqlx::Row;

use crate::{QueueJobStatus, QueuePgError, QueueStore};

pub const QUEUE_JOBS_METRIC_NAME: &str = "bitmagnet_queue_jobs_total";
pub const QUEUE_JOBS_METRIC_HELP: &str =
    "Number of tasks enqueued; broken down by queue and status.";
pub const INGEST_SHADOW_GOOSE_VERSION_METRIC_NAME: &str = "bitmagnet_ingest_shadow_goose_version";
pub const INGEST_SHADOW_SCRATCH_JOBS_METRIC_NAME: &str = "bitmagnet_ingest_shadow_scratch_jobs";
const INGEST_SHADOW_STATUS_SQL: &str = "WITH latest AS ( \
   SELECT DISTINCT ON (version_id) id, version_id, is_applied \
   FROM public.goose_db_version \
   ORDER BY version_id, id DESC \
 ), applied_head AS ( \
   SELECT version_id FROM latest WHERE is_applied ORDER BY id DESC LIMIT 1 \
 ) \
 SELECT \
   (SELECT version_id FROM applied_head) AS goose_version, \
   count(*) FILTER (WHERE status = 'pending')::bigint AS pending, \
   count(*) FILTER (WHERE status = 'retry')::bigint AS retry, \
   count(*) FILTER (WHERE status = 'failed')::bigint AS failed \
 FROM public.queue_jobs \
 WHERE queue = 'process_torrent_shadow'";

/// One fixed, read-only database snapshot for ingest-shadow admission and drain
/// planning. No caller-controlled queue name or status enters the query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestShadowStatusSnapshot {
    pub goose_version: i64,
    pub pending: u64,
    pub retry: u64,
    pub failed: u64,
}

/// One nonempty `process_torrent_batch` status group from the live queue table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessTorrentBatchStatusCount {
    pub queue: String,
    pub status: QueueJobStatus,
    pub count: u64,
}

impl QueueStore {
    /// Read the exact Goose head and the bounded scratch-queue status set.
    ///
    /// Both ingest-shadow roles already require read access to these two
    /// relations for startup admission and their normal runtime. Keeping the
    /// SQL fixed here avoids a general-purpose query or a new database
    /// capability solely for observability.
    pub async fn ingest_shadow_status_snapshot(
        &self,
    ) -> Result<IngestShadowStatusSnapshot, QueuePgError> {
        let row = sqlx::query(INGEST_SHADOW_STATUS_SQL)
            .fetch_one(self.pool())
            .await?;

        let goose_version: Option<i64> = row.try_get("goose_version")?;
        let goose_version = goose_version.ok_or(QueuePgError::InvalidMirrorConfig(
            "goose migration head is absent",
        ))?;
        Ok(IngestShadowStatusSnapshot {
            goose_version,
            pending: nonnegative_count("pending", row.try_get("pending")?)?,
            retry: nonnegative_count("retry", row.try_get("retry")?)?,
            failed: nonnegative_count("failed", row.try_get("failed")?)?,
        })
    }

    /// Build fresh, unregistered ingest-shadow gauge families for one scrape.
    /// Every bounded status child is present even when its value is zero.
    pub async fn ingest_shadow_status_metric_families(
        &self,
    ) -> Result<Vec<prometheus::proto::MetricFamily>, QueuePgError> {
        let snapshot = self.ingest_shadow_status_snapshot().await?;
        Ok(ingest_shadow_status_metric_families(&snapshot))
    }

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

fn nonnegative_count(field: &'static str, value: i64) -> Result<u64, QueuePgError> {
    u64::try_from(value).map_err(|_| QueuePgError::InvalidInteger { field, value })
}

fn ingest_shadow_status_metric_families(
    snapshot: &IngestShadowStatusSnapshot,
) -> Vec<prometheus::proto::MetricFamily> {
    let goose_version = prometheus::IntGauge::new(
        INGEST_SHADOW_GOOSE_VERSION_METRIC_NAME,
        "Applied Goose migration head observed by the ingest-shadow process.",
    )
    .expect("the ingest-shadow Goose metric descriptor must be valid");
    goose_version.set(snapshot.goose_version);

    let jobs = prometheus::IntGaugeVec::new(
        prometheus::Opts::new(
            INGEST_SHADOW_SCRATCH_JOBS_METRIC_NAME,
            "Current process_torrent_shadow jobs in the bounded active or failed status set.",
        ),
        &["status"],
    )
    .expect("the ingest-shadow scratch metric descriptor must be valid");
    for (status, count) in [
        ("pending", snapshot.pending),
        ("retry", snapshot.retry),
        ("failed", snapshot.failed),
    ] {
        jobs.with_label_values(&[status])
            .set(i64::try_from(count).expect("PostgreSQL bigint counts fit a Prometheus IntGauge"));
    }

    let mut families = goose_version.collect();
    families.extend(jobs.collect());
    families.sort_by(|left, right| left.name().cmp(right.name()));
    for family in &mut families {
        family.mut_metric().sort_by(|left, right| {
            let labels = |metric: &prometheus::proto::Metric| {
                metric
                    .get_label()
                    .iter()
                    .map(|label| (label.name().to_owned(), label.value().to_owned()))
                    .collect::<Vec<_>>()
            };
            labels(left).cmp(&labels(right))
        });
    }
    families
}

#[cfg(test)]
mod tests {
    use prometheus::Encoder as _;

    use super::{
        ingest_shadow_status_metric_families, IngestShadowStatusSnapshot, INGEST_SHADOW_STATUS_SQL,
    };

    #[test]
    fn ingest_shadow_head_uses_goose_row_identity_not_max_version() {
        assert!(INGEST_SHADOW_STATUS_SQL
            .contains("SELECT DISTINCT ON (version_id) id, version_id, is_applied"));
        assert!(INGEST_SHADOW_STATUS_SQL.contains("ORDER BY version_id, id DESC"));
        assert!(INGEST_SHADOW_STATUS_SQL
            .contains("SELECT version_id FROM latest WHERE is_applied ORDER BY id DESC LIMIT 1"));
        assert!(!INGEST_SHADOW_STATUS_SQL.contains("max(version_id)"));
    }

    #[test]
    fn ingest_shadow_metrics_are_fixed_complete_and_deterministic() {
        let families = ingest_shadow_status_metric_families(&IngestShadowStatusSnapshot {
            goose_version: 34,
            pending: 0,
            retry: 2,
            failed: 1,
        });
        let mut encoded = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&families, &mut encoded)
            .expect("encode ingest-shadow metrics");
        let text = String::from_utf8(encoded).expect("Prometheus text is UTF-8");

        assert!(text.contains("bitmagnet_ingest_shadow_goose_version 34"));
        assert!(text.contains("bitmagnet_ingest_shadow_scratch_jobs{status=\"pending\"} 0"));
        assert!(text.contains("bitmagnet_ingest_shadow_scratch_jobs{status=\"retry\"} 2"));
        assert!(text.contains("bitmagnet_ingest_shadow_scratch_jobs{status=\"failed\"} 1"));
        assert_eq!(
            text.matches("bitmagnet_ingest_shadow_scratch_jobs{")
                .count(),
            3
        );
    }
}
