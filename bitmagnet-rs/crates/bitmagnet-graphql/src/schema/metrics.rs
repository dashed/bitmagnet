//! SELECT-only implementation of `queue.metrics` and `torrent.metrics`.
//!
//! The SQL and ordering intentionally mirror the Go clients in
//! `internal/metrics/{queue,torrent}metrics/client.go`, including the legacy
//! queue time-filter boolean expression. That keeps dark-reader comparisons
//! meaningful while mutations remain unavailable in the Rust service.

use std::sync::Arc;

use async_graphql::{Error, MaybeUndefined, Result};
use async_trait::async_trait;
use bitmagnet_db::PgPool;
use chrono::{DateTime as ChronoDateTime, SecondsFormat, Utc};
use sqlx::{FromRow, Postgres, QueryBuilder};
use thiserror::Error;

use super::enums::{MetricsBucketDuration, QueueJobStatus};
use super::inputs::{QueueMetricsQueryInput, TorrentMetricsQueryInput};
use super::objects::{
    QueueMetricsBucket, QueueMetricsQueryResult, TorrentMetricsBucket, TorrentMetricsQueryResult,
};
use super::scalars::{DateTime, Duration};

/// PostgreSQL `date_trunc` granularity admitted by the GraphQL schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsBucket {
    Minute,
    Hour,
    Day,
}

impl MetricsBucket {
    const fn as_sql(self) -> &'static str {
        match self {
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
        }
    }
}

/// Normalized queue metrics request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueMetricsRequest {
    pub bucket: MetricsBucket,
    pub end_time: Option<ChronoDateTime<Utc>>,
    pub queues: Option<Vec<String>>,
    pub start_time: Option<ChronoDateTime<Utc>>,
    pub statuses: Option<Vec<String>>,
}

/// One queue metrics row before GraphQL scalar and enum mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueMetricsRecord {
    pub count: i64,
    pub created_at_bucket: ChronoDateTime<Utc>,
    pub latency_nanos: Option<i64>,
    pub queue: String,
    pub ran_at_bucket: Option<ChronoDateTime<Utc>>,
    pub status: String,
}

/// Normalized torrent metrics request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentMetricsRequest {
    pub bucket: MetricsBucket,
    pub end_time: Option<ChronoDateTime<Utc>>,
    pub sources: Option<Vec<String>>,
    pub start_time: Option<ChronoDateTime<Utc>>,
}

/// One torrent metrics row before GraphQL mapping.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct TorrentMetricsRecord {
    pub bucket: ChronoDateTime<Utc>,
    pub count: i64,
    pub source: String,
    pub updated: bool,
}

/// Typed failures from the SELECT-only metrics adapter.
#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("GraphQL metrics are unavailable without a PostgreSQL runtime")]
    Disabled,
    #[error("GraphQL metrics PostgreSQL read failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("queue.metrics returned invalid latency nanoseconds {0:?}")]
    InvalidLatency(String),
}

/// Runtime seam for both read-only metrics queries.
#[async_trait]
pub trait MetricsRuntime: Send + Sync {
    async fn queue_metrics(
        &self,
        request: QueueMetricsRequest,
    ) -> std::result::Result<Vec<QueueMetricsRecord>, MetricsError>;

    async fn torrent_metrics(
        &self,
        request: TorrentMetricsRequest,
    ) -> std::result::Result<Vec<TorrentMetricsRecord>, MetricsError>;
}

struct DisabledMetricsRuntime;

#[async_trait]
impl MetricsRuntime for DisabledMetricsRuntime {
    async fn queue_metrics(
        &self,
        _request: QueueMetricsRequest,
    ) -> std::result::Result<Vec<QueueMetricsRecord>, MetricsError> {
        Err(MetricsError::Disabled)
    }

    async fn torrent_metrics(
        &self,
        _request: TorrentMetricsRequest,
    ) -> std::result::Result<Vec<TorrentMetricsRecord>, MetricsError> {
        Err(MetricsError::Disabled)
    }
}

/// PostgreSQL implementation of both read-only metrics queries.
pub struct PgMetricsRuntime {
    pool: PgPool,
}

impl PgMetricsRuntime {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(Debug, FromRow)]
struct QueueMetricsRow {
    count: i64,
    created_at_bucket: ChronoDateTime<Utc>,
    latency_nanos: Option<String>,
    queue: String,
    ran_at_bucket: Option<ChronoDateTime<Utc>>,
    status: String,
}

#[async_trait]
impl MetricsRuntime for PgMetricsRuntime {
    async fn queue_metrics(
        &self,
        request: QueueMetricsRequest,
    ) -> std::result::Result<Vec<QueueMetricsRecord>, MetricsError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *transaction)
            .await?;

        let mut query =
            QueryBuilder::<Postgres>::new("SELECT queue, status::text AS status, date_trunc(");
        query.push_bind(request.bucket.as_sql());
        query.push(", created_at) AS created_at_bucket, date_trunc(");
        query.push_bind(request.bucket.as_sql());
        query.push(
            ", ran_at) AS ran_at_bucket, count(*)::bigint AS count, \
             CASE WHEN sum(ran_at - run_after) IS NULL THEN NULL \
             ELSE round(extract(epoch FROM sum(ran_at - run_after)) * 1000000000)::numeric::text \
             END AS latency_nanos FROM queue_jobs",
        );
        push_queue_filters(&mut query, &request);
        query.push(
            " GROUP BY queue_jobs.queue, queue_jobs.status, created_at_bucket, ran_at_bucket \
             ORDER BY queue_jobs.queue, queue_jobs.status, created_at_bucket, ran_at_bucket",
        );
        let rows = query
            .build_query_as::<QueueMetricsRow>()
            .fetch_all(&mut *transaction)
            .await?;
        transaction.commit().await?;

        rows.into_iter()
            .map(|row| {
                let latency_nanos = row
                    .latency_nanos
                    .map(|value| {
                        value
                            .parse::<i64>()
                            .map_err(|_| MetricsError::InvalidLatency(value))
                    })
                    .transpose()?;
                Ok(QueueMetricsRecord {
                    count: row.count,
                    created_at_bucket: row.created_at_bucket,
                    latency_nanos,
                    queue: row.queue,
                    ran_at_bucket: row.ran_at_bucket,
                    status: row.status,
                })
            })
            .collect()
    }

    async fn torrent_metrics(
        &self,
        request: TorrentMetricsRequest,
    ) -> std::result::Result<Vec<TorrentMetricsRecord>, MetricsError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *transaction)
            .await?;

        let mut query = QueryBuilder::<Postgres>::new("SELECT source, date_trunc(");
        query.push_bind(request.bucket.as_sql());
        query.push(
            ", updated_at) AS bucket, updated_at > (created_at + interval '1 hour') AS updated, \
             count(*)::bigint AS count FROM torrents_torrent_sources",
        );
        push_torrent_filters(&mut query, &request);
        query.push(" GROUP BY source, bucket, updated ORDER BY source, bucket, updated");
        let rows = query
            .build_query_as::<TorrentMetricsRecord>()
            .fetch_all(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(rows)
    }
}

fn push_queue_filters(query: &mut QueryBuilder<Postgres>, request: &QueueMetricsRequest) {
    let has_filters = request.start_time.is_some()
        || request.end_time.is_some()
        || request.queues.is_some()
        || request.statuses.is_some();
    if !has_filters {
        return;
    }
    query.push(" WHERE (");
    let mut separator = "";
    if let Some(value) = request.start_time {
        query.push("status::text != 'pending' OR created_at >= ");
        query.push_bind(value);
        query.push(" AND status::text = 'pending' OR ran_at >= ");
        query.push_bind(value);
        separator = " AND ";
    }
    if let Some(value) = request.end_time {
        query.push(separator);
        query.push("status::text != 'pending' OR created_at <= ");
        query.push_bind(value);
        query.push(" AND status::text = 'pending' OR ran_at <= ");
        query.push_bind(value);
        separator = " AND ";
    }
    if let Some(values) = &request.queues {
        query.push(separator);
        query.push("queue = ANY(");
        query.push_bind(values.clone());
        query.push("::text[])");
        separator = " AND ";
    }
    if let Some(values) = &request.statuses {
        query.push(separator);
        query.push("status::text = ANY(");
        query.push_bind(values.clone());
        query.push("::text[])");
    }
    query.push(")");
}

fn push_torrent_filters(query: &mut QueryBuilder<Postgres>, request: &TorrentMetricsRequest) {
    let has_filters =
        request.start_time.is_some() || request.end_time.is_some() || request.sources.is_some();
    if !has_filters {
        return;
    }
    query.push(" WHERE (");
    let mut separator = "";
    if let Some(value) = request.start_time {
        query.push("updated_at >= ");
        query.push_bind(value);
        separator = " AND ";
    }
    if let Some(value) = request.end_time {
        query.push(separator);
        query.push("updated_at <= ");
        query.push_bind(value);
        separator = " AND ";
    }
    if let Some(values) = &request.sources {
        query.push(separator);
        query.push("source = ANY(");
        query.push_bind(values.clone());
        query.push("::text[])");
    }
    query.push(")");
}

/// GraphQL context wrapper for both metrics runtime methods.
#[derive(Clone)]
pub struct MetricsRuntimeData(Arc<dyn MetricsRuntime>);

impl MetricsRuntimeData {
    #[must_use]
    pub fn new(runtime: Arc<dyn MetricsRuntime>) -> Self {
        Self(runtime)
    }

    #[must_use]
    pub fn disabled() -> Self {
        Self::new(Arc::new(DisabledMetricsRuntime))
    }

    #[must_use]
    pub fn pg(pool: PgPool) -> Self {
        Self::new(Arc::new(PgMetricsRuntime::new(pool)))
    }
}

pub(super) async fn resolve_queue(
    runtime: &MetricsRuntimeData,
    input: QueueMetricsQueryInput,
) -> Result<QueueMetricsQueryResult> {
    let request = QueueMetricsRequest {
        bucket: graphql_bucket(input.bucket_duration),
        end_time: graphql_optional_datetime(input.end_time, "endTime", "queue.metrics")?,
        queues: input.queues,
        start_time: graphql_optional_datetime(input.start_time, "startTime", "queue.metrics")?,
        statuses: input
            .statuses
            .map(|values| values.into_iter().map(status_name).collect()),
    };
    let records = runtime
        .0
        .queue_metrics(request)
        .await
        .map_err(|error| Error::new(error.to_string()))?;
    Ok(QueueMetricsQueryResult {
        buckets: records
            .into_iter()
            .map(|record| {
                Ok(QueueMetricsBucket {
                    count: graphql_i32(record.count, "queue.metrics count")?,
                    created_at_bucket: graphql_datetime(record.created_at_bucket),
                    latency: record
                        .latency_nanos
                        .filter(|value| *value > 0)
                        .map(|value| Duration(format_duration(value))),
                    queue: record.queue,
                    ran_at_bucket: record.ran_at_bucket.map(graphql_datetime),
                    status: graphql_status(&record.status)?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    })
}

pub(super) async fn resolve_torrent(
    runtime: &MetricsRuntimeData,
    input: TorrentMetricsQueryInput,
) -> Result<TorrentMetricsQueryResult> {
    let request = TorrentMetricsRequest {
        bucket: graphql_bucket(input.bucket_duration),
        end_time: graphql_optional_datetime(input.end_time, "endTime", "torrent.metrics")?,
        sources: input.sources,
        start_time: graphql_optional_datetime(input.start_time, "startTime", "torrent.metrics")?,
    };
    let records = runtime
        .0
        .torrent_metrics(request)
        .await
        .map_err(|error| Error::new(error.to_string()))?;
    Ok(TorrentMetricsQueryResult {
        buckets: records
            .into_iter()
            .map(|record| {
                Ok(TorrentMetricsBucket {
                    bucket: graphql_datetime(record.bucket),
                    count: graphql_i32(record.count, "torrent.metrics count")?,
                    source: record.source,
                    updated: record.updated,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    })
}

fn graphql_bucket(value: MetricsBucketDuration) -> MetricsBucket {
    match value {
        MetricsBucketDuration::Minute => MetricsBucket::Minute,
        MetricsBucketDuration::Hour => MetricsBucket::Hour,
        MetricsBucketDuration::Day => MetricsBucket::Day,
    }
}

fn graphql_optional_datetime(
    value: MaybeUndefined<DateTime>,
    field: &str,
    surface: &str,
) -> Result<Option<ChronoDateTime<Utc>>> {
    value
        .value()
        .map(|value| {
            ChronoDateTime::parse_from_rfc3339(&value.0)
                .map(|value| value.with_timezone(&Utc))
                .map_err(|error| Error::new(format!("{surface} {field} is not RFC3339: {error}")))
        })
        .transpose()
}

fn graphql_datetime(value: ChronoDateTime<Utc>) -> DateTime {
    let mut encoded = value.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let suffix = encoded
        .strip_suffix('Z')
        .expect("UTC RFC3339 values always use the Z suffix");
    let trimmed = suffix.trim_end_matches('0');
    let end = if trimmed.ends_with('.') {
        trimmed.len() - 1
    } else {
        trimmed.len()
    };
    encoded.replace_range(end..encoded.len() - 1, "");
    DateTime(encoded)
}

fn graphql_status(value: &str) -> Result<QueueJobStatus> {
    match value {
        "failed" => Ok(QueueJobStatus::Failed),
        "pending" => Ok(QueueJobStatus::Pending),
        "processed" => Ok(QueueJobStatus::Processed),
        "retry" => Ok(QueueJobStatus::Retry),
        _ => Err(Error::new(format!(
            "queue.metrics returned unknown status {value:?}"
        ))),
    }
}

fn graphql_i32(value: i64, field: &str) -> Result<i32> {
    i32::try_from(value)
        .map_err(|_| Error::new(format!("{field} is outside the GraphQL Int range")))
}

fn status_name(status: QueueJobStatus) -> String {
    match status {
        QueueJobStatus::Failed => "failed",
        QueueJobStatus::Pending => "pending",
        QueueJobStatus::Processed => "processed",
        QueueJobStatus::Retry => "retry",
    }
    .to_owned()
}

fn format_duration(nanos: i64) -> String {
    const NANOS_PER_SECOND: i64 = 1_000_000_000;
    const NANOS_PER_MINUTE: i64 = NANOS_PER_SECOND * 60;
    const NANOS_PER_HOUR: i64 = NANOS_PER_MINUTE * 60;
    const NANOS_PER_DAY: i64 = NANOS_PER_HOUR * 24;
    const NANOS_PER_WEEK: i64 = NANOS_PER_DAY * 7;
    const NANOS_PER_MONTH: i64 = NANOS_PER_HOUR * 730;
    const NANOS_PER_YEAR: i64 = NANOS_PER_HOUR * 24 * 365;

    let mut remaining = nanos;
    let mut encoded = String::from("P");
    for (unit, suffix) in [
        (NANOS_PER_YEAR, "Y"),
        (NANOS_PER_MONTH, "M"),
        (NANOS_PER_WEEK, "W"),
        (NANOS_PER_DAY, "D"),
    ] {
        let value = remaining / unit;
        if value != 0 {
            encoded.push_str(&format!("{value}{suffix}"));
            remaining %= unit;
        }
    }
    let hours = remaining / NANOS_PER_HOUR;
    remaining %= NANOS_PER_HOUR;
    let minutes = remaining / NANOS_PER_MINUTE;
    remaining %= NANOS_PER_MINUTE;
    if hours != 0 || minutes != 0 || remaining != 0 {
        encoded.push('T');
    }
    if hours != 0 {
        encoded.push_str(&format!("{hours}H"));
    }
    if minutes != 0 {
        encoded.push_str(&format!("{minutes}M"));
    }
    if remaining != 0 {
        let seconds = remaining / NANOS_PER_SECOND;
        let fractional = remaining % NANOS_PER_SECOND;
        if fractional == 0 {
            encoded.push_str(&format!("{seconds}S"));
        } else {
            let fraction = format!("{fractional:09}");
            encoded.push_str(&format!("{seconds}.{}S", fraction.trim_end_matches('0')));
        }
    }
    if encoded == "P" {
        encoded.push_str("T0S");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_graphql::{value, EmptySubscription};

    use super::*;
    use crate::schema::roots::{Mutation, Query};

    struct FakeRuntime {
        queue_request: Mutex<Option<QueueMetricsRequest>>,
        torrent_request: Mutex<Option<TorrentMetricsRequest>>,
    }

    #[async_trait]
    impl MetricsRuntime for FakeRuntime {
        async fn queue_metrics(
            &self,
            request: QueueMetricsRequest,
        ) -> std::result::Result<Vec<QueueMetricsRecord>, MetricsError> {
            *self.queue_request.lock().expect("queue request mutex") = Some(request);
            Ok(vec![
                QueueMetricsRecord {
                    count: 2,
                    created_at_bucket: "2024-06-01T00:00:00Z".parse().expect("created bucket"),
                    latency_nanos: Some(3_661_234_567_890),
                    queue: "process_torrent".to_owned(),
                    ran_at_bucket: Some("2024-06-01T01:00:00.12Z".parse().expect("ran bucket")),
                    status: "processed".to_owned(),
                },
                QueueMetricsRecord {
                    count: 1,
                    created_at_bucket: "2024-06-01T02:00:00Z".parse().expect("created bucket"),
                    latency_nanos: Some(0),
                    queue: "process_torrent".to_owned(),
                    ran_at_bucket: None,
                    status: "pending".to_owned(),
                },
            ])
        }

        async fn torrent_metrics(
            &self,
            request: TorrentMetricsRequest,
        ) -> std::result::Result<Vec<TorrentMetricsRecord>, MetricsError> {
            *self.torrent_request.lock().expect("torrent request mutex") = Some(request);
            Ok(vec![TorrentMetricsRecord {
                bucket: "2024-06-01T03:00:00Z".parse().expect("torrent bucket"),
                count: 4,
                source: "dht".to_owned(),
                updated: true,
            }])
        }
    }

    fn test_schema(runtime: Arc<FakeRuntime>) -> crate::schema::Schema {
        async_graphql::Schema::build(Query, Mutation, EmptySubscription)
            .data(MetricsRuntimeData::new(runtime))
            .finish()
    }

    #[tokio::test]
    async fn queue_metrics_normalizes_input_and_maps_go_scalars() {
        let runtime = Arc::new(FakeRuntime {
            queue_request: Mutex::new(None),
            torrent_request: Mutex::new(None),
        });
        let response = test_schema(runtime.clone())
            .execute(
                "{ queue { metrics(input: { bucketDuration: hour, \
                 startTime: \"2024-06-01T01:00:00+01:00\", endTime: null, \
                 queues: [], statuses: [processed] }) { buckets { queue status \
                 createdAtBucket ranAtBucket count latency } } } }",
            )
            .await;

        assert!(response.errors.is_empty(), "errors: {:?}", response.errors);
        assert_eq!(
            response.data,
            value!({ "queue": { "metrics": { "buckets": [
                {
                    "queue": "process_torrent",
                    "status": "processed",
                    "createdAtBucket": "2024-06-01T00:00:00Z",
                    "ranAtBucket": "2024-06-01T01:00:00.12Z",
                    "count": 2,
                    "latency": "PT1H1M1.23456789S",
                },
                {
                    "queue": "process_torrent",
                    "status": "pending",
                    "createdAtBucket": "2024-06-01T02:00:00Z",
                    "ranAtBucket": null,
                    "count": 1,
                    "latency": null,
                },
            ] } } })
        );
        assert_eq!(
            runtime
                .queue_request
                .lock()
                .expect("queue request mutex")
                .clone()
                .expect("queue request"),
            QueueMetricsRequest {
                bucket: MetricsBucket::Hour,
                end_time: None,
                queues: Some(Vec::new()),
                start_time: Some("2024-06-01T00:00:00Z".parse().expect("start")),
                statuses: Some(vec!["processed".to_owned()]),
            }
        );
    }

    #[tokio::test]
    async fn torrent_metrics_normalizes_input_and_maps_rows() {
        let runtime = Arc::new(FakeRuntime {
            queue_request: Mutex::new(None),
            torrent_request: Mutex::new(None),
        });
        let response = test_schema(runtime.clone())
            .execute(
                "{ torrent { metrics(input: { bucketDuration: day, sources: [\"dht\"], \
                 endTime: \"2024-06-02T00:00:00Z\" }) { buckets { source bucket updated count } } } }",
            )
            .await;

        assert!(response.errors.is_empty(), "errors: {:?}", response.errors);
        assert_eq!(
            response.data,
            value!({ "torrent": { "metrics": { "buckets": [{
                "source": "dht",
                "bucket": "2024-06-01T03:00:00Z",
                "updated": true,
                "count": 4,
            }] } } })
        );
        assert_eq!(
            runtime
                .torrent_request
                .lock()
                .expect("torrent request mutex")
                .clone()
                .expect("torrent request"),
            TorrentMetricsRequest {
                bucket: MetricsBucket::Day,
                end_time: Some("2024-06-02T00:00:00Z".parse().expect("end")),
                sources: Some(vec!["dht".to_owned()]),
                start_time: None,
            }
        );
    }

    #[tokio::test]
    async fn invalid_datetime_fails_before_runtime() {
        let response = crate::schema::schema()
            .execute(
                "{ torrent { metrics(input: { bucketDuration: minute, \
                 startTime: \"not-a-time\" }) { buckets { count } } } }",
            )
            .await;
        assert_eq!(response.errors.len(), 1);
        assert!(response.errors[0].message.contains("is not RFC3339"));
    }

    #[test]
    fn duration_format_matches_gqlgen_sosodev_decomposition() {
        assert_eq!(format_duration(0), "PT0S");
        assert_eq!(format_duration(1_234_567_890), "PT1.23456789S");
        assert_eq!(format_duration(3_661_234_567_890), "PT1H1M1.23456789S");
        assert_eq!(format_duration(34_858_800_000_000_000), "P1Y1M1W1DT1H");
    }
}
