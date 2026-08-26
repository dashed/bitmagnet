//! Bounded, SELECT-only implementation of `queue.jobs`.
//!
//! The production authority is `internal/gql/gqlmodel/queue_jobs.go` plus the
//! generic query/facet package. The adapter preserves that accepted-input
//! behavior while rejecting windows larger than the current WebUI contract.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_graphql::{Error, MaybeUndefined, Result};
use async_trait::async_trait;
use bitmagnet_db::PgPool;
use chrono::{DateTime as ChronoDateTime, SecondsFormat, Utc};
use sqlx::{FromRow, Postgres, QueryBuilder};
use thiserror::Error;

use super::enums::{QueueJobStatus, QueueJobsOrderByField};
use super::inputs::{
    QueueJobQueueFacetInput, QueueJobStatusFacetInput, QueueJobsFacetsInput, QueueJobsQueryInput,
};
use super::objects::{
    QueueJob, QueueJobQueueAgg, QueueJobStatusAgg, QueueJobsAggregations, QueueJobsQueryResult,
};
use super::scalars::DateTime;

const DEFAULT_QUEUE_JOBS_LIMIT: i32 = 10;
/// Largest page exposed by both the current React UI and legacy Angular UI.
pub const MAX_QUEUE_JOBS_LIMIT: i32 = 100;
/// Maximum accepted computed `(page - 1) * limit + offset` window.
pub const MAX_QUEUE_JOBS_OFFSET: i64 = 1_000_000;
/// Maximum entries accepted in queue and facet filter lists.
pub const MAX_QUEUE_JOBS_FILTER_VALUES: usize = 64;
/// Maximum Unicode scalar count accepted for a queue name.
pub const MAX_QUEUE_NAME_CHARS: usize = 256;

const QUEUE_FACET_VALUES: [&str; 2] = ["process_torrent", "process_torrent_batch"];
const STATUS_FACET_VALUES: [&str; 4] = ["failed", "pending", "processed", "retry"];

/// One field in the caller-controlled stable ordering list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueJobsOrderField {
    CreatedAt,
    RanAt,
    Priority,
}

/// A normalized queue-job order clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueJobsOrder {
    pub field: QueueJobsOrderField,
    pub descending: bool,
}

/// Normalized aggregation and filtering controls for one facet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueJobsFacetRequest {
    pub aggregate: bool,
    pub filter: Option<Vec<String>>,
}

/// Bounded request passed to a queue-jobs runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueJobsRequest {
    pub queues: Option<Vec<String>>,
    pub statuses: Option<Vec<String>>,
    pub limit: i64,
    pub offset: i64,
    pub total_count: bool,
    pub has_next_page: bool,
    pub queue_facet: Option<QueueJobsFacetRequest>,
    pub status_facet: Option<QueueJobsFacetRequest>,
    pub order_by: Vec<QueueJobsOrder>,
}

/// One row returned by the SELECT-only queue runtime.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct QueueJobRecord {
    pub id: String,
    pub queue: String,
    pub status: String,
    pub payload: String,
    pub priority: i32,
    pub retries: i32,
    pub max_retries: i32,
    pub run_after: ChronoDateTime<Utc>,
    pub ran_at: Option<ChronoDateTime<Utc>>,
    pub error: Option<String>,
    pub created_at: ChronoDateTime<Utc>,
}

/// One already ordered facet bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueJobsAggRecord {
    pub value: String,
    pub count: i64,
}

/// Runtime result before GraphQL scalar and enum mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueJobsRecord {
    pub total_count: i64,
    pub has_next_page: bool,
    pub items: Vec<QueueJobRecord>,
    pub queue_aggregations: Option<Vec<QueueJobsAggRecord>>,
    pub status_aggregations: Option<Vec<QueueJobsAggRecord>>,
}

/// Typed failures from the queue-jobs adapter.
#[derive(Debug, Error)]
pub enum QueueJobsError {
    #[error("queue.jobs is unavailable without a PostgreSQL runtime")]
    Disabled,
    #[error("queue.jobs PostgreSQL read failed: {0}")]
    Database(#[from] sqlx::Error),
}

/// Runtime seam for bounded queue-job reads.
#[async_trait]
pub trait QueueJobsRuntime: Send + Sync {
    async fn queue_jobs(
        &self,
        request: QueueJobsRequest,
    ) -> std::result::Result<QueueJobsRecord, QueueJobsError>;
}

struct DisabledQueueJobsRuntime;

#[async_trait]
impl QueueJobsRuntime for DisabledQueueJobsRuntime {
    async fn queue_jobs(
        &self,
        _request: QueueJobsRequest,
    ) -> std::result::Result<QueueJobsRecord, QueueJobsError> {
        Err(QueueJobsError::Disabled)
    }
}

/// PostgreSQL implementation of the queue-jobs seam.
pub struct PgQueueJobsRuntime {
    pool: PgPool,
}

impl PgQueueJobsRuntime {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl QueueJobsRuntime for PgQueueJobsRuntime {
    async fn queue_jobs(
        &self,
        request: QueueJobsRequest,
    ) -> std::result::Result<QueueJobsRecord, QueueJobsError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *transaction)
            .await?;

        let mut total_count = 0;
        if request.total_count {
            let mut query =
                QueryBuilder::<Postgres>::new("SELECT count(*)::bigint FROM queue_jobs");
            push_filters(&mut query, &request, true, true);
            total_count = query
                .build_query_scalar::<i64>()
                .fetch_one(&mut *transaction)
                .await?;
        }

        let mut items = Vec::new();
        if request.limit != 0 || request.has_next_page {
            let mut query = QueryBuilder::<Postgres>::new(
                "SELECT id, queue, status::text AS status, payload::text AS payload, \
                 priority, retries, max_retries, run_after, ran_at, error, created_at \
                 FROM queue_jobs",
            );
            push_filters(&mut query, &request, true, true);
            push_order(&mut query, &request.order_by);
            query.push(" LIMIT ");
            query.push_bind(if request.has_next_page {
                request.limit + 1
            } else {
                request.limit
            });
            query.push(" OFFSET ");
            query.push_bind(request.offset);
            items = query
                .build_query_as::<QueueJobRecord>()
                .fetch_all(&mut *transaction)
                .await?;
        }
        let has_next_page =
            request.has_next_page && i64::try_from(items.len()).unwrap_or(i64::MAX) > request.limit;
        if has_next_page {
            items.pop();
        }

        let queue_aggregations = if request
            .queue_facet
            .as_ref()
            .is_some_and(|facet| facet.aggregate)
        {
            Some(load_aggregations(&mut transaction, &request, AggregationKind::Queue).await?)
        } else {
            None
        };
        let status_aggregations = if request
            .status_facet
            .as_ref()
            .is_some_and(|facet| facet.aggregate)
        {
            Some(load_aggregations(&mut transaction, &request, AggregationKind::Status).await?)
        } else {
            None
        };

        transaction.commit().await?;
        Ok(QueueJobsRecord {
            total_count,
            has_next_page,
            items,
            queue_aggregations,
            status_aggregations,
        })
    }
}

#[derive(Clone, Copy)]
enum AggregationKind {
    Queue,
    Status,
}

async fn load_aggregations(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    request: &QueueJobsRequest,
    kind: AggregationKind,
) -> std::result::Result<Vec<QueueJobsAggRecord>, sqlx::Error> {
    let (expression, known_values, selected) = match kind {
        AggregationKind::Queue => (
            "queue",
            QUEUE_FACET_VALUES.as_slice(),
            request
                .queue_facet
                .as_ref()
                .and_then(|facet| facet.filter.as_ref()),
        ),
        AggregationKind::Status => (
            "status::text",
            STATUS_FACET_VALUES.as_slice(),
            request
                .status_facet
                .as_ref()
                .and_then(|facet| facet.filter.as_ref()),
        ),
    };
    let mut query = QueryBuilder::<Postgres>::new("SELECT ");
    query.push(expression);
    query.push(" AS value, count(*)::bigint AS count FROM queue_jobs");
    push_filters(
        &mut query,
        request,
        !matches!(kind, AggregationKind::Queue),
        !matches!(kind, AggregationKind::Status),
    );
    query.push(" GROUP BY ");
    query.push(expression);
    let rows = query
        .build_query_as::<(String, i64)>()
        .fetch_all(&mut **transaction)
        .await?;
    let counts = rows.into_iter().collect::<BTreeMap<_, _>>();
    Ok(known_values
        .iter()
        .filter_map(|value| {
            let count = counts.get(*value).copied().unwrap_or_default();
            if count > 0 || selected.is_some_and(|values| values.iter().any(|item| item == value)) {
                Some(QueueJobsAggRecord {
                    value: (*value).to_owned(),
                    count,
                })
            } else {
                None
            }
        })
        .collect())
}

fn push_filters(
    query: &mut QueryBuilder<Postgres>,
    request: &QueueJobsRequest,
    include_queue_facet: bool,
    include_status_facet: bool,
) {
    let mut has_where = false;
    let mut push = |sql: &str, values: &Vec<String>| {
        query.push(if has_where { " AND " } else { " WHERE " });
        has_where = true;
        query.push(sql);
        query.push(" = ANY(");
        query.push_bind(values.clone());
        query.push("::text[])");
    };
    if let Some(values) = &request.queues {
        push("queue", values);
    }
    if let Some(values) = &request.statuses {
        push("status::text", values);
    }
    if include_queue_facet {
        if let Some(values) = request
            .queue_facet
            .as_ref()
            .and_then(|facet| facet.filter.as_ref())
        {
            push("queue", values);
        }
    }
    if include_status_facet {
        if let Some(values) = request
            .status_facet
            .as_ref()
            .and_then(|facet| facet.filter.as_ref())
        {
            push("status::text", values);
        }
    }
}

fn push_order(query: &mut QueryBuilder<Postgres>, order_by: &[QueueJobsOrder]) {
    if order_by.is_empty() {
        return;
    }
    query.push(" ORDER BY ");
    for (index, order) in order_by.iter().enumerate() {
        if index > 0 {
            query.push(", ");
        }
        query.push(match order.field {
            QueueJobsOrderField::CreatedAt => "created_at",
            QueueJobsOrderField::RanAt => "ran_at",
            QueueJobsOrderField::Priority => "priority",
        });
        query.push(if order.descending { " DESC" } else { " ASC" });
    }
}

/// GraphQL context wrapper for a queue-jobs runtime.
#[derive(Clone)]
pub struct QueueJobsRuntimeData(Arc<dyn QueueJobsRuntime>);

impl QueueJobsRuntimeData {
    #[must_use]
    pub fn new(runtime: Arc<dyn QueueJobsRuntime>) -> Self {
        Self(runtime)
    }

    #[must_use]
    pub fn disabled() -> Self {
        Self::new(Arc::new(DisabledQueueJobsRuntime))
    }

    #[must_use]
    pub fn pg(pool: PgPool) -> Self {
        Self::new(Arc::new(PgQueueJobsRuntime::new(pool)))
    }
}

pub(super) async fn resolve(
    runtime: &QueueJobsRuntimeData,
    input: QueueJobsQueryInput,
) -> Result<QueueJobsQueryResult> {
    let request = normalize_input(input)?;
    let result = runtime
        .0
        .queue_jobs(request)
        .await
        .map_err(|error| Error::new(error.to_string()))?;
    Ok(QueueJobsQueryResult {
        total_count: graphql_i32(result.total_count, "totalCount")?,
        has_next_page: Some(result.has_next_page),
        items: result
            .items
            .into_iter()
            .map(graphql_job)
            .collect::<Result<Vec<_>>>()?,
        aggregations: QueueJobsAggregations {
            queue: result
                .queue_aggregations
                .map(|items| {
                    items
                        .into_iter()
                        .map(|item| {
                            Ok(QueueJobQueueAgg {
                                value: item.value.clone(),
                                label: item.value,
                                count: graphql_i32(item.count, "queue aggregation count")?,
                            })
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?,
            status: result
                .status_aggregations
                .map(|items| {
                    items
                        .into_iter()
                        .map(|item| {
                            Ok(QueueJobStatusAgg {
                                value: graphql_status(&item.value)?,
                                label: item.value,
                                count: graphql_i32(item.count, "status aggregation count")?,
                            })
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?,
        },
    })
}

fn graphql_job(item: QueueJobRecord) -> Result<QueueJob> {
    Ok(QueueJob {
        id: item.id.into(),
        queue: item.queue,
        status: graphql_status(&item.status)?,
        payload: item.payload,
        priority: item.priority,
        retries: item.retries,
        max_retries: item.max_retries,
        run_after: graphql_datetime(item.run_after),
        ran_at: item.ran_at.map(graphql_datetime),
        error: item.error,
        created_at: graphql_datetime(item.created_at),
    })
}

fn graphql_datetime(value: ChronoDateTime<Utc>) -> DateTime {
    DateTime(value.to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

fn graphql_status(value: &str) -> Result<QueueJobStatus> {
    match value {
        "failed" => Ok(QueueJobStatus::Failed),
        "pending" => Ok(QueueJobStatus::Pending),
        "processed" => Ok(QueueJobStatus::Processed),
        "retry" => Ok(QueueJobStatus::Retry),
        _ => Err(Error::new(format!(
            "queue.jobs returned unknown status {value:?}"
        ))),
    }
}

fn graphql_i32(value: i64, field: &str) -> Result<i32> {
    i32::try_from(value).map_err(|_| {
        Error::new(format!(
            "queue.jobs {field} is outside the GraphQL Int range"
        ))
    })
}

fn normalize_input(input: QueueJobsQueryInput) -> Result<QueueJobsRequest> {
    let QueueJobsQueryInput {
        facets,
        has_next_page,
        limit,
        offset,
        order_by,
        page,
        queues,
        statuses,
        total_count,
    } = input;
    validate_values("queues", queues.as_deref(), MAX_QUEUE_JOBS_FILTER_VALUES)?;
    if let Some(values) = &queues {
        if values
            .iter()
            .any(|value| value.chars().count() > MAX_QUEUE_NAME_CHARS)
        {
            return Err(Error::new(format!(
                "queue.jobs queue exceeds {MAX_QUEUE_NAME_CHARS} characters"
            )));
        }
    }
    validate_values("statuses", statuses.as_deref(), STATUS_FACET_VALUES.len())?;
    let statuses = statuses.map(|values| values.into_iter().map(status_name).collect());

    let limit = maybe_i32(limit).unwrap_or(DEFAULT_QUEUE_JOBS_LIMIT);
    if !(0..=MAX_QUEUE_JOBS_LIMIT).contains(&limit) {
        return Err(Error::new(format!(
            "queue.jobs limit must be between 0 and {MAX_QUEUE_JOBS_LIMIT}"
        )));
    }
    let page = maybe_i32(page).unwrap_or_default();
    let direct_offset = maybe_i32(offset).unwrap_or_default();
    if page < 0 || direct_offset < 0 {
        return Err(Error::new("queue.jobs page and offset must be nonnegative"));
    }
    let page_offset = if page > 0 {
        i64::from(page - 1)
            .checked_mul(i64::from(limit))
            .ok_or_else(|| Error::new("queue.jobs page offset overflow"))?
    } else {
        0
    };
    let offset = page_offset
        .checked_add(i64::from(direct_offset))
        .ok_or_else(|| Error::new("queue.jobs offset overflow"))?;
    if offset > MAX_QUEUE_JOBS_OFFSET {
        return Err(Error::new(format!(
            "queue.jobs computed offset exceeds {MAX_QUEUE_JOBS_OFFSET}"
        )));
    }

    let (queue_facet, status_facet) = match facets {
        MaybeUndefined::Value(QueueJobsFacetsInput { queue, status }) => (
            normalize_queue_facet(queue)?,
            normalize_status_facet(status)?,
        ),
        MaybeUndefined::Undefined | MaybeUndefined::Null => (None, None),
    };

    let mut normalized_order = Vec::<QueueJobsOrder>::new();
    for order in order_by.unwrap_or_default() {
        let field = match order.field {
            QueueJobsOrderByField::CreatedAt => QueueJobsOrderField::CreatedAt,
            QueueJobsOrderByField::RanAt => QueueJobsOrderField::RanAt,
            QueueJobsOrderByField::Priority => QueueJobsOrderField::Priority,
        };
        let descending = maybe_bool(order.descending).unwrap_or(false);
        if let Some(existing) = normalized_order.iter_mut().find(|item| item.field == field) {
            existing.descending = descending;
        } else {
            normalized_order.push(QueueJobsOrder { field, descending });
        }
    }

    Ok(QueueJobsRequest {
        queues,
        statuses,
        limit: i64::from(limit),
        offset,
        total_count: maybe_bool(total_count).unwrap_or(false),
        has_next_page: maybe_bool(has_next_page).unwrap_or(false),
        queue_facet,
        status_facet,
        order_by: normalized_order,
    })
}

fn normalize_queue_facet(
    input: MaybeUndefined<QueueJobQueueFacetInput>,
) -> Result<Option<QueueJobsFacetRequest>> {
    let MaybeUndefined::Value(input) = input else {
        return Ok(None);
    };
    validate_values(
        "queue facet filter",
        input.filter.as_deref(),
        MAX_QUEUE_JOBS_FILTER_VALUES,
    )?;
    if input.filter.as_ref().is_some_and(|values| {
        values
            .iter()
            .any(|value| value.chars().count() > MAX_QUEUE_NAME_CHARS)
    }) {
        return Err(Error::new(format!(
            "queue.jobs queue facet value exceeds {MAX_QUEUE_NAME_CHARS} characters"
        )));
    }
    Ok(Some(QueueJobsFacetRequest {
        aggregate: maybe_bool(input.aggregate).unwrap_or(false),
        filter: input.filter,
    }))
}

fn normalize_status_facet(
    input: MaybeUndefined<QueueJobStatusFacetInput>,
) -> Result<Option<QueueJobsFacetRequest>> {
    let MaybeUndefined::Value(input) = input else {
        return Ok(None);
    };
    validate_values(
        "status facet filter",
        input.filter.as_deref(),
        STATUS_FACET_VALUES.len(),
    )?;
    Ok(Some(QueueJobsFacetRequest {
        aggregate: maybe_bool(input.aggregate).unwrap_or(false),
        filter: input
            .filter
            .map(|values| values.into_iter().map(status_name).collect()),
    }))
}

fn validate_values<T>(field: &str, values: Option<&[T]>, max: usize) -> Result<()> {
    if values.is_some_and(|values| values.len() > max) {
        return Err(Error::new(format!(
            "queue.jobs {field} has more than {max} entries"
        )));
    }
    Ok(())
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

fn maybe_i32(value: MaybeUndefined<i32>) -> Option<i32> {
    value.value().copied()
}

fn maybe_bool(value: MaybeUndefined<bool>) -> Option<bool> {
    value.value().copied()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_graphql::{value, EmptySubscription};

    use super::*;
    use crate::schema::roots::{Mutation, Query};

    struct FakeRuntime {
        request: Mutex<Option<QueueJobsRequest>>,
    }

    #[async_trait]
    impl QueueJobsRuntime for FakeRuntime {
        async fn queue_jobs(
            &self,
            request: QueueJobsRequest,
        ) -> std::result::Result<QueueJobsRecord, QueueJobsError> {
            *self.request.lock().expect("request mutex") = Some(request);
            Ok(QueueJobsRecord {
                total_count: 1,
                has_next_page: false,
                items: vec![QueueJobRecord {
                    id: "job-1".to_owned(),
                    queue: "process_torrent".to_owned(),
                    status: "pending".to_owned(),
                    payload: "{\"kind\": \"fixture\"}".to_owned(),
                    priority: 4,
                    retries: 0,
                    max_retries: 3,
                    run_after: "2024-06-01T00:00:00Z".parse().expect("run_after"),
                    ran_at: None,
                    error: None,
                    created_at: "2024-06-01T00:00:00Z".parse().expect("created_at"),
                }],
                queue_aggregations: Some(vec![QueueJobsAggRecord {
                    value: "process_torrent".to_owned(),
                    count: 1,
                }]),
                status_aggregations: None,
            })
        }
    }

    #[tokio::test]
    async fn schema_maps_rows_and_normalizes_duplicate_order_fields() {
        let runtime = Arc::new(FakeRuntime {
            request: Mutex::new(None),
        });
        let schema = async_graphql::Schema::build(Query, Mutation, EmptySubscription)
            .data(QueueJobsRuntimeData::new(runtime.clone()))
            .finish();
        let response = schema
            .execute(
                "{ queue { jobs(input: { limit: 20, page: 2, offset: 1, \
                 totalCount: true, facets: { queue: { aggregate: true } }, \
                 orderBy: [{ field: priority }, { field: created_at, descending: true }, \
                 { field: priority, descending: true }] }) { totalCount hasNextPage \
                 items { id status ranAt } aggregations { queue { value label count } status { value } } } } }",
            )
            .await;

        assert!(response.errors.is_empty(), "errors: {:?}", response.errors);
        assert_eq!(
            response.data,
            value!({ "queue": { "jobs": {
                "totalCount": 1,
                "hasNextPage": false,
                "items": [{ "id": "job-1", "status": "pending", "ranAt": null }],
                "aggregations": {
                    "queue": [{ "value": "process_torrent", "label": "process_torrent", "count": 1 }],
                    "status": null,
                },
            } } })
        );
        let request = runtime
            .request
            .lock()
            .expect("request mutex")
            .clone()
            .expect("captured request");
        assert_eq!(request.offset, 21);
        assert_eq!(
            request.order_by,
            [
                QueueJobsOrder {
                    field: QueueJobsOrderField::Priority,
                    descending: true,
                },
                QueueJobsOrder {
                    field: QueueJobsOrderField::CreatedAt,
                    descending: true,
                },
            ]
        );
    }

    #[tokio::test]
    async fn bounds_fail_before_runtime() {
        let response = crate::schema::schema()
            .execute(format!(
                "{{ queue {{ jobs(input: {{ limit: {} }}) {{ totalCount }} }} }}",
                MAX_QUEUE_JOBS_LIMIT + 1
            ))
            .await;
        assert_eq!(response.errors.len(), 1);
        assert!(response.errors[0].message.contains("limit must be between"));
    }
}
