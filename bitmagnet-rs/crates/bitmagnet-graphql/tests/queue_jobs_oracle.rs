//! Replays the production-Go `queue.jobs` oracle through the Rust schema.

use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Variables};
use async_trait::async_trait;
use bitmagnet_graphql::schema::{Mutation, Query};
use bitmagnet_graphql::{
    QueueJobRecord, QueueJobsAggRecord, QueueJobsError, QueueJobsOrder, QueueJobsOrderField,
    QueueJobsRecord, QueueJobsRequest, QueueJobsRuntime, QueueJobsRuntimeData,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    id: String,
    queue: String,
    status: String,
    payload: String,
    priority: i32,
    retries: i32,
    max_retries: i32,
    run_after: DateTime<Utc>,
    ran_at: Option<DateTime<Utc>>,
    error: Option<String>,
    created_at: DateTime<Utc>,
}

impl From<Fixture> for QueueJobRecord {
    fn from(value: Fixture) -> Self {
        Self {
            id: value.id,
            queue: value.queue,
            status: value.status,
            payload: value.payload,
            priority: value.priority,
            retries: value.retries,
            max_retries: value.max_retries,
            run_after: value.run_after,
            ran_at: value.ran_at,
            error: value.error,
            created_at: value.created_at,
        }
    }
}

#[derive(Debug, Deserialize)]
struct OracleCase {
    id: String,
    input: Value,
    expected: Value,
}

#[derive(Clone)]
struct FixtureRuntime {
    rows: Vec<QueueJobRecord>,
}

#[async_trait]
impl QueueJobsRuntime for FixtureRuntime {
    async fn queue_jobs(
        &self,
        request: QueueJobsRequest,
    ) -> Result<QueueJobsRecord, QueueJobsError> {
        let mut matching = self
            .rows
            .iter()
            .filter(|row| matches_request(row, &request, true, true))
            .cloned()
            .collect::<Vec<_>>();
        matching.sort_by(|left, right| compare_rows(left, right, &request.order_by));

        let total_count = if request.total_count {
            i64::try_from(matching.len()).unwrap_or(i64::MAX)
        } else {
            0
        };
        let offset = usize::try_from(request.offset).unwrap_or(usize::MAX);
        let wanted =
            usize::try_from(request.limit).unwrap_or_default() + usize::from(request.has_next_page);
        let mut items = matching
            .into_iter()
            .skip(offset)
            .take(wanted)
            .collect::<Vec<_>>();
        let has_next_page = request.has_next_page
            && items.len() > usize::try_from(request.limit).unwrap_or_default();
        if has_next_page {
            items.pop();
        }

        let queue_aggregations = request
            .queue_facet
            .as_ref()
            .filter(|facet| facet.aggregate)
            .map(|facet| {
                aggregate(
                    &self.rows,
                    &request,
                    false,
                    true,
                    &["process_torrent", "process_torrent_batch"],
                    facet.filter.as_deref(),
                    |row| &row.queue,
                )
            });
        let status_aggregations = request
            .status_facet
            .as_ref()
            .filter(|facet| facet.aggregate)
            .map(|facet| {
                aggregate(
                    &self.rows,
                    &request,
                    true,
                    false,
                    &["failed", "pending", "processed", "retry"],
                    facet.filter.as_deref(),
                    |row| &row.status,
                )
            });

        Ok(QueueJobsRecord {
            total_count,
            has_next_page,
            items,
            queue_aggregations,
            status_aggregations,
        })
    }
}

#[tokio::test]
async fn rust_schema_matches_production_go_queue_jobs_oracle() {
    let fixtures: Vec<Fixture> = serde_json::from_slice(
        &fs::read(parity_dir().join("fixtures.json")).expect("read queue.jobs fixtures"),
    )
    .expect("decode queue.jobs fixtures");
    let runtime: Arc<dyn QueueJobsRuntime> = Arc::new(FixtureRuntime {
        rows: fixtures.into_iter().map(QueueJobRecord::from).collect(),
    });
    let schema = async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(QueueJobsRuntimeData::new(runtime))
        .finish();

    for case in load_cases(&parity_dir().join("corpus.jsonl")) {
        let response = schema
            .execute(
                Request::new(
                    "query QueueJobsParity($input: QueueJobsQueryInput!) {\
                     queue { jobs(input: $input) {\
                     totalCount hasNextPage items { id queue status payload priority retries \
                     maxRetries runAfter ranAt error createdAt } aggregations {\
                     queue { value label count } status { value label count } } } } }",
                )
                .variables(Variables::from_json(
                    serde_json::json!({ "input": case.input }),
                )),
            )
            .await;
        assert!(
            response.errors.is_empty(),
            "oracle case {:?} returned errors: {:?}",
            case.id,
            response.errors
        );
        assert_eq!(
            serde_json::to_value(response.data).expect("encode GraphQL response data"),
            serde_json::json!({ "queue": { "jobs": case.expected } }),
            "oracle case {:?}",
            case.id
        );
    }
}

fn matches_request(
    row: &QueueJobRecord,
    request: &QueueJobsRequest,
    include_queue_facet: bool,
    include_status_facet: bool,
) -> bool {
    matches_filter(&row.queue, request.queues.as_deref())
        && matches_filter(&row.status, request.statuses.as_deref())
        && (!include_queue_facet
            || matches_filter(
                &row.queue,
                request
                    .queue_facet
                    .as_ref()
                    .and_then(|facet| facet.filter.as_deref()),
            ))
        && (!include_status_facet
            || matches_filter(
                &row.status,
                request
                    .status_facet
                    .as_ref()
                    .and_then(|facet| facet.filter.as_deref()),
            ))
}

fn matches_filter(value: &str, filter: Option<&[String]>) -> bool {
    filter.is_none_or(|items| items.iter().any(|item| item == value))
}

fn compare_rows(
    left: &QueueJobRecord,
    right: &QueueJobRecord,
    order_by: &[QueueJobsOrder],
) -> Ordering {
    for order in order_by {
        let ordering = match order.field {
            QueueJobsOrderField::CreatedAt => left.created_at.cmp(&right.created_at),
            QueueJobsOrderField::RanAt => compare_nullable_timestamp(left.ran_at, right.ran_at),
            QueueJobsOrderField::Priority => left.priority.cmp(&right.priority),
        };
        let ordering = if order.descending {
            ordering.reverse()
        } else {
            ordering
        };
        if !ordering.is_eq() {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compare_nullable_timestamp(
    left: Option<DateTime<Utc>>,
    right: Option<DateTime<Utc>>,
) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn aggregate<'a>(
    rows: &'a [QueueJobRecord],
    request: &QueueJobsRequest,
    include_queue_facet: bool,
    include_status_facet: bool,
    values: &[&str],
    selected: Option<&[String]>,
    value_of: impl Fn(&'a QueueJobRecord) -> &'a str,
) -> Vec<QueueJobsAggRecord> {
    values
        .iter()
        .filter_map(|value| {
            let count = rows
                .iter()
                .filter(|row| {
                    matches_request(row, request, include_queue_facet, include_status_facet)
                        && value_of(row) == *value
                })
                .count();
            if count > 0 || selected.is_some_and(|items| items.iter().any(|item| item == value)) {
                Some(QueueJobsAggRecord {
                    value: (*value).to_owned(),
                    count: i64::try_from(count).unwrap_or(i64::MAX),
                })
            } else {
                None
            }
        })
        .collect()
}

fn load_cases(path: &Path) -> Vec<OracleCase> {
    fs::read_to_string(path)
        .expect("read queue.jobs corpus")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("decode queue.jobs oracle line"))
        .collect()
}

fn parity_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("testdata/parity/graphql-queue-jobs")
}
