//! Disabled-by-default, separately authorized queue mutation family.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_graphql::{Error, MaybeUndefined, Result};
use async_trait::async_trait;
use bitmagnet_db::PgPool;
use bitmagnet_queue::{
    insert_jobs_strict_with_executor, process_torrent_batch_job, GoTime, JobError,
    PreparedQueueJob, ProcessTorrentBatchParams, QueueJobOptions, QueuePgError,
    CLASSIFY_MODE_DEFAULT, CLASSIFY_MODE_REMATCH,
};
use chrono::{DateTime, Utc};
use serde_json::Value;
use thiserror::Error;

use super::enums::{ContentType, QueueJobStatus};
use super::inputs::{QueueEnqueueReprocessTorrentsBatchInput, QueuePurgeJobsInput};
use super::queue_jobs::{MAX_QUEUE_JOBS_FILTER_VALUES, MAX_QUEUE_NAME_CHARS};

/// Normalized request for `queue.purgeJobs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeJobsRequest {
    /// Optional queue-name filter. Empty selects the full queue domain.
    pub queues: Vec<String>,
    /// Optional status filter. Empty selects the full status domain.
    pub statuses: Vec<String>,
}

/// Normalized request for `queue.enqueueReprocessTorrentsBatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct EnqueueReprocessTorrentsBatchRequest {
    /// Atomically truncate all queue jobs before inserting the new job.
    pub purge: bool,
    pub batch_size: u64,
    pub chunk_size: u64,
    /// Nullable elements are preserved exactly like Go's `NullContentType`.
    pub content_types: Vec<Option<String>>,
    pub orphans: bool,
    pub classify_mode: i32,
    pub classifier_workflow: String,
    pub classifier_flags: Option<BTreeMap<String, Value>>,
}

/// Typed failures from the queue mutation adapter.
#[derive(Debug, Error)]
pub enum QueueMutationsError {
    /// The schema was built without the separately authenticated queue writer.
    #[error("queue mutations are disabled")]
    Disabled,
    /// Queue-job construction failed.
    #[error("queue mutation job construction failed: {0}")]
    Job(#[from] JobError),
    /// Queue-job validation or insertion failed.
    #[error("queue mutation queue write failed: {0}")]
    Queue(#[from] QueuePgError),
    /// A direct PostgreSQL purge or transaction operation failed.
    #[error("queue mutation PostgreSQL write failed: {0}")]
    Database(#[from] sqlx::Error),
}

/// Runtime seam for the two queue mutations.
#[async_trait]
pub trait QueueMutationsRuntime: Send + Sync {
    async fn purge_jobs(
        &self,
        request: PurgeJobsRequest,
    ) -> std::result::Result<(), QueueMutationsError>;

    async fn enqueue_reprocess_torrents_batch(
        &self,
        request: EnqueueReprocessTorrentsBatchRequest,
    ) -> std::result::Result<(), QueueMutationsError>;
}

struct DisabledQueueMutationsRuntime;

#[async_trait]
impl QueueMutationsRuntime for DisabledQueueMutationsRuntime {
    async fn purge_jobs(
        &self,
        _request: PurgeJobsRequest,
    ) -> std::result::Result<(), QueueMutationsError> {
        Err(QueueMutationsError::Disabled)
    }

    async fn enqueue_reprocess_torrents_batch(
        &self,
        _request: EnqueueReprocessTorrentsBatchRequest,
    ) -> std::result::Result<(), QueueMutationsError> {
        Err(QueueMutationsError::Disabled)
    }
}

#[derive(Clone)]
enum QueueMutationClock {
    System,
    Fixed {
        updated_before: DateTime<Utc>,
        run_after: DateTime<Utc>,
    },
}

impl QueueMutationClock {
    fn times(&self) -> (DateTime<Utc>, DateTime<Utc>) {
        match self {
            Self::System => (Utc::now(), Utc::now()),
            Self::Fixed {
                updated_before,
                run_after,
            } => (*updated_before, *run_after),
        }
    }
}

/// PostgreSQL implementation backed by a caller-owned, separately authorized pool.
pub struct PgQueueMutationsRuntime {
    pool: PgPool,
    clock: QueueMutationClock,
}

impl PgQueueMutationsRuntime {
    /// Constructs the production writer adapter.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            clock: QueueMutationClock::System,
        }
    }

    /// Constructs a deterministic adapter for disposable PostgreSQL proofs.
    #[must_use]
    pub fn fixed(pool: PgPool, updated_before: DateTime<Utc>, run_after: DateTime<Utc>) -> Self {
        Self {
            pool,
            clock: QueueMutationClock::Fixed {
                updated_before,
                run_after,
            },
        }
    }
}

#[async_trait]
impl QueueMutationsRuntime for PgQueueMutationsRuntime {
    async fn purge_jobs(
        &self,
        request: PurgeJobsRequest,
    ) -> std::result::Result<(), QueueMutationsError> {
        purge_jobs(&self.pool, &request).await?;
        Ok(())
    }

    async fn enqueue_reprocess_torrents_batch(
        &self,
        request: EnqueueReprocessTorrentsBatchRequest,
    ) -> std::result::Result<(), QueueMutationsError> {
        let (updated_before, run_after) = self.clock.times();
        let params = ProcessTorrentBatchParams {
            updated_before: GoTime::from_utc(updated_before),
            classify_mode: request.classify_mode,
            classifier_workflow: request.classifier_workflow,
            classifier_flags: request.classifier_flags,
            chunk_size: request.chunk_size,
            batch_size: request.batch_size,
            content_types: request.content_types,
            orphans: request.orphans,
            ..ProcessTorrentBatchParams::default()
        };
        let job = process_torrent_batch_job(&params, QueueJobOptions::default())?;
        let prepared = PreparedQueueJob::materialize_at(job, run_after)?;

        let mut transaction = self.pool.begin().await?;
        if request.purge {
            sqlx::query("TRUNCATE TABLE queue_jobs")
                .execute(&mut *transaction)
                .await?;
        }
        insert_jobs_strict_with_executor(&mut *transaction, std::slice::from_ref(&prepared))
            .await?;
        transaction.commit().await?;
        Ok(())
    }
}

async fn purge_jobs(pool: &PgPool, request: &PurgeJobsRequest) -> Result<(), sqlx::Error> {
    match (request.queues.is_empty(), request.statuses.is_empty()) {
        (true, true) => {
            sqlx::query("TRUNCATE TABLE queue_jobs")
                .execute(pool)
                .await?;
        }
        (false, true) => {
            sqlx::query("DELETE FROM queue_jobs WHERE queue = ANY($1::text[])")
                .bind(&request.queues)
                .execute(pool)
                .await?;
        }
        (true, false) => {
            sqlx::query("DELETE FROM queue_jobs WHERE status::text = ANY($1::text[])")
                .bind(&request.statuses)
                .execute(pool)
                .await?;
        }
        (false, false) => {
            sqlx::query(
                "DELETE FROM queue_jobs \
                 WHERE queue = ANY($1::text[]) AND status::text = ANY($2::text[])",
            )
            .bind(&request.queues)
            .bind(&request.statuses)
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

/// GraphQL context wrapper for the queue-mutation runtime.
#[derive(Clone)]
pub struct QueueMutationsRuntimeData(Arc<dyn QueueMutationsRuntime>);

impl QueueMutationsRuntimeData {
    /// Wraps an enabled runtime.
    #[must_use]
    pub fn new(runtime: Arc<dyn QueueMutationsRuntime>) -> Self {
        Self(runtime)
    }

    /// Constructs the default fail-loud runtime.
    #[must_use]
    pub fn disabled() -> Self {
        Self::new(Arc::new(DisabledQueueMutationsRuntime))
    }

    /// Constructs the production PostgreSQL writer runtime.
    #[must_use]
    pub fn pg(pool: PgPool) -> Self {
        Self::new(Arc::new(PgQueueMutationsRuntime::new(pool)))
    }

    /// Constructs a deterministic PostgreSQL writer runtime for parity proofs.
    #[must_use]
    pub fn pg_fixed(pool: PgPool, updated_before: DateTime<Utc>, run_after: DateTime<Utc>) -> Self {
        Self::new(Arc::new(PgQueueMutationsRuntime::fixed(
            pool,
            updated_before,
            run_after,
        )))
    }
}

pub(super) async fn resolve_purge(
    runtime: &QueueMutationsRuntimeData,
    input: QueuePurgeJobsInput,
) -> Result<()> {
    let request = normalize_purge(input)?;
    runtime
        .0
        .purge_jobs(request)
        .await
        .map_err(|error| Error::new(error.to_string()))
}

pub(super) async fn resolve_enqueue(
    runtime: &QueueMutationsRuntimeData,
    input: Option<QueueEnqueueReprocessTorrentsBatchInput>,
) -> Result<()> {
    let request = normalize_enqueue(input)?;
    runtime
        .0
        .enqueue_reprocess_torrents_batch(request)
        .await
        .map_err(|error| Error::new(error.to_string()))
}

fn normalize_purge(input: QueuePurgeJobsInput) -> Result<PurgeJobsRequest> {
    let queues = input.queues.unwrap_or_default();
    let statuses = input.statuses.unwrap_or_default();
    if queues.len() > MAX_QUEUE_JOBS_FILTER_VALUES {
        return Err(Error::new(format!(
            "queue.purgeJobs queues has more than {MAX_QUEUE_JOBS_FILTER_VALUES} entries"
        )));
    }
    if statuses.len() > MAX_QUEUE_JOBS_FILTER_VALUES {
        return Err(Error::new(format!(
            "queue.purgeJobs statuses has more than {MAX_QUEUE_JOBS_FILTER_VALUES} entries"
        )));
    }
    if queues
        .iter()
        .any(|queue| queue.chars().count() > MAX_QUEUE_NAME_CHARS)
    {
        return Err(Error::new(format!(
            "queue.purgeJobs queue exceeds {MAX_QUEUE_NAME_CHARS} characters"
        )));
    }
    Ok(PurgeJobsRequest {
        queues,
        statuses: statuses
            .into_iter()
            .map(queue_status_name)
            .map(str::to_owned)
            .collect(),
    })
}

fn normalize_enqueue(
    input: Option<QueueEnqueueReprocessTorrentsBatchInput>,
) -> Result<EnqueueReprocessTorrentsBatchRequest> {
    let Some(input) = input else {
        return Ok(EnqueueReprocessTorrentsBatchRequest {
            purge: false,
            batch_size: 0,
            chunk_size: 0,
            content_types: Vec::new(),
            orphans: false,
            classify_mode: CLASSIFY_MODE_DEFAULT,
            classifier_workflow: String::new(),
            classifier_flags: None,
        });
    };

    let batch_size = nonnegative_size("batchSize", input.batch_size)?;
    let chunk_size = nonnegative_size("chunkSize", input.chunk_size)?;
    let mut classifier_flags = BTreeMap::new();
    if input.apis_disabled.value().copied().unwrap_or(false) {
        classifier_flags.insert("apis_enabled".to_owned(), Value::Bool(false));
    }
    if input
        .local_search_disabled
        .value()
        .copied()
        .unwrap_or(false)
    {
        classifier_flags.insert("local_search_enabled".to_owned(), Value::Bool(false));
    }

    Ok(EnqueueReprocessTorrentsBatchRequest {
        purge: input.purge.value().copied().unwrap_or(false),
        batch_size,
        chunk_size,
        content_types: input
            .content_types
            .unwrap_or_default()
            .into_iter()
            .map(|value| value.map(content_type_name).map(str::to_owned))
            .collect(),
        orphans: input.orphans.value().copied().unwrap_or(false),
        classify_mode: if input.classifier_rematch.value().copied().unwrap_or(false) {
            CLASSIFY_MODE_REMATCH
        } else {
            CLASSIFY_MODE_DEFAULT
        },
        classifier_workflow: input
            .classifier_workflow
            .value()
            .cloned()
            .unwrap_or_default(),
        classifier_flags: (!classifier_flags.is_empty()).then_some(classifier_flags),
    })
}

fn nonnegative_size(name: &str, value: MaybeUndefined<i32>) -> Result<u64> {
    let value = value.value().copied().unwrap_or_default();
    u64::try_from(value)
        .map_err(|_| Error::new(format!("queue enqueue {name} must not be negative")))
}

const fn queue_status_name(status: QueueJobStatus) -> &'static str {
    match status {
        QueueJobStatus::Failed => "failed",
        QueueJobStatus::Pending => "pending",
        QueueJobStatus::Processed => "processed",
        QueueJobStatus::Retry => "retry",
    }
}

const fn content_type_name(content_type: ContentType) -> &'static str {
    match content_type {
        ContentType::Audiobook => "audiobook",
        ContentType::Comic => "comic",
        ContentType::Ebook => "ebook",
        ContentType::Game => "game",
        ContentType::Movie => "movie",
        ContentType::Music => "music",
        ContentType::Software => "software",
        ContentType::TvShow => "tv_show",
        ContentType::Xxx => "xxx",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_graphql::{value, EmptySubscription};

    use super::*;
    use crate::schema::roots::{Mutation, Query};

    #[derive(Debug, Clone, PartialEq)]
    enum Call {
        Purge(PurgeJobsRequest),
        Enqueue(EnqueueReprocessTorrentsBatchRequest),
    }

    struct FakeRuntime {
        calls: Arc<Mutex<Vec<Call>>>,
    }

    #[async_trait]
    impl QueueMutationsRuntime for FakeRuntime {
        async fn purge_jobs(
            &self,
            request: PurgeJobsRequest,
        ) -> std::result::Result<(), QueueMutationsError> {
            self.calls
                .lock()
                .expect("calls lock")
                .push(Call::Purge(request));
            Ok(())
        }

        async fn enqueue_reprocess_torrents_batch(
            &self,
            request: EnqueueReprocessTorrentsBatchRequest,
        ) -> std::result::Result<(), QueueMutationsError> {
            self.calls
                .lock()
                .expect("calls lock")
                .push(Call::Enqueue(request));
            Ok(())
        }
    }

    fn schema_with_fake(calls: Arc<Mutex<Vec<Call>>>) -> crate::schema::Schema {
        let runtime: Arc<dyn QueueMutationsRuntime> = Arc::new(FakeRuntime { calls });
        async_graphql::Schema::build(Query, Mutation, EmptySubscription)
            .data(QueueMutationsRuntimeData::new(runtime))
            .finish()
    }

    #[tokio::test]
    async fn graphql_queue_mutations_normalize_and_return_void() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let schema = schema_with_fake(Arc::clone(&calls));
        let purge = schema
            .execute(
                "mutation { queue { purgeJobs(input: { queues: [\"a\"], statuses: [retry] }) } }",
            )
            .await;
        assert!(purge.errors.is_empty(), "errors: {:?}", purge.errors);
        assert_eq!(purge.data, value!({ "queue": { "purgeJobs": null } }));

        let enqueue = schema
            .execute(
                "mutation { queue { enqueueReprocessTorrentsBatch(input: { \
                 purge: true, batchSize: 5, chunkSize: 25, contentTypes: [movie, null, tv_show], \
                 orphans: true, classifierRematch: true, classifierWorkflow: \"custom\", \
                 apisDisabled: true, localSearchDisabled: true }) } }",
            )
            .await;
        assert!(enqueue.errors.is_empty(), "errors: {:?}", enqueue.errors);

        let flags = BTreeMap::from([
            ("apis_enabled".to_owned(), Value::Bool(false)),
            ("local_search_enabled".to_owned(), Value::Bool(false)),
        ]);
        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![
                Call::Purge(PurgeJobsRequest {
                    queues: vec!["a".to_owned()],
                    statuses: vec!["retry".to_owned()],
                }),
                Call::Enqueue(EnqueueReprocessTorrentsBatchRequest {
                    purge: true,
                    batch_size: 5,
                    chunk_size: 25,
                    content_types: vec![Some("movie".to_owned()), None, Some("tv_show".to_owned()),],
                    orphans: true,
                    classify_mode: CLASSIFY_MODE_REMATCH,
                    classifier_workflow: "custom".to_owned(),
                    classifier_flags: Some(flags),
                }),
            ]
        );
    }

    #[tokio::test]
    async fn omitted_enqueue_input_uses_go_zero_values_and_negative_sizes_fail() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let schema = schema_with_fake(Arc::clone(&calls));
        let omitted = schema
            .execute("mutation { queue { enqueueReprocessTorrentsBatch } }")
            .await;
        assert!(omitted.errors.is_empty(), "errors: {:?}", omitted.errors);
        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![Call::Enqueue(EnqueueReprocessTorrentsBatchRequest {
                purge: false,
                batch_size: 0,
                chunk_size: 0,
                content_types: Vec::new(),
                orphans: false,
                classify_mode: CLASSIFY_MODE_DEFAULT,
                classifier_workflow: String::new(),
                classifier_flags: None,
            })]
        );

        let negative = schema
            .execute(
                "mutation { queue { enqueueReprocessTorrentsBatch(input: { batchSize: -1 }) } }",
            )
            .await;
        assert_eq!(negative.errors.len(), 1);
        assert!(negative.errors[0].message.contains("must not be negative"));
        assert_eq!(calls.lock().expect("calls lock").len(), 1);
    }
}
