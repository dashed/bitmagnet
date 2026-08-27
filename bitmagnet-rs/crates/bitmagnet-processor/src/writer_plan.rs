//! Composition of the supported processor writer intent.
//!
//! The loader, classifier materializer, volatile-field projection, and
//! transaction kernel deliberately landed as separate parity gates. This
//! module joins the first three into the exact input image expected by
//! [`crate::persist_write_set`]. [`persist_writer_plan`] is the deliberately
//! dormant mutation seam: it publishes partial-failure retry intent before
//! delegating to that transaction kernel. No queue consumer or application
//! runtime calls it yet; the ingest-shadow runtime remains comparison-only.

use std::collections::{BTreeMap, BTreeSet};

use bitmagnet_queue::{
    process_torrent_job, ProcessTorrentParams, ProtocolId, QueueJob, QueueJobOptions,
};
use futures::future::BoxFuture;
use sqlx::PgPool;

use crate::persist::{persist_write_set, validate_persistence_input};
use crate::supported_subset::{has_explicit_attach_flags_off, has_explicit_default_workflow};
use crate::{
    load_writer_torrents, project_unattached_persistence, BlockingManager, BoxError,
    MaterializeError, Materializer, PersistError, TorrentContentPersistence, WriteSet,
    WriterLoadError, WriterLoadedTorrent, WriterProjectionError,
};

/// A fully materialized supported-subset writer intent.
///
/// `persistence` is keyed by generated `torrent_contents.id` and has exactly
/// one entry for every row in `write_set.torrent_contents`. The fields are
/// private so callers cannot break that keyset after validation. Holding this
/// value performs no database mutation.
///
/// Failed hashes remain canonical in the write-set for comparison. The plan also
/// carries separate ordered retry intent, because queue payload order and missing-
/// hash multiplicity are fingerprint-significant. Matching Go's mutation boundary
/// requires any persisting runtime to enqueue that retry successfully before
/// persisting the successful portion of the same plan.
#[derive(Debug, PartialEq, Eq)]
pub struct WriterPlan {
    write_set: WriteSet,
    persistence: BTreeMap<String, TorrentContentPersistence>,
    retry_info_hashes: Vec<String>,
    retry_job: Option<QueueJob>,
}

impl WriterPlan {
    /// Borrow the canonical classification-derived and retry intent.
    #[must_use]
    pub fn write_set(&self) -> &WriteSet {
        &self.write_set
    }

    /// Borrow the exact volatile metadata keyed by `torrent_contents.id`.
    #[must_use]
    pub fn persistence(&self) -> &BTreeMap<String, TorrentContentPersistence> {
        &self.persistence
    }

    /// Hashes that a persisting writer must republish before persisting successes.
    ///
    /// Missing requested hashes form an order- and duplicate-preserving prefix;
    /// loaded classifier failures follow in Rust's first-request processing order.
    /// The canonical comparison set remains available through [`Self::write_set`].
    #[must_use]
    pub fn retry_info_hashes(&self) -> &[String] {
        &self.retry_info_hashes
    }

    /// Borrow the canonical partial-failure retry job, when this plan has both
    /// successful rows and failed hashes.
    ///
    /// An all-failed plan deliberately has no retry job: Go returns the source
    /// job's error instead of publishing a replacement in that case.
    #[must_use]
    pub fn retry_job(&self) -> Option<&QueueJob> {
        self.retry_job.as_ref()
    }

    /// Consume the validated plan into the transaction kernel's two inputs.
    ///
    /// This is a low-level escape hatch that discards retry publication intent.
    /// Persisting callers should use [`persist_writer_plan`] instead.
    #[must_use]
    pub fn into_parts(self) -> (WriteSet, BTreeMap<String, TorrentContentPersistence>) {
        (self.write_set, self.persistence)
    }
}

/// Durable publication boundary for a partial-failure `process_torrent` retry.
///
/// The coordinator supplies a Go-compatible [`QueueJob`]: its missing-hash prefix
/// is exact, while its loaded-failure suffix is one valid Go completion order.
/// Its fingerprint and max-retry value are derived by the shared queue contract.
/// An implementation must return only after the job has either been inserted
/// durably or an active-fingerprint conflict has been accepted as
/// `ON CONFLICT DO NOTHING` success.
pub trait RetryPublisher: Send + Sync {
    fn publish_retry<'a>(&'a self, job: &'a QueueJob) -> BoxFuture<'a, Result<(), BoxError>>;
}

/// Persist one validated writer plan with Go's retry-before-persist ordering.
///
/// Partial failures publish their canonical replacement job before the blocking
/// manager or PostgreSQL writer is invoked. Publication failure therefore leaves
/// the live write-set untouched. Publication is intentionally outside the writer
/// transaction, so a later persistence failure leaves the retry durable, exactly
/// as Go does. An all-failed plan returns [`PersistWriterPlanError::AllFailed`]
/// without publishing or persisting; the source queue job remains responsible for
/// its ordinary retry lifecycle. A deletion-only plan also returns without calling
/// the kernel, preserving Go's second pre-persist early return.
pub async fn persist_writer_plan<B, R>(
    pool: &PgPool,
    plan: &WriterPlan,
    retry_publisher: &R,
    blocking_manager: &B,
) -> Result<(), PersistWriterPlanError>
where
    B: BlockingManager + ?Sized,
    R: RetryPublisher + ?Sized,
{
    let persister = PgWriterPlanPersister {
        pool,
        blocking_manager,
    };
    persist_writer_plan_with(plan, retry_publisher, &persister).await
}

trait WriterPlanPersister: Send + Sync {
    fn persist<'a>(
        &'a self,
        write_set: &'a WriteSet,
        persistence: &'a BTreeMap<String, TorrentContentPersistence>,
    ) -> BoxFuture<'a, Result<(), PersistError>>;
}

struct PgWriterPlanPersister<'a, B: ?Sized> {
    pool: &'a PgPool,
    blocking_manager: &'a B,
}

impl<B> WriterPlanPersister for PgWriterPlanPersister<'_, B>
where
    B: BlockingManager + ?Sized,
{
    fn persist<'a>(
        &'a self,
        write_set: &'a WriteSet,
        persistence: &'a BTreeMap<String, TorrentContentPersistence>,
    ) -> BoxFuture<'a, Result<(), PersistError>> {
        Box::pin(persist_write_set(
            self.pool,
            write_set,
            persistence,
            self.blocking_manager,
        ))
    }
}

async fn persist_writer_plan_with<R, P>(
    plan: &WriterPlan,
    retry_publisher: &R,
    persister: &P,
) -> Result<(), PersistWriterPlanError>
where
    R: RetryPublisher + ?Sized,
    P: WriterPlanPersister + ?Sized,
{
    if plan.write_set.torrent_contents.is_empty() {
        return if plan.retry_info_hashes().is_empty() {
            // Go's processor has a second early return before `persist`: a
            // deletion-only batch is acknowledged without blocking or deleting.
            Ok(())
        } else {
            Err(PersistWriterPlanError::AllFailed {
                failed: plan.retry_info_hashes().len(),
            })
        };
    }

    if !plan.retry_info_hashes().is_empty() {
        let retry_job = plan
            .retry_job()
            .ok_or(PersistWriterPlanError::RetryJobMissing)?;
        retry_publisher
            .publish_retry(retry_job)
            .await
            .map_err(PersistWriterPlanError::RetryPublish)?;
    }

    persister
        .persist(&plan.write_set, &plan.persistence)
        .await?;
    Ok(())
}

/// Fail-closed errors from retry-before-persist coordination.
#[derive(Debug, thiserror::Error)]
pub enum PersistWriterPlanError {
    #[error("all {failed} classifications failed; the source job must own retry handling")]
    AllFailed { failed: usize },
    #[error("partial-failure writer plan is missing its canonical retry job")]
    RetryJobMissing,
    #[error("publishing the partial-failure retry job failed")]
    RetryPublish(#[source] BoxError),
    #[error(transparent)]
    Persist(#[from] PersistError),
}

/// Read one stable source image and compose its non-persisting writer intent.
///
/// The database work is limited to [`load_writer_torrents`]' read-only,
/// repeatable-read transaction. The returned plan is not persisted.
pub async fn load_writer_plan(
    pool: &PgPool,
    materializer: &Materializer,
    params: &ProcessTorrentParams,
) -> Result<WriterPlan, WriterPlanError> {
    validate_supported_params(params)?;
    let loaded = load_writer_torrents(pool, params).await?;
    compose_writer_plan(materializer, params, &loaded)
}

/// Compose the classifier write-set and its volatile persistence projection.
///
/// Hydrated classifier inputs are borrowed rather than cloned. This matters
/// because the loader permits bounded but potentially large decompressed file
/// lists. Attached-content rows fail closed in the projection until the
/// structured content TSV contract is implemented. Direct callers must supply
/// values already admitted by the bounded loader; this pure composer does not
/// reconstruct compressed-size or source-scan evidence from in-memory values.
pub fn compose_writer_plan(
    materializer: &Materializer,
    params: &ProcessTorrentParams,
    loaded: &[WriterLoadedTorrent],
) -> Result<WriterPlan, WriterPlanError> {
    validate_supported_params(params)?;
    let indexed = index_loaded_torrents(loaded)?;
    reject_unrequested_loaded_torrents(params, &indexed)?;
    let write_set =
        materializer.materialize_borrowed(params, loaded.iter().map(|torrent| &torrent.loaded))?;
    let persistence = project_persistence(&write_set, &indexed)?;
    validate_persistence_input(&write_set, &persistence)?;
    let retry_info_hashes = compose_retry_info_hashes(params, &indexed, &write_set);
    let retry_job = compose_retry_job(
        params,
        &retry_info_hashes,
        !write_set.torrent_contents.is_empty(),
    )?;
    Ok(WriterPlan {
        write_set,
        persistence,
        retry_info_hashes,
        retry_job,
    })
}

fn compose_retry_info_hashes(
    params: &ProcessTorrentParams,
    indexed: &BTreeMap<&str, &WriterLoadedTorrent>,
    write_set: &WriteSet,
) -> Vec<String> {
    let failed = write_set
        .failed_info_hashes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut retry_info_hashes = Vec::new();

    // Go seeds failedHashes from the search layer's missing result before any
    // classifiers finish. Preserve original request order and multiplicity for
    // that prefix even though WriteSet keeps a canonical comparison set.
    for requested in &params.info_hashes {
        let info_hash = requested.to_hex();
        if !indexed.contains_key(info_hash.as_str()) {
            retry_info_hashes.push(info_hash);
        }
    }

    // Rust classifies loaded torrents in first-request order. Go appends these
    // from concurrent goroutines, so its concrete completion order is not
    // deterministic; this deterministic suffix preserves Rust processing order.
    let mut processed = BTreeSet::new();
    for requested in &params.info_hashes {
        let info_hash = requested.to_hex();
        if processed.insert(info_hash.clone())
            && indexed.contains_key(info_hash.as_str())
            && failed.contains(info_hash.as_str())
        {
            retry_info_hashes.push(info_hash);
        }
    }

    retry_info_hashes
}

fn compose_retry_job(
    params: &ProcessTorrentParams,
    retry_info_hashes: &[String],
    has_successful_rows: bool,
) -> Result<Option<QueueJob>, WriterPlanError> {
    if retry_info_hashes.is_empty() || !has_successful_rows {
        return Ok(None);
    }

    let info_hashes = retry_info_hashes
        .iter()
        .map(|info_hash| {
            ProtocolId::from_hex(info_hash).map_err(|source| {
                WriterPlanError::InvalidRetryInfoHash {
                    info_hash: info_hash.clone(),
                    source,
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut retry_params = params.clone();
    retry_params.info_hashes = info_hashes;
    Ok(Some(process_torrent_job(
        &retry_params,
        QueueJobOptions::default(),
    )?))
}

fn reject_unrequested_loaded_torrents(
    params: &ProcessTorrentParams,
    indexed: &BTreeMap<&str, &WriterLoadedTorrent>,
) -> Result<(), WriterPlanError> {
    let requested = params
        .info_hashes
        .iter()
        .map(|info_hash| info_hash.to_hex())
        .collect::<std::collections::BTreeSet<_>>();
    if let Some(info_hash) = indexed
        .keys()
        .find(|info_hash| !requested.contains(**info_hash))
    {
        Err(WriterPlanError::UnrequestedLoadedTorrent {
            info_hash: (*info_hash).to_owned(),
        })
    } else {
        Ok(())
    }
}

fn validate_supported_params(params: &ProcessTorrentParams) -> Result<(), WriterPlanError> {
    if !has_explicit_default_workflow(params) {
        return Err(WriterPlanError::ClassifierWorkflowUnsupported);
    }
    if has_explicit_attach_flags_off(params) {
        Ok(())
    } else {
        Err(WriterPlanError::AttachFlagsNotExplicitlyDisabled)
    }
}

fn index_loaded_torrents(
    loaded: &[WriterLoadedTorrent],
) -> Result<BTreeMap<&str, &WriterLoadedTorrent>, WriterPlanError> {
    let mut indexed = BTreeMap::new();
    for torrent in loaded {
        if torrent.loaded.attach_hint_unsupported {
            return Err(WriterPlanError::AttachHintUnsupported {
                info_hash: torrent.loaded.info_hash.clone(),
            });
        }
        if torrent.loaded.info_hash != torrent.loaded.classifier_input.id {
            return Err(WriterPlanError::LoadedInfoHashMismatch {
                loaded_info_hash: torrent.loaded.info_hash.clone(),
                classifier_info_hash: torrent.loaded.classifier_input.id.clone(),
            });
        }
        if indexed
            .insert(torrent.loaded.info_hash.as_str(), torrent)
            .is_some()
        {
            return Err(WriterPlanError::DuplicateLoadedTorrent {
                info_hash: torrent.loaded.info_hash.clone(),
            });
        }
    }
    Ok(indexed)
}

fn project_persistence(
    write_set: &WriteSet,
    indexed: &BTreeMap<&str, &WriterLoadedTorrent>,
) -> Result<BTreeMap<String, TorrentContentPersistence>, WriterPlanError> {
    let mut persistence = BTreeMap::new();
    for row in &write_set.torrent_contents {
        let source = indexed.get(row.info_hash.as_str()).ok_or_else(|| {
            WriterPlanError::ProjectionSourceMissing {
                info_hash: row.info_hash.clone(),
            }
        })?;
        let projected = project_unattached_persistence(
            row,
            &source.loaded.classifier_input,
            source.torrent_snapshot,
            &source.source_snapshots,
        )
        .map_err(|source| WriterPlanError::Projection {
            info_hash: row.info_hash.clone(),
            source,
        })?;
        if persistence.insert(row.id.clone(), projected).is_some() {
            return Err(WriterPlanError::DuplicatePersistenceId { id: row.id.clone() });
        }
    }
    Ok(persistence)
}

/// Fail-closed errors from writer-plan composition.
#[derive(Debug, thiserror::Error)]
pub enum WriterPlanError {
    #[error(transparent)]
    Load(#[from] WriterLoadError),
    #[error(transparent)]
    Materialize(#[from] MaterializeError),
    #[error(transparent)]
    Persistence(#[from] PersistError),
    #[error("invalid retry info hash '{info_hash}' in a validated writer plan")]
    InvalidRetryInfoHash {
        info_hash: String,
        #[source]
        source: hex::FromHexError,
    },
    #[error(transparent)]
    RetryJob(#[from] bitmagnet_queue::JobError),
    #[error("writer plan requires ClassifierWorkflow to be explicitly 'default'")]
    ClassifierWorkflowUnsupported,
    #[error(
        "writer plan requires local_search_enabled, apis_enabled, and tmdb_enabled to be explicitly false"
    )]
    AttachFlagsNotExplicitlyDisabled,
    #[error("writer plan cannot reproduce the complete Go hint/enrichment path for {info_hash}")]
    AttachHintUnsupported { info_hash: String },
    #[error(
        "loaded torrent key {loaded_info_hash} does not match classifier key {classifier_info_hash}"
    )]
    LoadedInfoHashMismatch {
        loaded_info_hash: String,
        classifier_info_hash: String,
    },
    #[error("writer plan contains duplicate loaded torrent {info_hash}")]
    DuplicateLoadedTorrent { info_hash: String },
    #[error("writer plan contains unrequested loaded torrent {info_hash}")]
    UnrequestedLoadedTorrent { info_hash: String },
    #[error("writer plan has no projection source for materialized torrent {info_hash}")]
    ProjectionSourceMissing { info_hash: String },
    #[error("writer projection failed for {info_hash}")]
    Projection {
        info_hash: String,
        #[source]
        source: WriterProjectionError,
    },
    #[error("writer plan produced duplicate persistence metadata ID {id}")]
    DuplicatePersistenceId { id: String },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt;
    use std::sync::{Arc, Mutex};

    use bitmagnet_classifier::ClassifierInput;
    use bitmagnet_queue::{ProcessTorrentParams, ProtocolId, QueueJob, QueueJobStatus};
    use futures::future::BoxFuture;
    use serde_json::Value;
    use sqlx::postgres::PgPoolOptions;

    use super::{
        compose_retry_info_hashes, compose_retry_job, compose_writer_plan, index_loaded_torrents,
        persist_writer_plan, persist_writer_plan_with, project_persistence,
        validate_supported_params, PersistWriterPlanError, RetryPublisher, WriterPlan,
        WriterPlanError, WriterPlanPersister,
    };
    use crate::{
        BlockingManager, BoxError, LoadedTorrent, PersistError, TorrentContentPersistence,
        TorrentContentWrite, TorrentSnapshot, WriteSet, WriterLoadedTorrent, WriterProjectionError,
    };

    const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const HASH_C: &str = "cccccccccccccccccccccccccccccccccccccccc";

    fn loaded(info_hash: &str) -> WriterLoadedTorrent {
        WriterLoadedTorrent {
            loaded: LoadedTorrent {
                info_hash: info_hash.to_owned(),
                classifier_input: ClassifierInput {
                    id: info_hash.to_owned(),
                    name: "writer plan fixture".to_owned(),
                    size: 1,
                    files_status: "no_info".to_owned(),
                    extension: None,
                    files_count: None,
                    files: Vec::new(),
                    hint: None,
                    contents: Vec::new(),
                },
                existing_content_ids: Vec::new(),
                attach_hint_unsupported: false,
            },
            torrent_snapshot: TorrentSnapshot {
                created_at_micros: 1,
            },
            source_snapshots: Vec::new(),
        }
    }

    fn row(info_hash: &str) -> TorrentContentWrite {
        TorrentContentWrite {
            id: format!("{info_hash}:movie:?:?"),
            info_hash: info_hash.to_owned(),
            content_type: Some("movie".to_owned()),
            content_source: None,
            content_id: None,
            languages: Vec::new(),
            episodes: String::new(),
            video_resolution: None,
            video_source: None,
            video_codec: None,
            video_3d: None,
            video_modifier: None,
            release_group: None,
            size: 1,
            files_count: None,
        }
    }

    fn supported_params() -> ProcessTorrentParams {
        ProcessTorrentParams {
            classifier_workflow: "default".to_owned(),
            classifier_flags: Some(BTreeMap::from([
                ("apis_enabled".to_owned(), Value::Bool(false)),
                ("local_search_enabled".to_owned(), Value::Bool(false)),
                ("tmdb_enabled".to_owned(), Value::Bool(false)),
            ])),
            ..ProcessTorrentParams::default()
        }
    }

    fn plan(params: &ProcessTorrentParams, write_set: WriteSet) -> WriterPlan {
        let persistence = write_set
            .torrent_contents
            .iter()
            .map(|row| {
                (
                    row.id.clone(),
                    TorrentContentPersistence {
                        seeders: None,
                        leechers: None,
                        published_at_micros: 1,
                        tsv: String::new(),
                    },
                )
            })
            .collect();
        let retry_info_hashes = write_set.failed_info_hashes.clone();
        let retry_job = compose_retry_job(
            params,
            &retry_info_hashes,
            !write_set.torrent_contents.is_empty(),
        )
        .expect("compose retry fixture");
        WriterPlan {
            write_set,
            persistence,
            retry_info_hashes,
            retry_job,
        }
    }

    fn partial_failure_plan() -> WriterPlan {
        let mut params = supported_params();
        params.classify_mode = 1;
        params.info_hashes = [HASH_A, HASH_B, HASH_C]
            .into_iter()
            .map(|hash| ProtocolId::from_hex(hash).expect("fixture hash"))
            .collect();
        plan(
            &params,
            WriteSet {
                torrent_contents: vec![row(HASH_A)],
                failed_info_hashes: vec![HASH_B.to_owned(), HASH_C.to_owned()],
                ..WriteSet::default()
            },
        )
    }

    #[derive(Debug)]
    struct RetryFixtureError;

    impl fmt::Display for RetryFixtureError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("retry fixture failure")
        }
    }

    impl std::error::Error for RetryFixtureError {}

    struct FakeRetryPublisher {
        events: Arc<Mutex<Vec<&'static str>>>,
        jobs: Mutex<Vec<QueueJob>>,
        fail: bool,
    }

    impl FakeRetryPublisher {
        fn new(events: Arc<Mutex<Vec<&'static str>>>, fail: bool) -> Self {
            Self {
                events,
                jobs: Mutex::new(Vec::new()),
                fail,
            }
        }
    }

    impl RetryPublisher for FakeRetryPublisher {
        fn publish_retry<'a>(&'a self, job: &'a QueueJob) -> BoxFuture<'a, Result<(), BoxError>> {
            Box::pin(async move {
                self.events.lock().expect("events mutex").push("publish");
                self.jobs.lock().expect("jobs mutex").push(job.clone());
                if self.fail {
                    Err(Box::new(RetryFixtureError) as BoxError)
                } else {
                    Ok(())
                }
            })
        }
    }

    struct FakePersister {
        events: Arc<Mutex<Vec<&'static str>>>,
        fail: bool,
    }

    impl WriterPlanPersister for FakePersister {
        fn persist<'a>(
            &'a self,
            _write_set: &'a WriteSet,
            _persistence: &'a BTreeMap<String, TorrentContentPersistence>,
        ) -> BoxFuture<'a, Result<(), PersistError>> {
            Box::pin(async move {
                self.events.lock().expect("events mutex").push("persist");
                if self.fail {
                    Err(PersistError::Database(sqlx::Error::PoolClosed))
                } else {
                    Ok(())
                }
            })
        }
    }

    struct PanicBlocker;

    impl BlockingManager for PanicBlocker {
        fn block<'a>(&'a self, _info_hashes: &'a [String]) -> BoxFuture<'a, Result<(), BoxError>> {
            panic!("empty public coordinator fixture must not call the blocker")
        }
    }

    #[tokio::test]
    async fn partial_failure_publishes_canonical_retry_before_persistence() {
        let plan = partial_failure_plan();
        let events = Arc::new(Mutex::new(Vec::new()));
        let publisher = FakeRetryPublisher::new(Arc::clone(&events), false);
        let persister = FakePersister {
            events: Arc::clone(&events),
            fail: false,
        };

        persist_writer_plan_with(&plan, &publisher, &persister)
            .await
            .expect("partial failure publishes then persists");

        assert_eq!(
            events.lock().expect("events mutex").as_slice(),
            ["publish", "persist"]
        );
        let jobs = publisher.jobs.lock().expect("jobs mutex");
        assert_eq!(jobs.len(), 1);
        let job = &jobs[0];
        assert_eq!(job.queue, bitmagnet_queue::PROCESS_TORRENT);
        assert_eq!(job.status, QueueJobStatus::Pending);
        assert_eq!(job.max_retries, 2);
        assert_eq!(job.priority, 0);
        let retry: ProcessTorrentParams =
            serde_json::from_str(&job.payload).expect("decode retry payload");
        assert_eq!(retry.classify_mode, 1);
        assert_eq!(retry.classifier_workflow, "default");
        assert_eq!(retry.classifier_flags, supported_params().classifier_flags);
        assert_eq!(
            retry
                .info_hashes
                .iter()
                .map(|info_hash| info_hash.to_hex())
                .collect::<Vec<_>>(),
            [HASH_B, HASH_C]
        );
    }

    #[test]
    fn retry_job_preserves_distinct_missing_order_duplicates_and_exact_fingerprint() {
        let materializer = crate::Materializer::from_core().expect("compile core classifier");
        let mut params = supported_params();
        params.info_hashes = [HASH_C, HASH_B, HASH_A, HASH_C]
            .into_iter()
            .map(|hash| ProtocolId::from_hex(hash).expect("fixture hash"))
            .collect();

        let plan = compose_writer_plan(&materializer, &params, &[loaded(HASH_A)])
            .expect("one classified row plus ordered distinct and duplicate missing hashes");

        assert_eq!(plan.write_set.failed_info_hashes, [HASH_B, HASH_C]);
        assert_eq!(plan.retry_info_hashes(), [HASH_C, HASH_B, HASH_C]);
        assert_eq!(plan.write_set.torrent_contents.len(), 1);
        let retry_job = plan.retry_job().expect("partial failure retry job");
        assert_eq!(
            retry_job.payload,
            concat!(
                "{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{",
                "\"apis_enabled\":false,\"local_search_enabled\":false,",
                "\"tmdb_enabled\":false},\"InfoHashes\":[",
                "\"cccccccccccccccccccccccccccccccccccccccc\",",
                "\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",",
                "\"cccccccccccccccccccccccccccccccccccccccc\"]}"
            )
        );
        assert_eq!(
            retry_job.fingerprint,
            "b9d935513d1349daf6bcddff79e3c79184ff818e2a564cdb1fb4956aca28dd31"
        );
    }

    #[test]
    fn retry_order_prefixes_duplicate_missing_then_loaded_failures_in_processing_order() {
        let loaded = [loaded(HASH_A), loaded(HASH_B)];
        let indexed = index_loaded_torrents(&loaded).expect("index loaded retry fixtures");
        let mut params = supported_params();
        params.info_hashes = [HASH_C, HASH_B, HASH_A, HASH_C, HASH_B]
            .into_iter()
            .map(|hash| ProtocolId::from_hex(hash).expect("fixture hash"))
            .collect();
        let write_set = WriteSet {
            failed_info_hashes: vec![HASH_A.to_owned(), HASH_B.to_owned(), HASH_C.to_owned()],
            ..WriteSet::default()
        };

        assert_eq!(
            compose_retry_info_hashes(&params, &indexed, &write_set),
            [HASH_C, HASH_C, HASH_B, HASH_A]
        );
    }

    #[tokio::test]
    async fn retry_publish_failure_prevents_persistence() {
        let plan = partial_failure_plan();
        let events = Arc::new(Mutex::new(Vec::new()));
        let publisher = FakeRetryPublisher::new(Arc::clone(&events), true);
        let persister = FakePersister {
            events: Arc::clone(&events),
            fail: false,
        };

        let error = persist_writer_plan_with(&plan, &publisher, &persister)
            .await
            .expect_err("retry publication failure aborts before persistence");

        assert!(matches!(error, PersistWriterPlanError::RetryPublish(_)));
        assert_eq!(events.lock().expect("events mutex").as_slice(), ["publish"]);
    }

    #[tokio::test]
    async fn persistence_failure_does_not_retract_published_retry() {
        let plan = partial_failure_plan();
        let events = Arc::new(Mutex::new(Vec::new()));
        let publisher = FakeRetryPublisher::new(Arc::clone(&events), false);
        let persister = FakePersister {
            events: Arc::clone(&events),
            fail: true,
        };

        let error = persist_writer_plan_with(&plan, &publisher, &persister)
            .await
            .expect_err("late persistence failure is returned");

        assert!(matches!(
            error,
            PersistWriterPlanError::Persist(PersistError::Database(sqlx::Error::PoolClosed))
        ));
        assert_eq!(
            events.lock().expect("events mutex").as_slice(),
            ["publish", "persist"]
        );
        assert_eq!(publisher.jobs.lock().expect("jobs mutex").len(), 1);
    }

    #[tokio::test]
    async fn all_failed_returns_source_job_error_without_publish_or_persist() {
        let params = supported_params();
        let plan = plan(
            &params,
            WriteSet {
                failed_info_hashes: vec![HASH_A.to_owned(), HASH_B.to_owned()],
                ..WriteSet::default()
            },
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let publisher = FakeRetryPublisher::new(Arc::clone(&events), false);
        let persister = FakePersister {
            events: Arc::clone(&events),
            fail: false,
        };

        let error = persist_writer_plan_with(&plan, &publisher, &persister)
            .await
            .expect_err("all-failed source job must own retry handling");

        assert!(matches!(
            error,
            PersistWriterPlanError::AllFailed { failed: 2 }
        ));
        assert!(events.lock().expect("events mutex").is_empty());
        assert!(publisher.jobs.lock().expect("jobs mutex").is_empty());
    }

    #[tokio::test]
    async fn deletion_only_matches_go_second_early_return() {
        let params = supported_params();
        let plan = plan(
            &params,
            WriteSet {
                delete_info_hashes: vec![HASH_A.to_owned()],
                delete_ids: vec![format!("{HASH_A}:movie:?:?")],
                ..WriteSet::default()
            },
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let publisher = FakeRetryPublisher::new(Arc::clone(&events), false);
        let persister = FakePersister {
            events: Arc::clone(&events),
            fail: false,
        };

        persist_writer_plan_with(&plan, &publisher, &persister)
            .await
            .expect("Go acknowledges deletion-only batches before persist");

        assert!(events.lock().expect("events mutex").is_empty());
        assert!(publisher.jobs.lock().expect("jobs mutex").is_empty());
    }

    #[tokio::test]
    async fn public_publish_failure_does_not_touch_blocker_or_database() {
        let params = supported_params();
        let plan = plan(
            &params,
            WriteSet {
                torrent_contents: vec![row(HASH_A)],
                failed_info_hashes: vec![HASH_B.to_owned()],
                delete_info_hashes: vec![HASH_C.to_owned()],
                ..WriteSet::default()
            },
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let publisher = FakeRetryPublisher::new(Arc::clone(&events), true);
        let pool = PgPoolOptions::new()
            .connect_lazy("postgresql://localhost/bitmagnet_writer_plan_closed")
            .expect("parse lazy test pool URL");
        pool.close().await;

        let error = persist_writer_plan(&pool, &plan, &publisher, &PanicBlocker)
            .await
            .expect_err("publish failure must precede blocker and closed pool access");

        assert!(matches!(error, PersistWriterPlanError::RetryPublish(_)));
        assert_eq!(events.lock().expect("events mutex").as_slice(), ["publish"]);
    }

    #[test]
    fn supported_params_require_explicit_default_and_attach_flags_off() {
        validate_supported_params(&supported_params()).expect("explicit supported subset");

        let mut implicit_workflow = supported_params();
        implicit_workflow.classifier_workflow.clear();
        assert!(matches!(
            validate_supported_params(&implicit_workflow),
            Err(WriterPlanError::ClassifierWorkflowUnsupported)
        ));

        let mut inherited_flags = supported_params();
        inherited_flags.classifier_flags = None;
        assert!(matches!(
            validate_supported_params(&inherited_flags),
            Err(WriterPlanError::AttachFlagsNotExplicitlyDisabled)
        ));

        let mut enabled_flag = supported_params();
        enabled_flag
            .classifier_flags
            .as_mut()
            .expect("fixture flags")
            .insert("tmdb_enabled".to_owned(), Value::Bool(true));
        assert!(matches!(
            validate_supported_params(&enabled_flag),
            Err(WriterPlanError::AttachFlagsNotExplicitlyDisabled)
        ));
    }

    #[test]
    fn loaded_index_requires_unique_matching_keys() {
        let valid = loaded(HASH_A);
        assert_eq!(index_loaded_torrents(&[valid]).unwrap().len(), 1);

        let duplicate_a = loaded(HASH_A);
        let duplicate_b = loaded(HASH_A);
        assert!(matches!(
            index_loaded_torrents(&[duplicate_a, duplicate_b]),
            Err(WriterPlanError::DuplicateLoadedTorrent { info_hash })
                if info_hash == HASH_A
        ));

        let mut mismatched = loaded(HASH_A);
        mismatched.loaded.classifier_input.id = HASH_B.to_owned();
        assert!(matches!(
            index_loaded_torrents(&[mismatched]),
            Err(WriterPlanError::LoadedInfoHashMismatch {
                loaded_info_hash,
                classifier_info_hash,
            }) if loaded_info_hash == HASH_A && classifier_info_hash == HASH_B
        ));

        let mut unsupported = loaded(HASH_A);
        unsupported.loaded.attach_hint_unsupported = true;
        assert!(matches!(
            index_loaded_torrents(&[unsupported]),
            Err(WriterPlanError::AttachHintUnsupported { info_hash })
                if info_hash == HASH_A
        ));
    }

    #[test]
    fn composition_rejects_loaded_torrents_outside_the_request() {
        let materializer = crate::Materializer::from_core().expect("compile core classifier");
        let mut params = supported_params();
        params.info_hashes =
            vec![bitmagnet_queue::ProtocolId::from_hex(HASH_A).expect("fixture hash A")];
        let error = compose_writer_plan(&materializer, &params, &[loaded(HASH_A), loaded(HASH_B)])
            .expect_err("unrequested loaded torrent fails closed");
        assert!(matches!(
            error,
            WriterPlanError::UnrequestedLoadedTorrent { info_hash }
                if info_hash == HASH_B
        ));
    }

    #[test]
    fn composition_is_independent_of_loaded_torrent_order() {
        let materializer = crate::Materializer::from_core().expect("compile core classifier");
        let mut params = supported_params();
        params.info_hashes = vec![
            bitmagnet_queue::ProtocolId::from_hex(HASH_B).expect("fixture hash B"),
            bitmagnet_queue::ProtocolId::from_hex(HASH_A).expect("fixture hash A"),
            bitmagnet_queue::ProtocolId::from_hex(HASH_B).expect("duplicate fixture hash B"),
        ];

        let forward =
            compose_writer_plan(&materializer, &params, &[loaded(HASH_A), loaded(HASH_B)])
                .expect("compose forward loaded order");
        let reverse =
            compose_writer_plan(&materializer, &params, &[loaded(HASH_B), loaded(HASH_A)])
                .expect("compose reverse loaded order");

        assert_eq!(forward, reverse);
    }

    #[test]
    fn persistence_projection_requires_an_exact_unique_supported_keyset() {
        let loaded = [loaded(HASH_A)];
        let indexed = index_loaded_torrents(&loaded).expect("index valid source");
        let write_set = WriteSet {
            torrent_contents: vec![row(HASH_A)],
            ..WriteSet::default()
        };
        let projected = project_persistence(&write_set, &indexed).expect("project valid row");
        assert_eq!(projected.len(), 1);
        assert!(projected.contains_key(&write_set.torrent_contents[0].id));

        let missing = WriteSet {
            torrent_contents: vec![row(HASH_B)],
            ..WriteSet::default()
        };
        assert!(matches!(
            project_persistence(&missing, &indexed),
            Err(WriterPlanError::ProjectionSourceMissing { info_hash })
                if info_hash == HASH_B
        ));

        let mut attached_row = row(HASH_A);
        attached_row.content_source = Some("tmdb".to_owned());
        attached_row.content_id = Some("42".to_owned());
        let attached = WriteSet {
            torrent_contents: vec![attached_row],
            ..WriteSet::default()
        };
        assert!(matches!(
            project_persistence(&attached, &indexed),
            Err(WriterPlanError::Projection {
                source: WriterProjectionError::AttachedContentUnsupported,
                ..
            })
        ));

        let duplicated_row = row(HASH_A);
        let duplicate = WriteSet {
            torrent_contents: vec![duplicated_row.clone(), duplicated_row],
            ..WriteSet::default()
        };
        assert!(matches!(
            project_persistence(&duplicate, &indexed),
            Err(WriterPlanError::DuplicatePersistenceId { .. })
        ));
    }
}
