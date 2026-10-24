//! Phase-3 Lane P — processor orchestration + write-shadow.
//!
//! This crate implements the pure boundary at the centre of the processor:
//! consume a `process_torrent` payload plus already-loaded torrents, run Lane
//! C's classifier, and materialize the canonical write-set that Go constructs
//! before opening its persistence transaction. The persistence kernel supports
//! both unattached and complete attached-content images in one PostgreSQL
//! transaction, while the production shadow path remains flags-off and reads
//! stable and volatile-writer comparison images under the frozen restricted
//! role.
//! The durable supported-subset dark shadow runtime is included. Disconnected
//! writer planning can reuse a completely hydrated existing source-backed
//! association, while the production shadow still rejects that input under its
//! frozen ACL. Post-commit Tantivy dual-write and production-safe row-scoped
//! queue privileges remain later milestones. Contract:
//! `docs/dev/rust-rewrite/phase3-contracts.md` §5.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use bitmagnet_classifier::{
    Classification, Classifier, ClassifierError, ClassifierInput, ContentResolver, FlagValue,
    Flags, Outcome,
};
use bitmagnet_queue::{ProcessTorrentParams, ProtocolId};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod blocking_manager_adapter;
mod compare;
mod live;
/// The read-only loader that hydrates a classifier input from the database.
///
/// Public so a parity harness can reuse the REAL hint synthesis
/// (`load::effective_hint`) instead of hand-copying it. A second copy of that
/// logic is exactly the drift this work exists to catch.
pub mod load;
mod persist;
mod runtime;
mod shadow;
mod supported_subset;
mod worker;
mod writer_compare;
mod writer_load;
mod writer_plan;
mod writer_projection;

pub use compare::{
    compare_write_set, CompareError, ComparisonVerdict, DriftField, ShadowComparison,
    TorrentComparison,
};
pub use live::ResolverFailure;
pub use load::{load_torrents, LoadError};
pub use persist::{
    persist_write_set, BlockingManager, BoxError, ContentPersistence, ContentPersistenceKey,
    PersistError, TorrentContentPersistence,
};
pub use runtime::{
    CausalShadowComparison, MirrorMetrics, ShadowMetrics, ShadowRuntime, ShadowRuntimeError,
};
pub use shadow::{
    read_live_snapshot, LiveSnapshot, LiveTorrentSnapshot, LiveTorrentState, ShadowReadError,
};
pub use supported_subset::{check_payload_key_set, PayloadKeySetError};
pub use worker::{
    register_worker_metrics, ProcessTorrentWorker, ProcessTorrentWorkerConfig,
    DEFAULT_ABSTAIN_COOLOFF, DEFAULT_CHECK_INTERVAL, DEFAULT_JOB_TIMEOUT, DEFAULT_MAX_EXCLUDED,
    DEFAULT_SHUTDOWN_GRACE,
};
pub use writer_compare::{
    WriterCompareError, WriterComparison, WriterDriftField, WriterRowComparison,
};
pub use writer_load::{load_writer_torrents, WriterLoadError, WriterLoadedTorrent};
pub use writer_plan::{
    compose_writer_plan, load_writer_plan, persist_writer_plan, validate_supported_params,
    PersistWriterPlanError, PersistWriterPlanReceipt, RetryPublishReceipt, RetryPublisher,
    WriterFailureCause, WriterFailures, WriterPlan, WriterPlanError,
};
pub use writer_projection::{
    project_torrent_persistence, project_unattached_persistence, TorrentSnapshot,
    TorrentSourceSnapshot, WriterProjectionError,
};

/// A torrent after the processor's read/hydration step.
///
/// `classifier_input` is the effective classifier image: its hint has already
/// been resolved from any reusable `torrent_contents` association in the same
/// way the Go processor does before `runner.Run`. `existing_content_ids` is the
/// set used to materialize stale `torrent_contents` deletes.
#[derive(Debug)]
pub struct LoadedTorrent {
    pub info_hash: String,
    pub classifier_input: ClassifierInput,
    pub existing_content_ids: Vec<String>,
    /// Whether the writer can reproduce Go's handling of the explicit hint. A
    /// sourced hint reaches Go's pre-attach and `attach_local_content_by_id`,
    /// which need the live resolver the flags-off lane deliberately does not
    /// have.
    pub hint_admission: load::HintAdmission,
    /// Set when Go's choice among the existing associations depends on row
    /// order, which leaves the job to Go.
    pub content_ambiguity: Option<load::ContentAmbiguity>,
    /// Whether default-mode hint synthesis selected a reusable source-backed
    /// `torrent_contents` association.
    ///
    /// This is separate from [`Self::hint_admission`]: disconnected writer
    /// planning can hydrate and admit the selected existing association, while
    /// the production shadow runtime keeps its independent categorical
    /// `SourceBackedContentPresent` rejection and frozen ACL.
    pub source_backed_content_present: bool,
}

/// The stable, classification-derived projection of a `torrent_contents` row.
///
/// Volatile source snapshots and generated `tsv` values are deliberately not
/// part of this stable comparison image (contract §5.2(c)); the writer
/// comparator checks their separately projected persistence values.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentContentWrite {
    pub id: String,
    pub info_hash: String,
    pub content_type: Option<String>,
    pub content_source: Option<String>,
    pub content_id: Option<String>,
    pub languages: Vec<String>,
    pub episodes: String,
    pub video_resolution: Option<String>,
    pub video_source: Option<String>,
    pub video_codec: Option<String>,
    #[serde(rename = "video3d")]
    pub video_3d: Option<String>,
    pub video_modifier: Option<String>,
    pub release_group: Option<String>,
    pub size: u64,
    pub files_count: Option<u64>,
}

/// The stable projection of an attached `content` upsert.
///
/// The frozen flags-off corpus never attaches content; keeping the type in the
/// write-set makes the persistence boundary explicit for the next milestone.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContentWrite {
    pub content_type: String,
    pub source: String,
    pub id: String,
    pub title: String,
    pub release_year: Option<u16>,
    pub identifiers: BTreeMap<String, String>,
}

/// Canonical in-memory equivalent of Go's `persistPayload`, plus the hashes
/// that must be retried instead of persisted.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteSet {
    pub contents: Vec<ContentWrite>,
    pub torrent_contents: Vec<TorrentContentWrite>,
    pub delete_ids: Vec<String>,
    pub delete_info_hashes: Vec<String>,
    pub add_tags: BTreeMap<String, Vec<String>>,
    pub failed_info_hashes: Vec<String>,
}

impl WriteSet {
    fn canonicalize(&mut self) {
        self.contents.sort();
        self.contents.dedup();
        self.torrent_contents
            .sort_by(|a, b| (&a.info_hash, &a.id).cmp(&(&b.info_hash, &b.id)));
        self.delete_ids.sort();
        self.delete_ids.dedup();
        self.delete_info_hashes.sort();
        self.delete_info_hashes.dedup();
        self.failed_info_hashes.sort();
        self.failed_info_hashes.dedup();
        for tags in self.add_tags.values_mut() {
            tags.sort();
            tags.dedup();
        }
    }
}

/// Pure write-set materializer backed by Lane C's compiled classifier.
///
/// Classification of one batch's torrents is independent per torrent, so it
/// may run on a dedicated thread pool; folding the results into the write-set
/// stays serial and in requested order, so the output (including which error
/// wins) is identical for any parallelism.
pub struct Materializer {
    classifier: Classifier,
    /// `None` classifies serially on the calling thread. A dedicated pool
    /// rather than rayon's global one, so the worker's CPU budget is explicit
    /// and nothing else in the process competes for it.
    classify_pool: Option<rayon::ThreadPool>,
    /// The runtime a live resolver's I/O runs on; `None` for the core
    /// classifier's `NullContentResolver`, which never awaits anything.
    runtime: Option<tokio::runtime::Handle>,
}

// The classify pool shares one compiled classifier across its threads.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Classifier>();
};

/// One distinct requested hash, in first-request order.
enum Slot<'a> {
    Missing(String),
    Loaded(String, &'a LoadedTorrent),
}

/// A [`Slot`] after classification; `loaded` is `None` for a missing torrent.
struct Classified<'a> {
    info_hash: &'a str,
    loaded: Option<(&'a LoadedTorrent, Classification, Outcome)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClassifierFailure {
    pub info_hash: String,
    pub rendered_error: String,
}

#[derive(Debug, PartialEq)]
pub(crate) struct MaterializedWriterOutput {
    pub write_set: WriteSet,
    pub content_persistence: BTreeMap<ContentPersistenceKey, ContentPersistence>,
    pub classifier_failures: Vec<ClassifierFailure>,
    /// Torrents whose Go episodes map is allocated but empty (stored `{}`).
    pub empty_episode_objects: BTreeSet<String>,
}

impl Materializer {
    /// Compile the embedded core classifier used by the frozen parity corpus.
    pub fn from_core() -> Result<Self, MaterializeError> {
        Ok(Self {
            classifier: Classifier::from_core()?,
            classify_pool: None,
            runtime: None,
        })
    }

    /// Compile the embedded core classifier over a live content resolver, for
    /// the explicit flags-on subset.
    ///
    /// Every resolver call goes through [`live`]'s guard: any resolver error
    /// makes the materialization fail with
    /// [`MaterializeError::ResolverFailed`] instead of producing a write-set.
    ///
    /// 🚨 Classification drives the resolver's futures with
    /// `runtime.block_on`, which panics on one of the runtime's own threads.
    /// Compose off them, from `spawn_blocking`, as the worker does; with a
    /// classify pool the futures run on its threads. `runtime` must be a
    /// multi-thread runtime, whose workers drive the I/O.
    pub fn from_core_with_resolver(
        resolver: Arc<dyn ContentResolver>,
        runtime: tokio::runtime::Handle,
    ) -> Result<Self, MaterializeError> {
        Ok(Self {
            classifier: Classifier::from_core_with(Arc::new(live::AbstainingResolver::new(
                resolver,
            )))?,
            classify_pool: None,
            runtime: Some(runtime),
        })
    }

    /// Whether this materializer has a live resolver, and so supports the
    /// explicit flags-on subset.
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.runtime.is_some()
    }

    /// Classify each batch's torrents on `parallelism` dedicated threads.
    ///
    /// One keeps the serial path: no pool, each torrent classified on the
    /// calling thread just before it is folded.
    pub fn with_classify_parallelism(
        mut self,
        parallelism: NonZeroUsize,
    ) -> Result<Self, MaterializeError> {
        self.classify_pool = if parallelism.get() == 1 {
            None
        } else {
            Some(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(parallelism.get())
                    .thread_name(|index| format!("classify-{index}"))
                    .build()?,
            )
        };
        Ok(self)
    }

    /// Run the classifier and construct the same classification-derived rows
    /// and delete sets as Go's processor.
    ///
    /// Missing requested torrents and classifier terminal errors are recorded
    /// in `failed_info_hashes`, matching Go's republish boundary. Duplicate
    /// hashes in a queue payload are materialized once, matching
    /// `TorrentsWithMissingInfoHashes`.
    pub fn materialize(
        &self,
        params: &ProcessTorrentParams,
        torrents: Vec<LoadedTorrent>,
    ) -> Result<WriteSet, MaterializeError> {
        self.materialize_borrowed(params, torrents.iter())
    }

    /// Materialize without cloning the hydrated classifier inputs.
    ///
    /// The disconnected writer-plan composer retains the source snapshots
    /// beside each [`LoadedTorrent`]. Borrowing here lets it project volatile
    /// persistence metadata after classification without copying bounded but
    /// potentially large file lists.
    fn materialize_borrowed<'a>(
        &self,
        params: &ProcessTorrentParams,
        torrents: impl IntoIterator<Item = &'a LoadedTorrent>,
    ) -> Result<WriteSet, MaterializeError> {
        Ok(self
            .materialize_borrowed_with_failures(params, torrents)?
            .write_set)
    }

    fn materialize_borrowed_with_failures<'a>(
        &self,
        params: &ProcessTorrentParams,
        torrents: impl IntoIterator<Item = &'a LoadedTorrent>,
    ) -> Result<MaterializedWriterOutput, MaterializeError> {
        self.materialize_scoped(params, torrents, &Arc::default())
    }

    /// Materialize, recording live resolver failures in `scope`.
    fn materialize_scoped<'a>(
        &self,
        params: &ProcessTorrentParams,
        torrents: impl IntoIterator<Item = &'a LoadedTorrent>,
        scope: &Arc<live::ResolveScope>,
    ) -> Result<MaterializedWriterOutput, MaterializeError> {
        let workflow = resolved_workflow(params);
        let flags = classifier_flags(params.classifier_flags.as_ref())?;

        let mut loaded = BTreeMap::new();
        for torrent in torrents {
            validate_info_hash(&torrent.info_hash)?;
            if loaded.insert(torrent.info_hash.clone(), torrent).is_some() {
                return Err(MaterializeError::DuplicateLoadedTorrent);
            }
        }

        // A malformed requested hash is not returned until the fold reaches
        // it, so an earlier torrent's fold error still wins, as it did when
        // classification and folding were one loop.
        let mut slots = Vec::with_capacity(params.info_hashes.len());
        let mut invalid_request = None;
        let mut seen = BTreeSet::new();
        for requested in &params.info_hashes {
            let info_hash = match protocol_id_hex(requested) {
                Ok(info_hash) => info_hash,
                Err(error) => {
                    invalid_request = Some(error);
                    break;
                }
            };
            if !seen.insert(info_hash.clone()) {
                continue;
            }
            slots.push(match loaded.remove(&info_hash) {
                Some(torrent) => Slot::Loaded(info_hash, torrent),
                None => Slot::Missing(info_hash),
            });
        }

        let classify = |slot| self.classify_slot(workflow, &flags, slot, scope);
        let folded = match &self.classify_pool {
            // Serial: classify each torrent just before folding it, so a fold
            // error stops classification exactly where the one loop did.
            None => fold_classified(slots.iter().map(classify)),
            // `collect` on an indexed parallel iterator keeps slot order.
            Some(pool) => {
                let classified: Vec<_> = pool.install(|| slots.par_iter().map(classify).collect());
                fold_classified(classified)
            }
        };
        // A resolver failure taints the outcomes it produced, so it decides
        // before anything folded from them.
        if let Some(failure) = scope.failure() {
            return Err(MaterializeError::ResolverFailed(failure));
        }
        let mut output = folded?;
        if let Some(error) = invalid_request {
            return Err(error);
        }
        output.write_set.canonicalize();
        Ok(output)
    }

    fn classify_slot<'s>(
        &self,
        workflow: &str,
        flags: &Flags,
        slot: &'s Slot<'s>,
        scope: &Arc<live::ResolveScope>,
    ) -> Classified<'s> {
        match slot {
            Slot::Missing(info_hash) => Classified {
                info_hash,
                loaded: None,
            },
            // `Classifier::run` became async with the B′-0 dependency seam, but
            // `materialize` is called from a synchronous path. The core
            // `NullContentResolver` performs no I/O, so the future completes on
            // its first poll and `block_on` neither parks nor needs a runtime.
            // A live resolver does real I/O, so its future is driven on the
            // runtime that owns that I/O, from this (non-runtime) thread, with
            // this classification's resolver scope set.
            // 🔑 `classify`, not `run`: the normalized object reports
            // `contentAttached` as a bare boolean and carries no content row, so
            // going through it would discard exactly what an attached torrent
            // needs. See `Classifier::classify`.
            Slot::Loaded(info_hash, torrent) => {
                let classification =
                    self.classifier
                        .classify(workflow, flags, &torrent.classifier_input);
                let (result, outcome) = match &self.runtime {
                    None => futures::executor::block_on(classification),
                    Some(runtime) => runtime
                        .block_on(live::ResolveSlot::new(scope, info_hash).scope(classification)),
                };
                Classified {
                    info_hash,
                    loaded: Some((torrent, result, outcome)),
                }
            }
        }
    }

    /// Materialize one classifier result that was produced by replaying an
    /// observation-tape record.
    ///
    /// This is the same-state side of the Go/Rust rerun gate: the caller runs
    /// the Rust classifier over the record's exact embedded input and recorded
    /// dependency session, then hands the structured result and the captured
    /// processor state here. Unlike the live-row comparator, neither side is
    /// compared with a settled database image from a different run.
    pub fn materialize_replayed(
        &self,
        torrent: LoadedTorrent,
        result: Classification,
        outcome: Outcome,
    ) -> Result<WriteSet, MaterializeError> {
        validate_info_hash(&torrent.info_hash)?;

        let mut write_set = WriteSet::default();
        let _ = append_classification_write(&mut write_set, &torrent, result, outcome, true, true)?;
        write_set.canonicalize();
        Ok(write_set)
    }
}

/// Fold classified slots into the write-set in requested order.
///
/// 🚨 Order-sensitive: content-persistence registration keeps the first image
/// of each attached content and rejects a later conflicting one, and the
/// classifier-failure order is the retry job's fingerprint order.
fn fold_classified<'a>(
    classified: impl IntoIterator<Item = Classified<'a>>,
) -> Result<MaterializedWriterOutput, MaterializeError> {
    let mut write_set = WriteSet::default();
    let mut content_persistence = BTreeMap::new();
    let mut classifier_failures = Vec::new();
    let mut empty_episode_objects = BTreeSet::new();
    for Classified { info_hash, loaded } in classified {
        let Some((torrent, result, outcome)) = loaded else {
            write_set.failed_info_hashes.push(info_hash.to_owned());
            continue;
        };
        if result.episodes.is_empty() && !result.episodes.is_nil() {
            empty_episode_objects.insert(info_hash.to_owned());
        }
        let emit_content = if outcome.tag() == "classified" {
            if let Some(content) = result.content.as_ref() {
                register_content_persistence(&mut content_persistence, content)?
            } else {
                true
            }
        } else {
            true
        };
        if let Some(rendered_error) = append_classification_write(
            &mut write_set,
            torrent,
            result,
            outcome,
            false,
            emit_content,
        )? {
            classifier_failures.push(ClassifierFailure {
                info_hash: info_hash.to_owned(),
                rendered_error,
            });
        }
    }

    Ok(MaterializedWriterOutput {
        write_set,
        content_persistence,
        classifier_failures,
        empty_episode_objects,
    })
}

fn append_classification_write(
    write_set: &mut WriteSet,
    torrent: &LoadedTorrent,
    result: Classification,
    outcome: Outcome,
    include_identifiers: bool,
    emit_content: bool,
) -> Result<Option<String>, MaterializeError> {
    let info_hash = torrent.info_hash.clone();
    let rendered_error = match &outcome {
        Outcome::Unmatched(error) | Outcome::Error(error) => Some(error.clone()),
        Outcome::Classified | Outcome::Deleted(_) => None,
    };
    match outcome.tag() {
        "deleted" => write_set.delete_info_hashes.push(info_hash),
        "classified" => {
            // An attached content row belongs in the expected image:
            // `compare` reconstructs the end STATE, attaching each ContentWrite
            // to the torrents whose torrent_contents row references it.
            // Emitting nothing would leave the expected image without a row the
            // live one has.
            //
            // 🚨 This is deliberately NOT conditioned on whether Go would have
            // UPSERT-ed the row. Go upserts attached content only when
            // `Content.CreatedAt.IsZero()` (persist.go), so a reused row is not
            // rewritten -- but the comparison is end-state, not a literal
            // statement log, and the row is present either way.
            if emit_content {
                if let Some(content) = result.content.as_ref() {
                    write_set
                        .contents
                        .push(content_write(content, include_identifiers));
                }
            }
            if !result.tags.is_empty() {
                write_set
                    .add_tags
                    .insert(info_hash.clone(), result.tags.iter().cloned().collect());
            }
            let tc = torrent_content_write(torrent, result);
            for existing_id in &torrent.existing_content_ids {
                if existing_id != &tc.id {
                    write_set.delete_ids.push(existing_id.clone());
                }
            }
            write_set.torrent_contents.push(tc);
        }
        "unmatched" | "error" => write_set.failed_info_hashes.push(info_hash),
        other => return Err(MaterializeError::UnknownOutcome(other.to_string())),
    }

    Ok(rendered_error)
}

#[derive(Debug, thiserror::Error)]
pub enum MaterializeError {
    #[error(transparent)]
    Classifier(#[from] ClassifierError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("invalid info hash '{0}'; expected 40 lowercase hexadecimal characters")]
    InvalidInfoHash(String),
    #[error("loaded torrents contain a duplicate info hash")]
    DuplicateLoadedTorrent,
    #[error("classifier flag '{name}' has an unsupported runtime value: {value}")]
    UnsupportedFlag { name: String, value: Value },
    #[error("attached content is not exposed by the Lane C normalized result for {info_hash}")]
    AttachedContentUnsupported { info_hash: String },
    #[error("conflicting attached content images for {content_type}:{content_source}:{id}")]
    ConflictingAttachedContent {
        content_type: String,
        content_source: String,
        id: String,
    },
    #[error("classifier returned an unknown outcome '{0}'")]
    UnknownOutcome(String),
    #[error("building the classify thread pool: {0}")]
    ClassifyPool(#[from] rayon::ThreadPoolBuildError),
    #[error("content resolver failed, so the job is abstained before any side effect: {0}")]
    ResolverFailed(ResolverFailure),
}

/// Go's `classifier.workflow` default, which `processor.Process` uses for a
/// job that names no workflow.
pub(crate) const DEFAULT_CLASSIFIER_WORKFLOW: &str = "default";

/// The workflow a job runs: Go's `workflowName` in `processor.Process`.
///
/// Rust's default is fixed at Go's default config value; a Go deployment that
/// configures another `classifier.workflow` would resolve differently.
pub(crate) fn resolved_workflow(params: &ProcessTorrentParams) -> &str {
    if params.classifier_workflow.is_empty() {
        DEFAULT_CLASSIFIER_WORKFLOW
    } else {
        params.classifier_workflow.as_str()
    }
}

fn classifier_flags(raw: Option<&BTreeMap<String, Value>>) -> Result<Flags, MaterializeError> {
    let mut flags = Flags::new();
    for (name, value) in raw.into_iter().flatten() {
        if let Some(value) = value.as_bool() {
            flags.insert(name.clone(), FlagValue::Bool(value));
        } else {
            return Err(MaterializeError::UnsupportedFlag {
                name: name.clone(),
                value: value.clone(),
            });
        }
    }
    Ok(flags)
}

/// The `content` row an attached result implies.
///
/// The live-shadow path deliberately leaves `identifiers` empty because its
/// frozen role cannot read `content_attributes`. The same-input tape rerun has
/// the exact recorded content object and opts in so it can compare Go's full
/// classification-derived image without changing that production ACL contract.
fn content_write(content: &bitmagnet_model::Content, include_identifiers: bool) -> ContentWrite {
    let identifiers = if include_identifiers {
        content
            .attributes
            .iter()
            .filter(|attribute| attribute.key == "id")
            .map(|attribute| (attribute.source.clone(), attribute.value.clone()))
            .collect()
    } else {
        BTreeMap::new()
    };
    ContentWrite {
        content_type: content.content_type.as_str().to_owned(),
        source: content.source.clone(),
        id: content.id.clone(),
        title: content.title.clone(),
        release_year: content
            .release_year
            .and_then(|year| u16::try_from(year).ok()),
        identifiers,
    }
}

fn register_content_persistence(
    persistence: &mut BTreeMap<ContentPersistenceKey, ContentPersistence>,
    content: &bitmagnet_model::Content,
) -> Result<bool, MaterializeError> {
    let projected = ContentPersistence::from_content(content);
    let key = projected.key();
    match persistence.entry(key.clone()) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(projected);
            Ok(true)
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &projected => {
            Ok(false)
        }
        std::collections::btree_map::Entry::Occupied(_) => {
            Err(MaterializeError::ConflictingAttachedContent {
                content_type: key.content_type,
                content_source: key.source,
                id: key.id,
            })
        }
    }
}

fn torrent_content_write(torrent: &LoadedTorrent, result: Classification) -> TorrentContentWrite {
    // The canonical comparison image keeps languages as a sorted code set
    // (the Go write-set oracle fixture `language-0001-multi-french-german`
    // pins `["de","fr"]`). The persisted `torrent_contents.languages` column
    // is NOT this order: GORM serializes `Languages.Slice()` (natsort by
    // name, `["fr","de"]`), which the persistence kernel re-derives. The
    // live Go-vs-Rust worker differential caught the two being conflated.
    let mut languages = result.languages;
    languages.sort();
    languages.dedup();

    let content_type = result
        .content_type
        .map(|content_type| content_type.as_str().to_owned());
    // Go's `newTorrentContent` carries the attached content's ref onto the
    // torrent_contents row; with nothing attached both stay NULL.
    let (content_source, content_id) = result.content.as_ref().map_or((None, None), |content| {
        (Some(content.source.clone()), Some(content.id.clone()))
    });
    let id = infer_id(
        &torrent.info_hash,
        content_type.as_deref(),
        content_source.as_deref(),
        content_id.as_deref(),
    );
    let files_count = torrent
        .classifier_input
        .files_count
        .map(u64::from)
        .or_else(|| (torrent.classifier_input.files_status == "single").then_some(1));

    TorrentContentWrite {
        id,
        info_hash: torrent.info_hash.clone(),
        content_type,
        content_source,
        content_id,
        languages,
        episodes: result.episodes.to_string(),
        video_resolution: result.video_resolution.map(|v| v.as_str().to_owned()),
        video_source: result.video_source.map(|v| v.as_str().to_owned()),
        video_codec: result.video_codec.map(|v| v.as_str().to_owned()),
        video_3d: result.video_3d.map(|v| v.as_str().to_owned()),
        video_modifier: result.video_modifier.map(|v| v.as_str().to_owned()),
        release_group: result.release_group,
        size: torrent.classifier_input.size,
        files_count,
    }
}

fn infer_id(
    info_hash: &str,
    content_type: Option<&str>,
    content_source: Option<&str>,
    content_id: Option<&str>,
) -> String {
    format!(
        "{}:{}:{}:{}",
        info_hash,
        content_type.unwrap_or("?"),
        content_source.unwrap_or("?"),
        content_id.unwrap_or("?")
    )
}

fn protocol_id_hex(id: &ProtocolId) -> Result<String, MaterializeError> {
    let value = serde_json::to_value(id)?;
    let value = value
        .as_str()
        .ok_or_else(|| MaterializeError::InvalidInfoHash(value.to_string()))?;
    validate_info_hash(value)?;
    Ok(value.to_string())
}

fn validate_info_hash(value: &str) -> Result<(), MaterializeError> {
    if value.len() == 40
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(())
    } else {
        Err(MaterializeError::InvalidInfoHash(value.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;

    use bitmagnet_classifier::{Classification, ClassifierInput, InputContent, InputHint, Outcome};
    use bitmagnet_model::{Content, ContentType};
    use bitmagnet_queue::{ProcessTorrentParams, ProtocolId};
    use serde_json::Value;

    use super::{
        append_classification_write, infer_id, register_content_persistence, validate_info_hash,
        ContentWrite, LoadedTorrent, MaterializeError, Materializer, WriteSet,
    };

    fn loaded_torrent() -> LoadedTorrent {
        let info_hash = "0123456789abcdef0123456789abcdef01234567".to_owned();
        LoadedTorrent {
            info_hash: info_hash.clone(),
            classifier_input: ClassifierInput {
                id: info_hash,
                name: "failure-ledger fixture".to_owned(),
                size: 0,
                files_status: "unknown".to_owned(),
                extension: None,
                files_count: None,
                files: Vec::new(),
                hint: None,
                contents: Vec::new(),
            },
            existing_content_ids: Vec::new(),
            hint_admission: crate::load::HintAdmission::Supported,
            content_ambiguity: None,
            source_backed_content_present: false,
        }
    }

    fn named_torrent(info_hash: &str, name: &str) -> LoadedTorrent {
        let mut torrent = loaded_torrent();
        info_hash.clone_into(&mut torrent.info_hash);
        info_hash.clone_into(&mut torrent.classifier_input.id);
        name.clone_into(&mut torrent.classifier_input.name);
        torrent
    }

    fn movie(title: &str) -> Content {
        Content {
            content_type: ContentType::Movie,
            source: "tmdb".into(),
            id: "42".into(),
            title: title.into(),
            release_date: None,
            release_year: Some(1999),
            adult: None,
            original_language: None,
            original_title: None,
            overview: None,
            runtime: None,
            popularity: None,
            vote_average: None,
            vote_count: None,
            created_at: Some(1),
            updated_at: Some(2),
            tsv: Default::default(),
            collections: Vec::new(),
            attributes: Vec::new(),
        }
    }

    /// A torrent whose hint pre-attaches the hydrated `tmdb:42` movie row.
    fn pre_attached(info_hash: &str, title: &str) -> LoadedTorrent {
        let mut torrent = named_torrent(info_hash, "Reusable 1999 1080p");
        torrent.classifier_input.hint = Some(InputHint {
            content_type: "movie".into(),
            content_source: "tmdb".into(),
            content_id: "42".into(),
            ..InputHint::default()
        });
        torrent.classifier_input.contents = vec![InputContent {
            content_type: "movie".into(),
            content_source: "tmdb".into(),
            content_id: "42".into(),
            content: Some(movie(title)),
        }];
        torrent
    }

    fn params(info_hashes: &[&str]) -> ProcessTorrentParams {
        ProcessTorrentParams {
            classifier_workflow: "default".into(),
            classifier_flags: Some(
                ["apis_enabled", "local_search_enabled", "tmdb_enabled"]
                    .into_iter()
                    .map(|flag| (flag.to_owned(), Value::Bool(false)))
                    .collect(),
            ),
            info_hashes: info_hashes
                .iter()
                .map(|hash| ProtocolId::from_hex(hash).expect("fixture hash"))
                .collect(),
            ..ProcessTorrentParams::default()
        }
    }

    fn materializers() -> (Materializer, Materializer) {
        let serial = Materializer::from_core().expect("compile core classifier");
        let parallel = Materializer::from_core()
            .expect("compile core classifier")
            .with_classify_parallelism(NonZeroUsize::new(4).expect("nonzero"))
            .expect("build classify pool");
        assert!(serial.classify_pool.is_none() && parallel.classify_pool.is_some());
        (serial, parallel)
    }

    #[test]
    fn parallel_classification_materializes_exactly_the_serial_output() {
        // The whole Go write-set corpus: classified rows, stale and preserved
        // existing ids, and banned-keyword deletions.
        let mut torrents: Vec<LoadedTorrent> =
            include_str!("../tests/fixtures/write_set.golden.jsonl")
                .lines()
                .map(|line| {
                    let fixture: Value = serde_json::from_str(line).expect("decode fixture");
                    let input = &fixture["input"];
                    let info_hash = input["infoHash"].as_str().expect("fixture hash").to_owned();
                    let mut classifier_input: ClassifierInput =
                        serde_json::from_value(input["classifier"].clone())
                            .expect("classifier input");
                    classifier_input.id.clone_from(&info_hash);
                    LoadedTorrent {
                        info_hash,
                        classifier_input,
                        existing_content_ids: serde_json::from_value(
                            input["existingContentIds"].clone(),
                        )
                        .expect("existing ids"),
                        hint_admission: crate::load::HintAdmission::Supported,
                        content_ambiguity: None,
                        source_backed_content_present: false,
                    }
                })
                .collect();
        assert_eq!(torrents.len(), 330);
        // Go's allocated-but-empty episodes map (backwards range).
        let mut empty_range = named_torrent(
            "2222222222222222222222222222222222222222",
            "79. The Late Late Show With James Corden S01E79 - 9-21-15 Monday.flv",
        );
        empty_range.classifier_input.size = 399_111_392;
        "single".clone_into(&mut empty_range.classifier_input.files_status);
        empty_range.classifier_input.extension = Some("flv".into());
        torrents.push(empty_range);
        // The same attached content twice: registered once.
        torrents.push(pre_attached(
            "3333333333333333333333333333333333333333",
            "Reusable",
        ));
        torrents.push(pre_attached(
            "4444444444444444444444444444444444444444",
            "Reusable",
        ));

        const MISSING: &str = "5555555555555555555555555555555555555555";
        let hashes: Vec<String> = torrents.iter().map(|t| t.info_hash.clone()).collect();
        let mut requested: Vec<&str> = Vec::new();
        for (index, hash) in hashes.iter().rev().enumerate() {
            requested.push(hash);
            if index % 50 == 0 {
                requested.push(MISSING);
                requested.push(&hashes[index]);
            }
        }
        let mut params = params(&requested);

        let (serial, parallel) = materializers();
        let expected = serial
            .materialize_borrowed_with_failures(&params, &torrents)
            .expect("serial materialization");
        let write_set = &expected.write_set;
        assert!(!write_set.torrent_contents.is_empty());
        assert!(!write_set.delete_ids.is_empty());
        assert_eq!(write_set.delete_info_hashes.len(), 20);
        assert_eq!(write_set.contents.len(), 1);
        assert_eq!(expected.content_persistence.len(), 1);
        assert_eq!(write_set.failed_info_hashes, [MISSING]);
        assert_eq!(
            expected.empty_episode_objects.iter().collect::<Vec<_>>(),
            ["2222222222222222222222222222222222222222"]
        );
        assert_eq!(
            parallel
                .materialize_borrowed_with_failures(&params, &torrents)
                .expect("parallel materialization"),
            expected
        );

        // The default workflow never reports a per-torrent failure with the
        // attach flags off; an unknown workflow fails every torrent, so the
        // classifier-failure list (the retry job's fingerprint order) must
        // come out in first-request order.
        params.classifier_workflow = "no_such_workflow".into();
        let expected = serial
            .materialize_borrowed_with_failures(&params, &torrents)
            .expect("serial materialization");
        let mut first_requested: Vec<&str> = Vec::new();
        for hash in &requested {
            if *hash != MISSING && !first_requested.contains(hash) {
                first_requested.push(hash);
            }
        }
        assert_eq!(
            expected
                .classifier_failures
                .iter()
                .map(|failure| failure.info_hash.as_str())
                .collect::<Vec<_>>(),
            first_requested
        );
        assert_eq!(
            parallel
                .materialize_borrowed_with_failures(&params, &torrents)
                .expect("parallel materialization"),
            expected
        );
    }

    #[test]
    fn parallel_classification_reports_the_serial_fold_error() {
        const FIRST: &str = "6666666666666666666666666666666666666666";
        const CONFLICTING: &str = "7777777777777777777777777777777777777777";
        let torrents = [
            pre_attached(FIRST, "Reusable"),
            pre_attached(CONFLICTING, "Different"),
        ];
        let params = params(&[FIRST, CONFLICTING]);

        let (serial, parallel) = materializers();
        let errors = [&serial, &parallel].map(|materializer| {
            match materializer.materialize_borrowed_with_failures(&params, &torrents) {
                Err(error @ MaterializeError::ConflictingAttachedContent { .. }) => {
                    error.to_string()
                }
                other => panic!("expected conflicting attached content, got {other:?}"),
            }
        });
        assert_eq!(errors[0], errors[1]);
    }

    #[test]
    fn infer_id_matches_go_invalid_content_markers() {
        assert_eq!(
            infer_id("0123456789012345678901234567890123456789", None, None, None),
            "0123456789012345678901234567890123456789:?:?:?"
        );
    }

    #[test]
    fn canonicalize_sorts_and_deduplicates_delete_sets() {
        let mut write_set = WriteSet {
            contents: vec![
                ContentWrite {
                    content_type: "movie".into(),
                    source: "tmdb".into(),
                    id: "42".into(),
                    title: "Title".into(),
                    release_year: Some(2026),
                    identifiers: Default::default(),
                },
                ContentWrite {
                    content_type: "movie".into(),
                    source: "tmdb".into(),
                    id: "42".into(),
                    title: "Title".into(),
                    release_year: Some(2026),
                    identifiers: Default::default(),
                },
            ],
            delete_ids: vec!["z".into(), "a".into(), "z".into()],
            delete_info_hashes: vec!["b".into(), "a".into(), "b".into()],
            failed_info_hashes: vec!["f".into(), "e".into(), "f".into()],
            ..WriteSet::default()
        };
        write_set.canonicalize();
        assert_eq!(write_set.contents.len(), 1);
        assert_eq!(write_set.delete_ids, ["a", "z"]);
        assert_eq!(write_set.delete_info_hashes, ["a", "b"]);
        assert_eq!(write_set.failed_info_hashes, ["e", "f"]);
    }

    #[test]
    fn attached_content_registration_deduplicates_and_rejects_conflicts() {
        let content = Content {
            content_type: ContentType::Movie,
            source: "tmdb".into(),
            id: "42".into(),
            title: "Title".into(),
            release_date: None,
            release_year: Some(2026),
            adult: None,
            original_language: None,
            original_title: None,
            overview: None,
            runtime: None,
            popularity: None,
            vote_average: None,
            vote_count: None,
            created_at: None,
            updated_at: None,
            tsv: Default::default(),
            collections: Vec::new(),
            attributes: Vec::new(),
        };
        let mut persistence = BTreeMap::new();
        assert!(register_content_persistence(&mut persistence, &content).unwrap());
        assert!(!register_content_persistence(&mut persistence, &content).unwrap());

        let mut conflicting = content;
        conflicting.title = "Different".into();
        assert!(matches!(
            register_content_persistence(&mut persistence, &conflicting),
            Err(MaterializeError::ConflictingAttachedContent { .. })
        ));
    }

    #[test]
    fn info_hash_validation_is_strictly_lowercase_hex() {
        assert!(validate_info_hash("0123456789abcdef0123456789abcdef01234567").is_ok());
        assert!(validate_info_hash("0123456789ABCDEF0123456789ABCDEF01234567").is_err());
        assert!(validate_info_hash("short").is_err());
    }

    #[test]
    fn classifier_failures_preserve_exact_unmatched_and_error_strings() {
        let torrent = loaded_torrent();

        for outcome in [
            Outcome::Unmatched("unmatched: exact fixture".to_owned()),
            Outcome::Error("classify: exact fixture".to_owned()),
        ] {
            let expected = outcome.error_string().expect("failure outcome");
            let mut write_set = WriteSet::default();
            let rendered = append_classification_write(
                &mut write_set,
                &torrent,
                Classification::default(),
                outcome,
                false,
                true,
            )
            .expect("append failure write");

            assert_eq!(rendered.as_deref(), Some(expected.as_str()));
            assert_eq!(
                write_set.failed_info_hashes.as_slice(),
                std::slice::from_ref(&torrent.info_hash)
            );
        }
    }
}
