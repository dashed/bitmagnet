//! Phase-3 Lane P — processor orchestration + write-shadow.
//!
//! This crate implements the pure boundary at the centre of the processor:
//! consume a `process_torrent` payload plus already-loaded torrents, run Lane
//! C's classifier, and materialize the canonical write-set that Go constructs
//! before opening its persistence transaction. The supported unattached-content
//! path can be persisted in one PostgreSQL transaction, and the shadow path can
//! read stable and volatile-writer live comparison images under the frozen
//! restricted role.
//! The durable supported-subset dark shadow runtime is included; attached
//! content enrichment, post-commit Tantivy dual-write, and production-safe
//! row-scoped queue privileges remain later milestones. Contract:
//! `docs/dev/rust-rewrite/phase3-contracts.md` §5.

use std::collections::{BTreeMap, BTreeSet};

use bitmagnet_classifier::{
    Classification, Classifier, ClassifierError, ClassifierInput, FlagValue, Flags, Outcome,
};
use bitmagnet_queue::{ProcessTorrentParams, ProtocolId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod blocking_manager_adapter;
mod compare;
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
mod writer_compare;
mod writer_load;
mod writer_plan;
mod writer_projection;

pub use compare::{
    compare_write_set, CompareError, ComparisonVerdict, DriftField, ShadowComparison,
    TorrentComparison,
};
pub use load::{load_torrents, LoadError};
pub use persist::{
    persist_write_set, BlockingManager, BoxError, PersistError, TorrentContentPersistence,
};
pub use runtime::{
    CausalShadowComparison, MirrorMetrics, ShadowMetrics, ShadowRuntime, ShadowRuntimeError,
};
pub use shadow::{
    read_live_snapshot, LiveSnapshot, LiveTorrentSnapshot, LiveTorrentState, ShadowReadError,
};
pub use writer_compare::{
    WriterCompareError, WriterComparison, WriterDriftField, WriterRowComparison,
};
pub use writer_load::{load_writer_torrents, WriterLoadError, WriterLoadedTorrent};
pub use writer_plan::{compose_writer_plan, load_writer_plan, WriterPlan, WriterPlanError};
pub use writer_projection::{
    project_unattached_persistence, TorrentSnapshot, TorrentSourceSnapshot, WriterProjectionError,
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
    /// Whether Go would apply an explicit hint or reuse a source-backed
    /// association before classification.
    ///
    /// The shadow runtime REFUSES such a job outright rather than comparing it.
    ///
    /// 🚧 T9 landed the reuse half: the loader now hydrates the associated
    /// `content` rows and the classifier pre-attaches them exactly as
    /// `runner.Run` does, so a torrent excluded only by the source-backed-reuse
    /// clause could in principle now be compared. The flag is deliberately NOT
    /// narrowed yet — admitting those torrents changes which rows the write-set
    /// gate compares, which re-baselines it, so it needs a gate re-run as
    /// evidence rather than an assumption. The explicit-hint clause is still
    /// genuinely unsupported: a hinted content ID reaches
    /// `attach_local_content_by_id`, which needs a live resolver the shadow does
    /// not have.
    pub attach_hint_unsupported: bool,
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
pub struct Materializer {
    classifier: Classifier,
}

impl Materializer {
    /// Compile the embedded core classifier used by the frozen parity corpus.
    pub fn from_core() -> Result<Self, MaterializeError> {
        Ok(Self {
            classifier: Classifier::from_core()?,
        })
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
        let workflow = if params.classifier_workflow.is_empty() {
            "default"
        } else {
            params.classifier_workflow.as_str()
        };
        let flags = classifier_flags(params.classifier_flags.as_ref())?;

        let mut loaded = BTreeMap::new();
        for torrent in torrents {
            validate_info_hash(&torrent.info_hash)?;
            if loaded.insert(torrent.info_hash.clone(), torrent).is_some() {
                return Err(MaterializeError::DuplicateLoadedTorrent);
            }
        }

        let mut write_set = WriteSet::default();
        let mut seen = BTreeSet::new();
        for requested in &params.info_hashes {
            let info_hash = protocol_id_hex(requested)?;
            if !seen.insert(info_hash.clone()) {
                continue;
            }
            let Some(torrent) = loaded.remove(&info_hash) else {
                write_set.failed_info_hashes.push(info_hash);
                continue;
            };

            // `Classifier::run` became async with the B′-0 dependency seam, but
            // `materialize` is called from a synchronous path. The default
            // `NullContentResolver` performs no I/O, so the future completes on
            // its first poll and `block_on` neither parks nor needs a runtime.
            // 🔜 When lane B′-4 wires a real resolver in, this call actually
            // does I/O and `materialize` must itself become async — this
            // `block_on` is the marker for that change, not a permanent bridge.
            // 🔑 `classify`, not `run`: the normalized object reports
            // `contentAttached` as a bare boolean and carries no content row, so
            // going through it would discard exactly what an attached torrent
            // needs. See `Classifier::classify`.
            let (result, outcome) = futures::executor::block_on(self.classifier.classify(
                workflow,
                &flags,
                &torrent.classifier_input,
            ));
            append_classification_write(&mut write_set, torrent, result, outcome, false)?;
        }

        write_set.canonicalize();
        Ok(write_set)
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
        append_classification_write(&mut write_set, &torrent, result, outcome, true)?;
        write_set.canonicalize();
        Ok(write_set)
    }
}

fn append_classification_write(
    write_set: &mut WriteSet,
    torrent: &LoadedTorrent,
    result: Classification,
    outcome: Outcome,
    include_identifiers: bool,
) -> Result<(), MaterializeError> {
    let info_hash = torrent.info_hash.clone();
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
            if let Some(content) = result.content.as_ref() {
                write_set
                    .contents
                    .push(content_write(content, include_identifiers));
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

    Ok(())
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
    #[error("classifier returned an unknown outcome '{0}'")]
    UnknownOutcome(String),
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

fn torrent_content_write(torrent: &LoadedTorrent, result: Classification) -> TorrentContentWrite {
    // 🚨 The `torrent_contents.languages` COLUMN is alpha-2 code order, NOT
    // `Languages.Slice()` (natsort by name). Those are two different render
    // paths for the same set and they genuinely disagree: the Go write-set
    // oracle fixture `language-0001-multi-french-german` pins `["de","fr"]`
    // where Slice() gives `["fr","de"]` (French, German). Slice() is the
    // DISPLAY order and belongs to the classifier's normalized result; the
    // column is sorted here.
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
    use super::{infer_id, validate_info_hash, WriteSet};

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
            delete_ids: vec!["z".into(), "a".into(), "z".into()],
            delete_info_hashes: vec!["b".into(), "a".into(), "b".into()],
            failed_info_hashes: vec!["f".into(), "e".into(), "f".into()],
            ..WriteSet::default()
        };
        write_set.canonicalize();
        assert_eq!(write_set.delete_ids, ["a", "z"]);
        assert_eq!(write_set.delete_info_hashes, ["a", "b"]);
        assert_eq!(write_set.failed_info_hashes, ["e", "f"]);
    }

    #[test]
    fn info_hash_validation_is_strictly_lowercase_hex() {
        assert!(validate_info_hash("0123456789abcdef0123456789abcdef01234567").is_ok());
        assert!(validate_info_hash("0123456789ABCDEF0123456789ABCDEF01234567").is_err());
        assert!(validate_info_hash("short").is_err());
    }
}
