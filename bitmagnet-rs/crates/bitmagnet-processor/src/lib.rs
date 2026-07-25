//! Phase-3 Lane P — processor orchestration + write-shadow.
//!
//! This crate implements the pure boundary at the centre of the processor:
//! consume a `process_torrent` payload plus already-loaded torrents, run Lane
//! C's classifier, and materialize the canonical write-set that Go constructs
//! before opening its persistence transaction. The supported unattached-content
//! path can be persisted in one PostgreSQL transaction, and the shadow path can
//! read a stable live-row comparison image under the frozen restricted role.
//! The durable supported-subset dark shadow runtime is included; attached
//! content enrichment, post-commit Tantivy dual-write, and production-safe
//! row-scoped queue privileges remain later milestones. Contract:
//! `docs/dev/rust-rewrite/phase3-contracts.md` §5.

use std::collections::{BTreeMap, BTreeSet};

use bitmagnet_classifier::{Classifier, ClassifierError, ClassifierInput, FlagValue, Flags};
use bitmagnet_queue::{ProcessTorrentParams, ProtocolId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod compare;
mod load;
mod persist;
mod runtime;
mod shadow;

pub use compare::{
    compare_write_set, CompareError, ComparisonVerdict, DriftField, ShadowComparison,
    TorrentComparison,
};
pub use load::{load_torrents, LoadError};
pub use persist::{
    persist_write_set, BlockingManager, BoxError, PersistError, TorrentContentPersistence,
};
pub use runtime::{ShadowMetrics, ShadowRuntime, ShadowRuntimeError};
pub use shadow::{
    read_live_snapshot, LiveSnapshot, LiveTorrentSnapshot, LiveTorrentState, ShadowReadError,
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
    /// Lane P rejects this input until Rust models the complete hint and
    /// enrichment surface. The origin bit preserves SQL NULL semantics,
    /// including a non-null empty source string.
    pub attach_hint_unsupported: bool,
}

/// The stable, classification-derived projection of a `torrent_contents` row.
///
/// Volatile source snapshots and generated `tsv` values are deliberately not
/// part of the comparison image (contract §5.2(c)).
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
            let raw = futures::executor::block_on(self.classifier.run(
                workflow,
                &flags,
                &torrent.classifier_input,
            ));
            let result: ClassifierResult = serde_json::from_value(raw)?;
            match result.outcome.as_str() {
                "deleted" => write_set.delete_info_hashes.push(info_hash),
                "classified" => {
                    if result.content_attached {
                        return Err(MaterializeError::AttachedContentUnsupported { info_hash });
                    }
                    let tc = torrent_content_write(&torrent, result);
                    for existing_id in torrent.existing_content_ids {
                        if existing_id != tc.id {
                            write_set.delete_ids.push(existing_id);
                        }
                    }
                    write_set.torrent_contents.push(tc);
                }
                "unmatched" | "error" => write_set.failed_info_hashes.push(info_hash),
                other => {
                    return Err(MaterializeError::UnknownOutcome(other.to_string()));
                }
            }
        }

        write_set.canonicalize();
        Ok(write_set)
    }
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClassifierResult {
    content_type: String,
    languages: Vec<String>,
    episodes: String,
    video_resolution: Option<String>,
    video_source: Option<String>,
    video_codec: Option<String>,
    #[serde(rename = "video3d")]
    video_3d: Option<String>,
    video_modifier: Option<String>,
    release_group: Option<String>,
    content_attached: bool,
    outcome: String,
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

fn torrent_content_write(
    torrent: &LoadedTorrent,
    mut result: ClassifierResult,
) -> TorrentContentWrite {
    result.languages.sort();
    result.languages.dedup();

    let content_type = nonempty(result.content_type);
    // The flags-off result cannot attach content, so source/id are invalid in
    // exactly the same way as Go's `newTorrentContent` corpus path.
    let content_source = None;
    let content_id = None;
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
        languages: result.languages,
        episodes: result.episodes,
        video_resolution: result.video_resolution,
        video_source: result.video_source,
        video_codec: result.video_codec,
        video_3d: result.video_3d,
        video_modifier: result.video_modifier,
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

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
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
