//! The three queue-job payload types and their constructors, mirroring the Go
//! oracle byte-for-byte for the fingerprint (contract §1.2).
//!
//! Each payload's JSON is the fingerprint input, so field **declaration order**,
//! per-type **key casing**, and Go's `omitempty` semantics must match exactly:
//!
//! - `process_torrent` / `process_torrent_batch` → **PascalCase** field names.
//! - `blob_migration` → **camelCase** field names.
//! - `ClassifierFlags` is a Go `map[string]any`, so keys serialize **sorted**
//!   (a `BTreeMap` reproduces Go's alphabetical map-key sort).
//! - `omitempty` on **struct/array** fields (`InfoHashGreaterThan [20]byte`,
//!   `UpdatedBefore time.Time`) is NOT honored by Go's encoder — the zero value
//!   is ALWAYS emitted. These fields carry no `skip_serializing_if`.
//! - `InfoHashes` slice order is **preserved, not sorted** (fingerprint is
//!   order-sensitive).

use std::collections::BTreeMap;

use chrono::{DateTime, FixedOffset};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::id::ProtocolId;
use crate::job::{new_queue_job, JobError, QueueJob, QueueJobOptions};

/// Go's `time.Time` as it appears in a job payload. Go marshals both zero and
/// non-zero values as RFC3339Nano strings. The original representation is
/// retained so reserialization remains byte-compatible with the Go payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoTime(String);

impl GoTime {
    /// The zero `time.Time`, `"0001-01-01T00:00:00Z"`.
    #[must_use]
    pub fn zero() -> Self {
        Self("0001-01-01T00:00:00Z".to_string())
    }

    pub fn parsed(&self) -> Result<DateTime<FixedOffset>, chrono::ParseError> {
        DateTime::parse_from_rfc3339(&self.0)
    }
}

impl Serialize for GoTime {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for GoTime {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        DateTime::parse_from_rfc3339(&value)
            .map_err(|error| D::Error::custom(format!("invalid Go time {value:?}: {error}")))?;
        Ok(Self(value))
    }
}

fn deserialize_content_types<'de, D>(deserializer: D) -> Result<Vec<Option<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    const CONTENT_TYPES: &[&str] = &[
        "movie",
        "tv_show",
        "music",
        "ebook",
        "comic",
        "audiobook",
        "game",
        "software",
        "xxx",
    ];

    Vec::<Option<String>>::deserialize(deserializer)?
        .into_iter()
        .map(|value| match value {
            None => Ok(None),
            Some(value) => {
                let canonical = value.to_lowercase();
                if CONTENT_TYPES.contains(&canonical.as_str()) {
                    Ok(Some(canonical))
                } else {
                    Err(D::Error::custom(format!("invalid content type {value:?}")))
                }
            }
        })
        .collect()
}

/// `ClassifyMode` enum (`internal/processor/message.go:11-20`): `Default=0`,
/// `Rematch=1`; serialized as its integer value.
pub const CLASSIFY_MODE_DEFAULT: i32 = 0;
pub const CLASSIFY_MODE_REMATCH: i32 = 1;

fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

fn is_false(v: &bool) -> bool {
    !*v
}

/// Go `omitempty` on a `map` omits when `len == 0` (nil or empty alike).
fn flags_omit(v: &Option<BTreeMap<String, Value>>) -> bool {
    v.as_ref().is_none_or(BTreeMap::is_empty)
}

// ---------------------------------------------------------------------------
// process_torrent (`internal/processor/message.go`)
// ---------------------------------------------------------------------------

/// `process_torrent` payload — `internal/processor/MessageParams` (`:22-27`).
/// PascalCase; declaration order `ClassifyMode, ClassifierWorkflow,
/// ClassifierFlags, InfoHashes`. Only `InfoHashes` lacks `omitempty`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ProcessTorrentParams {
    #[serde(rename = "ClassifyMode", skip_serializing_if = "is_zero_i32")]
    pub classify_mode: i32,
    #[serde(
        rename = "ClassifierWorkflow",
        skip_serializing_if = "String::is_empty"
    )]
    pub classifier_workflow: String,
    #[serde(rename = "ClassifierFlags", skip_serializing_if = "flags_omit")]
    pub classifier_flags: Option<BTreeMap<String, Value>>,
    #[serde(rename = "InfoHashes")]
    pub info_hashes: Vec<ProtocolId>,
}

impl Default for ProcessTorrentParams {
    fn default() -> Self {
        Self {
            classify_mode: CLASSIFY_MODE_DEFAULT,
            classifier_workflow: String::new(),
            classifier_flags: None,
            info_hashes: Vec::new(),
        }
    }
}

/// `process_torrent` queue name (`internal/processor/message.go:9`).
pub const PROCESS_TORRENT: &str = "process_torrent";

/// Build a `process_torrent` job. Mirrors `processor.NewQueueJob`: injects
/// `MaxRetries(2)` then applies caller options (`message.go:29-35`).
///
/// # Errors
/// Propagates a JSON marshaling failure from [`new_queue_job`].
pub fn process_torrent_job(
    params: &ProcessTorrentParams,
    opts: QueueJobOptions,
) -> Result<QueueJob, JobError> {
    new_queue_job(PROCESS_TORRENT, params, opts.with_max_retries(2))
}

// ---------------------------------------------------------------------------
// process_torrent_batch (`internal/processor/batch/message.go`)
// ---------------------------------------------------------------------------

/// `process_torrent_batch` payload — `batch.MessageParams` (`:14-24`).
/// PascalCase. `InfoHashGreaterThan` (`[20]byte`) and `UpdatedBefore`
/// (`time.Time`) are struct/array types → ALWAYS emitted despite `omitempty`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ProcessTorrentBatchParams {
    #[serde(rename = "InfoHashGreaterThan")]
    pub info_hash_greater_than: ProtocolId,
    #[serde(rename = "UpdatedBefore")]
    pub updated_before: GoTime,
    #[serde(rename = "ClassifyMode", skip_serializing_if = "is_zero_i32")]
    pub classify_mode: i32,
    #[serde(
        rename = "ClassifierWorkflow",
        skip_serializing_if = "String::is_empty"
    )]
    pub classifier_workflow: String,
    #[serde(rename = "ClassifierFlags", skip_serializing_if = "flags_omit")]
    pub classifier_flags: Option<BTreeMap<String, Value>>,
    #[serde(rename = "ChunkSize", skip_serializing_if = "is_zero_u64")]
    pub chunk_size: u64,
    #[serde(rename = "BatchSize", skip_serializing_if = "is_zero_u64")]
    pub batch_size: u64,
    #[serde(
        rename = "ContentTypes",
        default,
        deserialize_with = "deserialize_content_types",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub content_types: Vec<Option<String>>,
    #[serde(rename = "Orphans", skip_serializing_if = "is_false")]
    pub orphans: bool,
}

impl Default for ProcessTorrentBatchParams {
    fn default() -> Self {
        Self {
            info_hash_greater_than: ProtocolId::zero(),
            updated_before: GoTime::zero(),
            classify_mode: CLASSIFY_MODE_DEFAULT,
            classifier_workflow: String::new(),
            classifier_flags: None,
            chunk_size: 0,
            batch_size: 0,
            content_types: Vec::new(),
            orphans: false,
        }
    }
}

impl ProcessTorrentBatchParams {
    /// Go's `MessageParams.ApisDisabled`: only an explicit boolean false lowers
    /// child-job priority from 10 to 4.
    #[must_use]
    pub fn apis_disabled(&self) -> bool {
        self.classifier_flags
            .as_ref()
            .and_then(|flags| flags.get("apis_enabled"))
            .and_then(Value::as_bool)
            == Some(false)
    }
}

/// `process_torrent_batch` queue name (`batch/message.go:12`).
pub const PROCESS_TORRENT_BATCH: &str = "process_torrent_batch";

/// Build a `process_torrent_batch` job. Mirrors `batch.NewQueueJob`: injects
/// `BatchSize=100`, `ChunkSize=10000` (when zero) **before** marshaling, then
/// `MaxRetries(2)` (`batch/message.go:42-55`).
///
/// # Errors
/// Propagates a JSON marshaling failure from [`new_queue_job`].
pub fn process_torrent_batch_job(
    params: &ProcessTorrentBatchParams,
    opts: QueueJobOptions,
) -> Result<QueueJob, JobError> {
    let mut params = params.clone();
    if params.batch_size == 0 {
        params.batch_size = 100;
    }
    if params.chunk_size == 0 {
        params.chunk_size = 10_000;
    }
    new_queue_job(PROCESS_TORRENT_BATCH, &params, opts.with_max_retries(2))
}

// ---------------------------------------------------------------------------
// blob_migration (`internal/blobmigration/queue/message.go`)
// ---------------------------------------------------------------------------

/// Default chunk size for blob migration (`DefaultChunkSize=2000`,
/// `blobmigration/queue/message.go:28` + `handler.go:39`).
pub const BLOB_MIGRATION_DEFAULT_CHUNK_SIZE: i64 = 2000;

/// `blob_migration` payload — `blobmigration/queue.MessageParams` (`:13-24`).
/// **camelCase**. Only `infoHashLessOrEqual` has `omitempty`; the other four
/// (including `infoHashGreaterThan`, a **hex string** here, not a `ProtocolId`)
/// are always emitted.
#[derive(Debug, Clone, Default, Serialize)]
pub struct BlobMigrationParams {
    #[serde(rename = "infoHashGreaterThan")]
    pub info_hash_greater_than: String,
    #[serde(
        rename = "infoHashLessOrEqual",
        skip_serializing_if = "String::is_empty"
    )]
    pub info_hash_less_or_equal: String,
    #[serde(rename = "rangeId")]
    pub range_id: i64,
    #[serde(rename = "numRanges")]
    pub num_ranges: i64,
    #[serde(rename = "chunkSize")]
    pub chunk_size: i64,
}

/// `blob_migration` queue name (`blobmigration/queue/message.go:5`).
pub const BLOB_MIGRATION: &str = "blob_migration";

/// Build a `blob_migration` job. Mirrors `queue.NewQueueJob`: injects
/// `ChunkSize=2000`, `NumRanges=1` (when zero) before marshaling, then
/// `MaxRetries(2)` (`blobmigration/queue/message.go:26-42`).
///
/// # Errors
/// Propagates a JSON marshaling failure from [`new_queue_job`].
pub fn blob_migration_job(
    params: &BlobMigrationParams,
    opts: QueueJobOptions,
) -> Result<QueueJob, JobError> {
    let mut params = params.clone();
    if params.chunk_size == 0 {
        params.chunk_size = BLOB_MIGRATION_DEFAULT_CHUNK_SIZE;
    }
    if params.num_ranges == 0 {
        params.num_ranges = 1;
    }
    new_queue_job(BLOB_MIGRATION, &params, opts.with_max_retries(2))
}

#[cfg(test)]
mod tests {
    use super::{ProcessTorrentBatchParams, ProcessTorrentParams, CLASSIFY_MODE_DEFAULT};

    #[test]
    fn process_torrent_decode_applies_go_zero_values_for_omitted_fields() {
        let params: ProcessTorrentParams =
            serde_json::from_str(r#"{"InfoHashes":[]}"#).expect("decode minimal Go payload");
        assert_eq!(params.classify_mode, CLASSIFY_MODE_DEFAULT);
        assert!(params.classifier_workflow.is_empty());
        assert!(params.classifier_flags.is_none());
        assert!(params.info_hashes.is_empty());
    }

    #[test]
    fn batch_decode_validates_time_and_canonicalizes_nullable_content_types() {
        let params: ProcessTorrentBatchParams = serde_json::from_str(
            r#"{"InfoHashGreaterThan":"0000000000000000000000000000000000000000","UpdatedBefore":"2026-08-12T04:05:06.123456789Z","ContentTypes":["MOVIE",null,"tv_show"]}"#,
        )
        .expect("decode canonical Go batch payload");
        assert_eq!(
            params.content_types,
            vec![Some("movie".to_string()), None, Some("tv_show".to_string())]
        );
        assert!(serde_json::from_str::<ProcessTorrentBatchParams>(
            r#"{"InfoHashGreaterThan":"0000000000000000000000000000000000000000","UpdatedBefore":"not-a-time"}"#
        )
        .is_err());
        assert!(serde_json::from_str::<ProcessTorrentBatchParams>(
            r#"{"InfoHashGreaterThan":"0000000000000000000000000000000000000000","UpdatedBefore":"0001-01-01T00:00:00Z","ContentTypes":["not-real"]}"#
        )
        .is_err());
    }
}
