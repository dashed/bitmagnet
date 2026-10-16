//! Pure, deterministic planning for one already-resolved DHT torrent batch.
//!
//! This module performs no lookup, route intake, database write, queue insert,
//! scrape send, clock read, metric update, or lifecycle work. It projects an
//! ordered request slice and an already-resolved v2 snapshot into inert write,
//! classifier, and scrape values for later owned stages.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::time::Duration;

use bitmagnet_dht::{DhtInfoHashTriageRequest, Id20};
use bitmagnet_metainfo::{InfoFilesError, ParsedInfo};
use bitmagnet_model::{
    file_extension_from_path, serialize_files, BlobFile, FileType, FilesStatus, InfoHash,
    TorrentFileSummary,
};
use bitmagnet_queue::{
    new_queue_job, process_torrent_job, ProcessTorrentParams, ProtocolId, QueueJob,
    QueueJobOptions, PROCESS_TORRENT_SHADOW,
};

use crate::DhtPersistTorrentRequest;

/// Go's production default retained-file threshold.
pub const DHT_TORRENT_DEFAULT_SAVE_FILES_THRESHOLD: u64 = 100;
/// Go's exact classifier job payload limit.
pub const DHT_TORRENT_CLASSIFIER_BATCH_LIMIT: usize = 100;
/// Go delays classifier jobs by one minute so the first scrape can run.
pub const DHT_TORRENT_CLASSIFIER_DELAY: Duration = Duration::from_secs(60);
/// Source key seeded by the production migration.
pub const DHT_TORRENT_SOURCE: &str = "dht";

/// Classifier queue selected for jobs emitted with persisted DHT torrents.
///
/// `Shadow` isolates classifier consumption from Go's live queue, but it does
/// not make torrent persistence itself read-only: the surrounding transaction
/// still writes every planned torrent, source, file, pieces, and queue row. It
/// also bypasses the existing mirror's admission/depth/cursor policy, and the
/// current shadow consumer rejects Go-default DHT payloads as unsupported.
/// Shadow activation therefore requires a separate ownership, compatibility,
/// and active-fingerprint collision gate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum DhtCrawlerClassifierQueue {
    Shadow,
    #[default]
    Live,
}

impl DhtCrawlerClassifierQueue {
    /// Exact PostgreSQL queue name selected by this target.
    #[must_use]
    pub const fn queue_name(self) -> &'static str {
        match self {
            Self::Shadow => PROCESS_TORRENT_SHADOW,
            Self::Live => bitmagnet_queue::PROCESS_TORRENT,
        }
    }
}

/// An already-resolved full-v2-to-primary snapshot supplied by a later lookup
/// adapter. The planner neither queries nor resolves conflicting database rows.
pub type DhtResolvedExistingV2 = BTreeMap<[u8; 32], Id20>;

/// Pure torrent-projection policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtTorrentPlanConfig {
    pub save_pieces: bool,
    pub save_files_threshold: u64,
}

impl Default for DhtTorrentPlanConfig {
    fn default() -> Self {
        Self {
            save_pieces: false,
            save_files_threshold: DHT_TORRENT_DEFAULT_SAVE_FILES_THRESHOLD,
        }
    }
}

/// Reusable, effect-free planner configured with one projection policy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtTorrentPlanner {
    config: DhtTorrentPlanConfig,
    classifier_queue: DhtCrawlerClassifierQueue,
}

impl DhtTorrentPlanner {
    /// Construct a planner. This performs no lookup or other external effect.
    #[must_use]
    pub const fn new(config: DhtTorrentPlanConfig) -> Self {
        Self::with_classifier_queue(config, DhtCrawlerClassifierQueue::Live)
    }

    /// Construct a planner with an explicit classifier queue target.
    ///
    /// This performs no lookup or other external effect.
    #[must_use]
    pub const fn with_classifier_queue(
        config: DhtTorrentPlanConfig,
        classifier_queue: DhtCrawlerClassifierQueue,
    ) -> Self {
        Self {
            config,
            classifier_queue,
        }
    }

    /// Return the immutable projection policy.
    #[must_use]
    pub const fn config(&self) -> DhtTorrentPlanConfig {
        self.config
    }

    /// Return the immutable classifier queue target.
    #[must_use]
    pub const fn classifier_queue(&self) -> DhtCrawlerClassifierQueue {
        self.classifier_queue
    }

    /// Produce the sorted, unique full-v2 keys carried by identity-valid
    /// requests that a caller must resolve before invoking [`Self::plan`].
    /// Identity-invalid input never causes lookup I/O.
    #[must_use]
    pub fn v2_lookup_keys(&self, requests: &[DhtPersistTorrentRequest]) -> Vec<[u8; 32]> {
        requests
            .iter()
            .filter(|request| request_carries_primary_identity(request))
            .filter_map(|request| request.meta_info.info_hash_v2())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Plan one ordered batch from an already-resolved v2 snapshot.
    #[must_use]
    pub fn plan(
        &self,
        requests: &[DhtPersistTorrentRequest],
        existing_v2: &DhtResolvedExistingV2,
    ) -> DhtTorrentPersistPlan {
        plan_with(
            requests,
            existing_v2,
            self.config,
            &mut |files| serialize_files(files).map_err(|error| error.to_string()),
            &mut |hashes| build_classifier_job(hashes, self.classifier_queue),
        )
    }
}

/// One parent `torrents` row without writer-owned timestamps.
///
/// This crawler-local DTO deliberately preserves Go's nullable
/// `file_extensions`: `None` means no blob was produced, while a successful
/// nonempty blob has `Some(sorted_extensions)`, including `Some(Vec::new())`
/// when no retained file has a recognized extension.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtTorrentWrite {
    pub info_hash: Id20,
    pub info_hash_v1: Option<Id20>,
    pub info_hash_v2: Option<[u8; 32]>,
    pub meta_version: u16,
    pub name: String,
    pub size: u64,
    pub private: bool,
    pub files_status: FilesStatus,
    pub files_count: Option<u32>,
    pub files_data: Option<Vec<u8>>,
    pub file_extensions: Option<Vec<String>>,
}

/// One retained `torrent_files` row. Its extension remains database-generated;
/// blob and summary extensions are independently derived from `path`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtTorrentFileWrite {
    pub info_hash: Id20,
    pub index: u32,
    pub path: String,
    pub size: u64,
}

/// One file-summary write without writer-owned timestamps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtTorrentFileSummaryWrite {
    pub summary: TorrentFileSummary,
    pub compressed_bytes: Option<u64>,
}

/// One DHT source association without writer-owned timestamps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtTorrentSourceLinkWrite {
    pub source: String,
    pub info_hash: Id20,
}

/// One optional pieces row without its writer-owned creation timestamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhtTorrentPiecesWrite {
    pub info_hash: Id20,
    pub piece_length: i64,
    pub pieces: Vec<u8>,
}

/// Stable planner diagnostics for Go-compatible availability fallbacks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DhtTorrentPlanDiagnostic {
    /// Go keeps the relational projection when optional blob encoding fails.
    BlobEncodingFailed {
        input_index: u64,
        info_hash: Id20,
        error: String,
    },
    /// A classifier group remains planned even when its inert job could not be
    /// constructed. Real `ProcessTorrentParams` serialization is infallible.
    QueueConstructionFailed { group_index: u64, error: String },
}

/// A request that could not be safely projected.
#[derive(Debug)]
pub struct DhtTorrentProjectionFailure {
    pub input_index: u64,
    pub info_hash: Id20,
    pub error: DhtTorrentProjectionError,
}

/// Checked failures that Go currently relies on upstream validation or native
/// integer behavior to avoid.
#[derive(Debug, PartialEq, Eq)]
pub enum DhtTorrentProjectionError {
    IdentityMismatch,
    InvalidNameUtf8,
    NameContainsNul,
    InvalidPathUtf8 {
        file_index: u64,
        component_index: u64,
    },
    PathComponentContainsNul {
        file_index: u64,
        component_index: u64,
    },
    PathContainsNul {
        file_index: u64,
    },
    InvalidFiles(InfoFilesError),
    FileCountOutOfRange(usize),
    FileIndexOutOfRange(usize),
    SummaryFileCountOutOfRange(usize),
    SummaryTotalSizeOutOfRange,
    InvalidPieceLength(i64),
}

impl fmt::Display for DhtTorrentProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityMismatch => formatter.write_str(
                "requested primary hash is neither the v1 identity nor truncated v2 identity",
            ),
            Self::InvalidNameUtf8 => formatter.write_str("torrent name is not valid UTF-8"),
            Self::NameContainsNul => {
                formatter.write_str("torrent name contains a PostgreSQL-incompatible NUL")
            }
            Self::InvalidPathUtf8 {
                file_index,
                component_index,
            } => write!(
                formatter,
                "torrent file {file_index} path component {component_index} is not valid UTF-8"
            ),
            Self::PathComponentContainsNul {
                file_index,
                component_index,
            } => write!(
                formatter,
                "torrent file {file_index} path component {component_index} contains a PostgreSQL-incompatible NUL"
            ),
            Self::PathContainsNul { file_index } => write!(
                formatter,
                "torrent file {file_index} joined path contains a PostgreSQL-incompatible NUL"
            ),
            Self::InvalidFiles(error) => write!(formatter, "invalid torrent files: {error}"),
            Self::FileCountOutOfRange(count) => {
                write!(formatter, "torrent file count {count} exceeds PostgreSQL integer range")
            }
            Self::FileIndexOutOfRange(index) => {
                write!(formatter, "torrent file index {index} exceeds PostgreSQL integer range")
            }
            Self::SummaryFileCountOutOfRange(count) => write!(
                formatter,
                "retained file-summary count {count} exceeds PostgreSQL integer range"
            ),
            Self::SummaryTotalSizeOutOfRange => formatter.write_str(
                "retained file-summary size exceeds PostgreSQL bigint range",
            ),
            Self::InvalidPieceLength(length) => {
                write!(formatter, "torrent piece length {length} is negative")
            }
        }
    }
}

impl Error for DhtTorrentProjectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidFiles(error) => Some(error),
            _ => None,
        }
    }
}

/// Exact planner conservation counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtTorrentPlanCounts {
    pub input: u64,
    pub v2_dropped: u64,
    pub primary_dropped: u64,
    pub projected: u64,
    pub projection_failed: u64,
}

impl DhtTorrentPlanCounts {
    /// Every input has exactly one terminal planner disposition.
    #[must_use]
    pub const fn conserves(self) -> bool {
        let Some(total) = self.v2_dropped.checked_add(self.primary_dropped) else {
            return false;
        };
        let Some(total) = total.checked_add(self.projected) else {
            return false;
        };
        let Some(total) = total.checked_add(self.projection_failed) else {
            return false;
        };
        self.input == total
    }
}

/// Writer-eligible rows that a future adapter may persist as one transaction.
///
/// This type makes no writer, transaction-execution, retry, or commit claim.
#[derive(Debug, Default)]
pub struct DhtTorrentTransactionPlan {
    pub torrents: Vec<DhtTorrentWrite>,
    pub files: Vec<DhtTorrentFileWrite>,
    pub file_summaries: Vec<DhtTorrentFileSummaryWrite>,
    pub sources: Vec<DhtTorrentSourceLinkWrite>,
    pub pieces: Vec<DhtTorrentPiecesWrite>,
    pub queue_jobs: Vec<QueueJob>,
}

/// All inert outputs of one pure planning call.
#[derive(Debug, Default)]
pub struct DhtTorrentPersistPlan {
    pub transaction: DhtTorrentTransactionPlan,
    pub classifier_groups: Vec<Vec<Id20>>,
    /// Deterministic first-primary input order. Go sends the same set through
    /// map iteration, whose order is unspecified.
    pub scrape_candidates: Vec<DhtInfoHashTriageRequest>,
    pub projection_failures: Vec<DhtTorrentProjectionFailure>,
    pub diagnostics: Vec<DhtTorrentPlanDiagnostic>,
    pub counts: DhtTorrentPlanCounts,
}

/// Plan one ordered batch without performing any external effect.
#[must_use]
pub fn plan_dht_torrent_batch(
    requests: &[DhtPersistTorrentRequest],
    existing_v2: &DhtResolvedExistingV2,
    config: DhtTorrentPlanConfig,
) -> DhtTorrentPersistPlan {
    DhtTorrentPlanner::new(config).plan(requests, existing_v2)
}

type BlobEncoder<'a> = dyn FnMut(&[BlobFile]) -> Result<Vec<u8>, String> + 'a;
type QueueBuilder<'a> = dyn FnMut(&[Id20]) -> Result<QueueJob, String> + 'a;

fn plan_with(
    requests: &[DhtPersistTorrentRequest],
    existing_v2: &DhtResolvedExistingV2,
    config: DhtTorrentPlanConfig,
    encode_blob: &mut BlobEncoder<'_>,
    build_job: &mut QueueBuilder<'_>,
) -> DhtTorrentPersistPlan {
    let mut plan = DhtTorrentPersistPlan::default();
    plan.counts.input = u64::try_from(requests.len()).expect("usize fits in u64");
    let mut batch_v2 = BTreeMap::<[u8; 32], Id20>::new();
    let mut primary_seen = BTreeSet::<Id20>::new();
    let mut hashes_to_classify = Vec::with_capacity(DHT_TORRENT_CLASSIFIER_BATCH_LIMIT);

    for (input_index, request) in requests.iter().enumerate() {
        let primary = request.info_hash;
        let identity_valid = request_carries_primary_identity(request);
        if identity_valid {
            if let Some(v2) = request.meta_info.info_hash_v2() {
                if is_v2_cross_primary_duplicate(primary, v2, existing_v2, &batch_v2) {
                    plan.counts.v2_dropped += 1;
                    continue;
                }
                batch_v2.insert(v2, primary);
            }
        }

        if !primary_seen.insert(primary) {
            plan.counts.primary_dropped += 1;
            continue;
        }

        plan.scrape_candidates.push(DhtInfoHashTriageRequest {
            info_hash: primary,
            source_node_addr: request.source_node_addr,
        });

        let input_index = u64::try_from(input_index).expect("usize fits in u64");
        if !identity_valid {
            plan.projection_failures.push(DhtTorrentProjectionFailure {
                input_index,
                info_hash: primary,
                error: DhtTorrentProjectionError::IdentityMismatch,
            });
            plan.counts.projection_failed += 1;
            continue;
        }
        match project_request(input_index, request, config, encode_blob) {
            Ok((projection, diagnostic)) => {
                if let Some(diagnostic) = diagnostic {
                    plan.diagnostics.push(diagnostic);
                }
                plan.transaction.files.extend(projection.files);
                if let Some(summary) = projection.summary {
                    plan.transaction.file_summaries.push(summary);
                }
                plan.transaction.sources.push(projection.source);
                if let Some(pieces) = projection.pieces {
                    plan.transaction.pieces.push(pieces);
                }
                plan.transaction.torrents.push(projection.torrent);
                hashes_to_classify.push(primary);
                plan.counts.projected += 1;
            }
            Err(error) => {
                plan.projection_failures.push(DhtTorrentProjectionFailure {
                    input_index,
                    info_hash: primary,
                    error,
                });
                plan.counts.projection_failed += 1;
            }
        }
    }
    plan.classifier_groups = classifier_groups(&hashes_to_classify);

    for (group_index, group) in plan.classifier_groups.iter().enumerate() {
        match build_job(group) {
            Ok(job) => plan.transaction.queue_jobs.push(job),
            Err(error) => {
                plan.diagnostics
                    .push(DhtTorrentPlanDiagnostic::QueueConstructionFailed {
                        group_index: u64::try_from(group_index).expect("usize fits in u64"),
                        error,
                    })
            }
        }
    }
    debug_assert!(plan.counts.conserves());
    debug_assert_eq!(
        u64::try_from(plan.scrape_candidates.len()).expect("usize fits in u64"),
        plan.counts
            .projected
            .checked_add(plan.counts.projection_failed)
            .expect("terminal counts cannot exceed input")
    );
    plan
}

fn request_carries_primary_identity(request: &DhtPersistTorrentRequest) -> bool {
    let primary = request.info_hash;
    request
        .meta_info
        .info_hash_v1()
        .is_some_and(|hash| hash == *primary.as_bytes())
        || request
            .meta_info
            .info_hash_v2()
            .is_some_and(|hash| &hash[..20] == primary.as_bytes())
}

fn is_v2_cross_primary_duplicate(
    primary: Id20,
    v2: [u8; 32],
    existing_v2: &DhtResolvedExistingV2,
    batch_v2: &BTreeMap<[u8; 32], Id20>,
) -> bool {
    existing_v2
        .get(&v2)
        .is_some_and(|stored| *stored != primary)
        || batch_v2.get(&v2).is_some_and(|stored| *stored != primary)
}

fn classifier_groups(hashes: &[Id20]) -> Vec<Vec<Id20>> {
    hashes
        .chunks(DHT_TORRENT_CLASSIFIER_BATCH_LIMIT)
        .map(<[Id20]>::to_vec)
        .collect()
}

struct Projection {
    torrent: DhtTorrentWrite,
    files: Vec<DhtTorrentFileWrite>,
    summary: Option<DhtTorrentFileSummaryWrite>,
    source: DhtTorrentSourceLinkWrite,
    pieces: Option<DhtTorrentPiecesWrite>,
}

fn project_request(
    input_index: u64,
    request: &DhtPersistTorrentRequest,
    config: DhtTorrentPlanConfig,
    encode_blob: &mut BlobEncoder<'_>,
) -> Result<(Projection, Option<DhtTorrentPlanDiagnostic>), DhtTorrentProjectionError> {
    let parsed: &ParsedInfo = &request.meta_info;
    let primary = request.info_hash;
    let v1 = parsed.info_hash_v1().map(|hash| {
        Id20::from_slice(&hash).expect("ParsedInfo v1 identity is statically twenty bytes")
    });
    let v2 = parsed.info_hash_v2();
    if v1 != Some(primary) && v2.map(|hash| &hash[..20] == primary.as_bytes()) != Some(true) {
        return Err(DhtTorrentProjectionError::IdentityMismatch);
    }

    let info = parsed.info();
    let name = std::str::from_utf8(info.best_name())
        .map_err(|_| DhtTorrentProjectionError::InvalidNameUtf8)?
        .to_owned();
    if name.contains('\0') {
        return Err(DhtTorrentProjectionError::NameContainsNul);
    }
    let size = u64::try_from(
        info.total_length()
            .map_err(DhtTorrentProjectionError::InvalidFiles)?,
    )
    .expect("Info::total_length rejects negative lengths");

    let mut files_status = FilesStatus::Single;
    let mut files_count = None;
    let mut file_writes = Vec::new();
    let mut blob_files = Vec::new();
    if info.is_dir() {
        let normalized = info
            .upverted_files()
            .map_err(DhtTorrentProjectionError::InvalidFiles)?;
        let v2_single =
            v2.is_some() && normalized.len() == 1 && normalized[0].best_path().len() <= 1;
        if !v2_single {
            if normalized.len() > i32::MAX as usize {
                return Err(DhtTorrentProjectionError::FileCountOutOfRange(
                    normalized.len(),
                ));
            }
            files_status = FilesStatus::Multi;
            files_count = Some(normalized.len() as u32);
            for (index, file) in normalized.iter().enumerate() {
                let index_u64 = u64::try_from(index).expect("usize fits in u64");
                if index_u64 >= config.save_files_threshold {
                    files_status = FilesStatus::OverThreshold;
                    break;
                }
                if index > i32::MAX as usize {
                    return Err(DhtTorrentProjectionError::FileIndexOutOfRange(index));
                }
                let mut components = Vec::with_capacity(file.best_path().len());
                for (component_index, component) in file.best_path().iter().enumerate() {
                    let component_index =
                        u64::try_from(component_index).expect("usize fits in u64");
                    let component = std::str::from_utf8(component).map_err(|_| {
                        DhtTorrentProjectionError::InvalidPathUtf8 {
                            file_index: index_u64,
                            component_index,
                        }
                    })?;
                    if component.contains('\0') {
                        return Err(DhtTorrentProjectionError::PathComponentContainsNul {
                            file_index: index_u64,
                            component_index,
                        });
                    }
                    components.push(component);
                }
                let path = components.join("/");
                if path.contains('\0') {
                    return Err(DhtTorrentProjectionError::PathContainsNul {
                        file_index: index_u64,
                    });
                }
                let file_size = u64::try_from(file.length()).map_err(|_| {
                    DhtTorrentProjectionError::InvalidFiles(InfoFilesError::NegativeFileLength {
                        path: file.best_path().to_vec(),
                        length: file.length(),
                    })
                })?;
                let index = index as u32;
                file_writes.push(DhtTorrentFileWrite {
                    info_hash: primary,
                    index,
                    path: path.clone(),
                    size: file_size,
                });
                blob_files.push(BlobFile {
                    index,
                    extension: file_extension_from_path(&path).unwrap_or_default(),
                    path,
                    size: file_size,
                });
            }
        }
    }

    let model_hash = InfoHash::new(*primary.as_bytes());
    let mut files_data = None;
    let mut file_extensions = None;
    let mut diagnostic = None;
    let mut summary = None;
    if !blob_files.is_empty() {
        let base_summary = checked_file_summary(model_hash, &blob_files)?;
        let compressed_bytes = match encode_blob(&blob_files) {
            Ok(blob) => {
                let length = u64::try_from(blob.len()).expect("usize fits in u64");
                file_extensions = Some(base_summary.extensions.clone());
                files_data = Some(blob);
                Some(length)
            }
            Err(error) => {
                diagnostic = Some(DhtTorrentPlanDiagnostic::BlobEncodingFailed {
                    input_index,
                    info_hash: primary,
                    error,
                });
                None
            }
        };
        summary = Some(DhtTorrentFileSummaryWrite {
            summary: base_summary,
            compressed_bytes,
        });
    }

    let torrent = DhtTorrentWrite {
        info_hash: primary,
        info_hash_v1: v1,
        info_hash_v2: v2,
        meta_version: u16::from(parsed.meta_version().as_u8()),
        name,
        size,
        private: info.private().unwrap_or(false),
        files_status,
        files_count,
        files_data,
        file_extensions,
    };
    let pieces = if config.save_pieces {
        let piece_length = info.piece_length();
        if piece_length < 0 {
            return Err(DhtTorrentProjectionError::InvalidPieceLength(piece_length));
        }
        Some(DhtTorrentPiecesWrite {
            info_hash: primary,
            piece_length,
            pieces: info.pieces().to_vec(),
        })
    } else {
        None
    };
    Ok((
        Projection {
            torrent,
            files: file_writes,
            summary,
            source: DhtTorrentSourceLinkWrite {
                source: DHT_TORRENT_SOURCE.to_owned(),
                info_hash: primary,
            },
            pieces,
        },
        diagnostic,
    ))
}

fn checked_file_summary(
    info_hash: InfoHash,
    files: &[BlobFile],
) -> Result<TorrentFileSummary, DhtTorrentProjectionError> {
    let file_count = u32::try_from(files.len())
        .map_err(|_| DhtTorrentProjectionError::SummaryFileCountOutOfRange(files.len()))?;
    let mut total_size = 0_u64;
    let mut largest_file_size = 0_u64;
    for file in files {
        total_size = total_size
            .checked_add(file.size)
            .ok_or(DhtTorrentProjectionError::SummaryTotalSizeOutOfRange)?;
        largest_file_size = largest_file_size.max(file.size);
    }
    let total_size = i64::try_from(total_size)
        .map_err(|_| DhtTorrentProjectionError::SummaryTotalSizeOutOfRange)?;
    let largest_file_size = i64::try_from(largest_file_size)
        .map_err(|_| DhtTorrentProjectionError::SummaryTotalSizeOutOfRange)?;

    let extensions: Vec<_> = files
        .iter()
        .filter_map(|file| file_extension_from_path(&file.path))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let has_video = extensions
        .iter()
        .any(|extension| FileType::from_extension(extension) == Some(FileType::Video));
    let has_subtitle = extensions
        .iter()
        .any(|extension| FileType::from_extension(extension) == Some(FileType::Subtitles));
    let has_audio = extensions
        .iter()
        .any(|extension| FileType::from_extension(extension) == Some(FileType::Audio));
    Ok(TorrentFileSummary {
        info_hash,
        file_count,
        total_size,
        largest_file_size,
        extensions,
        has_video,
        has_subtitle,
        has_audio,
    })
}

fn build_classifier_job(
    hashes: &[Id20],
    classifier_queue: DhtCrawlerClassifierQueue,
) -> Result<QueueJob, String> {
    let params = ProcessTorrentParams {
        info_hashes: hashes
            .iter()
            .map(|hash| ProtocolId::from_bytes(*hash.as_bytes()))
            .collect(),
        ..ProcessTorrentParams::default()
    };
    let options = QueueJobOptions::default().with_delay(DHT_TORRENT_CLASSIFIER_DELAY);
    match classifier_queue {
        DhtCrawlerClassifierQueue::Shadow => {
            new_queue_job(PROCESS_TORRENT_SHADOW, &params, options.with_max_retries(2))
        }
        DhtCrawlerClassifierQueue::Live => process_torrent_job(&params, options),
    }
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
    use std::sync::Arc;

    use bitmagnet_metainfo::parse_info_bytes;
    use bitmagnet_model::{deserialize_files, FileType};
    use bitmagnet_queue::{
        fingerprint, QueueJobStatus, DEFAULT_ARCHIVAL_DURATION, PROCESS_TORRENT,
    };
    use serde_json::Value;
    use sha1::Sha1;
    use sha2::Digest;

    use super::*;

    const FIXTURE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/dht/dht_crawler_persist_torrents.jsonl"
    ));

    fn fixture_rows() -> Vec<Value> {
        FIXTURE
            .lines()
            .map(|line| serde_json::from_str(line).expect("fixture row is valid JSON"))
            .collect()
    }

    fn id(value: &str) -> Id20 {
        Id20::from_hex(value).expect("fixture primary hash is valid")
    }

    fn full_hash(value: &str) -> [u8; 32] {
        decode_hex(value)
            .try_into()
            .expect("fixture v2 hash is 32 bytes")
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        fn nibble(byte: u8) -> u8 {
            match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                _ => panic!("fixture hex is lowercase"),
            }
        }

        assert_eq!(value.len() % 2, 0);
        value
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1]))
            .collect()
    }

    fn text<'a>(value: &'a Value, key: &str) -> &'a str {
        value[key]
            .as_str()
            .unwrap_or_else(|| panic!("{key} is text"))
    }

    fn number(value: &Value, key: &str) -> u64 {
        value[key]
            .as_u64()
            .unwrap_or_else(|| panic!("{key} is unsigned"))
    }

    fn fixture_request(case: &Value, source_node_addr: SocketAddr) -> DhtPersistTorrentRequest {
        let raw = decode_hex(text(case, "rawInfoHex"));
        let info_hash = id(text(case, "requestedInfoHash"));
        let parsed = parse_info_bytes(*info_hash.as_bytes(), &raw)
            .unwrap_or_else(|error| panic!("fixture metainfo must parse: {error}"));
        DhtPersistTorrentRequest {
            info_hash,
            source_node_addr,
            meta_info: Arc::new(parsed),
        }
    }

    fn fixture_config(case: &Value) -> DhtTorrentPlanConfig {
        DhtTorrentPlanConfig {
            save_pieces: case["savePieces"].as_bool().expect("savePieces is bool"),
            save_files_threshold: number(case, "saveFilesThreshold"),
        }
    }

    fn optional_id(value: &str) -> Option<Id20> {
        (!value.is_empty()).then(|| id(value))
    }

    fn optional_full_hash(value: &str) -> Option<[u8; 32]> {
        (!value.is_empty()).then(|| full_hash(value))
    }

    fn strings(value: &Value) -> Vec<String> {
        value
            .as_array()
            .expect("string list is an array")
            .iter()
            .map(|item| item.as_str().expect("list member is text").to_owned())
            .collect()
    }

    fn assert_fixture_projection(row: &Value) -> DhtTorrentPersistPlan {
        let cases = row["input"]["cases"].as_array().expect("cases array");
        assert!(!cases.is_empty());
        let source_node_addr = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 44), 6881));
        let requests: Vec<_> = cases
            .iter()
            .map(|case| fixture_request(case, source_node_addr))
            .collect();
        let planner = DhtTorrentPlanner::new(fixture_config(&cases[0]));
        let plan = planner.plan(&requests, &DhtResolvedExistingV2::new());
        let expected = row["expected"]["models"].as_array().expect("models array");

        assert_eq!(plan.counts.input, expected.len() as u64);
        assert_eq!(plan.counts.projected, expected.len() as u64);
        assert_eq!(plan.counts.v2_dropped, 0);
        assert_eq!(plan.counts.primary_dropped, 0);
        assert_eq!(plan.counts.projection_failed, 0);
        assert!(plan.counts.conserves());
        assert!(plan.projection_failures.is_empty());
        assert!(plan.diagnostics.is_empty());
        assert_eq!(plan.transaction.torrents.len(), expected.len());
        assert_eq!(plan.transaction.sources.len(), expected.len());
        assert_eq!(plan.scrape_candidates.len(), expected.len());
        assert_eq!(plan.classifier_groups.len(), 1);
        assert_eq!(plan.transaction.queue_jobs.len(), 1);

        for (index, model) in expected.iter().enumerate() {
            let torrent = &plan.transaction.torrents[index];
            let expected_hash = id(text(model, "infoHash"));
            assert_eq!(torrent.info_hash, expected_hash);
            assert_eq!(torrent.info_hash_v1, optional_id(text(model, "infoHashV1")));
            assert_eq!(
                torrent.info_hash_v2,
                optional_full_hash(text(model, "infoHashV2"))
            );
            assert_eq!(torrent.meta_version, number(model, "metaVersion") as u16);
            assert_eq!(torrent.name, text(model, "name"));
            assert_eq!(torrent.size, number(model, "size"));
            assert_eq!(
                torrent.private,
                model["private"].as_bool().expect("private is bool")
            );
            assert_eq!(torrent.files_status.as_str(), text(model, "filesStatus"));
            let expected_count = model["filesCountValid"]
                .as_bool()
                .expect("filesCountValid is bool")
                .then(|| number(model, "filesCount") as u32);
            assert_eq!(torrent.files_count, expected_count);

            let expected_files = model["files"].as_array();
            let actual_files: Vec<_> = plan
                .transaction
                .files
                .iter()
                .filter(|file| file.info_hash == expected_hash)
                .collect();
            match expected_files {
                None => assert!(actual_files.is_empty()),
                Some(files) => {
                    assert_eq!(actual_files.len(), files.len());
                    for (actual, expected_file) in actual_files.iter().zip(files) {
                        assert_eq!(u64::from(actual.index), number(expected_file, "index"));
                        assert_eq!(actual.path, text(expected_file, "path"));
                        assert_eq!(actual.size, number(expected_file, "size"));
                    }
                }
            }

            let expected_decoded = model["decodedFiles"].as_array();
            match expected_decoded {
                None => assert!(torrent.files_data.is_none()),
                Some(files) => {
                    let blob = torrent.files_data.as_deref().expect("blob is present");
                    let decoded = deserialize_files(blob).expect("Rust blob decodes");
                    assert_eq!(decoded.len(), files.len());
                    for (actual, expected_file) in decoded.iter().zip(files) {
                        assert_eq!(u64::from(actual.index), number(expected_file, "index"));
                        assert_eq!(actual.path, text(expected_file, "path"));
                        assert_eq!(actual.extension, text(expected_file, "extension"));
                        assert_eq!(actual.size, number(expected_file, "size"));
                    }
                }
            }
            let expected_extensions = model["fileExtensions"]
                .as_array()
                .map(|_| strings(&model["fileExtensions"]));
            assert_eq!(torrent.file_extensions, expected_extensions);

            let source = &plan.transaction.sources[index];
            assert_eq!(source.info_hash, expected_hash);
            assert_eq!(source.source, DHT_TORRENT_SOURCE);
            assert_eq!(plan.scrape_candidates[index].info_hash, expected_hash);
            assert_eq!(
                plan.scrape_candidates[index].source_node_addr,
                source_node_addr
            );

            let pieces = plan
                .transaction
                .pieces
                .iter()
                .find(|pieces| pieces.info_hash == expected_hash);
            if model["pieces"]["present"]
                .as_bool()
                .expect("pieces present marker")
            {
                let pieces = pieces.expect("pieces row is present");
                assert_eq!(
                    pieces.piece_length,
                    model["pieces"]["pieceLength"].as_i64().unwrap()
                );
                assert_eq!(
                    pieces.pieces,
                    decode_hex(text(&model["pieces"], "piecesHex"))
                );
            } else {
                assert!(pieces.is_none());
            }

            let summary =
                plan.transaction.file_summaries.iter().find(|summary| {
                    summary.summary.info_hash.as_bytes() == expected_hash.as_bytes()
                });
            if let Some(expected_decoded) = model["decodedFiles"]
                .as_array()
                .filter(|files| !files.is_empty())
            {
                let summary = summary.expect("summary is present");
                let derived_total: u64 = expected_decoded
                    .iter()
                    .map(|file| number(file, "size"))
                    .sum();
                let derived_largest = expected_decoded
                    .iter()
                    .map(|file| number(file, "size"))
                    .max()
                    .unwrap();
                let derived_extensions: Vec<_> = expected_decoded
                    .iter()
                    .map(|file| text(file, "extension"))
                    .filter(|extension| !extension.is_empty())
                    .map(str::to_owned)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                assert_eq!(
                    u64::from(summary.summary.file_count),
                    expected_decoded.len() as u64
                );
                assert_eq!(
                    summary.summary.total_size,
                    i64::try_from(derived_total).unwrap()
                );
                assert_eq!(
                    summary.summary.largest_file_size,
                    i64::try_from(derived_largest).unwrap()
                );
                assert_eq!(summary.summary.extensions, derived_extensions);
                assert_eq!(
                    summary.summary.has_video,
                    summary
                        .summary
                        .extensions
                        .iter()
                        .any(|extension| FileType::from_extension(extension)
                            == Some(FileType::Video))
                );
                assert_eq!(
                    summary.summary.has_subtitle,
                    summary.summary.extensions.iter().any(|extension| {
                        FileType::from_extension(extension) == Some(FileType::Subtitles)
                    })
                );
                assert_eq!(
                    summary.summary.has_audio,
                    summary
                        .summary
                        .extensions
                        .iter()
                        .any(|extension| FileType::from_extension(extension)
                            == Some(FileType::Audio))
                );
                assert_eq!(
                    summary.compressed_bytes,
                    torrent
                        .files_data
                        .as_ref()
                        .map(|blob| u64::try_from(blob.len()).expect("usize fits u64"))
                );
                if let Some(expected_summary) =
                    model.get("summary").filter(|value| !value.is_null())
                {
                    assert_eq!(
                        u64::from(summary.summary.file_count),
                        number(expected_summary, "fileCount")
                    );
                    assert_eq!(
                        summary.summary.total_size,
                        expected_summary["totalSize"].as_i64().unwrap()
                    );
                    assert_eq!(
                        summary.summary.largest_file_size,
                        expected_summary["largestFileSize"].as_i64().unwrap()
                    );
                    assert_eq!(
                        summary.summary.extensions,
                        strings(&expected_summary["extensions"])
                    );
                    assert_eq!(summary.summary.has_video, expected_summary["hasVideo"]);
                    assert_eq!(
                        summary.summary.has_subtitle,
                        expected_summary["hasSubtitle"]
                    );
                    assert_eq!(summary.summary.has_audio, expected_summary["hasAudio"]);
                }
            } else {
                assert!(summary.is_none());
            }
        }

        let projected: Vec<_> = plan
            .transaction
            .torrents
            .iter()
            .map(|torrent| torrent.info_hash)
            .collect();
        assert_eq!(plan.classifier_groups[0], projected);
        plan
    }

    fn append_bytes(output: &mut Vec<u8>, value: &[u8]) {
        output.extend_from_slice(value.len().to_string().as_bytes());
        output.push(b':');
        output.extend_from_slice(value);
    }

    fn raw_single(name: &[u8]) -> Vec<u8> {
        raw_single_fields(name, 16_384, &[0x22; 20])
    }

    fn raw_single_fields(name: &[u8], piece_length: i64, pieces: &[u8]) -> Vec<u8> {
        let mut raw = b"d6:lengthi4096e4:name".to_vec();
        append_bytes(&mut raw, name);
        raw.extend_from_slice(b"12:piece lengthi");
        raw.extend_from_slice(piece_length.to_string().as_bytes());
        raw.extend_from_slice(b"e6:pieces");
        append_bytes(&mut raw, pieces);
        raw.push(b'e');
        raw
    }

    fn raw_multi(path_component: &[u8]) -> Vec<u8> {
        let mut raw = b"d5:filesld6:lengthi1e4:pathl".to_vec();
        append_bytes(&mut raw, path_component);
        raw.extend_from_slice(b"eee4:name4:root12:piece lengthi16384e6:pieces20:");
        raw.extend_from_slice(&[0x33; 20]);
        raw.push(b'e');
        raw
    }

    fn request_from_raw(raw: Vec<u8>, source_node_addr: SocketAddr) -> DhtPersistTorrentRequest {
        let requested: [u8; 20] = Sha1::digest(&raw).into();
        let parsed = parse_info_bytes(requested, &raw).expect("test metainfo parses");
        DhtPersistTorrentRequest {
            info_hash: Id20::from_slice(&requested).unwrap(),
            source_node_addr,
            meta_info: Arc::new(parsed),
        }
    }

    fn id_from_number(value: u64) -> Id20 {
        let mut bytes = [0_u8; 20];
        bytes[12..].copy_from_slice(&value.to_be_bytes());
        Id20::from_slice(&bytes).unwrap()
    }

    #[test]
    fn defaults_public_traits_empty_lookup_and_conservation() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DhtTorrentPlanner>();
        assert_send_sync::<DhtTorrentPersistPlan>();
        assert_send_sync::<DhtTorrentTransactionPlan>();

        let planner = DhtTorrentPlanner::default();
        assert_eq!(
            planner.config(),
            DhtTorrentPlanConfig {
                save_pieces: false,
                save_files_threshold: 100,
            }
        );
        assert_eq!(planner.classifier_queue(), DhtCrawlerClassifierQueue::Live);
        assert_eq!(
            DhtCrawlerClassifierQueue::default().queue_name(),
            PROCESS_TORRENT
        );
        assert_eq!(
            DhtCrawlerClassifierQueue::Shadow.queue_name(),
            PROCESS_TORRENT_SHADOW
        );
        assert_eq!(DHT_TORRENT_CLASSIFIER_BATCH_LIMIT, 100);
        assert_eq!(DHT_TORRENT_CLASSIFIER_DELAY, Duration::from_secs(60));
        assert_eq!(DHT_TORRENT_SOURCE, "dht");
        assert!(planner.v2_lookup_keys(&[]).is_empty());

        let plan = planner.plan(&[], &DhtResolvedExistingV2::new());
        assert_eq!(plan.counts, DhtTorrentPlanCounts::default());
        assert!(plan.counts.conserves());
        assert!(plan.transaction.torrents.is_empty());
        assert!(plan.transaction.files.is_empty());
        assert!(plan.transaction.file_summaries.is_empty());
        assert!(plan.transaction.sources.is_empty());
        assert!(plan.transaction.pieces.is_empty());
        assert!(plan.transaction.queue_jobs.is_empty());
        assert!(plan.classifier_groups.is_empty());
        assert!(plan.scrape_candidates.is_empty());
        assert!(plan.projection_failures.is_empty());
        assert!(plan.diagnostics.is_empty());
    }

    #[test]
    fn fixture_rows_two_through_four_project_exact_models() {
        let rows = fixture_rows();
        for row in &rows[1..4] {
            assert_fixture_projection(row);
        }

        let cases = rows[3]["input"]["cases"].as_array().unwrap();
        let requests: Vec<_> = cases
            .iter()
            .map(|case| fixture_request(case, "[fe80::1%7]:6881".parse().unwrap()))
            .collect();
        let planner = DhtTorrentPlanner::default();
        let keys = planner.v2_lookup_keys(&[
            requests[1].clone(),
            requests[0].clone(),
            requests[1].clone(),
        ]);
        assert_eq!(keys.len(), 2);
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));

        let pure_v2_files = requests[0].meta_info.info().upverted_files().unwrap();
        assert_eq!(pure_v2_files.len(), 1);
        assert_eq!(pure_v2_files[0].pieces_root(), Some([0x11; 32]));
        // The frozen case proves structural parser/projection behavior, not
        // content-Merkle correctness for the synthetic payload.
    }

    #[test]
    fn fixture_row_five_v2_filter_cases_are_exact_and_stable() {
        let rows = fixture_rows();
        let row = &rows[4];
        let cases = row["input"]["dedupCases"].as_array().unwrap();
        let expected = row["expected"]["dedupCases"].as_array().unwrap();
        for (case, expected) in cases.iter().zip(expected) {
            let existing: DhtResolvedExistingV2 = case["existing"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| {
                    (
                        full_hash(text(item, "infoHashV2")),
                        id(text(item, "primaryInfoHash")),
                    )
                })
                .collect();
            let mut batch = BTreeMap::new();
            let mut kept = Vec::new();
            let mut dropped = 0_u64;
            for item in case["items"].as_array().unwrap() {
                let primary = id(text(item, "primaryInfoHash"));
                let v2 = text(item, "infoHashV2");
                if v2.is_empty() {
                    kept.push(primary);
                    continue;
                }
                let v2 = full_hash(v2);
                if is_v2_cross_primary_duplicate(primary, v2, &existing, &batch) {
                    dropped += 1;
                } else {
                    batch.insert(v2, primary);
                    kept.push(primary);
                }
            }
            let expected_kept: Vec<_> = expected["keptPrimaryInfoHashes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| id(value.as_str().unwrap()))
                .collect();
            assert_eq!(kept, expected_kept, "{}", text(case, "label"));
            assert_eq!(dropped, number(expected, "dropped"));
        }
    }

    #[test]
    fn failed_first_is_reserved_scraped_and_suppresses_later_primary() {
        let source = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 6881, 19, 7));
        let request = request_from_raw(raw_single(b"bad\0name"), source);
        let plan = DhtTorrentPlanner::default()
            .plan(&[request.clone(), request], &DhtResolvedExistingV2::new());

        assert_eq!(
            plan.counts,
            DhtTorrentPlanCounts {
                input: 2,
                primary_dropped: 1,
                projection_failed: 1,
                ..DhtTorrentPlanCounts::default()
            }
        );
        assert!(plan.counts.conserves());
        assert_eq!(plan.scrape_candidates.len(), 1);
        assert_eq!(plan.scrape_candidates[0].source_node_addr, source);
        assert_eq!(plan.projection_failures.len(), 1);
        assert_eq!(
            plan.projection_failures[0].error,
            DhtTorrentProjectionError::NameContainsNul
        );
        assert!(plan.transaction.torrents.is_empty());
        assert!(plan.classifier_groups.is_empty());
    }

    #[test]
    fn fixture_row_six_classifier_groups_and_jobs_are_exact() {
        let rows = fixture_rows();
        let expected = &rows[5]["expected"]["classifier"];
        let hashes: Vec<_> = (1..=101).map(id_from_number).collect();
        let groups = classifier_groups(&hashes);
        let expected_groups = expected["classifierGroups"].as_array().unwrap();
        assert_eq!(groups.len(), 2);
        for (actual, expected) in groups.iter().zip(expected_groups) {
            let expected: Vec<_> = expected
                .as_array()
                .unwrap()
                .iter()
                .map(|value| id(value.as_str().unwrap()))
                .collect();
            assert_eq!(*actual, expected);
        }

        let jobs: Vec<_> = groups
            .iter()
            .map(|group| build_classifier_job(group, DhtCrawlerClassifierQueue::Live).unwrap())
            .collect();
        for (actual, expected) in jobs.iter().zip(expected["queueJobs"].as_array().unwrap()) {
            assert_eq!(actual.queue, text(expected, "queue"));
            assert_eq!(actual.payload, text(expected, "payload"));
            assert_eq!(actual.fingerprint, text(expected, "fingerprint"));
            assert_eq!(actual.status, QueueJobStatus::Pending);
            assert_eq!(actual.status.as_str(), text(expected, "status"));
            assert_eq!(
                u64::from(actual.max_retries),
                number(expected, "maxRetries")
            );
            assert_eq!(
                i64::from(actual.priority),
                expected["priority"].as_i64().unwrap()
            );
            assert_eq!(actual.archival_duration, DEFAULT_ARCHIVAL_DURATION);
            assert_eq!(
                actual.archival_duration.as_nanos(),
                u128::from(number(expected, "archivalDurationNanoseconds"))
            );
            assert_eq!(
                actual.delay.as_millis(),
                u128::from(number(expected, "delayMillis"))
            );
        }
    }

    #[test]
    fn classifier_queue_target_changes_only_queue_and_fingerprint() {
        let hashes = [id_from_number(1), id_from_number(2)];
        let live = build_classifier_job(&hashes, DhtCrawlerClassifierQueue::Live).unwrap();
        let shadow = build_classifier_job(&hashes, DhtCrawlerClassifierQueue::Shadow).unwrap();

        assert_eq!(live.queue, PROCESS_TORRENT);
        assert_eq!(shadow.queue, PROCESS_TORRENT_SHADOW);
        assert_eq!(live.payload, shadow.payload);
        assert_eq!(live.status, shadow.status);
        assert_eq!(live.max_retries, 2);
        assert_eq!(live.max_retries, shadow.max_retries);
        assert_eq!(live.priority, shadow.priority);
        assert_eq!(live.archival_duration, shadow.archival_duration);
        assert_eq!(live.delay, DHT_TORRENT_CLASSIFIER_DELAY);
        assert_eq!(live.delay, shadow.delay);
        assert_eq!(
            live.fingerprint,
            fingerprint(PROCESS_TORRENT, &live.payload)
        );
        assert_eq!(
            shadow.fingerprint,
            fingerprint(PROCESS_TORRENT_SHADOW, &shadow.payload)
        );
        assert_ne!(live.fingerprint, shadow.fingerprint);
    }

    #[test]
    fn blob_failure_keeps_relational_projection_and_classifier() {
        let rows = fixture_rows();
        let case = &rows[2]["input"]["cases"][0];
        let request = fixture_request(case, "127.0.0.1:6881".parse().unwrap());
        let mut encoder = |_files: &[BlobFile]| Err("scripted encoder failure".to_owned());
        let mut jobs =
            |hashes: &[Id20]| build_classifier_job(hashes, DhtCrawlerClassifierQueue::Live);
        let plan = plan_with(
            &[request],
            &DhtResolvedExistingV2::new(),
            fixture_config(case),
            &mut encoder,
            &mut jobs,
        );

        assert_eq!(plan.transaction.torrents.len(), 1);
        assert_eq!(plan.transaction.files.len(), 3);
        assert_eq!(plan.transaction.sources.len(), 1);
        assert_eq!(plan.transaction.pieces.len(), 1);
        assert_eq!(plan.transaction.file_summaries.len(), 1);
        assert!(plan.transaction.torrents[0].files_data.is_none());
        assert!(plan.transaction.torrents[0].file_extensions.is_none());
        assert_eq!(plan.transaction.file_summaries[0].compressed_bytes, None);
        assert_eq!(plan.classifier_groups.len(), 1);
        assert_eq!(plan.transaction.queue_jobs.len(), 1);
        assert_eq!(plan.counts.projected, 1);
        assert!(matches!(
            &plan.diagnostics[..],
            [DhtTorrentPlanDiagnostic::BlobEncodingFailed { error, .. }]
                if error == "scripted encoder failure"
        ));
    }

    #[test]
    fn queue_failure_omits_only_job_and_records_diagnostic() {
        let rows = fixture_rows();
        let case = &rows[1]["input"]["cases"][0];
        let request = fixture_request(case, "127.0.0.1:6881".parse().unwrap());
        let mut encoder =
            |files: &[BlobFile]| serialize_files(files).map_err(|error| error.to_string());
        let mut jobs = |_hashes: &[Id20]| Err("scripted queue failure".to_owned());
        let plan = plan_with(
            &[request],
            &DhtResolvedExistingV2::new(),
            DhtTorrentPlanConfig::default(),
            &mut encoder,
            &mut jobs,
        );

        assert_eq!(plan.transaction.torrents.len(), 1);
        assert_eq!(plan.transaction.sources.len(), 1);
        assert_eq!(plan.classifier_groups.len(), 1);
        assert!(plan.transaction.queue_jobs.is_empty());
        assert!(matches!(
            &plan.diagnostics[..],
            [DhtTorrentPlanDiagnostic::QueueConstructionFailed {
                group_index: 0,
                error,
            }] if error == "scripted queue failure"
        ));
    }

    #[test]
    fn successful_extensionless_blob_preserves_nonnil_empty_extensions() {
        let request = request_from_raw(
            raw_multi(b"README"),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 6881)),
        );
        let plan = DhtTorrentPlanner::default().plan(&[request], &BTreeMap::new());
        assert_eq!(plan.transaction.files.len(), 1);
        assert!(plan.transaction.torrents[0].files_data.is_some());
        assert_eq!(
            plan.transaction.torrents[0].file_extensions,
            Some(Vec::new())
        );
        assert!(plan.transaction.file_summaries[0]
            .summary
            .extensions
            .is_empty());
    }

    #[test]
    fn checked_identity_utf8_and_postgres_nul_failures_are_typed() {
        let source = SocketAddr::from((Ipv4Addr::LOCALHOST, 6881));
        let valid = request_from_raw(raw_single(b"valid-name"), source);
        let mut mismatched = valid.clone();
        mismatched.info_hash = Id20::ZERO;
        let invalid_utf8 = request_from_raw(raw_single(&[0xff]), source);
        let nul_path = request_from_raw(raw_multi(b"bad\0path"), source);

        let cases = [
            (mismatched, DhtTorrentProjectionError::IdentityMismatch),
            (invalid_utf8, DhtTorrentProjectionError::InvalidNameUtf8),
            (
                nul_path,
                DhtTorrentProjectionError::PathComponentContainsNul {
                    file_index: 0,
                    component_index: 0,
                },
            ),
        ];
        for (request, expected) in cases {
            let plan = DhtTorrentPlanner::default().plan(&[request], &BTreeMap::new());
            assert_eq!(plan.counts.projection_failed, 1);
            assert_eq!(plan.projection_failures[0].error, expected);
            assert_eq!(plan.scrape_candidates.len(), 1);
            assert!(plan.transaction.torrents.is_empty());
        }
    }

    #[test]
    fn identity_invalid_v2_does_not_poison_later_valid_primary() {
        let rows = fixture_rows();
        let case = &rows[3]["input"]["cases"][0];
        let source = "[fe80::1%7]:6881".parse().unwrap();
        let valid = fixture_request(case, source);
        let mut invalid = valid.clone();
        invalid.info_hash = id("0101010101010101010101010101010101010101");

        let planner = DhtTorrentPlanner::default();
        assert!(planner
            .v2_lookup_keys(std::slice::from_ref(&invalid))
            .is_empty());
        assert_eq!(
            planner.v2_lookup_keys(&[invalid.clone(), valid.clone()]),
            vec![valid.meta_info.info_hash_v2().unwrap()]
        );

        let plan = planner.plan(&[invalid, valid.clone()], &BTreeMap::new());
        assert_eq!(
            plan.counts,
            DhtTorrentPlanCounts {
                input: 2,
                projected: 1,
                projection_failed: 1,
                ..DhtTorrentPlanCounts::default()
            }
        );
        assert!(plan.counts.conserves());
        assert_eq!(plan.counts.v2_dropped, 0);
        assert_eq!(plan.projection_failures.len(), 1);
        assert_eq!(
            plan.projection_failures[0].error,
            DhtTorrentProjectionError::IdentityMismatch
        );
        assert_eq!(plan.transaction.torrents.len(), 1);
        assert_eq!(plan.transaction.torrents[0].info_hash, valid.info_hash);
        assert_eq!(plan.scrape_candidates.len(), 2);
        assert_eq!(plan.scrape_candidates[0].source_node_addr, source);
        assert_eq!(plan.scrape_candidates[1].source_node_addr, source);
    }

    #[test]
    fn zero_threshold_accepts_multi_file_projection_without_retained_rows() {
        let request = request_from_raw(
            raw_multi(b"retained-at-positive-threshold.mkv"),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 6881)),
        );
        let config = DhtTorrentPlanConfig {
            save_files_threshold: 0,
            ..DhtTorrentPlanConfig::default()
        };
        let plan = DhtTorrentPlanner::new(config).plan(&[request], &BTreeMap::new());

        assert_eq!(plan.counts.projected, 1);
        assert!(plan.counts.conserves());
        assert_eq!(plan.transaction.torrents.len(), 1);
        assert_eq!(
            plan.transaction.torrents[0].files_status,
            FilesStatus::OverThreshold
        );
        assert_eq!(plan.transaction.torrents[0].files_count, Some(1));
        assert!(plan.transaction.files.is_empty());
        assert!(plan.transaction.file_summaries.is_empty());
        assert!(plan.transaction.torrents[0].files_data.is_none());
        assert!(plan.transaction.torrents[0].file_extensions.is_none());
        assert_eq!(plan.transaction.sources.len(), 1);
        assert_eq!(plan.classifier_groups.len(), 1);
    }

    #[test]
    fn save_pieces_keeps_empty_bytes_and_rejects_negative_piece_length() {
        let source = SocketAddr::from((Ipv4Addr::LOCALHOST, 6881));
        let config = DhtTorrentPlanConfig {
            save_pieces: true,
            ..DhtTorrentPlanConfig::default()
        };
        let empty = request_from_raw(raw_single_fields(b"empty-pieces", 0, &[]), source);
        let plan = DhtTorrentPlanner::new(config).plan(&[empty], &BTreeMap::new());
        assert_eq!(plan.counts.projected, 1);
        assert_eq!(plan.transaction.pieces.len(), 1);
        assert_eq!(plan.transaction.pieces[0].piece_length, 0);
        assert!(plan.transaction.pieces[0].pieces.is_empty());

        let negative =
            request_from_raw(raw_single_fields(b"negative-piece-length", -1, &[]), source);
        let plan =
            DhtTorrentPlanner::default().plan(std::slice::from_ref(&negative), &BTreeMap::new());
        assert_eq!(plan.counts.projected, 1);
        assert!(plan.transaction.pieces.is_empty());

        let plan = DhtTorrentPlanner::new(config).plan(&[negative], &BTreeMap::new());
        assert_eq!(plan.counts.projection_failed, 1);
        assert_eq!(
            plan.projection_failures[0].error,
            DhtTorrentProjectionError::InvalidPieceLength(-1)
        );
        assert_eq!(plan.scrape_candidates.len(), 1);
        assert!(plan.transaction.torrents.is_empty());
    }
}
