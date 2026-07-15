//! Bounded L3-candidate plus L1 exact-refine orchestration.
//!
//! The composer is a decorator: any failure before an authoritative refined
//! prefix exists declines the route so Lane G can execute its normal Lane-S
//! PostgreSQL query. Once chunked refinement has started, deadline and retained
//! memory caps serve the accumulated relevance-ordered prefix as an estimate;
//! they never turn a pathological path query into an unbounded PG fallback.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bitmagnet_model::{BlobError, BlobFile, InfoHash, Torrent};
use bitmagnet_proto::v1::{PathCandidatesRequest, SortBy, SuggestRequest};
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::time::{timeout_at, Instant};

use crate::api::SearchServe;
use crate::candidates::{CandidateSource, HealthGate};
use crate::config::ComposerConfig;
use crate::filters::{FileRow, FileRowSort, FileRowsResult, Filters, PathGroup};
use crate::metrics::{PathsearchMetrics, RouteResult};
use crate::pg::{
    empty_result, Aggregations, HydrateOptions, PgSearchBackend, QueryOptions, RefineMetadata,
    SearchRequest, SearchResult, SearchResultItem,
};
use crate::refine::{
    file_extension, files_for_refine_bounded, match_file, paginate, torrent_matches,
    BoundedRefineFiles, RefinePredicate,
};

/// L3 path candidate composer backed by Lane-S PostgreSQL hydration.
///
/// [`PgSearchBackend`] is the real Lane-S adapter seam. Candidate shaping
/// preserves structured query controls while removing inherited pagination.
/// The file-count limits are conservative contract bounds, not allocator
/// acceptance evidence; Rust heap sizing under production-shaped blobs remains
/// a required load-measurement gate.
pub struct Composer {
    candidates: Arc<dyn CandidateSource>,
    pg: Arc<dyn PgSearchBackend>,
    config: ComposerConfig,
    health: Option<HealthGate>,
    refine_slots: Arc<Semaphore>,
    metrics: Option<Arc<PathsearchMetrics>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefineCap {
    None,
    OversizedCandidate,
    RetainedFiles,
    DecodedBytes,
    RetainedBytes,
    Deadline,
}

struct RefineAccum<'a> {
    refined: &'a mut Vec<SearchResultItem>,
    retained_files: &'a mut u64,
    retained_bytes: &'a mut u64,
    chunk_decoded_bytes: &'a mut u64,
}

/// One exact-matching file retained by the shared C4 visitor.
///
/// The hydrated item is shared across all of its matching files while the
/// composer sorts or groups them. The raw blob and full decoded file list are
/// deliberately not retained here: the file row itself is the public file
/// payload, and the request-wide retained-file budget therefore bounds the
/// live C4 result set independently of matches per torrent.
struct MatchingFile {
    file: BlobFile,
    extension: String,
    torrent_content: Arc<SearchResultItem>,
}

struct TypeaheadBucket {
    text: String,
    first_seen: usize,
    torrents: HashSet<InfoHash>,
}

impl Composer {
    /// Creates a bounded composer. A missing health gate preserves the Go
    /// contract that trusts an empty L3 result; a wired false gate fails closed.
    #[must_use]
    pub fn new(
        candidates: Arc<dyn CandidateSource>,
        pg: Arc<dyn PgSearchBackend>,
        config: ComposerConfig,
        health: Option<HealthGate>,
    ) -> Self {
        let max_concurrent_refines = config
            .resolved_max_concurrent_refines()
            .clamp(1, Semaphore::MAX_PERMITS);
        Self {
            candidates,
            pg,
            config: config.normalized(),
            health,
            refine_slots: Arc::new(Semaphore::new(max_concurrent_refines)),
            metrics: None,
        }
    }

    /// Attaches the canonical C6 composer metrics facade.
    ///
    /// The constructor remains usable without metrics for focused tests and
    /// disabled composition roots. Production should pass the value returned by
    /// [`PathsearchMetrics::register`].
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<PathsearchMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn inc_route(&self, result: RouteResult) {
        if let Some(metrics) = &self.metrics {
            metrics.inc_route(result);
        }
    }

    fn record_refine_cap(&self, cap: RefineCap) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        match cap {
            RefineCap::RetainedFiles | RefineCap::DecodedBytes | RefineCap::RetainedBytes => {
                metrics.inc_refine_retained_capped();
            }
            RefineCap::Deadline => metrics.inc_refine_deadline_capped(),
            RefineCap::None | RefineCap::OversizedCandidate => {}
        }
    }

    fn bounded_files_for_refine(
        &self,
        torrent: &Torrent,
        chunk_decoded_bytes: &mut u64,
    ) -> Result<Option<BoundedRefineFiles>, RefineCap> {
        let remaining = self
            .config
            .refine_decoded_byte_budget
            .saturating_sub(*chunk_decoded_bytes);
        if remaining == 0 {
            return Err(RefineCap::DecodedBytes);
        }
        // MessagePack contains every decoded path/extension byte, so capping
        // raw output at half of the remaining allocation budget guarantees
        // raw + owned strings fit before the post-decode accounting check.
        let max_decompressed_bytes = self.config.max_refine_decompressed_bytes.min(remaining / 2);
        let max_decompressed_bytes = usize::try_from(max_decompressed_bytes).unwrap_or(usize::MAX);
        let max_owned_string_bytes = self.config.max_refine_decompressed_bytes.min(remaining);
        let max_owned_string_bytes = usize::try_from(max_owned_string_bytes).unwrap_or(usize::MAX);
        let max_files = usize::try_from(self.effective_file_cap()).unwrap_or(usize::MAX);

        match files_for_refine_bounded(
            torrent,
            max_decompressed_bytes,
            max_owned_string_bytes,
            max_files,
        ) {
            Ok(Some(decoded)) => {
                let decompressed_bytes =
                    u64::try_from(decoded.decompressed_bytes).unwrap_or(u64::MAX);
                let owned_string_bytes =
                    u64::try_from(decoded.owned_string_bytes).unwrap_or(u64::MAX);
                let decoded_allocation_bytes =
                    decompressed_bytes.saturating_add(owned_string_bytes);
                if owned_string_bytes > self.config.max_refine_decompressed_bytes
                    || chunk_decoded_bytes.saturating_add(decoded_allocation_bytes)
                        > self.config.refine_decoded_byte_budget
                {
                    tracing::warn!(
                        decompressed_bytes,
                        owned_string_bytes,
                        "pathsearch candidate reached the decoded allocation budget"
                    );
                    return Err(RefineCap::DecodedBytes);
                }
                *chunk_decoded_bytes = chunk_decoded_bytes.saturating_add(decoded_allocation_bytes);
                Ok(Some(decoded))
            }
            Ok(None) => Ok(None),
            Err(error @ BlobError::FileCountLimitExceeded { .. }) => {
                if let Some(metrics) = &self.metrics {
                    metrics.inc_refine_declined_oversized();
                }
                tracing::warn!(%error, "pathsearch candidate exceeded the decoded file-count ceiling");
                Err(RefineCap::OversizedCandidate)
            }
            Err(
                error @ (BlobError::DecompressedLimitExceeded { .. }
                | BlobError::OwnedStringLimitExceeded { .. }),
            ) => {
                tracing::warn!(%error, "pathsearch candidate reached a bounded decode ceiling");
                Err(RefineCap::DecodedBytes)
            }
            Err(error) => {
                tracing::warn!(%error, "pathsearch candidate blob decode failed");
                Err(RefineCap::None)
            }
        }
    }

    fn trust_empty(&self) -> bool {
        self.health.as_ref().is_none_or(|gate| gate())
    }

    fn empty_estimate() -> SearchResult {
        let mut result = empty_result();
        result.total_count_is_estimate = true;
        result
    }

    fn candidate_budget(&self, limit: u32, offset: u32) -> u32 {
        let need = u64::from(offset).saturating_add(u64::from(limit)).max(1);
        let budget = need.saturating_mul(u64::from(self.config.oversample_factor));
        budget
            .min(u64::from(self.config.max_candidates))
            .min(u64::from(self.config.max_decode_candidates))
            .try_into()
            .unwrap_or(u32::MAX)
    }

    fn effective_file_cap(&self) -> u32 {
        self.config
            .max_refine_files
            .min(self.config.refine_file_budget)
            .min(self.config.retained_file_budget)
    }

    fn route_deadline(&self, now: Instant) -> Instant {
        now.checked_add(self.config.route_timeout)
            .unwrap_or_else(|| {
                tracing::warn!(
                    ?self.config.route_timeout,
                    "pathsearch route timeout exceeds Instant range; using immediate safe deadline"
                );
                now
            })
    }

    fn slot_deadline(&self, now: Instant, route_deadline: Instant) -> Instant {
        if self.config.slot_wait.is_zero() {
            return route_deadline;
        }
        now.checked_add(self.config.slot_wait)
            .map_or(route_deadline, |deadline| route_deadline.min(deadline))
    }

    async fn candidate_ids(
        &self,
        filters: &Filters,
        limit: u32,
        offset: u32,
        sorts: Vec<SortBy>,
        deadline: Instant,
    ) -> Option<(Vec<InfoHash>, u64)> {
        let budget = self.candidate_budget(limit, offset);
        let request = PathCandidatesRequest {
            query: filters.query.clone(),
            limit: budget,
            oversample: 0,
            sort: sorts,
        };
        let response = match timeout_at(deadline, self.candidates.path_candidates(request)).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                tracing::warn!(%error, "pathsearch candidate call failed; falling back to PostgreSQL");
                self.inc_route(RouteResult::Error);
                return None;
            }
            Err(_) => {
                tracing::warn!(
                    "pathsearch candidate call reached route deadline; falling back to PostgreSQL"
                );
                self.inc_route(RouteResult::Error);
                return None;
            }
        };

        let ids = response
            .candidates
            .into_iter()
            .take(usize::try_from(budget).unwrap_or(usize::MAX))
            .filter_map(|candidate| match InfoHash::from_slice(&candidate.info_hash) {
                Ok(info_hash) => Some(info_hash),
                Err(error) => {
                    tracing::debug!(%error, "pathsearch returned malformed candidate info hash; skipping");
                    None
                }
            })
            .collect();

        Some((ids, response.candidate_total))
    }

    fn compressed_blob_cap(&self) -> u64 {
        self.config
            .max_refine_decompressed_bytes
            .min(self.config.refine_decoded_byte_budget)
    }

    fn file_count_of(&self, id: &InfoHash, metadata: &HashMap<InfoHash, RefineMetadata>) -> u64 {
        let cap = self.effective_file_cap();
        match metadata.get(id).and_then(|value| value.file_count) {
            Some(count) if count <= 0 => 0,
            Some(count) => u64::try_from(count).unwrap_or(u64::MAX).min(u64::from(cap)),
            None => u64::from(cap),
        }
    }

    fn decline_oversized(
        &self,
        ids: Vec<InfoHash>,
        metadata: &HashMap<InfoHash, RefineMetadata>,
    ) -> (Vec<InfoHash>, bool) {
        let file_cap = self.effective_file_cap();
        let compressed_cap = self.compressed_blob_cap();
        let mut byte_capped = false;
        let kept = ids
            .into_iter()
            .filter(|id| {
                let candidate = metadata.get(id).copied().unwrap_or_default();
                if let Some(count) = candidate
                    .file_count
                    .filter(|count| *count > i64::from(file_cap))
                {
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_refine_declined_oversized();
                    }
                    tracing::warn!(
                        info_hash = %id,
                        file_count = count,
                        cap = file_cap,
                        "pathsearch declining oversized candidate"
                    );
                    return false;
                }

                if let Some(bytes) = candidate
                    .compressed_bytes
                    .filter(|bytes| *bytes > compressed_cap)
                {
                    byte_capped = true;
                    tracing::warn!(
                        info_hash = %id,
                        compressed_bytes = bytes,
                        cap = compressed_cap,
                        "pathsearch declining candidate above the compressed blob ceiling"
                    );
                    return false;
                }

                true
            })
            .collect();
        (kept, byte_capped)
    }

    fn chunk_by_file_budget(
        &self,
        ids: Vec<InfoHash>,
        metadata: &HashMap<InfoHash, RefineMetadata>,
    ) -> Vec<Vec<InfoHash>> {
        let mut chunks = Vec::new();
        let mut current = Vec::new();
        let mut current_files = 0_u64;
        let mut current_compressed_bytes = 0_u64;
        let max_torrents = usize::try_from(self.config.max_chunk_torrents)
            .unwrap_or(usize::MAX)
            .max(1);
        let file_budget = u64::from(self.config.refine_file_budget);
        let compressed_budget = self.config.refine_decoded_byte_budget;

        for id in ids {
            let files = self.file_count_of(&id, metadata);
            let compressed_bytes = metadata
                .get(&id)
                .and_then(|value| value.compressed_bytes)
                .unwrap_or(0);
            if !current.is_empty()
                && (current.len() >= max_torrents
                    || current_files.saturating_add(files) > file_budget
                    || current_compressed_bytes.saturating_add(compressed_bytes)
                        > compressed_budget)
            {
                chunks.push(std::mem::take(&mut current));
                current_files = 0;
                current_compressed_bytes = 0;
            }
            current.push(id);
            current_files = current_files.saturating_add(files);
            current_compressed_bytes = current_compressed_bytes.saturating_add(compressed_bytes);
        }
        if !current.is_empty() {
            chunks.push(current);
        }
        chunks
    }

    async fn acquire_refine_slot(&self, deadline: Instant) -> Option<SemaphorePermit<'_>> {
        let slot_deadline = self.slot_deadline(Instant::now(), deadline);
        timeout_at(slot_deadline, self.refine_slots.acquire())
            .await
            .ok()
            .and_then(Result::ok)
    }

    fn ordered_chunk_items(
        &self,
        mut result: SearchResult,
        ids: &[InfoHash],
    ) -> Vec<SearchResultItem> {
        let mut ranks = HashMap::with_capacity(ids.len());
        for (rank, id) in ids.iter().copied().enumerate() {
            ranks.entry(id).or_insert(rank);
        }
        result
            .items
            .retain(|item| ranks.contains_key(&item.info_hash));
        result
            .items
            .sort_by_key(|item| ranks.get(&item.info_hash).copied().unwrap_or(usize::MAX));
        result.items
    }

    async fn hydrate_chunk(
        &self,
        request: &SearchRequest,
        ids: &[InfoHash],
        deadline: Instant,
    ) -> Result<Vec<SearchResultItem>, RefineCap> {
        let request = request.for_candidates(
            ids,
            HydrateOptions {
                files_data: true,
                max_files_data_bytes: Some(self.compressed_blob_cap()),
            },
        );
        match timeout_at(deadline, self.pg.torrent_content(request)).await {
            Ok(Ok(result)) => Ok(self.ordered_chunk_items(result, ids)),
            Ok(Err(error)) => {
                tracing::warn!(%error, "pathsearch candidate hydration failed; falling back to PostgreSQL");
                Err(RefineCap::None)
            }
            Err(_) => Err(RefineCap::Deadline),
        }
    }

    /// Runs the bounded candidate + chunked hydrate pipeline and retains every
    /// exact-matching file in L3 candidate order.
    ///
    /// This is the Rust port of Go's `visitMatchingFiles`. Collapse, file rows,
    /// and typeahead all share this path so none can bypass the candidate,
    /// per-torrent, chunk, retained, concurrency, or whole-route bounds.
    async fn collect_matching_files(
        &self,
        filters: &Filters,
        options: &QueryOptions,
        limit: u32,
        offset: u32,
        sorts: Vec<SortBy>,
        allow_byte_capped_prefix: bool,
    ) -> (Vec<MatchingFile>, u64, bool, RefineCap) {
        let predicate = filters.predicate();
        if predicate.is_empty_substr() || !self.eligible(&filters.query) {
            self.inc_route(RouteResult::Ineligible);
            return (Vec::new(), 0, false, RefineCap::None);
        }
        if !self.healthy() {
            self.inc_route(RouteResult::Fallback);
            return (Vec::new(), 0, false, RefineCap::None);
        }

        let deadline = self.route_deadline(Instant::now());
        let Some((ids, candidate_total)) = self
            .candidate_ids(filters, limit, offset, sorts, deadline)
            .await
        else {
            return (Vec::new(), 0, false, RefineCap::None);
        };
        if ids.is_empty() {
            return if self.trust_empty() {
                self.inc_route(RouteResult::Served);
                (Vec::new(), candidate_total, true, RefineCap::None)
            } else {
                self.inc_route(RouteResult::Fallback);
                (Vec::new(), 0, false, RefineCap::None)
            };
        }

        let metadata = match timeout_at(deadline, self.pg.refine_metadata(&ids)).await {
            Ok(Ok(metadata)) => metadata,
            Ok(Err(error)) => {
                tracing::warn!(%error, "pathsearch file-row count probe failed; falling back to PostgreSQL");
                self.inc_route(RouteResult::Error);
                return (Vec::new(), 0, false, RefineCap::None);
            }
            Err(_) => {
                self.inc_route(RouteResult::Error);
                return (Vec::new(), 0, false, RefineCap::None);
            }
        };
        let (kept, preflight_byte_capped) = self.decline_oversized(ids, &metadata);
        if kept.is_empty() {
            if preflight_byte_capped {
                self.record_refine_cap(RefineCap::DecodedBytes);
                if !allow_byte_capped_prefix {
                    self.inc_route(RouteResult::Fallback);
                    return (Vec::new(), 0, false, RefineCap::DecodedBytes);
                }
            }
            self.inc_route(RouteResult::Served);
            return (
                Vec::new(),
                candidate_total,
                true,
                if preflight_byte_capped {
                    RefineCap::DecodedBytes
                } else {
                    RefineCap::None
                },
            );
        }
        let chunks = self.chunk_by_file_budget(kept, &metadata);
        let _permit = match self.acquire_refine_slot(deadline).await {
            Some(permit) => permit,
            None => {
                if let Some(metrics) = &self.metrics {
                    metrics.inc_refine_shed();
                }
                tracing::warn!(
                    query = filters.query,
                    "pathsearch file-row refine slot unavailable; serving shed empty estimate"
                );
                self.inc_route(RouteResult::Served);
                return (Vec::new(), candidate_total, true, RefineCap::None);
            }
        };

        let retained_budget = u64::from(self.config.retained_file_budget);
        let retained_byte_budget = self.config.retained_byte_budget;
        let file_cap = u64::from(self.effective_file_cap());
        let mut matching = Vec::new();
        let mut retained_bytes = 0_u64;
        let mut cap = if preflight_byte_capped {
            RefineCap::DecodedBytes
        } else {
            RefineCap::None
        };

        'refine: for chunk in chunks {
            let mut chunk_decoded_bytes = 0_u64;
            if Instant::now() >= deadline {
                cap = RefineCap::Deadline;
                break;
            }
            let items = match self
                .hydrate_chunk(options.refine_request(), &chunk, deadline)
                .await
            {
                Ok(items) => items,
                Err(RefineCap::Deadline) => {
                    cap = RefineCap::Deadline;
                    break;
                }
                Err(_) => {
                    self.inc_route(RouteResult::Error);
                    return (Vec::new(), 0, false, RefineCap::None);
                }
            };

            for mut item in items {
                if Instant::now() >= deadline {
                    cap = RefineCap::Deadline;
                    break 'refine;
                }
                let decoded = match self
                    .bounded_files_for_refine(&item.torrent, &mut chunk_decoded_bytes)
                {
                    Ok(Some(decoded)) => decoded,
                    Err(RefineCap::OversizedCandidate) => continue,
                    Ok(None) | Err(RefineCap::None) => {
                        tracing::warn!(
                            info_hash = %item.info_hash,
                            "pathsearch file-row candidate files unobtainable; falling back to PostgreSQL"
                        );
                        self.inc_route(RouteResult::Fallback);
                        return (Vec::new(), 0, false, RefineCap::None);
                    }
                    Err(reason) => {
                        cap = reason;
                        break 'refine;
                    }
                };
                let files = decoded.files;
                let file_count = u64::try_from(files.len()).unwrap_or(u64::MAX);
                if file_count > file_cap {
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_refine_declined_oversized();
                    }
                    tracing::warn!(
                        info_hash = %item.info_hash,
                        file_count,
                        cap = file_cap,
                        "pathsearch declining file-row candidate whose decoded file count exceeds cap"
                    );
                    continue;
                }

                // The file row is the file payload. Drop the raw blob and do
                // not retain a second full decoded vector on every row.
                item.torrent.files_data = None;
                item.refine_files.clear();
                let item = Arc::new(item);
                for file in files {
                    if Instant::now() >= deadline {
                        cap = RefineCap::Deadline;
                        break 'refine;
                    }
                    if !match_file(&file, &predicate) {
                        continue;
                    }
                    let extension = file_extension(&file);
                    let file_bytes =
                        u64::try_from(file.owned_string_bytes().saturating_add(extension.len()))
                            .unwrap_or(u64::MAX);
                    if retained_bytes.saturating_add(file_bytes) > retained_byte_budget {
                        cap = RefineCap::RetainedBytes;
                        break 'refine;
                    }
                    matching.push(MatchingFile {
                        file,
                        extension,
                        torrent_content: Arc::clone(&item),
                    });
                    retained_bytes = retained_bytes.saturating_add(file_bytes);
                    if u64::try_from(matching.len()).unwrap_or(u64::MAX) >= retained_budget {
                        cap = RefineCap::RetainedFiles;
                        break 'refine;
                    }
                }
            }
        }

        self.record_refine_cap(cap);
        if cap != RefineCap::None {
            tracing::warn!(
                ?cap,
                matches = matching.len(),
                retained_bytes,
                "pathsearch serving bounded file-match prefix"
            );
        }
        if !allow_byte_capped_prefix
            && matches!(cap, RefineCap::DecodedBytes | RefineCap::RetainedBytes)
        {
            tracing::warn!(
                ?cap,
                "pathsearch route has no estimate signal for a byte-capped prefix; declining"
            );
            self.inc_route(RouteResult::Fallback);
            return (Vec::new(), 0, false, cap);
        }
        self.inc_route(RouteResult::Served);
        (matching, candidate_total, true, cap)
    }

    fn refine_chunk(
        &self,
        items: Vec<SearchResultItem>,
        predicate: &RefinePredicate,
        retain_refine_files: bool,
        deadline: Instant,
        accum: RefineAccum<'_>,
    ) -> Result<RefineCap, ()> {
        let file_cap = u64::from(self.effective_file_cap());
        for mut item in items {
            if Instant::now() >= deadline {
                return Ok(RefineCap::Deadline);
            }
            let decoded = match self
                .bounded_files_for_refine(&item.torrent, accum.chunk_decoded_bytes)
            {
                Ok(Some(decoded)) => decoded,
                Err(RefineCap::OversizedCandidate) => continue,
                Ok(None) | Err(RefineCap::None) => {
                    tracing::warn!(info_hash = %item.info_hash, "pathsearch candidate files unobtainable; falling back to PostgreSQL");
                    return Err(());
                }
                Err(reason) => return Ok(reason),
            };
            let file_owned_bytes = u64::try_from(decoded.owned_string_bytes).unwrap_or(u64::MAX);
            let files = decoded.files;
            let file_count = u64::try_from(files.len()).unwrap_or(u64::MAX);
            if file_count > file_cap {
                if let Some(metrics) = &self.metrics {
                    metrics.inc_refine_declined_oversized();
                }
                tracing::warn!(
                    info_hash = %item.info_hash,
                    file_count,
                    cap = file_cap,
                    "pathsearch declining candidate whose decoded file count exceeds cap"
                );
                continue;
            }
            if !torrent_matches(&files, predicate) {
                continue;
            }
            if retain_refine_files {
                if !accum.refined.is_empty()
                    && accum.retained_files.saturating_add(file_count)
                        > u64::from(self.config.retained_file_budget)
                {
                    return Ok(RefineCap::RetainedFiles);
                }
                if accum.retained_bytes.saturating_add(file_owned_bytes)
                    > self.config.retained_byte_budget
                {
                    return Ok(RefineCap::RetainedBytes);
                }
                *accum.retained_files = accum.retained_files.saturating_add(file_count);
                *accum.retained_bytes = accum.retained_bytes.saturating_add(file_owned_bytes);
                item.refine_files = files;
            } else {
                item.refine_files.clear();
            }
            item.torrent.files_data = None;
            accum.refined.push(item);
        }
        Ok(RefineCap::None)
    }

    async fn refined_aggregations(
        &self,
        request: &SearchRequest,
        refined: &[SearchResultItem],
        deadline: Instant,
    ) -> Aggregations {
        if refined.is_empty() || Instant::now() >= deadline {
            return Aggregations::new();
        }
        let ids: Vec<InfoHash> = refined.iter().map(|item| item.info_hash).collect();
        let mut request = request.for_candidates(
            &ids,
            HydrateOptions {
                files_data: false,
                max_files_data_bytes: None,
            },
        );
        // This call consumes only aggregations. An explicit zero limit makes
        // Lane S skip membership and scalar/content hydration while retaining
        // every facet query, matching Go's aggregation-only refine path.
        request.options.limit = Some(0);
        match timeout_at(deadline, self.pg.torrent_content(request)).await {
            Ok(Ok(result)) => result.aggregations,
            Ok(Err(error)) => {
                if let Some(metrics) = &self.metrics {
                    metrics.inc_refine_agg_error();
                }
                tracing::warn!(%error, "pathsearch refined-set aggregation failed; serving without facets");
                Aggregations::new()
            }
            Err(_) => Aggregations::new(),
        }
    }

    async fn compose_torrent_content(
        &self,
        filters: Filters,
        options: QueryOptions,
        limit: u32,
        offset: u32,
        sorts: Vec<SortBy>,
    ) -> (SearchResult, bool) {
        let predicate = filters.predicate();
        if predicate.is_empty_substr() || !self.eligible(&filters.query) {
            self.inc_route(RouteResult::Ineligible);
            return (empty_result(), false);
        }
        if !self.healthy() {
            self.inc_route(RouteResult::Fallback);
            return (empty_result(), false);
        }
        let deadline = self.route_deadline(Instant::now());
        let Some((ids, candidate_total)) = self
            .candidate_ids(&filters, limit, offset, sorts, deadline)
            .await
        else {
            return (empty_result(), false);
        };
        if ids.is_empty() {
            return if self.trust_empty() {
                self.inc_route(RouteResult::Served);
                (Self::empty_estimate(), true)
            } else {
                self.inc_route(RouteResult::Fallback);
                (empty_result(), false)
            };
        }
        let metadata = match timeout_at(deadline, self.pg.refine_metadata(&ids)).await {
            Ok(Ok(metadata)) => metadata,
            Ok(Err(error)) => {
                tracing::warn!(%error, "pathsearch file-count probe failed; falling back to PostgreSQL");
                self.inc_route(RouteResult::Error);
                return (empty_result(), false);
            }
            Err(_) => {
                self.inc_route(RouteResult::Error);
                return (empty_result(), false);
            }
        };
        let (kept, preflight_byte_capped) = self.decline_oversized(ids.clone(), &metadata);
        if kept.is_empty() {
            if preflight_byte_capped {
                self.record_refine_cap(RefineCap::DecodedBytes);
            }
            self.inc_route(RouteResult::Served);
            return (Self::empty_estimate(), true);
        }
        let chunks = self.chunk_by_file_budget(kept, &metadata);
        let _permit = match self.acquire_refine_slot(deadline).await {
            Some(permit) => permit,
            None => {
                if let Some(metrics) = &self.metrics {
                    metrics.inc_refine_shed();
                }
                self.inc_route(RouteResult::Served);
                tracing::warn!("pathsearch refine slot unavailable; serving shed empty estimate");
                return (Self::empty_estimate(), true);
            }
        };

        let mut refined = Vec::new();
        let mut retained_files = 0_u64;
        let mut retained_bytes = 0_u64;
        let mut cap = if preflight_byte_capped {
            RefineCap::DecodedBytes
        } else {
            RefineCap::None
        };
        for chunk in chunks {
            let mut chunk_decoded_bytes = 0_u64;
            if Instant::now() >= deadline {
                cap = RefineCap::Deadline;
                break;
            }
            let items = match self
                .hydrate_chunk(options.refine_request(), &chunk, deadline)
                .await
            {
                Ok(items) => items,
                Err(RefineCap::Deadline) => {
                    cap = RefineCap::Deadline;
                    break;
                }
                Err(_) => {
                    self.inc_route(RouteResult::Error);
                    return (empty_result(), false);
                }
            };
            match self.refine_chunk(
                items,
                &predicate,
                options.retain_refine_files,
                deadline,
                RefineAccum {
                    refined: &mut refined,
                    retained_files: &mut retained_files,
                    retained_bytes: &mut retained_bytes,
                    chunk_decoded_bytes: &mut chunk_decoded_bytes,
                },
            ) {
                Ok(RefineCap::None) => {}
                Ok(reason) => {
                    cap = reason;
                    break;
                }
                Err(()) => {
                    self.inc_route(RouteResult::Fallback);
                    return (empty_result(), false);
                }
            }
        }
        if cap != RefineCap::None {
            self.record_refine_cap(cap);
            tracing::warn!(
                ?cap,
                matches = refined.len(),
                retained_bytes,
                "pathsearch serving bounded refined prefix"
            );
        }

        let aggregations = self
            .refined_aggregations(&options.agg, &refined, deadline)
            .await;
        let refined_count = u64::try_from(refined.len()).unwrap_or(u64::MAX);
        let total_count = if candidate_total > u64::try_from(ids.len()).unwrap_or(u64::MAX)
            && candidate_total > refined_count
        {
            candidate_total
        } else {
            refined_count
        };
        let page = paginate(refined, u64::from(offset), u64::from(limit));
        let consumed =
            u64::from(offset).saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));
        self.inc_route(RouteResult::Served);
        (
            SearchResult {
                items: page,
                total_count,
                total_count_is_estimate: true,
                has_next_page: consumed < refined_count,
                aggregations,
            },
            true,
        )
    }
}

fn compare_matching_file(
    left: &MatchingFile,
    right: &MatchingFile,
    sort: &FileRowSort,
) -> Option<Ordering> {
    match sort.field.to_lowercase().as_str() {
        "size" => Some(left.file.size.cmp(&right.file.size)),
        "path" => Some(left.file.path.cmp(&right.file.path)),
        "extension" => Some(left.extension.cmp(&right.extension)),
        "index" => Some(left.file.index.cmp(&right.file.index)),
        "info_hash" | "infohash" => Some(
            left.torrent_content
                .info_hash
                .cmp(&right.torrent_content.info_hash),
        ),
        "last_seen" | "dht_last_seen_at" | "dhtlastseenat" => Some(
            left.torrent_content
                .dht_last_seen_at
                .cmp(&right.torrent_content.dht_last_seen_at),
        ),
        "seeders" => Some(
            left.torrent_content
                .seeders
                .cmp(&right.torrent_content.seeders),
        ),
        "published_at" => Some(
            left.torrent_content
                .published_at
                .cmp(&right.torrent_content.published_at),
        ),
        "updated_at" => Some(
            left.torrent_content
                .torrent_content_updated_at
                .cmp(&right.torrent_content.torrent_content_updated_at),
        ),
        _ => None,
    }
}

fn compare_matching_file_tie(left: &MatchingFile, right: &MatchingFile) -> Ordering {
    left.file
        .path
        .cmp(&right.file.path)
        .then_with(|| {
            left.torrent_content
                .info_hash
                .cmp(&right.torrent_content.info_hash)
        })
        .then_with(|| left.file.index.cmp(&right.file.index))
}

fn sort_matching_file_rows(rows: &mut [MatchingFile], sort_by: &[FileRowSort]) {
    let default_sort = FileRowSort {
        field: "size".to_owned(),
        descending: true,
    };
    let sorts = if sort_by.is_empty() {
        std::slice::from_ref(&default_sort)
    } else {
        sort_by
    };

    rows.sort_by(|left, right| {
        for sort in sorts {
            let Some(ordering) = compare_matching_file(left, right, sort) else {
                continue;
            };
            if ordering != Ordering::Equal {
                return if sort.descending {
                    ordering.reverse()
                } else {
                    ordering
                };
            }
        }
        compare_matching_file_tie(left, right)
    });
}

fn page_matching_file_rows(
    mut rows: Vec<MatchingFile>,
    offset: u32,
    limit: u32,
) -> (Vec<MatchingFile>, bool) {
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    if offset >= rows.len() {
        return (Vec::new(), false);
    }
    let mut page = rows.split_off(offset);
    if limit == 0 {
        return (page, false);
    }
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let has_next_page = page.len() > limit;
    if has_next_page {
        page.truncate(limit);
    }
    (page, has_next_page)
}

fn next_path_segment(prefix: &str, path: &str) -> Option<String> {
    let prefix = prefix.trim();
    if prefix.is_empty() || path.is_empty() {
        return None;
    }
    let lower_path = path.to_lowercase();
    let lower_prefix = prefix.to_lowercase();
    let start = lower_path.find(&lower_prefix)?;

    let mut segment_start = start;
    if let Some(slash) = prefix.rfind('/') {
        segment_start = start.checked_add(slash)?.checked_add(1)?;
    } else if let Some(slash) = path.get(..start)?.rfind('/') {
        segment_start = slash + 1;
    }
    let after_prefix = start.checked_add(prefix.len())?;
    if prefix.ends_with('/') {
        segment_start = after_prefix;
    }
    let suffix = path.get(after_prefix..)?;
    let segment_end = suffix
        .find('/')
        .map_or(path.len(), |slash| after_prefix + slash);
    if segment_end < segment_start {
        return None;
    }
    let segment = path.get(segment_start..segment_end)?;
    (!segment.is_empty()).then(|| segment.to_owned())
}

#[async_trait::async_trait]
impl SearchServe for Composer {
    async fn torrent_content(
        &self,
        filters: Filters,
        options: QueryOptions,
        limit: u32,
        offset: u32,
        sorts: Vec<SortBy>,
    ) -> crate::Result<(SearchResult, bool)> {
        Ok(self
            .compose_torrent_content(filters, options, limit, offset, sorts)
            .await)
    }

    async fn collapse_paths(
        &self,
        filters: Filters,
        options: QueryOptions,
        limit: u32,
        offset: u32,
        sorts: Vec<SortBy>,
    ) -> crate::Result<(Vec<PathGroup>, bool)> {
        let (matching, _, served, _) = self
            .collect_matching_files(&filters, &options, limit, offset, sorts, false)
            .await;
        if !served {
            return Ok((Vec::new(), false));
        }

        let mut groups = Vec::<PathGroup>::new();
        let mut group_index = HashMap::<String, usize>::new();
        let mut seen_torrent_path = HashSet::<(String, InfoHash)>::new();
        for row in matching {
            let info_hash = row.torrent_content.info_hash;
            let path = row.file.path;
            if !seen_torrent_path.insert((path.clone(), info_hash)) {
                continue;
            }
            let index = if let Some(index) = group_index.get(&path).copied() {
                index
            } else {
                let index = groups.len();
                group_index.insert(path.clone(), index);
                groups.push(PathGroup {
                    path,
                    info_hashes: Vec::new(),
                });
                index
            };
            groups[index].info_hashes.push(info_hash);
        }

        Ok((paginate(groups, u64::from(offset), u64::from(limit)), true))
    }

    async fn search_file_rows(
        &self,
        filters: Filters,
        options: QueryOptions,
        limit: u32,
        offset: u32,
        sort_by: Vec<FileRowSort>,
    ) -> crate::Result<(FileRowsResult, bool)> {
        let (mut matching, candidate_total, served, _) = self
            .collect_matching_files(
                &filters,
                &options,
                limit.saturating_add(1),
                offset,
                Vec::new(),
                true,
            )
            .await;
        if !served {
            return Ok((FileRowsResult::default(), false));
        }

        sort_matching_file_rows(&mut matching, &sort_by);
        let (page, has_next_page) = page_matching_file_rows(matching, offset, limit);
        let rows = page
            .into_iter()
            .map(|row| FileRow {
                info_hash: row.torrent_content.info_hash,
                index: row.file.index,
                path: row.file.path,
                extension: row.extension,
                size: row.file.size,
                torrent_content: (*row.torrent_content).clone(),
            })
            .collect();

        Ok((
            FileRowsResult {
                rows,
                total_count: candidate_total,
                total_count_is_estimate: true,
                has_next_page,
            },
            true,
        ))
    }

    async fn path_typeahead(
        &self,
        prefix: String,
        options: QueryOptions,
        limit: u32,
    ) -> crate::Result<(Vec<String>, bool)> {
        let filters = Filters {
            query: prefix.clone(),
            ..Filters::default()
        };
        let (matching, _, served, _) = self
            .collect_matching_files(&filters, &options, limit, 0, Vec::new(), false)
            .await;
        if !served {
            return Ok((Vec::new(), false));
        }

        let mut buckets = Vec::<TypeaheadBucket>::new();
        let mut bucket_index = HashMap::<String, usize>::new();
        for row in matching {
            let Some(segment) = next_path_segment(&prefix, &row.file.path) else {
                continue;
            };
            let key = segment.to_lowercase();
            let index = if let Some(index) = bucket_index.get(&key).copied() {
                index
            } else {
                let index = buckets.len();
                bucket_index.insert(key, index);
                buckets.push(TypeaheadBucket {
                    text: segment,
                    first_seen: index,
                    torrents: HashSet::new(),
                });
                index
            };
            buckets[index]
                .torrents
                .insert(row.torrent_content.info_hash);
        }
        buckets.sort_by(|left, right| {
            right
                .torrents
                .len()
                .cmp(&left.torrents.len())
                .then_with(|| left.first_seen.cmp(&right.first_seen))
                .then_with(|| left.text.cmp(&right.text))
        });
        if limit > 0 {
            buckets.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        }

        Ok((
            buckets.into_iter().map(|bucket| bucket.text).collect(),
            true,
        ))
    }

    async fn suggest(&self, prefix: String, limit: u32) -> crate::Result<(Vec<String>, bool)> {
        if !self.healthy() {
            return Ok((Vec::new(), false));
        }
        match self
            .candidates
            .suggest(SuggestRequest { prefix, limit })
            .await
        {
            Ok(response) => Ok((
                response
                    .suggestions
                    .into_iter()
                    .map(|suggestion| suggestion.value)
                    .collect(),
                true,
            )),
            Err(error) => {
                tracing::warn!(%error, "pathsearch suggest failed; declining route");
                Ok((Vec::new(), false))
            }
        }
    }

    fn eligible(&self, query: &str) -> bool {
        query.trim().len() >= usize::try_from(self.config.min_query_length).unwrap_or(usize::MAX)
    }

    fn healthy(&self) -> bool {
        self.health.as_ref().is_none_or(|gate| gate())
    }

    fn typeahead_enabled(&self) -> bool {
        self.config.typeahead_enabled
    }

    fn file_search_route_text_enabled(&self) -> bool {
        self.config.file_search_route_text
    }

    fn collapse_enabled(&self) -> bool {
        self.config.collapse_enabled
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    use bitmagnet_model::{serialize_files, BlobFile, FilesStatus, Torrent};
    use bitmagnet_proto::v1::{
        PathCandidate, PathCandidatesResponse, PathSearchHealth, SuggestResponse,
    };

    use super::*;
    use bitmagnet_search_query::{
        AggregationGroup, AggregationItem, FacetLogic, FacetRequest, TorrentContentFacet,
    };

    use crate::pg::{Criteria, SearchOptions};

    struct FakeCandidates {
        response: PathCandidatesResponse,
        fail: bool,
        delay: Duration,
        requested_limit: AtomicU32,
    }

    impl FakeCandidates {
        fn returning(ids: &[InfoHash], candidate_total: u64) -> Self {
            Self {
                response: PathCandidatesResponse {
                    candidates: ids
                        .iter()
                        .map(|id| PathCandidate {
                            info_hash: id.as_slice().to_vec(),
                            ..PathCandidate::default()
                        })
                        .collect(),
                    candidate_total,
                    estimated: true,
                },
                fail: false,
                delay: Duration::ZERO,
                requested_limit: AtomicU32::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl CandidateSource for FakeCandidates {
        async fn path_candidates(
            &self,
            request: PathCandidatesRequest,
        ) -> crate::Result<PathCandidatesResponse> {
            self.requested_limit.store(request.limit, Ordering::Relaxed);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            if self.fail {
                Err(crate::Error::Candidate("candidate failure".into()))
            } else {
                Ok(self.response.clone())
            }
        }

        async fn suggest(&self, _request: SuggestRequest) -> crate::Result<SuggestResponse> {
            Ok(SuggestResponse::default())
        }

        async fn health_check(&self) -> crate::Result<PathSearchHealth> {
            Ok(PathSearchHealth::default())
        }
    }

    struct FakePg {
        items: Vec<SearchResultItem>,
        counts: HashMap<InfoHash, i64>,
        calls: Mutex<Vec<SearchRequest>>,
        delayed_ids: HashSet<InfoHash>,
        delay: Duration,
        fail_counts: bool,
        fail_search: bool,
        fail_on_search_call: Option<usize>,
    }

    impl FakePg {
        fn new(items: Vec<SearchResultItem>, counts: HashMap<InfoHash, i64>) -> Self {
            Self {
                items,
                counts,
                calls: Mutex::new(Vec::new()),
                delayed_ids: HashSet::new(),
                delay: Duration::ZERO,
                fail_counts: false,
                fail_search: false,
                fail_on_search_call: None,
            }
        }

        fn calls(&self) -> Vec<SearchRequest> {
            self.calls.lock().expect("calls mutex poisoned").clone()
        }
    }

    #[async_trait::async_trait]
    impl PgSearchBackend for FakePg {
        async fn torrent_content(&self, request: SearchRequest) -> crate::Result<SearchResult> {
            let search_call = {
                let mut calls = self.calls.lock().expect("calls mutex poisoned");
                calls.push(request.clone());
                calls.len()
            };
            if self.fail_search || self.fail_on_search_call == Some(search_call) {
                return Err(crate::Error::Pg("search failure".into()));
            }
            let candidate_ids = candidate_ids(&request.options.filter);
            if request.hydrate.files_data
                && candidate_ids.iter().any(|id| self.delayed_ids.contains(id))
            {
                tokio::time::sleep(self.delay).await;
            }

            // Deliberately reverse the PG-natural row order: the composer must
            // restore the L3 relevance order before exact refine.
            let selected: HashSet<InfoHash> = candidate_ids.iter().copied().collect();
            let mut items: Vec<_> = self
                .items
                .iter()
                .rev()
                .filter(|item| selected.contains(&item.info_hash))
                .cloned()
                .collect();
            if !request.hydrate.files_data {
                for item in &mut items {
                    item.torrent.files_data = None;
                }
            } else if let Some(limit) = request.hydrate.max_files_data_bytes {
                for item in &mut items {
                    if item
                        .torrent
                        .files_data
                        .as_ref()
                        .is_some_and(|blob| u64::try_from(blob.len()).unwrap_or(u64::MAX) > limit)
                    {
                        item.torrent.files_data = None;
                    }
                }
            }
            let aggregations = if request.options.facets.iter().any(|facet| facet.aggregate) {
                Aggregations::from([(
                    "content_type".into(),
                    AggregationGroup {
                        label: "Content type".into(),
                        logic: FacetLogic::Or,
                        items: BTreeMap::from([(
                            "movie".into(),
                            AggregationItem {
                                label: "Movie".into(),
                                count: u64::try_from(items.len()).unwrap_or(u64::MAX),
                                is_estimate: false,
                            },
                        )]),
                    },
                )])
            } else {
                Aggregations::new()
            };
            Ok(SearchResult {
                items,
                total_count: 0,
                total_count_is_estimate: false,
                has_next_page: false,
                aggregations,
            })
        }

        async fn refine_metadata(
            &self,
            ids: &[InfoHash],
        ) -> crate::Result<HashMap<InfoHash, RefineMetadata>> {
            if self.fail_counts {
                return Err(crate::Error::Pg("file-count failure".into()));
            }
            let mut metadata = HashMap::new();
            for id in ids {
                let file_count = self.counts.get(id).copied();
                let compressed_bytes = self
                    .items
                    .iter()
                    .find(|item| item.info_hash == *id)
                    .and_then(|item| item.torrent.files_data.as_ref())
                    .map(|blob| u64::try_from(blob.len()).unwrap_or(u64::MAX));
                if file_count.is_some() || compressed_bytes.is_some() {
                    metadata.insert(
                        *id,
                        RefineMetadata {
                            file_count,
                            compressed_bytes,
                        },
                    );
                }
            }
            Ok(metadata)
        }
    }

    fn id(byte: u8) -> InfoHash {
        InfoHash::new([byte; bitmagnet_model::INFO_HASH_LEN])
    }

    fn candidate_ids(filter: &Option<Criteria>) -> Vec<InfoHash> {
        fn visit(criteria: &Criteria, out: &mut Vec<InfoHash>) {
            match criteria {
                Criteria::TorrentContentInfoHashIn(ids) => out.extend(ids.iter().copied()),
                Criteria::And(children) | Criteria::Or(children) => {
                    for child in children {
                        visit(child, out);
                    }
                }
                Criteria::Not(child) => visit(child, out),
                _ => {}
            }
        }

        let mut ids = Vec::new();
        if let Some(filter) = filter {
            visit(filter, &mut ids);
        }
        ids
    }

    fn files(count: usize, matches: bool) -> Vec<BlobFile> {
        (0..count)
            .map(|index| BlobFile {
                index: u32::try_from(index).unwrap_or(u32::MAX),
                path: if matches {
                    format!("inception/part-{index}.mkv")
                } else {
                    format!("interstellar/part-{index}.mkv")
                },
                extension: "mkv".into(),
                size: 100,
            })
            .collect()
    }

    fn item(byte: u8, file_count: usize, matches: bool) -> SearchResultItem {
        let info_hash = id(byte);
        let mut item = SearchResultItem::for_test(info_hash, format!("torrent-{byte}"), 100);
        item.torrent = Torrent {
            info_hash,
            name: format!("torrent-{byte}"),
            size: 100,
            private: false,
            files_status: FilesStatus::Multi,
            extension: None,
            files_count: u32::try_from(file_count).ok(),
            files_data: Some(serialize_files(&files(file_count, matches)).unwrap()),
            file_extensions: vec!["mkv".into()],
        };
        item.title = format!("preserved-title-{byte}");
        item
    }

    fn file(index: u32, path: &str, extension: &str, size: u64) -> BlobFile {
        BlobFile {
            index,
            path: path.to_owned(),
            extension: extension.to_owned(),
            size,
        }
    }

    fn item_with_files(byte: u8, files: Vec<BlobFile>) -> SearchResultItem {
        let mut result = item(byte, files.len(), true);
        result.torrent.files_data = Some(serialize_files(&files).unwrap());
        result
    }

    fn decompressed_blob_len(item: &SearchResultItem) -> u64 {
        u64::try_from(
            bitmagnet_model::deserialize_files_bounded(
                item.torrent.files_data.as_deref().unwrap(),
                usize::MAX,
                usize::MAX,
            )
            .unwrap()
            .decompressed_bytes,
        )
        .unwrap()
    }

    fn query_options() -> QueryOptions {
        let aggregate_facet = FacetRequest {
            facet: TorrentContentFacet::ContentType,
            aggregate: true,
            logic: None,
            filter: Default::default(),
        };
        QueryOptions {
            combined: SearchRequest::new(
                SearchOptions::default().with_facets([aggregate_facet.clone()]),
                HydrateOptions {
                    files_data: true,
                    max_files_data_bytes: None,
                },
            ),
            refine: Some(SearchRequest::new(
                SearchOptions::default(),
                HydrateOptions {
                    files_data: true,
                    max_files_data_bytes: None,
                },
            )),
            agg: SearchRequest::new(
                SearchOptions::default().with_facets([aggregate_facet]),
                HydrateOptions::default(),
            ),
            retain_refine_files: true,
        }
    }

    fn config() -> ComposerConfig {
        ComposerConfig {
            max_concurrent_refines: 1,
            ..ComposerConfig::default()
        }
    }

    async fn search(composer: &Composer, limit: u32, offset: u32) -> (SearchResult, bool) {
        composer
            .torrent_content(
                Filters {
                    query: "inception".into(),
                    ..Filters::default()
                },
                query_options(),
                limit,
                offset,
                Vec::new(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn restores_l3_order_refines_then_paginates_and_reaggregates_without_decode() {
        let candidate_source = Arc::new(FakeCandidates::returning(&[id(3), id(1), id(2)], 3));
        let pg = Arc::new(FakePg::new(
            vec![item(1, 1, false), item(2, 1, true), item(3, 1, true)],
            HashMap::from([(id(1), 1), (id(2), 1), (id(3), 1)]),
        ));
        let composer = Composer::new(candidate_source, pg.clone(), config(), None);

        let (result, served) = search(&composer, 1, 0).await;

        assert!(served);
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].info_hash, id(3));
        assert_eq!(result.items[0].title, "preserved-title-3");
        assert!(result.items[0].torrent.files_data.is_none());
        assert_eq!(result.items[0].refine_files.len(), 1);
        assert_eq!(result.items[0].refine_files[0].path, "inception/part-0.mkv");
        assert_eq!(result.total_count, 2);
        assert!(result.total_count_is_estimate);
        assert!(result.has_next_page);
        assert_eq!(result.aggregations["content_type"].items["movie"].count, 2);
        let calls = pg.calls();
        assert_eq!(calls.len(), 2);
        assert!(calls[0].hydrate.files_data);
        assert_eq!(
            calls[0].hydrate.max_files_data_bytes,
            Some(composer.config.max_refine_decompressed_bytes)
        );
        assert!(
            calls[0].options.facets.is_empty(),
            "even a one-chunk route must use refine, not candidate-set combined facets"
        );
        assert!(!calls[1].hydrate.files_data);
        assert!(calls[1].options.facets[0].aggregate);
        assert_eq!(calls[1].options.limit, Some(0));
        assert_eq!(candidate_ids(&calls[1].options.filter), vec![id(3), id(2)]);
    }

    #[test]
    fn refine_retains_decoded_files_only_for_matches_and_clears_raw_blob() {
        let candidates = Arc::new(FakeCandidates::returning(&[], 0));
        let pg = Arc::new(FakePg::new(Vec::new(), HashMap::new()));
        let composer = Composer::new(candidates, pg, config(), None);
        let predicate = Filters {
            query: "inception".into(),
            ..Filters::default()
        }
        .predicate();
        let mut refined = Vec::new();
        let mut retained_files = 0;
        let mut retained_bytes = 0;
        let mut chunk_decoded_bytes = 0;
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(1))
            .expect("small deadline must fit");

        let cap = composer
            .refine_chunk(
                vec![item(1, 2, false), item(2, 3, true)],
                &predicate,
                true,
                deadline,
                RefineAccum {
                    refined: &mut refined,
                    retained_files: &mut retained_files,
                    retained_bytes: &mut retained_bytes,
                    chunk_decoded_bytes: &mut chunk_decoded_bytes,
                },
            )
            .unwrap();

        assert_eq!(cap, RefineCap::None);
        assert_eq!(retained_files, 3);
        assert!(retained_bytes > 0);
        assert!(chunk_decoded_bytes > retained_bytes);
        assert_eq!(refined.len(), 1, "the nonmatch must not be retained");
        assert_eq!(refined[0].info_hash, id(2));
        assert!(refined[0].torrent.files_data.is_none());
        assert_eq!(refined[0].refine_files.len(), 3);
    }

    #[tokio::test]
    async fn candidate_window_is_hard_capped_and_uses_sidecar_total_when_truncated() {
        let candidate_source = Arc::new(FakeCandidates::returning(
            &[id(1), id(2), id(3), id(4)],
            10_000,
        ));
        let pg = Arc::new(FakePg::new(
            vec![item(1, 1, true), item(2, 1, true), item(3, 1, true)],
            HashMap::from([(id(1), 1), (id(2), 1), (id(3), 1)]),
        ));
        let mut cfg = config();
        cfg.max_candidates = 3;
        cfg.max_decode_candidates = 2;
        let composer = Composer::new(candidate_source.clone(), pg, cfg, None);

        let (result, served) = search(&composer, u32::MAX, u32::MAX).await;

        assert!(served);
        assert_eq!(candidate_source.requested_limit.load(Ordering::Relaxed), 2);
        assert_eq!(result.total_count, 10_000);
    }

    #[tokio::test]
    async fn declines_oversized_before_decode_and_chunks_within_both_bounds() {
        let candidate_source = Arc::new(FakeCandidates::returning(&[id(1), id(2), id(3)], 3));
        let pg = Arc::new(FakePg::new(
            vec![item(1, 4, true), item(2, 2, true), item(3, 2, true)],
            HashMap::from([(id(1), 4), (id(2), 2), (id(3), 2)]),
        ));
        let mut cfg = config();
        cfg.max_refine_files = 3;
        cfg.refine_file_budget = 2;
        cfg.max_chunk_torrents = 1;
        let metrics = Arc::new(PathsearchMetrics::new());
        let composer = Composer::new(candidate_source, pg.clone(), cfg, None)
            .with_metrics(Arc::clone(&metrics));

        let (result, served) = search(&composer, 10, 0).await;

        assert!(served);
        assert_eq!(
            result
                .items
                .iter()
                .map(|item| item.info_hash)
                .collect::<Vec<_>>(),
            vec![id(2), id(3)]
        );
        let hydrate_calls: Vec<_> = pg
            .calls()
            .into_iter()
            .filter(|call| call.hydrate.files_data)
            .collect();
        assert_eq!(hydrate_calls.len(), 2);
        assert!(hydrate_calls
            .iter()
            .all(|call| candidate_ids(&call.options.filter).len() == 1));
        assert!(hydrate_calls
            .iter()
            .all(|call| !candidate_ids(&call.options.filter).contains(&id(1))));
        assert_eq!(metrics.refine_declined_count(), 1);
        assert_eq!(metrics.route_count(RouteResult::Served), 1);
    }

    #[tokio::test]
    async fn retained_file_budget_serves_top_relevance_prefix() {
        let candidate_source = Arc::new(FakeCandidates::returning(&[id(1), id(2)], 2));
        let pg = Arc::new(FakePg::new(
            vec![item(1, 2, true), item(2, 2, true)],
            HashMap::from([(id(1), 2), (id(2), 2)]),
        ));
        let mut cfg = config();
        cfg.refine_file_budget = 2;
        cfg.retained_file_budget = 2;
        let metrics = Arc::new(PathsearchMetrics::new());
        let composer = Composer::new(candidate_source, pg.clone(), cfg, None)
            .with_metrics(Arc::clone(&metrics));

        let (result, served) = search(&composer, 10, 0).await;

        assert!(served);
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].info_hash, id(1));
        let calls = pg.calls();
        assert_eq!(
            candidate_ids(&calls.last().unwrap().options.filter),
            vec![id(1)]
        );
        assert!(!calls.last().unwrap().hydrate.files_data);
        assert_eq!(metrics.retained_capped_count(), 1);
        assert_eq!(metrics.route_count(RouteResult::Served), 1);
    }

    #[tokio::test]
    async fn unselected_files_are_not_retained_or_charged_to_retained_budgets() {
        let candidate_source = Arc::new(FakeCandidates::returning(&[id(1), id(2)], 2));
        let pg = Arc::new(FakePg::new(
            vec![item(1, 1, true), item(2, 1, true)],
            HashMap::from([(id(1), 1), (id(2), 1)]),
        ));
        let mut cfg = config();
        cfg.retained_file_budget = 1;
        cfg.retained_byte_budget = 1;
        let composer = Composer::new(candidate_source, pg, cfg, None);
        let mut options = query_options();
        options.retain_refine_files = false;

        let (result, served) = composer
            .torrent_content(
                Filters {
                    query: "inception".to_owned(),
                    ..Filters::default()
                },
                options,
                10,
                0,
                Vec::new(),
            )
            .await
            .unwrap();

        assert!(served);
        assert_eq!(result.items.len(), 2);
        assert!(result.items.iter().all(|item| item.refine_files.is_empty()));
        assert!(result
            .items
            .iter()
            .all(|item| item.torrent.files_data.is_none()));
    }

    #[tokio::test]
    async fn retained_byte_budget_accepts_exact_boundary_and_caps_plus_one() {
        let files = vec![file(0, "inception/movie.mkv", "mkv", 1)];
        let retained_bytes = u64::try_from(files[0].owned_string_bytes()).unwrap();

        for (budget, expected_items, expected_caps) in [
            (retained_bytes, 1, 0),
            (retained_bytes.saturating_sub(1), 0, 1),
        ] {
            let candidates = Arc::new(FakeCandidates::returning(&[id(1)], 1));
            let pg = Arc::new(FakePg::new(
                vec![item_with_files(1, files.clone())],
                HashMap::from([(id(1), 1)]),
            ));
            let metrics = Arc::new(PathsearchMetrics::new());
            let mut cfg = config();
            cfg.retained_byte_budget = budget;
            let composer =
                Composer::new(candidates, pg, cfg, None).with_metrics(Arc::clone(&metrics));

            let (result, served) = search(&composer, 10, 0).await;

            assert!(served);
            assert_eq!(result.items.len(), expected_items);
            assert_eq!(metrics.retained_capped_count(), expected_caps);
        }
    }

    #[tokio::test]
    async fn decoded_byte_budget_charges_nonmatches_before_later_candidates() {
        let first_path = "x".repeat(128);
        let second_path = format!("inception{}", "y".repeat(119));
        let first = item_with_files(1, vec![file(0, &first_path, "mkv", 1)]);
        let second = item_with_files(2, vec![file(0, &second_path, "mkv", 1)]);
        let raw_len = decompressed_blob_len(&first);
        assert_eq!(raw_len, decompressed_blob_len(&second));

        for (budget, expected_items, expected_caps) in [(raw_len * 2, 0, 1), (raw_len * 4, 1, 0)] {
            let candidates = Arc::new(FakeCandidates::returning(&[id(1), id(2)], 2));
            let pg = Arc::new(FakePg::new(
                vec![first.clone(), second.clone()],
                HashMap::from([(id(1), 1), (id(2), 1)]),
            ));
            let metrics = Arc::new(PathsearchMetrics::new());
            let mut cfg = config();
            cfg.max_refine_decompressed_bytes = raw_len;
            cfg.refine_decoded_byte_budget = budget;
            let composer =
                Composer::new(candidates, pg, cfg, None).with_metrics(Arc::clone(&metrics));

            let (result, served) = search(&composer, 10, 0).await;

            assert!(served);
            assert_eq!(result.items.len(), expected_items);
            assert_eq!(metrics.retained_capped_count(), expected_caps);
        }
    }

    #[tokio::test]
    async fn decompression_ceiling_bounds_a_high_expansion_candidate() {
        let files = vec![file(
            0,
            &format!("inception/{}", "x".repeat(8_192)),
            "mkv",
            1,
        )];
        let candidates = Arc::new(FakeCandidates::returning(&[id(1)], 1));
        let pg = Arc::new(FakePg::new(
            vec![item_with_files(1, files)],
            HashMap::from([(id(1), 1)]),
        ));
        let metrics = Arc::new(PathsearchMetrics::new());
        let mut cfg = config();
        cfg.max_refine_decompressed_bytes = 128;
        cfg.refine_decoded_byte_budget = 1_024;
        let composer = Composer::new(candidates, pg, cfg, None).with_metrics(Arc::clone(&metrics));

        let (result, served) = search(&composer, 10, 0).await;

        assert!(served);
        assert!(result.items.is_empty());
        assert!(result.total_count_is_estimate);
        assert_eq!(metrics.retained_capped_count(), 1);
    }

    #[tokio::test]
    async fn compressed_blob_ceiling_declines_before_sqlx_hydration() {
        let mut oversized = item(1, 1, true);
        oversized.torrent.files_data = Some(vec![0_u8; 129]);
        let candidates = Arc::new(FakeCandidates::returning(&[id(1)], 1));
        let pg = Arc::new(FakePg::new(vec![oversized], HashMap::from([(id(1), 1)])));
        let metrics = Arc::new(PathsearchMetrics::new());
        let mut cfg = config();
        cfg.max_refine_decompressed_bytes = 128;
        cfg.refine_decoded_byte_budget = 1_024;
        let backend: Arc<dyn PgSearchBackend> = pg.clone();
        let composer =
            Composer::new(candidates, backend, cfg, None).with_metrics(Arc::clone(&metrics));

        let (result, served) = search(&composer, 10, 0).await;

        assert!(served);
        assert!(result.items.is_empty());
        assert!(result.total_count_is_estimate);
        assert!(
            pg.calls().is_empty(),
            "oversized bytea must not be selected"
        );
        assert_eq!(metrics.retained_capped_count(), 1);
    }

    #[tokio::test]
    async fn route_deadline_serves_accumulated_prefix_not_pg_fallback() {
        let candidate_source = Arc::new(FakeCandidates::returning(&[id(1), id(2)], 2));
        let mut fake_pg = FakePg::new(
            vec![item(1, 1, true), item(2, 1, true)],
            HashMap::from([(id(1), 1), (id(2), 1)]),
        );
        fake_pg.delayed_ids.insert(id(2));
        fake_pg.delay = Duration::from_millis(100);
        let pg = Arc::new(fake_pg);
        let mut cfg = config();
        cfg.max_chunk_torrents = 1;
        cfg.route_timeout = Duration::from_millis(20);
        let metrics = Arc::new(PathsearchMetrics::new());
        let composer =
            Composer::new(candidate_source, pg, cfg, None).with_metrics(Arc::clone(&metrics));

        let (result, served) = search(&composer, 10, 0).await;

        assert!(served);
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].info_hash, id(1));
        assert!(result.total_count_is_estimate);
        assert_eq!(metrics.deadline_capped_count(), 1);
        assert_eq!(metrics.route_count(RouteResult::Served), 1);
    }

    #[tokio::test]
    async fn saturated_refine_slot_sheds_multi_and_single_chunk_routes() {
        let candidates = Arc::new(FakeCandidates::returning(&[id(1), id(2)], 2));
        let pg = Arc::new(FakePg::new(
            vec![item(1, 1, true), item(2, 1, true)],
            HashMap::from([(id(1), 1), (id(2), 1)]),
        ));
        let mut cfg = config();
        cfg.max_chunk_torrents = 1;
        cfg.slot_wait = Duration::from_millis(1);
        let metrics = Arc::new(PathsearchMetrics::new());
        let composer = Composer::new(candidates, pg, cfg, None).with_metrics(Arc::clone(&metrics));
        let held = composer.refine_slots.acquire().await.unwrap();

        let (shed, served) = search(&composer, 10, 0).await;

        assert!(served);
        assert!(shed.items.is_empty());
        assert_eq!(metrics.shed_count(), 1);
        assert_eq!(metrics.route_count(RouteResult::Served), 1);
        drop(held);

        let candidates = Arc::new(FakeCandidates::returning(&[id(1), id(2)], 2));
        let pg = Arc::new(FakePg::new(
            vec![item(1, 1, true), item(2, 1, true)],
            HashMap::from([(id(1), 1), (id(2), 1)]),
        ));
        let mut cfg = config();
        cfg.max_chunk_torrents = 2;
        cfg.slot_wait = Duration::from_millis(1);
        let composer = Composer::new(candidates, pg, cfg, None).with_metrics(Arc::clone(&metrics));
        let held = composer.refine_slots.acquire().await.unwrap();

        let (single_chunk_shed, served) = search(&composer, 10, 0).await;

        assert!(served);
        assert!(single_chunk_shed.items.is_empty());
        assert_eq!(metrics.shed_count(), 2);
        assert_eq!(metrics.route_count(RouteResult::Served), 2);
        drop(held);
    }

    #[tokio::test]
    async fn refined_aggregation_error_serves_items_and_is_observable() {
        let candidates = Arc::new(FakeCandidates::returning(&[id(1)], 1));
        let mut fake_pg = FakePg::new(vec![item(1, 1, true)], HashMap::from([(id(1), 1)]));
        fake_pg.fail_on_search_call = Some(2);
        let metrics = Arc::new(PathsearchMetrics::new());
        let composer = Composer::new(candidates, Arc::new(fake_pg), config(), None)
            .with_metrics(Arc::clone(&metrics));

        let (result, served) = search(&composer, 10, 0).await;

        assert!(served);
        assert_eq!(result.items.len(), 1);
        assert!(result.aggregations.is_empty());
        assert_eq!(metrics.agg_error_count(), 1);
        assert_eq!(metrics.route_count(RouteResult::Served), 1);
        assert_eq!(metrics.route_count(RouteResult::Error), 0);
    }

    #[tokio::test]
    async fn large_candidate_load_has_zero_unbounded_refines() {
        const CANDIDATES: usize = 200;
        const FILES_EACH: usize = 5;
        const CHUNK_FILE_BUDGET: u32 = 25;
        const RETAINED_FILE_BUDGET: u32 = 100;

        let ids = (1..=CANDIDATES)
            .map(|value| id(u8::try_from(value).expect("candidate id fits in u8")))
            .collect::<Vec<_>>();
        let items = (1..=CANDIDATES)
            .map(|value| {
                item(
                    u8::try_from(value).expect("candidate id fits in u8"),
                    FILES_EACH,
                    true,
                )
            })
            .collect::<Vec<_>>();
        let counts = ids
            .iter()
            .copied()
            .map(|info_hash| (info_hash, i64::try_from(FILES_EACH).unwrap()))
            .collect::<HashMap<_, _>>();
        let candidate_source = Arc::new(FakeCandidates::returning(&ids, 1_000_000));
        let pg = Arc::new(FakePg::new(items, counts.clone()));
        let metrics = Arc::new(PathsearchMetrics::new());
        let mut cfg = config();
        cfg.max_candidates = u32::try_from(CANDIDATES).unwrap();
        cfg.max_decode_candidates = u32::try_from(CANDIDATES).unwrap();
        cfg.max_refine_files = CHUNK_FILE_BUDGET;
        cfg.refine_file_budget = CHUNK_FILE_BUDGET;
        cfg.max_chunk_torrents = u32::try_from(CANDIDATES).unwrap();
        cfg.retained_file_budget = RETAINED_FILE_BUDGET;
        let composer = Composer::new(candidate_source.clone(), pg.clone(), cfg, None)
            .with_metrics(Arc::clone(&metrics));

        let (result, served) = search(&composer, 50, 0).await;

        assert!(served);
        assert!(result.total_count_is_estimate);
        assert_eq!(
            candidate_source.requested_limit.load(Ordering::Relaxed),
            200
        );
        assert_eq!(result.items.len(), 20);
        assert_eq!(
            result
                .items
                .iter()
                .map(|item| item.refine_files.len())
                .sum::<usize>(),
            usize::try_from(RETAINED_FILE_BUDGET).unwrap()
        );
        assert!(result
            .items
            .iter()
            .all(|item| item.torrent.files_data.is_none()));

        let hydrate_calls = pg
            .calls()
            .into_iter()
            .filter(|call| call.hydrate.files_data)
            .collect::<Vec<_>>();
        assert_eq!(
            hydrate_calls.len(),
            5,
            "four chunks fill the retained cap and only one bounded lookahead chunk may decode"
        );
        for call in &hydrate_calls {
            let ids = candidate_ids(&call.options.filter);
            let files = ids.iter().map(|info_hash| counts[info_hash]).sum::<i64>();
            assert!(
                files <= i64::from(CHUNK_FILE_BUDGET),
                "one hydration decoded {files} files above the chunk budget"
            );
            assert!(ids.len() <= 5);
        }
        assert_eq!(
            hydrate_calls
                .iter()
                .map(|call| candidate_ids(&call.options.filter).len())
                .sum::<usize>(),
            25,
            "the route must stop after one bounded lookahead chunk, not refine all 200 candidates"
        );
        assert_eq!(metrics.retained_capped_count(), 1);
        assert_eq!(metrics.deadline_capped_count(), 0);
        assert_eq!(metrics.shed_count(), 0);
        assert_eq!(metrics.route_count(RouteResult::Served), 1);
    }

    #[tokio::test]
    async fn unknown_summary_count_is_budgeted_at_cap_and_post_decode_guarded() {
        let candidates = Arc::new(FakeCandidates::returning(&[id(1), id(2)], 2));
        let pg = Arc::new(FakePg::new(
            vec![item(1, 4, true), item(2, 1, true)],
            HashMap::from([(id(2), 1)]),
        ));
        let mut cfg = config();
        cfg.max_refine_files = 3;
        cfg.refine_file_budget = 3;
        let metrics = Arc::new(PathsearchMetrics::new());
        let composer = Composer::new(candidates, pg, cfg, None).with_metrics(Arc::clone(&metrics));

        let (result, served) = search(&composer, 10, 0).await;

        assert!(served);
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].info_hash, id(2));
        assert_eq!(metrics.refine_declined_count(), 1);
        assert_eq!(metrics.route_count(RouteResult::Served), 1);
    }

    #[test]
    fn effective_per_torrent_cap_cannot_exceed_transient_or_retained_budgets() {
        let candidates = Arc::new(FakeCandidates::returning(&[], 0));
        let pg = Arc::new(FakePg::new(Vec::new(), HashMap::new()));
        let mut cfg = config();
        cfg.max_refine_files = 100;
        cfg.refine_file_budget = 50;
        cfg.retained_file_budget = 25;
        let composer = Composer::new(candidates, pg, cfg, None);

        assert_eq!(composer.effective_file_cap(), 25);
    }

    #[test]
    fn hostile_permit_and_duration_values_are_clamped_without_panicking() {
        let candidates = Arc::new(FakeCandidates::returning(&[], 0));
        let pg = Arc::new(FakePg::new(Vec::new(), HashMap::new()));
        let mut cfg = config();
        cfg.max_concurrent_refines = usize::MAX;
        cfg.route_timeout = Duration::MAX;
        cfg.slot_wait = Duration::MAX;
        let composer = Composer::new(candidates, pg, cfg, None);

        assert_eq!(
            composer.refine_slots.available_permits(),
            Semaphore::MAX_PERMITS
        );
        let now = Instant::now();
        assert!(now.checked_add(Duration::MAX).is_none());
        assert_eq!(composer.route_deadline(now), now);
        let route_deadline = now
            .checked_add(Duration::from_secs(1))
            .expect("small deadline must fit");
        assert_eq!(composer.slot_deadline(now, route_deadline), route_deadline);

        let candidates = Arc::new(FakeCandidates::returning(&[], 0));
        let pg = Arc::new(FakePg::new(Vec::new(), HashMap::new()));
        let mut cfg = config();
        cfg.max_concurrent_refines = 0;
        let composer = Composer::new(candidates, pg, cfg, None);
        assert!(composer.refine_slots.available_permits() >= 1);
    }

    #[tokio::test]
    async fn dependency_errors_and_unobtainable_files_fail_safe_to_pg() {
        let mut failing_l3 = FakeCandidates::returning(&[id(1)], 1);
        failing_l3.fail = true;
        let pg = Arc::new(FakePg::new(
            vec![item(1, 1, true)],
            HashMap::from([(id(1), 1)]),
        ));
        let composer = Composer::new(Arc::new(failing_l3), pg, config(), None);
        assert!(!search(&composer, 10, 0).await.1);

        let candidates = Arc::new(FakeCandidates::returning(&[id(1)], 1));
        let mut failing_counts = FakePg::new(vec![item(1, 1, true)], HashMap::new());
        failing_counts.fail_counts = true;
        let composer = Composer::new(candidates, Arc::new(failing_counts), config(), None);
        assert!(!search(&composer, 10, 0).await.1);

        let candidates = Arc::new(FakeCandidates::returning(&[id(1)], 1));
        let mut missing = item(1, 1, true);
        missing.torrent.files_data = None;
        let pg = Arc::new(FakePg::new(vec![missing], HashMap::from([(id(1), 1)])));
        let composer = Composer::new(candidates, pg, config(), None);
        assert!(!search(&composer, 10, 0).await.1);

        let candidates = Arc::new(FakeCandidates::returning(&[id(1)], 1));
        let mut failing_search = FakePg::new(vec![item(1, 1, true)], HashMap::from([(id(1), 1)]));
        failing_search.fail_search = true;
        let composer = Composer::new(candidates, Arc::new(failing_search), config(), None);
        assert!(!search(&composer, 10, 0).await.1);
    }

    #[tokio::test]
    async fn zero_candidates_are_authoritative_only_while_health_gate_is_healthy() {
        let pg = Arc::new(FakePg::new(Vec::new(), HashMap::new()));
        let unhealthy: HealthGate = Arc::new(|| false);
        let composer = Composer::new(
            Arc::new(FakeCandidates::returning(&[], 0)),
            pg.clone(),
            config(),
            Some(unhealthy),
        );
        assert!(!search(&composer, 10, 0).await.1);

        let healthy: HealthGate = Arc::new(|| true);
        let composer = Composer::new(
            Arc::new(FakeCandidates::returning(&[], 0)),
            pg,
            config(),
            Some(healthy),
        );
        let (result, served) = search(&composer, 10, 0).await;
        assert!(served);
        assert!(result.items.is_empty());
        assert!(result.total_count_is_estimate);
    }

    #[tokio::test]
    async fn file_rows_exact_refine_sort_page_and_candidate_total_match_go() {
        let candidates = Arc::new(FakeCandidates::returning(&[id(1), id(2), id(3)], 42));
        let pg = Arc::new(FakePg::new(
            vec![
                item_with_files(
                    1,
                    vec![
                        file(0, "Movies/Zeta/movie.mkv", "mkv", 900),
                        file(1, "Movies/Zeta/movie.txt", "txt", 1_200),
                        file(2, "Movies/Zeta/tiny-movie.mkv", "mkv", 100),
                    ],
                ),
                item_with_files(2, vec![file(0, "Movies/Other/other.mkv", "mkv", 800)]),
                item_with_files(3, vec![file(0, "Movies/Alpha/movie.mkv", "", 500)]),
            ],
            HashMap::from([(id(1), 3), (id(2), 1), (id(3), 1)]),
        ));
        let composer = Composer::new(candidates.clone(), pg, config(), None);

        let (result, served) = composer
            .search_file_rows(
                Filters {
                    query: "movie".to_owned(),
                    extensions: vec!["MKV".to_owned()],
                    min_size: 200,
                    max_size: 1_000,
                },
                query_options(),
                1,
                0,
                vec![FileRowSort {
                    field: "path".to_owned(),
                    descending: false,
                }],
            )
            .await
            .unwrap();

        assert!(served);
        assert_eq!(result.total_count, 42);
        assert!(result.total_count_is_estimate);
        assert!(result.has_next_page);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].info_hash, id(3));
        assert_eq!(result.rows[0].path, "Movies/Alpha/movie.mkv");
        assert_eq!(result.rows[0].extension, "mkv");
        assert_eq!(result.rows[0].size, 500);
        assert!(result.rows[0].torrent_content.torrent.files_data.is_none());
        assert!(result.rows[0].torrent_content.refine_files.is_empty());
        assert_eq!(candidates.requested_limit.load(Ordering::Relaxed), 8);
    }

    #[tokio::test]
    async fn file_rows_default_size_and_torrent_field_sorts_are_deterministic() {
        let mut missing = item_with_files(1, vec![file(0, "movie-missing.mkv", "mkv", 100)]);
        missing.seeders = None;
        missing.published_at = 10;
        missing.torrent_content_updated_at = 30;
        missing.dht_last_seen_at = None;
        let mut newest = item_with_files(2, vec![file(0, "movie-new.mkv", "mkv", 900)]);
        newest.seeders = Some(19);
        newest.published_at = 30;
        newest.torrent_content_updated_at = 10;
        newest.dht_last_seen_at = Some(30);
        let mut older = item_with_files(3, vec![file(0, "movie-old.mkv", "mkv", 500)]);
        older.seeders = Some(8);
        older.published_at = 20;
        older.torrent_content_updated_at = 20;
        older.dht_last_seen_at = Some(20);

        let candidates = Arc::new(FakeCandidates::returning(&[id(1), id(2), id(3)], 3));
        let pg = Arc::new(FakePg::new(
            vec![missing, newest, older],
            HashMap::from([(id(1), 1), (id(2), 1), (id(3), 1)]),
        ));
        let composer = Composer::new(candidates, pg, config(), None);
        let filters = Filters {
            query: "movie".to_owned(),
            ..Filters::default()
        };

        let (default, served) = composer
            .search_file_rows(filters.clone(), query_options(), 3, 0, Vec::new())
            .await
            .unwrap();
        assert!(served);
        assert_eq!(
            default
                .rows
                .iter()
                .map(|row| row.info_hash)
                .collect::<Vec<_>>(),
            vec![id(2), id(3), id(1)]
        );

        for (field, expected) in [
            ("last_seen", vec![id(2), id(3), id(1)]),
            ("seeders", vec![id(2), id(3), id(1)]),
            ("published_at", vec![id(2), id(3), id(1)]),
            ("updated_at", vec![id(1), id(3), id(2)]),
        ] {
            let (result, served) = composer
                .search_file_rows(
                    filters.clone(),
                    query_options(),
                    3,
                    0,
                    vec![FileRowSort {
                        field: field.to_owned(),
                        descending: true,
                    }],
                )
                .await
                .unwrap();
            assert!(served, "{field} route must serve");
            assert_eq!(
                result
                    .rows
                    .iter()
                    .map(|row| row.info_hash)
                    .collect::<Vec<_>>(),
                expected,
                "unexpected {field} ordering"
            );
        }
    }

    #[tokio::test]
    async fn collapse_groups_exact_paths_across_chunks_in_candidate_order() {
        let candidates = Arc::new(FakeCandidates::returning(&[id(1), id(2), id(3)], 3));
        let pg = Arc::new(FakePg::new(
            vec![
                item_with_files(
                    1,
                    vec![
                        file(0, "a/Movie.mkv", "mkv", 1),
                        file(1, "a/sample.txt", "txt", 1),
                        file(2, "a/Movie.mkv", "mkv", 1),
                    ],
                ),
                item_with_files(2, vec![file(0, "b/movie.mkv", "mkv", 2)]),
                item_with_files(3, vec![file(0, "a/Movie.mkv", "mkv", 3)]),
            ],
            HashMap::from([(id(1), 3), (id(2), 1), (id(3), 1)]),
        ));
        let mut cfg = config();
        cfg.max_chunk_torrents = 1;
        let composer = Composer::new(candidates, pg, cfg, None);

        let (groups, served) = composer
            .collapse_paths(
                Filters {
                    query: "movie".to_owned(),
                    ..Filters::default()
                },
                query_options(),
                10,
                0,
                Vec::new(),
            )
            .await
            .unwrap();

        assert!(served);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].path, "a/Movie.mkv");
        assert_eq!(groups[0].info_hashes, vec![id(1), id(3)]);
        assert_eq!(groups[1].path, "b/movie.mkv");
        assert_eq!(groups[1].info_hashes, vec![id(2)]);
    }

    #[tokio::test]
    async fn path_typeahead_dedupes_case_insensitively_and_counts_torrents() {
        let candidates = Arc::new(FakeCandidates::returning(&[id(1), id(2), id(3)], 3));
        let pg = Arc::new(FakePg::new(
            vec![
                item_with_files(
                    1,
                    vec![
                        file(0, "Movies/Inception/movie.mkv", "mkv", 1),
                        file(1, "Movies/INCEPTION/sample.mkv", "mkv", 1),
                    ],
                ),
                item_with_files(2, vec![file(0, "Movies/Interstellar/movie.mkv", "mkv", 1)]),
                item_with_files(3, vec![file(0, "Movies/Inception/bonus.mkv", "mkv", 1)]),
            ],
            HashMap::from([(id(1), 2), (id(2), 1), (id(3), 1)]),
        ));
        let composer = Composer::new(candidates, pg, config(), None);

        let (suggestions, served) = composer
            .path_typeahead("Movies/I".to_owned(), query_options(), 2)
            .await
            .unwrap();

        assert!(served);
        assert_eq!(suggestions, vec!["Inception", "Interstellar"]);
        assert_eq!(
            next_path_segment("Movies/", "Movies/Inception/movie.mkv"),
            Some("Inception".to_owned())
        );
        assert_eq!(
            next_path_segment("ception", "Movies/Inception/movie.mkv"),
            Some("Inception".to_owned())
        );
    }

    #[tokio::test]
    async fn c4_retained_budget_stops_after_one_bounded_lookahead_chunk() {
        let candidates = Arc::new(FakeCandidates::returning(&[id(1), id(2), id(3)], 3));
        let pg = Arc::new(FakePg::new(
            vec![item(1, 4, true), item(2, 4, true), item(3, 4, true)],
            HashMap::from([(id(1), 4), (id(2), 4), (id(3), 4)]),
        ));
        let metrics = Arc::new(PathsearchMetrics::new());
        let mut cfg = config();
        cfg.refine_file_budget = 4;
        cfg.max_chunk_torrents = 1;
        cfg.retained_file_budget = 5;
        let composer =
            Composer::new(candidates, pg.clone(), cfg, None).with_metrics(Arc::clone(&metrics));

        let (result, served) = composer
            .search_file_rows(
                Filters {
                    query: "inception".to_owned(),
                    ..Filters::default()
                },
                query_options(),
                20,
                0,
                Vec::new(),
            )
            .await
            .unwrap();

        assert!(served);
        assert_eq!(result.rows.len(), 5);
        assert!(result.total_count_is_estimate);
        let hydrate_calls = pg
            .calls()
            .into_iter()
            .filter(|call| call.hydrate.files_data)
            .collect::<Vec<_>>();
        assert_eq!(hydrate_calls.len(), 2);
        assert!(hydrate_calls
            .iter()
            .all(|call| candidate_ids(&call.options.filter).len() == 1));
        assert_eq!(metrics.retained_capped_count(), 1);
        assert_eq!(metrics.route_count(RouteResult::Served), 1);
    }

    #[tokio::test]
    async fn c4_byte_cap_is_estimated_for_file_rows_but_declined_elsewhere() {
        let candidates = Arc::new(FakeCandidates::returning(&[id(1)], 1));
        let pg = Arc::new(FakePg::new(
            vec![item_with_files(
                1,
                vec![file(0, "inception/long-path.mkv", "mkv", 1)],
            )],
            HashMap::from([(id(1), 1)]),
        ));
        let metrics = Arc::new(PathsearchMetrics::new());
        let mut cfg = config();
        cfg.retained_byte_budget = 1;
        let composer = Composer::new(candidates, pg, cfg, None).with_metrics(Arc::clone(&metrics));
        let filters = Filters {
            query: "inception".to_owned(),
            ..Filters::default()
        };

        let (file_rows, served) = composer
            .search_file_rows(filters.clone(), query_options(), 10, 0, Vec::new())
            .await
            .unwrap();
        assert!(served);
        assert!(file_rows.rows.is_empty());
        assert!(file_rows.total_count_is_estimate);

        let (groups, served) = composer
            .collapse_paths(filters, query_options(), 10, 0, Vec::new())
            .await
            .unwrap();
        assert!(!served);
        assert!(groups.is_empty());

        let (suggestions, served) = composer
            .path_typeahead("inception".to_owned(), query_options(), 10)
            .await
            .unwrap();
        assert!(!served);
        assert!(suggestions.is_empty());

        assert_eq!(metrics.retained_capped_count(), 3);
        assert_eq!(metrics.route_count(RouteResult::Served), 1);
        assert_eq!(metrics.route_count(RouteResult::Fallback), 2);
    }

    #[tokio::test]
    async fn c4_deadline_serves_accumulated_prefix_and_dependency_failure_falls_back() {
        let candidates = Arc::new(FakeCandidates::returning(&[id(1), id(2)], 2));
        let mut fake_pg = FakePg::new(
            vec![item(1, 1, true), item(2, 1, true)],
            HashMap::from([(id(1), 1), (id(2), 1)]),
        );
        fake_pg.delayed_ids.insert(id(2));
        fake_pg.delay = Duration::from_millis(100);
        let metrics = Arc::new(PathsearchMetrics::new());
        let mut cfg = config();
        cfg.max_chunk_torrents = 1;
        cfg.route_timeout = Duration::from_millis(20);
        let composer = Composer::new(candidates, Arc::new(fake_pg), cfg, None)
            .with_metrics(Arc::clone(&metrics));

        let (result, served) = composer
            .search_file_rows(
                Filters {
                    query: "inception".to_owned(),
                    ..Filters::default()
                },
                query_options(),
                10,
                0,
                Vec::new(),
            )
            .await
            .unwrap();
        assert!(served);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].info_hash, id(1));
        assert_eq!(metrics.deadline_capped_count(), 1);

        let mut failed = FakeCandidates::returning(&[id(1)], 1);
        failed.fail = true;
        let composer = Composer::new(
            Arc::new(failed),
            Arc::new(FakePg::new(
                vec![item(1, 1, true)],
                HashMap::from([(id(1), 1)]),
            )),
            config(),
            None,
        );
        let (_, served) = composer
            .path_typeahead("inception".to_owned(), query_options(), 10)
            .await
            .unwrap();
        assert!(!served);
    }
}
