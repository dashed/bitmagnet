//! Bounded L3-candidate plus L1 exact-refine orchestration.
//!
//! The composer is a decorator: any failure before an authoritative refined
//! prefix exists declines the route so Lane G can execute its normal Lane-S
//! PostgreSQL query. Once chunked refinement has started, deadline and retained
//! memory caps serve the accumulated relevance-ordered prefix as an estimate;
//! they never turn a pathological path query into an unbounded PG fallback.

use std::collections::HashMap;
use std::sync::Arc;

use bitmagnet_model::InfoHash;
use bitmagnet_proto::v1::{PathCandidatesRequest, SortBy, SuggestRequest};
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::time::{timeout_at, Instant};

use crate::api::SearchServe;
use crate::candidates::{CandidateSource, HealthGate};
use crate::config::ComposerConfig;
use crate::filters::{FileRowSort, FileRowsResult, Filters, PathGroup};
use crate::metrics::{PathsearchMetrics, RouteResult};
use crate::pg::{
    empty_result, Aggregations, HydrateOptions, PgSearchBackend, QueryOptions, SearchRequest,
    SearchResult, SearchResultItem,
};
use crate::refine::{files_for_refine, paginate, torrent_matches, RefinePredicate};

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
    Retained,
    Deadline,
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

    fn file_count_of(&self, id: &InfoHash, counts: &HashMap<InfoHash, i64>) -> u64 {
        let cap = self.effective_file_cap();
        match counts.get(id).copied() {
            Some(count) if count <= 0 => 0,
            Some(count) => u64::try_from(count).unwrap_or(u64::MAX).min(u64::from(cap)),
            None => u64::from(cap),
        }
    }

    fn decline_oversized(
        &self,
        ids: Vec<InfoHash>,
        counts: &HashMap<InfoHash, i64>,
    ) -> Vec<InfoHash> {
        let cap = self.effective_file_cap();
        ids.into_iter()
            .filter(|id| match counts.get(id).copied() {
                Some(count) if count > i64::from(cap) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_refine_declined_oversized();
                    }
                    tracing::warn!(
                        info_hash = %id,
                        file_count = count,
                        cap,
                        "pathsearch declining oversized candidate"
                    );
                    false
                }
                _ => true,
            })
            .collect()
    }

    fn chunk_by_file_budget(
        &self,
        ids: Vec<InfoHash>,
        counts: &HashMap<InfoHash, i64>,
    ) -> Vec<Vec<InfoHash>> {
        let mut chunks = Vec::new();
        let mut current = Vec::new();
        let mut current_files = 0_u64;
        let max_torrents = usize::try_from(self.config.max_chunk_torrents)
            .unwrap_or(usize::MAX)
            .max(1);
        let file_budget = u64::from(self.config.refine_file_budget);

        for id in ids {
            let files = self.file_count_of(&id, counts);
            if !current.is_empty()
                && (current.len() >= max_torrents
                    || current_files.saturating_add(files) > file_budget)
            {
                chunks.push(std::mem::take(&mut current));
                current_files = 0;
            }
            current.push(id);
            current_files = current_files.saturating_add(files);
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
        let request = request.for_candidates(ids, HydrateOptions { files_data: true });
        match timeout_at(deadline, self.pg.torrent_content(request)).await {
            Ok(Ok(result)) => Ok(self.ordered_chunk_items(result, ids)),
            Ok(Err(error)) => {
                tracing::warn!(%error, "pathsearch candidate hydration failed; falling back to PostgreSQL");
                Err(RefineCap::None)
            }
            Err(_) => Err(RefineCap::Deadline),
        }
    }

    fn refine_chunk(
        &self,
        items: Vec<SearchResultItem>,
        predicate: &RefinePredicate,
        deadline: Instant,
        refined: &mut Vec<SearchResultItem>,
        retained_files: &mut u64,
    ) -> Result<RefineCap, ()> {
        let file_cap = u64::from(self.effective_file_cap());
        for mut item in items {
            if Instant::now() >= deadline {
                return Ok(RefineCap::Deadline);
            }
            let Some(files) = files_for_refine(&item.torrent) else {
                tracing::warn!(info_hash = %item.info_hash, "pathsearch candidate files unobtainable; falling back to PostgreSQL");
                return Err(());
            };
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
            if !refined.is_empty()
                && retained_files.saturating_add(file_count)
                    > u64::from(self.config.retained_file_budget)
            {
                return Ok(RefineCap::Retained);
            }
            *retained_files = retained_files.saturating_add(file_count);
            item.torrent.files_data = None;
            item.refine_files = files;
            refined.push(item);
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
        let request = request.for_candidates(&ids, HydrateOptions { files_data: false });
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
        let counts = match timeout_at(deadline, self.pg.file_counts(&ids)).await {
            Ok(Ok(counts)) => counts,
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
        let kept = self.decline_oversized(ids.clone(), &counts);
        if kept.is_empty() {
            self.inc_route(RouteResult::Served);
            return (Self::empty_estimate(), true);
        }
        let chunks = self.chunk_by_file_budget(kept, &counts);
        let _permit = if chunks.len() > 1 {
            match self.acquire_refine_slot(deadline).await {
                Some(permit) => Some(permit),
                None => {
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_refine_shed();
                    }
                    self.inc_route(RouteResult::Served);
                    tracing::warn!(
                        "pathsearch refine slot unavailable; serving shed empty estimate"
                    );
                    return (Self::empty_estimate(), true);
                }
            }
        } else {
            None
        };

        let mut refined = Vec::new();
        let mut retained_files = 0_u64;
        let mut cap = RefineCap::None;
        for chunk in chunks {
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
                deadline,
                &mut refined,
                &mut retained_files,
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
            if let Some(metrics) = &self.metrics {
                match cap {
                    RefineCap::Retained => metrics.inc_refine_retained_capped(),
                    RefineCap::Deadline => metrics.inc_refine_deadline_capped(),
                    RefineCap::None => {}
                }
            }
            tracing::warn!(
                ?cap,
                matches = refined.len(),
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
        _filters: Filters,
        _options: QueryOptions,
        _limit: u32,
        _offset: u32,
        _sorts: Vec<SortBy>,
    ) -> crate::Result<(Vec<PathGroup>, bool)> {
        Ok((Vec::new(), false))
    }

    async fn search_file_rows(
        &self,
        _filters: Filters,
        _options: QueryOptions,
        _limit: u32,
        _offset: u32,
        _sort_by: Vec<FileRowSort>,
    ) -> crate::Result<(FileRowsResult, bool)> {
        Ok((FileRowsResult::default(), false))
    }

    async fn path_typeahead(
        &self,
        _prefix: String,
        _options: QueryOptions,
        _limit: u32,
    ) -> crate::Result<(Vec<String>, bool)> {
        Ok((Vec::new(), false))
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

        async fn file_counts(&self, ids: &[InfoHash]) -> crate::Result<HashMap<InfoHash, i64>> {
            if self.fail_counts {
                return Err(crate::Error::Pg("file-count failure".into()));
            }
            Ok(ids
                .iter()
                .filter_map(|id| self.counts.get(id).copied().map(|count| (*id, count)))
                .collect())
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
                HydrateOptions { files_data: true },
            ),
            refine: Some(SearchRequest::new(
                SearchOptions::default(),
                HydrateOptions { files_data: true },
            )),
            agg: SearchRequest::new(
                SearchOptions::default().with_facets([aggregate_facet]),
                HydrateOptions::default(),
            ),
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
        assert!(
            calls[0].options.facets.is_empty(),
            "even a one-chunk route must use refine, not candidate-set combined facets"
        );
        assert!(!calls[1].hydrate.files_data);
        assert!(calls[1].options.facets[0].aggregate);
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
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(1))
            .expect("small deadline must fit");

        let cap = composer
            .refine_chunk(
                vec![item(1, 2, false), item(2, 3, true)],
                &predicate,
                deadline,
                &mut refined,
                &mut retained_files,
            )
            .unwrap();

        assert_eq!(cap, RefineCap::None);
        assert_eq!(retained_files, 3);
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
    async fn saturated_multi_chunk_route_sheds_but_single_chunk_does_not_take_slot() {
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

        let (fast, served) = search(&composer, 10, 0).await;

        assert!(served);
        assert_eq!(fast.items.len(), 2);
        assert_eq!(metrics.shed_count(), 1);
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
}
