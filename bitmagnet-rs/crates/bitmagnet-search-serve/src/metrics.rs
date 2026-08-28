//! Canonical composer and dormant Tantivy-serve Prometheus metrics.
//!
//! The names and label sets in this module are the Phase-0 metric-name golden
//! contract. Collectors register with [`bitmagnet_common::metrics::registry`],
//! so they are exposed by the process-wide metrics server shared by every Rust
//! service. [`PathsearchMetrics::new`] and [`ServeMetrics::new`] deliberately
//! leave collectors unregistered for deterministic unit tests.

const PATH_PREFIX: &str = "bitmagnet_search_pathsearch";
const SERVE_PREFIX: &str = "bitmagnet_search_serve";
const PHASE_DURATION_BUCKET_START_SECONDS: f64 = 0.001;
const PHASE_DURATION_BUCKET_FACTOR: f64 = 2.0;
const PHASE_DURATION_BUCKET_COUNT: usize = 14;

/// Fixed, low-cardinality phase labels for ranked torrent-content composition.
///
/// Hydration produces one observation per attempted bounded chunk. Exact refine
/// produces one observation only after that chunk hydrates successfully. Every
/// other phase produces at most one observation per composer route attempt.
///
/// [`Self::RefineMetadata`] wraps the whole pre-hydration metadata backend call.
/// [`Self::RefineMetadataSummary`] times the summary-first probe and is observed
/// on every route attempt; [`Self::RefineMetadataTorrents`] times the torrents
/// blob-length fallback and is observed only when the summary set has a miss (no
/// summary row, or a NULL compressed_bytes) — a fully covered candidate set skips
/// that query, so this phase records ZERO observations on those routes. Downstream
/// measurement contracts must not assert a fixed observation count for it. This is
/// a deliberate, documented extension of the Phase-0 phase vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathsearchPhase {
    CandidateIds,
    RefineMetadata,
    RefineMetadataSummary,
    RefineMetadataTorrents,
    RefineSlotWait,
    HydrateChunk,
    RefineChunk,
    Aggregations,
    RouteTotal,
}

impl PathsearchPhase {
    const ALL: [Self; 9] = [
        Self::CandidateIds,
        Self::RefineMetadata,
        Self::RefineMetadataSummary,
        Self::RefineMetadataTorrents,
        Self::RefineSlotWait,
        Self::HydrateChunk,
        Self::RefineChunk,
        Self::Aggregations,
        Self::RouteTotal,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::CandidateIds => "candidate_ids",
            Self::RefineMetadata => "refine_metadata",
            Self::RefineMetadataSummary => "refine_metadata_summary",
            Self::RefineMetadataTorrents => "refine_metadata_torrents",
            Self::RefineSlotWait => "refine_slot_wait",
            Self::HydrateChunk => "hydrate_chunk",
            Self::RefineChunk => "refine_chunk",
            Self::Aggregations => "aggregations",
            Self::RouteTotal => "route_total",
        }
    }
}

/// Fixed candidate-cardinality labels for the pre-hydration metadata probe.
///
/// These are aggregate counters, never request or torrent identifiers. Their
/// deltas expose whether a slow fallback was caused by absent summary rows or
/// NULL denormalized byte counts without adding high-cardinality labels. Every
/// state counts unique candidates, and the fallback partition is exact:
/// `fallback_miss = missing_summary + null_bytes + schema_without_bytes`.
/// `fallback_miss` counts candidates routed to the fallback probe, even if that
/// subsequent PostgreSQL query fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefineMetadataCandidateState {
    Requested,
    SummaryRow,
    Covered,
    MissingSummary,
    NullBytes,
    SchemaWithoutBytes,
    FallbackMiss,
}

impl RefineMetadataCandidateState {
    const ALL: [Self; 7] = [
        Self::Requested,
        Self::SummaryRow,
        Self::Covered,
        Self::MissingSummary,
        Self::NullBytes,
        Self::SchemaWithoutBytes,
        Self::FallbackMiss,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::SummaryRow => "summary_row",
            Self::Covered => "covered",
            Self::MissingSummary => "missing_summary",
            Self::NullBytes => "null_bytes",
            Self::SchemaWithoutBytes => "schema_without_bytes",
            Self::FallbackMiss => "fallback_miss",
        }
    }
}

/// Stable labels for `bitmagnet_search_pathsearch_route_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteResult {
    /// L3 candidates and exact refine produced the served result.
    Served,
    /// The route declined cleanly and the resolver should use PostgreSQL.
    Fallback,
    /// The query did not pass the L3 eligibility guard.
    Ineligible,
    /// A sidecar, PostgreSQL, or blob failure forced fallback.
    Error,
}

impl RouteResult {
    fn label(self) -> &'static str {
        match self {
            Self::Served => "served",
            Self::Fallback => "fallback",
            Self::Ineligible => "ineligible",
            Self::Error => "error",
        }
    }
}

/// Stable labels for `bitmagnet_search_serve_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeOutcome {
    /// Tantivy returned hits that PostgreSQL hydrated and served.
    Served,
    /// The sidecar call failed and the router fell back to PostgreSQL.
    FallbackError,
    /// Tantivy returned an empty response that was not authoritative.
    FallbackEmpty,
    /// PostgreSQL could not hydrate the Tantivy hit set.
    FallbackHydrateError,
}

impl ServeOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Served => "served",
            Self::FallbackError => "fallback_error",
            Self::FallbackEmpty => "fallback_empty",
            Self::FallbackHydrateError => "fallback_hydrate_error",
        }
    }
}

/// L3 route, exact-refine, and health collectors.
#[derive(Clone, Debug)]
pub struct PathsearchMetrics {
    doc_count: prometheus::IntGauge,
    healthy: prometheus::IntGauge,
    watermark_epoch: prometheus::IntGauge,
    last_success_epoch: prometheus::IntGauge,
    health_checks: prometheus::IntCounterVec,
    routes: prometheus::IntCounterVec,
    refine_declined: prometheus::IntCounter,
    retained_capped: prometheus::IntCounter,
    deadline_capped: prometheus::IntCounter,
    refine_shed: prometheus::IntCounter,
    refine_agg_error: prometheus::IntCounter,
    refine_metadata_candidates: prometheus::IntCounterVec,
    phase_duration: prometheus::HistogramVec,
}

impl PathsearchMetrics {
    /// Constructs the canonical collectors without registering them.
    ///
    /// Use [`Self::register`] once in the process composition root. Keeping the
    /// constructor registration-free lets tests create isolated metric sets.
    #[must_use]
    pub fn new() -> Self {
        let metrics = Self {
            doc_count: int_gauge(
                &format!("{PATH_PREFIX}_doc_count"),
                "Number of documents in the L3 pathsearch index; zero when unreachable.",
            ),
            healthy: int_gauge(
                &format!("{PATH_PREFIX}_healthy"),
                "Whether the cached L3 pathsearch health gate trusts the sidecar.",
            ),
            watermark_epoch: int_gauge(
                &format!("{PATH_PREFIX}_watermark_epoch_seconds"),
                "Last observed L3 follow-loop watermark as Unix epoch seconds.",
            ),
            last_success_epoch: int_gauge(
                &format!("{PATH_PREFIX}_last_success_epoch_seconds"),
                "Unix epoch seconds of the last successful L3 health check.",
            ),
            health_checks: int_counter_vec(
                &format!("{PATH_PREFIX}_health_checks_total"),
                "Count of L3 health checks by outcome.",
                &["result"],
            ),
            routes: int_counter_vec(
                &format!("{PATH_PREFIX}_route_total"),
                "Count of L3-routed queries by outcome.",
                &["result"],
            ),
            refine_declined: int_counter(
                &format!("{PATH_PREFIX}_refine_declined_oversized_total"),
                "Candidates excluded because their file count exceeded the exact-refine cap.",
            ),
            retained_capped: int_counter(
                &format!("{PATH_PREFIX}_refine_retained_capped_total"),
                "Requests served as an estimate after reaching a retained-file or byte budget.",
            ),
            deadline_capped: int_counter(
                &format!("{PATH_PREFIX}_refine_deadline_capped_total"),
                "Requests served as an estimate after reaching the whole-route deadline.",
            ),
            refine_shed: int_counter(
                &format!("{PATH_PREFIX}_refine_shed_total"),
                "Multi-chunk refines shed because no bounded concurrency slot was available.",
            ),
            refine_agg_error: int_counter(
                &format!("{PATH_PREFIX}_refine_agg_error_total"),
                "Requests served without facets after refined-set aggregation failed.",
            ),
            refine_metadata_candidates: int_counter_vec(
                &format!("{PATH_PREFIX}_refine_metadata_candidates_total"),
                "Candidate cardinality observed by each bounded refine-metadata state.",
                &["state"],
            ),
            phase_duration: histogram_vec(
                &format!("{PATH_PREFIX}_phase_duration_seconds"),
                "Duration of ranked torrent-content composition phases. Hydrate is observed per attempted chunk; refine only after successful hydrate.",
                &["phase"],
                prometheus::exponential_buckets(
                    PHASE_DURATION_BUCKET_START_SECONDS,
                    PHASE_DURATION_BUCKET_FACTOR,
                    PHASE_DURATION_BUCKET_COUNT,
                )
                .expect("pathsearch phase-duration histogram buckets are valid"),
            ),
        };

        // Pre-initialize the finite label vocabulary. Besides exposing useful
        // zeroes at startup, this makes accidental label drift immediately
        // visible in scrape output and tests.
        for result in ["ok", "error"] {
            metrics.health_checks.with_label_values(&[result]);
        }
        for result in [
            RouteResult::Served,
            RouteResult::Fallback,
            RouteResult::Ineligible,
            RouteResult::Error,
        ] {
            metrics.routes.with_label_values(&[result.label()]);
        }
        for phase in PathsearchPhase::ALL {
            metrics.phase_duration.with_label_values(&[phase.label()]);
        }
        for state in RefineMetadataCandidateState::ALL {
            metrics
                .refine_metadata_candidates
                .with_label_values(&[state.label()]);
        }

        metrics
    }

    /// Constructs and registers the collectors in the common process registry.
    ///
    /// # Panics
    ///
    /// Panics if called more than once in a process or if another collector has
    /// already claimed one of the canonical names.
    #[must_use]
    pub fn register() -> Self {
        let metrics = Self::new();
        for collector in metrics.collectors() {
            bitmagnet_common::metrics::registry()
                .register(collector)
                .unwrap_or_else(|error| panic!("failed to register pathsearch metric: {error}"));
        }
        metrics
    }

    /// Publishes the cached L3 health snapshot.
    pub fn set_health(
        &self,
        healthy: bool,
        doc_count: i64,
        watermark_epoch: i64,
        last_success_epoch: i64,
    ) {
        self.healthy.set(i64::from(healthy));
        self.doc_count.set(doc_count);
        self.watermark_epoch.set(watermark_epoch);
        self.last_success_epoch.set(last_success_epoch);
    }

    /// Records one health-check outcome.
    pub fn inc_health_check(&self, ok: bool) {
        self.health_checks
            .with_label_values(&[if ok { "ok" } else { "error" }])
            .inc();
    }

    /// Records one terminal composer route outcome.
    pub fn inc_route(&self, result: RouteResult) {
        self.routes.with_label_values(&[result.label()]).inc();
    }

    /// Records one candidate declined before or after decode as oversized.
    pub fn inc_refine_declined_oversized(&self) {
        self.refine_declined.inc();
    }

    /// Records one request stopped by a cumulative retained-file or byte cap.
    pub fn inc_refine_retained_capped(&self) {
        self.retained_capped.inc();
    }

    /// Records one request stopped by the whole-route deadline.
    pub fn inc_refine_deadline_capped(&self) {
        self.deadline_capped.inc();
    }

    /// Records one multi-chunk request shed by the concurrency limiter.
    pub fn inc_refine_shed(&self) {
        self.refine_shed.inc();
    }

    /// Records one non-deadline refined-set aggregation failure.
    pub fn inc_refine_agg_error(&self) {
        self.refine_agg_error.inc();
    }

    /// Adds candidate cardinality for one fixed refine-metadata state.
    pub(crate) fn add_refine_metadata_candidates(
        &self,
        state: RefineMetadataCandidateState,
        count: usize,
    ) {
        self.refine_metadata_candidates
            .with_label_values(&[state.label()])
            .inc_by(u64::try_from(count).unwrap_or(u64::MAX));
    }

    /// Starts an RAII timer for one fixed ranked-search composition phase.
    ///
    /// Dropping the returned timer records exactly one observation. The phase
    /// enum prevents request data or other high-cardinality values from becoming
    /// Prometheus labels.
    pub(crate) fn start_phase_timer(&self, phase: PathsearchPhase) -> prometheus::HistogramTimer {
        self.phase_duration
            .with_label_values(&[phase.label()])
            .start_timer()
    }

    fn collectors(&self) -> Vec<Box<dyn prometheus::core::Collector>> {
        vec![
            Box::new(self.doc_count.clone()),
            Box::new(self.healthy.clone()),
            Box::new(self.watermark_epoch.clone()),
            Box::new(self.last_success_epoch.clone()),
            Box::new(self.health_checks.clone()),
            Box::new(self.routes.clone()),
            Box::new(self.refine_declined.clone()),
            Box::new(self.retained_capped.clone()),
            Box::new(self.deadline_capped.clone()),
            Box::new(self.refine_shed.clone()),
            Box::new(self.refine_agg_error.clone()),
            Box::new(self.refine_metadata_candidates.clone()),
            Box::new(self.phase_duration.clone()),
        ]
    }

    #[cfg(test)]
    pub(crate) fn route_count(&self, result: RouteResult) -> u64 {
        self.routes.with_label_values(&[result.label()]).get()
    }

    #[cfg(test)]
    pub(crate) fn health_check_count(&self, ok: bool) -> u64 {
        self.health_checks
            .with_label_values(&[if ok { "ok" } else { "error" }])
            .get()
    }

    #[cfg(test)]
    pub(crate) fn health_snapshot(&self) -> (bool, i64, i64, i64) {
        (
            self.healthy.get() == 1,
            self.doc_count.get(),
            self.watermark_epoch.get(),
            self.last_success_epoch.get(),
        )
    }

    #[cfg(test)]
    pub(crate) fn refine_declined_count(&self) -> u64 {
        self.refine_declined.get()
    }

    #[cfg(test)]
    pub(crate) fn retained_capped_count(&self) -> u64 {
        self.retained_capped.get()
    }

    #[cfg(test)]
    pub(crate) fn deadline_capped_count(&self) -> u64 {
        self.deadline_capped.get()
    }

    #[cfg(test)]
    pub(crate) fn shed_count(&self) -> u64 {
        self.refine_shed.get()
    }

    #[cfg(test)]
    pub(crate) fn agg_error_count(&self) -> u64 {
        self.refine_agg_error.get()
    }

    #[cfg(test)]
    fn refine_metadata_candidate_count(&self, state: RefineMetadataCandidateState) -> u64 {
        self.refine_metadata_candidates
            .with_label_values(&[state.label()])
            .get()
    }

    #[cfg(test)]
    pub(crate) fn phase_sample_count(&self, phase: PathsearchPhase) -> u64 {
        self.phase_duration
            .with_label_values(&[phase.label()])
            .get_sample_count()
    }
}

impl Default for PathsearchMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Dormant C5 Tantivy-serving outcome and health collectors.
///
/// C5 remains quality-gated and this facade does not enable it; it only freezes
/// the existing metric contract for the future fail-closed router.
#[derive(Clone)]
pub struct ServeMetrics {
    serve_total: prometheus::IntCounterVec,
    sidecar_healthy: prometheus::IntGauge,
    watermark_epoch: prometheus::IntGauge,
}

impl ServeMetrics {
    /// Constructs the canonical collectors without registering them.
    #[must_use]
    pub fn new() -> Self {
        let metrics = Self {
            serve_total: int_counter_vec(
                &format!("{SERVE_PREFIX}_total"),
                "Count of Tantivy main-search serve attempts by outcome.",
                &["outcome"],
            ),
            sidecar_healthy: int_gauge(
                &format!("{SERVE_PREFIX}_sidecar_healthy"),
                "Whether the main-search sidecar is currently serve-eligible.",
            ),
            watermark_epoch: int_gauge(
                &format!("{SERVE_PREFIX}_watermark_epoch_seconds"),
                "Last observed main-search follow-loop watermark as Unix epoch seconds.",
            ),
        };
        for outcome in [
            ServeOutcome::Served,
            ServeOutcome::FallbackError,
            ServeOutcome::FallbackEmpty,
            ServeOutcome::FallbackHydrateError,
        ] {
            metrics.serve_total.with_label_values(&[outcome.label()]);
        }
        metrics
    }

    /// Constructs and registers the collectors in the common process registry.
    ///
    /// # Panics
    ///
    /// Panics on duplicate or invalid registration.
    #[must_use]
    pub fn register() -> Self {
        let metrics = Self::new();
        for collector in metrics.collectors() {
            bitmagnet_common::metrics::registry()
                .register(collector)
                .unwrap_or_else(|error| panic!("failed to register serve metric: {error}"));
        }
        metrics
    }

    /// Records one dormant/future Tantivy serve attempt.
    pub fn inc_serve(&self, outcome: ServeOutcome) {
        self.serve_total.with_label_values(&[outcome.label()]).inc();
    }

    /// Publishes the cached future serving gate and watermark.
    pub fn set_health(&self, eligible: bool, watermark_epoch: i64) {
        self.sidecar_healthy.set(i64::from(eligible));
        self.watermark_epoch.set(watermark_epoch);
    }

    fn collectors(&self) -> Vec<Box<dyn prometheus::core::Collector>> {
        vec![
            Box::new(self.serve_total.clone()),
            Box::new(self.sidecar_healthy.clone()),
            Box::new(self.watermark_epoch.clone()),
        ]
    }
}

impl Default for ServeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

fn int_gauge(name: &str, help: &str) -> prometheus::IntGauge {
    prometheus::IntGauge::new(name, help)
        .unwrap_or_else(|error| panic!("invalid Prometheus gauge {name}: {error}"))
}

fn int_counter(name: &str, help: &str) -> prometheus::IntCounter {
    prometheus::IntCounter::new(name, help)
        .unwrap_or_else(|error| panic!("invalid Prometheus counter {name}: {error}"))
}

fn int_counter_vec(name: &str, help: &str, labels: &[&str]) -> prometheus::IntCounterVec {
    prometheus::IntCounterVec::new(prometheus::Opts::new(name, help), labels)
        .unwrap_or_else(|error| panic!("invalid Prometheus counter vector {name}: {error}"))
}

fn histogram_vec(
    name: &str,
    help: &str,
    labels: &[&str],
    buckets: Vec<f64>,
) -> prometheus::HistogramVec {
    prometheus::HistogramVec::new(
        prometheus::HistogramOpts::new(name, help).buckets(buckets),
        labels,
    )
    .unwrap_or_else(|error| panic!("invalid Prometheus histogram vector {name}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PHASE_ZERO_METRIC_GOLDEN: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/metric-names.golden"
    ));
    const RUST_ONLY_PHASE_DURATION_CONTRACT: &str =
        "bitmagnet_search_pathsearch_phase_duration_seconds{phase}";
    const RUST_ONLY_REFINE_METADATA_CONTRACT: &str =
        "bitmagnet_search_pathsearch_refine_metadata_candidates_total{state}";

    fn contract_lines(collectors: Vec<Box<dyn prometheus::core::Collector>>) -> Vec<String> {
        let mut lines = collectors
            .iter()
            .flat_map(|collector| collector.desc())
            .map(|desc| {
                let mut labels = desc.variable_labels.clone();
                labels.sort();
                format!("{}{{{}}}", desc.fq_name, labels.join(","))
            })
            .collect::<Vec<_>>();
        lines.sort();
        lines
    }

    #[test]
    fn metric_names_and_labels_match_phase_zero_golden_and_lane_p_alerts() {
        let mut actual = contract_lines(PathsearchMetrics::new().collectors());
        actual.extend(contract_lines(ServeMetrics::new().collectors()));
        actual.sort();

        let mut expected = PHASE_ZERO_METRIC_GOLDEN
            .lines()
            .filter(|line| line.starts_with(PATH_PREFIX) || line.starts_with(SERVE_PREFIX))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        expected.push(RUST_ONLY_PHASE_DURATION_CONTRACT.to_owned());
        expected.push(RUST_ONLY_REFINE_METADATA_CONTRACT.to_owned());
        expected.sort();

        assert!(!expected.is_empty(), "Phase-0 metric golden lost Lane C");
        assert_eq!(actual, expected);
    }

    #[test]
    fn pathsearch_facade_updates_every_canonical_series() {
        let metrics = PathsearchMetrics::new();
        metrics.set_health(true, 37, 1_700_000_000, 1_700_000_010);
        metrics.inc_health_check(true);
        metrics.inc_health_check(false);
        metrics.inc_route(RouteResult::Served);
        metrics.inc_route(RouteResult::Fallback);
        metrics.inc_route(RouteResult::Ineligible);
        metrics.inc_route(RouteResult::Error);
        metrics.inc_refine_declined_oversized();
        metrics.inc_refine_retained_capped();
        metrics.inc_refine_deadline_capped();
        metrics.inc_refine_shed();
        metrics.inc_refine_agg_error();
        for (index, state) in RefineMetadataCandidateState::ALL.into_iter().enumerate() {
            metrics.add_refine_metadata_candidates(state, index + 1);
        }
        for phase in PathsearchPhase::ALL {
            drop(metrics.start_phase_timer(phase));
        }

        assert_eq!(metrics.doc_count.get(), 37);
        assert_eq!(metrics.healthy.get(), 1);
        assert_eq!(metrics.watermark_epoch.get(), 1_700_000_000);
        assert_eq!(metrics.last_success_epoch.get(), 1_700_000_010);
        assert_eq!(metrics.health_checks.with_label_values(&["ok"]).get(), 1);
        assert_eq!(metrics.health_checks.with_label_values(&["error"]).get(), 1);
        for result in [
            RouteResult::Served,
            RouteResult::Fallback,
            RouteResult::Ineligible,
            RouteResult::Error,
        ] {
            assert_eq!(metrics.route_count(result), 1);
        }
        assert_eq!(metrics.refine_declined_count(), 1);
        assert_eq!(metrics.retained_capped_count(), 1);
        assert_eq!(metrics.deadline_capped_count(), 1);
        assert_eq!(metrics.shed_count(), 1);
        assert_eq!(metrics.agg_error_count(), 1);
        for (index, state) in RefineMetadataCandidateState::ALL.into_iter().enumerate() {
            assert_eq!(
                metrics.refine_metadata_candidate_count(state),
                u64::try_from(index + 1).unwrap()
            );
        }
        for phase in PathsearchPhase::ALL {
            assert_eq!(metrics.phase_sample_count(phase), 1);
        }
    }

    #[test]
    fn refine_metadata_subphases_extend_the_phase_vocabulary_uniquely() {
        assert!(PathsearchPhase::ALL.contains(&PathsearchPhase::RefineMetadataSummary));
        assert!(PathsearchPhase::ALL.contains(&PathsearchPhase::RefineMetadataTorrents));
        assert_eq!(
            PathsearchPhase::RefineMetadataSummary.label(),
            "refine_metadata_summary"
        );
        assert_eq!(
            PathsearchPhase::RefineMetadataTorrents.label(),
            "refine_metadata_torrents"
        );

        let mut labels = PathsearchPhase::ALL
            .iter()
            .map(|phase| phase.label())
            .collect::<Vec<_>>();
        let total = labels.len();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(labels.len(), total, "phase labels must stay unique");
    }

    #[test]
    fn refine_metadata_candidate_states_form_a_fixed_unique_vocabulary() {
        assert_eq!(
            RefineMetadataCandidateState::SchemaWithoutBytes.label(),
            "schema_without_bytes"
        );

        let mut labels = RefineMetadataCandidateState::ALL
            .iter()
            .map(|state| state.label())
            .collect::<Vec<_>>();
        assert_eq!(labels.len(), 7);
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(labels.len(), 7, "candidate-state labels must stay unique");
    }

    #[test]
    fn serve_facade_is_observable_without_enabling_c5() {
        let metrics = ServeMetrics::new();
        metrics.set_health(true, 1_700_000_000);
        for outcome in [
            ServeOutcome::Served,
            ServeOutcome::FallbackError,
            ServeOutcome::FallbackEmpty,
            ServeOutcome::FallbackHydrateError,
        ] {
            metrics.inc_serve(outcome);
            assert_eq!(
                metrics
                    .serve_total
                    .with_label_values(&[outcome.label()])
                    .get(),
                1
            );
        }
        assert_eq!(metrics.sidecar_healthy.get(), 1);
        assert_eq!(metrics.watermark_epoch.get(), 1_700_000_000);
    }

    #[test]
    fn registered_collectors_are_exposed_by_common_metrics_layer() {
        let pathsearch = PathsearchMetrics::register();
        let serve = ServeMetrics::register();
        pathsearch.inc_route(RouteResult::Served);
        pathsearch.add_refine_metadata_candidates(RefineMetadataCandidateState::MissingSummary, 3);
        drop(pathsearch.start_phase_timer(PathsearchPhase::RouteTotal));
        serve.inc_serve(ServeOutcome::FallbackEmpty);

        let text = bitmagnet_common::metrics::gather_text();
        assert!(text.lines().any(|line| {
            line == "bitmagnet_search_pathsearch_route_total{result=\"served\"} 1"
        }));
        assert!(text
            .lines()
            .any(|line| { line == "bitmagnet_search_serve_total{outcome=\"fallback_empty\"} 1" }));
        assert!(text
            .lines()
            .any(|line| line == "bitmagnet_search_pathsearch_healthy 0"));
        assert!(text.lines().any(|line| {
            line == "bitmagnet_search_pathsearch_phase_duration_seconds_count{phase=\"route_total\"} 1"
        }));
        assert!(text.lines().any(|line| {
            line == "bitmagnet_search_pathsearch_refine_metadata_candidates_total{state=\"missing_summary\"} 3"
        }));
    }
}
