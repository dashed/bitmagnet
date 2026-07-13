//! Canonical composer and dormant Tantivy-serve Prometheus metrics.
//!
//! The names and label sets in this module are the Phase-0 metric-name golden
//! contract. Collectors register with [`bitmagnet_common::metrics::registry`],
//! so they are exposed by the process-wide metrics server shared by every Rust
//! service. [`PathsearchMetrics::new`] and [`ServeMetrics::new`] deliberately
//! leave collectors unregistered for deterministic unit tests.

const PATH_PREFIX: &str = "bitmagnet_search_pathsearch";
const SERVE_PREFIX: &str = "bitmagnet_search_serve";

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
#[derive(Clone)]
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
                "Requests served as an estimate after reaching the retained-file budget.",
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

    /// Records one request stopped by the cumulative retained-file cap.
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

#[cfg(test)]
mod tests {
    use super::*;

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

        let expected = [
            "bitmagnet_search_pathsearch_doc_count{}",
            "bitmagnet_search_pathsearch_health_checks_total{result}",
            "bitmagnet_search_pathsearch_healthy{}",
            "bitmagnet_search_pathsearch_last_success_epoch_seconds{}",
            "bitmagnet_search_pathsearch_refine_agg_error_total{}",
            "bitmagnet_search_pathsearch_refine_deadline_capped_total{}",
            "bitmagnet_search_pathsearch_refine_declined_oversized_total{}",
            "bitmagnet_search_pathsearch_refine_retained_capped_total{}",
            "bitmagnet_search_pathsearch_refine_shed_total{}",
            "bitmagnet_search_pathsearch_route_total{result}",
            "bitmagnet_search_pathsearch_watermark_epoch_seconds{}",
            "bitmagnet_search_serve_sidecar_healthy{}",
            "bitmagnet_search_serve_total{outcome}",
            "bitmagnet_search_serve_watermark_epoch_seconds{}",
        ];

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
    }
}
