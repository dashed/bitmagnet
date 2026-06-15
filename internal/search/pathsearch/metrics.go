package pathsearch

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
)

const (
	namespace           = "bitmagnet"
	subsystemPathsearch = "search_pathsearch"
)

// RouteResult labels the outcome of an L3-routed query for the route counter.
// These let the gate-7 harness prove L3 was actually exercised (served > 0) and
// that eligible queries did not silently fall back (fallback ≈ 0).
type RouteResult string

const (
	// RouteServed: the composer answered the query from L3 candidates + exact
	// refine (including a trusted authoritative-empty). This is the counter the
	// gate-7 harness asserts MUST move — a flat served counter means L3 was never
	// reached (misconfig / silent PG fallback).
	RouteServed RouteResult = "served"
	// RouteFallback: the route was taken but the composer returned served=false
	// for a non-error reason (zero candidates while L3 unhealthy, or a candidate's
	// files were unobtainable) → the call site falls back to PostgreSQL.
	RouteFallback RouteResult = "fallback"
	// RouteIneligible: the query was ineligible for L3 (empty substring / shorter
	// than MinQueryLength) → the composer declined without dialing L3.
	RouteIneligible RouteResult = "ineligible"
	// RouteError: a hard error (L3 RPC error, or the candidate PG hydrate failed)
	// → the call site falls back to PostgreSQL and surfaces/ logs the error.
	RouteError RouteResult = "error"
)

// Metrics holds the Prometheus collectors for the L3 pathsearch route + health
// poller. It mirrors shadow.Metrics: a thin facade whose collectors are
// registered via the fx "prometheus_collectors" group (see Result/New). It is
// nil-safe — every method tolerates a nil receiver so the composer can run
// without metrics in unit tests.
type Metrics struct {
	docCount       prometheus.Gauge
	healthy        prometheus.Gauge
	watermarkEpoch prometheus.Gauge
	lastSuccess    prometheus.Gauge
	healthChecks   *prometheus.CounterVec
	routes         *prometheus.CounterVec
	refineDeclined prometheus.Counter
	retainedCapped prometheus.Counter
	deadlineCapped prometheus.Counter
	refineShed     prometheus.Counter
}

// NewMetrics constructs the pathsearch metrics + their collectors. The collectors
// are not registered here; use Collectors (or the fx Result from New).
func NewMetrics() *Metrics {
	return &Metrics{
		docCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystemPathsearch,
			Name:      "doc_count",
			Help:      "Number of documents in the L3 pathsearch index (from the health poller). 0 when unreachable.",
		}),
		healthy: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystemPathsearch,
			Name:      "healthy",
			Help:      "Whether the L3 pathsearch sidecar is currently trusted (1) or not (0), per the health poller's gate definition (SERVING + doc_count>0 [+ fresh]).",
		}),
		watermarkEpoch: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystemPathsearch,
			Name:      "watermark_epoch_seconds",
			Help:      "L3 follow-loop watermark as Unix epoch seconds (0 when follow is off / no tick yet).",
		}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystemPathsearch,
			Name:      "last_success_epoch_seconds",
			Help:      "Unix epoch seconds of the last successful L3 HealthCheck (0 if it has never succeeded — a misconfigured address stays 0).",
		}),
		healthChecks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystemPathsearch,
			Name:      "health_checks_total",
			Help:      "Count of L3 HealthCheck polls by outcome (ok|error).",
		}, []string{"result"}),
		routes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystemPathsearch,
			Name:      "route_total",
			Help:      "Count of L3-routed queries by outcome (served|fallback|ineligible|error).",
		}, []string{"result"}),
		refineDeclined: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystemPathsearch,
			Name:      "refine_declined_oversized_total",
			Help:      "Count of L3 path-search candidates EXCLUDED from exact-refine because their file_count exceeded the per-torrent sanity cap (MaxRefineFiles), either pre-decode (summary count) or post-decode (actual fileset). A fail-loud byte-bound (gate7-4): the candidate is dropped + logged rather than retained; the rest of the candidate set is still served.",
		}),
		retainedCapped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystemPathsearch,
			Name:      "refine_retained_capped_total",
			Help:      "Count of L3 path-search requests whose refined match set hit the cumulative RetainedFileBudget (gate7-4 robust byte-bound) and were served as a memory-capped top-relevance estimate instead of accumulating every matched fileset. Should be ~0 for normal/selective queries; a non-zero rate flags broad/high-match-rate queries hitting the retained bound.",
		}),
		deadlineCapped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystemPathsearch,
			Name:      "refine_deadline_capped_total",
			Help:      "Count of L3 path-search requests whose route exceeded the PathsearchRouteTimeout mid-refine (gate7-6 CPU/latency bound) and were served as a deadline-capped top-relevance estimate (the accumulated L3-ordered prefix) instead of running unbounded. Crucially this is NOT a PostgreSQL fallback: the route NEVER re-runs the broad-FTS PG path on deadline. Should be ~0 for normal/selective queries; a non-zero rate flags pathological whole-dir queries hitting the route deadline.",
		}),
		refineShed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystemPathsearch,
			Name:      "refine_shed_total",
			Help:      "Count of L3 path-search requests SHED because no refine concurrency slot (MaxConcurrentRefines) was available within PathsearchSlotWait/the route deadline (gate7-6 load-shedding). Only the EXPENSIVE multi-chunk path acquires a slot, so a saturated limiter never sheds a normal/selective (single-chunk) query. A shed request is served fail-loud as an empty estimate, NEVER a PostgreSQL broad-FTS fallback. A non-zero rate flags a concurrent burst of pathological whole-dir queries.",
		}),
	}
}

// SetHealth publishes the cached health snapshot to the gauges. healthy is the
// gate decision; docCount/watermarkEpoch are the last-observed sidecar values;
// lastSuccessEpoch is 0 until the first successful poll.
func (m *Metrics) SetHealth(healthy bool, docCount, watermarkEpoch, lastSuccessEpoch int64) {
	if m == nil {
		return
	}

	m.docCount.Set(float64(docCount))
	m.healthy.Set(boolGauge(healthy))
	m.watermarkEpoch.Set(float64(watermarkEpoch))
	m.lastSuccess.Set(float64(lastSuccessEpoch))
}

// IncHealthCheck records a single HealthCheck poll outcome.
func (m *Metrics) IncHealthCheck(ok bool) {
	if m == nil {
		return
	}

	result := "error"
	if ok {
		result = "ok"
	}

	m.healthChecks.WithLabelValues(result).Inc()
}

// IncRoute records a single L3-route outcome.
func (m *Metrics) IncRoute(result RouteResult) {
	if m == nil {
		return
	}

	m.routes.WithLabelValues(string(result)).Inc()
}

// IncRefineDeclinedOversized records one candidate excluded from exact-refine by
// the per-torrent file-count sanity cap (gate7-4 byte-bound). nil-safe.
func (m *Metrics) IncRefineDeclinedOversized() {
	if m == nil {
		return
	}

	m.refineDeclined.Inc()
}

// IncRefineRetainedCapped records one request served as a memory-capped estimate
// because its refined match set hit the cumulative RetainedFileBudget (gate7-4).
// nil-safe.
func (m *Metrics) IncRefineRetainedCapped() {
	if m == nil {
		return
	}

	m.retainedCapped.Inc()
}

// IncRefineDeadlineCapped records one request served as a deadline-capped estimate
// because the route exceeded PathsearchRouteTimeout mid-refine (gate7-6). It is
// the deadline analogue of IncRefineRetainedCapped: both serve the accumulated
// top-relevance prefix, never a PG fallback. nil-safe.
func (m *Metrics) IncRefineDeadlineCapped() {
	if m == nil {
		return
	}

	m.deadlineCapped.Inc()
}

// IncRefineShed records one request shed because no refine concurrency slot was
// available (gate7-6 load-shedding). nil-safe.
func (m *Metrics) IncRefineShed() {
	if m == nil {
		return
	}

	m.refineShed.Inc()
}

// Collectors returns all Prometheus collectors owned by the metrics.
func (m *Metrics) Collectors() []prometheus.Collector {
	if m == nil {
		return nil
	}

	return []prometheus.Collector{
		m.docCount,
		m.healthy,
		m.watermarkEpoch,
		m.lastSuccess,
		m.healthChecks,
		m.routes,
		m.refineDeclined,
		m.retainedCapped,
		m.deadlineCapped,
		m.refineShed,
	}
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}

	return 0
}

// MetricsResult is the fx output for the pathsearch metrics: the Metrics facade
// (injected into the composer + the health poller) plus each collector tagged
// into the shared "prometheus_collectors" group for registration. Mirrors
// shadow.Result.
type MetricsResult struct {
	fx.Out

	Metrics *Metrics

	DocCountCollector     prometheus.Collector `group:"prometheus_collectors"`
	HealthyCollector      prometheus.Collector `group:"prometheus_collectors"`
	WatermarkCollector    prometheus.Collector `group:"prometheus_collectors"`
	LastSuccessCollector  prometheus.Collector `group:"prometheus_collectors"`
	HealthChecksCollector prometheus.Collector `group:"prometheus_collectors"`
	RouteCollector        prometheus.Collector `group:"prometheus_collectors"`
	RefineDeclinedColl    prometheus.Collector `group:"prometheus_collectors"`
	RetainedCappedColl    prometheus.Collector `group:"prometheus_collectors"`
	DeadlineCappedColl    prometheus.Collector `group:"prometheus_collectors"`
	RefineShedColl        prometheus.Collector `group:"prometheus_collectors"`
}

// NewMetricsResult is the fx provider for the pathsearch metrics. It returns the
// Metrics facade for injection and each collector tagged into the
// "prometheus_collectors" group for registration.
func NewMetricsResult() MetricsResult {
	m := NewMetrics()

	return MetricsResult{
		Metrics:               m,
		DocCountCollector:     m.docCount,
		HealthyCollector:      m.healthy,
		WatermarkCollector:    m.watermarkEpoch,
		LastSuccessCollector:  m.lastSuccess,
		HealthChecksCollector: m.healthChecks,
		RouteCollector:        m.routes,
		RefineDeclinedColl:    m.refineDeclined,
		RetainedCappedColl:    m.retainedCapped,
		DeadlineCappedColl:    m.deadlineCapped,
		RefineShedColl:        m.refineShed,
	}
}
