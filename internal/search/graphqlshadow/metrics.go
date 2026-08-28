package graphqlshadow

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
)

const (
	namespace          = "bitmagnet"
	subsystemGraphQL   = "graphql_shadow"
	linearBucketWidth  = 0.1
	linearBucketCount  = 11
	latencyStartSecond = 0.001
)

// Metrics holds the Prometheus collectors for the GraphQL shadow. It is a thin,
// nil-safe facade: the shadow driver records one comparison (or a drop / error)
// per mirrored request, and the emitted graphql_shadow_* series are the feed the
// Phase-2 numeric gate (Lane P, P3) evaluates over the ≥7-day soak. Methods
// tolerate a nil receiver so the driver can run without metrics in tests.
type Metrics struct {
	comparisonsTotal prometheus.Counter
	sampledTotal     prometheus.Counter
	admittedTotal    prometheus.Counter
	droppedTotal     prometheus.Counter
	saturatedTotal   prometheus.Counter
	rustErrorTotal   prometheus.Counter
	refErrorTotal    prometheus.Counter
	jaccard          *prometheus.HistogramVec
	rbo              prometheus.Histogram
	top1Match        *prometheus.CounterVec
	resultCountDelta prometheus.Histogram
	totalCountDelta  prometheus.Histogram
	totalCountMatch  *prometheus.CounterVec
	facetMatch       *prometheus.CounterVec
	allFacetsMatch   *prometheus.CounterVec
	rustLatency      prometheus.Histogram
	refLatency       prometheus.Histogram
	latencyRatio     prometheus.Histogram
}

// NewMetrics constructs the GraphQL-shadow collectors (unregistered).
func NewMetrics() *Metrics {
	similarityBuckets := prometheus.LinearBuckets(0, linearBucketWidth, linearBucketCount)
	countDeltaBuckets := []float64{-1000, -100, -50, -20, -10, -5, -1, 0, 1, 5, 10, 20, 50, 100, 1000}
	latencyBuckets := prometheus.ExponentialBuckets(latencyStartSecond, 2, 14)

	m := &Metrics{
		comparisonsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "comparisons_total",
			Help: "Total number of GraphQL shadow comparisons performed (eligible query operations only).",
		}),
		sampledTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "sampled_total",
			Help: "Comparable GraphQL searches selected by the sampling draw.",
		}),
		admittedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "admitted_total",
			Help: "Sampled GraphQL shadow attempts admitted to the bounded Rust runner.",
		}),
		droppedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "dropped_total",
			Help: "Mirrored requests hard-dropped by the safety gate " +
				"(non-query operations or unclassifiable documents). " +
				"These make ZERO dark Rust calls.",
		}),
		saturatedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "saturated_total",
			Help: "Sampled GraphQL shadow comparisons dropped because the non-blocking concurrency limit was full.",
		}),
		rustErrorTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "rust_error_total",
			Help: "Eligible shadow requests where the dark Rust endpoint failed.",
		}),
		refErrorTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "reference_error_total",
			Help: "Eligible shadow requests whose already-computed Go response could not be projected for comparison.",
		}),
		jaccard: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "jaccard",
			Help:    "Jaccard set similarity between the Go reference and Rust top-K result InferIDs when at least one engine returned an item.",
			Buckets: similarityBuckets,
		}, []string{"k"}),
		rbo: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "rbo",
			Help:    "Rank-Biased Overlap (p=0.9) between the Go reference and Rust ranked results when at least one engine returned an item.",
			Buckets: similarityBuckets,
		}),
		top1Match: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "top1_match_total",
			Help: "Comparisons with at least one returned item, broken down by whether the top (rank-0) result matched.",
		}, []string{"matched"}),
		resultCountDelta: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "result_count_delta",
			Help:    "Difference in returned item count (Go reference minus Rust).",
			Buckets: countDeltaBuckets,
		}),
		totalCountDelta: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "total_count_delta",
			Help:    "Difference in reported totalCount (Go reference minus Rust).",
			Buckets: countDeltaBuckets,
		}),
		totalCountMatch: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "total_count_match_total",
			Help: "Comparisons broken down by whether totalCount matched exactly, " +
				"and whether the total was an estimate.",
		}, []string{"matched", "estimate"}),
		facetMatch: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "facet_match_total",
			Help: "Per-facet comparisons broken down by facet key and whether that " +
				"facet's value→count map matched exactly.",
		}, []string{"facet", "matched"}),
		allFacetsMatch: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "all_facets_match_total",
			Help: "Comparisons broken down by whether all 9 facets matched exactly.",
		}, []string{"matched"}),
		rustLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "rust_latency_seconds",
			Help:    "Dark Rust GraphQL request-entry to pre-write response-generation duration.",
			Buckets: latencyBuckets,
		}),
		refLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "reference_latency_seconds",
			Help:    "Primary Go request-entry to pre-write GraphQL response-generation duration.",
			Buckets: latencyBuckets,
		}),
		latencyRatio: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystemGraphQL, Name: "latency_ratio",
			Help:    "Ratio of Rust to Go reference query latency (seconds/seconds).",
			Buckets: prometheus.ExponentialBuckets(0.125, 2, 9),
		}),
	}

	// CounterVec and HistogramVec children do not exist until their first label
	// value is requested. If the process is scraped only after that first event,
	// Prometheus has no zero baseline and increase() omits the event. Pre-create
	// every finite label set used by the Phase-2 ranking and exact-count gates so
	// a pod's first success OR failure is always visible to fail-closed PromQL.
	for _, k := range []string{"20", "50"} {
		m.jaccard.WithLabelValues(k)
	}
	for _, matched := range []string{"true", "false"} {
		m.top1Match.WithLabelValues(matched)
		for _, estimate := range []string{"true", "false"} {
			m.totalCountMatch.WithLabelValues(matched, estimate)
		}
	}
	for _, facet := range FacetKeys {
		for _, matched := range []string{"true", "false"} {
			m.facetMatch.WithLabelValues(facet, matched)
		}
	}

	return m
}

// observe records a single completed comparison across all collectors. nil-safe.
func (m *Metrics) observe(c GraphQLComparison) {
	if m == nil {
		return
	}

	m.comparisonsTotal.Inc()

	// Empty/empty has no ranked result, so its vacuous similarities are not KPI
	// evidence. One-sided emptiness remains an observed mismatch.
	if c.RankingObserved() {
		m.jaccard.WithLabelValues("20").Observe(c.JaccardAt20)
		m.jaccard.WithLabelValues("50").Observe(c.JaccardAt50)
		m.rbo.Observe(c.RBO)
		m.top1Match.WithLabelValues(boolLabel(c.Top1Match)).Inc()
	}

	m.resultCountDelta.Observe(float64(c.PGCount - c.TantivyCount))
	m.totalCountDelta.Observe(float64(c.TotalCountDelta))
	m.totalCountMatch.WithLabelValues(boolLabel(c.TotalCountMatch), boolLabel(c.TotalCountIsEstimate)).Inc()

	for _, key := range FacetKeys {
		if !c.FacetObserved[key] {
			continue
		}

		m.facetMatch.WithLabelValues(key, boolLabel(c.FacetMatch[key])).Inc()
	}

	if c.AllFacetsObserved {
		m.allFacetsMatch.WithLabelValues(boolLabel(c.AllFacetsMatch)).Inc()
	}

	rustSeconds := c.TantivyLatency.Seconds()
	refSeconds := c.PGLatency.Seconds()

	m.rustLatency.Observe(rustSeconds)
	m.refLatency.Observe(refSeconds)

	if refSeconds > 0 {
		m.latencyRatio.Observe(rustSeconds / refSeconds)
	}
}

// incDropped records one mirrored request hard-dropped by the safety gate. This
// counter's whole purpose is to make the guard observable: every increment is a
// request that made ZERO dark Rust calls. nil-safe.
func (m *Metrics) incDropped() {
	if m == nil {
		return
	}

	m.droppedTotal.Inc()
}

func (m *Metrics) incSampled() {
	if m == nil {
		return
	}

	m.sampledTotal.Inc()
}

func (m *Metrics) incAdmitted() {
	if m == nil {
		return
	}

	m.admittedTotal.Inc()
}

func (m *Metrics) incRustError() {
	if m == nil {
		return
	}

	m.rustErrorTotal.Inc()
}

func (m *Metrics) incSaturated() {
	if m == nil {
		return
	}

	m.saturatedTotal.Inc()
}

func (m *Metrics) incReferenceError() {
	if m == nil {
		return
	}

	m.refErrorTotal.Inc()
}

// Collectors returns all Prometheus collectors owned by the metrics.
func (m *Metrics) Collectors() []prometheus.Collector {
	if m == nil {
		return nil
	}

	return []prometheus.Collector{
		m.comparisonsTotal, m.sampledTotal, m.admittedTotal, m.droppedTotal,
		m.saturatedTotal, m.rustErrorTotal, m.refErrorTotal,
		m.jaccard, m.rbo, m.top1Match, m.resultCountDelta, m.totalCountDelta,
		m.totalCountMatch, m.facetMatch, m.allFacetsMatch,
		m.rustLatency, m.refLatency, m.latencyRatio,
	}
}

func boolLabel(b bool) string {
	if b {
		return "true"
	}

	return "false"
}

// Result is the fx output for the GraphQL-shadow metrics: the Metrics facade
// (injected into the shadow driver) plus each collector tagged into the shared
// "prometheus_collectors" group for registration. Collectors are enumerated one
// per field to match the sibling shadow package's convention.
type Result struct {
	fx.Out

	Metrics *Metrics

	ComparisonsTotalCollector prometheus.Collector `group:"prometheus_collectors"`
	SampledTotalCollector     prometheus.Collector `group:"prometheus_collectors"`
	AdmittedTotalCollector    prometheus.Collector `group:"prometheus_collectors"`
	DroppedTotalCollector     prometheus.Collector `group:"prometheus_collectors"`
	SaturatedTotalCollector   prometheus.Collector `group:"prometheus_collectors"`
	RustErrorTotalCollector   prometheus.Collector `group:"prometheus_collectors"`
	RefErrorTotalCollector    prometheus.Collector `group:"prometheus_collectors"`
	JaccardCollector          prometheus.Collector `group:"prometheus_collectors"`
	RBOCollector              prometheus.Collector `group:"prometheus_collectors"`
	Top1MatchCollector        prometheus.Collector `group:"prometheus_collectors"`
	ResultCountDeltaCollector prometheus.Collector `group:"prometheus_collectors"`
	TotalCountDeltaCollector  prometheus.Collector `group:"prometheus_collectors"`
	TotalCountMatchCollector  prometheus.Collector `group:"prometheus_collectors"`
	FacetMatchCollector       prometheus.Collector `group:"prometheus_collectors"`
	AllFacetsMatchCollector   prometheus.Collector `group:"prometheus_collectors"`
	RustLatencyCollector      prometheus.Collector `group:"prometheus_collectors"`
	RefLatencyCollector       prometheus.Collector `group:"prometheus_collectors"`
	LatencyRatioCollector     prometheus.Collector `group:"prometheus_collectors"`
}

// New is the fx provider for the GraphQL-shadow metrics.
func New() Result {
	m := NewMetrics()

	return Result{
		Metrics:                   m,
		ComparisonsTotalCollector: m.comparisonsTotal,
		SampledTotalCollector:     m.sampledTotal,
		AdmittedTotalCollector:    m.admittedTotal,
		DroppedTotalCollector:     m.droppedTotal,
		SaturatedTotalCollector:   m.saturatedTotal,
		RustErrorTotalCollector:   m.rustErrorTotal,
		RefErrorTotalCollector:    m.refErrorTotal,
		JaccardCollector:          m.jaccard,
		RBOCollector:              m.rbo,
		Top1MatchCollector:        m.top1Match,
		ResultCountDeltaCollector: m.resultCountDelta,
		TotalCountDeltaCollector:  m.totalCountDelta,
		TotalCountMatchCollector:  m.totalCountMatch,
		FacetMatchCollector:       m.facetMatch,
		AllFacetsMatchCollector:   m.allFacetsMatch,
		RustLatencyCollector:      m.rustLatency,
		RefLatencyCollector:       m.refLatency,
		LatencyRatioCollector:     m.latencyRatio,
	}
}
