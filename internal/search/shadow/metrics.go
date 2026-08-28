package shadow

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
)

const (
	namespace        = "bitmagnet"
	subsystemShadow  = "search_shadow"
	subsystemTantivy = "search_tantivy"
)

// Metrics holds the Prometheus collectors for search shadow-mode comparisons.
// It is a thin facade: the search router calls Observe for each comparison and
// SetTantivyDocCount from the Tantivy health check. The underlying collectors
// are registered with Prometheus via the fx "prometheus_collectors" group — see
// New and Result. It is nil-safe: methods tolerate a nil receiver.
type Metrics struct {
	jaccard          *prometheus.HistogramVec
	rbo              prometheus.Histogram
	latencyRatio     prometheus.Histogram
	top1Match        *prometheus.CounterVec
	resultCountDelta prometheus.Histogram
	comparisonsTotal prometheus.Counter
	droppedTotal     prometheus.Counter
	tantivyDocCount  prometheus.Gauge
}

// NewMetrics constructs the shadow-mode metrics and their collectors. The
// collectors are not registered here; use Collectors (or the fx Result returned
// by New) to register them.
func NewMetrics() *Metrics {
	return &Metrics{
		jaccard: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystemShadow,
			Name:      "jaccard",
			Help:      "Jaccard set similarity between PostgreSQL and Tantivy top-K result IDs when at least one engine returned an item.",
			Buckets:   prometheus.LinearBuckets(0, 0.1, 11),
		}, []string{"k"}),
		rbo: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystemShadow,
			Name:      "rbo",
			Help:      "Rank-Biased Overlap (p=0.9) between PostgreSQL and Tantivy ranked results when at least one engine returned an item.",
			Buckets:   prometheus.LinearBuckets(0, 0.1, 11),
		}),
		latencyRatio: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystemShadow,
			Name:      "latency_ratio",
			Help:      "Ratio of Tantivy to PostgreSQL query latency (seconds/seconds).",
			Buckets:   prometheus.ExponentialBuckets(0.125, 2, 9),
		}),
		top1Match: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystemShadow,
			Name:      "top1_match_total",
			Help:      "Comparisons with at least one returned item, broken down by whether the top (rank-0) result matched.",
		}, []string{"matched"}),
		resultCountDelta: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystemShadow,
			Name:      "result_count_delta",
			Help:      "Difference in result count between the engines (PostgreSQL minus Tantivy).",
			Buckets:   []float64{-1000, -100, -50, -20, -10, -5, -1, 0, 1, 5, 10, 20, 50, 100, 1000},
		}),
		comparisonsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystemShadow,
			Name:      "comparisons_total",
			Help:      "Total number of shadow-mode comparisons performed.",
		}),
		droppedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystemShadow,
			Name:      "dropped_total",
			Help: "Total number of sampled shadow-mode comparisons dropped because " +
				"the shadow concurrency limit was saturated. Drops are expected back-pressure, not errors.",
		}),
		tantivyDocCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystemTantivy,
			Name:      "doc_count",
			Help:      "Number of documents currently in the Tantivy index (set from the health check).",
		}),
	}
}

// Observe records a single comparison across all relevant collectors.
func (m *Metrics) Observe(c Comparison) {
	if m == nil {
		return
	}

	m.comparisonsTotal.Inc()

	if c.RankingObserved() {
		m.jaccard.WithLabelValues("20").Observe(c.JaccardAt20)
		m.jaccard.WithLabelValues("50").Observe(c.JaccardAt50)
		m.rbo.Observe(c.RBO)
		m.top1Match.WithLabelValues(boolLabel(c.Top1Match)).Inc()
	}

	// The latency ratio is only meaningful with a positive PG baseline.
	if c.PGLatency > 0 {
		m.latencyRatio.Observe(c.TantivyLatency.Seconds() / c.PGLatency.Seconds())
	}

	m.resultCountDelta.Observe(float64(c.PGCount - c.TantivyCount))
}

// IncDropped records one sampled shadow comparison dropped because the
// concurrency limiter was saturated. nil-safe.
func (m *Metrics) IncDropped() {
	if m == nil {
		return
	}

	m.droppedTotal.Inc()
}

// SetTantivyDocCount records the current Tantivy index document count. It is
// expected to be called from the Tantivy health check.
func (m *Metrics) SetTantivyDocCount(count int64) {
	if m == nil {
		return
	}

	m.tantivyDocCount.Set(float64(count))
}

// Collectors returns all Prometheus collectors owned by the metrics, for
// registration with a registry.
func (m *Metrics) Collectors() []prometheus.Collector {
	if m == nil {
		return nil
	}

	return []prometheus.Collector{
		m.jaccard,
		m.rbo,
		m.latencyRatio,
		m.top1Match,
		m.resultCountDelta,
		m.comparisonsTotal,
		m.droppedTotal,
		m.tantivyDocCount,
	}
}

func boolLabel(b bool) string {
	if b {
		return "true"
	}

	return "false"
}

// Result is the fx output for the shadow metrics: the Metrics facade (injected
// into the search router) plus each collector tagged into the shared
// "prometheus_collectors" group for registration.
type Result struct {
	fx.Out

	Metrics *Metrics

	JaccardCollector          prometheus.Collector `group:"prometheus_collectors"`
	RBOCollector              prometheus.Collector `group:"prometheus_collectors"`
	LatencyRatioCollector     prometheus.Collector `group:"prometheus_collectors"`
	Top1MatchCollector        prometheus.Collector `group:"prometheus_collectors"`
	ResultCountDeltaCollector prometheus.Collector `group:"prometheus_collectors"`
	ComparisonsTotalCollector prometheus.Collector `group:"prometheus_collectors"`
	DroppedTotalCollector     prometheus.Collector `group:"prometheus_collectors"`
	DocCountCollector         prometheus.Collector `group:"prometheus_collectors"`
}

// New is the fx provider for the shadow metrics. It returns the Metrics facade
// for injection and each collector tagged into the "prometheus_collectors"
// group for registration.
func New() Result {
	m := NewMetrics()

	return Result{
		Metrics:                   m,
		JaccardCollector:          m.jaccard,
		RBOCollector:              m.rbo,
		LatencyRatioCollector:     m.latencyRatio,
		Top1MatchCollector:        m.top1Match,
		ResultCountDeltaCollector: m.resultCountDelta,
		ComparisonsTotalCollector: m.comparisonsTotal,
		DroppedTotalCollector:     m.droppedTotal,
		DocCountCollector:         m.tantivyDocCount,
	}
}
