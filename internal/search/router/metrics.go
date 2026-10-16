package router

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
)

const (
	serveMetricsNamespace = "bitmagnet"
	serveMetricsSubsystem = "search_serve"
)

// ServeMetrics holds the Prometheus collectors for Phase-6 main-search serving
// outcomes and the cached sidecar health gate. It is nil-safe: every method
// tolerates a nil receiver so tests and disabled configurations stay fail-closed
// without special metric plumbing.
type ServeMetrics struct {
	serveTotal     *prometheus.CounterVec
	sidecarHealthy prometheus.Gauge
	watermarkEpoch prometheus.Gauge
}

// NewServeMetrics constructs the serving metrics. The collectors are not
// registered here; use Collectors or NewServeMetricsResult.
func NewServeMetrics() *ServeMetrics {
	return &ServeMetrics{
		serveTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: serveMetricsNamespace,
			Subsystem: serveMetricsSubsystem,
			Name:      "total",
			Help:      "Count of Tantivy main-search serve attempts by outcome (served, fallback_error, fallback_empty, or fallback_hydrate_error).",
		}, []string{"outcome"}),
		sidecarHealthy: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: serveMetricsNamespace,
			Subsystem: serveMetricsSubsystem,
			Name:      "sidecar_healthy",
			Help:      "Whether the main-search sidecar was serve-eligible at the latest health poll (1) or not (0).",
		}),
		watermarkEpoch: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: serveMetricsNamespace,
			Subsystem: serveMetricsSubsystem,
			Name:      "watermark_epoch_seconds",
			Help:      "Last observed main-search follow-loop watermark as Unix epoch seconds.",
		}),
	}
}

// IncServe records one Tantivy serving outcome.
func (m *ServeMetrics) IncServe(outcome string) {
	if m == nil {
		return
	}

	m.serveTotal.WithLabelValues(outcome).Inc()
}

// SetHealth publishes the poller's cached serve decision and last-observed
// watermark to the health gauges.
func (m *ServeMetrics) SetHealth(eligible bool, watermarkEpoch int64) {
	if m == nil {
		return
	}

	healthy := float64(0)
	if eligible {
		healthy = 1
	}

	m.sidecarHealthy.Set(healthy)
	m.watermarkEpoch.Set(float64(watermarkEpoch))
}

// Collectors returns all Prometheus collectors owned by the serving metrics.
func (m *ServeMetrics) Collectors() []prometheus.Collector {
	if m == nil {
		return nil
	}

	return []prometheus.Collector{m.serveTotal, m.sidecarHealthy, m.watermarkEpoch}
}

// ServeMetricsResult is the fx output for serving metrics: the facade used by
// the router and poller plus every collector tagged into the shared Prometheus
// registration group.
type ServeMetricsResult struct {
	fx.Out

	// Metrics is the serving-metrics facade injected into the router and poller.
	Metrics *ServeMetrics
	// ServeTotalCollector registers the serve-outcome counter.
	ServeTotalCollector prometheus.Collector `group:"prometheus_collectors"`
	// SidecarHealthyCollector registers the cached health-gate gauge.
	SidecarHealthyCollector prometheus.Collector `group:"prometheus_collectors"`
	// WatermarkEpochCollector registers the last-observed watermark gauge.
	WatermarkEpochCollector prometheus.Collector `group:"prometheus_collectors"`
}

// NewServeMetricsResult provides the serving-metrics facade and tags all of its
// collectors for registration by the application's shared Prometheus registry.
func NewServeMetricsResult() ServeMetricsResult {
	m := NewServeMetrics()

	return ServeMetricsResult{
		Metrics:                 m,
		ServeTotalCollector:     m.serveTotal,
		SidecarHealthyCollector: m.sidecarHealthy,
		WatermarkEpochCollector: m.watermarkEpoch,
	}
}
