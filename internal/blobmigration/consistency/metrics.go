package consistency

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	ChecksTotal   prometheus.Counter
	ErrorsTotal   prometheus.Counter
	LastCheckAt   prometheus.Gauge
	LastErrorAt   prometheus.Gauge
}

func NewMetrics() *Metrics {
	return &Metrics{
		ChecksTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bitmagnet",
			Subsystem: "blob_consistency",
			Name:      "checks_total",
			Help:      "Total number of blob consistency checks performed.",
		}),
		ErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bitmagnet",
			Subsystem: "blob_consistency",
			Name:      "errors_total",
			Help:      "Total number of blob consistency errors detected.",
		}),
		LastCheckAt: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bitmagnet",
			Subsystem: "blob_consistency",
			Name:      "last_check_at",
			Help:      "Unix timestamp of the last consistency check.",
		}),
		LastErrorAt: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bitmagnet",
			Subsystem: "blob_consistency",
			Name:      "last_error_at",
			Help:      "Unix timestamp of the last consistency error (0 if no errors).",
		}),
	}
}

func (m *Metrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.ChecksTotal,
		m.ErrorsTotal,
		m.LastCheckAt,
		m.LastErrorAt,
	}
}
