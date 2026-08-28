package shadow

import (
	"time"

	"go.uber.org/zap"
)

// LogComparison emits a structured log line for a shadow-mode comparison when
// logDiscrepancies is enabled and the comparison actually represents a
// discrepancy (see Comparison.IsDiscrepancy). Comparisons where the engines
// agree at the head and on result count are not logged, keeping the log signal
// focused on divergences. A nil logger is tolerated as a no-op.
func LogComparison(logger *zap.SugaredLogger, query string, c Comparison, logDiscrepancies bool) {
	if logger == nil || !logDiscrepancies || !c.IsDiscrepancy() {
		return
	}

	logger.Infow("search shadow discrepancy",
		"query", query,
		"pg_latency_ms", durationMs(c.PGLatency),
		"tantivy_latency_ms", durationMs(c.TantivyLatency),
		"pg_count", c.PGCount,
		"tantivy_count", c.TantivyCount,
		"jaccard@20", c.JaccardAt20,
		"jaccard@50", c.JaccardAt50,
		"rbo", c.RBO,
		"top1", c.Top1Match,
	)
}

// durationMs converts a duration to fractional milliseconds (microsecond
// resolution) so that sub-millisecond latencies remain visible.
func durationMs(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}
