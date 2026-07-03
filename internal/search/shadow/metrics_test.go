package shadow

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestMetricsObserve(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	reg := prometheus.NewRegistry()

	for i, c := range m.Collectors() {
		require.NoErrorf(t, reg.Register(c), "register collector %d", i)
	}

	m.Observe(Comparison{
		JaccardAt20:    1.0,
		JaccardAt50:    1.0,
		RBO:            1.0,
		Top1Match:      true,
		PGCount:        10,
		TantivyCount:   10,
		PGLatency:      2 * time.Millisecond,
		TantivyLatency: 1 * time.Millisecond,
	})
	m.Observe(Comparison{
		JaccardAt20:    0.5,
		JaccardAt50:    0.4,
		RBO:            0.6,
		Top1Match:      false,
		PGCount:        5,
		TantivyCount:   8,
		PGLatency:      3 * time.Millisecond,
		TantivyLatency: 6 * time.Millisecond,
	})

	assert.InDelta(t, 2.0, testutil.ToFloat64(m.comparisonsTotal), epsilon)
	assert.InDelta(t, 1.0, testutil.ToFloat64(m.top1Match.WithLabelValues("true")), epsilon)
	assert.InDelta(t, 1.0, testutil.ToFloat64(m.top1Match.WithLabelValues("false")), epsilon)
	// The jaccard HistogramVec should have one series per label value ("20","50").
	assert.Equal(t, 2, testutil.CollectAndCount(m.jaccard))
}

func TestMetricsIncDropped(t *testing.T) {
	t.Parallel()

	m := NewMetrics()

	m.IncDropped()
	m.IncDropped()

	assert.InDelta(t, 2.0, testutil.ToFloat64(m.droppedTotal), epsilon)
}

func TestMetricsSetTantivyDocCount(t *testing.T) {
	t.Parallel()

	m := NewMetrics()

	m.SetTantivyDocCount(1234)
	assert.InDelta(t, 1234.0, testutil.ToFloat64(m.tantivyDocCount), epsilon)

	m.SetTantivyDocCount(42)
	assert.InDelta(t, 42.0, testutil.ToFloat64(m.tantivyDocCount), epsilon)
}

// TestNewResultRegisters verifies the fx Result exposes non-nil collectors that
// register cleanly (no duplicate fully-qualified names).
func TestNewResultRegisters(t *testing.T) {
	t.Parallel()

	r := New()
	require.NotNil(t, r.Metrics)

	collectors := []prometheus.Collector{
		r.JaccardCollector,
		r.RBOCollector,
		r.LatencyRatioCollector,
		r.Top1MatchCollector,
		r.ResultCountDeltaCollector,
		r.ComparisonsTotalCollector,
		r.DroppedTotalCollector,
		r.DocCountCollector,
	}
	reg := prometheus.NewRegistry()

	for i, c := range collectors {
		require.NotNilf(t, c, "collector %d", i)
		require.NoErrorf(t, reg.Register(c), "register collector %d", i)
	}
}

func TestLogComparison(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core).Sugar()

	// Disabled: nothing is logged even for a discrepancy.
	LogComparison(logger, "q", Comparison{Top1Match: false, JaccardAt20: 0.5}, false)
	assert.Zero(t, logs.Len())

	// Enabled with a discrepancy: one entry.
	LogComparison(logger, "q", Comparison{
		Top1Match:    false,
		JaccardAt20:  0.5,
		PGCount:      3,
		TantivyCount: 2,
	}, true)
	assert.Equal(t, 1, logs.Len())

	// Enabled but no discrepancy: still just the one entry (perfect match must
	// not log).
	LogComparison(logger, "q", Comparison{
		Top1Match:    true,
		JaccardAt20:  1.0,
		JaccardAt50:  1.0,
		RBO:          1.0,
		PGCount:      3,
		TantivyCount: 3,
	}, true)
	assert.Equal(t, 1, logs.Len())

	// Nil logger is a no-op (must not panic).
	LogComparison(nil, "q", Comparison{Top1Match: false}, true)
}
