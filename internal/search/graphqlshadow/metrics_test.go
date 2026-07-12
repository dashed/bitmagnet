package graphqlshadow

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsRegisterAndGather(t *testing.T) {
	m := NewMetrics()

	reg := prometheus.NewRegistry()
	for _, c := range m.Collectors() {
		if err := reg.Register(c); err != nil {
			t.Fatalf("register collector: %v", err)
		}
	}

	// Record one compared query and one drop.
	m.observe(CompareGraphQL(
		GraphQLResult{IDs: []string{"a"}, TotalCount: 1, Facets: map[string]FacetCounts{"content_type": {"movie": 1}}},
		GraphQLResult{IDs: []string{"a"}, TotalCount: 1, Facets: map[string]FacetCounts{"content_type": {"movie": 1}}},
		2*time.Millisecond, 3*time.Millisecond,
	))
	m.incDropped()
	m.incDropped()

	if got := testutil.ToFloat64(m.comparisonsTotal); got != 1 {
		t.Errorf("comparisons_total = %v, want 1", got)
	}

	if got := testutil.ToFloat64(m.droppedTotal); got != 2 {
		t.Errorf("dropped_total = %v, want 2", got)
	}

	// all 9 facets get a facet_match observation per comparison.
	if got := testutil.CollectAndCount(m.facetMatch); got != len(FacetKeys) {
		t.Errorf("facet_match series count = %d, want %d", got, len(FacetKeys))
	}
}

// TestMetricsNilSafe confirms every driver-invoked method tolerates a nil
// receiver (the driver may run without metrics).
func TestMetricsNilSafe(t *testing.T) {
	var m *Metrics

	m.observe(GraphQLComparison{})
	m.incDropped()
	m.incRustError()
	m.incReferenceError()

	if m.Collectors() != nil {
		t.Error("nil Metrics.Collectors() should be nil")
	}
}
