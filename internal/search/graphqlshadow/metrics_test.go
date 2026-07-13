package graphqlshadow

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsRegisterAndGather(t *testing.T) {
	t.Parallel()

	m := NewMetrics()

	reg := prometheus.NewRegistry()
	for _, c := range m.Collectors() {
		if err := reg.Register(c); err != nil {
			t.Fatalf("register collector: %v", err)
		}
	}

	// Record one compared query and one drop.
	result := GraphQLResult{
		IDs:            []string{"a"},
		TotalCount:     1,
		Facets:         map[string]FacetCounts{"content_type": {"movie": 1}},
		ObservedFacets: map[string]bool{"content_type": true},
	}
	m.observe(CompareGraphQL(result, result, 2*time.Millisecond, 3*time.Millisecond))
	m.incSampled()
	m.incAdmitted()
	m.incDropped()
	m.incDropped()
	m.incSaturated()

	if got := testutil.ToFloat64(m.comparisonsTotal); got != 1 {
		t.Errorf("comparisons_total = %v, want 1", got)
	}

	if got := testutil.ToFloat64(m.sampledTotal); got != 1 {
		t.Errorf("sampled_total = %v, want 1", got)
	}

	if got := testutil.ToFloat64(m.admittedTotal); got != 1 {
		t.Errorf("admitted_total = %v, want 1", got)
	}

	if got := testutil.ToFloat64(m.droppedTotal); got != 2 {
		t.Errorf("dropped_total = %v, want 2", got)
	}

	if got := testutil.ToFloat64(m.saturatedTotal); got != 1 {
		t.Errorf("saturated_total = %v, want 1", got)
	}

	// Only the one observed facet enters the per-facet denominator. A partial
	// comparison emits no all-facets denominator sample.
	if got := testutil.CollectAndCount(m.facetMatch); got != 1 {
		t.Errorf("facet_match series count = %d, want 1", got)
	}

	if got := testutil.CollectAndCount(m.allFacetsMatch); got != 0 {
		t.Errorf("all_facets_match series count = %d, want 0", got)
	}
}

func TestMetricsEmitAllFacetsOnlyForCompleteUnion(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	complete := GraphQLResult{
		Facets:         make(map[string]FacetCounts, len(FacetKeys)),
		ObservedFacets: make(map[string]bool, len(FacetKeys)),
	}

	for _, key := range FacetKeys {
		complete.Facets[key] = FacetCounts{}
		complete.ObservedFacets[key] = true
	}

	m.observe(CompareGraphQL(complete, complete, time.Millisecond, time.Millisecond))

	if got := testutil.CollectAndCount(m.facetMatch); got != len(FacetKeys) {
		t.Errorf("facet_match series count = %d, want %d", got, len(FacetKeys))
	}

	if got := testutil.ToFloat64(m.allFacetsMatch.WithLabelValues("true")); got != 1 {
		t.Errorf("all_facets_match{matched=true} = %v, want 1", got)
	}
}

// TestMetricsNilSafe confirms every driver-invoked method tolerates a nil
// receiver (the driver may run without metrics).
func TestMetricsNilSafe(t *testing.T) {
	t.Parallel()

	var m *Metrics

	m.observe(GraphQLComparison{})
	m.incDropped()
	m.incSampled()
	m.incAdmitted()
	m.incRustError()
	m.incSaturated()
	m.incReferenceError()

	if m.Collectors() != nil {
		t.Error("nil Metrics.Collectors() should be nil")
	}
}
