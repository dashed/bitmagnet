package graphqlshadow

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
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

func TestMetricsExcludeUndefinedEmptyTop1(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	empty := GraphQLResult{}

	m.observe(CompareGraphQL(empty, empty, time.Millisecond, time.Millisecond))

	// The finite ranking label sets exist at zero from process start so
	// Prometheus increase() can count the first event. Empty/empty must not add a
	// ranking observation to those pre-created children.
	if got := testutil.CollectAndCount(m.jaccard); got != 2 {
		t.Errorf("jaccard series count after empty/empty = %d, want 2 pre-initialized series", got)
	}
	for _, k := range []string{"20", "50"} {
		metric := &dto.Metric{}
		writer, ok := m.jaccard.WithLabelValues(k).(prometheus.Metric)
		if !ok {
			t.Fatalf("jaccard{%s} does not implement prometheus.Metric", k)
		}
		if err := writer.Write(metric); err != nil {
			t.Fatalf("write jaccard{%s}: %v", k, err)
		}
		if got := metric.GetHistogram().GetSampleCount(); got != 0 {
			t.Errorf("jaccard{%s} sample count after empty/empty = %d, want 0", k, got)
		}
	}

	rboMetric := &dto.Metric{}
	if err := m.rbo.Write(rboMetric); err != nil {
		t.Fatalf("write rbo metric: %v", err)
	}

	if got := rboMetric.GetHistogram().GetSampleCount(); got != 0 {
		t.Errorf("rbo sample count after empty/empty = %d, want 0", got)
	}

	if got := testutil.CollectAndCount(m.top1Match); got != 2 {
		t.Fatalf("top1_match series count after empty/empty = %d, want 2 pre-initialized series", got)
	}
	for _, matched := range []string{"true", "false"} {
		if got := testutil.ToFloat64(m.top1Match.WithLabelValues(matched)); got != 0 {
			t.Errorf("top1_match{matched=%s} after empty/empty = %v, want 0", matched, got)
		}
	}

	refOnly := GraphQLResult{IDs: []string{"a"}, TotalCount: 1}
	m.observe(CompareGraphQL(empty, refOnly, time.Millisecond, time.Millisecond))

	if got := testutil.CollectAndCount(m.jaccard); got != 2 {
		t.Errorf("jaccard series count after one-sided empty = %d, want 2", got)
	}

	rboMetric.Reset()
	if err := m.rbo.Write(rboMetric); err != nil {
		t.Fatalf("write rbo metric: %v", err)
	}

	if got := rboMetric.GetHistogram().GetSampleCount(); got != 1 {
		t.Errorf("rbo sample count after one-sided empty = %d, want 1", got)
	}

	if got := testutil.ToFloat64(m.top1Match.WithLabelValues("false")); got != 1 {
		t.Errorf("top1_match{matched=false} after one-sided empty = %v, want 1", got)
	}
}

func TestMetricsInitializeGateLabelSetsAtZero(t *testing.T) {
	t.Parallel()

	m := NewMetrics()

	if got := testutil.CollectAndCount(m.jaccard); got != 2 {
		t.Fatalf("jaccard initialized series = %d, want 2", got)
	}
	if got := testutil.CollectAndCount(m.top1Match); got != 2 {
		t.Fatalf("top1 initialized series = %d, want 2", got)
	}
	if got := testutil.CollectAndCount(m.totalCountMatch); got != 4 {
		t.Fatalf("total_count_match initialized series = %d, want 4", got)
	}

	for _, matched := range []string{"true", "false"} {
		if got := testutil.ToFloat64(m.top1Match.WithLabelValues(matched)); got != 0 {
			t.Errorf("top1_match{matched=%s} = %v, want 0", matched, got)
		}
		for _, estimate := range []string{"true", "false"} {
			if got := testutil.ToFloat64(m.totalCountMatch.WithLabelValues(matched, estimate)); got != 0 {
				t.Errorf("total_count_match{matched=%s,estimate=%s} = %v, want 0", matched, estimate, got)
			}
		}
	}
}

func TestMetricsCountFirstRankingAndExactFailures(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	rust := GraphQLResult{IDs: []string{"rust"}, TotalCount: 2}
	ref := GraphQLResult{IDs: []string{"go"}, TotalCount: 1}
	m.observe(CompareGraphQL(rust, ref, time.Millisecond, time.Millisecond))

	if got := testutil.ToFloat64(m.top1Match.WithLabelValues("false")); got != 1 {
		t.Errorf("first top1 failure = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.top1Match.WithLabelValues("true")); got != 0 {
		t.Errorf("top1 success after first failure = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.totalCountMatch.WithLabelValues("false", "false")); got != 1 {
		t.Errorf("first exact-count failure = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.totalCountMatch.WithLabelValues("true", "false")); got != 0 {
		t.Errorf("exact-count success after first failure = %v, want 0", got)
	}

	metric := &dto.Metric{}
	writer, ok := m.jaccard.WithLabelValues("20").(prometheus.Metric)
	if !ok {
		t.Fatal("jaccard{20} does not implement prometheus.Metric")
	}
	if err := writer.Write(metric); err != nil {
		t.Fatalf("write jaccard{20}: %v", err)
	}
	if got := metric.GetHistogram().GetSampleCount(); got != 1 {
		t.Errorf("first jaccard observation count = %d, want 1", got)
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
