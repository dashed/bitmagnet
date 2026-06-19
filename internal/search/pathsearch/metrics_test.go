package pathsearch

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetrics_NilSafe(t *testing.T) {
	t.Parallel()

	var m *Metrics
	// None of these may panic on a nil receiver (composer runs without metrics in
	// unit tests).
	m.IncRoute(RouteServed)
	m.IncHealthCheck(true)
	m.SetHealth(true, 1, 2, 3)

	if got := m.Collectors(); got != nil {
		t.Fatalf("nil Metrics.Collectors() = %v, want nil", got)
	}
}

func TestMetrics_IncRoute(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.IncRoute(RouteServed)
	m.IncRoute(RouteServed)
	m.IncRoute(RouteFallback)
	m.IncRoute(RouteIneligible)
	m.IncRoute(RouteError)

	cases := map[RouteResult]float64{
		RouteServed:     2,
		RouteFallback:   1,
		RouteIneligible: 1,
		RouteError:      1,
	}
	for result, want := range cases {
		got := testutil.ToFloat64(m.routes.WithLabelValues(string(result)))
		if got != want {
			t.Errorf("route_total{result=%q} = %v, want %v", result, got, want)
		}
	}
}

func TestMetrics_IncHealthCheck(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.IncHealthCheck(true)
	m.IncHealthCheck(true)
	m.IncHealthCheck(false)

	if got := testutil.ToFloat64(m.healthChecks.WithLabelValues("ok")); got != 2 {
		t.Errorf("health_checks_total{result=ok} = %v, want 2", got)
	}

	if got := testutil.ToFloat64(m.healthChecks.WithLabelValues("error")); got != 1 {
		t.Errorf("health_checks_total{result=error} = %v, want 1", got)
	}
}

func TestMetrics_SetHealth(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.SetHealth(true, 100, 1_700_000_000, 1_700_000_050)

	if got := testutil.ToFloat64(m.healthy); got != 1 {
		t.Errorf("healthy gauge = %v, want 1", got)
	}

	if got := testutil.ToFloat64(m.docCount); got != 100 {
		t.Errorf("doc_count gauge = %v, want 100", got)
	}

	if got := testutil.ToFloat64(m.watermarkEpoch); got != 1_700_000_000 {
		t.Errorf("watermark gauge = %v, want 1700000000", got)
	}

	if got := testutil.ToFloat64(m.lastSuccess); got != 1_700_000_050 {
		t.Errorf("last_success gauge = %v, want 1700000050", got)
	}

	m.SetHealth(false, 100, 1_700_000_000, 1_700_000_050)
	if got := testutil.ToFloat64(m.healthy); got != 0 {
		t.Errorf("healthy gauge after unhealthy = %v, want 0", got)
	}
}

func TestMetricsResult_AllCollectorsRegistered(t *testing.T) {
	t.Parallel()

	// The fx Result must surface every collector the facade owns, or some metric
	// silently never reaches the registry.
	if got := len(NewMetrics().Collectors()); got != 11 {
		t.Fatalf("Metrics owns %d collectors; update MetricsResult to surface them all", got)
	}
}
