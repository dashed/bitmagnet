package pathsearch

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
)

func routeCount(m *Metrics, r RouteResult) float64 {
	return testutil.ToFloat64(m.routes.WithLabelValues(string(r)))
}

// newComposerWithMetrics mirrors newTestComposer but attaches metrics + an
// optional health gate so the route-outcome counters can be asserted.
func newComposerWithMetrics(l3 candidateSource, pg torrentContentSearcher, m *Metrics, gate HealthGate) *Composer {
	opts := []ComposerOption{WithMetrics(m)}
	if gate != nil {
		opts = append(opts, WithHealthGate(gate))
	}

	return NewComposer(l3, pg, ComposerConfig{
		MinQueryLength:   3,
		OversampleFactor: 4,
		MaxCandidates:    1000,
	}, nil, opts...)
}

func TestComposer_Route_ServedCounter(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{
		Candidates: []*pb.PathCandidate{candidate(1)},
		Estimated:  true,
	}}
	pg := &fakePG{result: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		item(1, tf("Inception.2010.1080p.mkv", "mkv", 1)),
	}}}
	c := newComposerWithMetrics(l3, pg, m, nil)

	_, served, err := c.TorrentContent(context.Background(), Filters{Query: "1080p"}, QueryOptions{}, 10, 0, nil)
	if err != nil || !served {
		t.Fatalf("expected served, got served=%v err=%v", served, err)
	}

	if got := routeCount(m, RouteServed); got != 1 {
		t.Fatalf("route_total{served} = %v, want 1", got)
	}
}

func TestComposer_Route_IneligibleCounter(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	l3 := &fakeL3{}
	pg := &fakePG{}
	c := newComposerWithMetrics(l3, pg, m, nil)

	// Too short → ineligible, no L3 dial.
	_, served, err := c.TorrentContent(context.Background(), Filters{Query: "ab"}, QueryOptions{}, 10, 0, nil)
	if err != nil || served {
		t.Fatalf("expected not served, got served=%v err=%v", served, err)
	}

	if got := routeCount(m, RouteIneligible); got != 1 {
		t.Fatalf("route_total{ineligible} = %v, want 1", got)
	}

	if l3.gotQuery != "" {
		t.Fatal("ineligible query must NOT dial L3")
	}
}

func TestComposer_Route_ErrorCounter(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	l3 := &fakeL3{err: errors.New("L3 down")}
	pg := &fakePG{}
	c := newComposerWithMetrics(l3, pg, m, nil)

	_, served, err := c.TorrentContent(context.Background(), Filters{Query: "1080p"}, QueryOptions{}, 10, 0, nil)
	if err == nil || served {
		t.Fatalf("expected error + not served, got served=%v err=%v", served, err)
	}

	if got := routeCount(m, RouteError); got != 1 {
		t.Fatalf("route_total{error} = %v, want 1", got)
	}
}

// TestComposer_Route_TrustEmptyHealthy: zero candidates + healthy gate → trusted
// authoritative empty, counted as served.
func TestComposer_Route_TrustEmptyHealthy(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Estimated: true}} // zero candidates
	pg := &fakePG{}
	c := newComposerWithMetrics(l3, pg, m, func() bool { return true })

	_, served, err := c.TorrentContent(context.Background(), Filters{Query: "1080p"}, QueryOptions{}, 10, 0, nil)
	if err != nil || !served {
		t.Fatalf("healthy zero-candidate must serve authoritative empty, got served=%v err=%v", served, err)
	}

	if got := routeCount(m, RouteServed); got != 1 {
		t.Fatalf("route_total{served} = %v, want 1", got)
	}

	if pg.callCount != 0 {
		t.Fatal("authoritative empty must NOT hit PG")
	}
}

// TestComposer_Route_FailClosedUnhealthy: zero candidates + UNHEALTHY gate → the
// empty is NOT trusted; the composer falls back (served=false) so the call site
// reaches PG. This is the P0-3 fail-closed behavior the poller activates.
func TestComposer_Route_FailClosedUnhealthy(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Estimated: true}} // zero candidates
	pg := &fakePG{}
	c := newComposerWithMetrics(l3, pg, m, func() bool { return false })

	_, served, err := c.TorrentContent(context.Background(), Filters{Query: "1080p"}, QueryOptions{}, 10, 0, nil)
	if err != nil || served {
		t.Fatalf("unhealthy zero-candidate must fall back (served=false), got served=%v err=%v", served, err)
	}

	if got := routeCount(m, RouteFallback); got != 1 {
		t.Fatalf("route_total{fallback} = %v, want 1", got)
	}
}

func TestComposer_Healthy(t *testing.T) {
	t.Parallel()

	// nil composer → false (feature off).
	var nilC *Composer
	if nilC.Healthy() {
		t.Fatal("nil composer Healthy() must be false")
	}

	l3, pg := &fakeL3{}, &fakePG{}

	// No gate wired → healthy (preserves today's behavior: route always attempted).
	if !newComposerWithMetrics(l3, pg, NewMetrics(), nil).Healthy() {
		t.Fatal("composer with no health gate must report Healthy()==true")
	}

	// Gate false → not healthy (route skipped at the call site).
	if newComposerWithMetrics(l3, pg, NewMetrics(), func() bool { return false }).Healthy() {
		t.Fatal("composer with an unhealthy gate must report Healthy()==false")
	}

	// Gate true → healthy.
	if !newComposerWithMetrics(l3, pg, NewMetrics(), func() bool { return true }).Healthy() {
		t.Fatal("composer with a healthy gate must report Healthy()==true")
	}
}
