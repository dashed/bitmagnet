package searchfx

import (
	"context"
	"errors"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/search/pathsearch"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"go.uber.org/zap"
)

// fakeHealthChecker scripts a sequence of HealthCheck responses for the poller.
type fakeHealthChecker struct {
	resp *pb.PathSearchHealth
	err  error
}

func (f *fakeHealthChecker) HealthCheck(context.Context) (*pb.PathSearchHealth, error) {
	return f.resp, f.err
}

func serving(docCount uint64, watermark int64) *pb.PathSearchHealth {
	return &pb.PathSearchHealth{
		Status:         pb.PathSearchHealth_SERVING_STATUS_SERVING,
		DocCount:       docCount,
		WatermarkEpoch: watermark,
	}
}

// newPoller builds the poller's collaborators. The Metrics facade's increments
// are asserted in the pathsearch package's own tests; here we assert the
// route-gating behavior via the public HealthState API.
func newPoller() (*pathsearch.HealthState, *pathsearch.Metrics, *pathsearchPollState, *zap.SugaredLogger) {
	return pathsearch.NewHealthState(), pathsearch.NewMetrics(), &pathsearchPollState{}, zap.NewNop().Sugar()
}

func TestPoll_FirstFailureFailsClosed(t *testing.T) {
	t.Parallel()

	state, metrics, ps, log := newPoller()
	hc := &fakeHealthChecker{err: errors.New("connection refused")}
	cfg := NewDefaultConfig()

	pollPathsearchHealth(context.Background(), hc, state, metrics, cfg, log, ps, 1000)

	if state.Healthy() {
		t.Fatal("first failed poll must leave the route fail-closed (Healthy()==false)")
	}

	if ps.everSucceeded {
		t.Fatal("everSucceeded must stay false after a failing poll")
	}

	if got := state.LastSuccessEpoch(); got != 0 {
		t.Fatalf("LastSuccessEpoch after only-failures = %d, want 0 (misconfig stays 0)", got)
	}
}

func TestPoll_HealthyThenUnhealthyTransition(t *testing.T) {
	t.Parallel()

	state, metrics, ps, log := newPoller()
	cfg := NewDefaultConfig()

	// 1) SERVING + docs → healthy.
	hc := &fakeHealthChecker{resp: serving(500, 900)}
	pollPathsearchHealth(context.Background(), hc, state, metrics, cfg, log, ps, 1000)

	if !state.Healthy() {
		t.Fatal("SERVING + doc_count>0 must be healthy")
	}

	if !ps.everSucceeded {
		t.Fatal("everSucceeded must be true after a successful poll")
	}

	if got := state.LastSuccessEpoch(); got != 1000 {
		t.Fatalf("LastSuccessEpoch = %d, want 1000", got)
	}

	// 2) RPC error → unhealthy, but doc_count + last_success PRESERVED (not zeroed).
	hc.resp, hc.err = nil, errors.New("down")
	pollPathsearchHealth(context.Background(), hc, state, metrics, cfg, log, ps, 1100)

	if state.Healthy() {
		t.Fatal("error after healthy must flip to unhealthy")
	}

	_, doc, _, last := state.Snapshot()
	if doc != 500 {
		t.Fatalf("doc_count after blip = %d, want preserved 500", doc)
	}

	if last != 1000 {
		t.Fatalf("last_success after blip = %d, want preserved 1000", last)
	}
}

func TestPoll_ReachableButNotServing(t *testing.T) {
	t.Parallel()

	state, metrics, ps, log := newPoller()
	cfg := NewDefaultConfig()

	hc := &fakeHealthChecker{resp: &pb.PathSearchHealth{
		Status:   pb.PathSearchHealth_SERVING_STATUS_NOT_SERVING,
		DocCount: 500,
	}}
	pollPathsearchHealth(context.Background(), hc, state, metrics, cfg, log, ps, 1000)

	if state.Healthy() {
		t.Fatal("NOT_SERVING must be unhealthy even with docs")
	}

	if !ps.everSucceeded {
		t.Fatal("a successful RPC (even if not trusted) sets everSucceeded")
	}
}

func TestPoll_ServingButEmptyIndex(t *testing.T) {
	t.Parallel()

	state, metrics, ps, log := newPoller()
	cfg := NewDefaultConfig()

	hc := &fakeHealthChecker{resp: serving(0, 0)} // SERVING but doc_count==0
	pollPathsearchHealth(context.Background(), hc, state, metrics, cfg, log, ps, 1000)

	if state.Healthy() {
		t.Fatal("SERVING with an empty index (doc_count==0) must be unhealthy")
	}
}

func TestPoll_WatermarkFreshnessGate(t *testing.T) {
	t.Parallel()

	state, metrics, ps, log := newPoller()
	cfg := NewDefaultConfig()
	cfg.PathsearchMaxWatermarkLag = 60_000_000_000 // 60s in ns

	// watermark=900, now=1000 → lag 100s > 60s threshold → unhealthy despite SERVING.
	hc := &fakeHealthChecker{resp: serving(500, 900)}
	pollPathsearchHealth(context.Background(), hc, state, metrics, cfg, log, ps, 1000)

	if state.Healthy() {
		t.Fatal("watermark lag beyond the threshold must be unhealthy")
	}

	// Fresh watermark (now-watermark=10s < 60s) → healthy.
	state2, metrics2, ps2, _ := newPoller()
	hc2 := &fakeHealthChecker{resp: serving(500, 990)}
	pollPathsearchHealth(context.Background(), hc2, state2, metrics2, cfg, log, ps2, 1000)

	if !state2.Healthy() {
		t.Fatal("fresh watermark within threshold must be healthy")
	}
}

func TestPoll_FreshnessDisabledByDefault(t *testing.T) {
	t.Parallel()

	state, metrics, ps, log := newPoller()
	cfg := NewDefaultConfig() // PathsearchMaxWatermarkLag == 0 → freshness off

	// Ancient watermark, but the gate is disabled → still healthy.
	hc := &fakeHealthChecker{resp: serving(500, 1)}
	pollPathsearchHealth(context.Background(), hc, state, metrics, cfg, log, ps, 1_000_000)

	if !state.Healthy() {
		t.Fatal("with the freshness gate disabled (default), a stale watermark must NOT mark unhealthy")
	}
}
