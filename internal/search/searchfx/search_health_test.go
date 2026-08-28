package searchfx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/search/router"
	"github.com/bitmagnet-io/bitmagnet/internal/search/shadow"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type fakeSearchHealthChecker struct {
	resp *pb.HealthCheckResponse
	err  error
}

func (f *fakeSearchHealthChecker) HealthCheck(context.Context) (*pb.HealthCheckResponse, error) {
	return f.resp, f.err
}

func mainSearchServing(docCount uint64, watermark int64) *pb.HealthCheckResponse {
	return &pb.HealthCheckResponse{
		Status:         pb.HealthCheckResponse_SERVING_STATUS_SERVING,
		DocCount:       docCount,
		WatermarkEpoch: watermark,
	}
}

func newSearchPoller() (
	*router.HealthState,
	*shadow.Metrics,
	*router.ServeMetrics,
	*searchPollState,
	*zap.SugaredLogger,
) {
	return router.NewHealthState(), shadow.NewMetrics(), router.NewServeMetrics(),
		&searchPollState{}, zap.NewNop().Sugar()
}

func TestPollSearchHealthServingFreshIsEligible(t *testing.T) {
	t.Parallel()

	state, shadowMetrics, serveMetrics, ps, logger := newSearchPoller()
	cfg := NewDefaultConfig()
	hc := &fakeSearchHealthChecker{resp: mainSearchServing(500, 950)}

	pollSearchHealth(
		context.Background(), hc, state, shadowMetrics, serveMetrics, cfg, logger, ps, 1000,
	)

	assert.True(t, state.ServeEligible())
	assert.True(t, ps.everSucceeded)

	eligible, docCount, watermark, lastSuccess := state.Snapshot()
	assert.True(t, eligible)
	assert.Equal(t, int64(500), docCount)
	assert.Equal(t, int64(950), watermark)
	assert.Equal(t, int64(1000), lastSuccess)

	collectors := shadowMetrics.Collectors()
	require.NotEmpty(t, collectors)
	assert.InDelta(t, float64(500), testutil.ToFloat64(collectors[len(collectors)-1]), 0)
}

func TestPollSearchHealthFreshnessDenials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		maxStaleness time.Duration
		watermark    int64
	}{
		{name: "stale", maxStaleness: time.Minute, watermark: 900},
		{name: "missing watermark", maxStaleness: time.Minute, watermark: 0},
		{name: "disabled bound", maxStaleness: 0, watermark: 990},
		{name: "negative bound", maxStaleness: -time.Second, watermark: 990},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state, shadowMetrics, serveMetrics, ps, logger := newSearchPoller()
			cfg := NewDefaultConfig()
			cfg.MaxStaleness = tt.maxStaleness
			hc := &fakeSearchHealthChecker{resp: mainSearchServing(500, tt.watermark)}

			pollSearchHealth(
				context.Background(), hc, state, shadowMetrics, serveMetrics, cfg, logger, ps, 1000,
			)

			assert.False(t, state.ServeEligible())
			assert.True(t, ps.everSucceeded, "the RPC succeeded even though serving was denied")
		})
	}
}

func TestPollSearchHealthFirstFailureFailsClosed(t *testing.T) {
	t.Parallel()

	state, shadowMetrics, serveMetrics, ps, logger := newSearchPoller()
	hc := &fakeSearchHealthChecker{err: errors.New("connection refused")}

	pollSearchHealth(
		context.Background(), hc, state, shadowMetrics, serveMetrics,
		NewDefaultConfig(), logger, ps, 1000,
	)

	assert.False(t, state.ServeEligible())
	assert.False(t, ps.everSucceeded, "a failed first RPC must not mark the poller successful")
	assert.Zero(t, state.LastSuccessEpoch())
	assert.NotNil(t, ps.lastEligible, "the first failure must be recorded for change-only logging")
	assert.False(t, *ps.lastEligible)
}

func TestPollSearchHealthErrorPreservesLastKnown(t *testing.T) {
	t.Parallel()

	state, shadowMetrics, serveMetrics, ps, logger := newSearchPoller()
	cfg := NewDefaultConfig()
	hc := &fakeSearchHealthChecker{resp: mainSearchServing(500, 950)}
	pollSearchHealth(
		context.Background(), hc, state, shadowMetrics, serveMetrics, cfg, logger, ps, 1000,
	)

	hc.resp = nil
	hc.err = errors.New("sidecar down")
	pollSearchHealth(
		context.Background(), hc, state, shadowMetrics, serveMetrics, cfg, logger, ps, 1100,
	)

	eligible, docCount, watermark, lastSuccess := state.Snapshot()
	assert.False(t, eligible)
	assert.Equal(t, int64(500), docCount)
	assert.Equal(t, int64(950), watermark)
	assert.Equal(t, int64(1000), lastSuccess)
	assert.True(t, ps.everSucceeded)

	collectors := shadowMetrics.Collectors()
	require.NotEmpty(t, collectors)
	assert.InDelta(t, float64(500), testutil.ToFloat64(collectors[len(collectors)-1]), 0)
}

func TestPollSearchHealthNotServingIsIneligible(t *testing.T) {
	t.Parallel()

	state, shadowMetrics, serveMetrics, ps, logger := newSearchPoller()
	hc := &fakeSearchHealthChecker{resp: &pb.HealthCheckResponse{
		Status:         pb.HealthCheckResponse_SERVING_STATUS_NOT_SERVING,
		DocCount:       500,
		WatermarkEpoch: 990,
	}}

	pollSearchHealth(
		context.Background(), hc, state, shadowMetrics, serveMetrics,
		NewDefaultConfig(), logger, ps, 1000,
	)

	assert.False(t, state.ServeEligible())
	assert.True(t, ps.everSucceeded)
}
