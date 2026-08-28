package router

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHealthStateFailsClosedByDefault(t *testing.T) {
	t.Parallel()

	state := NewHealthState()
	assert.False(t, state.ServeEligible())
	assert.Zero(t, state.LastSuccessEpoch())
}

func TestHealthStateNilReceiverFailsClosed(t *testing.T) {
	t.Parallel()

	var state *HealthState
	assert.False(t, state.ServeEligible())
	assert.Zero(t, state.LastSuccessEpoch())

	eligible, docCount, watermark, lastSuccess := state.Snapshot()
	assert.False(t, eligible)
	assert.Zero(t, docCount)
	assert.Zero(t, watermark)
	assert.Zero(t, lastSuccess)

	state.SetHealthy(true, 1, 2, 3)
}

func TestHealthStateSetAndSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	state := NewHealthState()
	state.SetHealthy(true, 42, 1_700_000_000, 1_700_000_100)

	assert.True(t, state.ServeEligible())
	assert.Equal(t, int64(1_700_000_100), state.LastSuccessEpoch())

	eligible, docCount, watermark, lastSuccess := state.Snapshot()
	assert.True(t, eligible)
	assert.Equal(t, int64(42), docCount)
	assert.Equal(t, int64(1_700_000_000), watermark)
	assert.Equal(t, int64(1_700_000_100), lastSuccess)

	state.SetHealthy(false, docCount, watermark, lastSuccess)
	assert.False(t, state.ServeEligible())
}
