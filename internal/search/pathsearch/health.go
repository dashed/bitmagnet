package pathsearch

import "sync/atomic"

// HealthState is the cached, last-known L3 health published by the background
// health poller (see searchfx.registerPathsearchHealthReporter) and read on the
// hot path by the composer's HealthGate. It is safe for concurrent use (lock-free
// atomics) and NEVER performs a blocking RPC — the gate must be cheap.
//
// The zero value is the safe fail-closed default: Healthy() is false until the
// first successful poll flips it true. So at startup — before the first poll — and
// whenever L3 is unreachable/unhealthy, the route fails closed to PostgreSQL
// rather than trusting an un-probed sidecar (finding #4 / P0-3).
type HealthState struct {
	healthy        atomic.Bool
	docCount       atomic.Int64
	watermarkEpoch atomic.Int64
	// lastSuccessEpoch is Unix epoch seconds of the last successful HealthCheck, 0
	// if it has never succeeded (a misconfigured address stays 0 — visible).
	lastSuccessEpoch atomic.Int64
}

// NewHealthState returns a fail-closed HealthState (Healthy() == false).
func NewHealthState() *HealthState {
	return &HealthState{}
}

// Healthy reports the last-known trust decision for the L3 sidecar. It is the
// function wired into the composer via WithHealthGate. A nil receiver reports
// false (fail-closed).
func (h *HealthState) Healthy() bool {
	if h == nil {
		return false
	}

	return h.healthy.Load()
}

// SetHealthy records a fresh health observation from the poller. lastSuccessEpoch
// should be the poll time (epoch seconds) on a successful probe, or left as the
// previously stored value on failure (callers pass the current stored value).
func (h *HealthState) SetHealthy(healthy bool, docCount, watermarkEpoch, lastSuccessEpoch int64) {
	if h == nil {
		return
	}

	h.healthy.Store(healthy)
	h.docCount.Store(docCount)
	h.watermarkEpoch.Store(watermarkEpoch)
	h.lastSuccessEpoch.Store(lastSuccessEpoch)
}

// LastSuccessEpoch returns the epoch seconds of the last successful poll (0 if
// never). The poller reads it to preserve it across a failing poll.
func (h *HealthState) LastSuccessEpoch() int64 {
	if h == nil {
		return 0
	}

	return h.lastSuccessEpoch.Load()
}

// Snapshot returns the cached values for logging / the metrics publish.
func (h *HealthState) Snapshot() (healthy bool, docCount, watermarkEpoch, lastSuccessEpoch int64) {
	if h == nil {
		return false, 0, 0, 0
	}

	return h.healthy.Load(), h.docCount.Load(), h.watermarkEpoch.Load(), h.lastSuccessEpoch.Load()
}
