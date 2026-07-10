package router

import "sync/atomic"

// HealthState is the cached, last-known Tantivy main-search health published by
// the background poller (searchfx.registerSearchHealthReporter) and read on the
// hot path by the router's SERVE eligibility gate. It is lock-free and never
// performs a blocking RPC. The zero value is fail-closed (ServeEligible()==false)
// until the first successful poll flips it. Freshness is folded into the stored
// bool by the poller, so the hot-path read is a single atomic load. It mirrors
// pathsearch.HealthState.
type HealthState struct {
	serveEligible  atomic.Bool
	docCount       atomic.Int64
	watermarkEpoch atomic.Int64
	// lastSuccessEpoch is Unix epoch seconds of the last successful HealthCheck,
	// or 0 if it has never succeeded.
	lastSuccessEpoch atomic.Int64
}

// NewHealthState returns a fail-closed HealthState (ServeEligible() == false).
func NewHealthState() *HealthState {
	return &HealthState{}
}

// SetHealthy records a fresh poller observation. eligible is the poller's fully
// computed serve decision (reachable, SERVING, non-empty, and fresh).
func (h *HealthState) SetHealthy(eligible bool, docCount, watermarkEpoch, lastSuccessEpoch int64) {
	if h == nil {
		return
	}

	h.serveEligible.Store(eligible)
	h.docCount.Store(docCount)
	h.watermarkEpoch.Store(watermarkEpoch)
	h.lastSuccessEpoch.Store(lastSuccessEpoch)
}

// ServeEligible reports the last-known serve decision. A nil receiver reports
// false so every uninitialized or disabled path fails closed to PostgreSQL.
func (h *HealthState) ServeEligible() bool {
	if h == nil {
		return false
	}

	return h.serveEligible.Load()
}

// LastSuccessEpoch returns the epoch seconds of the last successful poll, or 0
// when the state is nil or no poll has succeeded.
func (h *HealthState) LastSuccessEpoch() int64 {
	if h == nil {
		return 0
	}

	return h.lastSuccessEpoch.Load()
}

// Snapshot returns the cached serve decision and the last-observed health
// values. A nil receiver returns all-zero values.
func (h *HealthState) Snapshot() (
	eligible bool,
	docCount, watermarkEpoch, lastSuccessEpoch int64,
) {
	if h == nil {
		return false, 0, 0, 0
	}

	return h.serveEligible.Load(), h.docCount.Load(), h.watermarkEpoch.Load(), h.lastSuccessEpoch.Load()
}
