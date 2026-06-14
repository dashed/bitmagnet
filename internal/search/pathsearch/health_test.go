package pathsearch

import "testing"

func TestHealthState_FailsClosedByDefault(t *testing.T) {
	t.Parallel()

	h := NewHealthState()
	if h.Healthy() {
		t.Fatal("a fresh HealthState must be fail-closed (Healthy()==false) until the first successful poll")
	}

	if got := h.LastSuccessEpoch(); got != 0 {
		t.Fatalf("LastSuccessEpoch on a fresh state = %d, want 0", got)
	}
}

func TestHealthState_NilReceiverFailsClosed(t *testing.T) {
	t.Parallel()

	var h *HealthState
	if h.Healthy() {
		t.Fatal("nil HealthState.Healthy() must be false (fail-closed)")
	}

	if got := h.LastSuccessEpoch(); got != 0 {
		t.Fatalf("nil LastSuccessEpoch = %d, want 0", got)
	}

	healthy, doc, wm, ls := h.Snapshot()
	if healthy || doc != 0 || wm != 0 || ls != 0 {
		t.Fatalf("nil Snapshot = (%v,%d,%d,%d), want all-zero", healthy, doc, wm, ls)
	}

	h.SetHealthy(true, 1, 2, 3) // must not panic
}

func TestHealthState_SetAndSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	h := NewHealthState()
	h.SetHealthy(true, 42, 1_700_000_000, 1_700_000_100)

	if !h.Healthy() {
		t.Fatal("Healthy() should be true after SetHealthy(true, ...)")
	}

	healthy, doc, wm, ls := h.Snapshot()
	if !healthy || doc != 42 || wm != 1_700_000_000 || ls != 1_700_000_100 {
		t.Fatalf("Snapshot = (%v,%d,%d,%d), want (true,42,1700000000,1700000100)", healthy, doc, wm, ls)
	}

	if got := h.LastSuccessEpoch(); got != 1_700_000_100 {
		t.Fatalf("LastSuccessEpoch = %d, want 1700000100", got)
	}

	// Transition back to unhealthy.
	h.SetHealthy(false, 42, 1_700_000_000, 1_700_000_100)
	if h.Healthy() {
		t.Fatal("Healthy() should be false after SetHealthy(false, ...)")
	}
}
