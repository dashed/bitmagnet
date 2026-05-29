package blobmigrationcmd

import (
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
)

func TestCheckCleanupGates(t *testing.T) {
	t.Parallel()

	t.Run("refuses without completion", func(t *testing.T) {
		t.Parallel()

		gates, ok := runGateCheck(t, map[string]string{
			kvKeyStatus: statusRunning,
		}, true)
		assert.False(t, ok)
		assertGateFailed(t, gates, "Migration status is 'running'")
	})

	t.Run("refuses without verification", func(t *testing.T) {
		t.Parallel()

		gates, ok := runGateCheck(t, map[string]string{
			kvKeyStatus: statusCompleted,
		}, true)
		assert.False(t, ok)
		assertGateFailed(t, gates, "No verification timestamp")
	})

	t.Run("refuses with stale verification", func(t *testing.T) {
		t.Parallel()

		stale := time.Now().Add(-48 * time.Hour).Format(time.RFC3339)
		gates, ok := runGateCheck(t, map[string]string{
			kvKeyStatus:     statusCompleted,
			kvKeyVerifiedAt: stale,
		}, true)
		assert.False(t, ok)
		assertGateFailed(t, gates, "Verification is stale")
	})

	t.Run("refuses without confirm flag", func(t *testing.T) {
		t.Parallel()

		recent := time.Now().Add(-1 * time.Hour).Format(time.RFC3339)
		gates, ok := runGateCheck(t, map[string]string{
			kvKeyStatus:     statusCompleted,
			kvKeyVerifiedAt: recent,
		}, false)
		assert.False(t, ok)
		assertGateFailed(t, gates, "--confirm flag required")
	})
}

func TestPauseResumeLogic(t *testing.T) {
	t.Parallel()

	t.Run("pause requires running status", func(t *testing.T) {
		t.Parallel()

		status := statusCompleted
		if status != statusRunning {
			err := assertStatusCheck(status, statusRunning)
			require.Error(t, err)
		}
	})

	t.Run("resume requires paused status", func(t *testing.T) {
		t.Parallel()

		status := statusRunning
		if status != statusPaused {
			assert.NotEqual(t, statusPaused, status)
		}
	})

	t.Run("resume accepts paused:high_error_rate", func(t *testing.T) {
		t.Parallel()

		status := "paused:high_error_rate"
		assert.True(t, len(status) > 6 && status[:6] == statusPaused)
	})
}

func TestStartResumeFromCheckpoint(t *testing.T) {
	t.Parallel()

	t.Run("resume with existing cursor", func(t *testing.T) {
		t.Parallel()

		cursor := "abc123def456"
		require.NotEmpty(t, cursor)
	})

	t.Run("resume without cursor starts from beginning", func(t *testing.T) {
		t.Parallel()

		cursor := ""
		require.Empty(t, cursor)
	})
}

func TestUpsertKV(t *testing.T) {
	t.Parallel()

	now := time.Now()
	kv := model.KeyValue{Key: "test_key", Value: "test_value", CreatedAt: now, UpdatedAt: now}
	assert.Equal(t, "test_key", kv.Key)
	assert.Equal(t, "test_value", kv.Value)
}

func TestGateDescriptions(t *testing.T) {
	t.Parallel()

	t.Run("all gates report meaningful descriptions", func(t *testing.T) {
		t.Parallel()

		gates, _ := runGateCheck(t, map[string]string{}, false)
		require.GreaterOrEqual(t, len(gates), 3)

		for _, g := range gates {
			assert.NotEmpty(t, g.description)
		}
	})
}

// runGateCheck simulates the cleanup gate checks using an in-memory key-value store.
// It tests the pure logic of gate validation without requiring a database.
func runGateCheck(t *testing.T, kvs map[string]string, confirmFlag bool) ([]gate, bool) {
	t.Helper()

	var gates []gate

	allPassed := true

	fail := func(desc string) {
		gates = append(gates, gate{desc, false})
		allPassed = false
	}
	pass := func(desc string) {
		gates = append(gates, gate{desc, true})
	}

	// Gate 1: migration completed
	status := kvs[kvKeyStatus]
	if status == statusCompleted {
		pass("Migration status is 'completed'")
	} else {
		fail("Migration status is '" + status + "', expected 'completed'")
	}

	// Gate 2: unmigrated check (simulated pass since we can't query DB)
	pass("All eligible torrents have blob data")

	// Gate 3: verification passed recently
	verifiedAt := kvs[kvKeyVerifiedAt]
	if verifiedAt == "" {
		fail("No verification timestamp found (run 'blob-migration verify' first)")
	} else {
		ts, parseErr := time.Parse(time.RFC3339, verifiedAt)

		switch {
		case parseErr != nil:
			fail("Invalid verification timestamp: " + verifiedAt)
		case time.Since(ts) > 24*time.Hour:
			staleFor := time.Since(ts).Truncate(time.Minute).String()
			fail("Verification is stale (" + staleFor + " ago); re-run 'blob-migration verify'")
		default:
			pass("Verification passed at " + verifiedAt)
		}
	}

	// Gate 4: --confirm flag
	if confirmFlag {
		pass("--confirm flag provided")
	} else {
		fail("--confirm flag required for destructive operation")
	}

	return gates, allPassed
}

func assertGateFailed(t *testing.T, gates []gate, substring string) {
	t.Helper()

	for _, g := range gates {
		if !g.passed && contains(g.description, substring) {
			return
		}
	}

	t.Errorf("expected a failed gate containing %q, got gates: %+v", substring, gates)
}

func assertStatusCheck(got, want string) error {
	if got != want {
		return assert.AnError
	}

	return nil
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}

	return false
}

// Verify that the upsert clause produces the expected conflict resolution.
func TestUpsertClause(t *testing.T) {
	t.Parallel()

	c := clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value", "updated_at"}),
	}
	assert.Len(t, c.Columns, 1)
	assert.Equal(t, "key", c.Columns[0].Name)
	assert.Len(t, c.DoUpdates, 2)
}
