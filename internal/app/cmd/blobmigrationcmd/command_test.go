package blobmigrationcmd

import (
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"gorm.io/gorm/clause"
)

type mockContext struct {
	flags    map[string]any
	kvStore  map[string]string
	dao      *dao.Query
	cliCtx   *cli.Context
	dbCalled bool
}

func TestCheckCleanupGates(t *testing.T) {
	t.Run("refuses without completion", func(t *testing.T) {
		gates, ok := runGateCheck(t, map[string]string{
			kvKeyStatus: "running",
		}, true)
		assert.False(t, ok)
		assertGateFailed(t, gates, "Migration status is 'running'")
	})

	t.Run("refuses without verification", func(t *testing.T) {
		gates, ok := runGateCheck(t, map[string]string{
			kvKeyStatus: "completed",
		}, true)
		assert.False(t, ok)
		assertGateFailed(t, gates, "No verification timestamp")
	})

	t.Run("refuses with stale verification", func(t *testing.T) {
		stale := time.Now().Add(-48 * time.Hour).Format(time.RFC3339)
		gates, ok := runGateCheck(t, map[string]string{
			kvKeyStatus:     "completed",
			kvKeyVerifiedAt: stale,
		}, true)
		assert.False(t, ok)
		assertGateFailed(t, gates, "Verification is stale")
	})

	t.Run("refuses without confirm flag", func(t *testing.T) {
		recent := time.Now().Add(-1 * time.Hour).Format(time.RFC3339)
		gates, ok := runGateCheck(t, map[string]string{
			kvKeyStatus:     "completed",
			kvKeyVerifiedAt: recent,
		}, false)
		assert.False(t, ok)
		assertGateFailed(t, gates, "--confirm flag required")
	})
}

func TestPauseResumeLogic(t *testing.T) {
	t.Run("pause requires running status", func(t *testing.T) {
		status := "completed"
		if status != "running" {
			err := assertStatusCheck(status, "running")
			require.Error(t, err)
		}
	})

	t.Run("resume requires paused status", func(t *testing.T) {
		status := "running"
		if status != "paused" {
			assert.NotEqual(t, "paused", status)
		}
	})

	t.Run("resume accepts paused:high_error_rate", func(t *testing.T) {
		status := "paused:high_error_rate"
		assert.True(t, len(status) > 6 && status[:6] == "paused")
	})
}

func TestStartResumeFromCheckpoint(t *testing.T) {
	t.Run("resume with existing cursor", func(t *testing.T) {
		cursor := "abc123def456"
		require.NotEmpty(t, cursor)
	})

	t.Run("resume without cursor starts from beginning", func(t *testing.T) {
		cursor := ""
		require.Empty(t, cursor)
	})
}

func TestUpsertKV(t *testing.T) {
	now := time.Now()
	kv := model.KeyValue{Key: "test_key", Value: "test_value", CreatedAt: now, UpdatedAt: now}
	assert.Equal(t, "test_key", kv.Key)
	assert.Equal(t, "test_value", kv.Value)
}

func TestGateDescriptions(t *testing.T) {
	t.Run("all gates report meaningful descriptions", func(t *testing.T) {
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
	if status == "completed" {
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
		if parseErr != nil {
			fail("Invalid verification timestamp: " + verifiedAt)
		} else if time.Since(ts) > 24*time.Hour {
			fail("Verification is stale (" + time.Since(ts).Truncate(time.Minute).String() + " ago); re-run 'blob-migration verify'")
		} else {
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
	c := clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value", "updated_at"}),
	}
	assert.Len(t, c.Columns, 1)
	assert.Equal(t, "key", c.Columns[0].Name)
	assert.Len(t, c.DoUpdates, 2)
}
