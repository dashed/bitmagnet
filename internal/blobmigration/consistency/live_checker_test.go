package consistency

import (
	"context"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
)

func TestLiveCheckerUsesBlobOnlyCheckInDropCompatibleReads(t *testing.T) {
	t.Cleanup(func() { search.SetFeatureFlags(search.FeatureFlags{}) })
	search.SetFeatureFlags(search.FeatureFlags{DropCompatibleReads: true})

	legacyCalled := false
	blobOnlyCalled := false
	lc := &LiveChecker{
		logger:  zap.NewNop().Sugar(),
		metrics: NewMetrics(),
		checkLegacy: func(context.Context, *dao.Query, int) (Summary, error) {
			legacyCalled = true
			return Summary{}, nil
		},
		checkBlobsOnly: func(context.Context, *dao.Query, int) (Summary, error) {
			blobOnlyCalled = true
			return Summary{TotalChecked: 1, Matches: 1}, nil
		},
	}

	lc.check(context.Background())

	if legacyCalled {
		t.Fatal("legacy torrent_files checker was called in drop-compatible read mode")
	}

	if !blobOnlyCalled {
		t.Fatal("blob-only checker was not called in drop-compatible read mode")
	}
}

func TestLiveCheckerUsesLegacyCheckByDefault(t *testing.T) {
	t.Cleanup(func() { search.SetFeatureFlags(search.FeatureFlags{}) })
	search.SetFeatureFlags(search.FeatureFlags{})

	legacyCalled := false
	blobOnlyCalled := false
	lc := &LiveChecker{
		logger:  zap.NewNop().Sugar(),
		metrics: NewMetrics(),
		checkLegacy: func(context.Context, *dao.Query, int) (Summary, error) {
			legacyCalled = true
			return Summary{TotalChecked: 1, Matches: 1}, nil
		},
		checkBlobsOnly: func(context.Context, *dao.Query, int) (Summary, error) {
			blobOnlyCalled = true
			return Summary{}, nil
		},
	}

	lc.check(context.Background())

	if !legacyCalled {
		t.Fatal("legacy checker was not called by default")
	}

	if blobOnlyCalled {
		t.Fatal("blob-only checker was called outside drop-compatible read mode")
	}
}

func TestLiveCheckerCountsBlobOnlyErrors(t *testing.T) {
	t.Cleanup(func() { search.SetFeatureFlags(search.FeatureFlags{}) })
	search.SetFeatureFlags(search.FeatureFlags{DropCompatibleReads: true})

	metrics := NewMetrics()
	lc := &LiveChecker{
		logger:  zap.NewNop().Sugar(),
		metrics: metrics,
		checkBlobsOnly: func(context.Context, *dao.Query, int) (Summary, error) {
			return Summary{TotalChecked: 1, Errors: 1}, nil
		},
	}

	lc.check(context.Background())

	var m dto.Metric
	if err := metrics.ErrorsTotal.Write(&m); err != nil {
		t.Fatalf("ErrorsTotal.Write: %v", err)
	}

	if got := m.GetCounter().GetValue(); got != 1 {
		t.Fatalf("ErrorsTotal = %v, want 1", got)
	}
}

func TestLiveCheckerSkipsTorrentFilesRepairInDropCompatibleReads(t *testing.T) {
	t.Cleanup(func() { search.SetFeatureFlags(search.FeatureFlags{}) })
	search.SetFeatureFlags(search.FeatureFlags{DropCompatibleReads: true})

	lc := &LiveChecker{logger: zap.NewNop().Sugar()}

	// A nil DAO would panic if healTorrent tried to clear files_data. In
	// drop-compatible read mode repair must be alert-only because torrent_files may
	// be absent.
	lc.healTorrent(context.Background(), [20]byte{})
}
