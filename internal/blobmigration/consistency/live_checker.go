package consistency

import (
	"context"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	dbsearch "github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type LiveChecker struct {
	dao            *dao.Query
	interval       time.Duration
	sampleSize     int
	logger         *zap.SugaredLogger
	metrics        *Metrics
	stopCh         chan struct{}
	cancel         context.CancelFunc
	checkLegacy    func(context.Context, *dao.Query, int) (Summary, error)
	checkBlobsOnly func(context.Context, *dao.Query, int) (Summary, error)
}

func NewLiveChecker(
	q *dao.Query,
	interval time.Duration,
	sampleSize int,
	logger *zap.SugaredLogger,
	metrics *Metrics,
) *LiveChecker {
	return &LiveChecker{
		dao:            q,
		interval:       interval,
		sampleSize:     sampleSize,
		logger:         logger.Named("blob_consistency"),
		metrics:        metrics,
		stopCh:         make(chan struct{}),
		checkLegacy:    CheckRandom,
		checkBlobsOnly: CheckRandomBlobsOnly,
	}
}

func (lc *LiveChecker) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	lc.cancel = cancel

	go lc.run(ctx)
}

func (lc *LiveChecker) Stop() {
	if lc.cancel != nil {
		lc.cancel()
	}

	close(lc.stopCh)
}

func (lc *LiveChecker) run(ctx context.Context) {
	ticker := time.NewTicker(lc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-lc.stopCh:
			return
		case <-ticker.C:
			lc.check(ctx)
		}
	}
}

func (lc *LiveChecker) check(ctx context.Context) {
	dropCompatible := !dbsearch.FeatureFlagsValue().AllowTorrentFilesRepair()
	check := lc.checkLegacy

	if dropCompatible {
		check = lc.checkBlobsOnly
	}

	if check == nil {
		if dropCompatible {
			check = CheckRandomBlobsOnly
		} else {
			check = CheckRandom
		}
	}

	summary, err := check(ctx, lc.dao, lc.sampleSize)
	if err != nil {
		lc.logger.Errorw("consistency check failed", "error", err)
		return
	}

	now := float64(time.Now().Unix())
	lc.metrics.ChecksTotal.Add(float64(summary.TotalChecked))
	lc.metrics.LastCheckAt.Set(now)

	if summary.Mismatches+summary.Errors > 0 {
		lc.metrics.ErrorsTotal.Add(float64(summary.Mismatches + summary.Errors))
		lc.metrics.LastErrorAt.Set(now)

		for _, detail := range summary.MismatchDetails {
			lc.logger.Warnw("blob/row mismatch detected",
				"info_hash", detail.InfoHash,
				"blob_files", detail.BlobFiles,
				"row_files", detail.RowFiles,
				"mismatches", detail.Mismatches,
			)
			lc.healTorrent(ctx, detail.InfoHash)
		}
	}

	lc.logger.Infow("consistency check completed",
		"checked", summary.TotalChecked,
		"matches", summary.Matches,
		"mismatches", summary.Mismatches,
		"errors", summary.Errors,
	)
}

func (lc *LiveChecker) healTorrent(ctx context.Context, infoHash [20]byte) {
	if !dbsearch.FeatureFlagsValue().AllowTorrentFilesRepair() {
		lc.logger.Warnw(
			"blob/row mismatch repair skipped in drop-compatible read mode",
			"info_hash", infoHash,
		)

		return
	}

	// Clear files_data AND the summary's compressed_bytes in one transaction:
	// compressed_bytes is a denorm of octet_length(files_data), so leaving a
	// covered summary row with a stale non-NULL byte length would keep the L3
	// read path (summary-first refine_metadata) trusting a value for a blob that
	// no longer exists. Nulling it routes the id back into the miss set until the
	// blob is re-migrated, restoring parity immediately.
	// Bind the raw 20-byte slice (not the [20]byte array, which the driver won't
	// encode as bytea) so both updates match the info_hash column.
	hash := infoHash[:]

	err := lc.dao.Torrent.UnderlyingDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Table("torrents").
			Where("info_hash = ?", hash).
			Update("files_data", nil).Error; err != nil {
			return err
		}

		return tx.Table("torrent_file_summary").
			Where("info_hash = ?", hash).
			Update("compressed_bytes", nil).Error
	})
	if err != nil {
		lc.logger.Errorw("failed to clear files_data for re-migration", "info_hash", infoHash, "error", err)
		return
	}

	lc.logger.Infow("cleared files_data for re-migration", "info_hash", infoHash)
}
