package consistency

import (
	"context"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"go.uber.org/zap"
)

type LiveChecker struct {
	dao        *dao.Query
	interval   time.Duration
	sampleSize int
	logger     *zap.SugaredLogger
	metrics    *Metrics
	stopCh     chan struct{}
	cancel     context.CancelFunc
}

func NewLiveChecker(
	q *dao.Query,
	interval time.Duration,
	sampleSize int,
	logger *zap.SugaredLogger,
	metrics *Metrics,
) *LiveChecker {
	return &LiveChecker{
		dao:        q,
		interval:   interval,
		sampleSize: sampleSize,
		logger:     logger.Named("blob_consistency"),
		metrics:    metrics,
		stopCh:     make(chan struct{}),
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
	summary, err := CheckRandom(ctx, lc.dao, lc.sampleSize)
	if err != nil {
		lc.logger.Errorw("consistency check failed", "error", err)
		return
	}

	now := float64(time.Now().Unix())
	lc.metrics.ChecksTotal.Add(float64(summary.TotalChecked))
	lc.metrics.LastCheckAt.Set(now)

	if summary.Mismatches > 0 {
		lc.metrics.ErrorsTotal.Add(float64(summary.Mismatches))
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
	err := lc.dao.Torrent.UnderlyingDB().WithContext(ctx).
		Table("torrents").
		Where("info_hash = ?", infoHash).
		Update("files_data", nil).Error
	if err != nil {
		lc.logger.Errorw("failed to clear files_data for re-migration", "info_hash", infoHash, "error", err)
		return
	}

	lc.logger.Infow("cleared files_data for re-migration", "info_hash", infoHash)
}
