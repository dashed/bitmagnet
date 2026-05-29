package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration/consistency"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/queue/handler"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	kvKeyStatus   = "blob_migration:status"
	kvKeyMigrated = "blob_migration:migrated_count"
	kvKeyCursor   = "blob_migration:cursor"

	consistencySampleRate = 0.05
	maxErrorRate          = 0.01
)

type Params struct {
	fx.In
	Dao    lazy.Lazy[*dao.Query]
	Logger *zap.SugaredLogger
}

type Result struct {
	fx.Out
	Handler lazy.Lazy[handler.Handler] `group:"queue_handlers"`
}

func New(p Params) Result {
	return Result{
		Handler: lazy.New(func() (handler.Handler, error) {
			d, err := p.Dao.Get()
			if err != nil {
				return handler.Handler{}, err
			}
			return handler.New(
				MessageName,
				newHandleFunc(d, p.Logger),
				handler.JobTimeout(10*time.Minute),
				handler.Concurrency(1),
			), nil
		}),
	}
}

func newHandleFunc(d *dao.Query, logger *zap.SugaredLogger) handler.Func {
	return func(ctx context.Context, job model.QueueJob) error {
		msg := &MessageParams{}
		if err := json.Unmarshal([]byte(job.Payload), msg); err != nil {
			return err
		}

		batchSize := msg.BatchSize
		if batchSize <= 0 {
			batchSize = 1000
		}

		cursor := msg.InfoHashGreaterThan

		infoHashes, err := queryDistinctInfoHashes(ctx, d, cursor, batchSize)
		if err != nil {
			return fmt.Errorf("querying info hashes: %w", err)
		}

		if len(infoHashes) == 0 {
			return setProgress(ctx, d, cursor, 0, "completed")
		}

		migrated, verifyErrors, err := processBatch(ctx, d, infoHashes, logger)
		if err != nil {
			return err
		}

		newCursor := infoHashes[len(infoHashes)-1].String()

		if err := setProgress(ctx, d, newCursor, migrated, "running"); err != nil {
			return err
		}

		if verifyErrors > 0 {
			errorRate := float64(verifyErrors) / float64(len(infoHashes))
			if errorRate > maxErrorRate {
				logger.Warnw("blob migration paused due to high error rate",
					"errorRate", errorRate, "errors", verifyErrors, "checked", len(infoHashes))
				return setProgress(ctx, d, newCursor, 0, "paused:high_error_rate")
			}
		}

		// Check if user requested a pause before self-chaining.
		if paused, _ := checkPaused(ctx, d); paused {
			logger.Infow("blob migration paused by user", "cursor", newCursor)
			return nil
		}

		if len(infoHashes) < batchSize {
			return setProgress(ctx, d, newCursor, 0, "completed")
		}

		nextJob, err := NewQueueJob(MessageParams{
			InfoHashGreaterThan: newCursor,
			BatchSize:           batchSize,
		})
		if err != nil {
			return fmt.Errorf("creating next job: %w", err)
		}

		return d.QueueJob.WithContext(ctx).Create(&nextJob)
	}
}

func queryDistinctInfoHashes(ctx context.Context, d *dao.Query, cursor string, limit int) ([]protocol.ID, error) {
	var hashes []protocol.ID

	q := d.TorrentFile.UnderlyingDB().WithContext(ctx).
		Table("torrent_files").
		Select("DISTINCT info_hash").
		Order("info_hash")

	if cursor != "" {
		q = q.Where("info_hash > ?", cursor)
	}

	err := q.Limit(limit).Pluck("info_hash", &hashes).Error

	return hashes, err
}

func processBatch(
	ctx context.Context,
	d *dao.Query,
	infoHashes []protocol.ID,
	logger *zap.SugaredLogger,
) (migrated int, verifyErrors int, err error) {
	for _, infoHash := range infoHashes {
		files, findErr := d.TorrentFile.WithContext(ctx).
			Where(d.TorrentFile.InfoHash.Eq(infoHash)).
			Order(d.TorrentFile.Index).
			Find()
		if findErr != nil {
			return migrated, verifyErrors, fmt.Errorf("reading files for %s: %w", infoHash, findErr)
		}

		if len(files) == 0 {
			continue
		}

		derefFiles := make([]model.TorrentFile, len(files))
		for i, f := range files {
			derefFiles[i] = *f
		}

		blob, serErr := blobmigration.SerializeFiles(derefFiles)
		if serErr != nil {
			return migrated, verifyErrors, fmt.Errorf("serializing files for %s: %w", infoHash, serErr)
		}

		exts := blobmigration.ExtractUniqueExtensions(derefFiles)
		summary := blobmigration.BuildFileSummary(infoHash, derefFiles)

		if updateErr := updateTorrent(ctx, d, infoHash, blob, exts); updateErr != nil {
			return migrated, verifyErrors, fmt.Errorf("updating torrent %s: %w", infoHash, updateErr)
		}

		if upsertErr := upsertFileSummary(ctx, d, summary); upsertErr != nil {
			return migrated, verifyErrors, fmt.Errorf("upserting summary for %s: %w", infoHash, upsertErr)
		}

		migrated++

		if rand.Float64() < consistencySampleRate {
			result, checkErr := consistency.CheckTorrent(ctx, d, infoHash)
			if checkErr != nil {
				logger.Warnw("consistency check error", "infoHash", infoHash, "error", checkErr)

				verifyErrors++
			} else if !result.Match {
				logger.Warnw("consistency mismatch", "infoHash", infoHash, "mismatches", result.Mismatches)

				verifyErrors++
			}
		}
	}

	return migrated, verifyErrors, nil
}

func updateTorrent(ctx context.Context, d *dao.Query, infoHash protocol.ID, blob []byte, exts []string) error {
	return d.Torrent.UnderlyingDB().WithContext(ctx).
		Table("torrents").
		Where("info_hash = ?", infoHash).
		Updates(map[string]any{
			"files_data":      blob,
			"file_extensions": gorm.Expr("?::jsonb", marshalJSON(exts)),
			"updated_at":      time.Now(),
		}).Error
}

func upsertFileSummary(ctx context.Context, d *dao.Query, summary model.TorrentFileSummary) error {
	now := time.Now()
	summary.CreatedAt = now
	summary.UpdatedAt = now

	return d.Torrent.UnderlyingDB().WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "info_hash"}},
			DoUpdates: clause.AssignmentColumns(
				[]string{
					"file_count",
					"total_size",
					"largest_file_size",
					"extensions",
					"has_video",
					"has_subtitle",
					"has_audio",
					"updated_at",
				},
			),
		}).
		Create(&summary).Error
}

func setProgress(ctx context.Context, d *dao.Query, cursor string, batchMigrated int, status string) error {
	db := d.Torrent.UnderlyingDB().WithContext(ctx)
	now := time.Now()

	upsertKV := func(key, value string) error {
		kv := model.KeyValue{Key: key, Value: value, CreatedAt: now, UpdatedAt: now}

		return db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "key"}},
			DoUpdates: clause.AssignmentColumns([]string{"value", "updated_at"}),
		}).Create(&kv).Error
	}

	if err := upsertKV(kvKeyStatus, status); err != nil {
		return err
	}

	if cursor != "" {
		if err := upsertKV(kvKeyCursor, cursor); err != nil {
			return err
		}
	}

	if batchMigrated > 0 {
		return db.Exec(
			"INSERT INTO key_values (key, value, created_at, updated_at) VALUES (?, ?, ?, ?) "+
				"ON CONFLICT (key) DO UPDATE SET "+
				"value = (COALESCE(key_values.value, '0')::int + EXCLUDED.value::int)::text, "+
				"updated_at = EXCLUDED.updated_at",
			kvKeyMigrated, fmt.Sprintf("%d", batchMigrated), now, now,
		).Error
	}

	return nil
}

func checkPaused(ctx context.Context, d *dao.Query) (bool, error) {
	var kv model.KeyValue

	err := d.Torrent.UnderlyingDB().WithContext(ctx).
		Table("key_values").
		Where("key = ?", kvKeyStatus).
		First(&kv).Error
	if err != nil {
		return false, err
	}

	return strings.HasPrefix(kv.Value, "paused"), nil
}

func marshalJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}

	return string(b)
}
