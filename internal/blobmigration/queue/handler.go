package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
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
	// Per-range checkpoint + done-flag keys: "<prefix><rangeID>".
	kvKeyCursorPrefix = "blob_migration:cursor:"
	kvKeyRangePrefix  = "blob_migration:range:"

	statusRunning   = "running"
	statusCompleted = "completed"

	// DefaultChunkSize is the number of torrents (distinct info_hashes) processed per chunk.
	DefaultChunkSize = 2000
	// DefaultConcurrency is the default number of parallel info_hash-range workers.
	DefaultConcurrency = 8

	consistencySampleRate = 0.05
	maxErrorRate          = 0.01
)

type Params struct {
	fx.In
	Config blobmigration.Config
	Dao    lazy.Lazy[*dao.Query]
	Logger *zap.SugaredLogger
}

type Result struct {
	fx.Out
	Handler lazy.Lazy[handler.Handler] `group:"queue_handlers"`
}

func New(p Params) Result {
	concurrency := int(p.Config.Parallelism)
	if concurrency < 1 {
		concurrency = DefaultConcurrency
	}

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
				// Parallel range-workers: K jobs are seeded by `start`, each owning a disjoint
				// info_hash range; Concurrency(K) lets the queue server run them simultaneously.
				handler.Concurrency(concurrency),
			), nil
		}),
	}
}

// chunkTorrent is one fully-read torrent ready to migrate.
type chunkTorrent struct {
	infoHash protocol.ID
	files    []model.TorrentFile
}

func newHandleFunc(d *dao.Query, logger *zap.SugaredLogger) handler.Func {
	return func(ctx context.Context, job model.QueueJob) error {
		msg := &MessageParams{}
		if err := json.Unmarshal([]byte(job.Payload), msg); err != nil {
			return err
		}

		chunkSize := msg.ChunkSize
		if chunkSize <= 0 {
			chunkSize = DefaultChunkSize
		}

		lower, hasLower, err := parseBound(msg.InfoHashGreaterThan)
		if err != nil {
			return fmt.Errorf("parsing lower bound: %w", err)
		}

		upper, hasUpper, err := parseBound(msg.InfoHashLessOrEqual)
		if err != nil {
			return fmt.Errorf("parsing upper bound: %w", err)
		}

		// Phase 1: the next chunk's distinct info_hashes (keyset, raw-bytea bounds, skipping already-
		// migrated torrents). Empty => this range is positionally complete.
		hashes, err := chunkHashes(ctx, d, lower, hasLower, upper, hasUpper, chunkSize)
		if err != nil {
			return fmt.Errorf("scanning chunk hashes: %w", err)
		}

		if len(hashes) == 0 {
			return finishRange(ctx, d, msg, logger)
		}

		// Phase 2: read every file for those torrents in ONE bounded, ordered scan; group in Go.
		maxHash := hashes[len(hashes)-1]

		torrents, err := readChunkFiles(ctx, d, lower, hasLower, maxHash)
		if err != nil {
			return fmt.Errorf("reading chunk files: %w", err)
		}

		migrated, verifyErrors, err := flushChunk(ctx, d, torrents, logger)
		if err != nil {
			return err
		}

		if err := setRangeProgress(ctx, d, msg.RangeID, maxHash.String(), migrated); err != nil {
			return err
		}

		if verifyErrors > 0 {
			errorRate := float64(verifyErrors) / float64(len(torrents))
			if errorRate > maxErrorRate {
				logger.Warnw("blob migration paused due to high error rate",
					"errorRate", errorRate, "errors", verifyErrors, "checked", len(torrents))

				return upsertKV(ctx, d, kvKeyStatus, "paused:high_error_rate")
			}
		}

		// Respect a user pause before self-chaining.
		if paused, _ := checkPaused(ctx, d); paused {
			logger.Infow("blob migration paused by user", "range", msg.RangeID, "cursor", maxHash.String())
			return nil
		}

		nextJob, err := NewQueueJob(MessageParams{
			InfoHashGreaterThan: maxHash.String(),
			InfoHashLessOrEqual: msg.InfoHashLessOrEqual,
			RangeID:             msg.RangeID,
			NumRanges:           msg.NumRanges,
			ChunkSize:           chunkSize,
		})
		if err != nil {
			return fmt.Errorf("creating next job: %w", err)
		}

		// Idempotent self-chain (queue de-dups on fingerprint; DoNothing makes a re-enqueue harmless).
		return d.QueueJob.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&nextJob)
	}
}

// parseBound parses a hex info_hash bound; "" => no bound.
func parseBound(hexStr string) (protocol.ID, bool, error) {
	if hexStr == "" {
		return protocol.ID{}, false, nil
	}

	id, err := protocol.ParseID(hexStr)
	if err != nil {
		return protocol.ID{}, false, err
	}

	return id, true, nil
}

// chunkHashes returns up to `limit` distinct info_hashes in (lower, upper] that are NOT yet migrated,
// ordered. Bounds bind as raw bytea (id[:]); the NOT EXISTS makes re-delivered/forked jobs no-ops.
func chunkHashes(
	ctx context.Context,
	d *dao.Query,
	lower protocol.ID, hasLower bool,
	upper protocol.ID, hasUpper bool,
	limit int,
) ([]protocol.ID, error) {
	var hashes []protocol.ID

	q := d.TorrentFile.UnderlyingDB().WithContext(ctx).
		Table("torrent_files tf").
		Select("DISTINCT tf.info_hash").
		Where("NOT EXISTS (SELECT 1 FROM torrent_file_summary s WHERE s.info_hash = tf.info_hash)").
		Order("tf.info_hash")

	if hasLower {
		q = q.Where("tf.info_hash > ?", lower)
	}

	if hasUpper {
		q = q.Where("tf.info_hash <= ?", upper)
	}

	err := q.Limit(limit).Pluck("tf.info_hash", &hashes).Error

	return hashes, err
}

// readChunkFiles reads every torrent_files row for info_hash in (lower, maxHash] in one ordered scan
// and groups them by info_hash (preserving index order). maxHash is the chunk's largest hash, so all
// torrents are read completely.
func readChunkFiles(
	ctx context.Context,
	d *dao.Query,
	lower protocol.ID, hasLower bool,
	maxHash protocol.ID,
) ([]chunkTorrent, error) {
	q := d.TorrentFile.UnderlyingDB().WithContext(ctx).
		Table("torrent_files tf").
		Select(`tf.info_hash, tf."index", tf.path, tf.size, tf.extension`).
		Where("tf.info_hash <= ?", maxHash).
		Where("NOT EXISTS (SELECT 1 FROM torrent_file_summary s WHERE s.info_hash = tf.info_hash)").
		Order(`tf.info_hash, tf."index"`)

	if hasLower {
		q = q.Where("tf.info_hash > ?", lower)
	}

	rows, err := q.Rows()
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var (
		out     []chunkTorrent
		cur     *chunkTorrent
		curHash protocol.ID
	)

	for rows.Next() {
		var f model.TorrentFile
		if scanErr := d.TorrentFile.UnderlyingDB().ScanRows(rows, &f); scanErr != nil {
			return nil, scanErr
		}

		if cur == nil || f.InfoHash != curHash {
			out = append(out, chunkTorrent{infoHash: f.InfoHash})
			cur = &out[len(out)-1]
			curHash = f.InfoHash
		}

		cur.files = append(cur.files, f)
	}

	return out, rows.Err()
}

// flushChunk serializes each torrent's blob/summary and writes the whole chunk in ONE transaction
// with two set-based statements (UPDATE torrents FROM VALUES, INSERT torrent_file_summary ON CONFLICT).
func flushChunk(
	ctx context.Context,
	d *dao.Query,
	torrents []chunkTorrent,
	logger *zap.SugaredLogger,
) (migrated int, verifyErrors int, err error) {
	if len(torrents) == 0 {
		return 0, 0, nil
	}

	now := time.Now()

	type prepared struct {
		infoHash protocol.ID
		blob     []byte
		extsJSON string
		summary  model.TorrentFileSummary
	}

	preps := make([]prepared, 0, len(torrents))

	for _, t := range torrents {
		if len(t.files) == 0 {
			continue
		}

		blob, serErr := blobmigration.SerializeFiles(t.files)
		if serErr != nil {
			return 0, 0, fmt.Errorf("serializing files for %s: %w", t.infoHash, serErr)
		}

		exts := blobmigration.ExtractUniqueExtensions(t.files)

		extsJSON, mErr := json.Marshal(exts)
		if mErr != nil {
			return 0, 0, fmt.Errorf("marshalling extensions for %s: %w", t.infoHash, mErr)
		}

		preps = append(preps, prepared{
			infoHash: t.infoHash,
			blob:     blob,
			extsJSON: string(extsJSON),
			summary:  blobmigration.BuildFileSummary(t.infoHash, t.files),
		})
	}

	if len(preps) == 0 {
		return 0, 0, nil
	}

	txErr := d.Torrent.UnderlyingDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// --- set-based UPDATE torrents ... FROM (VALUES ...) ---
		var ub strings.Builder

		uArgs := make([]any, 0, len(preps)*3)

		ub.WriteString(
			"UPDATE torrents AS t SET files_data = v.fd, file_extensions = v.fe, updated_at = now() " +
				"FROM (VALUES ",
		)

		for i, p := range preps {
			if i > 0 {
				ub.WriteString(",")
			}

			// info_hash is passed as a hex string + decode()'d to bytea: gorm expands a raw []byte
			// arg into per-byte placeholders in a raw Exec, and an UPDATE...FROM (VALUES ...) has no
			// target-type inference, so a string + decode is the unambiguous, non-expanding binding.
			ub.WriteString("(decode(?,'hex'),?::bytea,?::jsonb)")

			uArgs = append(uArgs, p.infoHash.String(), p.blob, p.extsJSON)
		}

		ub.WriteString(") AS v(info_hash, fd, fe) WHERE t.info_hash = v.info_hash")

		if uErr := tx.Exec(ub.String(), uArgs...).Error; uErr != nil {
			return uErr
		}

		// --- set-based INSERT torrent_file_summary ... ON CONFLICT ---
		var ib strings.Builder

		iArgs := make([]any, 0, len(preps)*10)

		ib.WriteString("INSERT INTO torrent_file_summary " +
			"(info_hash, file_count, total_size, largest_file_size, extensions, " +
			"has_video, has_subtitle, has_audio, created_at, updated_at) VALUES ")

		for i, p := range preps {
			if i > 0 {
				ib.WriteString(",")
			}

			ib.WriteString("(decode(?,'hex'),?,?,?,?::jsonb,?,?,?,?,?)")

			s := p.summary
			iArgs = append(
				iArgs,
				p.infoHash.String(),
				s.FileCount,
				s.TotalSize,
				s.LargestFileSize,
				p.extsJSON,
				s.HasVideo,
				s.HasSubtitle,
				s.HasAudio,
				now,
				now,
			)
		}

		ib.WriteString(" ON CONFLICT (info_hash) DO UPDATE SET " +
			"file_count = excluded.file_count, total_size = excluded.total_size, " +
			"largest_file_size = excluded.largest_file_size, extensions = excluded.extensions, " +
			"has_video = excluded.has_video, has_subtitle = excluded.has_subtitle, " +
			"has_audio = excluded.has_audio, updated_at = excluded.updated_at")

		return tx.Exec(ib.String(), iArgs...).Error
	})
	if txErr != nil {
		return 0, 0, fmt.Errorf("flushing chunk: %w", txErr)
	}

	migrated = len(preps)

	// 5% consistency sample (idempotent, read-only) — same safety valve as before.
	for _, p := range preps {
		if rand.Float64() >= consistencySampleRate {
			continue
		}

		result, checkErr := consistency.CheckTorrent(ctx, d, p.infoHash)
		if checkErr != nil {
			logger.Warnw("consistency check error", "infoHash", p.infoHash, "error", checkErr)

			verifyErrors++
		} else if !result.Match {
			logger.Warnw("consistency mismatch", "infoHash", p.infoHash, "mismatches", result.Mismatches)

			verifyErrors++
		}
	}

	return migrated, verifyErrors, nil
}

// setRangeProgress checkpoints this range's cursor + adds to the global (concurrency-safe) counter.
func setRangeProgress(ctx context.Context, d *dao.Query, rangeID int, cursorHex string, migrated int) error {
	if err := upsertKV(ctx, d, rangeCursorKey(rangeID), cursorHex); err != nil {
		return err
	}

	if migrated > 0 {
		return d.Torrent.UnderlyingDB().WithContext(ctx).Exec(
			"INSERT INTO key_values (key, value, created_at, updated_at) VALUES (?, ?, ?, ?) "+
				"ON CONFLICT (key) DO UPDATE SET "+
				"value = (COALESCE(key_values.value, '0')::int + EXCLUDED.value::int)::text, "+
				"updated_at = EXCLUDED.updated_at",
			kvKeyMigrated, strconv.Itoa(migrated), time.Now(), time.Now(),
		).Error
	}

	return nil
}

// finishRange marks a range done and, if all ranges are done, flips global status to completed.
func finishRange(ctx context.Context, d *dao.Query, msg *MessageParams, logger *zap.SugaredLogger) error {
	if err := upsertKV(ctx, d, rangeDoneKey(msg.RangeID), "done"); err != nil {
		return err
	}

	numRanges := msg.NumRanges
	if numRanges <= 0 {
		numRanges = 1
	}

	var done int64

	if err := d.Torrent.UnderlyingDB().WithContext(ctx).
		Table("key_values").
		Where("key LIKE ? AND value = ?", kvKeyRangePrefix+"%", "done").
		Count(&done).Error; err != nil {
		return err
	}

	logger.Infow("blob migration range complete", "range", msg.RangeID, "rangesDone", done, "numRanges", numRanges)

	if done >= int64(numRanges) {
		return upsertKV(ctx, d, kvKeyStatus, statusCompleted)
	}

	return nil
}

func rangeCursorKey(rangeID int) string { return kvKeyCursorPrefix + strconv.Itoa(rangeID) }
func rangeDoneKey(rangeID int) string   { return kvKeyRangePrefix + strconv.Itoa(rangeID) }

func upsertKV(ctx context.Context, d *dao.Query, key, value string) error {
	now := time.Now()
	kv := model.KeyValue{Key: key, Value: value, CreatedAt: now, UpdatedAt: now}

	return d.Torrent.UnderlyingDB().WithContext(ctx).
		Table("key_values").
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "key"}},
			DoUpdates: clause.AssignmentColumns([]string{"value", "updated_at"}),
		}).
		Create(&kv).Error
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
