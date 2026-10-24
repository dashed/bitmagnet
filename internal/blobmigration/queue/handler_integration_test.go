//go:build integration

package queue

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration/consistency"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	migrationssql "github.com/bitmagnet-io/bitmagnet/migrations"
	goose "github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// setupIntegrationDB connects to a THROWAWAY Postgres (POSTGRES_DSN), resets the public schema and
// runs all goose migrations. It drops the schema, so it must NEVER point at a real database.
// Because it resets `public`, run integration packages serially: `go test -tags integration -p 1 ./...`.
func setupIntegrationDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set, skipping integration test")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)

	sqlDB, err := db.DB()
	require.NoError(t, err)

	reset := func() {
		_, err := sqlDB.Exec("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
		require.NoError(t, err)
	}
	reset()

	goose.SetBaseFS(migrationssql.FS)
	require.NoError(t, goose.SetDialect("postgres"))
	goose.SetLogger(goose.NopLogger())
	require.NoError(t, goose.UpContext(context.Background(), sqlDB, "."))

	t.Cleanup(reset)

	return db
}

// hashFromBytes builds an info_hash from leading bytes (zero-padded). Lets tests place torrents at
// chosen points in the keyspace (incl. the <0x30 / >0x66 zone the old hex-string cursor mis-bound).
func hashFromBytes(b ...byte) protocol.ID {
	var id protocol.ID
	copy(id[:], b)

	return id
}

func seedTorrentWithFiles(t *testing.T, db *gorm.DB, infoHash protocol.ID, numFiles int) {
	t.Helper()

	paths := make([]string, numFiles)
	for i := range paths {
		paths[i] = fmt.Sprintf("videos/file_%03d.mkv", i)
	}

	seedTorrentPaths(t, db, infoHash, paths)
}

// seedTorrentPaths inserts a torrent + its torrent_files rows with the given paths.
func seedTorrentPaths(t *testing.T, db *gorm.DB, infoHash protocol.ID, paths []string) {
	t.Helper()

	now := time.Now()

	torrent := model.Torrent{
		InfoHash:    infoHash,
		Name:        fmt.Sprintf("t-%x", infoHash[:4]),
		Size:        uint(len(paths) * 1000),
		FilesStatus: model.FilesStatusMulti,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	require.NoError(t, db.Clauses(clause.OnConflict{DoNothing: true}).
		Omit("Extension", "FilesCount", "Hint", "Contents", "Sources", "Files", "Pieces", "Tags", "FilesData", "FileExts").
		Create(&torrent).Error)

	if len(paths) == 0 {
		return
	}

	files := make([]model.TorrentFile, len(paths))
	for i, p := range paths {
		files[i] = model.TorrentFile{
			InfoHash:  infoHash,
			Index:     uint(i),
			Path:      p,
			Size:      uint(1000 * (i + 1)),
			CreatedAt: now,
			UpdatedAt: now,
		}
	}

	require.NoError(t, db.Clauses(clause.OnConflict{DoNothing: true}).Omit("Extension").Create(&files).Error)
}

// testRanges mirrors command.computeRanges: k disjoint, gap-free (lower, upper] info_hash ranges.
func testRanges(k, chunkSize int) []MessageParams {
	step := 256 / k

	out := make([]MessageParams, 0, k)

	for i := 0; i < k; i++ {
		var lower, upper string

		if i > 0 {
			lower = hashFromBytes(byte(i * step)).String()
		}

		if i < k-1 {
			upper = hashFromBytes(byte((i + 1) * step)).String()
		}

		out = append(out, MessageParams{
			InfoHashGreaterThan: lower,
			InfoHashLessOrEqual: upper,
			RangeID:             i,
			NumRanges:           k,
			ChunkSize:           chunkSize,
		})
	}

	return out
}

// driveBackfill seeds k range jobs and drives them to completion in-process (standing in for the
// queue server): repeatedly run every pending blob_migration job through the real handler, then
// dequeue it, until none remain.
func driveBackfill(t *testing.T, db *gorm.DB, k, chunkSize int) {
	t.Helper()

	ctx := context.Background()
	d := dao.Use(db)
	fn := newHandleFunc(d, zap.NewNop().Sugar())

	for _, r := range testRanges(k, chunkSize) {
		job, err := NewQueueJob(r)
		require.NoError(t, err)
		require.NoError(t, db.Create(&job).Error)
	}

	for iter := 0; ; iter++ {
		require.Less(t, iter, 100000, "drive loop did not terminate")

		var jobs []model.QueueJob
		require.NoError(t, db.Where("queue = ? AND status = ?", MessageName, model.QueueJobStatusPending).
			Find(&jobs).Error)

		if len(jobs) == 0 {
			break
		}

		for _, job := range jobs {
			require.NoError(t, fn(ctx, job))
			require.NoError(t, db.Exec("DELETE FROM queue_jobs WHERE id = ?", job.ID).Error)
		}
	}
}

func kvVal(t *testing.T, db *gorm.DB, key string) string {
	t.Helper()

	var v string

	err := db.Table("key_values").Select("value").Where("key = ?", key).Scan(&v).Error
	require.NoError(t, err)

	return v
}

// migratedTotal sums the per-range migrated counters (blob_migration:migrated:<id>).
func migratedTotal(t *testing.T, db *gorm.DB) int64 {
	t.Helper()

	var n int64

	require.NoError(t, db.Table("key_values").
		Where("key LIKE ?", kvKeyMigratedPrefix+"%").
		Select("COALESCE(SUM(value::bigint), 0)").Scan(&n).Error)

	return n
}

func filesData(t *testing.T, db *gorm.DB, h protocol.ID) []byte {
	t.Helper()

	var row struct{ FilesData []byte }

	require.NoError(t, db.Table("torrents").Select("files_data").Where("info_hash = ?", h[:]).Scan(&row).Error)

	return row.FilesData
}

// TestBackfillFull_AllMigratedAndParity drives the full parallel-range streaming backfill over a
// varied set of torrents (different file counts, extension-less, CJK paths, and first-bytes spanning
// the whole keyspace), with a small chunk size to force many chunks + range boundaries. It asserts
// complete coverage, blob<->row parity, correct extensions, an HONEST migrated counter, and the
// completion barrier.
func TestBackfillFull_AllMigratedAndParity(t *testing.T) {
	db := setupIntegrationDB(t)

	type seed struct {
		h     protocol.ID
		paths []string
	}

	seeds := []seed{
		{hashFromBytes(0x00, 0x01), []string{"a/movie.mkv", "a/subs.srt", "a/info.nfo"}}, // first byte < 0x30
		{hashFromBytes(0x00, 0x02), []string{"README", "LICENSE"}},                       // extension-less -> '[]'
		{hashFromBytes(0x10), []string{"x.mp4", "y.mp4", "z.txt", "w.jpg"}},
		{hashFromBytes(0x33, 0x07), []string{"日本語/映画.mkv", "中文/电影.mp4"}}, // CJK
		{hashFromBytes(0x66, 0x09), make20(40, "data/blob_%03d.bin")},    // many files
		{hashFromBytes(0x80), []string{"only.flac"}},
		{hashFromBytes(0xC0, 0x05), []string{"vid.mkv", "vid.idx", "vid.sub"}},
		{hashFromBytes(0xFF, 0xFE), []string{"last.mkv", "last.nfo"}}, // first byte > 0x66
	}
	for _, s := range seeds {
		seedTorrentPaths(t, db, s.h, s.paths)
	}

	// Mark a fresh migration started (the command normally does this).
	require.NoError(t, db.Exec("INSERT INTO key_values (key,value,created_at,updated_at) VALUES (?,?,?,?)",
		kvKeyStatus, statusRunning, time.Now(), time.Now()).Error)

	driveBackfill(t, db, 4 /*ranges*/, 3 /*chunk*/)

	// Completion barrier flipped status.
	assert.Equal(t, statusCompleted, kvVal(t, db, kvKeyStatus))

	// Every torrent migrated exactly once.
	var summaryCount int64
	require.NoError(t, db.Table("torrent_file_summary").Count(&summaryCount).Error)
	assert.Equal(t, int64(len(seeds)), summaryCount, "every torrent should have exactly one summary")

	// Honest counter (no re-delivery overcount in a clean run).
	assert.Equal(t, int64(len(seeds)), migratedTotal(t, db), "migrated count must equal distinct torrents")

	for _, s := range seeds {
		fd := filesData(t, db, s.h)
		require.NotNil(t, fd, "files_data set for %x", s.h[:4])

		blobFiles, err := blobmigration.DeserializeFiles(fd)
		require.NoError(t, err)

		var rows []model.TorrentFile
		require.NoError(t, db.Table("torrent_files").Where("info_hash = ?", s.h[:]).Order(`"index"`).Find(&rows).Error)

		res := consistency.CompareFiles(blobFiles, rows)
		assert.True(t, res.Match, "blob<->row parity for %x: %+v", s.h[:4], res.Mismatches)

		// flushChunk stamps compressed_bytes = len(blob) = octet_length(files_data).
		var summaryBytes *int64
		require.NoError(t, db.Table("torrent_file_summary").
			Select("compressed_bytes").Where("info_hash = ?", s.h[:]).Scan(&summaryBytes).Error)
		require.NotNil(t, summaryBytes, "compressed_bytes set for %x", s.h[:4])
		assert.Equal(t, int64(len(fd)), *summaryBytes, "compressed_bytes == octet_length for %x", s.h[:4])
	}

	// Extension-less torrent gets '[]' (not NULL) in both columns.
	noExt := hashFromBytes(0x00, 0x02)

	var fe, ext string
	require.NoError(t, db.Table("torrents").Select("file_extensions").Where("info_hash = ?", noExt[:]).Scan(&fe).Error)
	require.NoError(t, db.Table("torrent_file_summary").Select("extensions").Where("info_hash = ?", noExt[:]).Scan(&ext).Error)
	assert.Equal(t, "[]", fe)
	assert.Equal(t, "[]", ext)
}

// TestBackfillByteaCursorCoverage is the regression for the hex-vs-bytea cursor bug: torrents whose
// info_hash leading byte is OUTSIDE the ASCII-hex range (0x30-0x66) must still be covered. With the
// old string-bound cursor they were skipped/misordered.
func TestBackfillByteaCursorCoverage(t *testing.T) {
	db := setupIntegrationDB(t)

	firstBytes := []byte{0x00, 0x05, 0x2f, 0x30, 0x67, 0x90, 0xab, 0xfe, 0xff}
	for i, fb := range firstBytes {
		seedTorrentWithFiles(t, db, hashFromBytes(fb, byte(i)), 2)
	}

	require.NoError(t, db.Exec("INSERT INTO key_values (key,value,created_at,updated_at) VALUES (?,?,?,?)",
		kvKeyStatus, statusRunning, time.Now(), time.Now()).Error)

	driveBackfill(t, db, 4, 2)

	var summaryCount int64
	require.NoError(t, db.Table("torrent_file_summary").Count(&summaryCount).Error)
	assert.Equal(t, int64(len(firstBytes)), summaryCount,
		"all torrents across the keyspace (incl. <0x30 and >0x66 leading bytes) must be covered")
}

// TestBackfillIdempotentRedelivery proves a re-executed chunk (the queue's at-least-once delivery)
// neither errors nor double-counts: the NOT EXISTS scan skips already-migrated torrents.
func TestBackfillIdempotentRedelivery(t *testing.T) {
	db := setupIntegrationDB(t)
	ctx := context.Background()
	d := dao.Use(db)

	for i := 0; i < 5; i++ {
		seedTorrentWithFiles(t, db, hashFromBytes(0x40, byte(i)), 3)
	}

	require.NoError(t, db.Exec("INSERT INTO key_values (key,value,created_at,updated_at) VALUES (?,?,?,?)",
		kvKeyStatus, statusRunning, time.Now(), time.Now()).Error)

	fn := newHandleFunc(d, zap.NewNop().Sugar())

	// A single-range job covering everything, chunk big enough for all 5.
	job, err := NewQueueJob(MessageParams{RangeID: 0, NumRanges: 1, ChunkSize: 100})
	require.NoError(t, err)

	require.NoError(t, fn(ctx, job))
	assert.Equal(t, int64(5), migratedTotal(t, db))

	// Re-execute the SAME job (redelivery). files_data IS NULL => 0 work => counter unchanged, no error.
	require.NoError(t, fn(ctx, job))
	assert.Equal(t, int64(5), migratedTotal(t, db), "re-delivery must not double-count")

	var summaryCount int64
	require.NoError(t, db.Table("torrent_file_summary").Count(&summaryCount).Error)
	assert.Equal(t, int64(5), summaryCount)
}

// TestBackfillResume checkpoints per range and resumes from the checkpoint without redoing or
// skipping work.
func TestBackfillResume(t *testing.T) {
	db := setupIntegrationDB(t)
	ctx := context.Background()
	d := dao.Use(db)

	const n = 12
	for i := 0; i < n; i++ {
		seedTorrentWithFiles(t, db, hashFromBytes(0x70, byte(i)), 2)
	}

	require.NoError(t, db.Exec("INSERT INTO key_values (key,value,created_at,updated_at) VALUES (?,?,?,?)",
		kvKeyStatus, statusRunning, time.Now(), time.Now()).Error)

	fn := newHandleFunc(d, zap.NewNop().Sugar())

	// Process exactly one chunk of 4 (single range), then "pause" by NOT self-chaining further.
	job, err := NewQueueJob(MessageParams{RangeID: 0, NumRanges: 1, ChunkSize: 4})
	require.NoError(t, err)
	require.NoError(t, fn(ctx, job))

	var afterOne int64
	require.NoError(t, db.Table("torrent_file_summary").Count(&afterOne).Error)
	require.Equal(t, int64(4), afterOne, "first chunk migrates 4")

	cursor := kvVal(t, db, rangeCursorKey(0))
	require.NotEmpty(t, cursor, "per-range cursor checkpointed")

	// Simulate a pause/crash: the chain is stopped (clear any self-chained pending jobs).
	require.NoError(t, db.Exec("DELETE FROM queue_jobs WHERE queue = ?", MessageName).Error)

	// Resume: re-seed a job from the persisted checkpoint cursor, then drive to completion.
	resumeJob, err := NewQueueJob(MessageParams{
		InfoHashGreaterThan: cursor,
		RangeID:             0,
		NumRanges:           1,
		ChunkSize:           4,
	})
	require.NoError(t, err)
	require.NoError(t, db.Create(&resumeJob).Error)

	for iter := 0; ; iter++ {
		require.Less(t, iter, 10000)

		var jobs []model.QueueJob
		require.NoError(t, db.Where("queue = ? AND status = ?", MessageName, model.QueueJobStatusPending).Find(&jobs).Error)

		if len(jobs) == 0 {
			break
		}

		for _, j := range jobs {
			require.NoError(t, fn(ctx, j))
			require.NoError(t, db.Exec("DELETE FROM queue_jobs WHERE id = ?", j.ID).Error)
		}
	}

	var total int64
	require.NoError(t, db.Table("torrent_file_summary").Count(&total).Error)
	assert.Equal(t, int64(n), total, "resume migrates the rest with no gaps/dupes")
	assert.Equal(t, int64(n), migratedTotal(t, db))
}

func make20(n int, format string) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf(format, i)
	}

	return out
}
