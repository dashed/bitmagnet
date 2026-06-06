//go:build integration

package queue

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
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

func seedTorrentWithFiles(t *testing.T, db *gorm.DB, infoHash protocol.ID, numFiles int) {
	t.Helper()
	now := time.Now()

	torrent := model.Torrent{
		InfoHash:    infoHash,
		Name:        fmt.Sprintf("regression-%x", infoHash[:4]),
		Size:        uint(numFiles * 1000),
		FilesStatus: model.FilesStatusMulti,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	require.NoError(t, db.Clauses(clause.OnConflict{DoNothing: true}).
		Omit("Extension", "FilesCount", "Hint", "Contents", "Sources", "Files", "Pieces", "Tags", "FilesData", "FileExts").
		Create(&torrent).Error)

	files := make([]model.TorrentFile, numFiles)
	for i := range files {
		files[i] = model.TorrentFile{
			InfoHash:  infoHash,
			Index:     uint(i),
			Path:      fmt.Sprintf("videos/file_%03d.mkv", i),
			Size:      uint(1000 * (i + 1)),
			CreatedAt: now,
			UpdatedAt: now,
		}
	}
	require.NoError(t, db.Clauses(clause.OnConflict{DoNothing: true}).Omit("Extension").Create(&files).Error)
}

// TestHandlerBatchWritesCorrectTables drives the REAL worker batch handler (newHandleFunc ->
// processBatch -> updateTorrent + upsertFileSummary + setProgress) against a real Postgres schema.
//
// REGRESSION: before the table-binding fix, upsertFileSummary/setProgress wrote through the
// torrents-bound session (d.Torrent.UnderlyingDB().Create(&nonTorrent)) and the batch failed with
// `column ... of relation "torrents" does not exist`. The pre-existing e2e test missed this because
// it reimplemented the writes with a clean *gorm.DB instead of calling the production functions.
func TestHandlerBatchWritesCorrectTables(t *testing.T) {
	db := setupIntegrationDB(t)
	ctx := context.Background()
	d := dao.Use(db)

	const n = 5
	hashes := make([]protocol.ID, n)
	for i := 0; i < n; i++ {
		hashes[i] = makeInfoHash(byte(0x21 + i))
		seedTorrentWithFiles(t, db, hashes[i], 3+i)
	}

	// Run one batch through the actual production handler. batchSize 1000 > n -> single final batch.
	fn := newHandleFunc(d, zap.NewNop().Sugar())
	job, err := NewQueueJob(MessageParams{BatchSize: 1000})
	require.NoError(t, err)
	require.NoError(t, fn(ctx, job),
		"real batch handler must write key_values/torrent_file_summary to their OWN tables (table-binding regression)")

	// files_data set on every torrent (updateTorrent -> torrents, already correct).
	// NB: scan the bytea into a struct field, not a bare []byte (GORM reads []uint8 as a row slice).
	for _, h := range hashes {
		var row struct{ FilesData []byte }
		require.NoError(t, db.Table("torrents").Select("files_data").Where("info_hash = ?", h).Scan(&row).Error)
		assert.NotNil(t, row.FilesData, "files_data should be set for %x", h[:4])
	}

	// torrent_file_summary populated 1:1 — the upsertFileSummary table-binding regression.
	var summaryCount int64
	require.NoError(t, db.Table("torrent_file_summary").Count(&summaryCount).Error)
	assert.Equal(t, int64(n), summaryCount, "upsertFileSummary must write to torrent_file_summary, not torrents")

	// key_values progress written — the setProgress/upsertKV table-binding regression.
	var status string
	require.NoError(t, db.Table("key_values").Select("value").Where("key = ?", kvKeyStatus).Scan(&status).Error)
	assert.Equal(t, "completed", status, "final batch (n<batchSize) should mark completed")
	var migrated string
	require.NoError(t, db.Table("key_values").Select("value").Where("key = ?", kvKeyMigrated).Scan(&migrated).Error)
	assert.Equal(t, fmt.Sprintf("%d", n), migrated)

	// Guard: the KV/summary writes must not have leaked stray rows into torrents.
	var torrentCount int64
	require.NoError(t, db.Table("torrents").Count(&torrentCount).Error)
	assert.Equal(t, int64(n), torrentCount, "no stray rows inserted into torrents")
}

// TestSetProgressAndSummaryTargetTables is a focused unit-level regression on the two worker write
// helpers, asserting each lands in its own table (independent of the full batch path).
func TestSetProgressAndSummaryTargetTables(t *testing.T) {
	db := setupIntegrationDB(t)
	ctx := context.Background()
	d := dao.Use(db)

	hash := makeInfoHash(0x71)
	seedTorrentWithFiles(t, db, hash, 4)

	// upsertFileSummary -> torrent_file_summary (NOT torrents). Build a complete summary via the
	// production helper so all NOT NULL columns (e.g. extensions) are populated.
	files := []model.TorrentFile{
		{InfoHash: hash, Index: 0, Path: "a/movie.mkv", Extension: model.NewNullString("mkv"), Size: 4000},
		{InfoHash: hash, Index: 1, Path: "a/subs.srt", Extension: model.NewNullString("srt"), Size: 50},
	}
	summary := blobmigration.BuildFileSummary(hash, files)
	require.NoError(t, upsertFileSummary(ctx, d, summary))
	var sc int64
	require.NoError(t, db.Table("torrent_file_summary").Where("info_hash = ?", hash).Count(&sc).Error)
	assert.Equal(t, int64(1), sc)

	// setProgress -> key_values status + cursor + migrated_count (NOT torrents).
	require.NoError(t, setProgress(ctx, d, hash.String(), 7, "running"))
	var status, cursor, migrated string
	require.NoError(t, db.Table("key_values").Select("value").Where("key = ?", kvKeyStatus).Scan(&status).Error)
	require.NoError(t, db.Table("key_values").Select("value").Where("key = ?", kvKeyCursor).Scan(&cursor).Error)
	require.NoError(t, db.Table("key_values").Select("value").Where("key = ?", kvKeyMigrated).Scan(&migrated).Error)
	assert.Equal(t, "running", status)
	assert.Equal(t, hash.String(), cursor)
	assert.Equal(t, "7", migrated)

	// setProgress is additive on migrated_count (raw SQL increment).
	require.NoError(t, setProgress(ctx, d, hash.String(), 3, "running"))
	require.NoError(t, db.Table("key_values").Select("value").Where("key = ?", kvKeyMigrated).Scan(&migrated).Error)
	assert.Equal(t, "10", migrated)
}

// TestBackfillExtensionlessTorrent reproduces the real-prod stall: a multi-file torrent whose files
// have NO extractable extension. ExtractUniqueExtensions returned a nil slice -> the json serializer
// wrote SQL NULL -> `null value in column "extensions" of relation "torrent_file_summary"` (and the
// same for torrents.file_extensions), both JSONB NOT NULL. Pre-fix the batch fails; post-fix the
// empty set serializes to '[]'.
func TestBackfillExtensionlessTorrent(t *testing.T) {
	db := setupIntegrationDB(t)
	ctx := context.Background()
	d := dao.Use(db)

	hash := makeInfoHash(0x91)
	now := time.Now()
	torrent := model.Torrent{
		InfoHash: hash, Name: "extensionless", Size: 3000,
		FilesStatus: model.FilesStatusMulti, CreatedAt: now, UpdatedAt: now,
	}
	require.NoError(t, db.Clauses(clause.OnConflict{DoNothing: true}).
		Omit("Extension", "FilesCount", "Hint", "Contents", "Sources", "Files", "Pieces", "Tags", "FilesData", "FileExts").
		Create(&torrent).Error)
	files := []model.TorrentFile{
		{InfoHash: hash, Index: 0, Path: "README", Size: 1000, CreatedAt: now, UpdatedAt: now},
		{InfoHash: hash, Index: 1, Path: "bin/payload", Size: 2000, CreatedAt: now, UpdatedAt: now},
	}
	require.NoError(t, db.Clauses(clause.OnConflict{DoNothing: true}).Omit("Extension").Create(&files).Error)

	fn := newHandleFunc(d, zap.NewNop().Sugar())
	job, err := NewQueueJob(MessageParams{BatchSize: 1000})
	require.NoError(t, err)
	require.NoError(t, fn(ctx, job), "batch must handle extension-less torrents (extensions -> '[]', not NULL)")

	var ext string
	require.NoError(t, db.Table("torrent_file_summary").Select("extensions").Where("info_hash = ?", hash).Scan(&ext).Error)
	assert.Equal(t, "[]", ext, "extensionless torrent should get summary extensions '[]'")

	var fe string
	require.NoError(t, db.Table("torrents").Select("file_extensions").Where("info_hash = ?", hash).Scan(&fe).Error)
	assert.Equal(t, "[]", fe, "extensionless torrent should get file_extensions '[]'")
}
