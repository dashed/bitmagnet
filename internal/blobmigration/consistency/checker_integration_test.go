//go:build integration

package consistency

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
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// setupIntegrationDB resets a THROWAWAY Postgres (POSTGRES_DSN) + runs migrations. Drops the schema —
// never point at a real DB. Run integration packages serially: `go test -tags integration -p 1 ./...`.
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

func hashFromBytes(b ...byte) protocol.ID {
	var id protocol.ID
	copy(id[:], b)

	return id
}

// seedMigratedTorrent inserts a torrent + its torrent_files rows AND a matching files_data blob
// (SerializeFiles of those files) — i.e. an already-migrated torrent that should verify as a match.
func seedMigratedTorrent(t *testing.T, db *gorm.DB, infoHash protocol.ID, paths []string) {
	t.Helper()

	now := time.Now()

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
	require.NoError(t, db.Clauses(clause.OnConflict{DoNothing: true}).Omit("Extension").Create(&files).Error)

	blob, err := blobmigration.SerializeFiles(files)
	require.NoError(t, err)
	require.NoError(t, db.Table("torrents").Where("info_hash = ?", infoHash).Update("files_data", blob).Error)
}

// TestCheckAll_AllMatch seeds migrated torrents across the keyspace and verifies them in parallel.
func TestCheckAll_AllMatch(t *testing.T) {
	db := setupIntegrationDB(t)

	const n = 40
	for i := 0; i < n; i++ {
		// spread across leading bytes so multiple parallel ranges get work
		seedMigratedTorrent(t, db, hashFromBytes(byte(i*6), byte(i)), []string{"a/x.mkv", "a/y.srt", "a/z.nfo"})
	}

	summary, err := CheckAll(context.Background(), dao.Use(db), 4 /*workers*/, 5 /*chunk*/, 0 /*full*/)
	require.NoError(t, err)

	assert.Equal(t, n, summary.TotalChecked)
	assert.Equal(t, n, summary.Matches)
	assert.Equal(t, 0, summary.Mismatches)
	assert.Equal(t, 0, summary.Errors)
}

// TestCheckAll_FullAllowsLegacyDuplicatePathCollapse verifies the D1 cleanup gate semantics:
// the stock full verifier accepts only the legacy torrent_files (info_hash,path) collapse.
func TestCheckAll_FullAllowsLegacyDuplicatePathCollapse(t *testing.T) {
	db := setupIntegrationDB(t)

	seedMigratedTorrent(t, db, hashFromBytes(0x10), []string{" ", " ", "video.mkv", " "})

	summary, err := CheckAll(context.Background(), dao.Use(db), 2, 2, 0 /*full*/)
	require.NoError(t, err)

	assert.Equal(t, 1, summary.TotalChecked)
	assert.Equal(t, 1, summary.Matches)
	assert.Equal(t, 0, summary.Mismatches)
	assert.Equal(t, 0, summary.Errors)
	assert.Equal(t, 1, summary.LegacyDuplicatePathTorrents)
	assert.Equal(t, 2, summary.LegacyDuplicatePathFiles)
}

// TestCheckAll_SampleKeepsStrictComparator documents that sampled verification remains a
// diagnostic path and does not apply the D1 duplicate-path exception.
func TestCheckAll_SampleKeepsStrictComparator(t *testing.T) {
	db := setupIntegrationDB(t)

	seedMigratedTorrent(t, db, hashFromBytes(0x20), []string{" ", " ", "video.mkv"})

	summary, err := CheckAll(context.Background(), dao.Use(db), 1, 2, 1 /*sample*/)
	require.NoError(t, err)

	assert.Equal(t, 1, summary.TotalChecked)
	assert.Equal(t, 0, summary.Matches)
	assert.Equal(t, 1, summary.Mismatches)
	assert.Equal(t, 0, summary.Errors)
}

func TestCheckAllFileIndexSetReportsRowOnlyRowsWithinChunkAndTail(t *testing.T) {
	db := setupIntegrationDB(t)

	blobOne := hashFromBytes(0x10)
	rowOnlyInChunk := hashFromBytes(0x20)
	blobTwo := hashFromBytes(0x30)
	rowOnlyTail := hashFromBytes(0x40)

	seedMigratedTorrent(t, db, blobOne, []string{"a.mkv"})
	seedMigratedTorrent(t, db, rowOnlyInChunk, []string{"row-only-chunk.mkv"})
	require.NoError(t, db.Table("torrents").Where("info_hash = ?", rowOnlyInChunk).Update("files_data", nil).Error)
	seedMigratedTorrent(t, db, blobTwo, []string{"b.mkv"})
	seedMigratedTorrent(t, db, rowOnlyTail, []string{"row-only-tail.mkv"})
	require.NoError(t, db.Table("torrents").Where("info_hash = ?", rowOnlyTail).Update("files_data", nil).Error)

	summary, err := CheckAllFileIndexSet(context.Background(), dao.Use(db), 1, 2)
	require.NoError(t, err)

	assert.Equal(t, 4, summary.TotalChecked)
	assert.Equal(t, 2, summary.Matches)
	assert.Equal(t, 2, summary.Mismatches)
	assert.Equal(t, 0, summary.Errors)

	details := make(map[protocol.ID]CheckResult)
	for _, detail := range summary.MismatchDetails {
		details[detail.InfoHash] = detail
	}

	for _, infoHash := range []protocol.ID{rowOnlyInChunk, rowOnlyTail} {
		detail, ok := details[infoHash]
		require.True(t, ok, "expected row-only mismatch for %s", infoHash)
		assert.Equal(t, 0, detail.BlobFiles)
		assert.Greater(t, detail.RowFiles, 0)
		require.NotEmpty(t, detail.Mismatches)
		assert.Equal(t, "missing_in_blob", detail.Mismatches[0].Field)
	}
}

// TestCheckAll_DetectsMismatch corrupts one torrent's blob (re-serialize a DIFFERENT file set) and
// asserts CheckAll flags exactly that one.
func TestCheckAll_DetectsMismatch(t *testing.T) {
	db := setupIntegrationDB(t)

	for i := 0; i < 10; i++ {
		seedMigratedTorrent(t, db, hashFromBytes(byte(i*20), byte(i)), []string{"a/x.mkv", "a/y.srt"})
	}

	// Corrupt one: write a blob whose files don't match its torrent_files rows.
	bad := hashFromBytes(0*20, 0)
	wrong, err := blobmigration.SerializeFiles([]model.TorrentFile{{Index: 0, Path: "WRONG.bin", Size: 999}})
	require.NoError(t, err)
	require.NoError(t, db.Table("torrents").Where("info_hash = ?", bad).Update("files_data", wrong).Error)

	summary, err := CheckAll(context.Background(), dao.Use(db), 4, 5, 0)
	require.NoError(t, err)

	assert.Equal(t, 10, summary.TotalChecked)
	assert.Equal(t, 1, summary.Mismatches, "exactly the corrupted torrent should mismatch")
	assert.Equal(t, 9, summary.Matches)
	require.NotEmpty(t, summary.MismatchDetails)
	assert.Equal(t, bad, summary.MismatchDetails[0].InfoHash)
}

// TestCheckAll_Sample caps the total checked (no ORDER BY RANDOM).
func TestCheckAll_Sample(t *testing.T) {
	db := setupIntegrationDB(t)

	const n = 60
	for i := 0; i < n; i++ {
		seedMigratedTorrent(t, db, hashFromBytes(byte(i*4), byte(i)), []string{"only.flac"})
	}

	summary, err := CheckAll(context.Background(), dao.Use(db), 4, 5, 20 /*sample*/)
	require.NoError(t, err)

	assert.Greater(t, summary.TotalChecked, 0)
	assert.LessOrEqual(t, summary.TotalChecked, n, "sample must not exceed the population")
	// ~20 requested (perRange rounding may overshoot a little); should be well under the full 60.
	assert.Less(t, summary.TotalChecked, n, "sample should check fewer than all")
	assert.Equal(t, 0, summary.Mismatches)
}
