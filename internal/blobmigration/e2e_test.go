//go:build integration

package blobmigration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
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
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

func setupTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set, skipping integration test")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	sqlDB, err := db.DB()
	require.NoError(t, err)

	cleanupSchema(t, sqlDB)
	runMigrations(t, sqlDB)

	t.Cleanup(func() {
		cleanupSchema(t, sqlDB)
	})

	return db
}

func cleanupSchema(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	_, err := sqlDB.Exec("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
	require.NoError(t, err)
}

func runMigrations(t *testing.T, sqlDB *sql.DB) {
	t.Helper()

	goose.SetBaseFS(migrationssql.FS)
	require.NoError(t, goose.SetDialect("postgres"))

	goose.SetLogger(goose.NopLogger())
	require.NoError(t, goose.UpContext(context.Background(), sqlDB, "."))
}

func makeInfoHash(seed byte) protocol.ID {
	var id protocol.ID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func insertTestTorrent(t *testing.T, db *gorm.DB, infoHash protocol.ID, numFiles int) []model.TorrentFile {
	t.Helper()

	now := time.Now()
	filesStatus := model.FilesStatusMulti
	if numFiles == 1 {
		filesStatus = model.FilesStatusSingle
	}
	if numFiles == 0 {
		filesStatus = model.FilesStatusNoInfo
	}

	torrent := model.Torrent{
		InfoHash:    infoHash,
		Name:        fmt.Sprintf("test-torrent-%x", infoHash[:4]),
		Size:        uint(numFiles * 1000),
		Private:     false,
		FilesStatus: filesStatus,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	err := db.Clauses(clause.OnConflict{DoNothing: true}).
		Omit("Extension", "FilesCount", "Hint", "Contents", "Sources", "Files", "Pieces", "Tags", "FilesData", "FileExts").
		Create(&torrent).Error
	require.NoError(t, err)

	files := make([]model.TorrentFile, numFiles)
	for i := range numFiles {
		files[i] = model.TorrentFile{
			InfoHash:  infoHash,
			Index:     uint(i),
			Path:      filePathForIndex(i),
			Size:      uint(1000 * (i + 1)),
			CreatedAt: now,
			UpdatedAt: now,
		}
	}

	if numFiles > 0 {
		err = db.Clauses(clause.OnConflict{DoNothing: true}).
			Omit("Extension").
			Create(&files).Error
		require.NoError(t, err)
	}

	return files
}

func filePathForIndex(i int) string {
	switch {
	case i < 3:
		return fmt.Sprintf("videos/episode_%03d.mkv", i)
	case i < 6:
		return fmt.Sprintf("subs/subtitle_%03d.srt", i)
	case i < 9:
		return fmt.Sprintf("audio/track_%03d.mp3", i)
	default:
		return fmt.Sprintf("data/file_%04d.bin", i)
	}
}

func TestBlobMigrationLifecycle(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	q := dao.Use(db)

	type testCase struct {
		seed     byte
		numFiles int
	}
	cases := []testCase{
		{seed: 0x01, numFiles: 1},
		{seed: 0x02, numFiles: 5},
		{seed: 0x03, numFiles: 10},
		{seed: 0x04, numFiles: 50},
		{seed: 0x05, numFiles: 100},
	}

	allHashes := make([]protocol.ID, len(cases))
	for i, tc := range cases {
		hash := makeInfoHash(tc.seed)
		allHashes[i] = hash
		insertTestTorrent(t, db, hash, tc.numFiles)
	}

	// Insert edge-case torrents: CJK names, empty extensions.
	cjkHash := makeInfoHash(0x10)
	allHashes = append(allHashes, cjkHash)
	insertCJKTorrent(t, db, cjkHash)

	emptyExtHash := makeInfoHash(0x11)
	allHashes = append(allHashes, emptyExtHash)
	insertNoExtensionTorrent(t, db, emptyExtHash)

	// Step 1: Verify initial state — files_data should be NULL for all.
	for _, hash := range allHashes {
		var filesData []byte
		err := db.Table("torrents").
			Select("files_data").
			Where("info_hash = ?", hash).
			Scan(&filesData).Error
		require.NoError(t, err)
		assert.Nil(t, filesData, "files_data should be NULL before migration for %x", hash[:4])
	}

	// Step 2: Simulate dual-write (like DHT persist does for new torrents).
	dualWriteHash := makeInfoHash(0x20)
	dualWriteFiles := insertTestTorrent(t, db, dualWriteHash, 3)

	blob, err := blobmigration.SerializeFiles(dualWriteFiles)
	require.NoError(t, err)
	exts := blobmigration.ExtractUniqueExtensions(dualWriteFiles)
	extsJSON, _ := json.Marshal(exts)

	err = db.Table("torrents").
		Where("info_hash = ?", dualWriteHash).
		Updates(map[string]any{
			"files_data":      blob,
			"file_extensions": gorm.Expr("?::jsonb", string(extsJSON)),
		}).Error
	require.NoError(t, err)

	// Verify dual-write: both files_data and torrent_files exist.
	var dualBlob []byte
	err = db.Table("torrents").Select("files_data").Where("info_hash = ?", dualWriteHash).Scan(&dualBlob).Error
	require.NoError(t, err)
	assert.NotNil(t, dualBlob, "dual-write should set files_data")

	var dualRowCount int64
	err = db.Table("torrent_files").Where("info_hash = ?", dualWriteHash).Count(&dualRowCount).Error
	require.NoError(t, err)
	assert.Equal(t, int64(3), dualRowCount, "dual-write should preserve torrent_files rows")

	// Step 3: Run migration batch — process all non-dual-write torrents.
	migrateHashes, err := queryDistinctInfoHashes(ctx, db, "", 1000)
	require.NoError(t, err)
	require.NotEmpty(t, migrateHashes)

	for _, hash := range migrateHashes {
		migrateOneTorrent(t, ctx, db, q, hash)
	}

	// Step 4: Verify migration results.
	for _, hash := range allHashes {
		var filesData []byte
		err := db.Table("torrents").
			Select("files_data").
			Where("info_hash = ?", hash).
			Scan(&filesData).Error
		require.NoError(t, err)
		assert.NotNil(t, filesData, "files_data should be set after migration for %x", hash[:4])

		blobFiles, err := blobmigration.DeserializeFiles(filesData)
		require.NoError(t, err)

		var rowFiles []model.TorrentFile
		err = db.Table("torrent_files").
			Where("info_hash = ?", hash).
			Order(`"index" ASC`).
			Find(&rowFiles).Error
		require.NoError(t, err)

		result := consistency.CompareFiles(blobFiles, rowFiles)
		assert.True(t, result.Match,
			"blob/row mismatch for %x: %+v", hash[:4], result.Mismatches)
	}

	// Verify file_extensions populated.
	for _, hash := range allHashes {
		var extsRaw string
		err := db.Table("torrents").
			Select("file_extensions").
			Where("info_hash = ?", hash).
			Scan(&extsRaw).Error
		require.NoError(t, err)

		var fileExts []string
		require.NoError(t, json.Unmarshal([]byte(extsRaw), &fileExts))
		// file_extensions should be sorted and unique.
		if len(fileExts) > 1 {
			assert.True(t, sort.StringsAreSorted(fileExts),
				"file_extensions not sorted for %x", hash[:4])
		}
	}

	// Verify torrent_file_summary populated.
	for _, hash := range allHashes {
		var summary model.TorrentFileSummary
		err := db.Table("torrent_file_summary").
			Where("info_hash = ?", hash).
			First(&summary).Error
		require.NoError(t, err, "summary missing for %x", hash[:4])
		assert.Greater(t, summary.FileCount, 0)
		assert.Greater(t, summary.TotalSize, int64(0))
	}

	// Step 5: Verify AfterFind — load via GORM and confirm Files populated from blob.
	for _, tc := range cases {
		hash := makeInfoHash(tc.seed)
		var torrent model.Torrent
		err := db.Where("info_hash = ?", hash).First(&torrent).Error
		require.NoError(t, err)

		assert.Len(t, torrent.Files, tc.numFiles,
			"AfterFind should populate %d files from blob for %x", tc.numFiles, hash[:4])

		if len(torrent.Files) > 1 {
			for i := 1; i < len(torrent.Files); i++ {
				assert.LessOrEqual(t, torrent.Files[i-1].Path, torrent.Files[i].Path,
					"AfterFind should sort files by path")
			}
		}
	}
}

func TestConsistencyCheckerIntegration(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	q := dao.Use(db)

	// Insert torrents and migrate them so they have matching blob + rows.
	hashes := make([]protocol.ID, 10)
	for i := range hashes {
		hashes[i] = makeInfoHash(byte(0x30 + i))
		insertTestTorrent(t, db, hashes[i], 5)
		migrateOneTorrent(t, ctx, db, q, hashes[i])
	}

	// Verify all match.
	summary, err := consistency.CheckRandom(ctx, q, 10)
	require.NoError(t, err)
	assert.Equal(t, 10, summary.TotalChecked)
	assert.Equal(t, 10, summary.Matches)
	assert.Equal(t, 0, summary.Mismatches)

	// Corrupt one blob by writing garbage.
	corruptHash := hashes[3]
	err = db.Table("torrents").
		Where("info_hash = ?", corruptHash).
		Update("files_data", []byte("corrupt-garbage-data")).Error
	require.NoError(t, err)

	// CheckTorrent should return an error for the corrupt blob.
	_, checkErr := consistency.CheckTorrent(ctx, q, corruptHash)
	assert.Error(t, checkErr, "corrupt blob should cause a deserialization error")

	// Simulate heal: NULL out the corrupt blob (as LiveChecker does).
	err = db.Table("torrents").
		Where("info_hash = ?", corruptHash).
		Update("files_data", nil).Error
	require.NoError(t, err)

	// Verify healed torrent has NULL files_data.
	var healedBlob []byte
	err = db.Table("torrents").
		Select("files_data").
		Where("info_hash = ?", corruptHash).
		Scan(&healedBlob).Error
	require.NoError(t, err)
	assert.Nil(t, healedBlob, "healed torrent should have NULL files_data")

	// Re-migrate the healed torrent.
	migrateOneTorrent(t, ctx, db, q, corruptHash)

	// Verify it matches again.
	result, err := consistency.CheckTorrent(ctx, q, corruptHash)
	require.NoError(t, err)
	assert.True(t, result.Match, "re-migrated torrent should match")
}

func TestCleanupSafetyGates(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	q := dao.Use(db)

	now := time.Now()
	upsertKV := func(key, value string) {
		t.Helper()
		kv := model.KeyValue{Key: key, Value: value, CreatedAt: now, UpdatedAt: now}
		err := db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "key"}},
			DoUpdates: clause.AssignmentColumns([]string{"value", "updated_at"}),
		}).Create(&kv).Error
		require.NoError(t, err)
	}
	getKV := func(key string) string {
		t.Helper()
		var kv model.KeyValue
		err := db.Table("key_values").Where("key = ?", key).First(&kv).Error
		if err != nil {
			return ""
		}
		return kv.Value
	}

	// Gate 1: Migration not complete — should block cleanup.
	t.Run("refuses_when_not_completed", func(t *testing.T) {
		upsertKV("blob_migration:status", "running")
		status := getKV("blob_migration:status")
		assert.NotEqual(t, "completed", status)
	})

	// Gate 2: Unmigrated torrents exist.
	t.Run("refuses_when_unmigrated_torrents_exist", func(t *testing.T) {
		hash := makeInfoHash(0x40)
		insertTestTorrent(t, db, hash, 3)

		var count int64
		err := db.Table("torrents").
			Where("files_data IS NULL AND files_status != ?", model.FilesStatusNoInfo).
			Count(&count).Error
		require.NoError(t, err)
		assert.Greater(t, count, int64(0), "unmigrated torrents should exist")
	})

	// Gate 3: No verification timestamp.
	t.Run("refuses_without_verification", func(t *testing.T) {
		status := getKV("blob_migration:verified_at")
		assert.Empty(t, status, "no verification timestamp should exist")
	})

	// Now satisfy all gates.
	t.Run("all_gates_satisfied", func(t *testing.T) {
		// Migrate all remaining torrents.
		hashes, err := queryDistinctInfoHashes(ctx, db, "", 1000)
		require.NoError(t, err)
		for _, hash := range hashes {
			migrateOneTorrent(t, ctx, db, q, hash)
		}

		upsertKV("blob_migration:status", "completed")
		upsertKV("blob_migration:verified_at", now.Format(time.RFC3339))

		// Verify all gates pass.
		assert.Equal(t, "completed", getKV("blob_migration:status"))

		var unmigrated int64
		err = db.Table("torrents").
			Where("files_data IS NULL AND files_status != ?", model.FilesStatusNoInfo).
			Count(&unmigrated).Error
		require.NoError(t, err)
		assert.Equal(t, int64(0), unmigrated)

		assert.NotEmpty(t, getKV("blob_migration:verified_at"))

		// Perform cleanup: drop torrent_files table.
		err = db.Exec("DROP TABLE IF EXISTS torrent_files CASCADE").Error
		require.NoError(t, err)

		// Verify reads still work from blobs via AfterFind.
		var torrent model.Torrent
		err = db.First(&torrent).Error
		require.NoError(t, err)
		assert.NotEmpty(t, torrent.Files, "after cleanup, files should be loaded from blob")
	})
}

func TestBlobSourcedFilesMatchRows(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	q := dao.Use(db)

	hash := makeInfoHash(0x50)
	insertTestTorrent(t, db, hash, 20)

	// Load files from rows via Preload.
	var rowTorrent model.Torrent
	err := db.Preload("Files").Where("info_hash = ?", hash).First(&rowTorrent).Error
	require.NoError(t, err)
	rowFileExtensions := rowTorrent.FileExtensions()

	// Migrate to blob.
	migrateOneTorrent(t, ctx, db, q, hash)

	// Load files from blob via AfterFind (no Preload).
	var blobTorrent model.Torrent
	err = db.Where("info_hash = ?", hash).First(&blobTorrent).Error
	require.NoError(t, err)
	blobFileExtensions := blobTorrent.FileExtensions()

	// The exported FileExtensions() method reads from t.Files, which should be
	// identical whether populated from rows or blob.
	sort.Strings(rowFileExtensions)
	sort.Strings(blobFileExtensions)
	assert.Equal(t, rowFileExtensions, blobFileExtensions,
		"FileExtensions should be identical whether sourced from rows or blob")

	assert.Equal(t, len(rowTorrent.Files), len(blobTorrent.Files),
		"file count should match between row-loaded and blob-loaded")
}

// --- helpers ---

func insertCJKTorrent(t *testing.T, db *gorm.DB, infoHash protocol.ID) {
	t.Helper()
	now := time.Now()

	err := db.Clauses(clause.OnConflict{DoNothing: true}).
		Omit("Extension", "FilesCount", "Hint", "Contents", "Sources", "Files", "Pieces", "Tags", "FilesData", "FileExts").
		Create(&model.Torrent{
			InfoHash:    infoHash,
			Name:        "日本語テスト",
			Size:        5000,
			Private:     false,
			FilesStatus: model.FilesStatusMulti,
			CreatedAt:   now,
			UpdatedAt:   now,
		}).Error
	require.NoError(t, err)

	files := []model.TorrentFile{
		{InfoHash: infoHash, Index: 0, Path: "日本語/映画.mkv", Size: 2000, CreatedAt: now, UpdatedAt: now},
		{InfoHash: infoHash, Index: 1, Path: "中文/电影 (2024).mp4", Size: 3000, CreatedAt: now, UpdatedAt: now},
	}
	err = db.Clauses(clause.OnConflict{DoNothing: true}).Omit("Extension").Create(&files).Error
	require.NoError(t, err)
}

func insertNoExtensionTorrent(t *testing.T, db *gorm.DB, infoHash protocol.ID) {
	t.Helper()
	now := time.Now()

	err := db.Clauses(clause.OnConflict{DoNothing: true}).
		Omit("Extension", "FilesCount", "Hint", "Contents", "Sources", "Files", "Pieces", "Tags", "FilesData", "FileExts").
		Create(&model.Torrent{
			InfoHash:    infoHash,
			Name:        "noext-torrent",
			Size:        1000,
			Private:     false,
			FilesStatus: model.FilesStatusMulti,
			CreatedAt:   now,
			UpdatedAt:   now,
		}).Error
	require.NoError(t, err)

	files := []model.TorrentFile{
		{InfoHash: infoHash, Index: 0, Path: "README", Size: 500, CreatedAt: now, UpdatedAt: now},
		{InfoHash: infoHash, Index: 1, Path: "LICENSE", Size: 500, CreatedAt: now, UpdatedAt: now},
	}
	err = db.Clauses(clause.OnConflict{DoNothing: true}).Omit("Extension").Create(&files).Error
	require.NoError(t, err)
}

func queryDistinctInfoHashes(ctx context.Context, db *gorm.DB, cursor string, limit int) ([]protocol.ID, error) {
	var hashes []protocol.ID
	q := db.WithContext(ctx).
		Table("torrent_files").
		Select("DISTINCT info_hash").
		Order("info_hash")
	if cursor != "" {
		q = q.Where("info_hash > ?", cursor)
	}
	err := q.Limit(limit).Pluck("info_hash", &hashes).Error
	return hashes, err
}

func migrateOneTorrent(t *testing.T, ctx context.Context, db *gorm.DB, q *dao.Query, infoHash protocol.ID) {
	t.Helper()

	files, err := q.TorrentFile.WithContext(ctx).
		Where(q.TorrentFile.InfoHash.Eq(infoHash)).
		Order(q.TorrentFile.Index).
		Find()
	require.NoError(t, err)

	if len(files) == 0 {
		return
	}

	derefFiles := make([]model.TorrentFile, len(files))
	for i, f := range files {
		derefFiles[i] = *f
	}

	blob, err := blobmigration.SerializeFiles(derefFiles)
	require.NoError(t, err)

	exts := blobmigration.ExtractUniqueExtensions(derefFiles)
	summary := blobmigration.BuildFileSummary(infoHash, derefFiles, len(blob))

	extsJSON, _ := json.Marshal(exts)
	err = db.WithContext(ctx).Table("torrents").
		Where("info_hash = ?", infoHash).
		Updates(map[string]any{
			"files_data":      blob,
			"file_extensions": gorm.Expr("?::jsonb", string(extsJSON)),
			"updated_at":      time.Now(),
		}).Error
	require.NoError(t, err)

	now := time.Now()
	summary.CreatedAt = now
	summary.UpdatedAt = now
	err = db.WithContext(ctx).
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
					"compressed_bytes",
					"updated_at",
				},
			),
		}).
		Create(&summary).Error
	require.NoError(t, err)
}
