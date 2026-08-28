//go:build integration

package gqlmodel

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	goose "github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	migrationssql "github.com/bitmagnet-io/bitmagnet/migrations"
)

const (
	zeroBlobHash        = "2222222222222222222222222222222222222222"
	missingSummaryHash  = "3333333333333333333333333333333333333333"
	mismatchedBytesHash = "4444444444444444444444444444444444444444"
)

// TestGenerateTorrentFilesParityPgFixtures recreates and seeds only the
// disposable POSTGRES_DSN database. It intentionally leaves the Goose-34
// schema in place for the Rust SELECT-only parity process that follows in CI.
func TestGenerateTorrentFilesParityPgFixtures(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set, skipping disposable PostgreSQL seeder")
	}
	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	db, err := gormDB.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, err = db.Exec("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	require.NoError(t, err)
	goose.SetBaseFS(migrationssql.FS)
	require.NoError(t, goose.SetDialect("postgres"))
	goose.SetLogger(goose.NopLogger())
	require.NoError(t, goose.UpContext(context.Background(), db, "."))

	fixtures := loadTorrentFilesFixtures(t)
	for _, fixture := range fixtures {
		files := make([]model.TorrentFile, 0, len(fixture.Files))
		for _, file := range fixture.Files {
			files = append(files, model.TorrentFile{
				Index:     file.Index,
				Path:      file.Path,
				Extension: model.NewNullString(file.Extension),
				Size:      file.Size,
			})
		}
		seedTorrentFilesBlob(t, db, fixture.InfoHash, files, true, 0)
	}

	seedTorrentFilesBlob(t, db, zeroBlobHash, nil, true, 0)
	seedTorrentFilesBlob(t, db, missingSummaryHash, []model.TorrentFile{{
		Index: 0,
		Path:  "missing/summary.mkv",
		Size:  1,
	}}, false, 0)
	seedTorrentFilesBlob(t, db, mismatchedBytesHash, []model.TorrentFile{{
		Index: 0,
		Path:  "mismatched/bytes.txt",
		Size:  2,
	}}, true, 1)
	seedTorrentTagsAndVerifyGoOracle(t, gormDB, db)
	seedQueueJobsAndVerifyGoOracle(t, gormDB, db)
}

func seedTorrentFilesBlob(
	t *testing.T,
	db *sql.DB,
	rawHash string,
	files []model.TorrentFile,
	withSummary bool,
	compressedBytesDelta int,
) {
	t.Helper()
	hash, err := protocol.ParseID(rawHash)
	require.NoError(t, err)
	now := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	var blob []byte
	status := "no_info"
	if files != nil {
		blob, err = blobmigration.SerializeFiles(files)
		require.NoError(t, err)
		status = "single"
		if len(files) > 1 {
			status = "multi"
		}
	}
	extensions, err := json.Marshal(blobmigration.ExtractUniqueExtensions(files))
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO torrents
		 (info_hash, name, size, private, files_status, file_extensions,
		  created_at, updated_at, files_data)
		 VALUES ($1, $2, $3, false, $4, $5::jsonb, $6, $6, $7)`,
		hash[:], rawHash, totalFileSize(files), status, string(extensions), now, blob,
	)
	require.NoError(t, err)
	if !withSummary {
		return
	}

	var compressedBytes any
	if blob != nil {
		compressedBytes = len(blob) + compressedBytesDelta
	}
	_, err = db.Exec(
		`INSERT INTO torrent_file_summary
		 (info_hash, file_count, total_size, largest_file_size, extensions,
		  has_video, has_subtitle, has_audio, compressed_bytes, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5::jsonb, false, false, false, $6, $7, $7)`,
		hash[:], len(files), totalFileSize(files), largestFileSize(files),
		string(extensions), compressedBytes, now,
	)
	require.NoError(t, err)
}

func totalFileSize(files []model.TorrentFile) uint {
	var total uint
	for _, file := range files {
		total += file.Size
	}
	return total
}

func largestFileSize(files []model.TorrentFile) uint {
	var largest uint
	for _, file := range files {
		if file.Size > largest {
			largest = file.Size
		}
	}
	return largest
}
