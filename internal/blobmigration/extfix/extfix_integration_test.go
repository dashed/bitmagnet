//go:build integration

package extfix

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	migrationssql "github.com/bitmagnet-io/bitmagnet/migrations"
	goose "github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

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

// TestBackfillExtensionsSyncsSummaryCompressedBytes verifies the e-backfill keeps
// torrent_file_summary.compressed_bytes equal to octet_length(files_data) after a
// re-encode that changes the blob's byte length.
func TestBackfillExtensionsSyncsSummaryCompressedBytes(t *testing.T) {
	db := setupIntegrationDB(t)
	q := dao.Use(db)
	ctx := context.Background()

	var infoHash protocol.ID
	copy(infoHash[:], []byte{0x51, 0x22})

	// A pre-G1 blob with an empty `e` on a path that has a real extension -> the
	// backfill rewrites it, populating `e` and changing the byte length.
	legacy := encodeLegacy(t, []legacyFile{
		{Index: 0, Path: "movie/video.mkv", Extension: "", Size: 1000},
		{Index: 1, Path: "movie/audio.mp3", Extension: "", Size: 500},
	})

	now := time.Now()
	require.NoError(t, db.Exec(
		"INSERT INTO torrents (info_hash, name, size, private, files_status, created_at, updated_at, files_data) "+
			"VALUES (?, 't', 0, false, 'multi', ?, ?, ?)",
		infoHash, now, now, legacy,
	).Error)
	// Seed the summary with the OLD (pre-rewrite) length, which the backfill must
	// correct to the new blob's length.
	require.NoError(t, db.Exec(
		"INSERT INTO torrent_file_summary (info_hash, file_count, compressed_bytes, created_at, updated_at) "+
			"VALUES (?, 2, ?, ?, ?)",
		infoHash, len(legacy), now, now,
	).Error)

	rep, err := BackfillExtensions(ctx, q, 1, 10, 0, false, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), rep.Fixed, "the legacy blob must be rewritten")

	var newLen int64
	require.NoError(t, db.Raw(
		"SELECT octet_length(files_data) FROM torrents WHERE info_hash = ?", infoHash,
	).Scan(&newLen).Error)

	var summaryBytes *int64
	require.NoError(t, db.Raw(
		"SELECT compressed_bytes FROM torrent_file_summary WHERE info_hash = ?", infoHash,
	).Scan(&summaryBytes).Error)
	require.NotNil(t, summaryBytes)
	require.Equal(t, newLen, *summaryBytes, "summary compressed_bytes must track the rewritten blob length")
}
