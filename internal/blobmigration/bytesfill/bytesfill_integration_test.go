//go:build integration

package bytesfill

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

// setupIntegrationDB connects to a THROWAWAY Postgres (POSTGRES_DSN), resets the
// public schema and runs all goose migrations. It DROPS the schema, so it must
// NEVER point at a real database. Run integration packages serially:
// `go test -tags integration -p 1 ./...`.
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

// seedRow inserts a torrent (with the given files_data) plus a summary row whose
// compressed_bytes starts NULL, so the backfill has something to fill. A nil
// filesData models a blob-less torrent whose octet_length is NULL.
func seedRow(t *testing.T, db *gorm.DB, infoHash protocol.ID, filesData []byte) {
	t.Helper()

	now := time.Now()
	require.NoError(t, db.Exec(
		"INSERT INTO torrents (info_hash, name, size, private, files_status, created_at, updated_at, files_data) "+
			"VALUES (?, ?, 0, false, 'multi', ?, ?, ?)",
		infoHash, "t", now, now, filesData,
	).Error)
	require.NoError(t, db.Exec(
		"INSERT INTO torrent_file_summary (info_hash, file_count, created_at, updated_at) "+
			"VALUES (?, 1, ?, ?)",
		infoHash, now, now,
	).Error)
}

func compressedBytes(t *testing.T, db *gorm.DB, infoHash protocol.ID) (int64, bool) {
	t.Helper()

	var got *int64
	require.NoError(t, db.Raw(
		"SELECT compressed_bytes FROM torrent_file_summary WHERE info_hash = ?", infoHash,
	).Scan(&got).Error)

	if got == nil {
		return 0, false
	}

	return *got, true
}

func TestBackfillCompressedBytes_FillsBlobLengthAndSkipsBlobless(t *testing.T) {
	db := setupIntegrationDB(t)
	q := dao.Use(db)
	ctx := context.Background()

	// Spread the rows across the leading-byte keyspace so a multi-worker run must
	// cover every range: 0x00, 0x40, 0x80, 0xC0.
	withBlob := hashFromBytes(0x00, 0x11)
	otherBlob := hashFromBytes(0x80, 0x22)
	blobless := hashFromBytes(0xC0, 0x33)

	seedRow(t, db, withBlob, []byte{1, 2, 3, 4, 5}) // octet_length 5
	seedRow(t, db, otherBlob, []byte{9, 9})         // octet_length 2
	seedRow(t, db, blobless, nil)                   // NULL files_data -> stays NULL

	rep, err := BackfillCompressedBytes(ctx, q, 4, 10, 0, false, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), rep.Scanned, "all three NULL rows scanned")

	got, ok := compressedBytes(t, db, withBlob)
	require.True(t, ok)
	require.Equal(t, int64(5), got)

	got, ok = compressedBytes(t, db, otherBlob)
	require.True(t, ok)
	require.Equal(t, int64(2), got)

	_, ok = compressedBytes(t, db, blobless)
	require.False(t, ok, "blob-less torrent keeps a NULL compressed_bytes")

	// Idempotent: a re-run finds only the still-NULL blob-less row and rewrites
	// nothing meaningful (blob-less stays NULL, filled rows are skipped by the
	// compressed_bytes IS NULL predicate).
	rep2, err := BackfillCompressedBytes(ctx, q, 4, 10, 0, false, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), rep2.Scanned, "only the blob-less NULL row remains to scan")

	got, ok = compressedBytes(t, db, withBlob)
	require.True(t, ok)
	require.Equal(t, int64(5), got, "already-filled row unchanged on re-run")
}

func TestBackfillCompressedBytes_DryRunWritesNothing(t *testing.T) {
	db := setupIntegrationDB(t)
	q := dao.Use(db)
	ctx := context.Background()

	infoHash := hashFromBytes(0x10, 0x01)
	seedRow(t, db, infoHash, []byte{1, 2, 3})

	rep, err := BackfillCompressedBytes(ctx, q, 1, 10, 0, true, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), rep.Scanned)
	require.Equal(t, int64(1), rep.Updated, "dry-run reports would-fill count")

	_, ok := compressedBytes(t, db, infoHash)
	require.False(t, ok, "dry-run must not write compressed_bytes")
}
