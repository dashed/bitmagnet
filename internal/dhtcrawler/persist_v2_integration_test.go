//go:build integration

package dhtcrawler

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	migrationssql "github.com/bitmagnet-io/bitmagnet/migrations"
	goose "github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupV2TestDB(t *testing.T) *gorm.DB {
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

	cleanupV2Schema(t, sqlDB)

	goose.SetBaseFS(migrationssql.FS)
	require.NoError(t, goose.SetDialect("postgres"))
	goose.SetLogger(goose.NopLogger())
	require.NoError(t, goose.UpContext(context.Background(), sqlDB, "."))

	t.Cleanup(func() {
		cleanupV2Schema(t, sqlDB)
	})

	return db
}

func cleanupV2Schema(t *testing.T, sqlDB *sql.DB) {
	t.Helper()

	_, err := sqlDB.Exec("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
	require.NoError(t, err)
}

// insertTorrentModel persists a Torrent built by createTorrentModel, omitting
// the associations/generated columns that are out of scope for these tests so
// the assertions can focus on the v1/v2/meta_version columns.
func insertTorrentModel(db *gorm.DB, torrent *model.Torrent) error {
	return db.Omit(
		"Extension", "Hint", "Contents", "Sources", "Files", "Pieces", "Tags",
	).Create(torrent).Error
}

func TestV2TorrentColumnsRoundTrip(t *testing.T) {
	db := setupV2TestDB(t)

	pureHash, pureParsed := loadFixtureParsed(t, "testdata/bittorrent-v2-test.torrent", true)
	pure, err := createTorrentModel(pureHash, pureParsed, false, 1000)
	require.NoError(t, err)
	require.NoError(t, insertTorrentModel(db, &pure))

	hybridHash, hybridParsed := loadFixtureParsed(t, "testdata/bittorrent-v2-hybrid-test.torrent", false)
	hybrid, err := createTorrentModel(hybridHash, hybridParsed, false, 1000)
	require.NoError(t, err)
	require.NoError(t, insertTorrentModel(db, &hybrid))

	// Read pure-v2 back and assert each column.
	var gotPure model.Torrent

	require.NoError(t, db.Where("info_hash = ?", pureHash).First(&gotPure).Error)
	assert.Equal(t, pureHash, gotPure.InfoHash)
	assert.Len(t, gotPure.InfoHash[:], 20)
	assert.Nil(t, gotPure.InfoHashV1)
	require.NotNil(t, gotPure.InfoHashV2)
	assert.Len(t, gotPure.InfoHashV2[:], 32)
	assert.Equal(t, *pure.InfoHashV2, *gotPure.InfoHashV2)
	assert.True(t, gotPure.MetaVersion.Valid)
	assert.Equal(t, uint16(2), gotPure.MetaVersion.Uint16)

	// Read hybrid back and assert each column.
	var gotHybrid model.Torrent

	require.NoError(t, db.Where("info_hash = ?", hybridHash).First(&gotHybrid).Error)
	assert.Equal(t, hybridHash, gotHybrid.InfoHash)
	require.NotNil(t, gotHybrid.InfoHashV1)
	assert.Equal(t, hybridHash, *gotHybrid.InfoHashV1)
	require.NotNil(t, gotHybrid.InfoHashV2)
	assert.Len(t, gotHybrid.InfoHashV2[:], 32)
	assert.True(t, gotHybrid.MetaVersion.Valid)
	assert.Equal(t, uint16(2), gotHybrid.MetaVersion.Uint16)
}

// TestV2DuplicateInfoHashV2Coexist models a hybrid torrent discovered on the DHT
// under BOTH its v1 and its truncated-v2 infohash: two rows with DIFFERENT 20-byte
// primary keys but the SAME full info_hash_v2. The info_hash_v2 index is
// intentionally NOT unique (migration 00023) so the second insert does not error —
// a UNIQUE index would abort the batched persist upsert (which only arbitrates
// ON CONFLICT (info_hash)) and roll back the whole batch. Exact v2-identity dedup
// is deferred to G1b.
func TestV2DuplicateInfoHashV2Coexist(t *testing.T) {
	db := setupV2TestDB(t)

	pureHash, pureParsed := loadFixtureParsed(t, "testdata/bittorrent-v2-test.torrent", true)
	first, err := createTorrentModel(pureHash, pureParsed, false, 1000)
	require.NoError(t, err)
	require.NoError(t, insertTorrentModel(db, &first))

	// A different PK but the SAME full v2 hash must be accepted (no unique index).
	dup, err := createTorrentModel(pureHash, pureParsed, false, 1000)
	require.NoError(t, err)

	var otherPK protocol.ID

	copy(otherPK[:], []byte("ZZZZZZZZZZZZZZZZZZZZ"))
	dup.InfoHash = otherPK
	dup.Sources = nil

	require.NoError(t, insertTorrentModel(db, &dup),
		"duplicate info_hash_v2 must be accepted (index is non-unique; dedup is G1b)")

	require.NotNil(t, first.InfoHashV2)

	var count int64

	require.NoError(t, db.Table("torrents").
		Where("info_hash_v2 = ?", first.InfoHashV2.Bytes()).
		Count(&count).Error)
	assert.Equal(t, int64(2), count, "both discoveries of the same v2 identity coexist")
}

func TestV2MultipleV1RowsCoexist(t *testing.T) {
	db := setupV2TestDB(t)

	now := time.Now()
	makeV1 := func(seed byte) *model.Torrent {
		var h protocol.ID
		for i := range h {
			h[i] = seed + byte(i)
		}

		v1 := h

		return &model.Torrent{
			InfoHash:    h,
			InfoHashV1:  &v1,
			MetaVersion: model.NewNullUint16(1),
			Name:        "v1-torrent",
			Size:        1000,
			FilesStatus: model.FilesStatusSingle,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
	}

	a := makeV1(0x01)
	require.NoError(t, insertTorrentModel(db, a))

	// Two rows with NULL info_hash_v2 must coexist (NULLs are distinct in PG).
	b := makeV1(0x80)
	require.NoError(t, insertTorrentModel(db, b))

	var count int64

	require.NoError(t, db.Table("torrents").Where("info_hash_v2 IS NULL").Count(&count).Error)
	assert.Equal(t, int64(2), count)
}

// TestV2MigrationUpDown applies migrations through v22, inserts a legacy v1 row,
// then applies 00023 and asserts the new columns exist and are backfilled, and
// that the down migration removes them.
func TestV2MigrationUpDown(t *testing.T) {
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

	cleanupV2Schema(t, sqlDB)
	t.Cleanup(func() { cleanupV2Schema(t, sqlDB) })

	ctx := context.Background()

	goose.SetBaseFS(migrationssql.FS)
	require.NoError(t, goose.SetDialect("postgres"))
	goose.SetLogger(goose.NopLogger())

	// Apply everything up to (but not including) the v2 migration.
	require.NoError(t, goose.UpToContext(ctx, sqlDB, ".", 22))

	hasColumn := func(col string) bool {
		var n int64

		require.NoError(t, db.Raw(
			`SELECT count(*) FROM information_schema.columns WHERE table_name = 'torrents' AND column_name = ?`,
			col,
		).Scan(&n).Error)

		return n > 0
	}

	assert.False(t, hasColumn("info_hash_v1"), "v2 columns must not exist before 00023")

	// Insert a legacy v1 row (raw map insert: model columns don't exist yet).
	now := time.Now()
	legacyHash := []byte("legacy-v1-hash-bytes")
	require.NoError(t, db.Table("torrents").Create(map[string]any{
		"info_hash":    legacyHash,
		"name":         "legacy",
		"size":         1000,
		"private":      false,
		"files_status": string(model.FilesStatusSingle),
		"created_at":   now,
		"updated_at":   now,
	}).Error)

	// Apply 00023.
	require.NoError(t, goose.UpContext(ctx, sqlDB, "."))

	assert.True(t, hasColumn("info_hash_v1"))
	assert.True(t, hasColumn("info_hash_v2"))
	assert.True(t, hasColumn("meta_version"))

	// Backfill: existing v1 row gets info_hash_v1 = info_hash, meta_version = 1,
	// info_hash_v2 NULL.
	row := struct {
		InfoHashV1  []byte
		InfoHashV2  []byte
		MetaVersion sql.NullInt64
	}{}
	require.NoError(t, db.Table("torrents").
		Select("info_hash_v1", "info_hash_v2", "meta_version").
		Where("info_hash = ?", legacyHash).
		Scan(&row).Error)
	assert.Equal(t, legacyHash, row.InfoHashV1)
	assert.Nil(t, row.InfoHashV2)
	assert.True(t, row.MetaVersion.Valid)
	assert.Equal(t, int64(1), row.MetaVersion.Int64)

	// Down migration removes the columns again.
	require.NoError(t, goose.DownContext(ctx, sqlDB, "."))
	assert.False(t, hasColumn("info_hash_v1"))
	assert.False(t, hasColumn("info_hash_v2"))
	assert.False(t, hasColumn("meta_version"))
}
