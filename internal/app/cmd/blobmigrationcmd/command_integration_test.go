//go:build integration

package blobmigrationcmd

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	migrationssql "github.com/bitmagnet-io/bitmagnet/migrations"
	goose "github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
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

// TestUpsertKVWritesKeyValuesTable exercises the REAL command upsertKV helper (used by
// start/pause/resume/verify) against a real Postgres schema.
//
// REGRESSION: before the table-binding fix, upsertKV wrote through d.Torrent.UnderlyingDB() (a
// torrents-bound session) so `start` panicked with `column "key" of relation "torrents" does not
// exist` — the migration could never even begin. The pre-existing tests missed this because they
// reimplemented the KV write with a clean *gorm.DB instead of calling the production helper.
func TestUpsertKVWritesKeyValuesTable(t *testing.T) {
	db := setupIntegrationDB(t)
	d := dao.Use(db)
	cliCtx := &cli.Context{Context: context.Background()}
	now := time.Now()

	// These are the exact writes `start` performs after the total-count query.
	require.NoError(t, upsertKV(cliCtx, d, kvKeyStatus, statusRunning, now),
		"upsertKV must target key_values, not torrents (table-binding regression)")
	require.NoError(t, upsertKV(cliCtx, d, kvKeyTotal, "15800000", now))

	// OnConflict path: re-upserting the same key updates in place (no error, no duplicate row).
	require.NoError(t, upsertKV(cliCtx, d, kvKeyStatus, statusCompleted, now))

	var status, total string
	require.NoError(t, db.Table("key_values").Select("value").Where("key = ?", kvKeyStatus).Scan(&status).Error)
	require.NoError(t, db.Table("key_values").Select("value").Where("key = ?", kvKeyTotal).Scan(&total).Error)
	assert.Equal(t, statusCompleted, status, "status should be updated in place by the OnConflict upsert")
	assert.Equal(t, "15800000", total)

	var n int64
	require.NoError(t, db.Table("key_values").Where("key LIKE ?", "blob_migration:%").Count(&n).Error)
	assert.Equal(t, int64(2), n, "exactly two key_values rows; the OnConflict upsert must not duplicate")
}

func TestVerifyFullWritesVerifiedAtWithLegacyDuplicatePaths(t *testing.T) {
	db := setupIntegrationDB(t)
	infoHash := commandHashFromBytes(0x10)
	seedCommandMigratedTorrent(t, db, infoHash, []string{" ", " ", "video.mkv", " "})

	output, err := runVerifyCommand(t, db, "--full")
	require.NoError(t, err, output)

	assert.Contains(t, output, "Verification PASSED.")
	assert.NotEmpty(t, getCommandKV(t, db, kvKeyVerifiedAt))
}

func TestVerifyFileIndexSetDoesNotWriteVerifiedAt(t *testing.T) {
	db := setupIntegrationDB(t)
	infoHash := commandHashFromBytes(0x20)
	seedCommandMigratedTorrent(t, db, infoHash, []string{" ", " ", "video.mkv"})
	old := "2000-01-02T03:04:05Z"
	require.NoError(t, upsertKV(&cli.Context{Context: context.Background()}, dao.Use(db), kvKeyVerifiedAt, old, time.Now()))

	output, err := runVerifyCommand(t, db, "--full", "--file-index-set")
	require.NoError(t, err, output)

	assert.Contains(t, output, "Verification PASSED (read-only; timestamp not updated).")
	assert.Equal(t, old, getCommandKV(t, db, kvKeyVerifiedAt))
}

func runVerifyCommand(t *testing.T, db *gorm.DB, args ...string) (string, error) {
	t.Helper()

	d := dao.Use(db)
	p := Params{
		Config: blobmigration.Config{Parallelism: 1, ChunkSize: 2},
		Dao: lazy.New(func() (*dao.Query, error) {
			return d, nil
		}),
	}

	app := cli.NewApp()
	var out bytes.Buffer
	app.Writer = &out
	app.Commands = []*cli.Command{p.verifyCmd()}

	runArgs := append([]string{"bitmagnet", "verify"}, args...)
	err := app.Run(runArgs)

	return out.String(), err
}

func commandHashFromBytes(b ...byte) protocol.ID {
	var id protocol.ID
	copy(id[:], b)

	return id
}

func seedCommandMigratedTorrent(t *testing.T, db *gorm.DB, infoHash protocol.ID, paths []string) {
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

func getCommandKV(t *testing.T, db *gorm.DB, key string) string {
	t.Helper()

	var value string
	require.NoError(t, db.Table("key_values").Select("value").Where("key = ?", key).Scan(&value).Error)

	return value
}
