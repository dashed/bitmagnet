//go:build integration

package dao_test

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	goose "github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	migrationssql "github.com/bitmagnet-io/bitmagnet/migrations"
)

type tagMutationCase struct {
	ID            string   `json:"id"`
	Operation     string   `json:"operation"`
	InfoHashes    []string `json:"infoHashes"`
	TagNames      []string `json:"tagNames"`
	ErrorContains string   `json:"errorContains"`
	Expected      []tagRow `json:"expected"`
}

type tagRow struct {
	InfoHash string `json:"infoHash"`
	Name     string `json:"name"`
}

var baselineTagRows = []tagRow{
	{InfoHash: "0123456789abcdef0123456789abcdef01234567", Name: "alpha"},
	{InfoHash: "0123456789abcdef0123456789abcdef01234567", Name: "beta"},
	{InfoHash: "1111111111111111111111111111111111111111", Name: "beta"},
	{InfoHash: "1111111111111111111111111111111111111111", Name: "gamma"},
}

// TestTorrentTagMutationParityCorpus proves the shared corpus against the Go
// DAO oracle. The Rust disposable-PG test consumes the same cases afterward.
func TestTorrentTagMutationParityCorpus(t *testing.T) {
	dsn := os.Getenv("BITMAGNET_GRAPHQL_MUTATION_TEST_ADMIN_DATABASE_URL")
	if dsn == "" {
		t.Skip("mutation parity admin DSN is not set")
	}
	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	db, err := gormDB.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	var databaseName string
	require.NoError(t, db.QueryRow("SELECT current_database()").Scan(&databaseName))
	require.Equal(t, "bitmagnet_graphql_mutation_test", databaseName,
		"mutation parity oracle refuses to reset a database without its exact disposable-name sentinel")

	_, err = db.Exec("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	require.NoError(t, err)
	goose.SetBaseFS(migrationssql.FS)
	require.NoError(t, goose.SetDialect("postgres"))
	goose.SetLogger(goose.NopLogger())
	require.NoError(t, goose.UpContext(context.Background(), db, "."))

	for _, oracle := range loadTagMutationCases(t) {
		t.Run(oracle.ID, func(t *testing.T) {
			seedTagMutationBaseline(t, db)
			hashes := parseHashes(t, oracle.InfoHashes)
			query := dao.Use(gormDB)
			var mutationErr error
			switch oracle.Operation {
			case "put":
				mutationErr = query.TorrentTag.Put(context.Background(), hashes, oracle.TagNames)
			case "set":
				mutationErr = query.TorrentTag.Set(context.Background(), hashes, oracle.TagNames)
			case "delete":
				mutationErr = query.TorrentTag.Delete(context.Background(), hashes, oracle.TagNames)
			default:
				t.Fatalf("unknown operation %q", oracle.Operation)
			}
			if oracle.ErrorContains == "" {
				require.NoError(t, mutationErr)
			} else {
				require.ErrorContains(t, mutationErr, oracle.ErrorContains)
			}
			require.Equal(t, oracle.Expected, readTagRows(t, db))
		})
	}
}

func seedTagMutationBaseline(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("TRUNCATE torrent_tags, torrents CASCADE")
	require.NoError(t, err)
	now := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	hashes := map[string]struct{}{}
	for _, row := range baselineTagRows {
		hashes[row.InfoHash] = struct{}{}
	}
	for rawHash := range hashes {
		hash, parseErr := protocol.ParseID(rawHash)
		require.NoError(t, parseErr)
		_, err = db.Exec(
			`INSERT INTO torrents (info_hash, name, size, private, created_at, updated_at)
			 VALUES ($1, $2, 0, false, $3, $3)`,
			hash[:], rawHash, now,
		)
		require.NoError(t, err)
	}
	for _, row := range baselineTagRows {
		hash, parseErr := protocol.ParseID(row.InfoHash)
		require.NoError(t, parseErr)
		_, err = db.Exec(
			`INSERT INTO torrent_tags (info_hash, name, created_at, updated_at)
			 VALUES ($1, $2, $3, $3)`,
			hash[:], row.Name, now,
		)
		require.NoError(t, err)
	}
}

func parseHashes(t *testing.T, values []string) []protocol.ID {
	t.Helper()
	result := make([]protocol.ID, 0, len(values))
	for _, value := range values {
		hash, err := protocol.ParseID(value)
		require.NoError(t, err)
		result = append(result, hash)
	}
	return result
}

func readTagRows(t *testing.T, db *sql.DB) []tagRow {
	t.Helper()
	rows, err := db.Query(
		`SELECT encode(info_hash, 'hex'), name
		 FROM torrent_tags ORDER BY info_hash, name`,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	result := make([]tagRow, 0)
	for rows.Next() {
		var row tagRow
		require.NoError(t, rows.Scan(&row.InfoHash, &row.Name))
		result = append(result, row)
	}
	require.NoError(t, rows.Err())
	return result
}

func loadTagMutationCases(t *testing.T) []tagMutationCase {
	t.Helper()
	file, err := os.Open(filepath.Join(tagMutationParityDir(t), "corpus.jsonl"))
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()

	cases := make([]tagMutationCase, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var oracle tagMutationCase
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &oracle))
		sort.Slice(oracle.Expected, func(i, j int) bool {
			return strings.Compare(
				oracle.Expected[i].InfoHash+"\x00"+oracle.Expected[i].Name,
				oracle.Expected[j].InfoHash+"\x00"+oracle.Expected[j].Name,
			) < 0
		})
		cases = append(cases, oracle)
	}
	require.NoError(t, scanner.Err())
	require.NotEmpty(t, cases)
	return cases
}

func tagMutationParityDir(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Join(
		filepath.Dir(filename), "..", "..", "..", "testdata", "parity", "graphql-torrent-tag-mutations",
	)
}
