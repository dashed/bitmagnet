//go:build integration

package gqlmodel

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	dbsearch "github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type torrentTagsOracleInput struct {
	Prefix     string   `json:"prefix,omitempty"`
	Exclusions []string `json:"exclusions,omitempty"`
}

type torrentTagsOracleCase struct {
	ID       string                    `json:"id"`
	Input    *torrentTagsOracleInput   `json:"input"`
	Expected torrentTagsOracleExpected `json:"expected"`
}

type torrentTagsOracleExpected struct {
	Suggestions []torrentTagsOracleSuggested `json:"suggestions"`
}

type torrentTagsOracleSuggested struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func seedTorrentTagsAndVerifyGoOracle(t *testing.T, gormDB *gorm.DB, db *sql.DB) {
	t.Helper()
	fixtures := loadTorrentFilesFixtures(t)
	require.Len(t, fixtures, 2)
	tagsByHash := map[string][]string{
		fixtures[0].InfoHash: append(
			[]string{"movie", "movie-hd", "trusted"},
			"tag-00", "tag-01", "tag-02", "tag-03", "tag-04", "tag-05",
			"tag-06", "tag-07", "tag-08", "tag-09", "tag-10", "tag-11",
		),
		fixtures[1].InfoHash: {"movie", "movie-hd", "music"},
		zeroBlobHash:         {"movie", "movie-old"},
		missingSummaryHash:   {"movie", "move", "trusted"},
		mismatchedBytesHash:  {"movie-old", "movie-hd", "trusted"},
	}
	now := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	for rawHash, tags := range tagsByHash {
		hash, err := protocol.ParseID(rawHash)
		require.NoError(t, err)
		for _, tag := range tags {
			_, err = db.Exec(
				`INSERT INTO torrent_tags (info_hash, name, created_at, updated_at)
				 VALUES ($1, $2, $3, $3)`,
				hash[:], tag, now,
			)
			require.NoError(t, err)
		}
	}

	result := dbsearch.New(dbsearch.Params{
		Query: lazy.New(func() (*dao.Query, error) { return dao.Use(gormDB), nil }),
	})
	searcher, err := result.Search.Get()
	require.NoError(t, err)
	for _, oracle := range loadTorrentTagsOracle(t) {
		query := dbsearch.SuggestTagsQuery{}
		if oracle.Input != nil {
			query.Prefix = oracle.Input.Prefix
			query.Exclusions = oracle.Input.Exclusions
		}
		actual, err := searcher.TorrentSuggestTags(context.Background(), query)
		require.NoError(t, err, "oracle case %q", oracle.ID)
		require.Equal(t, oracle.Expected, torrentTagsOracleExpectedFromGo(actual), "oracle case %q", oracle.ID)
	}
}

func torrentTagsOracleExpectedFromGo(result dbsearch.TorrentSuggestTagsResult) torrentTagsOracleExpected {
	// Go orders by SQL alias total_count, but SuggestedTag.Count has no matching
	// scan tag and therefore remains zero. The corpus intentionally records the
	// observable GraphQL contract instead of correcting Go in a Rust parity test.
	suggestions := make([]torrentTagsOracleSuggested, 0, len(result.Suggestions))
	for _, suggestion := range result.Suggestions {
		suggestions = append(suggestions, torrentTagsOracleSuggested{
			Name:  suggestion.Name,
			Count: suggestion.Count,
		})
	}
	return torrentTagsOracleExpected{Suggestions: suggestions}
}

func loadTorrentTagsOracle(t *testing.T) []torrentTagsOracleCase {
	t.Helper()
	file, err := os.Open(filepath.Join(torrentTagsParityDir(t), "corpus.jsonl"))
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()

	var cases []torrentTagsOracleCase
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var oracle torrentTagsOracleCase
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &oracle))
		cases = append(cases, oracle)
	}
	require.NoError(t, scanner.Err())
	require.NotEmpty(t, cases)
	return cases
}

func torrentTagsParityDir(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Join(filepath.Dir(filename), "..", "..", "..", "testdata", "parity", "graphql-torrent-tags")
}
