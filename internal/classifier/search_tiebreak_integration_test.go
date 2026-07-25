//go:build integration

package classifier

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	migrationssql "github.com/bitmagnet-io/bitmagnet/migrations"
	goose "github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// The classifier's content search orders by ts_rank_cd, which is degenerate for the
// quoted phrase queries it issues: in production a search for "cinderella" returns
// 70 movies all ranked exactly 1.0. Without a total order, which rows the LIMIT 10
// admits - and therefore which content a torrent is attached to - is decided by
// whichever plan the planner picked. These tests seed exactly that shape (more tied
// candidates than the limit) and assert the results are identical across repeated
// executions and across forced plan variations.

const (
	tiebreakTiedCandidates = 26
	tiebreakLimit          = 10
)

func openTiebreakDB(t *testing.T, dsn string) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	return db
}

func setupTiebreakDB(t *testing.T) (*gorm.DB, string) {
	t.Helper()

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set, skipping content-search tiebreak integration test")
	}

	db := openTiebreakDB(t, dsn)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Ping())

	_, err = sqlDB.Exec("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
	require.NoError(t, err)

	goose.SetBaseFS(migrationssql.FS)
	require.NoError(t, goose.SetDialect("postgres"))
	goose.SetLogger(goose.NopLogger())
	require.NoError(t, goose.UpContext(context.Background(), sqlDB, "."))

	return db, dsn
}

// insertContent seeds a content row with an explicit tsvector, so tests control the
// relevance rank exactly. weight 'D' is postgres' default and scores lowest under
// ts_rank_cd; 'A' scores highest.
func insertContent(t *testing.T, db *gorm.DB, id, title, lexemes string, weight rune) {
	t.Helper()

	require.NoError(t, db.Exec(`
		INSERT INTO content (type, source, id, title, tsv, created_at, updated_at)
		VALUES ('movie', 'tmdb', ?, ?, setweight(to_tsvector('simple', ?), ?), now(), now())
	`, id, title, lexemes, string(weight)).Error)
}

func insertContentAttribute(t *testing.T, db *gorm.DB, contentID, source, key, value string) {
	t.Helper()

	require.NoError(t, db.Exec(`
		INSERT INTO content_attributes
			(content_type, content_source, content_id, source, key, value, created_at, updated_at)
		VALUES ('movie', 'tmdb', ?, ?, ?, ?, now(), now())
	`, contentID, source, key, value).Error)
}

// seedTiedCandidates inserts more identically-ranked candidates than the search's
// limit, in reverse identity order so physical row order disagrees with the tiebreak,
// plus non-matching noise rows to give the planner a choice of plans.
func seedTiedCandidates(t *testing.T, db *gorm.DB) {
	t.Helper()

	for i := tiebreakTiedCandidates; i >= 1; i-- {
		insertContent(t, db, fmt.Sprintf("c%02d", i), "Cinderella", "cinderella", 'D')
	}

	for i := range 500 {
		title := fmt.Sprintf("Unrelated Feature %d", i)
		insertContent(t, db, fmt.Sprintf("n%04d", i), title, title, 'D')
	}

	require.NoError(t, db.Exec("ANALYZE content").Error)
}

func newLocalSearch(t *testing.T, db *gorm.DB) localSearch {
	t.Helper()

	daoQuery := dao.Use(db)
	resource := search.New(search.Params{Query: lazy.New(func() (*dao.Query, error) {
		return daoQuery, nil
	})})
	searcher, err := resource.Search.Get()
	require.NoError(t, err)

	return localSearch{Search: searcher}
}

type plannerVariant struct {
	name     string
	settings map[string]string
}

// plannerVariants forces materially different query plans. Each variant is applied at
// database scope and picked up by a freshly opened connection pool, which is
// pool-safe in a way that a session-level SET is not.
func plannerVariants() []plannerVariant {
	return []plannerVariant{
		{name: "default", settings: nil},
		{name: "no_seqscan", settings: map[string]string{"enable_seqscan": "off"}},
		{name: "no_index", settings: map[string]string{
			"enable_indexscan":  "off",
			"enable_bitmapscan": "off",
		}},
		{name: "parallel", settings: map[string]string{
			"parallel_setup_cost":             "0",
			"parallel_tuple_cost":             "0",
			"min_parallel_table_scan_size":    "0",
			"max_parallel_workers_per_gather": "4",
		}},
	}
}

// forEachPlan runs fn against a search bound to a connection pool configured for each
// planner variant.
func forEachPlan(t *testing.T, db *gorm.DB, dsn string, fn func(t *testing.T, l localSearch)) {
	t.Helper()

	var dbName string
	require.NoError(t, db.Raw("SELECT current_database()").Scan(&dbName).Error)

	for _, variant := range plannerVariants() {
		t.Run(variant.name, func(t *testing.T) {
			for setting, value := range variant.settings {
				require.NoError(t, db.Exec(
					fmt.Sprintf("ALTER DATABASE %q SET %s = %s", dbName, setting, value),
				).Error)
			}

			t.Cleanup(func() {
				for setting := range variant.settings {
					require.NoError(t, db.Exec(
						fmt.Sprintf("ALTER DATABASE %q RESET %s", dbName, setting),
					).Error)
				}
			})

			variantDB := openTiebreakDB(t, dsn)
			t.Cleanup(func() {
				if sqlDB, err := variantDB.DB(); err == nil {
					_ = sqlDB.Close()
				}
			})

			fn(t, newLocalSearch(t, variantDB))
		})
	}
}

func contentIDs(items []search.ContentResultItem) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}

	return ids
}

func searchIDs(t *testing.T, l localSearch, title string) []string {
	t.Helper()

	result, err := l.Content(
		context.Background(),
		contentBySearchOptions(model.ContentTypeMovie, title, model.Year(0))...,
	)
	require.NoError(t, err)

	return contentIDs(result.Items)
}

// The premise of the fix: every candidate really does score the same rank, so the
// ordering within the result set is decided entirely by the tiebreak.
func TestContentSearchCandidatesAreRankTied(t *testing.T) {
	db, _ := setupTiebreakDB(t)
	seedTiedCandidates(t, db)

	var distinctRanks int64

	require.NoError(t, db.Raw(`
		SELECT COUNT(DISTINCT ts_rank_cd(tsv, '''cinderella'''::tsquery))
		FROM content
		WHERE tsv @@ '''cinderella'''::tsquery
	`).Scan(&distinctRanks).Error)
	require.Equal(t, int64(1), distinctRanks,
		"expected every candidate to be rank-tied, which is what makes the ordering plan-dependent")

	var candidates int64
	require.NoError(t, db.Raw(`
		SELECT COUNT(*) FROM content WHERE tsv @@ '''cinderella'''::tsquery
	`).Scan(&candidates).Error)
	require.Greater(t, candidates, int64(tiebreakLimit),
		"expected more tied candidates than the search limit, so LIMIT truncates an untied set")
}

// With more tied candidates than the limit, the same 10 rows must come back in the
// same order every time - including under plans that scan the table differently.
// This is the case that could previously drop the correct match.
func TestContentSearchTruncatesTiedCandidatesDeterministically(t *testing.T) {
	db, dsn := setupTiebreakDB(t)
	seedTiedCandidates(t, db)

	want := make([]string, 0, tiebreakLimit)
	for i := 1; i <= tiebreakLimit; i++ {
		want = append(want, fmt.Sprintf("c%02d", i))
	}

	forEachPlan(t, db, dsn, func(t *testing.T, l localSearch) {
		for run := range 3 {
			got := searchIDs(t, l, "Cinderella")
			require.Equal(t, want, got, "run %d returned a different LIMIT window", run)
		}
	})
}

// The classification-facing assertion: identical titles are all levenshtein distance
// 0, so the winner is whichever candidate the search returned first. That has to be
// the same content on every execution.
func TestContentBySearchPicksSameContentEveryRun(t *testing.T) {
	db, dsn := setupTiebreakDB(t)
	seedTiedCandidates(t, db)

	forEachPlan(t, db, dsn, func(t *testing.T, l localSearch) {
		for run := range 3 {
			content, err := l.ContentBySearch(
				context.Background(), model.ContentTypeMovie, "Cinderella", model.Year(0),
			)
			require.NoError(t, err)
			require.Equal(t, "c01", content.ID, "run %d attached a different content", run)
			require.Equal(t, model.ContentTypeMovie, content.Type)
			require.Equal(t, "tmdb", content.Source)
		}
	})
}

// Relevance stays the primary ordering: a higher-ranked row wins even though the
// tiebreak would sort it last.
func TestContentSearchRelevanceOutranksTiebreak(t *testing.T) {
	db, dsn := setupTiebreakDB(t)
	seedTiedCandidates(t, db)
	// weight A scores highest under ts_rank_cd, and "z99" sorts last by identity.
	insertContent(t, db, "z99", "Cinderella", "cinderella", 'A')
	require.NoError(t, db.Exec("ANALYZE content").Error)

	forEachPlan(t, db, dsn, func(t *testing.T, l localSearch) {
		for run := range 3 {
			got := searchIDs(t, l, "Cinderella")
			require.Equal(t, "z99", got[0], "run %d did not rank the best match first", run)
			require.Equal(t, append([]string{"z99"}, "c01", "c02", "c03", "c04",
				"c05", "c06", "c07", "c08", "c09"), got)
		}
	})
}

// A non-tmdb ref resolves through content_attributes, where several content rows can
// carry the same identifier. The LIMIT 1 pick has to be reproducible.
func TestContentByIDAlternativeIdentifierMultiMatchIsDeterministic(t *testing.T) {
	db, dsn := setupTiebreakDB(t)

	// insert in reverse identity order so physical order disagrees with the tiebreak
	for _, id := range []string{"a03", "a02", "a01"} {
		insertContent(t, db, id, "Ambiguous Feature", "ambiguous feature", 'D')
		insertContentAttribute(t, db, id, "imdb", "id", "tt7654321")
	}

	require.NoError(t, db.Exec("ANALYZE content").Error)

	ref := model.ContentRef{Type: model.ContentTypeMovie, Source: "imdb", ID: "tt7654321"}

	forEachPlan(t, db, dsn, func(t *testing.T, l localSearch) {
		for run := range 3 {
			content, err := l.ContentByID(context.Background(), ref)
			require.NoError(t, err)
			require.Equal(t, "a01", content.ID, "run %d resolved a different content", run)
		}
	})
}
