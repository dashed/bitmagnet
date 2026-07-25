package classifier

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// newResolveDaoQuery returns a sqlmock-backed *dao.Query, needed only so the join /
// preload options can reference dao field expressions. No SQL is issued.
func newResolveDaoQuery(t *testing.T) *dao.Query {
	t.Helper()

	mockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = mockDB.Close() })

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn:                 mockDB,
		PreferSimpleProtocol: true,
	}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)

	return dao.Use(db)
}

type resolvedColumn struct {
	table string
	name  string
	desc  bool
}

func resolvedColumns(t *testing.T, opts ...query.Option) []resolvedColumn {
	t.Helper()

	resolved, err := query.ResolveOptions(newResolveDaoQuery(t), opts...)
	require.NoError(t, err)

	columns := make([]resolvedColumn, 0, len(resolved.OrderBy))
	for _, c := range resolved.OrderBy {
		columns = append(columns, resolvedColumn{
			table: c.Column.Table,
			name:  c.Column.Name,
			desc:  c.Desc,
		})
	}

	return columns
}

// The content search orders by relevance, which ties constantly: ts_rank_cd returns
// the same rank for every candidate of a typical quoted title query. The content
// primary key is appended so the LIMIT window, and the match picked from it, are the
// same on every execution regardless of the plan chosen.
func TestContentBySearchOrdersByRelevanceThenIdentity(t *testing.T) {
	t.Parallel()

	want := []resolvedColumn{
		{name: query.QueryStringRankField, desc: true},
		{table: model.TableNameContent, name: "type"},
		{table: model.TableNameContent, name: "source"},
		{table: model.TableNameContent, name: "id"},
	}

	t.Run("without year", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, want, resolvedColumns(t,
			contentBySearchOptions(model.ContentTypeMovie, "cinderella", model.Year(0))...))
	})

	t.Run("with year", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, want, resolvedColumns(t,
			contentBySearchOptions(model.ContentTypeMovie, "cinderella", model.Year(2002))...))
	})
}

// Relevance has to stay the primary ordering: the tiebreak may only order rows of
// equal rank, never outrank a better match.
func TestContentBySearchKeepsRelevanceAsPrimaryOrdering(t *testing.T) {
	t.Parallel()

	resolved, err := query.ResolveOptions(
		newResolveDaoQuery(t),
		contentBySearchOptions(model.ContentTypeMovie, "cinderella", model.Year(0))...,
	)
	require.NoError(t, err)
	require.NotEmpty(t, resolved.OrderBy)
	require.Equal(t, query.QueryStringRankField, resolved.OrderBy[0].Column.Name)
	require.True(t, resolved.OrderBy[0].Desc)
	require.True(t, resolved.HasTSQuery)
	require.True(t, resolved.Limit.Valid)
	require.Equal(t, uint(10), resolved.Limit.Uint)
}

func TestContentByIDOrdering(t *testing.T) {
	t.Parallel()

	t.Run("alternative identifier is ordered by identity", func(t *testing.T) {
		t.Parallel()

		// A non-tmdb ref resolves through content_attributes, which can match several
		// content rows - LIMIT 1 needs a total order to pick reproducibly.
		require.Equal(t, []resolvedColumn{
			{table: model.TableNameContent, name: "type"},
			{table: model.TableNameContent, name: "source"},
			{table: model.TableNameContent, name: "id"},
		}, resolvedColumns(t, contentByIDOptions(model.ContentRef{
			Type:   model.ContentTypeMovie,
			Source: "imdb",
			ID:     "tt0100000",
		})...))
	})

	t.Run("canonical identifier is unchanged", func(t *testing.T) {
		t.Parallel()

		// The tmdb ref hits the content primary key, which matches at most one row,
		// so it stays unordered.
		require.Empty(t, resolvedColumns(t, contentByIDOptions(model.ContentRef{
			Type:   model.ContentTypeMovie,
			Source: "tmdb",
			ID:     "1234",
		})...))
	})
}
