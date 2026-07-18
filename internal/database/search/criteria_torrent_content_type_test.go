package search

import (
	"context"
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

// renderDaoContext is a minimal query.DBContext backed by a sqlmock-driven
// *dao.Query so a DaoCriteria can be rendered to SQL offline (no DB round-trip;
// ToSQL runs in gorm DryRun mode). NewSubQuery is unused by the content-type
// criteria.
type renderDaoContext struct{ q *dao.Query }

func (c renderDaoContext) Query() *dao.Query { return c.q }
func (c renderDaoContext) TableName() string { return model.TableNameTorrentContent }
func (c renderDaoContext) NewSubQuery(context.Context) query.SubQuery {
	return nil
}

func newRenderDaoQuery(t *testing.T) *dao.Query {
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

func renderCriteriaSQL(t *testing.T, criteria query.Criteria) string {
	t.Helper()

	q := newRenderDaoQuery(t)
	raw, err := criteria.Raw(renderDaoContext{q: q})
	require.NoError(t, err)

	gormDB, ok := raw.Query.(*gorm.DB)
	require.True(t, ok, "DaoCriteria should render to a *gorm.DB")

	return gormDB.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return tx.Find(&[]model.TorrentContent{})
	})
}

// F3: the Torznab-specific criterion widens a strict `content_type IN (...)` to
// also admit unclassified (NULL) rows, so an exact title match against the ~64%
// of the corpus with a NULL content_type is no longer hidden. A row classified as
// a *different* type still matches neither branch and is excluded.
func TestTorrentContentTypeOrNullCriteria_RendersInOrIsNull(t *testing.T) {
	sql := renderCriteriaSQL(t, TorrentContentTypeOrNullCriteria(model.ContentTypeMovie))

	// gorm collapses a single-value IN to `= value`; the load-bearing part is the
	// disjunction with the IS NULL branch.
	require.Contains(t, sql, `"torrent_contents"."content_type" = 'movie'`)
	require.Contains(t, sql, `"torrent_contents"."content_type" IS NULL`)
	require.Contains(t, sql, " OR ")

	multi := renderCriteriaSQL(t,
		TorrentContentTypeOrNullCriteria(model.ContentTypeSoftware, model.ContentTypeGame))
	require.Contains(t, multi, `"torrent_contents"."content_type" IN ('software','game')`)
	require.Contains(t, multi, `"torrent_contents"."content_type" IS NULL`)
}

// The strict criterion the general search / GraphQL API uses must NOT admit NULL
// rows — it keeps exact content-type facet semantics.
func TestTorrentContentTypeCriteria_StaysStrict(t *testing.T) {
	sql := renderCriteriaSQL(t, TorrentContentTypeCriteria(model.ContentTypeMovie))

	require.Contains(t, sql, `"torrent_contents"."content_type" = 'movie'`)
	require.NotContains(t, sql, "IS NULL")
}
