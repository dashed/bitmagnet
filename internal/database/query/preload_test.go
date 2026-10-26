package query

import (
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gen/field"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// preloadSQL runs a content query that preloads relation through preloadArgs
// against a sqlmock database returning one content row, and returns every
// statement issued, in order.
func preloadSQL(t *testing.T, relation func(q *dao.Query) field.RelationField) []string {
	t.Helper()

	var (
		mu         sync.Mutex
		statements []string
	)

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(
		func(_, actual string) error {
			mu.Lock()
			defer mu.Unlock()

			statements = append(statements, actual)

			return nil
		},
	)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = mockDB.Close() })

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn:                 mockDB,
		PreferSimpleProtocol: true,
	}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)

	mock.ExpectQuery("content").WillReturnRows(
		sqlmock.NewRows([]string{"type", "source", "id", "title"}).AddRow("movie", "tmdb", "1", "a"),
	)
	mock.ExpectQuery("preload").WillReturnRows(sqlmock.NewRows([]string{"content_type"}))

	rel := relation(dao.Use(db))
	args, err := preloadArgs(rel)
	require.NoError(t, err)

	var contents []model.Content
	require.NoError(t, db.Model(&model.Content{}).Preload(rel.Name(), args...).Find(&contents).Error)

	return statements
}

// A relation's Clauses used to be dropped: the relation was handed to GORM as an
// inline condition, which GORM reads as a struct with no fields. An ORDER BY set
// on the relation has to reach the preload query.
func TestPreloadAppliesRelationClauses(t *testing.T) {
	t.Parallel()

	statements := preloadSQL(t, func(q *dao.Query) field.RelationField {
		return q.Content.Attributes.Clauses(clause.OrderBy{Columns: []clause.OrderByColumn{
			{Column: clause.Column{Table: model.TableNameContentAttribute, Name: "source"}},
			{Column: clause.Column{Table: model.TableNameContentAttribute, Name: "key"}},
		}})
	})

	require.Len(t, statements, 2)
	require.True(t, strings.HasPrefix(statements[1], `SELECT * FROM "content_attributes" WHERE`), statements[1])
	require.True(t, strings.HasSuffix(statements[1],
		` ORDER BY "content_attributes"."source","content_attributes"."key"`), statements[1])
}

// A relation without Clauses or Scopes renders exactly the preload query it
// always did: no condition, no ordering.
func TestPreloadOfPlainRelationIsUnchanged(t *testing.T) {
	t.Parallel()

	statements := preloadSQL(t, func(q *dao.Query) field.RelationField {
		return q.Content.Attributes.RelationField
	})

	require.Equal(t, []string{
		`SELECT * FROM "content"`,
		`SELECT * FROM "content_attributes" WHERE ("content_attributes"."content_type",` +
			`"content_attributes"."content_source","content_attributes"."content_id") IN (($1,$2,$3))`,
	}, statements)
}

func TestPreloadAppliesRelationScopes(t *testing.T) {
	t.Parallel()

	statements := preloadSQL(t, func(q *dao.Query) field.RelationField {
		return q.Content.Attributes.Scopes(func(tx *gorm.DB) *gorm.DB {
			return tx.Order("key DESC")
		})
	})

	require.Len(t, statements, 2)
	require.True(t, strings.HasSuffix(statements[1], " ORDER BY key DESC"), statements[1])
}

// Conditions, selects, order columns and paging have no lossless translation
// into GORM preload arguments, so they are refused rather than dropped.
func TestPreloadRejectsUntranslatableRelation(t *testing.T) {
	t.Parallel()

	q := dao.Use(newOfflineDB(t))

	for name, rel := range map[string]field.RelationField{
		"on":     q.Content.Attributes.On(q.ContentAttribute.Key.Eq("id")),
		"select": q.Content.Attributes.Select(q.ContentAttribute.Key),
		"order":  q.Content.Attributes.Order(q.ContentAttribute.Key),
		"limit":  q.Content.Attributes.Limit(1),
		"offset": q.Content.Attributes.Offset(1),
	} {
		_, err := preloadArgs(rel)
		require.Error(t, err, name)
	}
}
