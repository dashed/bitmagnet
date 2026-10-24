package search

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// recordingDaoQuery returns a *dao.Query on a sqlmock database that answers every
// query with no rows, and a function returning the statements issued so far.
func recordingDaoQuery(t *testing.T, queries int) (*dao.Query, func() []string) {
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

	for range queries {
		mock.ExpectQuery("any").WillReturnRows(sqlmock.NewRows([]string{"content_type"}))
	}

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn:                 mockDB,
		PreferSimpleProtocol: true,
	}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)

	return dao.Use(db), func() []string {
		mu.Lock()
		defer mu.Unlock()

		return append([]string(nil), statements...)
	}
}

// The collection links come back per content in query order, and that order
// decides where Content.UpdateTsv places each genre name, so the link query has to
// order by the linked collection's identity.
func TestContentCollectionsHydratorOrdersLinksByCollectionIdentity(t *testing.T) {
	t.Parallel()

	q, statements := recordingDaoQuery(t, 1)

	_, err := contentCollectionsHydrator{}.GetJoinSubs(
		context.Background(),
		renderDaoContext{q: q},
		[]model.ContentRef{{Type: model.ContentTypeMovie, Source: "tmdb", ID: "1"}},
	)
	require.NoError(t, err)

	got := statements()
	require.Len(t, got, 1)
	require.True(t, strings.HasPrefix(got[0], `SELECT * FROM "content_collections_content" WHERE`), got[0])
	require.True(t, strings.HasSuffix(got[0], ` ORDER BY "content_collections_content"."content_collection_type",`+
		`"content_collections_content"."content_collection_source",`+
		`"content_collections_content"."content_collection_id"`), got[0])
}

func TestContentAssociationOrderColumns(t *testing.T) {
	t.Parallel()

	q := newRenderDaoQuery(t)
	stmt := &gorm.Statement{DB: q.ContentAttribute.WithContext(context.Background()).UnderlyingDB()}

	ContentAttributeOrderBy().Build(stmt)
	require.Equal(t, `"content_attributes"."source","content_attributes"."key"`, stmt.SQL.String())

	stmt.SQL.Reset()
	ContentCollectionLinkOrderBy().Build(stmt)
	require.Equal(t, `"content_collections_content"."content_collection_type",`+
		`"content_collections_content"."content_collection_source",`+
		`"content_collections_content"."content_collection_id"`, stmt.SQL.String())
}
