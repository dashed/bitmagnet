package queue

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func renderContentTypesScope(t *testing.T, filters ...model.NullContentType) string {
	t.Helper()
	mockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = mockDB.Close() })
	database, err := gorm.Open(postgres.New(postgres.Config{
		Conn:                 mockDB,
		PreferSimpleProtocol: true,
	}), &gorm.Config{DryRun: true, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	d := dao.Use(database)
	query := d.Torrent.Scopes(contentTypesScope(d, filters)).UnderlyingDB()
	return query.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return tx.Find(&[]model.Torrent{})
	})
}

func TestContentTypesScopeKeepsNullBranchCorrelated(t *testing.T) {
	sql := renderContentTypesScope(t,
		model.NewNullContentType(model.ContentTypeMovie),
		model.NewNullContentType(nil),
	)
	require.Contains(t, sql, `"torrent_contents"."info_hash" = "torrents"."info_hash"`)
	require.Contains(t, sql, `("torrent_contents"."content_type" = 'movie' OR "torrent_contents"."content_type" IS NULL)`)
	require.NotContains(t, sql, `"torrents"."info_hash") OR`)
}

func TestContentTypesScopeSupportsNullOnly(t *testing.T) {
	sql := renderContentTypesScope(t, model.NewNullContentType(nil))
	require.Contains(t, sql, `"torrent_contents"."info_hash" = "torrents"."info_hash"`)
	require.Contains(t, sql, `"torrent_contents"."content_type" IS NULL`)
	require.NotContains(t, sql, " IN ()")
}
