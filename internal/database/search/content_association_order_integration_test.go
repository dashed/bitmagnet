//go:build integration

package search

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	migrationssql "github.com/bitmagnet-io/bitmagnet/migrations"
	goose "github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Content.UpdateTsv appends genre collection names and `id` attribute values to
// the tsvector in slice order, so a content's association order decides the
// lexeme positions persisted into torrent_contents.tsv. These tests seed
// associations in an order that disagrees with the required one, including
// values whose collation order differs from their byte order, and assert that
// local-search hydration returns them in the database's ORDER BY order under
// every forced plan.

func openAssociationOrderDB(t *testing.T, dsn string) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})

	return db
}

func setupAssociationOrderDB(t *testing.T) (*gorm.DB, string) {
	t.Helper()

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set, skipping content association order integration test")
	}

	db := openAssociationOrderDB(t, dsn)

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

type seededAttribute struct{ source, key, value string }

type seededCollection struct{ id, name string }

// Physical insertion orders below are deliberately not the required orders.
// "TVmaze" sorts after "tmdb" under en_US but before it bytewise, and "éclair"
// sorts before "imdb" under en_US but after every ASCII source bytewise; "ID"
// and "id" likewise swap. Genre ids "10" and "9" sort the same way both ways
// but not numerically, and "B7"/"a7" and "é7"/"f7" swap between the two.
var (
	orderTestAttributes = []seededAttribute{
		{"TVmaze", "id", "tvm-4"},
		{"tmdb", "poster_path", "/poster.jpg"},
		{"tvdb", "id", "81189"},
		{"tmdb", "id", "tmdb-id"},
		{"tmdb", "ID", "tmdb-ID"},
		{"imdb", "id", "tt0903747"},
		{"éclair", "id", "ecl-2"},
		{"tmdb", "backdrop_path", "/backdrop.jpg"},
	}
	orderTestGenres = []seededCollection{
		{"f7", "Foxtrot"},
		{"9", "Nine"},
		{"é7", "Echo"},
		{"B7", "Bravo"},
		{"10", "Ten"},
		{"a7", "Alpha"},
	}
)

func seedAssociationOrderContent(t *testing.T, db *gorm.DB, ids []string) {
	t.Helper()

	for _, source := range []string{"TVmaze", "éclair"} {
		require.NoError(t, db.Exec(`INSERT INTO metadata_sources (key, name, created_at, updated_at)
			VALUES (?, ?, now(), now())`, source, source).Error)
	}

	for _, genre := range orderTestGenres {
		require.NoError(t, db.Exec(`INSERT INTO content_collections (type, source, id, name, created_at, updated_at)
			VALUES ('genre', 'tmdb', ?, ?, now(), now())`, genre.id, genre.name).Error)
	}
	// A franchise collection sorts before every genre by type.
	require.NoError(t, db.Exec(`INSERT INTO content_collections (type, source, id, name, created_at, updated_at)
		VALUES ('franchise', 'tmdb', '77', 'Saga', now(), now())`).Error)

	for _, id := range ids {
		require.NoError(t, db.Exec(`INSERT INTO content (type, source, id, title, tsv, created_at, updated_at)
			VALUES ('movie', 'tmdb', ?, ?, to_tsvector('simple', ?), now(), now())`,
			id, "Ordered "+id, "ordered "+id).Error)

		for _, attribute := range orderTestAttributes {
			require.NoError(t, db.Exec(`INSERT INTO content_attributes
				(content_type, content_source, content_id, source, key, value, created_at, updated_at)
				VALUES ('movie', 'tmdb', ?, ?, ?, ?, now(), now())`,
				id, attribute.source, attribute.key, attribute.value+"-"+id).Error)
		}

		links := append([]seededCollection{{"77", ""}}, orderTestGenres...)
		slices.Reverse(links)

		for _, link := range links {
			collectionType := "genre"
			if link.id == "77" {
				collectionType = "franchise"
			}

			require.NoError(t, db.Exec(`INSERT INTO content_collections_content
				(content_type, content_source, content_id,
				 content_collection_type, content_collection_source, content_collection_id)
				VALUES ('movie', 'tmdb', ?, ?, 'tmdb', ?)`, id, collectionType, link.id).Error)
		}
	}

	require.NoError(t, db.Exec("ANALYZE").Error)
}

// associationOrderPlans forces materially different plans for the association
// queries, applied at database scope and picked up by a fresh connection pool.
func associationOrderPlans() []map[string]string {
	return []map[string]string{
		nil,
		{"enable_seqscan": "off"},
		{"enable_indexscan": "off", "enable_bitmapscan": "off", "enable_indexonlyscan": "off"},
		{"enable_sort": "off"},
	}
}

func forEachAssociationOrderPlan(t *testing.T, db *gorm.DB, dsn string, fn func(t *testing.T, s search)) {
	t.Helper()

	var dbName string
	require.NoError(t, db.Raw("SELECT current_database()").Scan(&dbName).Error)

	for i, settings := range associationOrderPlans() {
		t.Run(fmt.Sprintf("plan%d", i), func(t *testing.T) {
			for setting, value := range settings {
				require.NoError(t, db.Exec(fmt.Sprintf("ALTER DATABASE %q SET %s = %s", dbName, setting, value)).Error)
			}

			t.Cleanup(func() {
				for setting := range settings {
					require.NoError(t, db.Exec(fmt.Sprintf("ALTER DATABASE %q RESET %s", dbName, setting)).Error)
				}
			})

			fn(t, search{q: dao.Use(openAssociationOrderDB(t, dsn))})
		})
	}
}

// wantedAttributes is the database's own ordering of a content's attributes,
// which is what "ordered by (source, key) under the DB collation" means.
func wantedAttributes(t *testing.T, db *gorm.DB, id string) []string {
	t.Helper()

	var keys []string
	require.NoError(t, db.Raw(`SELECT source || '/' || key FROM content_attributes
		WHERE content_type = 'movie' AND content_source = 'tmdb' AND content_id = ?
		ORDER BY source, key`, id).Scan(&keys).Error)

	return keys
}

func wantedCollections(t *testing.T, db *gorm.DB, id string) []string {
	t.Helper()

	var keys []string
	require.NoError(t, db.Raw(`SELECT content_collection_type || '/' || content_collection_id
		FROM content_collections_content
		WHERE content_type = 'movie' AND content_source = 'tmdb' AND content_id = ?
		ORDER BY content_collection_type, content_collection_source, content_collection_id`, id).Scan(&keys).Error)

	return keys
}

func attributeKeys(content model.Content) []string {
	keys := make([]string, 0, len(content.Attributes))
	for _, attribute := range content.Attributes {
		keys = append(keys, attribute.Source+"/"+attribute.Key)
	}

	return keys
}

func collectionKeys(content model.Content) []string {
	keys := make([]string, 0, len(content.Collections))
	for _, collection := range content.Collections {
		keys = append(keys, collection.Type+"/"+collection.ID)
	}

	return keys
}

// The premise: the seeded values really do order differently by collation than
// by bytes, so the assertions below distinguish the two.
func TestContentAssociationOrderSeedExercisesCollation(t *testing.T) {
	db, _ := setupAssociationOrderDB(t)

	var collation string
	require.NoError(t, db.Raw("SELECT datcollate FROM pg_database WHERE datname = current_database()").
		Scan(&collation).Error)

	if collation == "C" || collation == "POSIX" || strings.HasPrefix(collation, "C.") {
		t.Skipf("database collation %q is bytewise; the collation cases cannot be told apart", collation)
	}

	seedAssociationOrderContent(t, db, []string{"1"})

	attributes := wantedAttributes(t, db, "1")
	require.False(t, slices.IsSorted(attributes), "attribute seed should not be byte-ordered under %s", collation)

	collections := wantedCollections(t, db, "1")
	require.False(t, slices.IsSorted(collections), "collection seed should not be byte-ordered under %s", collation)
}

// Local search hydrates every candidate's attributes and collections; both have to
// come back in the database's order, per content, whatever the plan.
func TestContentHydrationOrdersAssociations(t *testing.T) {
	db, dsn := setupAssociationOrderDB(t)

	ids := []string{"10", "9", "100"}
	seedAssociationOrderContent(t, db, ids)

	wantTsv := map[string]string{}

	forEachAssociationOrderPlan(t, db, dsn, func(t *testing.T, s search) {
		t.Helper()

		for run := range 3 {
			result, err := s.Content(
				context.Background(),
				query.Where(ContentTypeCriteria(model.ContentTypeMovie)),
				ContentOrderByIdentity(),
				ContentDefaultPreload(),
				ContentDefaultHydrate(),
			)
			require.NoError(t, err)
			require.Len(t, result.Items, len(ids))

			for _, item := range result.Items {
				require.Equal(t, wantedAttributes(t, db, item.ID), attributeKeys(item.Content),
					"run %d: content %s attributes", run, item.ID)
				require.Equal(t, wantedCollections(t, db, item.ID), collectionKeys(item.Content),
					"run %d: content %s collections", run, item.ID)

				content := item.Content
				content.UpdateTsv()

				tsv := content.Tsv.String()
				if want, ok := wantTsv[item.ID]; ok {
					require.Equal(t, want, tsv, "run %d: content %s tsvector", run, item.ID)
				} else {
					wantTsv[item.ID] = tsv
				}
			}
		}
	})
}

// The torrent-content hydration that feeds the processor's content reuse goes
// through the same preload and hydrator.
func TestTorrentContentContentHydrationOrdersAssociations(t *testing.T) {
	db, _ := setupAssociationOrderDB(t)
	seedAssociationOrderContent(t, db, []string{"10", "9"})

	refs := []model.ContentRef{
		{Type: model.ContentTypeMovie, Source: "tmdb", ID: "10"},
		{Type: model.ContentTypeMovie, Source: "tmdb", ID: "9"},
	}

	contents, err := torrentContentContentHydrator{}.GetSubs(
		context.Background(), renderDaoContext{q: dao.Use(db)}, refs,
	)
	require.NoError(t, err)
	require.Len(t, contents, len(refs))

	for _, content := range contents {
		require.Equal(t, wantedAttributes(t, db, content.ID), attributeKeys(content))
		require.Equal(t, wantedCollections(t, db, content.ID), collectionKeys(content))
	}
}
