//go:build integration

package torznab_test

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/parity"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/torznab"
	"github.com/bitmagnet-io/bitmagnet/internal/torznab/adapter"
	"github.com/bitmagnet-io/bitmagnet/internal/torznab/httpserver"
	"github.com/gin-gonic/gin"
	goose "github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	migrationssql "github.com/bitmagnet-io/bitmagnet/migrations"
)

// TestMain pins the process timezone to UTC so RSSDate rendering
// (Torznab pubDate/publishdate carry a numeric offset) is machine-independent.
// The pg driver returns timestamptz in time.Local; without this the goldens would
// bake in the generating host's offset. UTC matches the production cluster and the
// Rust parity target — the golden's dates are the canonical UTC rendering.
func TestMain(m *testing.M) {
	time.Local = time.UTC
	os.Exit(m.Run())
}

func setupV2TestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set, skipping integration test")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	sqlDB, err := db.DB()
	require.NoError(t, err)

	cleanupV2Schema(t, sqlDB)

	goose.SetBaseFS(migrationssql.FS)
	require.NoError(t, goose.SetDialect("postgres"))
	goose.SetLogger(goose.NopLogger())
	require.NoError(t, goose.UpContext(context.Background(), sqlDB, "."))

	t.Cleanup(func() {
		cleanupV2Schema(t, sqlDB)
	})

	return db
}

func cleanupV2Schema(t *testing.T, sqlDB *sql.DB) {
	t.Helper()

	_, err := sqlDB.Exec("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
	require.NoError(t, err)
}

func TestTorznabSearchGoldens(t *testing.T) {
	db := setupV2TestDB(t)
	fixtureInfohashes := seedTorznabFixtures(t, db)

	corpusPath := filepath.Join(repoRootTorznab(t), torznabParityDir, "corpus.jsonl")
	corpus, err := parity.LoadTorznabCorpus(corpusPath)
	require.NoError(t, err)

	searchClient, err := search.New(search.Params{
		Query: lazy.New(func() (*dao.Query, error) {
			return dao.Use(db), nil
		}),
	}).Search.Get()
	require.NoError(t, err)

	client := adapter.New(searchClient)
	engine := gin.New()
	err = httpserver.New(
		lazy.New[torznab.Client](func() (torznab.Client, error) {
			return client, nil
		}),
		torznab.Config{}.MergeDefaults(),
	).Apply(engine)
	require.NoError(t, err)

	for _, corpusQuery := range corpus {
		if corpusQuery.Kind == "caps" {
			continue
		}

		t.Run(corpusQuery.ID, func(t *testing.T) {
			request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, corpusQuery.Path, nil)
			require.NoError(t, err)

			response := httptest.NewRecorder()
			engine.ServeHTTP(response, request)
			require.Equal(t, http.StatusOK, response.Code, "query %q returned an unexpected status", corpusQuery.ID)

			raw := response.Body.Bytes()
			if corpusQuery.Kind == "search" && corpusQuery.HasExpect {
				assertTorznabExpectIDs(t, corpusQuery, fixtureInfohashes, raw)
			}

			normalized, err := parity.NormalizeTorznabXML(raw)
			require.NoError(t, err, "normalize query %q response", corpusQuery.ID)
			if corpusQuery.ID == "search-all" {
				require.Contains(t, string(normalized), `<torznab:attr name="files" value="3"/>`,
					"multi-file fixture did not hydrate from files_data")
			}

			goldenPath := filepath.Join(
				repoRootTorznab(t),
				torznabParityDir,
				corpusQuery.GoldenName(),
			)
			writeOrAssertTorznabGolden(t, goldenPath, normalized)
		})
	}
}

func seedTorznabFixtures(t *testing.T, db *gorm.DB) map[string]string {
	t.Helper()

	fixturesPath := filepath.Join(repoRootTorznab(t), torznabParityDir, "fixtures.jsonl")
	fixtures, err := parity.LoadTorznabFixtures(fixturesPath)
	require.NoError(t, err)

	q := dao.Use(db)
	ctx := context.Background()
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	infohashes := make(map[string]string, len(fixtures))

	for _, fixture := range fixtures {
		infohash := torznabFixtureInfoHash(fixture.ID)
		infohashes[fixture.ID] = infohash.String()

		filesStatus := model.FilesStatusSingle
		if len(fixture.Files) > 1 {
			filesStatus = model.FilesStatusMulti
		}
		torrent := &model.Torrent{
			InfoHash:    infohash,
			Name:        fixture.Title,
			Size:        uint(fixture.Size),
			Private:     false,
			FilesStatus: filesStatus,
			FileExts:    []string{},
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		err := q.Torrent.WithContext(ctx).Omit(
			q.Torrent.Extension,
			q.Torrent.Hint.Field(),
			q.Torrent.Contents.Field(),
			q.Torrent.Sources.Field(),
			q.Torrent.Files.Field(),
			q.Torrent.Pieces.Field(),
			q.Torrent.Tags.Field(),
			q.Torrent.FilesData,
			q.Torrent.FileExts,
		).Create(torrent)
		require.NoError(t, err, "create torrent fixture %q", fixture.ID)

		files := make([]model.TorrentFile, 0, len(fixture.Files))
		for index, path := range fixture.Files {
			files = append(files, model.TorrentFile{
				InfoHash: infohash,
				Index:    uint(index),
				Path:     path,
				Size:     uint(fixture.Size),
			})
		}
		filesBlob, err := blobmigration.SerializeFiles(files)
		require.NoError(t, err, "serialize files for fixture %q", fixture.ID)
		extensionsJSON, err := json.Marshal(blobmigration.ExtractUniqueExtensions(files))
		require.NoError(t, err, "marshal file extensions for fixture %q", fixture.ID)
		require.NoError(t, db.Table("torrents").
			Where("info_hash = ?", infohash).
			Updates(map[string]any{
				"files_data":      filesBlob,
				"file_extensions": gorm.Expr("?::jsonb", string(extensionsJSON)),
			}).Error, "update files blob for fixture %q", fixture.ID)

		if fixture.Seeders != nil || fixture.Leechers != nil {
			source := &model.TorrentsTorrentSource{
				Source:      "dht",
				InfoHash:    infohash,
				PublishedAt: sql.NullTime{Time: now, Valid: true},
				SeenCount:   1,
				CreatedAt:   now,
				UpdatedAt:   now,
			}
			if fixture.Seeders != nil {
				source.Seeders = model.NewNullUint(*fixture.Seeders)
			}
			if fixture.Leechers != nil {
				source.Leechers = model.NewNullUint(*fixture.Leechers)
			}
			require.NoError(t, q.TorrentsTorrentSource.WithContext(ctx).Create(source),
				"create torrent source for fixture %q", fixture.ID)
		}

		var content model.Content
		hasContent := fixture.TMDB != ""
		if hasContent {
			content = model.Content{
				Type:        model.ContentType(fixture.ContentType),
				Source:      "tmdb",
				ID:          fixture.TMDB,
				Title:       fixture.ContentTitle,
				ReleaseYear: model.Year(fixture.Year),
				CreatedAt:   now,
				UpdatedAt:   now,
			}
			content.UpdateTsv()
			require.NoError(t, q.Content.WithContext(ctx).Create(&content),
				"create content for fixture %q", fixture.ID)

			if fixture.IMDB != "" {
				require.NoError(t, q.ContentAttribute.WithContext(ctx).Create(&model.ContentAttribute{
					ContentType:   model.ContentType(fixture.ContentType),
					ContentSource: "tmdb",
					ContentID:     fixture.TMDB,
					Source:        "imdb",
					Key:           "id",
					Value:         fixture.IMDB,
					CreatedAt:     now,
					UpdatedAt:     now,
				}), "create IMDb attribute for fixture %q", fixture.ID)
			}
		}

		torrentContent := model.TorrentContent{
			InfoHash:    infohash,
			Size:        uint(fixture.Size),
			PublishedAt: base.Add(time.Duration(fixture.Pub) * 24 * time.Hour),
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if fixture.ContentType != "" {
			torrentContent.ContentType = model.NewNullContentType(model.ContentType(fixture.ContentType))
		}
		if hasContent {
			torrentContent.ContentSource = model.NewNullString("tmdb")
			torrentContent.ContentID = model.NewNullString(fixture.TMDB)
		}
		setTorznabFixtureMetadata(&torrentContent, fixture)

		episodes := model.Episodes{}
		for season, episodeList := range fixture.Episodes {
			if len(episodeList) == 0 {
				episodes = episodes.AddSeason(season)
				continue
			}
			for _, episode := range episodeList {
				episodes = episodes.AddEpisode(season, episode)
			}
		}
		torrentContent.Episodes = episodes

		torrent.Files = files
		torrentContent.Torrent = *torrent
		if hasContent {
			torrentContent.Content = content
		}
		torrentContent.UpdateTsv()
		torrentContent.Torrent = model.Torrent{}
		torrentContent.Content = model.Content{}
		require.NoError(t, q.TorrentContent.WithContext(ctx).Create(&torrentContent),
			"create torrent content for fixture %q", fixture.ID)
	}

	return infohashes
}

func setTorznabFixtureMetadata(torrentContent *model.TorrentContent, fixture parity.TorznabFixture) {
	switch fixture.VideoResolution {
	case "480p":
		torrentContent.VideoResolution = model.NewNullVideoResolution(model.VideoResolutionV480p)
	case "720p":
		torrentContent.VideoResolution = model.NewNullVideoResolution(model.VideoResolutionV720p)
	case "1080p":
		torrentContent.VideoResolution = model.NewNullVideoResolution(model.VideoResolutionV1080p)
	case "1440p":
		torrentContent.VideoResolution = model.NewNullVideoResolution(model.VideoResolutionV1440p)
	case "2160p":
		torrentContent.VideoResolution = model.NewNullVideoResolution(model.VideoResolutionV2160p)
	}

	switch fixture.Video3D {
	case "3d":
		torrentContent.Video3D = model.NewNullVideo3D(model.Video3DV3D)
	case "3d-sbs":
		torrentContent.Video3D = model.NewNullVideo3D(model.Video3DV3DSBS)
	case "3d-ou":
		torrentContent.Video3D = model.NewNullVideo3D(model.Video3DV3DOU)
	}

	switch fixture.VideoCodec {
	case "x264":
		torrentContent.VideoCodec = model.NewNullVideoCodec(model.VideoCodecX264)
	case "x265":
		torrentContent.VideoCodec = model.NewNullVideoCodec(model.VideoCodecX265)
	case "H264":
		torrentContent.VideoCodec = model.NewNullVideoCodec(model.VideoCodecH264)
	case "XviD":
		torrentContent.VideoCodec = model.NewNullVideoCodec(model.VideoCodecXviD)
	}

	if fixture.ReleaseGroup != "" {
		torrentContent.ReleaseGroup = model.NewNullString(fixture.ReleaseGroup)
	}
	if fixture.Seeders != nil {
		torrentContent.Seeders = model.NewNullUint(*fixture.Seeders)
	}
	if fixture.Leechers != nil {
		torrentContent.Leechers = model.NewNullUint(*fixture.Leechers)
	}
}

func torznabFixtureInfoHash(id string) protocol.ID {
	sum := sha1.Sum([]byte(id))
	return protocol.MustNewIDFromByteSlice(sum[:])
}

func assertTorznabExpectIDs(
	t *testing.T,
	query parity.CorpusQuery,
	fixtureInfohashes map[string]string,
	rawXML []byte,
) {
	t.Helper()

	want := make([]string, 0, len(query.ExpectIDs))
	for _, fixtureID := range query.ExpectIDs {
		infohash, ok := fixtureInfohashes[fixtureID]
		if !ok {
			t.Fatalf("query %q expectIds references unknown fixture %q", query.ID, fixtureID)
		}
		want = append(want, infohash)
	}

	got, err := parity.ExtractInfohashes(rawXML)
	require.NoError(t, err, "extract query %q response infohashes", query.ID)
	if slices.Equal(want, got) {
		return
	}

	diff := parity.DiffInfohashLists(query.ID, want, got)
	t.Fatalf(
		"query %q expectIds oracle mismatch\nexpectIds: %v\nwant infohashes: %v\n got infohashes: %v\nmissing from response: %v\nunexpected in response: %v\norder match: %t",
		query.ID,
		query.ExpectIDs,
		want,
		got,
		diff.GoOnly,
		diff.RustOnly,
		diff.OrderMatch,
	)
}
