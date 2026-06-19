package processor

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func newProcessorTestDao(t *testing.T) *dao.Query {
	t.Helper()

	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectClose()
	t.Cleanup(func() { require.NoError(t, mockDB.Close()) })

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn:                 mockDB,
		PreferSimpleProtocol: true,
	}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)

	return dao.Use(db)
}

func TestProcessorTorrentPreloadsSkipLegacyFiles(t *testing.T) {
	t.Parallel()

	q := newProcessorTestDao(t)
	preloads := processorTorrentPreloads(q)
	names := make([]string, 0, len(preloads))
	for _, preload := range preloads {
		names = append(names, preload.Name())
	}

	assert.ElementsMatch(t, []string{
		q.Torrent.Hint.RelationField.Name(),
		q.Torrent.Sources.RelationField.Name(),
	}, names)
	assert.NotContains(t, names, q.Torrent.Files.RelationField.Name(),
		"processor must not preload Torrent.Files because that association reads torrent_files")
}

func TestNewTorrentContentSearchDocumentUsesBlobHydratedFiles(t *testing.T) {
	t.Parallel()

	files := []model.TorrentFile{
		{Index: 1, Path: "b/subtitle.srt", Size: 100},
		{Index: 0, Path: "a/episode.mkv", Size: 1_000},
	}
	blob, err := blobmigration.SerializeFiles(files)
	require.NoError(t, err)

	torrent := model.Torrent{
		InfoHash:    testInfoHash(0xDD),
		Name:        "Blob Hydrated Torrent",
		FilesStatus: model.FilesStatusMulti,
		FilesCount:  model.NewNullUint(2),
		FilesData:   blob,
		Files: []model.TorrentFile{
			{
				Index: 0,
				Path:  "legacy/from-torrent-files.avi",
				Extension: model.NullString{
					String: "avi",
					Valid:  true,
				},
				Size: 999,
			},
		},
	}
	require.NoError(t, torrent.AfterFind(nil))

	tc := newTorrentContent(torrent, classification.Result{
		ContentAttributes: classification.ContentAttributes{
			ContentType: model.NewNullContentType(model.ContentTypeMovie),
		},
	})
	doc := tantivy.BuildDocument(tc)

	assert.Equal(t, []string{"a/episode.mkv", "b/subtitle.srt"}, doc.GetFilePaths())
	assert.Equal(t, []string{"mkv", "srt"}, doc.GetFileExtensions())
	assert.NotContains(t, doc.GetFilePaths(), "legacy/from-torrent-files.avi")
	assert.Equal(t, uint32(2), doc.GetFilesCount())
}

func TestIndexBatchIndexesBlobHydratedFiles(t *testing.T) {
	t.Parallel()

	files := []model.TorrentFile{
		{Index: 0, Path: "movie/feature.mkv", Size: 2_000},
		{Index: 1, Path: "movie/subtitle.srt", Size: 50},
	}
	blob, err := blobmigration.SerializeFiles(files)
	require.NoError(t, err)

	torrent := model.Torrent{
		InfoHash:    testInfoHash(0xDE),
		Name:        "Blob Hydrated Index Torrent",
		FilesStatus: model.FilesStatusMulti,
		FilesCount:  model.NewNullUint(2),
		FilesData:   blob,
	}
	require.NoError(t, torrent.AfterFind(nil))

	f := &fakeSearchIndexer{}
	c := processor{searchIndexer: f}
	c.indexBatch(context.Background(), []model.TorrentContent{
		newTorrentContent(torrent, classification.Result{
			ContentAttributes: classification.ContentAttributes{
				ContentType: model.NewNullContentType(model.ContentTypeMovie),
			},
		}),
	}, nil)

	require.Len(t, f.indexed, 1)
	assert.Equal(t, []string{"movie/feature.mkv", "movie/subtitle.srt"}, f.indexed[0].GetFilePaths())
	assert.Equal(t, []string{"mkv", "srt"}, f.indexed[0].GetFileExtensions())
}
