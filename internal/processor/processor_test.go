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
		q.Torrent.Hint.Name(),
		q.Torrent.Sources.Name(),
	}, names)
	assert.NotContains(t, names, q.Torrent.Files.Name(),
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

// Regression: reprocessing a torrent with no stored file list must not clear a
// rule-derived content type. See preservedRuleDerivedContentType — this is the
// `xxx` data-loss bug (~1.4M torrents prod-wide at ~21.5% of `xxx`).
func TestPreservedRuleDerivedContentType(t *testing.T) {
	t.Parallel()

	ruleDerivedXXX := []model.TorrentContent{{
		ContentType: model.NewNullContentType(model.ContentTypeXxx),
	}}
	sourcedMovie := []model.TorrentContent{{
		ContentType:   model.NewNullContentType(model.ContentTypeMovie),
		ContentSource: model.NewNullString("tmdb"),
		ContentID:     model.NewNullString("603"),
	}}

	for _, tc := range []struct {
		name        string
		filesStatus model.FilesStatus
		contents    []model.TorrentContent
		wantType    model.ContentType
		wantOK      bool
	}{
		// The two statuses that carry no file list by nature.
		{"no_info preserves rule-derived type", model.FilesStatusNoInfo, ruleDerivedXXX, model.ContentTypeXxx, true},
		{"over_threshold preserves rule-derived type", model.FilesStatusOverThreshold, ruleDerivedXXX, model.ContentTypeXxx, true},
		// Files present: the rules CAN evaluate, so a not-xxx verdict is a
		// legitimate reclassification and must still be allowed through.
		{"single does not preserve", model.FilesStatusSingle, ruleDerivedXXX, "", false},
		{"multi does not preserve", model.FilesStatusMulti, ruleDerivedXXX, "", false},
		// Sourced content is already handled by the reuse path in run(); this
		// fallback must not claim it.
		{"sourced content is not claimed", model.FilesStatusNoInfo, sourcedMovie, "", false},
		{"no existing contents", model.FilesStatusNoInfo, nil, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			contentType, ok := preservedRuleDerivedContentType(model.Torrent{
				FilesStatus: tc.filesStatus,
				Contents:    tc.contents,
			})
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantType, contentType)
		})
	}
}

func TestFilesStatusHasStoredFileList(t *testing.T) {
	t.Parallel()

	assert.False(t, model.FilesStatusNoInfo.HasStoredFileList())
	assert.False(t, model.FilesStatusOverThreshold.HasStoredFileList())
	assert.True(t, model.FilesStatusSingle.HasStoredFileList())
	assert.True(t, model.FilesStatusMulti.HasStoredFileList())
}
