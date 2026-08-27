package processor

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type fakeSearchIndexer struct {
	indexed []*pb.TorrentDocument
	deleted [][]byte
}

func (f *fakeSearchIndexer) IndexDocument(
	_ context.Context,
	doc *pb.TorrentDocument,
) (*pb.IndexDocumentResponse, error) {
	f.indexed = append(f.indexed, doc)

	return &pb.IndexDocumentResponse{Ok: true}, nil
}

func (f *fakeSearchIndexer) DeleteDocument(
	_ context.Context,
	infoHash []byte,
) (*pb.DeleteDocumentResponse, error) {
	f.deleted = append(f.deleted, infoHash)

	return &pb.DeleteDocumentResponse{Ok: true}, nil
}

func testInfoHash(b byte) protocol.ID {
	var id protocol.ID
	for i := range id {
		id[i] = b
	}

	return id
}

func testTorrentContent(b byte) model.TorrentContent {
	return model.TorrentContent{
		InfoHash: testInfoHash(b),
		Torrent:  model.Torrent{InfoHash: testInfoHash(b), Name: "torrent"},
	}
}

func TestTorrentContentUpdateAllKeepsPublishedAtInsertOnly(t *testing.T) {
	t.Parallel()

	mockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = mockDB.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn:                 mockDB,
		PreferSimpleProtocol: true,
	}), &gorm.Config{
		DryRun:                 true,
		SkipDefaultTransaction: true,
		Logger:                 logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	// The non-default sentinel forces GORM to project published_at on insert.
	// Its generated-model default tag must still exclude it from UpdateAll.
	content := testTorrentContent(0xAA)
	content.PublishedAt = time.Date(2017, time.May, 4, 3, 2, 1, 0, time.UTC)
	result := db.Clauses(clause.OnConflict{UpdateAll: true}).Create(&content)
	require.NoError(t, result.Error)
	statement := result.Statement.SQL.String()
	insertClause, updateClause, found := strings.Cut(statement, "DO UPDATE SET")
	require.True(t, found, "rendered SQL contains an UpdateAll conflict clause: %s", statement)
	updateAssignments, _, _ := strings.Cut(updateClause, " RETURNING")
	assert.Contains(t, insertClause, `"published_at"`)
	assert.NotContains(t, updateAssignments, `"published_at"`)
}

func TestIndexBatchIndexesEachContentAndDeletesEachInfoHash(t *testing.T) {
	t.Parallel()

	f := &fakeSearchIndexer{}
	c := processor{searchIndexer: f}

	c.indexBatch(
		context.Background(),
		[]model.TorrentContent{testTorrentContent(0xAA), testTorrentContent(0xBB)},
		[]protocol.ID{testInfoHash(0xCC)},
	)

	require.Len(t, f.indexed, 2, "one IndexDocument per torrent_content")
	assert.Len(t, f.deleted, 1, "one DeleteDocument per deleted info hash")
}

func TestIndexToSearchSidecarDisabledIsNoOp(t *testing.T) {
	t.Parallel()

	// A nil searchIndexer is the "search disabled" state: the dual-write must be
	// a no-op (it returns before spawning any work — nothing to index/panic).
	c := processor{searchIndexer: nil}

	assert.NotPanics(t, func() {
		c.indexToSearchSidecar(context.Background(), persistPayload{
			torrentContents: []model.TorrentContent{testTorrentContent(0xAA)},
		})
	})
}
