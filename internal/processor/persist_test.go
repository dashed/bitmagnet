package processor

import (
	"context"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
