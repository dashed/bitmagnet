package consistency

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompareFilesMatch(t *testing.T) {
	t.Parallel()

	files := []model.TorrentFile{
		{Index: 0, Path: "movie/video.mkv", Size: 1024000},
		{Index: 1, Path: "movie/subs.srt", Size: 5000},
	}

	result := CompareFiles(files, files)
	assert.True(t, result.Match)
	assert.Equal(t, 2, result.BlobFiles)
	assert.Equal(t, 2, result.RowFiles)
	assert.Empty(t, result.Mismatches)
}

func TestCompareFilesMismatchPath(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 0, Path: "movie/video.mkv", Size: 1024000},
	}
	rows := []model.TorrentFile{
		{Index: 0, Path: "movie/different.mkv", Size: 1024000},
	}

	result := CompareFiles(blob, rows)
	assert.False(t, result.Match)
	assert.Len(t, result.Mismatches, 1)
	assert.Equal(t, "path", result.Mismatches[0].Field)
	assert.Equal(t, "movie/different.mkv", result.Mismatches[0].Expected)
	assert.Equal(t, "movie/video.mkv", result.Mismatches[0].Got)
}

func TestCompareFilesMismatchSize(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 0, Path: "file.bin", Size: 100},
	}
	rows := []model.TorrentFile{
		{Index: 0, Path: "file.bin", Size: 200},
	}

	result := CompareFiles(blob, rows)
	assert.False(t, result.Match)
	assert.Len(t, result.Mismatches, 1)
	assert.Equal(t, "size", result.Mismatches[0].Field)
	assert.Equal(t, "200", result.Mismatches[0].Expected)
	assert.Equal(t, "100", result.Mismatches[0].Got)
}

func TestCompareFilesMissingInBlob(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 0, Path: "file.bin", Size: 100},
	}
	rows := []model.TorrentFile{
		{Index: 0, Path: "file.bin", Size: 100},
		{Index: 1, Path: "extra.txt", Size: 50},
	}

	result := CompareFiles(blob, rows)
	assert.False(t, result.Match)
	assert.Equal(t, 1, result.BlobFiles)
	assert.Equal(t, 2, result.RowFiles)
	assert.Len(t, result.Mismatches, 1)
	assert.Equal(t, "count", result.Mismatches[0].Field)
}

func TestCompareFilesExtraInBlob(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 0, Path: "file.bin", Size: 100},
		{Index: 1, Path: "extra.txt", Size: 50},
		{Index: 2, Path: "bonus.mp3", Size: 300},
	}
	rows := []model.TorrentFile{
		{Index: 0, Path: "file.bin", Size: 100},
	}

	result := CompareFiles(blob, rows)
	assert.False(t, result.Match)
	assert.Equal(t, 3, result.BlobFiles)
	assert.Equal(t, 1, result.RowFiles)
	assert.Equal(t, "count", result.Mismatches[0].Field)
}

func TestCompareFilesDifferentOrder(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 2, Path: "c.txt", Size: 300},
		{Index: 0, Path: "a.txt", Size: 100},
		{Index: 1, Path: "b.txt", Size: 200},
	}
	rows := []model.TorrentFile{
		{Index: 1, Path: "b.txt", Size: 200},
		{Index: 0, Path: "a.txt", Size: 100},
		{Index: 2, Path: "c.txt", Size: 300},
	}

	result := CompareFiles(blob, rows)
	assert.True(t, result.Match)
	assert.Empty(t, result.Mismatches)
}

func TestCompareFilesEmpty(t *testing.T) {
	t.Parallel()

	result := CompareFiles([]model.TorrentFile{}, []model.TorrentFile{})
	assert.True(t, result.Match)
	assert.Equal(t, 0, result.BlobFiles)
	assert.Equal(t, 0, result.RowFiles)
	assert.Empty(t, result.Mismatches)
}

func TestCompareFilesBlobEmptyRowsNot(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{}
	rows := []model.TorrentFile{
		{Index: 0, Path: "file.bin", Size: 100},
	}

	result := CompareFiles(blob, rows)
	assert.False(t, result.Match)
	assert.Equal(t, 0, result.BlobFiles)
	assert.Equal(t, 1, result.RowFiles)
	assert.Equal(t, "count", result.Mismatches[0].Field)
	assert.Equal(t, "1", result.Mismatches[0].Expected)
	assert.Equal(t, "0", result.Mismatches[0].Got)
}

func TestCompareFileIndexSetMatchDifferentOrder(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 2, Path: "c.txt", Size: 300},
		{Index: 0, Path: "a.txt", Size: 100},
		{Index: 1, Path: "b.txt", Size: 200},
	}
	rows := []model.TorrentFile{
		{Index: 1, Path: "b.txt", Size: 200},
		{Index: 0, Path: "a.txt", Size: 100},
		{Index: 2, Path: "c.txt", Size: 300},
	}

	result := CompareFileIndexSet(blob, rows)
	assert.True(t, result.Match)
	assert.Equal(t, 3, result.BlobFiles)
	assert.Equal(t, 3, result.RowFiles)
	assert.Empty(t, result.Mismatches)
}

func TestCompareFileIndexSetDetectsRowPathMismatch(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 0, Path: "a.txt", Size: 100},
	}
	rows := []model.TorrentFile{
		{Index: 0, Path: "different.txt", Size: 100},
	}

	result := CompareFileIndexSet(blob, rows)
	assert.False(t, result.Match)
	require.Len(t, result.Mismatches, 1)
	assert.Equal(t, "path", result.Mismatches[0].Field)
	assert.Equal(t, "different.txt", result.Mismatches[0].Expected)
	assert.Equal(t, "a.txt", result.Mismatches[0].Got)
}

func TestCompareFileIndexSetDetectsRowSizeMismatch(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 0, Path: "a.txt", Size: 100},
	}
	rows := []model.TorrentFile{
		{Index: 0, Path: "a.txt", Size: 999},
	}

	result := CompareFileIndexSet(blob, rows)
	assert.False(t, result.Match)
	require.Len(t, result.Mismatches, 1)
	assert.Equal(t, "size", result.Mismatches[0].Field)
	assert.Equal(t, "999", result.Mismatches[0].Expected)
	assert.Equal(t, "100", result.Mismatches[0].Got)
}

func TestCompareFileIndexSetMissingInBlob(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 0, Path: "a.bin"},
	}
	rows := []model.TorrentFile{
		{Index: 0, Path: "a.bin"},
		{Index: 1, Path: "b.bin"},
	}

	result := CompareFileIndexSet(blob, rows)
	assert.False(t, result.Match)
	assert.Len(t, result.Mismatches, 1)
	assert.Equal(t, "missing_in_blob", result.Mismatches[0].Field)
	assert.Equal(t, 1, result.Mismatches[0].FileIndex)
}

func TestCompareFileIndexSetMissingInTorrentFiles(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 0, Path: "a.bin"},
		{Index: 2, Path: "c.bin"},
	}
	rows := []model.TorrentFile{
		{Index: 0, Path: "a.bin"},
	}

	result := CompareFileIndexSet(blob, rows)
	assert.False(t, result.Match)
	assert.Len(t, result.Mismatches, 1)
	assert.Equal(t, "missing_in_torrent_files", result.Mismatches[0].Field)
	assert.Equal(t, 2, result.Mismatches[0].FileIndex)
}

func TestCompareFileIndexSetDuplicateIndexes(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 0},
		{Index: 0},
	}
	rows := []model.TorrentFile{
		{Index: 0},
	}

	result := CompareFileIndexSet(blob, rows)
	assert.False(t, result.Match)
	assert.Len(t, result.Mismatches, 1)
	assert.Equal(t, "duplicate_blob_index", result.Mismatches[0].Field)
	assert.Equal(t, "2", result.Mismatches[0].Got)
}

func TestCompareFileIndexSetAllowsLegacyDuplicatePathProjection(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 0, Path: " "},
		{Index: 1, Path: " "},
		{Index: 2, Path: "video.mkv"},
		{Index: 3, Path: " "},
	}
	rows := []model.TorrentFile{
		{Index: 0, Path: " "},
		{Index: 2, Path: "video.mkv"},
	}

	result := CompareFileIndexSet(blob, rows)
	assert.True(t, result.Match)
	assert.Equal(t, 4, result.BlobFiles)
	assert.Equal(t, 2, result.RowFiles)
	assert.Equal(t, 2, result.LegacyDuplicatePathFiles)
	assert.Empty(t, result.Mismatches)
}

func TestCompareFileIndexSetAllowsAnySurvivingDuplicatePathIndex(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 0, Path: " "},
		{Index: 1, Path: " "},
		{Index: 2, Path: "video.mkv"},
	}
	rows := []model.TorrentFile{
		{Index: 1, Path: " "},
		{Index: 2, Path: "video.mkv"},
	}

	result := CompareFileIndexSet(blob, rows)
	assert.True(t, result.Match)
	assert.Equal(t, 1, result.LegacyDuplicatePathFiles)
	assert.Empty(t, result.Mismatches)
}

func TestCompareFileIndexSetRejectsUnexplainedBlobOnlyIndex(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 0, Path: "kept.bin"},
		{Index: 1, Path: "orphan.bin"},
	}
	rows := []model.TorrentFile{
		{Index: 0, Path: "kept.bin"},
	}

	result := CompareFileIndexSet(blob, rows)
	assert.False(t, result.Match)
	require.Len(t, result.Mismatches, 1)
	assert.Equal(t, "missing_in_torrent_files", result.Mismatches[0].Field)
	assert.Equal(t, 1, result.Mismatches[0].FileIndex)
	assert.Equal(t, 0, result.LegacyDuplicatePathFiles)
}

func TestCompareFileIndexSetDuplicateRowIndex(t *testing.T) {
	t.Parallel()

	blob := []model.TorrentFile{
		{Index: 0, Path: "a.bin"},
	}
	rows := []model.TorrentFile{
		{Index: 0, Path: "a.bin"},
		{Index: 0, Path: "b.bin"},
	}

	result := CompareFileIndexSet(blob, rows)
	assert.False(t, result.Match)
	require.NotEmpty(t, result.Mismatches)
	assert.Equal(t, "duplicate_row_index", result.Mismatches[0].Field)
	assert.Equal(t, "2", result.Mismatches[0].Got)
}

func TestCompareFileIndexChunkCountsLegacyDuplicatePaths(t *testing.T) {
	t.Parallel()

	var infoHash protocol.ID
	infoHash[0] = 1

	files := []model.TorrentFile{
		{InfoHash: infoHash, Index: 0, Path: " ", Size: 10},
		{InfoHash: infoHash, Index: 1, Path: " ", Size: 20},
		{InfoHash: infoHash, Index: 2, Path: "video.mkv", Size: 30},
	}
	blob, err := blobmigration.SerializeFiles(files)
	require.NoError(t, err)

	var s Summary
	err = compareFileIndexChunk(
		&s,
		[]blobRow{{InfoHash: infoHash, FilesData: blob}},
		map[protocol.ID][]model.TorrentFile{
			infoHash: {
				{InfoHash: infoHash, Index: 0, Path: " ", Size: 10},
				{InfoHash: infoHash, Index: 2, Path: "video.mkv", Size: 30},
			},
		},
		0,
		verifyRange{},
		protocol.ID{},
		false,
		infoHash,
	)
	require.NoError(t, err)

	assert.Equal(t, 1, s.TotalChecked)
	assert.Equal(t, 1, s.Matches)
	assert.Equal(t, 0, s.Mismatches)
	assert.Equal(t, 1, s.LegacyDuplicatePathTorrents)
	assert.Equal(t, 1, s.LegacyDuplicatePathFiles)
}

func TestCompareFileIndexChunkDetectsRowOnlyTorrentInChunk(t *testing.T) {
	t.Parallel()

	var blobHash protocol.ID
	blobHash[0] = 1

	var rowOnlyHash protocol.ID
	rowOnlyHash[0] = 2

	files := []model.TorrentFile{{InfoHash: blobHash, Index: 0, Path: "video.mkv", Size: 10}}
	blob, err := blobmigration.SerializeFiles(files)
	require.NoError(t, err)

	var s Summary
	err = compareFileIndexChunk(
		&s,
		[]blobRow{{InfoHash: blobHash, FilesData: blob}},
		map[protocol.ID][]model.TorrentFile{
			blobHash: {
				{InfoHash: blobHash, Index: 0, Path: "video.mkv", Size: 10},
			},
			rowOnlyHash: {
				{InfoHash: rowOnlyHash, Index: 0, Path: "orphan.mkv", Size: 20},
			},
		},
		0,
		verifyRange{},
		protocol.ID{},
		false,
		rowOnlyHash,
	)
	require.NoError(t, err)

	assert.Equal(t, 2, s.TotalChecked)
	assert.Equal(t, 1, s.Matches)
	assert.Equal(t, 1, s.Mismatches)
	require.Len(t, s.MismatchDetails, 1)
	assert.Equal(t, rowOnlyHash, s.MismatchDetails[0].InfoHash)
	require.NotEmpty(t, s.MismatchDetails[0].Mismatches)
	assert.Equal(t, "missing_in_blob", s.MismatchDetails[0].Mismatches[0].Field)
}

func TestAddRowOnlyIndexMismatchesHandlesTailRows(t *testing.T) {
	t.Parallel()

	var tailHash protocol.ID
	tailHash[0] = 255

	var s Summary

	addRowOnlyIndexMismatches(&s, map[protocol.ID][]model.TorrentFile{
		tailHash: {
			{InfoHash: tailHash, Index: 0, Path: "tail.mkv", Size: 30},
		},
	})

	assert.Equal(t, 1, s.TotalChecked)
	assert.Equal(t, 1, s.Mismatches)
	require.Len(t, s.MismatchDetails, 1)
	assert.Equal(t, tailHash, s.MismatchDetails[0].InfoHash)
	require.NotEmpty(t, s.MismatchDetails[0].Mismatches)
	assert.Equal(t, "missing_in_blob", s.MismatchDetails[0].Mismatches[0].Field)
}
