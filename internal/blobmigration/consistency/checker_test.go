package consistency

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/stretchr/testify/assert"
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
