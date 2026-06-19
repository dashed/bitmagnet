package blobmigration

import (
	"fmt"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestSerializeDeserializeRoundTrip(t *testing.T) {
	t.Parallel()

	files := []model.TorrentFile{
		{
			Index:     0,
			Path:      "movie/video.mkv",
			Extension: model.NewNullString("mkv"),
			Size:      1024000,
		},
		{
			Index:     1,
			Path:      "movie/subs.srt",
			Extension: model.NewNullString("srt"),
			Size:      5000,
		},
		{
			Index:     2,
			Path:      "movie/readme.txt",
			Extension: model.NewNullString("txt"),
			Size:      200,
		},
	}

	data, err := SerializeFiles(files)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	result, err := DeserializeFiles(data)
	require.NoError(t, err)
	require.Len(t, result, len(files))

	for i, f := range files {
		assert.Equal(t, f.Index, result[i].Index)
		assert.Equal(t, f.Path, result[i].Path)
		assert.Equal(t, f.Extension, result[i].Extension)
		assert.Equal(t, f.Size, result[i].Size)
	}
}

// TestSerializeFiles_DerivesExtensionFromPath_G1 proves the G1 fix: SerializeFiles
// canonicalizes the stored `e` from the PATH, ignoring the caller's Extension — for
// the empty-e crawl case, a stale/wrong e, an extension-less path (empty e), and a
// multi-dot path (final token only).
func TestSerializeFiles_DerivesExtensionFromPath_G1(t *testing.T) {
	t.Parallel()

	files := []model.TorrentFile{
		{Index: 0, Path: "Season 1/Episode 1.mkv", Extension: model.NullString{}}, // empty e (crawl)
		{Index: 1, Path: "clip.mp4", Extension: model.NewNullString("avi")},       // stale/wrong e
		{Index: 2, Path: "README", Extension: model.NewNullString("bogus")},       // no real extension
		{Index: 3, Path: "a/b/c.tar.gz", Extension: model.NullString{}},           // multi-dot -> gz
	}

	data, err := SerializeFiles(files)
	require.NoError(t, err)

	got, err := DeserializeFiles(data)
	require.NoError(t, err)
	require.Len(t, got, 4)

	assert.Equal(t, "mkv", got[0].Extension.String)
	assert.Equal(t, "mp4", got[1].Extension.String)
	assert.Empty(t, got[2].Extension.String)
	assert.False(t, got[2].Extension.Valid)
	assert.Equal(t, "gz", got[3].Extension.String)

	for i := range got {
		assert.Equal(t,
			model.FileExtensionFromPath(got[i].Path).String,
			got[i].Extension.String,
			"file %d: stored e must equal path-derived extension", i,
		)
	}
}

func TestSerializeDeserializeEmpty(t *testing.T) {
	t.Parallel()

	data, err := SerializeFiles([]model.TorrentFile{})
	require.NoError(t, err)
	require.NotEmpty(t, data)

	result, err := DeserializeFiles(data)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestSerializeDeserializeSingleFile(t *testing.T) {
	t.Parallel()

	files := []model.TorrentFile{
		{
			Index:     0,
			Path:      "ubuntu-22.04.iso",
			Extension: model.NewNullString("iso"),
			Size:      3_700_000_000,
		},
	}

	data, err := SerializeFiles(files)
	require.NoError(t, err)

	result, err := DeserializeFiles(data)
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, files[0].Path, result[0].Path)
	assert.Equal(t, files[0].Size, result[0].Size)
}

func TestSerializeDeserializeLargeFileList(t *testing.T) {
	t.Parallel()

	files := make([]model.TorrentFile, 1500)
	for i := range files {
		files[i] = model.TorrentFile{
			Index:     uint(i),
			Path:      fmt.Sprintf("dir%d/subdir/file_%04d.mp3", i%10, i),
			Extension: model.NewNullString("mp3"),
			Size:      uint(44100 * (i + 1)),
		}
	}

	data, err := SerializeFiles(files)
	require.NoError(t, err)

	result, err := DeserializeFiles(data)
	require.NoError(t, err)
	require.Len(t, result, 1500)

	for i := range files {
		assert.Equal(t, files[i].Index, result[i].Index)
		assert.Equal(t, files[i].Path, result[i].Path)
		assert.Equal(t, files[i].Size, result[i].Size)
	}
}

func TestSerializeDeserializeSpecialCharacters(t *testing.T) {
	t.Parallel()

	files := []model.TorrentFile{
		{
			Index:     0,
			Path:      "日本語/映画.mkv", //nolint:gosmopolitan // intentional non-ASCII test path
			Extension: model.NewNullString("mkv"),
			Size:      1000,
		},
		{
			Index:     1,
			Path:      "файлы/фильм,часть1.avi",
			Extension: model.NewNullString("avi"),
			Size:      2000,
		},
		{
			Index:     2,
			Path:      "中文/电影 (2024).mp4", //nolint:gosmopolitan // intentional non-ASCII test path
			Extension: model.NewNullString("mp4"),
			Size:      3000,
		},
		{
			Index:     3,
			Path:      "path/with spaces & symbols!.flac",
			Extension: model.NewNullString("flac"),
			Size:      4000,
		},
	}

	data, err := SerializeFiles(files)
	require.NoError(t, err)

	result, err := DeserializeFiles(data)
	require.NoError(t, err)
	require.Len(t, result, len(files))

	for i, f := range files {
		assert.Equal(t, f.Path, result[i].Path)
	}
}

func TestExtractUniqueExtensions(t *testing.T) {
	t.Parallel()

	files := []model.TorrentFile{
		{Path: "a.mkv"},
		{Path: "b.mkv"},
		{Path: "c.srt"},
		{Path: "d.mp3"},
		{Path: "e.srt"},
		{Path: "no_extension"},
		{Path: "f.mkv"},
	}

	exts := ExtractUniqueExtensions(files)
	assert.Equal(t, []string{"mkv", "mp3", "srt"}, exts)
}

func TestExtractUniqueExtensionsEmpty(t *testing.T) {
	t.Parallel()

	// Non-nil empty slice (not nil): torrents.file_extensions / torrent_file_summary.extensions
	// are JSONB NOT NULL, and a nil slice serializes to SQL NULL.
	exts := ExtractUniqueExtensions(nil)
	assert.Equal(t, []string{}, exts)

	exts = ExtractUniqueExtensions([]model.TorrentFile{})
	assert.Equal(t, []string{}, exts)
}

func TestExtractUniqueExtensionsNoExtensions(t *testing.T) {
	t.Parallel()

	files := []model.TorrentFile{
		{Path: "no_ext_file"},
		{Path: "another_file"},
	}
	exts := ExtractUniqueExtensions(files)
	assert.Equal(t, []string{}, exts)
}

func TestBuildFileSummary(t *testing.T) {
	t.Parallel()

	var infoHash protocol.ID

	copy(infoHash[:], []byte("12345678901234567890"))

	files := []model.TorrentFile{
		{Index: 0, Path: "movie/video.mkv", Size: 1_500_000_000},
		{Index: 1, Path: "movie/audio.mp3", Size: 5_000_000},
		{Index: 2, Path: "movie/subs.srt", Size: 50_000},
		{Index: 3, Path: "movie/readme.txt", Size: 1_000},
	}

	summary := BuildFileSummary(infoHash, files)

	assert.Equal(t, infoHash, summary.InfoHash)
	assert.Equal(t, 4, summary.FileCount)
	assert.Equal(t, int64(1_505_051_000), summary.TotalSize)
	assert.Equal(t, int64(1_500_000_000), summary.LargestFileSize)
	assert.True(t, summary.HasVideo)
	assert.True(t, summary.HasAudio)
	assert.True(t, summary.HasSubtitle)
	assert.Equal(t, []string{"mkv", "mp3", "srt", "txt"}, summary.Extensions)
}

func TestBuildFileSummaryNoMedia(t *testing.T) {
	t.Parallel()

	var infoHash protocol.ID

	files := []model.TorrentFile{
		{Index: 0, Path: "data/file.csv", Size: 1000},
		{Index: 1, Path: "data/readme.txt", Size: 500},
	}

	summary := BuildFileSummary(infoHash, files)

	assert.False(t, summary.HasVideo)
	assert.False(t, summary.HasAudio)
	assert.False(t, summary.HasSubtitle)
	assert.Equal(t, 2, summary.FileCount)
}

func TestBuildFileSummaryEmpty(t *testing.T) {
	t.Parallel()

	var infoHash protocol.ID
	summary := BuildFileSummary(infoHash, nil)

	assert.Equal(t, 0, summary.FileCount)
	assert.Equal(t, int64(0), summary.TotalSize)
	assert.Equal(t, int64(0), summary.LargestFileSize)
	assert.False(t, summary.HasVideo)
	assert.False(t, summary.HasAudio)
	assert.False(t, summary.HasSubtitle)
}

func TestCompressionRatio(t *testing.T) {
	t.Parallel()

	files := make([]model.TorrentFile, 500)
	for i := range files {
		files[i] = model.TorrentFile{
			Index:     uint(i),
			Path:      fmt.Sprintf("Season %02d/Episode %02d - Title of Episode.mkv", i/20+1, i%20+1),
			Extension: model.NewNullString("mkv"),
			Size:      uint(700_000_000 + i*1000),
		}
	}

	compressed, err := SerializeFiles(files)
	require.NoError(t, err)

	compact := make([]compactFile, len(files))
	for i, f := range files {
		compact[i] = compactFile{
			Index:     int(f.Index),
			Path:      f.Path,
			Extension: f.Extension.String,
			Size:      f.Size,
		}
	}

	raw, err := msgpack.Marshal(compact)
	require.NoError(t, err)

	ratio := float64(len(compressed)) / float64(len(raw))
	t.Logf("Raw msgpack: %d bytes, Compressed: %d bytes, Ratio: %.2f%%", len(raw), len(compressed), ratio*100)
	assert.Less(t, ratio, 0.30, "compression ratio should be under 30%%")
}
