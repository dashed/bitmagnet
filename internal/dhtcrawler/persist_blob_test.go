package dhtcrawler

import (
	"testing"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	pmetainfo "github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTorrentModelWithBlob(t *testing.T) {
	t.Parallel()

	var hash protocol.ID

	copy(hash[:], []byte("01234567890123456789"))

	info := metainfo.Info{
		Name:        "test-torrent",
		PieceLength: 256 * 1024,
		Files: []metainfo.FileInfo{
			{Length: 1000, Path: []string{"dir", "video.mkv"}},
			{Length: 500, Path: []string{"dir", "subs.srt"}},
			{Length: 100, Path: []string{"dir", "readme.txt"}},
		},
	}

	torrent, err := createTorrentModel(hash, pmetainfo.ParsedInfo{Info: info, MetaVersion: 1}, false, 1000)
	require.NoError(t, err)

	assert.NotNil(t, torrent.FilesData)
	assert.NotEmpty(t, torrent.FilesData)

	files, err := blobmigration.DeserializeFiles(torrent.FilesData)
	require.NoError(t, err)
	assert.Len(t, files, 3)

	assert.NotNil(t, torrent.FileExts)
	assert.Contains(t, torrent.FileExts, "mkv")
	assert.Contains(t, torrent.FileExts, "srt")
	assert.Contains(t, torrent.FileExts, "txt")
}

func TestCreateTorrentModelSingleFile(t *testing.T) {
	t.Parallel()

	var hash protocol.ID

	copy(hash[:], []byte("01234567890123456789"))

	info := metainfo.Info{
		Name:        "movie.mkv",
		PieceLength: 256 * 1024,
		Length:      1_500_000_000,
	}

	torrent, err := createTorrentModel(hash, pmetainfo.ParsedInfo{Info: info, MetaVersion: 1}, false, 1000)
	require.NoError(t, err)

	assert.Nil(t, torrent.FilesData)
	assert.Nil(t, torrent.FileExts)
}

func TestCreateTorrentModelNoFiles(t *testing.T) {
	t.Parallel()

	var hash protocol.ID

	copy(hash[:], []byte("01234567890123456789"))

	info := metainfo.Info{
		Name:        "empty-torrent",
		PieceLength: 256 * 1024,
	}

	torrent, err := createTorrentModel(hash, pmetainfo.ParsedInfo{Info: info, MetaVersion: 1}, false, 1000)
	require.NoError(t, err)

	assert.Nil(t, torrent.FilesData)
	assert.Nil(t, torrent.FileExts)
}

func TestCreateTorrentModelOverThreshold(t *testing.T) {
	t.Parallel()

	var hash protocol.ID

	copy(hash[:], []byte("01234567890123456789"))

	files := make([]metainfo.FileInfo, 50)
	for i := range files {
		files[i] = metainfo.FileInfo{
			Length: int64(1000 * (i + 1)),
			Path:   []string{"dir", "file" + string(rune('a'+i%26)) + ".mp3"},
		}
	}

	info := metainfo.Info{
		Name:        "big-torrent",
		PieceLength: 256 * 1024,
		Files:       files,
	}

	torrent, err := createTorrentModel(hash, pmetainfo.ParsedInfo{Info: info, MetaVersion: 1}, false, 10)
	require.NoError(t, err)

	assert.NotNil(t, torrent.FilesData)

	deserialized, err := blobmigration.DeserializeFiles(torrent.FilesData)
	require.NoError(t, err)
	assert.Len(t, deserialized, 10)

	assert.NotNil(t, torrent.FileExts)
	assert.Contains(t, torrent.FileExts, "mp3")
}
