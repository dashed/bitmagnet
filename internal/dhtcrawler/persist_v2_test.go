package dhtcrawler

import (
	"bytes"
	"os"
	"testing"

	"github.com/anacrolix/torrent/metainfo"
	infohashv2 "github.com/anacrolix/torrent/types/infohash-v2"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	pmetainfo "github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loadFixtureParsed reads a .torrent fixture, derives the canonical 20-byte
// discovery hash (v1 SHA-1 for hybrid, truncated SHA-256 for pure-v2), and runs
// the real ParseMetaInfoBytes verifier — mirroring the crawl path exactly.
func loadFixtureParsed(t *testing.T, path string, pureV2 bool) (protocol.ID, pmetainfo.ParsedInfo) {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	mi, err := metainfo.Load(bytes.NewReader(raw))
	require.NoError(t, err)

	infoBytes := []byte(mi.InfoBytes)

	var hash protocol.ID

	if pureV2 {
		v2 := infohashv2.HashBytes(infoBytes)
		hash = protocol.ID(*v2.ToShort())
	} else {
		hash = protocol.ID(mi.HashInfoBytes())
	}

	parsed, err := pmetainfo.ParseMetaInfoBytes(hash, infoBytes)
	require.NoError(t, err)

	return hash, parsed
}

func TestCreateTorrentModelPureV2(t *testing.T) {
	t.Parallel()

	hash, parsed := loadFixtureParsed(t, "testdata/bittorrent-v2-test.torrent", true)

	torrent, err := createTorrentModel(hash, parsed, false, 1000)
	require.NoError(t, err)

	// Canonical PK is the truncated SHA-256, the value we crawled with.
	assert.Equal(t, hash, torrent.InfoHash)

	// Pure-v2: no v1 identity, full v2 identity recorded, meta_version 2.
	assert.Nil(t, torrent.InfoHashV1)
	require.NotNil(t, torrent.InfoHashV2)
	assert.True(t, torrent.MetaVersion.Valid)
	assert.Equal(t, uint16(2), torrent.MetaVersion.Uint16)

	// Multi-file: per-file rows with real paths from the v2 file tree.
	assert.Equal(t, model.FilesStatusMulti, torrent.FilesStatus)
	assert.NotEmpty(t, torrent.Files)
	require.True(t, torrent.FilesCount.Valid)
	assert.Equal(t, uint(len(parsed.Info.UpvertedFiles())), torrent.FilesCount.Uint)

	paths := make([]string, 0, len(torrent.Files))

	for _, f := range torrent.Files {
		assert.Equal(t, hash, f.InfoHash)
		assert.NotEmpty(t, f.Path)
		paths = append(paths, f.Path)
	}

	assert.Contains(t, paths, "asd-rupture.mp4", "real file-tree path should be enumerated")

	// Blob + extension index populated for the multi branch.
	assert.NotEmpty(t, torrent.FilesData)
	assert.Contains(t, torrent.FileExts, "mp4")
}

func TestCreateTorrentModelHybrid(t *testing.T) {
	t.Parallel()

	hash, parsed := loadFixtureParsed(t, "testdata/bittorrent-v2-hybrid-test.torrent", false)

	torrent, err := createTorrentModel(hash, parsed, false, 1000)
	require.NoError(t, err)

	// Hybrid: PK is the v1 SHA-1; both identities recorded; meta_version 2.
	assert.Equal(t, hash, torrent.InfoHash)
	require.NotNil(t, torrent.InfoHashV1)
	assert.Equal(t, hash, *torrent.InfoHashV1, "info_hash_v1 equals the v1 PK for a hybrid")
	require.NotNil(t, torrent.InfoHashV2)
	assert.False(t, torrent.InfoHashV2.IsZero())
	assert.True(t, torrent.MetaVersion.Valid)
	assert.Equal(t, uint16(2), torrent.MetaVersion.Uint16)

	// Files enumerated from the v2 file tree.
	assert.Equal(t, model.FilesStatusMulti, torrent.FilesStatus)
	assert.NotEmpty(t, torrent.Files)
}

func TestCreateTorrentModelV2SingleFile(t *testing.T) {
	t.Parallel()

	var hash protocol.ID

	copy(hash[:], []byte("01234567890123456789"))

	// A REAL v2 single-file info: the file tree root is a directory keyed by the
	// file name, with the file properties under the "" key — so FileTree.IsDir()
	// is TRUE even though there is a single top-level file. createTorrentModel must
	// still classify this as single (matching v1 single-file behaviour).
	info := metainfo.Info{
		Name:        "movie.mkv",
		PieceLength: 256 * 1024,
		MetaVersion: 2,
		FileTree: metainfo.FileTree{
			Dir: map[string]metainfo.FileTree{
				"movie.mkv": {File: metainfo.FileTreeFile{Length: 1_500_000_000}},
			},
		},
	}
	require.True(t, info.IsDir(), "a real v2 file-tree root is a directory keyed by name")
	require.Len(t, info.UpvertedFiles(), 1, "v2 single-file yields exactly one top-level file")

	var v2 protocol.InfoHashV2

	copy(v2[:], []byte("0123456789012345678901234567890a"))

	parsed := pmetainfo.ParsedInfo{Info: info, MetaVersion: 2, InfoHashV2: &v2}

	torrent, err := createTorrentModel(hash, parsed, false, 1000)
	require.NoError(t, err)

	// Single-file: classified Single, no file rows, no blob.
	assert.Equal(t, model.FilesStatusSingle, torrent.FilesStatus)
	assert.Empty(t, torrent.Files)
	assert.Nil(t, torrent.FilesData)
	assert.Nil(t, torrent.FileExts)
	assert.False(t, torrent.FilesCount.Valid)

	// v2 identity still recorded.
	require.NotNil(t, torrent.InfoHashV2)
	assert.Equal(t, uint16(2), torrent.MetaVersion.Uint16)
}

func TestCreateTorrentModelV2OverThreshold(t *testing.T) {
	t.Parallel()

	hash, parsed := loadFixtureParsed(t, "testdata/bittorrent-v2-test.torrent", true)

	total := len(parsed.Info.UpvertedFiles())
	require.Greater(t, total, 3, "fixture must have more files than the threshold")

	torrent, err := createTorrentModel(hash, parsed, false, 3)
	require.NoError(t, err)

	// Over threshold: status flips, full count retained, rows capped at threshold.
	assert.Equal(t, model.FilesStatusOverThreshold, torrent.FilesStatus)
	require.True(t, torrent.FilesCount.Valid)
	assert.Equal(t, uint(total), torrent.FilesCount.Uint)
	assert.Len(t, torrent.Files, 3)
}
