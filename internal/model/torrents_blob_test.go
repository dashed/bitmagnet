package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/klauspost/compress/zstd"
)

type testCompactFile struct {
	Index     int    `msgpack:"i"`
	Path      string `msgpack:"p"`
	Extension string `msgpack:"e"`
	Size      uint   `msgpack:"s"`
}

func makeTestBlob(t *testing.T, files []testCompactFile) []byte {
	t.Helper()
	raw, err := msgpack.Marshal(files)
	require.NoError(t, err)
	enc, _ := zstd.NewWriter(nil)
	return enc.EncodeAll(raw, nil)
}

func TestAfterFindWithBlobData(t *testing.T) {
	orig := FilesDataDeserializer
	FilesDataDeserializer = testDeserialize
	defer func() { FilesDataDeserializer = orig }()

	blob := makeTestBlob(t, []testCompactFile{
		{Index: 0, Path: "b/second.txt", Extension: "txt", Size: 200},
		{Index: 1, Path: "a/first.mkv", Extension: "mkv", Size: 1000},
	})

	torrent := Torrent{
		FilesData: blob,
		Files: []TorrentFile{
			{Index: 99, Path: "stale.bin", Size: 1},
		},
	}

	err := torrent.AfterFind(nil)
	require.NoError(t, err)

	require.Len(t, torrent.Files, 2)
	assert.Equal(t, "a/first.mkv", torrent.Files[0].Path)
	assert.Equal(t, "b/second.txt", torrent.Files[1].Path)
}

func TestAfterFindWithNilBlob(t *testing.T) {
	orig := FilesDataDeserializer
	FilesDataDeserializer = testDeserialize
	defer func() { FilesDataDeserializer = orig }()

	existing := []TorrentFile{
		{Index: 0, Path: "z.mp4", Size: 500},
		{Index: 1, Path: "a.mp4", Size: 300},
	}

	torrent := Torrent{
		FilesData: nil,
		Files:     existing,
	}

	err := torrent.AfterFind(nil)
	require.NoError(t, err)

	require.Len(t, torrent.Files, 2)
	assert.Equal(t, "a.mp4", torrent.Files[0].Path)
	assert.Equal(t, "z.mp4", torrent.Files[1].Path)
}

func TestAfterFindWithEmptyBlob(t *testing.T) {
	orig := FilesDataDeserializer
	FilesDataDeserializer = testDeserialize
	defer func() { FilesDataDeserializer = orig }()

	existing := []TorrentFile{
		{Index: 0, Path: "keep.txt", Size: 100},
	}

	torrent := Torrent{
		FilesData: []byte{},
		Files:     existing,
	}

	err := torrent.AfterFind(nil)
	require.NoError(t, err)

	require.Len(t, torrent.Files, 1)
	assert.Equal(t, "keep.txt", torrent.Files[0].Path)
}

func TestAfterFindWithCorruptBlob(t *testing.T) {
	orig := FilesDataDeserializer
	FilesDataDeserializer = testDeserialize
	defer func() { FilesDataDeserializer = orig }()

	existing := []TorrentFile{
		{Index: 0, Path: "fallback.txt", Size: 42},
	}

	torrent := Torrent{
		FilesData: []byte{0xFF, 0xFE, 0xFD, 0x00, 0x01},
		Files:     existing,
	}

	err := torrent.AfterFind(nil)
	require.NoError(t, err)

	require.Len(t, torrent.Files, 1)
	assert.Equal(t, "fallback.txt", torrent.Files[0].Path)
}

func TestAfterFindWithNilDeserializer(t *testing.T) {
	orig := FilesDataDeserializer
	FilesDataDeserializer = nil
	defer func() { FilesDataDeserializer = orig }()

	blob := makeTestBlob(t, []testCompactFile{
		{Index: 0, Path: "ignored.txt", Size: 1},
	})

	existing := []TorrentFile{
		{Index: 0, Path: "kept.txt", Size: 100},
	}

	torrent := Torrent{
		FilesData: blob,
		Files:     existing,
	}

	err := torrent.AfterFind(nil)
	require.NoError(t, err)

	require.Len(t, torrent.Files, 1)
	assert.Equal(t, "kept.txt", torrent.Files[0].Path)
}

func testDeserialize(data []byte) ([]TorrentFile, error) {
	dec, _ := zstd.NewReader(nil)
	raw, err := dec.DecodeAll(data, nil)
	if err != nil {
		return nil, err
	}
	var compact []testCompactFile
	if err := msgpack.Unmarshal(raw, &compact); err != nil {
		return nil, err
	}
	files := make([]TorrentFile, len(compact))
	for i, c := range compact {
		files[i] = TorrentFile{
			Index: uint(c.Index),
			Path:  c.Path,
			Extension: NullString{
				String: c.Extension,
				Valid:  c.Extension != "",
			},
			Size: c.Size,
		}
	}
	return files, nil
}
