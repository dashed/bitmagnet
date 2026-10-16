package extfix

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// legacyFile mirrors the on-wire compactFile {i,p,e,s}. The G1-fixed SerializeFiles
// always derives `e` from the path and so can no longer produce a non-canonical blob;
// this lets the test synthesize a pre-G1 blob (empty / stale `e`) to exercise fixBlob.
type legacyFile struct {
	Index     int    `msgpack:"i"`
	Path      string `msgpack:"p"`
	Extension string `msgpack:"e"`
	Size      uint   `msgpack:"s"`
}

func encodeLegacy(t *testing.T, files []legacyFile) []byte {
	t.Helper()

	raw, err := msgpack.Marshal(files)
	require.NoError(t, err)

	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)

	defer func() { _ = enc.Close() }()

	return enc.EncodeAll(raw, nil)
}

func TestFixBlob_PopulatesEmptyE(t *testing.T) {
	t.Parallel()

	blob := encodeLegacy(t, []legacyFile{
		{Index: 0, Path: "Season 1/Episode 1.mkv", Extension: "", Size: 100}, // empty e (crawl) -> fixable
		{Index: 1, Path: "subs/ep1.srt", Extension: "", Size: 5},             // empty e -> fixable
	})

	newBlob, needsFix, err := fixBlob(blob)
	require.NoError(t, err)
	require.True(t, needsFix)

	got, err := blobmigration.DeserializeFiles(newBlob)
	require.NoError(t, err)
	require.Len(t, got, 2)

	assert.Equal(t, "mkv", got[0].Extension.String)
	assert.Equal(t, "srt", got[1].Extension.String)

	for i := range got {
		assert.Equal(t,
			model.FileExtensionFromPath(got[i].Path).String,
			got[i].Extension.String,
			"file %d: backfilled e must equal path-derived extension", i,
		)
	}

	// Idempotent: a second pass over the fixed blob rewrites nothing.
	_, needsFix2, err := fixBlob(newBlob)
	require.NoError(t, err)
	assert.False(t, needsFix2)
}

func TestFixBlob_FixesStaleE(t *testing.T) {
	t.Parallel()

	blob := encodeLegacy(t, []legacyFile{{Index: 0, Path: "clip.mkv", Extension: "mp4", Size: 1}})

	newBlob, needsFix, err := fixBlob(blob)
	require.NoError(t, err)
	require.True(t, needsFix)

	got, err := blobmigration.DeserializeFiles(newBlob)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "mkv", got[0].Extension.String)
}

func TestFixBlob_SkipsCanonical(t *testing.T) {
	t.Parallel()

	// Already-canonical, including a legitimately extension-less file with empty e.
	blob := encodeLegacy(t, []legacyFile{
		{Index: 0, Path: "movie.mkv", Extension: "mkv", Size: 1},
		{Index: 1, Path: "README", Extension: "", Size: 1},
	})

	_, needsFix, err := fixBlob(blob)
	require.NoError(t, err)
	assert.False(t, needsFix)
}

func TestFixBlob_SerializerProducedBlobIsCanonical(t *testing.T) {
	t.Parallel()

	// A blob produced by the (G1-fixed) SerializeFiles needs no fixing by construction.
	blob, err := blobmigration.SerializeFiles([]model.TorrentFile{
		{Index: 0, Path: "Season 1/Episode 1.mkv"},
		{Index: 1, Path: "notes"},
	})
	require.NoError(t, err)

	_, needsFix, err := fixBlob(blob)
	require.NoError(t, err)
	assert.False(t, needsFix)
}

func TestFixBlob_CorruptBlobErrors(t *testing.T) {
	t.Parallel()

	_, _, err := fixBlob([]byte("not a valid zstd blob"))
	require.Error(t, err)
}
