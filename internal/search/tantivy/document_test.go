package tantivy

import (
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// infoHash returns a 20-byte info hash with every byte set to b.
func infoHash(b byte) protocol.ID {
	var id protocol.ID
	for i := range id {
		id[i] = b
	}

	return id
}

func torrentFile(index uint, path, ext string, size uint) model.TorrentFile {
	return model.TorrentFile{
		Index: index,
		Path:  path,
		Extension: model.NullString{
			String: ext,
			Valid:  ext != "",
		},
		Size: size,
	}
}

// classifiedTC mirrors the Rust transform's classified_row() + classified_files()
// fixture (bitmagnet-search/src/transform.rs): a fully-classified movie row with
// a three-file torrent (one extensionless).
func classifiedTC() model.TorrentContent {
	return model.TorrentContent{
		InfoHash:      infoHash(0xAB),
		ContentType:   model.NullContentType{ContentType: model.ContentTypeMovie, Valid: true},
		ContentSource: model.NewNullString("tmdb"),
		ContentID:     model.NewNullString("603"),
		Languages: model.Languages{
			model.Language("en"): {},
			model.Language("fr"): {},
		},
		VideoResolution: model.NullVideoResolution{VideoResolution: model.VideoResolutionV1080p, Valid: true},
		VideoSource:     model.NullVideoSource{VideoSource: model.VideoSourceBluRay, Valid: true},
		VideoCodec:      model.NullVideoCodec{VideoCodec: model.VideoCodecX265, Valid: true},
		Video3D:         model.NullVideo3D{Video3D: model.Video3DV3D, Valid: true},
		VideoModifier:   model.NullVideoModifier{VideoModifier: model.VideoModifierREMUX, Valid: true},
		ReleaseGroup:    model.NewNullString("GROUP"),
		Seeders:         model.NewNullUint(123),
		Leechers:        model.NewNullUint(4),
		PublishedAt:     time.Unix(1_700_000_000, 0),
		Size:            9_000_000_000,
		FilesCount:      model.NewNullUint(2),
		Torrent: model.Torrent{
			InfoHash:  infoHash(0xAB),
			Name:      "The Matrix 1999 1080p BluRay x265-GROUP",
			CreatedAt: time.Unix(1_650_000_000, 0),
			Files: []model.TorrentFile{
				torrentFile(0, "The.Matrix.1999.1080p.mkv", "mkv", 8_900_000_000),
				torrentFile(1, "The.Matrix.1999.1080p.srt", "srt", 50_000),
				// Extensionless path: contributes a path but no extension.
				torrentFile(2, "readme", "", 100),
			},
		},
		Content: model.Content{
			Type:          model.ContentTypeMovie,
			Source:        "tmdb",
			ID:            "603",
			Title:         "The Matrix",
			OriginalTitle: model.NewNullString("The Matrix"),
			ReleaseYear:   1999,
			Collections: []model.ContentCollection{
				// Deliberately reverse-sorted to prove BuildDocument sorts by name.
				{Type: "genre", Name: "sci-fi"},
				{Type: "genre", Name: "action"},
				// A non-genre collection must be ignored.
				{Type: "cast", Name: "Keanu Reeves"},
			},
		},
	}
}

// unclassifiedTC mirrors the Rust transform's unclassified_row(): every content
// field empty, no files.
func unclassifiedTC() model.TorrentContent {
	return model.TorrentContent{
		InfoHash:    infoHash(0x01),
		PublishedAt: time.Unix(1_600_000_000, 0),
		Size:        6_000_000_000,
		Torrent: model.Torrent{
			InfoHash:  infoHash(0x01),
			Name:      "ubuntu-24.04-desktop-amd64.iso",
			CreatedAt: time.Unix(1_550_000_000, 0),
		},
	}
}

func TestBuildDocumentClassified(t *testing.T) {
	t.Parallel()

	doc := BuildDocument(classifiedTC())

	require.Len(t, doc.GetInfoHash(), 20)

	for _, b := range doc.GetInfoHash() {
		assert.Equal(t, byte(0xAB), b)
	}

	assert.Equal(t, "The Matrix 1999 1080p BluRay x265-GROUP", doc.GetTorrentName())
	assert.Equal(t, "The Matrix", doc.GetContentTitle())
	assert.Equal(t, "The Matrix", doc.GetOriginalTitle())
	assert.Equal(t, uint32(1999), doc.GetReleaseYear())

	// Weight-C video fields: video_resolution + video_3d use Go's Label()
	// (leading "V" stripped); source/codec/modifier pass through raw.
	assert.Equal(
		t,
		"1080p",
		doc.GetVideoResolution(),
		"video_resolution sends Go's Label() (V1080p -> 1080p), matching the fixed transform.rs",
	)
	assert.Equal(t, "BluRay", doc.GetVideoSource())
	assert.Equal(t, "x265", doc.GetVideoCodec())
	assert.Equal(t, "3D", doc.GetVideo_3D(), "video_3d sends Go's Label() (V3D -> 3D)")
	assert.Equal(t, "REMUX", doc.GetVideoModifier())
	assert.Equal(t, "GROUP", doc.GetReleaseGroup())

	assert.Equal(t, pb.ContentType_CONTENT_TYPE_MOVIE, doc.GetContentType())
	assert.Equal(t, "tmdb", doc.GetContentSource())
	assert.Equal(t, "603", doc.GetContentId())

	// Genres come from the content's genre collections, sorted by name.
	assert.Equal(t, []string{"action", "sci-fi"}, doc.GetGenres())

	// File paths: every non-empty path (incl. the extensionless one), in order.
	assert.Equal(t, []string{
		"The.Matrix.1999.1080p.mkv",
		"The.Matrix.1999.1080p.srt",
		"readme",
	}, doc.GetFilePaths())
	// File extensions: distinct + sorted; "readme" contributes none.
	assert.Equal(t, []string{"mkv", "srt"}, doc.GetFileExtensions())

	// Languages: alpha-2 codes in name-sorted (natsort) order, matching the
	// stored JSONB order the backfill reads.
	assert.Equal(t, []string{"en", "fr"}, doc.GetLanguages())
	// audio_languages has no Postgres source and must always be empty.
	assert.Empty(t, doc.GetAudioLanguages())

	assert.Equal(t, uint32(123), doc.GetSeeders())
	assert.Equal(t, uint32(4), doc.GetLeechers())
	assert.Equal(t, uint32(2), doc.GetFilesCount())
	assert.Equal(t, uint64(9_000_000_000), doc.GetSize())
	assert.Equal(t, int64(1_700_000_000), doc.GetPublishedAt())
}

func TestBuildDocumentUnclassified(t *testing.T) {
	t.Parallel()

	doc := BuildDocument(unclassifiedTC())

	assert.Equal(t, "ubuntu-24.04-desktop-amd64.iso", doc.GetTorrentName())
	assert.Equal(t, uint64(6_000_000_000), doc.GetSize())
	assert.Equal(t, int64(1_600_000_000), doc.GetPublishedAt())

	// Everything classification-derived is the zero / empty value.
	assert.Equal(t, pb.ContentType_CONTENT_TYPE_UNKNOWN, doc.GetContentType())
	assert.Zero(t, doc.GetReleaseYear())
	assert.Empty(t, doc.GetContentTitle())
	assert.Empty(t, doc.GetOriginalTitle())
	assert.Empty(t, doc.GetContentSource())
	assert.Empty(t, doc.GetContentId())
	assert.Empty(t, doc.GetVideoResolution())
	assert.Empty(t, doc.GetVideo_3D())
	assert.Empty(t, doc.GetGenres())
	assert.Empty(t, doc.GetLanguages())
	assert.Empty(t, doc.GetFilePaths())
	assert.Empty(t, doc.GetFileExtensions())
	// No blob and no column -> files_count falls back to 0.
	assert.Zero(t, doc.GetFilesCount())
}

// TestDocIDEqualsInferID locks the cross-system invariant: the DocID derived
// from the built document (the sidecar's Tantivy upsert key) is byte-identical
// to model.TorrentContent.InferID() — itself the PostgreSQL torrent_contents.id
// generated column — for both a classified and an unclassified row. A re-run /
// dual-write therefore upserts the same document, never a duplicate.
func TestDocIDEqualsInferID(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]model.TorrentContent{
		"classified":   classifiedTC(),
		"unclassified": unclassifiedTC(),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := DocID(BuildDocument(tc))
			require.Equal(t, tc.InferID(), got, "DocID must equal InferID()")
		})
	}

	// Spot-check the exact literal forms.
	assert.Equal(t,
		"abababababababababababababababababababab:movie:tmdb:603",
		DocID(BuildDocument(classifiedTC())),
	)
	assert.Equal(t,
		"0101010101010101010101010101010101010101:?:?:?",
		DocID(BuildDocument(unclassifiedTC())),
	)
}

// TestFilesCountUsesColumnNotBlobLength mirrors the Rust test of the same name:
// files_count tracks the tc.files_count column for PG parity, not len(Files).
func TestFilesCountUsesColumnNotBlobLength(t *testing.T) {
	t.Parallel()

	tc := classifiedTC() // 3 files in the torrent, but files_count column == 2
	assert.Equal(t, uint32(2), BuildDocument(tc).GetFilesCount())

	tc.FilesCount = model.NullUint{} // unset -> 0, despite 3 files present
	assert.Zero(t, BuildDocument(tc).GetFilesCount())
}

// TestPublishedAtFallsBackToTorrentCreatedAt mirrors the SQL
// COALESCE(tc.published_at, t.created_at): a zero published_at uses the
// torrent's created_at.
func TestPublishedAtFallsBackToTorrentCreatedAt(t *testing.T) {
	t.Parallel()

	tc := classifiedTC()
	tc.PublishedAt = time.Time{} // zero -> fall back
	assert.Equal(t, tc.Torrent.CreatedAt.Unix(), BuildDocument(tc).GetPublishedAt())
}

// TestDistinctClassificationsShareInfoHash mirrors the Rust test: two
// classifications of one torrent share info_hash + name but differ by content_id,
// so their DocIDs differ and they coexist as distinct documents.
func TestDistinctClassificationsShareInfoHash(t *testing.T) {
	t.Parallel()

	a := classifiedTC()
	b := classifiedTC()
	b.ContentID = model.NewNullString("604")

	docA := BuildDocument(a)
	docB := BuildDocument(b)

	assert.Equal(t, docA.GetInfoHash(), docB.GetInfoHash())
	assert.Equal(t, docA.GetTorrentName(), docB.GetTorrentName())
	assert.NotEqual(t, docA.GetContentId(), docB.GetContentId())
	assert.NotEqual(t, DocID(docA), DocID(docB))
}
