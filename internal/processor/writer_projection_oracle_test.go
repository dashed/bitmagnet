package processor

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/stretchr/testify/require"
)

const writerProjectionPrintEnv = "BITMAGNET_PRINT_WRITER_PROJECTION_ORACLE"

var writerProjectionFixtureIDs = [...]string{
	"no-sources",
	"source-zero-is-present",
	"source-maxima-and-null-fallback",
	"source-maxima-permuted",
	"published-at-exact-cutoff-falls-back",
	"published-at-cutoff-plus-one-microsecond",
	"video-v1080p-and-v3dsbs",
	"video-v3dou-literal",
	"paths-ascii-reduction",
	"paths-utf8-prefix-split",
	"paths-utf8-suffix-split",
	"paths-overlong-lexeme-then-normal",
}

type writerProjectionFixture struct {
	ID       string                   `json:"id"`
	Input    writerProjectionInput    `json:"input"`
	Expected writerProjectionExpected `json:"expected"`
}

type writerProjectionInput struct {
	Torrent        writerProjectionTorrent        `json:"torrent"`
	Classification writerProjectionClassification `json:"classification"`
}

type writerProjectionTorrent struct {
	InfoHash        string                           `json:"infoHash"`
	Name            string                           `json:"name"`
	CreatedAtMicros int64                            `json:"createdAtMicros"`
	Files           []string                         `json:"files"`
	Sources         []writerProjectionSourceSnapshot `json:"sources"`
}

type writerProjectionSourceSnapshot struct {
	Seeders           *uint64 `json:"seeders"`
	Leechers          *uint64 `json:"leechers"`
	PublishedAtMicros *int64  `json:"publishedAtMicros"`
	CreatedAtMicros   int64   `json:"createdAtMicros"`
}

type writerProjectionClassification struct {
	ContentType     string  `json:"contentType"`
	VideoResolution *string `json:"videoResolution"`
	VideoSource     *string `json:"videoSource"`
	VideoCodec      *string `json:"videoCodec"`
	Video3D         *string `json:"video3d"`
	VideoModifier   *string `json:"videoModifier"`
	ReleaseGroup    *string `json:"releaseGroup"`
}

type writerProjectionExpected struct {
	Seeders           *uint64 `json:"seeders"`
	Leechers          *uint64 `json:"leechers"`
	PublishedAtMicros int64   `json:"publishedAtMicros"`
	Tsv               string  `json:"tsv"`
}

func TestWriterProjectionOracle(t *testing.T) {
	t.Parallel()

	fixtures := readWriterProjectionFixtures(t)
	require.Len(t, fixtures, len(writerProjectionFixtureIDs))

	actualFixtures := make([]writerProjectionFixture, 0, len(fixtures))
	for i, fixture := range fixtures {
		require.Equal(t, writerProjectionFixtureIDs[i], fixture.ID)
		actual := projectWriterFixture(t, fixture.Input)
		actualFixtures = append(actualFixtures, writerProjectionFixture{
			ID:       fixture.ID,
			Input:    fixture.Input,
			Expected: actual,
		})

		if os.Getenv(writerProjectionPrintEnv) == "" {
			t.Run(fixture.ID, func(t *testing.T) {
				t.Parallel()

				require.Equal(t, fixture.Expected, actual)
			})
		}
	}

	if os.Getenv(writerProjectionPrintEnv) != "" {
		encoded, err := json.MarshalIndent(actualFixtures, "", "  ")
		require.NoError(t, err)
		t.Logf("review and copy the regenerated oracle:\n%s", encoded)
	}
}

func readWriterProjectionFixtures(t *testing.T) []writerProjectionFixture {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	path := filepath.Join(
		filepath.Dir(filename),
		"..", "..", "testdata", "parity", "processor-writer-projection", "fixtures.json",
	)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var fixtures []writerProjectionFixture
	require.NoError(t, json.Unmarshal(raw, &fixtures))
	return fixtures
}

func projectWriterFixture(t *testing.T, input writerProjectionInput) writerProjectionExpected {
	t.Helper()
	torrent := model.Torrent{
		InfoHash:  protocol.MustParseID(input.Torrent.InfoHash),
		Name:      input.Torrent.Name,
		CreatedAt: time.UnixMicro(input.Torrent.CreatedAtMicros).UTC(),
		Files:     make([]model.TorrentFile, 0, len(input.Torrent.Files)),
		Sources:   make([]model.TorrentsTorrentSource, 0, len(input.Torrent.Sources)),
	}
	for i, path := range input.Torrent.Files {
		torrent.Files = append(torrent.Files, model.TorrentFile{Index: uint(i), Path: path})
	}
	for _, source := range input.Torrent.Sources {
		row := model.TorrentsTorrentSource{
			CreatedAt: time.UnixMicro(source.CreatedAtMicros).UTC(),
		}
		if source.Seeders != nil {
			row.Seeders = model.NewNullUint(uint(*source.Seeders))
		}
		if source.Leechers != nil {
			row.Leechers = model.NewNullUint(uint(*source.Leechers))
		}
		if source.PublishedAtMicros != nil {
			row.PublishedAt = sql.NullTime{
				Time:  time.UnixMicro(*source.PublishedAtMicros).UTC(),
				Valid: true,
			}
		}
		torrent.Sources = append(torrent.Sources, row)
	}
	require.NoError(t, torrent.AfterFind(nil))

	result := classification.Result{ContentAttributes: classification.ContentAttributes{
		ContentType: model.NewNullContentType(input.Classification.ContentType),
	}}
	if value := input.Classification.VideoResolution; value != nil {
		parsed, err := model.ParseVideoResolution(*value)
		require.NoError(t, err)
		result.VideoResolution = model.NewNullVideoResolution(parsed)
	}
	if value := input.Classification.VideoSource; value != nil {
		parsed, err := model.ParseVideoSource(*value)
		require.NoError(t, err)
		result.VideoSource = model.NewNullVideoSource(parsed)
	}
	if value := input.Classification.VideoCodec; value != nil {
		parsed, err := model.ParseVideoCodec(*value)
		require.NoError(t, err)
		result.VideoCodec = model.NewNullVideoCodec(parsed)
	}
	if value := input.Classification.Video3D; value != nil {
		parsed, err := model.ParseVideo3D(*value)
		require.NoError(t, err)
		result.Video3D = model.NewNullVideo3D(parsed)
	}
	if value := input.Classification.VideoModifier; value != nil {
		parsed, err := model.ParseVideoModifier(*value)
		require.NoError(t, err)
		result.VideoModifier = model.NewNullVideoModifier(parsed)
	}
	if value := input.Classification.ReleaseGroup; value != nil {
		result.ReleaseGroup = model.NewNullString(*value)
	}

	torrentContent := newTorrentContent(torrent, result)
	return writerProjectionExpected{
		Seeders:           nullableWriterProjectionUint(torrentContent.Seeders),
		Leechers:          nullableWriterProjectionUint(torrentContent.Leechers),
		PublishedAtMicros: torrentContent.PublishedAt.UnixMicro(),
		Tsv:               torrentContent.Tsv.String(),
	}
}

func nullableWriterProjectionUint(value model.NullUint) *uint64 {
	if !value.Valid {
		return nil
	}
	result := uint64(value.Uint)
	return &result
}
