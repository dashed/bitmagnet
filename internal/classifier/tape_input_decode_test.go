package classifier

import (
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/stretchr/testify/require"
)

func TestDecodeTapeClassifierInputRoundTripsStableDTO(t *testing.T) {
	t.Parallel()

	const subject = "0123456789abcdef0123456789abcdef01234567"
	infoHash := protocol.MustParseID(subject)
	language := model.ParseLanguage("ja")
	require.True(t, language.Valid)

	releaseYear := uint16(2024)
	adult := false
	originalTitle := "Original title"
	overview := "Overview"
	runtime := uint16(92)
	popularity := float32(12.5)
	voteAverage := float32(7.25)
	voteCount := uint(81)
	createdAt := time.Unix(1_700_000_000, 0).UTC()
	updatedAt := time.Unix(1_700_000_100, 0).UTC()

	torrent := model.Torrent{
		InfoHash:    infoHash,
		Name:        "Example.Show.S01E02.1080p.BluRay.x264-GRP.mkv",
		Size:        1_234_567,
		FilesStatus: model.FilesStatusMulti,
		Extension:   model.NewNullString("mkv"),
		FilesCount:  model.NewNullUint(2),
		Files: []model.TorrentFile{
			{InfoHash: infoHash, Index: 0, Path: "video/main.mkv", Extension: model.NewNullString("mkv"), Size: 1_200_000},
			{InfoHash: infoHash, Index: 1, Path: "subs/main.srt", Extension: model.NewNullString("srt"), Size: 34_567},
		},
		Hint: model.TorrentHint{
			InfoHash:        infoHash,
			ContentType:     model.ContentTypeTvShow,
			ContentSource:   model.NewNullString("imdb"),
			ContentID:       model.NewNullString("tt1234567"),
			Languages:       model.Languages{language.Language: {}},
			Episodes:        model.ParseEpisodes("S01E02"),
			VideoResolution: model.NewNullVideoResolution(model.VideoResolutionV1080p),
			VideoSource:     model.NewNullVideoSource(model.VideoSourceBluRay),
			VideoCodec:      model.NewNullVideoCodec(model.VideoCodecX264),
			ReleaseGroup:    model.NewNullString("GRP"),
		},
		Contents: []model.TorrentContent{{
			InfoHash:      infoHash,
			ContentType:   model.NewNullContentType(model.ContentTypeTvShow),
			ContentSource: model.NewNullString("tmdb"),
			ContentID:     model.NewNullString("123"),
			Content: model.Content{
				Type:             model.ContentTypeTvShow,
				Source:           "tmdb",
				ID:               "123",
				Title:            "Example Show",
				ReleaseDate:      model.NewDateFromParts(model.Year(releaseYear), time.January, 2),
				ReleaseYear:      model.Year(releaseYear),
				Adult:            model.NewNullBool(adult),
				OriginalLanguage: model.NewNullLanguage(language.Language),
				OriginalTitle:    model.NewNullString(originalTitle),
				Overview:         model.NewNullString(overview),
				Runtime:          model.NewNullUint16(runtime),
				Popularity:       model.NewNullFloat32(popularity),
				VoteAverage:      model.NewNullFloat32(voteAverage),
				VoteCount:        model.NewNullUint(voteCount),
				CreatedAt:        createdAt,
				UpdatedAt:        updatedAt,
				Collections: []model.ContentCollection{{
					Type: "network", Source: "tmdb", ID: "7", Name: "Example Network",
				}},
				Attributes: []model.ContentAttribute{{
					ContentType: model.ContentTypeTvShow, ContentSource: "tmdb", ContentID: "123",
					Source: "imdb", Key: "alternative_id", Value: "tt1234567",
				}},
			},
		}},
	}

	want, err := tape.Marshal(newTapeClassifierInput(subject, torrent))
	require.NoError(t, err)

	decoded, err := DecodeTapeClassifierInput(want)
	require.NoError(t, err)
	got, err := tape.Marshal(newTapeClassifierInput(subject, decoded))
	require.NoError(t, err)

	require.JSONEq(t, string(want), string(got))
}

func TestDecodeTapeClassifierInputRejectsNonInfoHashID(t *testing.T) {
	t.Parallel()

	_, err := DecodeTapeClassifierInput([]byte(`{
		"id":"fixture-name",
		"name":"fixture",
		"size":1,
		"filesStatus":"no_info",
		"files":[],
		"contents":[]
	}`))
	require.ErrorContains(t, err, "is not an info hash")
}
