package processor

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/stretchr/testify/require"
)

func TestMaterializeTapeRerunBuildsCanonicalProcessorImage(t *testing.T) {
	t.Parallel()

	const subject = "0123456789abcdef0123456789abcdef01234567"
	infoHash := protocol.MustParseID(subject)
	keepID := subject + ":movie:?:?"

	writeSet, err := MaterializeTapeRerun(classifier.TapeReplayResult{
		Record: tape.Record{
			Subject: subject,
			Attempt: 0,
			ProcessorState: &tape.ProcessorState{ExistingContentIDs: []string{
				"stale:b", keepID, "stale:a",
			}},
		},
		Torrent: model.Torrent{
			InfoHash:    infoHash,
			Name:        "Example.Movie.2024.1080p.BluRay.x264-GRP.mkv",
			Size:        1234,
			FilesStatus: model.FilesStatusSingle,
		},
		Classification: classification.Result{
			ContentAttributes: classification.ContentAttributes{
				ContentType:     model.NewNullContentType(model.ContentTypeMovie),
				Languages:       model.Languages{model.Language("fr"): {}, model.Language("de"): {}},
				VideoResolution: model.NewNullVideoResolution(model.VideoResolutionV1080p),
				VideoSource:     model.NewNullVideoSource(model.VideoSourceBluRay),
				VideoCodec:      model.NewNullVideoCodec(model.VideoCodecX264),
				ReleaseGroup:    model.NewNullString("GRP"),
			},
			Tags: map[string]struct{}{"z": {}, "a": {}},
		},
		Outcome: tape.RecordOutcome{Kind: tape.RecordCompleted},
	})
	require.NoError(t, err)

	require.Equal(t, []string{"stale:a", "stale:b"}, writeSet.DeleteIDs)
	require.Equal(t, []string{"a", "z"}, writeSet.AddTags[subject])
	require.Empty(t, writeSet.DeleteInfoHashes)
	require.Empty(t, writeSet.FailedInfoHashes)
	require.Len(t, writeSet.TorrentContents, 1)

	torrentContent := writeSet.TorrentContents[0]
	require.Equal(t, keepID, torrentContent.ID)
	require.Equal(t, []string{"de", "fr"}, torrentContent.Languages)
	require.NotNil(t, torrentContent.FilesCount)
	require.Equal(t, uint(1), *torrentContent.FilesCount)
	require.Equal(t, "movie", *torrentContent.ContentType)
	require.Equal(t, "V1080p", *torrentContent.VideoResolution)
	require.Equal(t, "BluRay", *torrentContent.VideoSource)
	require.Equal(t, "x264", *torrentContent.VideoCodec)
	require.Equal(t, "GRP", *torrentContent.ReleaseGroup)
}

func TestMaterializeTapeRerunRequiresProcessorStateForCompletedRecord(t *testing.T) {
	t.Parallel()

	const subject = "0123456789abcdef0123456789abcdef01234567"
	_, err := MaterializeTapeRerun(classifier.TapeReplayResult{
		Record:  tape.Record{Subject: subject},
		Torrent: model.Torrent{InfoHash: protocol.MustParseID(subject)},
		Outcome: tape.RecordOutcome{Kind: tape.RecordCompleted},
	})
	require.ErrorContains(t, err, "has no processorState")
}

func TestMaterializeTapeRerunMapsDeterministicTerminalOutcomes(t *testing.T) {
	t.Parallel()

	const subject = "0123456789abcdef0123456789abcdef01234567"
	for _, test := range []struct {
		name    string
		outcome tape.RecordOutcomeKind
		field   func(TapeRerunWriteSet) []string
	}{
		{
			name: "deleted", outcome: tape.RecordDeleted,
			field: func(writeSet TapeRerunWriteSet) []string { return writeSet.DeleteInfoHashes },
		},
		{
			name: "unmatched", outcome: tape.RecordUnmatched,
			field: func(writeSet TapeRerunWriteSet) []string { return writeSet.FailedInfoHashes },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			writeSet, err := MaterializeTapeRerun(classifier.TapeReplayResult{
				Record:  tape.Record{Subject: subject},
				Torrent: model.Torrent{InfoHash: protocol.MustParseID(subject)},
				Outcome: tape.RecordOutcome{Kind: test.outcome},
			})
			require.NoError(t, err)
			require.Equal(t, []string{subject}, test.field(writeSet))
		})
	}
}
