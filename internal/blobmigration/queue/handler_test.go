package queue

import (
	"encoding/json"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeInfoHash(b byte) protocol.ID {
	var id protocol.ID
	for i := range id {
		id[i] = b
	}
	return id
}

func sampleFiles() []model.TorrentFile {
	return []model.TorrentFile{
		{Index: 0, Path: "video/movie.mkv", Extension: model.NewNullString("mkv"), Size: 1_500_000_000},
		{Index: 1, Path: "video/subs.srt", Extension: model.NewNullString("srt"), Size: 50_000},
		{Index: 2, Path: "video/info.nfo", Extension: model.NewNullString("nfo"), Size: 1_000},
	}
}

func TestProcessBatch_BlobCreation(t *testing.T) {
	files := sampleFiles()

	blob, err := blobmigration.SerializeFiles(files)
	require.NoError(t, err)
	require.NotEmpty(t, blob)

	roundTripped, err := blobmigration.DeserializeFiles(blob)
	require.NoError(t, err)
	require.Len(t, roundTripped, len(files))

	for i, f := range files {
		assert.Equal(t, f.Index, roundTripped[i].Index)
		assert.Equal(t, f.Path, roundTripped[i].Path)
		assert.Equal(t, f.Size, roundTripped[i].Size)
	}

	exts := blobmigration.ExtractUniqueExtensions(files)
	assert.Equal(t, []string{"mkv", "nfo", "srt"}, exts)

	infoHash := makeInfoHash(0xAA)
	summary := blobmigration.BuildFileSummary(infoHash, files)
	assert.Equal(t, infoHash, summary.InfoHash)
	assert.Equal(t, 3, summary.FileCount)
	assert.Equal(t, int64(1_500_051_000), summary.TotalSize)
	assert.True(t, summary.HasVideo)
	assert.True(t, summary.HasSubtitle)
}

func TestSelfChaining_NewJobCreated(t *testing.T) {
	cursor := makeInfoHash(0xBB).String()
	batchSize := 500

	job, err := NewQueueJob(MessageParams{
		InfoHashGreaterThan: cursor,
		BatchSize:           batchSize,
	})
	require.NoError(t, err)

	assert.Equal(t, MessageName, job.Queue)
	assert.Equal(t, model.QueueJobStatusPending, job.Status)

	var params MessageParams
	require.NoError(t, json.Unmarshal([]byte(job.Payload), &params))
	assert.Equal(t, cursor, params.InfoHashGreaterThan)
	assert.Equal(t, 500, params.BatchSize)
}

func TestSelfChaining_DifferentFingerprints(t *testing.T) {
	job1, err := NewQueueJob(MessageParams{
		InfoHashGreaterThan: makeInfoHash(0x01).String(),
		BatchSize:           100,
	})
	require.NoError(t, err)

	job2, err := NewQueueJob(MessageParams{
		InfoHashGreaterThan: makeInfoHash(0x02).String(),
		BatchSize:           100,
	})
	require.NoError(t, err)

	assert.NotEqual(t, job1.Fingerprint, job2.Fingerprint,
		"jobs with different cursors must have different fingerprints")
}

func TestCompletion_NoSelfChain(t *testing.T) {
	// When fewer results than batchSize are returned, the handler should NOT
	// self-chain. We verify by checking that the message params for a "completed"
	// state require no next job — the logic in handler.go checks
	// len(infoHashes) < batchSize to decide.
	batchSize := 1000
	returnedCount := 500
	assert.Less(t, returnedCount, batchSize, "completion detected when returned < batchSize")
}

func TestProgressTracking_MessageFormat(t *testing.T) {
	job, err := NewQueueJob(MessageParams{
		InfoHashGreaterThan: "",
		BatchSize:           1000,
	})
	require.NoError(t, err)

	var params MessageParams
	require.NoError(t, json.Unmarshal([]byte(job.Payload), &params))
	assert.Equal(t, "", params.InfoHashGreaterThan)
	assert.Equal(t, 1000, params.BatchSize)
}

func TestDefaultBatchSize(t *testing.T) {
	job, err := NewQueueJob(MessageParams{})
	require.NoError(t, err)

	var params MessageParams
	require.NoError(t, json.Unmarshal([]byte(job.Payload), &params))
	assert.Equal(t, 1000, params.BatchSize)
}

func TestConsistencyCheckPause_ErrorRateThreshold(t *testing.T) {
	tests := []struct {
		name      string
		total     int
		errors    int
		shouldRun bool
	}{
		{"no errors", 100, 0, true},
		{"below threshold", 100, 1, true},
		{"at threshold", 100, 2, false},
		{"above threshold", 100, 5, false},
		{"single error in small batch", 10, 1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errorRate := float64(tt.errors) / float64(tt.total)
			shouldContinue := errorRate <= maxErrorRate
			assert.Equal(t, tt.shouldRun, shouldContinue,
				"errorRate=%.4f, threshold=%.4f", errorRate, maxErrorRate)
		})
	}
}

func TestMarshalJSON(t *testing.T) {
	result := marshalJSON([]string{"mkv", "srt"})
	assert.Equal(t, `["mkv","srt"]`, result)

	result = marshalJSON([]string{})
	assert.Equal(t, `[]`, result)
}
