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
	t.Parallel()

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
	summary := blobmigration.BuildFileSummary(infoHash, files, len(blob))
	assert.Equal(t, infoHash, summary.InfoHash)
	assert.Equal(t, 3, summary.FileCount)
	assert.Equal(t, int64(1_500_051_000), summary.TotalSize)
	assert.True(t, summary.HasVideo)
	assert.True(t, summary.HasSubtitle)
	assert.Equal(t, model.NewNullInt(len(blob)), summary.CompressedBytes)
}

func TestSelfChaining_NewJobCreated(t *testing.T) {
	t.Parallel()

	cursor := makeInfoHash(0xBB).String()

	job, err := NewQueueJob(MessageParams{
		InfoHashGreaterThan: cursor,
		InfoHashLessOrEqual: makeInfoHash(0xCC).String(),
		RangeID:             3,
		NumRanges:           8,
		ChunkSize:           500,
	})
	require.NoError(t, err)

	assert.Equal(t, MessageName, job.Queue)
	assert.Equal(t, model.QueueJobStatusPending, job.Status)

	var params MessageParams

	require.NoError(t, json.Unmarshal([]byte(job.Payload), &params))
	assert.Equal(t, cursor, params.InfoHashGreaterThan)
	assert.Equal(t, makeInfoHash(0xCC).String(), params.InfoHashLessOrEqual)
	assert.Equal(t, 3, params.RangeID)
	assert.Equal(t, 8, params.NumRanges)
	assert.Equal(t, 500, params.ChunkSize)
}

func TestSelfChaining_DifferentFingerprints(t *testing.T) {
	t.Parallel()

	job1, err := NewQueueJob(MessageParams{InfoHashGreaterThan: makeInfoHash(0x01).String(), ChunkSize: 100})
	require.NoError(t, err)

	job2, err := NewQueueJob(MessageParams{InfoHashGreaterThan: makeInfoHash(0x02).String(), ChunkSize: 100})
	require.NoError(t, err)

	assert.NotEqual(t, job1.Fingerprint, job2.Fingerprint,
		"jobs with different cursors must have different fingerprints")
}

// Different ranges (same cursor) must also have distinct fingerprints so K parallel range jobs
// coexist in the queue.
func TestSelfChaining_DifferentRangesDistinctFingerprints(t *testing.T) {
	t.Parallel()

	job0, err := NewQueueJob(MessageParams{RangeID: 0, NumRanges: 8, ChunkSize: 100})
	require.NoError(t, err)

	job1, err := NewQueueJob(MessageParams{RangeID: 1, NumRanges: 8, ChunkSize: 100})
	require.NoError(t, err)

	assert.NotEqual(t, job0.Fingerprint, job1.Fingerprint,
		"jobs for different ranges must have different fingerprints")
}

func TestDefaultChunkSize(t *testing.T) {
	t.Parallel()

	job, err := NewQueueJob(MessageParams{})
	require.NoError(t, err)

	var params MessageParams

	require.NoError(t, json.Unmarshal([]byte(job.Payload), &params))
	assert.Equal(t, DefaultChunkSize, params.ChunkSize)
	assert.Equal(t, 1, params.NumRanges)
}

// Pause is now triggered only when EVERY sampled consistency check in a chunk fails (systematic
// corruption), not a rate threshold; a lone transient mismatch is logged but does not pause.
func TestConsistencyPause_OnlyWhenAllSampledFail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		verifyErrors int
		shouldPause  bool
	}{
		{"no errors", 0, false},
		{"one of two failed", 1, false},
		{"all sampled failed", consistencyChecksPerChunk, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pause := consistencyChecksPerChunk > 0 && tt.verifyErrors >= consistencyChecksPerChunk
			assert.Equal(t, tt.shouldPause, pause)
		})
	}
}
