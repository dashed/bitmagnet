package manager

import (
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/stretchr/testify/require"
)

func TestNewReprocessTorrentsBatchJobPreservesGraphQLOptions(t *testing.T) {
	updatedBefore := time.Date(2026, time.August, 27, 20, 15, 16, 123456789, time.UTC)
	job, err := newReprocessTorrentsBatchJob(EnqueueReprocessTorrentsBatchRequest{
		BatchSize:           5,
		ChunkSize:           25,
		Orphans:             true,
		ClassifyMode:        processor.ClassifyModeRematch,
		ClassifierWorkflow:  "custom",
		ApisDisabled:        true,
		LocalSearchDisabled: true,
	}, updatedBefore)
	require.NoError(t, err)
	require.Equal(t, "process_torrent_batch", job.Queue)
	require.Equal(t, uint(2), job.MaxRetries)
	require.JSONEq(t,
		`{"InfoHashGreaterThan":"0000000000000000000000000000000000000000",`+
			`"UpdatedBefore":"2026-08-27T20:15:16.123456789Z",`+
			`"ClassifyMode":1,"ClassifierWorkflow":"custom",`+
			`"ClassifierFlags":{"apis_enabled":false,"local_search_enabled":false},`+
			`"ChunkSize":25,"BatchSize":5,"Orphans":true}`,
		job.Payload,
	)
}
