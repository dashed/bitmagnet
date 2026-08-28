package classifier

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/stretchr/testify/require"
)

func TestGoReplaysAuthoritativeProductionTape(t *testing.T) {
	const (
		digest       = "sha256:95ffc278681f50fbcee2a3498e4388378ffe78156bc432d403d2acc3c2c809ae"
		recordCount  = 2_000
		observations = 715
	)

	dir := filepath.Join(
		"..", "..", "testdata", "parity", "classifier-attach", "prod-20260811",
	)
	replay, err := tape.Load(dir, digest)
	require.NoError(t, err)
	replayer, err := NewTapeReplayer(replay)
	require.NoError(t, err)

	records := replay.Subjects()
	sort.Slice(records, func(i, j int) bool {
		if records[i].Subject != records[j].Subject {
			return records[i].Subject < records[j].Subject
		}
		return records[i].Attempt < records[j].Attempt
	})
	require.Len(t, records, recordCount)

	consumed := 0
	for _, record := range records {
		result, err := replayer.Run(context.Background(), record)
		require.NoErrorf(t, err, "%s#%d", record.Subject, record.Attempt)
		consumed += len(result.Record.Observations)
	}
	require.Equal(t, observations, consumed)
}
