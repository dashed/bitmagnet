package processor

import (
	"bytes"
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/stretchr/testify/require"
)

var updateTapeRerunGoReport = flag.Bool(
	"update-tape-rerun-go-report",
	false,
	"regenerate testdata/parity/classifier-tape-rerun/example/go-report.json",
)

const (
	tapeRerunCrossLanguageDir = "../../testdata/parity/classifier-tape-rerun/example"
	tapeRerunGoReportName     = "go-report.json"
)

// TestTapeRerunGoReportGolden is the Go-owned half of the cross-language gate.
// Rust executes the same traced tape and must emit this report byte-for-byte.
func TestTapeRerunGoReportGolden(t *testing.T) {
	digest, err := classifier.CoreEffectiveConfigDigest()
	require.NoError(t, err)
	replay, err := tape.Load(filepath.Join(tapeRerunCrossLanguageDir, "tape"), digest)
	require.NoError(t, err)
	report, err := ReplayClassifierTape(context.Background(), replay)
	require.NoError(t, err)

	encoded, err := tape.Marshal(report)
	require.NoError(t, err)
	encoded = append(encoded, '\n')
	path := filepath.Join(tapeRerunCrossLanguageDir, tapeRerunGoReportName)
	if *updateTapeRerunGoReport {
		require.NoError(t, os.WriteFile(path, encoded, 0o644))
		return
	}

	expected, err := os.ReadFile(path)
	require.NoError(t, err, "regenerate with -update-tape-rerun-go-report")
	require.True(t, bytes.Equal(expected, encoded), "Go rerun report changed; regenerate deliberately")
}
