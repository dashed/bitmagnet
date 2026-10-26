package classifiercmd

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/stretchr/testify/require"
)

const (
	testSourceCommit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testSourceTree   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestRunTapeParityPublishesExactReceipt(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "evidence")
	rustBinary := writeFakeRustRerunner(t, `cp "$(dirname "$output")/go-report.json" "$output"`)
	options := tapeParityOptions{
		TapeDir:      tapeParityFixtureDir(t),
		OutputDir:    outputDir,
		RustBinary:   rustBinary,
		SourceCommit: testSourceCommit,
		SourceTree:   testSourceTree,
	}

	require.NoError(t, runTapeParity(context.Background(), options))

	goReportPath := filepath.Join(outputDir, "go-report.json")
	rustReportPath := filepath.Join(outputDir, "rust-report.json")
	receiptPath := filepath.Join(outputDir, "receipt.json")
	goReport, err := os.ReadFile(goReportPath)
	require.NoError(t, err)
	rustReport, err := os.ReadFile(rustReportPath)
	require.NoError(t, err)
	require.Equal(t, goReport, rustReport)

	rawReceipt, err := os.ReadFile(receiptPath)
	require.NoError(t, err)
	var receipt tapeParityReceipt
	require.NoError(t, json.Unmarshal(rawReceipt, &receipt))
	require.Equal(t, tapeParitySchema, receipt.Schema)
	require.Equal(t, "bitmagnet.classifier-tape-rerun/v2", receipt.ReportSchema)
	require.Equal(t, testSourceCommit, receipt.SourceCommit)
	require.Equal(t, testSourceTree, receipt.SourceTree)
	require.Equal(t, 9, receipt.RecordCount)
	require.Equal(
		t,
		"sha256:95ffc278681f50fbcee2a3498e4388378ffe78156bc432d403d2acc3c2c809ae",
		receipt.EffectiveConfigDigest,
	)
	require.Equal(
		t,
		"sha256:c6febd6d4dbcc762050d5a4d38d401dc0d56f50f901b88fc252a382a83b455fe",
		receipt.AcquisitionPlanDigest,
	)
	require.True(t, receipt.ReportsEqual)

	goReportDigest, err := sha256File(goReportPath)
	require.NoError(t, err)
	rustReportDigest, err := sha256File(rustReportPath)
	require.NoError(t, err)
	require.Equal(t, goReportDigest, receipt.GoReportSHA256)
	require.Equal(t, rustReportDigest, receipt.RustReportSHA256)
	require.Equal(t, receipt.GoReportSHA256, receipt.RustReportSHA256)

	executable, err := os.Executable()
	require.NoError(t, err)
	assertReceiptFileDigest(t, receipt.ManifestSHA256, filepath.Join(options.TapeDir, tape.ManifestFileName))
	assertReceiptFileDigest(t, receipt.TapeSHA256, filepath.Join(options.TapeDir, tape.TapeFileName))
	assertReceiptFileDigest(t, receipt.ProvenanceSHA256, filepath.Join(options.TapeDir, tape.ProvenanceFileName))
	assertReceiptFileDigest(t, receipt.GoExecutableSHA256, executable)
	assertReceiptFileDigest(t, receipt.RustExecutableSHA256, rustBinary)
}

func TestRunTapeParityMismatchDoesNotPublishReceipt(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "evidence")
	rustBinary := writeFakeRustRerunner(t, `printf '%s\n' '{"different":true}' > "$output"`)

	err := runTapeParity(context.Background(), tapeParityOptions{
		TapeDir:      tapeParityFixtureDir(t),
		OutputDir:    outputDir,
		RustBinary:   rustBinary,
		SourceCommit: testSourceCommit,
		SourceTree:   testSourceTree,
	})

	require.ErrorContains(t, err, "Go and Rust tape rerun reports differ")
	require.FileExists(t, filepath.Join(outputDir, "go-report.json"))
	require.FileExists(t, filepath.Join(outputDir, "rust-report.json"))
	require.NoFileExists(t, filepath.Join(outputDir, "receipt.json"))
}

func TestRunTapeParityRejectsRustMutationOfGoReport(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "evidence")
	rustBinary := writeFakeRustRerunner(t, `
printf '%s\n' '{"replaced":true}' > "$(dirname "$output")/go-report.json"
cp "$(dirname "$output")/go-report.json" "$output"
`)

	err := runTapeParity(context.Background(), tapeParityOptions{
		TapeDir:      tapeParityFixtureDir(t),
		OutputDir:    outputDir,
		RustBinary:   rustBinary,
		SourceCommit: testSourceCommit,
		SourceTree:   testSourceTree,
	})

	require.ErrorContains(t, err, "Go tape rerun report changed while Rust ran")
	require.NoFileExists(t, filepath.Join(outputDir, "receipt.json"))
}

func TestRunTapeParityRejectsInvalidProvenance(t *testing.T) {
	valid := tapeParityOptions{
		TapeDir:      tapeParityFixtureDir(t),
		RustBinary:   writeFakeRustRerunner(t, `cp "$(dirname "$output")/go-report.json" "$output"`),
		SourceCommit: testSourceCommit,
		SourceTree:   testSourceTree,
	}

	for name, mutate := range map[string]func(*tapeParityOptions){
		"short source commit": func(options *tapeParityOptions) {
			options.SourceCommit = "abc123"
		},
		"uppercase source tree": func(options *tapeParityOptions) {
			options.SourceTree = strings.ToUpper(testSourceTree)
		},
		"non-hex source tree": func(options *tapeParityOptions) {
			options.SourceTree = strings.Repeat("z", 40)
		},
	} {
		t.Run(name, func(t *testing.T) {
			options := valid
			options.OutputDir = filepath.Join(t.TempDir(), "evidence")
			mutate(&options)

			err := runTapeParity(context.Background(), options)
			require.ErrorContains(t, err, "must be exactly 40 lowercase hexadecimal characters")
			require.NoDirExists(t, options.OutputDir)
		})
	}

	t.Run("missing provenance document", func(t *testing.T) {
		tapeDir := copyTapeFixtureWithoutProvenance(t)
		options := valid
		options.TapeDir = tapeDir
		options.OutputDir = filepath.Join(t.TempDir(), "evidence")

		err := runTapeParity(context.Background(), options)
		require.ErrorContains(t, err, tape.ProvenanceFileName)
		require.NoFileExists(t, filepath.Join(options.OutputDir, "receipt.json"))
	})
}

func TestRunTapeParityOutputDirectoryIsCreateOnly(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "evidence")
	options := tapeParityOptions{
		TapeDir:      tapeParityFixtureDir(t),
		OutputDir:    outputDir,
		RustBinary:   writeFakeRustRerunner(t, `cp "$(dirname "$output")/go-report.json" "$output"`),
		SourceCommit: testSourceCommit,
		SourceTree:   testSourceTree,
	}
	require.NoError(t, runTapeParity(context.Background(), options))

	receiptPath := filepath.Join(outputDir, "receipt.json")
	original, err := os.ReadFile(receiptPath)
	require.NoError(t, err)

	err = runTapeParity(context.Background(), options)
	require.ErrorContains(t, err, "create parity evidence directory")
	after, readErr := os.ReadFile(receiptPath)
	require.NoError(t, readErr)
	require.Equal(t, original, after)
}

func tapeParityFixtureDir(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Clean(filepath.Join(
		filepath.Dir(source),
		"..", "..", "..", "..",
		"testdata", "parity", "classifier-tape-rerun", "example", "tape",
	))
}

func writeFakeRustRerunner(t *testing.T, action string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bitmagnet-tape-rerun")
	script := `#!/bin/sh
set -eu
output=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --tape-dir) shift 2 ;;
    --output) output="$2"; shift 2 ;;
    *) exit 64 ;;
  esac
done
test -n "$output"
` + action + "\n"
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755))

	return path
}

func assertReceiptFileDigest(t *testing.T, actual, path string) {
	t.Helper()
	expected, err := sha256File(path)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func copyTapeFixtureWithoutProvenance(t *testing.T) string {
	t.Helper()
	source := tapeParityFixtureDir(t)
	destination := t.TempDir()
	for _, name := range []string{tape.ManifestFileName, tape.TapeFileName} {
		raw, err := os.ReadFile(filepath.Join(source, name))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(destination, name), raw, 0o644))
	}

	return destination
}
