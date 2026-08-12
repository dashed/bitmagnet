package classifiercmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/urfave/cli/v2"
)

const tapeParitySchema = "bitmagnet.classifier-tape-parity/v1"

type tapeParityOptions struct {
	TapeDir      string
	OutputDir    string
	RustBinary   string
	SourceCommit string
	SourceTree   string
}

type tapeParityReceipt struct {
	Schema                string `json:"schema"`
	ReportSchema          string `json:"reportSchema"`
	SourceCommit          string `json:"sourceCommit"`
	SourceTree            string `json:"sourceTree"`
	EffectiveConfigDigest string `json:"effectiveConfigDigest"`
	AcquisitionPlanDigest string `json:"acquisitionPlanDigest,omitempty"`
	RecordCount           int    `json:"recordCount"`
	ManifestSHA256        string `json:"manifestSha256"`
	TapeSHA256            string `json:"tapeSha256"`
	ProvenanceSHA256      string `json:"provenanceSha256"`
	GoExecutableSHA256    string `json:"goExecutableSha256"`
	RustExecutableSHA256  string `json:"rustExecutableSha256"`
	GoReportSHA256        string `json:"goReportSha256"`
	RustReportSHA256      string `json:"rustReportSha256"`
	ReportsEqual          bool   `json:"reportsEqual"`
}

func newTapeParityCommand() *cli.Command {
	return &cli.Command{
		Name:  "tape-parity",
		Usage: "Replay one classifier tape through Go and Rust and publish an exact-match receipt",
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:     "dir",
				Usage:    "Directory holding PROVENANCE.md, manifest.json, and tape.jsonl",
				Required: true,
			},
			&cli.PathFlag{
				Name:     "output-dir",
				Usage:    "Create-only evidence directory",
				Required: true,
			},
			&cli.PathFlag{
				Name:  "rust-binary",
				Usage: "Path to the bitmagnet-tape-rerun executable",
				Value: "/usr/local/bin/bitmagnet-tape-rerun",
			},
			&cli.StringFlag{
				Name:  "source-commit",
				Usage: "Exact 40-character Git commit represented by both executables",
				Value: os.Getenv("BITMAGNET_SOURCE_COMMIT"),
			},
			&cli.StringFlag{
				Name:  "source-tree",
				Usage: "Exact 40-character Git tree represented by both executables",
				Value: os.Getenv("BITMAGNET_SOURCE_TREE"),
			},
		},
		Action: func(ctx *cli.Context) error {
			return runTapeParity(ctx.Context, tapeParityOptions{
				TapeDir:      ctx.Path("dir"),
				OutputDir:    ctx.Path("output-dir"),
				RustBinary:   ctx.Path("rust-binary"),
				SourceCommit: ctx.String("source-commit"),
				SourceTree:   ctx.String("source-tree"),
			})
		},
	}
}

func runTapeParity(ctx context.Context, options tapeParityOptions) error {
	if err := validateGitObjectID("source commit", options.SourceCommit); err != nil {
		return err
	}
	if err := validateGitObjectID("source tree", options.SourceTree); err != nil {
		return err
	}
	if options.RustBinary == "" {
		return fmt.Errorf("rust binary path is required")
	}
	rustBinary, err := exec.LookPath(options.RustBinary)
	if err != nil {
		return fmt.Errorf("resolve Rust tape rerun executable %q: %w", options.RustBinary, err)
	}
	goExecutable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve Go executable: %w", err)
	}

	manifestPath := filepath.Join(options.TapeDir, tape.ManifestFileName)
	tapePath := filepath.Join(options.TapeDir, tape.TapeFileName)
	provenancePath := filepath.Join(options.TapeDir, tape.ProvenanceFileName)
	manifestDigest, err := sha256File(manifestPath)
	if err != nil {
		return err
	}
	tapeDigest, err := sha256File(tapePath)
	if err != nil {
		return err
	}
	provenanceDigest, err := sha256File(provenancePath)
	if err != nil {
		return err
	}
	goExecutableDigest, err := sha256File(goExecutable)
	if err != nil {
		return err
	}
	rustExecutableDigest, err := sha256File(rustBinary)
	if err != nil {
		return err
	}

	if err = os.Mkdir(options.OutputDir, 0o750); err != nil {
		return fmt.Errorf("create parity evidence directory %q: %w", options.OutputDir, err)
	}

	goReportPath := filepath.Join(options.OutputDir, "go-report.json")
	rustReportPath := filepath.Join(options.OutputDir, "rust-report.json")
	receiptPath := filepath.Join(options.OutputDir, "receipt.json")

	report, err := replayClassifierTape(ctx, options.TapeDir)
	if err != nil {
		return fmt.Errorf("replay classifier tape through Go: %w", err)
	}
	if err = writeCreateOnlyJSON(goReportPath, report); err != nil {
		return fmt.Errorf("write Go tape rerun report: %w", err)
	}
	goReportDigest, err := sha256File(goReportPath)
	if err != nil {
		return err
	}

	command := exec.CommandContext(
		ctx,
		rustBinary,
		"--tape-dir", options.TapeDir,
		"--output", rustReportPath,
	)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err = command.Run(); err != nil {
		return fmt.Errorf("replay classifier tape through Rust: %w", err)
	}

	goReportDigestAfterRust, err := sha256File(goReportPath)
	if err != nil {
		return err
	}
	if goReportDigestAfterRust != goReportDigest {
		return fmt.Errorf(
			"Go tape rerun report changed while Rust ran: before %s, after %s",
			goReportDigest,
			goReportDigestAfterRust,
		)
	}
	rustReportDigest, err := sha256File(rustReportPath)
	if err != nil {
		return err
	}
	if goReportDigest != rustReportDigest {
		return fmt.Errorf(
			"Go and Rust tape rerun reports differ: Go %s, Rust %s",
			goReportDigest,
			rustReportDigest,
		)
	}
	for path, before := range map[string]string{
		manifestPath:   manifestDigest,
		tapePath:       tapeDigest,
		provenancePath: provenanceDigest,
		goExecutable:   goExecutableDigest,
		rustBinary:     rustExecutableDigest,
	} {
		after, hashErr := sha256File(path)
		if hashErr != nil {
			return hashErr
		}
		if after != before {
			return fmt.Errorf("parity input %q changed during replay: before %s, after %s", path, before, after)
		}
	}

	receipt := tapeParityReceipt{
		Schema:                tapeParitySchema,
		ReportSchema:          report.Schema,
		SourceCommit:          options.SourceCommit,
		SourceTree:            options.SourceTree,
		EffectiveConfigDigest: report.EffectiveConfigDigest,
		AcquisitionPlanDigest: report.AcquisitionPlanDigest,
		RecordCount:           report.RecordCount,
		ReportsEqual:          true,
		GoReportSHA256:        goReportDigest,
		RustReportSHA256:      rustReportDigest,
		ManifestSHA256:        manifestDigest,
		TapeSHA256:            tapeDigest,
		ProvenanceSHA256:      provenanceDigest,
		GoExecutableSHA256:    goExecutableDigest,
		RustExecutableSHA256:  rustExecutableDigest,
	}
	if err = writeCreateOnlyJSON(receiptPath, receipt); err != nil {
		return fmt.Errorf("publish tape parity receipt: %w", err)
	}

	return nil
}

func validateGitObjectID(name, value string) error {
	if len(value) != 40 || strings.ToLower(value) != value {
		return fmt.Errorf("%s must be exactly 40 lowercase hexadecimal characters", name)
	}
	if _, err := hex.DecodeString(value); err != nil {
		return fmt.Errorf("%s must be exactly 40 lowercase hexadecimal characters: %w", name, err)
	}

	return nil
}

func sha256File(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %q for SHA-256: %w", path, err)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash %q with SHA-256: %w", path, err)
	}

	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}
