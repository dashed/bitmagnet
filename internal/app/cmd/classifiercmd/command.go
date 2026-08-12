package classifiercmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/urfave/cli/v2"
	"go.uber.org/fx"
	"gopkg.in/yaml.v3"
)

type Params struct {
	fx.In
	WorkflowSource lazy.Lazy[classifier.Source]
	Config         classifier.Config
}

type Result struct {
	fx.Out
	Command *cli.Command `group:"commands"`
}

var formatFlag = cli.StringFlag{
	Name:  "format",
	Usage: "Output format (json or yaml)",
	Value: "yaml",
}

func New(p Params) (Result, error) {
	return Result{Command: &cli.Command{
		Name: "classifier",
		Subcommands: []*cli.Command{
			{
				Name:  "show",
				Usage: "Show the classifier workflow source",
				Flags: []cli.Flag{
					&formatFlag,
				},
				Action: func(ctx *cli.Context) error {
					src, srcErr := p.WorkflowSource.Get()
					if srcErr != nil {
						return srcErr
					}
					return write(ctx.App.Writer, src, ctx.String("format"))
				},
			},
			{
				Name:  "digest",
				Usage: "Show the effective classifier configuration digest",
				Action: func(ctx *cli.Context) error {
					src, srcErr := p.WorkflowSource.Get()
					if srcErr != nil {
						return srcErr
					}
					digest, digestErr := classifier.EffectiveConfigDigest(src, p.Config.Workflow)
					if digestErr != nil {
						return digestErr
					}
					_, writeErr := fmt.Fprintln(ctx.App.Writer, digest)

					return writeErr
				},
			},
			{
				Name:  "schema",
				Usage: "Show the classifier JSON schema",
				Flags: []cli.Flag{
					&formatFlag,
				},
				Action: func(ctx *cli.Context) error {
					return write(
						ctx.App.Writer,
						classifier.DefaultJSONSchema(),
						ctx.String("format"),
					)
				},
			},
			{
				Name:  "tape-rerun",
				Usage: "Replay a traced classifier tape through Go and emit canonical processor write sets",
				Flags: []cli.Flag{
					&cli.PathFlag{
						Name:     "dir",
						Usage:    "Directory holding manifest.json and tape.jsonl",
						Required: true,
					},
					&cli.PathFlag{
						Name:     "output",
						Usage:    "Create-only JSON report path",
						Required: true,
					},
				},
				Action: func(ctx *cli.Context) error {
					digest, digestErr := classifier.CoreEffectiveConfigDigest()
					if digestErr != nil {
						return digestErr
					}
					replay, loadErr := tape.Load(ctx.Path("dir"), digest)
					if loadErr != nil {
						return loadErr
					}
					report, rerunErr := processor.ReplayClassifierTape(ctx.Context, replay)
					if rerunErr != nil {
						return rerunErr
					}
					return writeCreateOnlyJSON(ctx.Path("output"), report)
				},
			},
		},
	}}, nil
}

func writeCreateOnlyJSON(path string, value any) (err error) {
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}()

	encoded, err := tape.Marshal(value)
	if err != nil {
		return err
	}
	if _, err = temp.Write(append(encoded, '\n')); err != nil {
		return err
	}
	if err = temp.Sync(); err != nil {
		return err
	}
	if err = temp.Close(); err != nil {
		return err
	}
	// A hard link publishes the fully-written inode without overwriting an
	// existing evidence artifact. Source and destination share a directory, so
	// they are necessarily on the same filesystem.
	if err = os.Link(tempPath, path); err != nil {
		return fmt.Errorf("publish create-only JSON report %q: %w", path, err)
	}

	return nil
}

func write(writer io.Writer, src any, format string) error {
	var (
		output    []byte
		outputErr error
	)

	switch format {
	case "json":
		output, outputErr = json.MarshalIndent(src, "", "  ")
		output = append(output, '\n')
	case "yaml":
		output, outputErr = yaml.Marshal(src)
	default:
		outputErr = fmt.Errorf("unsupported format: %s", format)
	}

	if outputErr != nil {
		return outputErr
	}

	_, writeErr := writer.Write(output)

	return writeErr
}
