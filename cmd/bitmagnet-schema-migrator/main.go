package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bitmagnet-io/bitmagnet/internal/schemamigrator"
	"github.com/bitmagnet-io/bitmagnet/internal/version"
	"go.uber.org/zap"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	app := schemamigrator.NewApp(schemamigrator.Params{
		BuildInfo: schemamigrator.BuildInfo{
			Version:      version.GitTag,
			SourceCommit: version.SourceCommit,
			SourceTree:   version.SourceTree,
		},
		Getenv:    os.Getenv,
		Open:      schemamigrator.OpenPostgresSession(logger.Sugar()),
		Writer:    os.Stdout,
		ErrWriter: os.Stderr,
	})
	if err := app.RunContext(ctx, os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
