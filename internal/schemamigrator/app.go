// Package schemamigrator implements the bounded production schema-migration
// CLI. It deliberately does not assemble the full application Fx graph.
package schemamigrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/database/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

const (
	UpVersion   int64 = 33
	DownVersion int64 = 29

	versionSchema = "bitmagnet.schema-migrator-version/v1"
	postgresDSN   = "POSTGRES_DSN"
)

// BuildInfo is injected into the binary at build time and emitted by the
// version command so deployment receipts can bind execution to exact source.
type BuildInfo struct {
	Version      string `json:"version"`
	SourceCommit string `json:"sourceCommit"`
	SourceTree   string `json:"sourceTree"`
}

type versionReport struct {
	Schema string `json:"schema"`
	BuildInfo
}

// Migrator is intentionally narrower than migrations.Migrator: the production
// binary has no way to call the unbounded Up or single-step Down methods.
type Migrator interface {
	UpTo(context.Context, int64) error
	DownTo(context.Context, int64) error
}

// Session owns a migrator and its single database connection.
type Session struct {
	Migrator Migrator
	Close    func() error
}

// OpenSession is called only after CLI arguments and the exact target version
// have passed validation.
type OpenSession func(context.Context, string) (Session, error)

// Params contains the small, injectable boundary needed by the one-shot CLI.
type Params struct {
	BuildInfo BuildInfo
	Getenv    func(string) string
	Open      OpenSession
	Writer    io.Writer
	ErrWriter io.Writer
}

// NewApp creates a CLI with only exact-target migration and provenance
// reporting surfaces.
func NewApp(p Params) *cli.App {
	app := &cli.App{
		Name:                 "bitmagnet-schema-migrator",
		Usage:                "apply one bounded Bitmagnet Goose migration transition",
		HideVersion:          true,
		EnableBashCompletion: false,
		Writer:               p.Writer,
		ErrWriter:            p.ErrWriter,
		Commands: []*cli.Command{
			migrationCommand(p),
			{
				Name:  "version",
				Usage: "print exact build provenance as JSON",
				Action: func(*cli.Context) error {
					return writeVersion(p.Writer, p.BuildInfo)
				},
			},
		},
	}
	app.Setup()
	return app
}

func migrationCommand(p Params) *cli.Command {
	return &cli.Command{
		Name:  "migrate",
		Usage: "run one explicitly bounded Goose migration transition",
		Subcommands: []*cli.Command{
			migrationDirection(p, "up", fmt.Sprint(UpVersion)),
			migrationDirection(p, "down", fmt.Sprint(DownVersion)),
		},
	}
}

func migrationDirection(p Params, direction, requiredVersion string) *cli.Command {
	return &cli.Command{
		Name: direction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "version",
				Usage:    "required exact migration target",
				Required: true,
			},
		},
		Action: func(ctx *cli.Context) error {
			if ctx.NArg() != 0 {
				return fmt.Errorf("migrate %s accepts no positional arguments", direction)
			}
			version := ctx.String("version")
			if version != requiredVersion {
				return fmt.Errorf(
					"migrate %s requires --version %s; got %q",
					direction,
					requiredVersion,
					version,
				)
			}

			dsn := strings.TrimSpace(p.Getenv(postgresDSN))
			if dsn == "" {
				return errors.New("POSTGRES_DSN is required")
			}
			session, err := p.Open(ctx.Context, dsn)
			if err != nil {
				return fmt.Errorf("open migration session: %w", err)
			}
			if session.Close == nil {
				return errors.New("open migration session returned an incomplete session")
			}
			if session.Migrator == nil {
				return errors.Join(
					errors.New("open migration session returned an incomplete session"),
					session.Close(),
				)
			}

			var migrateErr error
			switch direction {
			case "up":
				migrateErr = session.Migrator.UpTo(ctx.Context, UpVersion)
			case "down":
				migrateErr = session.Migrator.DownTo(ctx.Context, DownVersion)
			default:
				migrateErr = fmt.Errorf("unsupported migration direction %q", direction)
			}
			return errors.Join(migrateErr, session.Close())
		},
	}
}

func writeVersion(w io.Writer, build BuildInfo) error {
	return json.NewEncoder(w).Encode(versionReport{
		Schema:    versionSchema,
		BuildInfo: build,
	})
}

// OpenPostgresSession opens one PostgreSQL connection and constructs the same
// embedded-Goose migrator used by the development migrate command.
func OpenPostgresSession(logger *zap.SugaredLogger) OpenSession {
	return func(ctx context.Context, dsn string) (Session, error) {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return Session{}, err
		}
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		if err := db.PingContext(ctx); err != nil {
			db.Close()
			return Session{}, err
		}
		return Session{
			Migrator: migrations.NewForSQLDB(db, logger),
			Close:    db.Close,
		}, nil
	}
}
