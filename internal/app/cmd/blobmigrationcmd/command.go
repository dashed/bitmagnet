package blobmigrationcmd

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration/consistency"
	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration/queue"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/urfave/cli/v2"
	"go.uber.org/fx"
	"gorm.io/gorm/clause"
)

const (
	kvKeyStatus     = "blob_migration:status"
	kvKeyMigrated   = "blob_migration:migrated_count"
	kvKeyCursor     = "blob_migration:cursor"
	kvKeyTotal      = "blob_migration:total_count"
	kvKeyStartedAt  = "blob_migration:started_at"
	kvKeyVerifiedAt = "blob_migration:verified_at"
)

type Params struct {
	fx.In
	Config blobmigration.Config
	Dao    lazy.Lazy[*dao.Query]
}

type Result struct {
	fx.Out
	Command *cli.Command `group:"commands"`
}

func New(p Params) (Result, error) {
	cmd := &cli.Command{
		Name:  "blob-migration",
		Usage: "Manage the torrent_files to blob migration",
		Subcommands: []*cli.Command{
			p.startCmd(),
			p.statusCmd(),
			p.pauseCmd(),
			p.resumeCmd(),
			p.verifyCmd(),
			p.cleanupCmd(),
		},
	}
	return Result{Command: cmd}, nil
}

func (p Params) startCmd() *cli.Command {
	return &cli.Command{
		Name:  "start",
		Usage: "Start the blob migration",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:  "batch-size",
				Value: int(p.Config.BatchSize),
				Usage: "number of torrents per migration batch",
			},
			&cli.BoolFlag{
				Name:  "resume",
				Usage: "resume from the last checkpoint instead of starting fresh",
			},
		},
		Action: func(ctx *cli.Context) error {
			d, err := p.Dao.Get()
			if err != nil {
				return err
			}

			status, _ := getKV(ctx, d, kvKeyStatus)
			if status == "running" {
				return fmt.Errorf("migration is already running; use 'status' to check progress or 'pause' to stop")
			}

			var cursor string
			if ctx.Bool("resume") {
				cursor, _ = getKV(ctx, d, kvKeyCursor)
				if cursor == "" {
					_, _ = fmt.Fprintln(ctx.App.Writer, "No checkpoint found, starting from the beginning.")
				} else {
					_, _ = fmt.Fprintf(ctx.App.Writer, "Resuming from cursor: %s\n", cursor)
				}
			}

			var totalCount int64
			if err := d.Torrent.UnderlyingDB().WithContext(ctx.Context).
				Table("torrent_files").
				Select("COUNT(DISTINCT info_hash)").
				Scan(&totalCount).Error; err != nil {
				return fmt.Errorf("counting torrents: %w", err)
			}

			now := time.Now()
			if err := upsertKV(ctx, d, kvKeyStatus, "running", now); err != nil {
				return err
			}
			if err := upsertKV(ctx, d, kvKeyTotal, strconv.FormatInt(totalCount, 10), now); err != nil {
				return err
			}
			if !ctx.Bool("resume") {
				if err := upsertKV(ctx, d, kvKeyStartedAt, now.Format(time.RFC3339), now); err != nil {
					return err
				}
				if err := upsertKV(ctx, d, kvKeyMigrated, "0", now); err != nil {
					return err
				}
			}

			job, err := queue.NewQueueJob(queue.MessageParams{
				InfoHashGreaterThan: cursor,
				BatchSize:           ctx.Int("batch-size"),
			})
			if err != nil {
				return fmt.Errorf("creating queue job: %w", err)
			}

			if err := d.QueueJob.WithContext(ctx.Context).Create(&job); err != nil {
				return fmt.Errorf("enqueuing job: %w", err)
			}

			_, _ = fmt.Fprintf(ctx.App.Writer, "Migration started. Total torrents with files: %d\n", totalCount)
			return nil
		},
	}
}

func (p Params) statusCmd() *cli.Command {
	return &cli.Command{
		Name:  "status",
		Usage: "Show migration progress",
		Action: func(ctx *cli.Context) error {
			d, err := p.Dao.Get()
			if err != nil {
				return err
			}

			kvs, err := d.KeyValue.WithContext(ctx.Context).
				Where(d.KeyValue.Key.Like("blob_migration:%")).
				Find()
			if err != nil {
				return fmt.Errorf("reading migration state: %w", err)
			}

			m := make(map[string]string, len(kvs))
			for _, kv := range kvs {
				m[kv.Key] = kv.Value
			}

			status := m[kvKeyStatus]
			if status == "" {
				_, _ = fmt.Fprintln(ctx.App.Writer, "No migration has been started.")
				return nil
			}

			tw := table.NewWriter()
			tw.SetOutputMirror(ctx.App.Writer)
			tw.AppendHeader(table.Row{"Property", "Value"})
			tw.AppendRow(table.Row{"Status", status})

			migrated, _ := strconv.ParseInt(m[kvKeyMigrated], 10, 64)
			total, _ := strconv.ParseInt(m[kvKeyTotal], 10, 64)
			tw.AppendRow(table.Row{"Migrated", migrated})
			tw.AppendRow(table.Row{"Total", total})

			if total > 0 {
				pct := float64(migrated) / float64(total) * 100
				tw.AppendRow(table.Row{"Progress", fmt.Sprintf("%.1f%%", pct)})
			}

			if cursor := m[kvKeyCursor]; cursor != "" {
				tw.AppendRow(table.Row{"Last Cursor", cursor})
			}

			if startedAt := m[kvKeyStartedAt]; startedAt != "" {
				t, parseErr := time.Parse(time.RFC3339, startedAt)
				if parseErr == nil {
					elapsed := time.Since(t).Truncate(time.Second)
					tw.AppendRow(table.Row{"Started At", startedAt})
					tw.AppendRow(table.Row{"Elapsed", elapsed.String()})
					if total > 0 && migrated > 0 && migrated < total {
						rate := float64(migrated) / elapsed.Seconds()
						remaining := float64(total-migrated) / rate
						eta := time.Duration(remaining) * time.Second
						tw.AppendRow(table.Row{"ETA", eta.Truncate(time.Second).String()})
					}
				}
			}

			if verifiedAt := m[kvKeyVerifiedAt]; verifiedAt != "" {
				tw.AppendRow(table.Row{"Verified At", verifiedAt})
			}

			tw.Render()
			return nil
		},
	}
}

func (p Params) pauseCmd() *cli.Command {
	return &cli.Command{
		Name:  "pause",
		Usage: "Pause the migration (running batches will finish, no new ones start)",
		Action: func(ctx *cli.Context) error {
			d, err := p.Dao.Get()
			if err != nil {
				return err
			}

			status, _ := getKV(ctx, d, kvKeyStatus)
			if status != "running" {
				return fmt.Errorf("migration is not running (current status: %s)", status)
			}

			if err := upsertKV(ctx, d, kvKeyStatus, "paused", time.Now()); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(ctx.App.Writer, "Migration paused. Current batch will finish, but no new batches will be queued.")
			return nil
		},
	}
}

func (p Params) resumeCmd() *cli.Command {
	return &cli.Command{
		Name:  "resume",
		Usage: "Resume a paused migration",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:  "batch-size",
				Value: int(p.Config.BatchSize),
				Usage: "number of torrents per migration batch",
			},
		},
		Action: func(ctx *cli.Context) error {
			d, err := p.Dao.Get()
			if err != nil {
				return err
			}

			status, _ := getKV(ctx, d, kvKeyStatus)
			if !strings.HasPrefix(status, "paused") {
				return fmt.Errorf("migration is not paused (current status: %s)", status)
			}

			cursor, _ := getKV(ctx, d, kvKeyCursor)

			if err := upsertKV(ctx, d, kvKeyStatus, "running", time.Now()); err != nil {
				return err
			}

			job, err := queue.NewQueueJob(queue.MessageParams{
				InfoHashGreaterThan: cursor,
				BatchSize:           ctx.Int("batch-size"),
			})
			if err != nil {
				return fmt.Errorf("creating queue job: %w", err)
			}

			if err := d.QueueJob.WithContext(ctx.Context).Create(&job); err != nil {
				return fmt.Errorf("enqueuing job: %w", err)
			}

			_, _ = fmt.Fprintf(ctx.App.Writer, "Migration resumed from cursor: %s\n", cursor)
			return nil
		},
	}
}

func (p Params) verifyCmd() *cli.Command {
	return &cli.Command{
		Name:  "verify",
		Usage: "Run a consistency check between blobs and torrent_files rows",
		Flags: []cli.Flag{
			&cli.Float64Flag{
				Name:  "sample-rate",
				Value: 0.1,
				Usage: "fraction of migrated torrents to sample (0.0-1.0)",
			},
			&cli.BoolFlag{
				Name:  "full",
				Usage: "verify all migrated torrents (overrides --sample-rate)",
			},
		},
		Action: func(ctx *cli.Context) error {
			d, err := p.Dao.Get()
			if err != nil {
				return err
			}

			var sampleSize int
			if ctx.Bool("full") {
				var count int64
				if err := d.Torrent.UnderlyingDB().WithContext(ctx.Context).
					Table("torrents").
					Where("files_data IS NOT NULL").
					Count(&count).Error; err != nil {
					return fmt.Errorf("counting migrated torrents: %w", err)
				}
				sampleSize = int(count)
				_, _ = fmt.Fprintf(ctx.App.Writer, "Running full verification on %d torrents...\n", sampleSize)
			} else {
				var count int64
				if err := d.Torrent.UnderlyingDB().WithContext(ctx.Context).
					Table("torrents").
					Where("files_data IS NOT NULL").
					Count(&count).Error; err != nil {
					return fmt.Errorf("counting migrated torrents: %w", err)
				}
				sampleSize = int(float64(count) * ctx.Float64("sample-rate"))
				if sampleSize < 1 {
					sampleSize = 1
				}
				_, _ = fmt.Fprintf(ctx.App.Writer, "Sampling %d of %d migrated torrents (%.0f%%)...\n",
					sampleSize, count, ctx.Float64("sample-rate")*100)
			}

			summary, err := consistency.CheckRandom(ctx.Context, d, sampleSize)
			if err != nil {
				return fmt.Errorf("verification failed: %w", err)
			}

			tw := table.NewWriter()
			tw.SetOutputMirror(ctx.App.Writer)
			tw.AppendHeader(table.Row{"Metric", "Value"})
			tw.AppendRow(table.Row{"Checked", summary.TotalChecked})
			tw.AppendRow(table.Row{"Matches", summary.Matches})
			tw.AppendRow(table.Row{"Mismatches", summary.Mismatches})
			tw.AppendRow(table.Row{"Errors", summary.Errors})
			tw.Render()

			if summary.Mismatches > 0 || summary.Errors > 0 {
				_, _ = fmt.Fprintln(ctx.App.Writer, "\nVerification FAILED.")
				for _, detail := range summary.MismatchDetails {
					_, _ = fmt.Fprintf(ctx.App.Writer, "  Mismatch: %s (blob=%d, rows=%d)\n",
						detail.InfoHash, detail.BlobFiles, detail.RowFiles)
				}
				return fmt.Errorf("verification found %d mismatches and %d errors", summary.Mismatches, summary.Errors)
			}

			now := time.Now()
			if err := upsertKV(ctx, d, kvKeyVerifiedAt, now.Format(time.RFC3339), now); err != nil {
				return err
			}

			_, _ = fmt.Fprintln(ctx.App.Writer, "\nVerification PASSED.")
			return nil
		},
	}
}

func (p Params) cleanupCmd() *cli.Command {
	return &cli.Command{
		Name:  "cleanup",
		Usage: "Drop the torrent_files table after migration is verified",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "confirm",
				Usage: "required flag to confirm destructive DROP TABLE operation",
			},
		},
		Action: func(ctx *cli.Context) error {
			d, err := p.Dao.Get()
			if err != nil {
				return err
			}

			gates, ok := p.checkCleanupGates(ctx, d)
			for _, g := range gates {
				status := "PASS"
				if !g.passed {
					status = "FAIL"
				}
				_, _ = fmt.Fprintf(ctx.App.Writer, "  [%s] %s\n", status, g.description)
			}

			if !ok {
				return fmt.Errorf("cleanup aborted: one or more safety gates failed")
			}

			_, _ = fmt.Fprintln(ctx.App.Writer, "\nAll safety gates passed. Dropping torrent_files table...")

			db := d.Torrent.UnderlyingDB().WithContext(ctx.Context)
			if err := db.Exec("DROP TABLE IF EXISTS torrent_files").Error; err != nil {
				return fmt.Errorf("DROP TABLE failed: %w", err)
			}

			_, _ = fmt.Fprintln(ctx.App.Writer, "torrent_files table dropped.")
			_, _ = fmt.Fprintln(ctx.App.Writer, "Running VACUUM...")

			if err := db.Exec("VACUUM").Error; err != nil {
				_, _ = fmt.Fprintf(ctx.App.Writer, "VACUUM failed (non-fatal): %v\n", err)
			} else {
				_, _ = fmt.Fprintln(ctx.App.Writer, "VACUUM complete.")
			}

			return nil
		},
	}
}

type gate struct {
	description string
	passed      bool
}

func (p Params) checkCleanupGates(ctx *cli.Context, d *dao.Query) ([]gate, bool) {
	var gates []gate
	allPassed := true

	fail := func(desc string) {
		gates = append(gates, gate{desc, false})
		allPassed = false
	}
	pass := func(desc string) {
		gates = append(gates, gate{desc, true})
	}

	// Gate 1: migration completed
	status, _ := getKV(ctx, d, kvKeyStatus)
	if status == "completed" {
		pass("Migration status is 'completed'")
	} else {
		fail(fmt.Sprintf("Migration status is '%s', expected 'completed'", status))
	}

	// Gate 2: no unmigrated torrents
	var unmigrated int64
	if err := d.Torrent.UnderlyingDB().WithContext(ctx.Context).
		Table("torrents").
		Where("files_data IS NULL AND files_status != ?", "no_info").
		Count(&unmigrated).Error; err != nil {
		fail(fmt.Sprintf("Failed to check unmigrated count: %v", err))
	} else if unmigrated > 0 {
		fail(fmt.Sprintf("%d torrents still have no blob data", unmigrated))
	} else {
		pass("All eligible torrents have blob data")
	}

	// Gate 3: verification passed recently
	verifiedAt, _ := getKV(ctx, d, kvKeyVerifiedAt)
	if verifiedAt == "" {
		fail("No verification timestamp found (run 'blob-migration verify' first)")
	} else {
		t, parseErr := time.Parse(time.RFC3339, verifiedAt)
		if parseErr != nil {
			fail(fmt.Sprintf("Invalid verification timestamp: %s", verifiedAt))
		} else if time.Since(t) > 24*time.Hour {
			fail(fmt.Sprintf("Verification is stale (%s ago); re-run 'blob-migration verify'", time.Since(t).Truncate(time.Minute)))
		} else {
			pass(fmt.Sprintf("Verification passed at %s", verifiedAt))
		}
	}

	// Gate 4: --confirm flag
	if ctx.Bool("confirm") {
		pass("--confirm flag provided")
	} else {
		fail("--confirm flag required for destructive operation")
	}

	return gates, allPassed
}

func getKV(ctx *cli.Context, d *dao.Query, key string) (string, error) {
	kv, err := d.KeyValue.WithContext(ctx.Context).
		Where(d.KeyValue.Key.Eq(key)).
		First()
	if err != nil {
		return "", err
	}
	return kv.Value, nil
}

func upsertKV(ctx *cli.Context, d *dao.Query, key, value string, now time.Time) error {
	kv := model.KeyValue{Key: key, Value: value, CreatedAt: now, UpdatedAt: now}
	return d.Torrent.UnderlyingDB().WithContext(ctx.Context).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "key"}},
			DoUpdates: clause.AssignmentColumns([]string{"value", "updated_at"}),
		}).
		Create(&kv).Error
}
