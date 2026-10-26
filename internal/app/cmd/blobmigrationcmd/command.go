package blobmigrationcmd

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration/bytesfill"
	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration/consistency"
	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration/extfix"
	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration/queue"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/urfave/cli/v2"
	"go.uber.org/fx"
	"gorm.io/gorm/clause"
)

const (
	kvKeyStatus     = "blob_migration:status"
	kvKeyTotal      = "blob_migration:total_count"
	kvKeyStartedAt  = "blob_migration:started_at"
	kvKeyVerifiedAt = "blob_migration:verified_at"
	// Per-range checkpoint, done-flag, and migrated-counter key prefixes (suffixed with the range id).
	// migrated is per-range (summed for status) to avoid single-row contention at high concurrency.
	kvKeyCursorPrefix   = "blob_migration:cursor:"
	kvKeyRangePrefix    = "blob_migration:range:"
	kvKeyMigratedPrefix = "blob_migration:migrated:"

	statusRunning   = "running"
	statusPaused    = "paused"
	statusCompleted = "completed"
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
			p.backfillExtCmd(),
			p.backfillBytesCmd(),
			p.cleanupCmd(),
		},
	}

	return Result{Command: cmd}, nil
}

func (p Params) startCmd() *cli.Command {
	return &cli.Command{
		Name:  "start",
		Usage: "Start the blob migration (parallel info_hash-range workers)",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:    "chunk-size",
				Aliases: []string{"batch-size"},
				Value:   int(p.Config.ChunkSize),
				Usage:   "torrents (distinct info_hashes) processed per chunk",
			},
			&cli.BoolFlag{
				Name:  "resume",
				Usage: "resume from per-range checkpoints instead of starting fresh",
			},
		},
		Action: func(ctx *cli.Context) error {
			d, err := p.Dao.Get()
			if err != nil {
				return err
			}

			status, _ := getKV(ctx, d, kvKeyStatus)
			if status == statusRunning {
				return fmt.Errorf(
					"migration is already running; use 'status' to check progress or 'pause' to stop",
				)
			}

			resume := ctx.Bool("resume")

			chunkSize := ctx.Int("chunk-size")
			if chunkSize <= 0 {
				chunkSize = int(p.Config.ChunkSize)
			}

			now := time.Now()

			if !resume {
				// Fresh start: clear any leftover jobs + per-range checkpoints/done-flags so the new
				// K-way seeding is the only state, then reset counters.
				if err := d.Torrent.UnderlyingDB().WithContext(ctx.Context).
					Exec("DELETE FROM queue_jobs WHERE queue = ?", queue.MessageName).Error; err != nil {
					return fmt.Errorf("clearing old jobs: %w", err)
				}

				if err := d.Torrent.UnderlyingDB().WithContext(ctx.Context).
					Exec("DELETE FROM key_values WHERE key LIKE ? OR key LIKE ? OR key LIKE ?",
						kvKeyCursorPrefix+"%", kvKeyRangePrefix+"%", kvKeyMigratedPrefix+"%").Error; err != nil {
					return fmt.Errorf("clearing range state: %w", err)
				}

				var totalCount int64
				if err := d.Torrent.UnderlyingDB().WithContext(ctx.Context).
					Table("torrent_files").
					Select("COUNT(DISTINCT info_hash)").
					Scan(&totalCount).Error; err != nil {
					return fmt.Errorf("counting torrents: %w", err)
				}

				for k, v := range map[string]string{
					kvKeyStatus:    statusRunning,
					kvKeyTotal:     strconv.FormatInt(totalCount, 10),
					kvKeyStartedAt: now.Format(time.RFC3339),
				} {
					if err := upsertKV(ctx, d, k, v, now); err != nil {
						return err
					}
				}
			} else if err := upsertKV(ctx, d, kvKeyStatus, statusRunning, now); err != nil {
				return err
			}

			seeded, err := p.seedRanges(ctx, d, chunkSize, resume)
			if err != nil {
				return fmt.Errorf("seeding range jobs: %w", err)
			}

			total, _ := getKV(ctx, d, kvKeyTotal)
			verb := "started"
			if resume {
				verb = "resumed"
			}

			_, _ = fmt.Fprintf(
				ctx.App.Writer,
				"Migration %s with %d parallel range workers (chunk-size %d). Total torrents with files: %s\n",
				verb,
				seeded,
				chunkSize,
				total,
			)

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

			// Sum the per-range migrated counters + count active/done ranges.
			var migrated int64

			rangesDone, rangesActive := 0, 0

			for k, v := range m {
				switch {
				case strings.HasPrefix(k, kvKeyMigratedPrefix):
					n, _ := strconv.ParseInt(v, 10, 64)
					migrated += n

					rangesActive++
				case strings.HasPrefix(k, kvKeyRangePrefix) && v == "done":
					rangesDone++
				}
			}

			total, _ := strconv.ParseInt(m[kvKeyTotal], 10, 64)
			tw.AppendRow(table.Row{"Migrated", migrated})
			tw.AppendRow(table.Row{"Total", total})

			if total > 0 {
				pct := float64(migrated) / float64(total) * 100
				tw.AppendRow(table.Row{"Progress", fmt.Sprintf("%.1f%%", pct)})
			}

			tw.AppendRow(table.Row{
				"Ranges",
				fmt.Sprintf("%d done / %d total", rangesDone, rangesActive+rangesDone),
			})

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
			if status != statusRunning {
				return fmt.Errorf("migration is not running (current status: %s)", status)
			}

			if err := upsertKV(ctx, d, kvKeyStatus, statusPaused, time.Now()); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(
				ctx.App.Writer,
				"Migration paused. Current batch will finish, but no new batches will be queued.",
			)
			return nil
		},
	}
}

func (p Params) resumeCmd() *cli.Command {
	return &cli.Command{
		Name:  "resume",
		Usage: "Resume a paused migration from per-range checkpoints",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:    "chunk-size",
				Aliases: []string{"batch-size"},
				Value:   int(p.Config.ChunkSize),
				Usage:   "torrents (distinct info_hashes) processed per chunk",
			},
		},
		Action: func(ctx *cli.Context) error {
			d, err := p.Dao.Get()
			if err != nil {
				return err
			}

			status, _ := getKV(ctx, d, kvKeyStatus)
			if !strings.HasPrefix(status, statusPaused) {
				return fmt.Errorf("migration is not paused (current status: %s)", status)
			}

			chunkSize := ctx.Int("chunk-size")
			if chunkSize <= 0 {
				chunkSize = int(p.Config.ChunkSize)
			}

			if err := upsertKV(ctx, d, kvKeyStatus, statusRunning, time.Now()); err != nil {
				return err
			}

			seeded, err := p.seedRanges(ctx, d, chunkSize, true)
			if err != nil {
				return fmt.Errorf("seeding range jobs: %w", err)
			}

			_, _ = fmt.Fprintf(
				ctx.App.Writer,
				"Migration resumed: %d range workers re-seeded from checkpoints (chunk-size %d).\n",
				seeded,
				chunkSize,
			)

			return nil
		},
	}
}

// numRanges is K — the number of parallel info_hash-range workers (= handler Concurrency).
func (p Params) numRanges() int {
	k := int(p.Config.Parallelism)
	if k < 1 {
		k = queue.DefaultConcurrency
	}

	return k
}

// computeRanges partitions the 20-byte info_hash space into k disjoint, gap-free (lower, upper]
// ranges by the leading byte. Bounds are hex (parsed to raw bytea by the handler). Range 0 has no
// lower bound (covers the smallest hashes); the last range has no upper bound (covers the largest).
func computeRanges(k int) []queue.MessageParams {
	if k < 1 {
		k = 1
	}

	step := 256 / k
	if step < 1 {
		step = 1
	}

	ranges := make([]queue.MessageParams, 0, k)

	for i := range k {
		var lower, upper string

		if i > 0 {
			var b protocol.ID
			b[0] = byte(i * step)
			lower = b.String()
		}

		if i < k-1 {
			var b protocol.ID
			b[0] = byte((i + 1) * step)
			upper = b.String()
		}

		ranges = append(ranges, queue.MessageParams{
			InfoHashGreaterThan: lower,
			InfoHashLessOrEqual: upper,
			RangeID:             i,
			NumRanges:           k,
		})
	}

	return ranges
}

// seedRanges enqueues one job per range. On resume it skips done ranges and starts each from its
// per-range checkpoint cursor.
func (p Params) seedRanges(ctx *cli.Context, d *dao.Query, chunkSize int, resume bool) (int, error) {
	seeded := 0

	for _, r := range computeRanges(p.numRanges()) {
		r.ChunkSize = chunkSize

		if resume {
			if done, _ := getKV(ctx, d, fmt.Sprintf("%s%d", kvKeyRangePrefix, r.RangeID)); done == "done" {
				continue
			}

			if cur, _ := getKV(ctx, d, fmt.Sprintf("%s%d", kvKeyCursorPrefix, r.RangeID)); cur != "" {
				r.InfoHashGreaterThan = cur
			}
		}

		job, err := queue.NewQueueJob(r)
		if err != nil {
			return seeded, err
		}

		if err := d.QueueJob.WithContext(ctx.Context).
			Clauses(clause.OnConflict{DoNothing: true}).Create(&job); err != nil {
			return seeded, err
		}

		seeded++
	}

	return seeded, nil
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
			&cli.BoolFlag{
				Name: "strict-e",
				Usage: "G1 gate: assert the RAW stored blob `e` == path-derived extension " +
					"(model.FileExtensionFromPath); reads only files_data, torrent_files-independent",
			},
			&cli.BoolFlag{
				Name: "file-index-set",
				Usage: "read-only crawl-path gate: compare torrent_files to the legacy duplicate-path " +
					"projection of decoded files_data; requires --full",
			},
		},
		Action: func(ctx *cli.Context) error {
			d, err := p.Dao.Get()
			if err != nil {
				return err
			}

			parallelism := int(p.Config.Parallelism)
			if parallelism < 1 {
				parallelism = queue.DefaultConcurrency
			}

			chunkSize := int(p.Config.ChunkSize)
			if chunkSize < 1 {
				chunkSize = queue.DefaultChunkSize
			}

			strictE := ctx.Bool("strict-e")
			fileIndexSet := ctx.Bool("file-index-set")
			if strictE && fileIndexSet {
				return fmt.Errorf("--strict-e and --file-index-set are mutually exclusive")
			}

			if fileIndexSet && !ctx.Bool("full") {
				return fmt.Errorf(
					"--file-index-set requires --full; sampled checks cannot prove set equality",
				)
			}

			// sampleSize 0 = full. Parallel streaming (no ORDER BY RANDOM / join / per-torrent reads).
			sampleSize := 0
			if ctx.Bool("full") {
				_, _ = fmt.Fprintf(
					ctx.App.Writer,
					"Running full parallel verification (%d workers, chunk %d)...\n",
					parallelism, chunkSize,
				)
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

				_, _ = fmt.Fprintf(
					ctx.App.Writer,
					"Sampling %d of %d migrated torrents (%.0f%%, %d workers)...\n",
					sampleSize, count, ctx.Float64("sample-rate")*100, parallelism,
				)
			}

			check := consistency.CheckAll
			if strictE {
				check = consistency.CheckAllStrictE

				_, _ = fmt.Fprintln(
					ctx.App.Writer,
					"strict-e mode: asserting raw blob `e` == path-derived extension (torrent_files-independent).",
				)
			}

			var summary consistency.Summary
			if fileIndexSet {
				_, _ = fmt.Fprintln(
					ctx.App.Writer,
					"file-index-set mode: read-only full parity for (info_hash, file_index), "+
						"modulo legacy duplicate-path collapse.",
				)

				summary, err = consistency.CheckAllFileIndexSet(ctx.Context, d, parallelism, chunkSize)
			} else {
				if ctx.Bool("full") && !strictE {
					_, _ = fmt.Fprintln(
						ctx.App.Writer,
						"full mode: allowing only legacy duplicate-path collapse under "+
							"PRIMARY KEY (info_hash, path).",
					)
				}

				summary, err = check(ctx.Context, d, parallelism, chunkSize, sampleSize)
			}
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
			if fileIndexSet || summary.LegacyDuplicatePathTorrents > 0 ||
				summary.LegacyDuplicatePathFiles > 0 {
				tw.AppendRow(
					table.Row{
						"Legacy duplicate-path torrents",
						summary.LegacyDuplicatePathTorrents,
					},
				)
				tw.AppendRow(table.Row{"Legacy duplicate-path files", summary.LegacyDuplicatePathFiles})
			}
			tw.Render()

			if summary.Mismatches > 0 || summary.Errors > 0 {
				_, _ = fmt.Fprintln(ctx.App.Writer, "\nVerification FAILED.")
				for _, detail := range summary.MismatchDetails {
					_, _ = fmt.Fprintf(ctx.App.Writer, "  Mismatch: %s (blob=%d, rows=%d)\n",
						detail.InfoHash, detail.BlobFiles, detail.RowFiles)
				}
				return fmt.Errorf(
					"verification found %d mismatches and %d errors",
					summary.Mismatches,
					summary.Errors,
				)
			}

			if fileIndexSet {
				_, _ = fmt.Fprintln(
					ctx.App.Writer,
					"\nVerification PASSED (read-only; timestamp not updated).",
				)
				return nil
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

func (p Params) backfillExtCmd() *cli.Command {
	return &cli.Command{
		Name: "backfill-ext",
		Usage: "G1: re-canonicalize the extension (`e`) stored in every files_data blob " +
			"(derive from path); rewrites only files_data, torrent_files-independent",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "dry-run",
				Usage: "scan + report fixable/skipped counts WITHOUT writing any blob",
			},
			&cli.IntFlag{
				Name:    "chunk-size",
				Aliases: []string{"batch-size"},
				Value:   int(p.Config.ChunkSize),
				Usage:   "torrents processed (and committed) per chunk",
			},
			&cli.IntFlag{
				Name:  "parallelism",
				Value: int(p.Config.Parallelism),
				Usage: "parallel info_hash-range workers (K=16 sustainable; K=32 crashed PG under WAL load)",
			},
			&cli.IntFlag{
				Name:  "limit",
				Value: 0,
				Usage: "cap total blobs scanned (bounded write smoke before the full run); 0 = everything",
			},
		},
		Action: func(ctx *cli.Context) error {
			d, err := p.Dao.Get()
			if err != nil {
				return err
			}

			parallelism := ctx.Int("parallelism")
			if parallelism < 1 {
				parallelism = int(p.Config.Parallelism)
			}

			if parallelism < 1 {
				parallelism = queue.DefaultConcurrency
			}

			chunkSize := ctx.Int("chunk-size")
			if chunkSize < 1 {
				chunkSize = int(p.Config.ChunkSize)
			}

			if chunkSize < 1 {
				chunkSize = queue.DefaultChunkSize
			}

			dryRun := ctx.Bool("dry-run")
			limit := ctx.Int("limit")

			mode := "rewriting"
			if dryRun {
				mode = "dry-run (no writes)"
			}

			limitMsg := "all"
			if limit > 0 {
				limitMsg = fmt.Sprintf("≤%d", limit)
			}

			_, _ = fmt.Fprintf(
				ctx.App.Writer,
				"G1 e-backfill: %s, %d workers, chunk %d, scanning %s blobs...\n",
				mode, parallelism, chunkSize, limitMsg,
			)

			// Throttled progress: log roughly every 250k scanned blobs. progress is
			// invoked under the aggregator mutex, so the closure's lastLogged is safe.
			var lastLogged int64

			progress := func(r extfix.Report) {
				if r.Scanned-lastLogged >= 250_000 {
					lastLogged = r.Scanned
					_, _ = fmt.Fprintf(
						ctx.App.Writer,
						"  progress: scanned=%d fixed=%d skipped=%d errors=%d\n",
						r.Scanned, r.Fixed, r.Skipped, r.Errors,
					)
				}
			}

			rep, err := extfix.BackfillExtensions(
				ctx.Context,
				d,
				parallelism,
				chunkSize,
				limit,
				dryRun,
				progress,
			)
			if err != nil {
				return fmt.Errorf("e-backfill failed: %w", err)
			}

			tw := table.NewWriter()
			tw.SetOutputMirror(ctx.App.Writer)
			tw.AppendHeader(table.Row{"Metric", "Value"})
			tw.AppendRow(table.Row{"Scanned", rep.Scanned})

			if dryRun {
				tw.AppendRow(table.Row{"Fixable (would rewrite)", rep.Fixed})
			} else {
				tw.AppendRow(table.Row{"Fixed (rewritten)", rep.Fixed})
			}

			tw.AppendRow(table.Row{"Skipped (already canonical)", rep.Skipped})
			tw.AppendRow(table.Row{"Errors", rep.Errors})
			tw.Render()

			if rep.Errors > 0 {
				return fmt.Errorf("e-backfill completed with %d blob errors", rep.Errors)
			}

			return nil
		},
	}
}

func (p Params) backfillBytesCmd() *cli.Command {
	return &cli.Command{
		Name: "backfill-bytes",
		Usage: "00026: fill torrent_file_summary.compressed_bytes = octet_length(files_data) " +
			"for rows written before the column existed; set-based UPDATE, torrents-heap-light",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "dry-run",
				Usage: "scan + report how many summary rows WOULD be filled WITHOUT writing",
			},
			&cli.IntFlag{
				Name:    "chunk-size",
				Aliases: []string{"batch-size"},
				Value:   bytesfill.DefaultChunkSize,
				Usage:   "summary rows filled (and committed) per chunk",
			},
			&cli.IntFlag{
				Name:  "parallelism",
				Value: int(p.Config.Parallelism),
				Usage: "parallel info_hash-range workers (K=16 sustainable; K=32 crashed PG under WAL load)",
			},
			&cli.IntFlag{
				Name:  "limit",
				Value: 0,
				Usage: "cap total summary rows scanned (bounded smoke before the full run); 0 = everything",
			},
		},
		Action: func(ctx *cli.Context) error {
			d, err := p.Dao.Get()
			if err != nil {
				return err
			}

			parallelism := ctx.Int("parallelism")
			if parallelism < 1 {
				parallelism = int(p.Config.Parallelism)
			}

			if parallelism < 1 {
				parallelism = queue.DefaultConcurrency
			}

			chunkSize := ctx.Int("chunk-size")
			if chunkSize < 1 {
				chunkSize = bytesfill.DefaultChunkSize
			}

			dryRun := ctx.Bool("dry-run")
			limit := ctx.Int("limit")

			mode := "filling"
			if dryRun {
				mode = "dry-run (no writes)"
			}

			limitMsg := "all"
			if limit > 0 {
				limitMsg = fmt.Sprintf("≤%d", limit)
			}

			_, _ = fmt.Fprintf(
				ctx.App.Writer,
				"00026 compressed_bytes backfill: %s, %d workers, chunk %d, scanning %s rows...\n",
				mode, parallelism, chunkSize, limitMsg,
			)

			// Throttled progress: log roughly every 500k scanned rows. progress is
			// invoked under the aggregator mutex, so the closure's lastLogged is safe.
			var lastLogged int64

			progress := func(r bytesfill.Report) {
				if r.Scanned-lastLogged >= 500_000 {
					lastLogged = r.Scanned
					_, _ = fmt.Fprintf(
						ctx.App.Writer,
						"  progress: scanned=%d updated=%d\n",
						r.Scanned, r.Updated,
					)
				}
			}

			rep, err := bytesfill.BackfillCompressedBytes(
				ctx.Context,
				d,
				parallelism,
				chunkSize,
				limit,
				dryRun,
				progress,
			)
			if err != nil {
				return fmt.Errorf("compressed_bytes backfill failed: %w", err)
			}

			tw := table.NewWriter()
			tw.SetOutputMirror(ctx.App.Writer)
			tw.AppendHeader(table.Row{"Metric", "Value"})
			tw.AppendRow(table.Row{"Scanned (compressed_bytes was NULL)", rep.Scanned})

			if dryRun {
				tw.AppendRow(table.Row{"Would fill", rep.Updated})
			} else {
				tw.AppendRow(table.Row{"Filled (rows affected)", rep.Updated})
			}

			tw.Render()

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

			_, _ = fmt.Fprintln(
				ctx.App.Writer,
				"\nAll safety gates passed. Dropping torrent_files table...",
			)

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

func (Params) checkCleanupGates(ctx *cli.Context, d *dao.Query) ([]gate, bool) {
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
	if status == statusCompleted {
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

		switch {
		case parseErr != nil:
			fail(fmt.Sprintf("Invalid verification timestamp: %s", verifiedAt))
		case time.Since(t) > 24*time.Hour:
			staleFor := time.Since(t).Truncate(time.Minute)
			fail(fmt.Sprintf("Verification is stale (%s ago); re-run 'blob-migration verify'", staleFor))
		default:
			pass(fmt.Sprintf("Verification passed at %s", verifiedAt))
		}
	}

	// Gate 4: runtime read path is DROP-compatible.
	if desc, ok := cleanupRuntimeReadGate(); ok {
		pass(desc)
	} else {
		fail(desc)
	}

	// Gate 5: --confirm flag
	if ctx.Bool("confirm") {
		pass("--confirm flag provided")
	} else {
		fail("--confirm flag required for destructive operation")
	}

	return gates, allPassed
}

func cleanupRuntimeReadGate() (string, bool) {
	return cleanupRuntimeReadGateForFlags(search.FeatureFlagsValue())
}

func cleanupRuntimeReadGateForFlags(flags search.FeatureFlags) (string, bool) {
	if !flags.DropCompatibleReads {
		return "SEARCH_FEATURES_DROP_COMPATIBLE_READS is false; " +
			"active runtime has not proven no-legacy-read mode", false
	}

	var missing []string
	if !flags.UseFileBrowserFromBlob() {
		missing = append(missing, "blob browser")
	}

	if !flags.UseFileExtensionsJSONB() {
		missing = append(missing, "JSONB extension filter")
	}

	if flags.AllowTorrentFilesRepair() {
		missing = append(missing, "blob-only repair")
	}

	if len(missing) > 0 {
		return "SEARCH_FEATURES_DROP_COMPATIBLE_READS is true, but effective " +
			"no-legacy-read gates are not forced: " + strings.Join(missing, ", "), false
	}

	return "SEARCH_FEATURES_DROP_COMPATIBLE_READS is true; blob browser, " +
		"JSONB extension filter, and blob-only repair gates are forced", true
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
		Table("key_values").
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "key"}},
			DoUpdates: clause.AssignmentColumns([]string{"value", "updated_at"}),
		}).
		Create(&kv).Error
}
