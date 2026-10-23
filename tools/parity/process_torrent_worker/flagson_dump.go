package main

// flagson-dump: everything `dump` prints, then raw row images of the tables a
// flags-on job writes. A raw section renders each row as ROW(...)::text, which
// keeps SQL NULL (an empty field) apart from '' ("") and from JSON null; it
// never goes through to_jsonb. Excluded columns, and why:
//
//   - created_at, updated_at, deleted_torrents.deleted_at: wall-clock write
//     timestamps (now() in Go, transaction time in Rust); Go's UpdateAll also
//     rewrites updated_at.
//   - queue_jobs.id: a random UUID per job (Go republishes failed hashes as
//     new jobs).
//   - queue_jobs.run_after, created_at: Go's random retry jitter and the
//     harness's retry collapsing; queue_jobs.ran_at: wall clock (rendered as
//     whether it is set, as in `dump`).
//
// Canonical fields. Both sections read queue_jobs through a temporary view
// (canonicalQueueJobs) that puts in canonical form only what Go alone, run
// more than once over the same corpus, varied (G-1); -raw-out writes the dump
// without it, and flagson-rawdiff names the field behind each difference
// between two raw dumps. None of them can hide a difference in the multiset
// of hashes or of error lines:
//
//   - payload InfoHashes of a job the processor queued (not the seed):
//     after the leading hashes that have no torrent row, sorted. Go appends
//     failed hashes to failedHashes from each torrent's goroutine as it
//     finishes (processor.go Process), so a job with two or more failures
//     republishes them in completion order. Missing hashes lead, in the
//     job's order, and stay put.
//   - fingerprint of such a job: sha256 of queue and payload
//     (model.NewQueueJob), so it follows the order above. It is recomputed
//     from the canonical payload only after checking that the stored
//     fingerprint is processor.NewQueueJob's for the stored payload; a row
//     that fails the check keeps its fingerprint, prefixed "unverified:".
//   - error: errors.Join(errs) under the handler's "job failed to process: "
//     prefix; errs gets each failure in completion order after the "missing
//     N info hashes" line, so the lines after that one are sorted.
//   - error: the client's ephemeral port in a reset's "read tcp
//     127.0.0.1:<port>->...". Go's net.OpError text names both ends.
//
// Evidence, G-1 of 2026-10-23 (m2a-runs/g1a on the parity workspace), as
// flagson-rawdiff classifies the raw dumps:
//
//   - fault corpus, Go alone three times: run 1 vs 2, 7 rows differ in
//     InfoHashes order (and fingerprint) and 6 in error line order; run 1 vs
//     3, 4 and 6; nothing else. The canonical dumps are identical.
//   - abstain corpus, Go alone twice: 4 rows differ in the error's client
//     port; nothing else. The canonical dumps are identical.

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
)

var flagsOnRawTables = []struct {
	table   string
	exclude []string
	replace map[string]string
}{
	{table: "content", exclude: []string{"created_at", "updated_at"}},
	{table: "content_attributes", exclude: []string{"created_at", "updated_at"}},
	{table: "content_collections", exclude: []string{"created_at", "updated_at"}},
	{table: "content_collections_content"},
	{table: "torrent_contents", exclude: []string{"created_at", "updated_at"}},
	{table: "torrent_tags", exclude: []string{"created_at", "updated_at"}},
	{table: "torrent_hints", exclude: []string{"created_at", "updated_at"}},
	{table: "deleted_torrents", exclude: []string{"deleted_at"}},
	{
		table:   "queue_jobs",
		exclude: []string{"id", "run_after", "created_at"},
		replace: map[string]string{"ran_at": "(t.ran_at IS NOT NULL)"},
	},
}

// canonicalQueueJobsView is the temporary view canonicalQueueJobs creates.
const canonicalQueueJobsView = "flagson_queue_jobs"

func runFlagsOnDump(args []string) error {
	fs := flag.NewFlagSet("flagson-dump", flag.ExitOnError)
	dsn := fs.String("dsn", "", "PostgreSQL DSN")
	seedJobs := fs.String("seed-jobs", "", "the corpus's jobs.jsonl: the seed's own jobs are never canonicalized")
	rawOut := fs.String("raw-out", "", "also write the dump without the canonical fields here")

	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if *rawOut != "" {
		f, err := os.Create(*rawOut)
		if err != nil {
			return err
		}

		if err := dumpAll(ctx, conn, f, "queue_jobs"); err != nil {
			_ = f.Close()
			return err
		}

		if err := f.Close(); err != nil {
			return err
		}
	}

	seeded := map[string]bool{}

	if *seedJobs != "" {
		jobs, err := readJSONL[plannedJob](*seedJobs, 0)
		if err != nil {
			return err
		}

		for _, j := range jobs {
			seeded[j.Fingerprint] = true
		}
	}

	stats, err := canonicalQueueJobs(ctx, conn, seeded)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(os.Stderr, "canonical queue_jobs: %s\n", stats)

	return dumpAll(ctx, conn, os.Stdout, canonicalQueueJobsView)
}

func dumpAll(ctx context.Context, conn *pgx.Conn, w io.Writer, queueJobs string) error {
	if err := dumpFrom(ctx, conn, w, queueJobs); err != nil {
		return err
	}

	return dumpRaw(ctx, conn, w, queueJobs)
}

// dumpRaw prints the raw sections, reading queue_jobs from queueJobs.
func dumpRaw(ctx context.Context, conn *pgx.Conn, w io.Writer, queueJobs string) error {
	for _, t := range flagsOnRawTables {
		columns, err := tableColumns(ctx, conn, t.table)
		if err != nil {
			return err
		}

		excluded := map[string]bool{}
		for _, c := range t.exclude {
			excluded[c] = true
		}

		var exprs []string

		for _, c := range columns {
			switch {
			case excluded[c]:
			case t.replace[c] != "":
				exprs = append(exprs, t.replace[c])
			default:
				exprs = append(exprs, "t."+pgx.Identifier{c}.Sanitize())
			}
		}

		from := pgx.Identifier{t.table}.Sanitize()
		if t.table == "queue_jobs" {
			from = pgx.Identifier{queueJobs}.Sanitize()
		}

		label := "raw:" + t.table + "(" + strings.Join(exprs, ",") + ")"
		sql := fmt.Sprintf(`SELECT line FROM (SELECT ROW(%s)::text AS line FROM %s t) s ORDER BY line COLLATE "C"`,
			strings.Join(exprs, ", "), from)

		if _, err := fmt.Fprintf(w, "%s\n", label); err != nil {
			return err
		}

		rows, err := conn.Query(ctx, sql)
		if err != nil {
			return fmt.Errorf("%s: %w", t.table, err)
		}

		n := 0

		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				rows.Close()
				return err
			}

			if _, err := fmt.Fprintf(w, "raw:%s\t%s\n", t.table, strings.ReplaceAll(line, "\n", `\n`)); err != nil {
				rows.Close()
				return err
			}

			n++
		}

		rows.Close()

		if err := rows.Err(); err != nil {
			return err
		}

		_, _ = fmt.Fprintf(os.Stderr, "raw:%s: %d rows\n", t.table, n)
	}

	return nil
}

func tableColumns(ctx context.Context, conn *pgx.Conn, table string) ([]string, error) {
	rows, err := conn.Query(ctx, `SELECT column_name::text FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, err
	}

	columns, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s not found", table)
	}

	return columns, nil
}
