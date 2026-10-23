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

func runFlagsOnDump(args []string) error {
	fs := flag.NewFlagSet("flagson-dump", flag.ExitOnError)
	dsn := fs.String("dsn", "", "PostgreSQL DSN")

	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if err := dump(ctx, conn, os.Stdout); err != nil {
		return err
	}

	return dumpRaw(ctx, conn, os.Stdout)
}

func dumpRaw(ctx context.Context, conn *pgx.Conn, w io.Writer) error {
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

		label := "raw:" + t.table + "(" + strings.Join(exprs, ",") + ")"
		sql := fmt.Sprintf(`SELECT line FROM (SELECT ROW(%s)::text AS line FROM %s t) s ORDER BY line COLLATE "C"`,
			strings.Join(exprs, ", "), pgx.Identifier{t.table}.Sanitize())

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
