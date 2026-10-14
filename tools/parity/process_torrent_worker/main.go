// Command process_torrent_worker seeds and dumps a disposable database for the
// Go-vs-Rust `process_torrent` worker differential.
//
// The differential seeds one database, clones it with `CREATE DATABASE ...
// TEMPLATE`, drains the same queued jobs with the Go queue server on one copy
// and the Rust `bitmagnet-process-torrent-worker` on the other, then diffs the
// two `dump` outputs. See run.sh next to this file.
//
// Every job is built with Go's own processor.NewQueueJob, so payload bytes and
// fingerprints are the Go producer's. Torrent file lists are written with Go's
// own blob serializer. Inputs come from the committed classifier corpora:
// testdata/parity/classifier/inputs.jsonl (330 synthetic cases, including file
// lists, explicit hints, and deletions) and the real-name replay corpus
// testdata/parity/classifier-replay/inputs.jsonl.
package main

import (
	"bufio"
	"context"
	"crypto/sha1" //nolint:gosec // deterministic fixture key, not security
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/jackc/pgx/v5"
)

const fixtureTime = "2024-01-02T03:04:05.123456Z"

type input struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Size        uint64 `json:"size"`
	FilesStatus string `json:"filesStatus"`
	FilesCount  *int   `json:"filesCount"`
	Files       []struct {
		Index uint   `json:"index"`
		Path  string `json:"path"`
		Size  uint   `json:"size"`
	} `json:"files"`
	Hint *struct {
		ContentType   string `json:"contentType"`
		ContentSource string `json:"contentSource"`
		ContentID     string `json:"contentId"`
	} `json:"hint"`
}

type golden struct {
	ID       string `json:"id"`
	Expected struct {
		Outcome string `json:"outcome"`
	} `json:"expected"`
}

type seeded struct {
	hash    protocol.ID
	sourced bool
	deleted bool
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return errors.New("usage: process_torrent_worker seed|dump [flags]")
	}

	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	dsn := fs.String("dsn", "", "disposable PostgreSQL DSN")
	root := fs.String("root", ".", "repository root")
	replayLimit := fs.Int("replay-limit", 0, "real-name replay inputs to seed (0 = none)")
	jobSize := fs.Int("job-size", 100, "info hashes per job (Go's DHT classifyBatchSize is 100)")

	if err := fs.Parse(os.Args[2:]); err != nil {
		return err
	}

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	switch os.Args[1] {
	case "seed":
		return seed(ctx, conn, *root, *replayLimit, *jobSize)
	case "dump":
		return dump(ctx, conn, os.Stdout)
	default:
		return fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
}

func readJSONL[T any](path string, limit int) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []T

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<24)

	for scanner.Scan() {
		if limit > 0 && len(out) >= limit {
			break
		}

		var v T
		if err := json.Unmarshal(scanner.Bytes(), &v); err != nil {
			return nil, err
		}

		out = append(out, v)
	}

	return out, scanner.Err()
}

func fixtureHash(corpus, id string) protocol.ID {
	sum := sha1.Sum([]byte("process-torrent-worker-parity/" + corpus + "/" + id)) //nolint:gosec

	return protocol.ID(sum)
}

func seed(ctx context.Context, conn *pgx.Conn, root string, replayLimit, jobSize int) error {
	synthetic, err := readJSONL[input](filepath.Join(root, "testdata/parity/classifier/inputs.jsonl"), 0)
	if err != nil {
		return err
	}

	goldens, err := readJSONL[golden](filepath.Join(root, "testdata/parity/classifier/corpus.golden.jsonl"), 0)
	if err != nil {
		return err
	}

	deleted := make(map[string]bool, len(goldens))
	for _, g := range goldens {
		deleted[g.ID] = g.Expected.Outcome == "deleted"
	}

	var replay []input
	if replayLimit > 0 {
		if replay, err = readJSONL[input](
			filepath.Join(root, "testdata/parity/classifier-replay/inputs.jsonl"), replayLimit,
		); err != nil {
			return err
		}
	}

	if _, err := conn.Exec(ctx, `INSERT INTO metadata_sources (key, name, created_at, updated_at)
		VALUES ('local', 'Local', now(), now()) ON CONFLICT DO NOTHING`); err != nil {
		return err
	}

	ts, _ := time.Parse(time.RFC3339Nano, fixtureTime)

	var all []seeded

	batch := &pgx.Batch{}
	flush := func() error {
		if batch.Len() == 0 {
			return nil
		}

		err := conn.SendBatch(ctx, batch).Close()
		batch = &pgx.Batch{}

		return err
	}

	add := func(corpus string, in input) error {
		hash := fixtureHash(corpus, in.ID)

		var filesData []byte

		if len(in.Files) > 0 {
			files := make([]model.TorrentFile, len(in.Files))
			for i, f := range in.Files {
				files[i] = model.TorrentFile{Index: f.Index, Path: f.Path, Size: f.Size}
			}

			if filesData, err = blobmigration.SerializeFiles(files); err != nil {
				return err
			}
		}

		batch.Queue(`INSERT INTO torrents
			(info_hash, name, size, private, files_status, files_count, files_data, created_at, updated_at)
			VALUES ($1, $2, $3, false, $4::"FilesStatus", $5, $6, $7, $7)`,
			hash[:], in.Name, in.Size, in.FilesStatus, in.FilesCount, filesData, ts)
		// Deterministic, varied swarm counts; every third row also has a
		// publication date so the projection's published_at branch is covered.
		var publishedAt *time.Time
		if hash[2]%3 == 0 {
			p := ts.Add(-time.Duration(hash[3]) * time.Hour)
			publishedAt = &p
		}

		batch.Queue(`INSERT INTO torrents_torrent_sources
			(source, info_hash, seeders, leechers, published_at, created_at, updated_at)
			VALUES ('dht', $1, $2, $3, $4, $5, $5)`,
			hash[:], int(hash[0])%50, int(hash[1])%20, publishedAt, ts)

		sourced := false

		if in.Hint != nil {
			sourced = in.Hint.ContentSource != ""

			var source, id *string
			if sourced {
				source, id = &in.Hint.ContentSource, &in.Hint.ContentID
			}

			batch.Queue(`INSERT INTO torrent_hints
				(info_hash, content_type, content_source, content_id, created_at, updated_at)
				VALUES ($1, $2, $3, $4, $5, $5)`,
				hash[:], in.Hint.ContentType, source, id, ts)
		}

		all = append(all, seeded{hash: hash, sourced: sourced, deleted: corpus == "synthetic" && deleted[in.ID]})

		if batch.Len() >= 1500 {
			return flush()
		}

		return nil
	}

	for _, in := range synthetic {
		if err := add("synthetic", in); err != nil {
			return err
		}
	}

	for _, in := range replay {
		if err := add("replay", in); err != nil {
			return err
		}
	}

	if err := flush(); err != nil {
		return err
	}

	jobs := planJobs(all, jobSize)

	base := ts.Add(-time.Hour)
	for i, hashes := range jobs {
		job, err := processor.NewQueueJob(processor.MessageParams{
			ClassifierWorkflow: "default",
			ClassifierFlags: classifier.Flags{
				"local_search_enabled": false,
				"apis_enabled":         false,
				"tmdb_enabled":         false,
			},
			InfoHashes: hashes,
		})
		if err != nil {
			return err
		}
		// A strictly increasing run_after makes both workers' claim order the
		// same total order.
		batch.Queue(`INSERT INTO queue_jobs
			(fingerprint, queue, status, payload, retries, max_retries, run_after,
			 archival_duration, created_at, priority)
			VALUES ($1, $2, 'pending', $3::jsonb, 0, $4, $5, $6::interval, $5, 0)`,
			job.Fingerprint, job.Queue, job.Payload, job.MaxRetries,
			base.Add(time.Duration(i)*time.Millisecond),
			// Go's model.Duration value ("168h0m0s"); its scanner rejects the
			// "7 days" rendering of a day-component interval.
			time.Duration(job.ArchivalDuration).String())
	}

	if err := flush(); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "seeded %d torrents and %d jobs\n", len(all), len(jobs))

	return nil
}

// planJobs groups hashes the way the differential needs: every torrent with a
// sourced explicit hint alone (the Rust worker abstains from those), half the
// expected deletions as one deletion-only job (Go's second early return), and
// the rest in fixed-size chunks. Every tenth chunk gains a missing hash
// (partial failure: retry publication before persistence) and three jobs carry
// only missing hashes (all-failed: retry lifecycle to `failed`).
func planJobs(all []seeded, jobSize int) [][]protocol.ID {
	var (
		jobs         [][]protocol.ID
		deletionOnly []protocol.ID
		rest         []protocol.ID
	)

	for _, s := range all {
		switch {
		case s.sourced:
			jobs = append(jobs, []protocol.ID{s.hash})
		case s.deleted && len(deletionOnly) < 10:
			deletionOnly = append(deletionOnly, s.hash)
		default:
			rest = append(rest, s.hash)
		}
	}

	if len(deletionOnly) > 0 {
		jobs = append(jobs, deletionOnly)
	}

	// One slot is kept free for the appended missing hash, so no job exceeds
	// jobSize (the Rust loader's 100-hash bound equals Go's DHT batch size).
	step := jobSize - 1
	for i := 0; i < len(rest); i += step {
		chunk := append([]protocol.ID(nil), rest[i:min(i+step, len(rest))]...)
		if (i/step)%10 == 0 {
			chunk = append(chunk, fixtureHash("missing", fmt.Sprint(i)))
		}

		jobs = append(jobs, chunk)
	}

	for i := range 3 {
		jobs = append(jobs, []protocol.ID{fixtureHash("all-missing", fmt.Sprint(i))})
	}

	return jobs
}

// dump prints the processor-owned image and the queue lifecycle image in a
// canonical order, excluding write timestamps, generated IDs, and Go's random
// retry jitter (run_after/ran_at).
func dump(ctx context.Context, conn *pgx.Conn, w io.Writer) error {
	queries := []struct{ label, sql string }{
		{"torrent_contents", `SELECT (to_jsonb(t) - 'created_at' - 'updated_at')::text
			FROM torrent_contents t ORDER BY id`},
		{"torrent_tags", `SELECT encode(info_hash, 'hex') || ' ' || name
			FROM torrent_tags ORDER BY 1`},
		{"content", `SELECT (to_jsonb(c) - 'created_at' - 'updated_at')::text
			FROM content c ORDER BY type, source, id`},
		{"torrents", `SELECT encode(info_hash, 'hex') FROM torrents ORDER BY 1`},
		{"deleted_torrents", `SELECT encode(info_hash, 'hex') FROM deleted_torrents ORDER BY 1`},
		{"torrent_hints", `SELECT (to_jsonb(h) - 'created_at' - 'updated_at')::text
			FROM torrent_hints h ORDER BY info_hash`},
		{"queue_jobs", `SELECT concat_ws(' | ', fingerprint, queue, status::text, payload::text,
				retries::text, max_retries::text, coalesce(error, '<null>'), priority::text,
				(ran_at IS NOT NULL)::text, (deadline IS NULL)::text)
			FROM queue_jobs ORDER BY fingerprint, status, retries`},
	}

	for _, q := range queries {
		rows, err := conn.Query(ctx, q.sql)
		if err != nil {
			return fmt.Errorf("%s: %w", q.label, err)
		}

		n := 0

		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				rows.Close()
				return err
			}

			if _, err := fmt.Fprintf(w, "%s\t%s\n", q.label, strings.ReplaceAll(line, "\n", `\n`)); err != nil {
				rows.Close()
				return err
			}

			n++
		}

		rows.Close()

		if err := rows.Err(); err != nil {
			return err
		}

		fmt.Fprintf(os.Stderr, "%s: %d rows\n", q.label, n)
	}

	return nil
}
