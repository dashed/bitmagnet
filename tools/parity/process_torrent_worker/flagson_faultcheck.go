package main

// flagson-faultcheck: checks that Go alone did with every fault job what its
// class says (flagson_faults.go), so each class covers what it claims. A model
// of Go's failure handling replays each job from its torrents' roles and fault
// steps:
//
//   - a fault call makes up to 4 requests while the steps are transport faults
//     (resty retries those only), and 1 otherwise; a 401 latches the process,
//     after which calls fail without a request;
//   - an attempt with failures and nothing classified fails the job: retried
//     while retries < 2, then failed, the error kept even if a later attempt
//     succeeds; an attempt with failures and something classified republishes
//     the failed hashes, missing ones included, as a new job;
//   - deletions persist only from an attempt that classified something.
//
// It then compares, per fault job, the multiset of (hashes, status, retries,
// error line count) over the job and every job queued from it, the requests
// per fault key, and which deleted-role torrents were deleted.

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// modelJob is one queue_jobs row the model expects.
type modelJob struct {
	hashes   []string // sorted
	status   string
	retries  int
	errLines int // 0: error NULL
}

func (j modelJob) String() string {
	return fmt.Sprintf("%s status=%s retries=%d errorLines=%d", strings.Join(j.hashes, ","), j.status, j.retries, j.errLines)
}

type faultModel struct {
	byHash   map[string]faultTorrent
	served   map[string]int
	requests map[string]int
	latched  bool
	deleted  map[string]bool
}

// call is one resolver call for a fault torrent's key.
func (m *faultModel) call(t faultTorrent) bool {
	for range 4 {
		if m.latched {
			return false
		}

		step := t.Steps[min(m.served[t.Key], len(t.Steps)-1)]
		m.served[t.Key]++
		m.requests[t.Key]++

		switch {
		case step == stepOK:
			return true
		case step == faultUnauthorized:
			m.latched = true
			return false
		case isTransportFault(step):
			continue
		default:
			return false
		}
	}

	return false
}

// attempt runs one attempt of a job: whether it failed, and the hashes it
// republishes when it did not.
func (m *faultModel) attempt(hashes []string) (failed bool, errLines int, republish []string) {
	var missing, failures, deletions []string

	classified := 0

	for _, h := range hashes {
		t := m.byHash[h]

		switch t.Role {
		case roleMissing:
			missing = append(missing, h)
		case roleDeleted:
			deletions = append(deletions, h)
		case roleFault:
			if m.call(t) {
				classified++
			} else {
				failures = append(failures, h)
			}
		default:
			classified++
		}
	}

	all := slices.Concat(missing, failures)

	if len(all) > 0 && classified == 0 {
		lines := len(failures)
		if len(missing) > 0 {
			lines++
		}

		return true, lines, nil
	}

	if classified > 0 {
		for _, h := range deletions {
			m.deleted[h] = true
		}
	}

	return false, 0, all
}

// simulate replays a fault job and every job queued from it.
func (m *faultModel) simulate(job faultJobPlan) []modelJob {
	var rows []modelJob

	queue := [][]string{nil}
	for _, t := range job.Torrents {
		queue[0] = append(queue[0], t.Hash)
	}

	for len(queue) > 0 {
		hashes := queue[0]
		queue = queue[1:]

		row := modelJob{status: "pending"}

		for {
			if row.status != "pending" {
				row.retries++
			}

			failed, lines, republish := m.attempt(hashes)
			if !failed {
				if len(republish) > 0 {
					queue = append(queue, republish)
				}

				row.status = "processed"

				break
			}

			row.errLines = lines

			if row.retries < 2 {
				row.status = "retry"
				continue
			}

			row.status = "failed"

			break
		}

		row.hashes = append([]string(nil), hashes...)
		sort.Strings(row.hashes)
		rows = append(rows, row)
	}

	return rows
}

func runFlagsOnFaultCheck(args []string) error {
	fs := flag.NewFlagSet("flagson-faultcheck", flag.ExitOnError)
	dsn := fs.String("dsn", "", "PostgreSQL DSN of a Go-alone run")
	seedDir := fs.String("seed", "", "the corpus's seed dir (faultplan.jsonl)")
	requestsPath := fs.String("requests", "", "the run's stub request log")

	if err := fs.Parse(args); err != nil {
		return err
	}

	plan, err := readJSONL[faultJobPlan](filepath.Join(*seedDir, "faultplan.jsonl"), 0)
	if err != nil {
		return err
	}

	entries, err := readJSONL[stubLogEntry](*requestsPath, 0)
	if err != nil {
		return err
	}

	got := map[string]int{}
	for _, e := range entries {
		got[e.Key]++
	}

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	jobsOf, err := faultJobRows(ctx, conn, plan)
	if err != nil {
		return err
	}

	deleted, err := deletedHashes(ctx, conn)
	if err != nil {
		return err
	}

	var violations []string

	type classCount struct{ jobs, matched int }

	classes := map[string]*classCount{}

	var classOrder []string

	for i, job := range plan {
		if classes[job.Class] == nil {
			classes[job.Class] = &classCount{}
			classOrder = append(classOrder, job.Class)
		}

		classes[job.Class].jobs++

		m := &faultModel{
			byHash: map[string]faultTorrent{}, served: map[string]int{}, requests: map[string]int{},
			deleted: map[string]bool{},
		}
		for _, t := range job.Torrents {
			m.byHash[t.Hash] = t
		}

		want := m.simulate(job)
		bad := compareModelRows(want, jobsOf[i])

		for _, t := range job.Torrents {
			switch t.Role {
			case roleFault:
				if got[t.Key] != m.requests[t.Key] {
					bad = append(bad, fmt.Sprintf("%d requests for %s, want %d", got[t.Key], t.Key, m.requests[t.Key]))
				}
			case roleDeleted:
				if deleted[t.Hash] != m.deleted[t.Hash] {
					bad = append(bad, fmt.Sprintf("torrent %s deleted %t, want %t", t.Hash, deleted[t.Hash], m.deleted[t.Hash]))
				}
			}
		}

		if len(bad) == 0 {
			classes[job.Class].matched++
			continue
		}

		for _, b := range bad {
			violations = append(violations, fmt.Sprintf("job %d (%s): %s", job.Index, job.Class, b))
		}
	}

	for _, c := range classOrder {
		_, _ = fmt.Fprintf(os.Stdout, "fault class %-26s %d/%d jobs as modelled\n", c, classes[c].matched, classes[c].jobs)
	}

	_, _ = fmt.Fprintf(os.Stdout, "faultcheck: %d fault jobs in %d classes, %d violations\n",
		len(plan), len(classOrder), len(violations))

	for _, v := range violations {
		_, _ = fmt.Fprintln(os.Stderr, "VIOLATION: "+v)
	}

	if len(violations) > 0 {
		return errors.New("flagson-faultcheck: violations")
	}

	return nil
}

// faultJobRows maps every process_torrent queue_jobs row to the fault job
// whose torrents it holds.
func faultJobRows(ctx context.Context, conn *pgx.Conn, plan []faultJobPlan) (map[int][]modelJob, error) {
	jobOf := map[string]int{}

	for i, job := range plan {
		for _, t := range job.Torrents {
			jobOf[t.Hash] = i
		}
	}

	rows, err := conn.Query(ctx, `SELECT status::text, retries, error, payload->'InfoHashes'
		FROM queue_jobs WHERE queue = $1`, processTorrentQueue)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[int][]modelJob{}

	for rows.Next() {
		var (
			row      modelJob
			jobError *string
			raw      []byte
		)

		if err := rows.Scan(&row.status, &row.retries, &jobError, &raw); err != nil {
			return nil, err
		}

		if err := json.Unmarshal(raw, &row.hashes); err != nil {
			return nil, err
		}

		if len(row.hashes) == 0 {
			continue
		}

		i, ok := jobOf[row.hashes[0]]
		if !ok {
			continue
		}

		for _, h := range row.hashes {
			if j, ok := jobOf[h]; !ok || j != i {
				return nil, fmt.Errorf("a queue job mixes fault job %d with other torrents: %v", plan[i].Index, row.hashes)
			}
		}

		if jobError != nil {
			row.errLines = len(strings.Split(*jobError, "\n"))
		}

		sort.Strings(row.hashes)
		out[i] = append(out[i], row)
	}

	return out, rows.Err()
}

func deletedHashes(ctx context.Context, conn *pgx.Conn) (map[string]bool, error) {
	rows, err := conn.Query(ctx, `SELECT encode(info_hash, 'hex') FROM deleted_torrents`)
	if err != nil {
		return nil, err
	}

	hashes, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}

	out := make(map[string]bool, len(hashes))
	for _, h := range hashes {
		out[h] = true
	}

	return out, nil
}

// compareModelRows compares two multisets of queue rows.
func compareModelRows(want, got []modelJob) []string {
	count := func(rows []modelJob) map[string]int {
		m := map[string]int{}
		for _, r := range rows {
			m[r.String()]++
		}

		return m
	}

	w, g := count(want), count(got)

	keys := make([]string, 0, len(w)+len(g))
	for k := range w {
		keys = append(keys, k)
	}

	for k := range g {
		if _, ok := w[k]; !ok {
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)

	var bad []string

	for _, k := range keys {
		if w[k] != g[k] {
			bad = append(bad, fmt.Sprintf("queue row %q: %d, want %d", k, g[k], w[k]))
		}
	}

	return bad
}
