package main

// flagson-abstained: after a Rust-only phase, checks that what Rust left in
// the queue is exactly the jobs it had to abstain from, untouched:
//
//   - every active, released job (pending or retry, run_after before the held
//     date) is a job of the expected slice, still pending with no retries and
//     no error;
//   - their number is the expected number;
//   - none of their torrents has a torrent_contents, torrent_tags or
//     deleted_torrents row, and none of the content their TMDB searches lead
//     to exists, so Rust wrote nothing for them;
//   - with -rust-log, the phase's Rust log has exactly one abstention line per
//     left job (the harness sets a cool-off longer than the phase) and none
//     for any other job, each with the expected reason and naming a torrent
//     of that job: "hint" for the attach-hint refusal before classification,
//     "resolver" for a content resolver failure, "none" for no abstention.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

func runFlagsOnAbstained(args []string) error {
	fs := flag.NewFlagSet("flagson-abstained", flag.ExitOnError)
	dsn := fs.String("dsn", "", "PostgreSQL DSN after the Rust phase")
	seedDir := fs.String("seed", "", "the corpus's seed dir (jobs.jsonl, plan.jsonl, notes.jsonl)")
	slice := fs.String("slice", sliceFault, "the slice every abstained job must belong to")
	expect := fs.Int("expect", -1, "the number of jobs Rust must have abstained from")
	rustLog := fs.String("rust-log", "", "the Rust worker's log of this phase alone")
	reason := fs.String("reason", "", "hint | resolver | none: the abstention every -rust-log line must give")

	if err := fs.Parse(args); err != nil {
		return err
	}

	jobs, err := readJSONL[plannedJob](filepath.Join(*seedDir, "jobs.jsonl"), 0)
	if err != nil {
		return err
	}

	planned, err := readJSONL[plannedTorrent](filepath.Join(*seedDir, "plan.jsonl"), 0)
	if err != nil {
		return err
	}

	notes, err := readJSONL[fixtureNote](filepath.Join(*seedDir, "notes.jsonl"), 0)
	if err != nil {
		return err
	}

	byFingerprint := map[string]plannedJob{}
	for _, j := range jobs {
		byFingerprint[j.Fingerprint] = j
	}

	plannedBy := map[string]plannedTorrent{}
	for _, p := range planned {
		plannedBy[p.Hash] = p
	}

	matchID := map[string]string{} // TMDB search key -> the id its match leads to
	for _, n := range notes {
		if n.Kind == "tmdb-layout" && n.ID != "" {
			matchID[n.Key] = n.ID
		}
	}

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, `SELECT id::text, fingerprint, status::text, retries, error IS NULL
		FROM queue_jobs
		WHERE status IN ('pending', 'retry') AND run_after < $1::timestamptz
		ORDER BY fingerprint`, heldRunAfter)
	if err != nil {
		return err
	}

	type activeJob struct {
		id, fingerprint, status string
		retries                 int
		noError                 bool
	}

	var active []activeJob

	for rows.Next() {
		var a activeJob
		if err := rows.Scan(&a.id, &a.fingerprint, &a.status, &a.retries, &a.noError); err != nil {
			rows.Close()
			return err
		}

		active = append(active, a)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return err
	}

	var violations []string

	var hashes [][]byte

	var tmdbIDs []string

	for _, a := range active {
		j, ok := byFingerprint[a.fingerprint]

		switch {
		case !ok:
			violations = append(violations, "active job not queued by the seed: "+a.fingerprint)
			continue
		case j.Slice != *slice:
			violations = append(violations, fmt.Sprintf("job %d (%s) of slice %s is active", j.Index, a.fingerprint, j.Slice))
		case a.status != "pending" || a.retries != 0 || !a.noError:
			violations = append(violations, fmt.Sprintf("job %d (%s) was touched: status %s, retries %d, error set %t",
				j.Index, a.fingerprint, a.status, a.retries, !a.noError))
		}

		for _, h := range j.InfoHashes {
			hashes = append(hashes, mustHash(h).Bytes())

			if c, ok := tmdbSearchOf(plannedBy[h]); ok {
				if id := matchID[planSearchKey(c)]; id != "" {
					tmdbIDs = append(tmdbIDs, id)
				}
			}
		}
	}

	for _, q := range []struct{ label, sql string }{
		{"torrent_contents", `SELECT count(*) FROM torrent_contents WHERE info_hash = ANY($1)`},
		{"torrent_tags", `SELECT count(*) FROM torrent_tags WHERE info_hash = ANY($1)`},
		{"deleted_torrents", `SELECT count(*) FROM deleted_torrents WHERE info_hash = ANY($1)`},
	} {
		var n int
		if err := conn.QueryRow(ctx, q.sql, hashes).Scan(&n); err != nil {
			return err
		}

		if n != 0 {
			violations = append(violations, fmt.Sprintf("%d %s rows written for abstained torrents", n, q.label))
		}
	}

	var contentRows int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM content WHERE source = 'tmdb' AND id = ANY($1)`,
		tmdbIDs).Scan(&contentRows); err != nil {
		return err
	}

	if contentRows != 0 {
		violations = append(violations, fmt.Sprintf("%d content rows exist for the TMDB ids abstained torrents lead to", contentRows))
	}

	if *expect >= 0 && len(active) != *expect {
		violations = append(violations, fmt.Sprintf("abstained from %d jobs, expected %d", len(active), *expect))
	}

	logged := ""

	if *rustLog != "" {
		hashesOf := map[string][]string{}
		for _, a := range active {
			hashesOf[a.id] = byFingerprint[a.fingerprint].InfoHashes
		}

		lines, logViolations, err := checkAbstainLog(*rustLog, *reason, hashesOf)
		if err != nil {
			return err
		}

		violations = append(violations, logViolations...)
		logged = fmt.Sprintf(", %d Rust abstention lines (reason %s)", lines, *reason)
	}

	_, _ = fmt.Fprintf(os.Stdout, "abstained %d jobs (expected %d), %d torrents, %d TMDB ids checked%s: %d violations\n",
		len(active), *expect, len(hashes), len(tmdbIDs), logged, len(violations))

	for _, v := range violations {
		_, _ = fmt.Fprintln(os.Stderr, "VIOLATION: "+v)
	}

	if len(violations) > 0 {
		return errors.New("flagson-abstained: violations")
	}

	return nil
}

var (
	ansiEscape  = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	logJobID    = regexp.MustCompile(`job_id=([0-9a-f-]{36})`)
	logInfoHash = regexp.MustCompile(`info_hash=([0-9a-f]{40})`)
	logHintHash = regexp.MustCompile(`cannot reproduce the complete Go hint/enrichment path for ([0-9a-f]{40})`)
)

// checkAbstainLog matches the Rust worker's abstention lines (worker.rs) to the
// jobs left behind, by queue job ID.
func checkAbstainLog(path, reason string, hashesOf map[string][]string) (int, []string, error) {
	if reason != "hint" && reason != "resolver" && reason != "none" {
		return 0, nil, fmt.Errorf("-reason %q: want hint, resolver or none", reason)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, nil, err
	}

	var violations []string

	seen := map[string]int{}
	lines := 0

	for _, line := range strings.Split(ansiEscape.ReplaceAllString(string(raw), ""), "\n") {
		if !strings.Contains(line, "abstaining from") {
			continue
		}

		lines++

		var kind string

		var hash []string

		switch {
		case strings.Contains(line, "abstaining from process_torrent job after a content resolver failure"):
			kind, hash = "resolver", logInfoHash.FindStringSubmatch(line)
		case strings.Contains(line, "abstaining from unsupported process_torrent job"):
			if hash = logHintHash.FindStringSubmatch(line); hash != nil {
				kind = "hint"
			}
		}

		id := logJobID.FindStringSubmatch(line)

		switch {
		case kind == "" || kind != reason:
			violations = append(violations, "unexpected Rust abstention: "+line)
		case id == nil || hash == nil:
			violations = append(violations, "unparsed Rust abstention: "+line)
		case hashesOf[id[1]] == nil:
			violations = append(violations, "Rust abstained from a job that is not left: "+line)
		case !slices.Contains(hashesOf[id[1]], hash[1]):
			violations = append(violations, "Rust abstention names a torrent outside its job: "+line)
		default:
			seen[id[1]]++
		}
	}

	for id := range hashesOf {
		if seen[id] != 1 {
			violations = append(violations, fmt.Sprintf("job %s left with %d Rust abstention lines, want 1", id, seen[id]))
		}
	}

	sort.Strings(violations)

	return lines, violations, nil
}
