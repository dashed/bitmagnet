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
//     "ambiguous" for associations Go picks among by row order (also before
//     classification), "conflict" for two images of one content (after it,
//     naming the content), "resolver" for a content resolver failure, "none"
//     for no abstention; and no resolver call started for a torrent of a job
//     left before classification.
//
// With -expect-mode (the hint corpora, flagson_hints.go) the expected jobs and
// reasons are each job's Expect for that worker generation (flags_on.sh
// HINT_EXPECT), and the corpus's seeded associations
// (seeded-associations.txt) must be as seeded.
//
// With -organic-bound (the organic corpus, flagson_organic.go) the expected
// jobs come from each job's slice and priority instead: a negative slice's
// jobs, never claimed (no line); the hinted slice's, claimed and abstained
// from ("hint"); and every other job above the bound, never claimed. Their
// TMDB content is not checked: the replay slices share search keys.

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
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
	organicBound := fs.Int("organic-bound", -1, "the organic corpus's Rust --organic-max-priority (replaces -slice, -reason, -expect)")
	expectMode := fs.String("expect-mode", "", "abstain | process: the hint corpora's worker generation (replaces -slice, -reason, -expect)")

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

	// The reason each job Rust must leave gives, by fingerprint.
	var wantReason map[string]string

	switch {
	case *organicBound >= 0:
		wantReason = organicLeft(jobs, *organicBound)
		*expect = len(wantReason)
	case *expectMode != "":
		if wantReason, err = hintExpectations(jobs, *expectMode); err != nil {
			return err
		}

		*expect = len(wantReason)
	}

	seeded, err := readSeededAssociations(*seedDir)
	if err != nil {
		return err
	}

	// TMDB ids whose content the seed holds already (a search's "existing"
	// category): Rust writing nothing leaves those as they are.
	seededTMDB := map[string]bool{}

	seededContent, err := os.ReadFile(filepath.Join(*seedDir, "seeded-content.txt"))
	if err != nil {
		return err
	}

	for _, ref := range strings.Split(string(seededContent), "\n") {
		if _, id, ok := strings.Cut(ref, "/"+model.SourceTmdb+"/"); ok {
			seededTMDB[id] = true
		}
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
		case wantReason != nil && wantReason[a.fingerprint] == "":
			violations = append(violations, fmt.Sprintf("job %d (%s) of slice %s at priority %d is active",
				j.Index, a.fingerprint, j.Slice, j.Priority))
		case wantReason == nil && j.Slice != *slice:
			violations = append(violations, fmt.Sprintf("job %d (%s) of slice %s is active", j.Index, a.fingerprint, j.Slice))
		case a.status != "pending" || a.retries != 0 || !a.noError:
			violations = append(violations, fmt.Sprintf("job %d (%s) was touched: status %s, retries %d, error set %t",
				j.Index, a.fingerprint, a.status, a.retries, !a.noError))
		}

		for _, h := range j.InfoHashes {
			hashes = append(hashes, mustHash(h).Bytes())

			if c, ok := tmdbSearchOf(plannedBy[h]); ok {
				if id := matchID[planSearchKey(c)]; id != "" && !seededTMDB[id] {
					tmdbIDs = append(tmdbIDs, id)
				}
			}
		}
	}

	activeFingerprints := map[string]bool{}
	for _, a := range active {
		activeFingerprints[a.fingerprint] = true
	}

	for _, fp := range sortedKeys(wantReason) {
		if !activeFingerprints[fp] {
			j := byFingerprint[fp]
			violations = append(violations, fmt.Sprintf("job %d (%s) of slice %s at priority %d was claimed",
				j.Index, fp, j.Slice, j.Priority))
		}
	}

	if *organicBound >= 0 {
		tmdbIDs = nil

		organicShares(jobs, wantReason, activeFingerprints)
	}

	fixtureTS, err := time.Parse(time.RFC3339Nano, fixtureTime)
	if err != nil {
		return err
	}

	// A seeded association untouched keeps its fixture-time updated_at.
	for _, q := range []struct {
		label, sql string
		args       []any
	}{
		{"torrent_contents", `SELECT count(*) FROM torrent_contents
			WHERE info_hash = ANY($1) AND NOT (id = ANY($2) AND updated_at = $3)`, []any{hashes, seeded, fixtureTS}},
		{"torrent_tags", `SELECT count(*) FROM torrent_tags WHERE info_hash = ANY($1)`, []any{hashes}},
		{"deleted_torrents", `SELECT count(*) FROM deleted_torrents WHERE info_hash = ANY($1)`, []any{hashes}},
	} {
		var n int
		if err := conn.QueryRow(ctx, q.sql, q.args...).Scan(&n); err != nil {
			return err
		}

		if n != 0 {
			violations = append(violations, fmt.Sprintf("%d %s rows written for abstained torrents", n, q.label))
		}
	}

	if want := seededOf(seeded, hashes); want > 0 {
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM torrent_contents WHERE info_hash = ANY($1) AND id = ANY($2)`,
			hashes, seeded).Scan(&n); err != nil {
			return err
		}

		if n != want {
			violations = append(violations, fmt.Sprintf("%d of the abstained torrents' %d seeded associations left", n, want))
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
		hashesOf, reasonOf := map[string][]string{}, map[string]string{}
		reasons := map[string]int{}

		for _, a := range active {
			hashesOf[a.id] = byFingerprint[a.fingerprint].InfoHashes
			reasonOf[a.id] = *reason

			if wantReason != nil {
				// A job left that should not be (a violation above) must
				// still have no abstention line.
				reasonOf[a.id] = abstainNone
				if r, ok := wantReason[a.fingerprint]; ok {
					reasonOf[a.id] = r
				}
			}

			reasons[reasonOf[a.id]]++
		}

		lines, logViolations, err := checkAbstainLog(*rustLog, reasonOf, hashesOf)
		if err != nil {
			return err
		}

		violations = append(violations, logViolations...)

		calls, callViolations, err := checkNoResolverCalls(*rustLog, reasonOf, hashesOf)
		if err != nil {
			return err
		}

		violations = append(violations, callViolations...)
		logged = fmt.Sprintf(", %d Rust abstention lines (reasons of the left jobs %v), %d resolver calls started for torrents left before classification",
			lines, reasons, calls)
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

// The abstention a left job's Rust log line gives (worker.rs): the
// attach-hint refusal, a content resolver failure, or no line at all (and
// abstainAmbiguous, abstainConflict: flagson_hints.go).
const (
	abstainHint     = "hint"
	abstainResolver = "resolver"
	abstainNone     = "none"
)

var (
	ansiEscape       = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	logJobID         = regexp.MustCompile(`job_id=([0-9a-f-]{36})`)
	logInfoHash      = regexp.MustCompile(`info_hash=([0-9a-f]{40})`)
	logHintHash      = regexp.MustCompile(`cannot reproduce the complete Go hint/enrichment path for ([0-9a-f]{40})`)
	logAmbiguousHash = regexp.MustCompile(`cannot reproduce Go's (?:reused association|preserved content type) for ([0-9a-f]{40})`)
	logReason        = regexp.MustCompile(` reason="?([a-z]+)"?`)
)

// logConflict is the case-E abstention's text, which names the content, not a
// torrent.
const logConflict = "conflicting attached content images for "

// readSeededAssociations reads a seed's association ids, if it has any.
func readSeededAssociations(seedDir string) ([]string, error) {
	raw, err := os.ReadFile(filepath.Join(seedDir, "seeded-associations.txt"))
	if errors.Is(err, os.ErrNotExist) {
		return []string{}, nil
	}

	if err != nil {
		return nil, err
	}

	ids := []string{}

	for _, line := range strings.Split(string(raw), "\n") {
		if line != "" {
			ids = append(ids, line)
		}
	}

	return ids, nil
}

// seededOf counts the seeded associations of these torrents (an id starts
// with its torrent's hex info hash).
func seededOf(seeded []string, hashes [][]byte) int {
	of := map[string]bool{}
	for _, h := range hashes {
		of[hex.EncodeToString(h)] = true
	}

	n := 0

	for _, id := range seeded {
		if hash, _, ok := strings.Cut(id, ":"); ok && of[hash] {
			n++
		}
	}

	return n
}

// checkNoResolverCalls: Rust left a hint or ambiguous job before classifying
// it, so no resolver call may have started for one of its torrents (the
// guard's debug events, flagson_attribution.go).
func checkNoResolverCalls(path string, reasonOf map[string]string, hashesOf map[string][]string) (int, []string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, nil, err
	}

	started := map[string]int{}

	for _, line := range strings.Split(ansiEscape.ReplaceAllString(string(raw), ""), "\n") {
		if !strings.Contains(line, eventCallStarted) {
			continue
		}

		if m := logInfoHash.FindStringSubmatch(line); m != nil {
			started[m[1]]++
		}
	}

	calls := 0

	var violations []string

	for id, hashes := range hashesOf {
		if reasonOf[id] != abstainHint && reasonOf[id] != abstainAmbiguous {
			continue
		}

		for _, h := range hashes {
			if n := started[h]; n > 0 {
				calls += n
				violations = append(violations, fmt.Sprintf("job %s (%s) left with %d resolver calls started for %s",
					id, reasonOf[id], n, h))
			}
		}
	}

	sort.Strings(violations)

	return calls, violations, nil
}

// checkAbstainLog matches the Rust worker's abstention lines (worker.rs) to the
// jobs left behind, by queue job ID: one line with the job's reason, or none
// for reason "none".
func checkAbstainLog(path string, reasonOf map[string]string, hashesOf map[string][]string) (int, []string, error) {
	for _, reason := range reasonOf {
		if !slices.Contains([]string{abstainHint, abstainAmbiguous, abstainConflict, abstainResolver, abstainNone}, reason) {
			return 0, nil, fmt.Errorf("reason %q: want hint, ambiguous, conflict, resolver or none", reason)
		}
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
			kind, hash = abstainResolver, logInfoHash.FindStringSubmatch(line)
		case strings.Contains(line, "abstaining from unsupported process_torrent job"):
			kind, hash = unsupportedAbstention(line)
		}

		id := logJobID.FindStringSubmatch(line)
		label := logReason.FindStringSubmatch(line)

		switch {
		case id == nil || kind == "" || (hash == nil && kind != abstainConflict):
			violations = append(violations, "unparsed Rust abstention: "+line)
		case label != nil && label[1] != kind:
			violations = append(violations, fmt.Sprintf("Rust abstention labelled %s for a %s text: %s", label[1], kind, line))
		case hashesOf[id[1]] == nil:
			violations = append(violations, "Rust abstained from a job that is not left: "+line)
		case kind != reasonOf[id[1]]:
			violations = append(violations, "unexpected Rust abstention: "+line)
		case hash != nil && !slices.Contains(hashesOf[id[1]], hash[1]):
			violations = append(violations, "Rust abstention names a torrent outside its job: "+line)
		default:
			seen[id[1]]++
		}
	}

	for id := range hashesOf {
		want := 1
		if reasonOf[id] == abstainNone {
			want = 0
		}

		if seen[id] != want {
			violations = append(violations, fmt.Sprintf("job %s left with %d Rust abstention lines, want %d", id, seen[id], want))
		}
	}

	sort.Strings(violations)

	return lines, violations, nil
}

// unsupportedAbstention classifies an "unsupported" abstention line by its
// error text: the hint and ambiguity texts name a torrent, the conflict text
// names the content.
func unsupportedAbstention(line string) (string, []string) {
	if hash := logHintHash.FindStringSubmatch(line); hash != nil {
		return abstainHint, hash
	}

	if hash := logAmbiguousHash.FindStringSubmatch(line); hash != nil {
		return abstainAmbiguous, hash
	}

	if strings.Contains(line, logConflict) {
		return abstainConflict, nil
	}

	return "", nil
}
