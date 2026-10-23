package main

// flagson-coverage: counts every coverage floor of the flags-on differential
// from real I/O, never from the generator's intent:
//
//   - endpoint counts from the stub's request log;
//   - attach-action I/O, local-search ties and null years from the Go tape
//     (CLASSIFIER_TAPE_DIR) of a Go-alone run, which records every action
//     entered and every local query and TMDB request it made;
//   - outcomes from that run's drained database, with "new" and "existing"
//     decided by whether the content existed before the job that attached it
//     ran (seeded, or attached from TMDB by an earlier job; jobs run
//     serialized in jobs.jsonl order).
//
// Only flags-on records and torrents in flags-on jobs count.
//
// The by-ID floors (attach-local-by-id, attach-tmdb-by-id, /find) are reached
// only through sourced hints, which the Rust worker abstains from until M3, so
// in M1 they measure the Go oracle's coverage over every corpus at a lower
// floor. Every other floor must be met by the main corpus alone, the one Go
// and Rust both classify.

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/jackc/pgx/v5"
)

type coverageFloor struct {
	name  string
	floor int
	byID  bool // counted over every corpus; otherwise over the main corpus only
}

const byIDFloorLabel = "Go oracle; Rust abstains until M3"

var coverageFloors = []coverageFloor{
	{endpointSearchMovie, 2000, false},
	{endpointSearchTV, 2000, false},
	{"/movie/{id}", 2000, false},
	{"/tv/{id}", 2000, false},
	{"/find", 500, true},
	{"attach-local-by-id", 500, true},
	{"attach-local-by-search", 2000, false},
	{"attach-tmdb-by-id", 500, true},
	{"attach-tmdb-by-search", 2000, false},
	{"attached-local", 500, false},
	{"attached-tmdb-new", 500, false},
	{"attached-tmdb-existing", 500, false},
	{"unmatched", 500, false},
	{outcomeDeleted, 500, false},
	{"equal-rank ties", 500, false},
	{"multi-genre/multi-id content", 500, false},
}

type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

type tapeRecord struct {
	Subject       string             `json:"subject"`
	Flags         map[string]any     `json:"flags"`
	ActionEntries []tapeActionEntry  `json:"actionEntries"`
	Observations  []tapeObservation  `json:"observations"`
	Outcome       *tapeRecordOutcome `json:"outcome"`
}

type tapeActionEntry struct {
	Name string `json:"name"`
}

type tapeObservation struct {
	Kind     string          `json:"kind"`
	Request  json.RawMessage `json:"request"`
	Outcome  string          `json:"outcome"`
	Response json.RawMessage `json:"response"`
}

type tapeRecordOutcome struct {
	Kind string `json:"kind"`
}

// Tape observation kinds and coverage verdicts.
const (
	observationTMDBRequest = "tmdb.request"
	verdictMet             = "MET"
)

type tapeTMDBRequest struct {
	Path        string            `json:"path"`
	QueryParams map[string]string `json:"queryParams"`
}

type tapeLocalBySearchRequest struct {
	BaseTitle string  `json:"baseTitle"`
	Year      *uint16 `json:"year"`
}

type tapeLocalResponse struct {
	Items []tapeLocalItem `json:"items"`
}

type tapeLocalItem struct {
	QueryStringRank string           `json:"queryStringRank"`
	Content         tapeLocalContent `json:"content"`
}

type tapeLocalContent struct {
	Type          string  `json:"type"`
	Source        string  `json:"source"`
	ID            string  `json:"id"`
	Title         string  `json:"title"`
	OriginalTitle *string `json:"originalTitle"`
}

type coverageCounts map[string]int

func runFlagsOnCoverage(args []string) error {
	fs := flag.NewFlagSet("flagson-coverage", flag.ExitOnError)

	var names, dirs, dsns stringList

	fs.Var(&names, "name", "corpus name (repeat, one per corpus)")
	fs.Var(&dirs, "dir", "corpus evidence dir: seed/ (jobs.jsonl, seeded-content.txt), stub.jsonl, tape/tape.jsonl")
	fs.Var(&dsns, "dsn", "the corpus's drained Go-alone database")
	out := fs.String("out", "", "write the report as JSON here too")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if len(names) == 0 || len(names) != len(dirs) || len(names) != len(dsns) {
		return errors.New("flagson-coverage: give -name, -dir and -dsn once per corpus")
	}

	if !slices.Contains(names, corpusMain) {
		return errors.New("flagson-coverage: the main corpus is required")
	}

	total := coverageCounts{}
	perCorpus := map[string]coverageCounts{}

	for i := range names {
		counts, err := corpusCoverage(dirs[i], dsns[i])
		if err != nil {
			return fmt.Errorf("%s: %w", names[i], err)
		}

		perCorpus[names[i]] = counts

		for k, v := range counts {
			total[k] += v
		}
	}

	keys := map[string]bool{}
	for _, c := range perCorpus {
		for k := range c {
			keys[k] = true
		}
	}

	floorOf := map[string]coverageFloor{}
	for _, f := range coverageFloors {
		floorOf[f.name] = f
		keys[f.name] = true
	}

	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}

	sort.Strings(sorted)

	_, _ = fmt.Fprintf(os.Stdout, "%-44s", "metric")

	for _, n := range names {
		_, _ = fmt.Fprintf(os.Stdout, " %10s", n)
	}

	_, _ = fmt.Fprintf(os.Stdout, " %10s %7s %-5s %s\n", "total", "floor", "over", "")

	failed := 0

	printRow := func(k string) {
		_, _ = fmt.Fprintf(os.Stdout, "%-44s", k)

		for _, n := range names {
			_, _ = fmt.Fprintf(os.Stdout, " %10d", perCorpus[n][k])
		}

		_, _ = fmt.Fprintf(os.Stdout, " %10d", total[k])

		if f, ok := floorOf[k]; ok {
			over, counted, status := corpusMain, perCorpus[corpusMain][k], verdictMet
			if f.byID {
				over, counted = "total", total[k]
			}

			if counted < f.floor {
				status = "NOT MET"
				failed++
			}

			if f.byID {
				status += " (" + byIDFloorLabel + ")"
			}

			_, _ = fmt.Fprintf(os.Stdout, " %7d %-5s %s", f.floor, over, status)
		}

		_, _ = fmt.Fprintln(os.Stdout)
	}

	for _, f := range coverageFloors {
		printRow(f.name)
	}

	_, _ = fmt.Fprintln(os.Stdout, "--")

	for _, k := range sorted {
		if _, isFloor := floorOf[k]; !isFloor {
			printRow(k)
		}
	}

	if *out != "" {
		if err := writeJSONFile(*out, map[string]any{"total": total, "corpora": perCorpus}); err != nil {
			return err
		}
	}

	if failed > 0 {
		return fmt.Errorf("coverage: %d floors not met", failed)
	}

	return nil
}

func isFlagsOn(flags map[string]any) bool {
	for _, k := range []string{"local_search_enabled", "apis_enabled", "tmdb_enabled"} {
		if v, ok := flags[k].(bool); !ok || !v {
			return false
		}
	}

	return true
}

func corpusCoverage(dir, dsn string) (coverageCounts, error) {
	c := coverageCounts{}

	jobs, err := readJSONL[plannedJob](filepath.Join(dir, "seed", "jobs.jsonl"), 0)
	if err != nil {
		return nil, err
	}

	jobOf := map[string]int{}
	flagsOnHash := map[string]bool{}

	for _, j := range jobs {
		for _, h := range j.InfoHashes {
			jobOf[h] = j.Index
			flagsOnHash[h] = j.FlagsOn
		}
	}

	// What the deleted floor is made of: distinct torrents, how many are
	// deletion copies, and the workflow rule the seed's plan saw delete each
	// (the plan runs the same classifier in process; the floor itself counts
	// the Go worker's tape).
	planned, err := readJSONL[plannedTorrent](filepath.Join(dir, "seed", "plan.jsonl"), 0)
	if err != nil {
		return nil, err
	}

	deleteRule := map[string]string{}
	for _, p := range planned {
		if p.Outcome == outcomeDeleted {
			deleteRule[p.Hash] = p.Error
		}
	}

	notes, err := readJSONL[fixtureNote](filepath.Join(dir, "seed", "notes.jsonl"), 0)
	if err != nil {
		return nil, err
	}

	deletionCopy := map[string]bool{}
	for _, n := range notes {
		if n.Kind == "deletion-copy" {
			deletionCopy[n.Key] = true
		}
	}

	deletedHashes := map[string]bool{}

	// Endpoints, from the stub log: every request a classification made,
	// whatever it was answered with (retries are separate lines).
	stub, err := readJSONL[stubLogEntry](filepath.Join(dir, "stub.jsonl"), 0)
	if err != nil {
		return nil, err
	}

	for _, e := range stub {
		c[e.Endpoint]++
		c[fmt.Sprintf("stub %s %d %s", e.Endpoint, e.Status, e.Source)]++
	}

	// The tape.
	tmdbAttached := map[string]bool{} // subject -> got a 200 details body
	hasIO := map[string]bool{}
	localByID := map[string]bool{}

	if err := forEachTapeRecord(filepath.Join(dir, "tape", "tape.jsonl"), func(r tapeRecord) error {
		if !isFlagsOn(r.Flags) {
			c["tape records flags-off"]++
			return nil
		}

		c["tape records flags-on"]++

		if r.Outcome != nil {
			c["tape outcome "+r.Outcome.Kind]++

			if r.Outcome.Kind == outcomeDeleted {
				c[outcomeDeleted]++

				if !deletedHashes[r.Subject] {
					deletedHashes[r.Subject] = true
					c["deleted, distinct torrents"]++

					if deletionCopy[r.Subject] {
						c["deleted, distinct torrents that are deletion copies"]++
					}

					c["deleted by rule: "+deleteRule[r.Subject]]++
				}
			}
		}

		entered := map[string]bool{}
		for _, a := range r.ActionEntries {
			entered[a.Name] = true
			c["action entered "+a.Name]++
		}

		searched, byIDTMDB := false, false
		sawSearch := false

		for _, o := range r.Observations {
			hasIO[r.Subject] = true

			switch o.Kind {
			case "local.content_by_id":
				c["attach-local-by-id"]++
				localByID[r.Subject] = true
			case "local.content_by_search":
				c["attach-local-by-search"]++
				countLocalSearch(c, o.Request, o.Response)
			case observationTMDBRequest:
				var req tapeTMDBRequest
				if err := json.Unmarshal(o.Request, &req); err != nil {
					return err
				}

				isSearch := strings.HasPrefix(req.Path, "/search/")

				switch {
				case isSearch:
					sawSearch = true
					searched = true

					if req.QueryParams["year"] == "" && req.QueryParams["first_air_date_year"] == "" {
						c["null-year tmdb searches"]++
					}
				case strings.HasPrefix(req.Path, "/find/"):
					byIDTMDB = true
				default: // details
					if !sawSearch {
						byIDTMDB = true
					}

					if o.Outcome == "ok" {
						tmdbAttached[r.Subject] = true
					}
				}
			}
		}

		if entered["attach_tmdb_content_by_search"] && searched {
			c["attach-tmdb-by-search"]++
		}

		if entered["attach_tmdb_content_by_id"] && byIDTMDB {
			c["attach-tmdb-by-id"]++
		}

		return nil
	}); err != nil {
		return nil, err
	}

	// Outcomes, from the drained database.
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	existing := map[string]bool{}

	seeded, err := os.ReadFile(filepath.Join(dir, "seed", "seeded-content.txt"))
	if err != nil {
		return nil, err
	}

	for _, line := range strings.Split(string(seeded), "\n") {
		if line != "" {
			existing[line] = true
		}
	}

	type attachment struct {
		hash, ref     string
		genres, ids   int
		releaseIsNull bool
	}

	rows, err := conn.Query(ctx, `SELECT encode(tc.info_hash, 'hex'),
			tc.content_type::text || '/' || tc.content_source || '/' || tc.content_id,
			(SELECT count(*) FROM content_collections_content l
				WHERE (l.content_type, l.content_source, l.content_id) = (tc.content_type, tc.content_source, tc.content_id)
				AND l.content_collection_type = 'genre'),
			(SELECT count(*) FROM content_attributes a
				WHERE (a.content_type, a.content_source, a.content_id) = (tc.content_type, tc.content_source, tc.content_id)
				AND a.key = 'id'),
			c.release_date IS NULL
		FROM torrent_contents tc JOIN content c
			ON (c.type, c.source, c.id) = (tc.content_type, tc.content_source, tc.content_id)
		WHERE tc.content_source IS NOT NULL`)
	if err != nil {
		return nil, err
	}

	var attached []attachment

	for rows.Next() {
		var a attachment
		if err := rows.Scan(&a.hash, &a.ref, &a.genres, &a.ids, &a.releaseIsNull); err != nil {
			rows.Close()
			return nil, err
		}

		if flagsOnHash[a.hash] {
			attached = append(attached, a)
		}
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Content attached from TMDB, by the job that first attached it.
	firstTMDBJob := map[string]int{}

	for _, a := range attached {
		if !tmdbAttached[a.hash] {
			continue
		}

		if j, ok := firstTMDBJob[a.ref]; !ok || jobOf[a.hash] < j {
			firstTMDBJob[a.ref] = jobOf[a.hash]
		}
	}

	attachedHash := map[string]bool{}

	for _, a := range attached {
		attachedHash[a.hash] = true

		switch {
		case !tmdbAttached[a.hash]:
			c["attached-local"]++

			if localByID[a.hash] {
				c["attached-local via by-id"]++
			}
		case existing[a.ref] || firstTMDBJob[a.ref] < jobOf[a.hash]:
			c["attached-tmdb-existing"]++

			if existing[a.ref] {
				c["attached-tmdb-existing (seeded)"]++
			} else {
				c["attached-tmdb-existing (earlier job)"]++
			}
		default:
			c["attached-tmdb-new"]++
		}

		if a.genres >= 2 || a.ids >= 2 {
			c["multi-genre/multi-id content"]++
		}

		if a.genres >= 2 && a.ids >= 2 {
			c["multi-genre and multi-id content"]++
		}

		if a.releaseIsNull {
			c["attached content with null release date"]++
		}
	}

	for h := range hasIO {
		if flagsOnHash[h] && !attachedHash[h] {
			c["unmatched"]++
		}
	}

	var deleted []string

	rows, err = conn.Query(ctx, `SELECT encode(info_hash, 'hex') FROM deleted_torrents`)
	if err != nil {
		return nil, err
	}

	if deleted, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
		return nil, err
	}

	// Go records only some deletions in deleted_torrents; the floor counts
	// the classifications that ended in `delete` (above).
	for _, h := range deleted {
		if flagsOnHash[h] {
			c["deleted_torrents rows"]++
		}
	}

	return c, nil
}

// countLocalSearch counts equal-rank ties: a candidate window in which two or
// more candidates share a rank and both match the base title exactly, so only
// the (type, source, id) tiebreak decides. A tie is collation-sensitive when
// the window's order disagrees with a bytewise sort of the tied identities,
// and numeric-sensitive when both ids are numbers and disagree with a numeric
// sort.
func countLocalSearch(c coverageCounts, rawReq, rawResp json.RawMessage) {
	var req tapeLocalBySearchRequest
	if json.Unmarshal(rawReq, &req) != nil {
		return
	}

	if req.Year == nil {
		c["null-year local searches"]++
	}

	var resp tapeLocalResponse
	if json.Unmarshal(rawResp, &resp) != nil || len(resp.Items) < 2 {
		return
	}

	byRank := map[string]int{}
	for _, it := range resp.Items {
		byRank[it.QueryStringRank]++
	}

	for _, n := range byRank {
		if n >= 2 {
			c["equal-rank windows"]++
			break
		}
	}

	title := strings.ToLower(req.BaseTitle)

	exact := map[string][]string{}

	for _, it := range resp.Items {
		if strings.ToLower(it.Content.Title) == title ||
			(it.Content.OriginalTitle != nil && strings.ToLower(*it.Content.OriginalTitle) == title) {
			exact[it.QueryStringRank] = append(exact[it.QueryStringRank],
				it.Content.Type+"\x00"+it.Content.Source+"\x00"+it.Content.ID)
		}
	}

	for _, ids := range exact {
		if len(ids) < 2 {
			continue
		}

		c["equal-rank ties"]++

		bytewise := append([]string(nil), ids...)
		sort.Strings(bytewise)

		if strings.Join(bytewise, "|") != strings.Join(ids, "|") {
			c["equal-rank ties, collation-sensitive"]++
		}

		if numericOrderDiffers(ids) {
			c["equal-rank ties, numeric-sensitive"]++
		}

		break
	}
}

func numericOrderDiffers(ids []string) bool {
	nums := make([]int64, len(ids))

	for i, id := range ids {
		parts := strings.Split(id, "\x00")

		n, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
		if err != nil {
			return false
		}

		nums[i] = n
	}

	return !sort.SliceIsSorted(nums, func(i, j int) bool { return nums[i] < nums[j] })
}

func forEachTapeRecord(path string, fn func(tapeRecord) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<28)

	for scanner.Scan() {
		var r tapeRecord
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			return err
		}

		if _, err := hex.DecodeString(r.Subject); err != nil {
			return fmt.Errorf("tape subject %q is not an info hash", r.Subject)
		}

		if err := fn(r); err != nil {
			return err
		}
	}

	return scanner.Err()
}

// ---- flagson-organic-coverage ----

// organicSliceCounts is what Go's tape shows one organic slice did.
type organicSliceCounts struct {
	records, mismatched, localSearches, tmdbSearches, tmdbOther int
	outcomes                                                    map[string]int
}

// Per-slice floors of a replay slice of the organic corpus: about one in
// twelve replay torrents searches (the first organic run: 101-129 local and
// 65-113 TMDB searches per slice of 1,524 torrents).
const (
	organicFloorRecords  = 1000 // classifications
	organicFloorSearches = 50   // local searches with local search on, TMDB searches with TMDB on
)

// organicExpectedFlags resolves every non-negative slice's attach flags from
// its jobs' payloads over Go's defaults; the slice's jobs must agree.
func organicExpectedFlags(jobs []plannedJob, defaults classifier.Flags) (map[string]map[string]bool, error) {
	expected := map[string]map[string]bool{}

	for _, j := range jobs {
		if sl, ok := organicSliceByName(j.Slice); !ok || sl.role == organicRoleNegative {
			continue
		}

		var params struct {
			ClassifierFlags map[string]any `json:"ClassifierFlags"`
		}

		if err := json.Unmarshal([]byte(j.Payload), &params); err != nil {
			return nil, fmt.Errorf("job %d: %w", j.Index, err)
		}

		resolved, ok := resolveAttachFlags(params.ClassifierFlags, defaults)
		if !ok {
			return nil, fmt.Errorf("job %d (%s): a non-boolean attach flag", j.Index, j.Slice)
		}

		if prev, seen := expected[j.Slice]; seen && fmt.Sprint(prev) != fmt.Sprint(resolved) {
			return nil, fmt.Errorf("slice %s: its jobs resolve to %v and %v", j.Slice, prev, resolved)
		}

		expected[j.Slice] = resolved
	}

	return expected, nil
}

// runFlagsOnOrganicCoverage counts, per slice, from a Go-alone run's tape:
// classifications, those whose effective attach flags differ from the
// slice's payloads resolved over Go's defaults (must be none), and the
// searches each flag allows (a floor when on, none when off).
func runFlagsOnOrganicCoverage(args []string) error {
	fs := flag.NewFlagSet("flagson-organic-coverage", flag.ExitOnError)
	dir := fs.String("dir", "", "a Go-alone run dir: seed/jobs.jsonl, tape/tape.jsonl")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := requireFlag("dir", *dir); err != nil {
		return err
	}

	jobs, err := readJSONL[plannedJob](filepath.Join(*dir, "seed", "jobs.jsonl"), 0)
	if err != nil {
		return err
	}

	expected, err := organicExpectedFlags(jobs, goFlagDefaults())
	if err != nil {
		return err
	}

	sliceOf := map[string]string{}
	for _, j := range jobs {
		for _, h := range j.InfoHashes {
			sliceOf[strings.ToLower(strings.TrimPrefix(h, "0x"))] = j.Slice
		}
	}

	counts := map[string]*organicSliceCounts{}

	if err := forEachTapeRecord(filepath.Join(*dir, "tape", "tape.jsonl"), func(r tapeRecord) error {
		name := sliceOf[r.Subject]

		c := counts[name]
		if c == nil {
			c = &organicSliceCounts{outcomes: map[string]int{}}
			counts[name] = c
		}

		c.records++

		if want, ok := expected[name]; ok {
			for _, k := range attachFlags {
				if got, isBool := r.Flags[k].(bool); !isBool || got != want[k] {
					c.mismatched++
					break
				}
			}
		}

		if r.Outcome != nil {
			c.outcomes[r.Outcome.Kind]++
		}

		for _, o := range r.Observations {
			switch o.Kind {
			case "local.content_by_search":
				c.localSearches++
			case observationTMDBRequest:
				var req tapeTMDBRequest
				if err := json.Unmarshal(o.Request, &req); err != nil {
					return err
				}

				if strings.HasPrefix(req.Path, "/search/") {
					c.tmdbSearches++
				} else {
					c.tmdbOther++
				}
			}
		}

		return nil
	}); err != nil {
		return err
	}

	var unmet []string

	_, _ = fmt.Fprintf(os.Stdout, "%-30s %-11s %8s %8s %8s %8s %8s  %s\n",
		"slice", "flags", "records", "mismatch", "local", "tmdb-srch", "tmdb-oth", "outcomes")

	for _, sl := range slices.Concat(organicRound1, organicRound2) {
		c := counts[sl.name]
		if c == nil {
			continue
		}

		want, resolved := expected[sl.name]
		flags := "-"

		if resolved {
			flags = ""
			for _, k := range attachFlags {
				flags += map[bool]string{true: "T", false: "F"}[want[k]]
			}
		}

		var verdicts []string

		check := func(ok bool, what string) {
			if !ok {
				verdicts = append(verdicts, what)
				unmet = append(unmet, sl.name+": "+what)
			}
		}

		check(c.mismatched == 0, "effective flags differ from the payload's")

		// Round 2's slices are smaller; flagson-rematch-coverage has its floors.
		floors := slices.ContainsFunc(organicRound1, func(r organicSlice) bool { return r.name == sl.name })

		if resolved && sl.role == "" {
			if floors {
				check(c.records >= organicFloorRecords, fmt.Sprintf("records < %d", organicFloorRecords))
			}

			switch {
			case !want["local_search_enabled"]:
				check(c.localSearches == 0, "local searches with local search off")
			case floors:
				check(c.localSearches >= organicFloorSearches, fmt.Sprintf("local searches < %d", organicFloorSearches))
			}

			switch {
			case !want["apis_enabled"] || !want["tmdb_enabled"]:
				check(c.tmdbSearches+c.tmdbOther == 0, "TMDB requests with TMDB off")
			case floors:
				check(c.tmdbSearches >= organicFloorSearches, fmt.Sprintf("TMDB searches < %d", organicFloorSearches))
			}
		}

		verdict := verdictMet
		if len(verdicts) > 0 {
			verdict = "NOT MET: " + strings.Join(verdicts, "; ")
		}

		_, _ = fmt.Fprintf(os.Stdout, "%-30s %-11s %8d %8d %8d %8d %8d  %v %s\n", sl.name, flags, c.records,
			c.mismatched, c.localSearches, c.tmdbSearches, c.tmdbOther, c.outcomes, verdict)
	}

	if c := counts[""]; c != nil {
		unmet = append(unmet, fmt.Sprintf("%d tape records of torrents in no organic job", c.records))
	}

	if len(unmet) > 0 {
		return fmt.Errorf("flagson-organic-coverage: %s", strings.Join(unmet, "; "))
	}

	return nil
}
