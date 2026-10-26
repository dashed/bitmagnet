package main

// The rematch phase (flags_on.sh PHASES=rematch): a second, reprocess round
// over Go's drained main round 1 (main go1), half of it in ClassifyMode
// rematch, half in the default mode, which reuses a sourced attachment. Both
// workers start from the same copy, so G-1 and G2 compare as for main.
//
// flagson-requeue prepares that copy:
//
//   - It queues again, with flags-on payloads, every torrent round 1 attached
//     to sourced content (attached), every movie or tv_show left without
//     content (unmatched), and the first -rule-derived torrents of another
//     content type without a source (rule-derived). A hash bit puts each in a
//     rematch or a default-mode job; a job holds one mode. Torrents with a
//     sourced explicit hint stay out: the Rust worker abstains from them until
//     M3b. Type-only hints stay, as a reprocess keeps them, unless
//     -keep-hints=false.
//   - No file list: a hash bit of the rule-derived torrents without a hint
//     loses its stored file list (files_status no_info), so their type
//     survives only through Go's rule-derived type preservation, which ignores
//     the mode.
//   - Better candidates: for a hash bit of the attached contents, a copy of the
//     row, its attributes and collection links under the id "0"+id. The
//     classifier's local search orders by rank, then identity (type, source,
//     id); the copy ties the original's rank, sorts before it, and is as near
//     to the title, so a torrent that searches again attaches the copy, while
//     a default-mode reuse keeps the original.
//
// rematchplan.jsonl records every requeued torrent; flagson-rematch-coverage
// counts the phase's floors from it and Go's round-2 database.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/jackc/pgx/v5"
)

const (
	sliceRematch = "rematch" // a ClassifyMode rematch job of the rematch phase
	sliceReuse   = "reuse"   // a default-mode job of the rematch phase

	kindAttached    = "attached"     // round 1 attached sourced content
	kindUnmatched   = "unmatched"    // a movie or tv_show round 1 left without content
	kindRuleDerived = "rule-derived" // another content type, without a source

	// betterCandidatePrefix makes a copy's id sort before the original's: 0
	// has the lowest primary weight of the digits and letters, and no id of
	// the corpus starts with 0 or with punctuation the collation would skip.
	betterCandidatePrefix = "0"
)

// attachment is a torrent's torrent_contents row as the rematch phase sees it;
// "" stands for NULL.
type attachment struct {
	ContentType string `json:"type,omitempty"`
	Source      string `json:"source,omitempty"`
	ID          string `json:"id,omitempty"`
}

func (a attachment) attached() bool { return a.Source != "" }

func (a attachment) String() string { return a.ContentType + "/" + a.Source + "/" + a.ID }

// rematchTorrent is one line of rematchplan.jsonl.
type rematchTorrent struct {
	Hash       string     `json:"hash"`
	Mode       string     `json:"mode"` // sliceRematch or sliceReuse
	Kind       string     `json:"kind"`
	Before     attachment `json:"before"`
	Better     bool       `json:"better,omitempty"`     // its content has a better candidate
	NoFileList bool       `json:"noFileList,omitempty"` // its stored file list was dropped
}

// Hint kinds of a torrent, for the rematch plan.
const (
	hintNone     = ""
	hintTypeOnly = "type"
	hintSourced  = "sourced"
)

// planRematch picks the requeued torrents, in info hash order, and the
// contents that get a better candidate.
func planRematch(before map[string]attachment, hints map[string]string, ruleDerived int) ([]rematchTorrent, []attachment) {
	betterOf := map[attachment]bool{}
	plan := make([]rematchTorrent, 0, len(before))

	var (
		better []attachment
		rules  int
	)

	for _, h := range sortedKeys(before) {
		a := before[h]
		if hints[h] == hintSourced {
			continue
		}

		t := rematchTorrent{Hash: h, Mode: sliceReuse, Before: a}
		if fixtureRand("rematch-mode/"+h).IntN(2) == 0 {
			t.Mode = sliceRematch
		}

		switch {
		case a.attached():
			t.Kind = kindAttached

			b, seen := betterOf[a]
			if !seen {
				b = fixtureRand("better/"+a.String()).IntN(2) == 0
				betterOf[a] = b

				if b {
					better = append(better, a)
				}
			}

			t.Better = b
		case a.ContentType == "movie" || a.ContentType == "tv_show":
			t.Kind = kindUnmatched
		case a.ContentType != "" && rules < ruleDerived:
			rules++
			t.Kind = kindRuleDerived
			t.NoFileList = hints[h] == hintNone && fixtureRand("no-file-list/"+h).IntN(2) == 0
		default:
			continue
		}

		plan = append(plan, t)
	}

	return plan, better
}

// rematchJobs chunks each mode's torrents into jobs of jobSize and alternates
// the two modes' jobs.
func rematchJobs(plan []rematchTorrent, jobSize int) []jobSpec {
	chunk := func(mode string, classifyMode processor.ClassifyMode) []jobSpec {
		var hashes []protocol.ID

		for _, t := range plan {
			if t.Mode == mode {
				hashes = append(hashes, mustHash(t.Hash))
			}
		}

		var specs []jobSpec

		for i := 0; i < len(hashes); i += jobSize {
			specs = append(specs, jobSpec{
				slice: mode, flagsOn: true, classifyMode: classifyMode,
				hashes: hashes[i:min(i+jobSize, len(hashes))],
			})
		}

		return specs
	}

	rematch, reuse := chunk(sliceRematch, processor.ClassifyModeRematch), chunk(sliceReuse, processor.ClassifyModeDefault)
	specs := make([]jobSpec, 0, len(rematch)+len(reuse))

	for i := range max(len(rematch), len(reuse)) {
		if i < len(rematch) {
			specs = append(specs, rematch[i])
		}

		if i < len(reuse) {
			specs = append(specs, reuse[i])
		}
	}

	return specs
}

// attachments maps every torrent with exactly one torrent_contents row to it,
// and to its hint kind.
func attachments(ctx context.Context, conn *pgx.Conn) (map[string]attachment, map[string]string, error) {
	rows, err := conn.Query(ctx, `SELECT encode(tc.info_hash, 'hex'), coalesce(tc.content_type::text, ''),
			coalesce(tc.content_source, ''), coalesce(tc.content_id, ''),
			CASE WHEN h.info_hash IS NULL THEN $1 WHEN h.content_source IS NULL THEN $2 ELSE $3 END
		FROM torrent_contents tc LEFT JOIN torrent_hints h USING (info_hash)`, hintNone, hintTypeOnly, hintSourced)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	out, hints, several := map[string]attachment{}, map[string]string{}, map[string]bool{}

	for rows.Next() {
		var (
			hash, hint string
			a          attachment
		)

		if err := rows.Scan(&hash, &a.ContentType, &a.Source, &a.ID, &hint); err != nil {
			return nil, nil, err
		}

		if _, dup := out[hash]; dup {
			several[hash] = true
		}

		out[hash], hints[hash] = a, hint
	}

	for h := range several {
		delete(out, h)
	}

	return out, hints, rows.Err()
}

// runFlagsOnRequeue prepares the rematch phase's seed (see above) in a copy
// of Go's drained main round 1.
func runFlagsOnRequeue(args []string) error {
	fs := flag.NewFlagSet("flagson-requeue", flag.ExitOnError)
	dsn := fs.String("dsn", "", "a copy of Go's drained main round 1")
	outDir := fs.String("out", "", "the phase's seed dir (jobs.jsonl, rematchplan.jsonl)")
	jobSize := fs.Int("job-size", 100, "info hashes per job")
	ruleDerived := fs.Int("rule-derived", 2400, "rule-derived torrents to queue again")
	keepHints := fs.Bool("keep-hints", true, "keep type-only hints, as a reprocess does")
	organicRoundOne := fs.String("organic", "",
		"organic round 1's seed dir: requeue its replay slices' torrents as round 2's organic slices (flagson_organic.go)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := errors.Join(requireFlag("dsn", *dsn), requireFlag("out", *outDir)); err != nil {
		return err
	}

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if !*keepHints {
		if _, err := conn.Exec(ctx, `DELETE FROM torrent_hints WHERE content_source IS NULL`); err != nil {
			return err
		}
	}

	before, hints, err := attachments(ctx, conn)
	if err != nil {
		return err
	}

	if *organicRoundOne != "" {
		if before, err = organicReplayAttachments(*organicRoundOne, before); err != nil {
			return err
		}
	}

	plan, better := planRematch(before, hints, *ruleDerived)

	if err := mutateForRematch(ctx, conn, plan, better); err != nil {
		return err
	}

	if *organicRoundOne != "" {
		return requeueOrganic(ctx, conn, *outDir, plan, len(better))
	}

	ts, _ := time.Parse(time.RFC3339Nano, fixtureTime)

	jobs, err := enqueueFlagsOnJobs(ctx, conn, rematchJobs(plan, *jobSize), ts)
	if err != nil {
		return err
	}

	if _, err := conn.Exec(ctx, "ANALYZE"); err != nil {
		return err
	}

	if err := writeJSONLFile(filepath.Join(*outDir, "jobs.jsonl"), jobs); err != nil {
		return err
	}

	if err := writeJSONLFile(filepath.Join(*outDir, "rematchplan.jsonl"), plan); err != nil {
		return err
	}

	counts := map[string]int{}
	for _, t := range plan {
		counts[t.Mode+" "+t.Kind]++
	}

	_, _ = fmt.Fprintf(os.Stderr, "flagson-requeue: %d torrents in %d jobs %v, %d better candidates\n",
		len(plan), len(jobs), counts, len(better))

	return nil
}

// mutateForRematch drops the no-file-list slice's file lists and inserts the
// better candidates.
func mutateForRematch(ctx context.Context, conn *pgx.Conn, plan []rematchTorrent, better []attachment) error {
	batch := &pgx.Batch{}

	for _, t := range plan {
		if t.NoFileList {
			batch.Queue(`UPDATE torrents SET files_status = 'no_info' WHERE info_hash = decode($1, 'hex')`, t.Hash)
		}
	}

	copies := []struct{ table, idColumn, where string }{
		{"content", "id", "type = $1 AND source = $2 AND id = $3"},
		{"content_attributes", "content_id", "content_type = $1 AND content_source = $2 AND content_id = $3"},
		{"content_collections_content", "content_id", "content_type = $1 AND content_source = $2 AND content_id = $3"},
	}

	for _, c := range copies {
		columns, err := storedColumns(ctx, conn, c.table)
		if err != nil {
			return err
		}

		selected := make([]string, len(columns))
		for i, col := range columns {
			selected[i] = col
			if col == c.idColumn {
				selected[i] = fmt.Sprintf("'%s' || %s", betterCandidatePrefix, col)
			}
		}

		insert := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s", c.table,
			strings.Join(columns, ", "), strings.Join(selected, ", "), c.table, c.where)

		for _, a := range better {
			batch.Queue(insert, a.ContentType, a.Source, a.ID)
		}
	}

	return conn.SendBatch(ctx, batch).Close()
}

// storedColumns lists a table's columns that are not generated, in order.
func storedColumns(ctx context.Context, conn *pgx.Conn, table string) ([]string, error) {
	rows, err := conn.Query(ctx, `SELECT column_name FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = $1 AND is_generated = 'NEVER'
		ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, err
	}

	columns, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns for table %s", table)
	}

	return columns, nil
}

// rematchFloor is one count of the rematch coverage, with its floor (0: info).
type rematchFloor struct {
	name       string
	got, floor int
}

// rematchFloors counts, from the plan and Go's round 2, what the rematch phase
// exercised.
func rematchFloors(plan []rematchTorrent, after map[string]attachment) []rematchFloor {
	count := func(keep func(t rematchTorrent, a attachment) bool) int {
		n := 0

		for _, t := range plan {
			if a, ok := after[t.Hash]; ok && keep(t, a) {
				n++
			}
		}

		return n
	}
	rematched := func(t rematchTorrent, a attachment) bool {
		return t.Mode == sliceRematch && t.Kind == kindAttached && a.attached()
	}
	copyOf := func(b attachment) attachment {
		return attachment{ContentType: b.ContentType, Source: b.Source, ID: betterCandidatePrefix + b.ID}
	}

	return []rematchFloor{
		{name: "rematch: attached in round 1 and again", floor: 2000, got: count(rematched)},
		{name: "rematch: attached to other content", floor: 500, got: count(func(t rematchTorrent, a attachment) bool {
			return rematched(t, a) && a != t.Before
		})},
		{name: "  of which: attached to its better candidate", got: count(func(t rematchTorrent, a attachment) bool {
			return rematched(t, a) && t.Better && a == copyOf(t.Before)
		})},
		{name: "rematch: attached to the same content", floor: 500, got: count(func(t rematchTorrent, a attachment) bool {
			return rematched(t, a) && a == t.Before
		})},
		{name: "  of which: its content had a better candidate", got: count(func(t rematchTorrent, a attachment) bool {
			return rematched(t, a) && t.Better && a == t.Before
		})},
		{name: "rule-derived type kept without a file list", floor: 500, got: count(func(t rematchTorrent, a attachment) bool {
			return t.NoFileList && a.ContentType == t.Before.ContentType
		})},
		{name: "  of which: rematch mode", got: count(func(t rematchTorrent, a attachment) bool {
			return t.NoFileList && t.Mode == sliceRematch && a.ContentType == t.Before.ContentType
		})},
		{name: "default mode: sourced attachment reused", floor: 500, got: count(func(t rematchTorrent, a attachment) bool {
			return t.Mode == sliceReuse && t.Kind == kindAttached && a == t.Before
		})},
		{name: "  of which: despite a better candidate", got: count(func(t rematchTorrent, a attachment) bool {
			return t.Mode == sliceReuse && t.Kind == kindAttached && t.Better && a == t.Before
		})},
		{name: "default mode: attached in round 1, other content now", got: count(func(t rematchTorrent, a attachment) bool {
			return t.Mode == sliceReuse && t.Kind == kindAttached && a != t.Before
		})},
		{name: "unmatched in round 1, attached now", got: count(func(t rematchTorrent, a attachment) bool {
			return t.Kind == kindUnmatched && a.attached()
		})},
		{name: "unmatched in round 1 and again", got: count(func(t rematchTorrent, a attachment) bool {
			return t.Kind == kindUnmatched && !a.attached()
		})},
	}
}

// rematchChanges counts, per mode, the requeued torrents and the jobs whose
// attachment round 2 changed: a different torrent_contents row, or no longer
// exactly one. These are information, not floors: a proof in which no
// attachment switches proves little. A job's mode is its torrents' plan mode
// (a job holds one mode), whatever its slice is called.
func rematchChanges(jobs []plannedJob, plan []rematchTorrent, after map[string]attachment) []rematchFloor {
	changed := make(map[string]bool, len(plan))
	modeOf := make(map[string]string, len(plan))

	for _, t := range plan {
		a, ok := after[t.Hash]
		changed[t.Hash] = !ok || a != t.Before
		modeOf[t.Hash] = t.Mode
	}

	modes := []string{sliceRematch, sliceReuse}
	rows := make([]rematchFloor, 0, 5*len(modes))

	for _, mode := range modes {
		var torrents, torrentsChanged, jobCount, jobsChanged int

		for _, t := range plan {
			if t.Mode == mode {
				torrents++

				if changed[t.Hash] {
					torrentsChanged++
				}
			}
		}

		for _, j := range jobs {
			if len(j.InfoHashes) == 0 || modeOf[j.InfoHashes[0]] != mode {
				continue
			}

			jobCount++

			if slices.ContainsFunc(j.InfoHashes, func(h string) bool { return changed[h] }) {
				jobsChanged++
			}
		}

		rows = append(rows,
			rematchFloor{name: mode + " jobs", got: jobCount},
			rematchFloor{name: "  of which: some attachment changed", got: jobsChanged},
			rematchFloor{name: "  of which: every attachment kept", got: jobCount - jobsChanged},
			rematchFloor{name: mode + " torrents: attachment changed", got: torrentsChanged},
			rematchFloor{name: mode + " torrents: attachment kept", got: torrents - torrentsChanged},
		)
	}

	return rows
}

func runFlagsOnRematchCoverage(args []string) error {
	fs := flag.NewFlagSet("flagson-rematch-coverage", flag.ExitOnError)
	dsn := fs.String("dsn", "", "Go's round-2 database")
	seedDir := fs.String("seed", "", "the phase's seed dir (rematchplan.jsonl)")
	floorScale := fs.Float64("floor-scale", 1, "scale every floor (rounded up) to a smaller corpus")

	if err := fs.Parse(args); err != nil {
		return err
	}

	plan, err := readJSONL[rematchTorrent](filepath.Join(*seedDir, "rematchplan.jsonl"), 0)
	if err != nil {
		return err
	}

	jobs, err := readJSONL[plannedJob](filepath.Join(*seedDir, "jobs.jsonl"), 0)
	if err != nil {
		return err
	}

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	after, _, err := attachments(ctx, conn)
	if err != nil {
		return err
	}

	var unmet []string

	_, _ = fmt.Fprintf(os.Stdout, "%-56s %8s %6s\n", "metric", "count", "floor")

	for _, f := range append(rematchFloors(plan, after), rematchChanges(jobs, plan, after)...) {
		f.floor = int(math.Ceil(float64(f.floor) * *floorScale))
		verdict := ""

		switch {
		case f.floor == 0:
		case f.got >= f.floor:
			verdict = verdictMet
		default:
			verdict = verdictNotMet
			unmet = append(unmet, f.name)
		}

		floor := ""
		if f.floor > 0 {
			floor = fmt.Sprint(f.floor)
		}

		_, _ = fmt.Fprintf(os.Stdout, "%-56s %8d %6s %s\n", f.name, f.got, floor, verdict)
	}

	missing := slices.DeleteFunc(slices.Clone(plan), func(t rematchTorrent) bool {
		_, ok := after[t.Hash]
		return ok
	})
	_, _ = fmt.Fprintf(os.Stdout, "requeued torrents without exactly one torrent_contents row after round 2: %d\n", len(missing))

	if len(unmet) > 0 {
		return fmt.Errorf("flagson-rematch-coverage: floors not met: %s", strings.Join(unmet, "; "))
	}

	return nil
}
