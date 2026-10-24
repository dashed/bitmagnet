package main

import (
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

func testIDs(from, n int) []protocol.ID {
	out := make([]protocol.ID, n)
	for i := range out {
		out[i] = testHash(from + i)
	}

	return out
}

// Every job holds one slice, every torrent is queued once, and a small
// group's jobs are spread through a large one's instead of bunched at an end.
func TestPlanHintedJobs(t *testing.T) {
	t.Parallel()

	specs := planHintedJobs([]hintGroup{
		{slice: sliceHint, hashes: testIDs(0, 1000), size: 100},
		{slice: sliceHintRefused, hashes: testIDs(1000, 8), size: 4},
		{slice: sliceHintGoFail, hashes: testIDs(2000, 1), size: 1},
	})

	seen := map[protocol.ID]bool{}
	sizes := map[string][]int{}
	order := make([]string, 0, len(specs))

	for _, spec := range specs {
		order = append(order, spec.slice)
		sizes[spec.slice] = append(sizes[spec.slice], len(spec.hashes))

		if spec.expect == nil || !spec.flagsOn {
			t.Fatalf("job of slice %s: expect %v, flags on %t", spec.slice, spec.expect, spec.flagsOn)
		}

		for _, h := range spec.hashes {
			if seen[h] {
				t.Fatalf("%s queued twice", h)
			}

			seen[h] = true
		}
	}

	if len(seen) != 1009 {
		t.Fatalf("queued %d torrents, want 1009", len(seen))
	}

	if want := []int{100, 100, 100, 100, 100, 100, 100, 100, 100, 100}; !slices.Equal(sizes[sliceHint], want) {
		t.Fatalf("hint job sizes %v", sizes[sliceHint])
	}

	// Hint jobs at 1/20, 3/20, ..., 19/20, refused at 1/4 and 3/4 (each after
	// the hint job at the same place: the group given first wins a tie),
	// go-fail at 1/2.
	want := []string{
		sliceHint, sliceHint, sliceHint, sliceHintRefused, sliceHint, sliceHint,
		sliceHintGoFail, sliceHint, sliceHint, sliceHint, sliceHintRefused, sliceHint, sliceHint,
	}
	if !slices.Equal(order, want) {
		t.Fatalf("job order\n got %v\nwant %v", order, want)
	}
}

// A group may be flags-off, carry extra flags, or give its jobs as they are.
func TestPlanHintedJobsGroupShapes(t *testing.T) {
	t.Parallel()

	given := [][]protocol.ID{testIDs(10, 4), testIDs(0, 1)}
	specs := planHintedJobs([]hintGroup{
		{slice: sliceHintFindFault, jobs: given},
		{slice: slicePayloadExtraOff, hashes: testIDs(100, 6), size: 5, flagsOff: true, extra: map[string]any{"delete_xxx": false}},
	})

	if len(specs) != 4 {
		t.Fatalf("%d jobs, want 4", len(specs))
	}

	for _, spec := range specs {
		switch spec.slice {
		case sliceHintFindFault:
			if !spec.flagsOn || spec.extraFlags != nil {
				t.Fatalf("find-fault job: flags on %t, extra %v", spec.flagsOn, spec.extraFlags)
			}
		default:
			if deleteXXX, ok := spec.extraFlags["delete_xxx"].(bool); spec.flagsOn || !ok || deleteXXX || len(spec.extraFlags) != 1 {
				t.Fatalf("payload job: flags on %t, extra %v", spec.flagsOn, spec.extraFlags)
			}
		}
	}

	// The given jobs keep their order and composition (no info-hash sort).
	if !slices.Equal(specs[0].hashes, given[0]) || !slices.Equal(specs[2].hashes, given[1]) {
		t.Fatalf("given jobs changed: %v, %v", specs[0].hashes, specs[2].hashes)
	}
}

func TestHintExpectations(t *testing.T) {
	t.Parallel()

	jobs := []plannedJob{
		{Index: 0, Fingerprint: "a", Slice: sliceHint, Expect: sliceExpect[sliceHint]},
		{Index: 1, Fingerprint: "b", Slice: sliceHintBare, Expect: sliceExpect[sliceHintBare]},
		{Index: 2, Fingerprint: "c", Slice: sliceConflict, Expect: sliceExpect[sliceConflict]},
		{Index: 3, Fingerprint: "d", Slice: sliceAmbiguous, Expect: sliceExpect[sliceAmbiguous]},
		{Index: 4, Fingerprint: "e", Slice: sliceHintRefused, Expect: sliceExpect[sliceHintRefused]},
		{Index: 5, Fingerprint: "f", Slice: sliceHintFlagsOff, Expect: sliceExpect[sliceHintFlagsOff]},
		{Index: 6, Fingerprint: "g", Slice: sliceEnrichedFlagsOff, Expect: sliceExpect[sliceEnrichedFlagsOff]},
	}

	for _, tc := range []struct {
		mode string
		want map[string]string
	}{
		{hintExpectAbstain, map[string]string{"a": abstainHint, "c": abstainHint, "e": abstainHint, "f": abstainHint, "g": abstainHint}},
		{hintExpectProcess, map[string]string{"c": abstainConflict, "d": abstainAmbiguous, "e": abstainHint}},
		{hintExpectOffline, map[string]string{
			"a": abstainUnclaimed, "b": abstainUnclaimed, "c": abstainUnclaimed, "d": abstainUnclaimed,
			"e": abstainUnclaimed, "f": abstainHint,
		}},
	} {
		got, err := hintExpectations(jobs, tc.mode)
		if err != nil {
			t.Fatal(err)
		}

		if len(got) != len(tc.want) {
			t.Fatalf("%s: %v, want %v", tc.mode, got, tc.want)
		}

		for fp, reason := range tc.want {
			if got[fp] != reason {
				t.Fatalf("%s: %v, want %v", tc.mode, got, tc.want)
			}
		}
	}

	if _, err := hintExpectations(jobs, "flags-off"); err == nil {
		t.Fatal("an unknown HINT_EXPECT was accepted")
	}

	if _, err := hintExpectations([]plannedJob{{Slice: sliceMain}}, hintExpectProcess); !errors.Is(err, errNoHintExpect) {
		t.Fatalf("a job without expectations: %v", err)
	}
}

// Every slice has an expectation for each generation (the payload corpus's
// for M3b and later only), and the flags-off worker never claims a flags-on
// job: every flags-on slice is unclaimed offline, no flags-off one is.
func TestSliceExpectGenerations(t *testing.T) {
	t.Parallel()

	flagsOn := []string{
		sliceHint, sliceEnriched, sliceHintBare, sliceHintRefused, sliceHintGoFail, sliceConflict, sliceAmbiguous,
		sliceAmbiguityControl, sliceHintFindFault, sliceEnrichedRematch,
	}

	// A payload slice's jobs: a top-level key no admission claims
	// (unclaimed), a flag beside the attach flags (payload), or canonical.
	payload, edited := map[string]bool{}, map[string]bool{}

	for _, ps := range payloadSlices {
		payload[ps.slice] = true
		edited[ps.slice] = ps.edit != ""

		// Go runs a flags-dup job under its classifierflags, the opposite.
		goOff := ps.flagsOff != (ps.edit == editFlagsKeyCase)
		if !goOff {
			flagsOn = append(flagsOn, ps.slice)
		}

		want := abstainNone

		switch {
		case ps.edit != "":
			want = abstainUnclaimed
		case ps.extra != nil:
			want = abstainPayload
		}

		if got := sliceExpect[ps.slice][hintExpectProcess]; got != want {
			t.Errorf("payload slice %s: process %q, want %q", ps.slice, got, want)
		}

		if goOff != slices.Contains(flagsOffSlices, ps.slice) {
			t.Errorf("payload slice %s: Go runs it with flags off %t, in flagsOffSlices %t", ps.slice, goOff, !goOff)
		}
	}

	for slice, expect := range sliceExpect {
		modes := []string{hintExpectProcess, hintExpectOffline}
		if !payload[slice] {
			modes = append(modes, hintExpectAbstain)
		}

		if len(expect) != len(modes) {
			t.Errorf("slice %s: %v, want the modes %v", slice, expect, modes)
		}

		for _, mode := range modes {
			if !slices.Contains(abstainReasons, expect[mode]) || expect[mode] == abstainResolver {
				t.Errorf("slice %s: %s expectation %q", slice, mode, expect[mode])
			}
		}

		if edited[slice] {
			if expect[hintExpectOffline] != abstainUnclaimed {
				t.Errorf("slice %s: offline %s, but no admission claims it", slice, expect[hintExpectOffline])
			}

			continue
		}

		if on, unclaimed := slices.Contains(flagsOn, slice), expect[hintExpectOffline] == abstainUnclaimed; on != unclaimed {
			t.Errorf("slice %s: flags on %t, offline %s", slice, on, expect[hintExpectOffline])
		}

		if (expect[hintExpectProcess] == abstainUnclaimed) || (expect[hintExpectAbstain] == abstainUnclaimed) {
			t.Errorf("slice %s: a generation with --admit-flags-on leaves it unclaimed", slice)
		}
	}

	for _, slice := range flagsOffSlices {
		if slices.Contains(flagsOn, slice) || sliceExpect[slice] == nil {
			t.Errorf("flags-off slice %s", slice)
		}
	}
}

// A payload edit keeps the job Go's decoder reads (infohashes binds to
// InfoHashes, Priority is ignored, the later of two case variants wins) and
// fingerprints the edited payload.
func TestPayloadEdit(t *testing.T) {
	t.Parallel()

	job, err := processor.NewQueueJob(processor.MessageParams{
		ClassifierWorkflow: "default", ClassifierFlags: flagsOffFlags(), InfoHashes: testIDs(0, 2),
	})
	if err != nil {
		t.Fatal(err)
	}

	for edit, keys := range map[payloadEdit][]string{
		editHashesKeyCase:   {"ClassifierFlags", "ClassifierWorkflow", "infohashes"},
		editExtraKey:        {"ClassifierFlags", "ClassifierWorkflow", "InfoHashes", "Priority"},
		editFlagsKeyCase:    {"ClassifierFlags", "ClassifierWorkflow", "InfoHashes", "classifierflags"},
		editWorkflowKeyCase: {"ClassifierFlags", "ClassifierWorkflow", "InfoHashes", "classifierworkflow"},
	} {
		edited, err := editedQueueJob(job, edit)
		if err != nil {
			t.Fatal(err)
		}

		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(edited.Payload), &fields); err != nil {
			t.Fatal(err)
		}

		if got := slices.Sorted(maps.Keys(fields)); !slices.Equal(got, keys) {
			t.Errorf("%s: keys %v, want %v", edit, got, keys)
		}

		var params processor.MessageParams
		if err := json.Unmarshal([]byte(edited.Payload), &params); err != nil || !slices.Equal(params.InfoHashes, testIDs(0, 2)) {
			t.Errorf("%s: Go decodes %+v (%v)", edit, params, err)
		}

		// Go runs a flags-dup job under the flags it does not claim by.
		wantFlags := flagsOffFlags()
		if edit == editFlagsKeyCase {
			wantFlags = flagsOnFlags()
		}

		if !maps.Equal(params.ClassifierFlags, wantFlags) || params.ClassifierWorkflow != "default" {
			t.Errorf("%s: Go runs %s with %v", edit, params.ClassifierWorkflow, params.ClassifierFlags)
		}

		want, err := model.NewQueueJob(processor.MessageName, json.RawMessage(edited.Payload), model.QueueJobMaxRetries(2))
		if err != nil || edited.Fingerprint != want.Fingerprint || edited.Fingerprint == job.Fingerprint || edited.MaxRetries != 2 {
			t.Errorf("%s: fingerprint %s, max retries %d (%v)", edit, edited.Fingerprint, edited.MaxRetries, err)
		}
	}

	if _, err := editedQueueJob(job, "rename-all"); err == nil {
		t.Error("an unknown edit applied")
	}
}

// The pseudo-titled slices of one corpus never share a title, so they share
// no search key: pseudoInput repeats every pseudoTitlePeriod indices.
func TestPseudoRangesDisjoint(t *testing.T) {
	t.Parallel()

	title := func(i int) string {
		w := strings.Fields(pseudoInput(i).Name)
		return w[0] + " " + w[1]
	}

	if title(7) != title(7+pseudoTitlePeriod) || title(7) == title(8) {
		t.Fatal("pseudoTitlePeriod is not pseudoInput's period")
	}

	span := func(from, n, step int) []int {
		out := make([]int, n)
		for i := range out {
			out[i] = from + i*step
		}

		return out
	}

	findFaults := 0
	for _, shape := range findFaultJobs {
		size := 1
		if shape.mixed {
			size = 4
		}

		findFaults += size * shape.count
	}

	for corpus, ranges := range map[string][][]int{
		corpusHint: {
			span(0, 5*len(refusedSourced), 2),
			span(2*5*len(refusedSourced)+1000, 3, 2),
			span(hintFlagsOffBase, hintFlagsOffCount, 1),
			span(hintFindFaultBase, findFaults, 1),
		},
		corpusEnriched: {
			span(0, 250, 1),
			span(400, 3*len(refusedEnriched), 1),
			span(500, len(goFailEnriched), 1),
			span(refusedFlagsOffBase, len(refusedEnriched), 1),
			span(enrichedFlagsOffBase, enrichedFlagsOffCount, 1),
			span(enrichedRematchBase, enrichedRematchCount, 1),
		},
	} {
		seen := map[string]int{}

		for _, r := range ranges {
			for _, i := range r {
				if prev, dup := seen[title(i)]; dup {
					t.Fatalf("%s: pseudo inputs %d and %d share the title %q", corpus, prev, i, title(i))
				}

				seen[title(i)] = i
			}
		}
	}
}

func TestFoldedSpelling(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		canonical string
		v         int
		want      string
	}{
		{"V1080p", 0, "v1080p"},
		{"V1080p", 3, "V1080P"},
		{"V1080p", 4, ""},
		{"x264", 0, ""}, // lower case is canonical
		{"x264", 3, "X264"},
		{"CAM", 3, ""}, // upper case is canonical
		{"WEBRip", 1, "webrip"},
	} {
		if got := foldedSpelling(tc.canonical, tc.v); got != tc.want {
			t.Errorf("foldedSpelling(%q, %d) = %q, want %q", tc.canonical, tc.v, got, tc.want)
		}
	}

	// Every spelling is one Go's scan reads back as the canonical value.
	for _, v := range model.VideoSourceValues() {
		for _, k := range []int{0, 3} {
			if s := foldedSpelling(v.String(), k); s != "" {
				got, err := model.ParseVideoSource(s)
				if err != nil || got != v {
					t.Errorf("%q reads back as %q (%v)", s, got, err)
				}
			}
		}
	}
}

// The shapes as Go's encoder writes them to the episodes column.
func TestHintEpisodesShapes(t *testing.T) {
	t.Parallel()

	r := fixtureRand("episodes-test")

	for shape, check := range map[string]func(map[string]map[string]struct{}) bool{
		catEpisodesSeason: func(e map[string]map[string]struct{}) bool {
			return len(e) == 1 && len(e[firstKey(e)]) == 0
		},
		catEpisodesMultiSeason: func(e map[string]map[string]struct{}) bool {
			for _, eps := range e {
				if len(eps) != 0 {
					return false
				}
			}

			return len(e) >= 2
		},
		catEpisodesRange: func(e map[string]map[string]struct{}) bool {
			return len(e) == 1 && len(e[firstKey(e)]) >= 2
		},
		catEpisodesMixed: func(e map[string]map[string]struct{}) bool {
			whole, partial := 0, 0

			for _, eps := range e {
				if len(eps) == 0 {
					whole++
				} else {
					partial++
				}
			}

			return whole == 1 && partial == 1
		},
	} {
		raw, err := json.Marshal(hintEpisodes(r, shape))
		if err != nil {
			t.Fatal(err)
		}

		var decoded map[string]map[string]struct{}
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}

		if !check(decoded) {
			t.Errorf("%s: %s", shape, raw)
		}
	}
}

func firstKey[V any](m map[string]V) string {
	for k := range m {
		return k
	}

	return ""
}

// The M3b abstention lines: hint and ambiguity name a torrent, conflict names
// the content; the reason label must agree with the text.
func TestCheckAbstainLogHintReasons(t *testing.T) {
	t.Parallel()

	h1, h2, h3 := testHash(1).String(), testHash(2).String(), testHash(3).String()
	job := func(n int) string { return "00000000-0000-0000-0000-00000000000" + string(rune('0'+n)) }

	lines := []string{
		`INFO abstaining from unsupported process_torrent job job_id=` + job(1) +
			` error=writer plan cannot reproduce the complete Go hint/enrichment path for ` + h1 + ` reason="hint" detail="content_id"`,
		`INFO abstaining from unsupported process_torrent job job_id=` + job(2) +
			` error=writer plan cannot reproduce Go's reused association for ` + h2 +
			`: more than one is eligible, and Go takes the first in row order reason="ambiguous" detail=""`,
		`INFO abstaining from unsupported process_torrent job job_id=` + job(3) +
			` error=conflicting attached content images for movie:tmdb:9001 reason="unsupported" detail=""`,
	}

	path := filepath.Join(t.TempDir(), "rust.log")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	hashesOf := map[string][]string{job(1): {h1}, job(2): {h2}, job(3): {h3}}
	reasonOf := map[string]string{job(1): abstainHint, job(2): abstainAmbiguous, job(3): abstainConflict}

	n, violations, err := checkAbstainLog(path, reasonOf, hashesOf)
	if err != nil || n != 3 || len(violations) != 0 {
		t.Fatalf("%d lines, %v, %v", n, violations, err)
	}

	// A mislabelled line and an unexpected reason are violations.
	mislabelled := strings.Replace(lines[0], `reason="hint"`, `reason="ambiguous"`, 1)
	if err := os.WriteFile(path, []byte(mislabelled+"\n"+lines[1]+"\n"+lines[2]+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	reasonOf[job(3)] = abstainHint

	_, violations, err = checkAbstainLog(path, reasonOf, hashesOf)
	if err != nil || len(violations) != 4 {
		t.Fatalf("want 4 violations (label, unexpected conflict, two jobs without their line), got %v (%v)", violations, err)
	}
}

// Payload lines name no torrent and carry no label; a reusable conflict is
// labelled conflict, an attached one unsupported; an unclaimed job has no
// line.
func TestCheckAbstainLogPayloadAndConflictLabels(t *testing.T) {
	t.Parallel()

	job := func(n int) string { return "00000000-0000-0000-0000-00000000000" + string(rune('0'+n)) }
	lines := []string{
		` INFO abstaining from process_torrent payload outside Go's key set job_id=` + job(1) +
			` error=classifier flag "delete_xxx" is not an attach flag`,
		` INFO abstaining from undecodable process_torrent payload job_id=` + job(2) + ` error=invalid type: null`,
		` INFO abstaining from unsupported process_torrent job job_id=` + job(3) +
			` error=conflicting reusable content images for movie:tmdb:42 reason="conflict" detail=""`,
	}

	path := filepath.Join(t.TempDir(), "rust.log")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	h := testHash(1).String()
	hashesOf := map[string][]string{job(1): {h}, job(2): {h}, job(3): {h}, job(4): {h}}
	reasonOf := map[string]string{job(1): abstainPayload, job(2): abstainPayload, job(3): abstainConflict, job(4): abstainUnclaimed}

	n, violations, err := checkAbstainLog(path, reasonOf, hashesOf)
	if err != nil || n != 3 || len(violations) != 0 {
		t.Fatalf("%d lines, %v, %v", n, violations, err)
	}

	// The reusable conflict labelled unsupported is a violation, and so is a
	// line for an unclaimed job.
	lines[2] = strings.Replace(lines[2], `reason="conflict"`, `reason="unsupported"`, 1)
	reasonOf[job(1)] = abstainUnclaimed

	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, violations, err = checkAbstainLog(path, reasonOf, hashesOf); err != nil || len(violations) != 3 {
		t.Fatalf("want 3 violations (label, unexpected payload, unclaimed job with a line), got %v (%v)", violations, err)
	}
}

func TestCheckNoResolverCalls(t *testing.T) {
	t.Parallel()

	h1, h2 := testHash(1).String(), testHash(2).String()
	log := "DEBUG content resolver call started info_hash=" + h1 + " call=\"tmdb.find\"\n" +
		"DEBUG content resolver call started info_hash=" + h2 + " call=\"local.search\"\n"

	path := filepath.Join(t.TempDir(), "rust.log")
	if err := os.WriteFile(path, []byte(log), 0o600); err != nil {
		t.Fatal(err)
	}

	// A conflict job is classified before it is refused; a hint job is not.
	calls, violations, err := checkNoResolverCalls(path,
		map[string]string{"a": abstainHint, "b": abstainConflict},
		map[string][]string{"a": {h1}, "b": {h2}})
	if err != nil || calls != 1 || len(violations) != 1 || !strings.Contains(violations[0], h1) {
		t.Fatalf("%d calls, %v, %v", calls, violations, err)
	}

	// Nor is a payload job, and an unclaimed one never is.
	for _, reason := range []string{abstainPayload, abstainUnclaimed} {
		calls, violations, err = checkNoResolverCalls(path, map[string]string{"a": reason}, map[string][]string{"a": {h2}})
		if err != nil || calls != 1 || len(violations) != 1 {
			t.Fatalf("%s: %d calls, %v, %v", reason, calls, violations, err)
		}
	}
}

func TestSeededOf(t *testing.T) {
	t.Parallel()

	h1, h2 := testHash(1), testHash(2)
	seeded := []string{h1.String() + ":movie:tmdb:1", h1.String() + ":xxx:?:?", h2.String() + ":movie:tmdb:2"}

	if n := seededOf(seeded, [][]byte{h1.Bytes()}); n != 2 {
		t.Fatalf("seededOf = %d, want 2", n)
	}
}

// organic2's seed has no seeded-content.txt: no TMDB id counts as seeded.
func TestReadSeededTMDB(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	ids, err := readSeededTMDB(dir)
	if err != nil || len(ids) != 0 {
		t.Fatalf("without the file: %v, %v", ids, err)
	}

	content := "movie/tmdb/7\nmovie/TVmaze/8\ntv_show/tmdb/9\n"
	if err := os.WriteFile(filepath.Join(dir, "seeded-content.txt"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	ids, err = readSeededTMDB(dir)
	if err != nil || len(ids) != 2 || !ids["7"] || !ids["9"] {
		t.Fatalf("got %v, %v; want 7 and 9", ids, err)
	}
}

// Every ambiguous shape's associations go in so that Go's first (heap order)
// is the last by id: a pick by id order cannot agree with Go's by accident.
func TestReverseByID(t *testing.T) {
	t.Parallel()

	tc := func(ct model.ContentType, id string) model.TorrentContent {
		c := model.TorrentContent{InfoHash: testHash(1), ContentType: model.NewNullContentType(ct)}
		if id != "" {
			c.ContentSource, c.ContentID = model.NewNullString(model.SourceTmdb), model.NewNullString(id)
		}

		return c
	}
	tcs := []model.TorrentContent{
		tc(model.ContentTypeMovie, "9963"), tc(model.ContentTypeMovie, "10000"),
		tc(model.ContentTypeTvShow, "10037"), tc(model.ContentTypeXxx, ""), tc(model.ContentTypeMovie, ""),
	}

	reverseByID(tcs)

	got := make([]string, len(tcs))
	for i, c := range tcs {
		got[i] = strings.TrimPrefix(c.InferID(), testHash(1).String()+":")
	}

	want := []string{"xxx:?:?", "tv_show:tmdb:10037", "movie:tmdb:9963", "movie:tmdb:10000", "movie:?:?"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestHintMetrics(t *testing.T) {
	t.Parallel()

	str := func(s string) *string { return &s }
	seeded := map[string]bool{"assoc": true}

	for _, tc := range []struct {
		name, category string
		tape           hintTape
		outcome        *hintOutcome
		want           []string
	}{
		{
			name: "imdb find then details", category: catFindNew,
			tape:    hintTape{finds: []string{"imdb_id"}, findDetails: true, tmdbRequests: 2, observations: 3},
			outcome: &hintOutcome{hint: map[string]*string{}, tcs: []hintTC{{id: "new", touched: true, values: map[string]*string{}}}},
			want:    []string{metricImdbFind},
		},
		{
			name: "pre-attached", category: catAssociationCanonical,
			outcome: &hintOutcome{hint: map[string]*string{}, tcs: []hintTC{{id: "assoc", touched: true, values: map[string]*string{}}}},
			want:    []string{metricPreAttached},
		},
		{
			name: "stale replaced", category: catAssociationOther,
			tape:    hintTape{byIDDetails: true, tmdbRequests: 1},
			outcome: &hintOutcome{hint: map[string]*string{}, tcs: []hintTC{{id: "new", touched: true, values: map[string]*string{}}}},
			want:    []string{metricTmdbByID, metricStaleGone},
		},
		{
			name: "stale left", category: catAssociationOther,
			outcome: &hintOutcome{hint: map[string]*string{}, tcs: []hintTC{{id: "assoc", values: map[string]*string{}}}},
		},
		{
			name: "folded video and an empty group", category: catVideo,
			outcome: &hintOutcome{
				hint: map[string]*string{"video_resolution": str("v1080p"), "release_group": str("")},
				tcs: []hintTC{{id: "x", touched: true, values: map[string]*string{
					"video_resolution": str("V1080p"), "release_group": str(""),
				}}},
			},
			want: []string{"video_resolution applied", metricFolded, metricGroupEmpty},
		},
		{
			name: "video not applied", category: catVideo,
			outcome: &hintOutcome{
				hint: map[string]*string{"video_codec": str("x264")},
				tcs:  []hintTC{{id: "x", touched: true, values: map[string]*string{"video_codec": str("x265")}}},
			},
		},
		{
			name: "find 404 failed", category: catFindFault404,
			tape:    hintTape{finds: []string{"imdb_id"}, tmdbRequests: 1},
			outcome: &hintOutcome{hint: map[string]*string{}},
			want:    []string{metricFind404Failed},
		},
		{
			name: "find 500 never asked", category: catFindFault500,
			outcome: &hintOutcome{hint: map[string]*string{}},
		},
		{
			name: "details 404 then searched", category: catFindFaultDetails,
			tape:    hintTape{finds: []string{"imdb_id"}, findDetails: true, searched: true, tmdbRequests: 3},
			outcome: &hintOutcome{hint: map[string]*string{}, tcs: []hintTC{{id: "h:movie:?:?", touched: true, values: map[string]*string{}}}},
			want:    []string{metricImdbFind, metricFindDetails404},
		},
		{
			name: "flags-off attached locally", category: catOffLocalMulti,
			outcome: &hintOutcome{hint: map[string]*string{}, tcs: []hintTC{{id: "h:movie:tmdb:1", touched: true, values: map[string]*string{}}}},
			want:    []string{metricOffAttached},
		},
		{
			name: "flags-off with a request", category: catOffLocalImdb,
			tape:    hintTape{tmdbRequests: 1},
			outcome: &hintOutcome{hint: map[string]*string{}, tcs: []hintTC{{id: "h:movie:tmdb:1", touched: true, values: map[string]*string{}}}},
		},
		{
			name: "flags-off unmatched", category: catOffUnmatched,
			outcome: &hintOutcome{hint: map[string]*string{}, tcs: []hintTC{{id: "h:movie:?:?", touched: true, values: map[string]*string{}}}},
			want:    []string{metricOffUnmatched},
		},
		{
			name: "rematch replaced the association", category: catRematchVideo,
			tape: hintTape{searched: true, tmdbRequests: 1},
			outcome: &hintOutcome{
				hint: map[string]*string{"video_source": str("BluRay")},
				tcs:  []hintTC{{id: "new", touched: true, values: map[string]*string{"video_source": str("BluRay")}}},
			},
			want: []string{"video_source applied", metricRematchVideo},
		},
		{
			name: "rematch kept the association", category: catRematchVideo,
			outcome: &hintOutcome{
				hint: map[string]*string{"video_source": str("BluRay")},
				tcs:  []hintTC{{id: "assoc", touched: true, values: map[string]*string{"video_source": str("BluRay")}}},
			},
			want: []string{"video_source applied"},
		},
	} {
		got := hintMetrics(tc.category, tc.tape, tc.outcome, seeded)
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}
