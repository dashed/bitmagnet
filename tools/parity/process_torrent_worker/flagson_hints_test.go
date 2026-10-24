package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
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

func TestHintExpectations(t *testing.T) {
	t.Parallel()

	jobs := []plannedJob{
		{Index: 0, Fingerprint: "a", Slice: sliceHint, Expect: sliceExpect[sliceHint]},
		{Index: 1, Fingerprint: "b", Slice: sliceHintBare, Expect: sliceExpect[sliceHintBare]},
		{Index: 2, Fingerprint: "c", Slice: sliceConflict, Expect: sliceExpect[sliceConflict]},
		{Index: 3, Fingerprint: "d", Slice: sliceAmbiguous, Expect: sliceExpect[sliceAmbiguous]},
		{Index: 4, Fingerprint: "e", Slice: sliceHintRefused, Expect: sliceExpect[sliceHintRefused]},
	}

	for _, tc := range []struct {
		mode string
		want map[string]string
	}{
		{hintExpectAbstain, map[string]string{"a": abstainHint, "c": abstainHint, "e": abstainHint}},
		{hintExpectProcess, map[string]string{"c": abstainConflict, "d": abstainAmbiguous, "e": abstainHint}},
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

func TestSliceExpectHasBothGenerations(t *testing.T) {
	t.Parallel()

	for slice, expect := range sliceExpect {
		for _, mode := range []string{hintExpectAbstain, hintExpectProcess} {
			if expect[mode] == "" {
				t.Errorf("slice %s has no %s expectation", slice, mode)
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
			` error=conflicting attached content images for movie:tmdb:9001 reason="conflict" detail=""`,
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
}

func TestSeededOf(t *testing.T) {
	t.Parallel()

	h1, h2 := testHash(1), testHash(2)
	seeded := []string{h1.String() + ":movie:tmdb:1", h1.String() + ":xxx:?:?", h2.String() + ":movie:tmdb:2"}

	if n := seededOf(seeded, [][]byte{h1.Bytes()}); n != 2 {
		t.Fatalf("seededOf = %d, want 2", n)
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
	} {
		got := hintMetrics(tc.category, tc.tape, tc.outcome, seeded)
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}
