package main

import (
	"fmt"
	"slices"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/processor"
)

// rematchFixture: 200 torrents attached to 50 contents, 50 unmatched, 100
// rule-derived (every fifth with a type-only hint), 10 of unknown type and 10
// with a sourced hint.
func rematchFixture() (map[string]attachment, map[string]string) {
	before, hints := map[string]attachment{}, map[string]string{}
	n := 0
	add := func(a attachment, hint string) {
		h := testHash(n).String()
		n++
		before[h], hints[h] = a, hint
	}

	for i := range 200 {
		add(attachment{ContentType: "movie", Source: "tmdb", ID: fmt.Sprint(100 + i%50)}, hintNone)
	}

	for range 50 {
		add(attachment{ContentType: "tv_show"}, hintNone)
	}

	for i := range 100 {
		hint := hintNone
		if i%5 == 0 {
			hint = hintTypeOnly
		}

		add(attachment{ContentType: "xxx"}, hint)
	}

	for range 10 {
		add(attachment{}, hintNone)
	}

	for range 10 {
		add(attachment{ContentType: "movie", Source: "tmdb", ID: "7"}, hintSourced)
	}

	return before, hints
}

func TestPlanRematch(t *testing.T) {
	t.Parallel()

	before, hints := rematchFixture()

	plan, better := planRematch(before, hints, 60)

	if again, betterAgain := planRematch(before, hints, 60); !slices.Equal(plan, again) || !slices.Equal(better, betterAgain) {
		t.Fatal("the plan is not deterministic")
	}

	kinds, modes := map[string]int{}, map[string]int{}
	betterOf := map[attachment]bool{}

	for _, tr := range plan {
		kinds[tr.Kind]++
		modes[tr.Mode]++

		if tr.Before != before[tr.Hash] {
			t.Errorf("%s: before %v, want %v", tr.Hash, tr.Before, before[tr.Hash])
		}

		if hints[tr.Hash] == hintSourced || tr.Before.ContentType == "" {
			t.Errorf("%s: requeued with hint %q, type %q", tr.Hash, hints[tr.Hash], tr.Before.ContentType)
		}

		if tr.NoFileList && (tr.Kind != kindRuleDerived || hints[tr.Hash] != hintNone) {
			t.Errorf("%s: no file list on a %s torrent with hint %q", tr.Hash, tr.Kind, hints[tr.Hash])
		}

		if tr.Kind == kindAttached {
			if b, seen := betterOf[tr.Before]; seen && b != tr.Better {
				t.Errorf("content %v: torrents disagree on its better candidate", tr.Before)
			}

			betterOf[tr.Before] = tr.Better
		}
	}

	if want := map[string]int{kindAttached: 200, kindUnmatched: 50, kindRuleDerived: 60}; fmt.Sprint(kinds) != fmt.Sprint(want) {
		t.Errorf("kinds %v, want %v", kinds, want)
	}

	if modes[sliceRematch] == 0 || modes[sliceReuse] == 0 {
		t.Errorf("modes %v, want both", modes)
	}

	var wantBetter []attachment

	for a, b := range betterOf {
		if b {
			wantBetter = append(wantBetter, a)
		}
	}

	if len(better) != len(wantBetter) || len(better) == 0 || len(better) == len(betterOf) {
		t.Errorf("%d better candidates of %d contents, want a share of them, each once", len(better), len(betterOf))
	}

	for _, a := range better {
		if !betterOf[a] {
			t.Errorf("better candidate for %v, whose torrents don't say so", a)
		}
	}
}

func TestRematchJobs(t *testing.T) {
	t.Parallel()

	before, hints := rematchFixture()
	plan, _ := planRematch(before, hints, 60)
	modeOf := map[string]string{}

	for _, tr := range plan {
		modeOf[tr.Hash] = tr.Mode
	}

	specs := rematchJobs(plan, 25)
	seen := map[string]bool{}

	for i, spec := range specs {
		wantMode := processor.ClassifyModeDefault
		if spec.slice == sliceRematch {
			wantMode = processor.ClassifyModeRematch
		}

		if spec.classifyMode != wantMode || !spec.flagsOn || len(spec.hashes) > 25 {
			t.Errorf("job %d: slice %s, mode %d, flags on %t, %d hashes", i, spec.slice, spec.classifyMode, spec.flagsOn, len(spec.hashes))
		}

		if i > 0 && i < 4 && spec.slice == specs[i-1].slice {
			t.Errorf("jobs %d and %d are both %s: the modes alternate", i-1, i, spec.slice)
		}

		for _, h := range spec.hashes {
			if modeOf[h.String()] != spec.slice || seen[h.String()] {
				t.Errorf("job %d: %s (mode %s, seen %t)", i, h, modeOf[h.String()], seen[h.String()])
			}

			seen[h.String()] = true
		}
	}

	if len(seen) != len(plan) {
		t.Errorf("%d torrents queued, want %d", len(seen), len(plan))
	}
}

func TestRematchFloors(t *testing.T) {
	t.Parallel()

	content := attachment{ContentType: "movie", Source: "tmdb", ID: "12"}
	copied := attachment{ContentType: "movie", Source: "tmdb", ID: "012"}
	other := attachment{ContentType: "movie", Source: "local", ID: "a1"}

	plan := []rematchTorrent{
		{Hash: "a", Mode: sliceRematch, Kind: kindAttached, Before: content, Better: true},
		{Hash: "b", Mode: sliceRematch, Kind: kindAttached, Before: content, Better: true},
		{Hash: "c", Mode: sliceRematch, Kind: kindAttached, Before: content},
		{Hash: "d", Mode: sliceReuse, Kind: kindAttached, Before: content, Better: true},
		{Hash: "e", Mode: sliceReuse, Kind: kindAttached, Before: content},
		{Hash: "f", Mode: sliceRematch, Kind: kindRuleDerived, Before: attachment{ContentType: "xxx"}, NoFileList: true},
		{Hash: "g", Mode: sliceReuse, Kind: kindRuleDerived, Before: attachment{ContentType: "xxx"}, NoFileList: true},
		{Hash: "h", Mode: sliceReuse, Kind: kindUnmatched, Before: attachment{ContentType: "movie"}},
		{Hash: "i", Mode: sliceRematch, Kind: kindUnmatched, Before: attachment{ContentType: "movie"}},
		{Hash: "j", Mode: sliceRematch, Kind: kindAttached, Before: content}, // deleted in round 2
	}
	after := map[string]attachment{
		"a": copied, "b": content, "c": other, "d": content, "e": other,
		"f": {ContentType: "xxx"}, "g": {},
		"h": other, "i": {ContentType: "movie"},
	}

	got := map[string]int{}
	for _, f := range rematchFloors(plan, after) {
		got[f.name] = f.got
	}

	jobs := []plannedJob{
		{Slice: sliceRematch, InfoHashes: []string{"a", "b"}},      // a changed
		{Slice: sliceRematch, InfoHashes: []string{"f", "i"}},      // both kept
		{Slice: sliceRematch, InfoHashes: []string{"c", "j"}},      // c changed, j's row deleted
		{Slice: sliceReuse, InfoHashes: []string{"d", "g", "h"}},   // g and h changed
		{Slice: sliceReuse, InfoHashes: []string{"e"}},             // e changed
		{Slice: sliceMissing, InfoHashes: []string{"not-planned"}}, // neither mode
	}
	wantChanges := []rematchFloor{
		{name: "rematch jobs", got: 3},
		{name: "  of which: some attachment changed", got: 2},
		{name: "  of which: every attachment kept", got: 1},
		{name: "rematch torrents: attachment changed", got: 3},
		{name: "rematch torrents: attachment kept", got: 3},
		{name: "reuse jobs", got: 2},
		{name: "  of which: some attachment changed", got: 2},
		{name: "  of which: every attachment kept", got: 0},
		{name: "reuse torrents: attachment changed", got: 3},
		{name: "reuse torrents: attachment kept", got: 1},
	}
	if changes := rematchChanges(jobs, plan, after); !slices.Equal(changes, wantChanges) {
		t.Errorf("changes %v, want %v", changes, wantChanges)
	}

	for name, want := range map[string]int{
		"rematch: attached in round 1 and again":               3,
		"rematch: attached to other content":                   2,
		"rematch: attached to the same content":                1,
		"  of which: attached to its better candidate":         1,
		"  of which: its content had a better candidate":       1,
		"rule-derived type kept without a file list":           1,
		"  of which: rematch mode":                             1,
		"default mode: sourced attachment reused":              1,
		"  of which: despite a better candidate":               1,
		"default mode: attached in round 1, other content now": 1,
		"unmatched in round 1, attached now":                   1,
		"unmatched in round 1 and again":                       1,
	} {
		if got[name] != want {
			t.Errorf("%s: %d, want %d", name, got[name], want)
		}
	}
}
