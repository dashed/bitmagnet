package main

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"
)

func testHashes(from, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = testHash(from + i).String()
	}

	return out
}

// Absent attach flags resolve to Go's defaults (all on), not to false
// (mutant 17).
func TestOrganicFlagsOn(t *testing.T) {
	t.Parallel()

	defaults := goFlagDefaults()

	for _, k := range attachFlags {
		if v, ok := defaults[k].(bool); !ok || !v {
			t.Fatalf("Go's default %s = %v, want true", k, defaults[k])
		}
	}

	for _, tc := range []struct {
		payload string
		on      bool
	}{
		{`{}`, true},
		{`{"ClassifierFlags":{}}`, true},
		{`{"ClassifierFlags":{"tmdb_enabled":true}}`, true},
		{`{"ClassifierFlags":{"local_search_enabled":true,"apis_enabled":true,"tmdb_enabled":true}}`, true},
		{`{"ClassifierFlags":{"tmdb_enabled":false}}`, false},
		{`{"ClassifierFlags":{"local_search_enabled":false}}`, false},
		{`{"ClassifierFlags":{"tmdb_enabled":"true"}}`, false},
		{`{"ClassifierFlags":{"apis_enabled":null}}`, false},
	} {
		var params struct {
			ClassifierFlags map[string]any `json:"ClassifierFlags"`
		}

		if err := json.Unmarshal([]byte(tc.payload), &params); err != nil {
			t.Fatal(err)
		}

		if got := organicFlagsOn(params.ClassifierFlags, defaults); got != tc.on {
			t.Errorf("%s: flags on %t, want %t", tc.payload, got, tc.on)
		}
	}
}

// The in-process producers' payload bytes and priorities.
func TestOrganicJobShapes(t *testing.T) {
	t.Parallel()

	hashes := testHashes(0, 2)
	sorted := slices.Sorted(slices.Values(hashes))
	array := func(hs []string) string {
		b, err := json.Marshal(hs)
		if err != nil {
			t.Fatal(err)
		}

		return string(b)
	}

	for _, tc := range []struct {
		slice, payload string
		priority       int
		flagsOn        bool
	}{
		{"dht", `{"InfoHashes":` + array(hashes) + `}`, 0, true},
		{"importer", `{"InfoHashes":` + array(hashes) + `}`, 20, true},
		{"retry", `{"ClassifierWorkflow":"default","InfoHashes":` + array(hashes) + `}`, 0, true},
		{"empty-workflow", `{"ClassifierWorkflow":"","InfoHashes":` + array(hashes) + `}`, 0, true},
		{"fault-rematch", `{"ClassifyMode":1,"InfoHashes":` + array(sorted) + `}`, 10, true},
		{
			"fault-local-off", `{"ClassifierFlags":{"local_search_enabled":false},"InfoHashes":` + array(sorted) + `}`,
			10, false,
		},
		{"negative-workflow-null", `{"ClassifierWorkflow":null,"InfoHashes":` + array(hashes) + `}`, 0, true},
		{"negative-mode-float", `{"ClassifyMode":1.0,"InfoHashes":` + array(hashes) + `}`, 0, true},
		{
			"negative-flag-string", `{"ClassifierFlags":{"tmdb_enabled":"true"},"InfoHashes":` + array(hashes) + `}`,
			0, false,
		},
		{"negative-hash-upper", `{"InfoHashes":` + strings.ToUpper(array(hashes)) + `}`, 0, true},
		{"negative-not-object", `[]`, 0, true},
	} {
		sl, ok := organicSliceByName(tc.slice)
		if !ok {
			t.Fatalf("no slice %s", tc.slice)
		}

		job, err := organicJob(sl, hashes)
		if err != nil {
			t.Fatal(err)
		}

		if job.Payload != tc.payload || job.Priority != tc.priority || job.FlagsOn != tc.flagsOn {
			t.Errorf("%s: payload %s priority %d flags on %t, want %s %d %t", tc.slice,
				job.Payload, job.Priority, job.FlagsOn, tc.payload, tc.priority, tc.flagsOn)
		}

		if job.MaxRetries != 2 || job.ArchivalDuration != (7*24*time.Hour).String() {
			t.Errorf("%s: max retries %d, archival %s", tc.slice, job.MaxRetries, job.ArchivalDuration)
		}
	}
}

// Every negative payload is JSON (Rust must refuse its shape, not fail to
// parse it), and hashes-150 holds 150 hashes.
func TestOrganicNegativePayloads(t *testing.T) {
	t.Parallel()

	n := 0

	for _, sl := range organicRound1 {
		if sl.role != organicRoleNegative {
			continue
		}

		n++

		payload := sl.raw(testHashes(0, sl.hashes))
		if !json.Valid([]byte(payload)) {
			t.Errorf("%s: not JSON: %s", sl.name, payload)
		}

		if sl.name == "negative-hashes-150" && strings.Count(payload, `"`) != 2*(150+1) {
			t.Errorf("%s: %s", sl.name, payload)
		}
	}

	if n < 20 {
		t.Errorf("%d negative slices", n)
	}
}

// Enqueue inserts the payload bytes as the producer wrote them (mutant 18).
func TestRawJobArgsVerbatim(t *testing.T) {
	t.Parallel()

	job := plannedJob{
		Fingerprint: "f", Payload: `{"InfoHashes": ["AB"], "ClassifyMode":1.0}`, Priority: 4, MaxRetries: 2,
		ArchivalDuration: "168h0m0s",
	}
	runAfter := time.Unix(0, 0)

	want := []any{"f", processTorrentQueue, job.Payload, uint(2), runAfter, "168h0m0s", 4}
	if got := rawJobArgs(job, runAfter); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("args %v, want %v", got, want)
	}
}

func TestOrganicLeft(t *testing.T) {
	t.Parallel()

	jobs := []plannedJob{
		{Slice: "dht", Fingerprint: "dht", Priority: 0},
		{Slice: "graphql-apis-off", Fingerprint: "apis-off", Priority: 4},
		{Slice: "graphql", Fingerprint: "graphql", Priority: 10},
		{Slice: "importer", Fingerprint: "importer", Priority: 20},
		{Slice: "hinted", Fingerprint: "hinted", Priority: 0},
		{Slice: "negative-mode-2", Fingerprint: "negative", Priority: 0},
		{Slice: "fault-rematch", Fingerprint: "fault", Priority: 10},
	}

	for _, tc := range []struct {
		bound int
		want  map[string]string
	}{
		{20, map[string]string{"hinted": "hint", "negative": "none"}},
		{10, map[string]string{"hinted": "hint", "negative": "none", "importer": "none"}},
		{0, map[string]string{
			"hinted": "hint", "negative": "none", "importer": "none", "graphql": "none", "apis-off": "none", "fault": "none",
		}},
	} {
		if got := organicLeft(jobs, tc.bound); !maps.Equal(got, tc.want) {
			t.Errorf("bound %d: %v, want %v", tc.bound, got, tc.want)
		}
	}
}

func TestOrganicRound2Slices(t *testing.T) {
	t.Parallel()

	plan := make([]rematchTorrent, 0, 1000)

	for i, h := range testHashes(0, 1000) {
		mode := sliceReuse
		if i%3 != 0 {
			mode = sliceRematch
		}

		plan = append(plan, rematchTorrent{Hash: h, Mode: mode})
	}

	modeOf := map[string]string{}
	for _, p := range plan {
		modeOf[p.Hash] = p.Mode
	}

	got := organicRound2Slices(plan)
	total := 0

	for i, sl := range organicRound2 {
		want := sliceReuse
		if i < organicRound2Rematch {
			want = sliceRematch
		}

		if len(got[sl.name]) == 0 {
			t.Errorf("slice %s: no jobs", sl.name)
		}

		for _, job := range got[sl.name] {
			if len(job) > organicJobSize {
				t.Errorf("slice %s: a job of %d hashes", sl.name, len(job))
			}

			for _, h := range job {
				total++

				if modeOf[h] != want {
					t.Errorf("slice %s: %s of mode %s", sl.name, h, modeOf[h])
				}
			}
		}
	}

	if total != len(plan) {
		t.Errorf("%d hashes in round 2's slices, want %d", total, len(plan))
	}
}

func TestInterleaveSlices(t *testing.T) {
	t.Parallel()

	bySlice := map[string][]plannedJob{
		"a": {{Fingerprint: "a0"}, {Fingerprint: "a1"}, {Fingerprint: "a2"}},
		"b": {{Fingerprint: "b0"}},
		"c": {{Fingerprint: "c0"}, {Fingerprint: "c1"}},
	}

	got := make([]string, 0, 6)

	for i, j := range interleaveSlices([]string{"a", "b", "c"}, bySlice) {
		if j.Index != i {
			t.Errorf("job %s has index %d, want %d", j.Fingerprint, j.Index, i)
		}

		got = append(got, j.Fingerprint)
	}

	if want := []string{"a0", "b0", "c0", "a1", "c1", "a2"}; !slices.Equal(got, want) {
		t.Errorf("order %v, want %v", got, want)
	}
}
