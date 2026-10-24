package parity

import (
	"bytes"
	"fmt"
	"strings"
)

// Options controls normalization and diagnostic collection.
type Options struct {
	Normalizer Normalizer // CanonicalJSON when nil.
	MaxDiffs   int        // 20 when zero.
}

// Diff describes one mismatched or errored fixture.
type Diff struct {
	ID       string
	Expected string
	Got      string
	Err      string
}

// Report summarizes a differential harness run.
type Report struct {
	Total      int
	Ran        int
	Matched    int
	Mismatched int
	Errored    int
	Diffs      []Diff
}

// OK reports whether every fixture run matched its expected value.
func (r Report) OK() bool {
	return r.Mismatched == 0 && r.Errored == 0
}

// String returns a human-readable summary followed by the collected diffs.
func (r Report) String() string {
	var out strings.Builder
	fmt.Fprintf(
		&out,
		"total=%d ran=%d matched=%d mismatched=%d errored=%d",
		r.Total,
		r.Ran,
		r.Matched,
		r.Mismatched,
		r.Errored,
	)

	for index, diff := range r.Diffs {
		fmt.Fprintf(&out, "\n\ndiff %d: id=%q", index+1, diff.ID)
		if diff.Err != "" {
			fmt.Fprintf(&out, "\n  error: %s", diff.Err)
		}
		if diff.Expected != "" {
			fmt.Fprintf(&out, "\n  expected: %s", diff.Expected)
		}
		if diff.Got != "" {
			fmt.Fprintf(&out, "\n  got: %s", diff.Got)
		}
	}

	omitted := r.Mismatched + r.Errored - len(r.Diffs)
	if omitted > 0 {
		fmt.Fprintf(&out, "\n\n... %d additional diffs omitted", omitted)
	}

	return out.String()
}

// Run applies d to fixtures for its subsystem and compares normalized output
// with each fixture's normalized expected value.
func Run(fixtures []Fixture, d Driver, opts Options) Report {
	normalizer := opts.Normalizer
	if normalizer == nil {
		normalizer = CanonicalJSON
	}

	maxDiffs := opts.MaxDiffs
	if maxDiffs == 0 {
		maxDiffs = 20
	}

	report := Report{Total: len(fixtures)}
	appendDiff := func(diff Diff) {
		if len(report.Diffs) < maxDiffs {
			report.Diffs = append(report.Diffs, diff)
		}
	}

	subsystem := d.Subsystem()
	for _, fixture := range fixtures {
		if fixture.Subsystem != subsystem {
			continue
		}

		report.Ran++
		got, err := d.Run(fixture.Input)
		if err != nil {
			report.Errored++
			appendDiff(Diff{ID: fixture.ID, Err: err.Error()})
			continue
		}

		normalizedGot, err := normalizer(got)
		if err != nil {
			report.Errored++
			appendDiff(Diff{
				ID:       fixture.ID,
				Expected: string(fixture.Expected),
				Got:      string(got),
				Err:      fmt.Sprintf("normalize driver output: %v", err),
			})
			continue
		}

		normalizedExpected, err := normalizer(fixture.Expected)
		if err != nil {
			report.Errored++
			appendDiff(Diff{
				ID:       fixture.ID,
				Expected: string(fixture.Expected),
				Got:      string(normalizedGot),
				Err:      fmt.Sprintf("normalize expected value: %v", err),
			})
			continue
		}

		if bytes.Equal(normalizedGot, normalizedExpected) {
			report.Matched++
			continue
		}

		report.Mismatched++
		appendDiff(Diff{
			ID:       fixture.ID,
			Expected: string(normalizedExpected),
			Got:      string(normalizedGot),
		})
	}

	return report
}
