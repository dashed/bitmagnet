package graphqlshadow

import (
	"testing"
	"time"
)

func TestCompareGraphQLIdentical(t *testing.T) {
	t.Parallel()

	res := GraphQLResult{
		IDs:        []string{"a", "b", "c"},
		TotalCount: 3,
		Facets: map[string]FacetCounts{
			"content_type": {"movie": 2, "tv_show": 1},
			"language":     {"en": 3},
		},
		ObservedFacets: map[string]bool{"content_type": true, "language": true},
	}

	c := CompareGraphQL(res, res, time.Millisecond, time.Millisecond)

	if !c.Top1Match {
		t.Error("Top1Match = false, want true")
	}

	if c.JaccardAt20 != 1.0 || c.RBO != 1.0 {
		t.Errorf("similarity not perfect: jaccard@20=%v rbo=%v", c.JaccardAt20, c.RBO)
	}

	if !c.TotalCountMatch || c.TotalCountDelta != 0 {
		t.Errorf("total-count mismatch: match=%v delta=%d", c.TotalCountMatch, c.TotalCountDelta)
	}

	if c.AllFacetsObserved || c.AllFacetsMatch || c.FacetsObserved != 2 || c.FacetsMatched != 2 {
		t.Errorf(
			"facet observation mismatch: all_observed=%v all_match=%v observed=%d matched=%d",
			c.AllFacetsObserved,
			c.AllFacetsMatch,
			c.FacetsObserved,
			c.FacetsMatched,
		)
	}

	if len(c.FacetDeltas) != 0 {
		t.Errorf("expected no facet deltas, got %v", c.FacetDeltas)
	}

	if c.IsDiscrepancy() {
		t.Error("IsDiscrepancy = true on identical results")
	}
}

func TestCompareGraphQLEmptyResultsAreNotDiscrepancy(t *testing.T) {
	t.Parallel()

	empty := GraphQLResult{}
	c := CompareGraphQL(empty, empty, time.Millisecond, time.Millisecond)

	if c.Top1Match {
		t.Error("Top1Match = true with no rank-0 result")
	}

	if c.IsDiscrepancy() {
		t.Error("IsDiscrepancy = true for identical empty results")
	}
}

func TestCompareGraphQLTotalCountDeltaSign(t *testing.T) {
	t.Parallel()

	// Convention: delta is reference(go) - candidate(rust).
	rust := GraphQLResult{IDs: []string{"a"}, TotalCount: 90}
	ref := GraphQLResult{IDs: []string{"a"}, TotalCount: 100}

	c := CompareGraphQL(rust, ref, time.Millisecond, time.Millisecond)

	if c.TotalCountDelta != 10 {
		t.Errorf("TotalCountDelta = %d, want 10 (ref 100 - rust 90)", c.TotalCountDelta)
	}

	if c.TotalCountMatch {
		t.Error("TotalCountMatch = true, want false")
	}

	if !c.IsDiscrepancy() {
		t.Error("IsDiscrepancy = false despite a total-count mismatch")
	}
}

func TestCompareGraphQLFacetDeltas(t *testing.T) {
	t.Parallel()

	rust := GraphQLResult{
		IDs: []string{"a", "b"},
		Facets: map[string]FacetCounts{
			"content_type": {"movie": 5, "tv_show": 3}, // tv_show differs, extra "audiobook" on ref
			"language":     {"en": 10},                 // identical
			"file_type":    {"video": 4},               // rust-only value "archive" absent
		},
		ObservedFacets: map[string]bool{"content_type": true, "language": true, "file_type": true},
	}
	ref := GraphQLResult{
		IDs: []string{"a", "b"},
		Facets: map[string]FacetCounts{
			"content_type": {"movie": 5, "tv_show": 4, "audiobook": 1},
			"language":     {"en": 10},
			"file_type":    {"video": 4, "archive": 2},
		},
		ObservedFacets: map[string]bool{"content_type": true, "language": true, "file_type": true},
	}

	c := CompareGraphQL(rust, ref, time.Millisecond, time.Millisecond)

	if !c.FacetMatch["language"] {
		t.Error("language facet should match")
	}

	if c.FacetMatch["content_type"] || c.FacetMatch["file_type"] {
		t.Error("content_type and file_type facets should not match")
	}

	// content_type: tv_show ref4-rust3=+1; audiobook ref1-rust0=+1; movie equal (absent).
	ctDeltas := c.FacetDeltas["content_type"]
	if ctDeltas["tv_show"] != 1 || ctDeltas["audiobook"] != 1 {
		t.Errorf("content_type deltas = %v, want tv_show:+1 audiobook:+1", ctDeltas)
	}

	if _, present := ctDeltas["movie"]; present {
		t.Errorf("movie should not appear in deltas (counts equal): %v", ctDeltas)
	}

	// file_type: archive present on ref only → +2.
	if c.FacetDeltas["file_type"]["archive"] != 2 {
		t.Errorf("file_type archive delta = %d, want 2", c.FacetDeltas["file_type"]["archive"])
	}

	if c.AllFacetsMatch {
		t.Error("AllFacetsMatch = true despite facet diffs")
	}

	// Six facets absent on both sides are unobserved and never enter a KPI
	// denominator. Of the three observed facets, only language matches.
	if c.FacetsObserved != 3 || c.FacetsMatched != 1 {
		t.Errorf("FacetsObserved/Matched = %d/%d, want 3/1", c.FacetsObserved, c.FacetsMatched)
	}
}

func TestCompareGraphQLOneSidedFacetPresenceIsObservedMismatch(t *testing.T) {
	t.Parallel()

	rust := GraphQLResult{
		Facets:         map[string]FacetCounts{},
		ObservedFacets: map[string]bool{},
	}
	ref := GraphQLResult{
		Facets:         map[string]FacetCounts{"release_year": {"2026": 4}},
		ObservedFacets: map[string]bool{"release_year": true},
	}

	comparison := CompareGraphQL(rust, ref, time.Millisecond, time.Millisecond)
	if !comparison.FacetObserved["release_year"] || comparison.FacetMatch["release_year"] {
		t.Fatalf("one-sided facet observation was not recorded as a mismatch: %+v", comparison)
	}

	if comparison.FacetDeltas["release_year"]["2026"] != 4 {
		t.Errorf("release_year delta = %v, want 2026:+4", comparison.FacetDeltas["release_year"])
	}

	if !comparison.IsDiscrepancy() {
		t.Error("one-sided facet presence must be a discrepancy")
	}
}

func TestCompareGraphQLOneSidedEmptyFacetIsObservedMismatch(t *testing.T) {
	t.Parallel()

	rust := GraphQLResult{
		Facets:         map[string]FacetCounts{},
		ObservedFacets: map[string]bool{},
	}
	ref := GraphQLResult{
		Facets:         map[string]FacetCounts{"release_year": {}},
		ObservedFacets: map[string]bool{"release_year": true},
	}

	comparison := CompareGraphQL(rust, ref, time.Millisecond, time.Millisecond)
	if !comparison.FacetObserved["release_year"] || comparison.FacetMatch["release_year"] {
		t.Fatalf("one-sided empty facet was not recorded as a mismatch: %+v", comparison)
	}

	if comparison.FacetsObserved != 1 || comparison.FacetsMatched != 0 {
		t.Errorf(
			"FacetsObserved/Matched = %d/%d, want 1/0",
			comparison.FacetsObserved,
			comparison.FacetsMatched,
		)
	}

	if !comparison.IsDiscrepancy() {
		t.Error("one-sided empty facet presence must be a discrepancy")
	}
}

func TestCompareGraphQLEstimateFlag(t *testing.T) {
	t.Parallel()

	rust := GraphQLResult{IDs: []string{"a"}, TotalCount: 500, TotalCountIsEstimate: true}
	ref := GraphQLResult{IDs: []string{"a"}, TotalCount: 512, TotalCountIsEstimate: true}

	c := CompareGraphQL(rust, ref, time.Millisecond, time.Millisecond)

	if !c.TotalCountIsEstimate {
		t.Error("TotalCountIsEstimate should be true when either side is an estimate")
	}

	if c.TotalCountMatch {
		t.Error("estimated totals 500 vs 512 should not be an exact match")
	}
}

func TestCompareGraphQLRankOrderMatters(t *testing.T) {
	t.Parallel()

	// Same set, different order: Top1 differs, Jaccard@20 stays 1.0.
	rust := GraphQLResult{IDs: []string{"a", "b", "c"}, TotalCount: 3}
	ref := GraphQLResult{IDs: []string{"c", "b", "a"}, TotalCount: 3}

	c := CompareGraphQL(rust, ref, time.Millisecond, time.Millisecond)

	if c.Top1Match {
		t.Error("Top1Match = true, want false (different rank-0)")
	}

	if c.JaccardAt20 != 1.0 {
		t.Errorf("JaccardAt20 = %v, want 1.0 (same set)", c.JaccardAt20)
	}

	if !c.IsDiscrepancy() {
		t.Error("IsDiscrepancy = false despite differing top result")
	}
}
