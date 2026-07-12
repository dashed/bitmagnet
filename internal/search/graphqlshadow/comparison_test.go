package graphqlshadow

import (
	"testing"
	"time"
)

func TestCompareGraphQLIdentical(t *testing.T) {
	res := GraphQLResult{
		IDs:        []string{"a", "b", "c"},
		TotalCount: 3,
		Facets: map[string]FacetCounts{
			"content_type": {"movie": 2, "tv_show": 1},
			"language":     {"en": 3},
		},
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

	if !c.AllFacetsMatch || c.FacetsMatched != len(FacetKeys) {
		t.Errorf("facets not all matched: all=%v matched=%d", c.AllFacetsMatch, c.FacetsMatched)
	}

	if len(c.FacetDeltas) != 0 {
		t.Errorf("expected no facet deltas, got %v", c.FacetDeltas)
	}

	if c.IsDiscrepancy() {
		t.Error("IsDiscrepancy = true on identical results")
	}
}

func TestCompareGraphQLTotalCountDeltaSign(t *testing.T) {
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
	rust := GraphQLResult{
		IDs: []string{"a", "b"},
		Facets: map[string]FacetCounts{
			"content_type": {"movie": 5, "tv_show": 3}, // tv_show differs, extra "audiobook" on ref
			"language":     {"en": 10},                 // identical
			"file_type":    {"video": 4},               // rust-only value "archive" absent
		},
	}
	ref := GraphQLResult{
		IDs: []string{"a", "b"},
		Facets: map[string]FacetCounts{
			"content_type": {"movie": 5, "tv_show": 4, "audiobook": 1},
			"language":     {"en": 10},
			"file_type":    {"video": 4, "archive": 2},
		},
	}

	c := CompareGraphQL(rust, ref, time.Millisecond, time.Millisecond)

	if c.FacetMatch["language"] != true {
		t.Error("language facet should match")
	}

	if c.FacetMatch["content_type"] != false || c.FacetMatch["file_type"] != false {
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

	// Of the 9 facets: 6 are absent on both sides → they match (empty==empty);
	// language matches; content_type and file_type do not. So 7 matched.
	if c.FacetsMatched != 7 {
		t.Errorf("FacetsMatched = %d, want 7", c.FacetsMatched)
	}
}

func TestCompareGraphQLEstimateFlag(t *testing.T) {
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
