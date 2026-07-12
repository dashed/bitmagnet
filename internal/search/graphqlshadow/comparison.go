package graphqlshadow

import (
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/search/shadow"
)

// FacetKeys is the canonical set of the 9 torrentContent facet keys the GraphQL
// shadow diffs, in a stable order. They are exactly the keys the resolver maps
// its aggregations to (internal/database/search/facet_*.go — the string
// constants are the contract).
var FacetKeys = []string{
	"content_type",
	"torrent_source",
	"torrent_tag",
	"file_type",
	"language",
	"content_genre",
	"release_year",
	"video_resolution",
	"video_source",
}

// FacetCounts maps a facet's item value to its aggregation count for a single
// facet (e.g. {"movie": 42, "tv_show": 17} for the content_type facet).
type FacetCounts map[string]int

// GraphQLResult is the engine-agnostic projection of a torrentContent.search
// response used for shadow comparison — the GraphQL analogue of the ordered
// ID slice the Tantivy shadow compares. It carries only what the numeric gate
// diffs, so the comparator stays decoupled from the async-graphql / gqlgen
// response types and identical whichever shadow mechanism ships.
type GraphQLResult struct {
	// IDs is the ordered (rank 0 first) list of torrentContent InferIDs.
	IDs []string
	// TotalCount is the reported total result count.
	TotalCount int
	// TotalCountIsEstimate mirrors the response's totalCountIsEstimate: when true
	// the total (and, on the same query, the facet counts) is a budgeted estimate,
	// so an exact match is not expected — the numeric gate applies a ratio
	// threshold (count-match ≥ 0.95) rather than requiring equality.
	TotalCountIsEstimate bool
	// Facets maps each facet key to its value→count map. A key absent from the map
	// (the query did not request that facet) is treated as an empty facet on that
	// side; a facet requested on one side but not the other is a mismatch.
	Facets map[string]FacetCounts
}

// GraphQLComparison holds the similarity + diff metrics for a single shadowed
// query: the reference is the live Go /graphql result, the candidate is the Rust
// result. It embeds the engine-agnostic shadow.Comparison over the ordered ID
// lists (Jaccard/RBO/Top1/result-count) and adds the GraphQL-specific total-count
// and per-facet-count diffs. All count deltas follow the shadow.Comparison
// convention of reference-minus-candidate (Go minus Rust).
type GraphQLComparison struct {
	shadow.Comparison

	// TotalCountRef / TotalCountRust are the reported totals from each side.
	TotalCountRef  int
	TotalCountRust int
	// TotalCountDelta is TotalCountRef - TotalCountRust.
	TotalCountDelta int
	// TotalCountMatch reports whether the two totals are exactly equal.
	TotalCountMatch bool
	// TotalCountIsEstimate is true when either side reported an estimated total;
	// the numeric gate should treat an exact-total mismatch leniently in that case.
	TotalCountIsEstimate bool

	// FacetDeltas maps facet key → value → (refCount - rustCount) for every value
	// whose counts differ between the two sides. A facet with no differing values
	// is absent from the map. Only the 9 FacetKeys are considered.
	FacetDeltas map[string]map[string]int
	// FacetMatch maps each of the 9 facet keys to whether that facet's full
	// value→count map is identical on both sides.
	FacetMatch map[string]bool
	// FacetsMatched is the number of the 9 facets that matched exactly.
	FacetsMatched int
	// AllFacetsMatch is true iff every one of the 9 facets matched exactly.
	AllFacetsMatch bool
}

// IsDiscrepancy reports whether the shadow disagreed in a way worth surfacing:
// the underlying result-set discrepancy (top result, top-20 set, or result
// count), OR the total count, OR any facet count differs. It generalises
// shadow.Comparison.IsDiscrepancy to the full GraphQL response.
func (c GraphQLComparison) IsDiscrepancy() bool {
	return c.Comparison.IsDiscrepancy() || !c.TotalCountMatch || !c.AllFacetsMatch
}

// CompareGraphQL computes the full GraphQL shadow comparison between the Rust
// candidate and the Go reference results, given their measured latencies. The
// ID-list similarity reuses shadow.Compare with the reference list first (so the
// embedded result-count delta stays reference-minus-candidate); the total-count
// and per-facet diffs are layered on top.
func CompareGraphQL(rust, ref GraphQLResult, rustLatency, refLatency time.Duration) GraphQLComparison {
	c := GraphQLComparison{
		Comparison:           shadow.Compare(ref.IDs, rust.IDs, refLatency, rustLatency),
		TotalCountRef:        ref.TotalCount,
		TotalCountRust:       rust.TotalCount,
		TotalCountDelta:      ref.TotalCount - rust.TotalCount,
		TotalCountMatch:      ref.TotalCount == rust.TotalCount,
		TotalCountIsEstimate: ref.TotalCountIsEstimate || rust.TotalCountIsEstimate,
		FacetDeltas:          map[string]map[string]int{},
		FacetMatch:           make(map[string]bool, len(FacetKeys)),
	}

	for _, key := range FacetKeys {
		deltas := diffFacet(ref.Facets[key], rust.Facets[key])

		matched := len(deltas) == 0
		c.FacetMatch[key] = matched

		if matched {
			c.FacetsMatched++
		} else {
			c.FacetDeltas[key] = deltas
		}
	}

	c.AllFacetsMatch = c.FacetsMatched == len(FacetKeys)

	return c
}

// diffFacet returns value → (refCount - rustCount) for every value whose count
// differs between the reference and candidate facet maps. A value present on one
// side only contributes its full count as the delta (the missing side counts as
// zero). An identical pair of facet maps returns an empty (non-nil) map.
func diffFacet(ref, rust FacetCounts) map[string]int {
	deltas := map[string]int{}

	for value, refCount := range ref {
		if refCount != rust[value] {
			deltas[value] = refCount - rust[value]
		}
	}

	for value, rustCount := range rust {
		if _, seen := ref[value]; seen {
			continue // already accounted for above
		}

		if rustCount != 0 {
			deltas[value] = -rustCount
		}
	}

	return deltas
}
