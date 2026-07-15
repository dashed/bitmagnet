// Package shadow implements "shadow mode" comparison between the legacy
// PostgreSQL search engine and the Tantivy search sidecar.
//
// The same query is executed against both engines and the two ranked result
// sets are compared for similarity. To stay decoupled from the gRPC/protobuf
// bindings, the comparator operates purely on already-extracted, ordered slices
// of stable result IDs (one per engine). The caller (the search router) is
// responsible for extracting those IDs — for a torrent content result the
// stable key is model.TorrentContent.InferID(), i.e.
// hex(info_hash):content_type:content_source:content_id — from each side.
package shadow

import "time"

// RBOPersistence is the persistence parameter p used for Rank-Biased Overlap.
// A value of 0.9 weights roughly the top ~10 ranks most heavily while still
// giving non-trivial weight to deeper ranks (the expected viewing depth is
// 1/(1-p) = 10). See rbo below for the formula and reference.
const RBOPersistence = 0.9

// jaccardK1 and jaccardK2 are the prefix depths at which Jaccard set similarity
// is measured (top-K of each list).
const (
	jaccardK1 = 20
	jaccardK2 = 50
)

// Comparison holds the similarity metrics for a single query run against both
// the PostgreSQL ("pg") and Tantivy engines.
type Comparison struct {
	// JaccardAt20 is the Jaccard set similarity over the top-20 IDs of each list.
	JaccardAt20 float64
	// JaccardAt50 is the Jaccard set similarity over the top-50 IDs of each list.
	JaccardAt50 float64
	// RBO is the extrapolated Rank-Biased Overlap of the two ranked lists.
	RBO float64
	// Top1Match reports whether both lists are non-empty and share the same
	// rank-0 (top) result.
	Top1Match bool
	// PGCount is the number of IDs returned by the PostgreSQL engine.
	PGCount int
	// TantivyCount is the number of IDs returned by the Tantivy engine.
	TantivyCount int
	// PGLatency is the measured latency of the PostgreSQL query.
	PGLatency time.Duration
	// TantivyLatency is the measured latency of the Tantivy query.
	TantivyLatency time.Duration
}

// IsDiscrepancy reports whether the two engines disagreed in a way worth
// surfacing: a comparable top result differs, the top-20 sets are not
// identical, or the engines returned a different number of results. Two empty
// result sets have no top result to compare and are not a discrepancy.
func (c Comparison) IsDiscrepancy() bool {
	top1Differs := (c.PGCount > 0 || c.TantivyCount > 0) && !c.Top1Match

	return top1Differs || c.JaccardAt20 < 1.0 || c.PGCount != c.TantivyCount
}

// Compare computes the similarity metrics for a single query, given the ordered
// result-ID slices and measured latencies from each engine. pgIDs/tantivyIDs
// must be the full ordered (rank 0 first) result lists; PGCount/TantivyCount are
// their lengths and the result-count delta is PGCount - TantivyCount.
//
// IDs within a single list are assumed to be unique (true for search result
// IDs); duplicates, if present, are treated defensively as already-seen and
// contribute no additional overlap.
func Compare(pgIDs, tantivyIDs []string, pgLatency, tantivyLatency time.Duration) Comparison {
	return Comparison{
		JaccardAt20:    jaccardAtK(pgIDs, tantivyIDs, jaccardK1),
		JaccardAt50:    jaccardAtK(pgIDs, tantivyIDs, jaccardK2),
		RBO:            rbo(pgIDs, tantivyIDs, RBOPersistence),
		Top1Match:      top1Match(pgIDs, tantivyIDs),
		PGCount:        len(pgIDs),
		TantivyCount:   len(tantivyIDs),
		PGLatency:      pgLatency,
		TantivyLatency: tantivyLatency,
	}
}

// top1Match reports whether both lists are non-empty and share the same first
// element. Empty lists (one or both) yield false: there is no top result to
// match.
func top1Match(a, b []string) bool {
	return len(a) > 0 && len(b) > 0 && a[0] == b[0]
}

// jaccardAtK returns the Jaccard similarity |A∩B| / |A∪B| over the top-k IDs of
// each list. Two empty top-k sets are treated as identical (1.0) rather than
// 0/0; otherwise a query for which both engines legitimately return nothing
// would register as total disagreement.
func jaccardAtK(a, b []string, k int) float64 {
	setA := topKSet(a, k)
	setB := topKSet(b, k)

	if len(setA) == 0 && len(setB) == 0 {
		return 1.0
	}

	intersection := 0

	for id := range setA {
		if _, ok := setB[id]; ok {
			intersection++
		}
	}

	union := len(setA) + len(setB) - intersection
	if union == 0 {
		return 1.0
	}

	return float64(intersection) / float64(union)
}

// topKSet returns the set of the first min(k, len(ids)) IDs.
func topKSet(ids []string, k int) map[string]struct{} {
	n := min(k, len(ids))
	set := make(map[string]struct{}, n)

	for i := range n {
		set[ids[i]] = struct{}{}
	}

	return set
}

// rbo computes the extrapolated Rank-Biased Overlap (RBO_EXT) of two ranked
// lists with persistence parameter p ∈ (0,1).
//
// Reference: Webber, Moffat & Zobel, "A Similarity Measure for Indefinite
// Rankings", ACM Transactions on Information Systems 28(4), 2010
// (https://doi.org/10.1145/1852102.1852106).
//
// Let X_d be the size of the overlap (set intersection) of the depth-d prefixes
// of the two lists, and A_d = X_d / d the agreement at depth d. Base RBO is the
// convergent, top-weighted sum
//
//	RBO = (1 - p) · Σ_{d=1}^{∞} p^{d-1} · A_d   ∈ [0, 1].
//
// For finite lists we cannot observe the infinite tail, so we use the
// extrapolated estimator RBO_EXT, which assumes the agreement seen at the
// deepest observable rank continues. With shorter length s and longer length l
// (s ≤ l), Eq. 32 of the paper gives
//
//	RBO_EXT = ((1-p)/p) · [ Σ_{d=1}^{l} (X_d/d)·p^d
//	                        + Σ_{d=s+1}^{l} (X_s·(d-s)/(s·d))·p^d ]
//	          + [ (X_l - X_s)/l + X_s/s ] · p^l
//
// which reduces, when s = l = k, to the equal-length estimator (Eq. 11)
//
//	RBO_EXT = (X_k/k)·p^k + ((1-p)/p) · Σ_{d=1}^{k} (X_d/d)·p^d.
//
// Identical lists give 1.0; disjoint lists give 0.0; a shorter list that is a
// prefix of the longer one extrapolates to 1.0 (maximally consistent).
func rbo(a, b []string, p float64) float64 {
	la, lb := len(a), len(b)

	// Both empty: vacuously identical.
	if la == 0 && lb == 0 {
		return 1.0
	}

	// Exactly one empty: no overlap possible (and avoids a division by the
	// shorter length of zero below).
	if la == 0 || lb == 0 {
		return 0.0
	}

	// shortLen ≤ longLen.
	shortLen, longLen := la, lb
	if la > lb {
		shortLen, longLen = lb, la
	}

	seenA := make(map[string]struct{}, la)
	seenB := make(map[string]struct{}, lb)
	overlap := 0
	xAtShort := 0

	sum1 := 0.0 // Σ_{d=1}^{l} (X_d/d)·p^d
	sum2 := 0.0 // Σ_{d=s+1}^{l} (X_s·(d-s)/(s·d))·p^d
	pd := 1.0   // p^d, accumulated iteratively to avoid math.Pow drift

	for d := 1; d <= longLen; d++ {
		pd *= p

		if d-1 < la {
			addOverlap(a[d-1], seenA, seenB, &overlap)
		}

		if d-1 < lb {
			addOverlap(b[d-1], seenB, seenA, &overlap)
		}

		if d == shortLen {
			xAtShort = overlap
		}

		sum1 += (float64(overlap) / float64(d)) * pd

		if d > shortLen {
			sum2 += (float64(xAtShort) * float64(d-shortLen) / (float64(shortLen) * float64(d))) * pd
		}
	}

	xAtLong := overlap
	pl := pd // after the loop pd == p^longLen

	tail := (float64(xAtLong-xAtShort)/float64(longLen) + float64(xAtShort)/float64(shortLen)) * pl

	return ((1-p)/p)*(sum1+sum2) + tail
}

// addOverlap records that the item x has been seen in its own list, and if it
// was already seen in the other list increments the running overlap count. A
// repeated item within the same list is ignored, so the overlap never
// double-counts duplicates.
func addOverlap(x string, own, other map[string]struct{}, overlap *int) {
	if _, seen := own[x]; seen {
		return
	}

	own[x] = struct{}{}

	if _, ok := other[x]; ok {
		*overlap++
	}
}
