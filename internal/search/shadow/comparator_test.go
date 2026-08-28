package shadow

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const epsilon = 1e-9

// rangeIDs returns string IDs for the half-open integer range [start, end).
func rangeIDs(start, end int) []string {
	out := make([]string, 0, end-start)
	for v := start; v < end; v++ {
		out = append(out, strconv.Itoa(v))
	}

	return out
}

func TestCompareIdentical(t *testing.T) {
	t.Parallel()

	a := []string{"a", "b", "c"}
	b := []string{"a", "b", "c"}
	c := Compare(a, b, time.Millisecond, time.Millisecond)

	assert.InDelta(t, 1.0, c.JaccardAt20, epsilon)
	assert.InDelta(t, 1.0, c.JaccardAt50, epsilon)
	assert.InDelta(t, 1.0, c.RBO, epsilon)
	assert.True(t, c.Top1Match)
	assert.Equal(t, 3, c.PGCount)
	assert.Equal(t, 3, c.TantivyCount)
}

func TestCompareDisjoint(t *testing.T) {
	t.Parallel()

	a := []string{"a", "b", "c"}
	b := []string{"x", "y", "z"}
	c := Compare(a, b, time.Millisecond, time.Millisecond)

	assert.InDelta(t, 0.0, c.JaccardAt20, epsilon)
	assert.InDelta(t, 0.0, c.JaccardAt50, epsilon)
	assert.InDelta(t, 0.0, c.RBO, epsilon)
	assert.False(t, c.Top1Match)
}

func TestCompareReversed(t *testing.T) {
	t.Parallel()

	a := []string{"a", "b", "c"}
	b := []string{"c", "b", "a"}
	c := Compare(a, b, time.Millisecond, time.Millisecond)

	// The two lists contain the same set, so the top-20 Jaccard is 1.0, but the
	// ordering differs and the top result is not shared.
	assert.InDelta(t, 1.0, c.JaccardAt20, epsilon)
	assert.False(t, c.Top1Match)
	// Hand-computed RBO_EXT (p=0.9): X_1=0, X_2=1, X_3=3.
	// ((1-p)/p)*(0 + 0.5*0.81 + 1*0.729) + 1*0.729 = 1.134/9 + 0.729 = 0.855.
	assert.InDelta(t, 0.855, c.RBO, epsilon)
}

func TestComparePartialOverlap(t *testing.T) {
	t.Parallel()

	a := []string{"a", "b", "c", "d"}
	b := []string{"c", "d", "e", "f"}
	c := Compare(a, b, time.Millisecond, time.Millisecond)

	// Intersection {c,d}=2, union {a,b,c,d,e,f}=6 -> 1/3.
	assert.InDelta(t, 1.0/3.0, c.JaccardAt20, epsilon)
	assert.False(t, c.Top1Match)
}

func TestCompareEmptyBoth(t *testing.T) {
	t.Parallel()

	c := Compare(nil, nil, time.Millisecond, time.Millisecond)

	// Two empty result sets agree vacuously.
	assert.InDelta(t, 1.0, c.JaccardAt20, epsilon)
	assert.InDelta(t, 1.0, c.JaccardAt50, epsilon)
	assert.InDelta(t, 1.0, c.RBO, epsilon)
	assert.False(t, c.Top1Match) // no top result exists
	assert.False(t, c.RankingObserved())
	assert.Equal(t, 0, c.PGCount)
	assert.Equal(t, 0, c.TantivyCount)
	assert.False(t, c.IsDiscrepancy())
}

func TestCompareOneEmpty(t *testing.T) {
	t.Parallel()

	a := []string{"a", "b"}
	c := Compare(a, nil, time.Millisecond, time.Millisecond)

	assert.InDelta(t, 0.0, c.JaccardAt20, epsilon)
	assert.InDelta(t, 0.0, c.RBO, epsilon)
	assert.False(t, c.Top1Match)
	assert.True(t, c.RankingObserved())
	assert.Equal(t, 2, c.PGCount)
	assert.Equal(t, 0, c.TantivyCount)
}

func TestCompareDifferentLengthsPrefix(t *testing.T) {
	t.Parallel()

	a := []string{"a", "b", "c", "d"}
	b := []string{"a", "b"}
	c := Compare(a, b, time.Millisecond, time.Millisecond)

	// b is a prefix of a, so RBO_EXT extrapolates to 1.0 (maximally consistent):
	// the unseen tail of b is assumed to continue agreeing.
	assert.InDelta(t, 1.0, c.RBO, epsilon)
	// Jaccard over the full (top-20) sets: intersection {a,b}=2, union=4 -> 0.5.
	assert.InDelta(t, 0.5, c.JaccardAt20, epsilon)
	assert.True(t, c.Top1Match)
	assert.Equal(t, 4, c.PGCount)
	assert.Equal(t, 2, c.TantivyCount)
}

// TestRBOKnownValue locks the RBO_EXT formula against a value computed by hand.
func TestRBOKnownValue(t *testing.T) {
	t.Parallel()

	a := []string{"a", "b", "c"}
	b := []string{"a", "c", "b"} // same set, ranks 2 and 3 swapped
	c := Compare(a, b, time.Millisecond, time.Millisecond)

	// X_1=1, X_2=1, X_3=3 (p=0.9):
	//   sum1 = 1*0.9 + 0.5*0.81 + 1*0.729 = 2.034
	//   RBO  = ((1-p)/p)*sum1 + (X_3/3)*p^3 = 2.034/9 + 0.729 = 0.226 + 0.729 = 0.955
	assert.InDelta(t, 0.955, c.RBO, epsilon)
	assert.True(t, c.Top1Match)
}

// TestJaccardTopKTruncation checks that the K cutoff is respected: the two lists
// agree on their top 20 IDs but diverge between ranks 20 and 30.
func TestJaccardTopKTruncation(t *testing.T) {
	t.Parallel()

	a := rangeIDs(1, 31)                                // 1..30
	b := append(rangeIDs(1, 21), rangeIDs(100, 110)...) // 1..20, then 100..109
	c := Compare(a, b, time.Millisecond, time.Millisecond)

	assert.InDelta(t, 1.0, c.JaccardAt20, epsilon)
	// top-50 covers all 30 of each: intersection {1..20}=20, union=40 -> 0.5.
	assert.InDelta(t, 0.5, c.JaccardAt50, epsilon)
	assert.True(t, c.Top1Match)
}

// TestRBOSymmetry verifies RBO_EXT is symmetric in its two arguments.
func TestRBOSymmetry(t *testing.T) {
	t.Parallel()

	cases := [][2][]string{
		{{"a", "b", "c"}, {"a", "c", "b"}},
		{{"a", "b", "c", "d"}, {"a", "b"}},
		{{"a", "b", "c"}, {"c", "b", "a"}},
		{rangeIDs(1, 10), rangeIDs(5, 15)},
	}

	for i, tc := range cases {
		ab := rbo(tc[0], tc[1], RBOPersistence)
		ba := rbo(tc[1], tc[0], RBOPersistence)

		assert.InDeltaf(t, ab, ba, epsilon, "case %d", i)
	}
}

// TestResultCountDelta confirms the count fields reflect the input list lengths.
func TestResultCountDelta(t *testing.T) {
	t.Parallel()

	a := rangeIDs(0, 12)
	b := rangeIDs(0, 7)
	c := Compare(a, b, time.Millisecond, time.Millisecond)

	assert.Equal(t, 5, c.PGCount-c.TantivyCount)
}

func TestIsDiscrepancy(t *testing.T) {
	t.Parallel()

	identical := Compare([]string{"a", "b"}, []string{"a", "b"}, time.Millisecond, time.Millisecond)
	assert.False(t, identical.IsDiscrepancy())

	reordered := Compare([]string{"a", "b"}, []string{"b", "a"}, time.Millisecond, time.Millisecond)
	assert.True(t, reordered.IsDiscrepancy())

	differentCount := Compare([]string{"a", "b", "c"}, []string{"a"}, time.Millisecond, time.Millisecond)
	assert.True(t, differentCount.IsDiscrepancy())

	empty := Compare(nil, nil, time.Millisecond, time.Millisecond)
	assert.False(t, empty.IsDiscrepancy())
}
