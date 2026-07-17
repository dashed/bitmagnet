package bytesfill

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeRanges_PartitionsKeyspaceWithoutGaps(t *testing.T) {
	t.Parallel()

	for _, k := range []int{1, 2, 4, 16, 32} {
		ranges := computeRanges(k)
		require.Len(t, ranges, k)

		// First range is open at the bottom, last range is open at the top.
		assert.False(t, ranges[0].hasLower, "k=%d first range must have no lower bound", k)
		assert.False(t, ranges[k-1].hasUpper, "k=%d last range must have no upper bound", k)

		// Adjacent ranges meet exactly: range i's upper == range i+1's lower, so
		// the half-open (lower, upper] slices tile the keyspace gap-free and
		// overlap-free.
		for i := 0; i < k-1; i++ {
			assert.True(t, ranges[i].hasUpper)
			assert.True(t, ranges[i+1].hasLower)
			assert.Equal(t, ranges[i].upper, ranges[i+1].lower,
				"k=%d ranges %d/%d must share a boundary", k, i, i+1)
		}
	}
}

func TestComputeRanges_ClampsInvalidK(t *testing.T) {
	t.Parallel()

	ranges := computeRanges(0)
	require.Len(t, ranges, 1)
	assert.False(t, ranges[0].hasLower)
	assert.False(t, ranges[0].hasUpper)
}
