package configresolver

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// SEARCH_SAMPLE_RATE=0.05 crash-looped the app at startup because the coercion
// switch had no float case (upstream gap). Pin the fix.
func TestCoerceStringValue_Float64(t *testing.T) {
	t.Parallel()

	v, err := coerceStringValue("0.05", reflect.TypeOf(float64(0)))
	require.NoError(t, err)
	require.InDelta(t, 0.05, v, 1e-9)

	_, err = coerceStringValue("not-a-float", reflect.TypeOf(float64(0)))
	require.Error(t, err)
}
