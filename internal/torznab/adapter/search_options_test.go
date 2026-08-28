package adapter

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/torznab"
	"github.com/stretchr/testify/assert"
)

// F6: an imdb/tmdb identifier lookup is constrained by content type only for the
// two functions where the type is unambiguous. For t=search/music/book the type
// is left unset (nil) so a TV imdbid requested via t=search is no longer forced
// to content.type='movie' and hidden.
func TestIdentifierContentType(t *testing.T) {
	cases := []struct {
		function string
		want     model.ContentType
	}{
		{torznab.FunctionMovie, model.ContentTypeMovie},
		{torznab.FunctionTV, model.ContentTypeTvShow},
		{torznab.FunctionSearch, ""},
		{torznab.FunctionMusic, ""},
		{torznab.FunctionBook, ""},
	}

	for _, tc := range cases {
		t.Run(tc.function, func(t *testing.T) {
			got := identifierContentType(tc.function)
			assert.Equal(t, tc.want, got)
			if tc.function == torznab.FunctionSearch ||
				tc.function == torznab.FunctionMusic ||
				tc.function == torznab.FunctionBook {
				assert.True(t, got.IsNil(),
					"untyped function %q must leave the identifier content type unset", tc.function)
			}
		})
	}
}
