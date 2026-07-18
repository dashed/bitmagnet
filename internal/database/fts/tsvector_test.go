package fts_test

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/database/fts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTsvector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantTsv fts.Tsvector
		wantStr string
	}{
		{
			input: " 'a':1A bb:2b 'cc ccc':3C  'dD''Dd''':4D e a bb:5 ",
			wantTsv: fts.Tsvector{
				"a": {
					1: 'A',
				},
				"bb": {
					2: 'B',
					5: 'D',
				},
				"cc ccc": {
					3: 'C',
				},
				"dD'Dd'": {
					4: 'D',
				},
				"e": {},
			},
			wantStr: "'a':1A 'bb':2B,5 'cc ccc':3C 'dD''Dd''':4 'e'",
		},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			t.Parallel()

			got, err := fts.ParseTsvector(test.input)

			require.NoError(t, err)
			assert.Equal(t, test.wantTsv, got)
			assert.Equal(t, test.wantStr, got.String())
		})
	}
}

// F7: AddTextBounded stops appending once the byte budget is exhausted and
// reports the remaining budget, so an oversized bag can't grow the vector past
// PostgreSQL's tsvector limit.
func TestAddTextBounded(t *testing.T) {
	t.Parallel()

	t.Run("stops once the budget is exhausted", func(t *testing.T) {
		t.Parallel()

		v := fts.Tsvector{}
		// A budget large enough for only the first couple of lexemes.
		remaining := v.AddTextBounded("alpha bravo charlie delta echo", fts.TsvectorWeightD, 30)

		assert.LessOrEqual(t, remaining, 0, "budget should be spent")
		assert.Contains(t, v, "alpha", "the first lexeme must be added")
		assert.NotContains(t, v, "echo", "later lexemes must be dropped once over budget")
	})

	t.Run("adds nothing when the budget is already gone", func(t *testing.T) {
		t.Parallel()

		v := fts.Tsvector{}
		remaining := v.AddTextBounded("alpha bravo", fts.TsvectorWeightD, 0)

		assert.Equal(t, 0, remaining)
		assert.Empty(t, v)
	})

	t.Run("adds every lexeme when the budget is ample", func(t *testing.T) {
		t.Parallel()

		v := fts.Tsvector{}
		remaining := v.AddTextBounded("alpha bravo charlie", fts.TsvectorWeightD, fts.MaxTsvectorBytes)

		assert.Greater(t, remaining, 0)
		for _, lexeme := range []string{"alpha", "bravo", "charlie"} {
			assert.Contains(t, v, lexeme)
		}
	})
}
