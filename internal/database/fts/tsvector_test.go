package fts_test

import (
	"strings"
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

	t.Run("stops once the budget cannot fit the next lexeme", func(t *testing.T) {
		t.Parallel()

		v := fts.Tsvector{}
		// "alpha" costs len+12 = 17; a 20-byte budget fits it but not "bravo".
		remaining := v.AddTextBounded("alpha bravo charlie delta echo", fts.TsvectorWeightD, 20)

		assert.Contains(t, v, "alpha", "the first lexeme must be added")
		assert.NotContains(t, v, "bravo", "later lexemes must be dropped once the budget can't fit them")
		assert.NotContains(t, v, "echo")
		// Charge-before-commit: the budget never overshoots (goes negative).
		assert.GreaterOrEqual(t, remaining, 0, "budget must not overshoot below zero")
		assert.Less(t, remaining, 20, "the added lexeme must have been charged")
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

		assert.Positive(t, remaining)
		for _, lexeme := range []string{"alpha", "bravo", "charlie"} {
			assert.Contains(t, v, lexeme)
		}
	})
}

// F7 follow-up: a single lexeme longer than PostgreSQL's per-word limit
// (2046 bytes) is dropped by both AddText and AddTextBounded — otherwise it
// throws `word is too long` and aborts the persist batch regardless of total
// tsv size. Normal-length neighbours are still indexed.
func TestPerLexemeByteCap(t *testing.T) {
	t.Parallel()

	oversized := strings.Repeat("a", fts.MaxLexemeBytes+1)
	atLimit := strings.Repeat("b", fts.MaxLexemeBytes)

	t.Run("AddText drops the oversized lexeme", func(t *testing.T) {
		t.Parallel()

		v := fts.Tsvector{}
		v.AddText("keepme "+oversized+" alsokeep", fts.TsvectorWeightA)

		assert.Contains(t, v, "keepme")
		assert.Contains(t, v, "alsokeep")
		assert.NotContains(t, v, oversized, "a >2046-byte lexeme must be dropped")
		for lexeme := range v {
			assert.LessOrEqual(t, len(lexeme), fts.MaxLexemeBytes)
		}
	})

	t.Run("AddText keeps a lexeme exactly at the limit", func(t *testing.T) {
		t.Parallel()

		v := fts.Tsvector{}
		v.AddText(atLimit, fts.TsvectorWeightA)

		assert.Contains(t, v, atLimit, "a 2046-byte lexeme is accepted by Postgres and must be kept")
	})

	t.Run("AddTextBounded drops the oversized lexeme", func(t *testing.T) {
		t.Parallel()

		v := fts.Tsvector{}
		v.AddTextBounded("keepme "+oversized+" alsokeep", fts.TsvectorWeightD, fts.MaxTsvectorBytes)

		assert.Contains(t, v, "keepme")
		assert.Contains(t, v, "alsokeep")
		assert.NotContains(t, v, oversized)
	})
}
