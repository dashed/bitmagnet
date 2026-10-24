package classifier

import (
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/stretchr/testify/require"
)

func TestNormalizeResultPreservesNonPersistentClassifierFields(t *testing.T) {
	result := classification.Result{
		ContentAttributes: classification.ContentAttributes{
			ContentType:   model.NewNullContentType(model.ContentTypeMovie),
			BaseTitle:     model.NewNullString("A full classifier result"),
			Date:          model.NewDateFromParts(2026, time.August, 12),
			Languages:     model.Languages{model.Language("en"): {}, model.Language("fr"): {}},
			LanguageMulti: true,
		},
	}

	got := NormalizeResult(result, nil)
	require.Equal(t, "movie", got.ContentType)
	require.Equal(t, "A full classifier result", *got.BaseTitle)
	require.Equal(t, &NormalizedDate{Year: 2026, Month: 8, Day: 12}, got.Date)
	require.Equal(t, []string{"en", "fr"}, got.Languages)
	require.True(t, got.LanguageMulti)
	require.False(t, got.ContentAttached)
	require.Equal(t, "classified", got.Outcome)
}

func TestNormalizeResultPreservesDeterministicTerminalOutcome(t *testing.T) {
	got := NormalizeResult(classification.Result{}, classification.ErrUnmatched)
	require.Equal(t, "unmatched", got.Outcome)
	require.Equal(t, classification.ErrUnmatched.Error(), got.Error)
	require.Empty(t, got.Languages)
}
