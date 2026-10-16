package classifier

import (
	"errors"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
)

// NormalizedResult is the complete stable classifier boundary shared by the
// corpus and same-input Go/Rust tape gates. It includes attributes that do not
// always affect persistence so an unchanged write set cannot hide drift.
type NormalizedResult struct {
	ContentType     string          `json:"contentType"`
	BaseTitle       *string         `json:"baseTitle"`
	Date            *NormalizedDate `json:"date"`
	Languages       []string        `json:"languages"`
	LanguageMulti   bool            `json:"languageMulti"`
	Episodes        string          `json:"episodes"`
	VideoResolution *string         `json:"videoResolution"`
	VideoSource     *string         `json:"videoSource"`
	VideoCodec      *string         `json:"videoCodec"`
	Video3D         *string         `json:"video3d"`
	VideoModifier   *string         `json:"videoModifier"`
	ReleaseGroup    *string         `json:"releaseGroup"`
	ContentAttached bool            `json:"contentAttached"`
	Outcome         string          `json:"outcome"`
	Error           string          `json:"error,omitempty"`
}

// NormalizedDate mirrors model.Date without relying on its database-oriented
// JSON representation.
type NormalizedDate struct {
	Year  int `json:"year"`
	Month int `json:"month"`
	Day   int `json:"day"`
}

// NormalizeResult projects Go's structured result and terminal error onto the
// frozen cross-language classifier contract.
func NormalizeResult(result classification.Result, err error) NormalizedResult {
	normalized := NormalizedResult{
		Languages:       make([]string, 0, len(result.Languages)),
		LanguageMulti:   result.LanguageMulti,
		Episodes:        result.Episodes.String(),
		ContentAttached: result.Content != nil,
		Outcome:         "classified",
	}
	if result.ContentType.Valid {
		normalized.ContentType = result.ContentType.ContentType.String()
	}
	if result.BaseTitle.Valid {
		normalized.BaseTitle = normalizedStringPointer(result.BaseTitle.String)
	}
	if !result.Date.IsNil() {
		normalized.Date = &NormalizedDate{
			Year:  int(result.Date.Year),
			Month: int(result.Date.Month),
			Day:   int(result.Date.Day),
		}
	}
	for _, language := range result.Languages.Slice() {
		normalized.Languages = append(normalized.Languages, language.String())
	}
	if result.VideoResolution.Valid {
		normalized.VideoResolution = normalizedStringPointer(result.VideoResolution.VideoResolution.String())
	}
	if result.VideoSource.Valid {
		normalized.VideoSource = normalizedStringPointer(result.VideoSource.VideoSource.String())
	}
	if result.VideoCodec.Valid {
		normalized.VideoCodec = normalizedStringPointer(result.VideoCodec.VideoCodec.String())
	}
	if result.Video3D.Valid {
		normalized.Video3D = normalizedStringPointer(result.Video3D.Video3D.String())
	}
	if result.VideoModifier.Valid {
		normalized.VideoModifier = normalizedStringPointer(result.VideoModifier.VideoModifier.String())
	}
	if result.ReleaseGroup.Valid {
		normalized.ReleaseGroup = normalizedStringPointer(result.ReleaseGroup.String)
	}
	if err == nil {
		return normalized
	}

	switch {
	case errors.Is(err, classification.ErrDeleteTorrent):
		normalized.Outcome = "deleted"
	case errors.Is(err, classification.ErrUnmatched):
		normalized.Outcome = "unmatched"
	default:
		normalized.Outcome = "error"
	}
	normalized.Error = err.Error()

	return normalized
}

func normalizedStringPointer(value string) *string {
	return &value
}
