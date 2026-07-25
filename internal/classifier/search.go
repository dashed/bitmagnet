package classifier

import (
	"context"
	"fmt"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
)

type LocalSearch interface {
	ContentByID(context.Context, model.ContentRef) (model.Content, error)
	ContentBySearch(context.Context, model.ContentType, string, model.Year) (model.Content, error)
}

type localSearch struct {
	search.Search
}

func (l localSearch) ContentByID(ctx context.Context, ref model.ContentRef) (model.Content, error) {
	result, err := l.Content(ctx, contentByIDOptions(ref)...)
	if err != nil {
		return model.Content{}, err
	}

	if len(result.Items) == 0 {
		return model.Content{}, classification.ErrUnmatched
	}

	return result.Items[0].Content, nil
}

func contentByIDOptions(ref model.ContentRef) []query.Option {
	options := []query.Option{
		query.Where(
			search.ContentTypeCriteria(ref.Type),
		),
		search.ContentDefaultPreload(),
		search.ContentDefaultHydrate(),
		query.Limit(1),
	}
	if ref.Source == "tmdb" {
		options = append(options, query.Where(
			search.ContentCanonicalIdentifierCriteria(model.ContentRef{
				Source: ref.Source,
				ID:     ref.ID,
			}),
		))
	} else {
		// Unlike the canonical identifier, an alternative identifier is not unique:
		// several content rows can carry the same attribute value, so order by the
		// canonical identity to make the LIMIT 1 pick reproducible.
		options = append(options, query.Where(
			search.ContentAlternativeIdentifierCriteria(model.ContentRef{
				Source: ref.Source,
				ID:     ref.ID,
			}),
		), search.ContentOrderByIdentity())
	}

	return options
}

func (l localSearch) ContentBySearch(
	ctx context.Context,
	ct model.ContentType,
	baseTitle string,
	year model.Year,
) (model.Content, error) {
	result, searchErr := l.Content(
		ctx,
		contentBySearchOptions(ct, baseTitle, year)...,
	)
	if searchErr != nil {
		return model.Content{}, searchErr
	}

	bestMatch, ok := levenshteinFindBestMatch[search.ContentResultItem](
		baseTitle,
		result.Items,
		func(item search.ContentResultItem) []string {
			candidates := []string{item.Title}
			if item.OriginalTitle.Valid {
				candidates = append(candidates, item.OriginalTitle.String)
			}

			return candidates
		},
	)
	if !ok {
		return model.Content{}, classification.ErrUnmatched
	}

	return bestMatch.Content, nil
}

func contentBySearchOptions(ct model.ContentType, baseTitle string, year model.Year) []query.Option {
	options := []query.Option{
		query.Where(search.ContentTypeCriteria(ct)),
		query.SearchString(fmt.Sprintf("\"%s\"", baseTitle)),
		// Relevance first, then the canonical identity as a tiebreak: ts_rank_cd
		// usually ties every candidate for these phrase queries, and the winner below
		// is whichever candidate the levenshtein search reaches first, so without a
		// total order both the LIMIT 10 window and the match picked from it depend on
		// the query plan.
		search.ContentOrderByQueryStringRankThenIdentity(),
		query.Limit(10),
		search.ContentDefaultPreload(),
		search.ContentDefaultHydrate(),
	}
	if !year.IsNil() {
		options = append(
			options,
			query.Where(search.ContentReleaseDateCriteria(model.NewDateRangeFromYear(year))),
		)
	}

	return options
}
