package adapter

import (
	"fmt"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/torznab"
)

func searchRequestToQueryOptions(r torznab.SearchRequest) ([]query.Option, error) {
	var options []query.Option

	switch r.Type {
	case torznab.FunctionSearch:
	case torznab.FunctionMovie:
		options = append(options, query.Where(search.TorrentContentTypeOrNullCriteria(model.ContentTypeMovie)))
	case torznab.FunctionTV:
		options = append(options, query.Where(search.TorrentContentTypeOrNullCriteria(model.ContentTypeTvShow)))

		if r.Season.Valid {
			episodes := make(model.Episodes)
			if r.Episode.Valid {
				episodes = episodes.AddEpisode(r.Season.Int, r.Episode.Int)
			} else {
				episodes = episodes.AddSeason(r.Season.Int)
			}

			options = append(options, query.Where(search.TorrentContentEpisodesCriteria(episodes)))
		}
	case torznab.FunctionMusic:
		options = append(options, query.Where(search.TorrentContentTypeOrNullCriteria(model.ContentTypeMusic)))
	case torznab.FunctionBook:
		options = append(options, query.Where(search.TorrentContentTypeOrNullCriteria(
			model.ContentTypeEbook,
			model.ContentTypeComic,
			model.ContentTypeAudiobook,
		)))
	default:
		return nil, torznab.Error{
			Code:        202,
			Description: fmt.Sprintf("no such function (%s)", r.Type),
		}
	}

	if r.Query != "" {
		order := search.TorrentContentOrderByRelevance
		if r.Profile.DisableOrderByRelevance {
			order = search.TorrentContentOrderByPublishedAt
		}

		options = append(options,
			query.SearchString(r.Query),
			query.OrderBy(order.Clauses(search.OrderDirectionDescending)...))
	}

	var catsCriteria []query.Criteria

	for _, cat := range r.Cats {
		var catCriteria []query.Criteria

		switch {
		case torznab.CategoryMovies.Has(cat):
			if r.Type != torznab.FunctionMovie || torznab.CategoryMovies.ID == cat {
				catCriteria = append(
					catCriteria,
					search.TorrentContentTypeOrNullCriteria(model.ContentTypeMovie),
				)
			}

			switch cat {
			case torznab.CategoryMoviesSD.ID:
				catCriteria = append(
					catCriteria,
					search.VideoResolutionCriteria(model.VideoResolutionV480p),
				)
			case torznab.CategoryMoviesHD.ID:
				catCriteria = append(catCriteria, search.VideoResolutionCriteria(
					model.VideoResolutionV720p,
					model.VideoResolutionV1080p,
					model.VideoResolutionV1440p,
					model.VideoResolutionV2160p,
				))
			case torznab.CategoryMoviesUHD.ID:
				catCriteria = append(
					catCriteria,
					search.VideoResolutionCriteria(model.VideoResolutionV2160p),
				)
			case torznab.CategoryMovies3D.ID:
				catCriteria = append(catCriteria, search.Video3DCriteria(
					model.Video3DV3D,
					model.Video3DV3DSBS,
					model.Video3DV3DOU,
				))
			}
		case torznab.CategoryTV.Has(cat):
			if r.Type != torznab.FunctionTV || torznab.CategoryTV.ID == cat {
				catCriteria = append(
					catCriteria,
					search.TorrentContentTypeOrNullCriteria(model.ContentTypeTvShow),
				)
			}

			switch cat {
			case torznab.CategoryTVSD.ID:
				catCriteria = append(
					catCriteria,
					search.VideoResolutionCriteria(model.VideoResolutionV480p),
				)
			case torznab.CategoryTVHD.ID:
				catCriteria = append(catCriteria, search.VideoResolutionCriteria(
					model.VideoResolutionV720p,
					model.VideoResolutionV1080p,
					model.VideoResolutionV1440p,
					model.VideoResolutionV2160p,
				))
			case torznab.CategoryTVUHD.ID:
				catCriteria = append(
					catCriteria,
					search.VideoResolutionCriteria(model.VideoResolutionV2160p),
				)
			}
		case torznab.CategoryXXX.Has(cat):
			catCriteria = append(catCriteria, search.TorrentContentTypeOrNullCriteria(model.ContentTypeXxx))
		case torznab.CategoryPC.Has(cat):
			catCriteria = append(catCriteria,
				search.TorrentContentTypeOrNullCriteria(model.ContentTypeSoftware, model.ContentTypeGame))
		case torznab.CategoryAudioAudiobook.Has(cat):
			catCriteria = append(catCriteria, search.TorrentContentTypeOrNullCriteria(model.ContentTypeAudiobook))
		case torznab.CategoryAudio.Has(cat):
			catCriteria = append(catCriteria, search.TorrentContentTypeOrNullCriteria(model.ContentTypeMusic))
		case torznab.CategoryBooksComics.Has(cat):
			catCriteria = append(catCriteria, search.TorrentContentTypeOrNullCriteria(model.ContentTypeComic))
		case torznab.CategoryBooks.Has(cat):
			options = append(options, query.Where(search.TorrentContentTypeOrNullCriteria(
				model.ContentTypeEbook,
				model.ContentTypeComic,
				model.ContentTypeAudiobook,
			)))
		}

		if len(catCriteria) > 0 {
			catsCriteria = append(catsCriteria, query.And(catCriteria...))
		}
	}

	if len(catsCriteria) > 0 {
		options = append(options, query.Where(query.Or(catsCriteria...)))
	}

	if r.IMDBID.Valid {
		imdbID := r.IMDBID.String
		if !strings.HasPrefix(imdbID, "tt") {
			imdbID = "tt" + imdbID
		}

		options = append(options, query.Where(search.ContentAlternativeIdentifierCriteria(model.ContentRef{
			Type:   identifierContentType(r.Type),
			Source: "imdb",
			ID:     imdbID,
		})))
	}

	if r.TMDBID.Valid {
		tmdbID := r.TMDBID.String

		options = append(options, query.Where(search.ContentCanonicalIdentifierCriteria(model.ContentRef{
			Type:   identifierContentType(r.Type),
			Source: "tmdb",
			ID:     tmdbID,
		})))
	}

	limit := r.Profile.DefaultLimit
	if r.Limit.Valid {
		limit = r.Limit.Uint
		if limit > r.Profile.MaxLimit {
			limit = r.Profile.MaxLimit
		}
	}

	options = append(options, query.Limit(limit))
	if r.Offset.Valid {
		options = append(options, query.Offset(r.Offset.Uint))
	}

	if len(r.Profile.Tags) > 0 {
		options = append(options, query.Where(search.TorrentTagCriteria(r.Profile.Tags...)))
	}

	return options, nil
}

// identifierContentType constrains an imdb/tmdb identifier lookup by content type
// only for the two functions where the type is unambiguous: t=movie ⇒ movie,
// t=tvsearch ⇒ tv_show. For t=search, t=music and t=book it returns the nil
// content type so the identifier matches regardless of type. The previous
// `if r.Type != FunctionTV { movie } else if r.Type != FunctionMovie { tvshow }`
// branches could never leave the type unset, forcing content.type='movie' for
// t=search — so a TV imdbid requested via t=search matched nothing (F6).
func identifierContentType(function string) model.ContentType {
	switch function {
	case torznab.FunctionMovie:
		return model.ContentTypeMovie
	case torznab.FunctionTV:
		return model.ContentTypeTvShow
	default:
		return ""
	}
}
