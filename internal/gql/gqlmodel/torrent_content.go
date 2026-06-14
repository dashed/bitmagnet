package gqlmodel

import (
	"context"
	"time"

	"github.com/99designs/gqlgen/graphql"
	q "github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/gql/gqlmodel/gen"
	"github.com/bitmagnet-io/bitmagnet/internal/maps"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/pathsearch"
)

type TorrentContentQuery struct {
	TorrentContentSearch search.TorrentContentSearch
	// Pathsearch is the L3 exact-refine composer, or nil when the pathsearch
	// feature is disabled. When nil (or its typeahead flag is off) Search takes
	// the existing PostgreSQL path unchanged.
	Pathsearch *pathsearch.Composer
}

type TorrentContent struct {
	ID              string
	InfoHash        protocol.ID
	ContentType     model.NullContentType
	ContentSource   model.NullString
	ContentID       model.NullString
	Title           string
	Languages       []model.Language `json:"omitempty"`
	Episodes        *Episodes
	VideoResolution model.NullVideoResolution
	VideoSource     model.NullVideoSource
	VideoCodec      model.NullVideoCodec
	Video3D         model.NullVideo3D
	VideoModifier   model.NullVideoModifier
	ReleaseGroup    model.NullString
	SearchString    string
	Seeders         model.NullUint
	Leechers        model.NullUint
	PublishedAt     time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
	Torrent         model.Torrent
	Content         *model.Content
}

type Episodes struct {
	Label   string
	Seasons []model.Season `json:"omitempty"`
}

func NewTorrentContentFromResultItem(item search.TorrentContentResultItem) TorrentContent {
	c := TorrentContent{
		ID:              item.ID,
		InfoHash:        item.InfoHash,
		ContentType:     item.ContentType,
		ContentSource:   item.ContentSource,
		ContentID:       item.ContentID,
		Title:           item.Title(),
		VideoResolution: item.VideoResolution,
		VideoSource:     item.VideoSource,
		VideoCodec:      item.VideoCodec,
		Video3D:         item.Video3D,
		VideoModifier:   item.VideoModifier,
		ReleaseGroup:    item.ReleaseGroup,
		Seeders:         item.Seeders,
		Leechers:        item.Leechers,
		PublishedAt:     item.PublishedAt,
		CreatedAt:       item.CreatedAt,
		UpdatedAt:       item.UpdatedAt,
		Torrent:         item.Torrent,
	}
	if item.Content.ID != "" {
		c.Content = &item.Content
	}

	languages := item.Languages.Slice()
	if len(languages) > 0 {
		c.Languages = languages
	}

	if len(item.Episodes) > 0 {
		c.Episodes = &Episodes{
			Label:   item.Episodes.String(),
			Seasons: item.Episodes.SeasonEntries(),
		}
	}

	return c
}

type TorrentSourceInfo struct {
	Key      string
	Name     string
	ImportID model.NullString
	Seeders  model.NullUint
	Leechers model.NullUint
}

func TorrentSourceInfosFromTorrent(t model.Torrent) []TorrentSourceInfo {
	sources := make([]TorrentSourceInfo, 0, len(t.Sources))

	for _, s := range t.Sources {
		sources = append(sources, TorrentSourceInfo{
			Key:      s.Source,
			Name:     s.TorrentSource.Name,
			ImportID: s.ImportID,
			Seeders:  s.Seeders,
			Leechers: s.Leechers,
		})
	}

	return sources
}

type TorrentContentSearchQueryInput struct {
	q.SearchParams
	Facets     *gen.TorrentContentFacetsInput
	OrderBy    []gen.TorrentContentOrderByInput
	InfoHashes graphql.Omittable[[]protocol.ID]
}

type TorrentContentSearchResult struct {
	TotalCount           uint
	TotalCountIsEstimate bool
	HasNextPage          bool
	Items                []TorrentContent
	Aggregations         gen.TorrentContentAggregations
}

func (t TorrentContentQuery) Search(
	ctx context.Context,
	input TorrentContentSearchQueryInput,
) (TorrentContentSearchResult, error) {
	hasQueryString := input.QueryString.Valid

	fullOrderBy := maps.NewInsertMap[search.TorrentContentOrderBy, search.OrderDirection]()

	for _, ob := range input.OrderBy {
		if ob.Field == gen.TorrentContentOrderByFieldRelevance && !hasQueryString {
			continue
		}

		direction := search.OrderDirectionAscending
		if desc, ok := ob.Descending.ValueOK(); ok && *desc {
			direction = search.OrderDirectionDescending
		}

		field, err := search.ParseTorrentContentOrderBy(ob.Field.String())
		if err != nil {
			return TorrentContentSearchResult{}, err
		}

		fullOrderBy.Set(field, direction)
	}

	// L3 path-typeahead route (flag-gated). TypeaheadEnabled() is nil-safe: with
	// the pathsearch feature off it returns false and this branch is skipped
	// entirely, so the PostgreSQL path below runs byte-identically to before.
	// served=false (ineligible / fail-loud fallback) also falls through to it.
	//
	// pathsearchOrderEligible additionally gates the route on the requested order:
	// L3 returns recall/score (relevance) order over a CAPPED oversample, so any
	// explicit structured sort (seeders/size/published_at/...) must take the
	// PostgreSQL path, which sorts over the full match set rather than the capped
	// candidate sample (P0-2).
	//
	// Healthy() gates the WHOLE route on the cached L3 health signal (finding #4):
	// when L3 is provably unhealthy (unreachable / not SERVING / empty index) the
	// route is skipped entirely so the query goes straight to PostgreSQL — avoiding
	// a per-query dial error + the latency of rediscovering L3 is down on every
	// request. The composer's trustEmpty() gate remains as defense-in-depth for the
	// race where health flips between this check and the candidate dial.
	if t.Pathsearch.TypeaheadEnabled() && hasQueryString &&
		t.Pathsearch.Eligible(input.QueryString.String) &&
		pathsearchOrderEligible(input.OrderBy) &&
		t.Pathsearch.Healthy() {
		result, served, err := t.Pathsearch.TorrentContent(
			ctx,
			pathSearchFilters(input),
			torrentContentBaseOptions(input, fullOrderBy),
			searchPageLimit(input.SearchParams),
			searchPageOffset(input.SearchParams),
			nil, // L3 sort is recall-selection only; PG applies the real order below
		)
		if err != nil {
			return TorrentContentSearchResult{}, err
		}

		if served {
			return transformTorrentContentSearchResult(result)
		}
	}

	options := []q.Option{
		q.DefaultOption(),
		search.TorrentContentCoreJoins(),
		search.HydrateTorrentContentContent(),
		search.HydrateTorrentContentTorrent(),
	}
	options = append(options, input.Option())

	if input.Facets != nil {
		options = append(options, torrentContentFacetsOption(*input.Facets))
	}

	if infoHashes, ok := input.InfoHashes.ValueOK(); ok {
		options = append(options, q.Where(search.TorrentContentInfoHashCriteria(infoHashes...)))
	}

	// FIND-2 (c5717827): on the PostgreSQL fallback path, rewrite a lone-relevance
	// web-UI default to seeders DESC (flag-gated, default off). The L3 route above
	// is the primary relevance-latency mitigation; this guards the PG path taken
	// when L3 is disabled/ineligible/not-served.
	orderBy := find2PopularitySortDefault(fullOrderBy, hasQueryString)
	options = append(options, search.TorrentContentFullOrderBy(orderBy).Option())

	result, resultErr := t.TorrentContentSearch.TorrentContent(ctx, options...)
	if resultErr != nil {
		return TorrentContentSearchResult{}, resultErr
	}

	return transformTorrentContentSearchResult(result)
}

// torrentContentBaseOptions builds the search options for the L3-routed query:
// the same joins/hydration/facets/info-hash filters and user ordering as the
// normal path, but WITHOUT pagination and WITHOUT the free-text tsquery (L3 owns
// the path text and the composer paginates in Go after exact-refine).
//
// CRITICAL (P0-1): unlike the PostgreSQL path, this MUST NOT carry a page limit.
// q.DefaultOption() sets Limit(10); pushing that into the candidate IN(...) query
// would cap it at 10 rows regardless of the candidate budget, so refine+paginate
// would operate over <=10 torrents and every page past the first would be empty.
// The candidate set is already bounded by the composer's MaxCandidates budget, so
// the IN-query returns ALL budgeted rows (no LIMIT) and the composer applies the
// real page window in Go after exact-refine. We keep DefaultOption's aggregation
// budget (needed for facet computation) but drop its Limit.
func torrentContentBaseOptions(
	input TorrentContentSearchQueryInput,
	fullOrderBy maps.InsertMap[search.TorrentContentOrderBy, search.OrderDirection],
) []q.Option {
	options := []q.Option{
		q.WithAggregationBudget(defaultAggregationBudget),
		search.TorrentContentCoreJoins(),
		search.HydrateTorrentContentContent(),
		search.HydrateTorrentContentTorrent(),
	}

	if input.Facets != nil {
		options = append(options, torrentContentFacetsOption(*input.Facets))
	}

	if infoHashes, ok := input.InfoHashes.ValueOK(); ok {
		options = append(options, q.Where(search.TorrentContentInfoHashCriteria(infoHashes...)))
	}

	options = append(options, search.TorrentContentFullOrderBy(fullOrderBy).Option())

	return options
}

// pathSearchFilters extracts the L3 exact-refine ingredients from the search
// input: the path free-text plus any extensions implied by a file-type facet
// (expanded via model.FileType.Extensions()).
func pathSearchFilters(input TorrentContentSearchQueryInput) pathsearch.Filters {
	f := pathsearch.Filters{Query: input.QueryString.String}

	if input.Facets == nil {
		return f
	}

	ft, ok := input.Facets.TorrentFileType.ValueOK()
	if !ok || ft == nil {
		return f
	}

	fileTypes, ok := ft.Filter.ValueOK()
	if !ok {
		return f
	}

	for _, fileType := range fileTypes {
		f.Extensions = append(f.Extensions, fileType.Extensions()...)
	}

	return f
}

// defaultPageSize mirrors q.DefaultOption's Limit(10): the page size the
// PostgreSQL path falls back to when GraphQL `limit` is omitted. The L3 route must
// size its candidate budget AND paginate from the SAME default — otherwise an
// omitted limit makes searchPageLimit return 0, collapsing candidateBudget to
// ~OversampleFactor and serving that tiny set (with paginate(limit=0) returning
// everything) as a falsely-complete result (P0-5).
const defaultPageSize uint = 10

// defaultAggregationBudget mirrors the aggregation budget set by q.DefaultOption
// (WithAggregationBudget(5_000)); the L3 baseOptions keep it (for facets) while
// dropping DefaultOption's page limit (see torrentContentBaseOptions, P0-1).
const defaultAggregationBudget float64 = 5_000

// pathsearchOrderEligible reports whether the requested ordering is compatible
// with the L3 route. L3 returns candidates in recall/score (relevance) order over
// a CAPPED oversample, and its seeders fast-field is hardcoded 0; PostgreSQL then
// re-sorts only that capped subset. So an explicit structured sort
// (seeders/size/published_at/files_count/...) over the capped sample is NOT the
// global top-N — the globally highest-ranked match may never have entered the
// candidate budget. Such queries must take the PostgreSQL path, which sorts over
// the full match set.
//
// Eligible orderings: an empty OrderBy (the webui's default for a query, which is
// relevance) and an explicit relevance sort. Any non-relevance field is
// ineligible. (P0-2)
func pathsearchOrderEligible(orderBy []gen.TorrentContentOrderByInput) bool {
	for _, ob := range orderBy {
		if ob.Field != gen.TorrentContentOrderByFieldRelevance {
			return false
		}
	}

	return true
}

// searchPageLimit / searchPageOffset replicate q.SearchParams.Option's page-window
// arithmetic so the composer can paginate in Go (the L3 route does not push the
// page window into PostgreSQL). An omitted limit resolves to defaultPageSize, the
// same default the PostgreSQL path uses via q.DefaultOption (P0-5).
func searchPageLimit(s q.SearchParams) uint {
	if s.Limit.Valid {
		return s.Limit.Uint
	}

	return defaultPageSize
}

func searchPageOffset(s q.SearchParams) uint {
	offset := uint(0)

	if s.Limit.Valid && s.Page.Valid && s.Page.Uint > 0 {
		offset += (s.Page.Uint - 1) * s.Limit.Uint
	}

	if s.Offset.Valid {
		offset += s.Offset.Uint
	}

	return offset
}

// find2PopularitySortDefault implements the FIND-2 mitigation (flag-gated,
// default OFF). The web UI sends `relevance` as the sole order for every typed
// query; `relevance` is ts_rank_cd over the whole match-set, which is a ~49s
// wall on broad common terms (e.g. "x264" → millions of matches). When the
// PopularitySortDefault flag is ON and the request is exactly that web-UI
// default — a single `relevance` clause together with a query string — we
// rewrite the order to `seeders DESC` (which already carries an info_hash
// tiebreak) and drop ts_rank_cd from the sort entirely. True relevance stays
// opt-in: any order that names a non-relevance field, or relevance alongside an
// explicit extra field, is passed through untouched, as is any query without a
// search string. See dv4-go-integration-notes.md for the UI-side alternative.
func find2PopularitySortDefault(
	orderBy maps.InsertMap[search.TorrentContentOrderBy, search.OrderDirection],
	hasQueryString bool,
) maps.InsertMap[search.TorrentContentOrderBy, search.OrderDirection] {
	if !search.FeatureFlagsValue().PopularitySortDefault || !hasQueryString {
		return orderBy
	}

	// Only rewrite the exact web-UI default: a lone `relevance` clause.
	if orderBy.Len() != 1 {
		return orderBy
	}

	if !orderBy.Has(search.TorrentContentOrderByRelevance) {
		return orderBy
	}

	rewritten := maps.NewInsertMap[search.TorrentContentOrderBy, search.OrderDirection]()
	rewritten.Set(search.TorrentContentOrderBySeeders, search.OrderDirectionDescending)

	return rewritten
}

func torrentContentFacetsOption(input gen.TorrentContentFacetsInput) q.Option {
	var qFacets []q.Facet
	if contentType, ok := input.ContentType.ValueOK(); ok {
		qFacets = append(qFacets, torrentContentTypeFacet(*contentType))
	}

	if torrentSource, ok := input.TorrentSource.ValueOK(); ok {
		qFacets = append(qFacets, torrentSourceFacet(*torrentSource))
	}

	if torrentTag, ok := input.TorrentTag.ValueOK(); ok {
		qFacets = append(qFacets, torrentTagFacet(*torrentTag))
	}

	if torrentFileType, ok := input.TorrentFileType.ValueOK(); ok {
		qFacets = append(qFacets, torrentFileTypeFacet(*torrentFileType))
	}

	if language, ok := input.Language.ValueOK(); ok {
		qFacets = append(qFacets, languageFacet(*language))
	}

	if genre, ok := input.Genre.ValueOK(); ok {
		qFacets = append(qFacets, genreFacet(*genre))
	}

	if releaseYear, ok := input.ReleaseYear.ValueOK(); ok {
		qFacets = append(qFacets, releaseYearFacet(*releaseYear))
	}

	if videoResolution, ok := input.VideoResolution.ValueOK(); ok {
		qFacets = append(qFacets, videoResolutionFacet(*videoResolution))
	}

	if videoSource, ok := input.VideoSource.ValueOK(); ok {
		qFacets = append(qFacets, videoSourceFacet(*videoSource))
	}

	return q.WithFacet(qFacets...)
}

func transformTorrentContentSearchResult(
	result q.GenericResult[search.TorrentContentResultItem],
) (TorrentContentSearchResult, error) {
	aggs, aggsErr := transformTorrentContentAggregations(result.Aggregations)
	if aggsErr != nil {
		return TorrentContentSearchResult{}, aggsErr
	}

	items := make([]TorrentContent, 0, len(result.Items))
	for _, item := range result.Items {
		items = append(items, NewTorrentContentFromResultItem(item))
	}

	return TorrentContentSearchResult{
		TotalCount:           result.TotalCount,
		TotalCountIsEstimate: result.TotalCountIsEstimate,
		HasNextPage:          result.HasNextPage,
		Items:                items,
		Aggregations:         aggs,
	}, nil
}

func transformTorrentContentAggregations(aggs q.Aggregations) (gen.TorrentContentAggregations, error) {
	var (
		result gen.TorrentContentAggregations
		err    error
	)

	result.ContentType, err = contentTypeAggs(aggs[search.TorrentContentTypeFacetKey].Items)
	if err != nil {
		return result, err
	}

	result.TorrentSource, err = torrentSourceAggs(aggs[search.TorrentSourceFacetKey].Items)
	if err != nil {
		return result, err
	}

	result.TorrentTag, err = torrentTagAggs(aggs[search.TorrentTagFacetKey].Items)
	if err != nil {
		return result, err
	}

	result.TorrentFileType, err = torrentFileTypeAggs(aggs[search.TorrentFileTypeFacetKey].Items)
	if err != nil {
		return result, err
	}

	result.Language, err = languageAggs(aggs[search.LanguageFacetKey].Items)
	if err != nil {
		return result, err
	}

	result.Genre, err = genreAggs(aggs[search.ContentGenreFacetKey].Items)
	if err != nil {
		return result, err
	}

	result.ReleaseYear, err = releaseYearAggs(aggs[search.ReleaseYearFacetKey].Items)
	if err != nil {
		return result, err
	}

	result.VideoResolution, err = videoResolutionAggs(aggs[search.VideoResolutionFacetKey].Items)
	if err != nil {
		return result, err
	}

	result.VideoSource, err = videoSourceAggs(aggs[search.VideoSourceFacetKey].Items)
	if err != nil {
		return result, err
	}

	return result, nil
}
