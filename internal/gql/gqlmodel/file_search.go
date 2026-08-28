package gqlmodel

import (
	"context"
	"errors"
	"fmt"
	"strings"

	q "github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/gql/gqlmodel/gen"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/filesearch"
	"github.com/bitmagnet-io/bitmagnet/internal/search/pathsearch"
)

// FileSearchQuery is the resolver-facing entry point for file-grained search and
// facets (DV-2 DuckDB sidecar), plus path typeahead (DV-3 path-FTS sidecar). It is
// wired behind the transport-neutral filesearch.Client interface so GraphQL does
// not depend on sidecar transport details.
//
// It is gated twice over: the FileSearchEnabled feature flag (default OFF) AND
// the injected Client (filesearch.Disabled() by default). Either being off means
// every call returns filesearch.ErrDisabled — the feature is dark until both the
// sidecar is deployed and the flag is flipped.
type FileSearchQuery struct {
	Client               filesearch.Client
	Pathsearch           *pathsearch.Composer
	TorrentContentSearch search.TorrentContentSearch
}

// FileSearchInput is the GraphQL-facing input (loosely typed). Validation,
// LIKE-escaping and clamping happen in filesearch.NewFileSearchInput.
type FileSearchInput struct {
	Query      string
	Extensions []string
	MinSize    uint64
	MaxSize    uint64
	InfoHash   *protocol.ID
	Sort       []filesearch.FileSort
	Limit      uint
	Offset     uint
	TotalCount *bool
}

type PathTypeaheadInput struct {
	Prefix string
	Limit  uint
}

func (q FileSearchQuery) client() filesearch.Client {
	if q.Client == nil {
		return filesearch.Disabled()
	}

	return q.Client
}

// Search runs a file-grained search. Returns filesearch.ErrDisabled unless the
// FileSearchEnabled flag is on AND a real client is wired.
func (q FileSearchQuery) Search(ctx context.Context, in FileSearchInput) (filesearch.FileSearchResult, error) {
	if !search.FeatureFlagsValue().FileSearchEnabled {
		return filesearch.FileSearchResult{}, filesearch.ErrDisabled
	}

	validated, err := filesearch.NewFileSearchInput(filesearch.FileSearchParams{
		Query:      in.Query,
		Extensions: in.Extensions,
		MinSize:    in.MinSize,
		MaxSize:    in.MaxSize,
		InfoHash:   in.InfoHash,
		Sort:       in.Sort,
		Limit:      in.Limit,
		Offset:     in.Offset,
		TotalCount: in.TotalCount,
	})
	if err != nil {
		return filesearch.FileSearchResult{}, err
	}

	hasTorrentSort := hasTorrentFieldSort(validated.Sort)

	if q.shouldRouteFileSearchText(validated) {
		result, served, routeErr := q.Pathsearch.SearchFileRows(
			ctx,
			fileSearchPathFilters(validated),
			fileSearchPathQueryOptions(),
			validated.Limit,
			validated.Offset,
			fileSearchPathSorts(validated.Sort),
		)
		if routeErr != nil {
			return filesearch.FileSearchResult{}, routeErr
		}

		if served {
			return fileRowsResult(result), nil
		}
	}

	if hasTorrentSort {
		return filesearch.FileSearchResult{}, filesearch.ErrTorrentSortRequiresRoutedPath
	}

	result, err := q.client().FileSearch(ctx, validated)
	if err != nil {
		return filesearch.FileSearchResult{}, err
	}

	return hydrateFileSearchResult(ctx, q.TorrentContentSearch, result)
}

// FileSearchFacets runs an L2 facet aggregation. Both the file-search master
// switch and the facets-specific switch must be enabled before input validation
// or client delegation.
func (q FileSearchQuery) FileSearchFacets(
	ctx context.Context,
	in filesearch.FacetsParams,
) (filesearch.FacetsResult, error) {
	flags := search.FeatureFlagsValue()
	if !flags.FileSearchEnabled || !flags.FileSearchFacetsEnabled {
		return filesearch.FacetsResult{}, filesearch.ErrDisabled
	}

	validated, err := filesearch.NewFacetsInput(in)
	if err != nil {
		return filesearch.FacetsResult{}, err
	}

	return q.client().Facets(ctx, validated)
}

// PathTypeahead returns path completions for a prefix. Returns
// filesearch.ErrDisabled unless the flag is on AND a real client is wired, and
// filesearch.ErrPrefixTooShort for prefixes under the min-chars threshold.
func (q FileSearchQuery) PathTypeahead(
	ctx context.Context,
	in PathTypeaheadInput,
) (filesearch.PathTypeaheadResult, error) {
	if !search.FeatureFlagsValue().FileSearchEnabled {
		return filesearch.PathTypeaheadResult{}, filesearch.ErrDisabled
	}

	validated, err := filesearch.NewPathTypeaheadInput(in.Prefix, in.Limit)
	if err != nil {
		return filesearch.PathTypeaheadResult{}, err
	}

	// L3 Suggest prefix-index route (flag-gated, default OFF). Falls through to
	// the candidate-derived adapter below on not-served/unhealthy/RPC-error, so it
	// never regresses the adapter. When the flag is OFF this block is skipped
	// entirely and the path below is byte-identical to before.
	if search.FeatureFlagsValue().FileSearchTypeaheadRPCEnabled {
		if suggestions, served, suggestErr := q.Pathsearch.Suggest(
			ctx,
			validated.Prefix,
			validated.Limit,
		); suggestErr == nil && served {
			return filesearch.PathTypeaheadResult{Suggestions: suggestions}, nil
		}
	}

	if q.Pathsearch.TypeaheadEnabled() && q.Pathsearch.Healthy() {
		if !q.Pathsearch.Eligible(validated.Prefix) {
			return filesearch.PathTypeaheadResult{}, filesearch.ErrPrefixTooShort
		}

		suggestions, served, routeErr := q.Pathsearch.PathTypeahead(
			ctx,
			validated.Prefix,
			fileSearchPathQueryOptions(),
			validated.Limit,
		)
		if routeErr != nil {
			return filesearch.PathTypeaheadResult{}, routeErr
		}

		if served {
			return filesearch.PathTypeaheadResult{Suggestions: suggestions}, nil
		}
	}

	return q.client().PathTypeahead(ctx, validated)
}

func (t TorrentContentQuery) FileSearch(ctx context.Context, in FileSearchInput) (filesearch.FileSearchResult, error) {
	return FileSearchQuery{
		Client:               t.FileSearchClient,
		Pathsearch:           t.Pathsearch,
		TorrentContentSearch: t.TorrentContentSearch,
	}.Search(ctx, in)
}

func (t TorrentContentQuery) FileSearchFacets(
	ctx context.Context,
	in filesearch.FacetsParams,
) (filesearch.FacetsResult, error) {
	return FileSearchQuery{
		Client:               t.FileSearchClient,
		Pathsearch:           t.Pathsearch,
		TorrentContentSearch: t.TorrentContentSearch,
	}.FileSearchFacets(ctx, in)
}

func (t TorrentContentQuery) PathTypeahead(
	ctx context.Context,
	in PathTypeaheadInput,
) (filesearch.PathTypeaheadResult, error) {
	return FileSearchQuery{Client: t.FileSearchClient, Pathsearch: t.Pathsearch}.PathTypeahead(ctx, in)
}

func (q FileSearchQuery) shouldRouteFileSearchText(in filesearch.FileSearchInput) bool {
	return strings.TrimSpace(in.Query) != "" &&
		in.InfoHash == nil &&
		q.Pathsearch.FileSearchRouteTextEnabled() &&
		q.Pathsearch.Healthy() &&
		q.Pathsearch.Eligible(in.Query)
}

func fileSearchPathFilters(in filesearch.FileSearchInput) pathsearch.Filters {
	return pathsearch.Filters{
		Query:      in.Query,
		Extensions: in.Extensions,
		MinSize:    uint64ToUint(in.MinSize),
		MaxSize:    uint64ToUint(in.MaxSize),
	}
}

func fileSearchPathQueryOptions() pathsearch.QueryOptions {
	refine := []q.Option{
		search.TorrentContentCoreJoins(),
		search.HydrateTorrentContentContent(),
		search.HydrateTorrentContentTorrent(),
	}

	return pathsearch.QueryOptions{Refine: refine}
}

func fileSearchPathSorts(sortBy []filesearch.FileSort) []pathsearch.FileRowSort {
	if len(sortBy) == 0 {
		return nil
	}

	out := make([]pathsearch.FileRowSort, 0, len(sortBy))
	for _, s := range sortBy {
		out = append(out, pathsearch.FileRowSort{
			Field:      s.Field,
			Descending: s.Descending,
		})
	}

	return out
}

func fileRowsResult(result pathsearch.FileRowsResult) filesearch.FileSearchResult {
	items := make([]filesearch.FileSearchItem, 0, len(result.Rows))
	for _, row := range result.Rows {
		items = append(items, filesearch.FileSearchItem{
			InfoHash:       row.InfoHash,
			Index:          row.Index,
			Path:           row.Path,
			Extension:      row.Extension,
			Size:           row.Size,
			TorrentContent: row.TorrentContent,
		})
	}

	return filesearch.FileSearchResult{
		Items:                items,
		TotalCount:           result.TotalCount,
		TotalCountIsEstimate: result.TotalCountIsEstimate,
		HasNextPage:          result.HasNextPage,
	}
}

func hasTorrentFieldSort(sorts []filesearch.FileSort) bool {
	for _, sort := range sorts {
		if filesearch.IsTorrentFieldSort(sort.Field) {
			return true
		}
	}

	return false
}

func hydrateFileSearchResult(
	ctx context.Context,
	searcher search.TorrentContentSearch,
	result filesearch.FileSearchResult,
) (filesearch.FileSearchResult, error) {
	if len(result.Items) == 0 {
		return result, nil
	}

	if searcher == nil {
		return filesearch.FileSearchResult{}, errors.New(
			"file search torrent content hydrator is not configured",
		)
	}

	infoHashes := fileSearchInfoHashes(result.Items)

	content, err := searcher.TorrentContent(ctx,
		search.TorrentContentCoreJoins(),
		search.HydrateTorrentContentContent(),
		search.HydrateTorrentContentTorrent(),
		q.Where(search.TorrentContentInfoHashCriteria(infoHashes...)),
	)
	if err != nil {
		return filesearch.FileSearchResult{}, err
	}

	byInfoHash := make(map[protocol.ID]search.TorrentContentResultItem, len(content.Items))
	for _, item := range content.Items {
		byInfoHash[item.InfoHash] = item
	}

	for i := range result.Items {
		item, ok := byInfoHash[result.Items[i].InfoHash]
		if !ok {
			return filesearch.FileSearchResult{}, fmt.Errorf(
				"file search torrent content missing for info_hash %s",
				result.Items[i].InfoHash,
			)
		}

		result.Items[i].TorrentContent = item
	}

	return result, nil
}

func fileSearchInfoHashes(items []filesearch.FileSearchItem) []protocol.ID {
	seen := make(map[protocol.ID]struct{}, len(items))
	out := make([]protocol.ID, 0, len(items))

	for _, item := range items {
		if _, ok := seen[item.InfoHash]; ok {
			continue
		}

		seen[item.InfoHash] = struct{}{}

		out = append(out, item.InfoHash)
	}

	return out
}

func NewFileSearchFacetsParams(input gen.FileSearchFacetsInput) filesearch.FacetsParams {
	var params filesearch.FacetsParams

	if query, ok := input.Query.ValueOK(); ok && query != nil {
		params.Query = *query
	}

	if extensions, ok := input.Extensions.ValueOK(); ok {
		params.Extensions = extensions
	}

	if minSize, ok := input.MinSize.ValueOK(); ok && minSize != nil && *minSize > 0 {
		params.MinSize = uint64(*minSize)
	}

	if maxSize, ok := input.MaxSize.ValueOK(); ok && maxSize != nil && *maxSize > 0 {
		params.MaxSize = uint64(*maxSize)
	}

	if fields, ok := input.Facets.ValueOK(); ok {
		params.Fields = make([]string, 0, len(fields))
		for _, field := range fields {
			params.Fields = append(params.Fields, field.String())
		}
	}

	return params
}

func NewFileSearchFacetsResult(result filesearch.FacetsResult) gen.FileSearchFacetsResult {
	facets := make([]gen.FileFacetAgg, 0, len(result.Facets))
	for _, facet := range result.Facets {
		// Skip facet fields this schema version doesn't know: a newer sidecar
		// may emit new fields, and casting an unknown string into the non-null
		// FileFacetField enum would make gqlgen emit an invalid enum value.
		field := gen.FileFacetField(facet.Field)
		if !field.IsValid() {
			continue
		}

		buckets := make([]gen.FileFacetBucketAgg, 0, len(facet.Buckets))
		for _, bucket := range facet.Buckets {
			buckets = append(buckets, gen.FileFacetBucketAgg{
				Value:      bucket.Value,
				Count:      boundedInt(bucket.Count),
				TotalSize:  boundedInt(bucket.TotalSize),
				IsEstimate: false,
			})
		}

		facets = append(facets, gen.FileFacetAgg{
			Field:   field,
			Buckets: buckets,
		})
	}

	return gen.FileSearchFacetsResult{Facets: facets}
}

func boundedInt(v uint64) int {
	maxInt := int(^uint(0) >> 1)
	if v > uint64(maxInt) {
		return maxInt
	}

	return int(v)
}

func uint64ToUint(v uint64) uint {
	maxUint := ^uint(0)
	if v > uint64(maxUint) {
		return maxUint
	}

	return uint(v)
}
