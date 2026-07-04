package gqlmodel

import (
	"context"
	"strings"

	q "github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/filesearch"
	"github.com/bitmagnet-io/bitmagnet/internal/search/pathsearch"
)

// FileSearchQuery is the resolver-facing entry point for the file-grained search
// (DV-2 DuckDB sidecar) and path typeahead (DV-3 path-FTS sidecar). It is wired
// behind the transport-neutral filesearch.Client interface so GraphQL does not
// depend on sidecar transport details.
//
// It is gated twice over: the FileSearchEnabled feature flag (default OFF) AND
// the injected Client (filesearch.Disabled() by default). Either being off means
// every call returns filesearch.ErrDisabled — the feature is dark until both the
// sidecar is deployed and the flag is flipped.
type FileSearchQuery struct {
	Client     filesearch.Client
	Pathsearch *pathsearch.Composer
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

	return q.client().FileSearch(ctx, validated)
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
	return FileSearchQuery{Client: t.FileSearchClient, Pathsearch: t.Pathsearch}.Search(ctx, in)
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
			InfoHash:  row.InfoHash,
			Index:     row.Index,
			Path:      row.Path,
			Extension: row.Extension,
			Size:      row.Size,
		})
	}

	return filesearch.FileSearchResult{
		Items:       items,
		TotalCount:  result.TotalCount,
		HasNextPage: result.HasNextPage,
	}
}

func uint64ToUint(v uint64) uint {
	maxUint := ^uint(0)
	if v > uint64(maxUint) {
		return maxUint
	}

	return uint(v)
}
