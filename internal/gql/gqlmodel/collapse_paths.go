package gqlmodel

import (
	"context"
	"errors"

	q "github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/pathsearch"
)

// TorrentContentCollapsePathsInput is the GraphQL-facing input for the
// collapse:path query. Unlike the search input it carries no facets: v1 collapses
// on the raw path free-text only. An omitted limit/offset resolves to 0 and is
// defaulted/clamped in CollapsePaths, exactly like the search route.
type TorrentContentCollapsePathsInput struct {
	QueryString string
	Limit       uint
	Offset      uint
}

// TorrentContentPathGroup is one distinct matched file path and the info hashes of
// the candidate torrents that contain a file at that path. v1 is UNHYDRATED: raw
// info hashes only; clients hydrate via the existing search-by-infoHashes.
type TorrentContentPathGroup struct {
	Path       string
	InfoHashes []protocol.ID
}

type TorrentContentCollapsePathsResult struct {
	Groups []TorrentContentPathGroup
}

// ErrPathCollapseUnavailable is returned when the collapse:path route cannot
// serve the query: the pathsearch composer is absent, the SEARCH_PATH_COLLAPSE
// feature is disabled, L3 is unhealthy, or the composer declined to serve
// (served=false). Unlike the TorrentContent route there is NO PostgreSQL fallback
// for collapse:path, so an unserved query is a hard error, never a silent empty.
var ErrPathCollapseUnavailable = errors.New("path collapse unavailable")

// ErrPathCollapseQueryTooShort is returned when the query is below the composer's
// minimum length for the L3 route (the same eligibility gate the search route uses
// to fall back to PostgreSQL; here it is a client error because there is no
// fallback).
var ErrPathCollapseQueryTooShort = errors.New("path collapse query too short")

// CollapsePaths exposes the L3-routed collapse:path capability
// (Composer.CollapsePaths) over GraphQL. It applies the SAME whole-route gate as
// the TorrentContent L3 branch — composer present + collapse enabled + query
// eligible + L3 healthy — before dialing the route (the documented gate-7 #95
// follow-up). CollapseEnabled()/Healthy() are nil-safe, so a disabled feature or
// nil composer yields ErrPathCollapseUnavailable rather than a panic. There is no
// PostgreSQL fallback: a failed gate or served=false is always a clear error.
func (t TorrentContentQuery) CollapsePaths(
	ctx context.Context,
	input TorrentContentCollapsePathsInput,
) (TorrentContentCollapsePathsResult, error) {
	if !t.Pathsearch.CollapseEnabled() || !t.Pathsearch.Healthy() {
		return TorrentContentCollapsePathsResult{}, ErrPathCollapseUnavailable
	}

	if !t.Pathsearch.Eligible(input.QueryString) {
		return TorrentContentCollapsePathsResult{}, ErrPathCollapseQueryTooShort
	}

	// Default an omitted limit to the same page size the search route uses, then
	// hard-clamp it: like the search route, limit sizes the candidate-decode budget,
	// so a hostile GraphQL limit must not size the per-request blob decode (gate-7
	// Finding B). offset passes through — the composer bounds the total budget.
	limit := input.Limit
	if limit == 0 {
		limit = defaultPageSize
	}

	if limit > maxPathSearchLimit {
		limit = maxPathSearchLimit
	}

	groups, served, err := t.Pathsearch.CollapsePaths(
		ctx,
		pathsearch.Filters{Query: input.QueryString},
		collapsePathsQueryOptions(),
		limit,
		input.Offset,
		nil, // L3 orders candidates by recall/score; collapse groups in first-seen order
	)
	if err != nil {
		return TorrentContentCollapsePathsResult{}, err
	}

	if !served {
		return TorrentContentCollapsePathsResult{}, ErrPathCollapseUnavailable
	}

	return newCollapsePathsResult(groups), nil
}

// collapsePathsQueryOptions builds the PostgreSQL option set the composer's
// chunked refine needs: the torrent (blob) hydrator + core joins so filesForRefine
// can decode each candidate's paths. It mirrors the Refine set of
// torrentContentQueryOptions minus facets/order/info-hash (collapse v1 carries no
// facets, and the composer re-imposes L3 order per chunk). It must NOT carry a page
// limit — the composer paginates the candidate budget in Go (P0-1).
func collapsePathsQueryOptions() pathsearch.QueryOptions {
	refine := []q.Option{
		search.TorrentContentCoreJoins(),
		search.HydrateTorrentContentContent(),
		search.HydrateTorrentContentTorrent(),
	}

	return pathsearch.QueryOptions{Refine: refine}
}

func newCollapsePathsResult(groups []pathsearch.PathGroup) TorrentContentCollapsePathsResult {
	out := make([]TorrentContentPathGroup, 0, len(groups))
	for _, g := range groups {
		out = append(out, TorrentContentPathGroup{Path: g.Path, InfoHashes: g.InfoHashes})
	}

	return TorrentContentCollapsePathsResult{Groups: out}
}
