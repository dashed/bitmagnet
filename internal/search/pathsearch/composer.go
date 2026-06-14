package pathsearch

import (
	"context"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"go.uber.org/zap"
)

// candidateSource is the slice of the L3 client the Composer depends on so tests
// can substitute a fake. *Client satisfies it.
type candidateSource interface {
	PathCandidates(ctx context.Context, req *pb.PathCandidatesRequest) (*pb.PathCandidatesResponse, error)
}

// torrentContentSearcher is the slice of the PostgreSQL search the Composer needs
// to hydrate + filter the L3 candidate set. search.Search satisfies it.
type torrentContentSearcher interface {
	TorrentContent(ctx context.Context, options ...query.Option) (search.TorrentContentResult, error)
}

var _ candidateSource = (*Client)(nil)

// ComposerConfig bounds and tunes the L3 route. Bounds are config values so the
// known broad-single-gram tail can never blob-decode an unbounded candidate set
// (the UX min-chars/debounce is the other half of that defense).
type ComposerConfig struct {
	// MinQueryLength is the server-side guard: queries shorter than this never
	// take the L3 route (broad-gram backpressure). 0 disables the guard.
	MinQueryLength int
	// OversampleFactor multiplies the requested page window (offset+limit) to size
	// the candidate budget, giving exact-refine headroom for false-positive drops.
	// < 1 is treated as 1.
	OversampleFactor uint
	// MaxCandidates hard-caps the candidates fetched and blob-decoded per request,
	// regardless of OversampleFactor. 0 means no cap (not recommended in prod).
	MaxCandidates uint
	// TypeaheadEnabled gates the UI path-typeahead route
	// (SEARCH_PATH_TYPEAHEAD_ENABLED). The master switch is the composer existing
	// at all (nil composer = pathsearch disabled).
	TypeaheadEnabled bool
	// CollapseEnabled gates routing collapse:path through L3 candidates
	// (SEARCH_PATH_COLLAPSE_L3_ENABLED).
	CollapseEnabled bool
}

// Filters carries the typed exact-refine ingredients the call site extracts from
// the search input: the path free-text plus the structured extension/size
// predicate L3 cannot evaluate.
type Filters struct {
	// Query is the raw path substring / free text. Required for the L3 route.
	Query string
	// Extensions is the set of allowed extensions (already expanded from any
	// file-type filter via model.FileType.Extensions()); empty = any.
	Extensions []string
	// MinSize / MaxSize bound file size in bytes; 0 = unbounded on that side.
	MinSize uint
	MaxSize uint
}

// predicate builds the lower-cased exact-refine predicate from the filters.
func (f Filters) predicate() refinePredicate {
	p := refinePredicate{
		substr:  strings.ToLower(strings.TrimSpace(f.Query)),
		minSize: f.MinSize,
		maxSize: f.MaxSize,
	}

	if len(f.Extensions) > 0 {
		p.extensions = make(map[string]struct{}, len(f.Extensions))
		for _, e := range f.Extensions {
			p.extensions[strings.ToLower(e)] = struct{}{}
		}
	}

	return p
}

// Composer routes a path-text search through the L3 candidate sidecar + in-process
// L1 exact-refine. A nil *Composer means the feature is off; call sites must
// short-circuit on the enable flag and a nil composer before invoking it.
type Composer struct {
	l3     candidateSource
	pg     torrentContentSearcher
	cfg    ComposerConfig
	logger *zap.SugaredLogger
}

// NewComposer builds a Composer. It is only constructed when the feature is
// enabled (see searchfx); the zero/nil *Composer is the safe disabled state.
func NewComposer(
	l3 candidateSource,
	pg torrentContentSearcher,
	cfg ComposerConfig,
	logger *zap.SugaredLogger,
) *Composer {
	if cfg.OversampleFactor < 1 {
		cfg.OversampleFactor = 1
	}

	return &Composer{l3: l3, pg: pg, cfg: cfg, logger: logger}
}

// Eligible reports whether the query is long enough for the L3 route (the
// server-side broad-gram guard, complementing the UI min-chars/debounce).
func (c *Composer) Eligible(queryText string) bool {
	return len(strings.TrimSpace(queryText)) >= c.cfg.MinQueryLength
}

// TypeaheadEnabled reports whether the UI path-typeahead route is enabled
// (SEARCH_PATH_TYPEAHEAD_ENABLED). A nil composer (feature off) reports false.
func (c *Composer) TypeaheadEnabled() bool {
	return c != nil && c.cfg.TypeaheadEnabled
}

// CollapseEnabled reports whether collapse:path should route through L3
// (SEARCH_PATH_COLLAPSE_L3_ENABLED). A nil composer (feature off) reports false.
func (c *Composer) CollapseEnabled() bool {
	return c != nil && c.cfg.CollapseEnabled
}

// candidateBudget sizes the candidate set to fetch/decode for a page window. It
// must cover offset+limit AFTER refine drops, hence the oversample multiplier,
// bounded by the hard MaxCandidates cap.
func (c *Composer) candidateBudget(limit, offset uint) uint {
	need := offset + limit
	if need == 0 {
		need = limit
	}

	if need == 0 {
		need = 1
	}

	budget := need * c.cfg.OversampleFactor
	if c.cfg.MaxCandidates > 0 && budget > c.cfg.MaxCandidates {
		budget = c.cfg.MaxCandidates
	}

	return budget
}

// candidates dials L3 for the page's candidate budget and returns the decoded
// info_hash set, in L3's returned (recall) order.
func (c *Composer) candidates(
	ctx context.Context,
	f Filters,
	limit, offset uint,
	sorts []*pb.SortBy,
) ([]protocol.ID, error) {
	resp, err := c.l3.PathCandidates(ctx, &pb.PathCandidatesRequest{
		Query: f.Query,
		Limit: uint32(c.candidateBudget(limit, offset)),
		Sort:  sorts,
	})
	if err != nil {
		return nil, err
	}

	out := make([]protocol.ID, 0, len(resp.GetCandidates()))

	for _, cand := range resp.GetCandidates() {
		id, idErr := protocol.NewIDFromByteSlice(cand.GetInfoHash())
		if idErr != nil {
			// A malformed candidate hash is an L3 bug, not a fatal request error;
			// skip it rather than fail the whole search.
			if c.logger != nil {
				c.logger.Debugw("pathsearch: skipping malformed candidate info_hash", "error", idErr)
			}

			continue
		}

		out = append(out, id)
	}

	return out, nil
}

// orderedCandidateRows restricts the PostgreSQL TorrentContent search to the L3
// candidate set, applying the caller's filters + ordering but NOT pagination —
// pagination happens in Go after refine (see TorrentContent). baseOptions MUST
// already exclude any page limit/offset and the path free-text (which L3 owns).
func (c *Composer) orderedCandidateRows(
	ctx context.Context,
	baseOptions []query.Option,
	ids []protocol.ID,
) (search.TorrentContentResult, error) {
	opts := make([]query.Option, 0, len(baseOptions)+1)
	opts = append(opts, baseOptions...)
	// files_data is part of the default torrent projection, so AfterFind decodes
	// the L1 blob into Torrent.Files; filesForRefine additionally decodes it
	// explicitly. Restrict to the candidate set:
	opts = append(opts, query.Where(search.TorrentContentInfoHashCriteria(ids...)))

	return c.pg.TorrentContent(ctx, opts...)
}

// TorrentContent runs the L3-routed path search and returns an exact, refined,
// paginated result. served=false means the caller should fall back to the normal
// PostgreSQL path: the query was ineligible (too short / no substring), or a
// candidate's files were unobtainable (fail-loud refine — never silently drop).
//
// Pipeline: L3 oversample (capped) -> PG IN(candidates) + filters + OrderBy (no
// page window) -> Go L1 blob-decode refine (drop false positives) -> paginate in
// Go -> estimated counts.
func (c *Composer) TorrentContent(
	ctx context.Context,
	f Filters,
	baseOptions []query.Option,
	limit, offset uint,
	sorts []*pb.SortBy,
) (result search.TorrentContentResult, served bool, err error) {
	pred := f.predicate()
	if pred.substr == "" || !c.Eligible(f.Query) {
		return search.TorrentContentResult{}, false, nil
	}

	ids, err := c.candidates(ctx, f, limit, offset, sorts)
	if err != nil {
		return search.TorrentContentResult{}, false, err
	}

	if len(ids) == 0 {
		// No candidates: an exact (estimated) empty result — nothing to fall back
		// to, this IS the answer for the path query.
		return search.TorrentContentResult{TotalCountIsEstimate: true}, true, nil
	}

	pgResult, err := c.orderedCandidateRows(ctx, baseOptions, ids)
	if err != nil {
		return search.TorrentContentResult{}, false, err
	}

	refined, ok := keepMatching(pgResult.Items, func(it search.TorrentContentResultItem) (bool, bool) {
		return torrentRefine(it.Torrent, pred)
	})
	if !ok {
		// CAVEAT B: a candidate's files were unobtainable — fail loud, fall back to
		// the plain PG path rather than serve a silently truncated result.
		if c.logger != nil {
			c.logger.Warnw("pathsearch: candidate files unobtainable; falling back to PostgreSQL",
				"query", f.Query)
		}

		return search.TorrentContentResult{}, false, nil
	}

	page := paginate(refined, offset, limit)

	return search.TorrentContentResult{
		Items:                page,
		TotalCount:           uint(len(refined)),
		TotalCountIsEstimate: true, // L3 counts torrents (recall), not exact files
		HasNextPage:          offset+limit < uint(len(refined)),
	}, true, nil
}

// PathGroup is one collapsed distinct path and the candidate torrents that
// contain a matching file at that path.
type PathGroup struct {
	Path       string
	InfoHashes []protocol.ID
}

// CollapsePaths routes collapse:path through L3 candidates + L1 exact-refine,
// returning distinct matched paths grouped across the candidate set, in
// first-seen (PG-ordered) order. served=false falls back as in TorrentContent.
//
// NOTE: this is the backend capability for SEARCH_PATH_COLLAPSE_L3_ENABLED; there
// is no existing GraphQL collapse field to attach it to, so the resolver wiring
// is a documented follow-up. The composition + refine semantics are unit-tested
// here so the route is ready when a collapse surface lands.
func (c *Composer) CollapsePaths(
	ctx context.Context,
	f Filters,
	baseOptions []query.Option,
	limit, offset uint,
	sorts []*pb.SortBy,
) (groups []PathGroup, served bool, err error) {
	pred := f.predicate()
	if pred.substr == "" || !c.Eligible(f.Query) {
		return nil, false, nil
	}

	ids, err := c.candidates(ctx, f, limit, offset, sorts)
	if err != nil {
		return nil, false, err
	}

	if len(ids) == 0 {
		return nil, true, nil
	}

	pgResult, err := c.orderedCandidateRows(ctx, baseOptions, ids)
	if err != nil {
		return nil, false, err
	}

	index := make(map[string]int)

	for i := range pgResult.Items {
		item := pgResult.Items[i]

		files, fok := filesForRefine(item.Torrent)
		if !fok {
			// Fail loud: cannot verify this candidate's paths.
			if c.logger != nil {
				c.logger.Warnw("pathsearch: collapse candidate files unobtainable; falling back",
					"query", f.Query)
			}

			return nil, false, nil
		}

		for _, path := range distinctMatchedPaths(files, pred) {
			gi, seen := index[path]
			if !seen {
				gi = len(groups)
				index[path] = gi
				groups = append(groups, PathGroup{Path: path})
			}

			groups[gi].InfoHashes = append(groups[gi].InfoHashes, item.Torrent.InfoHash)
		}
	}

	return paginate(groups, offset, limit), true, nil
}
