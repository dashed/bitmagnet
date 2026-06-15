package pathsearch

import (
	"context"
	"sort"
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

// HealthGate reports L3's last-known trustworthiness for the authoritative-empty
// decision: true when the sidecar is SERVING and acceptably fresh, false when it
// is down / mid-(re)build / lagging. It is consulted ONLY to decide whether a
// ZERO-candidate result is an authoritative empty or a possible false negative
// (see TorrentContent / CollapsePaths, P0-3).
//
// It is expected to read CACHED state published by a background health poller
// (REVIEW-P1 #95 / finding #4) — it is NOT called on the hot path and MUST NOT
// itself perform a blocking RPC. A nil gate means "no health signal wired": the
// composer then trusts an empty L3 response as authoritative, which is only valid
// once L3 recall coverage ⊇ PostgreSQL has been proven (the gate-7 T10
// prerequisite). See the zero-candidate branches below.
type HealthGate func() (healthy bool)

// ComposerOption customises an otherwise-default Composer.
type ComposerOption func(*Composer)

// WithHealthGate wires a cached L3 health signal used to validate
// authoritative-empty results (P0-3 / finding #4). nil leaves the composer
// trusting empty results as today.
func WithHealthGate(gate HealthGate) ComposerOption {
	return func(c *Composer) { c.health = gate }
}

// WithMetrics wires the L3 route-outcome counters (finding #4 / HP-3) so the
// gate-7 harness can prove L3 was actually served. nil is a no-op (the Metrics
// methods are nil-safe).
func WithMetrics(metrics *Metrics) ComposerOption {
	return func(c *Composer) { c.metrics = metrics }
}

// Composer routes a path-text search through the L3 candidate sidecar + in-process
// L1 exact-refine. A nil *Composer means the feature is off; call sites must
// short-circuit on the enable flag and a nil composer before invoking it.
type Composer struct {
	l3     candidateSource
	pg     torrentContentSearcher
	cfg    ComposerConfig
	logger *zap.SugaredLogger
	// health, when non-nil, gates the authoritative-empty decision: a
	// zero-candidate result is trusted as an exact empty only when health reports
	// healthy; otherwise the route falls back to PostgreSQL (P0-3).
	health HealthGate
	// metrics, when non-nil, counts route outcomes (served|fallback|ineligible|
	// error) so the gate-7 harness can prove L3 was actually exercised. nil-safe.
	metrics *Metrics
}

// NewComposer builds a Composer. It is only constructed when the feature is
// enabled (see searchfx); the zero/nil *Composer is the safe disabled state.
func NewComposer(
	l3 candidateSource,
	pg torrentContentSearcher,
	cfg ComposerConfig,
	logger *zap.SugaredLogger,
	opts ...ComposerOption,
) *Composer {
	if cfg.OversampleFactor < 1 {
		cfg.OversampleFactor = 1
	}

	c := &Composer{l3: l3, pg: pg, cfg: cfg, logger: logger}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// trustEmpty reports whether a zero-candidate L3 response should be served as an
// authoritative empty result. With no health gate wired it trusts the empty (the
// gate-7 coverage assumption); with a gate it trusts the empty only when L3 is
// healthy — a known-unhealthy/stale L3 may be returning a false negative for a
// torrent PostgreSQL still has, so the route falls back instead. (P0-3)
func (c *Composer) trustEmpty() bool {
	return c.health == nil || c.health()
}

// Healthy reports whether the L3 route should be attempted at all. It gates the
// WHOLE route at the call site (torrent_content.go): when L3 is provably
// unhealthy the route is skipped entirely so the query goes straight to
// PostgreSQL, avoiding a per-query dial error + the latency of discovering L3 is
// down on every request (finding #4). A nil composer reports false (feature
// off); with no health gate wired it reports true (preserves today's behavior:
// the route is always attempted). It reads only the cached HealthGate and never
// performs a blocking RPC.
func (c *Composer) Healthy() bool {
	if c == nil {
		return false
	}

	return c.health == nil || c.health()
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

// DefaultMaxCandidates is the hard candidate-budget ceiling enforced when the
// configured MaxCandidates is 0 (unset / zero-valued ComposerConfig). It exists
// so a missing or zero config can NEVER yield an unbounded blob-decode budget:
// the L3 route's per-request memory is ALWAYS bounded. Previously a 0 meant "no
// cap", which (combined with an unclamped user limit) is the gate-7 Finding B
// OOM/DoS — a single large request could decode an unbounded candidate set.
const DefaultMaxCandidates uint = 2000

// candidateBudget sizes the COUNT of candidate torrents to fetch and blob-decode
// for a page window. It must cover offset+limit AFTER refine drops, hence the
// oversample multiplier, bounded by the hard MaxCandidates cap.
//
// This is a candidate-COUNT bound, not a per-torrent BYTE bound (see the residual
// note in candidates()). It is the number candidates() TRUNCATES the sidecar
// response to before decoding — necessary because the sidecar treats the request
// Limit as a floor and adds its own oversample, so the cap is only actually
// honored by that truncation (gate-7 Finding B).
//
// The returned value is always bounded: a 0/unset MaxCandidates falls back to
// DefaultMaxCandidates rather than meaning "no cap", and the multiplication is
// overflow-guarded, so no input (a huge user limit, a deep offset, or a
// misconfigured/zero cap) can size an arbitrarily large decode set. The
// 0→default + overflow guards are defense-in-depth on top of the truncation.
func (c *Composer) candidateBudget(limit, offset uint) uint {
	need := offset + limit
	if need == 0 {
		need = limit
	}

	if need == 0 {
		need = 1
	}

	// The cap is ALWAYS applied; a 0/unset config is treated as the safe default,
	// never as "unbounded".
	maxCands := c.cfg.MaxCandidates
	if maxCands == 0 {
		maxCands = DefaultMaxCandidates
	}

	// OversampleFactor is normalized to >= 1 in NewComposer, so budget >= need
	// absent overflow; budget < need is therefore an unsigned-overflow sentinel
	// (a hostile offset+limit), which we clamp straight to the cap.
	budget := need * c.cfg.OversampleFactor
	if budget < need || budget > maxCands {
		budget = maxCands
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
	budget := c.candidateBudget(limit, offset)

	resp, err := c.l3.PathCandidates(ctx, &pb.PathCandidatesRequest{
		Query: f.Query,
		Limit: uint32(budget),
		Sort:  sorts,
	})
	if err != nil {
		return nil, err
	}

	// Hard-truncate the candidate set to `budget` BEFORE the PG IN(...) requery and
	// the per-torrent blob decode. The sidecar treats Limit as a FLOOR, not a cap:
	// it adds its own oversample (bitmagnet-rs pathsearch candidate_limit =
	// min(limit + DEFAULT_OVERSAMPLE(200), MAX_CANDIDATES(5000))) and returns up to
	// ~budget+200 candidates in relevance order. Decoding all of them is the gate-7
	// Finding B blow-up: a budget=2000 request received ~2200 candidates and
	// blob-decoded all of them, OOMing the 2Gi canary (exit 137). The budget was
	// bounded-but-large, NOT unbounded. Truncating to the top-`budget` keeps the
	// MOST RELEVANT set (the sidecar orders by relevance), so it is exactly the
	// MaxCandidates contract with ZERO recall-within-budget loss, and it makes the
	// ACTUAL decode count == the asserted budget (≤ MaxCandidates). This also bounds
	// the deep-offset worst case (candidateBudget(200,100000)=2000 → decode ≤2000).
	// NOTE: this caps the candidate COUNT, not per-torrent BYTES — a single
	// huge-fileset torrent (p99=743, max 88,561 files) can still spike memory on
	// decode despite the count cap; that per-torrent byte bound is the known T10
	// residual (deferred: a files-per-torrent truncate would silently drop matches,
	// violating the route's fail-loud refine invariant).
	cands := resp.GetCandidates()
	if uint(len(cands)) > budget {
		cands = cands[:budget]
	}

	out := make([]protocol.ID, 0, len(cands))

	for _, cand := range cands {
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

// orderItemsByIDs re-sorts the PostgreSQL candidate rows IN PLACE to match the L3
// recall order of ids. The candidate IN(...) requery returns rows in PG-natural
// order (no meaningful relevance order is available there: L3 owns the path
// free-text, so no tsquery/rank is pushed into PG), but the route only reaches L3
// for the relevance/unset ordering (structured sorts bypass it, P0-2), and L3's
// ngram-recall order IS the path-relevance the route advertises. Serving the PG
// order would therefore return results in the wrong order while claiming
// relevance. A stable sort by recall position restores it; rows whose info_hash is
// absent from the recall set (should not happen — candidates come from L3) sort to
// the end rather than being dropped. (#98 relevance-order)
func orderItemsByIDs(items []search.TorrentContentResultItem, ids []protocol.ID) {
	pos := make(map[protocol.ID]int, len(ids))

	for i, id := range ids {
		if _, seen := pos[id]; !seen {
			pos[id] = i
		}
	}

	rank := func(it search.TorrentContentResultItem) int {
		if p, ok := pos[it.Torrent.InfoHash]; ok {
			return p
		}

		return len(ids) // unknown info_hash → after all recall-ranked rows
	}

	sort.SliceStable(items, func(a, b int) bool {
		return rank(items[a]) < rank(items[b])
	})
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
		c.metrics.IncRoute(RouteIneligible)

		return search.TorrentContentResult{}, false, nil
	}

	ids, err := c.candidates(ctx, f, limit, offset, sorts)
	if err != nil {
		c.metrics.IncRoute(RouteError)

		return search.TorrentContentResult{}, false, err
	}

	if len(ids) == 0 {
		// No candidates. When L3 is healthy this IS the answer for the path query —
		// an exact (estimated) empty result, nothing to fall back to. But when L3 is
		// known-unhealthy/stale the empty may be a false negative for a torrent
		// PostgreSQL still has (mid-backfill / lagging / down), so fall back rather
		// than serve a false "no results". NOTE: the PG path matches name/tsv, not
		// arbitrary file paths, so that fallback is name-semantics — acceptable ONLY
		// because it is reached solely when L3 is provably unreliable. (P0-3 / #4)
		if c.trustEmpty() {
			c.metrics.IncRoute(RouteServed)

			return search.TorrentContentResult{TotalCountIsEstimate: true}, true, nil
		}

		c.metrics.IncRoute(RouteFallback)

		if c.logger != nil {
			c.logger.Warnw("pathsearch: zero L3 candidates while L3 unhealthy; falling back to PostgreSQL",
				"query", f.Query)
		}

		return search.TorrentContentResult{}, false, nil
	}

	pgResult, err := c.orderedCandidateRows(ctx, baseOptions, ids)
	if err != nil {
		c.metrics.IncRoute(RouteError)

		return search.TorrentContentResult{}, false, err
	}

	// Restore L3 recall (path-relevance) order: the PG IN(...) requery returns
	// PG-natural order, but the route advertises relevance. Re-sort to the
	// L3-returned id order BEFORE refine/paginate so the served page is in the
	// recall order, not PG order. (#98 relevance-order)
	orderItemsByIDs(pgResult.Items, ids)

	refined, ok := keepMatching(pgResult.Items, func(it search.TorrentContentResultItem) (bool, bool) {
		return torrentRefine(it.Torrent, pred)
	})
	if !ok {
		// CAVEAT B: a candidate's files were unobtainable — fail loud, fall back to
		// the plain PG path rather than serve a silently truncated result.
		c.metrics.IncRoute(RouteFallback)

		if c.logger != nil {
			c.logger.Warnw("pathsearch: candidate files unobtainable; falling back to PostgreSQL",
				"query", f.Query)
		}

		return search.TorrentContentResult{}, false, nil
	}

	page := paginate(refined, offset, limit)
	c.metrics.IncRoute(RouteServed)

	return search.TorrentContentResult{
		Items: page,
		// TotalCount is an honest CAPPED estimate: it counts the refined matches
		// within the candidate budget, NOT the global match total (which would
		// require scanning all of PG). TotalCountIsEstimate stays true so callers
		// treat it as a lower-bound-ish hint, not an exact count. We deliberately
		// ignore the client's totalCount flag here — an exact count is not available
		// on the L3 route. (#10)
		TotalCount:           uint(len(refined)),
		TotalCountIsEstimate: true,
		// HasNextPage is computed from rows actually consumed by THIS page, not from
		// offset+limit: with limit==0 paginate returns ALL remaining rows, so there
		// is no next page even though offset+0 < len(refined). Base it on whether
		// refined rows remain after the returned page. (#10)
		HasNextPage: offset+uint(len(page)) < uint(len(refined)),
		// Facets/aggregations PG computed for the candidate set must pass through —
		// hand-building the result previously dropped them, blanking the UI facet
		// sidebar on path searches. (#6)
		Aggregations: pgResult.Aggregations,
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
		c.metrics.IncRoute(RouteIneligible)

		return nil, false, nil
	}

	ids, err := c.candidates(ctx, f, limit, offset, sorts)
	if err != nil {
		c.metrics.IncRoute(RouteError)

		return nil, false, err
	}

	if len(ids) == 0 {
		// Same authoritative-empty gate as TorrentContent (P0-3): trust the empty
		// only when L3 is healthy, else fall back.
		if c.trustEmpty() {
			c.metrics.IncRoute(RouteServed)

			return nil, true, nil
		}

		c.metrics.IncRoute(RouteFallback)

		if c.logger != nil {
			c.logger.Warnw("pathsearch: zero L3 collapse candidates while L3 unhealthy; falling back",
				"query", f.Query)
		}

		return nil, false, nil
	}

	pgResult, err := c.orderedCandidateRows(ctx, baseOptions, ids)
	if err != nil {
		c.metrics.IncRoute(RouteError)

		return nil, false, err
	}

	// Collapse builds groups in first-seen order; reorder the candidate rows to L3
	// recall order so "first-seen" follows path-relevance, not PG-natural order
	// (same #98 fix as TorrentContent).
	orderItemsByIDs(pgResult.Items, ids)

	index := make(map[string]int)

	for i := range pgResult.Items {
		item := pgResult.Items[i]

		files, fok := filesForRefine(item.Torrent, pred)
		if !fok {
			// Fail loud: cannot verify this candidate's paths.
			c.metrics.IncRoute(RouteFallback)

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

	c.metrics.IncRoute(RouteServed)

	return paginate(groups, offset, limit), true, nil
}
