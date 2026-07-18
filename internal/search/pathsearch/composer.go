package pathsearch

import (
	"context"
	"errors"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// candidateSource is the slice of the L3 client the Composer depends on so tests
// can substitute a fake. *Client satisfies it.
type candidateSource interface {
	PathCandidates(ctx context.Context, req *pb.PathCandidatesRequest) (*pb.PathCandidatesResponse, error)
	Suggest(ctx context.Context, req *pb.SuggestRequest) (*pb.SuggestResponse, error)
}

// torrentContentSearcher is the slice of the PostgreSQL search the Composer needs
// to hydrate + filter the L3 candidate set. search.Search satisfies it.
//
// FileCounts is the gate7-4 byte-bound's cheap pre-decode probe: it reads the
// authoritative per-torrent file count (torrent_file_summary, PK-indexed; no
// blob decode) so the composer can decline pathologically large candidates and
// chunk the refine by a cumulative file budget BEFORE any files_data blob is
// hydrated/decoded.
type torrentContentSearcher interface {
	TorrentContent(ctx context.Context, options ...query.Option) (search.TorrentContentResult, error)
	FileCounts(ctx context.Context, ids []protocol.ID) (map[protocol.ID]int, error)
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
	// MaxDecodeCandidates bounds the per-request candidate blob-decode for LATENCY
	// (distinct from MaxCandidates, which bounds MEMORY). candidateBudget grows as
	// (offset+limit)×OversampleFactor, so a large page size makes decode — and thus
	// wall-clock — grow linearly (measured ≈13ms/torrent: limit 100 → 400 decodes ≈
	// 5.3s, far under the MaxCandidates=2000 memory ceiling but well over an
	// interactive budget). This caps the decode count so worst-case latency stays
	// bounded regardless of page size. 0 falls back to DefaultMaxDecodeCandidates
	// (never "no cap" — a 0/unset config is still latency-bounded, matching the
	// MaxCandidates philosophy). The default leaves page-1 sizes ≤50 unchanged; only
	// larger pages / deep offsets are bounded, which may serve a short page for a
	// high-recall query (totalCount/candidate_total still reflects the fuller set).
	MaxDecodeCandidates uint
	// TypeaheadEnabled gates the UI path-typeahead route
	// (SEARCH_PATH_TYPEAHEAD_ENABLED). The master switch is the composer existing
	// at all (nil composer = pathsearch disabled).
	TypeaheadEnabled bool
	// FileSearchRouteText gates routing GraphQL fileSearch queries with non-empty
	// path text through the L3 candidate + L1 refine pipeline
	// (SEARCH_FILE_SEARCH_ROUTE_TEXT). It is an ops rollback switch; the searchfx
	// default is true.
	FileSearchRouteText bool
	// CollapseEnabled gates routing collapse:path through L3 candidates
	// (SEARCH_PATH_COLLAPSE_L3_ENABLED).
	CollapseEnabled bool
	// MaxRefineFiles is the per-torrent file-count SANITY CAP: a candidate whose
	// authoritative file_count exceeds this is DECLINED (excluded + counted +
	// logged) rather than blob-decoded. 0 falls back to DefaultMaxRefineFiles. It
	// bounds the worst-case single-torrent decode so one pathological huge-fileset
	// torrent (real max ~159k files) can never spike memory regardless of the
	// candidate-count cap (gate7-4).
	MaxRefineFiles uint
	// RefineFileBudget is the cumulative file-count budget per refine CHUNK: the
	// kept candidate ids are split into contiguous chunks whose summed file_count
	// stays under this, so peak retained decoded files ≈ budget rather than the
	// whole candidate set. 0 falls back to DefaultRefineFileBudget.
	RefineFileBudget uint
	// MaxChunkTorrents caps the number of torrents per refine chunk regardless of
	// the file budget (so a chunk of many tiny torrents stays bounded too). 0
	// falls back to DefaultMaxChunkTorrents.
	MaxChunkTorrents uint
	// RetainedFileBudget caps the cumulative decoded files RETAINED across all
	// refined matches of one request: once appending the next match would exceed
	// it, the composer stops and serves the accumulated top-relevance prefix as a
	// memory-capped estimate. This is the bound that holds regardless of MATCH RATE
	// (the chunk budget bounds only the transient decode). 0 falls back to
	// DefaultRetainedFileBudget.
	RetainedFileBudget uint
	// RouteTimeout bounds the WHOLE L3 route (candidates + FileCounts + every chunk
	// decode + the Go exact-refine), not just the sidecar RPC (the client owns its
	// own RPC timeout, which nests under this). When it fires mid-route the composer
	// serves the accumulated top-relevance prefix as a deadline-capped estimate
	// (served=true) rather than letting an error bubble into the resolver's broad-FTS
	// PG fallback wall. 0 falls back to DefaultRouteTimeout (gate7-6). It is a
	// distinct knob from the sidecar PathsearchTimeout (the 5s RPC bound).
	RouteTimeout time.Duration
	// MaxConcurrentRefines bounds how many EXPENSIVE multi-chunk refines run at once
	// across BOTH routes (one shared semaphore). The cheap single-chunk fast path
	// never acquires a slot, so a saturated limiter never blocks/sheds a
	// normal/selective query. 0 falls back to runtime.NumCPU() (gate7-6).
	MaxConcurrentRefines int
	// SlotWait bounds how long a multi-chunk refine waits for a concurrency slot
	// before SHEDDING (serving an empty fail-loud estimate). 0 means wait up to the
	// route deadline for a slot (queue rather than shed eagerly); a small positive
	// value sheds fast under a burst (gate7-6).
	SlotWait time.Duration
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
	substr := strings.ToLower(strings.TrimSpace(f.Query))
	p := refinePredicate{
		substr:  substr,
		tokens:  tokenizeQuery(substr),
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
	// gate7-4 byte-bound knobs, resolved from ComposerConfig (defaults applied in
	// NewComposer). Stored as int for the chunk-budget arithmetic.
	maxRefineFiles     int
	refineFileBudget   int
	maxChunkTorrents   int
	retainedFileBudget int
	// gate7-6 CPU/latency bound, resolved from ComposerConfig in NewComposer.
	// routeTimeout bounds the whole route; sem (shared across both routes) bounds
	// concurrent EXPENSIVE multi-chunk refines; slotWait bounds the slot wait.
	routeTimeout time.Duration
	sem          *semaphore.Weighted
	slotWait     time.Duration
}

// Byte-bound defaults (gate7-4). A candidate's authoritative file_count gates
// whether and how it is blob-decoded BEFORE any decode happens:
//   - > DefaultMaxRefineFiles                  → declined (fail-loud, excluded).
//   - chunks summed to ≤ DefaultRefineFileBudget, ≤ DefaultMaxChunkTorrents each
//     → decoded one chunk at a time so peak TRANSIENT decode ≈ budget × ~1KB/file,
//     not the whole candidate set.
//
// 🚨 SIZING (gate7-4 stress, measured): the per-chunk RefineFileBudget bounds the
// TRANSIENT decode of ONE chunk, which happens entirely inside the PG hydrate
// query (gorm row scan + zstd-decompress + msgpack-decode + the []TorrentFile +
// path strings) BEFORE refineMatches/the retained cap ever runs. The 3Gi
// constrained-mem run measured ~1KB per file of transient decode (heap climbed
// ~+200MB per ~200k files); a budget of 1.5M files ⇒ ~1.5GB for ONE chunk ⇒
// OOMKill on high-fanout whole-dir queries (apple2_flop matched many
// 86k–131k-file MAME torrents). So the budget is sized so ONE chunk's decode
// fits a 2–3Gi pod with margin: 300k files × ~1KB ≈ 300MB transient.
//
// DefaultMaxRefineFiles == DefaultRefineFileBudget (both 300k): the sanity cap is
// pinned to the chunk budget so NO single torrent can ever form a chunk whose
// decode exceeds the budget — the per-chunk decode bound therefore holds for ALL
// inputs, not just the common case. 300k is ~1.9× the real-world max fileset
// (~159k files), so no real torrent is declined; only genuinely pathological ones
// are. The chunker's "always place the first id" rule means a single id equal to
// the budget still gets its own chunk (the chunker never stalls).
const (
	DefaultMaxRefineFiles   uint = 300_000
	DefaultRefineFileBudget uint = 300_000
	DefaultMaxChunkTorrents uint = 1024
	// DefaultRetainedFileBudget caps the CUMULATIVE decoded files RETAINED across
	// all refined matches of a single request (gate7-4, the robust DOWNSTREAM
	// bound). The per-chunk RefineFileBudget bounds the transient decode; this
	// bounds what stays alive in `refined` across chunks — without it a
	// high-match-rate query would retain every matched torrent's full fileset until
	// pagination (peak ≈ MaxCandidates × MaxRefineFiles). At ~200B per RETAINED
	// decoded file (the []TorrentFile struct + its path string, after the raw blob
	// is freed), 1M files ≈ ~200MB retained — fits alongside one chunk's ~300MB
	// transient in a 2–3Gi pod. When a request's matches would exceed it, the
	// composer serves the accumulated top-relevance prefix as a memory-capped
	// estimate. Whole-dir queries (~12 matched 130k-file torrents → ~1.5M files)
	// trip it after ~7 torrents → serve the 7 most relevant.
	DefaultRetainedFileBudget uint = 1_000_000
)

// DefaultRouteTimeout bounds the WHOLE L3 route when ComposerConfig.RouteTimeout
// is unset (gate7-6). It is deliberately NOT a reuse of the 5s sidecar RPC timeout
// (PathsearchTimeout): that bounds only the candidate RPC, while this bounds the
// route end-to-end (candidates + FileCounts + every chunk decode + the Go refine),
// closing the pathological-whole-dir latency tail (apple2_flop ran 14-16s
// unbounded). When it fires the route serves the accumulated top-relevance prefix,
// never the resolver's broad-FTS PG fallback (~49s).
const DefaultRouteTimeout = 8 * time.Second

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

	if cfg.MaxRefineFiles == 0 {
		cfg.MaxRefineFiles = DefaultMaxRefineFiles
	}

	if cfg.RefineFileBudget == 0 {
		cfg.RefineFileBudget = DefaultRefineFileBudget
	}

	if cfg.MaxChunkTorrents == 0 {
		cfg.MaxChunkTorrents = DefaultMaxChunkTorrents
	}

	if cfg.RetainedFileBudget == 0 {
		cfg.RetainedFileBudget = DefaultRetainedFileBudget
	}

	if cfg.RouteTimeout <= 0 {
		cfg.RouteTimeout = DefaultRouteTimeout
	}

	// 0 (or a hostile negative) → one slot per CPU. NumCPU is always >= 1, so the
	// semaphore is always constructed with a positive weight.
	maxConcurrent := cfg.MaxConcurrentRefines
	if maxConcurrent <= 0 {
		maxConcurrent = runtime.NumCPU()
	}

	c := &Composer{
		l3:                 l3,
		pg:                 pg,
		cfg:                cfg,
		logger:             logger,
		maxRefineFiles:     int(cfg.MaxRefineFiles),
		refineFileBudget:   int(cfg.RefineFileBudget),
		maxChunkTorrents:   int(cfg.MaxChunkTorrents),
		retainedFileBudget: int(cfg.RetainedFileBudget),
		routeTimeout:       cfg.RouteTimeout,
		sem:                semaphore.NewWeighted(int64(maxConcurrent)),
		slotWait:           cfg.SlotWait,
	}

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

// Suggest routes path typeahead through the L3 prefix-index Suggest RPC. It is
// nil-safe (nil composer => not served) and fails soft: when L3 is unhealthy or
// the RPC errors/returns Unavailable (for example, the prefix index was never
// built), it returns served=false with a nil error so the caller falls back to
// the candidate-derived adapter. A successful call returns served=true even
// when the suggestion list is empty (an empty answer is authoritative, not a
// fallback).
func (c *Composer) Suggest(
	ctx context.Context,
	prefix string,
	limit uint,
) (suggestions []string, served bool, err error) {
	if c == nil || c.l3 == nil || !c.Healthy() {
		return nil, false, nil
	}

	resp, rpcErr := c.l3.Suggest(ctx, &pb.SuggestRequest{
		Prefix: prefix,
		Limit:  uint32(limit),
	})
	if rpcErr != nil {
		if c.logger != nil {
			c.logger.Debugw("pathsearch: suggest RPC fell back to adapter", "error", rpcErr)
		}

		return nil, false, nil
	}

	out := make([]string, 0, len(resp.GetSuggestions()))
	for _, suggestion := range resp.GetSuggestions() {
		out = append(out, suggestion.GetValue())
	}

	return out, true, nil
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

// FileSearchRouteTextEnabled reports whether fileSearch text queries should be
// adapted onto the L3 candidate + L1 refine route
// (SEARCH_FILE_SEARCH_ROUTE_TEXT). A nil composer (feature off) reports false.
func (c *Composer) FileSearchRouteTextEnabled() bool {
	return c != nil && c.cfg.FileSearchRouteText
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

// DefaultMaxDecodeCandidates is the per-request decode ceiling enforced when the
// configured MaxDecodeCandidates is 0. It bounds LATENCY (not memory): at the
// measured ≈13ms/decoded-torrent, 200 candidates ≈ 2s, which is the flat cost of
// a limit-50 page today, so page-1 sizes ≤50 are unchanged while a limit-100 page
// drops from ≈400 decodes (≈5.3s) to 200 (≈2s). Chosen to leave normal UI paging
// byte-identical and only bound pathological page sizes / deep offsets.
const DefaultMaxDecodeCandidates uint = 200

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

	// The LATENCY cap is likewise always applied; 0/unset → the safe default so a
	// zero-valued config is never latency-unbounded.
	maxDecode := c.cfg.MaxDecodeCandidates
	if maxDecode == 0 {
		maxDecode = DefaultMaxDecodeCandidates
	}

	// OversampleFactor is normalized to >= 1 in NewComposer, so budget >= need
	// absent overflow; budget < need is therefore an unsigned-overflow sentinel
	// (a hostile offset+limit), which we clamp straight to the cap.
	budget := need * c.cfg.OversampleFactor
	if budget < need || budget > maxCands {
		budget = maxCands
	}

	// The LATENCY floor (maxDecode) applies ONLY to SHALLOW pages — those whose
	// window (offset+limit) fits within it. A shallow page keeps today's ~200-decode
	// fast first page unchanged. A DEEP page (need > maxDecode) LIFTS that floor so
	// the decode window can grow enough to REACH the requested offset — otherwise a
	// deep offset can never be served (the page would be empty because the window
	// stopped at 200 < offset). Growth stays bounded above by the hard memory ceiling
	// maxCands (already applied above): deep pagination is honest but never unbounded.
	// F5: honest deep pagination + raised decode cap (user-approved latency tradeoff;
	// the route deadline still backstops worst-case latency).
	//
	// PARITY COUPLING — if you change this decode cap, mind facetIDs: raising the
	// budget grows `refined`, and the facet aggregation is DELIBERATELY pinned to the
	// shallow ceiling (facetIDs bounds its input to maxDecode) so Go's budgeted_count
	// EXPLAIN cost never crosses the fixed 5000 aggregation budget and flips
	// isEstimate — which the Rust grouped facet path never does. The bound keeps
	// Go/Rust facet counts byte-identical on the shadow gate. Do not "simplify" it
	// away when touching the budget.
	if need <= maxDecode && budget > maxDecode {
		budget = maxDecode
	}

	return budget
}

// maxCandidatesCap is the effective hard decode ceiling (0/unset →
// DefaultMaxCandidates), i.e. the most candidates the route will ever decode for
// one request. Deep pagination cannot serve past it, so HasNextPage uses it as the
// honesty ceiling.
func (c *Composer) maxCandidatesCap() uint {
	if c.cfg.MaxCandidates == 0 {
		return DefaultMaxCandidates
	}

	return c.cfg.MaxCandidates
}

// decodeLatencyCap is the effective shallow-page decode ceiling (0/unset →
// DefaultMaxDecodeCandidates). It also bounds the id set fed to the facet
// aggregation (facetIDs).
func (c *Composer) decodeLatencyCap() uint {
	if c.cfg.MaxDecodeCandidates == 0 {
		return DefaultMaxDecodeCandidates
	}

	return c.cfg.MaxDecodeCandidates
}

// facetIDs bounds the refined id set fed to the decode-free facet aggregation to
// the shallow decode ceiling (decodeLatencyCap). Deep pagination lets `refined`
// grow toward MaxCandidates, but the facet aggregation costs an EXPLAIN over an
// IN(refined) list whose planner cost crosses the fixed aggregation budget (5000)
// once the list is large — which would flip counts to planner ESTIMATES on the Go
// path while the Rust grouped path stays exact, diverging the shadow gate. Capping
// the facet input at the shallow ceiling keeps the facet cost (and the exact
// counts) IDENTICAL to before deep pagination existed — `refined` never exceeded
// this bound then — so Go and Rust remain byte-identical. Facets are a global
// top-relevance sidebar, so the top-cap prefix is exactly the set they reflected
// before. (F5)
func (c *Composer) facetIDs(refined []search.TorrentContentResultItem) []protocol.ID {
	ids := infoHashesOf(refined)
	if cap := c.decodeLatencyCap(); uint(len(ids)) > cap {
		ids = ids[:cap]
	}

	return ids
}

// hasNextPage reports HONESTLY whether a page after this one exists (F5). The old
// signal was computed from the refined window ALONE, so it reported false the
// moment a page consumed the decoded window — even while TotalCount still
// advertised "About N" from candidate_total, stranding the user with no way to
// reach the rest. Two cases:
//
//  1. More refined rows remain in THIS decoded window past the served page → yes.
//  2. The window was budget-TRUNCATED (candidate_total exceeds what we decoded, so
//     more matching candidates exist) AND we have not yet hit the hard decode
//     ceiling (a deeper page grows its budget toward MaxCandidates and surfaces
//     them). At the ceiling honesty demands FALSE: the route never serves past
//     MaxCandidates candidates, so there is no reachable next page no matter what
//     candidate_total says.
//
// `ids` is the decoded candidate set; when truncated it equals the decode budget,
// so `len(ids) < maxCandidatesCap` is exactly "budget can still grow".
func (c *Composer) hasNextPage(
	offset uint,
	page, refined []search.TorrentContentResultItem,
	ids []protocol.ID,
	candidateTotal uint,
) bool {
	if offset+uint(len(page)) < uint(len(refined)) {
		return true
	}

	truncated := candidateTotal > uint(len(ids))
	belowCeiling := uint(len(ids)) < c.maxCandidatesCap()

	return truncated && belowCeiling
}

// candidates dials L3 for the page's candidate budget and returns the decoded
// info_hash set, in L3's returned (recall) order, plus the sidecar's
// candidate_total: the FULL count of torrent-docs matching the gram query
// before any limit/truncation (the sidecar runs an unconditional Count
// collector per request — pathsearch/query.rs — so this is already paid for
// and was previously discarded). candidate_total is an UPPER bound on the
// true match total (the gram conjunction is a superset of substring matches;
// refine drops the false positives), which makes it the honest TotalCount
// estimate whenever the decode window is budget-truncated. (#10 follow-up)
func (c *Composer) candidates(
	ctx context.Context,
	f Filters,
	limit, offset uint,
	sorts []*pb.SortBy,
) ([]protocol.ID, uint, error) {
	budget := c.candidateBudget(limit, offset)

	resp, err := c.l3.PathCandidates(ctx, &pb.PathCandidatesRequest{
		Query: f.Query,
		Limit: uint32(budget),
		Sort:  sorts,
	})
	if err != nil {
		return nil, 0, err
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

	return out, uint(resp.GetCandidateTotal()), nil
}

// QueryOptions carries the PostgreSQL option sets the chunked refine needs. The
// caller (gqlmodel) builds them so the composer never has to introspect opaque
// query.Options. All three MUST already exclude any page limit/offset and the
// path free-text (which L3 owns); the composer appends the candidate IN(...)
// filter and paginates in Go after exact-refine.
//
//   - Combined: the torrent (blob) hydrator + facets — i.e. TODAY's single-query
//     options. Used for the COMMON single-chunk fast path: one query that both
//     decodes the chunk and computes facets, byte-identical to pre-gate7-4.
//   - Refine:   the torrent (blob) hydrator but NO facets — used per chunk in the
//     multi-chunk path so each chunk decodes only its own files. Falls back to
//     Combined when empty (collapse / tests).
//   - Agg:      facets + aggregation budget but the torrent hydrator DROPPED, so
//     it computes facets over the full kept set with ZERO blob decode (files_data
//     is selected only by that hydrator). Used once in the multi-chunk path.
type QueryOptions struct {
	Combined []query.Option
	Refine   []query.Option
	Agg      []query.Option
}

// refineOptions returns the per-chunk decode options, defaulting to Combined when
// no dedicated Refine set was supplied (collapse + unit tests).
func (o QueryOptions) refineOptions() []query.Option {
	if len(o.Refine) > 0 {
		return o.Refine
	}

	return o.Combined
}

// candidateRows restricts the PostgreSQL TorrentContent search to a set of ids,
// applying the given options (hydrators/facets/order) but NOT pagination —
// pagination happens in Go after refine (see TorrentContent). options MUST
// already exclude any page limit/offset and the path free-text (which L3 owns).
func (c *Composer) candidateRows(
	ctx context.Context,
	options []query.Option,
	ids []protocol.ID,
) (search.TorrentContentResult, error) {
	opts := make([]query.Option, 0, len(options)+1)
	opts = append(opts, options...)
	// Restrict to the id set. When the torrent hydrator is present in options,
	// files_data is selected and AfterFind decodes the L1 blob into Torrent.Files;
	// when it is dropped (the Agg set) there is ZERO decode.
	opts = append(opts, query.Where(search.TorrentContentInfoHashCriteria(ids...)))

	return c.pg.TorrentContent(ctx, opts...)
}

// fileCountOf returns the (conservative) file count used for budgeting. A known
// count is clamped to the cap (oversized ids are declined upstream; this is
// belt-and-suspenders). An UNKNOWN id (absent from the summary + fallback) is
// treated AS the cap so a missing count never under-sizes a chunk.
func (c *Composer) fileCountOf(id protocol.ID, counts map[protocol.ID]int) int {
	n, ok := counts[id]
	if !ok || n > c.maxRefineFiles {
		return c.maxRefineFiles
	}

	if n < 0 {
		return 0
	}

	return n
}

// declineOversized drops, fail-loud, every candidate whose authoritative
// file_count exceeds the per-torrent sanity cap (gate7-4). Each drop increments
// the refine_declined_oversized counter and logs a warning; the REST of the
// candidate set is kept (in L3 order) and still served. An UNKNOWN count is NOT
// oversized (it is budgeted as the cap but still refined). This is the only NEW
// candidate drop the byte-bound introduces, and it is loud — never a silent
// truncation.
func (c *Composer) declineOversized(ids []protocol.ID, counts map[protocol.ID]int) []protocol.ID {
	kept := make([]protocol.ID, 0, len(ids))

	for _, id := range ids {
		if n, known := counts[id]; known && n > c.maxRefineFiles {
			c.metrics.IncRefineDeclinedOversized()

			if c.logger != nil {
				c.logger.Warnw(
					"pathsearch: declining oversized candidate; excluding from refine (still serving the rest)",
					"info_hash",
					id.String(),
					"file_count",
					n,
					"cap",
					c.maxRefineFiles,
				)
			}

			continue
		}

		kept = append(kept, id)
	}

	return kept
}

// chunkByFileBudget splits ids (already in L3 relevance order) into CONTIGUOUS
// chunks whose summed file_count stays within budget and whose length stays
// within maxLen. Contiguity in L3 order is what preserves global relevance order
// across chunks: refining chunk-by-chunk and concatenating yields exactly the
// same ordered set as a single pass. Because every kept id has count ≤ cap ==
// budget (the sanity cap is pinned to the chunk budget), each id always fits in a
// chunk (alone if necessary) and NO chunk's decode can exceed the budget, so the
// loop always makes progress and the transient-decode bound holds for all inputs.
func (c *Composer) chunkByFileBudget(ids []protocol.ID, counts map[protocol.ID]int) [][]protocol.ID {
	if len(ids) == 0 {
		return nil
	}

	var (
		chunks [][]protocol.ID
		cur    []protocol.ID
		curSum int
	)

	for _, id := range ids {
		n := c.fileCountOf(id, counts)
		// Start a new chunk when the current one is non-empty AND adding this id
		// would breach either bound. (Never split before the first id, so a single
		// id larger than the per-chunk targets still gets its own chunk.)
		if len(cur) > 0 && (len(cur) >= c.maxChunkTorrents || curSum+n > c.refineFileBudget) {
			chunks = append(chunks, cur)
			cur = nil
			curSum = 0
		}

		cur = append(cur, id)
		curSum += n
	}

	if len(cur) > 0 {
		chunks = append(chunks, cur)
	}

	return chunks
}

// restrictToSet keeps only the items whose info_hash is in ids, preserving the
// input order. In production the candidate IN(...) query already returns exactly
// the chunk's rows, so this is a no-op; it is defensive (and makes the chunk loop
// robust to a fake/over-broad result set) — never relaxing correctness.
func restrictToSet(
	items []search.TorrentContentResultItem,
	ids []protocol.ID,
) []search.TorrentContentResultItem {
	set := make(map[protocol.ID]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}

	out := make([]search.TorrentContentResultItem, 0, len(items))

	for _, it := range items {
		if _, ok := set[it.Torrent.InfoHash]; ok {
			out = append(out, it)
		}
	}

	return out
}

// infoHashesOf extracts the info_hashes of result items, preserving order. Used to
// (re)compute facets over the REFINED set (gate7-8): the aggregation must reflect
// what the route actually serves (post exact-refine), not the pre-refine candidate
// set. The slice may be empty, in which case the caller must NOT build an IN()
// query from it (an empty id set) — it serves empty facets instead.
func infoHashesOf(items []search.TorrentContentResultItem) []protocol.ID {
	ids := make([]protocol.ID, len(items))
	for i := range items {
		ids[i] = items[i].Torrent.InfoHash
	}

	return ids
}

// chunkRows queries one chunk's rows (decoding only that chunk's blobs), restores
// L3 recall order within the chunk, and restricts to the chunk id set. The
// returned items (and their decoded files) go out of scope at the call site after
// refineMatches has copied the kept matches, so the GC can reclaim the
// non-matched + blob bytes before the next chunk is fetched.
func (c *Composer) chunkRows(
	ctx context.Context,
	options []query.Option,
	chunk []protocol.ID,
) (items []search.TorrentContentResultItem, aggs query.Aggregations, err error) {
	res, err := c.candidateRows(ctx, options, chunk)
	if err != nil {
		return nil, nil, err
	}

	orderItemsByIDs(res.Items, chunk)

	return restrictToSet(res.Items, chunk), res.Aggregations, nil
}

// refineCap classifies WHY a refine stopped early before consuming every
// candidate (gate7-6). All non-none reasons still SERVE the accumulated
// top-relevance prefix (served=true, TotalCountIsEstimate=true); they differ only
// in which fail-loud metric fires.
type refineCap int

const (
	// capNone: the refine consumed its input without hitting a bound.
	capNone refineCap = iota
	// capRetained: the cumulative RetainedFileBudget was reached (gate7-4) — serve
	// the memory-capped prefix.
	capRetained
	// capDeadline: the route deadline fired mid-refine (gate7-6) — serve the
	// deadline-capped prefix instead of the broad-FTS PG fallback wall.
	capDeadline
)

// isRouteDeadline reports whether ctx is done specifically because a DEADLINE
// fired (our route timeout or an earlier parent deadline) — the signal to serve
// the accumulated prefix rather than fail-loud to PG. A parent CANCEL
// (ctx.Err()==context.Canceled) is deliberately NOT a route-deadline: the parent
// ctx is dead, so the resolver's PG fallback would error immediately (no wall),
// and the route returns served=false as before. This tests ctx.Err() (the
// authoritative reason we are out of time), not the wrapped gorm/pgx error.
func isRouteDeadline(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.DeadlineExceeded)
}

// acquireRefineSlot acquires one concurrency slot for an EXPENSIVE multi-chunk
// refine (gate7-6). With slotWait>0 it waits at most that long before shedding;
// with slotWait==0 it waits up to the route deadline (acqCtx==ctx) so a moderate
// burst queues rather than sheds eagerly. It returns false (SHED) when no slot
// becomes available in time. Only the caller's multi-chunk branch invokes it, so
// the single-chunk fast path is never blocked or shed. On success the caller MUST
// `defer c.sem.Release(1)`.
func (c *Composer) acquireRefineSlot(ctx context.Context) bool {
	acqCtx := ctx

	if c.slotWait > 0 {
		var cancel context.CancelFunc

		acqCtx, cancel = context.WithTimeout(ctx, c.slotWait)
		defer cancel()
	}

	return c.sem.Acquire(acqCtx, 1) == nil
}

// refineMatches exact-refines a chunk's ordered items and APPENDS the real
// matches to the running `refined` accumulator while enforcing the cumulative
// RETAINED-file budget (gate7-4 byte-bound, the robust bound). It does three
// things per item, in order:
//
//  1. resolve the file list (filesForRefine). ok=false → fail-loud (the caller
//     falls back to PostgreSQL); a single unobtainable candidate is never a
//     silent drop.
//  2. POST-DECODE per-torrent guard: an ACTUAL decoded fileset larger than
//     MaxRefineFiles (e.g. an unknown summary count that was budgeted as the cap
//     but turned out huge) is DECLINED here — counted (refine_declined_oversized)
//     + logged + dropped — rather than retained. This backstops the pre-decode
//     summary-count cap for the unknown-count case.
//  3. if it is a real match, FREE the raw msgpack blob (FilesData) — the decoded
//     Files are kept, the source bytes are not needed and are not a GraphQL field
//     — and append, UNLESS doing so would exceed the cumulative retained-file
//     budget. When it would, stop and report capped=true: the caller serves the
//     already-accumulated TOP-RELEVANCE prefix (candidates are L3-ordered) as a
//     memory-capped estimate. This is the only bound that holds regardless of
//     match rate: a broad query toward thousands of large-fileset torrents can no
//     longer retain every matched fileset at once.
//
// Selective queries (small match sets ≪ budget) never trip the cap, so they are
// byte-identical to before.
//
// gate7-6: ctx is threaded in so the per-item loop can cooperatively honor the
// route deadline (checkpoint c — bounds the Go match CPU on a huge chunk). When
// the route deadline has fired it returns capDeadline with ok=true so the caller
// serves the accumulated prefix (NOT a fail-loud fallback). The deadline-vs-cancel
// discriminator is ctx.Err()==DeadlineExceeded; a parent Cancel (ctx.Err()==
// Canceled) is NOT a deadline-cap — it leaves the loop normally and the caller's
// next checkpoint surfaces it.
func (c *Composer) refineMatches(
	ctx context.Context,
	items []search.TorrentContentResultItem,
	pred refinePredicate,
	refined []search.TorrentContentResultItem,
	retained int,
) (out []search.TorrentContentResultItem, newRetained int, capRsn refineCap, ok bool) {
	out = refined
	newRetained = retained

	for i := range items {
		// Checkpoint (c): cooperatively stop the per-item CPU when the route deadline
		// has fired, serving the accumulated top-relevance prefix.
		if isRouteDeadline(ctx) {
			return out, newRetained, capDeadline, true
		}

		it := items[i]

		files, fok := filesForRefine(it.Torrent)
		if !fok {
			return out, newRetained, capNone, false
		}

		if len(files) > c.maxRefineFiles {
			c.metrics.IncRefineDeclinedOversized()

			if c.logger != nil {
				c.logger.Warnw(
					"pathsearch: declining candidate after decode; actual file count exceeds cap (still serving the rest)",
					"info_hash",
					it.Torrent.InfoHash.String(),
					"files",
					len(files),
					"cap",
					c.maxRefineFiles,
				)
			}

			continue
		}

		if !torrentTokenMatch(files, it.Torrent.Name, pred) {
			continue // L3 false positive: some query token matches nowhere (F11)
		}

		// Stop before exceeding the retained budget — but always keep at least one
		// match (a single torrent's files ≤ cap ≤ budget, so this never stalls).
		if len(out) > 0 && newRetained+len(files) > c.retainedFileBudget {
			return out, newRetained, capRetained, true
		}

		it.Torrent.FilesData = nil // free the raw blob; decoded Files are retained
		out = append(out, it)
		newRetained += len(files)
	}

	return out, newRetained, capNone, true
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
// Pipeline: L3 oversample (capped) -> cheap per-torrent file-count probe
// (decline oversized, chunk by file budget) -> PG IN(chunk) + filters + OrderBy
// (no page window) per chunk -> Go L1 blob-decode refine (drop false positives)
// -> paginate in Go -> estimated counts. The chunking bounds peak blob-decode
// memory to one file-budget's worth regardless of how huge the matched torrents'
// filesets are (gate7-4 byte-bound).
func (c *Composer) TorrentContent(
	ctx context.Context,
	f Filters,
	opts QueryOptions,
	limit, offset uint,
	sorts []*pb.SortBy,
) (result search.TorrentContentResult, served bool, err error) {
	pred := f.predicate()
	if pred.substr == "" || !c.Eligible(f.Query) {
		c.metrics.IncRoute(RouteIneligible)

		return search.TorrentContentResult{}, false, nil
	}

	// gate7-6 route deadline: bound the WHOLE route (candidates + FileCounts + every
	// chunk decode + the Go refine), not just the sidecar RPC. candidates()'s nested
	// 5s RPC timeout still nests under this (Go honors the earliest deadline). When
	// it fires the route serves the accumulated top-relevance prefix as a
	// deadline-capped estimate, NEVER the resolver's broad-FTS PG fallback.
	ctx, cancel := context.WithTimeout(ctx, c.routeTimeout)
	defer cancel()

	ids, candidateTotal, err := c.candidates(ctx, f, limit, offset, sorts)
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

	// gate7-4 byte-bound: read the cheap, authoritative per-torrent file counts
	// BEFORE decoding any blob, decline oversized candidates fail-loud, then chunk
	// the refine by a cumulative file budget. A FileCounts error is a hard PG
	// error → fall back (the GraphQL layer runs the plain PG path).
	counts, err := c.pg.FileCounts(ctx, ids)
	if err != nil {
		c.metrics.IncRoute(RouteError)

		return search.TorrentContentResult{}, false, err
	}

	keptIDs := c.declineOversized(ids, counts)
	if len(keptIDs) == 0 {
		// Every candidate was declined as oversized (each already counted + logged).
		// Serve an honest estimated empty — the fail-loud drops already happened, so
		// this is not a silent truncation.
		c.metrics.IncRoute(RouteServed)

		return search.TorrentContentResult{TotalCountIsEstimate: true}, true, nil
	}

	chunks := c.chunkByFileBudget(keptIDs, counts)

	var (
		refined  []search.TorrentContentResultItem
		retained int
		capRsn   refineCap
		aggs     query.Aggregations
	)

	if len(chunks) == 1 {
		// COMMON fast path: the whole kept set fits one chunk (the normal case:
		// ≤2000 candidates × ~52 files ≈ 104k ≪ 300k budget). One query decodes the
		// chunk and the Go exact-refine drops false positives. The retained budget
		// never trips here (a single chunk's matched files ≤ chunk file budget ≤
		// retained budget).
		//
		// gate7-8: use the hydrator-only refineOptions() (NOT opts.Combined) — its
		// side-effect facets would be over the CANDIDATE set, but facets are now
		// computed once over the REFINED set after refine (see below), so we want
		// only the decode here.
		//
		// gate7-6: the cheap fast path does NOT acquire a concurrency slot, so a
		// saturated limiter never blocks/sheds a normal/selective query — only the
		// expensive multi-chunk path below contends for slots.
		items, _, qErr := c.chunkRows(ctx, opts.refineOptions(), chunks[0])
		if qErr != nil {
			// A deadline mid-query is NOT a hard error: serve the (empty here)
			// deadline-capped prefix rather than the broad-FTS PG fallback wall.
			if isRouteDeadline(ctx) {
				capRsn = capDeadline
			} else {
				c.metrics.IncRoute(RouteError)

				return search.TorrentContentResult{}, false, qErr
			}
		} else {
			var ok bool

			refined, retained, capRsn, ok = c.refineMatches(ctx, items, pred, nil, 0)
			if !ok {
				return c.refineFailLoud(f)
			}
		}
	} else {
		// Multi-chunk (the pathological 14-16s whole-dir path): bound concurrency.
		// gate7-6 — acquire ONE shared slot before the expensive work; if none is
		// available in time, SHED fail-loud (serve an empty estimate, NEVER a PG
		// broad-FTS fallback).
		if !c.acquireRefineSlot(ctx) {
			c.metrics.IncRefineShed()

			if c.logger != nil {
				c.logger.Warnw("pathsearch: refine concurrency slot unavailable; shedding (serving empty estimate)",
					"query", f.Query, "slot_wait", c.slotWait)
			}

			c.metrics.IncRoute(RouteServed)

			return search.TorrentContentResult{TotalCountIsEstimate: true}, true, nil
		}
		defer c.sem.Release(1)

		// Refine each chunk in L3 order, letting each chunk's decoded files fall out of
		// scope before the next chunk is fetched (peak TRANSIENT decode ≈ one file
		// budget), and accumulate matches under the RETAINED-file budget so peak
		// RETAINED files is bounded too — independent of how many candidates match.
		// gate7-8: facets are NOT computed here over the candidate set; they are
		// computed ONCE over the REFINED set after refine (see below), so this path no
		// longer runs the upfront decode-free candidate aggregation.
		for _, chunk := range chunks {
			// Checkpoint (a): stop before fetching the next chunk when the route
			// deadline has fired; serve the accumulated prefix.
			if isRouteDeadline(ctx) {
				capRsn = capDeadline

				break
			}

			items, _, cErr := c.chunkRows(ctx, opts.refineOptions(), chunk)
			if cErr != nil {
				// Checkpoint (b): a deadline mid-chunk-query is a deadline-cap, not a
				// RouteError → serve the prefix, not the PG wall.
				if isRouteDeadline(ctx) {
					capRsn = capDeadline

					break
				}

				c.metrics.IncRoute(RouteError)

				return search.TorrentContentResult{}, false, cErr
			}

			var ok bool

			refined, retained, capRsn, ok = c.refineMatches(ctx, items, pred, refined, retained)
			if !ok {
				return c.refineFailLoud(f)
			}

			if capRsn != capNone {
				// Retained-file budget reached OR the route deadline fired during
				// refine: stop decoding further chunks and serve the accumulated
				// TOP-RELEVANCE prefix (candidates are L3-ordered).
				break
			}
		}
	}

	switch capRsn {
	case capRetained:
		// Memory-capped estimate (gate7-4). Fail-loud: counted + logged, never a
		// silent over-allocation. The result stays TotalCountIsEstimate=true (always
		// true on this route) and serves the top-relevance prefix.
		c.metrics.IncRefineRetainedCapped()

		if c.logger != nil {
			c.logger.Warnw(
				"pathsearch: retained-file budget reached; serving memory-capped top-relevance result",
				"query",
				f.Query,
				"retained_files",
				retained,
				"budget",
				c.retainedFileBudget,
				"matches",
				len(refined),
			)
		}
	case capDeadline:
		// Deadline-capped estimate (gate7-6). Fail-loud: counted + logged. Serves the
		// accumulated top-relevance prefix (possibly empty) rather than the resolver's
		// broad-FTS PG fallback wall.
		c.metrics.IncRefineDeadlineCapped()

		if c.logger != nil {
			c.logger.Warnw(
				"pathsearch: route deadline reached; serving deadline-capped top-relevance result",
				"query",
				f.Query,
				"route_timeout",
				c.routeTimeout,
				"matches",
				len(refined),
			)
		}
	}

	// gate7-8: (re)compute the facet aggregations over the REFINED result set, not
	// the pre-refine candidate set. The sidebar must reconcile with the served items
	// — before this, facets were aggregated over the L3 candidate set BEFORE the Go
	// exact-refine dropped false positives, so an empty/short result could still show
	// phantom counts (e.g. totalCount=0 yet contentType=[audiobook 2, ebook 18]).
	// This pass is decode-free (opts.Agg drops the blob hydrator → ZERO blob decode)
	// and runs exactly ONCE, after refine; it adds no decode and no unbounded scan.
	// For capped results (capRetained/capDeadline) `refined` is the served prefix, so
	// facets are over that prefix — consistent with the served items.
	if len(refined) == 0 {
		// No refined matches → serve EMPTY facets. Do NOT build an IN() query from an
		// empty id set, and do NOT leak the candidate-set facets. This is the core
		// symptom fix: zero items ⇒ zero facet counts.
		aggs = query.Aggregations{}
	} else {
		aggRes, qErr := c.candidateRows(ctx, opts.Agg, c.facetIDs(refined))
		if qErr != nil {
			// gate7-9 (N2 graceful degradation): the decode+exact-refine ALREADY
			// succeeded and produced correct items; only this cheap decode-free facet
			// pass failed. A NON-deadline error here must NOT fail the user's whole
			// search — serve the refined items WITHOUT facets (empty sidebar) rather
			// than erroring (a 500) or counting a RouteError (which would run the
			// resolver's broad-FTS PG wall). Both branches set empty facets and fall
			// through to paginate+serve; they differ only in observability.
			if isRouteDeadline(ctx) {
				// Deadline mid-aggregation: already accounted for by the cap path
				// (capDeadline/deadline_capped above); serve w/o facets, no extra count.
				aggs = query.Aggregations{}
			} else {
				// Transient/structural agg failure: serve w/o facets, but make it
				// observable via a dedicated metric + warn log. NOT a RouteError.
				aggs = query.Aggregations{}

				c.metrics.IncRefineAggError()

				if c.logger != nil {
					c.logger.Warnw("pathsearch: refined-set aggregation failed; serving items WITHOUT facets (empty sidebar)",
						"query", f.Query, "matches", len(refined), "err", qErr)
				}
			}
		} else {
			aggs = aggRes.Aggregations
		}
	}

	page := paginate(refined, offset, limit)

	c.metrics.IncRoute(RouteServed)

	// TotalCount (#10 follow-up): when the decode window covered EVERY candidate
	// (candidateTotal <= len(ids)), len(refined) IS the complete exact-refined
	// match count — serve it as before. When the window was budget-truncated,
	// len(refined) is a badly-low lower bound (a "1080p" search with millions of
	// matches used to show ~200); the sidecar's candidate_total — the full
	// Tantivy Count it already computes per request — is a far closer UPPER
	// bound (gram-conjunction superset; refine only removes false positives), so
	// serve that instead. TotalCountIsEstimate stays true in both cases and the
	// web UI renders the `~`-prefixed rounded form. We still deliberately ignore
	// the client's totalCount flag — an exact global count is not available on
	// the L3 route.
	totalCount := uint(len(refined))
	if candidateTotal > uint(len(ids)) && candidateTotal > totalCount {
		totalCount = candidateTotal
	}

	return search.TorrentContentResult{
		Items:                page,
		TotalCount:           totalCount,
		TotalCountIsEstimate: true,
		HasNextPage:          c.hasNextPage(offset, page, refined, ids, candidateTotal),
		// Facets/aggregations are computed over the REFINED result set (gate7-8), so
		// the UI facet sidebar reconciles with the served items: empty result ⇒ empty
		// facets, N items ⇒ per-facet counts that sum to N. (#6 first restored
		// pass-through over the candidate set; gate7-8 moves it to the refined set via
		// the decode-free aggregation query over the refined id set.)
		Aggregations: aggs,
	}, true, nil
}

// refineFailLoud is the shared CAVEAT B exit: a candidate's files were
// unobtainable, so fall back to the plain PG path rather than serve a silently
// truncated result.
func (c *Composer) refineFailLoud(f Filters) (search.TorrentContentResult, bool, error) {
	c.metrics.IncRoute(RouteFallback)

	if c.logger != nil {
		c.logger.Warnw("pathsearch: candidate files unobtainable; falling back to PostgreSQL",
			"query", f.Query)
	}

	return search.TorrentContentResult{}, false, nil
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
	opts QueryOptions,
	limit, offset uint,
	sorts []*pb.SortBy,
) (groups []PathGroup, served bool, err error) {
	pred := f.predicate()
	if pred.substr == "" || !c.Eligible(f.Query) {
		c.metrics.IncRoute(RouteIneligible)

		return nil, false, nil
	}

	// gate7-6 route deadline — mirrors TorrentContent: bound the whole route, serve
	// the accumulated groups on deadline rather than the broad-FTS PG fallback.
	ctx, cancel := context.WithTimeout(ctx, c.routeTimeout)
	defer cancel()

	// CollapsePaths groups the refined window; a global candidate_total has no
	// per-group meaning here, so it is intentionally unused.
	ids, _, err := c.candidates(ctx, f, limit, offset, sorts)
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

	// gate7-4 byte-bound: same cheap file-count probe → decline oversized → chunk
	// by file budget as TorrentContent. The group index/order accumulates ACROSS
	// chunks; because chunks are contiguous in L3 order and ordered within, the
	// first-seen group order is identical to a single-pass collapse.
	counts, err := c.pg.FileCounts(ctx, ids)
	if err != nil {
		c.metrics.IncRoute(RouteError)

		return nil, false, err
	}

	keptIDs := c.declineOversized(ids, counts)
	if len(keptIDs) == 0 {
		c.metrics.IncRoute(RouteServed)

		return nil, true, nil
	}

	chunks := c.chunkByFileBudget(keptIDs, counts)

	// gate7-6: bound concurrency on the expensive multi-chunk path only; the cheap
	// single-chunk path bypasses the limiter (never blocked/shed). Shed fail-loud
	// (serve the empty accumulated set), NEVER a PG broad-FTS fallback.
	if len(chunks) > 1 {
		if !c.acquireRefineSlot(ctx) {
			c.metrics.IncRefineShed()

			if c.logger != nil {
				c.logger.Warnw(
					"pathsearch: collapse refine concurrency slot unavailable; shedding (serving empty)",
					"query",
					f.Query,
					"slot_wait",
					c.slotWait,
				)
			}

			c.metrics.IncRoute(RouteServed)

			return nil, true, nil
		}
		defer c.sem.Release(1)
	}

	index := make(map[string]int)

	deadlined := false

collapse:
	for _, chunk := range chunks {
		// Checkpoint (a): stop before fetching the next chunk on route deadline.
		if isRouteDeadline(ctx) {
			deadlined = true

			break
		}

		res, qErr := c.candidateRows(ctx, opts.refineOptions(), chunk)
		if qErr != nil {
			// Checkpoint (b): a deadline mid-query serves the accumulated groups, not
			// the PG fallback wall.
			if isRouteDeadline(ctx) {
				deadlined = true

				break
			}

			c.metrics.IncRoute(RouteError)

			return nil, false, qErr
		}

		orderItemsByIDs(res.Items, chunk)
		items := restrictToSet(res.Items, chunk)

		for i := range items {
			// Inner-loop checkpoint (c): bound the Go refine CPU on a huge chunk.
			if isRouteDeadline(ctx) {
				deadlined = true

				break collapse
			}

			item := items[i]

			files, fok := filesForRefine(item.Torrent)
			if !fok {
				// Fail loud: cannot verify this candidate's paths.
				c.metrics.IncRoute(RouteFallback)

				if c.logger != nil {
					c.logger.Warnw("pathsearch: collapse candidate files unobtainable; falling back",
						"query", f.Query)
				}

				return nil, false, nil
			}

			// POST-DECODE per-torrent guard (#3): an actual fileset larger than the
			// cap (an unknown summary count budgeted as cap that turned out huge) is
			// declined here + counted + dropped, same as TorrentContent.
			if len(files) > c.maxRefineFiles {
				c.metrics.IncRefineDeclinedOversized()

				if c.logger != nil {
					c.logger.Warnw("pathsearch: declining collapse candidate after decode; actual file count exceeds cap",
						"info_hash", item.Torrent.InfoHash.String(), "files", len(files), "cap", c.maxRefineFiles)
				}

				continue
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
		// chunk's items + decoded files fall out of scope here → reclaimable before
		// the next chunk is fetched.
	}

	if deadlined {
		// Deadline-capped estimate (gate7-6): serve the accumulated top-relevance
		// groups, fail-loud (counted + logged), never the PG fallback wall.
		c.metrics.IncRefineDeadlineCapped()

		if c.logger != nil {
			c.logger.Warnw("pathsearch: collapse route deadline reached; serving deadline-capped groups",
				"query", f.Query, "route_timeout", c.routeTimeout, "groups", len(groups))
		}
	}

	c.metrics.IncRoute(RouteServed)

	return paginate(groups, offset, limit), true, nil
}
