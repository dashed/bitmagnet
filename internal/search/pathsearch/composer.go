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
	// TypeaheadEnabled gates the UI path-typeahead route
	// (SEARCH_PATH_TYPEAHEAD_ENABLED). The master switch is the composer existing
	// at all (nil composer = pathsearch disabled).
	TypeaheadEnabled bool
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
	// gate7-4 byte-bound knobs, resolved from ComposerConfig (defaults applied in
	// NewComposer). Stored as int for the chunk-budget arithmetic.
	maxRefineFiles     int
	refineFileBudget   int
	maxChunkTorrents   int
	retainedFileBudget int
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

	c := &Composer{
		l3:                 l3,
		pg:                 pg,
		cfg:                cfg,
		logger:             logger,
		maxRefineFiles:     int(cfg.MaxRefineFiles),
		refineFileBudget:   int(cfg.RefineFileBudget),
		maxChunkTorrents:   int(cfg.MaxChunkTorrents),
		retainedFileBudget: int(cfg.RetainedFileBudget),
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
				c.logger.Warnw("pathsearch: declining oversized candidate; excluding from refine (still serving the rest)",
					"info_hash", id.String(), "file_count", n, "cap", c.maxRefineFiles)
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
func (c *Composer) refineMatches(
	items []search.TorrentContentResultItem,
	pred refinePredicate,
	refined []search.TorrentContentResultItem,
	retained int,
) (out []search.TorrentContentResultItem, newRetained int, capped, ok bool) {
	out = refined
	newRetained = retained

	for i := range items {
		it := items[i]

		files, fok := filesForRefine(it.Torrent, pred)
		if !fok {
			return out, newRetained, false, false
		}

		if len(files) > c.maxRefineFiles {
			c.metrics.IncRefineDeclinedOversized()

			if c.logger != nil {
				c.logger.Warnw("pathsearch: declining candidate after decode; actual file count exceeds cap (still serving the rest)",
					"info_hash", it.Torrent.InfoHash.String(), "files", len(files), "cap", c.maxRefineFiles)
			}

			continue
		}

		if !torrentMatches(files, pred) {
			continue // L3 false positive
		}

		// Stop before exceeding the retained budget — but always keep at least one
		// match (a single torrent's files ≤ cap ≤ budget, so this never stalls).
		if len(out) > 0 && newRetained+len(files) > c.retainedFileBudget {
			return out, newRetained, true, true
		}

		it.Torrent.FilesData = nil // free the raw blob; decoded Files are retained
		out = append(out, it)
		newRetained += len(files)
	}

	return out, newRetained, false, true
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
		capped   bool
		aggs     query.Aggregations
	)

	if len(chunks) == 1 {
		// COMMON fast path: the whole kept set fits one chunk (the normal case:
		// ≤2000 candidates × ~52 files ≈ 104k ≪ 300k budget). One combined query
		// decodes the chunk AND computes facets — byte-identical to pre-gate7-4, no
		// extra aggregation round-trip. The retained budget never trips here (a
		// single chunk's matched files ≤ chunk file budget ≤ retained budget).
		items, chunkAggs, qErr := c.chunkRows(ctx, opts.Combined, chunks[0])
		if qErr != nil {
			c.metrics.IncRoute(RouteError)

			return search.TorrentContentResult{}, false, qErr
		}

		var ok bool

		refined, retained, capped, ok = c.refineMatches(items, pred, nil, 0)
		if !ok {
			return c.refineFailLoud(f)
		}

		aggs = chunkAggs
	} else {
		// Multi-chunk: compute facets ONCE over the full kept set with the torrent
		// (blob) hydrator DROPPED — files_data is selected only by that hydrator, so
		// this aggregation pass does ZERO blob decode. Then refine each chunk in L3
		// order, letting each chunk's decoded files fall out of scope before the next
		// chunk is fetched (peak TRANSIENT decode ≈ one file budget), and accumulate
		// matches under the RETAINED-file budget so peak RETAINED files is bounded
		// too — independent of how many candidates match.
		aggRes, qErr := c.candidateRows(ctx, opts.Agg, keptIDs)
		if qErr != nil {
			c.metrics.IncRoute(RouteError)

			return search.TorrentContentResult{}, false, qErr
		}

		aggs = aggRes.Aggregations

		for _, chunk := range chunks {
			items, _, cErr := c.chunkRows(ctx, opts.refineOptions(), chunk)
			if cErr != nil {
				c.metrics.IncRoute(RouteError)

				return search.TorrentContentResult{}, false, cErr
			}

			var ok bool

			refined, retained, capped, ok = c.refineMatches(items, pred, refined, retained)
			if !ok {
				return c.refineFailLoud(f)
			}

			if capped {
				// Retained-file budget reached: stop decoding further chunks and serve
				// the accumulated TOP-RELEVANCE prefix (candidates are L3-ordered).
				break
			}
		}
	}

	if capped {
		// Memory-capped estimate. Fail-loud: counted + logged, never a silent
		// over-allocation. The result stays TotalCountIsEstimate=true (always true
		// on this route) and serves the top-relevance prefix.
		c.metrics.IncRefineRetainedCapped()

		if c.logger != nil {
			c.logger.Warnw("pathsearch: retained-file budget reached; serving memory-capped top-relevance result",
				"query", f.Query, "retained_files", retained, "budget", c.retainedFileBudget, "matches", len(refined))
		}
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
		// sidebar on path searches. (#6) In the multi-chunk path these come from the
		// dedicated decode-free aggregation query over the same kept id set.
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

	index := make(map[string]int)

	for _, chunk := range c.chunkByFileBudget(keptIDs, counts) {
		res, qErr := c.candidateRows(ctx, opts.refineOptions(), chunk)
		if qErr != nil {
			c.metrics.IncRoute(RouteError)

			return nil, false, qErr
		}

		orderItemsByIDs(res.Items, chunk)
		items := restrictToSet(res.Items, chunk)

		for i := range items {
			item := items[i]

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

	c.metrics.IncRoute(RouteServed)

	return paginate(groups, offset, limit), true, nil
}
