// Package router provides SearchRouter, a drop-in search.Search implementation
// that blends the legacy PostgreSQL search engine with the Tantivy sidecar
// according to a configured Mode (postgres / shadow / canary / tantivy).
//
// It is swapped in for the real search.Search via fx with no call-site changes
// (the GraphQL and Torznab adapters keep calling TorrentContent). PostgreSQL is
// the fail-closed default; shadow mode compares without affecting the response,
// while canary and Tantivy modes may serve a deliberately narrow query class
// from a healthy, fresh sidecar after hydrating its hits from PostgreSQL.
package router

import (
	"context"
	"hash/fnv"
	"math/rand"
	"sort"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/shadow"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// tantivySearcher is the slice of the Tantivy client the router depends on.
// *tantivy.Client satisfies it; tests substitute a fake.
type tantivySearcher interface {
	Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error)
}

// observer records shadow comparison outcomes. *shadow.Metrics satisfies it.
type observer interface {
	Observe(shadow.Comparison)
	IncDropped()
}

var (
	_ tantivySearcher = (*tantivy.Client)(nil)
	_ observer        = (*shadow.Metrics)(nil)
)

// Router is a search.Search that routes TorrentContent searches per its Config.
// It embeds the underlying PostgreSQL search.Search, so every other search
// method (content, queue jobs, torrents, torrent files) passes straight through;
// only TorrentContent is intercepted.
type Router struct {
	search.Search

	tantivy tantivySearcher
	builder requestBuilder
	metrics observer
	logger  *zap.SugaredLogger
	cfg     Config
	health  *HealthState

	serveMetrics *ServeMetrics

	// sample returns a value in [0,1) to gate shadow sampling; run executes the
	// background shadow task. Both are fields so tests can make the otherwise
	// non-deterministic, asynchronous shadow path deterministic and synchronous.
	sample func() float64
	run    func(func())
}

// New builds a Router wrapping the PostgreSQL search engine pg and the Tantivy
// client. Health gates serving, serveMetrics records serve outcomes, metrics
// records shadow comparisons, and logger receives fail-closed diagnostics.
func New(
	pg search.Search,
	client tantivySearcher,
	metrics observer,
	logger *zap.SugaredLogger,
	cfg Config,
	health *HealthState,
	serveMetrics *ServeMetrics,
) *Router {
	sem := semaphore.NewWeighted(int64(cfg.shadowMaxConcurrent()))

	return &Router{
		Search:       pg,
		tantivy:      client,
		builder:      optionRequestBuilder{},
		metrics:      metrics,
		logger:       logger,
		cfg:          cfg,
		health:       health,
		serveMetrics: serveMetrics,
		sample:       rand.Float64,
		run: func(f func()) {
			if !sem.TryAcquire(1) {
				if metrics != nil {
					metrics.IncDropped()
				}

				return
			}

			go func() {
				defer sem.Release(1)
				f()
			}()
		},
	}
}

// TorrentContent routes the narrow, eligible query class to Tantivy in canary or
// Tantivy mode while failing closed to PostgreSQL on every ineligible, unhealthy,
// timed-out, or failed path. PostgreSQL-served queries retain sampled background
// shadow comparison in the configured Tantivy-backed modes.
func (r *Router) TorrentContent(
	ctx context.Context,
	options ...query.Option,
) (search.TorrentContentResult, error) {
	// ModePostgres (and any invalid mode) is a pure passthrough: no request build,
	// no sampling draw, no sidecar. Deriving the Tantivy request replays every
	// option against the recorder and recovers a panic per filter option, so it
	// must never run on the master-switch-off path.
	if !r.cfg.Mode.tantivyBacked() {
		return r.Search.TorrentContent(ctx, options...)
	}

	var (
		req   *pb.SearchRequest
		meta  buildResult
		built bool
	)

	// The serving modes need the request shape before they can route, so they build
	// up front; shadow-only mode defers the build behind the sampling draw below.
	if r.cfg.Mode.serving() {
		req, meta = r.builder.build(options)
		built = true

		if r.shouldServe(req, meta) {
			if result, served := r.serveTantivy(ctx, req); served {
				return result, nil
			}
			// serveTantivy already logged and counted the fallback; PostgreSQL serves.
		}
	}

	start := time.Now()

	result, err := r.Search.TorrentContent(ctx, options...)
	if err != nil {
		return result, err
	}

	pgLatency := time.Since(start)

	if r.shouldShadow() {
		if !built {
			// Shadow-only mode: an unsampled query never pays for the option replay.
			req, meta = r.builder.build(options)
		}

		// Eligibility is resolved BEFORE the semaphore acquire and goroutine spawn,
		// so a query the comparison would skip never costs a background slot.
		if shadowEligible(req, meta) {
			// Detach from the request context so the comparison can outlive the
			// response and a cancelled request never aborts it; runShadow bounds it
			// with its own timeout.
			bg := context.WithoutCancel(ctx)

			r.run(func() { r.runShadow(bg, req, result, pgLatency) })
		} else if r.logger != nil {
			r.logger.Debugw(
				"search shadow: skipping ineligible query (unmapped filter or empty query string)",
				"query", req.GetQuery())
		}
	}

	return result, nil
}

// shadowEligible reports whether a query can be shadow-compared: the recorder
// mapped every filter (an unmapped filter would compare a PG-filtered result
// against an unfiltered Tantivy one) and the query carries free text. Structured
// sorts stay eligible on purpose — sortableFields exists precisely so the shadow
// comparison can mirror them; they are excluded from SERVING, not from comparison.
func shadowEligible(req *pb.SearchRequest, meta buildResult) bool {
	return meta.canCompare && req.GetQuery() != ""
}

// serveEligible applies the default-deny serving whitelist (design §2): a
// non-empty free-text query, no unmapped structured filters, relevance ordering
// only, and no facets/aggregations. Everything else stays on PostgreSQL.
func serveEligible(req *pb.SearchRequest, meta buildResult) bool {
	return req.GetQuery() != "" &&
		meta.canCompare &&
		len(req.GetSort()) == 0 &&
		!meta.hasFacets
}

// shouldServe applies request eligibility, mode, cached health, and sticky
// canary gates. ModeTantivy ignores CanaryPercent and serves the entire eligible
// class.
func (r *Router) shouldServe(req *pb.SearchRequest, meta buildResult) bool {
	if !r.cfg.Mode.serving() {
		return false
	}

	if !serveEligible(req, meta) || !r.health.ServeEligible() {
		return false
	}

	if r.cfg.Mode == ModeCanary && canaryBucket(req.GetQuery()) >= r.cfg.CanaryPercent {
		return false
	}

	return true
}

// serveTantivy executes the bounded serving RPC and hydrates its ranked hits
// from PostgreSQL. Any RPC or hydration error is swallowed and reported as
// served=false so the caller can run the unchanged PostgreSQL search.
func (r *Router) serveTantivy(
	ctx context.Context,
	req *pb.SearchRequest,
) (search.TorrentContentResult, bool) {
	sctx, cancel := context.WithTimeout(ctx, r.cfg.serveTimeout())
	defer cancel()

	resp, err := r.tantivy.Search(sctx, req)
	if err != nil {
		r.serveMetrics.IncServe("fallback_error")

		if r.logger != nil {
			r.logger.Debugw("search serve: tantivy query failed; falling back to PostgreSQL",
				"error", err, "query", req.GetQuery())
		}

		return search.TorrentContentResult{}, false
	}

	hits := resp.GetHits()
	offset := uint64(req.GetPagination().GetOffset())

	if len(hits) == 0 {
		if offset < resp.GetTotalHits() {
			r.serveMetrics.IncServe("fallback_empty")

			if r.logger != nil {
				r.logger.Debugw(
					"search serve: sidecar returned no hits inside the result window; falling back to PostgreSQL",
					"query", req.GetQuery(),
					"offset", offset,
					"total_hits", resp.GetTotalHits())
			}

			return search.TorrentContentResult{}, false
		}

		// Offset at/past the exact total: an authoritative empty page.
		r.serveMetrics.IncServe("served")

		return search.TorrentContentResult{
			TotalCount:           uint(resp.GetTotalHits()),
			TotalCountIsEstimate: false,
			HasNextPage:          false,
			Items:                nil,
		}, true
	}

	ids, infoHashes := tantivyResultKeys(resp)
	hydrated, err := r.hydrateByInfoHash(ctx, infoHashes)
	if err != nil {
		r.serveMetrics.IncServe("fallback_hydrate_error")

		if r.logger != nil {
			r.logger.Debugw("search serve: PostgreSQL hydration failed; falling back to full PostgreSQL search",
				"error", err, "query", req.GetQuery())
		}

		return search.TorrentContentResult{}, false
	}

	items := selectRankedItems(hydrated.Items, ids)

	result := search.TorrentContentResult{
		TotalCount:           uint(resp.GetTotalHits()),
		TotalCountIsEstimate: false,
		HasNextPage: offset+uint64(len(hits)) <
			resp.GetTotalHits(),
		Items: items,
	}
	r.serveMetrics.IncServe("served")

	return result, true
}

// hydrateByInfoHash reloads Tantivy hits through the PostgreSQL search layer's
// normal joins and hydrators, restricted to the recalled info-hash set and with
// no page window or tsquery. It calls the embedded search directly to avoid
// routing recursion.
func (r *Router) hydrateByInfoHash(
	ctx context.Context,
	ids []protocol.ID,
) (search.TorrentContentResult, error) {
	opts := []query.Option{
		search.TorrentContentCoreJoins(),
		search.HydrateTorrentContentTorrent(),
		search.HydrateTorrentContentContent(),
		query.Where(search.TorrentContentInfoHashCriteria(ids...)),
	}

	return r.Search.TorrentContent(ctx, opts...)
}

// tantivyResultKeys returns hit DocIDs in rank order plus a de-duplicated set of
// valid info hashes for PostgreSQL hydration. Invalid hashes remain represented
// in the rank list but cannot widen the hydration query.
func tantivyResultKeys(resp *pb.SearchResponse) (ids []string, infoHashes []protocol.ID) {
	hits := resp.GetHits()
	ids = make([]string, 0, len(hits))
	infoHashes = make([]protocol.ID, 0, len(hits))
	seen := make(map[protocol.ID]struct{}, len(hits))

	for _, hit := range hits {
		doc := hit.GetDocument()
		ids = append(ids, tantivy.DocID(doc))

		id, err := protocol.NewIDFromByteSlice(doc.GetInfoHash())
		if err != nil {
			continue
		}

		if _, exists := seen[id]; exists {
			continue
		}

		seen[id] = struct{}{}
		infoHashes = append(infoHashes, id)
	}

	return ids, infoHashes
}

// selectRankedItems drops hydrated rows Tantivy did not rank and returns the rest
// in Tantivy rank order.
//
// The PostgreSQL hydrate matches on info_hash alone, but Tantivy ranks per
// torrent_content row (info_hash + classification), and info_hash is not unique in
// that table. A torrent carrying several classifications therefore hydrates a
// sibling row for every classification while only the ranked DocID belongs on the
// page: keeping the siblings would overflow the requested limit with rows the
// engine never ranked. A ranked DocID missing from PostgreSQL (index drift) simply
// yields a shorter page, which is the fail-safe direction.
func selectRankedItems(
	items []search.TorrentContentResultItem,
	ids []string,
) []search.TorrentContentResultItem {
	positions := make(map[string]int, len(ids))
	for i, id := range ids {
		if _, exists := positions[id]; !exists {
			positions[id] = i
		}
	}

	ranked := make([]search.TorrentContentResultItem, 0, len(ids))

	for _, item := range items {
		if _, exists := positions[item.InferID()]; exists {
			ranked = append(ranked, item)
		}
	}

	sort.SliceStable(ranked, func(a, b int) bool {
		return positions[ranked[a].InferID()] < positions[ranked[b].InferID()]
	})

	return ranked
}

// shouldShadow reports whether this query should be shadow-compared: a
// Tantivy-backed mode, a positive sample rate, and a sampling draw under it.
func (r *Router) shouldShadow() bool {
	if !r.cfg.Mode.tantivyBacked() {
		return false
	}

	return r.cfg.SampleRate > 0 && r.sample() < r.cfg.SampleRate
}

// runShadow runs the query against Tantivy and records the comparison against
// the already-served PostgreSQL result. It is fully synchronous (the caller
// decides whether to run it in a goroutine via Router.run), so tests can invoke
// it directly.
func (r *Router) runShadow(
	ctx context.Context,
	req *pb.SearchRequest,
	pgResult search.TorrentContentResult,
	pgLatency time.Duration,
) {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.shadowTimeout())
	defer cancel()

	start := time.Now()

	resp, err := r.tantivy.Search(ctx, req)
	if err != nil {
		if r.logger != nil {
			r.logger.Debugw("search shadow: tantivy query failed",
				"error", err, "query", req.GetQuery())
		}

		return
	}

	tantivyLatency := time.Since(start)

	c := shadow.Compare(extractPGIDs(pgResult), extractTantivyIDs(resp), pgLatency, tantivyLatency)
	if r.metrics != nil {
		r.metrics.Observe(c)
	}

	shadow.LogComparison(r.logger, req.GetQuery(), c, r.cfg.LogDiscrepancies)
}

// extractPGIDs returns the stable result IDs of a PostgreSQL result in rank
// order. The stable key is TorrentContent.InferID() —
// hex(info_hash):content_type:content_source:content_id — which is exactly the
// Tantivy doc_id (see tantivy.DocID), so the two engines' IDs are comparable.
func extractPGIDs(result search.TorrentContentResult) []string {
	ids := make([]string, 0, len(result.Items))
	for i := range result.Items {
		ids = append(ids, result.Items[i].InferID())
	}

	return ids
}

// extractTantivyIDs returns the stable result IDs of a Tantivy response in rank
// order, derived from each hit's document the same way the index keys it.
func extractTantivyIDs(resp *pb.SearchResponse) []string {
	hits := resp.GetHits()

	ids := make([]string, 0, len(hits))
	for _, h := range hits {
		ids = append(ids, tantivy.DocID(h.GetDocument()))
	}

	return ids
}

// canaryBucket maps a query string to a stable bucket in [0,100). A query with a
// bucket below Config.CanaryPercent is served from Tantivy when every other gate
// passes; hashing the query keeps the routing decision sticky per
// query so a user re-running a search sees a consistent engine.
func canaryBucket(queryString string) float64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(queryString))

	return float64(h.Sum32()%10_000) / 100.0
}
