// Package router provides SearchRouter, a drop-in search.Search implementation
// that blends the legacy PostgreSQL search engine with the Tantivy sidecar
// according to a configured Mode (postgres / shadow / canary / tantivy).
//
// It is swapped in for the real search.Search via fx with no call-site changes
// (the GraphQL and Torznab adapters keep calling TorrentContent). In Phase 4 the
// PostgreSQL engine always serves the result; in the Tantivy-backed modes the
// router additionally runs the same query against Tantivy in the background and
// records — via the shadow comparator — how the two result sets compare, without
// ever affecting the served result or its latency. Serving results from Tantivy
// (canary / full cutover) is scaffolded here and completed in Phase 6.
package router

import (
	"context"
	"hash/fnv"
	"math/rand"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/search/shadow"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"go.uber.org/zap"
)

// tantivySearcher is the slice of the Tantivy client the router depends on.
// *tantivy.Client satisfies it; tests substitute a fake.
type tantivySearcher interface {
	Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error)
}

// observer records a single shadow comparison. *shadow.Metrics satisfies it.
type observer interface {
	Observe(shadow.Comparison)
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

	// sample returns a value in [0,1) to gate shadow sampling; run executes the
	// background shadow task. Both are fields so tests can make the otherwise
	// non-deterministic, asynchronous shadow path deterministic and synchronous.
	sample func() float64
	run    func(func())
}

// New builds a Router wrapping the PostgreSQL search engine pg and the Tantivy
// client, recording comparisons into metrics and discrepancies into logger.
func New(
	pg search.Search,
	client tantivySearcher,
	metrics observer,
	logger *zap.SugaredLogger,
	cfg Config,
) *Router {
	return &Router{
		Search:  pg,
		tantivy: client,
		builder: optionRequestBuilder{},
		metrics: metrics,
		logger:  logger,
		cfg:     cfg,
		sample:  rand.Float64,
		run:     func(f func()) { go f() },
	}
}

// TorrentContent serves the PostgreSQL result and, in the Tantivy-backed modes,
// fires a background shadow comparison for a sampled fraction of queries. The
// returned result and the call's latency are exactly those of the underlying
// PostgreSQL search; the comparison never blocks or alters them.
func (r *Router) TorrentContent(
	ctx context.Context,
	options ...query.Option,
) (search.TorrentContentResult, error) {
	start := time.Now()

	result, err := r.Search.TorrentContent(ctx, options...)
	if err != nil {
		return result, err
	}

	pgLatency := time.Since(start)

	if r.shouldShadow() {
		// Detach from the request context so the comparison can outlive the
		// response and a cancelled request never aborts it; runShadow bounds it
		// with its own timeout.
		bg := context.WithoutCancel(ctx)

		r.run(func() { r.runShadow(bg, options, result, pgLatency) })
	}

	// TODO(phase6): in ModeCanary / ModeTantivy, serve eligible queries
	// (canaryBucket vs Config.CanaryPercent) from Tantivy by hydrating the hits
	// back into TorrentContentResultItems from PostgreSQL. Until that lands,
	// PostgreSQL serves every mode and the Tantivy side is observation-only.
	return result, nil
}

// shouldShadow reports whether this query should be shadow-compared: a
// Tantivy-backed mode, a positive sample rate, and a sampling draw under it.
func (r *Router) shouldShadow() bool {
	switch r.cfg.Mode {
	case ModeShadow, ModeCanary, ModeTantivy:
	case ModePostgres:
		return false
	default:
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
	options []query.Option,
	pgResult search.TorrentContentResult,
	pgLatency time.Duration,
) {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.shadowTimeout())
	defer cancel()

	req, canCompare := r.builder.build(options)
	if !canCompare {
		// The query carried filters the request builder can't yet map to
		// pb.SearchFilters (Phase-5 work). Comparing a PG-filtered result against
		// an unfiltered Tantivy result would be a false discrepancy, so skip it
		// and don't pollute the similarity metrics; log for coverage visibility.
		if r.logger != nil {
			r.logger.Debugw("search shadow: skipping filtered query (filters not yet mapped to Tantivy)",
				"query", req.GetQuery())
		}

		return
	}

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
	r.metrics.Observe(c)
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
// bucket below Config.CanaryPercent is the set the canary will (in Phase 6)
// serve from Tantivy; hashing the query keeps the routing decision sticky per
// query so a user re-running a search sees a consistent engine.
func canaryBucket(queryString string) float64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(queryString))

	return float64(h.Sum32()%10_000) / 100.0
}
