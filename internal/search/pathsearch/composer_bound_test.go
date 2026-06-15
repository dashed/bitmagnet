package pathsearch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
)

// counterVal reads a nil-safe single counter for assertions.
func deadlineCappedCount(m *Metrics) float64 { return testutil.ToFloat64(m.deadlineCapped) }
func shedCount(m *Metrics) float64           { return testutil.ToFloat64(m.refineShed) }
func retainedCappedCount(m *Metrics) float64 { return testutil.ToFloat64(m.retainedCapped) }

// boundPG is a controllable PG fake for the gate7-6 route deadline / concurrency
// tests. Its TorrentContent returns realResult for the first (blockFromCall-1)
// data calls, then BLOCKS on ctx.Done() (simulating a slow chunk decode that the
// route deadline must cut off) and returns ctx.Err(); blockFromCall<=0 never
// blocks. A non-deadline hard error is returned via hardErr (when set, every call
// returns it immediately — the deadline-vs-real-error branch).
type boundPG struct {
	realResult    search.TorrentContentResult
	counts        map[protocol.ID]int
	callCount     int
	blockFromCall int
	hardErr       error
}

func (p *boundPG) TorrentContent(ctx context.Context, _ ...query.Option) (search.TorrentContentResult, error) {
	p.callCount++

	if p.hardErr != nil {
		return search.TorrentContentResult{}, p.hardErr
	}

	if p.blockFromCall > 0 && p.callCount >= p.blockFromCall {
		<-ctx.Done()

		return search.TorrentContentResult{}, ctx.Err()
	}

	return p.realResult, nil
}

func (p *boundPG) FileCounts(_ context.Context, ids []protocol.ID) (map[protocol.ID]int, error) {
	out := make(map[protocol.ID]int, len(ids))

	for _, id := range ids {
		if p.counts != nil {
			if n, ok := p.counts[id]; ok {
				out[id] = n

				continue
			}
		}

		out[id] = 1
	}

	return out, nil
}

// newBoundComposer builds a composer with explicit gate7-6 knobs (route timeout,
// concurrency, slot wait) and a forced-multi-chunk MaxChunkTorrents so a tiny
// corpus can exercise the expensive path.
func newBoundComposer(
	l3 candidateSource,
	pg torrentContentSearcher,
	m *Metrics,
	routeTimeout, slotWait time.Duration,
	maxConcurrent int,
	maxChunkTorrents uint,
) *Composer {
	return NewComposer(l3, pg, ComposerConfig{
		MinQueryLength:       3,
		OversampleFactor:     4,
		MaxCandidates:        1000,
		MaxChunkTorrents:     maxChunkTorrents,
		RouteTimeout:         routeTimeout,
		MaxConcurrentRefines: maxConcurrent,
		SlotWait:             slotWait,
	}, nil, WithMetrics(m))
}

// (1a) Deadline-cap on the single-chunk fast path: the chunk decode blocks past
// the route deadline → the route serves a deadline-capped estimate (served=true,
// isEstimate, deadline_capped++), NOT served=false and NOT a RouteError fallback
// (which would trigger the resolver's broad-FTS PG wall).
func TestComposer_RouteDeadline_SingleChunk_ServesCappedNotFallback(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: candList(1)}}
	pg := &boundPG{
		realResult:    search.TorrentContentResult{Items: []search.TorrentContentResultItem{item(1, tf("Inception.mkv", "mkv", 1))}},
		blockFromCall: 1, // block the very first (combined) query
	}

	c := newBoundComposer(l3, pg, m, 50*time.Millisecond, 0, 4, 0)

	res, served, err := c.TorrentContent(context.Background(), Filters{Query: "inception"}, QueryOptions{}, 10, 0, nil)
	if err != nil || !served {
		t.Fatalf("deadline must SERVE a capped estimate (served=true, err=nil), got served=%v err=%v", served, err)
	}

	if !res.TotalCountIsEstimate || len(res.Items) != 0 {
		t.Fatalf("deadline-capped single-chunk must be an empty estimate, got %d items estimate=%v", len(res.Items), res.TotalCountIsEstimate)
	}

	if got := deadlineCappedCount(m); got != 1 {
		t.Fatalf("refine_deadline_capped_total = %v, want 1", got)
	}

	if got := routeCount(m, RouteError); got != 0 {
		t.Fatalf("a deadline must NOT count as RouteError (would trigger PG broad-FTS wall), got error=%v", got)
	}

	if got := routeCount(m, RouteServed); got != 1 {
		t.Fatalf("route_total{served} = %v, want 1", got)
	}
}

// (1b) Deadline-cap on the multi-chunk path serves the ACCUMULATED top-relevance
// prefix: chunk-0 matches, then chunk-1's decode blocks past the deadline → the
// route serves chunk-0's match (not empty, not a fallback).
func TestComposer_RouteDeadline_MultiChunk_ServesAccumulatedPrefix(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: candList(1, 2)}}
	pg := &boundPG{
		realResult: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
			item(1, tf("inception/a.mkv", "mkv", 1)),
			item(2, tf("inception/b.mkv", "mkv", 2)),
		}},
		// gate7-8 call order (agg moved to AFTER refine): 1=chunk0 (id1), 2=chunk1
		// (id2, blocks past deadline → capDeadline with refined=[id1]), 3=refined-agg
		// (ctx already past deadline → fails fast → empty facets). Block from call 2.
		blockFromCall: 2,
	}

	// MaxChunkTorrents=1 forces one torrent per chunk → 2 chunks (multi-chunk path).
	c := newBoundComposer(l3, pg, m, 100*time.Millisecond, 0, 4, 1)

	res, served, err := c.TorrentContent(context.Background(), Filters{Query: "inception"}, QueryOptions{}, 10, 0, nil)
	if err != nil || !served {
		t.Fatalf("deadline must SERVE the accumulated prefix, got served=%v err=%v", served, err)
	}

	if len(res.Items) != 1 || res.Items[0].InfoHash != ih(1) {
		t.Fatalf("deadline-capped multi-chunk must serve chunk-0's match [id1], got %d items", len(res.Items))
	}

	if !res.TotalCountIsEstimate {
		t.Fatal("deadline-capped result must carry TotalCountIsEstimate=true")
	}

	if got := deadlineCappedCount(m); got != 1 {
		t.Fatalf("refine_deadline_capped_total = %v, want 1", got)
	}

	if got := routeCount(m, RouteError); got != 0 {
		t.Fatalf("deadline must NOT be a RouteError, got %v", got)
	}

	if pg.callCount != 3 {
		t.Fatalf("expected 3 PG calls (2 chunks + refined-agg fast-fail), got %d", pg.callCount)
	}
}

// (1c) The deadline-vs-real-error branch: a NON-deadline PG error still falls back
// loud as a RouteError (served=false), NOT a deadline-cap. This is the existing
// behavior the deadline branch must preserve.
func TestComposer_RouteDeadline_NonDeadlineErrorStillFallsBack(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: candList(1)}}
	pg := &boundPG{hardErr: errors.New("pg exploded")}

	// Generous route timeout so the ctx is NOT past its deadline when the error returns.
	c := newBoundComposer(l3, pg, m, 5*time.Second, 0, 4, 0)

	_, served, err := c.TorrentContent(context.Background(), Filters{Query: "inception"}, QueryOptions{}, 10, 0, nil)
	if err == nil || served {
		t.Fatalf("a non-deadline PG error must fall back (served=false, err!=nil), got served=%v err=%v", served, err)
	}

	if got := routeCount(m, RouteError); got != 1 {
		t.Fatalf("route_total{error} = %v, want 1", got)
	}

	if got := deadlineCappedCount(m); got != 0 {
		t.Fatalf("a real error must NOT be counted as deadline-capped, got %v", got)
	}
}

// (2) Shed: when the concurrency limiter is saturated, a multi-chunk request is
// SHED (served=true empty estimate + shed metric, NEVER a PG fallback), while a
// SINGLE-chunk request during the same saturation BYPASSES the limiter and is
// served normally (never shed).
func TestComposer_ConcurrencyLimiter_ShedsMultiChunkNotSingleChunk(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	pg := &boundPG{realResult: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		item(1, tf("inception/a.mkv", "mkv", 1)),
		item(2, tf("inception/b.mkv", "mkv", 2)),
	}}}

	// 1 slot, small slot wait so the shed is fast + deterministic, MaxChunkTorrents=1
	// so a 2-candidate query is multi-chunk and contends for the slot.
	c := newBoundComposer(&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: candList(1, 2)}}, pg, m,
		2*time.Second, 50*time.Millisecond, 1, 1)

	// Saturate the only slot (held for the duration of the multi-chunk attempt).
	if !c.sem.TryAcquire(1) {
		t.Fatal("precondition: should be able to take the only slot")
	}

	res, served, err := c.TorrentContent(context.Background(), Filters{Query: "inception"}, QueryOptions{}, 10, 0, nil)
	if err != nil || !served {
		t.Fatalf("a shed request must SERVE an empty estimate (served=true), got served=%v err=%v", served, err)
	}

	if len(res.Items) != 0 || !res.TotalCountIsEstimate {
		t.Fatalf("shed must be an empty estimate, got %d items estimate=%v", len(res.Items), res.TotalCountIsEstimate)
	}

	if got := shedCount(m); got != 1 {
		t.Fatalf("refine_shed_total = %v, want 1", got)
	}

	if pg.callCount != 0 {
		t.Fatalf("a shed request must NOT query PG (no broad-FTS fallback), got %d PG calls", pg.callCount)
	}

	if got := routeCount(m, RouteError); got != 0 {
		t.Fatalf("shed must NOT be a RouteError, got %v", got)
	}

	// A SINGLE-chunk request during the SAME saturation bypasses the limiter.
	singleL3 := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: candList(1)}}
	c2 := &Composer{
		l3:                 singleL3,
		pg:                 pg,
		cfg:                c.cfg,
		metrics:            m,
		maxRefineFiles:     c.maxRefineFiles,
		refineFileBudget:   c.refineFileBudget,
		maxChunkTorrents:   c.maxChunkTorrents,
		retainedFileBudget: c.retainedFileBudget,
		routeTimeout:       c.routeTimeout,
		sem:                c.sem, // SHARE the saturated semaphore
		slotWait:           c.slotWait,
	}

	res2, served2, err2 := c2.TorrentContent(context.Background(), Filters{Query: "inception"}, QueryOptions{}, 10, 0, nil)
	if err2 != nil || !served2 {
		t.Fatalf("a single-chunk request must be served even while saturated, got served=%v err=%v", served2, err2)
	}

	if len(res2.Items) != 1 {
		t.Fatalf("single-chunk request must return its match, got %d items", len(res2.Items))
	}

	if got := shedCount(m); got != 1 {
		t.Fatalf("a single-chunk request must NOT be shed; shed_total = %v, want 1 (unchanged)", got)
	}

	c.sem.Release(1)
}

// (3) The selective fast path is unaffected: a normal single-chunk query fires
// NEITHER new metric, bypasses the semaphore (the slot is free afterwards), and
// serves exactly as before.
func TestComposer_FastPath_UnaffectedByBounds(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: candList(1)}}
	pg := &boundPG{realResult: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		item(1, tf("Inception.2010.1080p.mkv", "mkv", 1)),
	}}}

	c := newBoundComposer(l3, pg, m, 5*time.Second, 50*time.Millisecond, 1, 0)

	res, served, err := c.TorrentContent(context.Background(), Filters{Query: "inception"}, QueryOptions{}, 10, 0, nil)
	if err != nil || !served {
		t.Fatalf("normal query must be served, got served=%v err=%v", served, err)
	}

	if len(res.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(res.Items))
	}

	if got := deadlineCappedCount(m); got != 0 {
		t.Fatalf("fast path must not fire deadline_capped, got %v", got)
	}

	if got := shedCount(m); got != 0 {
		t.Fatalf("fast path must not fire shed, got %v", got)
	}

	if got := retainedCappedCount(m); got != 0 {
		t.Fatalf("fast path must not fire retained_capped, got %v", got)
	}

	// The fast path never acquired the (single) slot.
	if !c.sem.TryAcquire(1) {
		t.Fatal("fast path must NOT hold a concurrency slot (it should be free)")
	}

	c.sem.Release(1)
}

// (4) No slot leak: after a multi-chunk request that deadline-caps AND after one
// that hard-errors, the acquired slot is released (defer), so the only slot is
// free again.
func TestComposer_ConcurrencyLimiter_ReleasesSlotOnDeadlineAndError(t *testing.T) {
	t.Parallel()

	// Deadline path.
	mD := NewMetrics()
	pgD := &boundPG{
		realResult:    search.TorrentContentResult{Items: []search.TorrentContentResultItem{item(1, tf("inception/a.mkv", "mkv", 1)), item(2, tf("inception/b.mkv", "mkv", 2))}},
		blockFromCall: 1, // block the first chunk decode → deadline before any refine (refined empty → no refined-agg query)
	}
	cD := newBoundComposer(&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: candList(1, 2)}}, pgD, mD,
		50*time.Millisecond, 0, 1, 1)

	_, served, err := cD.TorrentContent(context.Background(), Filters{Query: "inception"}, QueryOptions{}, 10, 0, nil)
	if err != nil || !served {
		t.Fatalf("deadline multi-chunk must serve, got served=%v err=%v", served, err)
	}

	if !cD.sem.TryAcquire(1) {
		t.Fatal("slot leaked on the deadline path (defer Release must run)")
	}

	cD.sem.Release(1)

	// Error path.
	mE := NewMetrics()
	pgE := &boundPG{hardErr: errors.New("boom"), counts: map[protocol.ID]int{}}
	cE := newBoundComposer(&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: candList(1, 2)}}, pgE, mE,
		5*time.Second, 0, 1, 1)

	_, servedE, errE := cE.TorrentContent(context.Background(), Filters{Query: "inception"}, QueryOptions{}, 10, 0, nil)
	if errE == nil || servedE {
		t.Fatalf("hard error multi-chunk must fall back (served=false), got served=%v err=%v", servedE, errE)
	}

	if !cE.sem.TryAcquire(1) {
		t.Fatal("slot leaked on the error path (defer Release must run)")
	}

	cE.sem.Release(1)
}
