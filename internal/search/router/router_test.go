package router

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/shadow"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
)

// --- fakes -----------------------------------------------------------------

// fakePGSearch is a search.Search whose only live method is TorrentContent; the
// rest are promoted from the embedded nil interface and never called in tests.
type fakePGSearch struct {
	search.Search

	result search.TorrentContentResult
	err    error
	calls  int
}

func (f *fakePGSearch) TorrentContent(
	context.Context,
	...query.Option,
) (search.TorrentContentResult, error) {
	f.calls++

	return f.result, f.err
}

type fakeTantivy struct {
	resp    *pb.SearchResponse
	err     error
	calls   int
	lastReq *pb.SearchRequest
}

func (f *fakeTantivy) Search(_ context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	f.calls++
	f.lastReq = req

	return f.resp, f.err
}

type spyObserver struct {
	comparisons []shadow.Comparison
	drops       int
}

func (s *spyObserver) Observe(c shadow.Comparison) {
	s.comparisons = append(s.comparisons, c)
}

func (s *spyObserver) IncDropped() {
	s.drops++
}

type blockingTantivy struct {
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func newBlockingTantivy() *blockingTantivy {
	return &blockingTantivy{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (f *blockingTantivy) Search(ctx context.Context, _ *pb.SearchRequest) (*pb.SearchResponse, error) {
	f.calls.Add(1)

	select {
	case f.started <- struct{}{}:
	default:
	}

	select {
	case <-f.release:
		return &pb.SearchResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type countingObserver struct {
	drops       atomic.Int32
	comparisons chan shadow.Comparison
}

func newCountingObserver() *countingObserver {
	return &countingObserver{comparisons: make(chan shadow.Comparison, 1)}
}

func (o *countingObserver) Observe(c shadow.Comparison) {
	o.comparisons <- c
}

func (o *countingObserver) IncDropped() {
	o.drops.Add(1)
}

// --- helpers ---------------------------------------------------------------

func infoHash(b byte) protocol.ID {
	var id protocol.ID
	for i := range id {
		id[i] = b
	}

	return id
}

// pgItem builds a classified PostgreSQL result item with a known InferID.
func pgItem(b byte, contentID string) search.TorrentContentResultItem {
	return search.TorrentContentResultItem{
		TorrentContent: model.TorrentContent{
			InfoHash:      infoHash(b),
			ContentType:   model.NullContentType{ContentType: model.ContentTypeMovie, Valid: true},
			ContentSource: model.NewNullString("tmdb"),
			ContentID:     model.NewNullString(contentID),
		},
	}
}

// tantivyHit builds a Tantivy hit whose DocID matches pgItem(b, contentID).
func tantivyHit(b byte, contentID string) *pb.SearchHit {
	return &pb.SearchHit{
		Document: &pb.TorrentDocument{
			InfoHash:      infoHash(b).Bytes(),
			ContentType:   pb.ContentType_CONTENT_TYPE_MOVIE,
			ContentSource: "tmdb",
			ContentId:     contentID,
		},
	}
}

func pgResult(items ...search.TorrentContentResultItem) search.TorrentContentResult {
	return search.TorrentContentResult{Items: items}
}

// newTestRouter wires a Router with the given fakes, a synchronous shadow runner
// and a fixed sampling draw, so the otherwise-async shadow path is deterministic.
func newTestRouter(
	pg search.Search,
	tv tantivySearcher,
	obs observer,
	cfg Config,
	sampleDraw float64,
) *Router {
	r := New(pg, tv, obs, nil, cfg)
	r.run = func(f func()) { f() }
	r.sample = func() float64 { return sampleDraw }

	return r
}

// --- tests -----------------------------------------------------------------

func TestPostgresModeNeverShadows(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "1"))}
	tv := &fakeTantivy{resp: &pb.SearchResponse{}}
	obs := &spyObserver{}
	r := newTestRouter(pg, tv, obs, Config{Mode: ModePostgres, SampleRate: 1}, 0)

	result, err := r.TorrentContent(context.Background())
	require.NoError(t, err)

	assert.Equal(t, pg.result, result, "postgres mode returns the PG result unchanged")
	assert.Equal(t, 0, tv.calls, "tantivy must not be queried in postgres mode")
	assert.Empty(t, obs.comparisons, "no comparison in postgres mode")
}

func TestPostgresModeDoesNotInvokeRunHook(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "1"))}
	tv := &fakeTantivy{resp: &pb.SearchResponse{}}
	obs := &spyObserver{}
	r := New(pg, tv, obs, nil, Config{
		Mode:                ModePostgres,
		SampleRate:          1,
		ShadowMaxConcurrent: 1,
	})
	r.sample = func() float64 { return 0 }

	runCalled := false
	r.run = func(func()) {
		runCalled = true
	}

	result, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
	require.NoError(t, err)

	assert.Equal(t, pg.result, result, "postgres mode returns the PG result unchanged")
	assert.False(t, runCalled, "disabled/postgres mode must not spawn a shadow goroutine")
	assert.Equal(t, 0, tv.calls, "tantivy must not be queried in postgres mode")
	assert.Empty(t, obs.comparisons, "no comparison in postgres mode")
	assert.Equal(t, 0, obs.drops, "disabled/postgres mode must not record drops")
}

func TestShadowModeServesPGAndObserves(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "1"), pgItem(0xBB, "2"))}
	tv := &fakeTantivy{resp: &pb.SearchResponse{Hits: []*pb.SearchHit{
		tantivyHit(0xAA, "1"),
		tantivyHit(0xBB, "2"),
	}}}
	obs := &spyObserver{}
	r := newTestRouter(pg, tv, obs, Config{Mode: ModeShadow, SampleRate: 1}, 0)

	result, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
	require.NoError(t, err)

	// Served result is exactly the PG result.
	assert.Equal(t, pg.result, result)
	// The same query reached Tantivy, with the raw query string.
	require.Equal(t, 1, tv.calls)
	assert.Equal(t, "matrix", tv.lastReq.GetQuery())
	// A comparison was recorded; identical ID lists agree perfectly.
	require.Len(t, obs.comparisons, 1)
	c := obs.comparisons[0]
	assert.Equal(t, 2, c.PGCount)
	assert.Equal(t, 2, c.TantivyCount)
	assert.True(t, c.Top1Match)
	assert.InDelta(t, 1.0, c.JaccardAt20, 1e-9)
}

func TestShadowRespectsSampleRate(t *testing.T) {
	t.Parallel()

	// Sampling draw 0.9 is above the 0.5 rate -> not sampled, no shadow.
	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "1"))}
	tv := &fakeTantivy{resp: &pb.SearchResponse{}}
	obs := &spyObserver{}
	r := newTestRouter(pg, tv, obs, Config{Mode: ModeShadow, SampleRate: 0.5}, 0.9)

	_, err := r.TorrentContent(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 0, tv.calls, "draw above sample rate must skip the shadow query")
	assert.Empty(t, obs.comparisons)
}

func TestZeroSampleRateNeverShadows(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "1"))}
	tv := &fakeTantivy{resp: &pb.SearchResponse{}}
	obs := &spyObserver{}
	r := newTestRouter(pg, tv, obs, Config{Mode: ModeShadow, SampleRate: 0}, 0)

	_, err := r.TorrentContent(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 0, tv.calls)
	assert.Empty(t, obs.comparisons)
}

func TestShadowTantivyErrorIsSwallowed(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "1"))}
	tv := &fakeTantivy{err: errors.New("sidecar down")}
	obs := &spyObserver{}
	r := newTestRouter(pg, tv, obs, Config{Mode: ModeShadow, SampleRate: 1}, 0)

	result, err := r.TorrentContent(context.Background())
	require.NoError(t, err, "a Tantivy failure must not surface to the caller")

	assert.Equal(t, pg.result, result)
	assert.Equal(t, 1, tv.calls)
	assert.Empty(t, obs.comparisons, "a failed Tantivy query records no comparison")
}

func TestPGErrorShortCircuitsBeforeShadow(t *testing.T) {
	t.Parallel()

	pgErr := errors.New("pg boom")
	pg := &fakePGSearch{err: pgErr}
	tv := &fakeTantivy{resp: &pb.SearchResponse{}}
	obs := &spyObserver{}
	r := newTestRouter(pg, tv, obs, Config{Mode: ModeShadow, SampleRate: 1}, 0)

	_, err := r.TorrentContent(context.Background())
	require.ErrorIs(t, err, pgErr)

	assert.Equal(t, 0, tv.calls, "no shadow query when PG itself errored")
	assert.Empty(t, obs.comparisons)
}

func TestCanaryModeStillShadows(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "1"))}
	tv := &fakeTantivy{resp: &pb.SearchResponse{Hits: []*pb.SearchHit{tantivyHit(0xAA, "1")}}}
	obs := &spyObserver{}
	r := newTestRouter(pg, tv, obs, Config{Mode: ModeCanary, SampleRate: 1}, 0)

	result, err := r.TorrentContent(context.Background())
	require.NoError(t, err)

	// Phase 4: canary still serves PG and observes (serving path is Phase 6).
	assert.Equal(t, pg.result, result)
	assert.Equal(t, 1, tv.calls)
	assert.Len(t, obs.comparisons, 1)
}

func TestShadowRunnerDropsWhenMaxConcurrentSaturated(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "1"))}
	tv := newBlockingTantivy()
	obs := newCountingObserver()
	r := New(pg, tv, obs, nil, Config{
		Mode:                ModeShadow,
		SampleRate:          1,
		ShadowTimeout:       time.Second,
		ShadowMaxConcurrent: 1,
	})
	r.sample = func() float64 { return 0 }

	_, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
	require.NoError(t, err)

	select {
	case <-tv.started:
	case <-time.After(time.Second):
		t.Fatal("first shadow comparison did not start")
	}

	secondDone := make(chan error, 1)
	go func() {
		_, callErr := r.TorrentContent(context.Background(), query.SearchString("matrix"))
		secondDone <- callErr
	}()

	select {
	case callErr := <-secondDone:
		require.NoError(t, callErr)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("second sampled query blocked instead of dropping the shadow comparison")
	}

	assert.Equal(t, int32(1), tv.calls.Load(), "saturated runner must not start a second Tantivy query")
	assert.Equal(t, int32(1), obs.drops.Load(), "saturated runner must count one dropped comparison")

	close(tv.release)

	select {
	case <-obs.comparisons:
	case <-time.After(time.Second):
		t.Fatal("first admitted shadow comparison did not finish")
	}
}

func TestShadowSkipsFilteredQueries(t *testing.T) {
	t.Parallel()

	// A filtered query can't be mapped to pb.SearchFilters yet, so comparing it
	// would be PG-filtered vs Tantivy-unfiltered — a false discrepancy. The
	// router must skip the Tantivy query and record no comparison.
	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "1"))}
	tv := &fakeTantivy{resp: &pb.SearchResponse{}}
	obs := &spyObserver{}
	r := newTestRouter(pg, tv, obs, Config{Mode: ModeShadow, SampleRate: 1}, 0)

	result, err := r.TorrentContent(
		context.Background(),
		query.SearchString("matrix"),
		query.Where(search.TorrentContentTypeCriteria(model.ContentTypeMovie)),
	)
	require.NoError(t, err)

	assert.Equal(t, pg.result, result, "PG result is still served unchanged")
	assert.Equal(t, 0, tv.calls, "filtered queries must not be shadow-queried")
	assert.Empty(t, obs.comparisons, "no comparison recorded for a filtered query")
}

func TestRequestBuilderExtractsQueryPaginationSort(t *testing.T) {
	t.Parallel()

	req, canCompare := optionRequestBuilder{}.build([]query.Option{
		query.SearchString("the matrix"),
		query.Limit(25),
		query.Offset(50),
		query.OrderBy(query.OrderByColumn{
			OrderByColumn: clause.OrderByColumn{
				Column: clause.Column{Name: "published_at"},
				Desc:   true,
			},
		}),
	})
	require.True(t, canCompare, "an unfiltered query is comparable")

	assert.Equal(t, "the matrix", req.GetQuery())
	require.NotNil(t, req.GetPagination())
	assert.Equal(t, uint32(25), req.GetPagination().GetLimit())
	assert.Equal(t, uint32(50), req.GetPagination().GetOffset())
	require.Len(t, req.GetSort(), 1)
	assert.Equal(t, "published_at", req.GetSort()[0].GetField())
	assert.True(t, req.GetSort()[0].GetDescending())
}

func TestRequestBuilderFlagsFilteredQueryNotComparable(t *testing.T) {
	t.Parallel()

	// A filter criterion compiles to opaque SQL needing a live *dao.Query, so the
	// recorder skips it and marks the request not comparable — but still extracts
	// the query string (filter -> pb.SearchFilters mapping is a Phase-5 gap).
	req, canCompare := optionRequestBuilder{}.build([]query.Option{
		query.SearchString("the matrix"),
		query.Where(search.TorrentContentTypeCriteria(model.ContentTypeMovie)),
	})

	assert.False(t, canCompare, "a filtered query must not be comparable")
	assert.Equal(t, "the matrix", req.GetQuery())
}

func TestRequestBuilderSkipsRelevanceOrdering(t *testing.T) {
	t.Parallel()

	// query_string_rank maps to Tantivy's default score order -> no explicit sort.
	req, canCompare := optionRequestBuilder{}.build([]query.Option{
		query.SearchString("x"),
		query.OrderByQueryStringRank(),
	})
	require.True(t, canCompare)
	assert.Empty(t, req.GetSort())
}

func TestCanaryBucketIsStickyAndInRange(t *testing.T) {
	t.Parallel()

	for _, q := range []string{"", "matrix", "ubuntu 24.04", "a longer query string here"} {
		b1 := canaryBucket(q)
		b2 := canaryBucket(q)
		assert.InDelta(t, b1, b2, 0, "bucket must be stable for a given query")
		assert.GreaterOrEqual(t, b1, 0.0)
		assert.Less(t, b1, 100.0)
	}
}

func TestExtractTantivyIDsMatchesPGInferID(t *testing.T) {
	t.Parallel()

	// The two engines' stable IDs must coincide for the same document.
	pgIDs := extractPGIDs(pgResult(pgItem(0xAA, "603")))
	tantivyIDs := extractTantivyIDs(&pb.SearchResponse{Hits: []*pb.SearchHit{tantivyHit(0xAA, "603")}})

	require.Len(t, pgIDs, 1)
	assert.Equal(t, pgIDs, tantivyIDs)
}
