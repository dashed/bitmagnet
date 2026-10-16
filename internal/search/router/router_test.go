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

	result  search.TorrentContentResult
	err     error
	calls   int
	options [][]query.Option
	handler func(context.Context, []query.Option) (search.TorrentContentResult, error)
}

func (f *fakePGSearch) TorrentContent(
	ctx context.Context,
	options ...query.Option,
) (search.TorrentContentResult, error) {
	f.calls++
	f.options = append(f.options, append([]query.Option(nil), options...))
	if f.handler != nil {
		return f.handler(ctx, options)
	}

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

type countingRequestBuilder struct {
	calls int
	req   *pb.SearchRequest
}

func (b *countingRequestBuilder) build([]query.Option) (*pb.SearchRequest, buildResult) {
	b.calls++
	if b.req != nil {
		return b.req, buildResult{canCompare: true}
	}

	return &pb.SearchRequest{}, buildResult{canCompare: true}
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
	r := New(pg, tv, obs, nil, cfg, NewHealthState(), nil)
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

	result, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
	require.NoError(t, err)

	assert.Equal(t, pg.result, result, "postgres mode returns the PG result unchanged")
	assert.Equal(t, 0, tv.calls, "tantivy must not be queried in postgres mode")
	assert.Empty(t, obs.comparisons, "no comparison in postgres mode")
}

func TestPostgresModeNeverBuildsTantivyRequest(t *testing.T) {
	t.Parallel()

	for _, mode := range []Mode{ModePostgres, Mode("bogus")} {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()

			pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "pg"))}
			tv := &fakeTantivy{resp: &pb.SearchResponse{}}
			builder := &countingRequestBuilder{}
			r := newTestRouter(pg, tv, &spyObserver{}, Config{
				Mode:       mode,
				SampleRate: 1,
			}, 0)
			r.builder = builder

			result, err := r.TorrentContent(
				context.Background(),
				query.SearchString("matrix"),
				query.Where(search.TorrentContentTypeCriteria(model.ContentTypeMovie)),
			)
			require.NoError(t, err)

			assert.Equal(t, pg.result, result, "passthrough must return the PG result unchanged")
			assert.Equal(t, 1, pg.calls)
			assert.Zero(t, builder.calls, "passthrough modes must not replay query options")
			assert.Zero(t, tv.calls)
		})
	}
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
	}, NewHealthState(), nil)
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

	_, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
	require.NoError(t, err)

	assert.Equal(t, 0, tv.calls, "draw above sample rate must skip the shadow query")
	assert.Empty(t, obs.comparisons)
}

func TestShadowOnlyModeSkipsBuildWhenUnsampled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sampleDraw float64
		wantBuilds int
		wantCalls  int
	}{
		{name: "unsampled", sampleDraw: 0.9, wantBuilds: 0, wantCalls: 0},
		{name: "sampled", sampleDraw: 0.1, wantBuilds: 1, wantCalls: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "pg"))}
			tv := &fakeTantivy{resp: &pb.SearchResponse{}}
			builder := &countingRequestBuilder{req: &pb.SearchRequest{Query: "matrix"}}
			r := newTestRouter(pg, tv, &spyObserver{}, Config{
				Mode:       ModeShadow,
				SampleRate: 0.5,
			}, tt.sampleDraw)
			r.builder = builder

			result, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
			require.NoError(t, err)

			assert.Equal(t, pg.result, result)
			assert.Equal(t, tt.wantBuilds, builder.calls)
			assert.Equal(t, tt.wantCalls, tv.calls)
		})
	}
}

func TestZeroSampleRateNeverShadows(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "1"))}
	tv := &fakeTantivy{resp: &pb.SearchResponse{}}
	obs := &spyObserver{}
	r := newTestRouter(pg, tv, obs, Config{Mode: ModeShadow, SampleRate: 0}, 0)

	_, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
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

	result, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
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

	_, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
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

	result, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
	require.NoError(t, err)

	// The fail-closed health state keeps canary on PG while shadowing remains live.
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
	}, NewHealthState(), nil)
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

func TestShadowSkipsEmptyQueryBeforeGoroutineSpawn(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "1"))}
	tv := &fakeTantivy{resp: &pb.SearchResponse{}}
	r := New(pg, tv, &spyObserver{}, nil, Config{
		Mode:       ModeShadow,
		SampleRate: 1,
	}, NewHealthState(), nil)

	r.sample = func() float64 { return 0 }

	runCalled := false
	r.run = func(func()) { runCalled = true }

	result, err := r.TorrentContent(context.Background(), query.SearchString(""))
	require.NoError(t, err)

	assert.Equal(t, pg.result, result)
	assert.False(t, runCalled, "empty queries must be rejected before spawning shadow work")
	assert.Zero(t, tv.calls)
}

func TestStructuredSortStillShadows(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "1"))}
	tv := &fakeTantivy{resp: &pb.SearchResponse{Hits: []*pb.SearchHit{tantivyHit(0xAA, "1")}}}
	obs := &spyObserver{}
	r := newTestRouter(pg, tv, obs, Config{Mode: ModeShadow, SampleRate: 1}, 0)
	r.health.SetHealthy(true, 1, 1, 1)

	result, err := r.TorrentContent(
		context.Background(),
		query.SearchString("matrix"),
		query.OrderBy(query.OrderByColumn{OrderByColumn: clause.OrderByColumn{
			Column: clause.Column{Name: "published_at"},
			Desc:   true,
		}}),
	)
	require.NoError(t, err)

	assert.Equal(t, pg.result, result)
	require.Equal(t, 1, tv.calls)
	require.Len(t, tv.lastReq.GetSort(), 1)
	assert.Equal(t, "published_at", tv.lastReq.GetSort()[0].GetField())
	assert.Len(t, obs.comparisons, 1)
}

func TestRequestBuilderExtractsQueryPaginationSort(t *testing.T) {
	t.Parallel()

	req, meta := optionRequestBuilder{}.build([]query.Option{
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
	require.True(t, meta.canCompare, "an unfiltered query is comparable")
	assert.False(t, meta.hasFacets)

	assert.Equal(t, "the matrix", req.GetQuery())
	require.NotNil(t, req.GetPagination())
	assert.Equal(t, uint32(25), req.GetPagination().GetLimit())
	assert.Equal(t, uint32(50), req.GetPagination().GetOffset())
	require.Len(t, req.GetSort(), 1)
	assert.Equal(t, "published_at", req.GetSort()[0].GetField())
	assert.True(t, req.GetSort()[0].GetDescending())
}

func TestRequestBuilderComparisonEligibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		queryString string
		extra       []query.Option
		wantCompare bool
	}{
		{name: "empty", queryString: "", wantCompare: true},
		{name: "whitespace only", queryString: " \t\n", wantCompare: true},
		{name: "free text", queryString: "the matrix", wantCompare: true},
		{
			name:        "facet criteria",
			queryString: "the matrix",
			extra: []query.Option{
				query.Where(search.TorrentContentTypeCriteria(model.ContentTypeMovie)),
			},
			wantCompare: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			options := append([]query.Option{query.SearchString(tt.queryString)}, tt.extra...)
			req, meta := optionRequestBuilder{}.build(options)

			assert.Equal(t, tt.wantCompare, meta.canCompare)
			assert.Equal(t, tt.queryString, req.GetQuery())
		})
	}
}

func TestRequestBuilderSkipsRelevanceOrdering(t *testing.T) {
	t.Parallel()

	// query_string_rank maps to Tantivy's default score order -> no explicit sort.
	req, meta := optionRequestBuilder{}.build([]query.Option{
		query.SearchString("x"),
		query.OrderByQueryStringRank(),
	})
	require.True(t, meta.canCompare)
	assert.Empty(t, req.GetSort())
}

func TestRequestBuilderCapturesFacetAndAggregationSignals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opt  query.Option
		want bool
	}{
		{name: "facet", opt: query.WithFacet(search.TorrentContentTypeFacet()), want: true},
		{name: "positive aggregation budget", opt: query.WithAggregationBudget(1), want: true},
		{name: "zero aggregation budget", opt: query.WithAggregationBudget(0), want: false},
		{name: "negative aggregation budget", opt: query.WithAggregationBudget(-1), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, meta := optionRequestBuilder{}.build([]query.Option{
				query.SearchString("matrix"),
				tt.opt,
			})
			assert.Equal(t, tt.want, meta.hasFacets)
		})
	}
}

func TestServeEligibilityDefaultDeny(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		options []query.Option
		want    bool
	}{
		{
			name:    "empty query",
			options: []query.Option{query.SearchString("")},
		},
		{
			name: "unmapped filter",
			options: []query.Option{
				query.SearchString("matrix"),
				query.Where(search.TorrentContentTypeCriteria(model.ContentTypeMovie)),
			},
		},
		{
			name: "structured sort",
			options: []query.Option{
				query.SearchString("matrix"),
				query.OrderBy(query.OrderByColumn{OrderByColumn: clause.OrderByColumn{
					Column: clause.Column{Name: "published_at"},
				}}),
			},
		},
		{
			name: "facet",
			options: []query.Option{
				query.SearchString("matrix"),
				query.WithFacet(search.TorrentContentTypeFacet()),
			},
		},
		{
			name: "aggregation budget",
			options: []query.Option{
				query.SearchString("matrix"),
				query.WithAggregationBudget(1),
			},
		},
		{
			name:    "plain relevance query",
			options: []query.Option{query.SearchString("matrix"), query.OrderByQueryStringRank()},
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, meta := optionRequestBuilder{}.build(tt.options)
			assert.Equal(t, tt.want, serveEligible(req, meta))
		})
	}
}

func TestTantivyModeIneligibleQueriesServePostgres(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		options []query.Option
	}{
		{name: "empty query", options: []query.Option{query.SearchString("")}},
		{
			name: "unmapped filter",
			options: []query.Option{
				query.SearchString("matrix"),
				query.Where(search.TorrentContentTypeCriteria(model.ContentTypeMovie)),
			},
		},
		{
			name: "structured sort",
			options: []query.Option{
				query.SearchString("matrix"),
				query.OrderBy(query.OrderByColumn{OrderByColumn: clause.OrderByColumn{
					Column: clause.Column{Name: "published_at"},
				}}),
			},
		},
		{
			name: "facet",
			options: []query.Option{
				query.SearchString("matrix"),
				query.WithFacet(search.TorrentContentTypeFacet()),
			},
		},
		{
			name: "aggregation budget",
			options: []query.Option{
				query.SearchString("matrix"),
				query.WithAggregationBudget(1),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "pg"))}
			tv := &fakeTantivy{resp: &pb.SearchResponse{}}
			r := newTestRouter(pg, tv, &spyObserver{}, Config{
				Mode:       ModeTantivy,
				SampleRate: 0,
			}, 0)
			r.health.SetHealthy(true, 100, 1000, 1000)

			result, err := r.TorrentContent(context.Background(), tt.options...)
			require.NoError(t, err)

			assert.Equal(t, pg.result, result)
			assert.Equal(t, 1, pg.calls)
			assert.Zero(t, tv.calls, "an ineligible request must not reach Tantivy serving")
		})
	}
}

func TestTantivyModeServesHydratedHitsInTantivyOrder(t *testing.T) {
	t.Parallel()

	first := pgItem(0xAA, "1")
	second := pgItem(0xBB, "2")
	hydrated := pgResult(first, second)
	hydrated.TotalCount = 99
	pg := &fakePGSearch{result: hydrated}
	tv := &fakeTantivy{resp: &pb.SearchResponse{
		Hits: []*pb.SearchHit{
			tantivyHit(0xBB, "2"),
			tantivyHit(0xAA, "1"),
		},
		TotalHits: 7,
	}}
	r := newTestRouter(pg, tv, &spyObserver{}, Config{
		Mode:       ModeTantivy,
		SampleRate: 0,
	}, 0)
	r.health.SetHealthy(true, 100, 1000, 1000)

	result, err := r.TorrentContent(
		context.Background(), query.SearchString("matrix"), query.Limit(2), query.Offset(1),
	)
	require.NoError(t, err)

	require.Equal(t, 1, tv.calls)
	require.Equal(t, 1, pg.calls, "serving should issue only the restricted hydration query")
	require.Len(t, pg.options, 1)
	assert.Len(t, pg.options[0], 4, "hydration uses core joins, both hydrators, and the info-hash restriction")
	hydrateReq, _ := optionRequestBuilder{}.build(pg.options[0])
	assert.Empty(t, hydrateReq.GetQuery(), "the hydration query must not replay the full-text tsquery")

	assert.Equal(t, uint(7), result.TotalCount)
	assert.False(t, result.TotalCountIsEstimate)
	assert.True(t, result.HasNextPage)
	assert.Nil(t, result.Aggregations)
	require.Len(t, result.Items, 2)
	assert.Equal(t, second.InferID(), result.Items[0].InferID())
	assert.Equal(t, first.InferID(), result.Items[1].InferID())
}

func TestServeDropsUnrankedSiblingClassifications(t *testing.T) {
	t.Parallel()

	ranked := pgItem(0xAA, "1")
	sibling := pgItem(0xAA, "2")
	pg := &fakePGSearch{result: pgResult(ranked, sibling)}
	resp := &pb.SearchResponse{
		Hits:      []*pb.SearchHit{tantivyHit(0xAA, "1")},
		TotalHits: 1,
	}
	tv := &fakeTantivy{resp: resp}
	r := newTestRouter(pg, tv, &spyObserver{}, Config{
		Mode:       ModeTantivy,
		SampleRate: 0,
	}, 0)
	r.health.SetHealthy(true, 100, 1000, 1000)

	result, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
	require.NoError(t, err)

	assert.Equal(t, 1, tv.calls)
	assert.Equal(t, 1, pg.calls, "serving should issue only the restricted hydration query")
	assert.Equal(t, uint(resp.GetTotalHits()), result.TotalCount)
	require.Len(t, result.Items, 1)
	assert.Equal(t, ranked.InferID(), result.Items[0].InferID())
}

func TestTantivyModeServesAuthoritativeEmptyWithoutPostgres(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "pg"))}
	tv := &fakeTantivy{resp: &pb.SearchResponse{TotalHits: 0}}
	r := newTestRouter(pg, tv, &spyObserver{}, Config{
		Mode:       ModeTantivy,
		SampleRate: 0,
	}, 0)
	r.health.SetHealthy(true, 100, 1000, 1000)

	result, err := r.TorrentContent(context.Background(), query.SearchString("nothing"))
	require.NoError(t, err)

	assert.Zero(t, result.TotalCount)
	assert.False(t, result.TotalCountIsEstimate)
	assert.False(t, result.HasNextPage)
	assert.Nil(t, result.Items)
	assert.Equal(t, 1, tv.calls)
	assert.Zero(t, pg.calls, "an authoritative empty result needs no hydration or full PG search")
}

func TestServeEmptyHitsInsideWindowFailsClosedToPostgres(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "pg"))}
	tv := &fakeTantivy{resp: &pb.SearchResponse{Hits: nil, TotalHits: 5}}
	r := newTestRouter(pg, tv, &spyObserver{}, Config{
		Mode:       ModeTantivy,
		SampleRate: 0,
	}, 0)
	r.health.SetHealthy(true, 100, 1000, 1000)

	result, err := r.TorrentContent(
		context.Background(), query.SearchString("matrix"), query.Limit(10),
	)
	require.NoError(t, err)

	assert.Equal(t, pg.result, result)
	assert.Equal(t, 1, tv.calls)
	assert.Equal(t, 1, pg.calls, "an anomalous empty page must run the full PostgreSQL search")
	require.Len(t, pg.options, 1)
	assert.Len(t, pg.options[0], 2, "fallback must receive the original full-search options")
}

func TestUnhealthySidecarKeepsTantivyModeOnPostgres(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "pg"))}
	tv := &fakeTantivy{resp: &pb.SearchResponse{}}
	r := newTestRouter(pg, tv, &spyObserver{}, Config{
		Mode:       ModeTantivy,
		SampleRate: 0,
	}, 0)
	r.health.SetHealthy(false, 100, 900, 1000)

	result, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
	require.NoError(t, err)

	assert.Equal(t, pg.result, result)
	assert.Equal(t, 1, pg.calls)
	assert.Zero(t, tv.calls)
}

func TestServeRPCErrorFailsClosedToPostgres(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "pg"))}
	tv := &fakeTantivy{err: errors.New("sidecar down")}
	r := newTestRouter(pg, tv, &spyObserver{}, Config{
		Mode:       ModeTantivy,
		SampleRate: 0,
	}, 0)
	r.health.SetHealthy(true, 100, 1000, 1000)

	result, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
	require.NoError(t, err)

	assert.Equal(t, pg.result, result)
	assert.Equal(t, 1, tv.calls)
	assert.Equal(t, 1, pg.calls)
}

func TestServeTimeoutFailsClosedToPostgres(t *testing.T) {
	t.Parallel()

	pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "pg"))}
	tv := newBlockingTantivy()
	r := newTestRouter(pg, tv, &spyObserver{}, Config{
		Mode:         ModeTantivy,
		SampleRate:   0,
		ServeTimeout: 10 * time.Millisecond,
	}, 0)
	r.health.SetHealthy(true, 100, 1000, 1000)

	result, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
	require.NoError(t, err)

	assert.Equal(t, pg.result, result)
	assert.Equal(t, int32(1), tv.calls.Load())
	assert.Equal(t, 1, pg.calls)
}

func TestHydrationErrorFailsClosedToFullPostgresSearch(t *testing.T) {
	t.Parallel()

	pgFallback := pgResult(pgItem(0xAA, "pg"))
	pg := &fakePGSearch{}
	pg.handler = func(context.Context, []query.Option) (search.TorrentContentResult, error) {
		if pg.calls == 1 {
			return search.TorrentContentResult{}, errors.New("hydrate failed")
		}

		return pgFallback, nil
	}
	tv := &fakeTantivy{resp: &pb.SearchResponse{
		Hits:      []*pb.SearchHit{tantivyHit(0xBB, "2")},
		TotalHits: 1,
	}}
	r := newTestRouter(pg, tv, &spyObserver{}, Config{
		Mode:       ModeTantivy,
		SampleRate: 0,
	}, 0)
	r.health.SetHealthy(true, 100, 1000, 1000)

	result, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
	require.NoError(t, err)

	assert.Equal(t, pgFallback, result)
	assert.Equal(t, 1, tv.calls)
	assert.Equal(t, 2, pg.calls, "failed hydration must be followed by the original full PG query")
}

func TestHealthyPostgresAndShadowModesNeverServe(t *testing.T) {
	t.Parallel()

	for _, mode := range []Mode{ModePostgres, ModeShadow} {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()

			pg := &fakePGSearch{result: pgResult(pgItem(0xAA, "pg"))}
			tv := &fakeTantivy{resp: &pb.SearchResponse{}}
			r := newTestRouter(pg, tv, &spyObserver{}, Config{
				Mode:       mode,
				SampleRate: 0,
			}, 0)
			r.health.SetHealthy(true, 100, 1000, 1000)

			result, err := r.TorrentContent(context.Background(), query.SearchString("matrix"))
			require.NoError(t, err)

			assert.Equal(t, pg.result, result)
			assert.Equal(t, 1, pg.calls)
			assert.Zero(t, tv.calls)
		})
	}
}

func TestCanaryServesOnlyBucketsBelowConfiguredPercent(t *testing.T) {
	t.Parallel()

	queryString := "sticky canary query"
	bucket := canaryBucket(queryString)

	insidePG := &fakePGSearch{result: pgResult(pgItem(0xAA, "pg"))}
	insideTV := &fakeTantivy{resp: &pb.SearchResponse{}}
	inside := newTestRouter(insidePG, insideTV, &spyObserver{}, Config{
		Mode:          ModeCanary,
		SampleRate:    0,
		CanaryPercent: bucket + 0.001,
	}, 0)
	inside.health.SetHealthy(true, 100, 1000, 1000)

	insideResult, err := inside.TorrentContent(context.Background(), query.SearchString(queryString))
	require.NoError(t, err)
	assert.Empty(t, insideResult.Items)
	assert.Equal(t, 1, insideTV.calls)
	assert.Zero(t, insidePG.calls)

	outsidePG := &fakePGSearch{result: pgResult(pgItem(0xBB, "pg"))}
	outsideTV := &fakeTantivy{resp: &pb.SearchResponse{}}
	outside := newTestRouter(outsidePG, outsideTV, &spyObserver{}, Config{
		Mode:          ModeCanary,
		SampleRate:    0,
		CanaryPercent: bucket,
	}, 0)
	outside.health.SetHealthy(true, 100, 1000, 1000)

	outsideResult, err := outside.TorrentContent(context.Background(), query.SearchString(queryString))
	require.NoError(t, err)
	assert.Equal(t, outsidePG.result, outsideResult)
	assert.Zero(t, outsideTV.calls)
	assert.Equal(t, 1, outsidePG.calls)
}

func TestSelectRankedItemsDropsUnrankedAndOrdersByRank(t *testing.T) {
	t.Parallel()

	first := pgItem(0xAA, "1")
	second := pgItem(0xBB, "2")
	sibling := pgItem(0xAA, "sibling")
	hydrated := []search.TorrentContentResultItem{first, sibling, second}

	items := selectRankedItems(hydrated, []string{second.InferID(), first.InferID()})

	assert.Equal(t, []string{
		second.InferID(), first.InferID(),
	}, extractPGIDs(pgResult(items...)))
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
