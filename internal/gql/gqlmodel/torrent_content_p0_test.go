package gqlmodel

import (
	"context"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/DATA-DOG/go-sqlmock"
	dao "github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	q "github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/gql/gqlmodel/gen"
	"github.com/bitmagnet-io/bitmagnet/internal/maps"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/pathsearch"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// newMockDaoQuery builds a *dao.Query backed by a sqlmock connection. The L3
// baseOptions reference dao field expressions (joins), so the option layer needs a
// real *dao.Query to resolve against; no SQL is executed (ResolveOptions only
// applies options, it never runs the query).
func newMockDaoQuery(t *testing.T) *dao.Query {
	t.Helper()

	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}

	t.Cleanup(func() { _ = mockDB.Close() })

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      mockDB,
		SkipInitializeWithVersion: true,
	}))
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	return dao.Use(db)
}

// P0-1: the L3 candidate IN(...) queries MUST NOT carry a page limit. Pre-fix
// torrentContentBaseOptions included q.DefaultOption() (Limit 10), so the
// candidate query was capped at 10 rows regardless of the candidate budget,
// truncating refine+paginate to <=10 torrents and emptying every page past the
// first. Resolving the options exposes the resolved limit the old fakes ignored.
// gate7-4 split the single set into Combined/Refine/Agg — NONE may impose a page
// limit.
func TestTorrentContentBaseOptions_NoPageLimit(t *testing.T) {
	daoQuery := newMockDaoQuery(t)

	input := TorrentContentSearchQueryInput{}
	orderBy := maps.NewInsertMap[search.TorrentContentOrderBy, search.OrderDirection]()

	opts := torrentContentQueryOptions(input, orderBy)

	for name, set := range map[string][]q.Option{
		"combined": opts.Combined,
		"refine":   opts.Refine,
		"agg":      opts.Agg,
	} {
		resolved, err := q.ResolveOptions(daoQuery, set...)
		if err != nil {
			t.Fatalf("ResolveOptions(%s): %v", name, err)
		}

		if resolved.Limit.Valid {
			t.Fatalf("L3 %s options must NOT impose a page limit, got Limit=%d; "+
				"DefaultOption's Limit(10) would cap the candidate IN-query (P0-1)", name, resolved.Limit.Uint)
		}
	}
}

// A facet's selected values are both an aggregation request and a membership
// predicate. The L3 refine must retain the predicate while suppressing the
// per-chunk aggregation; otherwise a contentType=tv_show request admits
// movie/null candidates and corrupts the served page before the one refined-set
// aggregation pass runs.
func TestTorrentContentQueryOptions_RefinePreservesFacetFiltersWithoutAggregation(t *testing.T) {
	daoQuery := newMockDaoQuery(t)
	aggregate := true
	tvShow := model.ContentTypeTvShow
	input := TorrentContentSearchQueryInput{
		Facets: &gen.TorrentContentFacetsInput{
			ContentType: graphql.OmittableOf(&gen.ContentTypeFacetInput{
				Aggregate: graphql.OmittableOf(&aggregate),
				Filter:    graphql.OmittableOf([]*model.ContentType{&tvShow}),
			}),
		},
	}
	opts := torrentContentQueryOptions(
		input,
		maps.NewInsertMap[search.TorrentContentOrderBy, search.OrderDirection](),
	)

	for name, wantAggregated := range map[string]bool{
		"combined": true,
		"refine":   false,
		"agg":      true,
	} {
		var optionSet []q.Option
		switch name {
		case "combined":
			optionSet = opts.Combined
		case "refine":
			optionSet = opts.Refine
		case "agg":
			optionSet = opts.Agg
		}

		resolved, err := q.ResolveOptions(daoQuery, optionSet...)
		if err != nil {
			t.Fatalf("ResolveOptions(%s): %v", name, err)
		}
		if len(resolved.Facets) != 1 {
			t.Fatalf("%s facets = %d, want 1 content-type predicate", name, len(resolved.Facets))
		}

		facet := resolved.Facets[0]
		if got := facet.IsAggregated(); got != wantAggregated {
			t.Errorf("%s content-type aggregate = %v, want %v", name, got, wantAggregated)
		}
		if !facet.Filter().HasKey(string(model.ContentTypeTvShow)) {
			t.Errorf("%s content-type facet lost tv_show filter: %#v", name, facet.Filter())
		}
		if facet.Logic() != model.FacetLogicOr {
			t.Errorf("%s content-type logic = %q, want OR", name, facet.Logic())
		}
	}
}

type facetAwareSearch struct {
	daoQuery    *dao.Query
	rows        []search.TorrentContentResultItem
	refineCalls int
	aggCalls    int
}

func (f *facetAwareSearch) TorrentContent(
	_ context.Context,
	options ...q.Option,
) (search.TorrentContentResult, error) {
	resolved, err := q.ResolveOptions(f.daoQuery, options...)
	if err != nil {
		return search.TorrentContentResult{}, err
	}

	var contentTypeFilter q.FacetFilter
	aggregated := false
	for _, facet := range resolved.Facets {
		if facet.Key() == search.TorrentContentTypeFacetKey {
			contentTypeFilter = facet.Filter()
		}
		aggregated = aggregated || facet.IsAggregated()
	}

	if aggregated {
		f.aggCalls++

		return search.TorrentContentResult{Aggregations: q.Aggregations{
			search.TorrentContentTypeFacetKey: {
				Items: q.AggregationItems{
					string(model.ContentTypeTvShow): {
						Label: string(model.ContentTypeTvShow),
						Count: 2,
					},
				},
			},
		}}, nil
	}

	f.refineCalls++
	items := make([]search.TorrentContentResultItem, 0, len(f.rows))
	for _, item := range f.rows {
		key := "null"
		if item.ContentType.Valid {
			key = item.ContentType.ContentType.String()
		}
		if len(contentTypeFilter) == 0 || contentTypeFilter.HasKey(key) {
			items = append(items, item)
		}
	}

	return search.TorrentContentResult{Items: items}, nil
}

func (*facetAwareSearch) FileCounts(
	_ context.Context,
	ids []protocol.ID,
) (map[protocol.ID]int, error) {
	counts := make(map[protocol.ID]int, len(ids))
	for _, id := range ids {
		counts[id] = 1
	}

	return counts, nil
}

func facetRouteItem(id protocol.ID, contentType *model.ContentType) search.TorrentContentResultItem {
	item := search.TorrentContentResultItem{TorrentContent: model.TorrentContent{
		InfoHash: id,
		Torrent: model.Torrent{
			InfoHash:    id,
			FilesStatus: model.FilesStatusMulti,
			Files: []model.TorrentFile{{
				Path: "Breaking Bad S01.mkv",
			}},
		},
	}}
	if contentType != nil {
		item.ContentType = model.NewNullContentType(*contentType)
	}

	return item
}

// End-to-end routed regression for the live idx11 failure: an out-of-filter
// movie and null candidate appear ahead of valid TV rows in L3 order. Refine
// must remove them, retain the order of the surviving TV rows, and run exactly
// one aggregation over the refined set. cached=true and FIND-2 are enabled to
// match production and prove they do not alter the served Lane-C order.
func TestSearch_PathsearchFacetFilterPreservesSurvivorOrder(t *testing.T) {
	previousFlags := search.FeatureFlagsValue()
	search.SetFeatureFlags(search.FeatureFlags{PopularitySortDefault: true})
	t.Cleanup(func() { search.SetFeatureFlags(previousFlags) })

	ids := make([]protocol.ID, 4)
	for i := range ids {
		ids[i][0] = byte(i + 1)
	}

	movie := model.ContentTypeMovie
	tvShow := model.ContentTypeTvShow
	pg := &facetAwareSearch{
		daoQuery: newMockDaoQuery(t),
		// Deliberately reverse PG-natural order; the composer must restore L3
		// order after the facet predicate removes ids 1 and 3.
		rows: []search.TorrentContentResultItem{
			facetRouteItem(ids[3], &tvShow),
			facetRouteItem(ids[2], nil),
			facetRouteItem(ids[1], &tvShow),
			facetRouteItem(ids[0], &movie),
		},
	}
	l3 := &recordingL3{resp: &pb.PathCandidatesResponse{
		Candidates: []*pb.PathCandidate{
			{InfoHash: ids[0].Bytes()},
			{InfoHash: ids[1].Bytes()},
			{InfoHash: ids[2].Bytes()},
			{InfoHash: ids[3].Bytes()},
		},
		CandidateTotal: 4,
	}}
	composer := pathsearch.NewComposer(l3, pg, pathsearch.ComposerConfig{
		TypeaheadEnabled: true,
		MinQueryLength:   3,
		OversampleFactor: 1,
		MaxCandidates:    100,
	}, nil)

	descending := true
	aggregate := true
	input := queryInput("breaking bad s01", gen.TorrentContentOrderByInput{
		Field:      gen.TorrentContentOrderByFieldRelevance,
		Descending: graphql.OmittableOf(&descending),
	})
	input.Cached = model.NewNullBool(true)
	input.Facets = &gen.TorrentContentFacetsInput{
		ContentType: graphql.OmittableOf(&gen.ContentTypeFacetInput{
			Aggregate: graphql.OmittableOf(&aggregate),
			Filter:    graphql.OmittableOf([]*model.ContentType{&tvShow}),
		}),
	}

	result, err := (TorrentContentQuery{
		TorrentContentSearch: pg,
		Pathsearch:           composer,
	}).Search(context.Background(), input)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Items) != 2 {
		t.Fatalf("filtered routed items = %d, want 2 tv_show rows", len(result.Items))
	}
	for i, want := range []protocol.ID{ids[1], ids[3]} {
		if got := result.Items[i].InfoHash; got != want {
			t.Errorf("filtered routed item %d = %s, want L3 survivor %s", i, got, want)
		}
		if got := result.Items[i].ContentType; !got.Valid || got.ContentType != model.ContentTypeTvShow {
			t.Errorf("filtered routed item %d content type = %+v, want tv_show", i, got)
		}
	}
	if pg.refineCalls != 1 || pg.aggCalls != 1 {
		t.Errorf("PG calls refine=%d agg=%d, want one filtered refine + one refined-set aggregation",
			pg.refineCalls, pg.aggCalls)
	}
	if got := result.Aggregations.ContentType; len(got) != 1 || got[0].Count != 2 {
		t.Errorf("refined content-type aggregation = %+v, want one tv_show bucket count=2", got)
	}
}

// P0-1 (contrast): the PostgreSQL path keeps DefaultOption's Limit(10) when the
// caller omits a limit — confirming the cap the L3 path must shed is real and that
// ResolveOptions observes it (i.e. the assertion above is not vacuous).
func TestDefaultOption_ImposesLimit10(t *testing.T) {
	daoQuery := newMockDaoQuery(t)

	resolved, err := q.ResolveOptions(daoQuery, q.DefaultOption())
	if err != nil {
		t.Fatalf("ResolveOptions: %v", err)
	}

	if !resolved.Limit.Valid || resolved.Limit.Uint != 10 {
		t.Fatalf(
			"DefaultOption must set Limit(10), got valid=%v uint=%d",
			resolved.Limit.Valid,
			resolved.Limit.Uint,
		)
	}
}

// P0-5: an omitted GraphQL limit must resolve to the same default page size the
// PostgreSQL path uses (DefaultOption's Limit(10)). Pre-fix searchPageLimit
// returned 0, which collapsed the candidate budget to ~OversampleFactor and made
// paginate(limit=0) serve that tiny set as a falsely-complete result.
func TestSearchPageLimit_OmittedUsesDefault(t *testing.T) {
	if got := searchPageLimit(q.SearchParams{}); got != defaultPageSize {
		t.Fatalf("omitted limit must resolve to defaultPageSize=%d, got %d (P0-5)", defaultPageSize, got)
	}

	if defaultPageSize != 10 {
		t.Fatalf("defaultPageSize must mirror DefaultOption's Limit(10), got %d", defaultPageSize)
	}

	if got := searchPageLimit(q.SearchParams{Limit: model.NewNullUint(50)}); got != 50 {
		t.Fatalf("explicit limit must be preserved, got %d", got)
	}
}

// Finding B (gate-7): the L3 route clamps the user-controlled page size to
// maxPathSearchLimit so a hostile `limit` can never size the per-request
// candidate blob-decode budget. Normal paging is unaffected; the PostgreSQL path
// (searchPageLimit) stays unclamped.
func TestPathSearchPageLimit_ClampsHostileLimit(t *testing.T) {
	// A huge attacker-controlled limit is clamped to the route maximum.
	if got := pathSearchPageLimit(q.SearchParams{Limit: model.NewNullUint(1_000_000)}); got != maxPathSearchLimit {
		t.Fatalf("hostile limit must clamp to maxPathSearchLimit=%d, got %d", maxPathSearchLimit, got)
	}

	// The exact Finding B repro (limit=3000) is clamped below the 2150 it served.
	if got := pathSearchPageLimit(q.SearchParams{Limit: model.NewNullUint(3000)}); got != maxPathSearchLimit {
		t.Fatalf("limit=3000 must clamp to maxPathSearchLimit=%d, got %d", maxPathSearchLimit, got)
	}

	// Normal UI paging is preserved unchanged.
	if got := pathSearchPageLimit(q.SearchParams{Limit: model.NewNullUint(50)}); got != 50 {
		t.Fatalf("in-range limit must be preserved, got %d", got)
	}

	// An omitted limit still resolves to the shared default (not the clamp).
	if got := pathSearchPageLimit(q.SearchParams{}); got != defaultPageSize {
		t.Fatalf("omitted limit must resolve to defaultPageSize=%d, got %d", defaultPageSize, got)
	}

	if maxPathSearchLimit > pathsearch.DefaultMaxCandidates {
		t.Fatalf("maxPathSearchLimit(%d) should not exceed the candidate cap (%d)",
			maxPathSearchLimit, pathsearch.DefaultMaxCandidates)
	}
}

// P0-2 (unit): the route order-eligibility predicate. Empty order (the webui
// default for a query == relevance) and an explicit relevance sort are eligible;
// any structured field is not.
func TestPathsearchOrderEligible(t *testing.T) {
	if !pathsearchOrderEligible(nil) {
		t.Fatal("empty order must be eligible (relevance default)")
	}

	if !pathsearchOrderEligible([]gen.TorrentContentOrderByInput{
		{Field: gen.TorrentContentOrderByFieldRelevance},
	}) {
		t.Fatal("explicit relevance order must be eligible")
	}

	for _, f := range []gen.TorrentContentOrderByField{
		gen.TorrentContentOrderByFieldSeeders,
		gen.TorrentContentOrderByFieldPublishedAt,
		gen.TorrentContentOrderByFieldSize,
		gen.TorrentContentOrderByFieldFilesCount,
	} {
		if pathsearchOrderEligible([]gen.TorrentContentOrderByInput{{Field: f}}) {
			t.Fatalf("structured sort %q must be ineligible for the L3 route (P0-2)", f)
		}
	}
}

// --- Search()-level routing fakes -------------------------------------------

// recordingL3 records whether the L3 candidate sidecar was consulted.
type recordingL3 struct {
	called bool
	resp   *pb.PathCandidatesResponse
}

func (r *recordingL3) PathCandidates(
	_ context.Context,
	_ *pb.PathCandidatesRequest,
) (*pb.PathCandidatesResponse, error) {
	r.called = true
	return r.resp, nil
}

func (*recordingL3) Suggest(
	context.Context,
	*pb.SuggestRequest,
) (*pb.SuggestResponse, error) {
	panic("unexpected Suggest call")
}

// recordingSearch records whether the PostgreSQL search path was consulted. It
// satisfies both search.TorrentContentSearch (the main path) and the composer's
// internal searcher (same method set).
type recordingSearch struct {
	called bool
}

func (r *recordingSearch) TorrentContent(
	_ context.Context,
	_ ...q.Option,
) (search.TorrentContentResult, error) {
	r.called = true
	return search.TorrentContentResult{}, nil
}

func (*recordingSearch) FileCounts(
	_ context.Context,
	_ []protocol.ID,
) (map[protocol.ID]int, error) {
	return map[protocol.ID]int{}, nil
}

func queryInput(query string, orderBy ...gen.TorrentContentOrderByInput) TorrentContentSearchQueryInput {
	return TorrentContentSearchQueryInput{
		SearchParams: q.SearchParams{
			QueryString: model.NewNullString(query),
			Limit:       model.NewNullUint(20),
		},
		OrderBy: orderBy,
	}
}

// P0-2 (integration): Search must route a relevance/default-ordered path query
// through L3, but bypass L3 for an explicit structured sort (which L3 cannot rank
// over its capped candidate sample) and serve it from PostgreSQL instead. Pre-fix
// Search took the L3 route regardless of OrderBy.
func TestSearch_StructuredSortBypassesL3(t *testing.T) {
	newQuery := func() (*recordingL3, *recordingSearch, TorrentContentQuery) {
		l3 := &recordingL3{resp: &pb.PathCandidatesResponse{}} // zero candidates -> healthy empty
		composerSearch := &recordingSearch{}
		composer := pathsearch.NewComposer(l3, composerSearch, pathsearch.ComposerConfig{
			TypeaheadEnabled: true,
			MinQueryLength:   3,
			OversampleFactor: 4,
			MaxCandidates:    1000,
		}, nil)

		mainSearch := &recordingSearch{}

		return l3, mainSearch, TorrentContentQuery{
			TorrentContentSearch: mainSearch,
			Pathsearch:           composer,
		}
	}

	// Relevance / default order -> L3 route taken.
	t.Run("relevance routes to L3", func(t *testing.T) {
		l3, mainSearch, tcq := newQuery()

		if _, err := tcq.Search(context.Background(), queryInput("inception")); err != nil {
			t.Fatalf("Search: %v", err)
		}

		if !l3.called {
			t.Fatal("relevance/default-ordered path query must consult L3")
		}

		if mainSearch.called {
			t.Fatal("relevance route served by L3 must not also hit the main PG search")
		}
	})

	// Explicit structured sort -> L3 bypassed, PG serves.
	t.Run("seeders sort bypasses L3", func(t *testing.T) {
		l3, mainSearch, tcq := newQuery()

		desc := true
		input := queryInput("inception", gen.TorrentContentOrderByInput{
			Field:      gen.TorrentContentOrderByFieldSeeders,
			Descending: graphql.OmittableOf(&desc),
		})

		if _, err := tcq.Search(context.Background(), input); err != nil {
			t.Fatalf("Search: %v", err)
		}

		if l3.called {
			t.Fatal("explicit structured sort must NOT consult L3 (recall order != global top-N) (P0-2)")
		}

		if !mainSearch.called {
			t.Fatal("structured sort must be served by the PostgreSQL path")
		}
	})
}
