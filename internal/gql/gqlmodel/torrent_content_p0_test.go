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

// P0-1: the L3 candidate IN(...) query MUST NOT carry a page limit. Pre-fix
// torrentContentBaseOptions included q.DefaultOption() (Limit 10), so the
// candidate query was capped at 10 rows regardless of the candidate budget,
// truncating refine+paginate to <=10 torrents and emptying every page past the
// first. Resolving the options exposes the resolved limit the old fakes ignored.
func TestTorrentContentBaseOptions_NoPageLimit(t *testing.T) {
	daoQuery := newMockDaoQuery(t)

	input := TorrentContentSearchQueryInput{}
	orderBy := maps.NewInsertMap[search.TorrentContentOrderBy, search.OrderDirection]()

	resolved, err := q.ResolveOptions(daoQuery, torrentContentBaseOptions(input, orderBy)...)
	if err != nil {
		t.Fatalf("ResolveOptions: %v", err)
	}

	if resolved.Limit.Valid {
		t.Fatalf("L3 baseOptions must NOT impose a page limit, got Limit=%d; "+
			"DefaultOption's Limit(10) would cap the candidate IN-query (P0-1)", resolved.Limit.Uint)
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
		t.Fatalf("DefaultOption must set Limit(10), got valid=%v uint=%d", resolved.Limit.Valid, resolved.Limit.Uint)
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
