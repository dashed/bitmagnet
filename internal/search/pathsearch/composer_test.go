package pathsearch

import (
	"context"
	"errors"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
)

// --- fakes -------------------------------------------------------------------

type fakeL3 struct {
	resp     *pb.PathCandidatesResponse
	err      error
	gotLimit uint32
	gotQuery string
}

func (f *fakeL3) PathCandidates(_ context.Context, req *pb.PathCandidatesRequest) (*pb.PathCandidatesResponse, error) {
	f.gotLimit = req.GetLimit()
	f.gotQuery = req.GetQuery()

	return f.resp, f.err
}

type fakePG struct {
	result    search.TorrentContentResult
	err       error
	gotOpts   int
	callCount int
}

func (f *fakePG) TorrentContent(_ context.Context, options ...query.Option) (search.TorrentContentResult, error) {
	f.gotOpts = len(options)
	f.callCount++

	return f.result, f.err
}

func ih(b byte) protocol.ID {
	var id protocol.ID
	id[0] = b

	return id
}

func candidate(b byte) *pb.PathCandidate {
	id := ih(b)

	return &pb.PathCandidate{InfoHash: id.Bytes()}
}

func item(b byte, files ...model.TorrentFile) search.TorrentContentResultItem {
	return search.TorrentContentResultItem{
		TorrentContent: model.TorrentContent{
			InfoHash: ih(b),
			Torrent: model.Torrent{
				InfoHash:    ih(b),
				FilesStatus: model.FilesStatusMulti,
				Files:       files,
			},
		},
	}
}

func newTestComposer(l3 candidateSource, pg torrentContentSearcher) *Composer {
	return NewComposer(l3, pg, ComposerConfig{
		MinQueryLength:   3,
		OversampleFactor: 4,
		MaxCandidates:    1000,
	}, nil)
}

// --- tests -------------------------------------------------------------------

func TestComposer_TorrentContent_RefinesAndPaginates(t *testing.T) {
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{
		Candidates:     []*pb.PathCandidate{candidate(1), candidate(2), candidate(3), candidate(4)},
		CandidateTotal: 4,
		Estimated:      true,
	}}
	// PG returns the candidate set in order; item 2 is an L3 false positive.
	pg := &fakePG{result: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		item(1, tf("Inception.2010.1080p.mkv", "mkv", 1)),
		item(2, tf("Independence.Day.mkv", "mkv", 2)), // false positive
		item(3, tf("Inception.2010.2160p.mkv", "mkv", 3)),
		item(4, tf("Inception.Soundtrack.flac", "flac", 4)),
	}}}

	c := newTestComposer(l3, pg)

	res, served, err := c.TorrentContent(context.Background(), Filters{Query: "inception"}, nil, 2, 0, nil)
	if err != nil || !served {
		t.Fatalf("expected served result, got served=%v err=%v", served, err)
	}

	if res.TotalCount != 3 || !res.TotalCountIsEstimate {
		t.Fatalf("expected estimated TotalCount=3 (false positive dropped), got %d estimate=%v",
			res.TotalCount, res.TotalCountIsEstimate)
	}

	if len(res.Items) != 2 || res.Items[0].InfoHash != ih(1) || res.Items[1].InfoHash != ih(3) {
		t.Fatalf("page must be [1,3] (false positive 2 dropped, not occupying a slot), got %d items", len(res.Items))
	}

	if !res.HasNextPage {
		t.Fatal("expected HasNextPage (refined=3 > offset+limit=2)")
	}
}

func TestComposer_TorrentContent_IneligibleShortQueryFallsBack(t *testing.T) {
	c := newTestComposer(&fakeL3{}, &fakePG{})

	_, served, err := c.TorrentContent(context.Background(), Filters{Query: "ab"}, nil, 10, 0, nil)
	if err != nil || served {
		t.Fatalf("short query must fall back (served=false), got served=%v err=%v", served, err)
	}
}

func TestComposer_TorrentContent_EmptyQueryFallsBack(t *testing.T) {
	c := newTestComposer(&fakeL3{}, &fakePG{})

	_, served, _ := c.TorrentContent(context.Background(), Filters{Query: "   "}, nil, 10, 0, nil)
	if served {
		t.Fatal("empty/whitespace query must fall back (served=false)")
	}
}

func TestComposer_TorrentContent_NoCandidatesIsEstimatedEmpty(t *testing.T) {
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{}}
	pg := &fakePG{}

	c := newTestComposer(l3, pg)

	res, served, err := c.TorrentContent(context.Background(), Filters{Query: "nomatch"}, nil, 10, 0, nil)
	if err != nil || !served {
		t.Fatalf("no candidates is a served (estimated empty) result, got served=%v err=%v", served, err)
	}

	if len(res.Items) != 0 || !res.TotalCountIsEstimate {
		t.Fatalf("expected empty estimated result, got %d items estimate=%v", len(res.Items), res.TotalCountIsEstimate)
	}

	if pg.callCount != 0 {
		t.Fatal("no candidates must short-circuit before hitting PG")
	}
}

// CAVEAT B at the composer level: a candidate whose files are unobtainable makes
// the whole route fail loud and fall back to PG (served=false), never a
// truncated result.
func TestComposer_TorrentContent_FailLoudFallback(t *testing.T) {
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: []*pb.PathCandidate{candidate(1)}}}
	pg := &fakePG{result: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		// multi-file, no Files, no FilesData -> unrefinable
		{TorrentContent: model.TorrentContent{InfoHash: ih(1), Torrent: model.Torrent{FilesStatus: model.FilesStatusMulti}}},
	}}}

	c := newTestComposer(l3, pg)

	_, served, err := c.TorrentContent(context.Background(), Filters{Query: "inception"}, nil, 10, 0, nil)
	if err != nil || served {
		t.Fatalf("unrefinable candidate must fail loud (served=false), got served=%v err=%v", served, err)
	}
}

func TestComposer_TorrentContent_L3ErrorPropagates(t *testing.T) {
	l3 := &fakeL3{err: errors.New("sidecar down")}
	c := newTestComposer(l3, &fakePG{})

	_, served, err := c.TorrentContent(context.Background(), Filters{Query: "inception"}, nil, 10, 0, nil)
	if err == nil || served {
		t.Fatalf("L3 error must propagate (served=false, err!=nil), got served=%v err=%v", served, err)
	}
}

func TestComposer_CandidateBudget(t *testing.T) {
	c := NewComposer(nil, nil, ComposerConfig{OversampleFactor: 4, MaxCandidates: 100}, nil)

	if got := c.candidateBudget(10, 0); got != 40 {
		t.Errorf("budget(limit=10,offset=0) = %d, want 40 (10*4)", got)
	}

	if got := c.candidateBudget(10, 20); got != 100 {
		t.Errorf("budget(limit=10,offset=20) = %d, want 100 (capped from 120)", got)
	}

	// OversampleFactor < 1 is normalized to 1.
	c2 := NewComposer(nil, nil, ComposerConfig{OversampleFactor: 0}, nil)
	if got := c2.candidateBudget(5, 0); got != 5 {
		t.Errorf("budget with factor<1 = %d, want 5 (factor normalized to 1)", got)
	}
}

func TestComposer_BudgetSentToL3(t *testing.T) {
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{}}
	c := newTestComposer(l3, &fakePG{})

	_, _, _ = c.TorrentContent(context.Background(), Filters{Query: "inception"}, nil, 10, 5, nil)

	// budget = (offset+limit)*factor = 15*4 = 60, under the 1000 cap.
	if l3.gotLimit != 60 {
		t.Fatalf("L3 request limit = %d, want 60 (oversampled budget)", l3.gotLimit)
	}

	if l3.gotQuery != "inception" {
		t.Fatalf("L3 query = %q, want inception", l3.gotQuery)
	}
}

// P0-3: a zero-candidate response is an authoritative empty ONLY when L3 is
// healthy. With a health gate reporting unhealthy, the route must fall back to PG
// (served=false) rather than serve a false "no results" that could mask a torrent
// PostgreSQL still has (mid-backfill / lagging / down). Pre-fix the composer had
// no gate and always returned served=true on zero candidates.
func TestComposer_TorrentContent_ZeroCandidates_UnhealthyFallsBack(t *testing.T) {
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{}} // zero candidates
	pg := &fakePG{}

	c := NewComposer(l3, pg, ComposerConfig{MinQueryLength: 3, OversampleFactor: 4, MaxCandidates: 1000}, nil,
		WithHealthGate(func() bool { return false }))

	_, served, err := c.TorrentContent(context.Background(), Filters{Query: "nomatch"}, nil, 10, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if served {
		t.Fatal("zero candidates + unhealthy L3 must fall back (served=false), not serve a false empty (P0-3)")
	}

	if pg.callCount != 0 {
		t.Fatal("must fall back BEFORE hitting the candidate PG query (the GraphQL layer runs the plain PG path)")
	}
}

// P0-3: with a healthy gate, a zero-candidate response is trusted as an exact
// empty (served=true) — the gate must not regress the authoritative-empty
// behaviour when L3 is healthy.
func TestComposer_TorrentContent_ZeroCandidates_HealthyServesEmpty(t *testing.T) {
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{}}
	pg := &fakePG{}

	c := NewComposer(l3, pg, ComposerConfig{MinQueryLength: 3, OversampleFactor: 4, MaxCandidates: 1000}, nil,
		WithHealthGate(func() bool { return true }))

	res, served, err := c.TorrentContent(context.Background(), Filters{Query: "nomatch"}, nil, 10, 0, nil)
	if err != nil || !served {
		t.Fatalf("zero candidates + healthy L3 is a served (estimated empty) result, got served=%v err=%v", served, err)
	}

	if len(res.Items) != 0 || !res.TotalCountIsEstimate {
		t.Fatalf("expected empty estimated result, got %d items estimate=%v", len(res.Items), res.TotalCountIsEstimate)
	}
}

func TestComposer_CollapsePaths(t *testing.T) {
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: []*pb.PathCandidate{candidate(1), candidate(2)}}}
	pg := &fakePG{result: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		item(1, tf("a/Movie.mkv", "mkv", 1), tf("a/sample.txt", "txt", 1)),
		item(2, tf("b/movie.mkv", "mkv", 2)),
	}}}

	c := newTestComposer(l3, pg)

	groups, served, err := c.CollapsePaths(context.Background(), Filters{Query: "movie"}, nil, 10, 0, nil)
	if err != nil || !served {
		t.Fatalf("expected served collapse, got served=%v err=%v", served, err)
	}

	if len(groups) != 2 {
		t.Fatalf("expected 2 distinct matched paths, got %d", len(groups))
	}

	if groups[0].Path != "a/Movie.mkv" || len(groups[0].InfoHashes) != 1 || groups[0].InfoHashes[0] != ih(1) {
		t.Fatalf("group 0 mismatch: %+v", groups[0])
	}

	if groups[1].Path != "b/movie.mkv" || groups[1].InfoHashes[0] != ih(2) {
		t.Fatalf("group 1 mismatch: %+v", groups[1])
	}
}
