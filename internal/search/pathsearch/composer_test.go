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
	resp         *pb.PathCandidatesResponse
	err          error
	gotLimit     uint32
	gotQuery     string
	suggestResp  *pb.SuggestResponse
	suggestErr   error
	suggestReq   *pb.SuggestRequest
	suggestCalls int
}

func (f *fakeL3) PathCandidates(_ context.Context, req *pb.PathCandidatesRequest) (*pb.PathCandidatesResponse, error) {
	f.gotLimit = req.GetLimit()
	f.gotQuery = req.GetQuery()

	return f.resp, f.err
}

func (f *fakeL3) Suggest(_ context.Context, req *pb.SuggestRequest) (*pb.SuggestResponse, error) {
	f.suggestCalls++
	f.suggestReq = req

	return f.suggestResp, f.suggestErr
}

type fakePG struct {
	result    search.TorrentContentResult
	err       error
	gotOpts   int
	callCount int
	// fcErr, if set, is returned by FileCounts (to exercise the pre-decode probe
	// error path).
	fcErr error
	// fileCounts, if non-nil, is the per-id count returned by FileCounts. Any
	// requested id absent from it (or a nil map) defaults to count 1 — so the whole
	// candidate set fits a single refine chunk (the common fast path), keeping the
	// pre-existing tests exercising today's single-query behavior.
	fileCounts map[protocol.ID]int
	// strictCounts, when true, returns ONLY ids present in fileCounts (absent ids
	// are OMITTED → "unknown", exercising the post-decode guard / cap budgeting).
	strictCounts bool
}

func (f *fakePG) TorrentContent(_ context.Context, options ...query.Option) (search.TorrentContentResult, error) {
	f.gotOpts = len(options)
	f.callCount++

	return f.result, f.err
}

func (f *fakePG) FileCounts(_ context.Context, ids []protocol.ID) (map[protocol.ID]int, error) {
	if f.fcErr != nil {
		return nil, f.fcErr
	}

	out := make(map[protocol.ID]int, len(ids))

	for _, id := range ids {
		if f.fileCounts != nil {
			if n, ok := f.fileCounts[id]; ok {
				out[id] = n
				continue
			}
		}

		if f.strictCounts {
			continue // omit → unknown
		}

		out[id] = 1
	}

	return out, nil
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

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		2,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("expected served result, got served=%v err=%v", served, err)
	}

	if res.TotalCount != 3 || !res.TotalCountIsEstimate {
		t.Fatalf("expected estimated TotalCount=3 (false positive dropped), got %d estimate=%v",
			res.TotalCount, res.TotalCountIsEstimate)
	}

	if len(res.Items) != 2 || res.Items[0].InfoHash != ih(1) || res.Items[1].InfoHash != ih(3) {
		t.Fatalf(
			"page must be [1,3] (false positive 2 dropped, not occupying a slot), got %d items",
			len(res.Items),
		)
	}

	if !res.HasNextPage {
		t.Fatal("expected HasNextPage (refined=3 > offset+limit=2)")
	}
}

// TestComposer_TorrentContent_TotalCountUsesCandidateTotalWhenTruncated is the
// #10 follow-up: when the candidate window is budget-truncated, TotalCount must
// serve the sidecar's full candidate_total (upper-bound estimate) instead of the
// badly-low refined-window count ("1080p" with millions of matches showed ~200).
// The untruncated case (TotalCount == refined count) is covered by
// TestComposer_TorrentContent_RefinesAndPaginates.
func TestComposer_TorrentContent_TotalCountUsesCandidateTotalWhenTruncated(t *testing.T) {
	const sidecarTotal = 5000

	// limit=2 * oversample(4) = budget 8; the sidecar returns 10 (its own +200
	// floor behavior in miniature) with a full-corpus candidate_total of 5000.
	cands := make([]*pb.PathCandidate, 0, 10)
	items := make([]search.TorrentContentResultItem, 0, 10)

	for i := 1; i <= 10; i++ {
		cands = append(cands, candidate(byte(i)))
		items = append(items, item(byte(i), tf("Inception.2010.1080p.mkv", "mkv", uint(i))))
	}

	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{
		Candidates:     cands,
		CandidateTotal: sidecarTotal,
		Estimated:      true,
	}}
	pg := &fakePG{result: search.TorrentContentResult{Items: items}}

	c := newTestComposer(l3, pg)

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		2,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("expected served result, got served=%v err=%v", served, err)
	}

	if res.TotalCount != sidecarTotal {
		t.Fatalf("truncated window must serve candidate_total=%d as TotalCount, got %d",
			sidecarTotal, res.TotalCount)
	}

	if !res.TotalCountIsEstimate {
		t.Fatal("TotalCountIsEstimate must stay true on the L3 route")
	}

	if len(res.Items) != 2 {
		t.Fatalf("page size must be unaffected by the count change, got %d items", len(res.Items))
	}
}

func TestComposer_TorrentContent_IneligibleShortQueryFallsBack(t *testing.T) {
	c := newTestComposer(&fakeL3{}, &fakePG{})

	_, served, err := c.TorrentContent(context.Background(), Filters{Query: "ab"}, QueryOptions{}, 10, 0, nil)
	if err != nil || served {
		t.Fatalf("short query must fall back (served=false), got served=%v err=%v", served, err)
	}
}

func TestComposer_TorrentContent_EmptyQueryFallsBack(t *testing.T) {
	c := newTestComposer(&fakeL3{}, &fakePG{})

	_, served, _ := c.TorrentContent(context.Background(), Filters{Query: "   "}, QueryOptions{}, 10, 0, nil)
	if served {
		t.Fatal("empty/whitespace query must fall back (served=false)")
	}
}

func TestComposer_TorrentContent_NoCandidatesIsEstimatedEmpty(t *testing.T) {
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{}}
	pg := &fakePG{}

	c := newTestComposer(l3, pg)

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "nomatch"},
		QueryOptions{},
		10,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("no candidates is a served (estimated empty) result, got served=%v err=%v", served, err)
	}

	if len(res.Items) != 0 || !res.TotalCountIsEstimate {
		t.Fatalf(
			"expected empty estimated result, got %d items estimate=%v",
			len(res.Items),
			res.TotalCountIsEstimate,
		)
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
		{
			TorrentContent: model.TorrentContent{
				InfoHash: ih(1),
				Torrent:  model.Torrent{InfoHash: ih(1), FilesStatus: model.FilesStatusMulti},
			},
		},
	}}}

	c := newTestComposer(l3, pg)

	_, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		10,
		0,
		nil,
	)
	if err != nil || served {
		t.Fatalf("unrefinable candidate must fail loud (served=false), got served=%v err=%v", served, err)
	}
}

func TestComposer_TorrentContent_L3ErrorPropagates(t *testing.T) {
	l3 := &fakeL3{err: errors.New("sidecar down")}
	c := newTestComposer(l3, &fakePG{})

	_, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		10,
		0,
		nil,
	)
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

// TestComposer_CandidateBudget_HardBounded asserts the candidate-budget COUNT is
// always bounded — a hostile/huge user limit, a deep offset, an unsigned
// overflow, and even a 0/unset MaxCandidates can never size an arbitrarily large
// budget. This is the REQUEST/decode-count bound; the truncation that makes the
// ACTUAL decode count obey it (the sidecar adds +200 oversample on top of Limit)
// is covered by TestComposer_Candidates_TruncatesToBudget.
func TestComposer_CandidateBudget_HardBounded(t *testing.T) {
	// A configured cap is honored no matter how large the requested window is.
	capped := NewComposer(nil, nil, ComposerConfig{OversampleFactor: 4, MaxCandidates: 2000}, nil)
	for _, tc := range []struct{ limit, offset uint }{
		{3000, 0},          // the exact Finding B repro (limit=3000 served 2150)
		{1 << 20, 0},       // absurd page size
		{50, 1 << 20},      // deep offset
		{1 << 40, 1 << 40}, // forces uint overflow in need*OversampleFactor
	} {
		if got := capped.candidateBudget(tc.limit, tc.offset); got > 2000 {
			t.Errorf(
				"budget(limit=%d,offset=%d) = %d, want <= MaxCandidates(2000)",
				tc.limit,
				tc.offset,
				got,
			)
		}
	}

	// MaxCandidates == 0 must NOT mean "unbounded" — it falls back to the hard
	// DefaultMaxCandidates so a misconfigured/zero cap is still memory-safe. Pin the
	// latency cap high here so this asserts the MEMORY fallback in isolation (the
	// latency cap's own default is exercised by TestComposer_CandidateBudget_DecodeLatencyCap).
	zeroCap := NewComposer(nil, nil, ComposerConfig{OversampleFactor: 4, MaxCandidates: 0, MaxDecodeCandidates: DefaultMaxCandidates}, nil)
	if got := zeroCap.candidateBudget(1<<20, 0); got != DefaultMaxCandidates {
		t.Errorf(
			"budget with MaxCandidates=0 = %d, want DefaultMaxCandidates(%d) — 0 must not be unbounded",
			got,
			DefaultMaxCandidates,
		)
	}
}

// TestComposer_CandidateBudget_DecodeLatencyCap asserts the LATENCY cap
// (MaxDecodeCandidates) bounds the decode COUNT independently of the memory cap,
// that a 0/unset value falls back to the safe default (never "no cap"), and that
// it leaves normal page-1 sizes (<=50 -> <=200 decodes) unchanged while bounding
// larger pages. This is the fix for the limit-100 relevance-search latency
// (candidateBudget grew as (offset+limit)*Oversample -> ~5.3s at limit 100).
func TestComposer_CandidateBudget_DecodeLatencyCap(t *testing.T) {
	// Unset -> DefaultMaxDecodeCandidates, and it dominates the large memory cap.
	def := NewComposer(nil, nil, ComposerConfig{OversampleFactor: 4, MaxCandidates: 2000}, nil)

	// The requested-page-size boundaries: 20/50 unchanged, 100/200 bounded to 200.
	for _, tc := range []struct {
		limit, offset, want uint
	}{
		{20, 0, 80},   // 20*4=80, under the cap (unchanged)
		{50, 0, 200},  // 50*4=200, exactly at the cap (unchanged)
		{100, 0, 200}, // 100*4=400 -> capped (the limit-100 fix: ~5.3s -> ~2s)
		{200, 0, 200}, // 200*4=800 -> capped (also the maxPathSearchLimit ceiling)
		// Offset interplay: candidateBudget covers offset+limit, so a deep page of a
		// SMALL limit still crosses the cap. Page 2 of 20 (offset 20) stays under;
		// page 3 (offset 40) crosses it.
		{20, 20, 160}, // need=40 -> 160, under the cap (unchanged)
		{20, 40, 200}, // need=60 -> 240 -> capped
		{20, 0, 80},   // page 1 of the same limit is unaffected (regression guard)
	} {
		if got := def.candidateBudget(tc.limit, tc.offset); got != tc.want {
			t.Errorf("budget(limit=%d, offset=%d) = %d, want %d", tc.limit, tc.offset, got, tc.want)
		}
	}

	// An explicit cap is honored and can bind below the memory cap FOR SHALLOW pages.
	tuned := NewComposer(nil, nil, ComposerConfig{OversampleFactor: 4, MaxCandidates: 2000, MaxDecodeCandidates: 120}, nil)
	if got := tuned.candidateBudget(100, 0); got != 120 {
		t.Errorf("budget(limit=100, decodeCap=120) = %d, want 120", got)
	}

	// F5: an absurd window is still bounded, but now by the MEMORY ceiling
	// (MaxCandidates) — the latency floor no longer caps deep windows, so deep
	// pagination can be served. Never "unbounded".
	if got := def.candidateBudget(1<<20, 0); got != DefaultMaxCandidates {
		t.Errorf("budget(huge window) = %d, want DefaultMaxCandidates(%d) — deep windows grow to the memory ceiling", got, DefaultMaxCandidates)
	}
}

// TestComposer_CandidateBudget_DeepPaginationGrows is the F5 core: the decode
// latency floor (MaxDecodeCandidates=200) applies ONLY to shallow pages, so a DEEP
// offset grows its decode window past 200 (up to MaxCandidates) — otherwise the
// page could never be served (the window would stop at 200 < offset). Page 1 and
// other shallow windows stay byte-identical to before.
func TestComposer_CandidateBudget_DeepPaginationGrows(t *testing.T) {
	c := NewComposer(nil, nil, ComposerConfig{OversampleFactor: 4, MaxCandidates: 2000}, nil)

	for _, tc := range []struct {
		name                string
		limit, offset, want uint
	}{
		// Shallow: unchanged (need <= MaxDecodeCandidates=200 keeps the 200 floor).
		{"page1 limit 50", 50, 0, 200},
		{"page1 limit 100 capped", 100, 0, 200},
		{"shallow offset 40", 20, 40, 200},
		// Deep: need > 200 lifts the floor and grows toward the memory ceiling.
		{"deep offset 400", 50, 400, 1800},    // need=450 -> 450*4=1800, under 2000
		{"deep offset 1000", 50, 1000, 2000},  // need=1050 -> 4200 -> capped to 2000
		{"deep small limit", 20, 400, 1680},   // need=420 -> 1680, servable (was 200)
	} {
		if got := c.candidateBudget(tc.limit, tc.offset); got != tc.want {
			t.Errorf("%s: budget(limit=%d, offset=%d) = %d, want %d", tc.name, tc.limit, tc.offset, got, tc.want)
		}
	}
}

// TestComposer_Candidates_TruncatesToBudget is the gate-7 Finding B regression:
// the sidecar treats the request Limit as a FLOOR and returns budget+oversample
// candidates (live: ~budget+200), so the composer MUST truncate to budget before
// the PG requery + blob decode, or it decodes the full over-large set and OOMs.
// Asserts the ACTUAL decoded candidate count == budget, and the request Limit
// sent to the sidecar == budget.
func TestComposer_Candidates_TruncatesToBudget(t *testing.T) {
	const (
		oversample uint = 4
		maxCands   uint = 1000
		limit      uint = 300 // need=300, 300*4=1200 -> capped to maxCands(1000)
	)

	wantBudget := maxCands // 1200 capped to 1000

	// Sidecar mirrors bitmagnet-rs: returns Limit + DEFAULT_OVERSAMPLE(200).
	const sidecarOversample = 200

	cands := make([]*pb.PathCandidate, 0, int(wantBudget)+sidecarOversample)
	for i := range int(wantBudget) + sidecarOversample {
		cands = append(cands, candidate(byte(i%256)))
	}

	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: cands, CandidateTotal: uint64(len(cands))}}
	// Pin the latency cap == memory cap so this test isolates the MEMORY-cap
	// truncation (the latency cap's own binding is covered by
	// TestComposer_CandidateBudget_DecodeLatencyCap).
	c := NewComposer(l3, nil, ComposerConfig{OversampleFactor: oversample, MaxCandidates: maxCands, MaxDecodeCandidates: maxCands}, nil)

	ids, gotTotal, err := c.candidates(context.Background(), Filters{Query: "matrix"}, limit, 0, nil)
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}

	if uint(l3.gotLimit) != wantBudget {
		t.Errorf("sidecar request Limit = %d, want budget %d", l3.gotLimit, wantBudget)
	}

	// The decoded set MUST be truncated to budget even though the sidecar returned
	// budget+200. (Distinct info_hashes; the helper cycles bytes so we compare the
	// returned slice length to the truncation bound, which is what gets decoded.)
	if uint(len(ids)) > wantBudget {
		t.Errorf(
			"decoded candidate count = %d, want <= budget %d (sidecar +200 not truncated → OOM)",
			len(ids),
			wantBudget,
		)
	}

	// candidate_total must survive the truncation untouched — it is the sidecar's
	// FULL match count, not the decoded-window size (#10 follow-up).
	if gotTotal != uint(len(cands)) {
		t.Errorf("candidateTotal = %d, want sidecar candidate_total %d", gotTotal, len(cands))
	}
}

func TestComposer_BudgetSentToL3(t *testing.T) {
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{}}
	c := newTestComposer(l3, &fakePG{})

	_, _, _ = c.TorrentContent(context.Background(), Filters{Query: "inception"}, QueryOptions{}, 10, 5, nil)

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

	_, served, err := c.TorrentContent(context.Background(), Filters{Query: "nomatch"}, QueryOptions{}, 10, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if served {
		t.Fatal("zero candidates + unhealthy L3 must fall back (served=false), not serve a false empty (P0-3)")
	}

	if pg.callCount != 0 {
		t.Fatal(
			"must fall back BEFORE hitting the candidate PG query (the GraphQL layer runs the plain PG path)",
		)
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

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "nomatch"},
		QueryOptions{},
		10,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf(
			"zero candidates + healthy L3 is a served (estimated empty) result, got served=%v err=%v",
			served,
			err,
		)
	}

	if len(res.Items) != 0 || !res.TotalCountIsEstimate {
		t.Fatalf(
			"expected empty estimated result, got %d items estimate=%v",
			len(res.Items),
			res.TotalCountIsEstimate,
		)
	}
}

// #6 — facets/aggregations PG computed for the candidate set must pass through the
// hand-built result. Pre-fix Composer.TorrentContent never copied
// pgResult.Aggregations, so the UI facet sidebar blanked on path searches.
func TestComposer_TorrentContent_PassesThroughAggregations(t *testing.T) {
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: []*pb.PathCandidate{candidate(1)}}}

	aggs := query.Aggregations{
		"content_type": query.AggregationGroup{Items: query.AggregationItems{
			"movie": query.AggregationItem{Label: "movie", Count: 7},
		}},
	}
	pg := &fakePG{result: search.TorrentContentResult{
		Items:        []search.TorrentContentResultItem{item(1, tf("Inception.2010.1080p.mkv", "mkv", 1))},
		Aggregations: aggs,
	}}

	c := newTestComposer(l3, pg)

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		10,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("expected served, got served=%v err=%v", served, err)
	}

	grp, ok := res.Aggregations["content_type"]
	if !ok || len(grp.Items) != 1 || grp.Items["movie"].Count != 7 {
		t.Fatalf("aggregations must pass through to the served result, got %+v (#6)", res.Aggregations)
	}
}

// gate7-8 (a) — candidates>0 but the Go exact-refine drops them ALL. The
// candidate-set facets PG would compute are PHANTOM counts for a result that serves
// zero items (the "lehman brothers trilogy" symptom: totalCount=0, items=[], yet
// contentType=[audiobook 2, ebook 18]). Facets must be recomputed over the REFINED
// (here empty) set → empty aggregations, NEVER the candidate-set phantoms.
func TestComposer_TorrentContent_AllRefinedOut_EmptyAggregations(t *testing.T) {
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: []*pb.PathCandidate{candidate(1), candidate(2)}}}
	phantom := query.Aggregations{
		"content_type": query.AggregationGroup{Items: query.AggregationItems{
			"audiobook": query.AggregationItem{Label: "audiobook", Count: 2},
			"ebook":     query.AggregationItem{Label: "ebook", Count: 18},
		}},
	}
	// Both candidate file paths are L3 false positives (no "inception"), so refine
	// drops them all. The fake would return `phantom` on ANY query — the empty-refined
	// branch must NOT issue the aggregation query (empty IN set) and must NOT leak it.
	pg := &fakePG{result: search.TorrentContentResult{
		Items: []search.TorrentContentResultItem{
			item(1, tf("Independence.Day.mkv", "mkv", 1)),
			item(2, tf("Some.Other.Movie.mkv", "mkv", 2)),
		},
		Aggregations: phantom,
	}}

	c := newTestComposer(l3, pg)

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		10,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("expected served, got served=%v err=%v", served, err)
	}

	if res.TotalCount != 0 || len(res.Items) != 0 {
		t.Fatalf(
			"all candidates refined out → empty result, got TotalCount=%d items=%d",
			res.TotalCount,
			len(res.Items),
		)
	}

	if len(res.Aggregations) != 0 {
		t.Fatalf(
			"empty result must carry EMPTY facets (no candidate-set phantom counts), got %+v (gate7-8)",
			res.Aggregations,
		)
	}
}

// gate7-8 (b) — when N>0 candidates survive refine, the facets are recomputed over
// the REFINED set, so the per-facet counts reconcile with the served item count:
// sum(facet) == N. (The fake returns the refined-set facets for the aggregation
// pass.)
func TestComposer_TorrentContent_RefinedAggregationsReconcile(t *testing.T) {
	l3 := &fakeL3{
		resp: &pb.PathCandidatesResponse{
			Candidates: []*pb.PathCandidate{candidate(1), candidate(2), candidate(3)},
		},
	}
	refinedAggs := query.Aggregations{
		"content_type": query.AggregationGroup{Items: query.AggregationItems{
			"movie": query.AggregationItem{Label: "movie", Count: 2},
			"tv":    query.AggregationItem{Label: "tv", Count: 1},
		}},
	}
	pg := &fakePG{result: search.TorrentContentResult{
		Items: []search.TorrentContentResultItem{
			item(1, tf("Inception.A.mkv", "mkv", 1)),
			item(2, tf("Inception.B.mkv", "mkv", 2)),
			item(3, tf("Inception.C.mkv", "mkv", 3)),
		},
		Aggregations: refinedAggs,
	}}

	c := newTestComposer(l3, pg)

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		10,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("expected served, got served=%v err=%v", served, err)
	}

	if res.TotalCount != 3 {
		t.Fatalf("expected TotalCount=3, got %d", res.TotalCount)
	}

	grp, ok := res.Aggregations["content_type"]
	if !ok {
		t.Fatalf("content_type facet must be present, got %+v", res.Aggregations)
	}

	var sum uint
	for _, it := range grp.Items {
		sum += it.Count
	}

	if sum != uint(res.TotalCount) {
		t.Fatalf(
			"facet counts must reconcile with items: sum(content_type)=%d, want N=%d (gate7-8)",
			sum,
			res.TotalCount,
		)
	}
}

// #98 (relevance-order) — L3 returns candidate ids in recall (path-relevance)
// order; the PG IN(...) requery returns PG-natural order. The served page MUST be
// in L3 id order, not PG order. Pre-fix the composer served pgResult.Items in PG
// order while advertising relevance. fakePG ignores the options and returns its
// items in a DIFFERENT order than L3's ids, so the reorder is the only thing that
// can make the page come back in recall order.
func TestComposer_TorrentContent_ServesInL3RecallOrder(t *testing.T) {
	// L3 recall order: 3, 1, 2.
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: []*pb.PathCandidate{
		candidate(3), candidate(1), candidate(2),
	}}}
	// PG returns the SAME torrents but in PG-natural order 1, 2, 3.
	pg := &fakePG{result: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		item(1, tf("Inception.A.mkv", "mkv", 1)),
		item(2, tf("Inception.B.mkv", "mkv", 2)),
		item(3, tf("Inception.C.mkv", "mkv", 3)),
	}}}

	c := newTestComposer(l3, pg)

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		10,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("expected served, got served=%v err=%v", served, err)
	}

	want := []byte{3, 1, 2}
	if len(res.Items) != len(want) {
		t.Fatalf("expected %d items, got %d", len(want), len(res.Items))
	}

	for i, b := range want {
		if res.Items[i].InfoHash != ih(b) {
			t.Fatalf(
				"item[%d] = %v, want recall order ih(%d); page must follow L3 recall order, not PG order (#98)",
				i,
				res.Items[i].InfoHash,
				b,
			)
		}
	}
}

// #10 — HasNextPage must be computed from rows actually consumed by THIS page, not
// from offset+limit. With limit==0 paginate returns ALL remaining rows, so there
// is no next page even though offset+0 < len(refined). Pre-fix HasNextPage was
// offset+limit < len(refined) → true for limit==0 (advertises a non-existent page).
func TestComposer_TorrentContent_HasNextPage(t *testing.T) {
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: []*pb.PathCandidate{
		candidate(1), candidate(2), candidate(3),
	}}}
	pg := &fakePG{result: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		item(1, tf("Inception.A.mkv", "mkv", 1)),
		item(2, tf("Inception.B.mkv", "mkv", 2)),
		item(3, tf("Inception.C.mkv", "mkv", 3)),
	}}}

	c := newTestComposer(l3, pg)

	// limit==0 → paginate returns all 3 refined rows; there is NO next page.
	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		0,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("expected served, got served=%v err=%v", served, err)
	}

	if len(res.Items) != 3 {
		t.Fatalf("limit==0 must return all refined rows, got %d", len(res.Items))
	}

	if res.HasNextPage {
		t.Fatal("limit==0 returns every remaining row → HasNextPage must be false (#10)")
	}

	// limit==2 over 3 refined rows → a real next page.
	res, _, _ = c.TorrentContent(context.Background(), Filters{Query: "inception"}, QueryOptions{}, 2, 0, nil)
	if !res.HasNextPage {
		t.Fatal("limit=2 over 3 refined rows must report HasNextPage=true")
	}

	// last page (offset=2, limit=2) consumes the final row → no next page.
	res, _, _ = c.TorrentContent(context.Background(), Filters{Query: "inception"}, QueryOptions{}, 2, 2, nil)
	if res.HasNextPage {
		t.Fatal("final page must report HasNextPage=false")
	}
}

// TestComposer_TorrentContent_HasNextPageHonestOnTruncation is F5: when a page
// EXHAUSTS its decoded window but the window was budget-truncated (candidate_total
// exceeds what we decoded), HasNextPage must be TRUE while there is still room to
// grow the decode budget — the old refined-only signal wrongly reported false while
// TotalCount advertised "About N". At the hard decode ceiling it must go back to
// false: the route can never serve past MaxCandidates candidates.
func TestComposer_TorrentContent_HasNextPageHonestOnTruncation(t *testing.T) {
	const sidecarTotal = 5000

	// The sidecar returns 8 matching candidates with a full-corpus candidate_total
	// of 5000. The page (offset=6, limit=2) consumes the whole 8-row refined window,
	// so the honest signal must come from the truncation branch, not leftover rows.
	mk := func() (cands []*pb.PathCandidate, items []search.TorrentContentResultItem) {
		for i := 1; i <= 8; i++ {
			cands = append(cands, candidate(byte(i)))
			items = append(items, item(byte(i), tf("Inception.2010.1080p.mkv", "mkv", uint(i))))
		}
		return cands, items
	}

	// Below the ceiling: MaxCandidates=1000. The 8-row window is fully consumed, but
	// candidate_total=5000 > 8 decoded AND 8 < 1000 ceiling → honest TRUE.
	cands, items := mk()
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: cands, CandidateTotal: sidecarTotal, Estimated: true}}
	pg := &fakePG{result: search.TorrentContentResult{Items: items}}
	below := newTestComposer(l3, pg) // MaxCandidates=1000, oversample=4

	res, served, err := below.TorrentContent(context.Background(), Filters{Query: "inception"}, QueryOptions{}, 2, 6, nil)
	if err != nil || !served {
		t.Fatalf("expected served, got served=%v err=%v", served, err)
	}
	if !res.HasNextPage {
		t.Fatal("truncated window below the ceiling must report HasNextPage=true (more candidates exist)")
	}

	// At the ceiling: MaxCandidates=8, so the decode window == the ceiling. Even
	// though candidate_total=5000 is larger, no deeper page can ever grow past 8
	// decoded candidates → honest FALSE.
	cands, items = mk()
	l3c := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: cands, CandidateTotal: sidecarTotal, Estimated: true}}
	pgc := &fakePG{result: search.TorrentContentResult{Items: items}}
	ceiling := NewComposer(l3c, pgc, ComposerConfig{MinQueryLength: 3, OversampleFactor: 4, MaxCandidates: 8}, nil)

	res, served, err = ceiling.TorrentContent(context.Background(), Filters{Query: "inception"}, QueryOptions{}, 2, 6, nil)
	if err != nil || !served {
		t.Fatalf("expected served, got served=%v err=%v", served, err)
	}
	if res.HasNextPage {
		t.Fatal("at the decode ceiling HasNextPage must be false — the route never serves past MaxCandidates")
	}
}

func TestComposer_CollapsePaths(t *testing.T) {
	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{Candidates: []*pb.PathCandidate{candidate(1), candidate(2)}}}
	pg := &fakePG{result: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		item(1, tf("a/Movie.mkv", "mkv", 1), tf("a/sample.txt", "txt", 1)),
		item(2, tf("b/movie.mkv", "mkv", 2)),
	}}}

	c := newTestComposer(l3, pg)

	groups, served, err := c.CollapsePaths(
		context.Background(),
		Filters{Query: "movie"},
		QueryOptions{},
		10,
		0,
		nil,
	)
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
