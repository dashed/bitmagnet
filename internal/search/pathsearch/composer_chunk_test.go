package pathsearch

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// itemFiles builds a candidate with n distinct matching files (each path contains
// "inception"), so its retained decoded fileset has length n.
func itemFiles(b byte, n int) search.TorrentContentResultItem {
	files := make([]model.TorrentFile, n)
	for i := range files {
		files[i] = tf("inception/part"+strconv.Itoa(i)+".mkv", "mkv", uint(i+1))
	}

	return item(b, files...)
}

// retainedFiles sums the decoded fileset lengths actually RETAINED in a result —
// the quantity the RetainedFileBudget bounds.
func retainedFiles(items []search.TorrentContentResultItem) int {
	total := 0
	for i := range items {
		total += len(items[i].Torrent.Files)
	}

	return total
}

// newChunkComposer builds a composer with tiny byte-bound knobs so a small test
// corpus forces multiple refine chunks. cap/budget/maxLen are the gate7-4
// constants under test.
func newChunkComposer(
	l3 candidateSource,
	pg torrentContentSearcher,
	m *Metrics,
	capFiles, budget, maxLen uint,
) *Composer {
	return NewComposer(l3, pg, ComposerConfig{
		MinQueryLength:   3,
		OversampleFactor: 4,
		MaxCandidates:    1000,
		MaxRefineFiles:   capFiles,
		RefineFileBudget: budget,
		MaxChunkTorrents: maxLen,
		// Large retained budget so these chunking tests never trip the retained cap
		// (that bound is exercised separately by the retained-budget tests).
		RetainedFileBudget: 1_000_000_000,
	}, nil, WithMetrics(m))
}

// newRetainedComposer builds a composer with explicit per-torrent cap, chunk
// budget, and RETAINED budget so the retained-file bound can be forced.
func newRetainedComposer(
	l3 candidateSource,
	pg torrentContentSearcher,
	m *Metrics,
	capFiles, chunkBudget, retainedBudget uint,
) *Composer {
	return NewComposer(l3, pg, ComposerConfig{
		MinQueryLength:     3,
		OversampleFactor:   4,
		MaxCandidates:      1000,
		MaxRefineFiles:     capFiles,
		RefineFileBudget:   chunkBudget,
		MaxChunkTorrents:   1024,
		RetainedFileBudget: retainedBudget,
	}, nil, WithMetrics(m))
}

func candList(bs ...byte) []*pb.PathCandidate {
	out := make([]*pb.PathCandidate, len(bs))
	for i, b := range bs {
		out[i] = candidate(b)
	}

	return out
}

// TestComposer_Chunking_EqualsSinglePass is the core gate7-4 invariant: refining
// the candidate set in cumulative-file-budget chunks yields BYTE-IDENTICAL
// Items/order/TotalCount/HasNextPage to a single-pass (no-chunk) reference. The
// corpus is many small + a couple of larger torrents plus an L3 false positive;
// the budgets are set so the chunked composer splits into >1 chunk while the
// reference composer (huge budget) does not.
func TestComposer_Chunking_EqualsSinglePass(t *testing.T) {
	// L3 recall order: 1,2,3,4,5,6 (2 = false positive, dropped by refine).
	cands := candList(1, 2, 3, 4, 5, 6)

	items := []search.TorrentContentResultItem{
		item(1, tf("Inception.A.mkv", "mkv", 1)),
		item(2, tf("Independence.Day.mkv", "mkv", 2)), // false positive (no "inception")
		item(3, tf("Inception.C.mkv", "mkv", 3)),
		item(4, tf("Inception.D.mkv", "mkv", 4)),
		item(5, tf("Inception.E.mkv", "mkv", 5)),
		item(6, tf("Inception.F.mkv", "mkv", 6)),
	}

	// Per-torrent file counts: a mix of small + larger. Sum = 4+1+4+1+4+1 = 15.
	counts := map[protocol.ID]int{
		ih(1): 4, ih(2): 1, ih(3): 4, ih(4): 1, ih(5): 4, ih(6): 1,
	}

	newPG := func() *fakePG {
		return &fakePG{
			result: search.TorrentContentResult{
				Items: append([]search.TorrentContentResultItem(nil), items...),
			},
			fileCounts: counts,
		}
	}

	// Reference: a single pass (budget far above the total, no chunk cap hit).
	refPG := newPG()
	ref := newChunkComposer(
		&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: cands}},
		refPG,
		NewMetrics(),
		1_000,
		1_000,
		1024,
	)

	refRes, refServed, refErr := ref.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		10,
		0,
		nil,
	)
	if refErr != nil || !refServed {
		t.Fatalf("reference: served=%v err=%v", refServed, refErr)
	}

	// gate7-8: the single-chunk fast path makes TWO PG queries — one chunk decode
	// (hydrator-only refineOptions, no facets) + one decode-free aggregation over the
	// REFINED set. Pre-gate7-8 it was a single combined decode+facet query.
	if refPG.callCount != 2 {
		t.Fatalf(
			"reference (single chunk) must make 2 PG queries (chunk decode + refined-agg), got %d",
			refPG.callCount,
		)
	}

	// Chunked: budget 6 with a per-torrent cap of 100 → splits the L3-ordered set
	// into contiguous chunks each summing ≤6: [1(4),2(1)]=5,+3(4)=9>6 → [1,2],
	// [3(4),4(1)]=5,+5(4)=9>6 → [3,4], [5(4),6(1)]=5 → [5,6]. Three chunks.
	chunkPG := newPG()
	chunked := newChunkComposer(
		&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: cands}},
		chunkPG,
		NewMetrics(),
		100,
		6,
		1024,
	)

	chunkRes, chunkServed, chunkErr := chunked.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		10,
		0,
		nil,
	)
	if chunkErr != nil || !chunkServed {
		t.Fatalf("chunked: served=%v err=%v", chunkServed, chunkErr)
	}

	// gate7-8: 3 chunk decode queries + 1 decode-free aggregation over the REFINED
	// set = 4 (the aggregation moved from BEFORE the chunk loop, over the candidate
	// set, to AFTER refine, over the refined set; the count is unchanged).
	if chunkPG.callCount != 4 {
		t.Fatalf("chunked path must make 3 chunk + 1 refined-agg PG queries = 4, got %d", chunkPG.callCount)
	}

	// Identical results: order, TotalCount, HasNextPage, and item identity.
	if chunkRes.TotalCount != refRes.TotalCount {
		t.Fatalf("TotalCount mismatch: chunked=%d ref=%d", chunkRes.TotalCount, refRes.TotalCount)
	}

	if chunkRes.HasNextPage != refRes.HasNextPage {
		t.Fatalf("HasNextPage mismatch: chunked=%v ref=%v", chunkRes.HasNextPage, refRes.HasNextPage)
	}

	if len(chunkRes.Items) != len(refRes.Items) {
		t.Fatalf("item count mismatch: chunked=%d ref=%d", len(chunkRes.Items), len(refRes.Items))
	}

	wantOrder := []byte{1, 3, 4, 5, 6} // 2 dropped (false positive), rest in L3 order

	for i := range chunkRes.Items {
		if chunkRes.Items[i].Torrent.InfoHash != refRes.Items[i].Torrent.InfoHash {
			t.Fatalf("item[%d] mismatch: chunked=%v ref=%v", i,
				chunkRes.Items[i].Torrent.InfoHash, refRes.Items[i].Torrent.InfoHash)
		}

		if chunkRes.Items[i].Torrent.InfoHash != ih(wantOrder[i]) {
			t.Fatalf(
				"item[%d] not in L3 order: got %v want ih(%d)",
				i,
				chunkRes.Items[i].Torrent.InfoHash,
				wantOrder[i],
			)
		}
	}
}

// TestComposer_Chunking_PaginationAcrossChunks asserts the in-Go page window is
// applied over the GLOBAL refined set, so a page can span a chunk boundary and a
// later page returns the correct chunk's rows (never empty / never duplicated).
func TestComposer_Chunking_PaginationAcrossChunks(t *testing.T) {
	cands := candList(1, 2, 3, 4)
	items := []search.TorrentContentResultItem{
		item(1, tf("Inception.A.mkv", "mkv", 1)),
		item(2, tf("Inception.B.mkv", "mkv", 2)),
		item(3, tf("Inception.C.mkv", "mkv", 3)),
		item(4, tf("Inception.D.mkv", "mkv", 4)),
	}
	counts := map[protocol.ID]int{ih(1): 4, ih(2): 4, ih(3): 4, ih(4): 4}

	pg := &fakePG{
		result: search.TorrentContentResult{
			Items: append([]search.TorrentContentResultItem(nil), items...),
		},
		fileCounts: counts,
	}
	// budget 6 → each torrent (4) pairs only with itself before exceeding: [1],[2],[3],[4]? 4+4=8>6 →
	// [1],[2],[3],[4].
	c := newChunkComposer(
		&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: cands}},
		pg,
		NewMetrics(),
		100,
		6,
		1024,
	)

	// Page 2 (offset 2, limit 2) over the 4 refined rows → items 3,4.
	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		2,
		2,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("served=%v err=%v", served, err)
	}

	if len(res.Items) != 2 || res.Items[0].Torrent.InfoHash != ih(3) || res.Items[1].Torrent.InfoHash != ih(4) {
		t.Fatalf("page 2 must be [3,4], got %d items", len(res.Items))
	}

	if res.HasNextPage {
		t.Fatal("final page must report HasNextPage=false")
	}

	if res.TotalCount != 4 {
		t.Fatalf("TotalCount must be 4 (all refined), got %d", res.TotalCount)
	}
}

// TestComposer_SanityCap_DeclinesOversized asserts a candidate whose file_count
// exceeds MaxRefineFiles is EXCLUDED from refine (fail-loud), the
// refine_declined_oversized counter is incremented, and the rest of the set is
// still served — never a silent truncation and never a route fallback.
func TestComposer_SanityCap_DeclinesOversized(t *testing.T) {
	m := NewMetrics()
	cands := candList(1, 2, 3)
	items := []search.TorrentContentResultItem{
		item(1, tf("Inception.A.mkv", "mkv", 1)),
		item(2, tf("Inception.HUGE.mkv", "mkv", 2)), // oversized → declined before decode
		item(3, tf("Inception.C.mkv", "mkv", 3)),
	}
	counts := map[protocol.ID]int{ih(1): 5, ih(2): 1_000, ih(3): 5} // cap = 500

	pg := &fakePG{
		result: search.TorrentContentResult{
			Items: append([]search.TorrentContentResultItem(nil), items...),
		},
		fileCounts: counts,
	}
	c := newChunkComposer(&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: cands}}, pg, m, 500, 1_500_000, 1024)

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		10,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("oversized-decline must still SERVE the rest, got served=%v err=%v", served, err)
	}

	if got := testutil.ToFloat64(m.refineDeclined); got != 1 {
		t.Fatalf("refine_declined_oversized_total = %v, want 1", got)
	}

	// Item 2 (oversized) excluded; 1 and 3 served in order.
	if len(res.Items) != 2 || res.Items[0].Torrent.InfoHash != ih(1) || res.Items[1].Torrent.InfoHash != ih(3) {
		t.Fatalf("oversized id must be excluded, serving [1,3]; got %d items", len(res.Items))
	}

	if res.TotalCount != 2 {
		t.Fatalf("TotalCount must exclude the declined id (want 2), got %d", res.TotalCount)
	}

	// The route was SERVED, not a fallback — the decline is loud-but-local.
	if got := routeCount(m, RouteFallback); got != 0 {
		t.Fatalf("oversized decline must NOT be a route fallback, got fallback=%v", got)
	}
}

// TestComposer_SanityCap_AllOversizedServesEmpty: if EVERY candidate is oversized
// the route serves an honest estimated-empty (each decline already counted), not
// a fallback and not a decode.
func TestComposer_SanityCap_AllOversizedServesEmpty(t *testing.T) {
	m := NewMetrics()
	cands := candList(1, 2)
	pg := &fakePG{
		result:     search.TorrentContentResult{Items: []search.TorrentContentResultItem{item(1), item(2)}},
		fileCounts: map[protocol.ID]int{ih(1): 1_000, ih(2): 1_000},
	}
	c := newChunkComposer(&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: cands}}, pg, m, 500, 1_500_000, 1024)

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		10,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("all-oversized must serve estimated empty, got served=%v err=%v", served, err)
	}

	if len(res.Items) != 0 || !res.TotalCountIsEstimate {
		t.Fatalf(
			"expected empty estimated result, got %d items estimate=%v",
			len(res.Items),
			res.TotalCountIsEstimate,
		)
	}

	if got := testutil.ToFloat64(m.refineDeclined); got != 2 {
		t.Fatalf("refine_declined_oversized_total = %v, want 2", got)
	}

	if pg.callCount != 0 {
		t.Fatal("all-oversized must NOT issue any decoding PG query")
	}
}

// TestComposer_FileCountsErrorFallsBack: a FileCounts probe error is a hard PG
// error → the route fails (served=false) so the GraphQL layer runs the plain PG
// path; the error counter moves.
func TestComposer_FileCountsErrorFallsBack(t *testing.T) {
	m := NewMetrics()
	pg := &fakePG{fcErr: errors.New("summary query failed")}
	c := newChunkComposer(
		&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: candList(1)}},
		pg,
		m,
		500,
		1_500_000,
		1024,
	)

	_, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		10,
		0,
		nil,
	)
	if err == nil || served {
		t.Fatalf("FileCounts error must propagate (served=false, err!=nil), got served=%v err=%v", served, err)
	}

	if got := routeCount(m, RouteError); got != 1 {
		t.Fatalf("route_total{error} = %v, want 1", got)
	}
}

// TestComposer_ChunkByFileBudget exercises the splitter directly: contiguous,
// budget-respecting, and MaxChunkTorrents-respecting; unknown counts treated as
// the cap.
func TestComposer_ChunkByFileBudget(t *testing.T) {
	c := NewComposer(nil, nil, ComposerConfig{
		MaxRefineFiles:   100,
		RefineFileBudget: 10,
		MaxChunkTorrents: 3,
	}, nil)

	ids := []protocol.ID{ih(1), ih(2), ih(3), ih(4), ih(5), ih(6)}

	t.Run("budget split, contiguous + in-order", func(t *testing.T) {
		counts := map[protocol.ID]int{ih(1): 4, ih(2): 4, ih(3): 4, ih(4): 4, ih(5): 4, ih(6): 4}
		// 4,8,+4=12>10 → [1,2]; 4,8,+4>10 → [3,4]; [5,6]. maxLen=3 not hit first.
		got := c.chunkByFileBudget(ids, counts)
		want := [][]byte{{1, 2}, {3, 4}, {5, 6}}
		assertChunks(t, got, want)
	})

	t.Run("maxChunkTorrents cap", func(t *testing.T) {
		// Tiny counts (1 each): budget 10 never trips; the length cap (3) does.
		counts := map[protocol.ID]int{ih(1): 1, ih(2): 1, ih(3): 1, ih(4): 1, ih(5): 1, ih(6): 1}
		got := c.chunkByFileBudget(ids, counts)
		want := [][]byte{{1, 2, 3}, {4, 5, 6}}
		assertChunks(t, got, want)
	})

	t.Run("unknown count treated as cap → its own chunk", func(t *testing.T) {
		// id 2 unknown → counted as cap (100) > budget(10) → it always starts/ends a
		// chunk alone; neighbours still pack by budget.
		counts := map[protocol.ID]int{ih(1): 1, ih(3): 1, ih(4): 1, ih(5): 1, ih(6): 1} // ih(2) absent
		got := c.chunkByFileBudget(ids, counts)
		// [1(1)] then 2(cap) breaks → [1],[2], then [3,4,5] (maxLen 3), [6].
		want := [][]byte{{1}, {2}, {3, 4, 5}, {6}}
		assertChunks(t, got, want)
	})

	t.Run("empty input", func(t *testing.T) {
		if got := c.chunkByFileBudget(nil, nil); got != nil {
			t.Fatalf("empty ids must yield nil chunks, got %v", got)
		}
	})
}

func assertChunks(t *testing.T, got [][]protocol.ID, want [][]byte) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("chunk count = %d, want %d (%v)", len(got), len(want), chunkBytes(got))
	}

	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("chunk[%d] len = %d, want %d (%v)", i, len(got[i]), len(want[i]), chunkBytes(got))
		}

		for j := range want[i] {
			if got[i][j] != ih(want[i][j]) {
				t.Fatalf(
					"chunk[%d][%d] = %v, want ih(%d) (%v)",
					i,
					j,
					got[i][j],
					want[i][j],
					chunkBytes(got),
				)
			}
		}
	}
}

func chunkBytes(chunks [][]protocol.ID) [][]byte {
	out := make([][]byte, len(chunks))

	for i, ch := range chunks {
		bs := make([]byte, len(ch))
		for j, id := range ch {
			bs[j] = id[0]
		}

		out[i] = bs
	}

	return out
}

// TestComposer_Collapse_ChunkingEqualsSinglePass: collapse groups accumulate
// across chunks in first-seen (L3) order, identical to a single pass.
func TestComposer_Collapse_ChunkingEqualsSinglePass(t *testing.T) {
	cands := candList(1, 2, 3)
	items := []search.TorrentContentResultItem{
		item(1, tf("a/Movie.mkv", "mkv", 1), tf("a/sample.txt", "txt", 1)),
		item(2, tf("b/movie.mkv", "mkv", 2)),
		item(3, tf("a/Movie.mkv", "mkv", 3)), // shares path with item 1's group
	}
	counts := map[protocol.ID]int{ih(1): 4, ih(2): 4, ih(3): 4}

	newPG := func() *fakePG {
		return &fakePG{
			result: search.TorrentContentResult{
				Items: append([]search.TorrentContentResultItem(nil), items...),
			},
			fileCounts: counts,
		}
	}

	ref := newChunkComposer(
		&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: cands}},
		newPG(),
		NewMetrics(),
		1_000,
		1_000,
		1024,
	)

	refGroups, refServed, refErr := ref.CollapsePaths(
		context.Background(),
		Filters{Query: "movie"},
		QueryOptions{},
		10,
		0,
		nil,
	)
	if refErr != nil || !refServed {
		t.Fatalf("reference collapse: served=%v err=%v", refServed, refErr)
	}

	// budget 6 → [1],[2],[3] (each 4; 4+4=8>6).
	chunked := newChunkComposer(
		&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: cands}},
		newPG(),
		NewMetrics(),
		100,
		6,
		1024,
	)

	chunkGroups, chunkServed, chunkErr := chunked.CollapsePaths(
		context.Background(),
		Filters{Query: "movie"},
		QueryOptions{},
		10,
		0,
		nil,
	)
	if chunkErr != nil || !chunkServed {
		t.Fatalf("chunked collapse: served=%v err=%v", chunkServed, chunkErr)
	}

	if len(chunkGroups) != len(refGroups) {
		t.Fatalf("group count mismatch: chunked=%d ref=%d", len(chunkGroups), len(refGroups))
	}

	for i := range refGroups {
		if chunkGroups[i].Path != refGroups[i].Path {
			t.Fatalf(
				"group[%d] path mismatch: chunked=%q ref=%q",
				i,
				chunkGroups[i].Path,
				refGroups[i].Path,
			)
		}

		if len(chunkGroups[i].InfoHashes) != len(refGroups[i].InfoHashes) {
			t.Fatalf("group[%d] %q hash count mismatch: chunked=%d ref=%d",
				i, refGroups[i].Path, len(chunkGroups[i].InfoHashes), len(refGroups[i].InfoHashes))
		}

		for j := range refGroups[i].InfoHashes {
			if chunkGroups[i].InfoHashes[j] != refGroups[i].InfoHashes[j] {
				t.Fatalf(
					"group[%d] %q hash[%d] mismatch: chunked=%v ref=%v",
					i,
					refGroups[i].Path,
					j,
					chunkGroups[i].InfoHashes[j],
					refGroups[i].InfoHashes[j],
				)
			}
		}
	}

	// Expected (only paths containing "movie"): "a/Movie.mkv" {1,3} (first-seen
	// from item 1, then item 3 joins the same group), then "b/movie.mkv" {2}.
	if len(refGroups) != 2 ||
		refGroups[0].Path != "a/Movie.mkv" || len(refGroups[0].InfoHashes) != 2 ||
		refGroups[1].Path != "b/movie.mkv" || len(refGroups[1].InfoHashes) != 1 {
		t.Fatalf("unexpected reference groups: %+v", refGroups)
	}
}

// TestComposer_RetainedBudget_CapsHighMatchRate is the gate7-4 ROBUST-bound
// regression (g7-reviewer): chunking bounds only the per-chunk transient decode,
// but `refined` accumulates every matched torrent's full Files across chunks. A
// high-match-rate query must STOP accumulating once the cumulative retained-file
// budget is reached and serve the accumulated TOP-RELEVANCE prefix as a
// memory-capped estimate — never retaining the whole match set at once.
func TestComposer_RetainedBudget_CapsHighMatchRate(t *testing.T) {
	m := NewMetrics()
	// 5 matched torrents, each with 4 matching files (retained 4 each).
	cands := candList(1, 2, 3, 4, 5)
	items := []search.TorrentContentResultItem{
		itemFiles(1, 4), itemFiles(2, 4), itemFiles(3, 4), itemFiles(4, 4), itemFiles(5, 4),
	}
	counts := map[protocol.ID]int{ih(1): 4, ih(2): 4, ih(3): 4, ih(4): 4, ih(5): 4}

	pg := &fakePG{
		result: search.TorrentContentResult{
			Items: append([]search.TorrentContentResultItem(nil), items...),
		},
		fileCounts: counts,
	}
	// chunkBudget 4 → one torrent per chunk (5 chunks). retainedBudget 10 → after
	// t1(4)+t2(4)=8, adding t3 (→12) exceeds 10 → cap; serve [t1,t2].
	c := newRetainedComposer(&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: cands}}, pg, m, 100, 4, 10)

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		50,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("memory-capped result must still SERVE, got served=%v err=%v", served, err)
	}

	if got := retainedFiles(res.Items); got > 10 {
		t.Fatalf("retained files = %d, must be ≤ RetainedFileBudget(10)", got)
	}

	if len(res.Items) != 2 || res.Items[0].Torrent.InfoHash != ih(1) || res.Items[1].Torrent.InfoHash != ih(2) {
		t.Fatalf("must serve the top-relevance prefix [1,2], got %d items", len(res.Items))
	}

	if !res.TotalCountIsEstimate {
		t.Fatal("a memory-capped result must be flagged TotalCountIsEstimate=true")
	}

	if got := testutil.ToFloat64(m.retainedCapped); got != 1 {
		t.Fatalf("refine_retained_capped_total = %v, want 1", got)
	}

	// The raw msgpack blob must be freed on served items (#2 — Files kept, FilesData not).
	for i := range res.Items {
		if res.Items[i].Torrent.FilesData != nil {
			t.Fatalf("served item %d must have FilesData freed (nil) after refine", i)
		}
	}
}

// TestComposer_RetainedBudget_SelectiveStaysExact: a selective query whose match
// set is well under the retained budget never trips the cap — all matches served,
// counter flat, == pre-cap behavior.
func TestComposer_RetainedBudget_SelectiveStaysExact(t *testing.T) {
	m := NewMetrics()
	cands := candList(1, 2, 3)
	items := []search.TorrentContentResultItem{itemFiles(1, 2), itemFiles(2, 2), itemFiles(3, 2)}
	counts := map[protocol.ID]int{ih(1): 2, ih(2): 2, ih(3): 2}

	pg := &fakePG{
		result: search.TorrentContentResult{
			Items: append([]search.TorrentContentResultItem(nil), items...),
		},
		fileCounts: counts,
	}
	// Generous chunk + retained budgets → single chunk, no cap.
	c := newRetainedComposer(
		&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: cands}},
		pg,
		m,
		500_000,
		1_500_000,
		1_500_000,
	)

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		50,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("served=%v err=%v", served, err)
	}

	if len(res.Items) != 3 || res.TotalCount != 3 {
		t.Fatalf(
			"selective query must return ALL 3 matches, got %d items / TotalCount=%d",
			len(res.Items),
			res.TotalCount,
		)
	}

	if got := testutil.ToFloat64(m.retainedCapped); got != 0 {
		t.Fatalf("selective query must NOT trip the retained cap, got refine_retained_capped_total=%v", got)
	}

	// gate7-8: the single-chunk fast path = 1 chunk decode + 1 decode-free refined
	// aggregation = 2 PG queries (was 1 combined query pre-gate7-8).
	if pg.callCount != 2 {
		t.Fatalf(
			"selective single-chunk query must make 2 PG queries (decode + refined-agg), got callCount=%d",
			pg.callCount,
		)
	}
}

// TestComposer_PostDecodeGuard_DeclinesUnknownOversized: an id ABSENT from both
// the summary and torrents.files_count (unknown → budgeted as cap, NOT
// pre-declined) whose ACTUAL decoded fileset exceeds MaxRefineFiles must be
// declined POST-DECODE (#4) — counted + dropped, the rest still served.
func TestComposer_PostDecodeGuard_DeclinesUnknownOversized(t *testing.T) {
	m := NewMetrics()
	cands := candList(1, 2, 3)
	items := []search.TorrentContentResultItem{
		itemFiles(1, 1),
		itemFiles(2, 5), // actual fileset 5 > cap 3, but its count is UNKNOWN
		itemFiles(3, 1),
	}
	// strict: only 1 and 3 have known counts; 2 is omitted → unknown.
	pg := &fakePG{
		result: search.TorrentContentResult{
			Items: append([]search.TorrentContentResultItem(nil), items...),
		},
		fileCounts:   map[protocol.ID]int{ih(1): 1, ih(3): 1},
		strictCounts: true,
	}
	// cap 3; generous chunk/retained budgets → single chunk fast path.
	c := newRetainedComposer(
		&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: cands}},
		pg,
		m,
		3,
		1_500_000,
		1_500_000,
	)

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		50,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("post-decode decline must still SERVE the rest, got served=%v err=%v", served, err)
	}

	if got := testutil.ToFloat64(m.refineDeclined); got != 1 {
		t.Fatalf("refine_declined_oversized_total = %v, want 1 (the unknown-count oversized blob)", got)
	}

	if len(res.Items) != 2 || res.Items[0].Torrent.InfoHash != ih(1) || res.Items[1].Torrent.InfoHash != ih(3) {
		t.Fatalf("post-decode-oversized id must be excluded, serving [1,3]; got %d items", len(res.Items))
	}

	if got := testutil.ToFloat64(m.retainedCapped); got != 0 {
		t.Fatalf("post-decode decline is not a retained-cap event, got %v", got)
	}
}

// TestComposer_HighFanout_PerChunkBoundedAndCapsGracefully mimics the gate7-4
// stress (apple2_flop: MANY candidate torrents each with a LARGE fileset). It
// asserts (a) the per-chunk DECODE is bounded — the chunker never produces a
// chunk summing over the budget, so one chunk's transient decode fits memory;
// (b) the retained cap FIRES and serves a capped TOP-RELEVANCE result with
// TotalCountIsEstimate=true (NOT an error/empty); (c) retained files ≤ budget.
func TestComposer_HighFanout_PerChunkBoundedAndCapsGracefully(t *testing.T) {
	m := NewMetrics()

	const (
		n          = 80 // candidate torrents
		filesEach  = 5  // each with a (scaled) large fileset
		capFiles   = 10 // sanity cap == chunk budget
		chunkBudg  = 10 // → 2 torrents per chunk
		retainBudg = 20 // → caps after 4 matched torrents (20 files)
	)

	bs := make([]byte, n)
	items := make([]search.TorrentContentResultItem, n)
	counts := make(map[protocol.ID]int, n)

	for i := range n {
		b := byte(i + 1)
		bs[i] = b
		items[i] = itemFiles(b, filesEach)
		counts[ih(b)] = filesEach
	}

	// (a) per-chunk DECODE bound: the chunker never exceeds the budget.
	cForChunks := newRetainedComposer(nil, nil, NewMetrics(), capFiles, chunkBudg, retainBudg)

	ids := make([]protocol.ID, n)
	for i, b := range bs {
		ids[i] = ih(b)
	}

	for ci, ch := range cForChunks.chunkByFileBudget(ids, counts) {
		sum := 0
		for _, id := range ch {
			sum += counts[id]
		}

		if sum > chunkBudg {
			t.Fatalf(
				"chunk %d sums %d files > budget %d — one chunk's decode could exceed memory",
				ci,
				sum,
				chunkBudg,
			)
		}
	}

	pg := &fakePG{
		result: search.TorrentContentResult{
			Items: append([]search.TorrentContentResultItem(nil), items...),
		},
		fileCounts: counts,
	}
	c := newRetainedComposer(
		&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: candList(bs...)}},
		pg,
		m,
		capFiles,
		chunkBudg,
		retainBudg,
	)

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		50,
		0,
		nil,
	)
	// (b) GRACEFUL: a high-fanout query must SERVE a capped result, never error/empty.
	if err != nil {
		t.Fatalf("high-fanout query must NOT error, got %v", err)
	}

	if !served {
		t.Fatal("high-fanout query must be SERVED (capped top-relevance), not fall back empty")
	}

	if !res.TotalCountIsEstimate {
		t.Fatal("a memory-capped result must carry TotalCountIsEstimate=true")
	}

	if got := testutil.ToFloat64(m.retainedCapped); got != 1 {
		t.Fatalf("retained cap must FIRE on high-fanout, refine_retained_capped_total=%v want 1", got)
	}

	// (c) retained files bounded; served set is the top-relevance prefix.
	if got := retainedFiles(res.Items); got > retainBudg {
		t.Fatalf("retained files = %d > RetainedFileBudget %d", got, retainBudg)
	}

	if len(res.Items) != 4 {
		t.Fatalf("expected the top-4 relevance prefix (4×5=20 files = budget), got %d items", len(res.Items))
	}

	for i := range res.Items {
		if res.Items[i].Torrent.InfoHash != ih(byte(i+1)) {
			t.Fatalf(
				"served item %d must be the top-relevance prefix ih(%d), got %v",
				i,
				i+1,
				res.Items[i].Torrent.InfoHash,
			)
		}

		if res.Items[i].Torrent.FilesData != nil {
			t.Fatalf("served item %d must have FilesData freed", i)
		}
	}

	// The loop must STOP early at the cap (not query all 40 chunks): 3 chunk queries
	// (chunks [1,2],[3,4],[5,6]; cap trips on chunk 3's first item) + 1 decode-free
	// aggregation over the REFINED prefix (gate7-8) = 4.
	if pg.callCount != 4 {
		t.Fatalf(
			"capped high-fanout must stop early: want 3 chunk + 1 refined-agg queries = 4, got %d",
			pg.callCount,
		)
	}
}

// gate7-8 (c) — a RETAINED-cap'd query serves a top-relevance PREFIX; its facets
// must be computed over THAT served prefix (the refined set), not the full candidate
// set, so the sidebar reconciles with the capped items (sum(facet) == served count)
// and is NOT blanked.
func TestComposer_TorrentContent_CappedResult_FacetsOverServedPrefix(t *testing.T) {
	m := NewMetrics()

	const (
		n          = 80
		filesEach  = 5
		capFiles   = 10
		chunkBudg  = 10
		retainBudg = 20 // caps after 4 matched torrents (4×5 = 20 files = budget)
	)

	bs := make([]byte, n)
	items := make([]search.TorrentContentResultItem, n)
	counts := make(map[protocol.ID]int, n)

	for i := range n {
		b := byte(i + 1)
		bs[i] = b
		items[i] = itemFiles(b, filesEach)
		counts[ih(b)] = filesEach
	}

	// The refined-agg pass (over the 4-item served prefix) returns these facets.
	prefixAggs := query.Aggregations{
		"content_type": query.AggregationGroup{Items: query.AggregationItems{
			"movie": query.AggregationItem{Label: "movie", Count: 4},
		}},
	}
	pg := &fakePG{
		result: search.TorrentContentResult{
			Items:        append([]search.TorrentContentResultItem(nil), items...),
			Aggregations: prefixAggs,
		},
		fileCounts: counts,
	}
	c := newRetainedComposer(
		&fakeL3{resp: &pb.PathCandidatesResponse{Candidates: candList(bs...)}},
		pg,
		m,
		capFiles,
		chunkBudg,
		retainBudg,
	)

	res, served, err := c.TorrentContent(
		context.Background(),
		Filters{Query: "inception"},
		QueryOptions{},
		50,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("capped query must serve, got served=%v err=%v", served, err)
	}

	if got := testutil.ToFloat64(m.retainedCapped); got != 1 {
		t.Fatalf("retained cap must FIRE, got refine_retained_capped_total=%v", got)
	}

	if res.TotalCount != 4 {
		t.Fatalf("capped result serves the 4-item top-relevance prefix → TotalCount=4, got %d", res.TotalCount)
	}

	grp, ok := res.Aggregations["content_type"]
	if !ok || len(grp.Items) == 0 {
		t.Fatalf(
			"capped result must carry facets over the served prefix (not blanked), got %+v (gate7-8)",
			res.Aggregations,
		)
	}

	var sum uint
	for _, it := range grp.Items {
		sum += it.Count
	}

	if sum != uint(res.TotalCount) {
		t.Fatalf(
			"capped-prefix facets must reconcile with served items: sum=%d want %d (gate7-8)",
			sum,
			res.TotalCount,
		)
	}
}
