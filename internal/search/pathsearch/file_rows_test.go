package pathsearch

import (
	"context"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
)

func TestComposer_SearchFileRows_RefinesFiltersSortsAndLimitPlusOne(t *testing.T) {
	t.Parallel()

	const candidateTotal = 42

	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{
		Candidates:     []*pb.PathCandidate{candidate(1), candidate(2), candidate(3)},
		CandidateTotal: candidateTotal,
		Estimated:      true,
	}}
	pg := &fakePG{result: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		item(1,
			tf("Movies/Zeta/movie.mkv", "mkv", 900),
			tf("Movies/Zeta/movie.txt", "txt", 1200),
			tf("Movies/Zeta/tiny-movie.mkv", "mkv", 100),
		),
		item(2, tf("Movies/Other/other.mkv", "mkv", 800)),
		item(3, tf("Movies/Alpha/movie.mkv", "mkv", 500)),
	}}}

	c := newTestComposer(l3, pg)

	res, served, err := c.SearchFileRows(
		context.Background(),
		Filters{
			Query:      "movie",
			Extensions: []string{"mkv"},
			MinSize:    200,
			MaxSize:    1000,
		},
		QueryOptions{},
		1,
		0,
		[]FileRowSort{{Field: FileRowSortPath}},
	)
	if err != nil || !served {
		t.Fatalf("SearchFileRows served=%v err=%v", served, err)
	}

	if res.TotalCount != candidateTotal || !res.TotalCountIsEstimate {
		t.Fatalf("TotalCount = %d estimate=%v, want candidate_total=%d estimate=true",
			res.TotalCount, res.TotalCountIsEstimate, candidateTotal)
	}

	if !res.HasNextPage {
		t.Fatal("limit+1 collection must report HasNextPage")
	}

	if len(res.Rows) != 1 {
		t.Fatalf("rows len = %d, want 1", len(res.Rows))
	}

	row := res.Rows[0]
	if row.InfoHash != ih(3) || row.Path != "Movies/Alpha/movie.mkv" ||
		row.Extension != "mkv" || row.Size != 500 {
		t.Fatalf("first row = %+v, want Alpha mkv row sorted by path ASC", row)
	}
}

func TestComposer_SearchFileRows_DefaultSortsBySizeDesc(t *testing.T) {
	t.Parallel()

	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{
		Candidates:     []*pb.PathCandidate{candidate(1), candidate(2)},
		CandidateTotal: 2,
		Estimated:      true,
	}}
	pg := &fakePG{result: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		item(1, tf("movie-small.mkv", "mkv", 100)),
		item(2, tf("movie-large.mkv", "mkv", 900)),
	}}}

	c := newTestComposer(l3, pg)

	res, served, err := c.SearchFileRows(
		context.Background(),
		Filters{Query: "movie"},
		QueryOptions{},
		2,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("SearchFileRows served=%v err=%v", served, err)
	}

	if len(res.Rows) != 2 || res.Rows[0].Path != "movie-large.mkv" || res.Rows[1].Path != "movie-small.mkv" {
		t.Fatalf("default sort must be size DESC, got %+v", res.Rows)
	}
}

func TestSortFileRows_TorrentLastSeenDescSortsNullLast(t *testing.T) {
	t.Parallel()

	oldSeen := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	newSeen := oldSeen.Add(2 * time.Hour)
	rows := []FileRow{
		fileRowWithDHTLastSeen("missing.mkv", nil),
		fileRowWithDHTLastSeen("new.mkv", &newSeen),
		fileRowWithDHTLastSeen("old.mkv", &oldSeen),
	}

	sortFileRows(rows, []FileRowSort{{Field: FileRowSortLastSeen, Descending: true}})

	got := []string{rows[0].Path, rows[1].Path, rows[2].Path}
	want := []string{"new.mkv", "old.mkv", "missing.mkv"}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("paths = %v, want %v", got, want)
		}
	}
}

func TestSortFileRows_TorrentSeeders(t *testing.T) {
	t.Parallel()

	rows := []FileRow{
		fileRowWithSeeders("few.mkv", 3),
		fileRowWithSeeders("many.mkv", 19),
		fileRowWithSeeders("some.mkv", 8),
	}

	sortFileRows(rows, []FileRowSort{{Field: FileRowSortSeeders, Descending: true}})

	got := []string{rows[0].Path, rows[1].Path, rows[2].Path}
	want := []string{"many.mkv", "some.mkv", "few.mkv"}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("paths = %v, want %v", got, want)
		}
	}
}

func TestComposer_SearchFileRows_CarriesTorrentContentFromVisitor(t *testing.T) {
	t.Parallel()

	seenAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	resultItem := item(1, tf("movie.mkv", "mkv", 700))
	resultItem.Seeders = model.NewNullUint(77)
	resultItem.Torrent.Sources = []model.TorrentsTorrentSource{{
		SeenCount: 11,
		Source:    "dht",
		UpdatedAt: seenAt,
	}}

	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{
		Candidates:     []*pb.PathCandidate{candidate(1)},
		CandidateTotal: 1,
		Estimated:      true,
	}}
	pg := &fakePG{result: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		resultItem,
	}}}

	c := newTestComposer(l3, pg)

	res, served, err := c.SearchFileRows(
		context.Background(),
		Filters{Query: "movie"},
		QueryOptions{},
		1,
		0,
		nil,
	)
	if err != nil || !served {
		t.Fatalf("SearchFileRows served=%v err=%v", served, err)
	}

	if len(res.Rows) != 1 {
		t.Fatalf("rows len = %d, want 1", len(res.Rows))
	}

	got := res.Rows[0].TorrentContent
	if !got.Seeders.Valid || got.Seeders.Uint != 77 {
		t.Fatalf("row seeders = %+v, want 77", got.Seeders)
	}

	if lastSeen := dhtLastSeenAt(got); lastSeen == nil || !lastSeen.Equal(seenAt) {
		t.Fatalf("row dht last seen = %v, want %v", lastSeen, seenAt)
	}
}

func TestComposer_PathTypeahead_DerivesDedupeAndOrdersSegments(t *testing.T) {
	t.Parallel()

	l3 := &fakeL3{resp: &pb.PathCandidatesResponse{
		Candidates:     []*pb.PathCandidate{candidate(1), candidate(2), candidate(3)},
		CandidateTotal: 3,
		Estimated:      true,
	}}
	pg := &fakePG{result: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		item(1,
			tf("Movies/Inception/movie.mkv", "mkv", 1),
			tf("Movies/Inception/sample.mkv", "mkv", 1),
		),
		item(2, tf("Movies/Interstellar/movie.mkv", "mkv", 1)),
		item(3, tf("Movies/Inception/bonus.mkv", "mkv", 1)),
	}}}

	c := newTestComposer(l3, pg)

	got, served, err := c.PathTypeahead(context.Background(), "Movies/I", QueryOptions{}, 2)
	if err != nil || !served {
		t.Fatalf("PathTypeahead served=%v err=%v", served, err)
	}

	want := []string{"Inception", "Interstellar"}
	if len(got) != len(want) {
		t.Fatalf("suggestions = %v, want %v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("suggestions = %v, want %v", got, want)
		}
	}
}

func fileRowWithDHTLastSeen(path string, lastSeenAt *time.Time) FileRow {
	row := FileRow{Path: path}
	if lastSeenAt != nil {
		row.TorrentContent.Torrent.Sources = []model.TorrentsTorrentSource{{
			Source:    "dht",
			UpdatedAt: *lastSeenAt,
		}}
	}

	return row
}

func fileRowWithSeeders(path string, seeders uint) FileRow {
	return FileRow{
		Path: path,
		TorrentContent: search.TorrentContentResultItem{
			TorrentContent: model.TorrentContent{Seeders: model.NewNullUint(seeders)},
		},
	}
}
