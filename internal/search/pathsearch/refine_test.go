package pathsearch

import (
	"reflect"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
)

func tf(path, ext string, size uint) model.TorrentFile {
	f := model.TorrentFile{Path: path, Size: size}
	if ext != "" {
		f.Extension = model.NewNullString(ext)
	}

	return f
}

func extSet(exts ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(exts))
	for _, e := range exts {
		m[e] = struct{}{}
	}

	return m
}

func TestMatchFile_SubstringDropsFalsePositive(t *testing.T) {
	p := refinePredicate{substr: "inception"}

	if matchFile(tf("movies/Interstellar.2014.mkv", "mkv", 0), p) {
		t.Fatal("expected non-matching path to be rejected (recall false positive)")
	}

	if !matchFile(tf("movies/Inception.2010.1080p.mkv", "mkv", 0), p) {
		t.Fatal("expected matching path to pass")
	}
}

func TestMatchFile_SubstringCaseInsensitive(t *testing.T) {
	p := refinePredicate{substr: "inception"} // predicate substr is pre-lowered

	if !matchFile(tf("Movies/INCEPTION.2010.MKV", "MKV", 0), p) {
		t.Fatal("expected case-insensitive path match")
	}
}

func TestMatchFile_ExtensionFilter(t *testing.T) {
	p := refinePredicate{substr: "show", extensions: extSet("mkv", "mp4")}

	if matchFile(tf("show.s01e01.avi", "avi", 0), p) {
		t.Fatal("avi should be excluded by extension set {mkv,mp4}")
	}

	if !matchFile(tf("show.s01e01.mp4", "mp4", 0), p) {
		t.Fatal("mp4 should pass extension set {mkv,mp4}")
	}
}

// G1: crawl-path torrents can have an empty blob Extension; refine must derive it
// from the real path so extension filtering stays correct.
func TestMatchFile_ExtensionDerivedFromPathWhenBlobEmpty(t *testing.T) {
	p := refinePredicate{substr: "movie", extensions: extSet("mkv")}

	if !matchFile(tf("movie.2021.mkv", "", 0), p) {
		t.Fatal("expected extension derived from path (mkv) to pass")
	}

	if matchFile(tf("movie.2021.avi", "", 0), p) {
		t.Fatal("expected path-derived avi to be excluded")
	}
}

func TestMatchFile_SizeBounds(t *testing.T) {
	p := refinePredicate{substr: "f", minSize: 1000, maxSize: 5000}

	for _, tc := range []struct {
		size uint
		want bool
	}{
		{999, false},
		{1000, true},
		{5000, true},
		{5001, false},
	} {
		if got := matchFile(tf("file.bin", "bin", tc.size), p); got != tc.want {
			t.Fatalf("size=%d: got %v want %v", tc.size, got, tc.want)
		}
	}
}

func TestMatchFile_SizeUnboundedWhenZero(t *testing.T) {
	p := refinePredicate{substr: "f"}

	if !matchFile(tf("file.bin", "bin", 0), p) {
		t.Fatal("zero size bounds must not filter")
	}
}

func TestMatchedFiles_ReturnsOnlyMatches(t *testing.T) {
	p := refinePredicate{substr: "ep", extensions: extSet("mkv")}
	files := []model.TorrentFile{
		tf("show/ep01.mkv", "mkv", 1),
		tf("show/ep02.mkv", "mkv", 2),
		tf("show/poster.jpg", "jpg", 3),
		tf("show/special.avi", "avi", 4),
	}

	got := matchedFiles(files, p)
	want := []model.TorrentFile{files[0], files[1]}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("matchedFiles = %v, want %v", got, want)
	}
}

// --- torrent-level resolution + fail-loud (CAVEAT B) + single-file (CAVEAT C) --

func multiFile(files ...model.TorrentFile) model.Torrent {
	return model.Torrent{FilesStatus: model.FilesStatusMulti, Files: files}
}

func TestTorrentRefine_DropsCandidateWithNoMatchingFile(t *testing.T) {
	p := refinePredicate{substr: "inception", extensions: extSet("mkv")}

	tor := multiFile(
		tf("sample/readme.txt", "txt", 10),
		tf("sample/Interstellar.mkv", "mkv", 100),
	)
	if matched, ok := torrentRefine(tor, p); !ok || matched {
		t.Fatalf("expected clean non-match (ok=true, matched=false), got matched=%v ok=%v", matched, ok)
	}

	tor.Files = append(tor.Files, tf("Inception.2010.mkv", "mkv", 100))
	if matched, ok := torrentRefine(tor, p); !ok || !matched {
		t.Fatalf("expected match kept, got matched=%v ok=%v", matched, ok)
	}
}

// CAVEAT B — a multi-file candidate whose files cannot be obtained (no preloaded
// Files relation AND no files_data blob) must NOT be silently dropped:
// torrentRefine signals ok=false so the composer fails loud / falls back.
func TestTorrentRefine_MultiFileNoFilesIsFailLoudNotSilentDrop(t *testing.T) {
	bad := model.Torrent{FilesStatus: model.FilesStatusMulti} // no Files, no FilesData

	if _, ok := torrentRefine(bad, refinePredicate{substr: "x"}); ok {
		t.Fatal("multi-file torrent with no obtainable files must report ok=false")
	}
}

// CAVEAT B — when the Files relation is unpopulated but the L1 files_data blob IS
// present, filesForRefine decodes it explicitly (not leaning on AfterFind).
func TestFilesForRefine_DecodesBlobWhenRelationUnpopulated(t *testing.T) {
	orig := model.FilesDataDeserializer
	t.Cleanup(func() { model.FilesDataDeserializer = orig })

	model.FilesDataDeserializer = func(_ []byte) ([]model.TorrentFile, error) {
		return []model.TorrentFile{tf("Inception.2010.mkv", "mkv", 7)}, nil
	}

	tor := model.Torrent{FilesStatus: model.FilesStatusMulti, FilesData: []byte("blob")}

	files, ok := filesForRefine(tor)
	if !ok || len(files) != 1 || files[0].Path != "Inception.2010.mkv" {
		t.Fatalf("expected blob-decoded files, got ok=%v files=%v", ok, files)
	}

	if matched, ok := torrentRefine(tor, refinePredicate{substr: "inception"}); !ok || !matched {
		t.Fatalf("expected decoded-blob torrent to match, got matched=%v ok=%v", matched, ok)
	}
}

// CAVEAT C — a single-file torrent with no file list verifies the substring (and
// path-derived ext / torrent size) against the torrent NAME, mirroring the Rust
// doc builder's single-file name fallback. It must NOT be wrongly dropped.
func TestTorrentRefine_SingleFileNameFallback(t *testing.T) {
	match := model.Torrent{FilesStatus: model.FilesStatusSingle, Name: "Inception.2010.1080p.mkv", Size: 1500}
	if matched, ok := torrentRefine(match, refinePredicate{substr: "inception", extensions: extSet("mkv")}); !ok || !matched {
		t.Fatalf("single-file name should match substring+ext, got matched=%v ok=%v", matched, ok)
	}

	noMatch := model.Torrent{FilesStatus: model.FilesStatusSingle, Name: "Interstellar.2014.mkv", Size: 1500}
	if matched, ok := torrentRefine(noMatch, refinePredicate{substr: "inception"}); !ok || matched {
		t.Fatalf("single-file name without substring must be a clean non-match, got matched=%v ok=%v", matched, ok)
	}

	tooSmall := model.Torrent{FilesStatus: model.FilesStatusSingle, Name: "Inception.mkv", Size: 100}
	if matched, _ := torrentRefine(tooSmall, refinePredicate{substr: "inception", minSize: 1000}); matched {
		t.Fatal("single-file surrogate must honor size bounds via torrent size")
	}
}

// --- refine-before-paginate (the key correctness guarantee) ------------------

type prow struct {
	id      string
	torrent model.Torrent
}

func prowRefine(p refinePredicate) func(prow) (bool, bool) {
	return func(r prow) (bool, bool) { return torrentRefine(r.torrent, p) }
}

// THE lead-requested test: a torrent L3 returned as a candidate (ngram path-bag
// hit) but whose files contain NO real-substring match must be (a) dropped and
// (b) must NOT occupy a page slot — refine happens BEFORE pagination.
func TestRefineBeforePaginate_FalsePositiveDroppedAndDoesNotOccupySlot(t *testing.T) {
	p := refinePredicate{substr: "inception"}

	ordered := []prow{
		{id: "A", torrent: multiFile(tf("Inception.2010.1080p.mkv", "mkv", 1))},
		{id: "B", torrent: multiFile(tf("Independence.Day.mkv", "mkv", 2))}, // false positive
		{id: "C", torrent: multiFile(tf("Inception.2010.2160p.mkv", "mkv", 3))},
		{id: "D", torrent: multiFile(tf("Inception.Soundtrack.flac", "flac", 4))},
	}

	refined, ok := keepMatching(ordered, prowRefine(p))
	if !ok {
		t.Fatal("keepMatching ok=false unexpectedly (all candidates had files)")
	}

	gotIDs := make([]string, len(refined))
	for i, r := range refined {
		gotIDs[i] = r.id
	}

	if !reflect.DeepEqual(gotIDs, []string{"A", "C", "D"}) {
		t.Fatalf("refined ids = %v, want [A C D] (false positive B dropped)", gotIDs)
	}

	page := pageIDs(paginate(refined, 0, 2))
	if !reflect.DeepEqual(page, []string{"A", "C"}) {
		t.Fatalf("page ids = %v, want [A C] — false positive must not occupy a slot", page)
	}

	if page2 := pageIDs(paginate(refined, 2, 2)); !reflect.DeepEqual(page2, []string{"D"}) {
		t.Fatalf("page2 = %v, want [D]", page2)
	}
}

// keepMatching must propagate ok=false (fail loud) when any candidate is
// unrefinable, so the composer falls back rather than truncating.
func TestKeepMatching_FailLoudPropagates(t *testing.T) {
	p := refinePredicate{substr: "inception"}
	ordered := []prow{
		{id: "A", torrent: multiFile(tf("Inception.mkv", "mkv", 1))},
		{id: "X", torrent: model.Torrent{FilesStatus: model.FilesStatusMulti}}, // unrefinable
	}

	if _, ok := keepMatching(ordered, prowRefine(p)); ok {
		t.Fatal("keepMatching must return ok=false when any candidate is unrefinable")
	}
}

func pageIDs(rows []prow) []string {
	ids := make([]string, len(rows))
	for i, r := range rows {
		ids[i] = r.id
	}

	return ids
}

func TestPaginate_OffsetPastEnd(t *testing.T) {
	rows := []prow{{id: "A"}, {id: "B"}}
	if got := paginate(rows, 5, 10); got != nil {
		t.Fatalf("offset past end should yield nil, got %v", got)
	}
}

func TestPaginate_ZeroLimitReturnsAllFromOffset(t *testing.T) {
	rows := []prow{{id: "A"}, {id: "B"}, {id: "C"}}
	if got := pageIDs(paginate(rows, 1, 0)); !reflect.DeepEqual(got, []string{"B", "C"}) {
		t.Fatalf("zero limit from offset 1 = %v, want [B C]", got)
	}
}

func TestDistinctMatchedPaths_CollapseGrouping(t *testing.T) {
	p := refinePredicate{substr: "movie"}
	files := []model.TorrentFile{
		tf("a/Movie.mkv", "mkv", 1),
		tf("b/movie.mkv", "mkv", 2),
		tf("a/Movie.mkv", "mkv", 1), // duplicate path -> collapsed
		tf("c/unrelated.txt", "txt", 3),
	}

	got := distinctMatchedPaths(files, p)
	want := []string{"a/Movie.mkv", "b/movie.mkv"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("distinctMatchedPaths = %v, want %v", got, want)
	}
}
