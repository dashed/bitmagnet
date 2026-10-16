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

// CAVEAT C / #9 — a single-file torrent with no file list verifies the substring
// and torrent size (both sound: the name IS the filename and t.Size IS the single
// file's size) against the torrent NAME, mirroring the Rust doc builder's
// single-file name fallback. It must NOT be wrongly dropped. The extension clause
// is covered separately in TestTorrentRefine_SingleFileExtFilterRefinesAgainstName.
func TestTorrentRefine_SingleFileNameFallback(t *testing.T) {
	// substring-only: sound surrogate, matches.
	match := model.Torrent{FilesStatus: model.FilesStatusSingle, Name: "Inception.2010.1080p.mkv", Size: 1500}
	if matched, ok := torrentRefine(match, refinePredicate{substr: "inception"}); !ok || !matched {
		t.Fatalf("single-file name should match substring, got matched=%v ok=%v", matched, ok)
	}

	// substring + size: size is sound for single-file (t.Size == the one file).
	if matched, ok := torrentRefine(match, refinePredicate{substr: "inception", minSize: 1000}); !ok || !matched {
		t.Fatalf("single-file name+size should match, got matched=%v ok=%v", matched, ok)
	}

	noMatch := model.Torrent{FilesStatus: model.FilesStatusSingle, Name: "Interstellar.2014.mkv", Size: 1500}
	if matched, ok := torrentRefine(noMatch, refinePredicate{substr: "inception"}); !ok || matched {
		t.Fatalf(
			"single-file name without substring must be a clean non-match, got matched=%v ok=%v",
			matched,
			ok,
		)
	}

	tooSmall := model.Torrent{FilesStatus: model.FilesStatusSingle, Name: "Inception.mkv", Size: 100}
	if matched, _ := torrentRefine(tooSmall, refinePredicate{substr: "inception", minSize: 1000}); matched {
		t.Fatal("single-file surrogate must honor size bounds via torrent size")
	}
}

// CAVEAT C / #9 — an extension predicate against a single-file name surrogate is
// SOUND: fileExtension() derives the ext from the name via
// model.FileExtensionFromPath, which is byte-identical to PG's generated
// torrents.extension column, model.Torrent.FileExtensions(), and the Tantivy doc
// builder — all of which take single-file extension from the NAME via the same
// regex. So the ext clause refines against the name like substring and size: a
// name-derived ext matching the filter is KEPT, a non-matching one is a clean
// drop, and the route NEVER falls back (ok stays true). The old behavior fail-loud
// (ok=false) forced every ext-faceted path query to PG.
func TestTorrentRefine_SingleFileExtFilterRefinesAgainstName(t *testing.T) {
	single := func(name string) model.Torrent {
		return model.Torrent{FilesStatus: model.FilesStatusSingle, Name: name, Size: 1500}
	}
	p := refinePredicate{substr: "inception", extensions: extSet("mkv")}

	// name-derived ext matches the filter -> kept, no fallback.
	if matched, ok := torrentRefine(single("Inception.2010.1080p.mkv"), p); !ok || !matched {
		t.Fatalf(
			"single-file name ext matching the filter must be kept (ok=true, matched=true), got matched=%v ok=%v",
			matched,
			ok,
		)
	}

	// name-derived ext does NOT match the filter -> clean drop, still no fallback.
	if matched, ok := torrentRefine(single("Inception.2010.1080p.avi"), p); !ok || matched {
		t.Fatalf(
			"single-file name ext not matching the filter must be a clean drop (ok=true, matched=false), got matched=%v ok=%v",
			matched,
			ok,
		)
	}

	// the surrogate is always usable now (ok=true), independent of any predicate.
	if _, ok := filesForRefine(single("Inception.2010.1080p.mkv")); !ok {
		t.Fatal("single-file name surrogate must always be usable (ok=true)")
	}
}

// --- F1 name rescue: keep a name-only match, but only when sound -------------

// A multi-file torrent whose FILES never contain the term but whose NAME does is
// kept when unfiltered (matching PG name-search semantics). The extension/size
// predicate is empty, so the name surrogate is sound.
func TestNameRescue_KeepsNameOnlyMatchUnfiltered(t *testing.T) {
	p := refinePredicate{substr: "sorefordays"}

	if !nameMatches("OmegaPACK.SoreForDays.Complete", p) {
		t.Fatal("name-only match with no filters must be rescued")
	}

	// Case-insensitive, mirroring PG lower-cased tsv/name search.
	if !nameMatches("omegapack.SOREFORDAYS.complete", p) {
		t.Fatal("name rescue must be case-insensitive")
	}

	if nameMatches("OmegaPACK.Something.Else", p) {
		t.Fatal("name without the substring must not be rescued")
	}
}

// The rescue is UNSOUND under an extension or size filter (a name carries neither
// a file extension nor a per-file size), so it must return false and let the
// candidate fall through to a normal drop.
func TestNameRescue_DropsUnderExtensionOrSizeFilter(t *testing.T) {
	name := "OmegaPACK.SoreForDays.Complete"

	if nameMatches(name, refinePredicate{substr: "sorefordays", extensions: extSet("mkv")}) {
		t.Fatal("name rescue must be disabled when an extension filter is active")
	}

	if nameMatches(name, refinePredicate{substr: "sorefordays", minSize: 1}) {
		t.Fatal("name rescue must be disabled when a min-size bound is active")
	}

	if nameMatches(name, refinePredicate{substr: "sorefordays", maxSize: 1}) {
		t.Fatal("name rescue must be disabled when a max-size bound is active")
	}
}

// End-to-end at the refineMatches decision: OmegaPACK-shaped candidate — 0 files
// match, term only in the name. Kept when unfiltered, dropped once any
// extension/size filter is present. This mirrors the composer's keep-decision
// `torrentMatches(files, pred) || nameMatches(name, pred)`.
func TestNameRescue_OmegaPACKShapedKeepDecision(t *testing.T) {
	files := []model.TorrentFile{
		tf("disc1/track01.flac", "flac", 10),
		tf("disc1/track02.flac", "flac", 20),
	}
	name := "OmegaPACK.SoreForDays.Complete"

	keep := func(p refinePredicate) bool {
		return torrentMatches(files, p) || nameMatches(name, p)
	}

	if !keep(refinePredicate{substr: "sorefordays"}) {
		t.Fatal("unfiltered name-only candidate must be kept")
	}

	if keep(refinePredicate{substr: "sorefordays", extensions: extSet("flac")}) {
		t.Fatal("name-only candidate must be dropped when an extension filter is present")
	}

	if keep(refinePredicate{substr: "sorefordays", minSize: 5}) {
		t.Fatal("name-only candidate must be dropped when a size filter is present")
	}
}

// A no_info torrent (no file list by nature) must be REFINABLE (ok=true) with an
// empty file list — NOT fail-loud — so the name rescue can keep it. A genuine
// multi-file torrent whose files are unobtainable still fails loud (CAVEAT B).
func TestFilesForRefine_NoInfoIsEmptyRefinableNotFailLoud(t *testing.T) {
	for _, status := range []model.FilesStatus{model.FilesStatusNoInfo, model.FilesStatusOverThreshold} {
		tor := model.Torrent{FilesStatus: status, Name: "OmegaPACK.SoreForDays.Complete"}

		files, ok := filesForRefine(tor)
		if !ok {
			t.Fatalf("%s torrent must be refinable (ok=true), not fail-loud", status)
		}

		if len(files) != 0 {
			t.Fatalf("%s torrent must resolve to an empty file list, got %v", status, files)
		}

		// Unfiltered: kept by name rescue at the composer's keep-decision.
		p := refinePredicate{substr: "sorefordays"}
		if !torrentMatches(files, p) && !nameMatches(tor.Name, p) {
			t.Fatalf("%s name-only torrent must be kept when unfiltered", status)
		}

		// Under an extension filter: cannot be satisfied → dropped (PG-consistent).
		pf := refinePredicate{substr: "sorefordays", extensions: extSet("mkv")}
		if torrentMatches(files, pf) || nameMatches(tor.Name, pf) {
			t.Fatalf("%s name-only torrent must be dropped under an extension filter", status)
		}
	}

	// A genuine multi-file torrent with no obtainable files is STILL fail-loud.
	bad := model.Torrent{FilesStatus: model.FilesStatusMulti, Name: "has.the.term"}
	if _, ok := filesForRefine(bad); ok {
		t.Fatal("multi-file torrent with no obtainable files must stay fail-loud (ok=false)")
	}
}

// --- F11 token-AND candidate keep --------------------------------------------

func TestTokenizeQuery(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"", []string{}},
		{"   ", []string{}},
		{"inception", []string{"inception"}},
		{"omegapack sorefordays", []string{"omegapack", "sorefordays"}},
		{"  omegapack   sorefordays  ", []string{"omegapack", "sorefordays"}},
		{"a\tb\nc", []string{"a", "b", "c"}},
	} {
		got := tokenizeQuery(tc.in)
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("tokenizeQuery(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// A single-token query must be byte-identical to the pre-F11 keep decision
// `torrentMatches(files, p) || nameMatches(name, p)` for every filter shape.
func TestTorrentTokenMatch_SingleTokenIdenticalToLegacyKeep(t *testing.T) {
	files := []model.TorrentFile{
		tf("movies/Inception.2010.1080p.mkv", "mkv", 1500),
		tf("movies/readme.txt", "txt", 10),
	}
	name := "Inception.2010.Bluray"

	for _, f := range []Filters{
		{Query: "inception"},
		{Query: "inception", Extensions: []string{"mkv"}},
		{Query: "inception", Extensions: []string{"avi"}},
		{Query: "readme"},                              // matches a file path, not the name
		{Query: "bluray"},                              // matches the name only (rescue)
		{Query: "bluray", Extensions: []string{"mkv"}}, // rescue disabled by ext filter
		{Query: "inception", MinSize: 1000},
		{Query: "inception", MinSize: 2000},
		{Query: "absent"},
	} {
		p := f.predicate()
		legacy := torrentMatches(files, p) || nameMatches(name, p)

		if got := torrentTokenMatch(files, name, p); got != legacy {
			t.Fatalf("query=%q filters=%+v: torrentTokenMatch=%v, legacy=%v", f.Query, f, got, legacy)
		}
	}
}

// The live regression: "OmegaPACK SoreForDays" against a torrent whose name/paths
// carry both words but NOT the verbatim phrase. Each token lives in a different
// string (one in the name, one in a path) — token-AND must keep it, while the
// pre-F11 verbatim-substring keep dropped it.
func TestTorrentTokenMatch_UnionAcrossNameAndPaths(t *testing.T) {
	files := []model.TorrentFile{
		tf("Emily Willis/SoreForDays - Part 1.mp4", "mp4", 100),
	}
	name := "Emily Willis - OmegaPACK Collection"
	p := Filters{Query: "OmegaPACK SoreForDays"}.predicate()

	if !torrentTokenMatch(files, name, p) {
		t.Fatal("both tokens present across name+paths must be kept (F11)")
	}

	// Pre-F11 verbatim keep drops it: the phrase is in neither the name nor a path.
	if torrentMatches(files, p) || nameMatches(name, p) {
		t.Fatal("guard: the verbatim phrase must NOT be a substring anywhere (else the test proves nothing)")
	}
}

// Both tokens in the same string (a single file path) is still a match.
func TestTorrentTokenMatch_BothTokensInOnePath(t *testing.T) {
	files := []model.TorrentFile{
		tf("shows/omegapack.sorefordays.part1.mkv", "mkv", 100),
	}
	p := Filters{Query: "omegapack sorefordays"}.predicate()

	if !torrentTokenMatch(files, "unrelated name", p) {
		t.Fatal("both tokens in one path must be kept")
	}
}

// One token absent everywhere → dropped, even if the other token matches.
func TestTorrentTokenMatch_MissingTokenDrops(t *testing.T) {
	files := []model.TorrentFile{
		tf("Emily Willis/SoreForDays - Part 1.mp4", "mp4", 100),
	}
	name := "Emily Willis Collection"
	p := Filters{Query: "OmegaPACK SoreForDays"}.predicate()

	if torrentTokenMatch(files, name, p) {
		t.Fatal("omegapack token is absent from name+paths → candidate must be dropped")
	}
}

func TestTorrentTokenMatch_CaseInsensitive(t *testing.T) {
	files := []model.TorrentFile{
		tf("DISC1/SoreForDays.MKV", "MKV", 100),
	}
	name := "OMEGAPACK release"
	p := Filters{Query: "omegapack sorefordays"}.predicate()

	if !torrentTokenMatch(files, name, p) {
		t.Fatal("token match must be case-insensitive across name+paths")
	}
}

// An empty query yields zero tokens; the route is gated on substr!="" before
// refine, but torrentTokenMatch must fail-closed (drop) rather than keep-all.
func TestTorrentTokenMatch_EmptyQueryDrops(t *testing.T) {
	files := []model.TorrentFile{tf("anything.mkv", "mkv", 1)}
	for _, q := range []string{"", "   "} {
		p := Filters{Query: q}.predicate()
		if len(p.tokens) != 0 {
			t.Fatalf("query %q should tokenize to zero tokens, got %v", q, p.tokens)
		}

		if torrentTokenMatch(files, "any name", p) {
			t.Fatalf("empty-token predicate (query %q) must drop, not keep-all", q)
		}
	}
}

// Superset property for multi-word: a candidate matched by the pre-F11 verbatim
// phrase (the space-joined query is a literal substring of one path) is STILL
// kept under token-AND. Token-AND is a strict superset of the old keep, so the
// verbatim case must never regress.
func TestTorrentTokenMatch_MultiTokenVerbatimSuperset(t *testing.T) {
	files := []model.TorrentFile{
		tf("movies/foo bar/release.mkv", "mkv", 100),
	}
	p := Filters{Query: "foo bar"}.predicate()

	// Precondition: this IS a verbatim-phrase match the pre-F11 keep would take.
	if !torrentMatches(files, p) {
		t.Fatal("guard: the space-joined phrase must be a literal path substring here")
	}

	if !torrentTokenMatch(files, "unrelated name", p) {
		t.Fatal("a verbatim multi-word phrase match must remain kept under token-AND")
	}
}

// Multi-token under a SIZE bound (symmetric to the extension-filter case): the
// name rescue is OFF under a size bound, so a token that appears ONLY in a file
// excluded by the size bound cannot be rescued → drop.
func TestTorrentTokenMatch_MultiTokenUnderSizeBound(t *testing.T) {
	files := []model.TorrentFile{
		tf("omegapack/sorefordays.part1.mkv", "mkv", 5_000),
		tf("omegapack/sample.mkv", "mkv", 5), // 'sample' only lives in the too-small file
	}
	name := "OmegaPACK SoreForDays Sample" // both tokens, but name ineligible under a size bound

	// Both tokens live in the in-bounds file → kept.
	kept := Filters{Query: "omegapack sorefordays", MinSize: 1_000}.predicate()
	if !torrentTokenMatch(files, name, kept) {
		t.Fatal("both tokens in an in-bounds file must be kept under the size bound")
	}

	// 'sample' only appears in the size-excluded file; the name cannot rescue it
	// under a size bound → dropped.
	dropped := Filters{Query: "sorefordays sample", MinSize: 1_000}.predicate()
	if torrentTokenMatch(files, name, dropped) {
		t.Fatal("a token that only matches a size-excluded file must not be rescued by the name")
	}
}

// Multi-token under an extension filter: the name rescue is OFF, so EVERY token
// must be found in a path of a file that passes the extension filter.
func TestTorrentTokenMatch_MultiTokenUnderExtensionFilter(t *testing.T) {
	files := []model.TorrentFile{
		tf("omegapack/sorefordays.part1.mkv", "mkv", 100),
		tf("omegapack/sample.avi", "avi", 5),
	}
	name := "OmegaPACK SoreForDays" // both tokens, but name is ineligible under ext filter

	// Both tokens live in the mkv path → kept.
	kept := Filters{Query: "omegapack sorefordays", Extensions: []string{"mkv"}}.predicate()
	if !torrentTokenMatch(files, name, kept) {
		t.Fatal("both tokens present in an mkv path must be kept under the mkv filter")
	}

	// 'part1' only appears in the mkv path here; 'avi' token only in the avi path,
	// which is excluded by the mkv filter → dropped (name cannot rescue under a filter).
	dropped := Filters{Query: "sorefordays avi", Extensions: []string{"mkv"}}.predicate()
	if torrentTokenMatch(files, name, dropped) {
		t.Fatal("a token that only matches an ext-excluded path must not be rescued by the name")
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
