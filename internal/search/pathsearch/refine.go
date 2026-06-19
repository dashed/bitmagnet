package pathsearch

import (
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
)

// refinePredicate is the exact-refine filter derived from the typed search
// input. L3 ngram path-bag recall is a SUPERSET, so the composer MUST verify the
// real case-insensitive path substring (substr) to drop torrent-level false
// positives, then apply the structured extension/size predicate — none of which
// L3 carries (PathCandidate has no path text, extension, or size).
type refinePredicate struct {
	// substr is the lower-cased real path substring to verify. Required: the
	// composer only takes the L3 route when there is path text to match.
	substr string
	// extensions is the set of allowed lower-cased extensions; empty = any.
	// Built from the typed file-type/extension filters, expanded via
	// model.FileType.Extensions().
	extensions map[string]struct{}
	// minSize / maxSize bound file size in bytes; 0 = unbounded on that side.
	minSize uint
	maxSize uint
}

// hasExtensionFilter reports whether an extension predicate is active.
func (p refinePredicate) hasExtensionFilter() bool { return len(p.extensions) > 0 }

// fileExtension returns the file's extension, lower-cased, deriving it from the
// path when the stored/blob Extension is empty. The blob's `e` field can be
// empty for crawl-path torrents (the G1 issue), so path derivation is required
// for correct extension filtering; this mirrors the live PG generated-column
// semantics (model.FileExtensionFromPath).
func fileExtension(f model.TorrentFile) string {
	if f.Extension.Valid && f.Extension.String != "" {
		return strings.ToLower(f.Extension.String)
	}

	return model.FileExtensionFromPath(f.Path).String // already lower-cased; "" when none
}

// matchFile reports whether a single file satisfies the predicate.
func matchFile(f model.TorrentFile, p refinePredicate) bool {
	if p.substr != "" && !strings.Contains(strings.ToLower(f.Path), p.substr) {
		return false
	}

	if p.hasExtensionFilter() {
		if _, ok := p.extensions[fileExtension(f)]; !ok {
			return false
		}
	}

	if p.minSize > 0 && f.Size < p.minSize {
		return false
	}

	if p.maxSize > 0 && f.Size > p.maxSize {
		return false
	}

	return true
}

// matchedFiles returns the files of a torrent that satisfy the predicate, in
// input order.
func matchedFiles(files []model.TorrentFile, p refinePredicate) []model.TorrentFile {
	var out []model.TorrentFile

	for _, f := range files {
		if matchFile(f, p) {
			out = append(out, f)
		}
	}

	return out
}

// torrentMatches reports whether a torrent keeps >=1 matching file. A candidate
// from L3 with zero matching files is a recall false positive and is dropped.
func torrentMatches(files []model.TorrentFile, p refinePredicate) bool {
	for _, f := range files {
		if matchFile(f, p) {
			return true
		}
	}

	return false
}

// filesForRefine resolves the file list to verify a candidate against. t.Files is
// NOT guaranteed populated for the L3 route: gqlmodel.Search hydrates torrents
// with config.files=false, so the torrent_files relation is not preloaded — the
// list is filled only by Torrent.AfterFind decoding the L1 FilesData blob. The
// composer therefore resolves files defensively rather than leaning on implicit
// AfterFind:
//
//	preloaded/AfterFind-decoded t.Files -> else explicit FilesData blob decode
//	-> else single-file name surrogate (CAVEAT C) -> else ok=false (CAVEAT B).
//
// ok=false means a candidate's files were genuinely unobtainable (a projection
// that omitted files_data, or — see CAVEAT C — a single-file torrent under an
// extension predicate where only the name is available); the caller must fail
// loud / fall back, NEVER silently drop it (that would hide real matches = worse
// than today).
//
// The predicate p is consulted ONLY for the single-file name-surrogate guard
// (CAVEAT C, #9): the rest of the resolution is predicate-independent.
func filesForRefine(t model.Torrent, p refinePredicate) (files []model.TorrentFile, ok bool) {
	if len(t.Files) > 0 {
		return t.Files, true
	}

	if len(t.FilesData) > 0 && model.FilesDataDeserializer != nil {
		if decoded, err := model.FilesDataDeserializer(t.FilesData); err == nil && len(decoded) > 0 {
			return decoded, true
		}
	}

	// No file list available. A single-file torrent legitimately carries its one
	// file as the torrent name — refine against a name surrogate so the same
	// matchFile predicate applies. This mirrors the Rust doc builder's single-file
	// name fallback. (CAVEAT C)
	//
	// SOUNDNESS (#9): for a single-file torrent the substring and size clauses are
	// trustworthy against the name surrogate — the name IS essentially the filename
	// (PG's own search matches the name/tsv), and t.Size IS the single file's size
	// (a single-file torrent has exactly one file == the whole payload). But the
	// EXTENSION clause is NOT: the display name is not a reliable carrier of the
	// real file extension (many single-file torrents are named release-style with
	// no extension, or end in an ext-looking token that isn't the file's), so a
	// path-derived ext from the name could wrongly include/exclude. So when an
	// extension predicate is active and we have only the name, we FAIL LOUD
	// (ok=false) — mirroring the multi-file no-files case — rather than serve a
	// confidently-wrong ext match. Without an ext filter the surrogate is sound and
	// is used as before.
	if t.SingleFile() {
		if p.hasExtensionFilter() {
			return nil, false
		}

		return []model.TorrentFile{{Path: t.Name, Size: t.Size}}, true
	}

	// Multi-file torrent with no obtainable file list: cannot verify.
	return nil, false
}

// torrentRefine reports whether a candidate torrent keeps a real match, and
// whether it could be refined at all (ok). ok=false propagates the fail-loud
// signal from filesForRefine. (CAVEAT B + C)
func torrentRefine(t model.Torrent, p refinePredicate) (matched, ok bool) {
	resolved, ok := filesForRefine(t, p)
	if !ok {
		return false, false
	}

	return torrentMatches(resolved, p), true
}

// keepMatching returns, in input (PG-ordered) order, only the rows that keep a
// real match — i.e. L3 torrent-level false positives (path-bag ngram hit but no
// file whose REAL path contains the substring) are dropped. The refine accessor
// returns (matched, ok) per row; ok=false (a candidate whose files were
// unobtainable) short-circuits and returns ok=false so the composer fails loud /
// falls back to the plain PG path rather than serving a silently truncated
// result. (CAVEAT B)
//
// CRITICAL: this runs BEFORE pagination, so a false positive never occupies a
// page slot and never inflates the page; PG must therefore NOT have applied the
// user's page limit/offset to the candidate IN-list (only OrderBy + a generous
// oversample bound). See paginate.
func keepMatching[T any](rows []T, refine func(T) (matched, ok bool)) (kept []T, ok bool) {
	kept = make([]T, 0, len(rows))

	for _, r := range rows {
		matched, rok := refine(r)
		if !rok {
			return nil, false
		}

		if matched {
			kept = append(kept, r)
		}
	}

	return kept, true
}

// paginate applies offset/limit in Go over the already-refined+ordered set. This
// is the ONLY place the user's page window is applied for the L3 route. The
// total it would report is ESTIMATED (L3 candidate_total is torrent-doc recall,
// not an exact refined-file count); the composer sets TotalCountIsEstimate=true.
func paginate[T any](rows []T, offset, limit uint) []T {
	if offset >= uint(len(rows)) {
		return nil
	}

	rows = rows[offset:]
	if limit > 0 && limit < uint(len(rows)) {
		rows = rows[:limit]
	}

	return rows
}

// distinctMatchedPaths is the collapse:path core: the de-duplicated set of real
// matched paths, across the refined candidate set, in first-seen order.
func distinctMatchedPaths(files []model.TorrentFile, p refinePredicate) []string {
	seen := make(map[string]struct{})

	var out []string

	for _, f := range matchedFiles(files, p) {
		if _, ok := seen[f.Path]; ok {
			continue
		}

		seen[f.Path] = struct{}{}
		out = append(out, f.Path)
	}

	return out
}
