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
	// composer only takes the L3 route when there is path text to match. It stays
	// the whole verbatim query — file-LEVEL filtering (matchFile / matchedFiles)
	// and the single-token candidate keep both verify it unchanged.
	substr string
	// tokens is the lower-cased whitespace-split query, used by the token-AND
	// candidate keep (torrentTokenMatch). A single-token query has tokens ==
	// []string{substr}, so the keep decision stays byte-identical to the verbatim
	// substr match; multi-word queries pass iff EVERY token matches somewhere in
	// the union of the name and file paths (F11).
	tokens []string
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

// nameMatches reports whether a candidate whose FILES do not match should still
// be kept because the search substring is present in its torrent display NAME.
//
// L3's path-bag now indexes the torrent name too (F1), for every files_status —
// including the ~21.9M no_info torrents that carry no file list at all and the
// ~17.5M multi-file torrents whose term lives only in the name. The file-level
// exact refine would still drop these, because no file path contains the term.
// This keeps them, matching PostgreSQL name-search semantics (PG matches the
// name/tsv, not arbitrary file paths).
//
// SOUNDNESS (CAVEAT C): a name carries the substring but NOT any file's extension
// or size. The rescue is therefore sound ONLY when no extension filter and no
// size bound is active; under either filter a name-only candidate cannot be
// proven to satisfy it and MUST fall through to a normal DROP (never fail-loud —
// it is a genuine non-match, not an unobtainable file list). A rescued torrent
// keeps whatever file list it has (possibly empty) and needs no >=1-matched-file
// invariant.
func nameMatches(name string, p refinePredicate) bool {
	if p.substr == "" || p.hasExtensionFilter() || p.minSize > 0 || p.maxSize > 0 {
		return false
	}

	return strings.Contains(strings.ToLower(name), p.substr)
}

// tokenizeQuery splits a lower-cased query into its whitespace-separated tokens,
// dropping empty tokens. It is fed the already lower-cased+trimmed substr, so
// strings.Fields (Unicode-whitespace split, empties dropped) yields the F11
// token set directly. A single-word query yields exactly []string{substr}.
func tokenizeQuery(loweredQuery string) []string {
	return strings.Fields(loweredQuery)
}

// torrentTokenMatch is the F11 token-AND candidate keep: a candidate is kept iff
// EVERY query token appears (case-insensitive substring) SOMEWHERE in the union
// of the torrent name and its file paths — tokens may match in different strings.
// This mirrors PostgreSQL FTS, which ANDs lexemes across the whole torrent tsv
// (name + paths) rather than requiring the verbatim phrase.
//
// It generalizes the pre-F11 keep decision (torrentMatches || nameMatches) by
// evaluating each token as its own single-substring predicate over the SAME
// structured extension/size filters: a token is satisfied when some file whose
// extension/size pass the filter has the token in its path (torrentMatches), or
// when the F1 name rescue is open and the name carries it (nameMatches). Reusing
// those two helpers keeps every existing soundness guard — the ext/size coupling
// on a single file and the name-rescue guard — intact per token.
//
// For a single-token query (tokens == []string{substr}) this is byte-identical
// to the old torrentMatches || nameMatches decision.
func torrentTokenMatch(files []model.TorrentFile, name string, p refinePredicate) bool {
	if len(p.tokens) == 0 {
		return false
	}

	for _, tok := range p.tokens {
		tp := p
		tp.substr = tok

		if !torrentMatches(files, tp) && !nameMatches(name, tp) {
			return false
		}
	}

	return true
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
// ok=false means a candidate's files were genuinely unobtainable — a multi-file
// projection that omitted files_data with no single-file name to fall back on;
// the caller must fail loud / fall back, NEVER silently drop it (that would hide
// real matches = worse than today).
func filesForRefine(t model.Torrent) (files []model.TorrentFile, ok bool) {
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
	// matchFile predicate applies to substring, extension, and size alike. This
	// mirrors the Rust doc builder's single-file name fallback. (CAVEAT C)
	//
	// SOUNDNESS INVARIANT (#9): every clause is trustworthy against the name
	// surrogate, so there is nothing to fail loud about. The substring and size
	// clauses are sound because the name IS the filename (PG's own search matches
	// the name/tsv) and t.Size IS the single file's size (a single-file torrent has
	// exactly one file == the whole payload). The EXTENSION clause is EQUALLY sound:
	// fileExtension() derives it from the name via model.FileExtensionFromPath, and
	// for a single-file torrent that derivation is byte-identical to every other
	// place the stack computes single-file extension — PG's generated
	// torrents.extension column (migrations/00002_files_status.sql), model.Torrent
	// .FileExtensions(), and the Tantivy doc builder (fileExtensionsForDoc) ALL take
	// the single-file extension from the NAME via the same regex. A name-surrogate
	// ext match therefore agrees with the PG column and the index by construction.
	if t.SingleFile() {
		return []model.TorrentFile{{Path: t.Name, Size: t.Size}}, true
	}

	// A no_info / over_threshold torrent has no stored file list BY NATURE (not a
	// missing-blob failure): no_info never had one, over_threshold's was too large
	// to persist. Resolve it to an EMPTY file list (ok=true) so the name-rescue in
	// the composer can keep it on the name path — mirroring PG, which matches such
	// torrents by name/tsv regardless of files, and correctly drops them under an
	// extension/size filter (they have no file rows to satisfy one). This is NOT a
	// CAVEAT-B fail-loud: those statuses legitimately carry no files, so there is
	// nothing "unobtainable" to fall back for.
	if t.FilesStatus == model.FilesStatusNoInfo || t.FilesStatus == model.FilesStatusOverThreshold {
		return nil, true
	}

	// Multi-file torrent whose files SHOULD exist but are unobtainable: cannot
	// verify — fail loud (CAVEAT B).
	return nil, false
}

// torrentRefine reports whether a candidate torrent keeps a real match, and
// whether it could be refined at all (ok). ok=false propagates the fail-loud
// signal from filesForRefine. (CAVEAT B)
func torrentRefine(t model.Torrent, p refinePredicate) (matched, ok bool) {
	resolved, ok := filesForRefine(t)
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
