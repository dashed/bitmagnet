package filesearch

import (
	"errors"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

// Input-hygiene limits (FB-B1d). These are deliberately conservative; the UI
// debounces typeahead and should mirror MinPrefixChars client-side.
const (
	// MaxQueryLen caps a free-text/path query length (runes).
	MaxQueryLen = 256
	// MaxPrefixLen caps a typeahead prefix length (runes).
	MaxPrefixLen = 128
	// MinPrefixChars is the minimum typeahead prefix length. The UI should
	// enforce the same minimum (and debounce ~150–250ms) so we never round-trip
	// 1-char prefixes that match almost everything.
	MinPrefixChars = 2
	// MaxExtensions caps how many extension filters are honoured.
	MaxExtensions = 64
	// MaxExtensionLen caps a single extension length.
	MaxExtensionLen = 32
	// DefaultLimit / MaxLimit bound file-search pagination.
	DefaultLimit = 20
	MaxLimit     = 100
	// DefaultTypeaheadLimit / MaxTypeaheadLimit bound typeahead suggestions.
	DefaultTypeaheadLimit = 10
	MaxTypeaheadLimit     = 25
)

var (
	// ErrPrefixTooShort is returned when a typeahead prefix is shorter than
	// MinPrefixChars after trimming.
	ErrPrefixTooShort = errors.New("typeahead prefix too short")
	// ErrEmptyQuery is returned when a file search has neither a query, an
	// extension filter, a size bound, nor an info hash to constrain it.
	ErrEmptyQuery = errors.New("file search requires a query, extension, size bound or info hash")
)

var knownFacetFields = map[string]struct{}{
	"extension": {},
}

// likeEscaper escapes the three LIKE/ILIKE metacharacters so user input is
// treated as a literal substring, never a pattern. Backslash MUST be escaped
// first. The result is meant to be used with an explicit `ESCAPE '\'` clause (or
// a parameterised pattern) on the SQL side.
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// EscapeLikePattern escapes %, _ and \ in s so it can be embedded safely inside
// a LIKE/ILIKE pattern (FB-B1d).
func EscapeLikePattern(s string) string {
	return likeEscaper.Replace(s)
}

// capRunes truncates s to at most n runes (not bytes), so multi-byte/CJK input
// is never split mid-rune.
func capRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}

	return string(r[:n])
}

func clampLimit(limit, def, maxLimit uint) uint {
	if limit == 0 {
		return def
	}

	if limit > maxLimit {
		return maxLimit
	}

	return limit
}

func normalizeExtensions(in []string) []string {
	if len(in) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(in))

	out := make([]string, 0, len(in))

	for _, e := range in {
		e = strings.ToLower(strings.TrimSpace(e))
		e = strings.TrimPrefix(e, ".")
		e = capRunes(e, MaxExtensionLen)

		if e == "" {
			continue
		}

		if _, ok := seen[e]; ok {
			continue
		}

		seen[e] = struct{}{}

		out = append(out, e)

		if len(out) >= MaxExtensions {
			break
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func normalizeFacetFields(in []string) []string {
	if len(in) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))

	for _, field := range in {
		field = strings.ToLower(strings.TrimSpace(field))
		if _, ok := knownFacetFields[field]; !ok {
			continue
		}

		if _, ok := seen[field]; ok {
			continue
		}

		seen[field] = struct{}{}
		out = append(out, field)
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

// FileSearchParams is the loosely-typed input the resolver passes in; it is
// validated and normalised by NewFileSearchInput.
type FileSearchParams struct {
	Query      string
	Extensions []string
	MinSize    uint64
	MaxSize    uint64
	InfoHash   *protocol.ID
	Sort       []FileSort
	Limit      uint
	Offset     uint
	TotalCount *bool
}

type FacetsParams struct {
	Query      string
	Extensions []string
	MinSize    uint64
	MaxSize    uint64
	Fields     []string
}

// NewFileSearchInput validates and normalises raw params into a FileSearchInput.
// It length-caps the query, escapes it for LIKE backends, normalises/dedupes
// extensions, clamps the limit, and rejects a wholly-unconstrained search.
func NewFileSearchInput(p FileSearchParams) (FileSearchInput, error) {
	query := capRunes(strings.TrimSpace(p.Query), MaxQueryLen)
	exts := normalizeExtensions(p.Extensions)

	if err := validateFileSorts(p.Sort, query); err != nil {
		return FileSearchInput{}, err
	}

	if query == "" && len(exts) == 0 && p.MinSize == 0 && p.MaxSize == 0 && p.InfoHash == nil {
		return FileSearchInput{}, ErrEmptyQuery
	}

	return FileSearchInput{
		Query:            query,
		QueryLikePattern: EscapeLikePattern(query),
		Extensions:       exts,
		MinSize:          p.MinSize,
		MaxSize:          p.MaxSize,
		InfoHash:         p.InfoHash,
		Sort:             p.Sort,
		Limit:            clampLimit(p.Limit, DefaultLimit, MaxLimit),
		Offset:           p.Offset,
		SkipTotalCount:   p.TotalCount != nil && !*p.TotalCount,
	}, nil
}

func NewFacetsInput(p FacetsParams) (FacetsInput, error) {
	query := capRunes(strings.TrimSpace(p.Query), MaxQueryLen)
	likePattern := ""
	if query != "" {
		likePattern = EscapeLikePattern(query)
	}

	return FacetsInput{
		Query:            query,
		QueryLikePattern: likePattern,
		Extensions:       normalizeExtensions(p.Extensions),
		MinSize:          p.MinSize,
		MaxSize:          p.MaxSize,
		Fields:           normalizeFacetFields(p.Fields),
	}, nil
}

func validateFileSorts(sorts []FileSort, query string) error {
	for _, sort := range sorts {
		if query == "" && IsTorrentFieldSort(sort.Field) {
			return ErrTorrentSortRequiresTextQuery
		}
	}

	return nil
}

// NewPathTypeaheadInput validates and normalises a typeahead prefix. It enforces
// MinPrefixChars, length-caps and escapes the prefix, and clamps the limit.
func NewPathTypeaheadInput(prefix string, limit uint) (PathTypeaheadInput, error) {
	prefix = strings.TrimSpace(prefix)
	if len([]rune(prefix)) < MinPrefixChars {
		return PathTypeaheadInput{}, ErrPrefixTooShort
	}

	prefix = capRunes(prefix, MaxPrefixLen)

	return PathTypeaheadInput{
		Prefix:            prefix,
		PrefixLikePattern: EscapeLikePattern(prefix),
		Limit:             clampLimit(limit, DefaultTypeaheadLimit, MaxTypeaheadLimit),
	}, nil
}
