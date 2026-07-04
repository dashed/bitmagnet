// Package filesearch defines the Go-side contract the GraphQL fileSearch +
// pathTypeahead resolvers depend on. It is a thin, transport-neutral interface
// so the real implementation — a gRPC client for the DuckDB file-search sidecar
// (DV-2) and the path-FTS sidecar (DV-3) — can be wired in later without
// touching the resolver layer.
//
// Until that sidecar is deployed and the FileSearchEnabled feature flag is
// flipped, the resolvers use the Disabled() client, which rejects every call
// with ErrDisabled. All input validation/hygiene (FB-B1d) lives here so it is
// enforced identically regardless of which client backs the interface.
package filesearch

import (
	"context"
	"errors"
	"strings"

	dbsearch "github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

// ErrDisabled is returned by the Disabled() client and by any client whose
// backing feature flag / sidecar is not enabled.
var ErrDisabled = errors.New("file search is not enabled")

var (
	// ErrInfoHashUnsupported is returned when a caller requests a single-torrent
	// file search. The current FileSearchService proto has no info_hash filter, so
	// returning broader cross-corpus rows would be incorrect.
	ErrInfoHashUnsupported = errors.New("file search info_hash filter is not supported by the sidecar")
	// ErrOffsetUnsupported is returned when offset pagination cannot be emulated
	// within the configured sidecar result window.
	ErrOffsetUnsupported = errors.New("file search offset exceeds the sidecar result window")
	// ErrPathTypeaheadUnsupported is returned by the real sidecar-backed client
	// until the proto grows a path-typeahead RPC.
	ErrPathTypeaheadUnsupported = errors.New("path typeahead is not supported by the sidecar")
	// ErrTorrentSortRequiresTextQuery is returned when a torrent-content sort is
	// requested for the L2 empty-query path, which only understands file fields.
	ErrTorrentSortRequiresTextQuery = errors.New("file search torrent-field sorts require a non-empty text query")
	// ErrTorrentSortRequiresRoutedPath is returned when a torrent-content sort
	// cannot be served by the L3 routed text path and would otherwise hit L2.
	ErrTorrentSortRequiresRoutedPath = errors.New(
		"file search torrent-field sorts require the routed text-search path",
	)
)

const (
	FileSortSize          = "size"
	FileSortPath          = "path"
	FileSortExtension     = "extension"
	FileSortIndex         = "index"
	FileSortInfoHash      = "info_hash"
	FileSortLastSeen      = "last_seen"
	FileSortDHTLastSeenAt = "dht_last_seen_at"
	FileSortSeeders       = "seeders"
	FileSortPublishedAt   = "published_at"
	FileSortUpdatedAt     = "updated_at"
)

// FileSearchInput is the validated input to a file-grained search. Build it with
// NewFileSearchInput so the hygiene rules (length caps, LIKE-metachar escaping,
// limit clamping) are always applied.
type FileSearchInput struct {
	// Query is the free-text / path query, already length-capped. The raw form
	// is kept for engines that tokenise it themselves; QueryLikePattern is the
	// escaped form for ILIKE/LIKE backends.
	Query string
	// QueryLikePattern is Query with %, _ and \ escaped (FB-B1d), safe to embed
	// in a LIKE/ILIKE pattern.
	QueryLikePattern string
	// Extensions restricts results to these file extensions (already normalised).
	Extensions []string
	// MinSize / MaxSize bound the file size in bytes (0 = unset).
	MinSize uint64
	MaxSize uint64
	// InfoHash, when set, scopes the search to a single torrent (the per-torrent
	// browser path).
	InfoHash *protocol.ID
	// Sort carries optional file-level ordering. Empty means backend default
	// (currently size DESC for the pathsearch adapter; sidecar default for L2).
	Sort []FileSort
	// Limit / Offset paginate the result; Limit is clamped to [1, MaxLimit].
	Limit  uint
	Offset uint
	// SkipTotalCount avoids the exact CountFiles RPC for first-page/low-latency
	// callers. Existing callers default to exact counts.
	SkipTotalCount bool
}

// FileSort is one optional file-search sort key. Supported fields are backend
// dependent; common fields are "size", "path", "extension", "index", and
// "info_hash". Torrent-content fields ("last_seen", "seeders",
// "published_at", "updated_at") are served only by the routed text-search path.
type FileSort struct {
	Field      string
	Descending bool
}

// FileSearchItem is a single matched file.
type FileSearchItem struct {
	InfoHash       protocol.ID
	Index          uint
	Path           string
	Extension      string
	Size           uint64
	TorrentContent dbsearch.TorrentContentResultItem
}

// FileSearchResult is a page of matched files.
type FileSearchResult struct {
	Items       []FileSearchItem
	TotalCount  uint
	HasNextPage bool
}

// PathTypeaheadInput is the validated input to a path typeahead. Build it with
// NewPathTypeaheadInput.
type PathTypeaheadInput struct {
	// Prefix is the (length-capped, escaped) typeahead prefix.
	Prefix string
	// PrefixLikePattern is Prefix with LIKE metacharacters escaped.
	PrefixLikePattern string
	// Limit is clamped to [1, MaxTypeaheadLimit].
	Limit uint
}

// PathTypeaheadResult is the list of suggested path completions.
type PathTypeaheadResult struct {
	Suggestions []string
}

// Client is the contract the resolvers depend on. Implementations: the DV-2/DV-3
// gRPC sidecar client (real), and Disabled() (no-op).
type Client interface {
	FileSearch(ctx context.Context, in FileSearchInput) (FileSearchResult, error)
	PathTypeahead(ctx context.Context, in PathTypeaheadInput) (PathTypeaheadResult, error)
}

// Disabled returns a Client that rejects every call with ErrDisabled. It is the
// safe default used while FileSearchEnabled is OFF.
func Disabled() Client {
	return disabledClient{}
}

type disabledClient struct{}

func (disabledClient) FileSearch(context.Context, FileSearchInput) (FileSearchResult, error) {
	return FileSearchResult{}, ErrDisabled
}

func (disabledClient) PathTypeahead(context.Context, PathTypeaheadInput) (PathTypeaheadResult, error) {
	return PathTypeaheadResult{}, ErrDisabled
}

func NormalizeFileSortField(field string) string {
	field = strings.ToLower(strings.TrimSpace(field))
	switch field {
	case "infohash":
		return FileSortInfoHash
	case "dhtlastseenat":
		return FileSortDHTLastSeenAt
	default:
		return field
	}
}

func IsTorrentFieldSort(field string) bool {
	switch NormalizeFileSortField(field) {
	case FileSortLastSeen, FileSortDHTLastSeenAt, FileSortSeeders, FileSortPublishedAt, FileSortUpdatedAt:
		return true
	default:
		return false
	}
}
