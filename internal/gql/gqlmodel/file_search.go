package gqlmodel

import (
	"context"

	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/filesearch"
)

// FileSearchQuery is the resolver-facing entry point for the file-grained search
// (DV-2 DuckDB sidecar) and path typeahead (DV-3 path-FTS sidecar). It is wired
// behind the transport-neutral filesearch.Client interface so the real gRPC
// client can be injected later without touching this layer.
//
// It is gated twice over: the FileSearchEnabled feature flag (default OFF) AND
// the injected Client (filesearch.Disabled() by default). Either being off means
// every call returns filesearch.ErrDisabled — the feature is dark until both the
// sidecar is deployed and the flag is flipped.
//
// NOTE: this type intentionally is NOT yet bound into the GraphQL schema. Adding
// `fileSearch` / `pathTypeahead` fields to graphql/schema/*.graphqls and running
// gqlgen is the final, trivial wiring step, deferred until the DV-2/DV-3 protos
// are frozen (see dv4-go-integration-notes.md §GraphQL wiring).
type FileSearchQuery struct {
	Client filesearch.Client
}

// FileSearchInput is the GraphQL-facing input (loosely typed). Validation,
// LIKE-escaping and clamping happen in filesearch.NewFileSearchInput.
type FileSearchInput struct {
	Query      string
	Extensions []string
	MinSize    uint64
	MaxSize    uint64
	InfoHash   *protocol.ID
	Limit      uint
	Offset     uint
}

func (q FileSearchQuery) client() filesearch.Client {
	if q.Client == nil {
		return filesearch.Disabled()
	}

	return q.Client
}

// Search runs a file-grained search. Returns filesearch.ErrDisabled unless the
// FileSearchEnabled flag is on AND a real client is wired.
func (q FileSearchQuery) Search(ctx context.Context, in FileSearchInput) (filesearch.FileSearchResult, error) {
	if !search.FeatureFlagsValue().FileSearchEnabled {
		return filesearch.FileSearchResult{}, filesearch.ErrDisabled
	}

	validated, err := filesearch.NewFileSearchInput(filesearch.FileSearchParams{
		Query:      in.Query,
		Extensions: in.Extensions,
		MinSize:    in.MinSize,
		MaxSize:    in.MaxSize,
		InfoHash:   in.InfoHash,
		Limit:      in.Limit,
		Offset:     in.Offset,
	})
	if err != nil {
		return filesearch.FileSearchResult{}, err
	}

	return q.client().FileSearch(ctx, validated)
}

// PathTypeahead returns path completions for a prefix. Returns
// filesearch.ErrDisabled unless the flag is on AND a real client is wired, and
// filesearch.ErrPrefixTooShort for prefixes under the min-chars threshold.
func (q FileSearchQuery) PathTypeahead(ctx context.Context, prefix string, limit uint) (filesearch.PathTypeaheadResult, error) {
	if !search.FeatureFlagsValue().FileSearchEnabled {
		return filesearch.PathTypeaheadResult{}, filesearch.ErrDisabled
	}

	validated, err := filesearch.NewPathTypeaheadInput(prefix, limit)
	if err != nil {
		return filesearch.PathTypeaheadResult{}, err
	}

	return q.client().PathTypeahead(ctx, validated)
}
