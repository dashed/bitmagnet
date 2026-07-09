package gqlmodel

import (
	"context"
	"errors"
	"testing"

	q "github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/pathsearch"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
)

// --- collapse:path resolver fakes --------------------------------------------

// collapseL3 is a fake L3 candidate source returning a fixed response. It
// satisfies the composer's internal candidateSource (exported PathCandidates).
type collapseL3 struct {
	resp *pb.PathCandidatesResponse
	err  error
}

func (f *collapseL3) PathCandidates(
	_ context.Context,
	_ *pb.PathCandidatesRequest,
) (*pb.PathCandidatesResponse, error) {
	return f.resp, f.err
}

// collapsePG is a fake composer-side searcher returning fixed items. FileCounts
// reports 1 file per candidate so the whole set fits a single refine chunk.
type collapsePG struct {
	result search.TorrentContentResult
}

func (f *collapsePG) TorrentContent(
	_ context.Context,
	_ ...q.Option,
) (search.TorrentContentResult, error) {
	return f.result, nil
}

func (*collapsePG) FileCounts(_ context.Context, ids []protocol.ID) (map[protocol.ID]int, error) {
	out := make(map[protocol.ID]int, len(ids))
	for _, id := range ids {
		out[id] = 1
	}

	return out, nil
}

func collapseIH(b byte) protocol.ID {
	var id protocol.ID
	id[0] = b

	return id
}

func collapseCandidate(b byte) *pb.PathCandidate {
	id := collapseIH(b)

	return &pb.PathCandidate{InfoHash: id.Bytes()}
}

// singleFileItem builds a single-file candidate whose one path IS its name, so
// the composer's name surrogate supplies the matched path with no blob decode.
func singleFileItem(b byte, name string) search.TorrentContentResultItem {
	return search.TorrentContentResultItem{
		TorrentContent: model.TorrentContent{
			InfoHash: collapseIH(b),
			Torrent: model.Torrent{
				InfoHash:    collapseIH(b),
				FilesStatus: model.FilesStatusSingle,
				Name:        name,
			},
		},
	}
}

// collapseComposer builds a Composer wired with the given fakes, collapse enabled,
// and a health gate fixed to `healthy`.
func collapseComposer(
	l3 *collapseL3,
	pg *collapsePG,
	healthy bool,
) *pathsearch.Composer {
	return pathsearch.NewComposer(
		l3,
		pg,
		pathsearch.ComposerConfig{
			CollapseEnabled:  true,
			MinQueryLength:   3,
			OversampleFactor: 4,
			MaxCandidates:    1000,
		},
		nil,
		pathsearch.WithHealthGate(func() bool { return healthy }),
	)
}

// --- tests -------------------------------------------------------------------

func TestClampCollapseLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		limit uint
		want  uint
	}{
		{name: "hostile", limit: 1_000_000, want: maxPathSearchLimit},
		{name: "omitted", limit: 0, want: defaultPageSize},
		{name: "in range", limit: 50, want: 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := clampCollapseLimit(tt.limit); got != tt.want {
				t.Fatalf("clampCollapseLimit(%d) = %d, want %d", tt.limit, got, tt.want)
			}
		})
	}
}

// A served collapse result maps composer PathGroups (path + raw info hashes) onto
// the GraphQL model. Two single-file candidates sharing one path collapse into a
// single group carrying both info hashes, in candidate order.
func TestCollapsePaths_ServedMapsGroups(t *testing.T) {
	t.Parallel()

	l3 := &collapseL3{resp: &pb.PathCandidatesResponse{
		Candidates: []*pb.PathCandidate{collapseCandidate(1), collapseCandidate(2)},
	}}
	pg := &collapsePG{result: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		singleFileItem(1, "Inception.2010.mkv"),
		singleFileItem(2, "Inception.2010.mkv"), // same path, different torrent -> one group
	}}}

	tcq := TorrentContentQuery{Pathsearch: collapseComposer(l3, pg, true)}

	res, err := tcq.CollapsePaths(context.Background(), TorrentContentCollapsePathsInput{QueryString: "inception"})
	if err != nil {
		t.Fatalf("CollapsePaths: %v", err)
	}

	if len(res.Groups) != 1 {
		t.Fatalf("expected 1 collapsed group, got %d: %+v", len(res.Groups), res.Groups)
	}

	g := res.Groups[0]
	if g.Path != "Inception.2010.mkv" {
		t.Fatalf("group path = %q, want %q", g.Path, "Inception.2010.mkv")
	}

	wantHashes := []protocol.ID{collapseIH(1), collapseIH(2)}
	if len(g.InfoHashes) != len(wantHashes) {
		t.Fatalf("group info hashes = %v, want %v", g.InfoHashes, wantHashes)
	}

	for i, h := range wantHashes {
		if g.InfoHashes[i] != h {
			t.Fatalf("info hash[%d] = %v, want %v", i, g.InfoHashes[i], h)
		}
	}
}

// A nil composer (feature off) is a hard error, not a silent empty — there is no
// PostgreSQL fallback for collapse:path.
func TestCollapsePaths_NilComposerErrors(t *testing.T) {
	t.Parallel()

	var tcq TorrentContentQuery // Pathsearch nil

	_, err := tcq.CollapsePaths(context.Background(), TorrentContentCollapsePathsInput{QueryString: "inception"})
	if !errors.Is(err, ErrPathCollapseUnavailable) {
		t.Fatalf("nil composer must return ErrPathCollapseUnavailable, got %v", err)
	}
}

// Collapse disabled on an otherwise-healthy composer is a hard error.
func TestCollapsePaths_DisabledErrors(t *testing.T) {
	t.Parallel()

	composer := pathsearch.NewComposer(
		&collapseL3{resp: &pb.PathCandidatesResponse{}},
		&collapsePG{},
		pathsearch.ComposerConfig{CollapseEnabled: false, MinQueryLength: 3},
		nil,
		pathsearch.WithHealthGate(func() bool { return true }),
	)
	tcq := TorrentContentQuery{Pathsearch: composer}

	_, err := tcq.CollapsePaths(context.Background(), TorrentContentCollapsePathsInput{QueryString: "inception"})
	if !errors.Is(err, ErrPathCollapseUnavailable) {
		t.Fatalf("disabled collapse must return ErrPathCollapseUnavailable, got %v", err)
	}
}

// An unhealthy L3 is a hard error: collapse never serves against a known-bad index
// and there is no fallback.
func TestCollapsePaths_UnhealthyErrors(t *testing.T) {
	t.Parallel()

	l3 := &collapseL3{resp: &pb.PathCandidatesResponse{Candidates: []*pb.PathCandidate{collapseCandidate(1)}}}
	pg := &collapsePG{result: search.TorrentContentResult{Items: []search.TorrentContentResultItem{
		singleFileItem(1, "Inception.2010.mkv"),
	}}}

	tcq := TorrentContentQuery{Pathsearch: collapseComposer(l3, pg, false)}

	_, err := tcq.CollapsePaths(context.Background(), TorrentContentCollapsePathsInput{QueryString: "inception"})
	if !errors.Is(err, ErrPathCollapseUnavailable) {
		t.Fatalf("unhealthy composer must return ErrPathCollapseUnavailable, got %v", err)
	}
}

// A query below the composer's min length is a distinct client error (there is no
// PG fallback to absorb it as the search route does).
func TestCollapsePaths_QueryTooShortErrors(t *testing.T) {
	t.Parallel()

	tcq := TorrentContentQuery{Pathsearch: collapseComposer(
		&collapseL3{resp: &pb.PathCandidatesResponse{}},
		&collapsePG{},
		true,
	)}

	_, err := tcq.CollapsePaths(context.Background(), TorrentContentCollapsePathsInput{QueryString: "ab"})
	if !errors.Is(err, ErrPathCollapseQueryTooShort) {
		t.Fatalf("too-short query must return ErrPathCollapseQueryTooShort, got %v", err)
	}
}

// A composer/sidecar error propagates as a GraphQL error, never a silent empty.
func TestCollapsePaths_ComposerErrorPropagates(t *testing.T) {
	t.Parallel()

	sidecarErr := errors.New("sidecar down")
	tcq := TorrentContentQuery{Pathsearch: collapseComposer(
		&collapseL3{err: sidecarErr},
		&collapsePG{},
		true,
	)}

	_, err := tcq.CollapsePaths(context.Background(), TorrentContentCollapsePathsInput{QueryString: "inception"})
	if !errors.Is(err, sidecarErr) {
		t.Fatalf("composer error must propagate, got %v", err)
	}
}
