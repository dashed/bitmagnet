package gqlmodel

import (
	"context"
	"testing"

	q "github.com/bitmagnet-io/bitmagnet/internal/database/query"
	dbsearch "github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/filesearch"
	"github.com/bitmagnet-io/bitmagnet/internal/search/pathsearch"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
)

type recordingFileSearchClient struct {
	fileSearchCalls    int
	pathTypeaheadCalls int
	fileSearchResult   filesearch.FileSearchResult
	pathResult         filesearch.PathTypeaheadResult
}

func (c *recordingFileSearchClient) FileSearch(
	context.Context,
	filesearch.FileSearchInput,
) (filesearch.FileSearchResult, error) {
	c.fileSearchCalls++

	return c.fileSearchResult, nil
}

func (c *recordingFileSearchClient) PathTypeahead(
	context.Context,
	filesearch.PathTypeaheadInput,
) (filesearch.PathTypeaheadResult, error) {
	c.pathTypeaheadCalls++

	return c.pathResult, nil
}

type fileRouteL3 struct {
	resp  *pb.PathCandidatesResponse
	calls int
}

func (f *fileRouteL3) PathCandidates(
	context.Context,
	*pb.PathCandidatesRequest,
) (*pb.PathCandidatesResponse, error) {
	f.calls++

	return f.resp, nil
}

type fileRoutePG struct {
	result dbsearch.TorrentContentResult
}

func (f *fileRoutePG) TorrentContent(
	context.Context,
	...q.Option,
) (dbsearch.TorrentContentResult, error) {
	return f.result, nil
}

func (*fileRoutePG) FileCounts(_ context.Context, ids []protocol.ID) (map[protocol.ID]int, error) {
	out := make(map[protocol.ID]int, len(ids))
	for _, id := range ids {
		out[id] = 1
	}

	return out, nil
}

func fileRouteID(b byte) protocol.ID {
	var id protocol.ID
	id[0] = b

	return id
}

func fileRouteCandidate(b byte) *pb.PathCandidate {
	id := fileRouteID(b)

	return &pb.PathCandidate{InfoHash: id.Bytes()}
}

func fileRouteItem(b byte, path string, size uint) dbsearch.TorrentContentResultItem {
	return dbsearch.TorrentContentResultItem{
		TorrentContent: model.TorrentContent{
			InfoHash: fileRouteID(b),
			Torrent: model.Torrent{
				InfoHash:    fileRouteID(b),
				FilesStatus: model.FilesStatusMulti,
				Files: []model.TorrentFile{{
					Index:     7,
					Path:      path,
					Extension: model.NewNullString("mkv"),
					Size:      size,
				}},
			},
		},
	}
}

func fileRouteComposer(
	routeText bool,
	l3 *fileRouteL3,
	pg *fileRoutePG,
) *pathsearch.Composer {
	return pathsearch.NewComposer(
		l3,
		pg,
		pathsearch.ComposerConfig{
			MinQueryLength:      3,
			OversampleFactor:    1,
			MaxCandidates:       100,
			TypeaheadEnabled:    true,
			FileSearchRouteText: routeText,
		},
		nil,
		pathsearch.WithHealthGate(func() bool { return true }),
	)
}

func enableFileSearchFeature(t *testing.T) {
	t.Helper()

	t.Cleanup(func() { dbsearch.SetFeatureFlags(dbsearch.FeatureFlags{}) })
	dbsearch.SetFeatureFlags(dbsearch.FeatureFlags{FileSearchEnabled: true})
}

func TestFileSearchRouteDecision_TextUsesPathsearch(t *testing.T) {
	t.Parallel()

	enableFileSearchFeature(t)

	l2 := &recordingFileSearchClient{fileSearchResult: filesearch.FileSearchResult{
		Items: []filesearch.FileSearchItem{{Path: "l2"}},
	}}
	l3 := &fileRouteL3{resp: &pb.PathCandidatesResponse{
		Candidates:     []*pb.PathCandidate{fileRouteCandidate(1)},
		CandidateTotal: 11,
		Estimated:      true,
	}}
	pg := &fileRoutePG{result: dbsearch.TorrentContentResult{
		Items: []dbsearch.TorrentContentResultItem{
			fileRouteItem(1, "Movies/Inception.2010.mkv", 700),
		},
	}}

	result, err := (FileSearchQuery{
		Client:     l2,
		Pathsearch: fileRouteComposer(true, l3, pg),
	}).Search(context.Background(), FileSearchInput{Query: "inception", Limit: 10})
	if err != nil {
		t.Fatalf("FileSearch: %v", err)
	}

	if l2.fileSearchCalls != 0 {
		t.Fatalf("text query must not call L2 when pathsearch route is enabled, calls=%d", l2.fileSearchCalls)
	}

	if l3.calls != 1 {
		t.Fatalf("text query must call pathsearch once, calls=%d", l3.calls)
	}

	if result.TotalCount != 11 || len(result.Items) != 1 || result.Items[0].Path != "Movies/Inception.2010.mkv" {
		t.Fatalf("pathsearch result = %+v, want candidate_total=11 and refined file row", result)
	}
}

func TestFileSearchRouteDecision_EmptyQueryUsesL2(t *testing.T) {
	t.Parallel()

	enableFileSearchFeature(t)

	l2 := &recordingFileSearchClient{fileSearchResult: filesearch.FileSearchResult{
		Items: []filesearch.FileSearchItem{{Path: "l2.mkv"}},
	}}
	l3 := &fileRouteL3{resp: &pb.PathCandidatesResponse{
		Candidates: []*pb.PathCandidate{fileRouteCandidate(1)},
	}}
	pg := &fileRoutePG{}

	result, err := (FileSearchQuery{
		Client:     l2,
		Pathsearch: fileRouteComposer(true, l3, pg),
	}).Search(context.Background(), FileSearchInput{Extensions: []string{"mkv"}, Limit: 10})
	if err != nil {
		t.Fatalf("FileSearch: %v", err)
	}

	if l2.fileSearchCalls != 1 {
		t.Fatalf("empty text with structured filters must call L2 once, calls=%d", l2.fileSearchCalls)
	}

	if l3.calls != 0 {
		t.Fatalf("empty text must not call pathsearch, calls=%d", l3.calls)
	}

	if len(result.Items) != 1 || result.Items[0].Path != "l2.mkv" {
		t.Fatalf("result = %+v, want L2 marker row", result)
	}
}

func TestFileSearchRouteDecision_FlagOffUsesL2(t *testing.T) {
	t.Parallel()

	enableFileSearchFeature(t)

	l2 := &recordingFileSearchClient{fileSearchResult: filesearch.FileSearchResult{
		Items: []filesearch.FileSearchItem{{Path: "l2-text.mkv"}},
	}}
	l3 := &fileRouteL3{resp: &pb.PathCandidatesResponse{
		Candidates: []*pb.PathCandidate{fileRouteCandidate(1)},
	}}
	pg := &fileRoutePG{}

	result, err := (FileSearchQuery{
		Client:     l2,
		Pathsearch: fileRouteComposer(false, l3, pg),
	}).Search(context.Background(), FileSearchInput{Query: "inception", Limit: 10})
	if err != nil {
		t.Fatalf("FileSearch: %v", err)
	}

	if l2.fileSearchCalls != 1 {
		t.Fatalf("flag-off text query must call L2 once, calls=%d", l2.fileSearchCalls)
	}

	if l3.calls != 0 {
		t.Fatalf("flag-off text query must not call pathsearch, calls=%d", l3.calls)
	}

	if len(result.Items) != 1 || result.Items[0].Path != "l2-text.mkv" {
		t.Fatalf("result = %+v, want L2 marker row", result)
	}
}

func TestPathTypeahead_UsesPathsearchWhenEnabled(t *testing.T) {
	t.Parallel()

	enableFileSearchFeature(t)

	l2 := &recordingFileSearchClient{
		pathResult: filesearch.PathTypeaheadResult{Suggestions: []string{"l2"}},
	}
	l3 := &fileRouteL3{resp: &pb.PathCandidatesResponse{
		Candidates:     []*pb.PathCandidate{fileRouteCandidate(1), fileRouteCandidate(2)},
		CandidateTotal: 2,
		Estimated:      true,
	}}
	pg := &fileRoutePG{result: dbsearch.TorrentContentResult{
		Items: []dbsearch.TorrentContentResultItem{
			fileRouteItem(1, "Movies/Inception/movie.mkv", 700),
			fileRouteItem(2, "Movies/Interstellar/movie.mkv", 700),
		},
	}}

	result, err := (FileSearchQuery{
		Client:     l2,
		Pathsearch: fileRouteComposer(true, l3, pg),
	}).PathTypeahead(context.Background(), PathTypeaheadInput{Prefix: "Movies/I", Limit: 2})
	if err != nil {
		t.Fatalf("PathTypeahead: %v", err)
	}

	if l2.pathTypeaheadCalls != 0 {
		t.Fatalf("pathsearch-enabled typeahead must not call L2, calls=%d", l2.pathTypeaheadCalls)
	}

	want := []string{"Inception", "Interstellar"}
	if len(result.Suggestions) != len(want) {
		t.Fatalf("suggestions = %v, want %v", result.Suggestions, want)
	}

	for i := range want {
		if result.Suggestions[i] != want[i] {
			t.Fatalf("suggestions = %v, want %v", result.Suggestions, want)
		}
	}
}
