package gqlmodel

import (
	"context"
	"errors"
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
	facetsCalls        int
	pathTypeaheadCalls int
	fileSearchResult   filesearch.FileSearchResult
	facetsInput        filesearch.FacetsInput
	facetsResult       filesearch.FacetsResult
	pathResult         filesearch.PathTypeaheadResult
}

func (c *recordingFileSearchClient) FileSearch(
	context.Context,
	filesearch.FileSearchInput,
) (filesearch.FileSearchResult, error) {
	c.fileSearchCalls++

	return c.fileSearchResult, nil
}

func (c *recordingFileSearchClient) Facets(
	_ context.Context,
	in filesearch.FacetsInput,
) (filesearch.FacetsResult, error) {
	c.facetsCalls++
	c.facetsInput = in

	return c.facetsResult, nil
}

func (c *recordingFileSearchClient) PathTypeahead(
	context.Context,
	filesearch.PathTypeaheadInput,
) (filesearch.PathTypeaheadResult, error) {
	c.pathTypeaheadCalls++

	return c.pathResult, nil
}

type fileRouteL3 struct {
	resp           *pb.PathCandidatesResponse
	calls          int
	suggestResp    *pb.SuggestResponse
	suggestErr     error
	suggestReq     *pb.SuggestRequest
	suggestCalls   int
	panicOnSuggest bool
}

func (f *fileRouteL3) PathCandidates(
	context.Context,
	*pb.PathCandidatesRequest,
) (*pb.PathCandidatesResponse, error) {
	f.calls++

	return f.resp, nil
}

func (f *fileRouteL3) Suggest(
	_ context.Context,
	req *pb.SuggestRequest,
) (*pb.SuggestResponse, error) {
	if f.panicOnSuggest {
		panic("Suggest must not be consulted while FileSearchTypeaheadRPCEnabled is off")
	}

	f.suggestCalls++
	f.suggestReq = req

	return f.suggestResp, f.suggestErr
}

type fileRoutePG struct {
	result    dbsearch.TorrentContentResult
	callCount int
}

func (f *fileRoutePG) TorrentContent(
	context.Context,
	...q.Option,
) (dbsearch.TorrentContentResult, error) {
	f.callCount++

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
			Seeders:  model.NewNullUint(uint(b)),
			Torrent: model.Torrent{
				InfoHash:    fileRouteID(b),
				Name:        path,
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

func enableFileSearchTypeaheadRPCFeature(t *testing.T) {
	t.Helper()

	t.Cleanup(func() { dbsearch.SetFeatureFlags(dbsearch.FeatureFlags{}) })
	dbsearch.SetFeatureFlags(dbsearch.FeatureFlags{
		FileSearchEnabled:             true,
		FileSearchTypeaheadRPCEnabled: true,
	})
}

//nolint:paralleltest // mutates global dbsearch feature flags via enableFileSearchFeature
func TestFileSearchRouteDecision_TextUsesPathsearch(t *testing.T) {
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

	// The L3 route's TotalCount is candidate_total (a torrent-doc recall upper
	// bound), so the estimate flag must survive fileRowsResult conversion — the
	// webui uses it to render "About N files" instead of presenting an estimate
	// as an exact count.
	if !result.TotalCountIsEstimate {
		t.Fatalf("pathsearch (L3) route TotalCountIsEstimate = false, want true")
	}

	if !result.Items[0].TorrentContent.Seeders.Valid || result.Items[0].TorrentContent.Seeders.Uint != 1 {
		t.Fatalf("pathsearch row torrent seeders = %+v, want 1", result.Items[0].TorrentContent.Seeders)
	}
}

//nolint:paralleltest // mutates global dbsearch feature flags via enableFileSearchFeature
func TestFileSearchRouteDecision_EmptyQueryUsesL2(t *testing.T) {
	enableFileSearchFeature(t)

	l2ID := fileRouteID(4)
	l2 := &recordingFileSearchClient{fileSearchResult: filesearch.FileSearchResult{
		Items: []filesearch.FileSearchItem{{InfoHash: l2ID, Path: "l2.mkv"}},
	}}
	l3 := &fileRouteL3{resp: &pb.PathCandidatesResponse{
		Candidates: []*pb.PathCandidate{fileRouteCandidate(1)},
	}}
	pg := &fileRoutePG{result: dbsearch.TorrentContentResult{
		Items: []dbsearch.TorrentContentResultItem{fileRouteItem(4, "l2.mkv", 99)},
	}}

	result, err := (FileSearchQuery{
		Client:               l2,
		Pathsearch:           fileRouteComposer(true, l3, pg),
		TorrentContentSearch: pg,
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

	if pg.callCount != 1 {
		t.Fatalf("L2 result must be hydrated with one TorrentContent query, calls=%d", pg.callCount)
	}

	if !result.Items[0].TorrentContent.Seeders.Valid || result.Items[0].TorrentContent.Seeders.Uint != 4 {
		t.Fatalf("L2 row torrent seeders = %+v, want 4", result.Items[0].TorrentContent.Seeders)
	}
}

//nolint:paralleltest // mutates global dbsearch feature flags via enableFileSearchFeature
func TestFileSearchRouteDecision_EmptyQueryRejectsTorrentSort(t *testing.T) {
	enableFileSearchFeature(t)

	l2 := &recordingFileSearchClient{}
	l3 := &fileRouteL3{resp: &pb.PathCandidatesResponse{
		Candidates: []*pb.PathCandidate{fileRouteCandidate(1)},
	}}
	pg := &fileRoutePG{}

	_, err := (FileSearchQuery{
		Client:               l2,
		Pathsearch:           fileRouteComposer(true, l3, pg),
		TorrentContentSearch: pg,
	}).Search(context.Background(), FileSearchInput{
		Extensions: []string{"mkv"},
		Limit:      10,
		Sort: []filesearch.FileSort{{
			Descending: true,
			Field:      filesearch.FileSortLastSeen,
		}},
	})
	if !errors.Is(err, filesearch.ErrTorrentSortRequiresTextQuery) {
		t.Fatalf("FileSearch err = %v, want ErrTorrentSortRequiresTextQuery", err)
	}

	if l2.fileSearchCalls != 0 {
		t.Fatalf("rejected torrent sort must not call L2, calls=%d", l2.fileSearchCalls)
	}

	if l3.calls != 0 {
		t.Fatalf("empty text must not call pathsearch, calls=%d", l3.calls)
	}
}

//nolint:paralleltest // mutates global dbsearch feature flags via enableFileSearchFeature
func TestFileSearchRouteDecision_FlagOffUsesL2(t *testing.T) {
	enableFileSearchFeature(t)

	l2ID := fileRouteID(5)
	l2 := &recordingFileSearchClient{fileSearchResult: filesearch.FileSearchResult{
		Items: []filesearch.FileSearchItem{{InfoHash: l2ID, Path: "l2-text.mkv"}},
	}}
	l3 := &fileRouteL3{resp: &pb.PathCandidatesResponse{
		Candidates: []*pb.PathCandidate{fileRouteCandidate(1)},
	}}
	pg := &fileRoutePG{result: dbsearch.TorrentContentResult{
		Items: []dbsearch.TorrentContentResultItem{fileRouteItem(5, "l2-text.mkv", 101)},
	}}

	result, err := (FileSearchQuery{
		Client:               l2,
		Pathsearch:           fileRouteComposer(false, l3, pg),
		TorrentContentSearch: pg,
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

//nolint:paralleltest // mutates global dbsearch feature flags via enableFileSearchFeature
func TestPathTypeahead_FlagOffUsesAdapterWithoutConsultingSuggest(t *testing.T) {
	enableFileSearchFeature(t)

	l2 := &recordingFileSearchClient{
		pathResult: filesearch.PathTypeaheadResult{Suggestions: []string{"l2"}},
	}
	l3 := &fileRouteL3{resp: &pb.PathCandidatesResponse{
		Candidates:     []*pb.PathCandidate{fileRouteCandidate(1), fileRouteCandidate(2)},
		CandidateTotal: 2,
		Estimated:      true,
	}, panicOnSuggest: true}
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

	if l3.suggestCalls != 0 {
		t.Fatalf("flag-off typeahead consulted Suggest %d times, want 0", l3.suggestCalls)
	}
}

//nolint:paralleltest // mutates global dbsearch feature flags
func TestPathTypeahead_FlagOnSuggestServedSkipsAdapter(t *testing.T) {
	enableFileSearchTypeaheadRPCFeature(t)

	l2 := &recordingFileSearchClient{
		pathResult: filesearch.PathTypeaheadResult{Suggestions: []string{"l2"}},
	}
	l3 := &fileRouteL3{
		resp: &pb.PathCandidatesResponse{
			Candidates: []*pb.PathCandidate{fileRouteCandidate(1)},
		},
		suggestResp: &pb.SuggestResponse{Suggestions: []*pb.Suggestion{
			{Value: "inception", Score: 20},
			{Value: "interstellar", Score: 10},
		}},
	}
	pg := &fileRoutePG{result: dbsearch.TorrentContentResult{
		Items: []dbsearch.TorrentContentResultItem{
			fileRouteItem(1, "Movies/Inception/movie.mkv", 700),
		},
	}}

	result, err := (FileSearchQuery{
		Client:     l2,
		Pathsearch: fileRouteComposer(true, l3, pg),
	}).PathTypeahead(context.Background(), PathTypeaheadInput{Prefix: "Movies/I", Limit: 2})
	if err != nil {
		t.Fatalf("PathTypeahead: %v", err)
	}

	want := []string{"inception", "interstellar"}
	if len(result.Suggestions) != len(want) {
		t.Fatalf("suggestions = %v, want %v", result.Suggestions, want)
	}

	for i := range want {
		if result.Suggestions[i] != want[i] {
			t.Fatalf("suggestions = %v, want %v", result.Suggestions, want)
		}
	}

	if l3.suggestCalls != 1 || l3.suggestReq.GetPrefix() != "Movies/I" || l3.suggestReq.GetLimit() != 2 {
		t.Fatalf("Suggest calls=%d req=%+v, want one validated request", l3.suggestCalls, l3.suggestReq)
	}

	if l3.calls != 0 || pg.callCount != 0 || l2.pathTypeaheadCalls != 0 {
		t.Fatalf("served Suggest must skip adapter: candidates=%d pg=%d l2=%d",
			l3.calls, pg.callCount, l2.pathTypeaheadCalls)
	}
}

//nolint:paralleltest // mutates global dbsearch feature flags
func TestPathTypeahead_FlagOnSuggestErrorFallsBackToAdapter(t *testing.T) {
	enableFileSearchTypeaheadRPCFeature(t)

	l2 := &recordingFileSearchClient{
		pathResult: filesearch.PathTypeaheadResult{Suggestions: []string{"l2"}},
	}
	l3 := &fileRouteL3{
		suggestErr: errors.New("suggest unavailable"),
		resp: &pb.PathCandidatesResponse{
			Candidates:     []*pb.PathCandidate{fileRouteCandidate(1), fileRouteCandidate(2)},
			CandidateTotal: 2,
			Estimated:      true,
		},
	}
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

	want := []string{"Inception", "Interstellar"}
	if len(result.Suggestions) != len(want) {
		t.Fatalf("suggestions = %v, want adapter result %v", result.Suggestions, want)
	}

	for i := range want {
		if result.Suggestions[i] != want[i] {
			t.Fatalf("suggestions = %v, want adapter result %v", result.Suggestions, want)
		}
	}

	if l3.suggestCalls != 1 || l3.calls != 1 || pg.callCount != 1 {
		t.Fatalf("fallback calls: suggest=%d candidates=%d pg=%d, want 1 each",
			l3.suggestCalls, l3.calls, pg.callCount)
	}

	if l2.pathTypeaheadCalls != 0 {
		t.Fatalf("candidate adapter served fallback, L2 calls=%d want 0", l2.pathTypeaheadCalls)
	}
}
