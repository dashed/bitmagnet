package filesearch

import (
	"context"
	"errors"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type fakeRPC struct {
	searchReq *pb.SearchFilesRequest
	countReq  *pb.CountFilesRequest
	facetsReq *pb.FacetsRequest
	healthReq *pb.FileHealthCheckRequest

	searchResp *pb.SearchFilesResponse
	countResp  *pb.CountFilesResponse
	facetsResp *pb.FacetsResponse
	healthResp *pb.FileHealthCheckResponse

	searchErr error
	countErr  error
	facetsErr error
}

func (f *fakeRPC) SearchFiles(
	_ context.Context,
	req *pb.SearchFilesRequest,
	_ ...grpc.CallOption,
) (*pb.SearchFilesResponse, error) {
	f.searchReq = req
	return f.searchResp, f.searchErr
}

func (f *fakeRPC) CountFiles(
	_ context.Context,
	req *pb.CountFilesRequest,
	_ ...grpc.CallOption,
) (*pb.CountFilesResponse, error) {
	f.countReq = req
	return f.countResp, f.countErr
}

func (f *fakeRPC) Facets(
	_ context.Context,
	req *pb.FacetsRequest,
	_ ...grpc.CallOption,
) (*pb.FacetsResponse, error) {
	f.facetsReq = req
	return f.facetsResp, f.facetsErr
}

func (f *fakeRPC) HealthCheck(
	_ context.Context,
	req *pb.FileHealthCheckRequest,
	_ ...grpc.CallOption,
) (*pb.FileHealthCheckResponse, error) {
	f.healthReq = req
	return f.healthResp, nil
}

func TestParseTarget(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"bitmagnet-filesearch.bitmagnet.svc:50052", "bitmagnet-filesearch.bitmagnet.svc:50052", false},
		{"127.0.0.1:50052", "127.0.0.1:50052", false},
		{"unix:/run/bitmagnet/filesearch.sock", "unix:/run/bitmagnet/filesearch.sock", false},
		{"unix:///run/bitmagnet/filesearch.sock", "unix:///run/bitmagnet/filesearch.sock", false},
		{"/run/bitmagnet/filesearch.sock", "unix:///run/bitmagnet/filesearch.sock", false},
		{"  127.0.0.1:50052  ", "127.0.0.1:50052", false},
		{"", "", true},
		{"   ", "", true},
	} {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()

			got, err := parseTarget(tc.in)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestNewClientLazyDialAndClose(t *testing.T) {
	t.Parallel()

	c, err := NewClient(Config{Address: "127.0.0.1:50052"})
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.NoError(t, c.Close())
}

func TestNewClientEmptyAddressErrors(t *testing.T) {
	t.Parallel()

	_, err := NewClient(Config{Address: ""})
	assert.Error(t, err)
}

func TestFileSearchSendsRawPathQueryNotLikePattern(t *testing.T) {
	t.Parallel()

	rpc := &fakeRPC{
		searchResp: &pb.SearchFilesResponse{},
		countResp:  &pb.CountFilesResponse{},
	}
	c := &SidecarClient{svc: rpc, maxRows: DefaultMaxRows}

	_, err := c.FileSearch(context.Background(), FileSearchInput{
		Query:            "50%_raw",
		QueryLikePattern: `50\%\_raw`,
		Extensions:       []string{"mkv"},
		MinSize:          10,
		MaxSize:          20,
		Limit:            5,
	})
	require.NoError(t, err)

	require.NotNil(t, rpc.searchReq)
	filters := rpc.searchReq.GetFilters()
	require.NotNil(t, filters)
	assert.Equal(t, "50%_raw", filters.GetPathQuery())
	assert.Equal(t, []string{"mkv"}, filters.GetExtensions())
	assert.Equal(t, uint64(10), filters.GetSizeMin())
	assert.Equal(t, uint64(20), filters.GetSizeMax())
	assert.False(t, rpc.searchReq.GetCollapseToTorrent())
	assert.Equal(t, uint32(5), rpc.searchReq.GetPagination().GetLimit())

	require.NotNil(t, rpc.countReq)
	assert.False(t, rpc.countReq.GetCollapseToTorrent())
	assert.Equal(t, "50%_raw", rpc.countReq.GetFilters().GetPathQuery())
}

func TestFileSearchMapsRowsOffsetHasNextAndCount(t *testing.T) {
	t.Parallel()

	id1 := protocol.MustParseID("1111111111111111111111111111111111111111")
	id2 := protocol.MustParseID("2222222222222222222222222222222222222222")
	id3 := protocol.MustParseID("3333333333333333333333333333333333333333")
	rpc := &fakeRPC{
		searchResp: &pb.SearchFilesResponse{
			Files: []*pb.FileHit{
				{InfoHash: id1.String(), FileIndex: 1, Path: "skip.mkv", Extension: "mkv", Size: 100},
				{InfoHash: id2.String(), FileIndex: 2, Path: "keep.mp4", Extension: "mp4", Size: 200},
				{InfoHash: id3.String(), FileIndex: 3, Path: "also.mkv", Extension: "mkv", Size: 300},
			},
			HasNext: true,
		},
		countResp: &pb.CountFilesResponse{Count: 42},
	}
	c := &SidecarClient{svc: rpc, maxRows: DefaultMaxRows}

	got, err := c.FileSearch(context.Background(), FileSearchInput{
		Query:  "keep",
		Limit:  2,
		Offset: 1,
	})
	require.NoError(t, err)

	assert.Equal(t, uint32(3), rpc.searchReq.GetPagination().GetLimit())
	assert.Equal(t, uint(42), got.TotalCount)
	assert.True(t, got.HasNextPage)
	require.Len(t, got.Items, 2)
	assert.Equal(t, id2, got.Items[0].InfoHash)
	assert.Equal(t, uint(2), got.Items[0].Index)
	assert.Equal(t, "keep.mp4", got.Items[0].Path)
	assert.Equal(t, uint64(200), got.Items[0].Size)
	assert.Equal(t, id3, got.Items[1].InfoHash)
}

func TestFileSearchSkipsCountWhenRequested(t *testing.T) {
	t.Parallel()

	id := protocol.MustParseID("1111111111111111111111111111111111111111")
	rpc := &fakeRPC{
		searchResp: &pb.SearchFilesResponse{
			Files: []*pb.FileHit{
				{InfoHash: id.String(), FileIndex: 1, Path: "fast.mkv", Extension: "mkv", Size: 100},
			},
			HasNext: true,
		},
		countResp: &pb.CountFilesResponse{Count: 42},
	}
	c := &SidecarClient{svc: rpc, maxRows: DefaultMaxRows}

	got, err := c.FileSearch(context.Background(), FileSearchInput{
		Query:          "fast",
		Limit:          1,
		SkipTotalCount: true,
	})
	require.NoError(t, err)

	require.NotNil(t, rpc.searchReq)
	assert.Nil(t, rpc.countReq)
	assert.Equal(t, uint(0), got.TotalCount)
	assert.True(t, got.HasNextPage)
	require.Len(t, got.Items, 1)
	assert.Equal(t, id, got.Items[0].InfoHash)
}

func TestFileSearchRejectsUnsupportedRequestShapes(t *testing.T) {
	t.Parallel()

	id := protocol.MustParseID("1111111111111111111111111111111111111111")
	c := &SidecarClient{svc: &fakeRPC{}, maxRows: 10}

	_, err := c.FileSearch(context.Background(), FileSearchInput{
		Query:    "x",
		InfoHash: &id,
		Limit:    1,
	})
	require.ErrorIs(t, err, ErrInfoHashUnsupported)

	_, err = c.FileSearch(context.Background(), FileSearchInput{
		Query:  "x",
		Limit:  5,
		Offset: 6,
	})
	require.ErrorIs(t, err, ErrOffsetUnsupported)
}

func TestFileSearchPropagatesRPCError(t *testing.T) {
	t.Parallel()

	want := errors.New("unavailable")
	c := &SidecarClient{
		svc:     &fakeRPC{searchErr: want},
		maxRows: DefaultMaxRows,
	}

	_, err := c.FileSearch(context.Background(), FileSearchInput{Query: "x", Limit: 1})
	assert.ErrorIs(t, err, want)
}

func TestFacetsSendsFiltersAndPreservesBucketOrder(t *testing.T) {
	t.Parallel()

	rpc := &fakeRPC{facetsResp: &pb.FacetsResponse{Facets: []*pb.FileFacet{
		{
			Field: "extension",
			Buckets: []*pb.FileFacetBucket{
				{Value: "mkv", Count: 7, TotalSize: 700},
				{Value: "mp4", Count: 3, TotalSize: 300},
			},
		},
	}}}
	c := &SidecarClient{svc: rpc, maxRows: DefaultMaxRows}

	got, err := c.Facets(context.Background(), FacetsInput{
		Query:            "50%_raw",
		QueryLikePattern: `50\%\_raw`,
		Extensions:       []string{"mkv", "mp4"},
		MinSize:          10,
		MaxSize:          20,
		Fields:           []string{"extension"},
	})
	require.NoError(t, err)

	require.NotNil(t, rpc.facetsReq)
	assert.Equal(t, []string{"extension"}, rpc.facetsReq.GetFacetFields())
	assert.Equal(t, "50%_raw", rpc.facetsReq.GetFilters().GetPathQuery())
	assert.Equal(t, []string{"mkv", "mp4"}, rpc.facetsReq.GetFilters().GetExtensions())
	assert.Equal(t, uint64(10), rpc.facetsReq.GetFilters().GetSizeMin())
	assert.Equal(t, uint64(20), rpc.facetsReq.GetFilters().GetSizeMax())

	require.Len(t, got.Facets, 1)
	assert.Equal(t, "extension", got.Facets[0].Field)
	require.Len(t, got.Facets[0].Buckets, 2)
	assert.Equal(t, "mkv", got.Facets[0].Buckets[0].Value)
	assert.Equal(t, uint64(7), got.Facets[0].Buckets[0].Count)
	assert.Equal(t, uint64(700), got.Facets[0].Buckets[0].TotalSize)
	assert.Equal(t, "mp4", got.Facets[0].Buckets[1].Value)
}

func TestPathTypeaheadUnsupportedUntilProtoExists(t *testing.T) {
	t.Parallel()

	c := &SidecarClient{svc: &fakeRPC{}, maxRows: DefaultMaxRows}
	_, err := c.PathTypeahead(context.Background(), PathTypeaheadInput{Prefix: "ab", Limit: 5})
	assert.ErrorIs(t, err, ErrPathTypeaheadUnsupported)
}
