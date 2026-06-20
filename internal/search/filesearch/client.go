package filesearch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const DefaultMaxRows = 500

// Config configures the L2 file-search sidecar client.
type Config struct {
	// Address of the filesearch sidecar. Either a Unix socket
	// ("unix:///run/bitmagnet/filesearch.sock" or a bare absolute path) or a TCP
	// "host:port". Production default is the ClusterIP
	// "bitmagnet-filesearch.bitmagnet.svc:50052".
	Address string
	// Timeout bounds each unary RPC. <= 0 leaves the timeout to the caller's
	// context.
	Timeout time.Duration
	// MaxRows bounds the row window used for client-side offset emulation while
	// the sidecar cursor is not usable. 0 falls back to DefaultMaxRows.
	MaxRows uint
}

type fileSearchRPC interface {
	SearchFiles(context.Context, *pb.SearchFilesRequest, ...grpc.CallOption) (*pb.SearchFilesResponse, error)
	CountFiles(context.Context, *pb.CountFilesRequest, ...grpc.CallOption) (*pb.CountFilesResponse, error)
	HealthCheck(context.Context, *pb.FileHealthCheckRequest, ...grpc.CallOption) (*pb.FileHealthCheckResponse, error)
}

// SidecarClient is a thin, safe wrapper over the generated FileSearchService.
// It implements Client and is safe for concurrent use.
type SidecarClient struct {
	conn    *grpc.ClientConn
	svc     fileSearchRPC
	timeout time.Duration
	maxRows uint
}

// NewClient dials (lazily — grpc.NewClient connects on first RPC) the L2
// filesearch sidecar at cfg.Address. Use HealthCheck to verify it is serving.
func NewClient(cfg Config) (*SidecarClient, error) {
	target, err := parseTarget(cfg.Address)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("filesearch: dial %q: %w", target, err)
	}

	return &SidecarClient{
		conn:    conn,
		svc:     pb.NewFileSearchServiceClient(conn),
		timeout: cfg.Timeout,
		maxRows: normalizeMaxRows(cfg.MaxRows),
	}, nil
}

func parseTarget(address string) (string, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", errors.New("filesearch: empty address")
	}

	switch {
	case strings.HasPrefix(address, "unix:"):
		return address, nil
	case strings.HasPrefix(address, "/"):
		return "unix://" + address, nil
	default:
		return address, nil
	}
}

func normalizeMaxRows(maxRows uint) uint {
	if maxRows == 0 {
		return DefaultMaxRows
	}

	maxUint32 := uint(^uint32(0))
	if maxRows > maxUint32 {
		return maxUint32
	}

	return maxRows
}

// Close tears down the underlying connection.
func (c *SidecarClient) Close() error {
	return c.conn.Close()
}

func (c *SidecarClient) FileSearch(ctx context.Context, in FileSearchInput) (FileSearchResult, error) {
	if in.InfoHash != nil {
		return FileSearchResult{}, ErrInfoHashUnsupported
	}

	requestLimit, err := c.requestLimit(in)
	if err != nil {
		return FileSearchResult{}, err
	}

	filters := buildFilters(in)
	searchReq := &pb.SearchFilesRequest{
		Filters: filters,
		Pagination: &pb.FilePagination{
			Limit: uint32(requestLimit),
		},
		CollapseToTorrent: false,
	}

	ctx, cancel := c.callCtx(ctx)
	defer cancel()

	resp, err := c.svc.SearchFiles(ctx, searchReq)
	if err != nil {
		return FileSearchResult{}, err
	}

	var totalCount uint
	if !in.SkipTotalCount {
		countReq := &pb.CountFilesRequest{
			Filters:           filters,
			CollapseToTorrent: false,
		}
		count, err := c.svc.CountFiles(ctx, countReq)
		if err != nil {
			return FileSearchResult{}, err
		}
		totalCount = boundedUint(count.GetCount())
	}

	items, err := convertHits(resp.GetFiles())
	if err != nil {
		return FileSearchResult{}, err
	}

	if in.Offset > 0 {
		if in.Offset >= uint(len(items)) {
			items = nil
		} else {
			items = items[in.Offset:]
		}
	}

	if uint(len(items)) > in.Limit {
		items = items[:in.Limit]
	}

	return FileSearchResult{
		Items:       items,
		TotalCount:  totalCount,
		HasNextPage: resp.GetHasNext(),
	}, nil
}

func (c *SidecarClient) PathTypeahead(context.Context, PathTypeaheadInput) (PathTypeaheadResult, error) {
	return PathTypeaheadResult{}, ErrPathTypeaheadUnsupported
}

func (c *SidecarClient) HealthCheck(ctx context.Context) (*pb.FileHealthCheckResponse, error) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()

	return c.svc.HealthCheck(ctx, &pb.FileHealthCheckRequest{})
}

func (c *SidecarClient) requestLimit(in FileSearchInput) (uint, error) {
	limit := in.Limit
	if limit == 0 {
		limit = DefaultLimit
	}

	if in.Offset > c.maxRows || limit > c.maxRows-in.Offset {
		return 0, fmt.Errorf("%w: offset=%d limit=%d max_rows=%d", ErrOffsetUnsupported, in.Offset, limit, c.maxRows)
	}

	return in.Offset + limit, nil
}

func buildFilters(in FileSearchInput) *pb.FileFilters {
	filters := &pb.FileFilters{
		Extensions: in.Extensions,
	}

	if in.Query != "" {
		q := in.Query
		filters.PathQuery = &q
	}

	if in.MinSize > 0 {
		minSize := in.MinSize
		filters.SizeMin = &minSize
	}

	if in.MaxSize > 0 {
		maxSize := in.MaxSize
		filters.SizeMax = &maxSize
	}

	return filters
}

func convertHits(hits []*pb.FileHit) ([]FileSearchItem, error) {
	items := make([]FileSearchItem, 0, len(hits))

	for _, hit := range hits {
		infoHash, err := protocol.ParseID(hit.GetInfoHash())
		if err != nil {
			return nil, fmt.Errorf("filesearch: parse info_hash %q: %w", hit.GetInfoHash(), err)
		}

		items = append(items, FileSearchItem{
			InfoHash:  infoHash,
			Index:     uint(hit.GetFileIndex()),
			Path:      hit.GetPath(),
			Extension: hit.GetExtension(),
			Size:      hit.GetSize(),
		})
	}

	return items, nil
}

func boundedUint(v uint64) uint {
	maxUint := ^uint(0)
	if v > uint64(maxUint) {
		return maxUint
	}

	return uint(v)
}

func (c *SidecarClient) callCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, c.timeout)
}
