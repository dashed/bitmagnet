// Package pathsearch is the Go client + backend composition for the L3
// per-torrent path-bag candidate sidecar (bitmagnet-rs PathSearchService). L3 is
// a RECALL engine: PathCandidates returns torrent-grained info_hash candidates
// for free-text path queries. It is NEVER the exact source — the Composer
// narrows the candidate set to the real path substring (+ extension/size) via
// in-process L1 blob decode before serving rows. See composer.go and refine.go.
//
// The whole feature is gated by SEARCH_PATHSEARCH_ENABLED (and friends) and is
// disabled by default: when off, the client/composer are never constructed and
// the backend behaves byte-identically to today.
package pathsearch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Config configures the sidecar Client. The wiring layer (searchfx) supplies it.
type Config struct {
	// Address of the pathsearch sidecar. Either a Unix socket
	// ("unix:///run/bitmagnet/pathsearch.sock" or a bare absolute path) or a TCP
	// "host:port". Production default is the ClusterIP
	// "bitmagnet-pathsearch.bitmagnet.svc:50053".
	Address string
	// Timeout bounds each unary RPC. <= 0 leaves the timeout to the caller's
	// context.
	Timeout time.Duration
}

// Client is a thin, safe wrapper over the generated PathSearchServiceClient. It
// is safe for concurrent use: the underlying *grpc.ClientConn multiplexes calls.
type Client struct {
	conn    *grpc.ClientConn
	svc     pb.PathSearchServiceClient
	timeout time.Duration
}

// NewClient dials (lazily — grpc.NewClient connects on first RPC) the pathsearch
// sidecar at cfg.Address. Use HealthCheck to verify it is actually serving.
func NewClient(cfg Config) (*Client, error) {
	target, err := parseTarget(cfg.Address)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("pathsearch: dial %q: %w", target, err)
	}

	return &Client{
		conn:    conn,
		svc:     pb.NewPathSearchServiceClient(conn),
		timeout: cfg.Timeout,
	}, nil
}

// parseTarget normalises a config address into a gRPC target string, choosing
// the Unix-socket or TCP transport (grpc-go's built-in "unix" and default "dns"
// resolvers do the rest). Mirrors the tantivy client so both sidecars dial with
// identical semantics.
func parseTarget(address string) (string, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", errors.New("pathsearch: empty address")
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

// Close tears down the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// PathCandidates asks L3 for an oversampled set of torrent-grained info_hash
// candidates for the free-text path query. The returned candidate_total is a
// torrent-doc recall count, not an exact matching-file count, and estimated is
// always true — the caller exact-refines and never presents these as file
// counts.
func (c *Client) PathCandidates(
	ctx context.Context,
	req *pb.PathCandidatesRequest,
) (*pb.PathCandidatesResponse, error) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()

	return c.svc.PathCandidates(ctx, req)
}

// Suggest asks L3's prefix index for path-segment completions.
func (c *Client) Suggest(ctx context.Context, req *pb.SuggestRequest) (*pb.SuggestResponse, error) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()

	return c.svc.Suggest(ctx, req)
}

// HealthCheck probes the sidecar's serving status, doc count, index size, and
// follow watermark. It reuses the shared empty HealthCheckRequest.
func (c *Client) HealthCheck(ctx context.Context) (*pb.PathSearchHealth, error) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()

	return c.svc.HealthCheck(ctx, &pb.HealthCheckRequest{})
}

// callCtx applies the per-call timeout when configured, otherwise returns ctx
// unchanged with a no-op cancel.
func (c *Client) callCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, c.timeout)
}
